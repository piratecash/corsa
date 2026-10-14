// Package netcoretest provides an in-memory implementation of
// netcore.Network for driving protocol-logic unit tests without opening
// real TCP sockets.
//
// The public shape mirrors Go's stdlib convention for test helpers
// (net/http → net/http/httptest). Consumers construct a Backend, register
// virtual connections with explicit ConnID/Direction/RemoteAddr, wire the
// Backend into a Service via node.NewServiceWithNetwork, and then observe
// frames the Service sends on the per-ConnID Outbound channel / inject
// inbound frames through Inject.
//
// Naming note.
//   - The lifecycle shutdown method is named Shutdown(), not Close(),
//     because netcore.Network already pins Close(ctx, id) as the
//     per-connection close. Go does not allow two methods with the same
//     name on one type, so the Backend-wide shutdown gets a distinct name.
//
// Invariants the Backend enforces:
//   - On every case RunNetworkContract (contract.go) covers, the Backend
//     answers as the production networkBridge does: the same suite runs
//     against the Backend here and against the bridge in package node. Any
//     semantic drift from the bridge is a Backend bug, not a test bug. The
//     cases the suite excludes, and why, are listed on it; they are
//     differences of the medium, not of the contract.
//   - Per-ConnID ordering of outbound frames is preserved: a frame that
//     leaves SendFrame before another was enqueued appears on Outbound(id)
//     strictly before the later one.
//   - Lifecycle is explicit, and closing is not unregistering, exactly as on
//     the bridge: Register adds; Close(ctx, id) releases the connection —
//     its sends are answered ErrSendChanClosed and its channels close — but
//     keeps the registration, so Enumerate still lists it, RemoteAddr still
//     reports it and a second Close is nil; Unregister removes it, as the
//     owner's lifecycle helper does in production. Closed(id) observes the
//     release. After Shutdown() every subsequent Send returns
//     ErrSendChanClosed and every Register panics.
//   - Teardown always terminates, and it terminates every send with it: a
//     sender inside the send path when Shutdown / Unregister / Close raises the
//     fence is answered ErrSendChanClosed rather than pinning them, whether it
//     was parked on a saturated channel or would have found room, and whether
//     the fence went up before it read it or after. That answer is about the
//     CONNECTION and not about the bytes: a frame already in the buffer stays
//     there and reaches a reader, because closing a channel does not discard
//     what is buffered. No lock is held across a blocking send, so "the Backend
//     was shut down during the send" is a reachable outcome and not just a
//     sentence in a doc comment.
//
// The Backend deliberately does not implement a writer goroutine per
// connection. Production *netcore.NetCore owns a single-writer invariant
// enforced by the goroutine; the Backend collapses that invariant into a
// buffered channel because tests assert against the observable queue, not
// against goroutine scheduling.
package netcoretest

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// defaultOutboundBuffer is the per-connection queue depth for Outbound
// frames. It mirrors the production writer-queue depth (netcore's
// sendChBuffer = 512) so the Backend returns ErrSendBufferFull at the
// same saturation point as the real writer goroutine would.
const defaultOutboundBuffer = 512

// ErrBackendClosed is returned to callers of Register after the Backend
// has been shut down via Shutdown(). It is distinct from the Network-layer
// sentinels (ErrSendChanClosed et al.) because it signals a lifecycle
// violation (Register-after-Shutdown), not a per-connection send failure.
var ErrBackendClosed = errors.New("netcoretest: backend closed")

// connSlot holds per-connection state for one registered ConnID.
// Each slot owns its own outbound and inbound channels so tests can drive
// both directions independently.
//
// done and senders are the teardown fence, and they exist because the two
// data channels ARE closed (unlike the production queue, see
// releaseSlotLocked): a close racing a send is a panic, and a sender that held
// the registry lock across the send made the close impossible to reach. done
// is the monotonic "this slot is finished" edge, raised BEFORE anything is
// closed, so a sender parked in a blocking offer selects its way out instead
// of waiting for a reader that will never come. senders counts the senders
// that are past the registry lookup, so the close can be ordered strictly
// after the last of them.
type connSlot struct {
	dir      netcore.Direction
	addr     string
	outbound chan []byte
	inbound  chan []byte
	done     chan struct{}
	senders  sync.WaitGroup
}

// Backend is an in-memory netcore.Network implementation. The zero value
// is not usable; call New or NewWithOptions.
type Backend struct {
	mu     sync.RWMutex
	conns  map[domain.ConnID]*connSlot
	closed bool

	// outboundBuffer is the per-connection queue depth. Tests that need
	// deterministic buffer-full semantics can override via Options.
	outboundBuffer int

	// lookupBarrier and offerBarrier, when non-nil, run inside every send path.
	// Nothing outside this package installs one, so each call site is a load and
	// a branch.
	//
	// They exist because "a sender is inside the send path" has no observable
	// edge from outside: the only proof is the answer the sender eventually
	// returns, which is the very thing a teardown test has to assert. A test
	// that approximated the window with a sleep would pin the scheduler rather
	// than the fence, and would pass on the deadlocking implementation whenever
	// Shutdown happened to win the race. Same shape and same reason as
	// netcore.NetCore.enqueueBarrier.
	//
	// They are TWO because the send path answers the teardown twice and the two
	// answers defend different things:
	//
	//   - lookupBarrier runs between the registry lookup and the pre-offer
	//     teardown check, so a fence raised there is seen by the check itself —
	//     the sender is past the door and can no longer be refused at it;
	//   - offerBarrier runs between that check and the channel offer, which is
	//     the window the check cannot cover: the fence goes up after the sender
	//     read it down, and only the post-offer re-check can answer honestly.
	//
	// Installed before the first send on this Backend and never changed
	// afterwards.
	lookupBarrier func()
	offerBarrier  func()
}

// Options configures a Backend at construction. All fields are optional.
type Options struct {
	// OutboundBuffer overrides the per-connection outbound queue depth.
	// Zero or negative falls back to defaultOutboundBuffer.
	OutboundBuffer int
}

// New returns a fresh Backend with default options.
func New() *Backend {
	return NewWithOptions(Options{})
}

// NewWithOptions returns a fresh Backend configured by opts.
func NewWithOptions(opts Options) *Backend {
	buf := opts.OutboundBuffer
	if buf <= 0 {
		buf = defaultOutboundBuffer
	}
	return &Backend{
		conns:          make(map[domain.ConnID]*connSlot),
		outboundBuffer: buf,
	}
}

// Network returns the Backend as a netcore.Network value. The return is
// the Backend itself — pointer identity is stable across calls, which the
// Service injection seam (NewServiceWithNetwork) relies on so every call
// to Service.Network() sees a single conversation.
func (b *Backend) Network() netcore.Network {
	return b
}

// Register adds a virtual connection to the Backend. Panics on duplicate
// ConnID or on Register-after-Shutdown, because both outcomes indicate a
// test bug that should not silently degrade.
func (b *Backend) Register(id domain.ConnID, dir netcore.Direction, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		panic(ErrBackendClosed)
	}
	if _, dup := b.conns[id]; dup {
		panic("netcoretest: duplicate Register for ConnID " + strconv.FormatUint(uint64(id), 10))
	}
	b.conns[id] = &connSlot{
		dir:      dir,
		addr:     addr,
		outbound: make(chan []byte, b.outboundBuffer),
		inbound:  make(chan []byte, b.outboundBuffer),
		done:     make(chan struct{}),
	}
}

// Unregister removes a virtual connection, releasing it first if Close has
// not: every sender parked on it is answered ErrSendChanClosed and both its
// channels close. A second call for the same id is a no-op. It is the
// Backend's counterpart of the owner's lifecycle helper in production
// (unregisterInboundConn, an outbound session's onClose), which removes the
// registry entry after the bridge's Close.
func (b *Backend) Unregister(id domain.ConnID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(id)
}

// Outbound returns the read side of the outbound queue for id. If id is
// not registered, Outbound returns nil — reads block forever. Callers
// that need to distinguish "not registered" from "registered but idle"
// should check via Enumerate or track registration themselves. After
// Close the connection is still registered and the channel returned is
// closed, so a reader sees the release as the end of the stream.
func (b *Backend) Outbound(id domain.ConnID) <-chan []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	slot := b.conns[id]
	if slot == nil {
		return nil
	}
	return slot.outbound
}

// Inbound returns the read side of the inbound channel for id, or nil if
// id is not registered. Exposed for tests that want to drive a
// consumer-side loop over Inject'd frames.
func (b *Backend) Inbound(id domain.ConnID) <-chan []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	slot := b.conns[id]
	if slot == nil {
		return nil
	}
	return slot.inbound
}

// Inject writes frame to id's inbound channel as if it had arrived from
// the remote peer. Returns:
//   - ctx.Err() if ctx is already cancelled on entry or is cancelled while
//     the inbound channel is full.
//   - ErrSendChanClosed if the Backend is shut down, or the connection
//     closed (Close) or unregistered, before or during the send.
//   - ErrUnknownConn if id is not registered.
//   - nil once the frame is on the inbound channel.
//
// Blocks while the inbound channel is full; ctx is the caller's own way out
// and teardown is the other one. It takes a ctx for the same reason
// SendFrameBlocking does: a test that saturates the channel on purpose must be
// able to end its own wait, and "the whole package run times out" is not an
// answer a helper may give.
//
// Inject is a primitive — the Backend does not reproduce the production
// dispatch pipeline. Tests that need protocol-level delivery drive the
// Service themselves using the frame they inject.
func (b *Backend) Inject(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, err := b.acquireForSend(id)
	if err != nil {
		return err
	}
	defer slot.senders.Done()

	b.runLookupBarrier()
	return b.offerBlocking(ctx, slot, slot.inbound, append([]byte(nil), frame...))
}

// Shutdown tears down every registered connection and makes the Backend
// reject subsequent Register / Inject / SendFrame / SendFrameSync /
// SendFrameBlocking calls. Idempotent. Named Shutdown, not Close, so Close
// can implement the netcore.Network contract for per-ConnID close.
//
// It returns even while senders are parked on saturated channels: each slot is
// released by its own done before the join for that slot, and the slots are
// independent, so the whole sweep is bounded by how long a released sender
// takes to return from a select.
func (b *Backend) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id := range b.conns {
		b.removeLocked(id)
	}
}

// removeLocked drops id from the registry and releases the slot if Close has
// not already. Caller must hold b.mu with write intent.
//
// Dropping the registration first means no NEW sender can resolve this slot —
// every later one is answered ErrUnknownConn (or ErrSendChanClosed after
// Shutdown) at the lookup, under this same lock. The release itself is
// releaseSlotLocked's fence.
func (b *Backend) removeLocked(id domain.ConnID) {
	slot, ok := b.conns[id]
	if !ok {
		return
	}
	delete(b.conns, id)
	releaseSlotLocked(slot)
}

// releaseSlotLocked releases every sender of slot and closes both its channels,
// whether or not the slot stays registered: Close keeps the registration,
// removeLocked has just dropped it. Idempotent — a released slot is left as it
// is, so a slot closed and later unregistered is released once. Caller must
// hold b.mu with write intent.
//
// The order is the whole fence and every step is load-bearing:
//
//  1. raise done, which releases every sender already parked in a blocking
//     offer with the ErrSendChanClosed the doc comments promise, and answers
//     every sender that resolves the slot afterwards — Close keeps it
//     resolvable — before it reaches an offer. Without this step a sender
//     waiting for a reader that will never arrive is waiting for the very
//     teardown it is blocking;
//  2. join the senders that are past the lookup. This is bounded precisely
//     BECAUSE step 1 came first, and a released sender needs nothing from the
//     registry on its way out, so joining it while holding the write lock
//     cannot close a cycle. A sender that resolves the slot after this join
//     reads done before any offer, so it never touches a closed channel, and
//     its Add follows this Wait under the registry lock, as WaitGroup reuse
//     requires;
//  3. close the channels. Reaching this line means no sender can be inside an
//     offer any more, so the close — the whole reason an earlier version
//     held the read lock across the send — can no longer land under one.
//
// Closing at all, rather than following the production queue and never closing
// (docs/protocol/network_core.md, "Queue ownership"), is deliberate: here the
// channel IS the observable, and "the connection went away" is delivered to
// tests as a closed channel — a `range` over Outbound(id) that terminates, or
// an `ok` of false telling the reader the frame it waited for will never come.
// The production queue has no such reader and can afford to let its buffer die
// with the object.
func releaseSlotLocked(slot *connSlot) {
	if tornDown(slot) {
		return
	}
	close(slot.done)
	slot.senders.Wait()
	close(slot.outbound)
	close(slot.inbound)
}

// -------- netcore.Network implementation --------

var _ netcore.Network = (*Backend)(nil)

// SendFrame enqueues frame onto id's outbound channel. Returns:
//   - ctx.Err() if ctx is already cancelled on entry (no enqueue attempted).
//   - ErrSendChanClosed if the Backend has been shut down, the connection
//     was closed before this call (Close keeps the registration, so the slot
//     still resolves), or it was torn down at any point after this call
//     resolved the slot — including after the frame reached the buffer.
//     Non-blocking or not, a sender past the registry lookup is owed the
//     same answer as its blocking twin: the connection is released and both
//     channels are closed or about to close.
//   - ErrUnknownConn if id is not registered.
//   - ErrSendBufferFull if the per-connection outbound channel is full
//     (mirrors production slow-peer eviction on writer-queue saturation) and
//     the connection is still registered — a full channel of a torn-down slot
//     is answered ErrSendChanClosed, because "come back later" would name a
//     connection that will not be there.
//   - nil on successful enqueue.
//
// The frame is copied before enqueue so the caller's backing array can be
// reused. This matches production behaviour where the writer goroutine
// owns its own buffer lifecycle.
func (b *Backend) SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error {
	return b.sendWithoutWaiting(ctx, id, frame)
}

// SendFrameSync answers exactly as SendFrame does, ErrSendBufferFull on a
// full channel included: the production bridge does not wait for room on its
// sync path either, only for the flush (netcore.Network.SendFrameSync), and a
// Backend that waited let tests pass on a wait production never makes. The
// flush itself has no Backend counterpart — there is no writer — so a nil
// means the frame is on the outbound channel.
func (b *Backend) SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error {
	return b.sendWithoutWaiting(ctx, id, frame)
}

// SendFrameBlocking blocks until one of: frame is accepted by the outbound
// channel, ctx ends, or the connection is torn down. The sentinel set is:
//   - ctx.Err() on cancellation or pre-cancelled ctx on entry.
//   - ErrSendChanClosed if the Backend is shut down, or the connection
//     closed or torn down, before or during send.
//   - ErrUnknownConn if id is not registered.
//   - nil on successful enqueue.
//
// It never answers ErrSendBufferFull: it waits for a reader of Outbound(id)
// to make room, as the bridge waits for its writer to take a frame out of the
// queue.
func (b *Backend) SendFrameBlocking(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, err := b.acquireForSend(id)
	if err != nil {
		return err
	}
	defer slot.senders.Done()

	b.runLookupBarrier()
	return b.offerBlocking(ctx, slot, slot.outbound, append([]byte(nil), frame...))
}

// sendWithoutWaiting is the one non-blocking outbound offer behind SendFrame
// and SendFrameSync, whose answers are the same by contract.
func (b *Backend) sendWithoutWaiting(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, err := b.acquireForSend(id)
	if err != nil {
		return err
	}
	defer slot.senders.Done()

	b.runLookupBarrier()
	if tornDown(slot) {
		return netcore.ErrSendChanClosed
	}
	b.runOfferBarrier()
	frameCopy := append([]byte(nil), frame...)
	accepted := false
	select {
	case slot.outbound <- frameCopy:
		accepted = true
	default:
	}
	// The teardown answer OVERRIDES both outcomes of the offer, and it has to be
	// asked again here: the pre-check above can only speak for the instant it
	// ran, and this send has no select to put the fence in. A saturated channel
	// of a released connection is not "come back later" either.
	if tornDown(slot) {
		return netcore.ErrSendChanClosed
	}
	if !accepted {
		return netcore.ErrSendBufferFull
	}
	return nil
}

// acquireForSend resolves id and registers the caller as an in-flight sender on
// the slot. On success the caller MUST release it with slot.senders.Done() —
// releaseSlotLocked joins that counter before it closes anything.
//
// The registry lock is held for the LOOKUP ONLY and is released before the
// offer. That is the fix for the deadlock the previous version had: it held the
// read lock across the send, so a sender parked on a saturated queue blocked
// every writer of b.mu — and Shutdown, Unregister and Close, the only three
// things that could ever unblock that sender, all need the write lock. The
// documented outcome "the Backend is shut down during the send" was
// unreachable by construction.
//
// Holding the lock was not arbitrary either: it was there to keep the close in
// releaseSlotLocked from landing on a live sender. The production queue solves
// the same dilemma by never closing at all and refusing through a monotonic gate
// (docs/protocol/network_core.md, "Queue ownership"); this Backend cannot copy
// that, because a closed channel is how its tests observe a connection going
// away. So it keeps the close and moves the ordering out of the mutex: done
// tells parked senders to leave, senders counts who is still inside, and the
// close runs after the join. Both halves of the dilemma are covered without a
// lock being held across a wait.
func (b *Backend) acquireForSend(id domain.ConnID) (*connSlot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, netcore.ErrSendChanClosed
	}
	slot := b.conns[id]
	if slot == nil {
		return nil, netcore.ErrUnknownConn
	}
	// Under the read lock, so it is ordered against the senders.Wait() in
	// releaseSlotLocked, which runs under the write lock: either this Add is
	// visible to that join, or the join has already returned — the slot was
	// gone from the map above, or Close released it and kept it there. A
	// sender on a released slot reads done before any offer, so it needs no
	// join, and its Add after the returned Wait is the reuse WaitGroup allows.
	slot.senders.Add(1)
	return slot, nil
}

// offerBlocking parks until ch accepts frame, ctx ends, or the slot is torn
// down. It is the only blocking wait in the Backend and it holds no lock.
//
// The teardown is read BEFORE the offer for the same reason SendFrame reads
// it: with room left in the channel both cases are ready and `select` picks
// between them at random, so a slot torn down under the sender answered the
// promised ErrSendChanClosed only about half the time. The saturated-channel
// tests never saw it — with nowhere for the offer to go the teardown arm is
// the only ready case and the answer is forced.
//
// And it is read AGAIN after the offer, because the pre-check speaks only for
// the instant it ran and the `done` ARM is not a priority: a fence raised while
// the sender was on its way to a channel with room in it leaves both cases
// ready, and `select` still chooses at random. Priority cannot be expressed by
// the order of cases in Go, so it is expressed by a re-check on the way out —
// the same shape awaitFlush uses on the production queue.
//
// WHAT THE ANSWER IS ABOUT is the connection, not the bytes. A frame accepted
// into the buffer stays there: closing a Go channel does not discard what is
// buffered, and releaseSlotLocked joins the senders before it closes anything,
// so a reader draining Outbound(id) still receives it. ErrSendChanClosed here
// means "the connection you addressed is released", which is the fact the
// caller acts on; the frame's own fate is not something a queue can report.
func (b *Backend) offerBlocking(ctx context.Context, slot *connSlot, ch chan []byte, frame []byte) error {
	if tornDown(slot) {
		return netcore.ErrSendChanClosed
	}
	b.runOfferBarrier()
	select {
	case ch <- frame:
	case <-slot.done:
		return netcore.ErrSendChanClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	if tornDown(slot) {
		return netcore.ErrSendChanClosed
	}
	return nil
}

// tornDown reports whether the slot's teardown fence is already up, i.e.
// whether the connection has been released — by Close, which keeps the
// registration, or with the registration by Unregister / Shutdown — and its
// channels are closed or about to be. It is asked on both sides of the offer
// by both send paths: a sender past the registry lookup can no longer be
// refused at the door, so this is where "the connection went away while you
// were inside" is decided — in one place, for the blocking and the
// non-blocking offer alike.
func tornDown(slot *connSlot) bool {
	select {
	case <-slot.done:
		return true
	default:
		return false
	}
}

// runLookupBarrier and runOfferBarrier fire the test-only synchronisation
// points described on the lookupBarrier / offerBarrier fields.
func (b *Backend) runLookupBarrier() {
	if b.lookupBarrier != nil {
		b.lookupBarrier()
	}
}

func (b *Backend) runOfferBarrier() {
	if b.offerBarrier != nil {
		b.offerBarrier()
	}
}

// Enumerate walks registered connections matching dir and calls fn with
// each ConnID. Does nothing if ctx is already cancelled on entry. The
// iteration holds b.mu.RLock for the full duration — fn must not call
// back into Backend methods that take b.mu with write intent (Register /
// Unregister / Shutdown / Close); pure-reader methods are safe.
func (b *Backend) Enumerate(ctx context.Context, dir netcore.Direction, fn func(domain.ConnID) bool) {
	if ctx.Err() != nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id, slot := range b.conns {
		if slot.dir != dir {
			continue
		}
		if !fn(id) {
			return
		}
	}
}

// Close implements netcore.Network.Close: graceful per-ConnID shutdown.
// Returns ErrUnknownConn if id is not registered, nil otherwise — a second
// Close included. ctx is not consulted, as in the production networkBridge:
// releasing a connection is not cancellable work, and a backend that skipped
// the close under a cancelled ctx would let a test pass on a teardown that
// leaks in production.
//
// Like the bridge, Close releases the connection and keeps its registration:
// the bridge closes the NetCore and leaves the registry entry to the owner's
// lifecycle helper, and a Backend that removed it here answered a second
// Close, a send, Enumerate and RemoteAddr differently from production. The
// registration goes with Unregister or Shutdown.
func (b *Backend) Close(_ context.Context, id domain.ConnID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	slot, ok := b.conns[id]
	if !ok {
		return netcore.ErrUnknownConn
	}
	releaseSlotLocked(slot)
	return nil
}

// Closed reports whether id is registered and has been released by Close.
// Close keeps the registration, so RemoteAddr and Enumerate do not change on
// it, and this is how a test observes one. An id that was never registered,
// or has been unregistered, answers false.
func (b *Backend) Closed(id domain.ConnID) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	slot := b.conns[id]
	return slot != nil && tornDown(slot)
}

// RemoteAddr returns the addr passed to Register for id, or "" if id is
// not registered. Matches the zero-value convention of the production
// networkBridge — empty string means "unknown", not an error.
func (b *Backend) RemoteAddr(id domain.ConnID) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	slot := b.conns[id]
	if slot == nil {
		return ""
	}
	return slot.addr
}
