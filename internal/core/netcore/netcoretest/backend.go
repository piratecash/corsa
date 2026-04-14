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
//   - SendFrame / SendFrameSync return exactly the sentinel error set
//     declared in internal/core/netcore/network.go. Any semantic drift
//     from production networkBridge is a Backend bug, not a test bug.
//   - Per-ConnID ordering of outbound frames is preserved: a frame that
//     leaves SendFrame before another was enqueued appears on Outbound(id)
//     strictly before the later one.
//   - Lifecycle is explicit: Register adds, Unregister / Close(ctx,id)
//     remove. Double-close is idempotent. After Shutdown() every
//     subsequent Send returns ErrSendChanClosed and every Register panics.
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
// sendChBuffer = 128) so the Backend returns ErrSendBufferFull at the
// same saturation point as the real writer goroutine would.
const defaultOutboundBuffer = 128

// ErrBackendClosed is returned to callers of Register after the Backend
// has been shut down via Shutdown(). It is distinct from the Network-layer
// sentinels (ErrSendChanClosed et al.) because it signals a lifecycle
// violation (Register-after-Shutdown), not a per-connection send failure.
var ErrBackendClosed = errors.New("netcoretest: backend closed")

// connSlot holds per-connection state for one registered ConnID.
// Each slot owns its own outbound and inbound channels so tests can drive
// both directions independently.
type connSlot struct {
	dir      netcore.Direction
	addr     string
	outbound chan []byte
	inbound  chan []byte
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
	}
}

// Unregister removes a virtual connection and closes both its channels.
// A second call for the same id is a no-op.
func (b *Backend) Unregister(id domain.ConnID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeLocked(id)
}

// Outbound returns the read side of the outbound queue for id. If id is
// not registered, Outbound returns nil — reads block forever. Callers
// that need to distinguish "not registered" from "registered but idle"
// should check via Enumerate or track registration themselves.
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
// the remote peer. Returns ErrUnknownConn if id is not registered, or
// ErrSendChanClosed if the Backend has been shut down. Blocks if the
// inbound channel is full; callers that need non-blocking semantics must
// drain the channel first.
//
// Inject is a primitive — the Backend does not reproduce the production
// dispatch pipeline. Tests that need protocol-level delivery drive the
// Service themselves using the frame they inject.
func (b *Backend) Inject(id domain.ConnID, frame []byte) error {
	b.mu.RLock()
	slot := b.conns[id]
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return netcore.ErrSendChanClosed
	}
	if slot == nil {
		return netcore.ErrUnknownConn
	}
	slot.inbound <- append([]byte(nil), frame...)
	return nil
}

// Shutdown tears down every registered connection and makes the Backend
// reject subsequent Register / Inject / SendFrame / SendFrameSync calls.
// Idempotent. Named Shutdown, not Close, so Close can implement the
// netcore.Network contract for per-ConnID close.
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

// removeLocked drops id from the registry and closes both channels.
// Caller must hold b.mu with write intent.
func (b *Backend) removeLocked(id domain.ConnID) {
	slot, ok := b.conns[id]
	if !ok {
		return
	}
	delete(b.conns, id)
	close(slot.outbound)
	close(slot.inbound)
}

// -------- netcore.Network implementation --------

var _ netcore.Network = (*Backend)(nil)

// SendFrame enqueues frame onto id's outbound channel. Returns:
//   - ctx.Err() if ctx is already cancelled on entry (no enqueue attempted).
//   - ErrSendChanClosed if the Backend has been shut down.
//   - ErrUnknownConn if id is not registered.
//   - ErrSendBufferFull if the per-connection outbound channel is full
//     (mirrors production slow-peer eviction on writer-queue saturation).
//   - nil on successful enqueue.
//
// The frame is copied before enqueue so the caller's backing array can be
// reused. This matches production behaviour where the writer goroutine
// owns its own buffer lifecycle.
func (b *Backend) SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, err := b.resolveForSend(id)
	if err != nil {
		return err
	}
	frameCopy := append([]byte(nil), frame...)
	select {
	case slot.outbound <- frameCopy:
		return nil
	default:
		return netcore.ErrSendBufferFull
	}
}

// SendFrameSync blocks until one of: frame is accepted by the outbound
// channel, ctx is cancelled, or the Backend is shut down. The sentinel
// set is:
//   - ctx.Err() on cancellation or pre-cancelled ctx on entry.
//   - ErrSendChanClosed if the Backend is shut down before or during send.
//   - ErrUnknownConn if id is not registered.
//   - nil on successful enqueue.
//
// Unlike SendFrame, SendFrameSync does not return ErrSendBufferFull: the
// caller explicitly opted into blocking semantics.
func (b *Backend) SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slot, err := b.resolveForSend(id)
	if err != nil {
		return err
	}
	frameCopy := append([]byte(nil), frame...)
	select {
	case slot.outbound <- frameCopy:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// resolveForSend looks up id under b.mu.RLock and returns the slot or a
// sentinel. Factored out of SendFrame / SendFrameSync to keep both send
// methods focused on their blocking-mode difference.
func (b *Backend) resolveForSend(id domain.ConnID) (*connSlot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, netcore.ErrSendChanClosed
	}
	slot := b.conns[id]
	if slot == nil {
		return nil, netcore.ErrUnknownConn
	}
	return slot, nil
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
// Returns ErrUnknownConn if id is not registered, ctx.Err() if ctx is
// cancelled on entry, nil otherwise. Idempotent in the sense that a
// second Close for an already-removed id returns ErrUnknownConn, not a
// duplicate-teardown error — matches production networkBridge.
func (b *Backend) Close(ctx context.Context, id domain.ConnID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.conns[id]; !ok {
		return netcore.ErrUnknownConn
	}
	b.removeLocked(id)
	return nil
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
