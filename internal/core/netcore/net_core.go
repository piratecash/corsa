package netcore

import (
	"net"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// NetCore owns a single network connection and is the single source of
// truth for inbound connection state: identity, address, capabilities,
// networks, auth, and traffic metering.
//
// The type enforces the single-writer invariant: net.Conn is a private
// field — the only way to send data is through Send() or SendSync(),
// which route through the internal write channel to a dedicated writer
// goroutine. This eliminates the class of bugs where code bypasses the
// designated writer goroutine by calling conn.Write() directly.
//
// Mutable fields (identity, address, caps, networks, auth, lastActivity)
// are protected by an internal RWMutex. Callers do not need to hold s.mu
// to read or write NetCore state — the accessors handle locking.
//
// Current scope: wraps inbound connections only. Outbound sessions continue
// to use peerSession until Phase 3, because servePeerSession owns both
// read and write and has request-reply semantics that require a different
// migration path.

type NetCore struct {
	id        ConnID
	direction Direction

	// mu protects mutable peer state below. Getters take RLock,
	// setters take Lock. Immutable fields (id, direction, connIDNum,
	// rawConn, metered, sendCh, writerDone) are not guarded.
	mu sync.RWMutex

	// Peer identity and capabilities — set during handshake.
	identity domain.PeerIdentity
	address  domain.PeerAddress
	caps     []domain.Capability
	networks map[domain.NetGroup]struct{}

	// Auth state (inbound only, nil for outbound).
	// The pointer is swapped atomically via SetAuth/ClearAuth;
	// connauth.State is never mutated in place after creation.
	auth *connauth.State

	// Diagnostics — updated on every received frame.
	lastActivity time.Time

	// isLocal is true when the remote end is a loopback address
	// (127.0.0.0/8, ::1). Set once at registration time, immutable after.
	isLocal bool

	// Writer goroutine state.
	sendCh     chan sendItem
	writerDone chan struct{}

	// Metering.
	metered *MeteredConn

	// Private — never exposed outside NetCore methods.
	rawConn   net.Conn
	closeOnce sync.Once

	// Immutable after construction.
	connIDNum uint64

	// writeDeadline is the per-write deadline applied by writerLoop before
	// each socket write. The value is direction-specific: inbound reuses the
	// generic connWriteTimeout, outbound reuses sessionWriteTimeout so that
	// slow-peer eviction for dialled sessions keeps the same back-pressure
	// characteristics it had before outbound writes were routed through the
	// managed send path. Immutable after construction for peerSession-owned
	// NetCores; newBootstrapNetCore overrides the value once, before any
	// Send* call can happen, and the writer goroutine reads the field only
	// on dequeue — see newBootstrapNetCore for why the override is safe.
	writeDeadline time.Duration
}

// sendItem carries serialised frame data through the per-connection send
// channel. When ack is non-nil the writer goroutine closes it after the
// data has been handed to the socket, letting the caller block until the
// write completes (used by SendSync for error-path frames that must reach
// the wire before the connection is torn down).
type sendItem struct {
	data []byte
	ack  chan struct{}
}

// ConnID is a monotonic connection identifier. Using a typed integer instead
// of net.Conn as a map key prevents accidental access to the raw connection.
type ConnID uint64

// Direction indicates whether a connection was initiated by us (outbound)
// or accepted from a remote peer (inbound).
type Direction int

const (
	Inbound Direction = iota
	Outbound
)

// sendChBuffer is the per-connection write queue depth. When the queue
// is full the peer is considered "too slow" and evicted (SendBufferFull).
// 128 frames ≈ 4–8 KB of typical JSON protocol traffic — large enough
// to absorb short bursts without dropping, small enough to detect
// genuinely unresponsive peers promptly.
const sendChBuffer = 128

// Options carries the peer state to populate at construction time.
// All fields are optional — omitted fields stay at zero value. Using a
// single struct instead of scattered Set* calls ensures that the peer
// state is configured atomically and nothing is forgotten.
type Options struct {
	Address      domain.PeerAddress
	Identity     domain.PeerIdentity
	Caps         []domain.Capability
	Networks     map[domain.NetGroup]struct{}
	LastActivity time.Time
}

// New creates a NetCore, applies opts, and starts the writer
// goroutine. The caller must eventually call Close() to release resources.
//
// Caps and Networks are cloned so the caller cannot mutate NetCore state
// through the original references.
func New(id ConnID, rawConn net.Conn, dir Direction, opts Options) *NetCore {
	metered, _ := rawConn.(*MeteredConn)

	pc := &NetCore{
		id:            id,
		direction:     dir,
		address:       opts.Address,
		identity:      opts.Identity,
		caps:          cloneCaps(opts.Caps),
		networks:      cloneNetworks(opts.Networks),
		lastActivity:  opts.LastActivity,
		sendCh:        make(chan sendItem, sendChBuffer),
		writerDone:    make(chan struct{}),
		rawConn:       rawConn,
		metered:       metered,
		connIDNum:     uint64(id),
		writeDeadline: writeDeadlineFor(dir),
	}
	go pc.writerLoop()
	return pc
}

// NewBootstrap wraps a one-shot outbound dial (e.g., syncPeer and other
// bootstrap/probe paths that run before a session is established) in a
// NetCore so that every write on conn goes through the single-writer
// invariant instead of raw io.WriteString. The NetCore is never registered
// in the Service connection registry — its sole job is to serialise writes
// on conn while the caller continues to read directly from a bufio.Reader
// over the same conn. Caller owns lifecycle via Close(), which closes the
// underlying conn and waits for the writer goroutine to exit.
//
// writeDeadline is taken from the caller's outer overall-operation budget
// (e.g., syncHandshakeTimeout for syncPeer) rather than the generic
// Outbound default (sessionWriteTimeout), because the bootstrap wrapper
// must not relax the caller's existing timing contract: if the outer code
// guarantees "this whole handshake finishes in 1.5s" via SetDeadline, the
// per-write budget has to fit inside that window, not exceed it.
//
// The writeDeadline override after construction is safe because no sender
// can enqueue data until the caller observes the returned *NetCore, and the
// writer goroutine reads pc.writeDeadline only on dequeue — so the field
// write happens-before any read by Go's memory model via the channel send.
func NewBootstrap(conn net.Conn, writeDeadline time.Duration) *NetCore {
	pc := New(ConnID(0), conn, Outbound, Options{})
	pc.writeDeadline = writeDeadline
	return pc
}

// writeDeadlineFor returns the per-write socket deadline for a given
// connection direction. Outbound (dialled) sessions historically used a
// tighter 3s deadline to keep slow-peer eviction responsive; inbound
// (accepted) connections used 30s. Keeping these values split per direction
// preserves the pre-migration back-pressure behaviour when outbound writes
// move off raw io.WriteString onto NetCore's managed send path.
func writeDeadlineFor(dir Direction) time.Duration {
	if dir == Outbound {
		return sessionWriteTimeout
	}
	return connWriteTimeout
}

// writerLoop is the single goroutine that drains sendCh and writes to
// rawConn. It exits when sendCh is closed (by Close()), draining any
// remaining buffered frames first.
func (pc *NetCore) writerLoop() {
	defer crashlog.DeferRecover()
	defer close(pc.writerDone)
	for item := range pc.sendCh {
		_ = pc.rawConn.SetWriteDeadline(time.Now().Add(pc.writeDeadline))
		if _, err := pc.rawConn.Write(item.data); err != nil {
			return
		}
		if item.ack != nil {
			close(item.ack)
		}
		_ = pc.rawConn.SetWriteDeadline(time.Time{})
	}
}

// SendStatus describes why a send operation succeeded or failed.
// Callers use this to choose the correct recovery action: buffer-full and
// timeout warrant closing the connection (slow peer eviction), while
// writerDone and channelClosed mean the connection is already dying and
// an extra Close() would interfere with orderly teardown.
//
// Zero value is an invalid sentinel — an uninitialised SendStatus
// cannot be confused with success.
type SendStatus int

const (
	SendStatusInvalid SendStatus = iota // zero value — must never appear in correct code
	SendOK                              // data accepted (and flushed, for sync path)
	SendBufferFull                      // send channel is full — peer too slow
	SendWriterDone                      // writer goroutine already exited
	SendTimeout                         // sync flush deadline expired
	SendChanClosed                      // sendCh closed during send (conn shutting down)
	SendMarshalError                    // frame serialisation failed — caller's data is bad
)

// String returns a human-readable label for diagnostics and logging.
func (s SendStatus) String() string {
	switch s {
	case SendStatusInvalid:
		return "INVALID(zero)"
	case SendOK:
		return "ok"
	case SendBufferFull:
		return "buffer_full"
	case SendWriterDone:
		return "writer_done"
	case SendTimeout:
		return "timeout"
	case SendChanClosed:
		return "chan_closed"
	case SendMarshalError:
		return "marshal_error"
	default:
		return "unknown"
	}
}

// Send enqueues a protocol frame for writing. Non-blocking — returns SendOK
// on success, SendBufferFull if the write queue is full, SendChanClosed
// if the connection is shutting down, or SendMarshalError if the frame
// cannot be serialised.
func (pc *NetCore) Send(frame protocol.Frame) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.SendRaw([]byte(line))
}

// SendRaw enqueues pre-serialized bytes for writing. Non-blocking.
func (pc *NetCore) SendRaw(data []byte) (result SendStatus) {
	// Catch send-on-closed-channel: Close() may close sendCh between
	// our check and the actual send below.
	defer func() {
		if r := recover(); r != nil {
			result = SendChanClosed
		}
	}()

	select {
	case pc.sendCh <- sendItem{data: data}:
		return SendOK
	default:
		return SendBufferFull
	}
}

// SendSync enqueues a frame and blocks until the writer goroutine flushes
// it to the socket.
func (pc *NetCore) SendSync(frame protocol.Frame) SendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return SendMarshalError
	}
	return pc.SendRawSync([]byte(line))
}

// SendRawSync enqueues pre-serialized bytes and waits for write completion.
//
// Fast-fail on full queue: returns SendBufferFull immediately if the write
// channel has no slot. This preserves the pre-PR2 contract relied on by
// inbound error paths (writeJSONFrameSync / enqueueFrameSync), where a
// saturated queue means the peer is unresponsive and must be evicted
// rather than kept alive while the caller blocks. Outbound control-plane
// writes that must not be starved by fire-and-forget traffic use
// SendRawSyncBlocking instead.
func (pc *NetCore) SendRawSync(data []byte) (result SendStatus) {
	ack := make(chan struct{})

	defer func() {
		if r := recover(); r != nil {
			result = SendChanClosed
		}
	}()

	select {
	case pc.sendCh <- sendItem{data: data, ack: ack}:
		// Enqueued — wait for the writer goroutine to flush.
	default:
		return SendBufferFull
	}

	select {
	case <-ack:
		return SendOK
	case <-pc.writerDone:
		return SendWriterDone
	case <-time.After(syncFlushTimeout):
		return SendTimeout
	}
}

// SendRawSyncBlocking enqueues pre-serialized bytes and waits for write
// completion, BLOCKING on enqueue until a slot is available. Used by
// outbound control-plane writes (handshake, heartbeat, subscribe_inbox,
// request-reply) that must not be starved by fire-and-forget relay traffic
// already queued on sendCh. The entire operation (enqueue + flush) is
// bounded by syncFlushTimeout so a stuck writer cannot hang the caller
// indefinitely, and writerDone unblocks immediately if the connection is
// being torn down.
//
// This method never returns SendBufferFull — backpressure from a saturated
// queue is reserved for the fire-and-forget Send / SendRaw path, which
// uses SendBufferFull as the slow-peer eviction signal.
func (pc *NetCore) SendRawSyncBlocking(data []byte) (result SendStatus) {
	ack := make(chan struct{})

	defer func() {
		if r := recover(); r != nil {
			result = SendChanClosed
		}
	}()

	// Bound the entire operation (enqueue + flush) by a single deadline so
	// a stuck writer can never hang the caller longer than syncFlushTimeout.
	deadline := time.NewTimer(syncFlushTimeout)
	defer deadline.Stop()

	select {
	case pc.sendCh <- sendItem{data: data, ack: ack}:
		// Enqueued — wait for the writer goroutine to flush.
	case <-pc.writerDone:
		return SendWriterDone
	case <-deadline.C:
		return SendTimeout
	}

	select {
	case <-ack:
		return SendOK
	case <-pc.writerDone:
		return SendWriterDone
	case <-deadline.C:
		return SendTimeout
	}
}

// WriterDone returns a channel that is closed when the per-connection
// writer goroutine exits. Callers use this to react to local teardown
// (socket closed, Close() called) independently of Send*() return values —
// e.g. servePeerSession breaks out of its read loop as soon as the writer
// dies, instead of waiting for the next heartbeat to notice.
func (pc *NetCore) WriterDone() <-chan struct{} {
	return pc.writerDone
}

// HasCapability returns true if the peer negotiated the given capability.
func (pc *NetCore) HasCapability(cap domain.Capability) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return hasCapability(pc.caps, cap)
}

// hasCapability is the package-local copy of the slice-contains helper.
// The node package keeps its own identical copy (file_integration.go) —
// duplicating the four-line helper is cheaper than pulling it across the
// package boundary through an exported symbol or shared dependency.
func hasCapability(caps []domain.Capability, target domain.Capability) bool {
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

// Identity returns the peer's Ed25519 fingerprint. Empty before handshake.
func (pc *NetCore) Identity() domain.PeerIdentity {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.identity
}

// SetIdentity is called once during handshake when the peer's identity
// is established.
func (pc *NetCore) SetIdentity(id domain.PeerIdentity) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.identity = id
}

// SetCapabilities records the negotiated capability set.
// The slice is cloned — the caller retains no write path into NetCore state.
func (pc *NetCore) SetCapabilities(caps []domain.Capability) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.caps = cloneCaps(caps)
}

// SetNetworks records the peer's reachable network groups.
// The map is cloned — the caller retains no write path into NetCore state.
func (pc *NetCore) SetNetworks(nets map[domain.NetGroup]struct{}) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.networks = cloneNetworks(nets)
}

// Address returns the overlay address declared during the hello handshake.
// Empty before the hello frame is processed.
func (pc *NetCore) Address() domain.PeerAddress {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.address
}

// SetAddress records the peer's overlay address during hello processing.
func (pc *NetCore) SetAddress(addr domain.PeerAddress) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.address = addr
}

// ConnIDNum returns the monotonic connection identifier as a plain uint64,
// suitable for diagnostics and logging. Immutable — no lock needed.
func (pc *NetCore) ConnIDNum() uint64 {
	return pc.connIDNum
}

// ConnID returns the typed connection identifier used as primary key in
// the Service connection registry. Immutable — no lock needed.
func (pc *NetCore) ConnID() ConnID {
	return ConnID(pc.connIDNum)
}

// Conn returns the underlying net.Conn the NetCore owns. Used by iteration
// helpers in the node-level connection registry that need to surface the
// raw conn handle in their callbacks after the registry was rekeyed from
// net.Conn to ConnID in PR 9.7. Immutable — rawConn is set once in New()
// and never reassigned.
func (pc *NetCore) Conn() net.Conn {
	return pc.rawConn
}

// Auth returns the connection's auth state, or nil for unauthenticated
// connections. The returned pointer is a snapshot — connauth.State is
// never mutated in place after creation, so the caller can safely read
// its fields without holding any lock.
func (pc *NetCore) Auth() *connauth.State {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.auth
}

// SetAuth stores the auth state for this connection.
func (pc *NetCore) SetAuth(state *connauth.State) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = state
}

// ClearAuth removes the auth state from this connection.
func (pc *NetCore) ClearAuth() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = nil
}

// LastActivity returns the timestamp of the last received frame.
func (pc *NetCore) LastActivity() time.Time {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.lastActivity
}

// SetLastActivity sets the last-activity timestamp explicitly (e.g. during
// hello processing when UTC is required).
func (pc *NetCore) SetLastActivity(t time.Time) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastActivity = t
}

// Networks returns a snapshot copy of the peer's reachable network groups.
// The returned map is safe to iterate and retain after the call returns.
func (pc *NetCore) Networks() map[domain.NetGroup]struct{} {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneNetworks(pc.networks)
}

// Capabilities returns a snapshot copy of the negotiated capability set.
// The returned slice is safe to iterate and retain after the call returns.
func (pc *NetCore) Capabilities() []domain.Capability {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneCaps(pc.caps)
}

// ApplyOpts overwrites NetCore state from an opts struct. This is the
// post-handshake counterpart of newNetCore's opts: the NetCore is created
// at accept time with empty opts (identity unknown yet), and ApplyOpts fills
// in the peer state once the hello frame arrives.
//
// Only non-zero fields in opts are applied — zero values are skipped so that
// a partial update (e.g. only Caps) does not blank out existing fields.
func (pc *NetCore) ApplyOpts(opts Options) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if opts.Address != "" {
		pc.address = opts.Address
	}
	if opts.Identity != "" {
		pc.identity = opts.Identity
	}
	if opts.Caps != nil {
		pc.caps = cloneCaps(opts.Caps)
	}
	if opts.Networks != nil {
		pc.networks = cloneNetworks(opts.Networks)
	}
	if !opts.LastActivity.IsZero() {
		pc.lastActivity = opts.LastActivity
	}
}

// cloneCaps returns a shallow copy of the capability slice, or nil.
func cloneCaps(src []domain.Capability) []domain.Capability {
	if src == nil {
		return nil
	}
	out := make([]domain.Capability, len(src))
	copy(out, src)
	return out
}

// cloneNetworks returns a shallow copy of the network-group set, or nil.
func cloneNetworks(src map[domain.NetGroup]struct{}) map[domain.NetGroup]struct{} {
	if src == nil {
		return nil
	}
	out := make(map[domain.NetGroup]struct{}, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// RemoteAddr returns the remote address string (for logging only).
func (pc *NetCore) RemoteAddr() string {
	return pc.rawConn.RemoteAddr().String()
}

// Dir returns the connection direction.
func (pc *NetCore) Dir() Direction {
	return pc.direction
}

// IsLocal reports whether the connection originates from a loopback address.
// Immutable after registration — no lock required.
func (pc *NetCore) IsLocal() bool {
	return pc.isLocal
}

// SetLocal marks the connection as local (loopback). Called once during
// registration; must not be changed afterwards.
func (pc *NetCore) SetLocal(v bool) {
	pc.isLocal = v
}

// TouchActivity updates the last-activity timestamp to time.Now().
//
// NOTE: uses time.Now() directly — the node package does not yet have a
// clock abstraction. Introducing one here alone would be inconsistent with
// the 40+ other time.Now() call sites in the package. Tracked as tech debt
// for a dedicated clock-migration task.
func (pc *NetCore) TouchActivity() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastActivity = time.Now()
}

// Close shuts down the connection. Order matters:
//  1. Close TCP — unblocks a writer goroutine stuck in conn.Write with
//     a 30s deadline. Without this step, close(sendCh) + <-writerDone
//     would hang until the deadline expires.
//  2. Close sendCh — signals the writer to drain remaining frames and exit.
//  3. Wait for writerDone — ensures all buffered frames are flushed (or
//     discarded on write error) before the method returns.
//
// Idempotent — safe to call multiple times.
func (pc *NetCore) Close() {
	pc.closeOnce.Do(func() {
		_ = pc.rawConn.Close()
		close(pc.sendCh)
		<-pc.writerDone
	})
}

// Metered returns the MeteredConn wrapper, or nil if not metered.
func (pc *NetCore) Metered() *MeteredConn {
	return pc.metered
}
