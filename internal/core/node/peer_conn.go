package node

import (
	"net"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// PeerConn owns a single network connection and is the single source of
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
// to read or write PeerConn state — the accessors handle locking.
//
// Current scope: wraps inbound connections only. Outbound sessions continue
// to use peerSession until Phase 3, because servePeerSession owns both
// read and write and has request-reply semantics that require a different
// migration path.
type PeerConn struct {
	id        connID
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
	// connAuthState is never mutated in place after creation.
	auth *connAuthState

	// Diagnostics — updated on every received frame.
	lastActivity time.Time

	// Writer goroutine state.
	sendCh     chan sendItem
	writerDone chan struct{}

	// Metering.
	metered *MeteredConn

	// Private — never exposed outside PeerConn methods.
	rawConn   net.Conn
	closeOnce sync.Once

	// Immutable after construction.
	connIDNum uint64
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

// connID is a monotonic connection identifier. Using a typed integer instead
// of net.Conn as a map key prevents accidental access to the raw connection.
type connID uint64

// Direction indicates whether a connection was initiated by us (outbound)
// or accepted from a remote peer (inbound).
type Direction int

const (
	Inbound Direction = iota
	Outbound
)

// sendChBuffer is the per-connection write queue depth. When the queue
// is full the peer is considered "too slow" and evicted (sendBufferFull).
// 128 frames ≈ 4–8 KB of typical JSON protocol traffic — large enough
// to absorb short bursts without dropping, small enough to detect
// genuinely unresponsive peers promptly.
const sendChBuffer = 128

// PeerConnOpts carries the peer state to populate at construction time.
// All fields are optional — omitted fields stay at zero value. Using a
// single struct instead of scattered Set* calls ensures that the peer
// state is configured atomically and nothing is forgotten.
type PeerConnOpts struct {
	Address      domain.PeerAddress
	Identity     domain.PeerIdentity
	Caps         []domain.Capability
	Networks     map[domain.NetGroup]struct{}
	LastActivity time.Time
}

// newPeerConn creates a PeerConn, applies opts, and starts the writer
// goroutine. The caller must eventually call Close() to release resources.
//
// Caps and Networks are cloned so the caller cannot mutate PeerConn state
// through the original references.
func newPeerConn(id connID, rawConn net.Conn, dir Direction, opts PeerConnOpts) *PeerConn {
	metered, _ := rawConn.(*MeteredConn)

	pc := &PeerConn{
		id:           id,
		direction:    dir,
		address:      opts.Address,
		identity:     opts.Identity,
		caps:         cloneCaps(opts.Caps),
		networks:     cloneNetworks(opts.Networks),
		lastActivity: opts.LastActivity,
		sendCh:       make(chan sendItem, sendChBuffer),
		writerDone:   make(chan struct{}),
		rawConn:      rawConn,
		metered:      metered,
		connIDNum:    uint64(id),
	}
	go pc.writerLoop()
	return pc
}

// writerLoop is the single goroutine that drains sendCh and writes to
// rawConn. It exits when sendCh is closed (by Close()), draining any
// remaining buffered frames first.
func (pc *PeerConn) writerLoop() {
	defer crashlog.DeferRecover()
	defer close(pc.writerDone)
	for item := range pc.sendCh {
		_ = pc.rawConn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
		if _, err := pc.rawConn.Write(item.data); err != nil {
			return
		}
		if item.ack != nil {
			close(item.ack)
		}
		_ = pc.rawConn.SetWriteDeadline(time.Time{})
	}
}

// sendStatus describes why a send operation succeeded or failed.
// Callers use this to choose the correct recovery action: buffer-full and
// timeout warrant closing the connection (slow peer eviction), while
// writerDone and channelClosed mean the connection is already dying and
// an extra Close() would interfere with orderly teardown.
//
// Zero value is an invalid sentinel — an uninitialised sendStatus
// cannot be confused with success.
type sendStatus int

const (
	sendStatusInvalid sendStatus = iota // zero value — must never appear in correct code
	sendOK                              // data accepted (and flushed, for sync path)
	sendBufferFull                      // send channel is full — peer too slow
	sendWriterDone                      // writer goroutine already exited
	sendTimeout                         // sync flush deadline expired
	sendChanClosed                      // sendCh closed during send (conn shutting down)
	sendMarshalError                    // frame serialisation failed — caller's data is bad
)

// Send enqueues a protocol frame for writing. Non-blocking — returns sendOK
// on success, sendBufferFull if the write queue is full, sendChanClosed
// if the connection is shutting down, or sendMarshalError if the frame
// cannot be serialised.
func (pc *PeerConn) Send(frame protocol.Frame) sendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return sendMarshalError
	}
	return pc.sendRaw([]byte(line))
}

// sendRaw enqueues pre-serialized bytes for writing. Non-blocking.
func (pc *PeerConn) sendRaw(data []byte) (result sendStatus) {
	// Catch send-on-closed-channel: Close() may close sendCh between
	// our check and the actual send below.
	defer func() {
		if r := recover(); r != nil {
			result = sendChanClosed
		}
	}()

	select {
	case pc.sendCh <- sendItem{data: data}:
		return sendOK
	default:
		return sendBufferFull
	}
}

// SendSync enqueues a frame and blocks until the writer goroutine flushes
// it to the socket.
func (pc *PeerConn) SendSync(frame protocol.Frame) sendStatus {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return sendMarshalError
	}
	return pc.sendRawSync([]byte(line))
}

// sendRawSync enqueues pre-serialized bytes and waits for write completion.
func (pc *PeerConn) sendRawSync(data []byte) (result sendStatus) {
	ack := make(chan struct{})

	defer func() {
		if r := recover(); r != nil {
			result = sendChanClosed
		}
	}()

	select {
	case pc.sendCh <- sendItem{data: data, ack: ack}:
		// Enqueued — wait for the writer goroutine to flush.
	default:
		return sendBufferFull
	}

	select {
	case <-ack:
		return sendOK
	case <-pc.writerDone:
		return sendWriterDone
	case <-time.After(syncFlushTimeout):
		return sendTimeout
	}
}

// HasCapability returns true if the peer negotiated the given capability.
func (pc *PeerConn) HasCapability(cap domain.Capability) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return hasCapability(pc.caps, cap)
}

// Identity returns the peer's Ed25519 fingerprint. Empty before handshake.
func (pc *PeerConn) Identity() domain.PeerIdentity {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.identity
}

// SetIdentity is called once during handshake when the peer's identity
// is established.
func (pc *PeerConn) SetIdentity(id domain.PeerIdentity) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.identity = id
}

// SetCapabilities records the negotiated capability set.
// The slice is cloned — the caller retains no write path into PeerConn state.
func (pc *PeerConn) SetCapabilities(caps []domain.Capability) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.caps = cloneCaps(caps)
}

// SetNetworks records the peer's reachable network groups.
// The map is cloned — the caller retains no write path into PeerConn state.
func (pc *PeerConn) SetNetworks(nets map[domain.NetGroup]struct{}) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.networks = cloneNetworks(nets)
}

// Address returns the overlay address declared during the hello handshake.
// Empty before the hello frame is processed.
func (pc *PeerConn) Address() domain.PeerAddress {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.address
}

// SetAddress records the peer's overlay address during hello processing.
func (pc *PeerConn) SetAddress(addr domain.PeerAddress) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.address = addr
}

// ConnIDNum returns the monotonic connection identifier as a plain uint64,
// suitable for diagnostics and logging. Immutable — no lock needed.
func (pc *PeerConn) ConnIDNum() uint64 {
	return pc.connIDNum
}

// Auth returns the connection's auth state, or nil for unauthenticated
// connections. The returned pointer is a snapshot — connAuthState is never
// mutated in place after creation, so the caller can safely read its fields
// without holding any lock.
func (pc *PeerConn) Auth() *connAuthState {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.auth
}

// SetAuth stores the auth state for this connection.
func (pc *PeerConn) SetAuth(state *connAuthState) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = state
}

// ClearAuth removes the auth state from this connection.
func (pc *PeerConn) ClearAuth() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.auth = nil
}

// LastActivity returns the timestamp of the last received frame.
func (pc *PeerConn) LastActivity() time.Time {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.lastActivity
}

// SetLastActivity sets the last-activity timestamp explicitly (e.g. during
// hello processing when UTC is required).
func (pc *PeerConn) SetLastActivity(t time.Time) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastActivity = t
}

// Networks returns a snapshot copy of the peer's reachable network groups.
// The returned map is safe to iterate and retain after the call returns.
func (pc *PeerConn) Networks() map[domain.NetGroup]struct{} {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneNetworks(pc.networks)
}

// Capabilities returns a snapshot copy of the negotiated capability set.
// The returned slice is safe to iterate and retain after the call returns.
func (pc *PeerConn) Capabilities() []domain.Capability {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return cloneCaps(pc.caps)
}

// ApplyOpts overwrites PeerConn state from an opts struct. This is the
// post-handshake counterpart of newPeerConn's opts: the PeerConn is created
// at accept time with empty opts (identity unknown yet), and ApplyOpts fills
// in the peer state once the hello frame arrives.
//
// Only non-zero fields in opts are applied — zero values are skipped so that
// a partial update (e.g. only Caps) does not blank out existing fields.
func (pc *PeerConn) ApplyOpts(opts PeerConnOpts) {
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
func (pc *PeerConn) RemoteAddr() string {
	return pc.rawConn.RemoteAddr().String()
}

// Dir returns the connection direction.
func (pc *PeerConn) Dir() Direction {
	return pc.direction
}

// TouchActivity updates the last-activity timestamp to time.Now().
//
// NOTE: uses time.Now() directly — the node package does not yet have a
// clock abstraction. Introducing one here alone would be inconsistent with
// the 40+ other time.Now() call sites in the package. Tracked as tech debt
// for a dedicated clock-migration task.
func (pc *PeerConn) TouchActivity() {
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
func (pc *PeerConn) Close() {
	pc.closeOnce.Do(func() {
		_ = pc.rawConn.Close()
		close(pc.sendCh)
		<-pc.writerDone
	})
}

// Metered returns the MeteredConn wrapper, or nil if not metered.
func (pc *PeerConn) Metered() *MeteredConn {
	return pc.metered
}
