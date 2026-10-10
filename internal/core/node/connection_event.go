package node

import "github.com/piratecash/corsa/internal/core/domain"

// ---------------------------------------------------------------------------
// Connection events — typed transitions for ConnectionManager event loop
// ---------------------------------------------------------------------------
//
// Two event families with different delivery guarantees:
//
//   SlotEvent — edge-triggered, loss is NOT acceptable.
//   Delivered via blocking send on slotEvents channel.
//   Producers: dial workers, the CM session goroutines started by
//   onCMSessionEstablished, add_peer (ManualPeerRequested) and
//   connect_only (retainOnlyRequest, through ConnectionManager.RetainOnly).
//
//   HintEvent — level-triggered, loss IS acceptable.
//   Delivered via non-blocking send on hintEvents channel.
//   Producers: handleConn (inbound close), peer exchange, bootstrap.

// SlotEvent marks an event whose loss would freeze a slot.
// All implementations carry SlotGeneration for stale-event detection.
type SlotEvent interface {
	slotEvent() // marker — sealed interface
}

// HintEvent marks an event that hints "check whether fill() is needed".
// Safe to drop: fill() always re-evaluates actual state.
type HintEvent interface {
	hintEvent() // marker — sealed interface
}

// ---------------------------------------------------------------------------
// Slot events (blocking send, loss not tolerated)
// ---------------------------------------------------------------------------

// ActiveSessionLost is emitted by servePeerSession when a previously active
// TCP session terminates. The event loop decides whether to reconnect or
// replace the slot.
type ActiveSessionLost struct {
	Address        domain.PeerAddress
	Identity       domain.PeerIdentity
	Error          error
	WasHealthy     bool
	SlotGeneration uint64
}

func (ActiveSessionLost) slotEvent() {}

// DialFailed is emitted by a dial worker when the connection attempt
// (including handshake) fails. The event loop decides whether to retry
// or replace the slot.
type DialFailed struct {
	Address        domain.PeerAddress
	Error          error
	Attempt        int
	Incompatible   bool
	SlotGeneration uint64
}

func (DialFailed) slotEvent() {}

// DialSucceeded is emitted by a dial worker when a session is successfully
// established. Ownership of Session transfers to the event loop upon
// successful delivery (emitSlot returns true).
type DialSucceeded struct {
	Address          domain.PeerAddress
	ConnectedAddress domain.PeerAddress // actual address from DialAddresses that succeeded
	Session          *peerSession
	SlotGeneration   uint64
}

func (DialSucceeded) slotEvent() {}

// SessionInitReady is emitted by the session goroutine (runCMSession) after
// initPeerSession succeeds and the session is registered with the Service; a
// session the manager evicted before that is never reported as ready.
// Promotes the slot from Initializing to Active,
// so ActiveCount() counts it and buildPeerExchangeResponse() advertises it;
// Slots() then reports it as Active instead of Initializing.
type SessionInitReady struct {
	Address        domain.PeerAddress
	SlotGeneration uint64
}

func (SessionInitReady) slotEvent() {}

// ---------------------------------------------------------------------------
// Hint events (non-blocking send, safe to drop)
// ---------------------------------------------------------------------------

// InboundClosed is emitted when the last inbound connection from a given IP
// closes (ref-count 1→0). fill() may reclaim the freed IP as an outbound
// candidate.
type InboundClosed struct {
	IP       string
	Identity domain.PeerIdentity
}

func (InboundClosed) hintEvent() {}

// NewPeersDiscovered is emitted after peer exchange or announce adds peers.
// If slots < max, fill() will pick them up.
type NewPeersDiscovered struct {
	Count int
}

func (NewPeersDiscovered) hintEvent() {}

// ManualPeerRequested is emitted by add_peer to enqueue an immediate dial
// for a manually specified peer. Unlike NewPeersDiscovered (which waits for
// fill → Candidates round-trip), this event creates a slot directly in the
// event loop, bypassing candidate filtering. Uses SlotEvent (blocking) to
// guarantee delivery — an operator add_peer should never be silently dropped.
type ManualPeerRequested struct {
	Address       domain.PeerAddress
	DialAddresses []domain.PeerAddress
}

func (ManualPeerRequested) slotEvent() {}

// retainOnlyRequest asks the event loop to evict every outbound slot except
// the live connect_only pin — the egress half of the pin. It is an event rather
// than a method that edits the table directly because the event loop is the
// only writer of the slot table: an eviction from the caller's goroutine could
// interleave with a handler that has released cm.mu but is still acting on the
// slot it just changed (OnSessionEstablished, a fill publishing its new slots),
// and the table, the published slot states and the teardown callbacks would
// then disagree about which slots exist.
//
// Unexported and built only by newRetainOnlyRequest, so every request the loop
// can receive carries a live ack channel: settling the zero value would close
// a nil channel and take the loop down.
type retainOnlyRequest struct {
	// keep is what the caller asked to retain. With a pin source wired
	// (ConnectionManagerConfig.ConnectOnlyFn) the loop retains the LIVE pin
	// instead and keep only labels the request in logs; see handleRetainOnly.
	keep domain.PeerAddress
	// applied is closed once the request is settled: by the event loop after
	// the evictions and their teardown callbacks, or by the shutdown drain
	// when the loop stopped before reaching it.
	applied chan struct{}
}

func newRetainOnlyRequest(keep domain.PeerAddress) retainOnlyRequest {
	return retainOnlyRequest{keep: keep, applied: make(chan struct{})}
}

// settle releases the waiter. Called exactly once per request: by
// handleRetainOnly, or by the shutdown drain for a request the loop never
// reached — the two are mutually exclusive because a request leaves the
// channel exactly once. A request without an ack channel has no waiter to
// release; tolerating it keeps a request built without newRetainOnlyRequest
// from taking the event loop down with a close of a nil channel.
func (r retainOnlyRequest) settle() {
	if r.applied == nil {
		return
	}
	close(r.applied)
}

func (retainOnlyRequest) slotEvent() {}

// BootstrapReady is emitted once after initial peer loading completes.
// Triggers the first fill() — no outbound connections start before this.
type BootstrapReady struct{}

func (BootstrapReady) hintEvent() {}
