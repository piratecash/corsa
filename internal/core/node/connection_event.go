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
//   Producers: dial workers, servePeerSession goroutines.
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

// SessionInitReady is emitted by the init goroutine (onCMSessionEstablished)
// after initPeerSession succeeds. Promotes the slot from Initializing to Active,
// making it visible to Slots(), ActiveCount(), and buildPeerExchangeResponse().
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

// BootstrapReady is emitted once after initial peer loading completes.
// Triggers the first fill() — no outbound connections start before this.
type BootstrapReady struct{}

func (BootstrapReady) hintEvent() {}
