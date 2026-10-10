package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connbudget"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// ---------------------------------------------------------------------------
// ConnectionManager — event-driven outbound connection lifecycle
// ---------------------------------------------------------------------------
//
// ConnectionManager owns a fixed-size array of slots. Each slot tracks one
// outbound peer through its lifecycle: Queued → Dialing → Initializing →
// Active (or Reconnecting / RetryWait on failure). Service wires it in
// NewService; the lock rules it shares with Service are in docs/locking.md.
//
// Single-writer invariant: every mutation of the slot table (slots,
// generation, orphanReservations), every TopicSlotStateChanged publication
// and every OnSessionEstablished / OnSessionTeardown callback happens on the
// event loop goroutine (Run). Nothing else edits the table — dial workers,
// session goroutines and operator commands (add_peer, connect_only) only
// send typed events. That is what lets a handler release cm.mu and still act
// on the slot it just changed: no other writer can run until it returns.
//
// Two event channels plus a dedicated bootstrap signal:
//   - slotEvents: blocking send, loss not tolerated (DialFailed, DialSucceeded,
//     ActiveSessionLost, SessionInitReady, ManualPeerRequested,
//     retainOnlyRequest)
//   - hintEvents: non-blocking send, safe to drop (InboundClosed, NewPeersDiscovered)
//   - bootstrapCh: one-shot, guaranteed delivery (NotifyBootstrapReady closes it)

// ---------------------------------------------------------------------------
// Slot state machine
// ---------------------------------------------------------------------------
//
// The typed enum for slot lifecycle (queued / dialing / initializing / active
// / reconnecting / retry_wait) lives in domain.SlotState. Producer (this
// file) and all consumers (peer_management.go, NodeStatusMonitor,
// active-connections RPC, tests) reference the same constants, so a typo
// or rename surfaces as a compile error instead of a silent "unknown"
// fallback on the wire. See domain/peer.go for the type definition and
// wire-contract note.

// slot is the internal bookkeeping record for one outbound connection.
// Only mutated by the event loop goroutine (under cm.mu.Lock); cm.mu exists
// for the readers on other goroutines, not to arbitrate between writers.
type slot struct {
	Address          domain.PeerAddress
	DialAddresses    []domain.PeerAddress
	ConnectedAddress domain.PeerAddress // actual endpoint after fallback dial
	State            domain.SlotState
	RetryCount       int
	Generation       uint64 // incremented on every state transition
	Session          *peerSession

	// reservation is this slot's unit of the shared connection budget. It is
	// taken BEFORE the first dial starts and handed along the attempt — the
	// slot is where it rests between attempts, not what owns it.
	//
	// The distinction matters because removing a slot does NOT cancel a dial
	// already in flight: the socket still gets opened and is closed later, in
	// handleDialSucceeded. Releasing on slot removal would therefore free
	// capacity while the connection it paid for still exists. Instead the
	// reservation is MOVED to orphanReservations, keyed by the generation the
	// in-flight worker carries, and released when that attempt reports back.
	reservation *connbudget.Reservation
}

// ---------------------------------------------------------------------------
// SessionInfo — callback payload for Service integration
// ---------------------------------------------------------------------------

// SessionInfo carries the information Service needs to perform side-effects
// when a session is established or torn down (routing registration,
// score updates, heartbeat, pending frame flush, etc.).
//
// Session and SlotGeneration carry the slot's lifecycle:
//   - Session: the live TCP session. In OnSessionEstablished Service runs
//     init and the serve loop (heartbeat) on it, but the slot keeps the
//     pointer and CM still closes the transport itself when it evicts the
//     slot (tearDownSession, after cm.mu is released); a session whose loss
//     its goroutine reports, the goroutine has already closed itself.
//     OnSessionTeardown carries the same pointer only so Service can tell
//     whether its map entry still belongs to this session (pointer-compare
//     ownership guard).
//   - SlotGeneration: the generation the slot got when it entered
//     Initializing. Service saves it and passes it back in
//     SessionInitReady / ActiveSessionLost so the CM event loop can
//     detect stale events. Set only in OnSessionEstablished; zero on
//     teardown.
type SessionInfo struct {
	Address        domain.PeerAddress // canonical slot address (primary)
	DialAddress    domain.PeerAddress // actual TCP address used (may differ from Address when fallback port was used)
	Identity       domain.PeerIdentity
	Capabilities   []domain.Capability
	ConnID         domain.ConnID
	Session        *peerSession // non-nil for OnSessionEstablished and OnSessionTeardown; used for pointer-compare ownership guard
	SlotGeneration uint64       // non-zero only in OnSessionEstablished
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	reconnectMaxRetries  = 3
	reconnectBackoffBase = 2 * time.Second
	reconnectBackoffMax  = 10 * time.Second
	hintEventBuffer      = 8

	// periodicFillInterval controls how often the event loop re-runs
	// fill() regardless of incoming events. Without this, the CM is
	// purely reactive: if bootstrap fill() finds 0 eligible candidates
	// (all in cooldown) and no slot/hint events arrive, the node sits
	// at whatever connection count it has indefinitely. The ticker
	// ensures newly-eligible peers (cooldown expired, ban expired) are
	// picked up within this window.
	periodicFillInterval = 30 * time.Second
)

// slotEventBuffer returns the buffer size for slotEvents channel.
func slotEventBuffer(maxSlots int) int {
	return maxSlots * 2
}

// ---------------------------------------------------------------------------
// DialResult — returned by dialFn
// ---------------------------------------------------------------------------

// DialResult carries the outcome of a successful dial attempt.
type DialResult struct {
	Session          *peerSession
	ConnectedAddress domain.PeerAddress
}

// ---------------------------------------------------------------------------
// ConnectionManager
// ---------------------------------------------------------------------------

// ConnectionManagerConfig holds dependencies injected at construction time.
type ConnectionManagerConfig struct {
	// MaxSlotsFn returns the current slot limit. Called on every fill().
	MaxSlotsFn func() int

	// Budget is the SHARED connection ceiling (inbound + outbound + attempts
	// in flight). It is injected rather than reached for, because the whole
	// point is that this manager and the Service consult the SAME object
	// without either one taking the other's lock — connbudget's mutex is a
	// leaf, so the forbidden cm.mu → peerMu edge never appears.
	//
	// Nil is legal and reserves freely: it is the "no budget wired" case
	// used by tests, not a silent zero ceiling.
	Budget *connbudget.Budget

	// Provider supplies filtered, sorted candidates.
	Provider *PeerProvider

	// DialFn performs TCP connect + handshake. Returns DialResult on success.
	// Must respect ctx for cancellation.
	DialFn func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error)

	// OnSessionEstablished is called synchronously in the event loop
	// after a dial succeeded and passed the generation check. Ordering
	// contract the callback can rely on:
	//   - the slot is already in Initializing, NOT Active, and that state
	//     (with the peer's Identity) is visible to Slots() readers BEFORE
	//     the callback starts. Observing Initializing therefore does not
	//     mean the callback has run.
	//   - the slot stays exactly as handed over — in the table, Initializing,
	//     at info.SlotGeneration, its session open — until the callback
	//     returns. Every slot mutation runs on the event loop, which is busy
	//     running the callback, so no eviction (RetainOnly included) and no
	//     OnSessionTeardown for this session can happen meanwhile.
	//   - cm.mu is NOT held. PeerProvider.Candidates / KnownPeers hold
	//     pp.mu.RLock while calling QueuedFn → QueuedIPs (cm.mu.RLock), an
	//     edge pp.mu → cm.mu, and Service's callback takes pp.mu.Lock
	//     (promotePeerAddress → PeerProvider.Add); under cm.mu that would
	//     close a cycle, besides adding cm.mu → Service-domain edges
	//     (docs/locking.md).
	//   - the slot becomes Active only when the callee emits
	//     SessionInitReady with info.SlotGeneration; init failure is
	//     reported with ActiveSessionLost carrying the same generation.
	// It runs on the event loop, so it must not block on I/O. Service
	// keeps it to non-blocking bookkeeping and launches the session
	// goroutine, which runs initPeerSession and, on success, registers the
	// session, emits SessionInitReady and then does markPeerConnected,
	// pending frame flush, routing table registration and the serve loop
	// (heartbeat). A session this manager evicted meanwhile is neither
	// registered nor promoted.
	OnSessionEstablished func(SessionInfo)

	// OnSessionTeardown is called on the event loop, after cm.mu is
	// released, when an active or initializing slot is deactivated. The
	// session is already closed — tearDownSession closes it immediately
	// before the callback, outside cm.mu, unless its own goroutine closed it
	// first — and the first closer has recorded WHY (peerSession.closedBy):
	// a local eviction (shrinkToLimit, the add_peer eviction, RetainOnly,
	// the connect_only pin, shutdown) or the session's owner, for a loss the
	// session goroutine has already reported. Service uses the callback only to withdraw the session from
	// its session maps so no producer picks it and a replacement for the same
	// address cannot collide with it. Accounting for the peer — setup
	// failure, disconnect, routing deregistration — belongs to the session
	// goroutine, which reads the recorded reason and charges the peer only
	// when the session was not closed by a local eviction.
	OnSessionTeardown func(SessionInfo)

	// OnStaleSession is called when handleDialSucceeded detects a
	// generation mismatch and discards the session. openPeerSessionForCM
	// deliberately returns a transport-ready session without touching
	// Service-level maps; the only Service-side bookkeeping registered
	// before DialSucceeded is emitted is the fallback→primary entry in
	// Service.dialOrigin (added by dialForCM when a non-primary address
	// was used). Because onCMSessionEstablished never runs for a stale
	// generation, Service still has to evict that dialOrigin entry so
	// stale fallback mappings do not leak past slot replacement.
	//
	// The callback receives the raw *peerSession so Service can key
	// cleanup by session.address. CM itself closes the session after
	// the callback returns — Service must not close it.
	OnStaleSession func(session *peerSession)

	// OnDialFailed is called synchronously in the event loop when a
	// dial attempt fails. Service uses it to update health/ban state
	// (markPeerDisconnected, penalizeOldProtocolPeer) BEFORE fill()
	// re-queries Candidates(). Without this callback, replace + fill
	// can re-pick the same peer we just gave up on.
	OnDialFailed func(address domain.PeerAddress, err error, incompatible bool)

	// IsSetupFailureBannedFn reports whether the address is currently
	// inside a local setup-failure cooldown (see setup_failure.go).
	// Consulted by handleActiveSessionLost on the WasHealthy=false path
	// BEFORE the retry/backoff cycle: a banned address skips retry
	// outright and goes straight to replaceSlotLocked + fill(), so the
	// next candidate from PeerProvider gets the slot without waiting
	// for reconnectMaxRetries failed retries to elapse.
	//
	// Without this gate the cooldown only takes effect when fill()
	// picks a NEW candidate — but retryAfterBackoff bypasses
	// PeerProvider entirely (it dials slot.DialAddresses directly), so
	// a peer that just tripped the threshold keeps getting dialled for
	// another reconnectMaxRetries cycle. The gate closes that gap.
	//
	// May be nil — callers that do not implement setup-failure
	// tracking are unaffected.
	IsSetupFailureBannedFn func(domain.PeerAddress) bool

	// BackoffFn returns the backoff duration for the given retry attempt.
	// When nil, exponential backoff with base 2s / max 10s is used.
	// Injected for testability (zero backoff in tests).
	BackoffFn func(attempt int) time.Duration

	// NowFn returns current time. Injected for testability.
	NowFn func() time.Time

	// FillInterval overrides periodicFillInterval for testing.
	// When zero, the default periodicFillInterval is used.
	FillInterval time.Duration

	// DialPacerInterval controls the global rate limit on outbound dial
	// spawns. Each dial worker (except ManualPeerRequested) must acquire
	// a token from the pacer before invoking DialFn. Zero disables the
	// pacer entirely — the legacy "spawn all workers in a tight loop"
	// behaviour is preserved so tests and configurations that do not
	// opt in see no change.
	//
	// Production default is wired in Service.go (300ms / burst 3). See
	// dial_pacer.go for the rationale.
	DialPacerInterval time.Duration

	// DialPacerBurst is the token bucket capacity (cold-start parallel
	// dials allowed before pacing engages). Ignored when
	// DialPacerInterval is zero. Negative values are normalised to 0.
	DialPacerBurst int

	// EventBus is used to publish TopicSlotStateChanged when a slot
	// transitions between states. May be nil (tests, standalone usage).
	EventBus *ebus.Bus

	// ConnectOnlyFn reports the live connect_only egress pin, the same
	// source PeerProvider.Candidates reads. handleRetainOnly consults it so
	// that a RetainOnly request the operator has since superseded — re-pinned
	// to another peer or cleared — evicts nothing: applied late, RetainOnly(A)
	// would otherwise tear down the slot of a newer pin B, or the slots of a
	// node whose operator has just restored unrestricted egress. Must be
	// lock-free: it runs on the event loop.
	//
	// May be nil (tests, standalone usage): every request is then applied as
	// asked.
	ConnectOnlyFn func() (domain.PeerAddress, bool)

	// RetainOnlyEnqueued is a TEST-ONLY observation point, nil in
	// production: RetainOnly calls it on the caller's goroutine right after
	// its request was accepted into the slot-event queue. Tests that hold the
	// event loop use it to know the request is pending behind the hold,
	// instead of guessing from the queue length. It is configuration, like
	// every other dependency, so it is fixed before Run and never changes.
	RetainOnlyEnqueued func()
}

// ConnectionManager manages the lifecycle of outbound connection slots.
type ConnectionManager struct {
	// mu protects slots for concurrent read access from QueuedIPs/Slots/ActiveCount.
	// The event loop (Run) is the sole writer of slots, generation and
	// orphanReservations — it holds Lock during mutations; RetainOnly, the
	// one operator-driven eviction, reaches it as an event too. External
	// readers hold RLock.
	mu         sync.RWMutex
	slots      []*slot
	generation uint64 // monotonic counter for slot generations

	// orphanReservations holds budget units whose slot is gone while their
	// dial is still in flight, keyed by the slot generation the worker
	// carries. The dial that reports back under that generation releases it,
	// which is the only moment at which the socket it may have opened is
	// known to be closed.
	//
	// Bounded by the number of dials in flight, which is bounded by the slot
	// limit; drained on shutdown so nothing is leaked past Run.
	orphanReservations map[uint64]*connbudget.Reservation

	config     ConnectionManagerConfig
	slotEvents chan SlotEvent
	hintEvents chan HintEvent
	ctx        context.Context

	// bootstrapCh is closed by NotifyBootstrapReady(). The event loop
	// selects on this channel; once closed, bootstrapped flips to true
	// and fill() becomes available to other hint events.
	// Separate from hintEvents so a burst of ordinary hints cannot
	// crowd out the one-shot bootstrap signal.
	bootstrapCh   chan struct{}
	bootstrapOnce sync.Once

	// bootstrapped is set to true when bootstrapCh fires. Until then,
	// fill() is suppressed for non-bootstrap hint events.
	// Only accessed from the single-threaded event loop — no sync needed.
	bootstrapped bool

	// dialWg tracks in-flight dial goroutines. shutdown() waits for all
	// workers to finish before draining channels. This guarantees that
	// drainChannels sees every event a dial worker will ever emit —
	// eliminating the race where a worker's DialSucceeded is enqueued into
	// the buffered channel after drain has already returned and its session
	// is never closed. Producers outside dialWg (session goroutines,
	// add_peer, RetainOnly) can still enqueue after the drain; none of them
	// hands the loop a resource it would leak, and RetainOnly's waiter also
	// watches cm.ctx so it never waits for a drain that already ran.
	dialWg sync.WaitGroup

	// startOnce enforces the single-start invariant: Run() must be called
	// exactly once. Unlike accepting, this is never reset — a second
	// Run() always panics, even after shutdown.
	startOnce sync.Once
	startUsed atomic.Bool // set inside startOnce.Do; checked to detect duplicate

	// accepting gates EmitSlot: 1 = event loop is running and will
	// consume events, 0 = pre-Run or post-shutdown. Set to 1 by Run()
	// after cm.ctx is published, cleared to 0 by shutdown() before
	// draining. Separate from start guard so shutdown can close the
	// emit gate without reopening the start gate.
	accepting atomic.Int32

	// readyCh is closed by Run() after accepting is set to 1.
	// Callers that need to wait for the event loop to be ready can
	// select on this channel. Used by tests and startup orchestration.
	readyCh chan struct{}

	// pacer rate-limits outbound dial spawns to protect CPU during
	// reconnect storms. Nil when DialPacerInterval is zero.
	// Acquired by dialWorker / retryAfterBackoff; bypassed by
	// dialWorkerImmediate (ManualPeerRequested path).
	pacer *dialPacer
}

// NewConnectionManager creates a ConnectionManager. Call Run(ctx) to start
// the event loop.
func NewConnectionManager(cfg ConnectionManagerConfig) *ConnectionManager {
	if cfg.NowFn == nil {
		cfg.NowFn = time.Now
	}
	if cfg.BackoffFn == nil {
		cfg.BackoffFn = backoffDuration
	}
	maxSlots := cfg.MaxSlotsFn()
	return &ConnectionManager{
		slots:              make([]*slot, 0, maxSlots),
		orphanReservations: make(map[uint64]*connbudget.Reservation),
		config:             cfg,
		slotEvents:         make(chan SlotEvent, slotEventBuffer(maxSlots)),
		hintEvents:         make(chan HintEvent, hintEventBuffer),
		bootstrapCh:        make(chan struct{}),
		readyCh:            make(chan struct{}),
		// newDialPacer returns nil when interval <= 0; nil pacer is
		// treated as "disabled" by Acquire-site nil checks below, so
		// disabled mode pays no overhead on the hot path.
		pacer: newDialPacer(cfg.DialPacerInterval, cfg.DialPacerBurst),
	}
}

// ---------------------------------------------------------------------------
// Shared connection budget
// ---------------------------------------------------------------------------

// reserveOutbound takes one unit of the shared ceiling for one dial attempt.
// A nil budget reserves freely, so call sites need no nil check.
func (cm *ConnectionManager) reserveOutbound() (*connbudget.Reservation, error) {
	return cm.config.Budget.Reserve(connbudget.DirectionOutbound)
}

// orphanReservationLocked moves a slot's reservation to the in-flight table
// when the slot goes away while its dial is still running, and releases it
// outright when nothing is in flight.
//
// This is the whole reason the reservation is not simply released on removal:
// removing a slot does not cancel a dial, so the capacity must stay accounted
// until the attempt that may still open a socket reports back. Caller holds
// cm.mu.
func (cm *ConnectionManager) orphanReservationLocked(s *slot) {
	if s == nil || s.reservation == nil {
		return
	}
	reservation := s.reservation
	s.reservation = nil

	if !dialInFlight(s.State) {
		reservation.Release()
		return
	}
	// A generation is used by exactly one attempt, so it can only ever hold
	// one orphan. Releasing anything already parked under it would be a
	// double release of a live attempt's capacity.
	if _, exists := cm.orphanReservations[s.Generation]; exists {
		reservation.Release()
		return
	}
	cm.orphanReservations[s.Generation] = reservation
}

// takeReservationLocked detaches a slot's reservation WITHOUT parking it. It is
// for the one case orphaning would be wrong: the attempt has already reported
// back, so no further event will ever arrive under its generation and a parked
// unit would be held until shutdown.
//
// ⚠️ This is the P1 that review found: a terminal dial failure (incompatible
// peer, retries exhausted) removes the slot while its state still says
// "dialing", so removeSlotLocked parked the unit for an attempt that had just
// ended. A handful of such failures exhausted the outbound capacity even with
// the shared ceiling off, because the per-direction limit applies regardless.
//
// Caller holds cm.mu and must release the returned reservation.
func (cm *ConnectionManager) takeReservationLocked(s *slot) *connbudget.Reservation {
	if s == nil {
		return nil
	}
	reservation := s.reservation
	s.reservation = nil
	return reservation
}

// takeOrphanLocked removes the reservation parked for a generation and hands
// it to the caller WITHOUT releasing it. Callers that still have to close a
// socket use this: capacity must outlive the socket it paid for, so the
// release happens after the close, not before it. Caller holds cm.mu.
func (cm *ConnectionManager) takeOrphanLocked(generation uint64) *connbudget.Reservation {
	reservation, ok := cm.orphanReservations[generation]
	if !ok {
		return nil
	}
	delete(cm.orphanReservations, generation)
	return reservation
}

// releaseOrphanLocked releases the reservation parked for a generation, if the
// attempt that carried it left one behind. For paths where nothing is left to
// close — a failed dial produced no socket. Caller holds cm.mu.
func (cm *ConnectionManager) releaseOrphanLocked(generation uint64) {
	cm.takeOrphanLocked(generation).Release()
}

// releaseAllOrphans drains the table on shutdown. Without it a node that
// stopped with dials in flight would leave capacity accounted against a
// budget nobody will ever consult again — harmless in a process that is
// exiting, wrong in a test that reuses one.
func (cm *ConnectionManager) releaseAllOrphans() {
	cm.mu.Lock()
	orphans := cm.orphanReservations
	cm.orphanReservations = make(map[uint64]*connbudget.Reservation)
	cm.mu.Unlock()

	for _, reservation := range orphans {
		reservation.Release()
	}
}

// evictionWouldHelp answers whether evicting THIS victim can clear the refusal
// the budget just returned. Two independent reasons it cannot:
//
//   - the refusal is not about slots. The shared ceiling and the outbound
//     reserve are not freed by giving up an outbound slot — that capacity is
//     held by somebody else's inbound connections;
//   - the victim's unit would not actually be freed. A slot whose dial is
//     still in flight keeps its reservation parked (removal does not cancel a
//     dial, see orphanReservationLocked), so evicting it costs the attempt and
//     returns nothing. THIS is the case where the direction limit masks an
//     equally exhausted ceiling: the count does not go down, so the retry is
//     refused again and the session was closed for nothing.
//
// A nil victim cannot help by definition.
func evictionWouldHelp(refusal error, victim *slot) bool {
	if victim == nil {
		return false
	}
	if !errors.Is(refusal, connbudget.ErrDirectionLimit) {
		return false
	}
	return !dialInFlight(victim.State)
}

// dialInFlight reports whether a slot in this state has a worker that may
// still open a socket. RetryWait counts: its goroutine is sleeping, and it
// will dial when the backoff elapses.
func dialInFlight(state domain.SlotState) bool {
	switch state {
	case domain.SlotStateDialing, domain.SlotStateReconnecting, domain.SlotStateRetryWait:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Event emission (called from dial workers / external goroutines)
// ---------------------------------------------------------------------------

// EmitSlot sends a slot event with blocking semantics and ctx guard.
// Returns true if the event was delivered, false if the event loop is
// not running (pre-Run or post-shutdown). On false the caller retains
// ownership of any resources (e.g. Session).
func (cm *ConnectionManager) EmitSlot(event SlotEvent) bool {
	// A nil abandon channel never fires: the send waits for the loop alone.
	return cm.emitSlotUnless(nil, event)
}

// emitSlotUnless is EmitSlot that also gives up when abandon fires — for a
// producer whose own caller can walk away: RetainOnly, and the operator dial
// add_peer and connect_only enqueue (Service.enqueueAddedPeerDial).
func (cm *ConnectionManager) emitSlotUnless(abandon <-chan struct{}, event SlotEvent) bool {
	if cm.accepting.Load() != 1 {
		return false
	}
	select {
	case cm.slotEvents <- event:
		return true
	case <-cm.ctx.Done():
		return false
	case <-abandon:
		return false
	}
}

// NotifyBootstrapReady signals that bootstrap is complete and the manager
// may begin outbound dialling. Safe to call multiple times — only the
// first call has effect. Delivery is guaranteed: uses a dedicated channel
// (closed on signal) that cannot be crowded out by ordinary hints.
//
// Safe to call before Run() starts: close(bootstrapCh) is permanent and
// the event loop will observe it on its first select iteration. This
// eliminates the startup race between go cm.Run(ctx) and the bootstrap
// signal — no readiness handshake required.
func (cm *ConnectionManager) NotifyBootstrapReady() {
	cm.bootstrapOnce.Do(func() {
		close(cm.bootstrapCh)
	})
}

// Ready returns a channel that is closed once the event loop is running
// and accepting events. Useful for startup orchestration and tests.
func (cm *ConnectionManager) Ready() <-chan struct{} {
	return cm.readyCh
}

// EmitHint sends a hint event with non-blocking semantics.
// If the channel is full the event is silently dropped — this is safe
// because fill() always re-evaluates actual state.
// Rejected when the event loop is not running (pre-Run or post-shutdown).
func (cm *ConnectionManager) EmitHint(event HintEvent) {
	if cm.accepting.Load() != 1 {
		return
	}
	select {
	case cm.hintEvents <- event:
	default:
		log.Debug().Str("event", hintEventName(event)).Msg("cm: hint event dropped (buffer full)")
	}
}

// emitSlotStateChanged publishes a slot state transition on
// TopicSlotStateChanged. Called from the single-threaded event loop after
// slot.State is updated.
//
// Publisher-side dedup is intentionally NOT applied here even though the
// retry / reconnect / eviction paths can re-enter the same SlotState for
// the same address (e.g. two consecutive failures both landing in
// domain.SlotStateRetryWait). The reason is that ebus delivery is lossy — if a
// subscriber inbox is full, Publish drops the event rather than block.
// A publisher-side memo would treat the dropped publish as delivered and
// suppress all subsequent byte-identical emissions, leaving the subscriber
// permanently stale. Slot-state transitions are inherently distinct moments
// in the connection lifecycle, so emitting them unconditionally is the
// safe default; any accidental duplication is bounded by the cardinality
// of the state machine and by the downstream delta filter in
// NodeStatusMonitor.applySlotStateDelta.
//
// Safe: ebus.Publish is non-blocking and uses its own mutex.
func (cm *ConnectionManager) emitSlotStateChanged(address domain.PeerAddress, state domain.SlotState) {
	if cm.config.EventBus == nil {
		return
	}
	ebus.PublishSlotStateChanged(cm.config.EventBus, address, state.String())
}

// emitSlotRemoved publishes the "slot removed" signal (empty state string)
// for an address on TopicSlotStateChanged. Equivalent to a direct
// EventBus.Publish; kept as a helper only to localize the nil-bus guard
// and keep call sites uniform with emitSlotStateChanged.
func (cm *ConnectionManager) emitSlotRemoved(address domain.PeerAddress) {
	if cm.config.EventBus == nil {
		return
	}
	ebus.PublishSlotStateChanged(cm.config.EventBus, address, "")
}

// ---------------------------------------------------------------------------
// Public read-only API (thread-safe, called from arbitrary goroutines)
// ---------------------------------------------------------------------------

// QueuedIPs returns the set of IPs currently held in CM slots
// (any state). Used by PeerProvider to avoid offering duplicates.
func (cm *ConnectionManager) QueuedIPs() map[string]struct{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]struct{}, len(cm.slots))
	for _, s := range cm.slots {
		host, _, ok := splitHostPort(string(s.Address))
		if ok {
			result[host] = struct{}{}
		}
	}
	return result
}

// ActiveCount returns the number of slots in Active state.
func (cm *ConnectionManager) ActiveCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	count := 0
	for _, s := range cm.slots {
		if s.State == domain.SlotStateActive {
			count++
		}
	}
	return count
}

// SlotCount returns the total number of slots (all states).
func (cm *ConnectionManager) SlotCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.slots)
}

// SlotInfo is the RPC-facing view of a single slot.  State carries the
// typed domain.SlotState (underlying string) so producers and consumers
// compile-share the enum vocabulary; JSON serialization is unchanged —
// the underlying string type serializes as its raw label.
type SlotInfo struct {
	Address          domain.PeerAddress   `json:"address"`
	State            domain.SlotState     `json:"state"`
	RetryCount       int                  `json:"retry_count"`
	Generation       uint64               `json:"generation"`
	Identity         *domain.PeerIdentity `json:"identity,omitempty"`
	DialAddresses    []domain.PeerAddress `json:"dial_addresses"`
	ConnectedAddress *domain.PeerAddress  `json:"connected_address,omitempty"`
}

// Slots returns a snapshot of all slots for diagnostics / RPC.
func (cm *ConnectionManager) Slots() []SlotInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make([]SlotInfo, 0, len(cm.slots))
	for _, s := range cm.slots {
		// Copy DialAddresses to prevent callers from mutating internal state.
		dialAddrs := make([]domain.PeerAddress, len(s.DialAddresses))
		copy(dialAddrs, s.DialAddresses)

		info := SlotInfo{
			Address:       s.Address,
			State:         s.State,
			RetryCount:    s.RetryCount,
			Generation:    s.Generation,
			DialAddresses: dialAddrs,
		}
		if (s.State == domain.SlotStateActive || s.State == domain.SlotStateInitializing) && s.Session != nil {
			id := s.Session.peerIdentity
			info.Identity = &id
			addr := s.ConnectedAddress
			info.ConnectedAddress = &addr
		}
		result = append(result, info)
	}
	return result
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

// Run starts the event loop. Blocks until ctx is cancelled.
// Panics if called more than once — two event loops would race on slot
// mutations and channel consumption, violating the single-writer invariant.
func (cm *ConnectionManager) Run(ctx context.Context) {
	// Single-start guard: startOnce.Do runs at most once. If this is the
	// first call, startUsed is set inside Do. If startUsed was already set,
	// this is a duplicate — panic without touching any runtime state.
	duplicate := true
	cm.startOnce.Do(func() {
		duplicate = false
		cm.startUsed.Store(true)
	})
	if duplicate {
		panic("connection_manager: Run called more than once")
	}

	// Publish ctx, then open the emit gate. The atomic Store on accepting
	// creates a happens-before edge (Go memory model), so any goroutine
	// that reads accepting==1 is guaranteed to see cm.ctx.
	cm.ctx = ctx
	cm.accepting.Store(1)
	close(cm.readyCh)

	// bootstrapSel is nil-ed after BootstrapReady fires so the closed
	// channel doesn't wake every select iteration.
	bootstrapSel := cm.bootstrapCh

	// Periodic fill ticker: ensures newly-eligible peers (cooldown
	// expired, ban expired) are picked up even when no events arrive.
	// Without this, the CM is purely reactive and can get stuck at
	// zero outbound slots if all candidates are in cooldown at startup.
	// Only active after bootstrap completes — pre-bootstrap fills are
	// suppressed because the peer list is not yet loaded.
	fillInterval := cm.config.FillInterval
	if fillInterval == 0 {
		fillInterval = periodicFillInterval
	}
	fillTicker := time.NewTicker(fillInterval)
	defer fillTicker.Stop()

	for {
		// Phase 1: drain all pending slot events (priority).
		select {
		case ev := <-cm.slotEvents:
			cm.handleSlotEvent(ctx, ev)
			continue
		default:
		}

		// Phase 2: slot events, hint events, bootstrap, periodic fill, or shutdown.
		select {
		case ev := <-cm.slotEvents:
			cm.handleSlotEvent(ctx, ev)
		case ev := <-cm.hintEvents:
			cm.handleHintEvent(ctx, ev)
		case <-bootstrapSel:
			cm.bootstrapped = true
			bootstrapSel = nil // stop re-selecting on closed channel
			cm.fill(ctx)
		case <-fillTicker.C:
			if cm.bootstrapped {
				cm.fill(ctx)
			}
		case <-ctx.Done():
			cm.shutdown()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Event handlers (called from event loop goroutine, hold mu.Lock for mutations)
// ---------------------------------------------------------------------------

func (cm *ConnectionManager) handleSlotEvent(ctx context.Context, event SlotEvent) {
	switch ev := event.(type) {
	case ActiveSessionLost:
		cm.handleActiveSessionLost(ctx, ev)
	case DialFailed:
		cm.handleDialFailed(ctx, ev)
	case DialSucceeded:
		cm.handleDialSucceeded(ctx, ev)
	case SessionInitReady:
		cm.handleSessionInitReady(ctx, ev)
	case ManualPeerRequested:
		cm.handleManualPeer(ctx, ev)
	case retainOnlyRequest:
		cm.handleRetainOnly(ctx, ev)
	}
}

func (cm *ConnectionManager) handleHintEvent(ctx context.Context, event HintEvent) {
	// BootstrapReady is handled via bootstrapCh in the event loop select,
	// not through hintEvents. If one arrives here, ignore it.
	switch event.(type) {
	case BootstrapReady:
		// no-op: handled via dedicated bootstrapCh
	case InboundClosed:
		if !cm.bootstrapped {
			return
		}
		cm.fill(ctx)
	case NewPeersDiscovered:
		if !cm.bootstrapped {
			return
		}
		cm.fill(ctx)
	}
}

// handleManualPeer creates a slot and starts dialling immediately for a peer
// added via add_peer. Unlike fill(), this bypasses Candidates() filtering —
// including the subnet-diversity gate (one connection per /24 for IPv4,
// /64 for IPv6): the operator explicitly requested this peer, so a
// same-subnet connection is allowed here and only here.
//
// Steps: refuse a peer the live connect_only pin forbids; read what lives
// outside the manager (the slot limit, eviction scores) BEFORE cm.mu, since
// both reach into the embedder; under cm.mu, dedup and admit
// (admitManualPeerLocked); after it, publish and dial.
func (cm *ConnectionManager) handleManualPeer(ctx context.Context, ev ManualPeerRequested) {
	if cm.refuseManualPeerOutsideLivePin(ev.Address) {
		return
	}
	maxSlots := cm.config.MaxSlotsFn()
	scores := cm.slotScoresForEviction()

	cm.mu.Lock()
	if cm.manualPeerDuplicateLocked(ev.Address) {
		cm.mu.Unlock()
		return
	}
	admission := cm.admitManualPeerLocked(ev, maxSlots, scores)
	cm.mu.Unlock()

	cm.publishManualPeerAdmission(ctx, admission)
}

// refuseManualPeerOutsideLivePin reports, and logs, a manual peer the live
// connect_only pin forbids dialling. The Service already tells the operator
// so in the add_peer reply; this is the manager's own guard for a pin that
// moved after the request was sent.
func (cm *ConnectionManager) refuseManualPeerOutsideLivePin(address domain.PeerAddress) bool {
	pin, refused := cm.manualPeerOutsideLivePin(address)
	if refused {
		log.Warn().
			Str("address", string(address)).
			Str("pin", connectOnlyPinLabel(pin)).
			Msg("cm: manual peer not dialled — connect_only pins egress to another peer")
	}
	return refused
}

// manualPeerDuplicateLocked reports whether address already has a slot, by
// exact address or — another port on the same host — by IP: two slots to one
// host would defeat the point of a manual pick. Caller holds cm.mu.Lock.
func (cm *ConnectionManager) manualPeerDuplicateLocked(address domain.PeerAddress) bool {
	if cm.findSlotLocked(address) != nil {
		log.Debug().Str("address", string(address)).Msg("cm: manual peer already has a slot")
		return true
	}
	targetIP, _, _ := splitHostPort(string(address))
	if targetIP == "" {
		return false
	}
	for _, existing := range cm.slots {
		ip, _, _ := splitHostPort(string(existing.Address))
		if ip == targetIP {
			log.Debug().
				Str("address", string(address)).
				Str("existing", string(existing.Address)).
				Msg("cm: manual peer IP already has a slot")
			return true
		}
	}
	return false
}

// manualPeerAdmission is what admitManualPeerLocked decided: the slot it
// appended — nil when the manual peer was refused — and the eviction it made
// room with, to publish once cm.mu is released.
type manualPeerAdmission struct {
	admitted  *slot
	evictions slotEvictions
}

// admitManualPeerLocked decides whether the manual peer gets a slot, and
// evicts one to make room when the table is full.
//
// The operator's dial pays the same budget as any other, and the POLICY
// here has to answer two questions that reviews found conflated twice.
//
// ⚠️ First conflation: the original order evicted before reserving, so a
// refusal by the SHARED ceiling cost a live session and bought nothing.
// ⚠️ Second: the fix made the slot-limit check CONDITIONAL on a budget
// error, which broke the manager's own invariant whenever the budget said
// yes — no budget wired, or a ceiling wider than MaxSlotsFn — and let the
// slot table grow past its maximum.
//
// Both are avoided by asking the two questions SEPARATELY:
//
//  1. is the slot table full? That is the manager's own invariant and
//     holds whether or not a budget exists;
//  2. does the shared ceiling have room for one more outbound?
//
// Eviction is justified by (1) alone, and ONLY when (2) can be satisfied
// afterwards. Killing a session to make room the ceiling would refuse
// anyway is exactly what §0.1.1 forbids — and, crucially, "refused by the
// direction limit" does not by itself mean the ceiling has room: the
// budget reports the most specific reason, so a node against BOTH limits
// answers ErrDirectionLimit while the shared ceiling is equally full.
//
// Caller holds cm.mu.Lock; maxSlots and scores were read before it.
func (cm *ConnectionManager) admitManualPeerLocked(ev ManualPeerRequested, maxSlots int, scores map[*slot]int) manualPeerAdmission {
	var evictions slotEvictions
	reservation, budgetErr := cm.reserveOutbound()

	// Evict when the slot table is what stands in the way — either the budget
	// already said yes (so only the invariant blocks us) or the refusal is one
	// that evicting THIS victim can actually clear.
	if len(cm.slots) >= maxSlots {
		victim := cm.findLowestScoringSlotLocked(scores)
		switch {
		case budgetErr != nil && !evictionWouldHelp(budgetErr, victim):
			// The refusal is not about slots, or the victim's unit would
			// not be freed by evicting it. Closing a session for room
			// that never appears is what §0.1.1 forbids.
			log.Warn().
				Err(budgetErr).
				Str("address", string(ev.Address)).
				Msg("cm: manual peer refused by connection budget; nothing evicted")
			return manualPeerAdmission{}
		case victim == nil:
			// Nothing to evict: the invariant wins over operator intent,
			// and the reservation we may hold must not be leaked.
			reservation.Release()
			log.Warn().
				Str("address", string(ev.Address)).
				Msg("cm: manual peer refused — slot table full with no evictable slot")
			return manualPeerAdmission{}
		default:
			log.Info().
				Str("evicted", string(victim.Address)).
				Str("manual", string(ev.Address)).
				Msg("cm: evicting slot to make room for manual peer")
			// Removal settles the victim's own unit: a live session
			// releases it here, an in-flight dial keeps it parked.
			evictions = cm.evictSlotsLocked([]*slot{victim})
			if budgetErr != nil {
				reservation, budgetErr = cm.reserveOutbound()
			}
		}
	}

	if budgetErr != nil {
		log.Warn().
			Err(budgetErr).
			Str("address", string(ev.Address)).
			Msg("cm: manual peer refused by connection budget")
		return manualPeerAdmission{evictions: evictions}
	}

	dialAddrs := ev.DialAddresses
	if len(dialAddrs) == 0 {
		dialAddrs = []domain.PeerAddress{ev.Address}
	}
	admitted := &slot{
		Address:       ev.Address,
		DialAddresses: dialAddrs,
		State:         domain.SlotStateDialing,
		Generation:    cm.nextGenerationLocked(),
		reservation:   reservation,
	}
	cm.slots = append(cm.slots, admitted)
	return manualPeerAdmission{admitted: admitted, evictions: evictions}
}

// publishManualPeerAdmission publishes what admitManualPeerLocked did and
// starts the dial, with cm.mu released: the eviction's removal and teardown
// first, then the new slot. The admitted slot is read without cm.mu because
// this runs on the event loop, its only writer.
func (cm *ConnectionManager) publishManualPeerAdmission(ctx context.Context, admission manualPeerAdmission) {
	cm.publishEvictions(admission.evictions)
	admitted := admission.admitted
	if admitted == nil {
		return
	}
	cm.emitSlotStateChanged(admitted.Address, domain.SlotStateDialing)

	log.Info().
		Str("address", string(admitted.Address)).
		Uint64("generation", admitted.Generation).
		Msg("cm: manual peer enqueued for immediate dial")

	// Manual dials bypass the global pacer — operator intent overrides
	// storm-protection. See dialWorkerImmediate / dial_pacer.go.
	cm.dialWg.Add(1)
	go cm.dialWorkerImmediate(ctx, admitted.Address, admitted.DialAddresses, admitted.Generation)
}

// slotScoresForEviction scores every slot for findLowestScoringSlotLocked.
// Score reaches into Service (PeerProvider.HealthFn takes peerMu), so it must
// run BEFORE cm.mu is taken — under it, it would be a cm.mu → peerMu edge.
// Reading cm.slots without cm.mu is safe here because this runs on the event
// loop, the only writer of the table, and it stays valid until the loop
// itself changes the table.
func (cm *ConnectionManager) slotScoresForEviction() map[*slot]int {
	scores := make(map[*slot]int, len(cm.slots))
	if cm.config.Provider == nil {
		return scores
	}
	for _, s := range cm.slots {
		scores[s] = cm.config.Provider.Score(s.Address)
	}
	return scores
}

// findLowestScoringSlotLocked returns the slot with the lowest score for eviction.
// Prefers non-active slots. Returns nil only if no slots exist. scores comes
// from slotScoresForEviction; a slot it has no entry for scores 0.
// Caller must hold cm.mu.Lock.
func (cm *ConnectionManager) findLowestScoringSlotLocked(scores map[*slot]int) *slot {
	var best *slot
	bestScore := int(^uint(0) >> 1) // max int
	bestActive := true

	for _, s := range cm.slots {
		isActive := s.State == domain.SlotStateActive
		score := scores[s]

		// Prefer non-active over active; within same category, prefer lower score.
		if best == nil ||
			(bestActive && !isActive) ||
			(bestActive == isActive && score < bestScore) {
			best = s
			bestScore = score
			bestActive = isActive
		}
	}
	return best
}

func (cm *ConnectionManager) handleActiveSessionLost(ctx context.Context, ev ActiveSessionLost) {
	// Pre-compute the setup-failure cooldown status BEFORE taking
	// cm.mu — IsSetupFailureBannedFn is wired to Service.IsSetupFailureBanned
	// in production, which takes peerMu.RLock. Calling it under cm.mu
	// would introduce a cm.mu → peerMu edge in the storm hot path
	// (every cm_session_setup_failed lands here). Computing the flag
	// outside the lock keeps the edge family unchanged. The slot
	// generation guard below still catches any race window between
	// this lookup and the lock — a banned address that recently became
	// unbanned would at worst replace a stale slot, which is harmless.
	var setupBanned bool
	if !ev.WasHealthy && cm.config.IsSetupFailureBannedFn != nil {
		setupBanned = cm.config.IsSetupFailureBannedFn(ev.Address)
	}

	cm.mu.Lock()

	s := cm.findSlotLocked(ev.Address)
	if s == nil {
		cm.mu.Unlock()
		return
	}
	if s.Generation != ev.SlotGeneration {
		cm.mu.Unlock()
		return
	}

	// Cleanup side-effects before reconnect. The session goroutine reported
	// this loss itself and has already accounted for it.
	teardown := cm.deactivateSlotLocked(s, slotDeactivationOutcomeReported)

	// WasHealthy distinguishes two failure modes:
	//
	//   true  — a session that was fully operational (servePeerSession ran)
	//           lost its connection (EOF, timeout, remote close). The peer
	//           was healthy recently, so reset retry count and reconnect
	//           immediately.
	//
	//   false — the session never became operational because post-handshake
	//           setup (syncPeerSession) failed. This is
	//           semantically a dial failure — the peer is reachable but
	//           unable to complete the application protocol. Without
	//           backoff, the CM would spin an infinite reconnect loop
	//           against a permanently bad peer while starving candidates.
	//           Route through the same retry/replace logic as DialFailed.
	if !ev.WasHealthy {
		// Setup-failure cooldown gate (B1 closing-the-gap).
		// retryAfterBackoff dials slot.DialAddresses directly without
		// consulting PeerProvider, so a peer that just tripped the
		// setupFailureBanThreshold would keep getting dialled for
		// another reconnectMaxRetries cycle (=14s of agitation at the
		// default 2-4-8s backoff) before the cooldown can take effect
		// via fill()->Candidates(). Short-circuit here: if the address
		// is currently banned by the local setup-failure cooldown,
		// skip the retry cycle entirely, replace the slot now, and let
		// fill() pick a different candidate that the gate permits.
		// setupBanned was computed at function entry (outside cm.mu).
		s.RetryCount++

		if setupBanned || s.RetryCount > reconnectMaxRetries {
			log.Info().
				Str("address", string(ev.Address)).
				Int("retries", s.RetryCount).
				Bool("setup_banned", setupBanned).
				Msg("cm: replacing slot after repeated setup failures")

			replaceTeardown := cm.replaceSlotLocked(s, slotDeactivationOutcomeReported)
			addr := s.Address
			cm.mu.Unlock()

			// Slot removed — emit empty state so subscribers clear the peer.
			cm.emitSlotRemoved(addr)

			cm.tearDownSession(teardown)
			cm.tearDownSession(replaceTeardown)
			if cm.config.OnDialFailed != nil {
				cm.config.OnDialFailed(ev.Address, ev.Error, false)
			}
			cm.fill(ctx)
			return
		}

		addr := s.Address
		s.State = domain.SlotStateRetryWait
		gen := cm.nextGenerationLocked()
		s.Generation = gen
		dialAddrs := s.DialAddresses
		retryCount := s.RetryCount

		cm.mu.Unlock()

		cm.emitSlotStateChanged(addr, domain.SlotStateRetryWait)

		cm.tearDownSession(teardown)
		if cm.config.OnDialFailed != nil {
			cm.config.OnDialFailed(addr, ev.Error, false)
		}

		log.Debug().
			Str("address", string(addr)).
			Int("attempt", retryCount).
			Msg("cm: scheduling retry with backoff after setup failure")

		cm.dialWg.Add(1)
		go cm.retryAfterBackoff(ctx, addr, dialAddrs, gen, retryCount)
		return
	}

	// WasHealthy: true — genuine connection loss from a working session.
	// Reset retry count and reconnect immediately.
	s.State = domain.SlotStateReconnecting
	s.RetryCount = 0
	gen := cm.nextGenerationLocked()
	s.Generation = gen
	addr := s.Address
	dialAddrs := s.DialAddresses

	cm.mu.Unlock()

	cm.emitSlotStateChanged(addr, domain.SlotStateReconnecting)

	cm.tearDownSession(teardown)

	log.Info().
		Str("address", string(ev.Address)).
		Str("identity", ev.Identity.String()).
		Err(ev.Error).
		Msg("cm: active session lost, reconnecting")

	cm.dialWg.Add(1)
	go cm.dialWorker(ctx, addr, dialAddrs, gen)
}

func (cm *ConnectionManager) handleDialFailed(ctx context.Context, ev DialFailed) {
	cm.mu.Lock()

	s := cm.findSlotLocked(ev.Address)
	if s == nil {
		// The attempt outlived its slot: release the capacity parked for
		// it. No socket was opened — this is the failure path.
		cm.releaseOrphanLocked(ev.SlotGeneration)
		cm.mu.Unlock()
		return
	}
	if s.Generation != ev.SlotGeneration {
		cm.releaseOrphanLocked(ev.SlotGeneration)
		cm.mu.Unlock()
		return
	}

	s.RetryCount++

	if ev.Incompatible || s.RetryCount > reconnectMaxRetries {
		log.Debug().
			Str("address", string(ev.Address)).
			Bool("incompatible", ev.Incompatible).
			Int("retries", s.RetryCount).
			Msg("cm: replacing slot")

		// The attempt is OVER: this handler is its final event, so nothing
		// will arrive later to release a parked unit. Detach before the
		// removal that would otherwise park it, and release once the lock
		// is gone.
		finished := cm.takeReservationLocked(s)
		teardown := cm.replaceSlotLocked(s, slotDeactivationOutcomeReported)
		replacedAddr := s.Address
		cm.mu.Unlock()

		finished.Release()

		// Slot removed — emit empty state so subscribers clear the peer.
		cm.emitSlotRemoved(replacedAddr)

		cm.tearDownSession(teardown)

		// Notify Service BEFORE fill() so health/ban state is updated
		// and Candidates() won't return the same failed peer again.
		if cm.config.OnDialFailed != nil {
			cm.config.OnDialFailed(ev.Address, ev.Error, ev.Incompatible)
		}

		cm.fill(ctx)
	} else {
		addr := s.Address
		s.State = domain.SlotStateRetryWait
		gen := cm.nextGenerationLocked()
		s.Generation = gen
		dialAddrs := s.DialAddresses
		retryCount := s.RetryCount

		cm.mu.Unlock()

		cm.emitSlotStateChanged(addr, domain.SlotStateRetryWait)

		// Notify Service about the failure (score update) even for retries.
		if cm.config.OnDialFailed != nil {
			cm.config.OnDialFailed(addr, ev.Error, ev.Incompatible)
		}

		log.Debug().
			Str("address", string(addr)).
			Int("attempt", retryCount).
			Msg("cm: scheduling retry with backoff")

		cm.dialWg.Add(1)
		go cm.retryAfterBackoff(ctx, addr, dialAddrs, gen, retryCount)
	}
}

func (cm *ConnectionManager) handleDialSucceeded(_ context.Context, ev DialSucceeded) {
	cm.mu.Lock()

	s := cm.findSlotLocked(ev.Address)
	if s == nil || s.Generation != ev.SlotGeneration {
		// Late success after the slot was replaced or removed. The socket
		// below is real, so its unit is taken OUT of the parking table here
		// but released only AFTER the socket is closed: releasing first
		// would let another attempt occupy the capacity while the old
		// socket is still open, which is the overshoot the ceiling exists
		// to prevent.
		stale := cm.takeOrphanLocked(ev.SlotGeneration)
		cm.mu.Unlock()
		defer stale.Release()
		// Stale: slot already replaced or transitioned.
		// openPeerSessionForCM left Service maps untouched, but dialForCM
		// may have registered a fallback→primary entry in Service.dialOrigin
		// before the dial succeeded. OnStaleSession lets Service drop that
		// entry so it does not survive into the next slot generation. CM
		// owns the close here — the callback must not close the session.
		if ev.Session != nil {
			if cm.config.OnStaleSession != nil {
				cm.config.OnStaleSession(ev.Session)
			}
			_ = ev.Session.Close()
		}
		return
	}

	info := cm.beginInitSlotLocked(s, ev)
	cm.mu.Unlock()

	// Published after the unlock, like every other slot state: a synchronous
	// subscriber may read the table. The single writer keeps the order — the
	// state is out before the callback runs.
	cm.emitSlotStateChanged(info.Address, domain.SlotStateInitializing)

	if cm.config.OnSessionEstablished != nil {
		cm.config.OnSessionEstablished(info)
	}
}

// handleSessionInitReady promotes a slot from Initializing to Active after
// the application-level setup (initPeerSession) succeeds.
// Until this event arrives, ActiveCount() does not count the slot and
// buildPeerExchangeResponse() does not advertise it — preventing
// advertisement of peers that are not yet usable. Slots() still reports it,
// labelled Initializing, for diagnostics.
func (cm *ConnectionManager) handleSessionInitReady(_ context.Context, ev SessionInitReady) {
	cm.mu.Lock()

	s := cm.findSlotLocked(ev.Address)
	if s == nil || s.Generation != ev.SlotGeneration {
		cm.mu.Unlock()
		return
	}
	if s.State != domain.SlotStateInitializing {
		// Already promoted, deactivated, or replaced — stale event.
		cm.mu.Unlock()
		return
	}

	cm.promoteSlotLocked(s)
	cm.mu.Unlock()

	// After the unlock: a synchronous subscriber may read the table.
	cm.emitSlotStateChanged(ev.Address, domain.SlotStateActive)
}

// ---------------------------------------------------------------------------
// Slot lifecycle operations (called under mu.Lock)
// ---------------------------------------------------------------------------

// fill creates slots for available candidates up to maxSlotsFn().
// Executes synchronously in the event loop.
//
// If the dynamic limit has decreased since the last call, fill first
// evicts excess slots (non-active preferred) to bring the count back
// within bounds before attempting to add new ones.
func (cm *ConnectionManager) fill(ctx context.Context) {
	if cm.config.Provider == nil {
		return
	}

	maxSlots := cm.config.MaxSlotsFn()

	// Pin first: while connect_only is live, only the pinned slot may exist.
	// Shrinking first would pick its victims among ALL slots and prefers the
	// non-active ones — exactly the state of a pinned slot that is still
	// dialling or initializing right after connect_only.
	//
	// The pin is read here and again by Candidates() below. If the operator
	// moves it in between, this fill may keep the old pin's slot and append
	// the new pin's; the retention request that follows every pin write, and
	// the next fill, apply the rule to the new pin and evict the old one.
	cm.enforceLivePinRule()
	// Shrink: if the limit dropped, evict excess slots.
	cm.shrinkToLimit(maxSlots)

	// Read slot count under lock to check free capacity.
	cm.mu.RLock()
	currentSlots := len(cm.slots)
	cm.mu.RUnlock()

	freeSlots := maxSlots - currentSlots
	if freeSlots <= 0 {
		return
	}

	// Candidates() is called OUTSIDE cm.mu to avoid lock ordering issue:
	// PeerProvider.Candidates() takes pp.mu.RLock and calls cm.QueuedIPs()
	// which takes cm.mu.RLock. If we held cm.mu.Lock here, we'd deadlock.
	candidates := cm.config.Provider.Candidates()
	if len(candidates) == 0 {
		return
	}

	cm.mu.Lock()

	// Re-check: slot count may have changed between RUnlock and Lock
	// only if another writer ran in between. Every slot mutation runs on
	// this event loop, which is busy here, so it cannot — still, the
	// defensive re-check costs nothing.
	freeSlots = maxSlots - len(cm.slots)
	if freeSlots <= 0 {
		cm.mu.Unlock()
		return
	}

	toAdd := freeSlots
	if toAdd > len(candidates) {
		toAdd = len(candidates)
	}

	// Phase 1: reserve ALL slots before launching goroutines.
	type dialTask struct {
		address       domain.PeerAddress
		dialAddresses []domain.PeerAddress
		generation    uint64
	}
	tasks := make([]dialTask, 0, toAdd)

	for i := 0; i < toAdd; i++ {
		// The budget is taken BEFORE the dial exists, because a socket
		// being opened costs the same whether or not the handshake ever
		// finishes. Refusal ends the fill: the ceiling is global, so the
		// next candidate would be refused for the same reason.
		reservation, err := cm.reserveOutbound()
		if err != nil {
			log.Debug().
				Err(err).
				Str("address", string(candidates[i].Address)).
				Msg("cm: dial refused by connection budget")
			break
		}

		gen := cm.nextGenerationLocked()
		s := &slot{
			Address:       candidates[i].Address,
			DialAddresses: candidates[i].DialAddresses,
			State:         domain.SlotStateDialing,
			Generation:    gen,
			reservation:   reservation,
		}
		cm.slots = append(cm.slots, s)
		tasks = append(tasks, dialTask{
			address:       s.Address,
			dialAddresses: s.DialAddresses,
			generation:    gen,
		})
	}

	cm.mu.Unlock()

	// Emit slot state changes outside the lock.
	for _, t := range tasks {
		cm.emitSlotStateChanged(t.address, domain.SlotStateDialing)
	}

	// Phase 2: launch dial workers (outside lock).
	for _, t := range tasks {
		log.Debug().
			Str("address", string(t.address)).
			Uint64("generation", t.generation).
			Msg("cm: starting dial worker")

		cm.dialWg.Add(1)
		go cm.dialWorker(ctx, t.address, t.dialAddresses, t.generation)
	}
}

// shrinkToLimit evicts excess slots when the dynamic limit has decreased.
// Non-active slots (queued, dialing, retry_wait, reconnecting) are evicted
// first. Active slots are evicted last — only when all non-active slots are
// already gone and the count is still above the limit.
// Callbacks (OnSessionTeardown) are invoked outside the lock.
func (cm *ConnectionManager) shrinkToLimit(maxSlots int) {
	cm.mu.Lock()
	excess := len(cm.slots) - maxSlots
	if excess <= 0 {
		cm.mu.Unlock()
		return
	}

	log.Info().
		Int("current", len(cm.slots)).
		Int("max", maxSlots).
		Int("excess", excess).
		Msg("cm: shrinking slots to new limit")

	// Phase 1: collect victims. Prefer non-active slots.
	var victims []*slot
	// First pass: non-active.
	for _, s := range cm.slots {
		if len(victims) >= excess {
			break
		}
		if s.State != domain.SlotStateActive {
			victims = append(victims, s)
		}
	}
	// Second pass: active (only if still over limit).
	for _, s := range cm.slots {
		if len(victims) >= excess {
			break
		}
		if s.State == domain.SlotStateActive {
			victims = append(victims, s)
		}
	}

	evictions := cm.evictSlotsLocked(victims)
	cm.mu.Unlock()

	cm.publishEvictions(evictions)
}

// slotEvictions is what evictSlotsLocked leaves for the caller to publish
// once cm.mu is released: the removed addresses and the teardown payloads of
// the sessions it closed.
type slotEvictions struct {
	removed   []domain.PeerAddress
	teardowns []*sessionTeardown
}

// evictSlotsLocked deactivates and removes victims on a LOCAL decision — the
// slot limit shrank, or connect_only pinned egress to another peer. Their
// sessions are closed as local evictions, so the session goroutines do not
// charge the peers for a teardown this node chose. Caller holds cm.mu.Lock
// and must hand the result to publishEvictions after releasing it.
func (cm *ConnectionManager) evictSlotsLocked(victims []*slot) slotEvictions {
	evictions := slotEvictions{removed: make([]domain.PeerAddress, 0, len(victims))}
	for _, v := range victims {
		evictions.removed = append(evictions.removed, v.Address)
		evictions.teardowns = append(evictions.teardowns, cm.deactivateSlotLocked(v, slotDeactivationLocalEviction))
		cm.removeSlotLocked(v)
	}
	return evictions
}

// publishEvictions emits the slot-removed signal (empty state) for every
// evicted address so subscribers see the peers disappear from CM tracking,
// then closes the evicted sessions and runs their teardown callbacks. Runs on
// the event loop after cm.mu is released: closing a session is I/O and its
// onClose takes peerMu, and the callbacks reach into Service — none of which
// may happen under cm.mu.
func (cm *ConnectionManager) publishEvictions(evictions slotEvictions) {
	for _, addr := range evictions.removed {
		cm.emitSlotRemoved(addr)
	}
	cm.tearDownSessions(evictions.teardowns)
}

// RetainOnly makes the event loop apply the connect_only pin rule now and waits
// for it: every outbound slot other than the pinned one is evicted. It is the
// egress half of the pin; incoming connections are tracked outside the
// ConnectionManager (Service ipState domain), so they are untouched here.
//
// The rule is applied against the LIVE pin (ConnectionManagerConfig.
// ConnectOnlyFn), not against keep: requests queue behind other events, so by
// the time one is handled the operator may have re-pinned or cleared the pin,
// and only the live value says which slot may stay. keep is what is retained
// only when no pin source is wired (tests, standalone use). See
// enforceLivePinRule.
//
// The eviction runs on the event loop (retainOnlyRequest → handleRetainOnly),
// never on the caller's goroutine: the loop is the only writer of the slot
// table, and an eviction from outside could land between a handler's unlock
// and the work it does with the slot it just changed (see retainOnlyRequest).
//
// true means the loop has applied the rule: the evicted slots are gone, their
// removal published, their sessions closed and their teardown callbacks run.
// false means it is not confirmed — the loop is not running (before Run there
// are no slots; shutdown clears the table itself), or ctx or the loop's
// context ended first. A request already queued may still be applied after a
// false; every fill re-applies the rule anyway, so a lost request costs at
// most the time until the next fill.
//
// Caller contract: never call it from the event loop (a CM callback), and never
// while holding cm.mu, PeerProvider.mu or any Service domain mutex. The loop
// takes all of those while handling the events queued ahead of this one, so
// waiting for it under any of them is a deadlock — and from the loop itself the
// request could never be reached at all.
func (cm *ConnectionManager) RetainOnly(ctx context.Context, keep domain.PeerAddress) bool {
	request := newRetainOnlyRequest(keep)
	if !cm.emitSlotUnless(ctx.Done(), request) {
		return false
	}
	if cm.config.RetainOnlyEnqueued != nil {
		cm.config.RetainOnlyEnqueued()
	}
	return cm.awaitRetainOnly(ctx, request)
}

// awaitRetainOnly waits until the event loop has settled an enqueued request,
// ctx ends, or the loop's context ends. Only valid once the request was
// accepted by emitSlotUnless: that proves Run had published cm.ctx (accepting
// is set after it), so reading it here is safe. Watching cm.ctx matters: a
// producer is not tracked by dialWg, so its request can land in the channel
// after shutdown has drained it, and nobody settles that one.
//
// When the request was settled AND a context ended by the time the waiter
// wakes, select would pick either; the settled request wins, because the
// eviction did run and false would say it had not.
func (cm *ConnectionManager) awaitRetainOnly(ctx context.Context, request retainOnlyRequest) bool {
	select {
	case <-request.applied:
		return true
	case <-ctx.Done():
	case <-cm.ctx.Done():
	}
	select {
	case <-request.applied:
		return true
	default:
		return false
	}
}

// handleRetainOnly applies a retainOnlyRequest on the event loop and releases
// its waiter last, so RetainOnly returns only once the evictions, their
// publications, their teardown callbacks and any refill have all happened.
func (cm *ConnectionManager) handleRetainOnly(ctx context.Context, ev retainOnlyRequest) {
	defer ev.settle()

	keep, pinned := cm.retentionTarget(ev.keep)
	if !pinned {
		log.Info().
			Str("requested", connectOnlyPinLabel(ev.keep)).
			Msg("cm: retain-only request found no live connect_only pin, nothing evicted")
		return
	}
	cm.retainOnlyAddress(keep)
	cm.refillMissingLivePin(ctx, keep)
}

// refillMissingLivePin fills at once when retention left the live pin without
// a slot — its own manual dial can have been refused by the same-host dedup
// while an evicted port of that host still held a slot — instead of leaving
// egress at zero until the periodic fill. Only with a pin source wired:
// without one keep is a test label and Candidates() is not pin-gated, so a
// fill would re-dial the peers just evicted. Runs on the event loop.
func (cm *ConnectionManager) refillMissingLivePin(ctx context.Context, keep domain.PeerAddress) {
	if cm.config.ConnectOnlyFn == nil || !cm.bootstrapped {
		return
	}
	cm.mu.RLock()
	present := cm.findSlotLocked(keep) != nil
	cm.mu.RUnlock()
	if present {
		return
	}
	cm.fill(ctx)
}

// retentionTarget is the address the pin rule keeps: the live pin when a pin
// source is wired — pinned=false when the operator has cleared it — and the
// requested address otherwise.
func (cm *ConnectionManager) retentionTarget(requested domain.PeerAddress) (domain.PeerAddress, bool) {
	if cm.config.ConnectOnlyFn == nil {
		return requested, true
	}
	return cm.config.ConnectOnlyFn()
}

// enforceLivePinRule applies the pin rule on every fill: while a pin is live,
// the slot table holds only the live pin. RetainOnly applies the same rule on
// demand; doing it here as well makes the rule level-triggered, so a slot that
// a lost request (an abandoned RPC, a pin write whose retention never reached
// the loop) left behind is evicted at the next fill — the periodic ticker
// bounds that — instead of living for as long as it stays connected. No pin
// source, or no live pin: nothing to enforce.
func (cm *ConnectionManager) enforceLivePinRule() {
	if cm.config.ConnectOnlyFn == nil {
		return
	}
	pin, pinned := cm.config.ConnectOnlyFn()
	if !pinned {
		return
	}
	cm.retainOnlyAddress(pin)
}

// retainOnlyAddress evicts every slot whose address differs from keep. Mirrors
// shrinkToLimit's teardown discipline: slots are detached and removed under
// cm.mu; removals are published and sessions closed and torn down after the
// lock is released. keep is matched verbatim against slot.Address (the dial
// address); a keep that matches no slot evicts everything — the pinned dial is
// enqueued separately (ManualPeerRequested), or by the next fill.
func (cm *ConnectionManager) retainOnlyAddress(keep domain.PeerAddress) {
	cm.mu.Lock()
	var victims []*slot
	for _, s := range cm.slots {
		if s.Address != keep {
			victims = append(victims, s)
		}
	}
	if len(victims) == 0 {
		cm.mu.Unlock()
		return
	}

	log.Info().
		Str("keep", connectOnlyPinLabel(keep)).
		Int("evicting", len(victims)).
		Int("slots", len(cm.slots)).
		Msg("cm: retaining only pinned peer, evicting other outbound slots")

	evictions := cm.evictSlotsLocked(victims)
	cm.mu.Unlock()

	cm.publishEvictions(evictions)
}

// manualPeerOutsideLivePin reports whether a live connect_only pin forbids
// dialling address: under a pin this node dials only the pinned peer, and an
// add_peer of anyone else would otherwise open a slot the next fill evicts.
func (cm *ConnectionManager) manualPeerOutsideLivePin(address domain.PeerAddress) (domain.PeerAddress, bool) {
	if cm.config.ConnectOnlyFn == nil {
		return "", false
	}
	pin, pinned := cm.config.ConnectOnlyFn()
	return pin, pinned && pin != address
}

// beginInitSlotLocked transitions a slot to Initializing after a successful
// TCP handshake. The slot is NOT yet Active — application-level setup
// (initPeerSession) has not completed. ActiveCount() and
// buildPeerExchangeResponse() count only Active slots, so this peer is not
// advertised until SessionInitReady arrives; Slots() does report it, as
// Initializing with its Identity, for diagnostics.
//
// Caller must hold cm.mu.Lock. Returns SessionInfo for the caller to publish
// the Initializing state and invoke OnSessionEstablished AFTER releasing the
// lock — so the state is visible to Slots() readers, and published, before the
// callback has run.
func (cm *ConnectionManager) beginInitSlotLocked(s *slot, ev DialSucceeded) SessionInfo {
	s.State = domain.SlotStateInitializing
	s.Session = ev.Session
	s.ConnectedAddress = ev.ConnectedAddress
	s.RetryCount = 0
	s.Generation = cm.nextGenerationLocked()

	log.Info().
		Str("address", string(s.Address)).
		Str("connected_via", string(ev.ConnectedAddress)).
		Str("identity", ev.Session.peerIdentity.String()).
		Msg("cm: slot initializing")

	return SessionInfo{
		Address:        s.Address,
		DialAddress:    ev.Session.address,
		Identity:       ev.Session.peerIdentity,
		Capabilities:   ev.Session.capabilities,
		ConnID:         ev.Session.connID,
		Session:        ev.Session,
		SlotGeneration: s.Generation,
	}
}

// promoteSlotLocked transitions a slot from Initializing to Active.
// Called when the application-level init (initPeerSession) succeeds.
// Caller must hold cm.mu.Lock and publish the Active state after releasing it.
func (cm *ConnectionManager) promoteSlotLocked(s *slot) {
	s.State = domain.SlotStateActive

	log.Info().
		Str("address", string(s.Address)).
		Str("connected_via", string(s.ConnectedAddress)).
		Msg("cm: slot activated")
}

// slotDeactivationReason says why the event loop takes a session away from its
// slot. It is recorded on the session when the loop closes it (see
// peerSession.closedBy), because the session goroutine that may still be
// running on it must know whether the failure it is about to observe on the
// closed transport is the peer's or this node's doing.
type slotDeactivationReason int

const (
	// slotDeactivationLocalEviction: this node decided to drop the slot —
	// shrinkToLimit, the add_peer eviction, RetainOnly, the connect_only pin,
	// shutdown. Says nothing about the peer, so the session goroutine charges
	// it nothing.
	slotDeactivationLocalEviction slotDeactivationReason = iota + 1
	// slotDeactivationOutcomeReported: the session goroutine or dial worker
	// has already reported the loss (ActiveSessionLost, DialFailed) and
	// charged it. The session goroutine closes its session before it reports,
	// so the loop's close is then a no-op; see sessionCloser.
	slotDeactivationOutcomeReported
)

// sessionCloser maps the reason onto the close reason the session records if
// the loop's close is the first one.
func (r slotDeactivationReason) sessionCloser() peerSessionCloser {
	switch r {
	case slotDeactivationLocalEviction:
		return peerSessionClosedByLocalEviction
	default:
		// slotDeactivationOutcomeReported, and any value nobody chose. The
		// owner has normally closed the session already and this close
		// changes nothing; if it ever comes first, it is recorded as the
		// owner's, which is the reason that never excuses the peer.
		return peerSessionClosedByOwner
	}
}

// sessionTeardown is a session the event loop has taken away from its slot
// but not closed yet: the close is I/O and its onClose takes peerMu, so it
// runs after cm.mu is released, in tearDownSession.
type sessionTeardown struct {
	info   SessionInfo
	closer peerSessionCloser
}

// deactivateSlotLocked detaches the session of an active or initializing slot.
// Called before reconnect, replace, eviction or shutdown. It does NOT close the
// session: closing is I/O, and the session's onClose takes peerMu, so a close
// here would be I/O under cm.mu and a cm.mu → peerMu edge. The caller hands the
// result to tearDownSession after releasing cm.mu. Returns nil when the slot
// had no session to tear down.
//
// Handles both domain.SlotStateActive and domain.SlotStateInitializing — during init the
// slot already holds a Session that must be closed on failure or shutdown.
//
// Whoever closes a session first records why (peerSession.closedBy). For a
// local eviction that is this manager, through tearDownSession; for a loss the
// session goroutine reported, the goroutine already closed it. The session
// goroutine accounts for the close by that reason and emits
// ActiveSessionLost, which the generation guard suppresses, because the
// caller either removed the slot or moved its generation on.
//
// Caller holds cm.mu.Lock.
func (cm *ConnectionManager) deactivateSlotLocked(s *slot, reason slotDeactivationReason) *sessionTeardown {
	if (s.State != domain.SlotStateActive && s.State != domain.SlotStateInitializing) || s.Session == nil {
		return nil
	}

	teardown := &sessionTeardown{
		info: SessionInfo{
			Address:      s.Address,
			DialAddress:  s.Session.address,
			Identity:     s.Session.peerIdentity,
			Capabilities: s.Session.capabilities,
			ConnID:       s.Session.connID,
			Session:      s.Session, // kept for pointer-compare ownership guard in onCMSessionTeardown
		},
		closer: reason.sessionCloser(),
	}
	s.Session = nil
	s.ConnectedAddress = ""

	return teardown
}

// tearDownSession closes a detached session and then runs OnSessionTeardown
// for it. Runs on the event loop with cm.mu released. The order is the
// contract registerCMSession relies on: the close records the reason before
// the callback takes peerMu, so a registration that runs after the callback's
// section sees the reason and a registration that ran before it is withdrawn
// by the callback. Nil-safe for slots that had no session.
func (cm *ConnectionManager) tearDownSession(teardown *sessionTeardown) {
	if teardown == nil {
		return
	}
	// The close error is the transport's own; the slot is given up either
	// way and nothing here could act on it.
	_ = teardown.info.Session.closeAs(teardown.closer)
	if cm.config.OnSessionTeardown != nil {
		cm.config.OnSessionTeardown(teardown.info)
	}
}

// tearDownSessions is tearDownSession for every entry, in order.
func (cm *ConnectionManager) tearDownSessions(teardowns []*sessionTeardown) {
	for _, teardown := range teardowns {
		cm.tearDownSession(teardown)
	}
}

// replaceSlotLocked removes a slot whose peer is exhausted.
// Caller must hold cm.mu.Lock. Returns the detached session, if the slot had
// one, for tearDownSession once cm.mu is released.
func (cm *ConnectionManager) replaceSlotLocked(s *slot, reason slotDeactivationReason) *sessionTeardown {
	teardown := cm.deactivateSlotLocked(s, reason)
	cm.removeSlotLocked(s)
	return teardown
}

// ---------------------------------------------------------------------------
// Dial workers (run in goroutines, never touch slots directly)
// ---------------------------------------------------------------------------

func (cm *ConnectionManager) dialWorker(ctx context.Context, address domain.PeerAddress, dialAddresses []domain.PeerAddress, generation uint64) {
	defer cm.dialWg.Done()

	// Pacer gate: hold off the DialFn until a token is available so a
	// thundering-herd fill or reconnect burst is smeared in time. Nil
	// pacer (DialPacerInterval == 0) is the disabled-mode fast path.
	if cm.pacer != nil {
		if !cm.pacer.Acquire(ctx) {
			// ctx cancelled before a token arrived — caller (event loop)
			// is shutting down. Best-effort emit DialFailed so the slot
			// transitions out of Dialing instead of dangling.
			_ = cm.EmitSlot(DialFailed{
				Address:        address,
				Error:          ctx.Err(),
				SlotGeneration: generation,
			})
			return
		}
	}

	cm.dialWorkerBody(ctx, address, dialAddresses, generation)
}

// dialWorkerImmediate is the pacer-bypassing variant used by
// handleManualPeer. Operator-driven actions (addpeer) must dial
// immediately even when the global rate-limit would otherwise hold
// them — the operator already accepted the cost by typing the command.
// Apart from the missing Acquire, the body is identical.
func (cm *ConnectionManager) dialWorkerImmediate(ctx context.Context, address domain.PeerAddress, dialAddresses []domain.PeerAddress, generation uint64) {
	defer cm.dialWg.Done()
	cm.dialWorkerBody(ctx, address, dialAddresses, generation)
}

// dialWorkerBody is the dial → result → emit logic shared by paced
// and immediate workers. Factored out so the pacer gate sits in a
// single, obvious location (dialWorker) rather than getting copy-pasted.
func (cm *ConnectionManager) dialWorkerBody(ctx context.Context, address domain.PeerAddress, dialAddresses []domain.PeerAddress, generation uint64) {
	result, err := cm.config.DialFn(ctx, dialAddresses)
	if err != nil {
		// Best-effort: ctx cancelled means no one is listening.
		_ = cm.EmitSlot(DialFailed{
			Address:        address,
			Error:          err,
			Incompatible:   errors.Is(err, errIncompatibleProtocol),
			SlotGeneration: generation,
		})
		return
	}

	if !cm.EmitSlot(DialSucceeded{
		Address:          address,
		ConnectedAddress: result.ConnectedAddress,
		Session:          result.Session,
		SlotGeneration:   generation,
	}) {
		// Shutdown: event loop won't consume this. We own the session.
		_ = result.Session.Close()
	}
}

func (cm *ConnectionManager) retryAfterBackoff(ctx context.Context, address domain.PeerAddress, dialAddresses []domain.PeerAddress, generation uint64, attempt int) {
	defer cm.dialWg.Done()

	backoff := cm.config.BackoffFn(attempt)

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		return
	}

	// Pacer gate after backoff so retries respect the same global rate
	// limit as fill() — without this a burst of retries from many slots
	// can fire simultaneously the instant their backoff elapses.
	if cm.pacer != nil {
		if !cm.pacer.Acquire(ctx) {
			_ = cm.EmitSlot(DialFailed{
				Address:        address,
				Error:          ctx.Err(),
				Attempt:        attempt,
				SlotGeneration: generation,
			})
			return
		}
	}

	result, err := cm.config.DialFn(ctx, dialAddresses)
	if err != nil {
		// Best-effort: shutdown means no one is listening.
		_ = cm.EmitSlot(DialFailed{
			Address:        address,
			Error:          err,
			Attempt:        attempt,
			Incompatible:   errors.Is(err, errIncompatibleProtocol),
			SlotGeneration: generation,
		})
		return
	}

	if !cm.EmitSlot(DialSucceeded{
		Address:          address,
		ConnectedAddress: result.ConnectedAddress,
		Session:          result.Session,
		SlotGeneration:   generation,
	}) {
		_ = result.Session.Close()
	}
}

func backoffDuration(attempt int) time.Duration {
	d := reconnectBackoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > reconnectBackoffMax {
			d = reconnectBackoffMax
			break
		}
	}
	return d
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

func (cm *ConnectionManager) shutdown() {
	// 0. Close the emit gate so EmitSlot rejects new events immediately.
	//    Workers that already passed the guard may still enqueue into the
	//    buffered channel — drainChannels() below handles those.
	//    Note: startOnce is NOT reset — a second Run() will still panic.
	cm.accepting.Store(0)

	cm.mu.Lock()

	log.Info().Int("slots", len(cm.slots)).Msg("cm: shutting down")

	// 1. Deactivate all active slots (close sessions + session-map
	//    cleanup). Shutdown is a local decision: the peers did nothing.
	teardowns := make([]*sessionTeardown, 0, len(cm.slots))
	for _, s := range cm.slots {
		teardowns = append(teardowns, cm.deactivateSlotLocked(s, slotDeactivationLocalEviction))
	}

	// 2. Clear slots, settling each slot's budget unit on the way out. The
	//    dials still in flight keep theirs until they report back; the
	//    orphan table is drained after the workers are joined below.
	for _, s := range cm.slots {
		cm.orphanReservationLocked(s)
	}
	cm.slots = nil

	cm.mu.Unlock()

	// 3. Close the sessions and invoke teardown callbacks outside the lock.
	cm.tearDownSessions(teardowns)

	// 4. Wait for all in-flight dial goroutines to finish.
	//    After this returns, no goroutine can emit into slotEvents/hintEvents,
	//    so drainChannels will see every event that will ever be produced.
	cm.dialWg.Wait()

	// 5. Drain channels: pick up events from workers that finished
	//    between ctx.Done() and now. Close any stale sessions.
	cm.drainChannels()

	// 6. Every dial worker has returned and every stale session it may have
	//    produced is closed, so nothing accounted against the budget is
	//    still alive. Release what is left.
	cm.releaseAllOrphans()
}

func (cm *ConnectionManager) drainChannels() {
	for {
		select {
		case ev := <-cm.slotEvents:
			cm.settleUndeliveredSlotEvent(ev)
		case <-cm.hintEvents:
			// ignore
		default:
			return // channels empty
		}
	}
}

// settleUndeliveredSlotEvent releases what an event the loop will never handle
// still holds: a dialled session that nobody else will close, and a RetainOnly
// waiter that would otherwise wait for an application that never comes. The
// slot table is already empty, so a RetainOnly request has nothing left to
// evict. Every other event carries nothing to release.
func (cm *ConnectionManager) settleUndeliveredSlotEvent(event SlotEvent) {
	switch ev := event.(type) {
	case DialSucceeded:
		if ev.Session != nil {
			// The dial worker handed the session over and will not close it;
			// its close error is of no use to a shutdown.
			_ = ev.Session.closeAs(peerSessionClosedByLocalEviction)
		}
	case retainOnlyRequest:
		ev.settle()
	}
}

// ---------------------------------------------------------------------------
// Internal helpers (called under mu.Lock or from event loop)
// ---------------------------------------------------------------------------

func (cm *ConnectionManager) findSlotLocked(address domain.PeerAddress) *slot {
	for _, s := range cm.slots {
		if s.Address == address {
			return s
		}
	}
	return nil
}

func (cm *ConnectionManager) removeSlotLocked(target *slot) {
	for i, s := range cm.slots {
		if s == target {
			// Settle the budget BEFORE the slot stops existing: a dial
			// still in flight keeps the capacity accounted (parked by
			// generation), anything else releases it now.
			cm.orphanReservationLocked(s)
			cm.slots = append(cm.slots[:i], cm.slots[i+1:]...)
			return
		}
	}
}

func (cm *ConnectionManager) nextGenerationLocked() uint64 {
	cm.generation++
	return cm.generation
}

// ---------------------------------------------------------------------------
// Event name helpers (for logging)
// ---------------------------------------------------------------------------

func hintEventName(ev HintEvent) string {
	switch ev.(type) {
	case InboundClosed:
		return "InboundClosed"
	case NewPeersDiscovered:
		return "NewPeersDiscovered"
	case BootstrapReady:
		return "BootstrapReady"
	default:
		return "unknown"
	}
}
