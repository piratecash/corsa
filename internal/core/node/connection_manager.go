package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// ---------------------------------------------------------------------------
// ConnectionManager — event-driven outbound connection lifecycle
// ---------------------------------------------------------------------------
//
// Stage 2 deliverable (see docs/connection-manager.md): self-contained
// component with full test coverage. Production wiring into Service
// (replacing ensurePeerSessions / bootstrapLoop) is Stage 3.
//
// ConnectionManager owns a fixed-size array of slots. Each slot tracks one
// outbound peer through its lifecycle: Queued → Dialing → Active (or
// Reconnecting / RetryWait / Replacing on failure).
//
// All slot mutations happen inside a single-goroutine event loop (Run).
// Dial workers communicate exclusively through typed events — they never
// touch slots directly.
//
// Two event channels plus a dedicated bootstrap signal:
//   - slotEvents: blocking send, loss not tolerated (DialFailed, DialSucceeded, ActiveSessionLost)
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
// Only mutated by the event loop goroutine (under cm.mu.Lock).
type slot struct {
	Address          domain.PeerAddress
	DialAddresses    []domain.PeerAddress
	ConnectedAddress domain.PeerAddress // actual endpoint after fallback dial
	State            domain.SlotState
	RetryCount       int
	Generation       uint64 // incremented on every state transition
	Session          *peerSession
}

// ---------------------------------------------------------------------------
// SessionInfo — callback payload for Service integration
// ---------------------------------------------------------------------------

// SessionInfo carries the information Service needs to perform side-effects
// when a session is established or torn down (routing registration,
// score updates, heartbeat, pending frame flush, etc.).
//
// Session and SlotGeneration are populated only in OnSessionEstablished:
//   - Session: the live TCP session. Service starts servePeerSession /
//     heartbeat against it. Ownership transfers to Service.
//   - SlotGeneration: the slot's generation at activation time. Service
//     saves it and later passes it back in ActiveSessionLost so the CM
//     event loop can detect stale events.
//
// On teardown both fields are zero/nil — Service already holds the
// session reference from the earlier OnSessionEstablished call.
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

	// Provider supplies filtered, sorted candidates.
	Provider *PeerProvider

	// DialFn performs TCP connect + handshake. Returns DialResult on success.
	// Must respect ctx for cancellation.
	DialFn func(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error)

	// OnSessionEstablished is called synchronously in the event loop
	// when a slot transitions to Active. Service uses it for:
	//   - markPeerConnected (score +10)
	//   - routing table registration
	//   - heartbeat goroutine launch
	//   - pending frame flush
	OnSessionEstablished func(SessionInfo)

	// OnSessionTeardown is called synchronously in the event loop
	// when an active slot is being deactivated. Service uses it for:
	//   - routing table deregistration
	//   - markPeerDisconnected (score update)
	OnSessionTeardown func(SessionInfo)

	// OnStaleSession is called when handleDialSucceeded detects a
	// generation mismatch and discards the session. The dial worker's
	// openPeerSessionForCM registered the session in Service.sessions
	// before returning; since onCMSessionEstablished never runs for
	// the stale generation, Service must remove the orphaned entry.
	//
	// The callback receives the raw *peerSession (not SessionInfo)
	// so Service can compare by pointer — only deleting s.sessions[addr]
	// when it still points to this exact session, avoiding accidental
	// removal of a newer valid session for the same address.
	OnStaleSession func(session *peerSession)

	// OnDialFailed is called synchronously in the event loop when a
	// dial attempt fails. Service uses it to update health/ban state
	// (markPeerDisconnected, penalizeOldProtocolPeer) BEFORE fill()
	// re-queries Candidates(). Without this callback, replace + fill
	// can re-pick the same peer we just gave up on.
	OnDialFailed func(address domain.PeerAddress, err error, incompatible bool)

	// BackoffFn returns the backoff duration for the given retry attempt.
	// When nil, exponential backoff with base 2s / max 10s is used.
	// Injected for testability (zero backoff in tests).
	BackoffFn func(attempt int) time.Duration

	// NowFn returns current time. Injected for testability.
	NowFn func() time.Time

	// FillInterval overrides periodicFillInterval for testing.
	// When zero, the default periodicFillInterval is used.
	FillInterval time.Duration

	// EventBus is used to publish TopicSlotStateChanged when a slot
	// transitions between states. May be nil (tests, standalone usage).
	EventBus *ebus.Bus
}

// ConnectionManager manages the lifecycle of outbound connection slots.
type ConnectionManager struct {
	// mu protects slots for concurrent read access from QueuedIPs/Slots/ActiveCount.
	// The event loop (Run) is the sole writer — it holds Lock during mutations.
	// External readers hold RLock.
	mu         sync.RWMutex
	slots      []*slot
	generation uint64 // monotonic counter for slot generations

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
	// drainChannels sees every event that will ever be emitted —
	// eliminating the race where EmitSlot enqueues into a buffered
	// channel after drain has already returned.
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
		slots:       make([]*slot, 0, maxSlots),
		config:      cfg,
		slotEvents:  make(chan SlotEvent, slotEventBuffer(maxSlots)),
		hintEvents:  make(chan HintEvent, hintEventBuffer),
		bootstrapCh: make(chan struct{}),
		readyCh:     make(chan struct{}),
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
	if cm.accepting.Load() != 1 {
		return false
	}
	select {
	case cm.slotEvents <- event:
		return true
	case <-cm.ctx.Done():
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
// added via add_peer. Unlike fill(), this bypasses Candidates() filtering
// (the operator explicitly requested this peer).
//
// Dedup: checks both exact address AND IP to prevent two slots to the same host.
// Slot limit: if at capacity, evicts the lowest-scoring non-active slot (or the
// lowest-scoring active slot as last resort) to make room — operator intent takes
// priority, but the maxSlots invariant is preserved.
func (cm *ConnectionManager) handleManualPeer(ctx context.Context, ev ManualPeerRequested) {
	dialAddrs := ev.DialAddresses
	if len(dialAddrs) == 0 {
		dialAddrs = []domain.PeerAddress{ev.Address}
	}

	targetIP, _, _ := splitHostPort(string(ev.Address))

	cm.mu.Lock()

	// Dedup by exact address.
	if cm.findSlotLocked(ev.Address) != nil {
		cm.mu.Unlock()
		log.Debug().Str("address", string(ev.Address)).Msg("cm: manual peer already has a slot")
		return
	}

	// Dedup by IP — another port on the same host already has a slot.
	if targetIP != "" {
		for _, existing := range cm.slots {
			ip, _, _ := splitHostPort(string(existing.Address))
			if ip == targetIP {
				cm.mu.Unlock()
				log.Debug().
					Str("address", string(ev.Address)).
					Str("existing", string(existing.Address)).
					Msg("cm: manual peer IP already has a slot")
				return
			}
		}
	}

	// Enforce slot limit: evict one slot if at capacity.
	maxSlots := cm.config.MaxSlotsFn()
	var teardownInfo *SessionInfo
	var evictedAddr domain.PeerAddress
	if len(cm.slots) >= maxSlots {
		victim := cm.findLowestScoringSlotLocked()
		if victim != nil {
			evictedAddr = victim.Address
			log.Info().
				Str("evicted", string(victim.Address)).
				Str("manual", string(ev.Address)).
				Msg("cm: evicting slot to make room for manual peer")
			if info := cm.deactivateSlotLocked(victim); info != nil {
				teardownInfo = info
			}
			cm.removeSlotLocked(victim)
		}
	}

	gen := cm.nextGenerationLocked()
	s := &slot{
		Address:       ev.Address,
		DialAddresses: dialAddrs,
		State:         domain.SlotStateDialing,
		Generation:    gen,
	}
	cm.slots = append(cm.slots, s)
	cm.mu.Unlock()

	// Emit slot events outside the lock: eviction (removal) then new dial.
	if evictedAddr != "" {
		cm.emitSlotRemoved(evictedAddr)
	}
	cm.emitSlotStateChanged(ev.Address, domain.SlotStateDialing)

	// Invoke teardown callback outside lock.
	if teardownInfo != nil && cm.config.OnSessionTeardown != nil {
		cm.config.OnSessionTeardown(*teardownInfo)
	}

	log.Info().
		Str("address", string(ev.Address)).
		Uint64("generation", gen).
		Msg("cm: manual peer enqueued for immediate dial")

	cm.dialWg.Add(1)
	go cm.dialWorker(ctx, ev.Address, dialAddrs, gen)
}

// findLowestScoringSlotLocked returns the slot with the lowest score for eviction.
// Prefers non-active slots. Returns nil only if no slots exist.
// Caller must hold cm.mu.Lock.
func (cm *ConnectionManager) findLowestScoringSlotLocked() *slot {
	var best *slot
	bestScore := int(^uint(0) >> 1) // max int
	bestActive := true

	for _, s := range cm.slots {
		isActive := s.State == domain.SlotStateActive
		score := 0
		if cm.config.Provider != nil {
			score = cm.config.Provider.Score(s.Address)
		}

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

	// Cleanup side-effects before reconnect.
	teardownInfo := cm.deactivateSlotLocked(s)

	// WasHealthy distinguishes two failure modes:
	//
	//   true  — a session that was fully operational (servePeerSession ran)
	//           lost its connection (EOF, timeout, remote close). The peer
	//           was healthy recently, so reset retry count and reconnect
	//           immediately.
	//
	//   false — the session never became operational because post-handshake
	//           setup (subscribe_inbox / syncPeerSession) failed. This is
	//           semantically a dial failure — the peer is reachable but
	//           unable to complete the application protocol. Without
	//           backoff, the CM would spin an infinite reconnect loop
	//           against a permanently bad peer while starving candidates.
	//           Route through the same retry/replace logic as DialFailed.
	if !ev.WasHealthy {
		s.RetryCount++

		if s.RetryCount > reconnectMaxRetries {
			log.Info().
				Str("address", string(ev.Address)).
				Int("retries", s.RetryCount).
				Msg("cm: replacing slot after repeated setup failures")

			replaceTeardown := cm.replaceSlotLocked(s)
			addr := s.Address
			cm.mu.Unlock()

			// Slot removed — emit empty state so subscribers clear the peer.
			cm.emitSlotRemoved(addr)

			if teardownInfo != nil && cm.config.OnSessionTeardown != nil {
				cm.config.OnSessionTeardown(*teardownInfo)
			}
			if replaceTeardown != nil && cm.config.OnSessionTeardown != nil {
				cm.config.OnSessionTeardown(*replaceTeardown)
			}
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

		if teardownInfo != nil && cm.config.OnSessionTeardown != nil {
			cm.config.OnSessionTeardown(*teardownInfo)
		}
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

	if teardownInfo != nil && cm.config.OnSessionTeardown != nil {
		cm.config.OnSessionTeardown(*teardownInfo)
	}

	log.Info().
		Str("address", string(ev.Address)).
		Str("identity", string(ev.Identity)).
		Err(ev.Error).
		Msg("cm: active session lost, reconnecting")

	cm.dialWg.Add(1)
	go cm.dialWorker(ctx, addr, dialAddrs, gen)
}

func (cm *ConnectionManager) handleDialFailed(ctx context.Context, ev DialFailed) {
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

	s.RetryCount++

	if ev.Incompatible || s.RetryCount > reconnectMaxRetries {
		log.Debug().
			Str("address", string(ev.Address)).
			Bool("incompatible", ev.Incompatible).
			Int("retries", s.RetryCount).
			Msg("cm: replacing slot")

		teardownInfo := cm.replaceSlotLocked(s)
		replacedAddr := s.Address
		cm.mu.Unlock()

		// Slot removed — emit empty state so subscribers clear the peer.
		cm.emitSlotRemoved(replacedAddr)

		if teardownInfo != nil && cm.config.OnSessionTeardown != nil {
			cm.config.OnSessionTeardown(*teardownInfo)
		}

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
		cm.mu.Unlock()
		// Stale: slot already replaced or transitioned.
		// The dial worker's openPeerSessionForCM registered the session in
		// Service.sessions before returning. Since onCMSessionEstablished
		// will never run for a stale generation, call OnStaleSession so
		// Service can remove the now-orphaned entry from s.sessions.
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

	if cm.config.OnSessionEstablished != nil {
		cm.config.OnSessionEstablished(info)
	}
}

// handleSessionInitReady promotes a slot from Initializing to Active after
// the application-level setup (subscribe_inbox + syncPeerSession) succeeds.
// Until this event arrives, Slots() and buildPeerExchangeResponse() do not
// expose the slot — preventing advertisement of peers that are not yet usable.
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
	// (another event could have been processed in between, but since
	// fill() is called from the single-threaded event loop this
	// cannot happen — still, defensive re-check costs nothing).
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
		gen := cm.nextGenerationLocked()
		s := &slot{
			Address:       candidates[i].Address,
			DialAddresses: candidates[i].DialAddresses,
			State:         domain.SlotStateDialing,
			Generation:    gen,
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

	// Deactivate and remove victims. Capture addresses before removal.
	var teardowns []SessionInfo
	evictedAddrs := make([]domain.PeerAddress, 0, len(victims))
	for _, v := range victims {
		evictedAddrs = append(evictedAddrs, v.Address)
		if info := cm.deactivateSlotLocked(v); info != nil {
			teardowns = append(teardowns, *info)
		}
		cm.removeSlotLocked(v)
	}

	cm.mu.Unlock()

	// Emit slot-removed events outside the lock so subscribers see the
	// peer disappear from CM tracking. Empty state signals removal.
	for _, addr := range evictedAddrs {
		cm.emitSlotRemoved(addr)
	}

	// Invoke teardown callbacks outside the lock.
	if cm.config.OnSessionTeardown != nil {
		for _, info := range teardowns {
			cm.config.OnSessionTeardown(info)
		}
	}
}

// beginInitSlotLocked transitions a slot to Initializing after a successful
// TCP handshake. The slot is NOT yet Active — application-level setup
// (subscribe_inbox, syncPeerSession) has not completed. Slots(), ActiveCount(),
// and buildPeerExchangeResponse() only expose Active slots, so this peer is
// invisible to peer exchange and diagnostics until SessionInitReady arrives.
//
// Caller must hold cm.mu.Lock. Returns SessionInfo for the caller to invoke
// OnSessionEstablished AFTER releasing the lock.
func (cm *ConnectionManager) beginInitSlotLocked(s *slot, ev DialSucceeded) SessionInfo {
	s.State = domain.SlotStateInitializing
	s.Session = ev.Session
	s.ConnectedAddress = ev.ConnectedAddress
	s.RetryCount = 0
	s.Generation = cm.nextGenerationLocked()

	cm.emitSlotStateChanged(s.Address, domain.SlotStateInitializing)

	log.Info().
		Str("address", string(s.Address)).
		Str("connected_via", string(ev.ConnectedAddress)).
		Str("identity", string(ev.Session.peerIdentity)).
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
// Caller must hold cm.mu.Lock.
func (cm *ConnectionManager) promoteSlotLocked(s *slot) {
	s.State = domain.SlotStateActive

	log.Info().
		Str("address", string(s.Address)).
		Str("connected_via", string(s.ConnectedAddress)).
		Msg("cm: slot activated")

	// Safe to call under cm.mu — ebus uses its own RWMutex.
	cm.emitSlotStateChanged(s.Address, domain.SlotStateActive)
}

// deactivateSlotLocked performs cleanup when an active or initializing slot
// loses its session. Called before reconnect, replace, or shutdown. Closes the
// TCP transport and returns SessionInfo if teardown callback is needed. The
// caller must invoke OnSessionTeardown AFTER releasing the lock.
// Returns nil when the slot had no session to tear down.
//
// Handles both domain.SlotStateActive and domain.SlotStateInitializing — during init the
// slot already holds a Session that must be closed on failure or shutdown.
//
// CM owns the transport lifecycle: it closes the underlying connection here.
// Service's servePeerSession detects the close (EOF) and emits
// ActiveSessionLost — which the generation guard suppresses because the
// slot's generation was already incremented by the caller.
func (cm *ConnectionManager) deactivateSlotLocked(s *slot) *SessionInfo {
	if (s.State != domain.SlotStateActive && s.State != domain.SlotStateInitializing) || s.Session == nil {
		return nil
	}

	info := SessionInfo{
		Address:      s.Address,
		DialAddress:  s.Session.address,
		Identity:     s.Session.peerIdentity,
		Capabilities: s.Session.capabilities,
		ConnID:       s.Session.connID,
		Session:      s.Session, // kept for pointer-compare ownership guard in onCMSessionTeardown
	}

	_ = s.Session.Close()
	s.Session = nil
	s.ConnectedAddress = ""

	return &info
}

// replaceSlotLocked removes a slot whose peer is exhausted.
// Caller must hold cm.mu.Lock. Returns teardown info if the slot was active.
func (cm *ConnectionManager) replaceSlotLocked(s *slot) *SessionInfo {
	info := cm.deactivateSlotLocked(s)
	cm.removeSlotLocked(s)
	return info
}

// ---------------------------------------------------------------------------
// Dial workers (run in goroutines, never touch slots directly)
// ---------------------------------------------------------------------------

func (cm *ConnectionManager) dialWorker(ctx context.Context, address domain.PeerAddress, dialAddresses []domain.PeerAddress, generation uint64) {
	defer cm.dialWg.Done()

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

	// 1. Deactivate all active slots (close sessions + routing cleanup).
	var teardowns []SessionInfo
	for _, s := range cm.slots {
		if info := cm.deactivateSlotLocked(s); info != nil {
			teardowns = append(teardowns, *info)
		}
	}

	// 2. Clear slots.
	cm.slots = nil

	cm.mu.Unlock()

	// 3. Invoke teardown callbacks outside the lock.
	if cm.config.OnSessionTeardown != nil {
		for _, info := range teardowns {
			cm.config.OnSessionTeardown(info)
		}
	}

	// 4. Wait for all in-flight dial goroutines to finish.
	//    After this returns, no goroutine can emit into slotEvents/hintEvents,
	//    so drainChannels will see every event that will ever be produced.
	cm.dialWg.Wait()

	// 5. Drain channels: pick up events from workers that finished
	//    between ctx.Done() and now. Close any stale sessions.
	cm.drainChannels()
}

func (cm *ConnectionManager) drainChannels() {
	for {
		select {
		case ev := <-cm.slotEvents:
			if ds, ok := ev.(DialSucceeded); ok && ds.Session != nil {
				_ = ds.Session.Close()
			}
		case <-cm.hintEvents:
			// ignore
		default:
			return // channels empty
		}
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
