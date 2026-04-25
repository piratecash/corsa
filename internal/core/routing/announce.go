package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
)

const (
	// DefaultAnnounceInterval is the periodic announcement interval.
	// Every 30 seconds, the node sends its routing table to all peers
	// that support mesh_routing_v1.
	DefaultAnnounceInterval = 30 * time.Second

	// ForcedFullSyncMultiplier controls how often a forced full sync is
	// sent to each peer: every ForcedFullSyncMultiplier * DefaultAnnounceInterval.
	// At 30s interval this means a full sync roughly every 5 minutes.
	ForcedFullSyncMultiplier = 10

	// MinForcedFullSyncInterval is the minimum time between forced full
	// sync attempts for a single peer. Prevents flood when a peer is
	// repeatedly marked NeedsFullResync.
	MinForcedFullSyncInterval = DefaultAnnounceInterval
)

// PeerSender abstracts the ability to send routing announcement frames
// to a specific peer. node.Service implements this interface to decouple
// the routing package from the network layer.
//
// Two wire frames are carried by two distinct methods so that the choice
// of wire format lives on the call site and cannot silently flip inside
// the implementation. The call-site contract is:
//   - connect-time and forced-full paths always use SendAnnounceRoutes
//     (legacy announce_routes wire frame) — this is the "First-sync
//     wire-frame invariant" documented in docs/routing.md;
//   - the delta path chooses between SendAnnounceRoutes (peer on v1 only)
//     and SendRoutesUpdate (peer on v2) based on the peer's capability
//     snapshot captured at cycle start — see AnnounceLoop.announceToAllPeers.
//
// The invariant "the implementation never silently flips the wire format"
// is preserved: each call site decides once, and the interface provides no
// method that could accept the wrong frame. See docs/routing.md for the
// durable announce-plane file map.
type PeerSender interface {
	// SendAnnounceRoutes sends a list of AnnounceEntry items as a legacy
	// announce_routes frame to the peer identified by peerAddress.
	//
	// This is the legacy v1 wire path. It is used for:
	//   - connect-time full sync (always, regardless of v2 capability —
	//     initial sync after session establishment is always the legacy
	//     frame so mixed-version networks never see an unexpected
	//     routes_update);
	//   - periodic forced full sync in the announce loop;
	//   - delta updates for peers that have NOT negotiated the
	//     CapMeshRoutingV2 capability.
	//
	// ctx is the caller's request/cycle context: a pre-cancelled ctx
	// fails fast without touching the transport, and a mid-flight cancel
	// during the inbound sync-flush wait aborts the send rather than
	// consuming the full syncFlushTimeout. This is the contract that
	// makes fanoutAnnounceRoutes cancellable end-to-end — a stuck
	// inbound hairpin socket no longer pins the caller for 5 s once
	// its cycle/shutdown context has cancelled.
	//
	// peerAddress is the transport address used by node.Service to
	// locate the session. Returns true if the frame was enqueued
	// successfully. A false return collapses ctx-cancel, ctx-deadline,
	// transport timeout, writer-done, buffer-full and unregistered-conn
	// into one negative outcome; the PeerSender implementation handles
	// observability internally.
	SendAnnounceRoutes(ctx context.Context, peerAddress PeerAddress, routes []AnnounceEntry) bool

	// SendRoutesUpdate sends a v2 routes_update frame carrying an
	// incremental delta to the peer identified by peerAddress.
	//
	// This method is the v2 capability-gated incremental path. The
	// announce loop picks SendRoutesUpdate only when all of the following
	// hold at cycle start:
	//   - the peer's negotiated capability snapshot contains both
	//     CapMeshRoutingV1 and CapMeshRoutingV2;
	//   - the cycle is a delta send (the peer already has a baseline);
	//   - the crosscheck between AnnouncePeerState.Capabilities and
	//     AnnounceTarget.Capabilities agrees (divergence forces a full
	//     resync via MarkInvalid — see announceToAllPeers).
	//
	// SendRoutesUpdate MUST NOT be used for:
	//   - the first sync after session establishment (initial sync is
	//     always legacy announce_routes — the peer has no baseline to
	//     diff against);
	//   - forced full resync (forced full is always legacy).
	//
	// Semantics of ctx, peerAddress, and the bool return value match
	// SendAnnounceRoutes.
	SendRoutesUpdate(ctx context.Context, peerAddress PeerAddress, delta []AnnounceEntry) bool
}

// AnnounceLoop runs periodic and triggered routing announcements. It
// owns a background goroutine that wakes every DefaultAnnounceInterval
// and sends the local routing table to all capable peers. Triggered
// updates (connect, disconnect) bypass the timer and send immediately.
//
// The loop uses per-peer announce state (via AnnounceStateRegistry) to:
//   - aggregate raw table entries into canonical peer-specific snapshots;
//   - compare each snapshot with the previously sent one;
//   - send only changed entries (delta) to peers that already received
//     a full sync;
//   - suppress no-op sends when the snapshot is unchanged;
//   - force periodic full sync at configurable intervals.
//
// The loop is designed to be started once from node.Service.Run and
// stopped on context cancellation.
type AnnounceLoop struct {
	table    *Table
	sender   PeerSender
	interval time.Duration

	// triggerCh receives signals to send an immediate update.
	// Buffered to 1 so multiple rapid triggers coalesce.
	triggerCh chan struct{}

	// peersFn returns the current list of peers that support
	// mesh_routing_v1. Each entry is (transport_address, identity).
	peersFn func() []AnnounceTarget

	// stateRegistry manages per-peer announce send state. Owned by the
	// AnnounceLoop but architecturally belongs to the service layer.
	stateRegistry *AnnounceStateRegistry

	mu      sync.Mutex
	running bool

	// cycleCounter provides a monotonic announce_cycle_id for log correlation.
	cycleCounter atomic.Uint64
}

// AnnounceTarget identifies a peer for announcement purposes.
//
// Capabilities is an immutable per-cycle snapshot of the peer's negotiated
// capability set, captured by the peersFn implementation under the same
// peer-state lock that produced Address and Identity. The snapshot is taken
// at cycle start precisely so that per-peer goroutines inside
// announceToAllPeers can decide on a wire format (legacy announce_routes vs
// future v2 routes_update) without re-entering the Service's peer mutex per
// peer — that re-entry pattern collides with writer-preferring sync.RWMutex
// semantics and has been observed to starve reads under load.
//
// AnnounceLoop and any downstream consumer MUST treat Capabilities as
// read-only; producers build a fresh slice per target so mutation here cannot
// corrupt session state. Consumers that need a specific capability check
// should range over the slice directly rather than re-fetching capabilities
// from the Service. Until routing-announce v2 lands, the announce loop does
// not branch on Capabilities at all: the field is plumbed so that v2 can be
// added by a single call-site change in announceToAllPeers without touching
// the peersFn contract again.
type AnnounceTarget struct {
	// Address is the transport address used to enqueue frames.
	Address PeerAddress
	// Identity is the peer's Ed25519 fingerprint — used for split horizon.
	Identity PeerIdentity
	// Capabilities is the peer's negotiated capability snapshot for this
	// announce cycle. See the type doc for ownership and mutation rules.
	Capabilities []PeerCapability
}

// AnnounceLoopOption configures the AnnounceLoop.
type AnnounceLoopOption func(*AnnounceLoop)

// WithAnnounceInterval overrides the default periodic interval.
func WithAnnounceInterval(d time.Duration) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.interval = d
	}
}

// WithStateRegistry injects an existing state registry. When not set,
// a default registry is created.
func WithStateRegistry(r *AnnounceStateRegistry) AnnounceLoopOption {
	return func(a *AnnounceLoop) {
		a.stateRegistry = r
	}
}

// NewAnnounceLoop creates a new loop. peersFn is called on every tick to
// discover which peers should receive announcements.
func NewAnnounceLoop(
	table *Table,
	sender PeerSender,
	peersFn func() []AnnounceTarget,
	opts ...AnnounceLoopOption,
) *AnnounceLoop {
	a := &AnnounceLoop{
		table:     table,
		sender:    sender,
		interval:  DefaultAnnounceInterval,
		triggerCh: make(chan struct{}, 1),
		peersFn:   peersFn,
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.stateRegistry == nil {
		a.stateRegistry = NewAnnounceStateRegistry()
	}
	return a
}

// StateRegistry returns the per-peer announce state registry. Used by
// node.Service to integrate session lifecycle events (connect, disconnect)
// with the announce state.
func (a *AnnounceLoop) StateRegistry() *AnnounceStateRegistry {
	return a.stateRegistry
}

// Run starts the periodic announce loop. It blocks until ctx is cancelled.
func (a *AnnounceLoop) Run(ctx context.Context) {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
	}()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.announceToAllPeers(ctx)
		case <-a.triggerCh:
			a.announceToAllPeers(ctx)
			// Reset the ticker so we don't double-announce shortly after
			// a triggered update.
			ticker.Reset(a.interval)
		}
	}
}

// TriggerUpdate requests an immediate announcement cycle. Safe to call
// from any goroutine. Multiple rapid calls coalesce into a single cycle.
func (a *AnnounceLoop) TriggerUpdate() {
	select {
	case a.triggerCh <- struct{}{}:
	default:
		// Already pending — coalesce.
	}
}

// PendingTrigger reports whether a triggered update is queued but not yet
// consumed by the announce loop. Intended for unit tests that verify
// TriggerUpdate was called without running the full loop.
func (a *AnnounceLoop) PendingTrigger() bool {
	select {
	case <-a.triggerCh:
		// Was pending — put it back so the loop still sees it.
		a.triggerCh <- struct{}{}
		return true
	default:
		return false
	}
}

// announceToAllPeers sends the routing table to every capable peer,
// applying split horizon per peer and per-peer delta/cache logic.
//
// Per-peer work runs in its own goroutine so that a single stuck inbound
// socket — bounded per peer by syncFlushTimeout inside
// sender.SendAnnounceRoutes — cannot serialise delivery to the rest. The
// wall-clock of a cycle is the slowest peer, not N × slowest. AnnouncePeerState
// is thread-safe (embedded mutex) and each goroutine operates on its own
// peer's state, so there is no cross-peer contention. Cycle-level counters
// become atomic; the summary log waits for every goroutine to finish.
//
// Mode selection (v1 vs v2) is derived at the start of each per-peer step
// from the per-cycle AnnounceTarget.Capabilities (captured by peersFn at
// cycle start under the peer-state lock that produced Address and Identity).
// AnnouncePeerState.capabilities is the persistent mirror used by the
// classification, but it is reconciled to the same target caps at the very
// start of each per-peer goroutine via UpdateCapabilities. Without that
// reconciliation the persistent snapshot can drift away from the chosen
// session: peersFn dedupes overlapping sessions for one identity by map
// iteration order, so successive cycles can pick different sessions; a
// partial close (a routing-capable session that previously refreshed the
// snapshot disappears while an older routing-capable session remains) leaves
// the persistent caps describing a session that is no longer the target.
// Reconciling at cycle start makes classifyDeltaMode read two agreeing
// snapshots in production. The divergence path remains as a defensive net:
// if a future caller introduces a second writer to AnnouncePeerState.capabilities
// or routes around the cycle-time sync, MarkInvalid + legacy delta on the
// current cycle is the safe fallback. Forced-full and connect-time paths
// are unaffected — they always use SendAnnounceRoutes regardless of
// capability (see sendFullAnnounce / sendLegacyFull / docs/routing.md
// "First-sync wire-frame invariant").
func (a *AnnounceLoop) announceToAllPeers(ctx context.Context) {
	cycleID := a.cycleCounter.Add(1)

	// Refresh TTL of own-origin direct routes unconditionally.
	a.table.RefreshDirectPeers()

	peers := a.peersFn()
	if len(peers) == 0 {
		return
	}

	// Periodic eviction of stale disconnected peer state.
	a.stateRegistry.EvictStale()

	var (
		totalRaw               atomic.Int32
		totalAggregated        atomic.Int32
		totalDelta             atomic.Int32
		skippedNoop            atomic.Int32
		forcedFull             atomic.Int32
		coalescedTrigger       atomic.Int32
		deltaV2                atomic.Int32
		deltaV1                atomic.Int32
		modeDivergence         atomic.Int32
		v2DowngradedNoBaseline atomic.Int32
	)

	now := a.stateRegistry.clock()
	forcedFullSyncInterval := time.Duration(ForcedFullSyncMultiplier) * a.interval

	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, peer := range peers {
		go func(peer AnnounceTarget) {
			defer wg.Done()
			defer crashlog.DeferRecover()

			// Early-abort when the cycle context is cancelled — avoids
			// blocking a per-peer goroutine for up to syncFlushTimeout
			// when shutdown is already in progress.
			if ctx.Err() != nil {
				return
			}

			peerState := a.stateRegistry.GetOrCreate(peer.Identity)

			// Reconcile persistent capability snapshot with the per-cycle
			// AnnounceTarget caps. peersFn (in production: node.routingCapablePeers)
			// dedupes overlapping sessions for one identity by map iteration
			// order, so two cycles can pick different sessions when several
			// routing-capable sessions for the same peer have different
			// negotiated caps. A partial close (the last cap-refreshing
			// session disappears while an older routing-capable session
			// remains) can also leave AnnouncePeerState.capabilities
			// describing a session that is no longer the chosen target.
			// Syncing here makes the persistent snapshot a derived view of
			// the same session this cycle picked: classifyDeltaMode below
			// reads two agreeing capability lists and the divergence path
			// stays purely defensive. Without this sync, classifyDeltaMode
			// would keep firing divergence; MarkInvalid only flips
			// needsFullResync and never refreshes caps, so the peer would
			// be stuck in legacy/forced-full forever.
			a.stateRegistry.UpdateCapabilities(peer.Identity, peer.Capabilities)

			// Build peer-specific raw entries from table.
			rawRoutes := a.table.AnnounceTo(peer.Identity)
			totalRaw.Add(int32(len(rawRoutes)))

			// Build canonical aggregated snapshot.
			snapshot := BuildAnnounceSnapshot(rawRoutes)
			totalAggregated.Add(int32(len(snapshot.Entries)))

			// Read peer state atomically to decide send mode.
			view := peerState.View()
			needsFull := view.NeedsFullResync || view.LastSentSnapshot == nil

			// Check if periodic forced full sync is due.
			if !needsFull && !view.LastSuccessfulFullSyncAt.IsZero() {
				if now.Sub(view.LastSuccessfulFullSyncAt) > forcedFullSyncInterval {
					needsFull = true
				}
			}

			if needsFull {
				// Rate limit forced full sync attempts — but only when the
				// peer already has a baseline. A peer that never received
				// any data (LastSentSnapshot==nil, e.g. after a failed
				// first attempt) must retry without delay.
				if view.LastSentSnapshot != nil &&
					!view.LastFullSyncAttemptAt.IsZero() &&
					now.Sub(view.LastFullSyncAttemptAt) < MinForcedFullSyncInterval {
					// Too soon — skip this cycle for this peer.
					coalescedTrigger.Add(1)
					return
				}

				a.sendFullAnnounce(ctx, cycleID, peer, peerState, snapshot, now)
				forcedFull.Add(1)
				return
			}

			// Delta path.
			delta := ComputeDelta(view.LastSentSnapshot, snapshot)
			totalDelta.Add(int32(len(delta)))

			if len(delta) == 0 {
				// No changes — suppress send.
				skippedNoop.Add(1)
				return
			}

			// Mode selection. Both sources of the peer's capability set must
			// agree on CapMeshRoutingV2 before the cycle may pick the v2 wire
			// path. Disagreement is treated as "state changed underneath us":
			// MarkInvalid forces the next cycle to take the forced-full path,
			// and this cycle falls back to legacy delta so the peer still
			// learns the change under whichever wire format it actually
			// understands. CapMeshRoutingV2 is opt-in on top of v1; a peer
			// that advertises v2 without v1 is treated as v1-only because the
			// first-sync invariant (legacy announce_routes) is gated on v1.
			mode := classifyDeltaMode(view.CapabilitiesSnapshot, peer.Capabilities)
			// Wire-baseline gate: v2 routes_update may be sent only after a
			// legacy announce_routes frame has actually gone out to this peer
			// in the current session. The empty-snapshot short-circuit in
			// connect-time and forced-full paths records a successful baseline
			// locally without emitting any wire frame, so HasSentWireBaseline
			// stays false and the peer's v2 receive gate would drop the first
			// routes_update and emit request_resync. Force the legacy delta
			// path here so the very first observable wire frame is the
			// baseline, exactly as required by the symmetric receive-side
			// invariant. After the legacy send succeeds, MarkWireBaselineSent
			// in sendIncrementalAnnounce flips the flag and subsequent cycles
			// take the v2 path normally.
			if mode == deltaModeV2 && !view.HasSentWireBaseline {
				v2DowngradedNoBaseline.Add(1)
				log.Debug().
					Uint64("announce_cycle_id", cycleID).
					Str("peer_identity", string(peer.Identity)).
					Str("peer_address", string(peer.Address)).
					Msg("announce_v2_downgraded_no_wire_baseline")
				mode = deltaModeV1
			}
			switch mode {
			case deltaModeV2:
				deltaV2.Add(1)
				a.sendIncrementalAnnounceV2(ctx, cycleID, peer, peerState, snapshot, delta, now)
			case deltaModeV1:
				deltaV1.Add(1)
				a.sendIncrementalAnnounce(ctx, cycleID, peer, peerState, snapshot, delta, now)
			case deltaModeDivergence:
				modeDivergence.Add(1)
				a.stateRegistry.MarkInvalid(peer.Identity)
				log.Warn().
					Uint64("announce_cycle_id", cycleID).
					Str("peer_identity", string(peer.Identity)).
					Str("peer_address", string(peer.Address)).
					Msg("announce_mode_divergence_fallback_to_legacy_delta")
				a.sendIncrementalAnnounce(ctx, cycleID, peer, peerState, snapshot, delta, now)
			}
		}(peer)
	}
	wg.Wait()

	log.Debug().
		Uint64("announce_cycle_id", cycleID).
		Int("peers", len(peers)).
		Int("raw_routes_count", int(totalRaw.Load())).
		Int("aggregated_routes_count", int(totalAggregated.Load())).
		Int("delta_routes_count", int(totalDelta.Load())).
		Int("announce_skipped_noop", int(skippedNoop.Load())).
		Int("announce_forced_full", int(forcedFull.Load())).
		Int("announce_trigger_coalesced", int(coalescedTrigger.Load())).
		Int("announce_delta_v1", int(deltaV1.Load())).
		Int("announce_delta_v2", int(deltaV2.Load())).
		Int("announce_mode_divergence", int(modeDivergence.Load())).
		Int("announce_v2_downgraded_no_wire_baseline", int(v2DowngradedNoBaseline.Load())).
		Msg("announce_cycle_complete")
}

// deltaMode enumerates the wire-format outcomes of mode selection on a
// delta send. Kept internal to the routing package because the tri-state
// (v1, v2, divergence) is an implementation detail of announceToAllPeers.
type deltaMode int

const (
	// deltaModeV1 means legacy announce_routes is used — either because
	// the peer did not advertise CapMeshRoutingV2 or because v2 was not
	// advertised alongside v1 (see type doc on CapMeshRoutingV2).
	deltaModeV1 deltaMode = iota
	// deltaModeV2 means both sources agree on CapMeshRoutingV2 and the
	// cycle takes the v2 routes_update wire path.
	deltaModeV2
	// deltaModeDivergence means the two capability sources disagree. The
	// peer is marked NeedsFullResync and the cycle falls back to legacy
	// delta so the convergence signal still lands.
	deltaModeDivergence
)

// classifyDeltaMode derives the wire-format mode for a v1+ delta send.
// Both inputs must contain CapMeshRoutingV2 for v2 to be picked, and v2
// is meaningful only when CapMeshRoutingV1 is also advertised in that
// same input. In production both inputs are equal: the announce loop
// reconciles AnnouncePeerState.capabilities to the per-cycle
// AnnounceTarget.Capabilities via UpdateCapabilities at the start of
// each per-peer goroutine, so by the time this function runs both
// arguments come from the same chosen target. The two-input shape and
// the deltaModeDivergence outcome are kept as a defensive net for any
// future caller that bypasses the cycle-time sync — see the doc comment
// on AnnounceLoop.announceToAllPeers for the reconciliation contract.
func classifyDeltaMode(stateCaps []PeerCapability, targetCaps []PeerCapability) deltaMode {
	stateV2 := hasCapV2Both(stateCaps)
	targetV2 := hasCapV2Both(targetCaps)
	switch {
	case stateV2 && targetV2:
		return deltaModeV2
	case !stateV2 && !targetV2:
		return deltaModeV1
	default:
		return deltaModeDivergence
	}
}

// hasCapV2Both reports whether the capability slice contains both
// CapMeshRoutingV1 and CapMeshRoutingV2. v2 without v1 is treated as
// v1-only because the first-sync invariant (legacy announce_routes)
// is gated on v1.
func hasCapV2Both(caps []PeerCapability) bool {
	var v1, v2 bool
	for _, c := range caps {
		switch c {
		case domain.CapMeshRoutingV1:
			v1 = true
		case domain.CapMeshRoutingV2:
			v2 = true
		}
	}
	return v1 && v2
}

// sendFullAnnounce sends a complete announce snapshot to the peer and
// updates cache state on success via thread-safe Record* methods.
//
// Forced-full / connect-time / periodic-forced-full MUST use the legacy
// announce_routes wire frame. See the "First-sync wire-frame invariant"
// section in docs/routing.md for the normative contract: the first
// full-sync after session establishment — and every later forced full
// resync — is always legacy regardless of peer capabilities, because the
// peer has no baseline to diff a v2 routes_update against. Wire-send goes
// through sendLegacyFull so the single helper is the only in-package call
// site that invokes PeerSender.SendAnnounceRoutes for full sync; do NOT
// add a branch on peer.Capabilities here that picks SendRoutesUpdate.
func (a *AnnounceLoop) sendFullAnnounce(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	snapshot *AnnounceSnapshot,
	now time.Time,
) {
	state.RecordFullSyncAttempt(now)

	// Empty snapshot after split horizon / empty table: record a successful
	// full-sync baseline without sending a wire frame. The peer learns
	// nothing new, and the cache is primed so subsequent cycles use delta.
	// This short-circuit predates the first-sync invariant and is orthogonal
	// to the "First-sync wire-frame invariant" section in docs/routing.md:
	// empty-baseline intentionally emits neither legacy nor v2 wire frame.
	if len(snapshot.Entries) == 0 {
		state.RecordFullSyncSuccess(snapshot, now)
		return
	}

	if !a.sendLegacyFull(ctx, peer, snapshot) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("routes", len(snapshot.Entries)).
			Msg("announce_full_send_failed")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	// A non-empty legacy announce_routes frame went out — the peer has now
	// observed a wire baseline in this session, so the v2 receive gate is
	// open. Subsequent deltas may pick the v2 wire frame when caps agree.
	state.MarkWireBaselineSent()
	state.RecordFullSyncSuccess(snapshot, now)
}

// sendLegacyFull sends a full snapshot via the legacy announce_routes wire
// frame. This is the only helper allowed for forced-full / connect-time /
// periodic-forced-full paths inside the routing package; see the
// "First-sync wire-frame invariant" section in docs/routing.md for the
// full rationale.
//
// Naming is deliberate: any future change that wants a v2 routes_update
// branch on full sync would have to either rename or bypass this helper,
// and both options are a loud review tripwire. The helper does not inspect
// peer.Capabilities — full sync is always legacy, by definition. The v2
// delta path uses PeerSender.SendRoutesUpdate directly from
// sendIncrementalAnnounce (once the v2 frame lands), never via this helper.
//
// ctx propagates the caller's cancellation/deadline down to the transport;
// see PeerSender.SendAnnounceRoutes doc for the ctx contract. Returns the
// same bool the sender returned — collapsing every transport-drop class to
// a single negative outcome so the caller can keep the success/failure
// path uniform with the delta-send helper.
func (a *AnnounceLoop) sendLegacyFull(
	ctx context.Context,
	peer AnnounceTarget,
	snapshot *AnnounceSnapshot,
) bool {
	return a.sender.SendAnnounceRoutes(ctx, peer.Address, snapshot.Entries)
}

// sendIncrementalAnnounce sends only changed entries to the peer via the
// legacy announce_routes wire frame and updates cache to the full new
// snapshot on success via thread-safe Record* methods. Used when the
// peer has not negotiated CapMeshRoutingV2 (v1-only delta path) and as
// the mode-divergence fallback — see announceToAllPeers for the
// classification rules.
func (a *AnnounceLoop) sendIncrementalAnnounce(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	fullSnapshot *AnnounceSnapshot,
	delta []AnnounceEntry,
	now time.Time,
) {
	if !a.sender.SendAnnounceRoutes(ctx, peer.Address, delta) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("delta_routes", len(delta)).
			Msg("announce_delta_send_failed_v1")
		// Cache remains in previous state; next cycle will retry.
		return
	}
	// A legacy announce_routes frame went out — even on the v1 delta path,
	// the peer's v2 receive gate (if any) treats this arrival as the wire
	// baseline. Flip the flag so future cycles may pick the v2 frame when
	// both capability sources agree.
	state.MarkWireBaselineSent()
	// Cache stores the full snapshot, not just the delta payload.
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}

// sendIncrementalAnnounceV2 sends the delta via the v2 routes_update wire
// frame. Only picked by announceToAllPeers when both capability sources
// agree on CapMeshRoutingV2 and a baseline exists on the peer side — the
// first-sync invariant guarantees the baseline is always delivered via
// the legacy announce_routes frame first (see sendFullAnnounce /
// sendLegacyFull).
//
// On success the cache is updated exactly like the v1 delta path: the
// full new snapshot is stored so the next cycle can compute a fresh
// diff. A send failure leaves the cache untouched; the next cycle
// retries with a fresh delta recomputation.
func (a *AnnounceLoop) sendIncrementalAnnounceV2(
	ctx context.Context,
	cycleID uint64,
	peer AnnounceTarget,
	state *AnnouncePeerState,
	fullSnapshot *AnnounceSnapshot,
	delta []AnnounceEntry,
	now time.Time,
) {
	if !a.sender.SendRoutesUpdate(ctx, peer.Address, delta) {
		log.Debug().
			Uint64("announce_cycle_id", cycleID).
			Str("peer_identity", string(peer.Identity)).
			Str("peer_address", string(peer.Address)).
			Int("delta_routes", len(delta)).
			Msg("announce_delta_send_failed_v2")
		return
	}
	state.RecordDeltaSendSuccess(fullSnapshot, now)
}
