package routing

import "time"

// table_health.go hosts the Table-level writer methods that mutate
// the per-(Identity, Uplink) RouteHealthState introduced in Phase 2
// (docs/protocol/route_health.md). These methods are the
// only sanctioned writers into healthStore — direct healthStore
// access from outside the routing package is not exposed; callers
// (relay hop_ack receive path in PR 11.2, probe sender / handler in
// PR 11.3) go through Table.MarkHopAck etc. so a single writer
// invariant per state holds: Table's t.mu.Lock() serialises every
// mutation.
//
// Eviction of stale health entries is driven by storage-side
// cleanup, not by WithdrawRoute / RemoveDirectPeer directly.
// reconcileHealthLocked (in table_mutation.go) walks the
// health store after TickTTL / CompactExpired and drops any
// entry whose backing (Identity, Uplink) claim is gone from
// storage. Until that reconcile fires, health entries for
// withdrawn routes live as harmless orphans in the map: Lookup
// filters withdrawn claims before health is consulted, so an
// orphan entry never influences ranking. See docs/locking.md
// "Cluster-mesh Phase 2" for the full eviction contract.

// MarkHopAck records a relay_hop_ack confirmation for the
// (identity, uplink) pair. It transitions the corresponding
// RouteHealthState into HealthGood and folds a non-zero rtt sample
// into the EWMA estimate via UpdateRTT.
//
// PR 11.2 contract: rtt is the round-trip latency measured by the
// caller (relay forward send timestamp → hop_ack receive timestamp).
// When the hop_ack receive path does not have the original send
// timestamp threaded through, callers pass rtt=0 — UpdateRTT then
// preserves the prior EWMA value untouched (see score.go's cold-start
// guard). Probe-driven RTT samples in PR 11.3 are the primary source
// of fresh measurements; this entry point exists so that organic
// relay traffic does not let RouteHealthState drift to Questionable
// while the route is actually healthy.
//
// Lock contract: acquires t.mu in W mode. Callers must not hold any
// other domain mutex of node.Service (peerMu / deliveryMu / ...) at
// invocation time — see docs/locking.md ("Cluster-mesh Phase 2 —
// routing.Table.mu ownership of RouteHealthState"). The wire-send of
// the hop_ack is already complete by the time the receive path calls
// this, so there is no socket I/O inside the critical section.
func (t *Table) MarkHopAck(identity, uplink PeerIdentity, rtt time.Duration) {
	if identity.IsZero() || uplink.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	state := t.health.ensureLocked(identity, uplink, now)
	prevDead, prevCooled := healthProjectionSig(state)
	state.applyHopAck(now)
	if rtt > 0 {
		state.RTT = UpdateRTT(state.RTT, rtt)
	}
	// Phase 3 PR 12.2 — fold the reputation primitive into the same
	// critical section the Phase 2 health update runs in. A
	// concurrent MarkHopFailure could otherwise interleave between
	// applyHopAck and applyHopAckSuccess and bump ConsecutiveFailures
	// past the BlackHoleThreshold while the success that should
	// reset it sits midway through this method. See
	// docs/cluster-mesh/phase-3-multipath-reputation.md §2.5 for
	// the locking-ownership decision (reputation lives inside
	// RouteHealthState under the same t.mu) and §4.2 for the
	// wire-up contract.
	state.applyHopAckSuccess(now)
	// PR 11.28 P2#1: health is part of the published Snapshot
	// (Snapshot.Health, populated atomically by the publisher's
	// Table.SnapshotIncremental alongside Routes). Without marking dirty here the snapshot
	// publisher's ConsumeDirty fast-path skips the rebuild on
	// health-only mutations, leaving fetchRouteLookup/fetchRouteHealth
	// reading stale labels until the next structural route mutation.
	// The dirty flag is shared between the routes view and the
	// health view because both ride the same atomic snapshot
	// pointer in node.Service.
	t.dirty.Store(true)
	if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
		t.markRouteChangedLocked(identity)
	}
}

// ConfirmHopAck is the atomic counterpart of MarkHopAck used by
// the node-level confirmRouteViaHopAck path. It folds three
// previously separate steps into one t.mu.Lock critical section
// so a concurrent withdrawal/expiry/compaction cannot interleave
// and leave orphan health behind:
//
//  1. Inspect raw storage for a live (identity, uplink) claim
//     (unfiltered, sees withdrawn/expired entries explicitly).
//  2. If the claim is absent, withdrawn, or expired: return
//     applied=false and do NOT touch health.
//  3. If the claim is live: refresh health to Good (applyHopAck +
//     EWMA via UpdateRTT when rtt > 0), then promote the claim's
//     Source to RouteSourceHopAck if it was below that tier.
//
// PR 11.12 P2#1.2 (atomicity fix): the prior split design called
// Table.InspectTriple → Table.MarkHopAck → Table.UpdateRoute as
// three independent Lock acquisitions. Between the first and the
// second, another writer could withdraw / TickTTL-expire / cap-
// evict the storage claim, but MarkHopAck would still proceed and
// stamp Good health on a pair without a backing claim. Folding
// the three steps under one Lock makes the TOCTOU window
// structurally impossible.
//
// rtt is folded into the EWMA via UpdateRTT when > 0; rtt == 0
// preserves the prior EWMA value (callers without a sender-side
// timestamp use 0 — probe acks carry the RTT measurement
// separately).
//
// Returns:
//   - status: the routeStore.ApplyUpdate result when source
//     promotion ran (RouteAccepted on actual promotion,
//     RouteUnchanged when the claim was already at HopAck or
//     better, RouteRejected on internal-only refusal — see the
//     ApplyUpdate doc-comment). The caller usually only needs
//     `applied` for branching.
//   - applied: true if a live claim was found and health
//     refreshed (regardless of whether source promotion was
//     needed). False means the pair has no live claim and the
//     ack was a structural no-op.
//
// Lock contract: acquires t.mu in W mode. Callers must not hold
// any other domain mutex of node.Service at invocation time. The
// wire send of the hop_ack is already complete.
func (t *Table) ConfirmHopAck(identity, uplink PeerIdentity, rtt time.Duration) (RouteUpdateStatus, bool) {
	if identity.IsZero() || uplink.IsZero() {
		return RouteRejected, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()

	// Raw storage lookup — unfiltered, sees withdrawn/expired
	// entries explicitly. The store-level method is *Locked-like
	// (no internal mutex), safe to call inside t.mu.
	entry := t.store.InspectTriple(RouteTriple{Identity: identity, NextHop: uplink})
	if entry == nil {
		return RouteRejected, false
	}
	if entry.IsWithdrawn() || entry.IsExpired(now) {
		return RouteRejected, false
	}

	// Health refresh — same mutations MarkHopAck performs, but
	// without releasing t.mu first. ensureLocked never creates a
	// state for an empty (Identity, Uplink) pair (the empty-check
	// at the top of MarkHopAck guards that); we already validated
	// non-empty above.
	state := t.health.ensureLocked(identity, uplink, now)
	prevDead, prevCooled := healthProjectionSig(state)
	state.applyHopAck(now)
	if rtt > 0 {
		state.RTT = UpdateRTT(state.RTT, rtt)
	}
	// Phase 3 PR 12.2 — atomic reputation update, paired with the
	// applyHopAck above for the same TOCTOU reason MarkHopAck pairs
	// the two calls. ConfirmHopAck is the "with live-storage proof"
	// flavour of MarkHopAck, so a success here is doubly authentic
	// (storage check + hop-ack arrival) and the reputation bump
	// reflects that without any additional verification.
	state.applyHopAckSuccess(now)
	// PR 11.28 P2#1: health mutation always lands here (whether or
	// not the source promotion fires below), so mark dirty so the
	// snapshot publisher picks up the new Health label on the next
	// tick. The source-promotion path also re-marks via
	// store.ApplyUpdate's mutated flag, but doing it here keeps the
	// "health refresh implies dirty" invariant local to this
	// function regardless of which branch runs.
	t.dirty.Store(true)
	if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
		t.markRouteChangedLocked(identity)
	}

	// Source promotion: only needed when the live claim is below
	// HopAck (Announcement → HopAck). HopAck-or-better claims
	// (HopAck itself, Direct) need no further trust upgrade.
	if entry.Source >= RouteSourceHopAck {
		return RouteUnchanged, true
	}

	confirmed := *entry
	confirmed.Source = RouteSourceHopAck

	// Defensive validate — the storage entry passed validation
	// when admitted, but the synthesised promotion variant could
	// in principle fail invariants tied to Source (e.g. Direct
	// requires NextHop==Identity; we're synthesising HopAck which
	// has no such tie). Treat a malformed synthesis as "health
	// refreshed, promotion skipped" rather than a hard failure
	// because the health side effect already landed.
	if err := confirmed.Validate(); err != nil {
		return RouteRejected, true
	}

	status, mutated, wireChanged := t.store.ApplyUpdate(confirmed, now)
	if mutated {
		t.dirty.Store(true)
		// Snapshot always, but journal only when the wire projection changed —
		// same rationale as UpdateRoute. A HopAck source promotion that only
		// refreshes ExpiresAt is wire-identical (wireChanged=false) and must not
		// drive a cursor-mode re-emit; a sig/Extra upgrade on the same
		// reconfirmation journals normally.
		t.markSnapDirtyNoJournalLocked(identity)
		if wireChanged {
			t.markRouteChangedLocked(identity)
		}
	}
	return status, true
}

// MarkProbeAck records a route_probe_ack_v1 reception for the
// (identity, uplink) pair. The reachable flag mirrors the responder's
// answer; rtt is the sender-measured round-trip latency from probe
// send timestamp to ack receive timestamp (NOT the ack payload's
// rtt_ms field — that is informational only per the zero-trust
// budget, see docs/protocol/route_health.md).
//
// State machine semantics (delegated to RouteHealthState.applyProbeAck):
//
//   - reachable=true → HealthGood, ProbeFailures reset to zero,
//     LastHopAck refreshed (the ack itself is proof of reachability).
//     Atomic storage-claim check (PR 11.24 P2#2): the pair MUST
//     have a live, non-withdrawn, non-expired claim in storage,
//     otherwise the ack is dropped without creating a health
//     entry. A late ack arriving after TickTTL or cap-eviction
//     dropped the underlying claim would otherwise leave an
//     orphan HealthGood entry; a subsequent fresh UpdateRoute
//     for the same pair would survive its eviction-on-accept
//     branch (which only clears Bad/Dead, see
//     table_mutation.go), letting the new still-unverified claim
//     inherit Good/RTT from the stale ack — a verify-before-trust
//     bypass.
//   - reachable=false → ProbeFailures incremented; pair currently in
//     Bad is first pulled back to Questionable (the responder
//     answered, so the uplink itself is alive), then re-evaluated
//     against HealthProbeFailureThreshold. Dead pairs stay Dead —
//     see the applyProbeAck doc-comment for the Dead-sticky
//     rationale. A reachable=false ack for an absent pair is
//     dropped — negative evidence does not create state (review
//     concern P2#2): without this guard a late ack arriving after
//     TickTTL evicted the underlying storage claim would resurrect
//     the pair as Good with fresh LastHopAck.
//
// rtt is folded into the EWMA estimate via UpdateRTT when > 0; rtt=0
// preserves the prior RTT value untouched.
//
// Lock contract: acquires t.mu in W mode. Callers must not hold any
// other domain mutex of node.Service at invocation time — see
// docs/locking.md ("Cluster-mesh Phase 2"). The wire send of the ack
// is already complete by the time this is called.
func (t *Table) MarkProbeAck(identity, uplink PeerIdentity, reachable bool, rtt time.Duration) {
	if identity.IsZero() || uplink.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	var state *RouteHealthState
	if reachable {
		// Positive evidence: atomically verify a live storage
		// claim exists for the pair before touching health. This
		// is the same anti-orphan pattern Table.ConfirmHopAck uses
		// for hop_ack ingest; see the PR 11.24 P2#2 note on the
		// reachable=true bullet above for the verify-before-trust
		// bypass that would otherwise occur on a late ack after
		// TickTTL / cap-eviction.
		entry := t.store.InspectTriple(RouteTriple{Identity: identity, NextHop: uplink})
		if entry == nil || entry.IsWithdrawn() || entry.IsExpired(now) {
			return
		}
		state = t.health.ensureLocked(identity, uplink, now)
	} else {
		// Negative evidence: only update an existing entry. Do
		// NOT resurrect an evicted pair as Good.
		state = t.health.getLocked(identity, uplink)
		if state == nil {
			return
		}
	}
	prevDead, prevCooled := healthProjectionSig(state)
	state.applyProbeAck(reachable, now)
	if rtt > 0 {
		state.RTT = UpdateRTT(state.RTT, rtt)
	}
	// PR 11.28 P2#1: Snapshot.Health needs republish on every
	// health mutation; see MarkHopAck doc-comment.
	t.dirty.Store(true)
	if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
		t.markRouteChangedLocked(identity)
	}
}

// MarkProbeFailure records a probe timeout — no route_probe_ack_v1
// arrived within HealthProbeTimeout. Increments ProbeFailures; if it
// crosses HealthProbeFailureThreshold the pair transitions to Bad
// (independently of the passive 122s hop_ack timeline). A pair
// already in Dead stays Dead — only a real confirmation can resurrect
// it.
//
// Drops silently when no health entry exists for the pair: a probe
// timeout against an absent pair is negative evidence and must not
// resurrect the entry as Good with a fresh timestamp (review concern
// P2#2). The most common cause is a TickTTL eviction that ran
// between probe Register and the timeout fire, leaving the registry
// entry as the sole reference to a pair the storage has already
// dropped.
//
// Lock contract: same as MarkProbeAck. Caller is the probe sender's
// timeout watcher; the probe has already missed its deadline, so
// there is no concurrent wire I/O for this (identity, uplink) pair
// to coordinate with.
func (t *Table) MarkProbeFailure(identity, uplink PeerIdentity) {
	if identity.IsZero() || uplink.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.health.getLocked(identity, uplink)
	if state == nil {
		return
	}
	prevDead, prevCooled := healthProjectionSig(state)
	state.applyProbeFailure(t.clock())
	// PR 11.28 P2#1: Snapshot.Health republish trigger; see
	// MarkHopAck doc-comment.
	t.dirty.Store(true)
	if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
		t.markRouteChangedLocked(identity)
	}
}

// MarkHopFailure records a relay hop-ack timeout for the
// (identity, uplink) pair on the Phase 3 reputation primitives. The
// production caller is node.Service.onRelayHopAckTimeout (wired up in
// internal/core/node/routing_relay.go, fired by the relay state
// store's TTL ticker when a forwarded message's HopAckRemainingTicks
// elapses with no matching relay_hop_ack arrival).
//
// MarkHopFailure mutates ONLY the reputation surface
// (HopAckAttempts / ReliabilityScore / ConsecutiveFailures /
// CooldownUntil). The Phase 2 health state machine
// (Health / LastHopAck / ProbeFailures / TransitionAt) is owned by
// MarkHopAck / MarkProbeAck / MarkProbeFailure / TickHealth and stays
// untouched here. The split is deliberate
// (docs/cluster-mesh/phase-3-multipath-reputation.md §4.1): a single
// relay hop-ack timeout is weak evidence about the route's overall
// liveness — the passive 122-second hop_ack timeline and the active
// probe path own the Health label, while reputation aggregates
// success/failure ratios for ranking and black-hole detection.
//
// Anti-orphan guards (matching MarkProbeAck(reachable=false) and
// ConfirmHopAck's storage check):
//
//   - Absent (identity, uplink) pair → no-op. A timeout fired against
//     a pair the storage has already dropped (TickTTL eviction, cap
//     eviction, WithdrawRoute tombstone) must not resurrect a phantom
//     reputation entry that the next legitimate UpdateRoute admission
//     would carry forward as "this uplink already has consecutive
//     failures".
//   - Withdrawn or expired storage claim → no-op. Reputation should
//     not paint a route red that is already gone for unrelated
//     reasons.
//   - Empty identity or uplink → no-op (defensive guard).
//
// Lock contract: acquires t.mu in W mode. Callers must not hold any
// other domain mutex of node.Service at invocation time
// (relayStateStore.mu is acceptable because it is disjoint from
// routing.Table.mu and the production caller releases it before
// invoking this method — see node.Service.onRelayHopAckTimeout). The
// hop-ack budget has already elapsed by the time this is called, so
// there is no concurrent wire I/O for this (identity, uplink) pair to
// coordinate with.
func (t *Table) MarkHopFailure(identity, uplink PeerIdentity) {
	if identity.IsZero() || uplink.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	// Storage check: absent / withdrawn / expired claim → no-op.
	// The raw InspectTriple call mirrors ConfirmHopAck's anti-
	// orphan pattern (PR 11.12 P2#1.2): we inspect storage
	// explicitly so a timeout fired against a route that has
	// already been withdrawn or aged out does not register a
	// failure against a peer who is no longer carrying the route
	// for us.
	entry := t.store.InspectTriple(RouteTriple{Identity: identity, NextHop: uplink})
	if entry == nil {
		return
	}
	now := t.clock()
	if entry.IsWithdrawn() || entry.IsExpired(now) {
		return
	}

	// Anti-orphan guard symmetric to Phase 2 MarkProbeAck
	// (reachable=false) and MarkProbeFailure: getLocked returns
	// nil for pairs whose health entry was evicted (TickTTL,
	// reconcileHealthLocked after cap pressure), and a single
	// late hop-ack timeout must NOT resurrect such a pair as a
	// fresh Good-labelled entry with ConsecutiveFailures=1. The
	// Phase 3 plan §4.2 spells this out as "если pair не tracked —
	// no-op (нет previous evidence — не создаём synthetic Bad-
	// сигнал)". The pair gets re-seeded only when positive
	// evidence (hop_ack, probe_ack reachable=true) or fresh wire-
	// frame admission (UpdateRoute) creates a Questionable
	// placeholder; subsequent failures then accumulate normally.
	//
	// In production Phase 2's UpdateRoute admission always seeds
	// a Questionable health entry for every newly-learned
	// (Identity, Uplink) pair, so the "live claim, no health"
	// case is the eviction-then-late-timeout race — exactly the
	// case the guard exists for.
	state := t.health.getLocked(identity, uplink)
	if state == nil {
		return
	}
	prevDead, prevCooled := healthProjectionSig(state)
	state.applyHopAckFailure(now)
	// PR 11.28 P2#1: Snapshot.Health (and the Phase 3 reputation
	// fields it includes) needs republish on every mutation; see
	// MarkHopAck doc-comment.
	t.dirty.Store(true)
	if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
		t.markRouteChangedLocked(identity)
	}
}

// ClearDirectPairCooldown lifts an armed black-hole cooldown (and the
// ConsecutiveFailures streak feeding it) for the (peer, peer) direct
// pair after a session to the peer has been (re-)established and its
// identity confirmed by the handshake.
//
// Production caller: node.Service.onPeerSessionEstablished on the
// withdrawal-grace reconnect branch (tryCancelPendingWithdrawal ==
// true), where AddDirectPeer is intentionally NOT called because the
// direct route never left the table. That is exactly the scenario in
// which the cooldown gets armed in the first place: during the grace
// window the route stays selectable, relay attempts toward the dead
// peer keep timing out, and the 5th consecutive hop-ack failure arms
// CooldownUntil. Reconnecting inside the window cancels the deferred
// withdrawal but — without this method — would leave the cooldown
// hiding the still-present direct route from Lookup /
// fetchRouteLookup for up to BlackHoleCooldown ("peer online, route
// count=0"). The AddDirectPeer path performs the equivalent clear
// inline; see the matching block in table_mutation.go.
//
// Semantics mirror the organic-recovery clear in applyHopAckSuccess:
// reliability EMA and attempt counters are untouched,
// LastCooldownClearedAt is stamped only when an armed cooldown was
// actually lifted (keeping the PR 12.6 partial-traffic shaping grace
// engaged). No-op when the pair is untracked or has nothing to clear.
//
// Returns true when state changed (snapshot republish was scheduled).
// Safe to call from any goroutine; acquires t.mu in W mode.
func (t *Table) ClearDirectPairCooldown(peerIdentity PeerIdentity) bool {
	if peerIdentity.IsZero() {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.health.getLocked(peerIdentity, peerIdentity)
	if state == nil {
		return false
	}
	if !state.applySessionEstablishedLocked(t.clock()) {
		return false
	}
	t.dirty.Store(true)
	// applySessionEstablishedLocked lifting an armed cooldown un-filters the
	// direct pair from the projection, so journal the change. (Direct pair:
	// identity == uplink == peerIdentity.)
	t.markRouteChangedLocked(peerIdentity)
	return true
}

// HealthSnapshot returns a deep copy of every tracked
// RouteHealthState. Backs the fetchRouteHealth RPC handler in
// internal/core/rpc/routing_commands.go (PR 11.5) AND the
// fetchRouteLookup handler (which needs full per-pair tiers for
// CompositeScore once Snapshot.Health was narrowed to Dead∪cooled;
// fetchRouteLookup gates the call on the target having a live route,
// so a lookup miss never reaches here) — the handlers
// call this synchronously per request, no intermediate
// atomic.Pointer cache. Each call takes t.mu in R mode, walks
// the ENTIRE healthStore — O(total tracked (Identity, Uplink)
// pairs), which scales with the whole routing table, NOT with the
// direct-peer count: on a dense node this is hundreds of thousands
// of pairs (exactly why the periodic Snapshot.Health copy was
// narrowed to the Dead∪cooled subset). It deep-copies each state
// into the returned slice and releases the lock. Returns nil when
// the store is empty so callers can compare cheaply.
//
// Lock window. Reading under R mode means concurrent
// HealthSnapshot calls do not block each other, but they do
// compete with writer methods (MarkHopAck, MarkProbeAck,
// TickHealth, UpdateRoute's health bookkeeping) on the same
// t.mu. Go's sync.RWMutex prioritises writers, so a queued
// writer blocks subsequent RPC reads until it completes. The
// map walk and deep copy are O(total tracked health pairs),
// which on a dense mesh is large — so although fetchRouteHealth
// and fetchRouteLookup are on-demand diagnostics (and
// fetchRouteLookup additionally gates the call on a live route to
// skip misses), a high rate of these RPCs reintroduces real
// copy/lock pressure. If either ever turns into a hot path that
// conflicts with the writer workload, migrate to the
// atomic.Pointer snapshot pattern used by peerHealthSnap in
// internal/core/node/peer_health_snapshot.go.
//
// Safe to call from any goroutine.
func (t *Table) HealthSnapshot() []RouteHealthState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.health.snapshotLocked()
}

// QuestionableHealthTargets returns the (Identity, Uplink) pairs whose
// health is currently HealthQuestionable — the probe scheduler's working
// set. Unlike HealthSnapshot it does not deep-copy every tracked state,
// only the Questionable subset, so the periodic probeTick no longer
// allocates a full-set copy each pass (see
// healthStore.questionableTargetsLocked). Safe to call from any goroutine.
func (t *Table) QuestionableHealthTargets() []HealthProbeTarget {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.health.questionableTargetsLocked()
}

// TickHealth walks every tracked RouteHealthState and applies the
// passive-timeline transition (Good→Questionable→Bad→Dead) based
// on hop-ack idle. Without this tick the state machine cannot
// advance from Good for a pair that has not received any hop-ack
// or probe traffic — Lookup would keep returning a stale-but-
// "Good" pair and the probe sender would never see any
// Questionable pair to probe.
//
// Designed to be called from the probe-sender goroutine at
// HealthProbeInterval cadence (30 s in production): every tick
// the sender first ages every pair through the passive timeline,
// then iterates the Questionable subset to schedule probes (bounded
// per tick by maxProbesPerTick). This keeps the active probe path
// eventually-firing rather than inert.
//
// Active-path transitions (Bad on ProbeFailures threshold, Good
// on ack receipt) are driven by MarkProbeAck / MarkProbeFailure
// / MarkHopAck and run independently of this tick.
//
// PR 11.27 P2#2 — direct pairs are skipped. Direct (Identity ==
// Uplink) routes have their lifecycle bound to the session
// (AddDirectPeer / RemoveDirectPeer), not to hop_ack idle: a
// "quiet" direct peer (live session, no organic relay traffic
// generating hop_ack on this pair) would otherwise age through
// Good → Questionable → Bad → Dead on the 60/122/182s timeline
// even though the session is healthy. Relay-only direct peers
// (no mesh_route_probe_v1 capability — see routing/session)
// cannot be revived by active probing, so the passive timeline
// would silently deprioritise them. Skipping direct pairs here
// keeps their health bound to session liveness: the entry
// stays at whatever AddDirectPeer seeded (Good) until
// RemoveDirectPeer / reconnect rewrites it, or until external
// confirmation evidence (hop_ack on the rare relay-through-direct
// case) refreshes it.
//
// Lock contract: acquires t.mu in W mode. Callers must not hold
// any other domain mutex of node.Service at invocation time.
// Safe to call from any goroutine.
func (t *Table) TickHealth() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	mutated := false
	for key, state := range t.health.states {
		// Phase 3 PR 12.4 — release expired black-hole cooldowns
		// on every pair (Direct included). The PR 11.27 P2#2
		// skip below applies only to the Good→Questionable→Bad→
		// Dead passive idle promotion, NOT to the reputation
		// primitive's cooldown lifecycle: a Direct peer that
		// reliably missed 5 hop_acks earns the same 2-minute
		// cooldown as a transit pair, and the auto-clear must
		// fire on the same cadence regardless of source. The
		// no-op-on-zero / future-deadline branches inside
		// applyCooldownExpiryLocked keep this cheap for the
		// common case where no pair is cooled down.
		prevDead, prevCooled := healthProjectionSig(state)
		before := state.CooldownUntil
		state.applyCooldownExpiryLocked(now)
		if !before.Equal(state.CooldownUntil) {
			mutated = true
		}

		if key.Identity != key.Uplink {
			// Non-direct pairs age on the passive timeline. Direct
			// pairs (Identity == Uplink) are session-driven and skip
			// aging — see the doc-comment above — but still ran the
			// cooldown-expiry release above and reach the journal check.
			prev := state.Health
			state.applyIdleTick(now, t.probeBackoff)
			if state.Health != prev {
				mutated = true
			}
		}

		// Phase 3 change journal: record the identity only when the
		// projection-relevant signal (Dead / cooled) flipped on this tick,
		// not on every Good↔Questionable↔Bad transition.
		if d, c := healthProjectionSig(state); d != prevDead || c != prevCooled {
			t.markRouteChangedLocked(key.Identity)
		}
	}
	// PR 11.28 P2#1: passive aging mutates the published
	// Snapshot.Health only when at least one pair actually
	// transitioned. We refrain from marking dirty on every tick to
	// avoid waking the snapshot publisher on no-op tick passes —
	// the routes view rarely changes per tick, and a clean tick
	// here should not force a full rebuild.
	if mutated {
		t.dirty.Store(true)
	}
}

// HealthFor returns the current RouteHealth label for the
// (identity, uplink) pair plus a "tracked" flag. Used by the
// relay-path Phase 2 fast-recovery trigger (TableRouter.Route /
// tryForwardViaRoutingTable in internal/core/node/) to detect
// when the chosen route is HealthBad and fire route_query in
// the background while still forwarding via the Bad route as
// best-effort — without this signal the relay path waits for a
// Dead transition or an actual send failure before triggering
// recovery, which can stretch the unhealthy window by minutes
// (passive timeline = 122 s for Questionable→Bad, +60 s for
// Bad→Dead, plus the rate-limited query handshake afterwards).
//
// Returns (HealthGood, false) for pairs with no recorded
// health state — callers should treat the not-tracked case as
// "no Phase 2 signal available, defer to traditional behaviour"
// rather than as "healthy".
//
// Lock contract: acquires t.mu in R mode. Cheap (single map
// lookup); safe for the hot relay path.
func (t *Table) HealthFor(identity, uplink PeerIdentity) (RouteHealth, bool) {
	if identity.IsZero() || uplink.IsZero() {
		return HealthGood, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	state := t.health.getLocked(identity, uplink)
	if state == nil {
		return HealthGood, false
	}
	return state.Health, true
}

// RouteReputationState is the public projection of the Phase 3
// PR 12.1 reputation primitives carried inside RouteHealthState.
// Kept as a separate type so the RPC layer (Phase 3 PR 12.7
// fetchRouteReputation in internal/core/rpc/routing_commands.go)
// can publish a stable shape without depending on the internal
// RouteHealthState struct — RouteHealthState mixes Phase 2
// health-machine fields and the Phase 3 reputation fields, and
// fetchRouteReputation should not surface the health-machine
// fields a second time (fetchRouteHealth already does).
type RouteReputationState struct {
	Identity            PeerIdentity
	Uplink              PeerIdentity
	HopAckAttempts      uint64
	HopAckSuccesses     uint64
	ReliabilityScore    float64
	ConsecutiveFailures int
	CooldownUntil       time.Time
}

// ReputationSnapshot returns a deep copy of every tracked
// (Identity, Uplink) reputation state as a RouteReputationState
// slice. Backs the Phase 3 PR 12.7 fetchRouteReputation RPC
// handler — the handler calls this synchronously per request,
// no intermediate atomic.Pointer cache. Each call takes t.mu in
// R mode, walks the healthStore (≤ MaxOutgoingPeers +
// MaxIncomingPeers entries in production), projects each
// reputation field set into the returned slice, and releases
// the lock. Returns nil when the store is empty so callers can
// compare cheaply.
//
// Lock window. Same trade-off as HealthSnapshot: reading under
// R mode lets concurrent ReputationSnapshot calls share the
// lock, but they still compete with writer methods
// (MarkHopAck, MarkHopFailure, MarkProbeAck, etc.) on the same
// t.mu. Map walk + struct projection is O(N) where N is bounded
// by the per-identity uplink cap and the active identity
// count — a non-blocking workload in practice. Promotion to
// the atomic.Pointer snapshot pattern is reserved for future
// telemetry-driven tuning if the read path ever becomes hot
// enough to conflict with the writer workload.
//
// ReputationSnapshot is a pure read — it MUST NOT trigger any
// side effect (no probe send, no digest emit, no reputation
// mutation). The trust-budget invariant from Phase 3 §2.3
// extends to the observability surface: reading reputation is
// strictly an internal observation, never gossiped or
// transferred between nodes.
//
// Safe to call from any goroutine.
func (t *Table) ReputationSnapshot() []RouteReputationState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.health == nil || len(t.health.states) == 0 {
		return nil
	}
	out := make([]RouteReputationState, 0, len(t.health.states))
	for _, state := range t.health.states {
		out = append(out, RouteReputationState{
			Identity:            state.Identity,
			Uplink:              state.Uplink,
			HopAckAttempts:      state.HopAckAttempts,
			HopAckSuccesses:     state.HopAckSuccesses,
			ReliabilityScore:    state.ReliabilityScore,
			ConsecutiveFailures: state.ConsecutiveFailures,
			CooldownUntil:       state.CooldownUntil,
		})
	}
	return out
}

// ForceHealthForTest is a public test-only seam that lets test code
// in other packages drive RouteHealthState into a specific health
// without replaying the full state-machine timeline through
// applyHopAck / applyIdleTick. Phase 2 health transitions go
// through periodic ticking and probe replies, neither of which is
// cheap to set up in node-package tests that only want to assert
// "given pair X is Questionable, do Y".
//
// The "ForTest" suffix is the conventional Go signal that production
// code must NOT call this — it bypasses the state machine invariants
// that protect normal lifecycle assumptions (TransitionAt timestamp
// consistency, ProbeFailures counter semantics, etc.).
//
// Lock contract: acquires t.mu in W mode like every other writer.
func (t *Table) ForceHealthForTest(identity, uplink PeerIdentity, h RouteHealth) {
	if identity.IsZero() || uplink.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	state := t.health.ensureLocked(identity, uplink, now)
	state.Health = h
	state.TransitionAt = now
}

// healthProjectionSig returns the two health bits that gate the announce
// projection for an (identity, uplink) pair: dead (excluded by the live-winner
// HealthDead filter) and cooled (excluded by the black-hole cooldown filter).
// When either flips, AnnounceProjectionFor's output for the identity changes,
// so the identity must be recorded in the Phase 3 change journal. A transition
// between non-Dead tiers (Good/Questionable/Bad) or an RTT-only update does NOT
// flip these bits and is intentionally not journalled — that would flood the
// ring on every hop_ack. Caller must hold t.mu.
func healthProjectionSig(state *RouteHealthState) (dead, cooled bool) {
	return state.Health == HealthDead, !state.CooldownUntil.IsZero()
}
