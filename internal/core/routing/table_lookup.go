package routing

// This file holds the Table-level read-side entry points. The actual
// storage-shape projections live on routeStore (see
// route_store_lookup.go); these wrappers exist because the read
// methods are part of Table's public surface and because they own
// the t.mu acquisition that routeStore's *Locked methods require.
// Table.Lookup and Table.Snapshot additionally layer routing-domain
// invariants on top of the raw storage view: the synthetic
// self-route ("you can always reach yourself") is a Table concern,
// not a storage concern, so it lives here rather than inside
// routeStore.

// Announceable returns routes suitable for announcing to a specific
// peer, applying split horizon (claim.Uplink != excludeVia, which
// would echo what the peer just told us) and own-identity filtering
// (Identity != excludeVia, which avoids sending the peer routes
// claiming the peer itself as the destination — degenerate "you can
// reach yourself via me" claims, and on pre-A1 receivers this also
// avoids the own-origin anti-spoof trip when Identity happens to
// equal that receiver's localOrigin). Withdrawn and expired routes
// are also excluded.
//
// Post-Phase-A there is no per-Origin filter: storage no longer
// keeps a transit Origin so there is nothing to compare against
// excludeVia — see routeStore.AnnounceableFor and uplink_claim.go.
//
// Note: we do NOT send fake hops=16 withdrawals here — that would
// violate the per-(Identity, sender) SeqNo invariant on receivers.
func (t *Table) Announceable(excludeVia PeerIdentity) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Phase 2 (PR 11.23 P2): pass the health-store's Dead probe
	// so locally-Dead pairs are suppressed from the announceable
	// set — same contract as Lookup's Dead filter. healthStore.
	// isDeadLocked is a pure map read under t.mu.RLock(); it
	// does not take any additional lock.
	//
	// Phase 3 PR 12.4: also pass the cooldown probe so cooled-
	// down (Identity, Uplink) pairs are dropped from the
	// announceable set. The closure captures `now` at call time
	// so isCooledDownLocked compares against the same clock
	// reading the rest of the call uses.
	now := t.clock()
	cooledDown := func(identity, uplink PeerIdentity) bool {
		return t.health.isCooledDownLocked(identity, uplink, now)
	}
	return t.store.AnnounceableFor(excludeVia, now, t.health.isDeadLocked, cooledDown)
}

// AnnounceTo returns the wire-safe AnnounceEntry projection of routes
// to announce to a specific peer, applying split horizon (claim.Uplink
// != excludeVia, would echo what the peer just told us), own-identity
// filtering (Identity != excludeVia, degenerate "you can reach yourself
// via me" claims), per-Identity live-winner selection, and own-origin
// tombstone passthrough (so own-origin withdrawals can be retried via
// the announce delta mechanism). Post-Phase-A there is no per-Origin
// filter — storage no longer keeps a transit Origin.
//
// AnnounceTo runs under the writer lock because AnnounceProjectionFor
// updates per-Identity outbound SeqNo state (routeStore.outboundContent,
// outboundMax, outboundPeerMax) so the wire SeqNo timeline stays
// monotonically non-decreasing against receivers' (Identity, sender)
// dedup. See route_store.go's outboundContent / outboundBroadcastMax /
// outboundPeerMax field doc-comments and nextOutboundSeqLockedPerPeer
// for the rationale. The cost is straightforward: one announce cycle through
// the loop briefly blocks concurrent readers of the routing table,
// which is fine — AnnounceTo is called at most once per peer per
// announce cycle (~30s cadence), and concurrent table reads on the
// hot path go through the lock-free Snapshot cache in node.Service,
// not direct Table calls.
func (t *Table) AnnounceTo(excludeVia PeerIdentity) []AnnounceEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Phase 1 P2: AnnounceProjectionFor can engage a per-Identity
	// SeqNo flap-cap hold-down as a side effect of recording an
	// outbound-SeqNo advance — when that happens
	// RouteCapStats.SeqNoFlapHoldowns bumps, which is part of the
	// published Snapshot. Without marking dirty here the snapshot
	// publisher's ConsumeDirty fast-path (see
	// node/routing_snapshot.go::rebuildRoutingSnapshot) skips the
	// rebuild and fetch_route_summary keeps reporting the prior
	// counter value until some unrelated mutation arrives — an
	// operator-visible regression for a counter that exists
	// specifically to surface the engage event.
	//
	// The return-value contract was widened to (entries, mutated)
	// so AnnounceTo can react inside the same writer-lock critical
	// section the mutation happened in.
	// Phase 2 (PR 11.23 P2): same health-store probe as
	// Announceable above — locally-Dead claims are excluded from
	// the live-winner candidate pool so we stop re-advertising
	// them to neighbours. If all claims for an Identity are Dead,
	// AnnounceProjectionFor falls through to the tombstone branch
	// (own-direct tombstones emit; transit tombstones never do).
	//
	// Phase 3 PR 12.4: black-hole cooldown filter on the same
	// projection path — symmetric with Announceable above.
	now := t.clock()
	cooledDown := func(identity, uplink PeerIdentity) bool {
		return t.health.isCooledDownLocked(identity, uplink, now)
	}
	entries, mutated := t.store.AnnounceProjectionFor(excludeVia, now, t.health.isDeadLocked, cooledDown)
	if mutated {
		t.dirty.Store(true)
	}
	return entries
}

// AnnounceTargetFor returns the per-receiver wire projection of
// a single (target) identity for a specific requester —
// effectively one iteration of AnnounceProjectionFor scoped to
// a single identity. Used by handleRouteQuery to construct
// route_query_response_v1 with the same outbound-SeqNo namespace
// as a regular announce_routes frame to the same receiver: the
// wire SeqNo comes from nextOutboundSeqLockedPerPeer just like
// AnnounceProjectionFor's per-Identity emit, so subsequent
// announce_routes cycles to the same receiver see a
// monotonically non-decreasing SeqNo timeline. Without this
// alignment a raw `claim.SeqNo` on the wire would diverge from
// the responder's outbound-counter namespace and the receiver
// could reject later legitimate announces as stale, or trip the
// cross-Origin lineage guard via a mismatched LastIngressOrigin
// (PR 11.32 P2).
//
// Returns (entry, uplink, true) on a live winner; (zero,
// "", false) when no non-Dead non-withdrawn non-expired claim is
// selectable. Direct (own-origin) claims are exempt from the
// Dead filter, mirroring AnnounceProjectionFor's PR 11.24 / 11.25
// behaviour. Tombstones are NOT projected — route_query is a
// positive-existence query; withdrawals propagate through
// announce_routes only.
//
// uplink is the live winner's claim.Uplink, kept on the return
// signature so the wire frame can carry best_uplink as
// informational metadata (the receiver ignores it for routing
// decisions — see handleRouteQueryResponse's BestUplink note).
//
// Side effects: advances the per-receiver outbound SeqNo for the
// target identity via nextOutboundSeqLockedPerPeer. That watermark
// is internal to routeStore and is NOT part of routing.Snapshot, so
// a normal emit does NOT mark the table dirty — waking the snapshot
// publisher for a change it cannot observe would let route_query
// traffic keep the publisher rebuilding once per second for nothing.
// The table is marked dirty ONLY when the advance engages the SeqNo
// flap-cap hold-down (the `armed` branch below): that bumps the
// published SeqNoFlapHoldowns CapStats counter, which the snapshot
// DOES carry. This mirrors AnnounceTo / AnnounceProjectionFor, where
// `mutated` (and thus dirty) is set only on the armed path.
//
// Lock contract: acquires t.mu in W mode. Callers must not hold
// any other domain mutex of node.Service at invocation time.
func (t *Table) AnnounceTargetFor(target, requester PeerIdentity) (AnnounceEntry, PeerIdentity, bool) {
	if target.IsZero() || requester.IsZero() || target == requester {
		return AnnounceEntry{}, PeerIdentity{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()

	// PR 11.33 P2 — SeqNo flap-cap hold-down gate. Mirrors
	// AnnounceProjectionFor's top-of-loop
	// isInSeqHoldDownLocked check (route_store_lookup.go).
	// When an Identity's outbound-SeqNo advance rate crossed
	// MaxSeqAdvancePerWindow inside SeqAdvanceWindow, wire emit
	// for X is suppressed to every peer for the entire
	// DefaultSeqHoldDownDuration window — and that suppression
	// applies to route_query_response too, otherwise a peer
	// receives a route_query answer for a target that the
	// announce path is explicitly NOT advertising. Return
	// Found=false here so the responder reports "no route" while
	// hold-down is active. Receive-side state stays accurate
	// regardless (this gate is wire-emit only).
	if t.flap.isInSeqHoldDownLocked(target, now) {
		return AnnounceEntry{}, PeerIdentity{}, false
	}

	bucket, ok := t.store.buckets[target]
	if !ok {
		return AnnounceEntry{}, PeerIdentity{}, false
	}

	// Pick live winner using the SAME CompositeScore ranking
	// Table.Lookup applies (sortRoutesByCompositeScoreLocked).
	// PR 11.34 P2 — earlier the picker used isBetterLiveClaim
	// (Hops + SeqNo + Extra + Uplink), which is the announce-plane's
	// wire-stability comparator. That disagreed with Lookup's
	// CompositeScore winner whenever health / RTT / source-trust
	// flipped the ranking, so a route_query response could
	// advertise a 2-hop Bad path while the relay path's Lookup
	// already selected a 3-hop Good alternative — defeating the
	// whole point of route_query as a recovery mechanism after a
	// Bad/Dead transition.
	//
	// Aligning the picker with Lookup's CompositeScore means
	// AnnounceTargetFor advertises whatever the responder would
	// itself select for that target. The Dead filter with Direct
	// exemption is the same one applied at the Lookup level; the
	// Direct + Dead → Bad-equivalent score substitution mirrors
	// sortRoutesByCompositeScoreLocked so a Dead Direct claim is
	// deprioritised but selectable as last resort (PR 11.25 / 11.26).
	scoreClaim := func(claim *UplinkClaim) float64 {
		// Mirror sortRoutesByCompositeScoreLocked exactly: fetch
		// the health state once (Local has no health by design,
		// but UplinkClaim cannot be Local — synthesised at
		// Lookup-time only — so we skip that guard here), then
		// apply the Direct + Dead → Bad-equivalent substitution
		// before scoring. Keeps the two CompositeScore call sites
		// (Lookup's sort and this picker) on identical inputs.
		h := t.health.getLocked(target, claim.Uplink)
		if claim.Source == RouteSourceDirect && h != nil && h.Health == HealthDead {
			adjusted := *h
			adjusted.Health = HealthBad
			h = &adjusted
		}
		return CompositeScore(uint8(claim.Hops), claim.Source, h, claim.AttestedSigVerified)
	}
	var liveWinner *UplinkClaim
	var liveWinnerScore float64
	for i := range bucket {
		claim := &bucket[i]
		if claim.Uplink == requester {
			continue
		}
		if claim.IsExpired(now) || claim.IsWithdrawn() {
			continue
		}
		// Dead filter with Direct exemption — symmetric with the
		// Lookup-side and AnnounceProjectionFor filters
		// (table_lookup.go::Lookup, route_store_lookup.go's PR
		// 11.24 P2#1 / 11.25 split).
		if claim.Source != RouteSourceDirect && t.health.isDeadLocked(target, claim.Uplink) {
			continue
		}
		// Phase 3 PR 12.4: black-hole cooldown filter, no Direct
		// exemption — a cooled-down direct uplink would carry the
		// route_query response into the same dead-end the
		// reputation primitive already learned to avoid.
		if t.health.isCooledDownLocked(target, claim.Uplink, now) {
			continue
		}
		score := scoreClaim(claim)
		if liveWinner == nil || score > liveWinnerScore {
			w := *claim
			liveWinner = &w
			liveWinnerScore = score
		}
	}
	if liveWinner == nil {
		return AnnounceEntry{}, PeerIdentity{}, false
	}

	// emitOrigin matches AnnounceProjectionFor's sender-originated
	// synthesis: localOrigin (post-Phase-A wire-Origin contract),
	// with the empty-fallback-to-identity guard for test fixtures
	// that build Tables without WithLocalOrigin.
	emitOrigin := t.localOrigin
	if emitOrigin.IsZero() {
		emitOrigin = target
	}

	sig := outboundEmitSig{
		Uplink:    liveWinner.Uplink,
		Hops:      liveWinner.Hops,
		Withdrawn: false,
		ExtraSig:  string(normalizeExtra(liveWinner.Extra)),
		// Round-14: include AttestedSig bytes in the targeted-
		// projection content key — matches AnnounceProjectionFor's
		// live emit branch. See outboundEmitSig type doc.
		AttestedSig: string(liveWinner.AttestedSig),
	}
	wireSeqNo, armed := t.store.nextOutboundSeqLockedPerPeer(target, requester, sig, liveWinner.SeqNo, liveWinner.LastIngressOrigin, now)
	if armed {
		// PR 11.33 P2 — the advance we just recorded crossed the
		// SeqNo flap-cap threshold and engaged hold-down INSIDE
		// this call. The top-of-function gate already passed
		// (hold-down was not active when we entered), so without
		// this extra suppression the peer that triggered the
		// engage would still receive the over-threshold emit —
		// only the NEXT call would observe suppression. That
		// contradicts the "wire emit suppressed to EVERY peer"
		// contract documented on the SeqNo flap cap; mirrors
		// AnnounceProjectionFor's symmetric branch
		// (route_store_lookup.go).
		//
		// Storage state (outboundContent, outboundMax,
		// outboundPeerMax) is intentionally kept past this
		// branch: when hold-down expires the cache hit on the
		// same sig will reuse the burnt SeqNo on the receiver's
		// first post-release emit, so the receiver's monotonic-
		// SeqNo timeline never goes backwards.
		//
		// Mark dirty so the snapshot publisher republishes the
		// SeqNoFlapHoldowns counter bump (visible via
		// fetchRouteSummary). This is the ONLY published-state change
		// AnnounceTargetFor can produce — the normal emit below only
		// advances the unpublished outbound-SeqNo watermark, so it
		// must NOT mark dirty (see the side-effects note above).
		t.dirty.Store(true)
		return AnnounceEntry{}, PeerIdentity{}, false
	}

	return AnnounceEntry{
		Identity: target,
		Origin:   emitOrigin,
		Hops:     int(liveWinner.Hops),
		SeqNo:    wireSeqNo,
		Extra:    liveWinner.Extra,
		// Phase 4 13.2-A: targeted-projection mirror of
		// AnnounceProjectionFor — forward the attested-links signature
		// stored on the winning UplinkClaim.
		AttestedSig:         liveWinner.AttestedSig,
		AttestedSigVerified: liveWinner.AttestedSigVerified,
	}, liveWinner.Uplink, true
}

// Lookup returns all selectable routes for the given identity,
// ranked by the Phase 2 CompositeScore (composite of hops, RTT
// EWMA, RouteHealth penalty, and source-trust bonus — see
// docs/protocol/route_health.md). HealthDead claims
// are filtered out as "do not select"; HealthBad / HealthQuestionable
// remain in the result but with reduced scores so the caller picks
// them only when no better path exists.
//
// Backward-compat (nil-health path): when a (Identity, Uplink) pair
// has no RouteHealthState yet (cold-start, no hop_ack / probe seen),
// CompositeScore falls back to base − hops × 10 + sourceBonus, which
// reproduces the pre-Phase-2 by-hops-with-source-priority ordering
// modulo trust-tier ties. This keeps callers that have not yet wired
// health into their flow unchanged.
//
// When the queried identity matches the node's own localOrigin, a
// synthetic local route (Hops=0, RouteSourceLocal) is prepended. The
// synthetic entry has no health state and is scored as nil-health, so
// it lands at the top of the ranked result (RouteSourceLocal carries
// the same +20 source bonus as Direct, and Hops=0 gives the maximum
// base score). The self-route injection lives here rather than in
// routeStore because it is a routing-domain invariant, not storage
// state.
func (t *Table) Lookup(identity PeerIdentity) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	var candidates []RouteEntry
	if !t.localOrigin.IsZero() && identity == t.localOrigin {
		candidates = append(candidates, t.localRouteEntry())
	}
	candidates = append(candidates, t.store.LookupActive(identity, now)...)

	// Empty candidate set returns nil (not an empty slice) to keep the
	// pre-Phase-2 callers that probe `routes == nil` working. The
	// pre-allocation below would otherwise produce a non-nil empty
	// slice and break existing nil-equality checks.
	if len(candidates) == 0 {
		return nil
	}

	// Filter HealthDead claims and rank the remainder by composite
	// score. The filter is keyed strictly on Health==HealthDead, NOT
	// on `score < 0`: the base formula (100 − hops×10) goes negative
	// for hops > 10 (a legitimate very-bad route), and after PR 11.35
	// P2 strict-tier penalties (Q=−250, Bad=−500) every Questionable
	// and Bad claim — including 1-hop low-RTT Direct ones — also
	// scores well below zero. Sign-based filtering cannot tell any
	// of these legitimately-selectable last-resort claims apart from
	// the Dead sentinel, so the only correct Dead gate is the Health
	// field check. Phase 2 still selects high-hop / Questionable /
	// Bad routes as last resort, while Dead claims are excluded
	// outright (the wire withdrawal / gossip fallback machinery
	// takes over).
	//
	// PR 11.25 P2 — Direct exemption. RouteSourceDirect claims are
	// EXEMPT from the Dead filter, mirroring the symmetric exemption
	// on the announce projection (route_store_lookup.go). Direct
	// routes are session-bound: their storage lifecycle is driven by
	// AddDirectPeer / RemoveDirectPeer, and a live storage claim
	// means the underlying session is alive (RemoveDirectPeer would
	// have tombstoned it on disconnect). A HealthDead label on a
	// Direct claim means "no organic hop_ack traffic has flowed
	// lately" — a symptom of message-flow patterns, NOT of session
	// loss. Excluding such a claim from Lookup would force the
	// relay path to fall back to gossip / trigger a route query
	// while the announce plane keeps advertising the route, leaving
	// local forwarding and outbound announce semantics in
	// disagreement. Keep Direct claims selectable; the
	// Questionable / Bad penalties from CompositeScore are
	// sufficient to deprioritise them against confirmed
	// alternatives if any exist.
	//
	// The filter pass is O(N) where N is at most
	// MaxOutgoingPeers + MaxIncomingPeers (≤24 in densely connected
	// production meshes) plus the optional self-route.
	var result []RouteEntry
	for _, entry := range candidates {
		health := t.lookupHealthLocked(identity, entry)
		if health != nil && health.Health == HealthDead && entry.Source != RouteSourceDirect {
			continue
		}
		// Phase 3 PR 12.4: black-hole cooldown filter. Unlike the
		// Dead filter above, there is NO Direct exemption — the
		// 5 consecutive hop-ack failures that armed the cooldown
		// are empirical evidence that the peer cannot deliver,
		// regardless of whether the underlying session is alive.
		// Suppressing Direct here matches the announce-side
		// filter (route_store_lookup.go) so local forwarding and
		// outbound announce semantics agree. The arm auto-clears
		// via TickHealth's applyCooldownExpiryLocked sweep after
		// BlackHoleCooldown elapses, so this is self-healing.
		// Self-route (RouteSourceLocal) is excluded from the
		// check structurally — its NextHop is the local identity
		// itself, which is never tracked in the health store.
		if entry.Source != RouteSourceLocal && t.health.isCooledDownLocked(identity, entry.NextHop, now) {
			continue
		}
		result = append(result, entry)
	}
	sortRoutesByCompositeScoreLocked(result, t.health, identity)
	return result
}

// LookupForRelay is the Phase 3 PR 12.6 multi-path traffic
// shaping wrapper over Lookup. It returns the same ranked result
// as Lookup, EXCEPT when (a) the supplied `hint` is divisible by
// ShapingProbeRatio AND (b) the top-ranked candidate is a
// shaping candidate (isShapingCandidate) AND (c) at least one
// alternative non-shaping candidate exists earlier in the
// ranked tail. In that single case the top entry yields its
// slot to the highest-ranked non-shaping alternative; the
// shaping candidate moves to index 1. All other ranking remains
// untouched.
//
// The effect is a roughly 1-in-ShapingProbeRatio rotation that
// pushes relay traffic onto an alternative when the favourite
// is under-observed (warmup) or just exited cooldown — see
// isShapingCandidate's doc-comment for the qualifying criteria.
// Phase 3 §4.6 records the design rationale and the choice of
// ratio.
//
// Caller-provided `hint`: Service.nextRelayShapingHint()
// (atomic.Uint64 increment) so the rotation cadence stays
// deterministic across concurrent relay forwards without
// requiring per-Table counter state. The function is therefore
// stateless w.r.t. shaping; this also keeps tests trivially
// deterministic — `LookupForRelay(identity, 0)` always reorders
// when the conditions hold, `LookupForRelay(identity, 1)`
// always returns the plain Lookup order.
//
// Constraints the shaping pass respects:
//
//   - The alternative MUST be in a healthy tier (Good or
//     Questionable). Lookup already drops cooled-down pairs, but it
//     keeps fully-observed Bad pairs (merely low-scored) and Direct
//     Dead pairs (direct exemption), so the wrapper applies its own
//     tier gate via isShapingAlternativeAcceptable — the rotation
//     never diverts traffic onto a known-worse path (Phase 3 §4.6).
//   - The alternative MUST itself be a non-shaping candidate;
//     swapping one shaping candidate for another would just
//     move the warmup gap around without accelerating sample
//     accrual.
//
// Returns nil when Lookup itself returns nil. Lock contract:
// inherits Lookup's t.mu.RLock window.
func (t *Table) LookupForRelay(identity PeerIdentity, hint uint64) []RouteEntry {
	routes := t.Lookup(identity)
	if len(routes) < 2 {
		return routes
	}
	if ShapingProbeRatio == 0 || hint%ShapingProbeRatio != 0 {
		return routes
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.clock()

	topHealth := t.lookupHealthLocked(identity, routes[0])
	if !isShapingCandidate(topHealth, now) {
		return routes
	}

	// Find the highest-ranked acceptable alternative. Routes is sorted
	// descending by CompositeScore, so the first qualifying entry in
	// routes[1:] is the best alternative. "Acceptable" means it is
	// neither a shaping candidate itself NOR a Bad/Dead pair: Lookup
	// keeps fully-observed Bad pairs (only low-scored) and Direct Dead
	// pairs (direct exemption), so a plain !isShapingCandidate check
	// would happily promote a known-worse path over a merely-warming
	// Good one. See isShapingAlternativeAcceptable / Phase 3 §4.6.
	for i := 1; i < len(routes); i++ {
		altHealth := t.lookupHealthLocked(identity, routes[i])
		if !isShapingAlternativeAcceptable(altHealth, now) {
			continue
		}
		// Promote routes[i] to index 0, demote the original
		// shaping candidate to index 1. Other entries stay
		// in their relative order — this matches the
		// "single rotation slot" semantic the plan describes.
		shaped := make([]RouteEntry, 0, len(routes))
		shaped = append(shaped, routes[i])
		for j := 0; j < len(routes); j++ {
			if j == i {
				continue
			}
			shaped = append(shaped, routes[j])
		}
		return shaped
	}

	// No non-shaping alternative — leave the order alone. The
	// shaping pass exists to ACCELERATE reliability accrual; if
	// every alternative is also under-observed there is no
	// point rotating among them.
	return routes
}

// lookupHealthLocked returns the RouteHealthState for the given
// route entry, or nil when (a) the route is the synthetic self-route
// (RouteSourceLocal — never tracked), (b) the health store is nil
// (defensive — should not happen post-NewTable), or (c) no state
// has been recorded for the (identity, uplink) pair yet.
//
// Caller must hold t.mu in R mode.
func (t *Table) lookupHealthLocked(identity PeerIdentity, entry RouteEntry) *RouteHealthState {
	if entry.Source == RouteSourceLocal {
		return nil
	}
	if t.health == nil {
		return nil
	}
	return t.health.getLocked(identity, entry.NextHop)
}

// InspectTriple returns the raw route entry for the given lookup key,
// or nil if no claim matches. Post-Phase-A the dedup key is
// (Identity, Uplink=NextHop) — the key.Origin component is retained
// on the public API for backward compatibility but is IGNORED inside
// routeStore.InspectTriple. Unlike Lookup, this does not filter by
// withdrawn/expired status — callers see the entry exactly as stored.
// Intended for diagnostics and debugging (e.g. explaining why
// UpdateRoute rejected an incoming entry).
func (t *Table) InspectTriple(key RouteTriple) *RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.store.InspectTriple(key)
}

// Snapshot returns an immutable point-in-time view of the entire table.
// All fields (routes, counts, flap state, cap counters) are captured
// under a single lock acquisition, ensuring a self-consistent response
// for RPC consumers.
//
// When localOrigin is configured, a synthetic local route (Hops=0,
// RouteSourceLocal) is injected for the node's own identity, ensuring
// that RPC consumers always see a route to the local node. The
// self-route is read-time only — it is NOT counted in
// TotalEntries/ActiveEntries because those counters describe
// persisted table state and must stay consistent with ActiveSize().
func (t *Table) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()

	// routeStore.SnapshotRoutes walks storage once, building a deep
	// copy of every bucket and counting total/active entries in the
	// same pass. The copy is independent of storage, so concurrent
	// mutators after this method returns cannot tear the published
	// view.
	routes, totalEntries, activeEntries := t.store.SnapshotRoutes(now)

	snap := Snapshot{
		Routes:        routes,
		TakenAt:       now,
		TotalEntries:  totalEntries,
		ActiveEntries: activeEntries,
	}

	// Inject synthetic local route for own identity. See Lookup for
	// the rationale — the self-route is a routing-domain invariant
	// layered on top of the raw storage view.
	if !t.localOrigin.IsZero() {
		localEntry := t.localRouteEntry()
		snap.Routes[t.localOrigin] = append(
			[]RouteEntry{localEntry},
			snap.Routes[t.localOrigin]...,
		)
	}

	// Capture flap state atomically with routes — same t.mu critical
	// section, same `now` reading. FlapDetector.snapshotLocked applies
	// the window-trim / hold-down-normalize rules described on
	// FlapEntry; see flap.go for the exact contract.
	snap.FlapState = t.flap.snapshotLocked(now)

	// Phase 2 health snapshot (PR 11.27 P2#1) captured inside the
	// same RLock as routes/flap so consumers that need to apply
	// Lookup-style ranking (HealthDead filter, CompositeScore) read
	// a self-consistent view without a second t.mu.RLock round
	// trip. fetchRouteLookup is the primary consumer; without this
	// the RPC would call HealthSnapshot separately and break the
	// documented hot-read contract (cached snapshot, no routing
	// mutex per request).
	snap.Health = t.health.snapshotLocked()

	// CapStats is read AFTER the routes/flap pass: a writer that
	// landed an admission decision after we released that pass would
	// already have moved past the t.mu critical section, so its
	// counters reflect a state that is at most one tick stale relative
	// to the routes view. Acceptable for monitoring purposes.
	snap.CapStats = t.store.CapStats()

	return snap
}

// SnapshotIncremental is the copy-on-write variant of Snapshot used by
// the single-threaded snapshot publisher (Service.rebuildRoutingSnapshot).
// It reuses the previous projection for identities untouched since the
// last call and deep-copies only dirty / bulk-invalidated buckets, which
// removes the per-rebuild full-table route allocation that dominated
// routing churn on otherwise-idle nodes. TotalEntries / ActiveEntries are
// recomputed exactly every call, so time-driven expiry stays accurate.
//
// forceFull bypasses the reuse path and re-copies every bucket. The
// publisher passes it on a periodic cadence to heal any route mutation
// that set the coarse dirty flag but failed to mark its identity dirty
// (the safety net for the broad mutation surface in route_store_*.go).
// A full re-copy ALSO happens when a bulk mutation set snapFullDirty or on
// the cold-start (no reuse cache); the second return value reports whether
// THIS call performed a full re-copy (for any of those reasons) so the
// publisher can timestamp its self-heal cadence off actual fulls rather
// than only the ones it explicitly requested.
//
// NOT safe for concurrent callers: the reuse cache (snapRawCache) and the
// consumed snap dirty-set assume a single publisher. Any other consumer
// that needs an independent, always-complete copy must call Snapshot.
//
// Unlike Snapshot this takes t.mu in W mode because it consumes
// (clears) the snap dirty-set and updates the reuse cache. The write lock
// is held only for the projection build; the publisher runs at most once
// per routingSnapshotMinInterval so the added writer contention is
// negligible against the eliminated allocation.
func (t *Table) SnapshotIncremental(forceFull bool) (Snapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()

	full := forceFull || t.snapFullDirty || t.snapRawCache == nil
	raw, totalEntries, activeEntries := t.store.SnapshotRoutesIncremental(
		t.snapRawCache, t.snapDirtyIDs, full, now,
	)
	t.snapRawCache = raw
	t.snapFullDirty = false
	if len(t.snapDirtyIDs) > 0 {
		// Drop the consumed set; a fresh map keeps memory bounded to the
		// churn of the next interval rather than the high-water mark.
		t.snapDirtyIDs = make(map[PeerIdentity]struct{})
	}

	// The published Routes map must not alias snapRawCache: the self-route
	// injection below rewrites Routes[localOrigin], and that key must stay
	// the raw (no self-route) slice in the cache for the next reuse. A
	// shallow top-level copy keeps the per-identity slices shared
	// (read-only, copy-on-write) while isolating the map structure.
	routes := make(map[PeerIdentity][]RouteEntry, len(raw)+1)
	for id, slice := range raw {
		routes[id] = slice
	}

	snap := Snapshot{
		Routes:        routes,
		TakenAt:       now,
		TotalEntries:  totalEntries,
		ActiveEntries: activeEntries,
	}

	// Self-route injection, flap / health / cap capture — identical to
	// Snapshot, same `now`, same critical section. append() on a fresh
	// one-element literal allocates a new backing array, so the cached
	// raw slice for localOrigin is never mutated in place.
	if !t.localOrigin.IsZero() {
		localEntry := t.localRouteEntry()
		snap.Routes[t.localOrigin] = append(
			[]RouteEntry{localEntry},
			snap.Routes[t.localOrigin]...,
		)
	}
	snap.FlapState = t.flap.snapshotLocked(now)
	snap.Health = t.health.snapshotLocked()
	snap.CapStats = t.store.CapStats()

	return snap, full
}

// sortRoutesByCompositeScoreLocked sorts routes by Phase 2
// CompositeScore (descending — highest score wins, lands at index 0).
// The function is a stable insertion sort to keep the deterministic
// ordering tests rely on when scores tie (insertion sort preserves
// original order for equal compare results).
//
// The synthetic self-route (RouteSourceLocal) is naturally ranked
// first because its hop-0, +20-source-bonus composite score (~120)
// dominates any storage-side claim (max realistic ≈ 100 for a 1-hop
// Direct with low RTT and Good health).
//
// Lives at Table level (not inside routeStore) because Lookup applies
// it AFTER the synthetic self-route is merged with the storage
// results — the sort needs to see the local route as a candidate.
//
// Caller must hold t.mu in R mode (Lookup holds RLock; the health
// store reads are O(1) per entry).
func sortRoutesByCompositeScoreLocked(routes []RouteEntry, health *healthStore, identity PeerIdentity) {
	if len(routes) < 2 {
		return
	}
	scoreOf := func(r RouteEntry) float64 {
		var h *RouteHealthState
		if health != nil && r.Source != RouteSourceLocal {
			h = health.getLocked(identity, r.NextHop)
		}
		// PR 11.26 P2 — Direct Dead score adjustment. The PR 11.25
		// Direct-exempt branch keeps Dead Direct claims in the
		// candidate set (session is alive even if no organic hop_ack
		// flowed), but CompositeScore returns the -1 "excluded"
		// sentinel for ANY Dead, sinking Direct below every transit
		// alternative. That contradicts the exemption rationale:
		// Direct should stay selectable, just deprioritised. Treat
		// Dead Direct as Bad for scoring — same scoreHealthBadPenalty
		// as any other Bad pair, which lets a Good or Questionable
		// transit alternative win (the desired behaviour: prefer
		// confirmed paths) while still ranking Dead Direct above
		// other Bad transit options thanks to Direct's +20 source
		// bonus. PR 11.35 P2 widened the Bad penalty so the
		// strict-tier invariant holds across the score; Dead Direct
		// is therefore "strictly below every non-Bad alternative,
		// strictly above every transit Bad" under the same rule
		// applied to a genuine Bad Direct.
		if r.Source == RouteSourceDirect && h != nil && h.Health == HealthDead {
			adjusted := *h
			adjusted.Health = HealthBad
			h = &adjusted
		}
		return CompositeScore(uint8(r.Hops), r.Source, h, r.AttestedSigVerified)
	}
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && scoreOf(routes[j]) > scoreOf(routes[j-1]); j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}
