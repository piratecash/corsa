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
	return t.store.AnnounceableFor(excludeVia, t.clock())
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
	entries, mutated := t.store.AnnounceProjectionFor(excludeVia, t.clock())
	if mutated {
		t.dirty.Store(true)
	}
	return entries
}

// Lookup returns all non-withdrawn, non-expired routes for the given
// identity, sorted by preference: source priority
// (local > direct > hop_ack > announcement), then by hops ascending
// within the same source tier.
//
// When the queried identity matches the node's own localOrigin, a
// synthetic local route (Hops=0, RouteSourceLocal) is prepended. This
// ensures a node can always resolve a route to itself without
// requiring an external peer session or an explicit table entry. The
// self-route injection lives here rather than in routeStore because
// it is a routing-domain invariant, not storage state.
func (t *Table) Lookup(identity PeerIdentity) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []RouteEntry
	if t.localOrigin != "" && identity == t.localOrigin {
		result = append(result, t.localRouteEntry())
	}
	result = append(result, t.store.LookupActive(identity, t.clock())...)
	sortRoutes(result)
	return result
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
	if t.localOrigin != "" {
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

	// CapStats is read AFTER the routes/flap pass: a writer that
	// landed an admission decision after we released that pass would
	// already have moved past the t.mu critical section, so its
	// counters reflect a state that is at most one tick stale relative
	// to the routes view. Acceptable for monitoring purposes.
	snap.CapStats = t.store.CapStats()

	return snap
}

// sortRoutes sorts routes by preference: source priority first
// (direct > hop_ack > announcement), then by hops ascending.
//
// Lives at Table level rather than inside routeStore because Lookup
// applies it AFTER the synthetic self-route is merged with the
// storage results — the sort needs to see the local route as the
// highest-priority candidate.
func sortRoutes(routes []RouteEntry) {
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && isBetter(&routes[j], &routes[j-1]); j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}
