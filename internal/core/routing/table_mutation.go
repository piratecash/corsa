package routing

// This file holds the Table-level write-side entry points. The
// actual storage-shape operations live on routeStore (see
// route_store_mutation.go); these wrappers exist because:
//   - pre-mu validation (RouteEntry.Validate, RouteSourceLocal
//     reservation, foreign-origin RouteSourceDirect anti-spoof) is
//     not a storage concern and must run BEFORE t.mu is acquired;
//   - flap detection (Penalized flag, withdrawal recording) is
//     orthogonal to routing storage and lives on FlapDetector;
//   - the public Table API is what callers in node.Service /
//     internal/core/rpc / tests depend on. Wrappers preserve the
//     same signatures.
//
// Every wrapper follows the same shape: pre-mu validation → acquire
// t.mu → delegate to routeStore + flap → propagate the
// "mutated"/"return value" signal → mark t.dirty if mutated.

// AddDirectPeerResult describes the outcome of AddDirectPeer.
type AddDirectPeerResult struct {
	// Entry is the route entry that was created or refreshed.
	Entry RouteEntry

	// Penalized is true when the peer triggered flap detection and the
	// route was created with a shortened TTL. The caller can use this
	// to decide whether to delay or suppress the announcement.
	Penalized bool
}

// RemoveDirectPeerResult describes the outcome of RemoveDirectPeer.
type RemoveDirectPeerResult struct {
	// Withdrawals contains wire-ready AnnounceEntry items for direct routes
	// that this node originated. SeqNo is already incremented and Hops is
	// set to HopsInfinity. The caller sends these as-is in announce_routes
	// frames — no further seq arithmetic is needed.
	Withdrawals []AnnounceEntry

	// TransitInvalidated is the count of transit routes (learned via
	// announcement or hop_ack) that were silently marked as withdrawn
	// locally. No wire withdrawal is emitted for these — the originating
	// node is responsible for its own withdrawals.
	TransitInvalidated int

	// ExposedBackups lists identities where the withdrawal/invalidation of
	// routes through the disconnected peer exposed a surviving non-withdrawn,
	// non-expired backup route via a different next-hop. The caller can use
	// this to trigger event-driven pending queue drains — same semantics as
	// TickTTL's exposed return value.
	ExposedBackups []PeerIdentity
}

// TickTTLResult holds the outcome of a TTL sweep.
type TickTTLResult struct {
	// Exposed lists identities whose primary route expired but at least one
	// non-withdrawn backup route survives — callers can drain pending frames
	// to the surviving route immediately.
	Exposed []PeerIdentity

	// Removed is the total number of individual route entries that expired
	// across all identities. Zero means the routing table was not modified.
	Removed int
}

// UpdateRoute inserts or updates a route in the table. The dedup key is
// (Identity, Uplink=NextHop) — Phase 1 P0 Phase A collapsed the legacy
// (Identity, Origin, NextHop) triple to per-uplink storage. Origin is
// still part of the wire-shape RouteEntry: the caller is expected to
// have parsed it off the announce frame for foreign-origin anti-spoof
// on RouteSourceDirect (see below), and AdmitDirect/ApplyUpdate may
// also stamp it on the synthesised wire withdrawal SeqNo state; once
// admission is done, Origin is dropped from storage. If an existing
// claim shares the same (Identity, Uplink) pair:
//
//   - If incoming SeqNo > existing SeqNo: replace unconditionally.
//   - If incoming SeqNo == existing SeqNo: replace only if the incoming
//     source has a higher trust rank or fewer hops.
//   - If incoming SeqNo < existing SeqNo: reject (stale announcement).
//
// Tombstone-resurrection guard: an incoming live entry with
// SeqNo <= existing tombstone's SeqNo is rejected — a live entry must
// be strictly newer than the most recent withdrawal we observed on the
// same uplink, otherwise a stale re-announce could resurrect a peer
// the upstream has already withdrawn.
//
// Returns (RouteAccepted, nil) if the route was new or improved,
// (RouteUnchanged, nil) if an existing alive route was found but not
// improved, (RouteRejected, nil) if rejected by tombstone/SeqNo rules,
// or (RouteRejected, err) if the entry is malformed.
//
// Pre-mu validation (Validate + RouteSourceLocal guard + foreign-origin
// RouteSourceDirect anti-spoof) runs OUTSIDE t.mu to surface caller
// errors fast and avoid taking the lock for inputs the store would
// have rejected anyway. The actual dedup/admission/SeqNo logic lives
// in routeStore.ApplyUpdate.
func (t *Table) UpdateRoute(entry RouteEntry) (RouteUpdateStatus, error) {
	if err := entry.Validate(); err != nil {
		return RouteRejected, err
	}

	// RouteSourceLocal is purely synthetic — it exists only in
	// Lookup/Snapshot results and must never be persisted in the
	// table. Allowing it would let any caller inject a zero-hop
	// highest-trust route for an arbitrary identity, bypassing all
	// real routing.
	if entry.Source == RouteSourceLocal {
		return RouteRejected, ErrLocalSourceReserved
	}

	// Direct routes must originate from this node. A RouteSourceDirect
	// entry with a foreign Origin would outrank all announcement /
	// hop_ack routes in Lookup and never be eligible for own-origin
	// withdrawal on disconnect.
	if entry.Source == RouteSourceDirect && t.localOrigin != "" && entry.Origin != t.localOrigin {
		return RouteRejected, ErrDirectForeignOrigin
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	status, mutated := t.store.ApplyUpdate(entry, t.clock())
	if mutated {
		t.dirty.Store(true)
	}
	return status, nil
}

// WithdrawRoute marks a specific route as withdrawn by setting hops to
// HopsInfinity. This should be called when processing an incoming withdrawal
// from the wire (hops=16, incremented SeqNo). For local peer disconnects,
// use RemoveDirectPeer instead.
//
// Returns true if the withdrawal was applied.
func (t *Table) WithdrawRoute(identity, origin, nextHop PeerIdentity, seqNo uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := RouteTriple{Identity: identity, Origin: origin, NextHop: nextHop}
	mutated := t.store.WithdrawTriple(key, seqNo, t.clock())
	if mutated {
		t.dirty.Store(true)
	}
	return mutated
}

// AddDirectPeer registers a directly connected peer in the routing table.
// It creates a direct route (Hops=1, Source=RouteSourceDirect) with
// Origin set to this node's localOrigin and an auto-incremented SeqNo
// from the monotonic counter.
//
// Idempotent: if the peer already has an active (non-withdrawn) direct
// route originated by this node, AddDirectPeer returns the existing
// entry without incrementing SeqNo. This prevents unnecessary SeqNo
// churn and triggered updates on duplicate connect events or
// additional sessions to the same peer identity.
//
// SeqNo is only incremented when the route is new or was previously
// withdrawn (reconnect after disconnect).
//
// Flap dampening: if the peer is in hold-down (too many recent
// disconnects), Result.Penalized is set. The caller can use this
// to suppress or delay the triggered announcement.
//
// Returns AddDirectPeerResult and nil error on success.
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) AddDirectPeer(peerIdentity PeerIdentity) (AddDirectPeerResult, error) {
	if t.localOrigin == "" {
		return AddDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return AddDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	penalized := t.flap.isInHoldDownLocked(peerIdentity, now)
	entry, mutated := t.store.AdmitDirectPeer(peerIdentity, now)
	if mutated {
		t.dirty.Store(true)
	}
	return AddDirectPeerResult{Entry: entry, Penalized: penalized}, nil
}

// RemoveDirectPeer handles a peer disconnect. It withdraws the direct
// route originated by this node (with an auto-incremented SeqNo) and
// silently invalidates all transit routes learned through that peer.
//
// The returned Withdrawals are wire-ready: SeqNo is already incremented,
// Hops is HopsInfinity, and the entries are in AnnounceEntry form. The
// caller sends them as-is in announce_routes frames.
//
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) RemoveDirectPeer(peerIdentity PeerIdentity) (RemoveDirectPeerResult, error) {
	if t.localOrigin == "" {
		return RemoveDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return RemoveDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.flap.recordWithdrawalLocked(peerIdentity, now)
	withdrawals, transitInvalidated, exposed := t.store.InvalidateAllVia(peerIdentity, now)

	// Always dirty: recordWithdrawalLocked mutated flap state
	// (withdrawTimes at minimum, possibly hold-down arming) and
	// InvalidateAllVia rewrites every affected route's
	// Hops/SeqNo/ExpiresAt. Both are part of the published Snapshot.
	t.dirty.Store(true)

	return RemoveDirectPeerResult{
		Withdrawals:        withdrawals,
		TransitInvalidated: transitInvalidated,
		ExposedBackups:     exposed,
	}, nil
}

// InvalidateTransitRoutes sets hops=HopsInfinity on all non-direct routes
// whose NextHop matches peerIdentity. Unlike RemoveDirectPeer this does
// NOT generate wire withdrawals and does NOT touch direct routes — it is
// a defense-in-depth cleanup for peers that had mesh_routing_v1 (could
// advertise routes) but not mesh_relay_v1 (no direct route was created).
// Returns the number of transit routes invalidated and a slice of identities
// where the invalidation exposed a surviving non-withdrawn backup route via
// a different next-hop (same semantics as TickTTL's exposed return value).
func (t *Table) InvalidateTransitRoutes(peerIdentity PeerIdentity) (int, []PeerIdentity) {
	if peerIdentity == "" {
		return 0, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	invalidated, exposed := t.store.InvalidateTransitVia(peerIdentity, t.clock())
	if invalidated > 0 {
		t.dirty.Store(true)
	}
	return invalidated, exposed
}

// TickTTL removes expired routes from the table and cleans up stale
// flap detection state. Should be called periodically (e.g., every
// second or every few seconds).
//
// Returns identities where at least one entry expired AND at least one
// non-withdrawn, non-expired route survives — indicating that a backup
// route has been exposed by the expiry. The caller can use this to
// trigger event-driven pending queue drains.
//
// Only ExpiresAt is checked — withdrawn (Hops >= HopsInfinity) entries
// are kept until their ExpiresAt elapses. This preserves tombstones
// created by WithdrawRoute that guard against resurrection from delayed
// lower-SeqNo announcements. RemoveDirectPeer and InvalidateTransitRoutes
// set a short ExpiresAt on withdrawn entries so they are cleaned up
// promptly without breaking tombstone semantics.
func (t *Table) TickTTL() TickTTLResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	totalRemoved, exposed := t.store.CompactExpired(now)
	// Snapshot exposes both Routes and FlapState. Mark dirty if
	// either changed; a no-op tick (nothing expired and no flap-state
	// cleanup) must not force a republish.
	flapMutated := t.flap.tickLocked(now)
	if totalRemoved > 0 || flapMutated {
		t.dirty.Store(true)
	}
	return TickTTLResult{Exposed: exposed, Removed: totalRemoved}
}

// RefreshDirectPeers is a no-op retained for API compatibility.
//
// Direct routes use ExpiresAt=zero (never expire by time). Their
// lifetime is managed entirely by the session lifecycle:
// AddDirectPeer creates them on socket connect, RemoveDirectPeer
// withdraws them on socket close. No periodic TTL refresh is needed.
//
// Previously, direct routes had a finite TTL that required periodic
// refresh. This created a race condition: if TickTTL removed an
// expired direct route before the next refresh cycle, the route was
// permanently lost (AddDirectPeer would not be called again for an
// already-counted session).
func (t *Table) RefreshDirectPeers() int {
	return 0
}
