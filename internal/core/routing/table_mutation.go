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

import "time"

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
	if entry.Source == RouteSourceDirect && !t.localOrigin.IsZero() && entry.Origin != t.localOrigin {
		return RouteRejected, ErrDirectForeignOrigin
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	status, mutated, wireChanged := t.store.ApplyUpdate(entry, t.clock())
	if mutated {
		t.dirty.Store(true)
		// Snapshot always (ApplyUpdate may have refreshed ExpiresAt, a Snapshot
		// field), but journal — which drives the cursor-mode delta — only when the
		// announce WIRE projection actually changed. A same-SeqNo/same-hops TTL
		// refresh is mutated-but-wire-identical (wireChanged=false): journaling it
		// would re-emit an unchanged delta and fan out unchanged routes across the
		// mesh on every refresh. A sig/Extra upgrade on the same reconfirmation IS
		// wire content (wireChanged=true) and journals normally, so it still
		// propagates as a delta rather than waiting for the forced full.
		t.markSnapDirtyNoJournalLocked(entry.Identity)
		if wireChanged {
			t.markRouteChangedLocked(entry.Identity)
		}
	}
	// Phase 2 health bookkeeping at the live-update boundary
	// (review concerns P1#1 / P1#2 / 11.21 P2#1):
	//
	//   1. Resurrection eviction. When admission accepts a live
	//      claim and a stale Bad/Dead health entry survives for
	//      the same (Identity, Uplink) pair, drop it so the next
	//      Lookup does not filter the fresh claim out under the
	//      stale "do-not-select" label. Good/Questionable entries
	//      are preserved — only the labels that exclude the pair
	//      from selection need to be cleared.
	//   2. New-pair ensure. After step 1, if no health entry
	//      exists for the pair (cold start OR just-evicted Bad/
	//      Dead), create a fresh Questionable entry. Without this
	//      step newly accepted routes would stay nil-health
	//      forever: invisible to fetchRouteHealth, never aged by
	//      TickHealth, and never picked up by the probe-sender
	//      ticker (which iterates HealthSnapshot for Questionable
	//      pairs). The documented verify-before-trust contract
	//      (docs/routing.md "Capability gating") reduces to "the
	//      pair waits in Questionable for the next 30s probe tick"
	//      once the entry exists.
	//
	// PR 11.21 P2#1 gate: liveness reconfirmation runs only when
	// ApplyUpdate actually mutated storage. RouteAccepted always
	// mutates (admission, SeqNo bump, or hops improvement);
	// RouteUnchanged is conditional — ApplyUpdate returns
	// (RouteUnchanged, true) for a same-SeqNo same-hops
	// reconfirmation that refreshed ExpiresAt, but it returns
	// (RouteUnchanged, false) for a same-SeqNo WORSE-hops
	// announcement that was explicitly ignored to defend against
	// path-poisoning (see route_store_mutation.go::ApplyUpdate
	// "sameHopsReconfirmation" branch). Treating the latter as a
	// fresh liveness signal would let a worse-hops announce
	// resurrect a Dead pair through the eviction branch — Lookup
	// would then select the old optimistic route again. Gating on
	// `mutated` is correct because mutated==true means storage
	// either accepted the entry or refreshed its TTL, both of
	// which are legitimate "peer is still talking about this
	// route" signals; mutated==false means storage explicitly
	// kept the prior state, so neither liveness reset nor
	// resurrection should fire.
	if mutated && !entry.IsWithdrawn() {
		existing := t.health.getLocked(entry.Identity, entry.NextHop)
		if existing != nil && (existing.Health == HealthBad || existing.Health == HealthDead) {
			t.health.evictUplinkLocked(entry.Identity, entry.NextHop)
			existing = nil
		}
		if existing == nil {
			// Seed the fresh / just-evicted entry as
			// HealthQuestionable rather than HealthGood. Good is
			// reserved for confirmed reachability (hop_ack or
			// probe_ack(reachable=true)) — see RouteHealth's type
			// doc on the Good constant. Seeding Good on every
			// accepted announce would let a peer-attested claim
			// (an Announcement source frame, including a
			// route_query_response_v1) bypass local
			// verify-before-trust and earn the full composite-
			// score bonus for up to one passive idle window
			// (60s) without any probe round-trip. Starting at
			// Questionable means the probe loop picks the pair
			// up on its next 30 s tick, sends a probe, and
			// either confirms (→ Good) or escalates failures
			// (→ Bad). The Questionable ranking penalty
			// (scoreHealthQPenalty) is intentional: it ensures an
			// unverified new uplink doesn't outrank an existing
			// Good alternative until the probe loop has cleared
			// it. PR 11.35 P2 sized the penalty to enforce
			// strict-tier ordering, so the new pair is below
			// every Good alternative regardless of its hop count
			// or RTT.
			state := t.health.ensureLocked(entry.Identity, entry.NextHop, t.clock())
			state.Health = HealthQuestionable
		}
	}
	return status, nil
}

// IdentitiesViaUplink returns the destination identities that have
// an active (non-withdrawn, non-expired) claim through the given
// uplink. The result is a defensive copy — caller may mutate freely.
//
// Used by the Phase 4 13.3 poison-reverse emit hooks to snapshot
// the set of identities reachable via a soon-to-be-removed uplink,
// so the node can emit route_poison_v1 about each of them to its
// OTHER direct peers immediately after the uplink loss. Without
// this snapshot the post-removal storage no longer carries the
// information (InvalidateAllVia marks claims withdrawn and the
// caller has no list of affected identities — only a count).
//
// Locking: takes t.mu.RLock for the bucket walk; the result is
// safe to use outside the lock because it carries value-typed
// PeerIdentity entries (no aliasing into storage).
func (t *Table) IdentitiesViaUplink(uplink PeerIdentity) []PeerIdentity {
	if uplink.IsZero() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := t.clock()
	var out []PeerIdentity
	for identity, bucket := range t.store.buckets {
		for i := range bucket {
			c := &bucket[i]
			if c.Uplink != uplink {
				continue
			}
			if c.IsWithdrawn() || c.IsExpired(now) {
				continue
			}
			out = append(out, identity)
			break // only need to add each identity once
		}
	}
	return out
}

// InvalidateUplinkClaim is the Phase 4 13.3 poison-reverse
// counterpart of WithdrawRoute: it withdraws the storage claim for
// the (identity, uplink) pair without any origin or wire-SeqNo
// plumbing. The receive handler for a route_poison_v1 frame knows
// only the identity (from the frame payload) and the uplink (the
// session-level peer that delivered the frame), so the SeqNo is
// synthesised here as `current + 1` to keep the withdrawal
// monotonic against the existing claim's resurrection guard.
//
// Idempotency. The lookup is "live claim only" (peekLiveUplinkSeqLocked
// skips withdrawn tombstones and expired claims), so a duplicate
// poison from the same peer for the same identity returns false
// without touching storage, without bumping SeqNo, and without
// publishing a fresh route-change event. This matters because a
// non-idempotent poison would let any peer inflate the tombstone
// SeqNo by sending the same frame repeatedly — the legitimate
// recovery announce uses the origin's native SeqNo and could end
// up ranking below the inflated tombstone and be rejected by the
// resurrection-guard. The idempotent path keeps the SeqNo space
// bounded by the actual claim lifecycle, not by how many duplicate
// poison frames an upstream peer happens to emit.
//
// Trust budget (overview §4.2): poison invalidates ONLY the
// (identity, uplink) slot the sender owns. Other uplinks for the
// same identity are untouched (the route is still reachable through
// them at their own claim's SeqNo), and the origin's claim — which
// flows via per-Identity SeqNo monotonicity — is not affected. This
// is the structural reason a misbehaving peer cannot use poison to
// spoof an origin withdrawal.
//
// Returns whether the storage actually mutated. The Table.dirty
// flag is flipped on success so the snapshot publisher refreshes
// (a poisoned slot may expose a surviving backup uplink that the
// next Lookup will now prefer).
func (t *Table) InvalidateUplinkClaim(identity, uplink PeerIdentity) bool {
	if identity.IsZero() || uplink.IsZero() {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	seqNo, ok := t.store.peekLiveUplinkSeqLocked(identity, uplink, now)
	if !ok {
		// No LIVE claim for this (identity, uplink) — either we
		// never learned about it, it expired via TTL, or a prior
		// poison already tombstoned it. Returning false short-
		// circuits the side effects (drain, log, route-change
		// event) for the no-op path, which makes repeated
		// route_poison_v1 frames idempotent at the storage
		// layer — see the idempotency note above.
		return false
	}
	// Origin field on the WithdrawTriple key is preserved for
	// LastIngressOrigin / SeenOriginSeqs bookkeeping only — storage
	// dedup is per-(Identity, Uplink) after Phase A. Using uplink
	// as origin matches the sender-originated semantic the poison
	// sender attests to.
	key := RouteTriple{Identity: identity, Origin: uplink, NextHop: uplink}
	mutated := t.store.WithdrawTriple(key, seqNo+1, now)
	if mutated {
		t.dirty.Store(true)
		t.markSnapDirtyLocked(identity)
	}
	return mutated
}

// WithdrawRoute marks a specific route as withdrawn by setting hops to
// HopsInfinity. This should be called when processing an incoming withdrawal
// from the wire (hops=16, incremented SeqNo). For local peer disconnects,
// use RemoveDirectPeer instead.
//
// Returns true if the withdrawal was applied.
//
// A withdrawal for the zero (absent) identity is never legitimate: it
// would tombstone a claim under the zero key, polluting the snapshot and
// TotalEntries without ever corresponding to a real route. It is rejected
// before any mutation — symmetric with UpdateRoute, where RouteEntry.Validate
// rejects a zero Identity.
func (t *Table) WithdrawRoute(identity, origin, nextHop PeerIdentity, seqNo uint64) bool {
	if identity.IsZero() {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := RouteTriple{Identity: identity, Origin: origin, NextHop: nextHop}
	mutated := t.store.WithdrawTriple(key, seqNo, t.clock())
	if mutated {
		t.dirty.Store(true)
		t.markSnapDirtyLocked(identity)
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
	if t.localOrigin.IsZero() {
		return AddDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity.IsZero() {
		return AddDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	penalized := t.flap.isInHoldDownLocked(peerIdentity, now)
	entry, mutated := t.store.AdmitDirectPeer(peerIdentity, now)
	if mutated {
		t.dirty.Store(true)
		// Only the storage mutation changes the route projection; the
		// health-only resets below affect snap.Health (still fully
		// copied), not the incremental Routes map.
		t.markSnapDirtyLocked(peerIdentity)
	}

	// PR 11.26 P2#3 — health seeding for direct peers. UpdateRoute's
	// admission path seeds RouteHealthState for every fresh claim
	// (see UpdateRoute's PR 11.9 P1#1 / 11.21 P2#1 health
	// bookkeeping), but AdmitDirectPeer bypasses UpdateRoute and
	// would leave the new direct claim with NO health entry —
	// invisible to fetchRouteHealth observability, never aged by
	// TickHealth, never picked up by the probe-sender ticker.
	//
	// For Direct, the session establishment IS the confirmation —
	// we just completed the handshake. Seed Health=Good rather than
	// Questionable (the verify-before-trust seed used for
	// announcement-only claims): the Phase 2 probe loop should not
	// fire probes against a direct peer because the session
	// liveness is observable through ping/pong already. Confirmed
	// stays false here because we have not received any organic
	// hop_ack on this pair yet — applyHopAck / applyProbeAck flip
	// the flag when actual reachability evidence arrives.
	//
	// The keying pair (Identity == Uplink == peerIdentity) matches
	// AdmitDirectPeer's storage shape (Direct claims always have
	// NextHop = Identity by RouteEntry.Validate). The health reset
	// is gated to preserve the connect-storm fast-path
	// (TestAddDirectPeerIdempotentDoesNotMarkDirty in
	// table_dirty_test.go): on an idempotent re-add with health
	// already Good and no probe failures the reset is a true
	// no-op, so we neither mutate the published Snapshot.Health
	// nor mark dirty. Reset (and dirty-mark) fire on:
	//
	//   - mutated == true: AdmitDirectPeer admitted a new claim
	//     or reactivated a tombstone. New session ⇒ inherit
	//     historical degraded health from the prior session would
	//     start the new live connection pre-degraded.
	//   - existing == nil: first-ever ensure (cold start).
	//   - existing.Health != HealthGood OR existing.ProbeFailures > 0:
	//     defensive — should be unreachable in production because
	//     TickHealth skips Direct pairs (PR 11.27 P2#2), but covers
	//     paths like ForceHealthForTest in test fixtures.
	//
	// RTT EWMA is preserved across the reset because per-pair
	// latency does not change across a quick reconnect — losing
	// it would re-anchor the EWMA at zero on the next sample.
	// Confirmed is preserved too: it is monotonic-up by design
	// (see RouteHealthState.Confirmed doc-comment), and a peer
	// that proved reachability on the prior session stays
	// "historically confirmed" across a session boundary. New
	// hop_ack / probe_ack evidence on the new session is what
	// would have flipped it anyway.
	//
	// PR 11.30 P3 — LastHopAck preservation. We deliberately do
	// NOT stamp LastHopAck=now in the reset path. The combination
	// of (Confirmed preserved) + (LastHopAck=now) would emit a
	// synthetic fresh last_hop_ack on the wire (fetchRouteHealth
	// gates emission on Confirmed; see routing_commands.go and
	// the Confirmed flag doc in health.go), claiming a real ack
	// timestamp that never happened on the new session. Keeping
	// the historical LastHopAck means: for a never-confirmed pair
	// the RPC still omits the field (gated by Confirmed=false);
	// for a historically-confirmed pair the RPC shows the actual
	// last real-evidence timestamp, which may legitimately be
	// older than the current session. The session-start moment
	// IS recorded — TransitionAt below — so operators have a
	// distinct "session bounced at T" signal separate from
	// "last real hop_ack at T". TickHealth skips Direct pairs
	// (PR 11.27 P2#2), so a stale LastHopAck never decays the
	// pair on the passive timeline.
	existing := t.health.getLocked(peerIdentity, peerIdentity)
	needsReset := mutated || existing == nil ||
		existing.Health != HealthGood || existing.ProbeFailures > 0
	if needsReset {
		state := t.health.ensureLocked(peerIdentity, peerIdentity, now)
		state.Health = HealthGood
		state.TransitionAt = now
		state.ProbeFailures = 0
		// LastHopAck intentionally NOT reset — see doc-comment
		// above. For a fresh ensureLocked (existing==nil) the
		// store already stamps LastHopAck=now as a placeholder
		// (used internally as an idle-timer reference; gated out
		// of the wire by Confirmed=false). For an existing
		// entry, the historical timestamp survives.
		// State change is part of the published Snapshot.Health,
		// so the snapshot publisher needs to refresh. mutated==true
		// already marked dirty above; the health-only reset paths
		// (idempotent storage + degraded health) need the explicit
		// mark too.
		t.dirty.Store(true)
	}

	// Black-hole cooldown clear on session establishment. The health
	// reset above deliberately preserves reputation fields, but an
	// armed CooldownUntil for the (peer, peer) direct pair would keep
	// the just-confirmed route hidden from Lookup / fetchRouteLookup
	// (the PR 12.4 filter has NO Direct exemption) for up to
	// BlackHoleCooldown after the reconnect. The completed handshake
	// is positive liveness evidence on par with an organic late
	// hop-ack (applyHopAckSuccess), so the arm is lifted here. See
	// applySessionEstablishedLocked for the full contract; the
	// grace-window reconnect path that skips AddDirectPeer entirely
	// gets the same clear via Table.ClearDirectPairCooldown.
	if state := t.health.getLocked(peerIdentity, peerIdentity); state != nil {
		if state.applySessionEstablishedLocked(now) {
			t.dirty.Store(true)
			// Lifting the armed cooldown un-filters the direct pair from the
			// announce projection. In the idempotent reconnect path storage did
			// not mutate (no markSnapDirtyLocked), so journal the change
			// explicitly — the symmetric path Table.ClearDirectPairCooldown
			// already does. (Direct pair: identity == uplink == peerIdentity.)
			t.markRouteChangedLocked(peerIdentity)
		}
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
	if t.localOrigin.IsZero() {
		return RemoveDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity.IsZero() {
		return RemoveDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.flap.recordWithdrawalLocked(peerIdentity, now)
	withdrawals, transitInvalidated, affected, exposed := t.store.InvalidateAllVia(peerIdentity, now)
	// Eagerly evict the health states backing the just-invalidated routes
	// (identity, uplink=peerIdentity). Same reasoning as InvalidateTransitRoutes:
	// without this they linger for a whole tombstone TTL until CompactExpired +
	// reconcileHealthLocked, accumulating O(identities) stale entries per
	// departed peer under churn.
	if t.health != nil {
		for _, identity := range affected {
			t.health.evictUplinkLocked(identity, peerIdentity)
		}
	}
	// The peer is gone as a RECEIVER too: drop its per-receiver outbound
	// SeqNo watermarks so they cannot leak (it is never emitted to again;
	// a reconnect gets a fresh full sync). This is the safe, lifecycle
	// bound for the (•, peer) half of outboundPeerMax.
	t.store.forgetReceiverLocked(peerIdentity)

	// Always dirty: recordWithdrawalLocked mutated flap state
	// (withdrawTimes at minimum, possibly hold-down arming) and
	// InvalidateAllVia rewrites every affected route's
	// Hops/SeqNo/ExpiresAt. Both are part of the published Snapshot.
	t.dirty.Store(true)
	// The snapshot needs a full re-copy (InvalidateAllVia rewrote every affected
	// route's Hops/SeqNo/ExpiresAt and may have shrunk buckets), but the affected
	// identity set IS known, so journal those identities precisely instead of a
	// bulk reset. A bulk reset would force every cursor-mode peer into a
	// rate-limited full sync on each direct-peer disconnect — delaying the
	// withdrawal and defeating the delta savings under churn. `affected` is the
	// complete set of destinations reachable through the lost uplink (peer's own
	// direct claim included; exposed backups are a subset), i.e. exactly the
	// identities whose announce projection changed.
	t.markSnapshotFullDirtyLocked()
	for _, identity := range affected {
		t.markRouteChangedLocked(identity)
	}

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
	if peerIdentity.IsZero() {
		return 0, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	invalidated, affected, exposed := t.store.InvalidateTransitVia(peerIdentity, t.clock())
	if invalidated > 0 {
		// Eagerly evict the health states backing the just-tombstoned transit
		// claims (identity, uplink=peerIdentity). Without this they linger until
		// the tombstone's TTL elapses AND CompactExpired physically removes the
		// claim AND reconcileHealthLocked runs (it only fires on totalRemoved>0)
		// — so under peer churn the health store accumulates O(identities) stale
		// entries per departed peer for a whole TTL window (observed as the
		// dominant time-growing heap allocator). Evicting now is consistent with
		// reconcileHealthLocked's own intent: a later announce that resurrects
		// the (Identity, Uplink) pair must not be filtered by a stale Bad/Dead
		// label, so it gets a fresh placeholder via ensureLocked.
		if t.health != nil {
			for _, identity := range affected {
				t.health.evictUplinkLocked(identity, peerIdentity)
			}
		}
		t.dirty.Store(true)
		// Snapshot needs a full re-copy, but the affected set is known — journal
		// it precisely so cursor-mode peers get a targeted delta rather than a
		// rate-limited full sync (see the RemoveDirectPeer rationale).
		t.markSnapshotFullDirtyLocked()
		for _, identity := range affected {
			t.markRouteChangedLocked(identity)
		}
	}
	return invalidated, exposed
}

// RefreshRoutesVia renews the TTL of every live route learned through `via`,
// as if `via` had just re-announced them. It is the receiver half of the
// digest-as-heartbeat freshness mechanism: when an inbound
// route_sync_digest_v1 matches our via-peer view (handleRouteSyncDigest), the
// routes through that peer are provably current, so we extend their lifetime
// without the peer shipping a full announce. Returns the number of claims
// refreshed (0 when the peer offers no learned routes).
//
// No dirty mark / journal: ExpiresAt is not a wire-projected field, so a TTL
// bump must not synthesise an announce delta — see refreshViaLocked.
func (t *Table) RefreshRoutesVia(via PeerIdentity, now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.store.refreshViaLocked(via, now)
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
	totalRemoved, affected, exposed := t.store.CompactExpired(now)
	// Snapshot exposes both Routes and FlapState. Mark dirty if
	// either changed; a no-op tick (nothing expired and no flap-state
	// cleanup) must not force a republish.
	seqHoldReleased, flapMutated := t.flap.tickLocked(now)
	// Phase 3: each identity whose SeqNo flap-cap hold-down was released
	// re-appears in the announce projection, so journal the change. (If an
	// AnnounceTo on the announce path cleared it first, this set is empty for
	// it — whichever path clears it journals it.)
	for _, id := range seqHoldReleased {
		t.markRouteChangedLocked(id)
	}
	// Phase 2 health reconciliation: when CompactExpired physically
	// removes a claim, the matching RouteHealthState entry is no
	// longer backed by storage. Without the reconciliation step,
	// a future announce that resurrects the same (Identity, Uplink)
	// pair could be filtered out of Lookup by a stale Bad/Dead
	// health label — see docs/protocol/route_health.md
	// §4.7 Resolved decision #6 (tight sync). Reconciliation runs
	// only when storage actually removed something so the common
	// no-op tick stays cheap.
	if totalRemoved > 0 {
		t.reconcileHealthLocked()
	}
	// Phase 3 PR 12.5: reap stale digest snapshots on the same
	// cadence as the route TTL sweep. Entries that aged past
	// SessionDigestCacheTTL without a reconnect are dead weight;
	// keeping them lets the cache grow unboundedly across a long
	// outage where the peer view has likely drifted anyway. The
	// helper is a no-op when the cache is empty, so steady-state
	// nodes pay nothing.
	t.pruneExpiredDigestSnapshotsLocked(now)
	// Collapse outbound-SeqNo state back to the live working set on the
	// same cadence: evict the outboundContent reuse cache (dead / TTL-aged
	// / gone-identity shapes) and drop outboundPeerMax watermarks whose
	// destination identity is fully gone. Without this they grew per
	// distinct wire-content shape over the node's lifetime — the v1.0.47
	// cluster-mesh memory growth. (Disconnected RECEIVERS are handled
	// separately, on RemoveDirectPeer via forgetReceiverLocked, not here —
	// outboundPeerMax is a high-water watermark and must not be TTL-aged.)
	// Pure maintenance: no route state changes, so it does not affect the
	// dirty flag.
	t.store.pruneOutboundCachesLocked(now)
	if totalRemoved > 0 || flapMutated {
		t.dirty.Store(true)
	}
	// CompactExpired removed claims (dropped buckets / shrunken lengths), so the
	// published Snapshot needs a full incremental re-copy. But the affected
	// identity set IS known, so journal those precisely instead of a journal bulk
	// reset — a bulk reset would force every cursor-mode peer into a rate-limited
	// full sync on every TTL sweep that evicts anything, defeating the delta
	// savings under churn (the dominant cost in the alloc profile). flap-only
	// ticks do not change the Routes projection, so they do not re-copy.
	if totalRemoved > 0 {
		t.markSnapshotFullDirtyLocked()
		for _, identity := range affected {
			t.markRouteChangedLocked(identity)
		}
	}
	return TickTTLResult{Exposed: exposed, Removed: totalRemoved}
}

// reconcileHealthLocked drops every RouteHealthState entry whose
// (Identity, Uplink) pair has no remaining claim in storage.
// Invoked after operations that physically remove claims
// (TickTTL.CompactExpired). The walk is O(N_health) per call but
// only fires after a non-empty CompactExpired pass; for steady-
// state nodes (no expirations on a tick) the cost is zero.
//
// Caller must hold t.mu in W mode.
func (t *Table) reconcileHealthLocked() {
	if t.health == nil || len(t.health.states) == 0 {
		return
	}
	for key := range t.health.states {
		if _, idx := t.store.findByUplinkLocked(key.Identity, key.Uplink); idx < 0 {
			delete(t.health.states, key)
		}
	}
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
