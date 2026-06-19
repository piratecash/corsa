package routing

import "time"

// This file hosts the read-side storage projections: targeted
// (Identity, Origin, NextHop) inspection (InspectTriple), per-
// identity active-route lookup (LookupActive), and the two
// neighbor-facing projection helpers used by AnnounceLoop
// (AnnounceableFor for the routing-domain shape,
// AnnounceProjectionFor for the wire-shape).
//
// All four operate on per-(Identity, Uplink) UplinkClaim
// storage and synthesise the boundary RouteEntry / AnnounceEntry
// shape on the fly. Per the Phase A migration contract every
// synthesised value carries Origin = localOrigin (with fallback
// to Identity when localOrigin is empty), matching the wire-
// emit shape that pre-A1 receivers expect for own-direct
// withdrawal anti-spoof. The phase-1 doc originally proposed
// Origin = Identity here, but switching to localOrigin lets
// live and withdrawal frames land on the same
// (Identity, Origin=sender, NextHop=sender) triple on legacy
// receivers — see uplink_claim.go for the full rationale.

// announceDigestEntriesForLocked returns the (Identity, lastWireSeqNo)
// pairs this store has announced to `peer` and would STILL announce —
// i.e. the per-peer outbound view the peer currently holds as its
// "routes via this node". It is the sender-side counterpart of a
// receiver's SyncDigestFor(thisNode): when the two nodes are in sync the
// sets are identical, which is exactly the digest-match the Phase 3
// reconnect optimisation needs.
//
// Why outboundPeerMax (not claim.SeqNo): the wire SeqNo the receiver
// stored for (Identity, via=thisNode) is the per-peer synthesised value
// from nextOutboundSeqLockedPerPeer, recorded in outboundPeerMax[
// (Identity, peer)] — NOT the local claim's native SeqNo (which is the
// upstream uplink's wire value, a different number at each hop). Reading
// outboundPeerMax reproduces precisely what the peer stored, so the two
// digests line up. Using the via-peer set (SyncDigestFor(peer)) here
// instead — as the pre-fix code did — compared two disjoint sets
// (routes-via-peer vs routes-the-peer-has-via-us) and so almost never
// matched in a normal topology.
//
// An identity is included only when it is still announceable to `peer`
// (a live, non-Dead, non-cooled winner via some uplink != peer exists):
// a since-withdrawn or gone-Dead route is no longer part of what the
// peer should have, so dropping it makes the digest mismatch and the
// reconnect safely falls back to a full sync.
//
// Read-only: no SeqNo synthesis, no cache mutation, no hold-down arming.
// Caller must hold t.mu (R mode suffices).
func (s *routeStore) announceDigestEntriesForLocked(peer PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool) []DigestEntry {
	var entries []DigestEntry
	for key, entry := range s.outboundPeerMax {
		if key.Peer != peer {
			continue
		}
		identity := key.Identity
		if identity == peer {
			// Split-horizon: never digest "reach the peer via itself".
			continue
		}
		if !s.hasAnnounceableWinnerLocked(identity, peer, now, isDead, isCooledDown) {
			continue
		}
		entries = append(entries, DigestEntry{Identity: identity, MaxSeqNo: entry.seq})
	}
	return entries
}

// hasAnnounceableWinnerLocked reports whether the store currently holds a
// live winner for `identity` reachable via some uplink OTHER than
// `excludeVia` that would survive the announce filters (not withdrawn,
// not expired, not Dead unless Direct, not cooled-down). It mirrors the
// candidate-eligibility rules of AnnounceProjectionFor without performing
// the winner ranking or any mutation — it only answers "would we still
// announce this identity to excludeVia at all".
//
// Caller must hold t.mu (R mode suffices).
func (s *routeStore) hasAnnounceableWinnerLocked(identity, excludeVia PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool) bool {
	bucket, ok := s.buckets[identity]
	if !ok {
		return false
	}
	for i := range bucket {
		claim := &bucket[i]
		if claim.Uplink == excludeVia {
			continue
		}
		if claim.IsWithdrawn() || claim.IsExpired(now) {
			continue
		}
		// Dead filter with the same Direct exemption AnnounceProjectionFor
		// applies: a Dead transit claim is excluded, but a Dead Direct
		// claim stays announceable (its lifecycle is session-bound).
		if isDead != nil && claim.Source != RouteSourceDirect && isDead(identity, claim.Uplink) {
			continue
		}
		if isCooledDown != nil && isCooledDown(identity, claim.Uplink) {
			continue
		}
		return true
	}
	return false
}

// LookupActive returns all non-withdrawn, non-expired claims for
// the given identity, projected back to RouteEntry in storage
// order. The owning Table layers preference sorting plus the
// synthetic local-route injection on top — those policies live
// on Table because the synthetic self-route is a routing-domain
// invariant ("you can always reach yourself"), not storage state.
//
// Caller must hold t.mu (reader OK).
func (s *routeStore) LookupActive(identity PeerIdentity, now time.Time) []RouteEntry {
	bucket := s.buckets[identity]
	var result []RouteEntry
	for _, claim := range bucket {
		if !claim.IsWithdrawn() && !claim.IsExpired(now) {
			result = append(result, toRouteEntry(identity, s.localOrigin, claim))
		}
	}
	return result
}

// InspectTriple returns a value-copy of the claim matching the
// (Identity, NextHop=Uplink) projection of key, synthesised as a
// boundary RouteEntry, or nil when no claim exists. The
// key.Origin component is preserved on the public API for
// backward compatibility with Table.InspectTriple callers but is
// ignored internally — after Phase A the dedup key is per-
// (Identity, Uplink).
//
// Unlike LookupActive, InspectTriple does NOT filter by
// withdrawn/expired status — callers see the claim exactly as
// stored. Intended for diagnostics (e.g. explaining why
// ApplyUpdate rejected an incoming entry).
//
// Returns a copy rather than the alias so callers cannot
// accidentally mutate storage via the pointer; same defensive
// convention as the pre-routeStore Table.InspectTriple. Caller
// must hold t.mu (reader OK).
func (s *routeStore) InspectTriple(key RouteTriple) *RouteEntry {
	bucket, idx := s.findByUplinkLocked(key.Identity, key.NextHop)
	if idx < 0 {
		return nil
	}
	dup := toRouteEntry(key.Identity, s.localOrigin, bucket[idx])
	return &dup
}

// AnnounceableFor returns routes suitable for announcing to a
// specific peer, applying:
//
//   - split horizon: claim.Uplink == excludeVia → skip (would
//     echo what the peer just told us);
//   - own-identity filter: identity == excludeVia → skip (the
//     peer would always reject a route announce naming themself
//     as the destination — degenerate "you can reach yourself
//     via me" claim — and on pre-A1 receivers this also avoids
//     the own-origin anti-spoof trip when receiver.localOrigin
//     happens to equal Identity).
//
// Withdrawn and expired claims are also excluded — Announceable
// is the routing-domain projection, not the wire-shape; wire
// withdrawal retries go through AnnounceProjectionFor.
//
// We do NOT send fake hops=16 withdrawals from this method —
// that would violate the per-origin SeqNo invariant.
//
// Phase 2 health filter (PR 11.23 P2 / 11.24 P2#1 / 11.25): when
// an `isDead` callback is supplied, transit claims (Source !=
// RouteSourceDirect) whose (Identity, Uplink) pair is currently
// marked HealthDead are suppressed from the result. A locally-
// Dead transit route stops being advertised so neighbours do not
// keep refreshing a path we already gave up on.
//
// RouteSourceDirect claims are EXEMPT from the filter — their
// lifecycle is session-driven (AddDirectPeer / RemoveDirectPeer
// emit real wire withdrawals on disconnect), and a HealthDead
// label on a Direct claim signals "no organic hop_ack lately",
// not "route is gone". Silently dropping a Direct claim from
// announce would violate the own-origin withdrawal contract on
// the HealthDead constant (health.go): neighbours would keep
// the prior live route until TTL (~120 s) instead of seeing an
// authoritative withdrawal. The symmetric exemption applies on
// the Lookup-side filter (table_lookup.go) — local forwarding
// and outbound announce semantics must agree on Direct routes.
// Pass nil to skip filtering (test fixtures, pre-Phase-2 callers).
//
// Caller must hold t.mu (reader OK); isDead is invoked under
// that lock and must not block on it.
func (s *routeStore) AnnounceableFor(excludeVia PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool) []RouteEntry {
	var result []RouteEntry
	for identity, bucket := range s.buckets {
		if identity == excludeVia {
			continue
		}
		for _, claim := range bucket {
			if claim.Uplink == excludeVia {
				continue
			}
			if claim.IsWithdrawn() || claim.IsExpired(now) {
				continue
			}
			// PR 11.24 P2#1: Direct (own-origin) claims are
			// EXEMPT from the Phase 2 Dead filter. Direct routes
			// have ExpiresAt=zero by design and their lifecycle is
			// driven by AddDirectPeer / RemoveDirectPeer — the
			// session-disconnect path emits a real wire withdrawal
			// (hops=16 + SeqNo bump). The HealthDead label on a
			// Direct claim signals "no organic hop_ack traffic has
			// flowed lately", NOT "the route is gone"; the session
			// is still alive and we know our own connectivity.
			// Suppressing the announce here would let neighbours
			// keep the prior live route until TTL (≈120 s) while
			// we silently stopped advertising — that is NOT the
			// withdrawal semantic the HealthDead contract promises
			// for own-origin routes. Foreign-origin (transit)
			// claims stay subject to the filter: the doc says
			// transit Dead routes converge via TTL expiry or
			// origin's own withdrawal, which is exactly what
			// suppressing the announce accomplishes for them.
			if claim.Source != RouteSourceDirect && isDead != nil && isDead(identity, claim.Uplink) {
				continue
			}
			// Phase 3 PR 12.4: black-hole cooldown filter. Unlike
			// the Dead filter above, the cooldown gate applies to
			// every Source — including Direct. Reputation's 5
			// consecutive failures already recorded the empirical
			// "this peer cannot deliver" signal; advertising the
			// route while we ourselves know it is a black hole
			// would set neighbours up to forward through us into
			// the same dead-end. The cooldown auto-clears after
			// BlackHoleCooldown so the suppression is self-
			// healing — unlike Dead which sticks until external
			// confirmation arrives.
			if isCooledDown != nil && isCooledDown(identity, claim.Uplink) {
				continue
			}
			result = append(result, toRouteEntry(identity, s.localOrigin, claim))
		}
	}
	return result
}

// AnnounceProjectionFor returns the wire-safe AnnounceEntry
// projection of claims to announce to a specific peer, applying
// split horizon, own-identity filtering, the own-origin tombstone
// passthrough rule, the per-Identity live-winner selection, AND
// the Phase 1 P2 SeqNo flap-cap hold-down filter (Identities whose
// outbound-SeqNo advance rate crossed MaxSeqAdvancePerWindow inside
// SeqAdvanceWindow are skipped for DefaultSeqHoldDownDuration; see
// flap.go::isInSeqHoldDownLocked).
//
// Per-Identity collapse: pre-A1 AnnounceProjectionFor emitted one
// entry per (Identity, Uplink) claim (post-Stage-3 / Stage-4 in
// announce_builder.go did the per-Identity collapse downstream).
// After A1's outbound SeqNo synthesis (route_store.go's
// outboundContent + outboundMax pair), emitting non-winning
// claims would each bump the per-Identity outbound counter —
// non-winner emits would burn SeqNo space and force every cycle's
// delta to drag a flapping SeqNo on every Identity. So
// AnnounceProjectionFor now selects the live winner per Identity
// itself (min Hops → higher SeqNo → Extra tie-break →
// deterministic Uplink lex) and emits only the winner plus any
// own-origin tombstones in the no-live-alternative case (see
// next paragraph). BuildAnnounceSnapshot's per-Identity stage
// downstream is a no-op for routing-driven inputs (it still
// defends against synthetic test-fixture inputs).
//
// Live-vs-tombstone exclusivity: when a live winner is present
// for an Identity, own-direct tombstones for the SAME Identity
// are suppressed on the wire. The live winner already overrides
// any prior wire state on the receiver (pre-A1 receivers key on
// (Identity, Origin, NextHop) with our Origin=localOrigin and
// NextHop=sender, so both live and tombstone for one Identity
// collide on a single triple slot; post-A1 receivers key on
// (Identity, Uplink) and resolve by SeqNo). Emitting BOTH would
// produce a fatal ordering hazard: nextOutboundSeqLockedPerPeer
// advances outboundPeerMax on the live emit, so the immediately
// following tombstone emit assigns a STRICTLY GREATER SeqNo than
// the live winner. The receiver's monotonic-SeqNo timeline then
// applies the tombstone last, leaving the receiver thinking the
// Identity is unreachable even though we just told them about
// the backup uplink. Tombstones are useful only when we cannot
// offer a live alternative — the receiver still needs to learn
// the prior route is dead. "Own-origin" after Phase A is
// signalled by claim.Source == RouteSourceDirect — every Direct
// claim in storage is by construction created by AddDirectPeer
// (own-origin), and Origin is no longer stored to be checked
// against localOrigin. Transit tombstones (Source != Direct) —
// withdrawals we relayed but did not originate — are always
// excluded; only the origin may emit wire withdrawals.
//
// Wire SeqNos come from nextOutboundSeqLockedPerPeer rather than
// claim.SeqNo directly: outbound SeqNo is monotonically
// non-decreasing on the receiver's (Identity, sender) timeline
// regardless of which uplink contributed the claim. Both the
// live winner and any retried own-direct tombstone emits go
// through the per-peer helper, which consults both the cross-
// peer broadcast watermark (outboundBroadcastMax) AND the
// per-receiver high water (outboundPeerMax[(Identity, excludeVia)])
// so a same-peer winner-switch A→B→A always issues a fresh
// SeqNo that the receiver will accept. The original tombstone
// broadcast happened in InvalidateAllVia (via
// nextOutboundSeqLockedBroadcast) and already advanced
// outboundBroadcastMax; this per-peer path advances only
// excludeVia's outboundPeerMax.
// See route_store.go's outboundContent / outboundBroadcastMax /
// outboundPeerMax fields and nextOutboundSeqLockedPerPeer for
// the full contract.
//
// AnnounceProjectionFor mutates outbound state (outboundContent,
// outboundMax, outboundPeerMax) so the caller MUST hold t.mu in
// WRITER mode. Table.AnnounceTo upgraded its lock to Lock for
// this reason.
//
// Returns the wire-shape entries plus a `mutated` flag that signals
// "snapshot-visible state changed during this call". Today the
// signal fires when a Phase 1 P2 SeqNo flap-cap hold-down arms
// (RouteCapStats.SeqNoFlapHoldowns bumps inside the published
// Snapshot), so Table.AnnounceTo can mark t.dirty and the snapshot
// publisher republishes the new counter value immediately —
// without the propagation the operator-facing counter would lag
// until some unrelated mutation arrived. Outbound SeqNo cache /
// watermark mutations are intentionally NOT reflected: they are
// not snapshot-visible and a republish on every cycle would be
// wasted work.
// Phase 2 health filter (PR 11.23 P2 / 11.24 P2#1 / 11.25):
// when an `isDead` callback is supplied, transit claims (Source
// != RouteSourceDirect) whose (Identity, Uplink) pair is
// currently HealthDead are excluded from the live-winner
// candidate pool. RouteSourceDirect claims are EXEMPT — Direct
// lifecycle is session-driven (AddDirectPeer /
// RemoveDirectPeer), so a HealthDead label there means "no
// organic hop_ack lately" rather than "route is gone";
// suppressing a Direct claim from announce would silently
// abandon the route on the wire (no authoritative withdrawal)
// and force neighbours to wait out TTL. The Lookup-side filter
// in table_lookup.go applies the same Direct exemption — local
// forwarding and outbound announce semantics must agree.
//
// If ALL live transit candidates for an Identity are Dead and
// no Direct candidate exists, no live winner is selected and
// the tombstone branch fires per the existing rules (own-direct
// tombstones emit; transit tombstones never emit). Pass nil to
// disable filtering — test fixtures and pre-Phase-2 callers do
// this.
func (s *routeStore) AnnounceProjectionFor(excludeVia PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool) ([]AnnounceEntry, bool) {
	origin := s.localOrigin

	// result is a pooled projection buffer (see announceEntryBufPool): it is
	// returned up through Table.AnnounceTo and MUST be handed back via
	// Table.ReleaseAnnounceEntries after BuildAnnounceSnapshot consumes it.
	result := getAnnounceEntryBuf()
	mutated := false
	for identity, bucket := range s.buckets {
		var m bool
		result, m = s.projectIdentityLocked(identity, bucket, excludeVia, origin, now, isDead, isCooledDown, result)
		mutated = mutated || m
	}
	return result, mutated
}

// projectChangedFor is the deploy-2 cursor-authoritative projection: it emits
// the wire entries for ONLY the destination identities in `changed` (the change
// journal's set since a peer's cursor), instead of sweeping every bucket. The
// per-identity body (projectIdentityLocked) is shared with AnnounceProjectionFor,
// and the per-receiver wire SeqNo for an identity depends only on that identity's
// own outbound state (outboundContent / outboundBroadcastMax / outboundPeerMax),
// never on other identities — so projecting a subset assigns the IDENTICAL SeqNo
// the full sweep would. Identities absent from s.buckets are skipped (fully gone,
// no tombstone): the full path emits nothing for them either, since ComputeDelta
// only walks present entries.
//
// Caller holds t.mu (write). Returns the pooled projection buffer (release via
// Table.ReleaseAnnounceEntries) and whether outbound SeqNo state mutated in a
// way that requires a republish.
func (s *routeStore) projectChangedFor(excludeVia PeerIdentity, changed []PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool) ([]AnnounceEntry, bool) {
	origin := s.localOrigin
	result := getAnnounceEntryBuf()
	mutated := false
	for _, identity := range changed {
		bucket, ok := s.buckets[identity]
		if !ok {
			continue
		}
		var m bool
		result, m = s.projectIdentityLocked(identity, bucket, excludeVia, origin, now, isDead, isCooledDown, result)
		mutated = mutated || m
	}
	return result, mutated
}

// projectIdentityLocked appends the wire AnnounceEntry(s) for ONE destination
// identity to result and returns the (possibly grown) slice plus whether it
// mutated outbound SeqNo state in a way that requires a republish (a SeqNo
// flap-cap hold-down engaged inside the emit). It is the shared per-identity
// body of AnnounceProjectionFor (full sweep) and projectChangedFor (journal
// subset), so both assign the identical per-receiver wire SeqNo. Caller holds
// t.mu (write).
func (s *routeStore) projectIdentityLocked(identity PeerIdentity, bucket []UplinkClaim, excludeVia, origin PeerIdentity, now time.Time, isDead, isCooledDown func(identity, uplink PeerIdentity) bool, result []AnnounceEntry) ([]AnnounceEntry, bool) {
	if identity == excludeVia {
		return result, false
	}

	// Phase 1 P2: skip Identities currently in SeqNo flap-cap
	// hold-down. Wire emit for X is suppressed to every peer for
	// the entire DefaultSeqHoldDownDuration window once the
	// per-Identity advance rate has crossed
	// MaxSeqAdvancePerWindow — the suppression is what bounds
	// the per-cycle delta dispatcher's work, the per-peer
	// announce baseline cache size, and the CPU cost of
	// BuildAnnounceSnapshot when a single Identity hot-loops.
	// Receive-side state stays accurate (ApplyUpdate keeps
	// processing legitimate ingest); only the wire-emit side is
	// gated. See flap.go::isInSeqHoldDownLocked.
	if s.flap.isInSeqHoldDownLocked(identity, now) {
		return result, false
	}

	// emitOrigin defaults to localOrigin (Phase A migration
	// contract — sender-originated semantic, same for live
	// and withdrawal so pre-A1 receivers track them under one
	// (Identity, sender, sender) triple). Empty fallback to
	// identity matches toRouteEntry, keeping test fixtures
	// without WithLocalOrigin from emitting Origin == "".
	emitOrigin := origin
	if emitOrigin.IsZero() {
		emitOrigin = identity
	}

	var liveWinner *UplinkClaim
	var tombstones []UplinkClaim
	for i := range bucket {
		claim := &bucket[i]
		if claim.Uplink == excludeVia {
			continue
		}
		if claim.IsExpired(now) {
			continue
		}
		if claim.IsWithdrawn() {
			if claim.Source == RouteSourceDirect {
				tombstones = append(tombstones, *claim)
			}
			continue
		}
		// Phase 2 health filter (PR 11.23 P2 / 11.24 P2#1):
		// skip locally-Dead transit pairs so they are not
		// re-advertised. Direct (own-origin) claims are
		// exempt — their lifecycle is session-driven via
		// AddDirectPeer / RemoveDirectPeer, which emit real
		// wire withdrawals on disconnect. Health-Dead on a
		// Direct claim is a "no recent organic hop_ack"
		// signal, NOT "route is gone", and silently dropping
		// it from announce would violate the own-origin
		// withdrawal contract on the HealthDead constant
		// (health.go) — neighbours would keep the prior live
		// route until TTL (~120 s) instead of seeing an
		// authoritative withdrawal. See AnnounceableFor for
		// the symmetric exemption.
		if claim.Source != RouteSourceDirect && isDead != nil && isDead(identity, claim.Uplink) {
			continue
		}
		// Phase 3 PR 12.4: black-hole cooldown filter, no
		// Direct exemption — mirror of the AnnounceableFor
		// branch. The arm comes from reputation's 5
		// consecutive hop-ack failures (PR 12.2 wiring) and
		// auto-clears after BlackHoleCooldown.
		if isCooledDown != nil && isCooledDown(identity, claim.Uplink) {
			continue
		}
		if liveWinner == nil || isBetterLiveClaim(claim, liveWinner) {
			w := *claim
			liveWinner = &w
		}
	}

	if liveWinner != nil {
		sig := outboundEmitSig{
			Uplink:    liveWinner.Uplink,
			Hops:      liveWinner.Hops,
			Withdrawn: false,
			ExtraSig:  string(normalizeExtra(liveWinner.Extra)),
			// Round-14 fix: include AttestedSig bytes in the
			// content cache key so a sig-only upgrade in
			// storage (reconfirmation path) produces a fresh
			// outbound SeqNo. Without this the cache hits on
			// the unsigned prior emit and the new signed
			// content ships at the burnt SeqNo, which the
			// receiver rejects as stale.
			AttestedSig: string(liveWinner.AttestedSig),
		}
		// Per-peer emit: only excludeVia's receiver state
		// advances on this AnnounceTo call. The per-peer
		// helper consults both outboundBroadcastMax (cross-
		// peer watermark) AND outboundPeerMax[(Identity,
		// excludeVia)] (this peer's last-emitted high water)
		// so a winner-switch A→B→A on the same peer's
		// timeline forces a fresh SeqNo. See
		// nextOutboundSeqLockedPerPeer's doc-comment.
		wireSeqNo, armed := s.nextOutboundSeqLockedPerPeer(identity, excludeVia, sig, liveWinner.SeqNo, liveWinner.LastIngressOrigin, now)
		if armed {
			// The advance we just recorded crossed the SeqNo
			// flap-cap threshold and engaged hold-down INSIDE
			// this call. The top-of-loop isInSeqHoldDownLocked
			// gate already passed (hold-down was not active
			// when we entered this iteration), so without this
			// extra suppression the peer that triggered the
			// engage would still receive the over-threshold
			// emit — only the NEXT AnnounceTo cycle on the
			// next peer would observe suppression. That
			// contradicts the "wire emit suppressed to EVERY
			// peer" contract documented on the SeqNo flap cap.
			//
			// Storage state (outboundContent, outboundMax,
			// outboundPeerMax) is intentionally kept past this
			// branch: when hold-down expires the cache hit on
			// the same sig will reuse the burnt SeqNo on the
			// receiver's first post-release emit, so the
			// receiver's monotonic-SeqNo timeline never goes
			// backwards and ComputeDelta stays a no-op on
			// stable content. Mark dirty so the snapshot
			// publisher republishes the SeqNoFlapHoldowns
			// bump.
			return result, true
		}
		result = append(result, AnnounceEntry{
			Identity: identity,
			Origin:   emitOrigin,
			Hops:     int(liveWinner.Hops),
			SeqNo:    wireSeqNo,
			Extra:    liveWinner.Extra,
			// Phase 4 13.2-A: carry the stored attested-links
			// signature through the projection so a v3 re-emit
			// forwards the original signer's bytes unchanged.
			// Empty for legacy/unsigned claims — the v3 build path
			// omits the wire "sig" field via omitempty.
			AttestedSig:         liveWinner.AttestedSig,
			AttestedSigVerified: liveWinner.AttestedSigVerified,
		})
		// Live winner suppresses own-direct tombstone emits
		// for this Identity on the wire — see the live-vs-
		// tombstone exclusivity paragraph in this function's
		// doc-comment for the ordering-hazard rationale.
		// Storage retains the tombstone (it still gates the
		// SeqNo-resurrection invariant for THIS uplink in
		// ApplyUpdate); we just don't advertise it alongside
		// a live alternative.
		return result, false
	}
	mutated := false
	for _, tomb := range tombstones {
		sig := outboundEmitSig{
			Uplink:    tomb.Uplink,
			Hops:      tomb.Hops,
			Withdrawn: true,
			ExtraSig:  string(normalizeExtra(tomb.Extra)),
			// Round-14: same content-key contract as the live
			// branch above — see liveWinner comment for the
			// sig-upgrade fresh-SeqNo rationale.
			AttestedSig: string(tomb.AttestedSig),
		}
		// Per-peer tombstone retry: announce-delta withdrawal
		// redelivery to a peer where the original
		// RemoveDirectPeer fanout did not land. The original
		// broadcast advanced outboundBroadcastMax; this
		// per-peer retry advances only excludeVia's
		// outboundPeerMax. Only reached when no live winner
		// exists for this Identity — otherwise the live
		// winner's emit above already informs the receiver
		// of the current state via a strictly newer SeqNo,
		// and trailing a tombstone behind it would invert
		// the receiver's view (see doc-comment).
		wireSeqNo, armed := s.nextOutboundSeqLockedPerPeer(identity, excludeVia, sig, tomb.SeqNo, tomb.LastIngressOrigin, now)
		if armed {
			// Same threshold-crossing suppression as the
			// live-winner branch above: the peer whose emit
			// engaged the hold-down must NOT receive the
			// over-threshold frame, otherwise the
			// "suppressed to EVERY peer" contract is broken
			// on the engage cycle itself. Break out of the
			// tombstones loop too — subsequent tombstones
			// for this Identity in this cycle are equally
			// part of the suppressed wire emit.
			mutated = true
			break
		}
		result = append(result, AnnounceEntry{
			Identity: identity,
			Origin:   emitOrigin,
			Hops:     HopsInfinity,
			SeqNo:    wireSeqNo,
			Extra:    tomb.Extra,
			// Round-13 fix: forward the stored attested-links
			// signature on tombstone projections, mirroring
			// the liveWinner branch above. Without this the
			// per-peer tombstone retry (and the full-sync
			// withdrawal redelivery this branch feeds) drops
			// the sig that route_store_mutation.go::
			// InvalidateAllVia carefully preserved on the
			// live→tombstone transition. buildRouteAnnounceV3Frame
			// would then emit the withdrawal with sig="" even
			// though storage still held the destination
			// identity's attestation — breaking the
			// attested-links forwarding contract for the
			// withdrawal half of the v3 wire stream. The
			// canonical signing bytes are (identity ||
			// extra) and exclude per-emitter wire fields
			// (hops, epoch, seq_no), so the sig stays valid
			// on the tombstone exactly the way it stayed
			// valid across the live→withdrawn transition.
			AttestedSig:         tomb.AttestedSig,
			AttestedSigVerified: tomb.AttestedSigVerified,
		})
	}
	return result, mutated
}

// isBetterLiveClaim returns true when `cand` should replace
// `incumbent` as the per-Identity live winner during
// AnnounceProjectionFor's collapse. Mirrors
// isBetterAnnounceEntry's ordering in announce_builder.go,
// Round-26 alignment:
//
//	min Hops → higher SeqNo → verified-sig (verified beats
//	unsigned) → Extra tie-break (compareExtra > 0 wins) →
//	deterministic Uplink lex tie-break.
//
// Extra participates because it is part of the wire content
// (RouteEntry.Extra is forward-compatible relay data), and
// different Extra values must produce a deterministic winner
// so the assigned outbound SeqNo is stable across cycles.
// Uplink takes the role of Origin in the AnnounceEntry-level
// helper — both are deterministic stable identifiers of the
// claim's source.
//
// Round-26: verified-sig participates AHEAD of compareExtra
// (it was AHEAD of Uplink lex in Round-25, but that still let
// an unsigned claim with lex-larger Extra beat a verified
// claim with smaller Extra — the Extra tie fired first and
// the verified comparator never ran). Lookup /
// AnnounceTargetFor already prefer verified claims via
// scoreSignedBonus (score.go) and do NOT consult Extra; the
// announce projection MUST agree, otherwise the periodic
// AnnounceTo cycle would emit the unsigned uplink purely on
// Extra lex, dropping the destination identity's attestation
// from the wire on every downstream re-emit. SeqNo stability
// across cycles is preserved because outboundEmitSig keys on
// (Uplink, Hops, Withdrawn, ExtraSig, AttestedSig): when the
// winner switches due to verified-sig flipping, the content
// key changes and a fresh SeqNo is assigned exactly as it
// would for any other content change. Uplink lex stays as the
// final deterministic tie so two equally-attested claims with
// equal Extra still converge on a stable winner.
func isBetterLiveClaim(cand, incumbent *UplinkClaim) bool {
	if cand.Hops != incumbent.Hops {
		return cand.Hops < incumbent.Hops
	}
	if cand.SeqNo != incumbent.SeqNo {
		return cand.SeqNo > incumbent.SeqNo
	}
	if cand.AttestedSigVerified != incumbent.AttestedSigVerified {
		return cand.AttestedSigVerified
	}
	if cmp := compareExtra(cand.Extra, incumbent.Extra); cmp != 0 {
		return cmp > 0
	}
	return cand.Uplink.Compare(incumbent.Uplink) < 0
}
