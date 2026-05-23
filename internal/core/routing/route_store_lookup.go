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
// Caller must hold t.mu (reader OK).
func (s *routeStore) AnnounceableFor(excludeVia PeerIdentity, now time.Time) []RouteEntry {
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
func (s *routeStore) AnnounceProjectionFor(excludeVia PeerIdentity, now time.Time) ([]AnnounceEntry, bool) {
	origin := s.localOrigin

	var result []AnnounceEntry
	mutated := false
	for identity, bucket := range s.buckets {
		if identity == excludeVia {
			continue
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
			continue
		}

		// emitOrigin defaults to localOrigin (Phase A migration
		// contract — sender-originated semantic, same for live
		// and withdrawal so pre-A1 receivers track them under one
		// (Identity, sender, sender) triple). Empty fallback to
		// identity matches toRouteEntry, keeping test fixtures
		// without WithLocalOrigin from emitting Origin == "".
		emitOrigin := origin
		if emitOrigin == "" {
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
				mutated = true
				continue
			}
			result = append(result, AnnounceEntry{
				Identity: identity,
				Origin:   emitOrigin,
				Hops:     int(liveWinner.Hops),
				SeqNo:    wireSeqNo,
				Extra:    liveWinner.Extra,
			})
			// Live winner suppresses own-direct tombstone emits
			// for this Identity on the wire — see the live-vs-
			// tombstone exclusivity paragraph in this function's
			// doc-comment for the ordering-hazard rationale.
			// Storage retains the tombstone (it still gates the
			// SeqNo-resurrection invariant for THIS uplink in
			// ApplyUpdate); we just don't advertise it alongside
			// a live alternative.
			continue
		}
		for _, tomb := range tombstones {
			sig := outboundEmitSig{
				Uplink:    tomb.Uplink,
				Hops:      tomb.Hops,
				Withdrawn: true,
				ExtraSig:  string(normalizeExtra(tomb.Extra)),
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
			})
		}
	}
	return result, mutated
}

// isBetterLiveClaim returns true when `cand` should replace
// `incumbent` as the per-Identity live winner during
// AnnounceProjectionFor's collapse. Mirrors
// isBetterAnnounceEntry's ordering in announce_builder.go:
// min Hops → higher SeqNo → Extra tie-break (compareExtra >
// 0 wins) → deterministic Uplink lex tie-break. Extra
// participates because it is part of the wire content
// (RouteEntry.Extra is forward-compatible relay data), and
// different Extra values must produce a deterministic winner
// so the assigned outbound SeqNo is stable across cycles.
// Uplink takes the role of Origin in the AnnounceEntry-level
// helper — both are deterministic stable identifiers of the
// claim's source.
func isBetterLiveClaim(cand, incumbent *UplinkClaim) bool {
	if cand.Hops != incumbent.Hops {
		return cand.Hops < incumbent.Hops
	}
	if cand.SeqNo != incumbent.SeqNo {
		return cand.SeqNo > incumbent.SeqNo
	}
	if cmp := compareExtra(cand.Extra, incumbent.Extra); cmp != 0 {
		return cmp > 0
	}
	return cand.Uplink < incumbent.Uplink
}
