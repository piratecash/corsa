package routing

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/rs/zerolog/log"
)

// AnnounceSnapshot is a canonical, aggregated, peer-specific view of
// route entries ready for wire transmission. The shape contract has
// two distinct layers and the doc-comment is explicit about which
// layer owns which invariant so future work modifies the correct
// one:
//
//  1. PRODUCTION LAYER — routeStore.AnnounceProjectionFor (the
//     upstream of BuildAnnounceSnapshot for the AnnounceLoop path)
//     enforces the live-vs-tombstone exclusivity: when a live
//     winner exists for an Identity, the corresponding own-direct
//     tombstone is suppressed on the wire. See
//     route_store_lookup.go's AnnounceProjectionFor doc for the
//     receiver-timeline ordering hazard that motivates the
//     suppression (nextOutboundSeqLockedPerPeer assigns the
//     tombstone a strictly greater SeqNo than the live winner
//     under per-peer cache semantics, which would flip the
//     receiver onto a withdrawn state).
//
//  2. STRUCTURAL LAYER — BuildAnnounceSnapshot ITSELF does NOT
//     enforce exclusivity. It accepts arbitrary AnnounceEntry
//     inputs and runs deterministic per-Identity aggregation. A
//     synthetic test fixture, or any caller that bypasses
//     AnnounceProjectionFor, may feed in BOTH a live entry and a
//     tombstone for the same Identity; the snapshot will retain
//     both as independent slots (the live winner under the
//     live-collapse, the tombstone under the tombstone-collapse).
//     ComputeDelta treats them as independent because its key is
//     (Identity, IsWithdrawn) — different IsWithdrawn → different
//     delta slot — so the structural-layer pass-through is
//     consistent with the delta contract.
//
// After full aggregation, for each Identity the snapshot carries at
// most:
//
//   - ONE live winner per Identity — classical distance-vector
//     "best route per destination" semantic. Post-Phase-A storage
//     is per-(Identity, Uplink), and Table.AnnounceTo →
//     routeStore.AnnounceProjectionFor already collapses to the
//     live winner per Identity inline (min Hops → higher SeqNo →
//     Extra tie-break → deterministic Uplink lex) so this stage
//     downstream is a no-op for routing-driven inputs. It still
//     defends against synthetic test-fixture inputs that bypass
//     AnnounceProjectionFor and pass multiple live AnnounceEntry
//     values per Identity (e.g. legacy fixture seeding).
//
//   - At most ONE withdrawal tombstone (Hops >= HopsInfinity,
//     matching `RouteEntry.IsWithdrawn` semantics in types.go).
//     Tombstones serve the retry-via-delta path for withdrawals:
//     a freshly-disconnected destination with no surviving uplink
//     alternative needs to tell receivers "this destination is
//     dead" so they evict their stale own-origin bucket pointing
//     at us instead of waiting for natural TTL expiry. The
//     production layer suppresses tombstones when a live winner
//     is present for the same Identity (see point 1 above); the
//     structural layer does NOT. Future work that needs to
//     change live-vs-tombstone wire policy must change
//     AnnounceProjectionFor, not the snapshot stage — touching
//     the snapshot would silently affect synthetic test fixtures
//     and the delta contract without fixing the production
//     ordering hazard.
//
//     Per-Identity tombstone collapse. Pre-collapse the snapshot
//     used to preserve EVERY Hops>=HopsInfinity input entry
//     verbatim, but ComputeDelta keys on (Identity, IsWithdrawn)
//     — multiple tombstones per Identity in oldSnap collapse to a
//     single slot in oldIdx (last write wins) and identical
//     snapshots would then yield a non-empty delta because
//     earlier-position tombstones in newSnap fail to match the
//     surviving slot on SeqNo. The snapshot now collapses
//     tombstones per Identity with the same min-Hops → higher-
//     SeqNo → Extra tie-break → Origin lex order used for live
//     winners. In production this is a no-op because
//     routeStore.AnnounceProjectionFor already emits at most one
//     own-direct tombstone per Identity (storage is per-(Identity,
//     Uplink) and only Source==Direct tombstones make it through);
//     the collapse defends against synthetic test-fixture inputs.
//
//     Function contract: BuildAnnounceSnapshot has NO localOrigin
//     parameter and DOES NOT distinguish own-origin from transit
//     tombstones. The ONLY tombstones that are valid to emit on
//     the wire are own-origin tombstones (only the origin may emit
//     a wire withdrawal). Production callers MUST filter transit
//     tombstones before invoking BuildAnnounceSnapshot —
//     Table.AnnounceTo / routeStore.AnnounceProjectionFor enforces
//     this filter today (only Source==Direct tombstones make it
//     through). Test callers that bypass Table.AnnounceTo should
//     pass own-origin tombstones only, or accept that the snapshot
//     may carry a transit-tombstone entry that would be protocol-
//     invalid if actually sent.
//
// Entries are sorted deterministically by `(Identity, Hops, Origin)`:
// Identity ascending; within an Identity, the live winner (Hops <= 15)
// precedes the tombstone (Hops = 16) when both arrive in synthetic
// input bypassing the production live-vs-tombstone exclusivity; and
// among entries with the same (Identity, Hops), Origin lex order
// pins the order. This ordering is stable and independent of table
// iteration order. ComputeDelta keys on `(Identity, IsWithdrawn)`
// post-A1 so live and tombstone for the same Identity track
// independently — see ComputeDelta doc.
//
// A nil *AnnounceSnapshot represents "no snapshot sent yet" (empty
// state) and is semantically different from a snapshot with zero
// entries.
//
// Storage refactor history. Phase 0.x hot-fix introduced the
// per-Identity live-winner + tombstone-passthrough Stage 4 on top of
// the pre-existing per-(Identity, Origin, NextHop) RouteEntry triple
// storage; entries arrived with diverse Origin lineages (avg 12.7
// origins per identity in 100-node field data — 6072 stored rows
// across 1440 (Identity, Origin) buckets reaching 113 identities)
// and Stages 1-3 deduplicated per Origin before Stage 4 collapsed
// across Origins.
//
// Phase 1 P0 Phase A (this codebase's current state) swapped storage
// to per-(Identity, Uplink) UplinkClaim values with Origin dropped,
// and pushed the per-Identity live-winner selection into
// routeStore.AnnounceProjectionFor itself (so non-winning claims
// never enter the outbound SeqNo accounting and burn cache space —
// see route_store_lookup.go's AnnounceProjectionFor doc-comment).
// Wire emits carry Origin = localOrigin (sender-originated
// semantic, kept consistent across live and withdrawal frames for
// pre-A1 receiver compat — see uplink_claim.go for the migration
// contract). The Stage 3 per-(Identity, Origin) collapse turned
// destructive in this regime: Origin is constant across all
// entries we feed it, so Stage 3 would merge live and tombstone
// for the same Identity into one slot and lose whichever lost the
// max-SeqNo race. Stage 3 was therefore dropped; Stage 4 alone
// handles the per-Identity collapse with tombstone passthrough
// (now a defensive no-op for routing-driven inputs because
// AnnounceProjectionFor already collapsed), and ComputeDelta's
// key shifted to (Identity, IsWithdrawn) for the same reason.
type AnnounceSnapshot struct {
	Entries []AnnounceEntry
}

// Equal returns true when two snapshots carry the same canonical content.
// Both snapshots must be produced by BuildAnnounceSnapshot (sorted and
// aggregated) for this comparison to be meaningful.
func (s *AnnounceSnapshot) Equal(other *AnnounceSnapshot) bool {
	if s == nil || other == nil {
		return s == other
	}
	if len(s.Entries) != len(other.Entries) {
		return false
	}
	for i := range s.Entries {
		if !announceEntryEqual(s.Entries[i], other.Entries[i]) {
			return false
		}
	}
	return true
}

// ComputeDelta returns entries from newSnap that are new or changed
// relative to oldSnap.
//
// Key for delta tracking is (Identity, IsWithdrawn) — post-Phase-A
// every wire emit carries Origin = localOrigin, so the pre-A1
// (Identity, Origin) key would collapse the live winner and the
// own-origin tombstone for the same Identity into a single slot in
// oldIdx and lose whichever was inserted first. The IsWithdrawn axis
// keeps them in separate slots: at most one live entry per Identity
// AND at most one tombstone per Identity in the snapshot — both
// invariants come from BuildAnnounceSnapshot's Stage 3 per-Identity
// collapse (and from routeStore.AnnounceProjectionFor upstream, which
// emits at most one own-direct tombstone per Identity in production).
// Without the per-Identity tombstone collapse two identical synthetic
// snapshots with multiple tombstones per Identity would yield a
// non-empty delta — see BuildAnnounceSnapshot's tombstone-passthrough
// doc-comment for the contract.
//
// Two scenarios this keying handles correctly:
//
//   - Live winner uplink shift: when the per-Identity winner switches
//     from `(X, NextHop=A)` to `(X, NextHop=B)` because B announced
//     a shorter path, both snapshots carry `(X, Hops=N, SeqNo=...)`
//     under the (X, live) key. SeqNo / Hops / Extra comparison
//     decides whether to emit the delta entry.
//   - Tombstone alongside live alternate: a SYNTHETIC / structural
//     case for ComputeDelta only. The two distinct tracking keys
//     `{X, withdrawn=true}` and `{X, withdrawn=false}` exist so
//     synthetic test-fixture snapshots that hand both an own-
//     direct tombstone and a live alternate for the same Identity
//     to ComputeDelta (bypassing the production projection layer)
//     produce stable, independent delta tracking — without the
//     IsWithdrawn dimension on the tracking key, the two would
//     collide and the second-listed one would silently overwrite
//     the first. In PRODUCTION this combination never reaches
//     ComputeDelta: `routeStore.AnnounceProjectionFor`'s live-vs-
//     tombstone exclusivity rule (route_store_lookup.go) SUPPRESSES
//     the own-direct tombstone on the wire whenever a live alternate
//     exists for the same Identity, precisely to avoid the
//     `nextOutboundSeqLockedPerPeer` ordering hazard where the
//     trailing tombstone would be assigned a strictly greater
//     SeqNo than the live winner and flip the receiver to
//     withdrawn. The retry-via-delta path for tombstones only
//     fires when there is NO live alternate — that single
//     (X, tombstone) entry flows through ComputeDelta normally on
//     its `{X, withdrawn=true}` slot.
//
// Entries present in oldSnap but absent in newSnap are NOT included
// — absence is not an implicit withdrawal (see routing.md
// invariant). Callers should use the forced full sync path when no
// previous snapshot exists. If oldSnap is nil (defensive), every
// entry in newSnap is treated as new — equivalent to a full send.
func ComputeDelta(oldSnap, newSnap *AnnounceSnapshot) []AnnounceEntry {
	if newSnap == nil {
		return nil
	}
	if oldSnap == nil {
		// No previous baseline — all entries are new.
		result := make([]AnnounceEntry, len(newSnap.Entries))
		copy(result, newSnap.Entries)
		return result
	}

	oldIdx := make(map[announceTrackKey]AnnounceEntry, len(oldSnap.Entries))
	for _, e := range oldSnap.Entries {
		oldIdx[trackKeyFor(e)] = e
	}

	var delta []AnnounceEntry
	for _, e := range newSnap.Entries {
		old, found := oldIdx[trackKeyFor(e)]
		if !found {
			delta = append(delta, e)
			continue
		}
		if old.SeqNo != e.SeqNo ||
			old.Hops != e.Hops ||
			!normalizedExtraEqual(old.Extra, e.Extra) ||
			// Round-14: a sig-only upgrade in storage
			// (reconfirmation path admitted a verified sig at the
			// same SeqNo + same Hops + same Extra) changes only
			// AttestedSig. Without including the bytes here the
			// delta would be empty, the AnnounceLoop would
			// suppress the cycle as a no-op, and the signed
			// attestation would never reach the peer — leaving
			// the destination identity unverified on the receiver
			// forever. AttestedSigVerified is NOT compared: it
			// is a local-only observation that never travels on
			// the wire, so a local verify-flip without any wire-
			// byte change must NOT manufacture a delta.
			!bytes.Equal(old.AttestedSig, e.AttestedSig) {
			delta = append(delta, e)
		}
	}
	return delta
}

// trackKeyFor projects an AnnounceEntry onto its ComputeDelta
// tracking key. Live entries and tombstones for the same Identity
// land in distinct buckets so both flow through ComputeDelta
// independently — see ComputeDelta doc-comment for the rationale.
func trackKeyFor(e AnnounceEntry) announceTrackKey {
	return announceTrackKey{Identity: e.Identity, Withdrawn: e.Hops >= HopsInfinity}
}

// BuildAnnounceSnapshot takes raw AnnounceEntry items from
// Table.AnnounceTo and produces a canonical, aggregated,
// deterministically sorted snapshot ready for wire comparison and
// send.
//
// Aggregation stages (post-Phase-A):
//
//  1. Exact wire-dedup by (Identity, Origin, SeqNo, Hops, Extra).
//     Defensive — Table.AnnounceTo / routeStore.AnnounceProjectionFor
//     should not produce duplicates, but this catches any synthesised
//     test-fixture input.
//  2. Per-(Identity, Origin, SeqNo) aggregation: pick best entry
//     (min Hops, then deterministic Extra tie-break). Post-A1 every
//     entry carries Origin = localOrigin so this is effectively
//     per-(Identity, SeqNo); multiple uplinks happening to relay
//     the same foreign SeqNo collapse here.
//  3. Per-Identity aggregation. Live entries (Hops < HopsInfinity)
//     and withdrawal tombstones (Hops >= HopsInfinity, matching
//     RouteEntry.IsWithdrawn) are tracked in separate per-Identity
//     slots so a tombstone can survive alongside a live winner —
//     the retry-via-delta path for withdrawals requires both to
//     reach the wire. Each slot independently picks the best
//     candidate via isBetterAnnounceEntry (min Hops → higher
//     SeqNo → deterministic Extra tie-break → Origin lex), so the
//     post-Stage-3 invariant is "at most one live + at most one
//     tombstone per Identity". In production
//     routeStore.AnnounceProjectionFor already emits at most one
//     own-direct tombstone per Identity, so the tombstone collapse
//     is a no-op for routing-driven inputs; the collapse defends
//     against synthetic test-fixture inputs that hand the builder
//     multiple tombstones per Identity, which would otherwise
//     conflict with ComputeDelta's (Identity, IsWithdrawn) keying
//     and produce a non-empty delta for two identical snapshots
//     (see the AnnounceSnapshot type-level doc-comment for the
//     full contract and the TestComputeDelta_IdempotentOn
//     MultiTombstoneSnapshots regression).
//
// Pre-A1 had a separate Stage 3 ("per-(Identity, Origin), keep only
// max SeqNo") between Stages 2 and 4. With Origin = localOrigin
// constant across all entries post-A1, that stage collapsed live and
// own-direct tombstone for the same Identity into a single slot —
// whichever had the higher SeqNo won, the other was silently lost,
// breaking the tombstone-passthrough invariant. Stage 3 was therefore
// dropped; the per-Identity stage (now Stage 3 in the renumbered list
// above) sees the per-(Identity, Origin, SeqNo) winners directly and
// applies the live-vs-tombstone split there. ComputeDelta's key
// shifted to (Identity, IsWithdrawn) for the same reason.
//
// Round-5i tightened the tombstone side of the Stage 3 collapse:
// pre-collapse tombstones were preserved verbatim, but ComputeDelta's
// (Identity, IsWithdrawn) keying would have produced non-empty
// deltas for two identical synthetic snapshots carrying multiple
// tombstones per Identity. The tombstone slot now picks a single
// winner under the same isBetterAnnounceEntry ordering as live
// entries, matching the snapshot to the delta-tracking contract.
//
// The result is sorted by (Identity, Hops, Origin): Identity
// ascending; within an Identity, the live winner (Hops <= 15)
// precedes the tombstone (Hops = 16). Each (Identity, Hops) pair is
// unique post-collapse, so the Origin component of the sort key is
// just a tie-break left in for shape stability against future
// changes.
//
// Field-data motivation (May 2026): a 100-node mesh routes.json
// snapshot showed 6072 RouteEntry rows, 1440 (Identity, Origin)
// buckets, only 113 reachable identities — i.e. 12.7× redundancy
// on the wire. Post-A1 the per-Identity collapse moved upstream
// into `routeStore.AnnounceProjectionFor`: Table.AnnounceTo emits
// one LIVE WINNER per Identity (selected inline by min Hops →
// higher SeqNo → verified-sig tie-break (Round-26:
// AttestedSigVerified=true beats unsigned, mirrors
// scoreSignedBonus in Lookup/AnnounceTargetFor) → Extra tie-break
// → deterministic Uplink lex) plus any own-direct tombstones in
// the no-live-alternative case.
// BuildAnnounceSnapshot therefore runs as a thin defensive pass
// on routing-driven inputs — the per-Identity stage below is a
// no-op for AnnounceProjectionFor output and only fires on
// synthetic test-fixture inputs that bypass the projection layer
// and hand in multiple live claims per Identity directly.
func BuildAnnounceSnapshot(raw []AnnounceEntry) *AnnounceSnapshot {
	if len(raw) == 0 {
		return &AnnounceSnapshot{}
	}

	// Stage 1: exact wire-dedup.
	type wireKey struct {
		Identity PeerIdentity
		Origin   PeerIdentity
		SeqNo    uint64
		Hops     int
		Extra    string // normalized canonical bytes
		// Round-19 alignment: include the attested-links signature
		// in the dedup key, since Round-14 promoted AttestedSig to
		// wire content (it now participates in ComputeDelta and the
		// outboundEmitSig content cache). Treating two entries that
		// differ only in sig as exact-duplicates here would silently
		// drop one of them before the per-Identity collapse, which
		// could lose a verified-bytes upgrade if the producer ever
		// hands BuildAnnounceSnapshot two pre-collapse copies (one
		// signed, one not). In production AnnounceProjectionFor
		// always emits one entry per (Identity) winner so this case
		// is degenerate, but aligning the key with the rest of the
		// wire-content contract removes the foot-gun. Raw bytes →
		// string conversion (Go allows []byte→string for arbitrary
		// bytes; the string just holds the byte sequence) keeps
		// wireKey map-comparable.
		AttestedSig string
	}
	seen := make(map[wireKey]struct{}, len(raw))
	deduped := make([]AnnounceEntry, 0, len(raw))
	for _, e := range raw {
		// normalizeExtra is used ONLY to build the dedup key — the
		// emitted AnnounceEntry below carries e.Extra verbatim
		// (Round-24 fix). The attested-links contract
		// (RouteAnnounceV3Entry.CanonicalSigningBytes in
		// internal/core/protocol/frame_route_announce_v3.go) hashes
		// Extra dword-for-dword as it appears on the wire, and
		// docs/protocol/attested_links.md "Canonical signing bytes"
		// promises verbatim transit. If we stored the normalized
		// bytes in the emitted entry, a transit re-emit of a signed
		// frame would carry mutated Extra bytes alongside the
		// original AttestedSig — every downstream peer with
		// mesh_attested_links_v1 negotiated would fail verification
		// and drop the entry. Normalization is fine in the dedup key
		// because the key is internal-only — never touches the wire.
		ne := normalizeExtra(e.Extra)
		k := wireKey{
			Identity:    e.Identity,
			Origin:      e.Origin,
			SeqNo:       e.SeqNo,
			Hops:        e.Hops,
			Extra:       string(ne),
			AttestedSig: string(e.AttestedSig),
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		deduped = append(deduped, AnnounceEntry{
			Identity: e.Identity,
			Origin:   e.Origin,
			Hops:     e.Hops,
			SeqNo:    e.SeqNo,
			// Round-24: emit verbatim Extra bytes, not the
			// normalized form — see the comment above for the
			// attested-links pair-consistency contract.
			Extra: e.Extra,
			// Phase 4 13.2-A: carry the attested-links signature
			// through the dedup stage so a v3 re-emit forwards the
			// original signer's bytes unchanged.
			AttestedSig:         e.AttestedSig,
			AttestedSigVerified: e.AttestedSigVerified,
		})
	}

	// Stage 2: per-(Identity, Origin, SeqNo, Withdrawn) — pick best
	// by min Hops, then deterministic Extra tie-break.
	//
	// The Withdrawn axis was added in Phase 1 A1 round 4. Without
	// it, a live entry and an own-direct tombstone for the same
	// Identity with identical (Origin, SeqNo) — which is now
	// possible because the per-Identity outbound SeqNo synthesis
	// can land them on the same value when sigs differ but the
	// underlying claim's nativeSeqNo coincides — collide on the
	// Stage 2 key and the min-Hops winner (live, Hops=N) replaces
	// the tombstone (Hops=16). The tombstone is lost BEFORE the
	// per-Identity tombstone-passthrough stage gets to preserve it,
	// breaking the retry-via-delta path for withdrawals. Splitting
	// the key on Withdrawn keeps live and tombstone in different
	// Stage 2 buckets even when their (Origin, SeqNo) coincide.
	//
	// Invariant: Extra MUST be identical for all entries with the
	// same (Identity, Origin, SeqNo, Withdrawn) lineage. The origin
	// sets Extra once and every relay forwards it unchanged. If
	// Extra differs across next-hop variants, a relay node has
	// corrupted the payload or a protocol version mismatch exists.
	// We log the violation and fall back to deterministic tie-break
	// to preserve convergence.
	type seqKey struct {
		Identity  PeerIdentity
		Origin    PeerIdentity
		SeqNo     uint64
		Withdrawn bool
	}
	bestBySeq := make(map[seqKey]AnnounceEntry, len(deduped))
	for _, e := range deduped {
		k := seqKey{Identity: e.Identity, Origin: e.Origin, SeqNo: e.SeqNo, Withdrawn: e.Hops >= HopsInfinity}
		existing, found := bestBySeq[k]
		if !found {
			bestBySeq[k] = e
			continue
		}
		// Diagnostic: Extra should be identical per (Identity, Origin, SeqNo).
		if !normalizedExtraEqual(existing.Extra, e.Extra) {
			log.Warn().
				Str("identity", string(e.Identity)).
				Str("origin", string(e.Origin)).
				Uint64("seq", e.SeqNo).
				Str("extra_existing", string(existing.Extra)).
				Str("extra_incoming", string(e.Extra)).
				Int("hops_existing", existing.Hops).
				Int("hops_incoming", e.Hops).
				Msg("announce_aggregation_extra_mismatch")
		}
		if e.Hops < existing.Hops {
			bestBySeq[k] = e
		} else if e.Hops == existing.Hops {
			if compareExtra(e.Extra, existing.Extra) > 0 {
				bestBySeq[k] = e
			}
		}
	}

	// Stage 3: per-Identity — collapse LIVE entries to a single
	// winner per destination (classical DV "best route per
	// destination" announce semantic), BUT preserve own-origin
	// withdrawal tombstones alongside the live winner. They must
	// propagate independently:
	//
	// Withdrawal tombstones (Hops >= HopsInfinity) are emitted by
	// Table.AnnounceTo only for own-origin routes (transit tombstones
	// are filtered out earlier — only the origin may emit wire
	// withdrawals; the routeStore.AnnounceProjectionFor passthrough
	// enforces Source==Direct). When this node is the origin AND
	// has just lost a direct peer X, it produces a tombstone
	// (Identity=X, Hops=16). At the same time the table may still
	// hold a live alternate claim for X (e.g. learned via
	// announcement through another uplink): (Identity=X, Hops=2). If
	// the per-Identity collapse picked by min-Hops alone, the live
	// alternate would beat the tombstone (Hops=2 < Hops=16) and the
	// tombstone would never reach the wire — receivers where the
	// immediate disconnect fanout (fanoutAnnounceRoutes in
	// internal/core/node/routing_announce.go) failed would keep
	// their stale own-origin bucket pointing at us until natural
	// TTL expiry, possibly preferring our stale shorter path over
	// the live backup. The retry-via-delta path promised by
	// Table.AnnounceTo's tombstone passthrough would silently break.
	//
	// Fix: split the per-Identity bucket into (a) own-origin
	// tombstones, passed through verbatim, (b) the best live winner
	// across all live entries. Both are emitted in the resulting
	// snapshot. Receivers see two AnnounceEntry rows for the
	// recently-disconnected Identity — one tombstone updating the
	// stale own-origin bucket, one live entry refreshing the
	// alternate path. Both are well-formed wire frames; legacy
	// per-(Identity, Origin, NextHop) cap admission and SeqNo
	// monotonicity continue to apply on receivers (with the
	// migration-contract caveats documented in uplink_claim.go for
	// the per-Identity SeqNo-namespace interaction).
	type identityGroup struct {
		// tombBest is the single tombstone winner per Identity. The
		// snapshot carries AT MOST one tombstone per Identity so that
		// ComputeDelta's `(Identity, IsWithdrawn)` tracking key
		// covers the entire tombstone set for an Identity in one
		// slot. In production routeStore.AnnounceProjectionFor emits
		// at most one own-direct tombstone per Identity (storage is
		// per-(Identity, Uplink) and only Source==Direct tombstones
		// make it through), so the collapse is a no-op for routing-
		// driven inputs. The collapse remains defensive against
		// synthetic test-fixture inputs that hand BuildAnnounceSnapshot
		// multiple tombstones per Identity with different Origins —
		// without this collapse, two identical synthetic snapshots
		// would produce a non-empty ComputeDelta because the
		// (Identity, IsWithdrawn) keyed oldIdx silently overwrites
		// all but the last tombstone, and the surviving slot would
		// not match the earlier tombstones in newSnap on SeqNo.
		//
		// Selection mirrors live-winner ordering for the same
		// determinism: highest SeqNo wins (latest withdrawal info),
		// then Extra tie-break, then Origin lex tie-break. The
		// "own-origin only" guarantee is enforced by the caller
		// (routeStore.AnnounceProjectionFor filters transit tombstones
		// upstream); see the AnnounceSnapshot doc-comment for the
		// full contract.
		tombBest *AnnounceEntry
		liveBest *AnnounceEntry // best live winner across all live entries
	}
	groups := make(map[PeerIdentity]*identityGroup, len(bestBySeq))
	for _, e := range bestBySeq {
		g, ok := groups[e.Identity]
		if !ok {
			g = &identityGroup{}
			groups[e.Identity] = g
		}
		cand := e // capture for &-addressing
		if e.Hops >= HopsInfinity {
			// Withdrawal tombstone. The `>=` classification matches
			// `RouteEntry.IsWithdrawn()` in types.go: any Hops at or
			// above HopsInfinity is treated as withdrawn throughout
			// the routing model (receive path, table state, snapshot
			// publisher). Strict equality here would diverge from the
			// rest of the codebase if a malformed AnnounceEntry with
			// Hops > HopsInfinity ever reached this stage — that
			// entry would slip through as a «live» entry with insane
			// Hops, polluting the per-Identity live-winner ranking.
			// `RouteEntry.Validate()` rejects Hops > 16 at insertion,
			// and `Table.AnnounceTo` projects from validated rows, so
			// production cannot trigger the divergence — but the
			// contract is consistent now.
			//
			// Table.AnnounceTo upstream additionally guarantees this is
			// own-origin (transit tombstones filtered out before
			// reaching us); see the AnnounceSnapshot doc for the full
			// caller contract.
			if g.tombBest == nil || isBetterAnnounceEntry(cand, *g.tombBest) {
				g.tombBest = &cand
			}
			continue
		}
		// Live entry — accumulate into per-Identity winner.
		if g.liveBest == nil || isBetterAnnounceEntry(cand, *g.liveBest) {
			g.liveBest = &cand
		}
	}

	// Build sorted result. Sort by (Identity, Hops, Origin) so:
	//   - Identity grouping is stable;
	//   - within an Identity, the live winner (Hops <= 15) precedes
	//     the tombstone (Hops = 16) for deterministic Equal()
	//     comparison;
	//   - the (Identity, Hops) pair is unique post-collapse (each
	//     Identity has at most one live and at most one tombstone),
	//     so the Origin component of the sort key just keeps the
	//     comparison total-ordered against future shape changes.
	result := make([]AnnounceEntry, 0, len(groups)*2)
	for _, g := range groups {
		if g.liveBest != nil {
			result = append(result, *g.liveBest)
		}
		if g.tombBest != nil {
			result = append(result, *g.tombBest)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Identity != result[j].Identity {
			return result[i].Identity < result[j].Identity
		}
		if result[i].Hops != result[j].Hops {
			return result[i].Hops < result[j].Hops
		}
		return result[i].Origin < result[j].Origin
	})

	return &AnnounceSnapshot{Entries: result}
}

// isBetterAnnounceEntry returns true when `candidate` should replace
// `incumbent` as the per-Identity winner in BuildAnnounceSnapshot's
// Stage 3 aggregation. The helper is used for BOTH the live-entry
// collapse AND the tombstone collapse (round-5i): each slot
// independently picks one winner under the same ordering, so a live
// entry never competes against a tombstone in the same comparison
// (callers route each candidate to the right slot based on
// Hops >= HopsInfinity before invoking this helper).
//
// Comparison priority:
//
//  1. min Hops wins (shorter path is preferred — classical DV metric).
//  2. higher SeqNo wins on hops tie. Higher SeqNo is a soft signal
//     that the lineage is more recently confirmed; among paths of
//     equal length we prefer the freshest. NOTE: this is the
//     OPPOSITE of Stage 2's SeqNo handling, which selects max-SeqNo
//     within a single per-(Identity, Origin, SeqNo) group to enforce
//     monotonicity. The per-Identity stage ranks ACROSS all post-
//     Stage-2 entries, where SeqNo monotonicity does not apply
//     (uplinks relay foreign SeqNos from independent timelines —
//     post-A1 the sender-scoped outbound SeqNo synthesis in
//     routeStore.nextOutboundSeqLockedPerPeer keeps the wire
//     timeline monotonic; this stage's comparison stays a
//     heuristic), so higher SeqNo is just a tie-break heuristic.
//  3. compareExtra > 0 wins on (hops, seq) tie — deterministic
//     canonical byte ordering, last-resort tie-break.
//  4. Lower Origin lex order wins on full (hops, seq, extra) tie —
//     deterministic stability across map iteration orders.
func isBetterAnnounceEntry(candidate, incumbent AnnounceEntry) bool {
	if candidate.Hops != incumbent.Hops {
		return candidate.Hops < incumbent.Hops
	}
	if candidate.SeqNo != incumbent.SeqNo {
		return candidate.SeqNo > incumbent.SeqNo
	}
	if cmp := compareExtra(candidate.Extra, incumbent.Extra); cmp != 0 {
		return cmp > 0
	}
	// Final deterministic tie-break: lower Origin wins. Without this,
	// map iteration order at the start of the per-Identity stage
	// would non-deterministically pick which entry gets «inserted
	// first» when (Hops, SeqNo, Extra) all tie, so successive snapshot
	// builds could emit different Origins for the same wire-stable
	// destination — that flaps no-op suppression and triggers
	// redundant delta sends. Lower Origin is an arbitrary but stable
	// choice; receivers see consistent origin pinning until something
	// actually changes (better hops, fresher SeqNo, etc.). Post-A1
	// every entry carries Origin = localOrigin, so this tie-break is
	// degenerate in practice — kept for forward-compat with any future
	// re-introduction of multi-Origin emit shapes.
	return candidate.Origin < incumbent.Origin
}

// announceTrackKey is the per-Identity tracking key for ComputeDelta
// after the Phase A storage swap. Live entry and own-direct
// tombstone for the same Identity differ in IsWithdrawn, so they
// occupy distinct slots in the delta-tracking map and flow through
// ComputeDelta independently.
//
// Pre-A1 the type was named announceLineageKey and keyed on
// (Identity, Origin); see ComputeDelta doc-comment for why the
// migration to (Identity, Withdrawn) was necessary.
type announceTrackKey struct {
	Identity  PeerIdentity
	Withdrawn bool
}

// normalizeExtra normalizes json.RawMessage for canonical comparison.
// nil and "null" are treated as equivalent and both normalize to nil.
// Surrounding whitespace is stripped so that " {\"a\":1} " and
// "{\"a\":1}" are considered equal.
func normalizeExtra(extra json.RawMessage) json.RawMessage {
	if len(extra) == 0 {
		return nil
	}
	trimmed := bytes.TrimSpace(extra)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return trimmed
}

// normalizedExtraEqual compares two Extra values after normalization.
func normalizedExtraEqual(a, b json.RawMessage) bool {
	na := normalizeExtra(a)
	nb := normalizeExtra(b)
	if na == nil && nb == nil {
		return true
	}
	if na == nil || nb == nil {
		return false
	}
	return bytes.Equal(na, nb)
}

// compareExtra implements the canonical tie-break ordering for Extra.
// nil < any non-nil; non-nil compared by lexicographic byte order.
// Returns -1, 0, or 1.
func compareExtra(a, b json.RawMessage) int {
	na := normalizeExtra(a)
	nb := normalizeExtra(b)
	if na == nil && nb == nil {
		return 0
	}
	if na == nil {
		return -1
	}
	if nb == nil {
		return 1
	}
	return bytes.Compare(na, nb)
}

// announceEntryEqual compares two AnnounceEntry values for canonical equality.
// AttestedSig participates because Round-14 promoted it to wire content
// (ComputeDelta + outboundEmitSig already key on the bytes), so two
// entries that differ only in sig must NOT compare equal — otherwise
// AnnounceSnapshot.Equal would treat a verified-bytes upgrade as the
// same snapshot and any test or downstream check based on Equal would
// silently miss the change. AttestedSigVerified is local-only and never
// participates (matches the Round-14 ComputeDelta rule).
func announceEntryEqual(a, b AnnounceEntry) bool {
	return a.Identity == b.Identity &&
		a.Origin == b.Origin &&
		a.SeqNo == b.SeqNo &&
		a.Hops == b.Hops &&
		normalizedExtraEqual(a.Extra, b.Extra) &&
		bytes.Equal(a.AttestedSig, b.AttestedSig)
}
