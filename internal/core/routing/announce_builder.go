package routing

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/rs/zerolog/log"
)

// AnnounceSnapshot is a canonical, aggregated, peer-specific view of route
// entries ready for wire transmission. After full aggregation, for each
// Identity the snapshot carries at most:
//
//   - ONE live winner across all live `(Identity, Origin)` lineages —
//     classical distance-vector "best route per destination" semantic.
//     Multiple live origin lineages we know locally collapse to a single
//     winner per destination on the send side; receivers see one live
//     entry per Identity from us instead of one per `(Identity, Origin)`
//     bucket we knew.
//
//   - ANY withdrawal tombstones (Hops >= HopsInfinity, matching
//     `RouteEntry.IsWithdrawn` semantics in types.go) — passed through
//     verbatim alongside the live winner. They propagate independently
//     of the live-winner collapse so the retry-via-delta path for
//     withdrawals keeps working: without this passthrough, a freshly-
//     disconnected destination with a live alternate via another peer
//     would silently lose its tombstone in stage 4 and receivers where
//     the immediate disconnect fanout failed would keep their stale
//     own-origin bucket pointing at us until natural TTL expiry.
//
//     Function contract: BuildAnnounceSnapshot has NO localOrigin
//     parameter and DOES NOT distinguish own-origin from transit
//     tombstones — every Hops>=HopsInfinity entry in the input is
//     preserved unchanged. The ONLY tombstones that are valid to emit
//     on the wire are own-origin tombstones (only the origin may emit
//     a wire withdrawal). Production callers MUST filter transit
//     tombstones before invoking BuildAnnounceSnapshot — Table.AnnounceTo
//     enforces this filter today. Test callers that bypass Table.AnnounceTo
//     should pass own-origin tombstones only, or accept that the
//     snapshot may carry transit-tombstone entries that would be
//     protocol-invalid if actually sent.
//
// Entries are sorted deterministically by `(Identity, Hops, Origin)`:
// Identity ascending; within an Identity, the live winner (Hops <= 15)
// precedes tombstones (Hops = 16); and among entries with the same
// (Identity, Hops), Origin lex order pins the order. This ordering is
// stable and independent of table iteration order. ComputeDelta keys on
// `(Identity, Origin)` to handle the multi-entry-per-Identity case —
// see ComputeDelta doc-comment.
//
// A nil *AnnounceSnapshot represents "no snapshot sent yet" (empty state)
// and is semantically different from a snapshot with zero entries.
//
// Storage refactor note: this is the send-side aggregation hot-fix.
// Receive-side storage is still per-(Identity, Origin, NextHop) triple
// in routing.Table — a separate refactor (per-uplink claim model)
// handles that path. The hot-fix is wire-only: legacy peers continue
// to receive `(Identity, Origin, Hops, SeqNo)` frames, just one (live
// winner) plus optional tombstone per Identity from us instead of N
// entries per `(Identity, Origin)` lineage we previously emitted (N
// was avg ~12.7 in field data on a 100-node mesh — 6072 stored
// RouteEntry rows / 1440 (Identity, Origin) buckets / 113 reachable
// identities). See docs/routing.md «Per-Identity aggregation hot-fix»
// for the operator-facing description.
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
// relative to oldSnap. Identity key for comparison is `(Identity, Origin)`:
// after the per-Identity Stage 4 aggregation, a snapshot may carry
// multiple entries per Identity (the live winner plus 0+ own-origin
// tombstones — see AnnounceSnapshot doc), so we cannot key on Identity
// alone. Each (Identity, Origin) lineage is tracked independently, which
// matches the receiver's per-(Identity, Origin) cap admission and SeqNo
// monotonicity.
//
// Two scenarios this keying handles correctly:
//
//   - Live winner Origin shift: when the per-Identity winner switches
//     from `(X, OriginA)` to `(X, OriginB)` because B announced a
//     shorter path, ComputeDelta sees `(X, OriginB)` as a NEW key —
//     emit it. The old `(X, OriginA)` lineage is silently dropped from
//     our snapshot; absence is not implicit withdrawal (routing.md
//     invariant), receivers' `(X, OriginA)` buckets stop being
//     refreshed by us and TTL-expire on the standard 120 s schedule.
//   - Tombstone alongside live alternate: when this node is the origin
//     and disconnects from X, snapshot carries both
//     `(X, localOrigin, Hops=16)` tombstone and `(X, somePeer, Hops=2)`
//     live alternate. Both have distinct (Identity, Origin) keys, both
//     get tracked independently by ComputeDelta, both propagate to
//     receivers. The retry-via-delta path for tombstones works.
//
// Entries present in oldSnap but absent in newSnap are NOT included —
// absence is not an implicit withdrawal (see routing.md invariant).
// Callers should use the forced full sync path when no previous snapshot
// exists. If oldSnap is nil (defensive), every entry in newSnap is
// treated as new — equivalent to a full send.
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

	oldIdx := make(map[announceLineageKey]AnnounceEntry, len(oldSnap.Entries))
	for _, e := range oldSnap.Entries {
		oldIdx[announceLineageKey{Identity: e.Identity, Origin: e.Origin}] = e
	}

	var delta []AnnounceEntry
	for _, e := range newSnap.Entries {
		key := announceLineageKey{Identity: e.Identity, Origin: e.Origin}
		old, found := oldIdx[key]
		if !found {
			delta = append(delta, e)
			continue
		}
		if old.SeqNo != e.SeqNo ||
			old.Hops != e.Hops ||
			!normalizedExtraEqual(old.Extra, e.Extra) {
			delta = append(delta, e)
		}
	}
	return delta
}

// BuildAnnounceSnapshot takes raw AnnounceEntry items from Table.AnnounceTo
// and produces a canonical, aggregated, deterministically sorted snapshot
// ready for wire comparison and send.
//
// Aggregation follows four stages:
//  1. Exact wire-dedup by (Identity, Origin, SeqNo, Hops, Extra).
//  2. Per-(Identity, Origin, SeqNo) aggregation: pick best entry (min Hops,
//     then deterministic Extra tie-break).
//  3. Per-(Identity, Origin) aggregation: keep only max SeqNo entry.
//  4. Per-Identity aggregation with tombstone passthrough:
//     (a) Own-origin withdrawal tombstones (Hops >= HopsInfinity,
//     matching RouteEntry.IsWithdrawn) are preserved verbatim —
//     passed through unchanged so the retry-via-delta path for
//     withdrawals stays functional.
//     (b) Live entries (Hops < HopsInfinity) collapse across origin
//     lineages to a SINGLE winner per Identity, classical
//     distance-vector "best route per destination" semantic.
//     Selection priority: min Hops, then higher SeqNo, then
//     deterministic Extra tie-break, then Origin lex order tie-break
//     for stability across map iteration orders.
//     The winning live entry's Origin field is preserved on the wire —
//     legacy peers continue to apply per-(Identity, Origin) cap
//     admission and SeqNo monotonicity normally; our contribution to
//     their storage drops from N per known origin lineage to (1 live
//     winner + 0..1 tombstone) per Identity.
//
// The result is sorted by (Identity, Hops, Origin): Identity ascending;
// within an Identity, live winner (Hops <= 15) precedes tombstones
// (Hops = 16); same-Hops entries ordered by Origin lex.
//
// Field-data motivation (May 2026): a 100-node mesh routes.json snapshot
// showed 6072 RouteEntry rows, 1440 (Identity, Origin) buckets, only 113
// reachable identities — i.e. 12.7× redundancy on the wire. Stage 4
// directly maps wire-volume reduction onto the inflation factor: 1440
// snapshot entries per peer per cycle drop to ~113 (plus rare tombstones
// on disconnect events).
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
	}
	seen := make(map[wireKey]struct{}, len(raw))
	deduped := make([]AnnounceEntry, 0, len(raw))
	for _, e := range raw {
		ne := normalizeExtra(e.Extra)
		k := wireKey{
			Identity: e.Identity,
			Origin:   e.Origin,
			SeqNo:    e.SeqNo,
			Hops:     e.Hops,
			Extra:    string(ne),
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
			Extra:    ne,
		})
	}

	// Stage 2: per-(Identity, Origin, SeqNo) — pick best by min Hops,
	// then deterministic Extra tie-break.
	//
	// Invariant: Extra MUST be identical for all entries with the same
	// (Identity, Origin, SeqNo) lineage. The origin sets Extra once and
	// every relay forwards it unchanged. If Extra differs across
	// next-hop variants, a relay node has corrupted the payload or
	// a protocol version mismatch exists. We log the violation and
	// fall back to deterministic tie-break to preserve convergence.
	type seqKey struct {
		Identity PeerIdentity
		Origin   PeerIdentity
		SeqNo    uint64
	}
	bestBySeq := make(map[seqKey]AnnounceEntry, len(deduped))
	for _, e := range deduped {
		k := seqKey{Identity: e.Identity, Origin: e.Origin, SeqNo: e.SeqNo}
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

	// Stage 3: per-(Identity, Origin) — keep only max SeqNo.
	type lineageKey = announceLineageKey
	bestByLineage := make(map[lineageKey]AnnounceEntry, len(bestBySeq))
	for _, e := range bestBySeq {
		k := lineageKey{Identity: e.Identity, Origin: e.Origin}
		existing, found := bestByLineage[k]
		if !found {
			bestByLineage[k] = e
			continue
		}
		if e.SeqNo > existing.SeqNo {
			bestByLineage[k] = e
		} else if e.SeqNo == existing.SeqNo {
			// Same SeqNo after stage 2 — apply best-entry rule again.
			if e.Hops < existing.Hops {
				bestByLineage[k] = e
			} else if e.Hops == existing.Hops {
				if compareExtra(e.Extra, existing.Extra) > 0 {
					bestByLineage[k] = e
				}
			}
		}
	}

	// Stage 4: per-Identity — collapse LIVE Origin lineages to a single
	// winner per destination (classical DV "best route per destination"
	// announce semantic), BUT preserve own-origin withdrawal tombstones
	// alongside the live winner. They must propagate independently:
	//
	// Withdrawal tombstones (Hops >= HopsInfinity) are emitted by
	// Table.AnnounceTo only for own-origin routes (transit tombstones
	// are filtered out earlier — only the origin may emit wire
	// withdrawals). When this node is the origin AND has just lost a
	// direct peer X, it produces a tombstone (Identity=X,
	// Origin=localOrigin, Hops=16). At the same time the table may
	// still hold a live alternate lineage for X (e.g. learned via
	// announcement from another peer): (Identity=X, Origin=somePeer,
	// Hops=2). If Stage 4 collapsed by min-Hops alone, the live
	// alternate would beat the tombstone (Hops=2 < Hops=16) and the
	// tombstone would never reach the wire — receivers where the
	// immediate disconnect fanout (fanoutAnnounceRoutes in
	// internal/core/node/routing_announce.go) failed would keep their
	// stale (Identity=X, Origin=localOrigin) bucket pointing at us
	// until natural TTL expiry, possibly preferring our
	// stale shorter path over the live backup. The retry-via-delta
	// path promised by Table.AnnounceTo's tombstone passthrough would
	// silently break.
	//
	// Fix: split the per-Identity bucket into (a) own-origin
	// tombstones, passed through verbatim, (b) the best live winner
	// across all live origin lineages. Both are emitted in the
	// resulting snapshot. Receivers see two AnnounceEntry rows for
	// the recently-disconnected Identity — one tombstone updating
	// the stale own-origin bucket, one live entry refreshing/teaching
	// the alternate path. Both are well-formed wire frames; legacy
	// per-(Identity, Origin) cap admission and SeqNo monotonicity
	// continue to apply on receivers.
	type identityGroup struct {
		// tombstones holds withdrawal entries (Hops >= HopsInfinity,
		// matching RouteEntry.IsWithdrawn) for this Identity,
		// preserved verbatim. Stage 4 itself does NOT check Origin —
		// every Hops>=HopsInfinity entry that reaches this point is
		// appended. The "own-origin only" guarantee is enforced by the
		// caller (Table.AnnounceTo filters transit tombstones
		// upstream); see the AnnounceSnapshot doc-comment for the
		// full contract.
		tombstones []AnnounceEntry
		liveBest   *AnnounceEntry // best live winner across all live origin lineages
	}
	groups := make(map[PeerIdentity]*identityGroup, len(bestByLineage))
	for _, e := range bestByLineage {
		g, ok := groups[e.Identity]
		if !ok {
			g = &identityGroup{}
			groups[e.Identity] = g
		}
		if e.Hops >= HopsInfinity {
			// Withdrawal tombstone — passthrough preserved. The `>=`
			// classification matches `RouteEntry.IsWithdrawn()` in
			// types.go: any Hops at or above HopsInfinity is treated as
			// withdrawn throughout the routing model (receive path,
			// table state, snapshot publisher). Strict equality here
			// would diverge from the rest of the codebase if a
			// malformed AnnounceEntry with Hops > HopsInfinity ever
			// reached this stage — that entry would slip through as a
			// «live» entry with insane Hops, polluting the per-Identity
			// live-winner ranking. `RouteEntry.Validate()` rejects
			// Hops > 16 at insertion, and `Table.AnnounceTo` projects
			// from validated rows, so production cannot trigger the
			// divergence — but the contract is consistent now.
			//
			// Table.AnnounceTo upstream additionally guarantees this is
			// own-origin (transit tombstones filtered out before
			// reaching us); see the AnnounceSnapshot doc for the full
			// caller contract.
			g.tombstones = append(g.tombstones, e)
			continue
		}
		// Live entry — accumulate into per-Identity winner.
		cand := e // capture for &-addressing
		if g.liveBest == nil || isBetterAnnounceEntry(cand, *g.liveBest) {
			g.liveBest = &cand
		}
	}

	// Build sorted result. Sort by (Identity, Hops, Origin) so:
	//   - Identity grouping is stable;
	//   - within an Identity, the live winner (Hops <= 15) precedes
	//     tombstones (Hops = 16) for deterministic Equal() comparison;
	//   - among entries with same (Identity, Hops), Origin lex order
	//     pins the order. In normal operation each Identity has at
	//     most one tombstone (single own-origin), so the (Hops, Origin)
	//     ordering is fully determined.
	result := make([]AnnounceEntry, 0, len(groups)*2)
	for _, g := range groups {
		if g.liveBest != nil {
			result = append(result, *g.liveBest)
		}
		result = append(result, g.tombstones...)
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
// `incumbent` as the live-winner candidate in stage-4 per-Identity
// aggregation. ONLY live entries (Hops < HopsInfinity) are compared via
// this helper — own-origin tombstones bypass the comparison and are
// emitted alongside the live winner (see Stage 4 in BuildAnnounceSnapshot).
//
// Comparison priority:
//
//  1. min Hops wins (shorter path is preferred — classical DV metric).
//  2. higher SeqNo wins on hops tie. Higher SeqNo is a soft signal that
//     the lineage is more recently confirmed by its origin; among paths
//     of equal length we prefer the freshest. NOTE: this is the OPPOSITE
//     of stage 2/3 SeqNo handling, which selects max-SeqNo within a
//     SINGLE Origin lineage to enforce monotonicity. Stage 4 ranks
//     ACROSS Origin lineages where SeqNo monotonicity does not apply
//     (different origins advance their own counters independently),
//     so higher SeqNo is just a tie-break heuristic.
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
	// map iteration order at the start of stage 4 would non-deterministically
	// pick which origin lineage gets «inserted first» when (Hops, SeqNo,
	// Extra) all tie across origins, so successive snapshot builds could
	// emit different Origins for the same wire-stable destination — that
	// flaps no-op suppression and triggers redundant delta sends. Lower
	// Origin is an arbitrary but stable choice; receivers see consistent
	// origin pinning until something actually changes (better hops, fresher
	// SeqNo, etc.).
	return candidate.Origin < incumbent.Origin
}

// announceLineageKey is the identity key for per-peer announce snapshots.
type announceLineageKey struct {
	Identity PeerIdentity
	Origin   PeerIdentity
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
func announceEntryEqual(a, b AnnounceEntry) bool {
	return a.Identity == b.Identity &&
		a.Origin == b.Origin &&
		a.SeqNo == b.SeqNo &&
		a.Hops == b.Hops &&
		normalizedExtraEqual(a.Extra, b.Extra)
}
