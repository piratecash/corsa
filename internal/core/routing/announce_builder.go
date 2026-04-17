package routing

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/rs/zerolog/log"
)

// AnnounceSnapshot is a canonical, aggregated, peer-specific view of route
// entries ready for wire transmission. After full aggregation each
// (Identity, Origin) pair appears at most once. Entries are sorted
// deterministically by (Identity, Origin) so that equality comparison
// between two snapshots is stable and independent of table iteration order.
//
// A nil *AnnounceSnapshot represents "no snapshot sent yet" (empty state)
// and is semantically different from a snapshot with zero entries.
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
// relative to oldSnap. Identity key for comparison is (Identity, Origin).
// Entries present in oldSnap but absent in newSnap are NOT included —
// absence is not an implicit withdrawal (see routing.md invariant).
//
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
		if old.SeqNo != e.SeqNo || old.Hops != e.Hops || !normalizedExtraEqual(old.Extra, e.Extra) {
			delta = append(delta, e)
		}
	}
	return delta
}

// BuildAnnounceSnapshot takes raw AnnounceEntry items from Table.AnnounceTo
// and produces a canonical, aggregated, deterministically sorted snapshot
// ready for wire comparison and send.
//
// Aggregation follows three stages:
//  1. Exact wire-dedup by (Identity, Origin, SeqNo, Hops, Extra).
//  2. Per-(Identity, Origin, SeqNo) aggregation: pick best entry (min Hops,
//     then deterministic Extra tie-break).
//  3. Per-(Identity, Origin) aggregation: keep only max SeqNo entry.
//
// The result is sorted by (Identity, Origin).
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

	// Build sorted result.
	result := make([]AnnounceEntry, 0, len(bestByLineage))
	for _, e := range bestByLineage {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Identity != result[j].Identity {
			return result[i].Identity < result[j].Identity
		}
		return result[i].Origin < result[j].Origin
	})

	return &AnnounceSnapshot{Entries: result}
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
