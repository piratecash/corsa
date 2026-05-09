package routing

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestBuildAnnounceSnapshot_DeduplicatesNextHopVariants(t *testing.T) {
	// Several route entries with different NextHop but same (identity, origin, seq)
	// should collapse into one announce entry after aggregation.
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 3},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 3},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 4},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry after dedup, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Hops != 3 {
		t.Fatalf("expected min hops=3, got %d", snap.Entries[0].Hops)
	}
}

func TestBuildAnnounceSnapshot_MaxSeqWins(t *testing.T) {
	// For one (identity, origin), after stage 2 there may be entries with
	// different seq. Stage 3 should keep only the max seq.
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 3, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 7, Hops: 4},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].SeqNo != 7 {
		t.Fatalf("expected max seq=7, got %d", snap.Entries[0].SeqNo)
	}
}

func TestBuildAnnounceSnapshot_MinHopsSelected(t *testing.T) {
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 5},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 3},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Hops != 2 {
		t.Fatalf("expected min hops=2, got %d", snap.Entries[0].Hops)
	}
}

func TestBuildAnnounceSnapshot_ExtraTieBreak(t *testing.T) {
	// With same (identity, origin, seq, hops) but different extra,
	// the entry with larger canonical extra should win.
	extraA := json.RawMessage(`{"a":1}`)
	extraB := json.RawMessage(`{"b":2}`)

	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: extraA},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: extraB},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	// {"b":2} > {"a":1} lexicographically
	if string(snap.Entries[0].Extra) != `{"b":2}` {
		t.Fatalf("expected extra={\"b\":2}, got %s", string(snap.Entries[0].Extra))
	}
}

func TestBuildAnnounceSnapshot_NilVsNullExtraEquivalent(t *testing.T) {
	// Extra=nil and Extra=json.RawMessage("null") should produce
	// identical canonical entries and identical snapshots.
	raw1 := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: nil},
	}
	raw2 := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: json.RawMessage("null")},
	}

	snap1 := BuildAnnounceSnapshot(raw1)
	snap2 := BuildAnnounceSnapshot(raw2)

	if !snap1.Equal(snap2) {
		t.Fatal("snapshots with nil and \"null\" Extra should be equal")
	}
}

func TestBuildAnnounceSnapshot_NilExtraLosesToNonNil(t *testing.T) {
	// nil extra < non-nil extra in tie-break
	extra := json.RawMessage(`{"key":"val"}`)
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: nil},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2, Extra: extra},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Extra == nil {
		t.Fatal("expected non-nil extra to win over nil")
	}
}

func TestBuildAnnounceSnapshot_DeterministicSort(t *testing.T) {
	// Stage 4 of BuildAnnounceSnapshot may emit MULTIPLE entries per
	// Identity (one live winner across origin lineages + zero or more
	// own-origin tombstones preserved alongside — see
	// _TombstonePreservedAlongsideLive for the multi-entry case). This
	// test focuses specifically on the all-live input path: every
	// candidate has Hops < HopsInfinity, so the tombstone-passthrough
	// branch is empty and only the live-winner collapse fires. The
	// resulting snapshot here happens to have one entry per Identity,
	// which makes the deterministic-sort and tie-break invariants
	// easy to read.
	//
	// Pins:
	//   1. result is sorted by (Identity, Hops, Origin) — Identity
	//      ascending; within an Identity, live winners (Hops ≤ 15)
	//      precede tombstones (Hops = 16); Origin lex tie-break;
	//   2. when multiple origins tie completely on (Hops, SeqNo, Extra),
	//      the deterministic tie-break in isBetterAnnounceEntry picks
	//      the lower Origin lexicographically — same input always
	//      produces the same wire output.
	raw := []AnnounceEntry{
		{Identity: "zzz", Origin: "aaa", SeqNo: 1, Hops: 1},
		{Identity: "aaa", Origin: "zzz", SeqNo: 1, Hops: 1},
		{Identity: "aaa", Origin: "aaa", SeqNo: 1, Hops: 1},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 2 {
		t.Fatalf("expected 2 entries after per-Identity aggregation, got %d", len(snap.Entries))
	}
	// Sorted by Identity. Identity "aaa" had two competing origins
	// (aaa, zzz) tied on hops=1, seq=1, no Extra → lower Origin wins → "aaa".
	if snap.Entries[0].Identity != "aaa" || snap.Entries[0].Origin != "aaa" {
		t.Fatalf("first entry should be (Identity=aaa, Origin=aaa) by per-Identity tie-break, got (%s,%s)",
			snap.Entries[0].Identity, snap.Entries[0].Origin)
	}
	// Identity "zzz" had one entry → unchanged.
	if snap.Entries[1].Identity != "zzz" || snap.Entries[1].Origin != "aaa" {
		t.Fatalf("second entry should be (Identity=zzz, Origin=aaa), got (%s,%s)",
			snap.Entries[1].Identity, snap.Entries[1].Origin)
	}

	// Run the build again — output must be byte-identical (deterministic
	// across map iteration orders).
	for i := 0; i < 5; i++ {
		again := BuildAnnounceSnapshot(raw)
		if !snap.Equal(again) {
			t.Fatalf("non-deterministic output across runs (iteration %d)", i)
		}
	}
}

func TestBuildAnnounceSnapshot_EmptyInput(t *testing.T) {
	snap := BuildAnnounceSnapshot(nil)
	if snap == nil {
		t.Fatal("expected non-nil snapshot for empty input")
	}
	if len(snap.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(snap.Entries))
	}
}

func TestAnnounceSnapshot_EqualNilSnapshots(t *testing.T) {
	var a, b *AnnounceSnapshot
	if !a.Equal(b) {
		t.Fatal("two nil snapshots should be equal")
	}
}

func TestAnnounceSnapshot_NilVsNonNilNotEqual(t *testing.T) {
	var a *AnnounceSnapshot
	b := &AnnounceSnapshot{}
	if a.Equal(b) {
		t.Fatal("nil and non-nil snapshot should not be equal")
	}
}

func TestComputeDelta_OriginChangeSameIdentityEmitsAsChange(t *testing.T) {
	// ComputeDelta indexes oldSnap by the lineage tuple `(Identity,
	// Origin)`, NOT by Identity alone. The (Identity, Origin) keying
	// is required because Stage 4 may put multiple entries for a
	// single Identity into the snapshot (one live winner + zero or
	// more own-origin tombstones), and Identity-alone keying would
	// collapse those into a single oldIdx slot, losing the per-lineage
	// change-detection that the tombstone-retry path relies on.
	//
	// This test exercises the live-winner-Origin-shift case: between
	// two cycles the winning Origin for `dest1` changes from `originA`
	// (hops=3) to `originB` (hops=1). ComputeDelta sees the new
	// `(dest1, originB)` key — not in oldIdx — and emits the fresh
	// winner. The old `(dest1, originA)` lineage from us is silently
	// dropped from subsequent snapshots; receivers' stale
	// `(dest1, originA)` buckets TTL-expire on the standard 120 s
	// schedule (absence is not implicit withdrawal — see routing.md).
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "originA", SeqNo: 5, Hops: 3},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "originB", SeqNo: 7, Hops: 1},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry on Origin change, got %d", len(delta))
	}
	if delta[0].Origin != "originB" || delta[0].Hops != 1 {
		t.Fatalf("expected new winning Origin=originB Hops=1 in delta, got Origin=%s Hops=%d",
			delta[0].Origin, delta[0].Hops)
	}
}

func TestComputeDelta_NewEntry(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
			{Identity: "dest2", Origin: "origin2", SeqNo: 1, Hops: 3},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
	if delta[0].Identity != "dest2" {
		t.Fatalf("expected dest2, got %s", delta[0].Identity)
	}
}

func TestComputeDelta_ChangedSeqNo(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 2, Hops: 2},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
}

func TestComputeDelta_ChangedHops(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 5},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 3},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected 1 delta entry, got %d", len(delta))
	}
}

func TestComputeDelta_NoChange(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 0 {
		t.Fatalf("expected 0 delta entries, got %d", len(delta))
	}
}

func TestComputeDelta_NoChangeMultiEntrySameIdentity(t *testing.T) {
	// Pins the (Identity, Origin) keying invariant for ComputeDelta in
	// the multi-entry-per-Identity scenario: when both old and new
	// snapshots carry the SAME live winner + own-origin tombstone for
	// the same Identity, the delta MUST be empty. No-op suppression
	// downstream depends on this — without it every announce cycle
	// during a stable disconnect-with-alternate window would re-emit
	// both entries unnecessarily, defeating no-op suppression.
	//
	// This test would silently break under a regression to «ComputeDelta
	// keys on Identity alone»: the oldIdx map would collapse the two
	// entries (live winner, tombstone) for the same Identity into a
	// single slot — only the last-iterated old entry would be remembered,
	// and the new snapshot's other entry (whichever was overwritten in
	// the index) would be flagged as «not found in oldIdx» and emitted
	// as a fresh delta entry. By keying ComputeDelta on (Identity,
	// Origin), each lineage is tracked independently and the delta
	// correctly comes out empty.
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			// Live winner via somePeer.
			{Identity: "dest", Origin: "somePeer", SeqNo: 12, Hops: 3},
			// Own-origin tombstone for the same Identity.
			{Identity: "dest", Origin: "ourselves", SeqNo: 7, Hops: HopsInfinity},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest", Origin: "somePeer", SeqNo: 12, Hops: 3},
			{Identity: "dest", Origin: "ourselves", SeqNo: 7, Hops: HopsInfinity},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 0 {
		t.Fatalf("expected empty delta when both snapshots carry identical "+
			"live winner + tombstone for same Identity (regression for "+
			"Identity-only delta keying), got %d entries: %+v",
			len(delta), delta)
	}
}

func TestComputeDelta_TombstoneAdvancesIndependentlyFromLiveWinner(t *testing.T) {
	// Sibling test to _NoChangeMultiEntrySameIdentity: when the live
	// winner stays unchanged but the own-origin tombstone advances
	// (e.g. SeqNo bump on subsequent withdrawal of a re-added route),
	// only the tombstone entry should appear in the delta — NOT the
	// live winner.
	//
	// Identity-only delta keying would collapse both entries to a
	// single slot in oldIdx; the change-detection compare would then
	// see whichever entry happened to be indexed last as «changed»
	// (Origin / SeqNo / Hops differ from the new-snapshot entry that
	// won the same map slot), and emit BOTH entries spuriously. With
	// (Identity, Origin) keying each lineage's change is tracked
	// independently — only the actually-changed tombstone is in the
	// delta.
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest", Origin: "somePeer", SeqNo: 12, Hops: 3},
			{Identity: "dest", Origin: "ourselves", SeqNo: 5, Hops: HopsInfinity},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest", Origin: "somePeer", SeqNo: 12, Hops: 3},
			// Tombstone advanced (re-disconnect after re-add).
			{Identity: "dest", Origin: "ourselves", SeqNo: 9, Hops: HopsInfinity},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 1 {
		t.Fatalf("expected exactly 1 delta entry (advanced tombstone only, live winner unchanged), got %d: %+v",
			len(delta), delta)
	}
	if delta[0].Origin != "ourselves" || delta[0].SeqNo != 9 || delta[0].Hops != HopsInfinity {
		t.Fatalf("expected delta to contain advanced tombstone (Origin=ourselves SeqNo=9 Hops=%d), got %+v",
			HopsInfinity, delta[0])
	}
}

func TestComputeDelta_RemovedEntryNotInDelta(t *testing.T) {
	// Entry in old but not in new — should NOT appear in delta
	// (absence is not implicit withdrawal).
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
			{Identity: "dest2", Origin: "origin2", SeqNo: 1, Hops: 3},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 0 {
		t.Fatalf("expected 0 delta entries for removed route, got %d", len(delta))
	}
}

func TestNormalizeExtra(t *testing.T) {
	tests := []struct {
		name    string
		input   json.RawMessage
		wantNil bool
	}{
		{"nil", nil, true},
		{"empty", json.RawMessage{}, true},
		{"null_literal", json.RawMessage("null"), true},
		{"null_with_spaces", json.RawMessage("  null  "), true},
		{"valid_json", json.RawMessage(`{"key":"val"}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeExtra(tt.input)
			if tt.wantNil && result != nil {
				t.Fatalf("expected nil, got %s", string(result))
			}
			if !tt.wantNil && result == nil {
				t.Fatal("expected non-nil")
			}
		})
	}
}

func TestCompareExtra(t *testing.T) {
	tests := []struct {
		name string
		a, b json.RawMessage
		want int
	}{
		{"nil_nil", nil, nil, 0},
		{"nil_nonempty", nil, json.RawMessage(`{}`), -1},
		{"nonempty_nil", json.RawMessage(`{}`), nil, 1},
		{"null_nil", json.RawMessage("null"), nil, 0},
		{"equal", json.RawMessage(`{"a":1}`), json.RawMessage(`{"a":1}`), 0},
		{"a_less", json.RawMessage(`{"a":1}`), json.RawMessage(`{"b":2}`), -1},
		{"a_greater", json.RawMessage(`{"b":2}`), json.RawMessage(`{"a":1}`), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareExtra(tt.a, tt.b)
			if got != tt.want {
				t.Fatalf("compareExtra: expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestComputeDelta_NilOldSnapTreatsAllAsNew(t *testing.T) {
	// Defensive nil guard: when oldSnap is nil, every entry in newSnap
	// should be returned as delta (equivalent to a full send).
	newSnap := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
			{Identity: "dest2", Origin: "origin2", SeqNo: 3, Hops: 1},
		},
	}

	delta := ComputeDelta(nil, newSnap)
	if len(delta) != 2 {
		t.Fatalf("expected 2 delta entries for nil oldSnap, got %d", len(delta))
	}
}

func TestComputeDelta_NilNewSnapReturnsNil(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "origin1", SeqNo: 1, Hops: 2},
		},
	}

	delta := ComputeDelta(old, nil)
	if delta != nil {
		t.Fatalf("expected nil delta for nil newSnap, got %d entries", len(delta))
	}
}

func TestNormalizeExtra_WhitespaceStripped(t *testing.T) {
	// Surrounding whitespace must be stripped so that " {\"a\":1} " and
	// "{\"a\":1}" produce the same canonical form.
	a := json.RawMessage(`  {"a":1}  `)
	b := json.RawMessage(`{"a":1}`)

	na := normalizeExtra(a)
	nb := normalizeExtra(b)

	if !normalizedExtraEqual(na, nb) {
		t.Fatalf("whitespace-surrounded and clean Extra should be equal: %q vs %q", na, nb)
	}
}

func TestBuildAnnounceSnapshot_PerIdentityAggregationCollapsesOrigins(t *testing.T) {
	// Verifies the stage-4 per-Identity aggregation hot-fix (classical
	// distance-vector «best route per destination» announce semantic).
	// Multiple Origin lineages for the same Identity collapse to a
	// single winner picked by min Hops, then higher SeqNo, then Extra
	// canonical compare, then deterministic Origin lex order.
	//
	// Field-data motivation: routes.json snapshot showed avg 12.7
	// origins per identity → 1440 (Identity, Origin) buckets vs 113
	// distinct destinations. Stage 4 collapses 1440 wire entries to
	// ~113 per cycle (12.7× outbound reduction).
	raw := []AnnounceEntry{
		// Two paths to dest1 via origin1 — different hops, same SeqNo
		// (stage 2 picks min hops within a (Identity, Origin, SeqNo)
		// bucket).
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 4},
		// dest1 via origin2 with shorter path (hops=1) — wins stage 4.
		{Identity: "dest1", Origin: "origin2", SeqNo: 3, Hops: 1},
		// Different dest entirely — survives stage 4 trivially.
		{Identity: "dest2", Origin: "origin1", SeqNo: 1, Hops: 3},
	}

	snap := BuildAnnounceSnapshot(raw)

	// After stage 4: 2 distinct identities → 2 entries. The per-Origin
	// granularity is gone from wire output (it remains observable
	// inside Table.routes for receive-side cap/SeqNo bookkeeping).
	if len(snap.Entries) != 2 {
		t.Fatalf("expected 2 entries after per-Identity aggregation, got %d", len(snap.Entries))
	}

	byIdentity := make(map[PeerIdentity]AnnounceEntry, len(snap.Entries))
	for _, e := range snap.Entries {
		byIdentity[e.Identity] = e
	}

	// dest1 winner: origin2 (hops=1 beats origin1's hops=2).
	d1, ok := byIdentity["dest1"]
	if !ok {
		t.Fatal("missing dest1 in aggregated snapshot")
	}
	if d1.Origin != "origin2" {
		t.Fatalf("expected dest1 winner Origin=origin2 (shorter hops), got Origin=%s", d1.Origin)
	}
	if d1.Hops != 1 {
		t.Fatalf("expected dest1 winner Hops=1, got %d", d1.Hops)
	}
	if d1.SeqNo != 3 {
		t.Fatalf("expected dest1 winner SeqNo=3 (origin2's), got %d", d1.SeqNo)
	}

	// dest2 unchanged.
	d2, ok := byIdentity["dest2"]
	if !ok {
		t.Fatal("missing dest2 in aggregated snapshot")
	}
	if d2.Origin != "origin1" || d2.Hops != 3 || d2.SeqNo != 1 {
		t.Fatalf("dest2 should be unchanged (origin1, hops=3, seq=1), got (%s, %d, %d)",
			d2.Origin, d2.Hops, d2.SeqNo)
	}
}

func TestBuildAnnounceSnapshot_PerIdentityTieBreakBySeqNoAcrossOrigins(t *testing.T) {
	// When multiple Origin lineages tie on Hops, stage-4 prefers higher
	// SeqNo. SeqNo across origins is just a freshness heuristic — origins
	// advance their counters independently, so this is NOT a monotonicity
	// claim; it's the secondary tie-break documented in
	// isBetterAnnounceEntry.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "origA", SeqNo: 5, Hops: 2},
		{Identity: "dest", Origin: "origB", SeqNo: 12, Hops: 2}, // newer SeqNo, same hops — wins.
		{Identity: "dest", Origin: "origC", SeqNo: 8, Hops: 2},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry after per-Identity aggregation, got %d", len(snap.Entries))
	}
	got := snap.Entries[0]
	if got.Origin != "origB" || got.SeqNo != 12 {
		t.Fatalf("expected Origin=origB SeqNo=12 (highest SeqNo on hops tie), got Origin=%s SeqNo=%d",
			got.Origin, got.SeqNo)
	}
}

func TestBuildAnnounceSnapshot_PerIdentityTieBreakByExtraAcrossOrigins(t *testing.T) {
	// When (Hops, SeqNo) tie across origins, isBetterAnnounceEntry's
	// THIRD-priority tie-break picks the entry with the
	// lexicographically larger canonical Extra (the same compareExtra
	// helper used in stage 2 within a single (Identity, Origin)
	// lineage). This pins the per-Identity Extra tie-break ordering
	// for stage 4 explicitly — earlier tests cover stage 2's
	// (Identity, Origin, SeqNo) Extra disambiguation and stage 4's
	// (Hops, SeqNo, Origin) priorities, but not stage 4's Extra
	// position between SeqNo and Origin.
	//
	// Test setup: three competing origin lineages tied on Hops=2 and
	// SeqNo=5, with distinct Extra blobs. Origin lex order would pick
	// `aaa` (lowest), but `compareExtra > 0` is checked BEFORE the
	// Origin tie-break, so the entry whose Extra is lexicographically
	// largest must win. `{"z":1}` > `{"a":1}` and `{"m":1}` byte-wise.
	extraA := json.RawMessage(`{"a":1}`)
	extraM := json.RawMessage(`{"m":1}`)
	extraZ := json.RawMessage(`{"z":1}`)

	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "aaa", SeqNo: 5, Hops: 2, Extra: extraA},
		{Identity: "dest", Origin: "bbb", SeqNo: 5, Hops: 2, Extra: extraZ}, // largest Extra — wins
		{Identity: "dest", Origin: "ccc", SeqNo: 5, Hops: 2, Extra: extraM},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	got := snap.Entries[0]
	if got.Origin != "bbb" {
		t.Fatalf("expected Origin=bbb (largest Extra `{\"z\":1}` wins Stage 4 Extra tie-break before Origin lex), got Origin=%s",
			got.Origin)
	}
	if !normalizedExtraEqual(got.Extra, extraZ) {
		t.Fatalf("expected winning Extra=%s, got Extra=%s", extraZ, got.Extra)
	}
}

func TestBuildAnnounceSnapshot_PerIdentityTieBreakByOriginLexicographic(t *testing.T) {
	// When (Hops, SeqNo, Extra) all tie across origins, the deterministic
	// final tie-break in isBetterAnnounceEntry picks the lower Origin
	// lexicographically. This pins stable wire output across map
	// iteration orders — without it successive snapshot builds could
	// pick different origins for the same destination, flapping no-op
	// suppression and triggering redundant delta sends.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "ccc", SeqNo: 1, Hops: 2},
		{Identity: "dest", Origin: "aaa", SeqNo: 1, Hops: 2}, // lower Origin — wins by final tie-break.
		{Identity: "dest", Origin: "bbb", SeqNo: 1, Hops: 2},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Origin != "aaa" {
		t.Fatalf("expected lower Origin=aaa to win deterministic tie-break, got %s",
			snap.Entries[0].Origin)
	}
}

func TestBuildAnnounceSnapshot_TombstonePreservedAlongsideLive(t *testing.T) {
	// P1 review fix (round 16.x): own-origin withdrawal tombstones MUST
	// be preserved in the snapshot alongside the live winner for the
	// same Identity. Pre-fix Stage 4 collapsed by min Hops alone, so
	// HopsInfinity (=16) lost to any live entry's Hops <= 15 — tombstone
	// silently dropped from the snapshot. That broke the retry-via-delta
	// promise for own-origin withdrawals: peers where the immediate
	// disconnect fanout failed kept their stale (Identity, ourselves)
	// bucket pointing at us until natural TTL.
	//
	// Post-fix: tombstones bypass the live-winner collapse and ride
	// alongside in the snapshot. Both propagate to receivers.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "ourselves", SeqNo: 10, Hops: HopsInfinity}, // own-origin tombstone
		{Identity: "dest", Origin: "somePeer", SeqNo: 5, Hops: 4},              // live alternate
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 2 {
		t.Fatalf("expected 2 entries (live winner + tombstone), got %d: %+v", len(snap.Entries), snap.Entries)
	}

	// Sort order: Identity ASC, Hops ASC, Origin ASC.
	// Live winner (Hops=4) precedes tombstone (Hops=16).
	live := snap.Entries[0]
	if live.Hops != 4 || live.Origin != "somePeer" {
		t.Fatalf("expected live winner first: Origin=somePeer Hops=4, got Origin=%s Hops=%d",
			live.Origin, live.Hops)
	}

	tomb := snap.Entries[1]
	if tomb.Hops != HopsInfinity || tomb.Origin != "ourselves" || tomb.SeqNo != 10 {
		t.Fatalf("expected own-origin tombstone preserved: Origin=ourselves Hops=%d SeqNo=10, got Origin=%s Hops=%d SeqNo=%d",
			HopsInfinity, tomb.Origin, tomb.Hops, tomb.SeqNo)
	}
}

func TestBuildAnnounceSnapshot_TombstoneEmittedWhenNoLiveAlternate(t *testing.T) {
	// When the only entry for an Identity is an own-origin tombstone
	// (no live alternate path remains), the snapshot still carries the
	// tombstone — receivers learn the route is gone. This is the
	// non-edge case of the P1 fix: tombstone-only snapshot entry.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "ourselves", SeqNo: 10, Hops: HopsInfinity},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 tombstone entry, got %d", len(snap.Entries))
	}
	got := snap.Entries[0]
	if got.Hops != HopsInfinity || got.Origin != "ourselves" {
		t.Fatalf("expected tombstone (Origin=ourselves Hops=%d), got (%s, %d)",
			HopsInfinity, got.Origin, got.Hops)
	}
}

func TestBuildAnnounceSnapshot_TombstoneAndLiveSurviveSplitHorizonAndAggregation(t *testing.T) {
	// Realistic scenario: this node was directly connected to dest, then
	// disconnected — table holds (a) own-origin tombstone for dest, and
	// (b) live announcement-learned alternate via somePeer. Both must
	// reach the receiver so:
	//   - tombstone updates receiver's (dest, ourselves) bucket → withdrawn
	//   - live alternate refreshes receiver's (dest, somePeer) bucket
	// Receiver's Lookup then picks the alternate path, no longer
	// preferring the now-stale (dest, ourselves) → us → dest route.
	raw := []AnnounceEntry{
		// Own-origin tombstone (post-disconnect from dest):
		{Identity: "dest", Origin: "ourselves", SeqNo: 7, Hops: HopsInfinity},
		// Live transit-learned alternate (still in table from earlier announcement):
		{Identity: "dest", Origin: "somePeer", SeqNo: 12, Hops: 3},
		// Another live alternate via different origin (we knew before disconnect):
		{Identity: "dest", Origin: "anotherPeer", SeqNo: 8, Hops: 5},
		// Different identity — sanity check that aggregation still works:
		{Identity: "other", Origin: "somePeer", SeqNo: 1, Hops: 2},
	}

	snap := BuildAnnounceSnapshot(raw)
	// Expected:
	//   - dest: 1 live winner (somePeer, hops=3 beats anotherPeer hops=5) + 1 tombstone
	//   - other: 1 live winner
	if len(snap.Entries) != 3 {
		t.Fatalf("expected 3 entries (dest live + dest tombstone + other live), got %d: %+v",
			len(snap.Entries), snap.Entries)
	}

	// Sort: dest first (lex < other), within dest: live (hops=3) before tombstone (hops=16).
	if snap.Entries[0].Identity != "dest" || snap.Entries[0].Hops != 3 || snap.Entries[0].Origin != "somePeer" {
		t.Fatalf("expected dest live winner first (Origin=somePeer Hops=3), got %+v", snap.Entries[0])
	}
	if snap.Entries[1].Identity != "dest" || snap.Entries[1].Hops != HopsInfinity || snap.Entries[1].Origin != "ourselves" {
		t.Fatalf("expected dest tombstone second (Origin=ourselves Hops=%d), got %+v",
			HopsInfinity, snap.Entries[1])
	}
	if snap.Entries[2].Identity != "other" || snap.Entries[2].Origin != "somePeer" {
		t.Fatalf("expected other live winner third, got %+v", snap.Entries[2])
	}
}

func TestBuildAnnounceSnapshot_MultipleTombstonesSameIdentityAllPreserved(t *testing.T) {
	// Defensive: in production Table.AnnounceTo guarantees that
	// tombstones reaching BuildAnnounceSnapshot are own-origin only,
	// and there is exactly one own-origin per Identity (localOrigin is
	// unique to this node). So in practice there is at most ONE
	// tombstone per Identity in the input.
	//
	// This test validates the defensive case where multiple tombstone
	// entries with different Origins reach Stage 4: they are all
	// preserved (none silently dropped). The test pins the contract,
	// not the production scenario.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "origA", SeqNo: 7, Hops: HopsInfinity},
		{Identity: "dest", Origin: "origB", SeqNo: 9, Hops: HopsInfinity},
		{Identity: "dest", Origin: "origC", SeqNo: 5, Hops: HopsInfinity},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 3 {
		t.Fatalf("expected all 3 tombstones preserved (no live alternate to collapse against), got %d", len(snap.Entries))
	}
	// All entries should be tombstones (Hops=Infinity), sorted by Origin lex.
	expectedOrigins := []PeerIdentity{"origA", "origB", "origC"}
	for i, want := range expectedOrigins {
		if snap.Entries[i].Hops != HopsInfinity {
			t.Fatalf("entry %d: expected Hops=HopsInfinity, got %d", i, snap.Entries[i].Hops)
		}
		if snap.Entries[i].Origin != want {
			t.Fatalf("entry %d: expected Origin=%s (sorted by lex), got Origin=%s",
				i, want, snap.Entries[i].Origin)
		}
	}
}

func TestBuildAnnounceSnapshot_FieldDataInflationCollapse(t *testing.T) {
	// Regression test pinning the wire-volume reduction motivation
	// behind the hot-fix. Synthesizes a routes.json-style scenario:
	// 113 destinations × 12 origins per destination × 4 hops variations
	// = 5424 raw entries. After full aggregation pipeline:
	//
	//   - Stage 1 dedups exact duplicates.
	//   - Stages 2-3 collapse to one entry per (Identity, Origin) pair → 1356.
	//   - Stage 4 collapses to one entry per Identity → 113.
	//
	// The 12.7× compression ratio is the operator-visible win.
	const (
		identityCount      = 113
		originsPerIdentity = 12
		hopsVariants       = 4
	)

	raw := make([]AnnounceEntry, 0, identityCount*originsPerIdentity*hopsVariants)
	for i := 0; i < identityCount; i++ {
		identity := PeerIdentity(formatHex16(i, "id"))
		for o := 0; o < originsPerIdentity; o++ {
			origin := PeerIdentity(formatHex16(o, "or"))
			for h := 1; h <= hopsVariants; h++ {
				raw = append(raw, AnnounceEntry{
					Identity: identity,
					Origin:   origin,
					SeqNo:    uint64(o + 1), // origin-stable seq
					Hops:     h,
				})
			}
		}
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != identityCount {
		t.Fatalf("expected %d entries (one per Identity after stage 4), got %d",
			identityCount, len(snap.Entries))
	}

	// Every entry should be hops=1 (best across all variants for any origin).
	for _, e := range snap.Entries {
		if e.Hops != 1 {
			t.Fatalf("expected aggregated entry Hops=1, got %d for Identity=%s", e.Hops, e.Identity)
		}
	}
}

// formatHex16 produces a stable 16-byte synthetic identity for test data.
func formatHex16(idx int, prefix string) string {
	return prefix + "00000000000000" + string(rune('a'+idx%26)) + string(rune('a'+(idx/26)%26))
}

func TestBuildAnnounceSnapshot_ExtraMismatchWarning(t *testing.T) {
	// When entries share (identity, origin, seq) but carry different Extra,
	// the invariant is violated (origin sets Extra once; relay corruption or
	// protocol mismatch). BuildAnnounceSnapshot must:
	//   1. Still produce a deterministic winner via tie-break.
	//   2. Emit an announce_aggregation_extra_mismatch diagnostic warning.

	// Capture zerolog output.
	var buf bytes.Buffer
	origLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Logger()
	defer func() { log.Logger = origLogger }()

	extraA := json.RawMessage(`{"a":1}`)
	extraB := json.RawMessage(`{"b":2}`)

	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 10, Hops: 3, Extra: extraA},
		{Identity: "dest1", Origin: "origin1", SeqNo: 10, Hops: 3, Extra: extraB},
	}

	snap := BuildAnnounceSnapshot(raw)

	// Verify deterministic aggregation result.
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry after aggregation, got %d", len(snap.Entries))
	}
	// {"b":2} > {"a":1} lexicographically — should win tie-break.
	if string(snap.Entries[0].Extra) != `{"b":2}` {
		t.Fatalf("expected tie-break winner {\"b\":2}, got %s", string(snap.Entries[0].Extra))
	}

	// Verify diagnostic warning was emitted.
	logged := buf.String()
	if !bytes.Contains([]byte(logged), []byte("announce_aggregation_extra_mismatch")) {
		t.Fatalf("expected announce_aggregation_extra_mismatch warning in log, got: %s", logged)
	}
}
