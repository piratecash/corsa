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

func TestBuildAnnounceSnapshot_HigherSeqWinsOnHopsTie(t *testing.T) {
	// Pre-A1 this test pinned Stage 3's "max SeqNo within (Identity,
	// Origin)" invariant. Stage 3 was removed in A1 round 3 (it
	// collapsed live + tombstone for the same Identity under
	// Origin = localOrigin constant — see announce_builder.go
	// AnnounceSnapshot doc-comment). The per-Identity stage now
	// decides via isBetterAnnounceEntry: min Hops first, higher SeqNo
	// as the Hops tie-break.
	//
	// The Hops-different version of this test was misleading: it
	// suggested SeqNo dominates Hops. That was never the routing
	// invariant — pre-A1 it just happened to read as max-SeqNo
	// because Stage 3 ran BEFORE Stage 4's Hops comparison, and
	// inputs in this test had different SeqNos so Stage 3's max-SeqNo
	// rule picked one before Stage 4 saw Hops. Post-A1 we collapse
	// in one step and Hops wins outright.
	//
	// Fixture redesigned to actually exercise the SeqNo tie-break:
	// inputs share Hops, so the higher SeqNo wins the per-Identity
	// comparison.
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 3, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 7, Hops: 2},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].SeqNo != 7 {
		t.Fatalf("expected max seq=7 (Hops tie → higher SeqNo wins), got %d", snap.Entries[0].SeqNo)
	}
}

func TestBuildAnnounceSnapshot_MinHopsBeatsHigherSeq(t *testing.T) {
	// Companion to TestBuildAnnounceSnapshot_HigherSeqWinsOnHopsTie:
	// pins the contract that Hops dominates SeqNo in the per-Identity
	// stage. Lower Hops wins even though the loser has the higher
	// SeqNo. This is the classical DV "shorter path is better"
	// invariant — never broken by Stage 3 pre-A1 either (Stage 3
	// only ran when SeqNo differed AND Hops differed for the same
	// (Identity, Origin); the resulting single survivor then went
	// through Stage 4's Hops comparison anyway). Post-A1 the
	// Hops-first decision is just visible without the Stage 3
	// intermediate.
	raw := []AnnounceEntry{
		{Identity: "dest1", Origin: "origin1", SeqNo: 3, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 7, Hops: 4},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap.Entries))
	}
	if snap.Entries[0].Hops != 2 {
		t.Fatalf("expected min hops=2 (Hops dominates SeqNo), got %d", snap.Entries[0].Hops)
	}
	if snap.Entries[0].SeqNo != 3 {
		t.Fatalf("expected the lower-Hops entry's SeqNo=3, got %d", snap.Entries[0].SeqNo)
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

// TestComputeDelta_FieldChangeSameIdentityEmits exercises the
// "fields differ between cycles" path of ComputeDelta. Post-A1 the
// delta is keyed by `(Identity, IsWithdrawn)` — see the
// ComputeDelta doc-comment and the announceTrackKey type — so an
// Origin shift on its own does NOT cross tracking-key boundaries.
// What DOES drive the emit here is the SeqNo / Hops diff that the
// field-equality check inside ComputeDelta detects against the
// cached old entry under the same (dest1, withdrawn=false) slot.
//
// The Origin field changes in this fixture too (`originA →
// originB`) — that mirrors a real pre-A1 transit shifting its
// per-Identity winner across Origin lineages — but it is NOT what
// the delta path triggers on. For the Origin-only invariant pin
// see TestComputeDelta_OriginOnlyDiffNoEmit below.
func TestComputeDelta_FieldChangeSameIdentityEmits(t *testing.T) {
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
		t.Fatalf("expected 1 delta entry on SeqNo / Hops change, got %d", len(delta))
	}
	if delta[0].Origin != "originB" || delta[0].Hops != 1 || delta[0].SeqNo != 7 {
		t.Fatalf("expected fresh entry (Origin=originB Hops=1 SeqNo=7) in delta, got Origin=%s Hops=%d SeqNo=%d",
			delta[0].Origin, delta[0].Hops, delta[0].SeqNo)
	}
}

// TestComputeDelta_OriginOnlyDiffNoEmit pins the sibling invariant
// that emerged from the post-A1 `(Identity, IsWithdrawn)` tracking
// key shift: a snapshot delta whose ONLY difference vs. the cached
// previous snapshot is the Origin field — with SeqNo / Hops /
// Withdrawn / Extra all identical — MUST NOT produce a delta
// entry. Pre-A1 this was an "emit" case (the old (Identity, Origin)
// key would have classified the new Origin as a fresh tracking
// slot); post-A1 the tracking key drops Origin and the field-
// equality check below the key match treats Origin-only diffs as
// noise (Origin = localOrigin on every post-A1 wire emit, so it
// shouldn't move when storage stays the same).
func TestComputeDelta_OriginOnlyDiffNoEmit(t *testing.T) {
	old := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "originA", SeqNo: 5, Hops: 3},
		},
	}
	new := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{Identity: "dest1", Origin: "originB", SeqNo: 5, Hops: 3},
		},
	}

	delta := ComputeDelta(old, new)
	if len(delta) != 0 {
		t.Fatalf("Origin-only diff with identical SeqNo / Hops MUST produce empty delta post-A1; got %d entries: %+v", len(delta), delta)
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
	// Pins the (Identity, IsWithdrawn) keying invariant for
	// ComputeDelta in the multi-entry-per-Identity scenario: when
	// both old and new snapshots carry the SAME live winner +
	// own-origin tombstone for the same Identity, the delta MUST
	// be empty. No-op suppression downstream depends on this —
	// without it every announce cycle during a stable disconnect-
	// with-alternate window would re-emit both entries unnecessarily.
	//
	// This test would silently break under a regression to
	// «ComputeDelta keys on Identity alone»: the oldIdx map would
	// collapse live + tombstone for the same Identity into a single
	// slot — only the last-iterated old entry would survive, and
	// the new snapshot's other entry would be flagged as "not found
	// in oldIdx" and emitted as a fresh delta entry. Pre-A1 we
	// keyed on (Identity, Origin) — that worked because the live
	// winner and own-origin tombstone naturally carried different
	// Origin values. After A1 every emit carries Origin = localOrigin
	// (see uplink_claim.go migration contract), so (Identity, Origin)
	// would collapse and the key shifted to (Identity, IsWithdrawn).
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

// TestComputeDelta_IdempotentOnMultiTombstoneSnapshots reproduces the
// contract round-5i tightened: two identical synthetic snapshots,
// each with multiple tombstones per Identity in the RAW input, MUST
// yield an empty delta after collapse. Pre-collapse the snapshot
// preserved all tombstones verbatim and the (Identity, IsWithdrawn)
// keyed ComputeDelta would emit the non-surviving tombstones as
// "changed" on the second snapshot.
func TestComputeDelta_IdempotentOnMultiTombstoneSnapshots(t *testing.T) {
	raw := []AnnounceEntry{
		{Identity: "destX", Origin: "origA", SeqNo: 10, Hops: HopsInfinity},
		{Identity: "destX", Origin: "origB", SeqNo: 20, Hops: HopsInfinity},
		{Identity: "destX", Origin: "origC", SeqNo: 30, Hops: HopsInfinity},
	}

	snap1 := BuildAnnounceSnapshot(raw)
	snap2 := BuildAnnounceSnapshot(raw)

	delta := ComputeDelta(snap1, snap2)
	if len(delta) != 0 {
		t.Fatalf("identical snapshots must yield empty delta; got %d entries: %+v", len(delta), delta)
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

func TestBuildAnnounceSnapshot_MultipleTombstonesSameIdentityCollapse(t *testing.T) {
	// Defensive: in production Table.AnnounceTo guarantees that
	// tombstones reaching BuildAnnounceSnapshot are own-origin only,
	// and routeStore.AnnounceProjectionFor emits at most one
	// own-direct tombstone per Identity. So in practice there is
	// at most ONE tombstone per Identity in the input.
	//
	// Round-5i tightened the snapshot contract: if a synthetic input
	// (test fixture bypassing AnnounceProjectionFor) hands the
	// builder multiple tombstones for one Identity, Stage 3 collapses
	// them to a single winner under the live-winner ordering (min
	// Hops → higher SeqNo → Extra tie-break → Origin lex). This
	// keeps the snapshot consistent with ComputeDelta's
	// (Identity, IsWithdrawn) tracking key — see
	// TestComputeDelta_IdempotentOnMultiTombstoneSnapshots for the
	// follow-up contract.
	raw := []AnnounceEntry{
		{Identity: "dest", Origin: "origA", SeqNo: 7, Hops: HopsInfinity},
		{Identity: "dest", Origin: "origB", SeqNo: 9, Hops: HopsInfinity},
		{Identity: "dest", Origin: "origC", SeqNo: 5, Hops: HopsInfinity},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 1 {
		t.Fatalf("expected per-Identity tombstone collapse to one winner, got %d entries: %+v", len(snap.Entries), snap.Entries)
	}
	winner := snap.Entries[0]
	if winner.Hops != HopsInfinity {
		t.Fatalf("expected the surviving entry to be a tombstone (Hops=HopsInfinity), got Hops=%d", winner.Hops)
	}
	if winner.SeqNo != 9 {
		t.Fatalf("expected tombstone winner SeqNo=9 (highest among the three), got SeqNo=%d", winner.SeqNo)
	}
	if winner.Origin != "origB" {
		t.Fatalf("expected tombstone winner Origin=origB (matches SeqNo=9), got Origin=%s", winner.Origin)
	}
}

func TestBuildAnnounceSnapshot_FieldDataInflationCollapse(t *testing.T) {
	// Regression test pinning the wire-volume reduction motivation
	// behind the hot-fix. Synthesizes a routes.json-style scenario:
	// 113 destinations × 12 origins per destination × 4 hops variations
	// = 5424 raw entries. After the full aggregation pipeline:
	//
	//   - Stage 1 dedups exact duplicates.
	//   - Stage 2 collapses to one entry per (Identity, Origin, SeqNo,
	//     Withdrawn) — defensive dedup for synthetic test-fixture
	//     inputs that bypass AnnounceProjectionFor. The pre-A1
	//     per-(Identity, Origin) max-SeqNo stage (Stage 3) was
	//     removed in Phase 1 because `Origin = localOrigin` is now
	//     constant across all post-A1 emits.
	//   - The per-Identity collapse runs directly on Stage 2 output
	//     and collapses to one entry per Identity → 113.
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

// TestAnnounceTo_LiveSuppressesOwnTombstoneOnWire is the end-to-end
// integration test for the live-vs-tombstone exclusivity invariant
// in AnnounceProjectionFor (see route_store_lookup.go's doc-
// comment). Storage retains the own-direct tombstone after
// RemoveDirectPeer for the SeqNo-resurrection guard (ApplyUpdate
// uses it to reject stale lower-SeqNo replays on the same
// (Identity, Uplink) pair), but the wire emit must NOT trail the
// tombstone behind the live winner — nextOutboundSeqLockedPerPeer
// assigns the tombstone a strictly higher SeqNo than the live
// winner (peerMax has just advanced), so the receiver's
// monotonic-SeqNo timeline would apply the tombstone LAST and
// drop the Identity as unreachable despite us having a live
// backup uplink to offer.
//
// Fixture:
//
//   - Local node S (WithLocalOrigin "node-A") has a direct route
//     to X via AddDirectPeer.
//   - S also learned a transit route to X via uplink Y.
//   - S disconnects from X — own-direct claim becomes a
//     tombstone (Source=Direct, Withdrawn=true). The transit
//     claim via Y stays live.
//   - Storage holds two claims in bucket[X]: own-direct tombstone
//     (Uplink=X) + live transit (Uplink=Y).
//
// Expected snapshot for AnnounceTo("peer-Z"):
//
//   - Exactly ONE entry for peer-X: the live transit winner via
//     Y (Hops < HopsInfinity). The own-direct tombstone for X is
//     suppressed on the wire because a live alternative exists.
//   - ComputeDelta against an empty oldSnap emits only the live
//     entry — there is no tombstone slot to track.
func TestAnnounceTo_LiveSuppressesOwnTombstoneOnWire(t *testing.T) {
	const (
		nodeA  PeerIdentity = "node-A"
		peerX  PeerIdentity = "peer-X"
		peerY  PeerIdentity = "peer-Y"
		peerZ  PeerIdentity = "peer-Z"
		origin PeerIdentity = "some-foreign-origin"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X): %v", err)
	}

	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: origin, NextHop: peerY,
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != nil || status != RouteAccepted {
		t.Fatalf("UpdateRoute(peer-X via peer-Y) status=%v err=%v", status, err)
	}

	if _, err := tbl.RemoveDirectPeer(peerX); err != nil {
		t.Fatalf("RemoveDirectPeer(peer-X): %v", err)
	}

	raw := tbl.AnnounceTo(peerZ)
	snap := BuildAnnounceSnapshot(raw)

	var live, tombstone *AnnounceEntry
	for i := range snap.Entries {
		e := &snap.Entries[i]
		if e.Identity != peerX {
			continue
		}
		if e.Hops >= HopsInfinity {
			if tombstone != nil {
				t.Fatalf("multiple tombstones for peer-X in snapshot: %+v", snap.Entries)
			}
			tombstone = e
		} else {
			if live != nil {
				t.Fatalf("multiple live entries for peer-X in snapshot: %+v", snap.Entries)
			}
			live = e
		}
	}
	if live == nil {
		t.Fatalf("live entry for peer-X missing from snapshot: %+v", snap.Entries)
	}
	if tombstone != nil {
		t.Fatalf("tombstone for peer-X must be suppressed when live winner exists: %+v", snap.Entries)
	}

	delta := ComputeDelta(&AnnounceSnapshot{}, snap)
	var deltaLive, deltaTombstone bool
	for _, e := range delta {
		if e.Identity != peerX {
			continue
		}
		if e.Hops >= HopsInfinity {
			deltaTombstone = true
		} else {
			deltaLive = true
		}
	}
	if !deltaLive {
		t.Fatalf("ComputeDelta dropped live peer-X entry: %+v", delta)
	}
	if deltaTombstone {
		t.Fatalf("ComputeDelta surfaced suppressed tombstone for peer-X: %+v", delta)
	}
}

// TestAnnounceTo_TombstoneEmittedWithoutLiveAlternative pins the
// other half of the live-vs-tombstone exclusivity contract: when
// there is no live winner for an Identity (only an own-direct
// tombstone in storage), the tombstone IS emitted so the receiver
// learns the prior route is dead and evicts it from their table.
// Without this path the receiver would keep the live route in
// their table at hops=N until TTL elapsed — a several-minute window
// where send_message frames would be routed at us with no surviving
// uplink to forward them.
func TestAnnounceTo_TombstoneEmittedWithoutLiveAlternative(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X): %v", err)
	}
	// No transit route exists for peer-X — peer-X is reachable
	// ONLY via the direct claim. Disconnecting strips the live
	// claim and leaves the tombstone alone in storage.
	if _, err := tbl.RemoveDirectPeer(peerX); err != nil {
		t.Fatalf("RemoveDirectPeer(peer-X): %v", err)
	}

	raw := tbl.AnnounceTo(peerZ)
	snap := BuildAnnounceSnapshot(raw)

	var live, tombstone *AnnounceEntry
	for i := range snap.Entries {
		e := &snap.Entries[i]
		if e.Identity != peerX {
			continue
		}
		if e.Hops >= HopsInfinity {
			if tombstone != nil {
				t.Fatalf("multiple tombstones for peer-X: %+v", snap.Entries)
			}
			tombstone = e
		} else {
			if live != nil {
				t.Fatalf("unexpected live entry for peer-X (no alternative uplink): %+v", snap.Entries)
			}
			live = e
		}
	}
	if live != nil {
		t.Fatalf("unexpected live entry for peer-X (no alternative uplink): %+v", snap.Entries)
	}
	if tombstone == nil {
		t.Fatalf("own-direct tombstone for peer-X must be emitted when no live alternative exists: %+v", snap.Entries)
	}
}

// TestAnnounceTo_LiveDominatesTombstoneOnReceiverTimeline encodes
// the receiver-side correctness rationale for the live-vs-
// tombstone exclusivity invariant: the wire SeqNo of the emitted
// per-Identity content must let the receiver settle on the LIVE
// state for any Identity that has a live winner in our storage.
// Equivalent post-A1 framing: the highest SeqNo for the Identity
// in the emit must come from the live entry, never from a
// tombstone trailing it. The current implementation enforces this
// by suppressing the tombstone outright; this test exercises the
// invariant directly on the emitted wire entries so a future
// implementation that re-introduces tombstones (e.g. with a
// different ordering scheme) still has to honour the receiver-
// timeline contract.
func TestAnnounceTo_LiveDominatesTombstoneOnReceiverTimeline(t *testing.T) {
	const (
		nodeA  PeerIdentity = "node-A"
		peerX  PeerIdentity = "peer-X"
		peerY  PeerIdentity = "peer-Y"
		peerZ  PeerIdentity = "peer-Z"
		origin PeerIdentity = "some-foreign-origin"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X): %v", err)
	}
	if status, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: origin, NextHop: peerY,
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	}); err != nil || status != RouteAccepted {
		t.Fatalf("UpdateRoute(peer-X via peer-Y) status=%v err=%v", status, err)
	}
	if _, err := tbl.RemoveDirectPeer(peerX); err != nil {
		t.Fatalf("RemoveDirectPeer(peer-X): %v", err)
	}

	wire := tbl.AnnounceTo(peerZ)

	var liveSeqNo uint64
	var liveSeen, tombstoneSeen bool
	var maxSeqNo uint64
	var maxIsLive bool
	for _, e := range wire {
		if e.Identity != peerX {
			continue
		}
		if e.Hops >= HopsInfinity {
			tombstoneSeen = true
			if e.SeqNo > maxSeqNo {
				maxSeqNo = e.SeqNo
				maxIsLive = false
			}
		} else {
			liveSeen = true
			liveSeqNo = e.SeqNo
			if e.SeqNo > maxSeqNo {
				maxSeqNo = e.SeqNo
				maxIsLive = true
			}
		}
	}
	if !liveSeen {
		t.Fatalf("expected live entry for peer-X on the wire: %+v", wire)
	}
	if tombstoneSeen {
		// The current implementation suppresses the tombstone
		// outright. If a future change re-introduces it, the
		// receiver-side invariant below still pins the
		// correctness contract.
		t.Logf("tombstone for peer-X present alongside live; verifying receiver-side ordering")
	}
	if !maxIsLive {
		t.Fatalf("receiver-timeline invariant violated: tombstone SeqNo %d > live SeqNo %d for peer-X — receiver would converge on withdrawn state", maxSeqNo, liveSeqNo)
	}
}

// TestAnnounceTo_OutboundSeqNoForwardOnSigRevisit reproduces the
// A→B→A regression flagged in the round-5 review. Content-keyed
// outbound SeqNo cache MUST NOT reuse a stale entry when a
// broadcast emit since then has advanced every connected peer's
// receiver-side timeline past the cached value: AddDirect (sigA
// per-peer, seq=1), RemoveDirect (sigB tombstone broadcast,
// seq=2 → broadcast watermark advances to 2), reconnect-AddDirect
// (sigA again with native seq=3). The naive content cache would
// return seq=1 on the reconnect's emit, which peer-Z would reject
// (its tombstone-resurrection guard requires SeqNo > 2). The
// staleness check `cached.SeqNo >= outboundBroadcastMax` forces
// a fresh assignment that strictly exceeds the receiver's
// current view.
//
// Companion test TestAnnounceTo_OutboundSeqNoStableAcrossPeers
// AndCycles guards the OTHER side of the contract: per-peer
// emits must NOT advance the broadcast watermark, or sequential
// AnnounceTo(peerP) / AnnounceTo(peerQ) calls under split horizon
// would invalidate each other's cache and burn SeqNo on every
// announce cycle.
func TestAnnounceTo_OutboundSeqNoForwardOnSigRevisit(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	// Step 1: AddDirect → first live emit. sigA cache miss.
	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer 1: %v", err)
	}
	live1 := tbl.AnnounceTo(peerZ)
	var live1Seq uint64
	for _, e := range live1 {
		if e.Identity == peerX && e.Hops < HopsInfinity {
			live1Seq = e.SeqNo
			break
		}
	}
	if live1Seq == 0 {
		t.Fatalf("expected first live emit for peer-X: %+v", live1)
	}

	// Step 2: RemoveDirect → tombstone emit. sigB cache miss
	// (different Hops + Withdrawn). outboundMax advances.
	res, err := tbl.RemoveDirectPeer(peerX)
	if err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}
	if len(res.Withdrawals) != 1 {
		t.Fatalf("expected one wire withdrawal, got %d", len(res.Withdrawals))
	}
	tombstoneSeq := res.Withdrawals[0].SeqNo
	if tombstoneSeq <= live1Seq {
		t.Fatalf("tombstone SeqNo must be > prior live SeqNo: tombstone=%d, live1=%d", tombstoneSeq, live1Seq)
	}

	// Step 3: Reconnect AddDirect → second live emit. sigA
	// matches the step-1 entry but the cache is now stale
	// (outboundMax has advanced to the tombstone's SeqNo).
	// The fix forces a fresh assignment > tombstone SeqNo.
	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer 2: %v", err)
	}
	live2 := tbl.AnnounceTo(peerZ)
	var live2Seq uint64
	for _, e := range live2 {
		if e.Identity == peerX && e.Hops < HopsInfinity {
			live2Seq = e.SeqNo
			break
		}
	}
	if live2Seq == 0 {
		t.Fatalf("expected second live emit for peer-X after reconnect: %+v", live2)
	}
	if live2Seq <= tombstoneSeq {
		t.Fatalf("reconnect emit SeqNo regression: live2=%d must be > tombstone=%d to survive receiver's resurrection guard",
			live2Seq, tombstoneSeq)
	}
}

// TestAnnounceTo_PropagatesExtraOnWire reproduces the P1.2
// regression flagged in the round-4 review. AnnounceProjectionFor
// must copy claim.Extra into the AnnounceEntry it emits — the
// Extra field is the documented forward-compatible relay payload
// (see RouteEntry.Extra in types.go and the wire framing in
// node/routing_announce.go). Dropping it on wire emit would
// silently strip future-protocol-version fields from the
// announce frames.
//
// The companion contract is that isBetterLiveClaim picks a
// deterministic winner across uplinks whose Extras differ, so the
// emitted Extra is stable across cycles (same content → same
// SeqNo via the content-keyed outboundContent cache; ComputeDelta
// no-op). The Extra value is part of the content-sig so a future
// Extra change forces a fresh SeqNo assignment.
func TestAnnounceTo_PropagatesExtraOnWire(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerY PeerIdentity = "peer-Y"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	extra := json.RawMessage(`{"onion_box":"deadbeef","future":42}`)
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "some-origin", NextHop: peerY,
		Hops: 3, SeqNo: 7, Source: RouteSourceAnnouncement,
		Extra: extra,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	entries := tbl.AnnounceTo(peerZ)
	var live *AnnounceEntry
	for i := range entries {
		if entries[i].Identity == peerX && entries[i].Hops < HopsInfinity {
			live = &entries[i]
			break
		}
	}
	if live == nil {
		t.Fatalf("expected live entry for peer-X in AnnounceTo output: %+v", entries)
	}
	if string(live.Extra) != string(extra) {
		t.Fatalf("Extra lost on wire emit: got %q, want %q", string(live.Extra), string(extra))
	}
}

// TestAnnounceTo_OutboundSeqNoStableAcrossPeersAndCycles reproduces
// the P1.1 split-horizon flapping concern flagged in the round-4
// review. Two peers P1 and P2 each pick a DIFFERENT live winner for
// the same destination X due to split-horizon (each peer's identity
// is the uplink of the other's preferred backup). Sequential per-
// peer AnnounceTo calls in the same announce cycle MUST NOT
// invalidate each other's outbound SeqNo — the cache is content-
// keyed (per (Identity, sig)), so each peer's winner gets its own
// stable SeqNo regardless of how many other peers' AnnounceTo
// calls land between two cycles for the same peer.
//
// Without the content-keyed fix, peer-keyed or single-cache
// implementations would have flapped the SeqNo on every per-peer
// call (the previous peer's sig overrides the cache, the next
// peer's call sees a sig mismatch and bumps). ComputeDelta would
// then see the outbound entry as "changed" every cycle, breaking
// the delta no-op invariant and forcing a routes_update on every
// announce cycle for a stable mesh.
func TestAnnounceTo_OutboundSeqNoStableAcrossPeersAndCycles(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerP PeerIdentity = "peer-P"
		peerQ PeerIdentity = "peer-Q"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	// Two live transit claims for peer-X, one via peer-P and one
	// via peer-Q. Split-horizon: AnnounceTo(P) sees winner via
	// peer-Q (peer-P filtered as excludeVia); AnnounceTo(Q) sees
	// winner via peer-P (peer-Q filtered). Each peer's winner has
	// a different sig (different Uplink).
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "some-origin", NextHop: peerP,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(via peer-P): %v", err)
	}
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "some-origin", NextHop: peerQ,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(via peer-Q): %v", err)
	}

	// Cycle 1 — sequential per-peer AnnounceTo.
	cycle1P := tbl.AnnounceTo(peerP)
	cycle1Q := tbl.AnnounceTo(peerQ)

	// Cycle 2 — same content, sequential per-peer AnnounceTo.
	cycle2P := tbl.AnnounceTo(peerP)
	cycle2Q := tbl.AnnounceTo(peerQ)

	findEntry := func(entries []AnnounceEntry, id PeerIdentity) *AnnounceEntry {
		for i := range entries {
			if entries[i].Identity == id && entries[i].Hops < HopsInfinity {
				return &entries[i]
			}
		}
		return nil
	}

	c1p := findEntry(cycle1P, peerX)
	c1q := findEntry(cycle1Q, peerX)
	c2p := findEntry(cycle2P, peerX)
	c2q := findEntry(cycle2Q, peerX)
	if c1p == nil || c1q == nil || c2p == nil || c2q == nil {
		t.Fatalf("expected peer-X entry in all four AnnounceTo calls; got c1p=%v c1q=%v c2p=%v c2q=%v",
			c1p, c1q, c2p, c2q)
	}

	if c1p.SeqNo != c2p.SeqNo {
		t.Fatalf("peer-P's outbound SeqNo for peer-X flapped between cycles: cycle1=%d, cycle2=%d", c1p.SeqNo, c2p.SeqNo)
	}
	if c1q.SeqNo != c2q.SeqNo {
		t.Fatalf("peer-Q's outbound SeqNo for peer-X flapped between cycles: cycle1=%d, cycle2=%d", c1q.SeqNo, c2q.SeqNo)
	}
}

// TestAnnounceTo_BroadcastWatermarkInvalidatesCachedLiveSig
// reproduces the contract between the split-horizon stability
// invariant (per-peer emits MUST NOT poison the cache) and the
// A→B→A guard (broadcast emits MUST invalidate stale cache
// entries that fall below the new watermark).
//
// Fixture:
//   - WithLocalOrigin "node-A".
//   - Two own-direct peers X and Y (so AddDirectPeer/Remove on Y
//     can drive the broadcast watermark while X is the Identity
//     under test).
//   - Cycle 1: AnnounceTo(Z) emits sigX live for Identity=X via
//     uplink=X. Per-peer, watermark stays at 0.
//   - Cycle 2: AnnounceTo(Z) emits sigX again — cache hit, same
//     SeqNo (delta no-op). Watermark still 0.
//   - RemoveDirectPeer(Y) — broadcast tombstone for Y. This
//     advances outboundBroadcastMax[Y] BUT NOT
//     outboundBroadcastMax[X]. Identity-scoped watermarks
//     prevent cross-identity poisoning.
//   - Cycle 3: AnnounceTo(Z) emits sigX again — cache hit, same
//     SeqNo. X's watermark unaffected by Y's broadcast.
//   - RemoveDirectPeer(X) — broadcast tombstone for X. Now
//     outboundBroadcastMax[X] advances past the cached sigX
//     SeqNo. (Cache entry sigX is now formally stale, but it
//     won't be looked up again until reconnect.)
//   - AddDirectPeer(X) reconnect, AnnounceTo(Z): sigX cache hit,
//     but cached SeqNo < watermark → fresh assignment > prior
//     tombstone SeqNo.
func TestAnnounceTo_BroadcastWatermarkInvalidatesCachedLiveSig(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerY PeerIdentity = "peer-Y"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X): %v", err)
	}
	if _, err := tbl.AddDirectPeer(peerY); err != nil {
		t.Fatalf("AddDirectPeer(peer-Y): %v", err)
	}

	findX := func(entries []AnnounceEntry) *AnnounceEntry {
		for i := range entries {
			if entries[i].Identity == peerX && entries[i].Hops < HopsInfinity {
				return &entries[i]
			}
		}
		return nil
	}

	cycle1 := findX(tbl.AnnounceTo(peerZ))
	if cycle1 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 1")
	}
	cycle2 := findX(tbl.AnnounceTo(peerZ))
	if cycle2 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 2")
	}
	if cycle1.SeqNo != cycle2.SeqNo {
		t.Fatalf("per-peer emits poisoned cache: cycle1=%d cycle2=%d (should be equal)", cycle1.SeqNo, cycle2.SeqNo)
	}

	// Broadcast withdrawal for peer-Y — advances Y's watermark,
	// MUST NOT advance X's.
	if _, err := tbl.RemoveDirectPeer(peerY); err != nil {
		t.Fatalf("RemoveDirectPeer(peer-Y): %v", err)
	}
	cycle3 := findX(tbl.AnnounceTo(peerZ))
	if cycle3 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 3")
	}
	if cycle3.SeqNo != cycle1.SeqNo {
		t.Fatalf("cross-identity broadcast leaked into peer-X cache: cycle1=%d cycle3=%d (should be equal — peer-Y's broadcast must not advance peer-X's watermark)",
			cycle1.SeqNo, cycle3.SeqNo)
	}

	// Broadcast withdrawal for peer-X — advances X's watermark
	// past the cached sigX SeqNo.
	res, err := tbl.RemoveDirectPeer(peerX)
	if err != nil {
		t.Fatalf("RemoveDirectPeer(peer-X): %v", err)
	}
	if len(res.Withdrawals) != 1 {
		t.Fatalf("expected one withdrawal, got %d", len(res.Withdrawals))
	}
	tombstoneSeq := res.Withdrawals[0].SeqNo
	if tombstoneSeq <= cycle1.SeqNo {
		t.Fatalf("tombstone SeqNo must exceed prior live SeqNo: tombstone=%d live=%d", tombstoneSeq, cycle1.SeqNo)
	}

	// Reconnect → sigX cache hit but cached SeqNo < watermark
	// → fresh assignment > tombstone.
	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X) reconnect: %v", err)
	}
	cycle4 := findX(tbl.AnnounceTo(peerZ))
	if cycle4 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 4")
	}
	if cycle4.SeqNo <= tombstoneSeq {
		t.Fatalf("reconnect SeqNo regression: cycle4=%d must be > tombstone=%d", cycle4.SeqNo, tombstoneSeq)
	}
}

// TestAnnounceTo_PerPeerWinnerSwitchABA reproduces the round-5e
// regression: a single peer's per-receiver timeline advances when
// the AnnounceTo winner for that peer changes between cycles. A
// later cycle reverting to the original winner sig MUST NOT
// reuse the cached SeqNo from the original cycle — that SeqNo is
// below the peer's current view (the intermediate cycle moved it
// forward), and the receiver would reject the re-emit as stale.
//
// Scenario (no broadcasts; pure per-peer winner switch):
//
//   - Two transit claims for peer-X live: via peer-P (preferred,
//     lower hops) and via peer-Q (backup).
//   - Cycle 1: AnnounceTo(peer-Z) — winner is sigP. Emits sigP@K1.
//     outboundPeerMax[(X, Z)] = K1.
//   - InvalidateTransitVia(peer-P) silently withdraws the
//     preferred claim — NO wire withdrawal. Q is now the live
//     winner.
//   - Cycle 2: AnnounceTo(peer-Z) — winner is sigQ. Emits
//     sigQ@K2 (K2 > K1 because cache miss bumps outboundMax).
//     outboundPeerMax[(X, Z)] = K2.
//   - UpdateRoute restores peer-P's claim (fresh seq, ApplyUpdate
//     accepts; peer-P becomes the live winner again).
//   - Cycle 3: AnnounceTo(peer-Z) — winner reverts to sigP.
//     outboundContent[(X, sigP)] still holds K1. broadcastMax
//     is zero (no broadcasts have happened), so the
//     broadcast-only fix from round 5d would reuse K1 — but
//     peer-Z's per-receiver high water is K2, so the per-peer
//     watermark check must force a fresh K3 > K2.
//
// Without outboundPeerMax tracking this test fails with the
// receiver-side rejection symptom: cycle3.SeqNo <= cycle2.SeqNo.
func TestAnnounceTo_PerPeerWinnerSwitchABA(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerP PeerIdentity = "peer-P"
		peerQ PeerIdentity = "peer-Q"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	// Two transit claims, peer-P preferred (lower hops).
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(via peer-P): %v", err)
	}
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerQ,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(via peer-Q): %v", err)
	}

	findX := func(entries []AnnounceEntry) *AnnounceEntry {
		for i := range entries {
			if entries[i].Identity == peerX && entries[i].Hops < HopsInfinity {
				return &entries[i]
			}
		}
		return nil
	}

	// Cycle 1: winner is via peer-P (Hops=2).
	cycle1 := findX(tbl.AnnounceTo(peerZ))
	if cycle1 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 1")
	}

	// Silent invalidation of peer-P's transit claim — no wire
	// withdrawal, but peer-P's claim becomes Withdrawn locally.
	// Winner shifts to peer-Q.
	tbl.InvalidateTransitRoutes(peerP)

	// Cycle 2: winner is via peer-Q (sigQ). Different sig from
	// cycle 1 (different Uplink), so cache miss; fresh allocation
	// > cycle1.SeqNo.
	cycle2 := findX(tbl.AnnounceTo(peerZ))
	if cycle2 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 2 (winner via peer-Q)")
	}
	if cycle2.SeqNo <= cycle1.SeqNo {
		t.Fatalf("cycle 2 winner-switch must bump SeqNo: cycle1=%d cycle2=%d", cycle1.SeqNo, cycle2.SeqNo)
	}

	// Restore peer-P's claim (fresh SeqNo > existing tombstone).
	// peer-P becomes the live winner again (Hops=2 < Q's Hops=3).
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 2, SeqNo: 20, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute(via peer-P restore): %v", err)
	}

	// Cycle 3: winner reverts to via peer-P (sigP). The cache
	// holds sigP at cycle1.SeqNo. broadcastMax is 0 (no
	// broadcasts), so the broadcast-only watermark would reuse.
	// The per-receiver watermark for peer-Z (set to cycle2.SeqNo
	// in cycle 2) catches the staleness and forces a fresh emit.
	cycle3 := findX(tbl.AnnounceTo(peerZ))
	if cycle3 == nil {
		t.Fatalf("expected live emit for peer-X in cycle 3 (winner via peer-P again)")
	}
	if cycle3.SeqNo <= cycle2.SeqNo {
		t.Fatalf("per-peer winner-switch A→B→A regression: cycle3=%d must be > cycle2=%d (peer-Z's timeline advanced past cycle1's SeqNo via cycle 2)",
			cycle3.SeqNo, cycle2.SeqNo)
	}
}

// TestAnnounceTo_CacheHitRefreshesPerPeerMax reproduces the
// round-5f regression: nextOutboundSeqLockedPerPeer must bump
// outboundPeerMax on cache HIT (not just on fresh allocation).
//
// The outboundContent cache is shared across peers — same
// (Identity, sig) → one SeqNo slot. Another peer's per-peer
// winner-switch A→B→A can bump the cached value above THIS
// peer's previous peerMax. When we then reuse the cache for THIS
// peer, we genuinely emit the higher SeqNo, advancing this
// peer's receiver-side timeline. Without refreshing peerMax,
// the per-receiver watermark stays stale, and a follow-up cache
// hit for a DIFFERENT sig stored at a value in
// `(stalePeerMax, currentCache]` would pass the staleness check
// at a SeqNo below the peer's actual receiver view — receiver
// rejects as stale.
//
// Fixture (all transit, no broadcasts so broadcastMax stays
// zero and the bug is isolated to outboundPeerMax):
//   - Identity peer-X has two transit claims: via peer-P
//     (Hops=2, preferred winner) and via peer-Q (Hops=3, backup).
//   - cycle 1: AnnounceTo(peer-1) — winner sigP, cache miss
//     emits sigP@10. outboundContent[(X, sigP)]=10,
//     outboundPeerMax[(X, 1)]=10.
//   - cycle 2: AnnounceTo(peer-2) — winner sigP, cache HIT @10.
//     outboundPeerMax[(X, 2)] is unset (0). Without the fix it
//     stays at 0; with the fix it advances to 10.
//   - Switch winner to sigQ (peer-P route worsens).
//   - cycle 3: AnnounceTo(peer-2) — winner sigQ, cache miss,
//     emits sigQ@11. outboundPeerMax[(X, 2)]=11.
//   - Switch winner back to sigP.
//   - cycle 4: AnnounceTo(peer-2) — winner sigP, cache hit @10
//     but peerMax[(X, 2)]=11, 10<11 stale → fresh emit @12.
//     outboundContent[(X, sigP)]=12, peerMax[(X, 2)]=12.
//
// Now peer-1 gets the bumped cache:
//   - cycle 5: AnnounceTo(peer-1) — winner sigP, cache HIT @12.
//     broadcastMax=0, peerMax[(X, 1)]=10 (from cycle 1). 12>=10
//     OK, reuse 12. WITHOUT FIX: peerMax[(X, 1)] stays at 10;
//     peer-1 has actually received sigP@12 so its receiver
//     timeline is at 12.
//   - Switch winner to sigQ.
//   - cycle 6: AnnounceTo(peer-1) — winner sigQ, cache hit @11.
//     broadcastMax=0. peerMax[(X, 1)]=10 (without fix) or 12
//     (with fix).
//   - Without fix: 11>=10 OK, reuse 11. peer-1 saw sigP@12
//     then sigQ@11 — 11 < 12 → receiver rejects as stale.
//   - With fix: 11>=12 FAIL → fresh emit > 12.
//
// Assertion: cycle6.SeqNo > cycle5.SeqNo. Without the cache-hit
// peerMax refresh this fails with `cycle6=11 cycle5=12`.
func TestAnnounceTo_CacheHitRefreshesPerPeerMax(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerP PeerIdentity = "peer-P"
		peerQ PeerIdentity = "peer-Q"
		peer1 PeerIdentity = "peer-1"
		peer2 PeerIdentity = "peer-2"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	// Initial topology: peer-P preferred (Hops=2), peer-Q backup
	// (Hops=3). Same SeqNo so the tie-break is purely Hops.
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute initial peer-P: %v", err)
	}
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerQ,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute initial peer-Q: %v", err)
	}

	findX := func(entries []AnnounceEntry) *AnnounceEntry {
		for i := range entries {
			if entries[i].Identity == peerX && entries[i].Hops < HopsInfinity {
				return &entries[i]
			}
		}
		return nil
	}

	// cycle 1: peer-1 receives sigP for the first time (cache miss).
	cycle1 := findX(tbl.AnnounceTo(peer1))
	if cycle1 == nil {
		t.Fatalf("cycle 1: expected live emit to peer-1")
	}

	// cycle 2: peer-2 receives sigP (cache hit, same winner).
	// peer-2's first emit for X.
	cycle2 := findX(tbl.AnnounceTo(peer2))
	if cycle2 == nil {
		t.Fatalf("cycle 2: expected live emit to peer-2")
	}
	if cycle2.SeqNo != cycle1.SeqNo {
		t.Fatalf("cycle 2 should reuse cache (same content as cycle 1): got %d, want %d", cycle2.SeqNo, cycle1.SeqNo)
	}

	// Worsen peer-P → peer-Q becomes the winner.
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 4, SeqNo: 11, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute worsen peer-P: %v", err)
	}

	// cycle 3: peer-2 receives sigQ (cache miss → fresh).
	cycle3 := findX(tbl.AnnounceTo(peer2))
	if cycle3 == nil {
		t.Fatalf("cycle 3: expected live emit to peer-2 with new winner sigQ")
	}
	if cycle3.SeqNo <= cycle2.SeqNo {
		t.Fatalf("cycle 3 must bump SeqNo on winner switch: cycle2=%d cycle3=%d", cycle2.SeqNo, cycle3.SeqNo)
	}

	// Restore peer-P → peer-P becomes winner again.
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 2, SeqNo: 12, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute restore peer-P: %v", err)
	}

	// cycle 4: peer-2 receives sigP again. cache@10 stale for
	// peer-2 (its peerMax is 11 from cycle 3), so fresh emit
	// bumps the shared cache for sigP above 11.
	cycle4 := findX(tbl.AnnounceTo(peer2))
	if cycle4 == nil {
		t.Fatalf("cycle 4: expected live emit to peer-2")
	}
	if cycle4.SeqNo <= cycle3.SeqNo {
		t.Fatalf("cycle 4 fresh allocation for peer-2: cycle3=%d cycle4=%d", cycle3.SeqNo, cycle4.SeqNo)
	}

	// cycle 5: peer-1 receives sigP — cache hit at cycle4.SeqNo
	// (peer-2's fresh value). peer-1's peerMax was set to
	// cycle1.SeqNo in cycle 1; without the fix it would stay
	// stale; with the fix it advances to cycle4.SeqNo.
	cycle5 := findX(tbl.AnnounceTo(peer1))
	if cycle5 == nil {
		t.Fatalf("cycle 5: expected live emit to peer-1")
	}
	if cycle5.SeqNo != cycle4.SeqNo {
		t.Fatalf("cycle 5 should reuse cycle 4's bumped cache for peer-1: got %d, want %d", cycle5.SeqNo, cycle4.SeqNo)
	}

	// Worsen peer-P again → winner switches to sigQ.
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "origin", NextHop: peerP,
		Hops: 4, SeqNo: 13, Source: RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute worsen peer-P again: %v", err)
	}

	// cycle 6: peer-1 receives sigQ. The cache for sigQ holds
	// cycle3.SeqNo (set during peer-2's cycle 3 fresh emit).
	// peer-1's actual receiver timeline is at cycle5.SeqNo
	// (=cycle4.SeqNo). Without the cache-hit peerMax refresh,
	// peerMax[(X, peer-1)] is still cycle1.SeqNo and the cache
	// hit at cycle3.SeqNo passes the staleness check — emit
	// cycle3.SeqNo to peer-1 which is BELOW peer-1's view.
	cycle6 := findX(tbl.AnnounceTo(peer1))
	if cycle6 == nil {
		t.Fatalf("cycle 6: expected live emit to peer-1 with sigQ")
	}
	if cycle6.SeqNo <= cycle5.SeqNo {
		t.Fatalf("cache-hit peerMax refresh regression: cycle6=%d must be > cycle5=%d "+
			"(peer-1's timeline advanced through cycle5 via the cross-peer cache bump in cycle 4)",
			cycle6.SeqNo, cycle5.SeqNo)
	}
}

// TestAnnounceTo_OutboundSeqNoStaysAboveOwnTombstone reproduces
// the P1.1 regression flagged in the A1 review: after S withdraws
// its own direct route to X (own counter at SeqNo=N) and then
// picks up a transit backup whose foreign SeqNo<N, the wire
// frame must NOT carry the foreign SeqNo verbatim — the
// receiver's tombstone-resurrection guard requires strictly
// newer SeqNo, so a lower SeqNo would be silently dropped and
// X would stay withdrawn on the receiver until TTL elapsed.
//
// nextOutboundSeqLockedBroadcast (for the tombstone fanout) and
// nextOutboundSeqLockedPerPeer (for the AnnounceTo re-emit)
// synthesise the wire SeqNo against the outboundContent cache
// plus the outboundMax / outboundBroadcastMax watermarks. A
// naive AnnounceProjectionFor that just passed claim.SeqNo
// through would fail this test.
//
// Fixture:
//   - WithLocalOrigin "node-A".
//   - AddDirectPeer X — own direct, own counter = 1.
//   - RemoveDirectPeer X — tombstone, own counter = 2,
//     outboundContent[(X, withdrawn=true sig)] = SeqNo=2,
//     outboundBroadcastMax[X] = 2.
//   - UpdateRoute transit X via Y with foreign SeqNo=1 (LOWER
//     than own counter).
//   - AnnounceTo(peer Z) — wire-emit the live winner.
//
// Expected: wire live entry for X has SeqNo > 2 (the prior
// tombstone wire SeqNo). Without the fix the wire SeqNo would
// be 1 (foreign), which the receiver rejects.
func TestAnnounceTo_OutboundSeqNoStaysAboveOwnTombstone(t *testing.T) {
	const (
		nodeA PeerIdentity = "node-A"
		peerX PeerIdentity = "peer-X"
		peerY PeerIdentity = "peer-Y"
		peerZ PeerIdentity = "peer-Z"
	)

	tbl := NewTable(WithLocalOrigin(nodeA))

	if _, err := tbl.AddDirectPeer(peerX); err != nil {
		t.Fatalf("AddDirectPeer(peer-X): %v", err)
	}
	res, err := tbl.RemoveDirectPeer(peerX)
	if err != nil {
		t.Fatalf("RemoveDirectPeer(peer-X): %v", err)
	}
	if len(res.Withdrawals) != 1 {
		t.Fatalf("expected exactly one wire withdrawal, got %d", len(res.Withdrawals))
	}
	tombstoneSeqNo := res.Withdrawals[0].SeqNo

	// Foreign transit claim with a SeqNo deliberately BELOW the
	// own tombstone counter — this is exactly the scenario the
	// outbound SeqNo synthesis is designed to guard against.
	const foreignSeqNo uint64 = 1
	if foreignSeqNo >= tombstoneSeqNo {
		t.Fatalf("test misconfigured: foreign SeqNo %d must be < tombstone SeqNo %d to exercise the regression",
			foreignSeqNo, tombstoneSeqNo)
	}
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: peerX, Origin: "some-foreign-origin", NextHop: peerY,
		Hops: 3, SeqNo: foreignSeqNo, Source: RouteSourceAnnouncement,
	})
	if err != nil || status != RouteAccepted {
		t.Fatalf("UpdateRoute(transit): status=%v err=%v", status, err)
	}

	entries := tbl.AnnounceTo(peerZ)
	var liveWireSeqNo uint64
	var foundLive bool
	for _, e := range entries {
		if e.Identity != peerX {
			continue
		}
		if e.Hops < HopsInfinity {
			if foundLive {
				t.Fatalf("multiple live entries for peer-X in AnnounceTo output: %+v", entries)
			}
			foundLive = true
			liveWireSeqNo = e.SeqNo
		}
	}
	if !foundLive {
		t.Fatalf("expected live entry for peer-X in AnnounceTo output: %+v", entries)
	}
	if liveWireSeqNo <= tombstoneSeqNo {
		t.Fatalf("outbound SeqNo regression: live wire SeqNo=%d must be > tombstone wire SeqNo=%d to survive receiver's resurrection guard",
			liveWireSeqNo, tombstoneSeqNo)
	}
}

// TestBuildAnnounceSnapshot_LiveAndTombstoneSameSeqNoBothSurvive
// is the synthetic-fixture regression test for the Stage 2 key
// fix in A1 round 4. Pre-fix, Stage 2 keyed by (Identity, Origin,
// SeqNo) — when a live entry and an own-direct tombstone for the
// same Identity happened to carry the same Origin and SeqNo (the
// outbound SeqNo synthesis can land them on the same value, or a
// pathological test fixture can fabricate it), the Stage 2
// min-Hops tie-breaker picked the live entry (Hops=N < Hops=16),
// silently dropping the tombstone BEFORE the per-Identity
// tombstone-passthrough stage got to preserve it. The Withdrawn
// axis added to seqKey keeps live and tombstone in different
// Stage 2 buckets even at identical (Origin, SeqNo).
func TestBuildAnnounceSnapshot_LiveAndTombstoneSameSeqNoBothSurvive(t *testing.T) {
	const (
		peerX  PeerIdentity = "peer-X"
		sender PeerIdentity = "sender-localOrigin"
	)
	const collidingSeqNo uint64 = 42

	raw := []AnnounceEntry{
		// Own-direct tombstone for peer-X. Hops=16 marks it
		// withdrawn.
		{Identity: peerX, Origin: sender, SeqNo: collidingSeqNo, Hops: HopsInfinity},
		// Live backup for peer-X with the SAME (Origin, SeqNo).
		{Identity: peerX, Origin: sender, SeqNo: collidingSeqNo, Hops: 2},
	}

	snap := BuildAnnounceSnapshot(raw)

	var live, tombstone *AnnounceEntry
	for i := range snap.Entries {
		e := &snap.Entries[i]
		if e.Identity != peerX {
			continue
		}
		if e.Hops >= HopsInfinity {
			tombstone = e
		} else {
			live = e
		}
	}
	if live == nil {
		t.Fatalf("live entry dropped — Stage 2 (Identity, Origin, SeqNo) key collision regression: %+v", snap.Entries)
	}
	if tombstone == nil {
		t.Fatalf("tombstone entry dropped — Stage 2 (Identity, Origin, SeqNo) key collision regression: %+v", snap.Entries)
	}
}
