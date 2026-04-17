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
	raw := []AnnounceEntry{
		{Identity: "zzz", Origin: "aaa", SeqNo: 1, Hops: 1},
		{Identity: "aaa", Origin: "zzz", SeqNo: 1, Hops: 1},
		{Identity: "aaa", Origin: "aaa", SeqNo: 1, Hops: 1},
	}

	snap := BuildAnnounceSnapshot(raw)
	if len(snap.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap.Entries))
	}
	// Should be sorted by (Identity, Origin)
	if snap.Entries[0].Identity != "aaa" || snap.Entries[0].Origin != "aaa" {
		t.Fatalf("first entry should be (aaa,aaa), got (%s,%s)",
			snap.Entries[0].Identity, snap.Entries[0].Origin)
	}
	if snap.Entries[1].Identity != "aaa" || snap.Entries[1].Origin != "zzz" {
		t.Fatalf("second entry should be (aaa,zzz), got (%s,%s)",
			snap.Entries[1].Identity, snap.Entries[1].Origin)
	}
	if snap.Entries[2].Identity != "zzz" || snap.Entries[2].Origin != "aaa" {
		t.Fatalf("third entry should be (zzz,aaa), got (%s,%s)",
			snap.Entries[2].Identity, snap.Entries[2].Origin)
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
		name     string
		input    json.RawMessage
		wantNil  bool
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

func TestBuildAnnounceSnapshot_SplitHorizonPreservedAfterAggregation(t *testing.T) {
	// Verify that aggregation does not break identity-key semantics
	// that split horizon relies on. After aggregation, each (Identity, Origin)
	// pair appears exactly once, so a receiver doing split horizon filtering
	// by Origin can still work correctly.
	raw := []AnnounceEntry{
		// Two paths to dest1 via origin1 — different hops.
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 2},
		{Identity: "dest1", Origin: "origin1", SeqNo: 5, Hops: 4},
		// Same dest1 but different origin (origin2) — must survive as separate entry.
		{Identity: "dest1", Origin: "origin2", SeqNo: 3, Hops: 1},
		// Different dest entirely.
		{Identity: "dest2", Origin: "origin1", SeqNo: 1, Hops: 3},
	}

	snap := BuildAnnounceSnapshot(raw)

	// Should have exactly 3 distinct (Identity, Origin) pairs.
	if len(snap.Entries) != 3 {
		t.Fatalf("expected 3 entries after aggregation, got %d", len(snap.Entries))
	}

	// Verify each pair is present and deduplicated correctly.
	found := make(map[announceLineageKey]AnnounceEntry)
	for _, e := range snap.Entries {
		k := announceLineageKey{Identity: e.Identity, Origin: e.Origin}
		found[k] = e
	}

	e1, ok := found[announceLineageKey{Identity: "dest1", Origin: "origin1"}]
	if !ok {
		t.Fatal("missing (dest1, origin1)")
	}
	if e1.Hops != 2 {
		t.Fatalf("expected min hops=2 for (dest1,origin1), got %d", e1.Hops)
	}

	e2, ok := found[announceLineageKey{Identity: "dest1", Origin: "origin2"}]
	if !ok {
		t.Fatal("missing (dest1, origin2)")
	}
	if e2.SeqNo != 3 {
		t.Fatalf("expected seqNo=3 for (dest1,origin2), got %d", e2.SeqNo)
	}

	_, ok = found[announceLineageKey{Identity: "dest2", Origin: "origin1"}]
	if !ok {
		t.Fatal("missing (dest2, origin1)")
	}
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
