package node

import (
	"fmt"
	"testing"
	"time"
)

// receipt_dedup_test.go pins the rotating hashed dedup contract that replaced
// the unbounded seenReceipts map: recall within the window, eviction after two
// rotations (the time bound), Delete on both generations, the long-jump reset,
// and the hard cardinality cap enforced by eviction — always recording so a
// duplicate is still caught at capacity (the memory bound under a distinct-key
// flood). See receipt_dedup.go.
//
// maxEntries is passed as 0 (cap disabled) in the window/dedup tests so they
// exercise rotation in isolation; the cap has its own test.

func TestRotatingHashDedup_AddThenHasInSameWindow(t *testing.T) {
	r := newRotatingHashDedup(receiptDedupRotation, 0, nil)
	r.Add("rcpt-1")
	if !r.Has("rcpt-1") {
		t.Fatal("Add then Has in same window must return true")
	}
	if r.Has("rcpt-unknown") {
		t.Fatal("exact set must NOT report an unseen key as present")
	}
}

func TestRotatingHashDedup_MarkIfAbsentReportsDuplicate(t *testing.T) {
	r := newRotatingHashDedup(receiptDedupRotation, 0, nil)
	if r.MarkIfAbsent("k") {
		t.Fatal("first MarkIfAbsent must report absent (false)")
	}
	if !r.MarkIfAbsent("k") {
		t.Fatal("second MarkIfAbsent on same key must report duplicate (true)")
	}
}

func TestRotatingHashDedup_SurvivesOneRotation(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Minute, 0, clock)

	r.Add("kept")
	now = now.Add(time.Minute + time.Second) // current → previous
	if !r.Has("kept") {
		t.Fatal("key must survive ONE rotation (previous generation still queried)")
	}
}

func TestRotatingHashDedup_EvictedAfterTwoRotations(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Minute, 0, clock)

	r.Add("doomed")
	now = now.Add(time.Minute + time.Second) // doomed → previous
	r.Add("trigger")                         // forces rotation evaluation
	now = now.Add(time.Minute + time.Second) // doomed evicted from previous
	if r.Has("doomed") {
		t.Fatal("key must be evicted after two rotations (time upper bound)")
	}
	if !r.Has("trigger") {
		t.Fatal("post-rotation key must survive its own one rotation")
	}
}

func TestRotatingHashDedup_DeleteRemovesFromBothGenerations(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Minute, 0, clock)

	r.Add("rollback")
	now = now.Add(time.Minute + time.Second) // rollback now in previous
	r.Delete("rollback")
	if r.Has("rollback") {
		t.Fatal("Delete must remove the key from the previous generation too")
	}
	if r.MarkIfAbsent("rollback") {
		t.Fatal("after Delete the key must read as absent (retry eligibility restored)")
	}
}

func TestRotatingHashDedup_LongJumpResetsBothGenerations(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Minute, 0, clock)

	r.Add("ancient")
	now = now.Add(10 * time.Minute) // slept past the window
	if r.Has("ancient") {
		t.Fatal("clock jump past 2x rotation must clear both generations")
	}
}

func TestRotatingHashDedup_LenBoundedByWindow(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Minute, 0, clock)

	for i := 0; i < 1000; i++ {
		r.Add(fmt.Sprintf("w1-%d", i))
	}
	now = now.Add(time.Minute + time.Second)
	for i := 0; i < 1000; i++ {
		r.Add(fmt.Sprintf("w2-%d", i))
	}
	now = now.Add(time.Minute + time.Second) // w1 evicted, only w2 remains
	for i := 0; i < 1000; i++ {
		r.Add(fmt.Sprintf("w3-%d", i))
	}
	if got := r.Len(); got > 2000 {
		t.Fatalf("retained keys must stay bounded by ~2 windows, got %d", got)
	}
	if r.Has("w1-0") {
		t.Fatal("oldest-window key must have been evicted")
	}
}

func TestRotatingHashDedup_HardCapBoundsCardinality(t *testing.T) {
	// A flood of DISTINCT keys within a single window must not grow the set
	// past the cap — this is the memory-exhaustion guard. Frozen clock so the
	// time-driven rotation never fires: cap-driven early rotation alone must
	// hold the line.
	const limit = 1000
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Hour, limit, clock)

	for i := 0; i < 100_000; i++ {
		r.Add(fmt.Sprintf("flood-%d", i))
		if r.Len() > limit {
			t.Fatalf("cardinality must never exceed the cap, got %d at i=%d", r.Len(), i)
		}
	}
	if got := r.Len(); got == 0 || got > limit {
		t.Fatalf("after a flood the set must be non-empty and within the cap, got %d", got)
	}
}

func TestRotatingHashDedup_AlwaysRecordsEvenAtCap(t *testing.T) {
	// The cap is enforced by EVICTION, not refusal: a novel key is ALWAYS
	// recorded, even past capacity, so a later duplicate is caught and the
	// caller (storeDeliveryReceipt) never re-appends the same receipt forever.
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingHashDedup(time.Hour, 4, clock) // tiny cap

	// Flood well past the cap.
	for i := 0; i < 1000; i++ {
		r.Add(fmt.Sprintf("flood-%d", i))
	}
	// A brand-new key, arriving over the cap, must still be recorded.
	if r.MarkIfAbsent("fresh") {
		t.Fatal("a novel key must report absent (false) on first sight")
	}
	if !r.MarkIfAbsent("fresh") {
		t.Fatal("a just-recorded key must dedupe immediately, even past the cap")
	}
}
