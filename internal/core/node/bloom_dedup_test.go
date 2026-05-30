package node

import (
	"fmt"
	"testing"
	"time"
)

// bloom_dedup_test.go pins the Phase 4 13.4 rotating Bloom dedup
// contract: no false negatives within the current-window, eviction
// after two rotations, false-positive rate bounded by the m/k
// parameters, and concurrent Add/Has under the internal mutex.

func TestRotatingBloomDedup_AddThenHasInSameWindow(t *testing.T) {
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
	r.Add("msg-1")
	if !r.Has("msg-1") {
		t.Fatal("Add then Has in same window must return true (no false negative)")
	}
	if r.Has("msg-unknown") {
		// Not a hard failure — fp possible but extremely unlikely on a
		// single unknown key against an empty filter. Flag as warning
		// via Logf so a regression in the hashing path stands out.
		t.Logf("warning: unknown key reported as present — possible FP on empty filter")
	}
}

func TestRotatingBloomDedup_SurvivesOneRotation(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, time.Minute, clock)

	r.Add("kept")
	// Advance one rotation: kept moves from current → previous; still Has.
	now = now.Add(time.Minute + time.Second)
	if !r.Has("kept") {
		t.Fatal("key must survive ONE rotation (previous filter still queried)")
	}
}

func TestRotatingBloomDedup_EvictedAfterTwoRotations(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, time.Minute, clock)

	r.Add("doomed")
	// First rotation: doomed in previous.
	now = now.Add(time.Minute + time.Second)
	r.Add("trigger") // force rotation evaluation
	// Second rotation: doomed evicted from previous (previous = empty,
	// current was the post-first-rotation filter which holds "trigger").
	now = now.Add(time.Minute + time.Second)
	if r.Has("doomed") {
		t.Fatal("key must be evicted after two rotations (eviction window upper bound)")
	}
	// trigger should still survive one rotation.
	if !r.Has("trigger") {
		t.Fatal("post-rotation key must survive its own one rotation")
	}
}

func TestRotatingBloomDedup_LongJumpResetsBothFilters(t *testing.T) {
	// A clock jump beyond two rotation intervals (node slept past the
	// window) must reset BOTH filters — keeping the previous from N
	// intervals ago would extend the dedup persistence beyond the
	// documented upper bound. See rotateIfDueLocked.
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, time.Minute, clock)
	r.Add("ancient")
	now = now.Add(10 * time.Minute)
	if r.Has("ancient") {
		t.Fatal("clock jump past 2x rotation must clear both filters")
	}
}

func TestRotatingBloomDedup_FalsePositiveRateUnderTarget(t *testing.T) {
	// Stress: add 10k unique keys and probe 10k different ones. FP
	// rate must stay under the 0.1% spec target with margin.
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
	for i := 0; i < 10000; i++ {
		r.Add(fmt.Sprintf("known-%d", i))
	}
	fp := 0
	for i := 0; i < 10000; i++ {
		if r.Has(fmt.Sprintf("probe-%d", i)) {
			fp++
		}
	}
	// 0.1% of 10k = 10. Allow up to 20 (2x margin for randomness).
	if fp > 20 {
		t.Fatalf("false-positive rate too high: got %d/10000 (>0.2%%)", fp)
	}
}

func TestRotatingBloomDedup_AddIsIdempotent(t *testing.T) {
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
	r.Add("dup")
	r.Add("dup")
	r.Add("dup")
	if !r.Has("dup") {
		t.Fatal("repeated Add must remain Has=true")
	}
}

func TestRotatingBloomDedup_ConcurrentAccess(t *testing.T) {
	// Smoke test for the internal mutex: parallel Add/Has must not
	// race or panic. Race detector catches misuse if present.
	r := newRotatingBloomDedup(bloomDedupBits, bloomDedupHashes, bloomDedupRotation, nil)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(id int) {
			for j := 0; j < 1000; j++ {
				key := fmt.Sprintf("g%d-k%d", id, j)
				r.Add(key)
				_ = r.Has(key)
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
