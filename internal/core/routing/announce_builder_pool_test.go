package routing

import (
	"fmt"
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_builder_pool_test.go pins the contract that BuildAnnounceSnapshot's
// pooled scratch (seen / deduped / bestBySeq / groups, recycled via sync.Pool)
// never leaks state across calls: a recycled map/slice carrying entries from a
// previous, larger input must produce a result identical to a cold call. Run
// the concurrency test under `-race` to catch any accidental sharing of pooled
// scratch between the per-peer announce goroutines.

func entriesEqual(a, b []AnnounceEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Identity != b[i].Identity ||
			a[i].Origin != b[i].Origin ||
			a[i].SeqNo != b[i].SeqNo ||
			a[i].Hops != b[i].Hops ||
			string(a[i].Extra) != string(b[i].Extra) ||
			string(a[i].AttestedSig) != string(b[i].AttestedSig) {
			return false
		}
	}
	return true
}

// liveAndTombInput exercises all three aggregation stages (dedup, per-SeqNo
// collapse, per-Identity live+tombstone split) so the pooled groups map sees
// both a liveBest and a tombBest slot populated.
func liveAndTombInput(n int) []AnnounceEntry {
	raw := make([]AnnounceEntry, 0, n*3)
	for i := 0; i < n; i++ {
		id := domaintest.ID(fmt.Sprintf("dest-%d", i))
		origin := domaintest.ID(fmt.Sprintf("origin-%d", i))
		// Two live duplicates (dedup + min-Hops) plus an own-origin tombstone.
		raw = append(raw,
			AnnounceEntry{Identity: id, Origin: origin, SeqNo: 5, Hops: 3},
			AnnounceEntry{Identity: id, Origin: origin, SeqNo: 5, Hops: 4},
			AnnounceEntry{Identity: id, Origin: origin, SeqNo: 6, Hops: HopsInfinity},
		)
	}
	return raw
}

func TestBuildAnnounceSnapshot_PoolReuseNoCrossCallLeak(t *testing.T) {
	// A large input grows the pooled scratch; a subsequent small input must
	// reuse the cleared scratch and return exactly what a cold call returns.
	big := liveAndTombInput(50)
	small := liveAndTombInput(2)
	empty := []AnnounceEntry{}

	// Reference results computed with a freshly-grown pool state. Since the
	// pool is package-global, we capture each reference by comparing the
	// shape we expect rather than a separate "cold" process: every identity
	// must yield exactly one live winner (min Hops=3) and one tombstone.
	wantPerIdentity := func(snap *AnnounceSnapshot, n int) {
		t.Helper()
		if len(snap.Entries) != n*2 {
			t.Fatalf("expected %d entries (%d live + %d tombstone), got %d", n*2, n, n, len(snap.Entries))
		}
		live, tomb := 0, 0
		for _, e := range snap.Entries {
			if e.Hops >= HopsInfinity {
				tomb++
				continue
			}
			live++
			if e.Hops != 3 {
				t.Fatalf("live winner must be min Hops=3, got %d", e.Hops)
			}
		}
		if live != n || tomb != n {
			t.Fatalf("expected %d live + %d tombstone, got %d + %d", n, n, live, tomb)
		}
	}

	// Drive the pool through grow → shrink → empty → grow again.
	wantPerIdentity(BuildAnnounceSnapshot(big), 50)
	wantPerIdentity(BuildAnnounceSnapshot(small), 2)

	if got := BuildAnnounceSnapshot(empty); len(got.Entries) != 0 {
		t.Fatalf("empty input must yield no entries, got %d", len(got.Entries))
	}

	// After the small/empty calls recycled the (previously large) scratch,
	// a big call must again be fully correct — no stale identities survive.
	wantPerIdentity(BuildAnnounceSnapshot(big), 50)
}

func TestBuildAnnounceSnapshot_PoolReuseDeterministic(t *testing.T) {
	// The same input built twice in a row (second call reusing the first
	// call's recycled scratch) must be byte-for-byte identical.
	in := liveAndTombInput(20)
	first := BuildAnnounceSnapshot(in)
	second := BuildAnnounceSnapshot(in)
	if !entriesEqual(first.Entries, second.Entries) {
		t.Fatal("repeated BuildAnnounceSnapshot on identical input diverged — pooled scratch leaked state")
	}
}

func TestBuildAnnounceSnapshot_PoolConcurrentSafe(t *testing.T) {
	// Each goroutine builds from its own distinct input and compares against
	// the single-threaded reference for that input. With pooled scratch this
	// is the case that breaks if two goroutines ever share a map. Run under
	// -race.
	const workers = 32
	inputs := make([][]AnnounceEntry, workers)
	want := make([]*AnnounceSnapshot, workers)
	for w := 0; w < workers; w++ {
		// Distinct identity namespace per worker so any cross-goroutine
		// scratch bleed would surface as a wrong/extra identity.
		raw := make([]AnnounceEntry, 0, 8)
		for i := 0; i < 8; i++ {
			id := domaintest.ID(fmt.Sprintf("w%d-dest-%d", w, i))
			origin := domaintest.ID(fmt.Sprintf("w%d-origin-%d", w, i))
			raw = append(raw,
				AnnounceEntry{Identity: id, Origin: origin, SeqNo: 9, Hops: 2},
				AnnounceEntry{Identity: id, Origin: origin, SeqNo: 9, Hops: 5},
			)
		}
		inputs[w] = raw
		want[w] = BuildAnnounceSnapshot(raw)
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for iter := 0; iter < 200; iter++ {
				got := BuildAnnounceSnapshot(inputs[w])
				if !entriesEqual(got.Entries, want[w].Entries) {
					t.Errorf("worker %d iter %d: pooled scratch produced a wrong snapshot", w, iter)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}
