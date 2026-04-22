package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// TestPeerHealthFramesDoesNotBlockOnSMu is the regression test for the
// fetch_peer_health hang observed under the same writer-storm conditions that
// used to freeze fetch_network_stats (see §B.6 in the bug postmortem).
//
// Before the atomic snapshot fix, peerHealthFrames() started with
// s.mu.RLock().  Go's RWMutex is writer-preferring: once any writer queues on
// s.mu.Lock(), subsequent RLock callers block behind it.  A writer holding
// s.mu for tens of seconds (sendAnnounceRoutesToInbound on a sync network
// write, admission decisions under s.mu.Lock, etc.) therefore starved
// ProbeNode / fetch_peer_health until the command timeout.
//
// The fix: peerHealthFrames() reads a precomputed snapshot via atomic load.
// The snapshot is rebuilt by a background refresher under a short s.mu.RLock.
// The RPC reader never acquires s.mu.
//
// This test holds s.mu.Lock for 2 s from a helper goroutine and asserts that
// peerHealthFrames() completes within 100 ms.  With the old implementation
// the call would block for the full 2 s.
func TestPeerHealthFramesDoesNotBlockOnSMu(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Seed a realistic health map so the rebuilt snapshot has non-trivial
	// content — the test should prove freshness of the cached data, not
	// just that an empty snapshot loads quickly.
	addr := domain.PeerAddress("test-peer-1")
	svc.mu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     1111,
		BytesReceived: 2222,
		Connected:     true,
		Direction:     peerDirectionOutbound,
	}
	svc.mu.Unlock()

	// Prime the snapshot once BEFORE the writer grabs s.mu.  In production
	// hotReadsRefreshLoop does this every networkStatsSnapshotInterval; in
	// this test we skip the Run() wiring and prime directly.
	svc.rebuildPeerHealthSnapshot()

	snap := svc.loadPeerHealthSnapshot()
	if snap == nil {
		t.Fatal("expected primed peer-health snapshot, got nil")
	}
	if len(snap.records) == 0 {
		t.Fatal("primed snapshot has zero records despite seeded health")
	}

	// Hold s.mu.Lock for 2 s from a helper goroutine.
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		svc.mu.Lock()
		close(writerStarted)
		time.Sleep(2 * time.Second)
		svc.mu.Unlock()
		close(writerDone)
	}()
	<-writerStarted

	start := time.Now()
	frames := svc.peerHealthFrames()
	elapsed := time.Since(start)

	// 100 ms leaves plenty of headroom for slow CI with GOMAXPROCS=1 while
	// still catching any regression that pulls the handler back onto s.mu.
	const maxTolerated = 100 * time.Millisecond
	if elapsed > maxTolerated {
		t.Fatalf("peerHealthFrames took %s while writer held s.mu; expected < %s (RPC still coupled to s.mu)", elapsed, maxTolerated)
	}

	// Content sanity: seeded peer appears with the byte counts we set.
	var found bool
	for _, f := range frames {
		if f.Address != string(addr) {
			continue
		}
		found = true
		if f.BytesSent != 1111 || f.BytesReceived != 2222 {
			t.Fatalf("expected sent=1111/received=2222, got sent=%d/received=%d", f.BytesSent, f.BytesReceived)
		}
		if f.TotalTraffic != 3333 {
			t.Fatalf("expected total_traffic=3333, got %d", f.TotalTraffic)
		}
	}
	if !found {
		t.Fatalf("seeded peer %q missing from frames", addr)
	}

	<-writerDone
}

// TestPeerHealthFramesSnapshotMissReturnsNil pins the lock-free hot-path
// contract: when no snapshot has been primed (e.g. a unit test that bypasses
// Run() and therefore skips primeHotReadSnapshots), peerHealthFrames()
// returns nil rather than synchronously rebuilding.  The synchronous
// rebuild was removed because it acquired s.mu.RLock on the RPC goroutine
// and re-coupled the hot path to s.mu — exactly the starvation shape the
// snapshot infrastructure was built to eliminate.
//
// In production, Run() calls primeHotReadSnapshots() before the listener
// opens so this branch never fires on real traffic.
func TestPeerHealthFramesSnapshotMissReturnsNil(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	if snap := svc.loadPeerHealthSnapshot(); snap != nil {
		t.Fatalf("expected nil snapshot before any rebuild; got %+v", snap)
	}

	frames := svc.peerHealthFrames()
	if frames != nil {
		t.Fatalf("expected nil frames on snapshot miss (lock-free contract); got %d", len(frames))
	}

	// The handler must NOT silently populate the cache as a side effect.
	// Doing so would mean the RPC path had reached s.mu.RLock, violating
	// the lock-free contract.
	if svc.loadPeerHealthSnapshot() != nil {
		t.Fatal("peerHealthFrames() must not synchronously rebuild on miss (lock-free contract)")
	}
}

// TestPeerHealthSnapshotRefreshReflectsMutations verifies that the refresher
// picks up mutations of s.health between rebuilds and publishes a fresh
// *peerHealthSnapshot pointer, not an in-place mutation.
func TestPeerHealthSnapshotRefreshReflectsMutations(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("mutation-peer-1")

	svc.mu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     100,
		BytesReceived: 200,
		Connected:     true,
		Direction:     peerDirectionOutbound,
	}
	svc.mu.Unlock()

	svc.rebuildPeerHealthSnapshot()
	snap1 := svc.loadPeerHealthSnapshot()
	if snap1 == nil || len(snap1.records) == 0 {
		t.Fatalf("first snapshot empty: %+v", snap1)
	}
	if snap1.records[0].health.BytesSent != 100 {
		t.Fatalf("first snapshot: expected sent=100, got %d", snap1.records[0].health.BytesSent)
	}

	// Mutate health.
	svc.mu.Lock()
	svc.health[addr].BytesSent = 999
	svc.mu.Unlock()

	// Until the refresher runs, the cached snapshot still reflects the old
	// value — bounded staleness, asserted here so it does not silently
	// regress into eager-update.
	if svc.loadPeerHealthSnapshot().records[0].health.BytesSent != 100 {
		t.Fatal("cache must be bounded-stale until refresh")
	}

	svc.rebuildPeerHealthSnapshot()
	snap2 := svc.loadPeerHealthSnapshot()
	if snap2.records[0].health.BytesSent != 999 {
		t.Fatalf("second snapshot: expected sent=999, got %d", snap2.records[0].health.BytesSent)
	}
	if snap1 == snap2 {
		t.Fatal("rebuild must publish a new *peerHealthSnapshot, not mutate the old one")
	}
}

// TestPeerHealthSnapshotConcurrentLoadDoesNotRace runs the RPC path and the
// refresher concurrently for a short burst.  Under the atomic.Pointer
// contract this must not race; the -race detector will flag any accidental
// shared-state mutation.
func TestPeerHealthSnapshotConcurrentLoadDoesNotRace(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildPeerHealthSnapshot()

	const workers = 8
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				svc.rebuildPeerHealthSnapshot()
			}
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = svc.peerHealthFrames()
			}
		}()
	}
	wg.Wait()

	if svc.loadPeerHealthSnapshot() == nil {
		t.Fatal("cache nil after concurrent rebuild/load burst")
	}
}
