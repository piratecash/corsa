package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestNetworkStatsFrameDoesNotBlockOnSMu is the regression test for the
// 116-second fetch_network_stats stall observed in production when a writer
// held s.peerMu (sendAnnounceRoutesToInbound waiting on a sync network
// write).  The "SMu" in the test name preserves the historical identifier
// from the pre-split era — the lock has since been renamed s.peerMu.
//
// Before the atomic snapshot fix, networkStatsFrame() acquired s.peerMu.RLock
// directly.  Go's RWMutex is writer-preferring: once any writer queues on
// s.peerMu.Lock(), subsequent RLock callers block behind it.  The RPC would
// therefore sit in s.peerMu.RLock() for the entire writer hold time — up to
// the command timeout (≈90 s) — freezing the UI.
//
// The fix: networkStatsFrame() reads a precomputed snapshot via atomic
// load.  The snapshot is rebuilt by a background refresher under a short
// s.peerMu.RLock.  RPC reader never acquires s.peerMu.
//
// The test holds s.peerMu.Lock for 2 seconds from a helper goroutine and asserts
// that networkStatsFrame() completes within 100 ms.  With the old
// implementation the call would block for the full 2 s; with the fix it
// returns almost instantly because the snapshot load path never touches
// s.peerMu.
func TestNetworkStatsFrameDoesNotBlockOnSMu(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Seed a realistic health map so the rebuilt snapshot has non-trivial
	// content — the test should prove freshness of the cached data, not
	// just that an empty snapshot loads quickly.
	addr := domain.PeerAddress("test-peer-1")
	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     1000,
		BytesReceived: 2000,
		Connected:     true,
	}
	svc.peerMu.Unlock()

	// Prime the snapshot once BEFORE the writer grabs s.peerMu.  In production
	// the background refresher does this every networkStatsSnapshotInterval;
	// in this test we skip the Run() wiring and prime directly.
	svc.rebuildNetworkStatsSnapshot()

	// Sanity: the cached snapshot reflects the seeded health entry.
	snap := svc.loadNetworkStatsSnapshot()
	if snap == nil {
		t.Fatal("expected primed snapshot, got nil")
	}
	if snap.knownPeers == 0 {
		t.Fatal("primed snapshot has zero known peers despite seeded health")
	}

	// Hold s.peerMu.Lock for 2 s from a helper goroutine — the exact pattern
	// that starves RLock readers in production.
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		svc.peerMu.Lock()
		close(writerStarted)
		time.Sleep(2 * time.Second)
		svc.peerMu.Unlock()
		close(writerDone)
	}()

	// Wait for the writer to actually hold the mutex before measuring the
	// RPC timing.  Without this, the test could race and call the RPC
	// before the writer acquired s.peerMu, giving a false pass.
	<-writerStarted

	start := time.Now()
	frame := svc.networkStatsFrame()
	elapsed := time.Since(start)

	// 100 ms is 20× the snapshot refresh interval (500 ms would be too
	// close to the writer hold; 10 ms would flap on CI).  A fast machine
	// returns in microseconds; a slow CI with GOMAXPROCS=1 still fits
	// comfortably.
	const maxTolerated = 100 * time.Millisecond
	if elapsed > maxTolerated {
		t.Fatalf("networkStatsFrame took %s while writer held s.peerMu; expected < %s (RPC still coupled to s.peerMu)", elapsed, maxTolerated)
	}

	// Content sanity: the frame returned is the primed snapshot, not an
	// empty fallback.  This distinguishes a real fix from a stub that just
	// returns quickly without data.
	if frame.NetworkStats == nil {
		t.Fatal("expected non-nil NetworkStats in returned frame")
	}
	if frame.NetworkStats.KnownPeers == 0 {
		t.Fatal("returned frame has zero known_peers despite primed snapshot")
	}
	if frame.NetworkStats.TotalTraffic != 3000 {
		t.Fatalf("expected total_traffic=3000 from seeded health, got %d", frame.NetworkStats.TotalTraffic)
	}

	// Let the writer finish so t.Cleanup can run without the writer leak.
	<-writerDone
}

// TestNetworkStatsFrameSnapshotMissReturnsEmptyFrame pins the lock-free
// hot-path contract: when no snapshot has been primed (e.g. a unit test
// that bypasses Run() and therefore skips primeHotReadSnapshots),
// networkStatsFrame() returns an empty-but-valid network_stats frame
// rather than synchronously rebuilding.  The synchronous rebuild was
// removed because it acquired s.peerMu.RLock on the RPC goroutine and
// re-coupled the hot path to s.peerMu — exactly the starvation shape the
// snapshot infrastructure was built to eliminate.
//
// In production, Run() calls primeHotReadSnapshots() before the
// listener opens so this branch never fires on real traffic.
func TestNetworkStatsFrameSnapshotMissReturnsEmptyFrame(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	if snap := svc.loadNetworkStatsSnapshot(); snap != nil {
		t.Fatalf("expected nil snapshot before any rebuild; got %+v", snap)
	}

	frame := svc.networkStatsFrame()
	if frame.Type != "network_stats" {
		t.Fatalf("expected network_stats frame type, got %q", frame.Type)
	}
	if frame.NetworkStats == nil {
		t.Fatal("expected non-nil NetworkStats (empty-but-valid) on snapshot miss")
	}
	if frame.NetworkStats.TotalTraffic != 0 ||
		frame.NetworkStats.ConnectedPeers != 0 ||
		frame.NetworkStats.KnownPeers != 0 ||
		len(frame.NetworkStats.PeerTraffic) != 0 {
		t.Fatalf("expected zeroed NetworkStats on snapshot miss, got %+v", frame.NetworkStats)
	}

	// The handler must NOT silently populate the cache as a side effect.
	// Doing so would mean the RPC path had reached s.peerMu.RLock, violating
	// the lock-free contract.
	if svc.loadNetworkStatsSnapshot() != nil {
		t.Fatal("networkStatsFrame() must not synchronously rebuild on miss (lock-free contract)")
	}
}

// TestNetworkStatsSnapshotRefreshReflectsMutations verifies that the
// refresher observes changes in s.health between rebuilds — protecting
// against a regression where the rebuild somehow captured a frozen copy.
func TestNetworkStatsSnapshotRefreshReflectsMutations(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("traffic-peer-1")

	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     100,
		BytesReceived: 200,
		Connected:     true,
	}
	svc.peerMu.Unlock()

	svc.rebuildNetworkStatsSnapshot()
	snap1 := svc.loadNetworkStatsSnapshot()
	if snap1 == nil || snap1.totalSent != 100 || snap1.totalReceived != 200 {
		t.Fatalf("first snapshot: expected sent=100/recv=200, got %+v", snap1)
	}

	// Mutate health — simulates an accumulate* call finishing.
	svc.peerMu.Lock()
	svc.health[addr].BytesSent = 500
	svc.health[addr].BytesReceived = 700
	svc.peerMu.Unlock()

	// Until the refresher runs, the cached snapshot still reflects the
	// previous value.  That is intentional (bounded staleness) and is
	// asserted here so it does not silently regress into eager-update.
	snapStale := svc.loadNetworkStatsSnapshot()
	if snapStale.totalSent != 100 {
		t.Fatalf("cache must be bounded-stale until refresh: expected 100, got %d", snapStale.totalSent)
	}

	svc.rebuildNetworkStatsSnapshot()
	snap2 := svc.loadNetworkStatsSnapshot()
	if snap2 == nil || snap2.totalSent != 500 || snap2.totalReceived != 700 {
		t.Fatalf("second snapshot: expected sent=500/recv=700, got %+v", snap2)
	}

	// Identity check: the Store replaced the pointer, not mutated it in
	// place.  Mutating in place would race with concurrent RPC loads.
	if snap1 == snap2 {
		t.Fatal("rebuild must publish a new *networkStatsSnapshot, not mutate the old one")
	}
}

// TestNetworkStatsSnapshotConcurrentLoadDoesNotRace runs the RPC path and
// the refresher concurrently for a short burst.  Under the atomic.Pointer
// contract this must not race; the -race detector will flag any accidental
// shared-state mutation.
func TestNetworkStatsSnapshotConcurrentLoadDoesNotRace(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildNetworkStatsSnapshot()

	const workers = 8
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(workers * 2)
	// Rebuilders.
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				svc.rebuildNetworkStatsSnapshot()
			}
		}()
	}
	// Readers.
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				frame := svc.networkStatsFrame()
				if frame.Type != "network_stats" {
					t.Errorf("unexpected frame type: %q", frame.Type)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Final invariant: after the burst, the cache is still populated.
	if svc.loadNetworkStatsSnapshot() == nil {
		t.Fatal("cache nil after concurrent rebuild/load burst")
	}
}

// Ensure we exercise the exported protocol type so package imports stay
// honest if the protocol schema changes.
var _ = protocol.NetworkStatsFrame{}
