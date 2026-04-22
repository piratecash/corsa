package node

import (
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// TestBuildPeerExchangeResponseDoesNotBlockOnSMu is the regression test for
// the get_peers hang described in §B.6 of the bug postmortem.  Before the
// atomic snapshot fix, buildPeerExchangeResponse() acquired s.peerMu.RLock() to
// iterate s.health looking for inbound-connected peers.  Go's RWMutex is
// writer-preferring: any queued writer blocks subsequent RLock callers, so a
// long-running writer on s.peerMu (sendAnnounceRoutesToInbound on a sync
// network write, admission decisions, etc.) starved get_peers for the full
// writer hold time regardless of how short the RLock section itself was.
//
// The fix: buildPeerExchangeResponse() reads a precomputed peersExchangeSnapshot
// via atomic load.  The snapshot is rebuilt by hotReadsRefreshLoop under a
// short s.peerMu.RLock.  The RPC handler never acquires s.peerMu.
//
// This test holds s.peerMu.Lock for 2 s from a helper goroutine and asserts that
// buildPeerExchangeResponse() completes within 100 ms.  With the old
// implementation the call would block for the full 2 s.
func TestBuildPeerExchangeResponseDoesNotBlockOnSMu(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Seed an inbound-connected health entry plus a persistedMeta row so the
	// rebuilt snapshot has non-trivial content — the test should prove the
	// handler picks up real data from the cache, not just that an empty
	// snapshot loads quickly.
	inboundAddr := domain.PeerAddress("198.51.100.7:5050")
	svc.peerMu.Lock()
	svc.health[inboundAddr] = &peerHealth{
		Address:   inboundAddr,
		Connected: true,
		Direction: peerDirectionInbound,
	}
	now := time.Now()
	svc.persistedMeta[inboundAddr] = &peerEntry{
		Address:       inboundAddr,
		AddedAt:       &now,
		AnnounceState: announceStateAnnounceable,
	}
	svc.peerMu.Unlock()

	// Prime the snapshot BEFORE the writer grabs s.peerMu.  In production
	// hotReadsRefreshLoop does this every networkStatsSnapshotInterval; in
	// this test we skip the Run() wiring and prime directly.
	svc.rebuildPeersExchangeSnapshot()

	snap := svc.loadPeersExchangeSnapshot()
	if snap == nil {
		t.Fatal("expected primed peers_exchange snapshot, got nil")
	}
	if len(snap.inboundConnected) == 0 {
		t.Fatal("primed snapshot has zero inbound peers despite seeded health")
	}
	if _, ok := snap.announceable[inboundAddr]; !ok {
		t.Fatalf("primed snapshot missing announceable entry for seeded peer %q", inboundAddr)
	}

	// Hold s.peerMu.Lock for 2 s from a helper goroutine — the same writer
	// storm shape that starved RLock callers in production.
	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		svc.peerMu.Lock()
		close(writerStarted)
		time.Sleep(2 * time.Second)
		svc.peerMu.Unlock()
		close(writerDone)
	}()
	<-writerStarted

	start := time.Now()
	addrs := svc.buildPeerExchangeResponse(nil)
	elapsed := time.Since(start)

	// 100 ms leaves plenty of headroom for slow CI with GOMAXPROCS=1 while
	// still catching any regression that pulls the handler back onto s.peerMu.
	const maxTolerated = 100 * time.Millisecond
	if elapsed > maxTolerated {
		t.Fatalf("buildPeerExchangeResponse took %s while writer held s.peerMu; expected < %s (RPC still coupled to s.peerMu)", elapsed, maxTolerated)
	}

	// Content sanity: seeded inbound address is present in the response.
	// This distinguishes a real fix from a stub that just returns quickly
	// without data.
	var found bool
	for _, a := range addrs {
		if a == inboundAddr {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("seeded inbound peer %q missing from get_peers response: %v", inboundAddr, addrs)
	}

	<-writerDone
}

// TestPeersExchangeSnapshotMissDoesNotRebuild pins the lock-free hot-path
// contract: when no snapshot has been primed (e.g. a unit test that bypasses
// Run() and therefore skips primeHotReadSnapshots), buildPeerExchangeResponse()
// must NOT synchronously rebuild.  The synchronous rebuild was removed because
// it acquired s.peerMu.RLock on the RPC goroutine and re-coupled the hot path to
// s.peerMu — exactly the starvation shape the snapshot infrastructure was
// built to eliminate.  The handler must still return without panicking on
// nil snapshots (it degrades to an empty/default response).
//
// In production, Run() calls primeHotReadSnapshots() before the listener
// opens so this branch never fires on real traffic.
func TestPeersExchangeSnapshotMissDoesNotRebuild(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	if snap := svc.loadPeersExchangeSnapshot(); snap != nil {
		t.Fatalf("expected nil snapshot before any rebuild; got %+v", snap)
	}

	// The call itself must not panic on a nil snapshot — every pxSnap and
	// cmSlotsSnap dereference is guarded.
	_ = svc.buildPeerExchangeResponse(nil)

	// The handler must NOT silently populate the cache as a side effect.
	// Doing so would mean the RPC path had reached s.peerMu.RLock, violating
	// the lock-free contract.
	if svc.loadPeersExchangeSnapshot() != nil {
		t.Fatal("buildPeerExchangeResponse() must not synchronously rebuild on miss (lock-free contract)")
	}
}

// TestPeersExchangeSnapshotRefreshReflectsMutations verifies that the
// refresher picks up mutations of s.persistedMeta / s.health between rebuilds
// and publishes a fresh *peersExchangeSnapshot pointer, not an in-place
// mutation.
func TestPeersExchangeSnapshotRefreshReflectsMutations(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("203.0.113.4:5050")

	// First snapshot: peer is known in persistedMeta but NOT announceable.
	// get_peers must treat it as suppressed.
	now := time.Now()
	svc.peerMu.Lock()
	svc.persistedMeta[addr] = &peerEntry{
		Address:       addr,
		AddedAt:       &now,
		AnnounceState: announceStateDirectOnly,
	}
	svc.health[addr] = &peerHealth{
		Address:   addr,
		Connected: true,
		Direction: peerDirectionInbound,
	}
	svc.peerMu.Unlock()

	svc.rebuildPeersExchangeSnapshot()
	snap1 := svc.loadPeersExchangeSnapshot()
	if snap1 == nil {
		t.Fatal("first snapshot nil after rebuild")
	}
	if _, known := snap1.knownInPersistedMeta[addr]; !known {
		t.Fatalf("first snapshot missing %q from knownInPersistedMeta", addr)
	}
	if _, announce := snap1.announceable[addr]; announce {
		t.Fatalf("first snapshot unexpectedly flagged %q announceable", addr)
	}
	if snap1.isAnnounceable(addr) {
		t.Fatal("first snapshot isAnnounceable() must be false for direct_only peer")
	}

	// Mutate persistedMeta: flip the announce state to announceable.
	svc.peerMu.Lock()
	svc.persistedMeta[addr].AnnounceState = announceStateAnnounceable
	svc.peerMu.Unlock()

	// Until the refresher runs, the cached snapshot still reflects the old
	// decision — bounded staleness, asserted here so it does not silently
	// regress into eager-update.
	if svc.loadPeersExchangeSnapshot().isAnnounceable(addr) {
		t.Fatal("cache must be bounded-stale until refresh (still direct_only)")
	}

	svc.rebuildPeersExchangeSnapshot()
	snap2 := svc.loadPeersExchangeSnapshot()
	if !snap2.isAnnounceable(addr) {
		t.Fatalf("second snapshot: expected %q announceable after flip", addr)
	}
	if snap1 == snap2 {
		t.Fatal("rebuild must publish a new *peersExchangeSnapshot, not mutate the old one")
	}
}

// TestPeersExchangeSnapshotConcurrentLoadDoesNotRace runs the RPC path and
// the refresher concurrently for a short burst.  Under the atomic.Pointer
// contract this must not race; the -race detector will flag any accidental
// shared-state mutation.
func TestPeersExchangeSnapshotConcurrentLoadDoesNotRace(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildPeersExchangeSnapshot()

	const workers = 8
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				svc.rebuildPeersExchangeSnapshot()
			}
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_ = svc.buildPeerExchangeResponse(nil)
			}
		}()
	}
	wg.Wait()

	if svc.loadPeersExchangeSnapshot() == nil {
		t.Fatal("cache nil after concurrent rebuild/load burst")
	}
}
