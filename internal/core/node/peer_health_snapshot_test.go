package node

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// TestPeerHealthFramesDoesNotBlockOnSMu is the regression test for the
// fetch_peer_health hang observed under the same writer-storm conditions that
// used to freeze fetch_network_stats (see §B.6 in the bug postmortem).
//
// Before the atomic snapshot fix, peerHealthFrames() started with
// s.peerMu.RLock().  Go's RWMutex is writer-preferring: once any writer queues on
// s.peerMu.Lock(), subsequent RLock callers block behind it.  A writer holding
// s.peerMu for tens of seconds (sendAnnounceRoutesToInbound on a sync network
// write, admission decisions under s.peerMu.Lock, etc.) therefore starved
// ProbeNode / fetch_peer_health until the command timeout.
//
// The fix: peerHealthFrames() reads a precomputed snapshot via atomic load.
// The snapshot is rebuilt by a background refresher under a short s.peerMu.RLock.
// The RPC reader never acquires s.peerMu.
//
// This test holds s.peerMu.Lock for 2 s from a helper goroutine and asserts that
// peerHealthFrames() completes within 100 ms.  With the old implementation
// the call would block for the full 2 s.
func TestPeerHealthFramesDoesNotBlockOnSMu(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Seed a realistic health map so the rebuilt snapshot has non-trivial
	// content — the test should prove freshness of the cached data, not
	// just that an empty snapshot loads quickly.
	addr := domain.PeerAddress("test-peer-1")
	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     1111,
		BytesReceived: 2222,
		Connected:     true,
		Direction:     peerDirectionOutbound,
	}
	svc.peerMu.Unlock()

	// Prime the snapshot once BEFORE the writer grabs s.peerMu.  In production
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

	// Hold s.peerMu.Lock for 2 s from a helper goroutine.
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
	frames := svc.peerHealthFrames()
	elapsed := time.Since(start)

	// 100 ms leaves plenty of headroom for slow CI with GOMAXPROCS=1 while
	// still catching any regression that pulls the handler back onto s.peerMu.
	const maxTolerated = 100 * time.Millisecond
	if elapsed > maxTolerated {
		t.Fatalf("peerHealthFrames took %s while writer held s.peerMu; expected < %s (RPC still coupled to s.peerMu)", elapsed, maxTolerated)
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
// rebuild was removed because it acquired s.peerMu.RLock on the RPC goroutine
// and re-coupled the hot path to s.peerMu — exactly the starvation shape
// the snapshot infrastructure was built to eliminate.
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
	// Doing so would mean the RPC path had reached s.peerMu.RLock, violating
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

	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{
		Address:       addr,
		BytesSent:     100,
		BytesReceived: 200,
		Connected:     true,
		Direction:     peerDirectionOutbound,
	}
	svc.peerMu.Unlock()

	svc.rebuildPeerHealthSnapshot()
	snap1 := svc.loadPeerHealthSnapshot()
	if snap1 == nil || len(snap1.records) == 0 {
		t.Fatalf("first snapshot empty: %+v", snap1)
	}
	if snap1.records[0].health.BytesSent != 100 {
		t.Fatalf("first snapshot: expected sent=100, got %d", snap1.records[0].health.BytesSent)
	}

	// Mutate health.
	svc.peerMu.Lock()
	svc.health[addr].BytesSent = 999
	svc.peerMu.Unlock()

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

// TestMaybeRebuildPeerHealthSnapshot_GatesOnRecentReader pins the headless-node
// optimisation: the periodic refresher's gated entry point rebuilds only when
// a consumer has read peer-health within peerHealthRebuildIdleAfter, and is a
// no-op otherwise.  Rebuild publishes a fresh pointer, so a changed pointer
// means a rebuild ran and an unchanged pointer means it was skipped.
func TestMaybeRebuildPeerHealthSnapshot_GatesOnRecentReader(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("gate-peer-1")
	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{Address: addr, Connected: true, Direction: peerDirectionOutbound}
	svc.peerMu.Unlock()

	// Prime once so there is a baseline snapshot pointer to compare against.
	svc.rebuildPeerHealthSnapshot()
	base := svc.loadPeerHealthSnapshot()
	if base == nil {
		t.Fatal("expected primed snapshot")
	}

	// No reader has ever called peerHealthFrames (access timestamp is zero):
	// the gated rebuild must skip, leaving the pointer untouched.
	svc.maybeRebuildPeerHealthSnapshot()
	if svc.loadPeerHealthSnapshot() != base {
		t.Fatal("gated rebuild ran despite no reader ever accessing the snapshot")
	}

	// Simulate a stale reader (last access well outside the idle window):
	// still a no-op.
	svc.peerHealthAccessNanos.Store(time.Now().Add(-2 * peerHealthRebuildIdleAfter).UnixNano())
	svc.maybeRebuildPeerHealthSnapshot()
	if svc.loadPeerHealthSnapshot() != base {
		t.Fatal("gated rebuild ran for a reader outside the idle window")
	}

	// Simulate a fresh reader: the gated rebuild must now run and publish a
	// new pointer.
	svc.peerHealthAccessNanos.Store(time.Now().UnixNano())
	svc.maybeRebuildPeerHealthSnapshot()
	if svc.loadPeerHealthSnapshot() == base {
		t.Fatal("gated rebuild skipped despite a recent reader")
	}

	// And peerHealthFrames itself must record access, re-arming the gate.
	svc.peerHealthAccessNanos.Store(0)
	_ = svc.peerHealthFrames()
	if svc.peerHealthAccessNanos.Load() == 0 {
		t.Fatal("peerHealthFrames must record reader access")
	}
}

// TestRebuildPeerHealthSnapshot_InboundIndexMatchesPerPeerScan verifies the
// Level-1 churn fix: rebuildPeerHealthSnapshot now builds the inbound
// address→{ids,caps} index ONCE and looks peers up in it, instead of a
// per-peer inbound scan (each a full s.conns walk) for conn IDs and caps.
// This pins that the indexed path still surfaces ALL inbound conn IDs
// for an address and the inbound capabilities — i.e. the dedup did not drop
// or reorder data relative to the per-peer scan contract.
func TestRebuildPeerHealthSnapshot_InboundIndexMatchesPerPeerScan(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("indexed-peer-1")
	caps := []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1}

	// Two inbound connections that both declared the same overlay address —
	// inboundConnIDs must carry BOTH ids.
	mk := func(id netcore.ConnID, ip string, port int) net.Conn {
		local, remote := net.Pipe()
		t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
		conn := &fakeConn{Conn: local, remoteAddr: &net.TCPAddr{IP: net.ParseIP(ip), Port: port}}
		core := netcore.New(id, conn, netcore.Inbound, netcore.Options{})
		t.Cleanup(func() { core.Close() })
		core.SetAddress(addr)
		core.SetCapabilities(caps)
		svc.peerMu.Lock()
		svc.setTestConnEntryLocked(conn, &connEntry{core: core, tracked: true})
		svc.peerMu.Unlock()
		return conn
	}
	mk(netcore.ConnID(101), "10.0.0.11", 30011)
	mk(netcore.ConnID(102), "10.0.0.12", 30012)

	svc.peerMu.Lock()
	svc.health[addr] = &peerHealth{Address: addr, Connected: true, Direction: peerDirectionInbound}
	svc.peerMu.Unlock()

	svc.rebuildPeerHealthSnapshot()
	snap := svc.loadPeerHealthSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}

	var rec *peerHealthRecord
	for i := range snap.records {
		if snap.records[i].health.Address == addr {
			rec = &snap.records[i]
			break
		}
	}
	if rec == nil {
		t.Fatalf("address %q missing from snapshot records", addr)
	}

	// Both inbound conn IDs present (order is non-deterministic — check as a set).
	gotIDs := map[uint64]bool{}
	for _, id := range rec.inboundConnIDs {
		gotIDs[id] = true
	}
	if len(rec.inboundConnIDs) != 2 || !gotIDs[101] || !gotIDs[102] {
		t.Fatalf("inboundConnIDs = %v, want {101,102}", rec.inboundConnIDs)
	}

	// Capabilities resolved through the index (no session → inbound fallback).
	wantCaps := domain.CapabilityStrings(caps)
	if len(rec.capabilities) != len(wantCaps) {
		t.Fatalf("capabilities = %v, want %v", rec.capabilities, wantCaps)
	}
	gotCaps := map[string]bool{}
	for _, c := range rec.capabilities {
		gotCaps[c] = true
	}
	for _, c := range wantCaps {
		if !gotCaps[c] {
			t.Fatalf("capabilities = %v, missing %q (want %v)", rec.capabilities, c, wantCaps)
		}
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
