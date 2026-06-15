package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestLoadRoutingSnapshotEmptyBeforePrime pins the lock-free hot-path
// contract for the routing snapshot: before primeHotReadSnapshots() runs
// (e.g. unit tests that bypass Run()), loadRoutingSnapshot() returns an
// empty-but-valid routing.Snapshot. The RPC handlers and file router
// callback never see a synchronous fallback that would re-couple them
// to routing.Table.t.mu.RLock — exactly the starvation shape the
// snapshot infrastructure was built to eliminate for network_stats /
// peer_health / peers_exchange / cm_slots in phase 1.
func TestLoadRoutingSnapshotEmptyBeforePrime(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	if got := svc.routingSnap.Load(); got != nil {
		t.Fatalf("expected nil snapshot before any rebuild; got %+v", got)
	}

	snap := svc.loadRoutingSnapshot()
	if snap.Routes != nil {
		t.Fatalf("expected nil Routes map on cold-start snapshot; got %+v", snap.Routes)
	}
	if snap.TotalEntries != 0 {
		t.Fatalf("expected zero TotalEntries on cold-start; got %d", snap.TotalEntries)
	}
	if snap.ActiveEntries != 0 {
		t.Fatalf("expected zero ActiveEntries on cold-start; got %d", snap.ActiveEntries)
	}
}

// TestRebuildRoutingSnapshotPrimesEmptyTable verifies the cold-start path:
// the very first rebuildRoutingSnapshot() must publish a snapshot even
// when ConsumeDirty returns false (the freshly constructed table has not
// been mutated). Without this guarantee primeHotReadSnapshots() — which
// runs before the listener opens — would leave the snapshot pointer nil
// and the first RPC after start would hit the empty-fallback branch
// instead of the cached payload.
func TestRebuildRoutingSnapshotPrimesEmptyTable(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Sanity: cold-start state.
	if svc.routingSnap.Load() != nil {
		t.Fatal("test setup: expected nil snapshot before rebuild")
	}
	if svc.routingTable.IsDirty() {
		t.Fatal("test setup: expected freshly constructed table to be clean")
	}

	svc.rebuildRoutingSnapshot()

	if svc.routingSnap.Load() == nil {
		t.Fatal("first rebuild did not publish a snapshot for an empty table")
	}
	// The published snapshot must reflect the current localOrigin self-route
	// projection (TotalEntries==0, but Routes carries the synthetic local
	// entry through Snapshot()).
	loaded := svc.loadRoutingSnapshot()
	if loaded.TotalEntries != 0 {
		t.Fatalf("expected zero TotalEntries on empty table; got %d", loaded.TotalEntries)
	}
}

// TestRebuildRoutingSnapshotSkipsCleanRebuilds verifies the dirty-flag
// economy: after the first publish, a rebuild on a clean table must NOT
// allocate a new snapshot pointer. Without this, the 500 ms refresher
// would deep-copy the entire routing table on every tick regardless of
// writer activity — defeating the whole point of the dirty flag.
func TestRebuildRoutingSnapshotSkipsCleanRebuilds(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildRoutingSnapshot()

	first := svc.routingSnap.Load()
	if first == nil {
		t.Fatal("test setup: first rebuild did not publish")
	}

	// Second rebuild without any table mutation in between — pointer
	// must be unchanged. Clear the coalescing throttle so the rebuild
	// actually reaches the dirty-flag gate (otherwise the min-interval
	// would short-circuit it first and the test would not exercise the gate).
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	second := svc.routingSnap.Load()
	if second != first {
		t.Fatal("clean rebuild allocated a new snapshot; dirty-flag gate is not effective")
	}
}

// TestRebuildRoutingSnapshotSeesRecentMutation verifies that a writer
// flagging the table as dirty causes the next rebuild to publish a
// fresh snapshot pointer. This pins the writer→publisher edge.
func TestRebuildRoutingSnapshotSeesRecentMutation(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildRoutingSnapshot()

	first := svc.routingSnap.Load()
	if first == nil {
		t.Fatal("test setup: first rebuild did not publish")
	}

	// Mutate the table — any accepted UpdateRoute marks dirty.
	_, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if !svc.routingTable.IsDirty() {
		t.Fatal("UpdateRoute did not mark the table dirty; writer→publisher edge broken")
	}

	// Clear the coalescing throttle so this rebuild is not deferred by the
	// min-interval (it fired moments ago for the first publish).
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	second := svc.routingSnap.Load()
	if second == first {
		t.Fatal("rebuild after mutation reused the previous snapshot pointer")
	}
	if svc.loadRoutingSnapshot().TotalEntries != 1 {
		t.Fatalf("expected TotalEntries=1 after UpdateRoute; got %d", svc.loadRoutingSnapshot().TotalEntries)
	}
}

// TestRebuildRoutingSnapshotCoalescesWithinInterval pins the churn cure:
// even when the table is dirty, a rebuild within routingSnapshotMinInterval of
// the previous publish is coalesced (no deep copy), and the dirty bit is
// preserved so the next eligible rebuild still sees the change.
func TestRebuildRoutingSnapshotCoalescesWithinInterval(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildRoutingSnapshot() // cold publish, arms the throttle
	first := svc.routingSnap.Load()
	if first == nil {
		t.Fatal("test setup: first rebuild did not publish")
	}

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	// Within the interval: dirty, but coalesced — same pointer, dirty kept.
	svc.rebuildRoutingSnapshot()
	if svc.routingSnap.Load() != first {
		t.Fatal("dirty rebuild within the min-interval must be coalesced (no new snapshot)")
	}
	if !svc.routingTable.IsDirty() {
		t.Fatal("coalesced rebuild must preserve the dirty bit for the next eligible tick")
	}

	// Past the interval: the preserved dirty bit now produces a fresh publish.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if svc.routingSnap.Load() == first {
		t.Fatal("rebuild past the min-interval must publish the pending change")
	}
}

// TestServiceRoutingSnapshotReturnsCachedNotFresh is the deterministic
// regression test for the cache-routing path. It pins the contract that
// Service.RoutingSnapshot reads the published atomic.Pointer rather
// than re-snapshotting the table on each call: a mutation applied
// AFTER the last publish must NOT be visible through
// Service.RoutingSnapshot until rebuildRoutingSnapshot runs.
//
// The reverse — Service.RoutingSnapshot calling Table.Snapshot directly —
// would be a regression of the Phase A contract (the very coupling that
// freezes fetchRouteTable under writer storms). This test catches that
// regression deterministically with a single mutation and three reads,
// no scheduler luck or wall-clock timing involved. Lock-free behaviour
// of atomic.Pointer.Load itself is verified separately in package
// routing (see TestPublishedSnapshotPointerLoadIsLockFreeUnderWriteLock),
// where direct access to t.mu allows a clean deterministic hold.
func TestServiceRoutingSnapshotReturnsCachedNotFresh(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Initial publish on the empty table.
	svc.rebuildRoutingSnapshot()
	if got := svc.RoutingSnapshot().TotalEntries; got != 0 {
		t.Fatalf("after empty-table prime, RoutingSnapshot.TotalEntries = %d; want 0", got)
	}

	// Mutate the table without rebuilding. Table.Snapshot now returns
	// 1 entry; Service.RoutingSnapshot must still return 0 because the
	// cached pointer was published BEFORE the mutation.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	if fresh := svc.routingTable.Snapshot(); fresh.TotalEntries != 1 {
		t.Fatalf("Table.Snapshot should observe the mutation immediately; got TotalEntries=%d, want 1",
			fresh.TotalEntries)
	}
	if cached := svc.RoutingSnapshot(); cached.TotalEntries != 0 {
		t.Fatalf("Service.RoutingSnapshot returned %d entries while no rebuild happened since publish; "+
			"this means the cached path regressed and now reads Table.Snapshot directly, "+
			"reintroducing the t.mu.RLock coupling under writer storms",
			cached.TotalEntries)
	}

	// Explicit rebuild — now the cached pointer matches the table. Clear the
	// coalescing throttle so the rebuild is not deferred by the min-interval.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if got := svc.RoutingSnapshot().TotalEntries; got != 1 {
		t.Fatalf("after rebuild, RoutingSnapshot.TotalEntries = %d; want 1", got)
	}
}

// TestRebuildRoutingSnapshotRacePostConsume verifies the documented race
// window: a writer that flags dirty after ConsumeDirty has already
// returned true (and the rebuild has started) leaves the next refresh
// observing dirty=true again. The publisher must therefore make at most
// one extra rebuild on the next tick — never miss the writer entirely.
//
// The test cannot deterministically interleave the goroutine schedule
// the way the production race does, so it asserts the equivalent
// observable contract: after ConsumeDirty returns true and the table is
// mutated again before the rebuild publishes, the next rebuild sees
// dirty=true and produces a third pointer with the new state.
func TestRebuildRoutingSnapshotRacePostConsume(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Initial publish.
	svc.rebuildRoutingSnapshot()
	first := svc.routingSnap.Load()

	// Writer #1 — dirty=true.
	_, _ = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	})
	// Refresher consumes and rebuilds — second pointer with 1 entry.
	// Clear the coalescing throttle so each rebuild in this race contract
	// fires immediately rather than being deferred by the min-interval.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	second := svc.routingSnap.Load()
	if second == first || svc.loadRoutingSnapshot().TotalEntries != 1 {
		t.Fatal("rebuild after first writer did not publish updated snapshot")
	}

	// Writer #2 lands AFTER the rebuild — flag is true again.
	_, _ = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("delta"),
		Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	})
	if !svc.routingTable.IsDirty() {
		t.Fatal("writer #2 did not flip dirty back to true")
	}

	// Next refresh tick — must produce a third pointer reflecting both writers.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	third := svc.routingSnap.Load()
	if third == second {
		t.Fatal("subsequent rebuild missed writer #2; dirty re-arming is broken")
	}
	if svc.loadRoutingSnapshot().TotalEntries != 2 {
		t.Fatalf("expected TotalEntries=2 after both writers; got %d",
			svc.loadRoutingSnapshot().TotalEntries)
	}
}

// TestServiceRoutingSnapshotReflectsHoldDownExpiry pins end-to-end that
// the cached routing snapshot — the path consumed by fetchRouteSummary,
// the desktop UI and any operator script — observes hold-down expiry
// once TickTTL has run after the deadline. Hold-down expiry is
// time-derived (the wall clock advances past fs.holdDownUntil without
// any writer event), so the publisher cannot observe it directly; the
// chain is TickTTL clears holdDownUntil + marks dirty → refresh
// republishes. End-to-end visibility for the cached InHoldDown=true →
// false transition is therefore bounded by TickTTL_interval (≈10 s in
// production) + the structural publish bound (routingSnapshotMinInterval
// floor + a refresh tick, ~1–1.5 s) ≈ 11–11.5 s, not by a single refresh tick.
// See docs/routing.md "Snapshot freshness" for the full contract.
//
// This test exercises the second half of that chain: it advances the
// clock, calls TickTTL once, then rebuildRoutingSnapshot, and asserts
// the cached snapshot reflects InHoldDown=false. The TickTTL cadence
// itself is owned by routingTableTTLLoop in routing_announce.go and is
// out of scope here.
//
// The bug this guards: TickTTL used to skip clearing fs.holdDownUntil
// when hold-down expired but withdrawTimes were still inside the
// flapWindow. The peer was kept in flapState (correctly — withdrawals
// are still recent), but the cached FlapEntry.InHoldDown stayed true
// for up to ~90 s on default settings. Operators reading
// fetchRouteSummary saw stale "still in hold-down" with a HoldDownUntil
// timestamp far in the past.
//
// This test substitutes the Service's routingTable with one driven by
// a controllable clock so the time advance is deterministic — there is
// no time.Sleep, the test runs in microseconds.
func TestServiceRoutingSnapshotReflectsHoldDownExpiry(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	// Substitute the production routingTable with a clock-controlled
	// instance. The substitution is safe in this unit test because
	// the Service was constructed but Run() was never called, so no
	// background goroutine holds a reference to the original table.
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	clockNow := now
	clock := func() time.Time { return clockNow }
	localID := domaintest.ID("self")
	svc.routingTable = routing.NewTable(
		routing.WithClock(clock),
		routing.WithLocalOrigin(localID),
		routing.WithFlapWindow(120*time.Second),
		routing.WithFlapThreshold(2),
		routing.WithHoldDownDuration(30*time.Second),
	)

	// Force a flap burst to arm hold-down.
	if _, err := svc.routingTable.AddDirectPeer(domaintest.ID("peerA")); err != nil {
		t.Fatalf("AddDirectPeer #1: %v", err)
	}
	if _, err := svc.routingTable.RemoveDirectPeer(domaintest.ID("peerA")); err != nil {
		t.Fatalf("RemoveDirectPeer #1: %v", err)
	}
	if _, err := svc.routingTable.AddDirectPeer(domaintest.ID("peerA")); err != nil {
		t.Fatalf("AddDirectPeer #2: %v", err)
	}
	if _, err := svc.routingTable.RemoveDirectPeer(domaintest.ID("peerA")); err != nil {
		t.Fatalf("RemoveDirectPeer #2: %v", err)
	}

	// Publish — cached snapshot now reports InHoldDown=true.
	svc.rebuildRoutingSnapshot()
	cached := svc.RoutingSnapshot()
	var primed *routing.FlapEntry
	for i := range cached.FlapState {
		if cached.FlapState[i].PeerIdentity == domaintest.ID("peerA") {
			primed = &cached.FlapState[i]
			break
		}
	}
	if primed == nil {
		t.Fatal("test setup: peerA not present in cached FlapState after burst")
	}
	if !primed.InHoldDown {
		t.Fatalf("test setup: expected InHoldDown=true after burst, got %+v", primed)
	}

	// Advance the table's clock past holdDownUntil but stay inside
	// flapWindow so withdrawTimes are NOT trimmed away.
	clockNow = now.Add(40 * time.Second)

	// TickTTL must mark the table dirty so the next refresh
	// republishes; rebuildRoutingSnapshot then sees a clean
	// InHoldDown=false in the cached snapshot. Clear the coalescing throttle
	// so the rebuild is not deferred by the min-interval.
	svc.routingTable.TickTTL()
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	cached = svc.RoutingSnapshot()
	var refreshed *routing.FlapEntry
	for i := range cached.FlapState {
		if cached.FlapState[i].PeerIdentity == domaintest.ID("peerA") {
			refreshed = &cached.FlapState[i]
			break
		}
	}
	if refreshed == nil {
		t.Fatal("peerA dropped from FlapState too eagerly; withdrawTimes were still inside flapWindow")
	}
	if refreshed.InHoldDown {
		t.Fatalf("cached FlapState still reports InHoldDown=true after hold-down expired and a republish; "+
			"TickTTL did not clear holdDownUntil and/or did not mark the table dirty. Entry: %+v", refreshed)
	}
	if !refreshed.HoldDownUntil.IsZero() {
		t.Fatalf("cached HoldDownUntil should be zero after expiry, got %s", refreshed.HoldDownUntil)
	}
	if refreshed.RecentWithdrawals == 0 {
		t.Fatal("RecentWithdrawals dropped to zero too eagerly; withdrawTimes were still inside the window")
	}
}

// TestRebuildRoutingSnapshotPeriodicFullSelfHeal verifies the wall-clock
// self-heal cadence for the copy-on-write incremental projection AND that
// it never wakes a clean table. The self-heal upgrades a rebuild that is
// happening anyway (the table was dirty) to a full re-copy once
// routingSnapshotFullInterval has elapsed since the last full one; a clean
// idle table is always skipped (no wake), so the cure does not regress the
// headless idle invariant honoured by the sibling hot-read snapshots.
func TestRebuildRoutingSnapshotPeriodicFullSelfHeal(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	svc.rebuildRoutingSnapshot() // cold publish — forced full (lastFull was 0)
	first := svc.routingSnap.Load()
	if first == nil {
		t.Fatal("test setup: first rebuild did not publish")
	}
	if svc.lastRoutingFullSnapAtNanos.Load() == 0 {
		t.Fatal("cold-start rebuild did not stamp the full-snapshot timestamp")
	}

	mutate := func(nextHop routing.PeerIdentity) {
		t.Helper()
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: nextHop,
			Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("UpdateRoute: %v", err)
		}
	}

	// Key invariant (P2 fix): a CLEAN table is skipped even when the full
	// interval has elapsed. Age the full timestamp, clear the throttle, but
	// make no mutation — the publisher must NOT wake.
	svc.lastRoutingFullSnapAtNanos.Store(time.Now().Add(-routingSnapshotFullInterval - time.Second).UnixNano())
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if svc.routingSnap.Load() != first {
		t.Fatal("clean table was woken by the self-heal interval; headless idle invariant regressed")
	}

	// A dirty rebuild while the interval has NOT elapsed: republishes, but
	// stays incremental (lastFull unchanged).
	svc.lastRoutingFullSnapAtNanos.Store(time.Now().UnixNano())
	freshFull := svc.lastRoutingFullSnapAtNanos.Load()
	mutate(domaintest.ID("charlie"))
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	second := svc.routingSnap.Load()
	if second == first {
		t.Fatal("dirty rebuild did not republish")
	}
	if svc.lastRoutingFullSnapAtNanos.Load() != freshFull {
		t.Fatal("incremental rebuild within the interval restamped the full timestamp")
	}

	// A dirty rebuild AFTER the interval elapsed: upgraded to a full
	// re-copy, which restamps lastRoutingFullSnapAtNanos.
	aged := time.Now().Add(-routingSnapshotFullInterval - time.Second).UnixNano()
	svc.lastRoutingFullSnapAtNanos.Store(aged)
	mutate(domaintest.ID("delta"))
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if svc.routingSnap.Load() == second {
		t.Fatal("dirty rebuild after the interval did not republish")
	}
	if svc.lastRoutingFullSnapAtNanos.Load() <= aged {
		t.Fatal("self-heal did not upgrade the dirty rebuild to a full re-copy (timestamp not restamped)")
	}
}

// TestPrimeHotReadSnapshotsCoversRouting asserts that the routing snapshot
// is part of the prime sequence — without this, RPC handlers running
// immediately after Run() opens the listener could observe a nil
// snapshot and fall through to a synchronous Table.Snapshot() rebuild,
// re-coupling the hot path to t.mu.RLock. The full prime chain is
// invariant-critical: any future refactor that drops one of the five
// rebuilds must trip this test.
func TestPrimeHotReadSnapshotsCoversRouting(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)

	if svc.routingSnap.Load() != nil {
		t.Fatal("test setup: expected nil routing snapshot before prime")
	}

	svc.primeHotReadSnapshots()

	if svc.routingSnap.Load() == nil {
		t.Fatal("primeHotReadSnapshots did not publish the routing snapshot")
	}
}
