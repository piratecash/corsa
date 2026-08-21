package node

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
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

func TestReachableIDsSnapshotNormalizesDirectOnlyClassification(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	peer := domaintest.ID("reachable-direct-only")
	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	svc.rebuildRoutingSnapshot()

	if reachable := svc.ReachableIDsSnapshot(); !reachable[peer] {
		t.Fatalf("direct-only identity missing from public reachability set: %v", reachable)
	}
}

// TestRoutingSnapshotEventOrdersReachability is the §1.2 regression test of
// docs/refactoring/identity-discovery-lookup.md: after a table mutation, the
// snapshot-reason TopicRouteTableChanged event must arrive strictly AFTER the
// fresh snapshot is readable — a subscriber that reconciles reachability on
// this event observes the mutated route, never the previous generation.
func TestRoutingSnapshotEventOrdersReachability(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus

	alice := domaintest.ID("alice")
	observed := make(chan bool, 8)
	bus.Subscribe(ebus.TopicRouteTableChanged, func(change ebus.RouteTableChange) {
		if change.Reason != domain.RouteChangeSnapshot {
			return
		}
		// Read through the same accessor a reachability consumer uses: the
		// event's contract is that this read is already fresh.
		best := svc.RoutingSnapshot().BestRoute(alice)
		observed <- best != nil && best.Source != routing.RouteSourceLocal
	})

	// Prime: publishes the first snapshot event over an empty table.
	svc.rebuildRoutingSnapshot()
	select {
	case reachable := <-observed:
		if reachable {
			t.Fatal("empty table reported alice reachable")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prime rebuild published no snapshot event")
	}

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: alice, Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	select {
	case reachable := <-observed:
		if !reachable {
			t.Fatal("the snapshot event fired but the snapshot did not carry the mutation: the ordering guarantee is broken")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("post-mutation rebuild published no snapshot event")
	}
}

func TestRoutingSnapshotPersistsLastOnlineWhenFinalRouteDisappears(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus
	t.Cleanup(bus.Shutdown)
	var presenceChanges []ebus.IdentityPresenceChange
	bus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		presenceChanges = append(presenceChanges, change)
	}, ebus.WithSync())

	peer := domaintest.ID("last-online-routed-peer")
	firstOrigin := domaintest.ID("last-online-origin-a")
	firstHop := domaintest.ID("last-online-hop-a")
	secondOrigin := domaintest.ID("last-online-origin-b")
	secondHop := domaintest.ID("last-online-hop-b")
	witness := domaintest.ID("last-online-network-witness")
	witnessOrigin := domaintest.ID("last-online-witness-origin")
	witnessHop := domaintest.ID("last-online-witness-hop")
	if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}

	// Prime the previous-generation side of the transition detector, then
	// publish two independent live paths for the contact.
	svc.rebuildRoutingSnapshot()
	for _, route := range []routing.RouteEntry{
		{Identity: peer, Origin: firstOrigin, NextHop: firstHop, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
		{Identity: peer, Origin: secondOrigin, NextHop: secondHop, Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
		{Identity: witness, Origin: witnessOrigin, NextHop: witnessHop, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
	} {
		if _, err := svc.routingTable.UpdateRoute(route); err != nil {
			t.Fatalf("UpdateRoute(%s): %v", route.NextHop, err)
		}
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	// Losing only one path must keep the contact online and leave the durable
	// timestamp empty.
	if !svc.routingTable.WithdrawRoute(peer, firstOrigin, firstHop, 2) {
		t.Fatal("withdraw first route returned false")
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt; !got.IsZero() {
		t.Fatalf("last_online_at after losing one of two paths = %v, want zero", got)
	}

	want := time.Date(2026, time.August, 21, 18, 5, 0, 0, time.UTC)
	svc.presenceClock = func() time.Time { return want }
	if !svc.routingTable.WithdrawRoute(peer, secondOrigin, secondHop, 2) {
		t.Fatal("withdraw final route returned false")
	}
	// A slow trust-store write must not hold up the routing refresher. Lock the
	// disk-write serializer and require the snapshot publish itself to finish;
	// the tracked background writer may wait until we release it below.
	svc.trust.saveMu.Lock()
	svc.lastRoutingSnapAtNanos.Store(0)
	rebuilt := make(chan struct{})
	go func() {
		svc.rebuildRoutingSnapshot()
		close(rebuilt)
	}()
	select {
	case <-rebuilt:
		svc.trust.saveMu.Unlock()
	case <-time.After(5 * time.Second):
		svc.trust.saveMu.Unlock()
		t.Fatal("routing snapshot publisher blocked on trust-store disk I/O")
	}
	svc.WaitBackground()

	got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt
	if !got.Equal(want) {
		t.Fatalf("last_online_at = %v, want injected observation time %v", got, want)
	}
	var offlineEventFound bool
	for _, change := range presenceChanges {
		if !change.ChangedAt.Equal(got) {
			continue
		}
		if change.Source != domain.PeerIdentityFromWire(svc.identity.Address) {
			t.Fatalf("presence source = %s, want local identity %s", change.Source, svc.identity.Address)
		}
		for _, identity := range change.Identities {
			if identity == peer {
				offlineEventFound = true
			}
		}
	}
	if !offlineEventFound {
		t.Fatalf("no offline identity presence event carried persisted timestamp %v", got)
	}
}

func TestRoutingSnapshotDoesNotAttributeTotalCollapseToContacts(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus
	t.Cleanup(bus.Shutdown)
	var offlineChanges []ebus.IdentityPresenceChange
	bus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		offlineChanges = append(offlineChanges, change)
	}, ebus.WithSync())
	first := domaintest.ID("collapse-contact-a")
	second := domaintest.ID("collapse-contact-b")
	firstOrigin := domaintest.ID("collapse-origin-a")
	secondOrigin := domaintest.ID("collapse-origin-b")
	firstHop := domaintest.ID("collapse-hop-a")
	secondHop := domaintest.ID("collapse-hop-b")
	for _, peer := range []domain.PeerIdentity{first, second} {
		if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-" + peer.String()}); err != nil || !stored {
			t.Fatalf("remember %s: stored=%v err=%v", peer, stored, err)
		}
	}

	svc.rebuildRoutingSnapshot()
	for _, route := range []routing.RouteEntry{
		{Identity: first, Origin: firstOrigin, NextHop: firstHop, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
		{Identity: second, Origin: secondOrigin, NextHop: secondHop, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement},
	} {
		if _, err := svc.routingTable.UpdateRoute(route); err != nil {
			t.Fatalf("UpdateRoute(%s): %v", route.Identity, err)
		}
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	offlineChanges = nil

	if !svc.routingTable.WithdrawRoute(first, firstOrigin, firstHop, 2) ||
		!svc.routingTable.WithdrawRoute(second, secondOrigin, secondHop, 2) {
		t.Fatal("failed to withdraw both routes for total-collapse setup")
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	contacts := svc.trust.trustedContacts()
	if len(offlineChanges) != 0 {
		t.Fatalf("total local collapse emitted offline presence changes: %+v", offlineChanges)
	}
	for _, peer := range []domain.PeerIdentity{first, second} {
		if got := contacts[peer.String()].LastOnlineAt; !got.IsZero() {
			t.Fatalf("total local collapse assigned %s last_online_at=%v", peer, got)
		}
	}
}

func TestRemoteEOFDirectDisconnectPersistsLastOnlineOnTotalCollapse(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus
	t.Cleanup(bus.Shutdown)

	peer := domaintest.ID("single-direct-contact")
	if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}
	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	svc.rebuildRoutingSnapshot()
	svc.routeWithdrawalGracePeriodTest = -1
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1

	var observed []ebus.IdentityPresenceChange
	bus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		observed = append(observed, change)
	}, ebus.WithSync())

	svc.onPeerSessionClosedWithError(peer, []domain.Capability{domain.CapMeshRelayV1}, io.EOF)
	// The later snapshot owns ReachableIDs but must not publish the same direct
	// offline transition a second time.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	svc.WaitBackground()

	got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt
	if got.IsZero() {
		t.Fatal("peer-initiated final direct disconnect left last_online_at at zero")
	}
	if len(observed) != 1 {
		t.Fatalf("direct disconnect emitted %d presence events, want exactly one: %+v", len(observed), observed)
	}
	if observed[0].Source != domain.PeerIdentityFromWire(svc.identity.Address) {
		t.Fatalf("presence source = %s, want local identity %s", observed[0].Source, svc.identity.Address)
	}
	if len(observed[0].Identities) != 1 || observed[0].Identities[0] != peer {
		t.Fatalf("presence identities = %v, want [%s]", observed[0].Identities, peer)
	}
}

func TestRemoteEOFDirectDisconnectPersistsSessionCloseTimeAcrossGrace(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	peer := domaintest.ID("grace-timestamp-direct-contact")
	if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}
	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	svc.rebuildRoutingSnapshot()
	const grace = 300 * time.Millisecond
	svc.routeWithdrawalGracePeriodTest = grace
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1
	want := time.Date(2026, time.August, 21, 17, 31, 47, 155000000, time.UTC)
	svc.presenceClock = func() time.Time { return want }

	svc.onPeerSessionClosedWithError(peer, []domain.Capability{domain.CapMeshRelayV1}, io.EOF)

	deadline := time.Now().Add(3 * time.Second)
	var got time.Time
	for time.Now().Before(deadline) {
		got = svc.trust.trustedContacts()[peer.String()].LastOnlineAt
		if !got.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got.IsZero() {
		t.Fatal("grace withdrawal did not persist last_online_at")
	}
	if !got.Equal(want) {
		t.Fatalf("last_online_at=%v, want session-close time %v; grace must not be included", got, want)
	}
}

func TestAmbiguousDirectTransportFailureDoesNotPersistPeerPresence(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	peer := domaintest.ID("ambiguous-direct-contact")
	witness := domaintest.ID("ambiguous-direct-witness")
	if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}
	for _, identity := range []domain.PeerIdentity{peer, witness} {
		if _, err := svc.routingTable.AddDirectPeer(identity); err != nil {
			t.Fatalf("AddDirectPeer(%s): %v", identity, err)
		}
	}
	svc.rebuildRoutingSnapshot()
	svc.routeWithdrawalGracePeriodTest = -1
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1

	// A timeout can be caused by our own interface or route. It still feeds
	// disconnect-storm classification, but is not identity-scoped offline
	// evidence and therefore must not become durable last_online_at.
	svc.onPeerSessionClosedWithError(
		peer,
		[]domain.Capability{domain.CapMeshRelayV1},
		errors.New("read tcp: i/o timeout"),
	)
	// Keep another live route as a connectivity witness and publish the route
	// snapshot after the direct removal. The snapshot path must not reinterpret
	// an ambiguous direct close as transit-presence evidence.
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	svc.WaitBackground()

	if got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt; !got.IsZero() {
		t.Fatalf("ambiguous transport failure assigned peer last_online_at=%v", got)
	}
}

func TestAmbiguousDirectThenTransitLossIsSnapshotGenerationIndependent(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		rebuildBetweenRouteLosses bool
	}{
		{name: "same_generation"},
		{name: "separate_generations", rebuildBetweenRouteLosses: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc := newTestService(t, config.NodeTypeFull)
			peer := domaintest.ID("mixed-peer")
			backup := domaintest.ID("mixed-backup")
			witness := domaintest.ID("mixed-witness")
			if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
				t.Fatalf("remember peer: stored=%v err=%v", stored, err)
			}
			for _, identity := range []domain.PeerIdentity{peer, witness} {
				if _, err := svc.routingTable.AddDirectPeer(identity); err != nil {
					t.Fatalf("AddDirectPeer(%s): %v", identity, err)
				}
			}
			if status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
				Identity: peer,
				Origin:   backup,
				NextHop:  backup,
				Hops:     2,
				SeqNo:    2,
				Source:   routing.RouteSourceAnnouncement,
			}); err != nil || status != routing.RouteAccepted {
				t.Fatalf("add transit fallback: status=%v err=%v", status, err)
			}
			svc.rebuildRoutingSnapshot()
			svc.routeWithdrawalGracePeriodTest = -1
			svc.identitySessions[peer] = 1
			svc.identityRelaySessions[peer] = 1
			want := time.Date(2026, time.August, 21, 19, 0, 0, 0, time.UTC)
			svc.presenceClock = func() time.Time { return want }

			// The direct timeout is ambiguous and therefore not owned by lifecycle.
			// The transit fallback then disappears while a separate witness remains
			// reachable. Snapshot must own the combined transition regardless of
			// whether a rebuild lands between the two route losses.
			svc.onPeerSessionClosedWithError(
				peer,
				[]domain.Capability{domain.CapMeshRelayV1},
				errors.New("read tcp: i/o timeout"),
			)
			if tc.rebuildBetweenRouteLosses {
				svc.lastRoutingSnapAtNanos.Store(0)
				svc.rebuildRoutingSnapshot()
			}
			if !svc.routingTable.WithdrawRoute(peer, backup, backup, 3) {
				t.Fatal("withdraw transit fallback returned false")
			}
			svc.lastRoutingSnapAtNanos.Store(0)
			svc.rebuildRoutingSnapshot()
			svc.WaitBackground()

			if got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt; !got.Equal(want) {
				t.Fatalf("last_online_at=%v, want snapshot observation %v", got, want)
			}
		})
	}
}

func TestLocalDirectTeardownDoesNotPersistPeerPresence(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	peer := domaintest.ID("locally-evicted-direct-contact")
	if stored, err := svc.trust.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}
	if _, err := svc.routingTable.AddDirectPeer(peer); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	svc.rebuildRoutingSnapshot()
	svc.routeWithdrawalGracePeriodTest = -1
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1

	svc.onPeerSessionClosedWithCause(peer, []domain.Capability{domain.CapMeshRelayV1}, sessionCloseLocalEviction)
	svc.WaitBackground()

	if got := svc.trust.trustedContacts()[peer.String()].LastOnlineAt; !got.IsZero() {
		t.Fatalf("local teardown assigned peer last_online_at=%v", got)
	}
}

func TestAmbiguousDirectFailureAfterReconnectIsNotAttributedBySnapshot(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus
	t.Cleanup(bus.Shutdown)
	peer := domaintest.ID("direct-marker-reconnected-peer")
	witness := domaintest.ID("direct-marker-witness")
	for _, identity := range []domain.PeerIdentity{peer, witness} {
		if _, err := svc.routingTable.AddDirectPeer(identity); err != nil {
			t.Fatalf("AddDirectPeer(%s): %v", identity, err)
		}
	}
	svc.rebuildRoutingSnapshot()
	svc.routeWithdrawalGracePeriodTest = -1
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1

	var observed []ebus.IdentityPresenceChange
	bus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		observed = append(observed, change)
	}, ebus.WithSync())

	svc.onPeerSessionClosedWithError(peer, []domain.Capability{domain.CapMeshRelayV1}, io.EOF)
	svc.onPeerSessionEstablished(peer, []domain.Capability{domain.CapMeshRelayV1})
	svc.onPeerSessionClosedWithError(peer, []domain.Capability{domain.CapMeshRelayV1}, errors.New("read tcp: i/o timeout"))
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	if len(observed) != 1 {
		t.Fatalf("presence events = %d, want only the clean-EOF transition: %+v", len(observed), observed)
	}
}

func TestTransitLossAfterDirectFallbackUsesSnapshotPresence(t *testing.T) {
	t.Parallel()

	svc := newTestService(t, config.NodeTypeFull)
	bus := ebus.New()
	svc.eventBus = bus
	t.Cleanup(bus.Shutdown)
	peer := domaintest.ID("fallback-peer")
	backup := domaintest.ID("fallback-backup")
	witness := domaintest.ID("fallback-witness")
	for _, identity := range []domain.PeerIdentity{peer, witness} {
		if _, err := svc.routingTable.AddDirectPeer(identity); err != nil {
			t.Fatalf("AddDirectPeer(%s): %v", identity, err)
		}
	}
	if status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: peer,
		Origin:   backup,
		NextHop:  backup,
		Hops:     2,
		SeqNo:    2,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil || status != routing.RouteAccepted {
		t.Fatalf("add backup route: status=%v err=%v", status, err)
	}
	svc.rebuildRoutingSnapshot()
	svc.routeWithdrawalGracePeriodTest = -1
	svc.identitySessions[peer] = 1
	svc.identityRelaySessions[peer] = 1

	var observed []ebus.IdentityPresenceChange
	bus.Subscribe(ebus.TopicIdentityPresenceChanged, func(change ebus.IdentityPresenceChange) {
		observed = append(observed, change)
	}, ebus.WithSync())

	// The direct route disappears, but the transit fallback keeps the identity
	// reachable, so neither lifecycle nor snapshot should emit offline yet.
	svc.onPeerSessionClosedWithError(peer, []domain.Capability{domain.CapMeshRelayV1}, io.EOF)
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()
	if len(observed) != 0 {
		t.Fatalf("direct loss with a live fallback emitted presence: %+v", observed)
	}

	want := time.Date(2026, time.August, 21, 18, 30, 0, 0, time.UTC)
	svc.presenceClock = func() time.Time { return want }
	if !svc.routingTable.WithdrawRoute(peer, backup, backup, 3) {
		t.Fatal("withdraw backup route returned false")
	}
	svc.lastRoutingSnapAtNanos.Store(0)
	svc.rebuildRoutingSnapshot()

	if len(observed) != 1 {
		t.Fatalf("transit fallback loss emitted %d events, want one: %+v", len(observed), observed)
	}
	if !observed[0].ChangedAt.Equal(want) {
		t.Fatalf("transit ChangedAt=%v, want %v", observed[0].ChangedAt, want)
	}
}

func TestPresenceSnapshotWithoutLocalIdentityDoesNotPanic(t *testing.T) {
	t.Parallel()

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("nil-identity-local-origin")))
	peer := domaintest.ID("nil-identity-peer")
	witness := domaintest.ID("nil-identity-witness")
	for _, identity := range []domain.PeerIdentity{peer, witness} {
		if _, err := table.AddDirectPeer(identity); err != nil {
			t.Fatalf("AddDirectPeer(%s): %v", identity, err)
		}
	}
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc := &Service{routingTable: table, eventBus: bus}
	svc.rebuildRoutingSnapshot()
	if _, err := table.RemoveDirectPeer(peer); err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}
	svc.lastRoutingSnapAtNanos.Store(0)

	// A minimal/headless fixture has no local identity and therefore cannot
	// source an identity.presence.changed event, but rebuilding its route
	// snapshot must remain safe.
	svc.rebuildRoutingSnapshot()
}
