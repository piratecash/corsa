package routing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

// controllableSender records calls and allows controlling success/failure
// via setFailNext. Used alongside a mockery MockPeerSender.
type controllableSender struct {
	mu        sync.Mutex
	calls     []sendCall
	failNextN int
}

func (cs *controllableSender) callCount() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.calls)
}

func (cs *controllableSender) getCalls() []sendCall {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cp := make([]sendCall, len(cs.calls))
	copy(cp, cs.calls)
	return cp
}

func (cs *controllableSender) setFailNext(n int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.failNextN = n
}

// newControllableMockPeerSender creates a MockPeerSender backed by a
// controllableSender that records every call and can simulate failures.
func newControllableMockPeerSender(t *testing.T) (*routingmocks.MockPeerSender, *controllableSender) {
	t.Helper()
	cs := &controllableSender{}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, addr routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			cs.mu.Lock()
			defer cs.mu.Unlock()
			cs.calls = append(cs.calls, sendCall{PeerAddress: addr, Routes: routes})
			if cs.failNextN > 0 {
				cs.failNextN--
				return false
			}
			return true
		},
	).Maybe()
	// Digest-as-heartbeat: periodic deadline emits a digest before the fallback
	// full; not an announce wire frame, so it is unrecorded.
	m.EXPECT().SendRouteSyncDigest(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()
	return m, cs
}

func TestAnnounceLoop_NoopSuppression(t *testing.T) {
	// Validates that an unchanged-snapshot delta cycle produces no
	// wire send (no-op suppression), independent of the periodic
	// forced-full-sync cadence which is allowed to fire on its own
	// schedule for freshness.
	//
	// Design note: this test deliberately uses a LONG AnnounceInterval
	// (10s) so the periodic forced-full deadline (forcedCap=100s)
	// cannot fire during the test budget — the test then exercises
	// the no-op suppression path explicitly via `TriggerUpdate()`.
	// An older version of this test ran with AnnounceInterval=50ms
	// and waited 250ms expecting "exactly 1 send", which silently
	// depended on the pre-Phase-0 fixed 30s rate-limit window
	// blocking subsequent forced syncs. After round 15.6 clamped
	// the rate-limit window to forcedCap (=100ms at 50ms interval),
	// forced full syncs legitimately fire several times in 250ms,
	// breaking the "exactly 1 send" assertion. The new shape is
	// trigger-based and clock-independent: forced-full firing on
	// schedule is a CORRECT behaviour the test must not penalise.
	//
	// Counter assertion (round 15.9): the wire-call count alone is
	// NOT enough — a regression that took follow-up triggers down
	// the !isDeltaCycle early-return path (e.g. by removing the
	// triggerCh handler's `lastDeltaCycleAt = time.Time{}` reset)
	// would also leave the wire count at 1 and silently pass.
	// `NoopSuppressedTotal()` advances ONLY when the per-peer
	// goroutine reached `ComputeDelta` and the result was empty, so
	// asserting it advanced ≥3 after three follow-up triggers
	// pins that the suppression actually came from the no-op
	// branch and not from any other early-return.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	// AnnounceInterval=10s → forcedCap=100s; periodic forced-full
	// deadline never fires during the ≤500ms test budget. Initial
	// sync is forced via TriggerUpdate to avoid waiting 10s for
	// the first natural tick.
	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Step 1 — force the initial sync via TriggerUpdate. The trigger
	// path runs announceToAllPeers synchronously inside Run's select;
	// the peer has no baseline (LastSentSnapshot=nil) so needsFull
	// fires forced full sync. Wait for it to be recorded.
	loop.TriggerUpdate()
	deadline := time.Now().Add(300 * time.Millisecond)
	for rec.callCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rec.callCount() != 1 {
		t.Fatalf("expected 1 send after initial trigger, got %d", rec.callCount())
	}

	// Snapshot baseline before follow-up triggers. The forced-full
	// initial sync above does NOT reach ComputeDelta (it goes through
	// sendFullAnnounce / needsFull branch), so the no-op counter
	// must still be zero at this point. Still, capture as a baseline
	// rather than asserting ==0 — the contract under test is that
	// the counter ADVANCES across the follow-ups, not its precise
	// pre-state.
	noopBefore := loop.NoopSuppressedTotal()

	// Step 2 — the table is unchanged from the initial-sync snapshot.
	// Each subsequent TriggerUpdate must run a delta cycle that
	// computes an empty delta and suppresses the wire send (no-op
	// path, `announce_skipped_noop` + `noopSuppressedTotal`).
	const followups = 3
	for i := 0; i < followups; i++ {
		loop.TriggerUpdate()
		// Wait for the trigger to be consumed and the per-peer
		// goroutine to land at the no-op branch. Polling the counter
		// removes the tight coupling to a fixed sleep that broke
		// historically when the loop hot-path got slower.
		gateDeadline := time.Now().Add(200 * time.Millisecond)
		expected := noopBefore + uint64(i+1)
		for loop.NoopSuppressedTotal() < expected && time.Now().Before(gateDeadline) {
			time.Sleep(2 * time.Millisecond)
		}
	}

	cancel()
	<-done

	// Total send count must still be 1: only the initial sync went
	// out; the three follow-up triggers all hit the no-op
	// suppression branch because the snapshot was unchanged.
	if got := rec.callCount(); got != 1 {
		t.Fatalf("expected 1 send (initial only); follow-up triggers on unchanged snapshot must be suppressed by no-op path; got %d", got)
	}

	// Counter must have advanced at least once per follow-up
	// trigger. Strict equality is avoided because a stray cycle
	// (extremely unlikely at AnnounceInterval=10s during a sub-
	// second test, but possible under very heavy CI load) would
	// flake the test without indicating a real regression. The
	// minimum bound (advanced ≥ followups) is what pins the
	// invariant: each follow-up trigger reached ComputeDelta and
	// produced an empty delta.
	if got := loop.NoopSuppressedTotal() - noopBefore; got < uint64(followups) {
		t.Fatalf("expected NoopSuppressedTotal to advance >= %d after %d follow-up triggers (proves delta path was reached, not an early-return); got delta=%d",
			followups, followups, got)
	}
}

func TestAnnounceLoop_FailedSendPreservesCache(t *testing.T) {
	// When send fails, the cache should not be updated. The next cycle
	// should retry.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	// Fail first send.
	rec.setFailNext(1)

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Wait for a few cycles.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	calls := rec.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 sends (failed + retry), got %d", len(calls))
	}

	// Both calls should send the same routes (first failed, second retry).
	if len(calls[0].Routes) != len(calls[1].Routes) {
		t.Fatalf("retry should send same routes: first=%d, second=%d",
			len(calls[0].Routes), len(calls[1].Routes))
	}
}

func TestAnnounceLoop_DeltaOnlyAfterFullSync(t *testing.T) {
	// After initial full sync, adding a new route should produce a
	// delta-only send with just the new entry.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	// Start with two direct peers so the initial full sync contains
	// at least 2 routes — making the delta (1 new route) strictly smaller.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer(domaintest.ID("peer-E")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	firstCalls := rec.callCount()
	if firstCalls != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", firstCalls)
	}

	// Add another direct peer.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-D")); err != nil {
		t.Fatal(err)
	}

	// Second trigger: should send delta only.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := rec.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(calls))
	}

	// First call: full sync (should include peer-B and peer-E routes).
	firstRoutes := calls[0].Routes
	// Second call: delta (should include only peer-D route).
	secondRoutes := calls[1].Routes

	if len(secondRoutes) >= len(firstRoutes) {
		t.Fatalf("delta should be smaller than full: full=%d, delta=%d",
			len(firstRoutes), len(secondRoutes))
	}

	// Delta should contain the new route.
	foundD := false
	for _, r := range secondRoutes {
		if r.Identity == domaintest.ID("peer-D") {
			foundD = true
		}
	}
	if !foundD {
		t.Fatal("delta should contain peer-D route")
	}

	cancel()
	<-done
}

func TestAnnounceLoop_RateLimitForcedFullSync(t *testing.T) {
	// When a peer is marked NeedsFullResync and forced full sync was
	// attempted recently, it should be skipped until interval passes.
	now := time.Now()
	clock := func() time.Time { return now }

	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(clock))

	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond),
		routing.WithStateRegistry(registry),
	)

	// Manually set up state: simulate a peer that had a successful baseline
	// but now needs forced full resync (e.g. after reconnect).
	state := registry.GetOrCreate(domaintest.ID("peer-C"))
	// Establish a prior baseline so the rate limiter applies.
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, 0, now.Add(-1*time.Minute))
	// Mark as needing full resync (simulating reconnect).
	state.SetNeedsFullResyncForTest()
	// Record a recent full sync attempt to trigger rate limiting.
	state.RecordFullSyncAttempt(now.Add(-10 * time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Trigger and wait.
	loop.TriggerUpdate()
	time.Sleep(30 * time.Millisecond)

	cancel()
	<-done

	// Should have been rate-limited — no sends.
	if rec.callCount() != 0 {
		t.Fatalf("expected 0 sends due to rate limit, got %d", rec.callCount())
	}
}

func TestAnnounceLoop_UnchangedTriggerNoSend(t *testing.T) {
	// After initial full sync, a TriggerUpdate with no table changes
	// should not produce any additional sends. This verifies that
	// delta suppression works correctly on triggered cycles too.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", rec.callCount())
	}

	// Second trigger: no table changes — should be suppressed.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() != 1 {
		t.Fatalf("expected still 1 call (unchanged trigger suppressed), got %d", rec.callCount())
	}

	cancel()
	<-done
}

func TestAnnounceLoop_NewPeerAlwaysGetsFull(t *testing.T) {
	// A brand new peer should always receive a full sync on the first cycle.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer(domaintest.ID("peer-D")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	calls := rec.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	// Full sync should include both peer-B and peer-D routes.
	if len(calls[0].Routes) < 2 {
		t.Fatalf("expected at least 2 routes in full sync, got %d", len(calls[0].Routes))
	}
}

func TestAnnounceLoop_ReconnectedPeerGetsForcedFullSync(t *testing.T) {
	// After a peer disconnects and reconnects (MarkDisconnected +
	// MarkReconnected), the cache is invalidated and the next announce
	// cycle sends a full sync — not a delta from the stale cache.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	registry := routing.NewAnnounceStateRegistry()
	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second),
		routing.WithStateRegistry(registry),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", rec.callCount())
	}

	// Simulate disconnect + reconnect.
	registry.MarkDisconnected(domaintest.ID("peer-C"))
	registry.MarkReconnected(domaintest.ID("peer-C"), nil)

	// Verify state requires full resync.
	state := registry.Get(domaintest.ID("peer-C"))
	view := state.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after reconnect")
	}

	// Second trigger: should send full sync again (not delta).
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := rec.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(calls))
	}

	// Both sends should have the same number of routes (full sync).
	if len(calls[0].Routes) != len(calls[1].Routes) {
		t.Fatalf("reconnect should trigger full sync: first=%d, second=%d",
			len(calls[0].Routes), len(calls[1].Routes))
	}

	cancel()
	<-done
}

func TestAnnounceLoop_FailedWithdrawalRetriedViaDelta(t *testing.T) {
	// When the immediate own-origin withdrawal fails for a peer, the
	// tombstone in the table should appear in the next announce snapshot
	// and be delivered via delta to that peer.
	table := routing.NewTable(
		routing.WithLocalOrigin(domaintest.ID("node-A")),
		routing.WithDefaultTTL(120*time.Second),
	)
	sender, rec := newControllableMockPeerSender(t)

	// Add a direct peer and establish baseline.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync (peer-B route at hops=1).
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() != 1 {
		t.Fatalf("expected 1 call, got %d", rec.callCount())
	}

	// Simulate disconnect: remove the direct peer (creates tombstone).
	// The immediate withdrawal path would normally send to all peers,
	// but we're testing the retry-via-delta mechanism.
	if _, err := table.RemoveDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	// Next announce cycle: the tombstone (hops=16) should appear in
	// the snapshot and delta should include the withdrawal.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := rec.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(calls))
	}

	// Second call should contain the withdrawal (hops=HopsInfinity).
	secondCall := calls[1]
	foundWithdrawal := false
	for _, r := range secondCall.Routes {
		if r.Identity == domaintest.ID("peer-B") && r.Hops == routing.HopsInfinity {
			foundWithdrawal = true
		}
	}
	if !foundWithdrawal {
		t.Fatalf("delta should contain peer-B withdrawal (hops=%d), got %+v",
			routing.HopsInfinity, secondCall.Routes)
	}

	cancel()
	<-done
}

func TestAnnounceLoop_PartialDeltaDoesNotDestroyExistingRoutes(t *testing.T) {
	// Verify that a delta send containing only new routes does not
	// remove existing routes from the receiver's table. This is the
	// fundamental "announce frame is not a destructive snapshot" invariant.
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))

	// Set up receiver table with existing route.
	receiver := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-C")))

	// Add direct peer (peer-B) to sender's table.
	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	// Simulate receiver learning about peer-B from a previous full sync.
	_, err := receiver.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("peer-B"),
		Origin:   domaintest.ID("node-A"),
		NextHop:  domaintest.ID("node-A"),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify route exists in receiver.
	routes := receiver.Lookup(domaintest.ID("peer-B"))
	if len(routes) == 0 {
		t.Fatal("expected peer-B route in receiver before delta")
	}

	// Now sender adds peer-D (new route).
	if _, err := table.AddDirectPeer(domaintest.ID("peer-D")); err != nil {
		t.Fatal(err)
	}

	// Build snapshots for delta computation. Post-Phase-A
	// ComputeDelta keys on (Identity, IsWithdrawn) and the
	// "changed" comparison checks SeqNo, Hops, and Extra — Origin
	// does NOT participate in either, so the Origin value on the
	// manually-built oldSnap is independent of what AnnounceTo
	// emits as far as delta-matching is concerned. We still set
	// Origin to domaintest.ID("node-A") here to match the sender table's
	// localOrigin (created with WithLocalOrigin(domaintest.ID("node-A")), and
	// AnnounceTo emits Origin = domaintest.ID("node-A") — the sender-originated
	// migration contract from route_store_lookup.go), keeping the
	// fixture's shape representative of a real cached snapshot
	// even though equality is unaffected.
	oldSnap := routing.BuildAnnounceSnapshot([]routing.AnnounceEntry{
		{Identity: domaintest.ID("peer-B"), Origin: domaintest.ID("node-A"), SeqNo: 1, Hops: 1},
	})
	newSnap := routing.BuildAnnounceSnapshot(table.AnnounceTo(domaintest.ID("peer-C")))

	delta := routing.ComputeDelta(oldSnap, newSnap)

	// Delta should only contain the new peer-D route.
	foundB := false
	foundD := false
	for _, e := range delta {
		if e.Identity == domaintest.ID("peer-B") {
			foundB = true
		}
		if e.Identity == domaintest.ID("peer-D") {
			foundD = true
		}
	}
	if foundB {
		t.Fatal("delta should not contain unchanged peer-B route")
	}
	if !foundD {
		t.Fatal("delta should contain new peer-D route")
	}

	// Apply delta to receiver (simulating what handleAnnounceRoutes does).
	for _, e := range delta {
		_, updateErr := receiver.UpdateRoute(routing.RouteEntry{
			Identity: e.Identity,
			Origin:   e.Origin,
			NextHop:  domaintest.ID("node-A"),
			Hops:     e.Hops + 1,
			SeqNo:    e.SeqNo,
			Source:   routing.RouteSourceAnnouncement,
		})
		if updateErr != nil {
			t.Fatalf("delta apply failed: %v", updateErr)
		}
	}

	// Verify both routes exist in receiver after delta.
	routesB := receiver.Lookup(domaintest.ID("peer-B"))
	if len(routesB) == 0 {
		t.Fatal("peer-B route should still exist after delta apply")
	}
	routesD := receiver.Lookup(domaintest.ID("peer-D"))
	if len(routesD) == 0 {
		t.Fatal("peer-D route should exist after delta apply")
	}
}
