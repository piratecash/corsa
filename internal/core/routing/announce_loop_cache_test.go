package routing

import (
	"context"
	"sync"
	"testing"
	"time"
)

// controllablePeerSender records calls and allows controlling success/failure.
type controllablePeerSender struct {
	mu         sync.Mutex
	calls      []mockSendCall
	failNextN  int
}

func (m *controllablePeerSender) SendAnnounceRoutes(peerAddress PeerAddress, routes []AnnounceEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, mockSendCall{PeerAddress: peerAddress, Routes: routes})
	if m.failNextN > 0 {
		m.failNextN--
		return false
	}
	return true
}

func (m *controllablePeerSender) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *controllablePeerSender) getCalls() []mockSendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockSendCall, len(m.calls))
	copy(cp, m.calls)
	return cp
}

func (m *controllablePeerSender) setFailNext(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNextN = n
}

func TestAnnounceLoop_NoopSuppression(t *testing.T) {
	// After the first full sync, subsequent cycles with no table changes
	// should not produce any sends.
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Wait for several periodic cycles.
	time.Sleep(250 * time.Millisecond)
	cancel()
	<-done

	// First cycle should send (full sync). Subsequent cycles should be
	// no-ops because the table hasn't changed. Expect exactly 1 send.
	calls := sender.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 send (initial full sync only), got %d", len(calls))
	}
}

func TestAnnounceLoop_FailedSendPreservesCache(t *testing.T) {
	// When send fails, the cache should not be updated. The next cycle
	// should retry.
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	// Fail first send.
	sender.setFailNext(1)

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(50*time.Millisecond))

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

	calls := sender.getCalls()
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
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	// Start with two direct peers so the initial full sync contains
	// at least 2 routes — making the delta (1 new route) strictly smaller.
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer("peer-E"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	firstCalls := sender.callCount()
	if firstCalls != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", firstCalls)
	}

	// Add another direct peer.
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatal(err)
	}

	// Second trigger: should send delta only.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := sender.getCalls()
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
		if r.Identity == "peer-D" {
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

	registry := NewAnnounceStateRegistry(WithRegistryClock(clock))

	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(50*time.Millisecond),
		WithStateRegistry(registry),
	)

	// Manually set up state: simulate a peer that had a successful baseline
	// but now needs forced full resync (e.g. after reconnect).
	state := registry.GetOrCreate("peer-C")
	// Establish a prior baseline so the rate limiter applies.
	state.RecordFullSyncSuccess(&AnnounceSnapshot{}, now.Add(-1*time.Minute))
	// Mark as needing full resync (simulating reconnect).
	state.mu.Lock()
	state.needsFullResync = true
	state.mu.Unlock()
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
	if sender.callCount() != 0 {
		t.Fatalf("expected 0 sends due to rate limit, got %d", sender.callCount())
	}
}

func TestAnnounceLoop_UnchangedTriggerNoSend(t *testing.T) {
	// After initial full sync, a TriggerUpdate with no table changes
	// should not produce any additional sends. This verifies that
	// delta suppression works correctly on triggered cycles too.
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if sender.callCount() != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", sender.callCount())
	}

	// Second trigger: no table changes — should be suppressed.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if sender.callCount() != 1 {
		t.Fatalf("expected still 1 call (unchanged trigger suppressed), got %d", sender.callCount())
	}

	cancel()
	<-done
}

func TestAnnounceLoop_NewPeerAlwaysGetsFull(t *testing.T) {
	// A brand new peer should always receive a full sync on the first cycle.
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

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

	calls := sender.getCalls()
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
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &controllablePeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	registry := NewAnnounceStateRegistry()
	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second),
		WithStateRegistry(registry),
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

	if sender.callCount() != 1 {
		t.Fatalf("expected 1 call after first trigger, got %d", sender.callCount())
	}

	// Simulate disconnect + reconnect.
	registry.MarkDisconnected("peer-C")
	registry.MarkReconnected("peer-C")

	// Verify state requires full resync.
	state := registry.Get("peer-C")
	view := state.View()
	if !view.NeedsFullResync {
		t.Fatal("expected NeedsFullResync=true after reconnect")
	}

	// Second trigger: should send full sync again (not delta).
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := sender.getCalls()
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
	table := NewTable(
		WithLocalOrigin("node-A"),
		WithDefaultTTL(120*time.Second),
	)
	sender := &controllablePeerSender{}

	// Add a direct peer and establish baseline.
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// First trigger: full sync (peer-B route at hops=1).
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if sender.callCount() != 1 {
		t.Fatalf("expected 1 call, got %d", sender.callCount())
	}

	// Simulate disconnect: remove the direct peer (creates tombstone).
	// The immediate withdrawal path would normally send to all peers,
	// but we're testing the retry-via-delta mechanism.
	if _, err := table.RemoveDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Next announce cycle: the tombstone (hops=16) should appear in
	// the snapshot and delta should include the withdrawal.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	calls := sender.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 calls, got %d", len(calls))
	}

	// Second call should contain the withdrawal (hops=HopsInfinity).
	secondCall := calls[1]
	foundWithdrawal := false
	for _, r := range secondCall.Routes {
		if r.Identity == "peer-B" && r.Hops == HopsInfinity {
			foundWithdrawal = true
		}
	}
	if !foundWithdrawal {
		t.Fatalf("delta should contain peer-B withdrawal (hops=%d), got %+v",
			HopsInfinity, secondCall.Routes)
	}

	cancel()
	<-done
}

func TestAnnounceLoop_PartialDeltaDoesNotDestroyExistingRoutes(t *testing.T) {
	// Verify that a delta send containing only new routes does not
	// remove existing routes from the receiver's table. This is the
	// fundamental "announce frame is not a destructive snapshot" invariant.
	table := NewTable(WithLocalOrigin("node-A"))

	// Set up receiver table with existing route.
	receiver := NewTable(WithLocalOrigin("node-C"))

	// Add direct peer (peer-B) to sender's table.
	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Simulate receiver learning about peer-B from a previous full sync.
	_, err := receiver.UpdateRoute(RouteEntry{
		Identity: "peer-B",
		Origin:   "node-A",
		NextHop:  "node-A",
		Hops:     2,
		SeqNo:    1,
		Source:   RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify route exists in receiver.
	routes := receiver.Lookup("peer-B")
	if len(routes) == 0 {
		t.Fatal("expected peer-B route in receiver before delta")
	}

	// Now sender adds peer-D (new route).
	if _, err := table.AddDirectPeer("peer-D"); err != nil {
		t.Fatal(err)
	}

	// Build snapshots for delta computation.
	oldSnap := BuildAnnounceSnapshot([]AnnounceEntry{
		{Identity: "peer-B", Origin: "node-A", SeqNo: 1, Hops: 1},
	})
	newSnap := BuildAnnounceSnapshot(table.AnnounceTo("peer-C"))

	delta := ComputeDelta(oldSnap, newSnap)

	// Delta should only contain the new peer-D route.
	foundB := false
	foundD := false
	for _, e := range delta {
		if e.Identity == "peer-B" {
			foundB = true
		}
		if e.Identity == "peer-D" {
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
		_, updateErr := receiver.UpdateRoute(RouteEntry{
			Identity: e.Identity,
			Origin:   e.Origin,
			NextHop:  "node-A",
			Hops:     e.Hops + 1,
			SeqNo:    e.SeqNo,
			Source:   RouteSourceAnnouncement,
		})
		if updateErr != nil {
			t.Fatalf("delta apply failed: %v", updateErr)
		}
	}

	// Verify both routes exist in receiver after delta.
	routesB := receiver.Lookup("peer-B")
	if len(routesB) == 0 {
		t.Fatal("peer-B route should still exist after delta apply")
	}
	routesD := receiver.Lookup("peer-D")
	if len(routesD) == 0 {
		t.Fatal("peer-D route should exist after delta apply")
	}
}
