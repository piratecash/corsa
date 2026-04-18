package routing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
)

type senderRecorder struct {
	mu    sync.Mutex
	calls []sendCall
}

type sendCall struct {
	PeerAddress routing.PeerAddress
	Routes      []routing.AnnounceEntry
}

func (r *senderRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *senderRecorder) lastCall() sendCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return sendCall{}
	}
	return r.calls[len(r.calls)-1]
}

func newMockPeerSender(t *testing.T) (*routingmocks.MockPeerSender, *senderRecorder) {
	t.Helper()
	rec := &senderRecorder{}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything).RunAndReturn(
		func(addr routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			rec.mu.Lock()
			rec.calls = append(rec.calls, sendCall{PeerAddress: addr, Routes: routes})
			rec.mu.Unlock()
			return true
		},
	).Maybe()
	return m, rec
}

func TestAnnounceLoopPeriodicSend(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Wait for at least one periodic cycle.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if rec.callCount() == 0 {
		t.Fatal("expected at least one periodic announcement, got 0")
	}

	call := rec.lastCall()
	if call.PeerAddress != "addr-C" {
		t.Fatalf("expected peer address addr-C, got %s", call.PeerAddress)
	}
	if len(call.Routes) == 0 {
		t.Fatal("expected at least one route in the announcement")
	}
}

func TestAnnounceLoopTriggeredUpdate(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	// Use a very long interval so only triggered updates fire.
	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Trigger an immediate update.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if rec.callCount() == 0 {
		t.Fatal("expected triggered announcement, got 0")
	}

	cancel()
	<-done
}

func TestAnnounceLoopSplitHorizon(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Announce to peer-B: the direct route for peer-B was learned via
	// peer-B (NextHop == peer-B), so it should be excluded by split horizon.
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-B", Identity: "peer-B"},
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

	// Split horizon should exclude routes learned from peer-B when
	// announcing to peer-B. The direct route has NextHop == "peer-B",
	// which equals the announce target identity "peer-B", so no routes
	// should be sent.
	if rec.callCount() != 0 {
		t.Fatalf("expected 0 calls due to split horizon, got %d", rec.callCount())
	}
}

func TestAnnounceLoopNoPeersNoSend(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// No peers to announce to.
	peers := func() []routing.AnnounceTarget {
		return nil
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if rec.callCount() != 0 {
		t.Fatalf("expected 0 sends with no peers, got %d", rec.callCount())
	}
}

func TestAnnounceLoopTriggerCoalesces(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
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

	// Fire multiple triggers rapidly — they should coalesce.
	for i := 0; i < 10; i++ {
		loop.TriggerUpdate()
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Due to coalescing, we expect far fewer than 10 sends.
	// At least 1 (the first trigger), at most a few.
	count := rec.callCount()
	if count == 0 {
		t.Fatal("expected at least 1 triggered send")
	}
	if count > 5 {
		t.Fatalf("expected coalescing to limit sends, got %d", count)
	}
}

func TestAnnounceLoopRunOnlyOnce(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))
	sender, _ := newMockPeerSender(t)
	peers := func() []routing.AnnounceTarget { return nil }

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done1 := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done1)
	}()
	time.Sleep(10 * time.Millisecond)

	// Second Run should return immediately (already running).
	done2 := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done2)
	}()

	select {
	case <-done2:
		// OK, returned immediately.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second Run call should return immediately when loop is already running")
	}

	cancel()
	<-done1
}
