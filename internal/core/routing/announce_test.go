package routing

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockPeerSender records calls to SendAnnounceRoutes.
type mockPeerSender struct {
	mu    sync.Mutex
	calls []mockSendCall
}

type mockSendCall struct {
	PeerAddress PeerAddress
	Routes      []AnnounceEntry
}

func (m *mockPeerSender) SendAnnounceRoutes(peerAddress PeerAddress, routes []AnnounceEntry) bool {
	m.mu.Lock()
	m.calls = append(m.calls, mockSendCall{PeerAddress: peerAddress, Routes: routes})
	m.mu.Unlock()
	return true
}

func (m *mockPeerSender) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockPeerSender) lastCall() mockSendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return mockSendCall{}
	}
	return m.calls[len(m.calls)-1]
}

func TestAnnounceLoopPeriodicSend(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}

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

	// Wait for at least one periodic cycle.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if sender.callCount() == 0 {
		t.Fatal("expected at least one periodic announcement, got 0")
	}

	call := sender.lastCall()
	if call.PeerAddress != "addr-C" {
		t.Fatalf("expected peer address addr-C, got %s", call.PeerAddress)
	}
	if len(call.Routes) == 0 {
		t.Fatal("expected at least one route in the announcement")
	}
}

func TestAnnounceLoopTriggeredUpdate(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-C", Identity: "peer-C"},
		}
	}

	// Use a very long interval so only triggered updates fire.
	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	// Trigger an immediate update.
	loop.TriggerUpdate()
	time.Sleep(50 * time.Millisecond)

	if sender.callCount() == 0 {
		t.Fatal("expected triggered announcement, got 0")
	}

	cancel()
	<-done
}

func TestAnnounceLoopSplitHorizon(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Announce to peer-B: the direct route for peer-B was learned via
	// peer-B (NextHop == peer-B), so it should be excluded by split horizon.
	peers := func() []AnnounceTarget {
		return []AnnounceTarget{
			{Address: "addr-B", Identity: "peer-B"},
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

	// Split horizon should exclude routes learned from peer-B when
	// announcing to peer-B. The direct route has NextHop == "peer-B",
	// which equals the announce target identity "peer-B", so no routes
	// should be sent.
	if sender.callCount() != 0 {
		t.Fatalf("expected 0 calls due to split horizon, got %d", sender.callCount())
	}
}

func TestAnnounceLoopNoPeersNoSend(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}

	if _, err := table.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// No peers to announce to.
	peers := func() []AnnounceTarget {
		return nil
	}

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if sender.callCount() != 0 {
		t.Fatalf("expected 0 sends with no peers, got %d", sender.callCount())
	}
}

func TestAnnounceLoopTriggerCoalesces(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}

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

	// Fire multiple triggers rapidly — they should coalesce.
	for i := 0; i < 10; i++ {
		loop.TriggerUpdate()
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Due to coalescing, we expect far fewer than 10 sends.
	// At least 1 (the first trigger), at most a few.
	count := sender.callCount()
	if count == 0 {
		t.Fatal("expected at least 1 triggered send")
	}
	if count > 5 {
		t.Fatalf("expected coalescing to limit sends, got %d", count)
	}
}

func TestAnnounceLoopRunOnlyOnce(t *testing.T) {
	table := NewTable(WithLocalOrigin("node-A"))
	sender := &mockPeerSender{}
	peers := func() []AnnounceTarget { return nil }

	loop := NewAnnounceLoop(table, sender, peers,
		WithAnnounceInterval(10*time.Second))

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
