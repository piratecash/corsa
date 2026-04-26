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

// applyingPeerSender is a test PeerSender that records each SendAnnounceRoutes
// invocation AND applies the carried entries to a receiver-side routing table
// the same way node.Service.applyAnnounceEntries would: Hops+1, NextHop set to
// the sender identity, Source=RouteSourceAnnouncement. This lets the test drive
// real BuildAnnounceSnapshot/ComputeDelta/UpdateRoute paths end-to-end without
// pulling in the node package.
type applyingPeerSender struct {
	mu             sync.Mutex
	calls          int
	notify         chan struct{}
	senderIdentity routing.PeerIdentity
	receiverTable  *routing.Table
}

func newApplyingPeerSender(
	t *testing.T,
	senderIdentity routing.PeerIdentity,
	receiverTable *routing.Table,
) (*routingmocks.MockPeerSender, *applyingPeerSender) {
	t.Helper()
	rec := &applyingPeerSender{
		senderIdentity: senderIdentity,
		receiverTable:  receiverTable,
		notify:         make(chan struct{}, 64),
	}
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, routes []routing.AnnounceEntry) bool {
			rec.apply(routes)
			return true
		},
	).Maybe()
	m.EXPECT().SendRoutesUpdate(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ routing.PeerAddress, delta []routing.AnnounceEntry) bool {
			rec.apply(delta)
			return true
		},
	).Maybe()
	return m, rec
}

func (r *applyingPeerSender) apply(entries []routing.AnnounceEntry) {
	for _, e := range entries {
		hops := e.Hops + 1
		if hops > routing.HopsInfinity {
			hops = routing.HopsInfinity
		}
		_, _ = r.receiverTable.UpdateRoute(routing.RouteEntry{
			Identity: e.Identity,
			Origin:   e.Origin,
			NextHop:  r.senderIdentity,
			Hops:     hops,
			SeqNo:    e.SeqNo,
			Source:   routing.RouteSourceAnnouncement,
			Extra:    e.Extra,
		})
	}
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *applyingPeerSender) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// waitForCallCount blocks until at least want sends have been observed or the
// real-time deadline elapses. Real-time, not mock-time, on purpose: the
// AnnounceLoop goroutine schedules its work on the host scheduler and we need
// a wall-clock deadline to bound the test, independent of the simulated clock
// that drives ExpiresAt arithmetic inside the table.
func (r *applyingPeerSender) waitForCallCount(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if r.callCount() >= want {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("expected at least %d sends within %s, got %d", want, timeout, r.callCount())
		}
		select {
		case <-r.notify:
		case <-time.After(remaining):
		}
	}
}

// TestAnnounceLoop_ForcedFullSync_RefreshesLearnedRouteBeforeExpiry is the
// end-to-end regression guard for the "learned routes silently expire on
// neighbors between forced full syncs" bug. Two routing tables share a mock
// clock; the hub holds a direct route, the receiver mirrors the learned
// transit route. Driving forced-full cycles spaced one full refresh interval
// apart must keep the receiver's ExpiresAt strictly increasing and never let
// the route expire — otherwise the suppress-on-no-op path on the send side
// or the no-extend-on-RouteUnchanged path on the receive side has regressed.
//
// Both halves of the contract are verified by this single test:
//
//  1. The send side must actually emit a refresh frame at intervals tighter
//     than the receiver's TTL — guaranteed by the
//     ForcedFullSyncMultiplier*DefaultAnnounceInterval <= DefaultTTL/2
//     invariant that TestRoutingConstants_RefreshIntervalInvariant guards.
//
//  2. The receive side must extend ExpiresAt on a same-SeqNo same-source
//     same-hops re-application of an alive route — without this, the wire
//     refresh hits Table.UpdateRoute, returns RouteUnchanged, and leaves the
//     existing entry to age out at its original deadline.
func TestAnnounceLoop_ForcedFullSync_RefreshesLearnedRouteBeforeExpiry(t *testing.T) {
	var (
		clockMu sync.Mutex
		current = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	)
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return current
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		defer clockMu.Unlock()
		current = current.Add(d)
	}

	const (
		hubIdentity      routing.PeerIdentity = "1111111111111111111111111111111111111111"
		targetIdentity   routing.PeerIdentity = "2222222222222222222222222222222222222222"
		receiverIdentity routing.PeerIdentity = "3333333333333333333333333333333333333333"
		receiverAddress  routing.PeerAddress  = "addr-receiver"
	)

	hubTable := routing.NewTable(
		routing.WithClock(nowFn),
		routing.WithLocalOrigin(hubIdentity),
	)
	if _, err := hubTable.AddDirectPeer(targetIdentity); err != nil {
		t.Fatalf("hub.AddDirectPeer: %v", err)
	}

	receiverTable := routing.NewTable(
		routing.WithClock(nowFn),
		routing.WithLocalOrigin(receiverIdentity),
	)

	mockSender, rec := newApplyingPeerSender(t, hubIdentity, receiverTable)

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: receiverAddress, Identity: receiverIdentity},
		}
	}

	registry := routing.NewAnnounceStateRegistry(routing.WithRegistryClock(nowFn))
	loop := routing.NewAnnounceLoop(hubTable, mockSender, peers,
		// A long real-time interval ensures the periodic ticker never fires
		// during the test — every cycle is driven explicitly via TriggerUpdate.
		routing.WithAnnounceInterval(routing.DefaultAnnounceInterval),
		routing.WithStateRegistry(registry),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	target := routing.RouteTriple{
		Identity: targetIdentity,
		Origin:   hubIdentity,
		NextHop:  hubIdentity,
	}

	// First trigger: forced full sync (no prior baseline).
	loop.TriggerUpdate()
	rec.waitForCallCount(t, 1, time.Second)

	initial := receiverTable.InspectTriple(target)
	if initial == nil {
		t.Fatalf("receiver did not learn the direct route after first sync")
	}
	if initial.IsExpired(nowFn()) {
		t.Fatalf("initial route is already expired: ExpiresAt=%s now=%s",
			initial.ExpiresAt, nowFn())
	}
	prevExpiry := initial.ExpiresAt

	// refreshInterval is exactly one forced-full sync cadence — no jitter,
	// no padding. The production check is `>=`, so a cycle landing
	// precisely at multiplier*interval must trigger the full sync.
	// Padding the test with `+ time.Second` would mask a regression where
	// the predicate slips back to strict `>` and the real cadence on
	// production tickers becomes (multiplier+1)*interval. We drive enough
	// cycles to cover well past two TTL windows; any single iteration
	// where the receiver entry expires or its ExpiresAt fails to advance
	// is a regression.
	refreshInterval := time.Duration(routing.ForcedFullSyncMultiplier) * routing.DefaultAnnounceInterval
	totalSpan := 2 * routing.DefaultTTL
	iterations := int(totalSpan / refreshInterval)
	if iterations < 2 {
		iterations = 2
	}

	for i := 0; i < iterations; i++ {
		advance(refreshInterval)
		loop.TriggerUpdate()
		rec.waitForCallCount(t, i+2, time.Second)

		snap := receiverTable.InspectTriple(target)
		if snap == nil {
			t.Fatalf("iteration %d: receiver lost the route entirely (now=%s)",
				i, nowFn())
		}
		if !snap.ExpiresAt.After(prevExpiry) {
			t.Fatalf("iteration %d: ExpiresAt did not advance after refresh: prev=%s new=%s now=%s",
				i, prevExpiry, snap.ExpiresAt, nowFn())
		}
		if snap.IsExpired(nowFn()) {
			t.Fatalf("iteration %d: route is expired despite refresh: ExpiresAt=%s now=%s",
				i, snap.ExpiresAt, nowFn())
		}
		prevExpiry = snap.ExpiresAt
	}
}
