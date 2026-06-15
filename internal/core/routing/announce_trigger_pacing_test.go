package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// ---------------------------------------------------------------------------
// Trigger pacing (WithTriggerMinSpacing) — the anti-CPU-churn throttle
// for trigger-driven announce cycles.
//
// Contract under test:
//  1. The FIRST trigger (no recent trigger-driven cycle) runs
//     immediately — pacing must not delay a lone event.
//  2. Triggers inside the cooldown window are deferred to the window
//     boundary and coalesce into ONE cycle — a route-churn storm of N
//     triggers produces exactly one follow-up delta cycle, not N.
//  3. A trigger arriving after the window runs immediately again.
//  4. A pending deferred trigger never starves propagation: the
//     periodic ticker keeps firing on its own schedule.
//
// The routing-package tests that drive cycles via back-to-back
// TriggerUpdate calls rely on pacing being DISABLED by default in
// NewAnnounceLoop; production opts in via WithTriggerMinSpacing (see
// node.Service announce-loop wiring).
// ---------------------------------------------------------------------------

// pacingFixture builds a running loop with one announce target and a
// long periodic interval so only trigger-driven cycles fire (unless a
// test overrides the interval).
func pacingFixture(t *testing.T, spacing, interval time.Duration) (*routing.Table, *senderRecorder, *routing.AnnounceLoop, func()) {
	t.Helper()
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newMockPeerSender(t)

	if _, err := table.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(interval),
		routing.WithTriggerMinSpacing(spacing))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		loop.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done
	}
	return table, rec, loop, stop
}

// addTransitRoute seeds target-<seq> via peer-B so the next delta
// cycle has something new to send toward addr-C (peer-C) — split
// horizon does not filter it (NextHop peer-B != peer-C).
func addTransitRoute(t *testing.T, table *routing.Table, identity string, seq uint64) {
	t.Helper()
	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID(identity),
		Origin:   domaintest.ID("peer-B"),
		NextHop:  domaintest.ID("peer-B"),
		Hops:     2,
		SeqNo:    seq,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatalf("UpdateRoute(%s): status=%v err=%v", identity, status, err)
	}
}

// waitCallCount polls until the recorder reaches want calls or the
// deadline passes; returns the final count.
func waitCallCount(rec *senderRecorder, want int, deadline time.Duration) int {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if rec.callCount() >= want {
			return rec.callCount()
		}
		time.Sleep(5 * time.Millisecond)
	}
	return rec.callCount()
}

func TestAnnounceLoopTriggerPacing_FirstTriggerImmediate(t *testing.T) {
	t.Parallel()

	_, rec, loop, stop := pacingFixture(t, 10*time.Second, 10*time.Second)
	defer stop()

	loop.TriggerUpdate()

	if got := waitCallCount(rec, 1, time.Second); got == 0 {
		t.Fatal("first trigger must run immediately despite pacing; got 0 sends")
	}
}

func TestAnnounceLoopTriggerPacing_StormCoalescesIntoOneDeferredCycle(t *testing.T) {
	t.Parallel()

	const spacing = 400 * time.Millisecond
	table, rec, loop, stop := pacingFixture(t, spacing, time.Hour)
	defer stop()

	// Cycle 1: initial full sync (immediate — first trigger).
	loop.TriggerUpdate()
	if got := waitCallCount(rec, 1, time.Second); got != 1 {
		t.Fatalf("setup: first triggered cycle expected 1 send, got %d", got)
	}

	// Storm: 8 table mutations, each followed by TriggerUpdate, all
	// well inside the pacing window. Without pacing each trigger
	// would run its own delta cycle and most would produce a wire
	// send (every iteration adds a new route); with pacing they must
	// coalesce into exactly ONE deferred delta cycle at the window
	// boundary.
	for i := 1; i <= 8; i++ {
		addTransitRoute(t, table, "target-"+string(rune('0'+i)), uint64(i))
		loop.TriggerUpdate()
		time.Sleep(10 * time.Millisecond)
	}

	// The deferred cycle fires at most `spacing` after cycle 1.
	got := waitCallCount(rec, 2, spacing+time.Second)
	if got < 2 {
		t.Fatalf("deferred coalesced cycle never fired: %d sends", got)
	}
	// Give a residual window to catch extra (non-coalesced) cycles.
	time.Sleep(150 * time.Millisecond)
	if got := rec.callCount(); got != 2 {
		t.Fatalf("storm of 8 triggers must coalesce into ONE deferred cycle (2 sends total), got %d", got)
	}
	// The single deferred delta must carry the whole storm.
	if last := rec.lastCall(); len(last.Routes) < 8 {
		t.Fatalf("coalesced delta expected >=8 routes, got %d", len(last.Routes))
	}
}

func TestAnnounceLoopTriggerPacing_TriggerAfterWindowRunsImmediately(t *testing.T) {
	t.Parallel()

	const spacing = 100 * time.Millisecond
	table, rec, loop, stop := pacingFixture(t, spacing, time.Hour)
	defer stop()

	loop.TriggerUpdate()
	if got := waitCallCount(rec, 1, time.Second); got != 1 {
		t.Fatalf("setup: first triggered cycle expected 1 send, got %d", got)
	}

	// Let the cooldown window expire, then trigger with fresh state.
	time.Sleep(spacing + 50*time.Millisecond)
	addTransitRoute(t, table, "target-late", 1)
	start := time.Now()
	loop.TriggerUpdate()

	if got := waitCallCount(rec, 2, time.Second); got < 2 {
		t.Fatal("post-window trigger never produced a cycle")
	}
	// "Immediately" here means well under the pacing window — the
	// cycle must not have been deferred by another full spacing.
	if elapsed := time.Since(start); elapsed > spacing {
		t.Fatalf("post-window trigger took %v — looks deferred despite expired window (spacing=%v)", elapsed, spacing)
	}
}

func TestAnnounceLoopTriggerPacing_PeriodicTickerUnaffectedByPendingDeferral(t *testing.T) {
	t.Parallel()

	// Pacing window is effectively infinite: after the first trigger
	// every later trigger stays deferred for the whole test. The
	// PERIODIC ticker (50ms) must still propagate new state — pacing
	// must never starve the announce plane.
	table, rec, loop, stop := pacingFixture(t, time.Hour, 50*time.Millisecond)
	defer stop()

	loop.TriggerUpdate()
	if got := waitCallCount(rec, 1, time.Second); got == 0 {
		t.Fatal("setup: first triggered cycle never fired")
	}

	// This trigger is deferred for ~1h; only the periodic ticker can
	// deliver the route below.
	addTransitRoute(t, table, "target-periodic", 1)
	loop.TriggerUpdate()

	if got := waitCallCount(rec, 2, 2*time.Second); got < 2 {
		t.Fatal("periodic ticker did not propagate state while a deferred trigger was pending — pacing starves the announce plane")
	}
}
