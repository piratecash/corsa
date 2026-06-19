package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_cursor_commit_test.go covers the cursor commit contract end-to-end:
// the change-journal cursor advances ONLY when the delta send actually
// succeeded (RecordCursorAdvance runs in the send helper's onSuccess). A cursor
// that advanced on a failed send would move past the journal entry that produced
// the delta, so the retried delta next cycle would no longer be projected and
// the route would silently never reach the peer. The test fails a delta send,
// then asserts the retry re-delivers the same route.

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within deadline")
	}
}

func TestAnnounceLoop_CursorHoldsOnSendFailure(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	// A route present before the initial sync so the forced-full baseline emits
	// a real wire frame (an empty snapshot would short-circuit without a send),
	// which also flips the legacy wire baseline so subsequent deltas are sent.
	if _, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("dest-X"), Origin: domaintest.ID("orig-X"),
		NextHop: domaintest.ID("uplink-X"), Hops: 2, SeqNo: 1,
		Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatal(err)
	}

	loop := routing.NewAnnounceLoop(table, sender, peers,
		routing.WithAnnounceInterval(10*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { loop.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	// Initial forced-full baseline → one send.
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 1 })

	// A new journaled route peer-C must learn → produces a delta next cycle.
	if _, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("dest-Y"), Origin: domaintest.ID("orig-Y"),
		NextHop: domaintest.ID("uplink-Y"), Hops: 2, SeqNo: 1,
		Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatal(err)
	}

	// Fail the next delta send: onSuccess (RecordCursorAdvance) must not run, so
	// the cursor stays put and dest-Y remains in the journal window.
	rec.setFailNext(1)
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 2 })

	// Retry: because the cursor held, AnnounceDeltaTo re-projects dest-Y and the
	// delta goes out successfully now. (If the cursor had advanced on the failed
	// send, dest-Y would be past it and the retry would be a no-op — no 3rd send,
	// and this wait would time out.)
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 3 })

	cancel()
	<-done

	calls := rec.getCalls()
	if len(calls) < 3 {
		t.Fatalf("expected at least 3 sends (full + failed delta + retry), got %d", len(calls))
	}
	// The retry (3rd send) must carry dest-Y — proof the failed send did not
	// advance the cursor past it.
	retry := calls[2]
	foundY := false
	for _, r := range retry.Routes {
		if r.Identity == domaintest.ID("dest-Y") {
			foundY = true
		}
	}
	if !foundY {
		t.Fatalf("retry delta must re-deliver dest-Y (cursor must hold on failure); got %+v", retry.Routes)
	}
}
