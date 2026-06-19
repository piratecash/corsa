package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_shadow_commit_test.go covers the Phase 3 deploy-1 cursor commit
// contract end-to-end: the change-journal cursor must advance only when the
// delta send actually succeeded. A cursor that advanced on a failed send would
// move past the journal entry that produced the delta, so the retried delta
// next cycle would no longer be covered by the journal — surfacing as a false
// ShadowDivergenceTotal. The test asserts that count stays zero across a
// fail-then-retry delta.

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

func TestAnnounceLoop_ShadowCursorHoldsOnSendFailure(t *testing.T) {
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

	// Fail the next delta send: the cursor must NOT advance past dest-Y.
	rec.setFailNext(1)
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 2 })

	// Retry: the same delta goes out successfully now.
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 3 })

	cancel()
	<-done

	// dest-Y was journaled and its delta was failed-then-retried. If the cursor
	// had advanced on the failed send, the retry would find dest-Y past the
	// cursor and not in the journal window → a false divergence. Zero proves the
	// cursor held on failure.
	if got := loop.ShadowDivergenceTotal(); got != 0 {
		t.Fatalf("shadow divergence must stay 0 (cursor must not advance on send failure); got %d", got)
	}
}
