package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/routing"
)

// announce_cursor_mode_test.go covers the cursor-authoritative delta path
// end-to-end: a delta cycle must emit ONLY the routes the change journal
// recorded since the peer's cursor — not a full rebuild — proving
// AnnounceDeltaTo drives the wire send. Cursor mode is unconditional, so the
// loop is constructed normally with no opt-in.

func TestAnnounceLoop_CursorMode_DeltaSendsOnlyChanged(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(domaintest.ID("node-A")))
	sender, rec := newControllableMockPeerSender(t)

	// peer-C advertises no caps → legacy v1 delta path → SendAnnounceRoutes,
	// which the mock records (routes = delta on a delta cycle, full on forced).
	peers := func() []routing.AnnounceTarget {
		return []routing.AnnounceTarget{
			{Address: "addr-C", Identity: domaintest.ID("peer-C")},
		}
	}

	// A route present before the first sync so the forced-full baseline emits a
	// real legacy frame (flips the wire baseline so later v1 deltas send).
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

	// First sync: no baseline → forced-full. Carries dest-X.
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 1 })

	// Journal a new route the peer must learn → cursor delta next cycle.
	if _, err := table.UpdateRoute(routing.RouteEntry{
		Identity: domaintest.ID("dest-Y"), Origin: domaintest.ID("orig-Y"),
		NextHop: domaintest.ID("uplink-Y"), Hops: 2, SeqNo: 1,
		Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatal(err)
	}
	loop.TriggerUpdate()
	waitUntil(t, func() bool { return rec.callCount() >= 2 })

	cancel()
	<-done

	calls := rec.getCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 sends (forced-full + cursor delta), got %d", len(calls))
	}

	// The cursor delta cycle (second send) must carry ONLY dest-Y — proof it is
	// a journal-driven delta, not a full re-projection (which would also carry
	// dest-X).
	delta := calls[1].Routes
	if len(delta) != 1 {
		t.Fatalf("cursor delta must carry exactly the one changed route, got %d: %+v", len(delta), delta)
	}
	if delta[0].Identity != domaintest.ID("dest-Y") {
		t.Fatalf("cursor delta carried %s, expected dest-Y", delta[0].Identity)
	}
}
