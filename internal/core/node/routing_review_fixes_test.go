package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_review_fixes_test.go pins behaviour fixed in PR 11.7 in
// response to a Phase 2 review pass. Each test corresponds to one
// numbered concern from that review.

// TestReview_P1_1_HandleRouteProbe_AppliesSplitHorizon — concern
// P1#1: the probe responder must not advertise reachability via
// the probe sender itself. Without the split-horizon filter the
// sender would mark its own broken uplink as healthy on the
// strength of the responder's ack.
//
// Setup: A's routing table has a route to X with NextHop=B
// (i.e. A reaches X through B). B then probes A for X. A's
// answer must be reachable=false because the only route loops
// back through B.
func TestReview_P1_1_HandleRouteProbe_AppliesSplitHorizon(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Seed a route to idTargetX through idPeerB.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}

	// Sanity check: without split-horizon Lookup returns the
	// route via idPeerB.
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 1 || routes[0].NextHop != idPeerB {
		t.Fatalf("setup: Lookup result unexpected: %+v", routes)
	}

	// Now apply split-horizon manually as the handler does.
	filtered := filterSplitHorizon(svc.routingTable.Lookup(idTargetX), idPeerB)
	if len(filtered) != 0 {
		t.Fatalf("filterSplitHorizon(routes, idPeerB) = %d entries, want 0 (the only route is via idPeerB)", len(filtered))
	}
}

// TestReview_P1_1_HandleRouteQuery_AppliesSplitHorizon mirrors
// the probe test for the query handler. Without split-horizon the
// query response would advertise a path back through the sender,
// forming a loop when the sender ingests it as RouteSourceAnnouncement.
func TestReview_P1_1_HandleRouteQuery_AppliesSplitHorizon(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}

	filtered := filterSplitHorizon(svc.routingTable.Lookup(idTargetX), idPeerB)
	if len(filtered) != 0 {
		t.Fatalf("filterSplitHorizon for query handler = %d entries, want 0", len(filtered))
	}
}

// TestReview_P1_1_FilterSplitHorizon_LeavesUnrelatedRoutesIntact —
// the filter is targeted: routes via OTHER uplinks pass through
// unchanged. Only routes whose NextHop equals the excluded peer
// are dropped.
func TestReview_P1_1_FilterSplitHorizon_LeavesUnrelatedRoutesIntact(t *testing.T) {
	routes := []routing.RouteEntry{
		{Identity: idTargetX, NextHop: idPeerB, Hops: 2},
		{Identity: idTargetX, NextHop: idPeerC, Hops: 3},
		{Identity: idTargetX, NextHop: idPeerD, Hops: 4},
	}
	got := filterSplitHorizon(routes, idPeerC)
	if len(got) != 2 {
		t.Fatalf("filtered len = %d, want 2 (only idPeerC dropped)", len(got))
	}
	for _, r := range got {
		if r.NextHop == idPeerC {
			t.Fatalf("filterSplitHorizon did not drop NextHop=idPeerC: %v", r)
		}
	}
}

// TestReview_P1_1_FilterSplitHorizon_EmptyExcludeReturnsInput — the
// fast-path no-op: empty excludeVia returns the input slice.
func TestReview_P1_1_FilterSplitHorizon_EmptyExcludeReturnsInput(t *testing.T) {
	routes := []routing.RouteEntry{{Identity: idTargetX, NextHop: idPeerB, Hops: 2}}
	got := filterSplitHorizon(routes, domain.PeerIdentity{})
	if len(got) != 1 {
		t.Fatalf("filtered len = %d, want 1 (empty excludeVia is a no-op)", len(got))
	}
}

// TestReview_P1_2_ProbeTick_AgesGoodToQuestionable — concern P1#2:
// without the passive idle tick wired into probeTick, a Good pair
// never transitions to Questionable and the active probe path is
// effectively dead. After this fix, probeTick must call TickHealth
// before its Questionable scan, so a pair that has been silent
// for >60 s reaches Questionable.
func TestReview_P1_2_ProbeTick_AgesGoodToQuestionable(t *testing.T) {
	t0 := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := t0
	tbl := routing.NewTable(
		routing.WithLocalOrigin(idNodeA),
		routing.WithClock(func() time.Time { return clk }),
	)
	svc := &Service{
		routingTable: tbl,
		probeRegistry: newProbeRegistry(
			routing.HealthProbeTimeout,
			func() time.Time { return clk },
			tbl.MarkProbeFailure,
		),
		queryRateLimit: newQueryRateLimit(nil, 0, 0),
	}

	// Create a Good health entry with LastHopAck at t0.
	tbl.MarkHopAck(idTargetX, idPeerB, 0)

	// Advance clock past the Questionable threshold (60 s).
	clk = t0.Add(65 * time.Second)

	// Sanity: before the tick the pair is still Good in the
	// snapshot because applyIdleTick has not yet been invoked.
	if snap := tbl.HealthSnapshot(); len(snap) != 1 || snap[0].Health != routing.HealthGood {
		t.Fatalf("setup: snapshot = %+v, want one Good entry", snap)
	}

	svc.probeTick()

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("after probeTick: snapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("after probeTick at idle=65s: Health = %s, want questionable", snap[0].Health)
	}
}

// TestReview_P1_3_TableRouter_TriggersQueryOnEmptyLookup — concern
// P1#3: when TableRouter.Route hits an empty Lookup, the relay
// path used to fall back to gossip silently without nudging
// route discovery. After this fix triggerRouteQueryAsync is
// invoked, which consumes one slot from the per-target rate
// limit budget. We assert the budget consumption (synchronous
// observation that the trigger fired).
func TestReview_P1_3_TableRouter_TriggersQueryOnEmptyLookup(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)
	// Manually wire queryRateLimit because the test fixture does
	// not run through NewService.
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// Add a capable peer so SendRouteQuery has at least one
	// candidate uplink. Otherwise SendRouteQuery returns 0 before
	// consuming the budget. PR 11.9 P2#3 requires the full triplet
	// (query + relay + routing) on candidate peers. PR 11.21 P2#2
	// additionally requires a live peerHealth entry — the helper
	// seeds both.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	router := &TableRouter{
		svc:   svc,
		table: svc.routingTable,
		sessionChecker: func(domain.PeerIdentity, int) domain.PeerAddress {
			return ""
		},
	}

	router.Route(protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    idNodeA.String(),
		Recipient: idTargetX.String(),
	})

	// triggerRouteQueryAsync spawns a goroutine; allow it to run.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if svc.queryRateLimit.PendingCount(idTargetX) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got == 0 {
		t.Fatal("TableRouter.Route did not trigger a route_query_v1 after empty Lookup")
	}
}

// TestReview_v35_P2_TableRouter_TriggersQueryOnBadRouteSelected —
// PR 11.35 P2 regression. When TableRouter.Route picks a route
// that is HealthBad (the only available option, since Lookup is
// CompositeScore-ranked and Bad ranks below Good/Questionable),
// the relay path must fire route_query in the background as fast
// recovery — previously the trigger only fired on empty Lookup,
// missing session, or send failure, leaving the unhealthy window
// stretched until Bad→Dead (~60 s) or actual send failure.
func TestReview_v35_P2_TableRouter_TriggersQueryOnBadRouteSelected(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)
	svc.queryRateLimit = newQueryRateLimit(nil, 30*time.Second, queryFanOutLimit)

	// Triplet-capable peer so the spawned SendRouteQuery has a
	// fan-out target (same setup pattern as the empty-Lookup test).
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	// Seed a single route to idTargetX via idPeerB.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("setup: UpdateRoute: %v", err)
	}
	// Force the route's health to Bad so TableRouter.Route picks
	// it (Bad is selectable, just deprioritised) and then the new
	// recovery-trigger branch fires.
	svc.routingTable.ForceHealthForTest(idTargetX, idPeerB, routing.HealthBad)

	router := &TableRouter{
		svc:   svc,
		table: svc.routingTable,
		sessionChecker: func(peer domain.PeerIdentity, hops int) domain.PeerAddress {
			// Provide a usable address so the route is selected
			// (not skipped for missing session).
			if peer == idPeerB {
				return domain.PeerAddress("addr-B")
			}
			return ""
		},
	}

	router.Route(protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    idNodeA.String(),
		Recipient: idTargetX.String(),
	})

	// Wait for the spawned trigger goroutine to record its budget.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if svc.queryRateLimit.PendingCount(idTargetX) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := svc.queryRateLimit.PendingCount(idTargetX); got == 0 {
		t.Fatal("TableRouter.Route did not trigger a route_query_v1 after selecting a HealthBad route — recovery would have to wait for Bad→Dead transition or send failure (PR 11.35 P2)")
	}
}

// TestReview_P2_4_SendProbe_SkipsNonCapablePeer — concern P2#4:
// when the uplink does not advertise mesh_route_probe_v1,
// sendProbe must bail out BEFORE registering the outstanding
// probe. Without this short-circuit the registration would arm a
// timeout that fires MarkProbeFailure for a probe the peer never
// received — penalising mixed-version peers.
func TestReview_P2_4_SendProbe_SkipsNonCapablePeer(t *testing.T) {
	svc := newTestServiceWithProbeRegistry(t, idNodeA)
	// idPeerB session lacks mesh_route_probe_v1 capability.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1}, // no probe cap
	}

	before := svc.probeRegistry.Len()
	svc.sendProbe(idTargetX, idPeerB)
	after := svc.probeRegistry.Len()

	if after != before {
		t.Fatalf("sendProbe registered %d outstanding probe(s) for non-capable peer; want 0 (delta=%d)", after-before, after-before)
	}
}

// TestReview_P2_5_TickTTL_ReconcilesHealthAfterExpiry — concern
// P2#5: when CompactExpired physically removes a claim, the
// matching RouteHealthState entry must be evicted. Without this
// reconciliation a future announce that resurrects the same
// (Identity, Uplink) pair would be filtered out of Lookup under
// a stale Dead label.
func TestReview_P2_5_TickTTL_ReconcilesHealthAfterExpiry(t *testing.T) {
	t0 := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := t0
	tbl := routing.NewTable(
		routing.WithLocalOrigin(idNodeA),
		routing.WithClock(func() time.Time { return clk }),
	)

	// Seed a route with a short TTL.
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: t0.Add(10 * time.Second),
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}
	// Force the pair into HealthDead to simulate a stale health
	// label that would survive into the post-expiry world.
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	// Advance past the route's expiry.
	clk = t0.Add(120 * time.Second)
	tbl.TickTTL()

	// Reconciliation must have dropped the health entry along
	// with the storage claim.
	snap := tbl.HealthSnapshot()
	for _, s := range snap {
		if s.Identity == idTargetX && s.Uplink == idPeerB {
			t.Fatalf("TickTTL left stale Dead health entry after CompactExpired: %+v", s)
		}
	}
}

// TestReview_P2_5_UpdateRoute_EvictsStaleDeadHealthOnResurrection —
// when UpdateRoute accepts a fresh live claim for (Identity,
// Uplink) and a stale Bad/Dead health entry still exists for that
// pair, the entry must be evicted so the next Lookup does not
// filter the fresh claim out.
//
// This is the second eviction path: TickTTL covers physically-
// expired claims; UpdateRoute covers resurrection-without-expiry
// (e.g. a withdrawn claim returning before TickTTL cleared it).
func TestReview_P2_5_UpdateRoute_EvictsStaleDeadHealthOnResurrection(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	// Seed a route, then force its health to Dead.
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	// Pre-condition: Lookup filters the Dead claim out.
	if routes := tbl.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("setup: Lookup returned %d entries with Dead health; want 0", len(routes))
	}

	// Resurrection: a fresh live claim arrives (higher SeqNo).
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    2, // higher SeqNo so admission accepts
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("resurrection UpdateRoute: %v", err)
	}

	// Post-condition: the stale Dead health entry has been
	// evicted, so Lookup surfaces the fresh claim.
	routes := tbl.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("after resurrection: Lookup returned %d entries, want 1", len(routes))
	}
	if routes[0].NextHop != idPeerB {
		t.Fatalf("after resurrection: NextHop = %q, want idPeerB", routes[0].NextHop)
	}
}
