package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_review_fixes_v3_test.go pins behaviour fixed in PR 11.9
// in response to a third Phase 2 review pass.

// --- P1 #1: Questionable initial state, not Good ---

// TestReview_v3_P1_1_UpdateRoute_SeedsQuestionable — fresh claim
// must NOT enter Good directly; Good is reserved for confirmed
// reachability (hop_ack or probe_ack). Seeding Good would let a
// peer-attested claim bypass local verify-before-trust and earn
// the full composite-score bonus for up to one passive idle
// window (60 s) without any probe round-trip.
func TestReview_v3_P1_1_UpdateRoute_SeedsQuestionable(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (fresh claim must be probed before it earns Good)", snap[0].Health)
	}
}

// TestReview_v3_P1_1_UpdateRoute_ResurrectionFromDeadSeedsQuestionable —
// after a Bad/Dead pair is evicted (PR 11.8 P2#5), the seeded
// replacement must also be Questionable, not Good. Otherwise a
// route that was Dead, expired, then re-announced would skip
// verify-before-trust.
func TestReview_v3_P1_1_UpdateRoute_ResurrectionFromDeadSeedsQuestionable(t *testing.T) {
	tbl := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}
	tbl.ForceHealthForTest(idTargetX, idPeerB, routing.HealthDead)

	// Resurrection via fresh live announce.
	if _, err := tbl.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 2, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("resurrection UpdateRoute: %v", err)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (resurrected pair must be re-verified, not seeded Good)", snap[0].Health)
	}
}

// --- P2 #2: TickHealth runs under overload ---

// TestReview_v3_P2_2_ProbeTick_AgesUnderOverload — Phase 0
// overload gate disables the probe-send step ONLY; passive aging
// must continue so routes converge to Bad/Dead on the slower
// hop_ack timeline. Without this fix stale Good routes stayed
// selectable during overload precisely when the node was most
// degraded.
func TestReview_v3_P2_2_ProbeTick_AgesUnderOverload(t *testing.T) {
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

	// Engage overload (threshold=1 is below any real goroutine count).
	svc.overloadMonitor = newOverloadMonitor(1)
	if !svc.overloadMonitor.IsOverloaded() {
		t.Skip("overload gate did not engage at threshold=1 — runtime layout may have changed")
	}

	// Seed a Good pair with hop_ack at t0.
	tbl.MarkHopAck(idTargetX, idPeerB, 0)

	// Advance clock past the Questionable threshold and tick.
	clk = t0.Add(65 * time.Second)
	svc.probeTick()

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != routing.HealthQuestionable {
		t.Fatalf("Health after 65s idle + overload tick = %s, want questionable (aging must run even under overload)", snap[0].Health)
	}
	// And no probe should have been registered (probe send IS gated).
	if got := svc.probeRegistry.Len(); got != 0 {
		t.Fatalf("probeRegistry.Len = %d under overload; want 0 (probe send must be gated)", got)
	}
}

// --- P2 #3: query fan-out + ingest require relay+routing caps ---

// TestReview_v3_P2_3_PeersWithRouteQueryCap_RequiresTriplet — a
// peer with only mesh_route_query_v1 (no relay/routing) must not
// be selected for query fan-out, because the ingested route from
// its response would be data-plane-unusable through that peer.
func TestReview_v3_P2_3_PeersWithRouteQueryCap_RequiresTriplet(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Peer with only the query cap → MUST be skipped (lacks
	// relay+routing). Health is irrelevant because capability
	// check rejects first.
	seedHealthyPeerSession(t, svc, "addr-B", idPeerB, []domain.Capability{domain.CapMeshRouteQueryV1})
	// Peer with the full triplet → MUST be selected. PR 11.21
	// P2#2 also requires health, hence the helper.
	seedHealthyPeerSession(t, svc, "addr-C", idPeerC, []domain.Capability{
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
	})

	candidates := svc.peersWithRouteQueryCap()
	if len(candidates) != 1 {
		t.Fatalf("peersWithRouteQueryCap returned %d candidates, want 1 (only triplet-capable peer)", len(candidates))
	}
	if candidates[0] != idPeerC {
		t.Fatalf("candidate = %q, want idPeerC", candidates[0])
	}
}

// TestReview_v3_P2_3_HandleRouteQueryResponse_RejectsMissingRelayCap —
// the receive-side ingest guard. If a peer somehow gets through
// the dispatcher (e.g. capability advertisement race or buggy
// peer) without mesh_relay_v1 + mesh_routing_v1, the response
// must be dropped before UpdateRoute writes an unusable route.
func TestReview_v3_P2_3_HandleRouteQueryResponse_RejectsMissingRelayCap(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Peer with only the query cap.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRouteQueryV1},
	}

	svc.handleRouteQueryResponse(idPeerB, protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        1,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       2,
		BestSeqNo:      42,
	})

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("Lookup len = %d, want 0 (response from non-relay-capable peer must not produce a route)", len(routes))
	}
}

// TestReview_v3_P2_3_HandleRouteQueryResponse_AcceptsFromTripletCapable —
// happy-path counterpart: a response from a peer with the full
// triplet (query + relay + routing) is ingested normally.
func TestReview_v3_P2_3_HandleRouteQueryResponse_AcceptsFromTripletCapable(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{
			domain.CapMeshRouteQueryV1,
			domain.CapMeshRelayV1,
			domain.CapMeshRoutingV1,
		},
	}

	svc.handleRouteQueryResponse(idPeerB, protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        1,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       2,
		BestSeqNo:      42,
	})

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup len = %d, want 1 (response from triplet-capable peer must ingest)", len(routes))
	}
	if routes[0].NextHop != idPeerB {
		t.Fatalf("NextHop = %q, want idPeerB", routes[0].NextHop)
	}
}

// TestReview_v3_P2_3_PeerHasCapabilities_TripletCheck — direct
// unit test for the helper that backs both the fan-out and the
// ingest guard. Confirms ALL caps must be present.
func TestReview_v3_P2_3_PeerHasCapabilities_TripletCheck(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{
			domain.CapMeshRelayV1,
			domain.CapMeshRoutingV1,
			domain.CapMeshRouteQueryV1,
		},
	}
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		peerIdentity: idPeerC,
		capabilities: []domain.Capability{
			domain.CapMeshRouteQueryV1, // missing relay + routing
		},
	}

	if !svc.peerHasCapabilities(idPeerB, domain.CapMeshRelayV1, domain.CapMeshRoutingV1) {
		t.Fatal("peerHasCapabilities returned false for triplet-capable peer; want true")
	}
	if svc.peerHasCapabilities(idPeerC, domain.CapMeshRelayV1, domain.CapMeshRoutingV1) {
		t.Fatal("peerHasCapabilities returned true for query-only peer; want false (missing relay+routing)")
	}
	if svc.peerHasCapabilities("unknown-fp", domain.CapMeshRelayV1) {
		t.Fatal("peerHasCapabilities returned true for unknown peer; want false")
	}
}
