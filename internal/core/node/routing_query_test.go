package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_query_test.go tests the Phase 2 PR 11.4b receive-side
// dispatcher hook-ins for route_query_v1 / route_query_response_v1.
// The sender-side rate-limited on-demand trigger lands in PR 11.4c;
// here we only exercise the handler half of the round trip.

// installTripletCapableUplinkForQueryTest wires a synthetic
// outbound peerSession with the full route_query ingest triplet
// (route_query_v1 + relay_v1 + routing_v1). PR 11.9 P2#3 requires
// the responder to advertise relay+routing so the ingested
// transit route is data-plane-usable; tests that exercise the
// ingest happy path call this helper to satisfy the gate.
func installTripletCapableUplinkForQueryTest(svc *Service, identity domain.PeerIdentity) {
	addr := domain.PeerAddress("addr-" + identity.String())
	svc.sessions[addr] = &peerSession{
		address:      addr,
		peerIdentity: identity,
		capabilities: []domain.Capability{
			domain.CapMeshRouteQueryV1,
			domain.CapMeshRelayV1,
			domain.CapMeshRoutingV1,
		},
	}
}

// TestHandleRouteQuery_RespondsFoundWhenRouteExists — happy path:
// the responder's local routing.Table has at least one non-Dead
// route to the queried target → the handler computes Lookup, picks
// the best entry (CompositeScore-ranked first), and would emit a
// Found=true response. We assert by confirming Lookup returns the
// expected best uplink — the handler's wire send is verified by
// the lower-level routing_probe_test pattern (no panic without a
// netcore writer).
func TestHandleRouteQuery_RespondsFoundWhenRouteExists(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Seed a route to idTargetX via idPeerB.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    7,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("setup: expected seeded route to surface from Lookup")
	}
	if routes[0].NextHop != idPeerB {
		t.Fatalf("setup: Lookup[0].NextHop = %q, want idPeerB", routes[0].NextHop)
	}

	query := protocol.RouteQueryFrame{
		Type:           protocol.RouteQueryFrameType,
		QueryID:        100,
		TargetIdentity: idTargetX,
	}
	// The handler discards the sendFrameViaNetwork error when no
	// writer is registered; the call must complete without panic.
	svc.handleRouteQuery(domain.ConnID(0), idPeerC, query)
}

// TestHandleRouteQuery_RespondsNotFoundWhenNoRoute — mirror case:
// no route in the table → handler answers Found=false. We confirm
// the cold-table precondition; the wire-send is exercised by the
// no-panic completion.
func TestHandleRouteQuery_RespondsNotFoundWhenNoRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("setup: expected empty Lookup for cold target, got %d", len(routes))
	}

	query := protocol.RouteQueryFrame{
		Type:           protocol.RouteQueryFrameType,
		QueryID:        101,
		TargetIdentity: idTargetX,
	}
	svc.handleRouteQuery(domain.ConnID(0), idPeerC, query)
}

// TestHandleRouteQuery_NoForwarding_SingleHopOnly — query handling
// is strictly single-hop. The handler MUST NOT inject the queried
// target into any forwarding pipeline or table mutation. After the
// handler runs we assert that no new routes appeared in the table.
func TestHandleRouteQuery_NoForwarding_SingleHopOnly(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	query := protocol.RouteQueryFrame{
		Type:           protocol.RouteQueryFrameType,
		QueryID:        7,
		TargetIdentity: idTargetX,
	}
	svc.handleRouteQuery(domain.ConnID(0), idPeerC, query)

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("handleRouteQuery injected route into table: %v", routes)
	}
}

// TestHandleRouteQueryResponse_IngestsAsAnnouncement — happy path:
// Found=true response feeds the standard UpdateRoute admission
// pipeline. The created entry uses NextHop=sender,
// Hops=resp.BestHops+1, Source=RouteSourceAnnouncement.
func TestHandleRouteQueryResponse_IngestsAsAnnouncement(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	installTripletCapableUplinkForQueryTest(svc, idPeerB)

	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        100,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD, // informational; ingest uses senderIdentity (idPeerB) as NextHop
		BestHops:       2,
		BestSeqNo:      42,
	}
	svc.handleRouteQueryResponse(idPeerB, resp)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup returned %d routes after ingest, want 1", len(routes))
	}
	got := routes[0]
	if got.NextHop != idPeerB {
		t.Fatalf("NextHop = %q, want idPeerB (sender, not response.BestUplink)", got.NextHop)
	}
	if got.Hops != 3 {
		t.Fatalf("Hops = %d, want 3 (response.BestHops=2 + 1)", got.Hops)
	}
	if got.SeqNo != 42 {
		t.Fatalf("SeqNo = %d, want 42", got.SeqNo)
	}
	if got.Source != routing.RouteSourceAnnouncement {
		t.Fatalf("Source = %s, want announcement", got.Source)
	}
}

// TestHandleRouteQueryResponse_FoundFalseIsNoop — when the responder
// confirms no route exists, the receiver records nothing. The table
// stays empty.
func TestHandleRouteQueryResponse_FoundFalseIsNoop(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        100,
		TargetIdentity: idTargetX,
		Found:          false,
	}
	svc.handleRouteQueryResponse(idPeerB, resp)

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("Found=false response caused ingest: %v", routes)
	}
}

// TestHandleRouteQueryResponse_DropsZeroHops — Found=true with
// BestHops=0 is an invalid wire shape (a hops=0 route would be the
// responder itself, which is a direct route, not something single-
// hop query response models). The handler drops it without poisoning
// the table.
func TestHandleRouteQueryResponse_DropsZeroHops(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        100,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       0,
		BestSeqNo:      42,
	}
	svc.handleRouteQueryResponse(idPeerB, resp)

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("Hops=0 response caused ingest: %v", routes)
	}
}

// TestHandleRouteQueryResponse_DropsSelfTarget — pathological case:
// peer claims to be reaching itself through a transit. The handler
// drops without ingest — direct routes belong to the AddDirectPeer
// path, not the query response path.
func TestHandleRouteQueryResponse_DropsSelfTarget(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        100,
		TargetIdentity: idPeerB, // target == sender
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       2,
		BestSeqNo:      42,
	}
	svc.handleRouteQueryResponse(idPeerB, resp)

	if routes := svc.routingTable.Lookup(idPeerB); len(routes) != 0 {
		t.Fatalf("self-target response caused ingest: %v", routes)
	}
}

// TestHandleRouteQueryResponse_DropsTargetEqualsLocalIdentity —
// PR 11.22 P2 regression. The receive-side ingest must drop
// responses whose target_identity equals our own fingerprint,
// mirroring the announce-plane guard in
// routing_announce.go::handleAnnounceRoutes ("Skip routes about
// ourselves"). Otherwise an attacker (or a buggy peer) could
// claim "I have a route to you" and force-feed us a synthetic
// transit entry (Origin = localIdentity, NextHop = sender) for
// our own identity — that entry would survive in storage,
// leak through HealthSnapshot, and could be projected to other
// peers because AnnounceProjection's excludeVia filter looks at
// NextHop, not Origin.
//
// The triplet-cap setup is included so the only thing that
// could reject the ingest is the local-self guard itself; this
// pins exactly that branch.
func TestHandleRouteQueryResponse_DropsTargetEqualsLocalIdentity(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	installTripletCapableUplinkForQueryTest(svc, idPeerB)

	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        101,
		TargetIdentity: idNodeA, // attacker claims to reach US
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       2,
		BestSeqNo:      42,
	}
	svc.handleRouteQueryResponse(idPeerB, resp)

	if routes := svc.routingTable.Lookup(idNodeA); len(routes) != 0 {
		// Lookup returns the synthetic self-route (RouteSourceLocal,
		// hops=0) injected by Table.Lookup when the queried identity
		// matches localOrigin. The attacker's claim must NOT be in
		// the storage layer. Filter local-source routes out of the
		// assertion — only an injected transit entry would fail
		// the test.
		for _, r := range routes {
			if r.Source != routing.RouteSourceLocal {
				t.Fatalf("local-identity-as-target response ingested a transit route: %+v", r)
			}
		}
	}
	// Defensive: HealthSnapshot should have no entry for
	// (idNodeA, idPeerB) — the ingest aborted before
	// UpdateRoute's health bookkeeping ran.
	for _, st := range svc.routingTable.HealthSnapshot() {
		if st.Identity == idNodeA && st.Uplink == idPeerB {
			t.Fatalf("local-identity-as-target response created health entry: %+v", st)
		}
	}
}

// TestHandleRouteQueryResponse_IngestRespectsAdmission — second
// ingest with stale SeqNo is rejected by the standard admission
// pipeline (SeqNo monotonicity). This confirms that query responses
// flow through the same guards as regular announce_routes — no
// privileged ingestion path.
func TestHandleRouteQueryResponse_IngestRespectsAdmission(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	installTripletCapableUplinkForQueryTest(svc, idPeerB)

	// Initial ingest at SeqNo=42, Hops=3 (BestHops=2+1).
	svc.handleRouteQueryResponse(idPeerB, protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        100,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       2,
		BestSeqNo:      42,
	})

	// Second ingest at lower SeqNo=10 — must be rejected.
	svc.handleRouteQueryResponse(idPeerB, protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        101,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerD,
		BestHops:       1, // would be better if accepted
		BestSeqNo:      10,
	})

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("Lookup len = %d, want 1", len(routes))
	}
	if routes[0].SeqNo != 42 {
		t.Fatalf("SeqNo = %d, want 42 (stale SeqNo replay must be rejected)", routes[0].SeqNo)
	}
	if routes[0].Hops != 3 {
		t.Fatalf("Hops = %d, want 3 (original ingest preserved)", routes[0].Hops)
	}
}

// TestP2PWireCommands_IncludesRouteQueryTypes — auth gate
// invariant: the route_query types are members of p2pWireCommands
// so that an unauthenticated peer sending one gets auth_required
// rather than unknown_command.
func TestP2PWireCommands_IncludesRouteQueryTypes(t *testing.T) {
	if !isP2PWireCommand(protocol.RouteQueryFrameType) {
		t.Fatalf("p2pWireCommands missing %q", protocol.RouteQueryFrameType)
	}
	if !isP2PWireCommand(protocol.RouteQueryResponseFrameType) {
		t.Fatalf("p2pWireCommands missing %q", protocol.RouteQueryResponseFrameType)
	}
}

// TestLocalCapabilities_AdvertisesRouteQueryV1 — local capability
// advertisement must include mesh_route_query_v1.
func TestLocalCapabilities_AdvertisesRouteQueryV1(t *testing.T) {
	caps := localCapabilities(false)
	for _, c := range caps {
		if c == domain.CapMeshRouteQueryV1 {
			return
		}
	}
	t.Fatalf("localCapabilities does not advertise %q: %v", domain.CapMeshRouteQueryV1, caps)
}
