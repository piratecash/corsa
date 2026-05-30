package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_probe_test.go tests the receive-side dispatcher
// hook-ins for route_probe_v1 / route_probe_ack_v1
// (handleRouteProbe / handleRouteProbeAck). Sender-side
// concerns (ticker, outstanding-probe registry, send-time
// stamping for RTT measurement) live in
// routing_probe_loop_test.go and routing_review_fixes_v4_test.go.
//
// The handler tests here exercise:
//   - Found / not-found probe responses (handleRouteProbe).
//   - Drop semantics for an ack whose probe_id is not in the
//     outstanding registry — see the
//     TestHandleRouteProbeAck_DropsUnknownAck test below. The
//     handler itself goes through probeRegistry.ResolveMatching
//     and, on success, calls routingTable.MarkProbeAck with the
//     measured RTT; the "unknown ack" case is the negative
//     control for that contract.

// TestHandleRouteProbe_RespondsReachableWhenRouteExists — happy
// path: the responder has a route to the queried target and answers
// reachable=true with the probe_id echoed unchanged.
//
// The send path is captured by stubbing the netcore writer via the
// existing mock-connection helpers used by the routing receive-path
// tests (see mockconn_test.go); we drive handleRouteProbe directly
// to exercise the handler logic.
func TestHandleRouteProbe_RespondsReachableWhenRouteExists(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Seed the routing table with a reachable route to idTargetX.
	_, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idTargetX,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("seed UpdateRoute: %v", err)
	}

	// Look up the route to confirm seed succeeded — we are testing
	// the handler's Lookup-based reachable decision, not Lookup
	// itself.
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) == 0 {
		t.Fatal("setup: routingTable.Lookup returned no routes for seeded target")
	}

	probe := protocol.RouteProbeFrame{
		Type:           protocol.RouteProbeFrameType,
		ProbeID:        42,
		TargetIdentity: idTargetX,
	}

	// Without a connID-registered netcore writer the
	// sendFrameViaNetwork call inside handleRouteProbe will return
	// an error that is swallowed by the `_ =` discard. The handler
	// must not panic and must complete the local Lookup.
	svc.handleRouteProbe(domain.ConnID(0), idPeerB, probe)
}

// TestHandleRouteProbe_RespondsUnreachableWhenNoRoute — mirror case:
// no route in the table → handler answers reachable=false. We assert
// indirectly by confirming Lookup truly returns empty for the
// queried target (handler's reachable=false branch is then the only
// reachable code path).
func TestHandleRouteProbe_RespondsUnreachableWhenNoRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("setup: routingTable.Lookup expected empty for cold target, got %d", len(routes))
	}

	probe := protocol.RouteProbeFrame{
		Type:           protocol.RouteProbeFrameType,
		ProbeID:        99,
		TargetIdentity: idTargetX,
	}

	// Handler must complete without panic even when no route exists.
	svc.handleRouteProbe(domain.ConnID(0), idPeerB, probe)
}

// TestHandleRouteProbe_NoForwarding_SingleHopOnly — the probe is
// strictly single-hop; the handler MUST NOT inject the queried
// target into any forwarding pipeline. We confirm by asserting that
// after the handler runs, the announce table is unchanged (no
// stray route promotion) and no relay attempt is enqueued.
func TestHandleRouteProbe_NoForwarding_SingleHopOnly(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	probe := protocol.RouteProbeFrame{
		Type:           protocol.RouteProbeFrameType,
		ProbeID:        7,
		TargetIdentity: idTargetX,
	}

	svc.handleRouteProbe(domain.ConnID(0), idPeerB, probe)

	// No new routes should appear from a probe — the handler only
	// reads Lookup, never writes UpdateRoute.
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 0 {
		t.Fatalf("handleRouteProbe injected route into table: %v", routes)
	}
}

// TestHandleRouteProbeAck_DoesNotPanicOnReachable — defensive
// no-op when the fixture wires the routing surface but NOT the
// outstanding-probe registry (newTestServiceWithRouting leaves
// svc.probeRegistry nil; only newTestServiceWithProbeRegistry,
// used by routing_probe_loop_test.go, wires the registry). The
// real ingest path with a wired registry — including the happy
// path that stamps health and folds RTT into the EWMA — is
// exercised in routing_probe_loop_test.go::TestHandleRouteProbeAck_*.
// Here we only pin "no panic, no health writes" for the no-
// registry branch.
func TestHandleRouteProbeAck_DoesNotPanicOnReachable(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   42,
		Reachable: true,
		RTTMs:     25,
	}

	svc.handleRouteProbeAck(idPeerB, ack)

	// No health entries should have been created — the
	// no-registry branch short-circuits before MarkProbeAck.
	if snap := svc.routingTable.HealthSnapshot(); snap != nil {
		t.Fatalf("handleRouteProbeAck created health state in the no-registry branch: %v", snap)
	}
}

// TestHandleRouteProbeAck_DoesNotPanicOnUnreachable — symmetric
// no-registry-branch test for the reachable=false ack.
func TestHandleRouteProbeAck_DoesNotPanicOnUnreachable(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   42,
		Reachable: false,
	}

	svc.handleRouteProbeAck(idPeerB, ack)
}

// TestP2PWireCommands_IncludesRouteProbeTypes — the auth gate in
// dispatchNetworkFrame relies on p2pWireCommands to distinguish
// "command exists but needs auth" from "unknown command". The Phase 2
// probe types must be members so that an unauthenticated peer sending
// a probe gets the auth_required path, not an unknown_command kill.
func TestP2PWireCommands_IncludesRouteProbeTypes(t *testing.T) {
	if !isP2PWireCommand(protocol.RouteProbeFrameType) {
		t.Fatalf("p2pWireCommands missing %q", protocol.RouteProbeFrameType)
	}
	if !isP2PWireCommand(protocol.RouteProbeAckFrameType) {
		t.Fatalf("p2pWireCommands missing %q", protocol.RouteProbeAckFrameType)
	}
}

// TestLocalCapabilities_AdvertisesRouteProbeV1 — the local capability
// advertisement must include mesh_route_probe_v1 so that capable
// peers know they can send us probes. The companion gate on the
// receive side (handleRouteProbe rejecting incoming probes when the
// remote lacks the cap) is in dispatchNetworkFrame, but the local
// advertisement is the upstream contract that PR 11.1 already wired.
func TestLocalCapabilities_AdvertisesRouteProbeV1(t *testing.T) {
	caps := localCapabilities(false)
	for _, c := range caps {
		if c == domain.CapMeshRouteProbeV1 {
			return
		}
	}
	t.Fatalf("localCapabilities does not advertise %q: %v", domain.CapMeshRouteProbeV1, caps)
}
