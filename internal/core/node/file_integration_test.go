package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestIsPeerReachable_DirectSessionWithCapability verifies that a peer with
// a direct outbound session carrying file_transfer_v1 is reported reachable.
func TestIsPeerReachable_DirectSessionWithCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with direct file_transfer_v1 session should be reachable")
	}
}

// TestIsPeerReachable_DirectSessionWithoutCapability verifies that a direct
// session lacking file_transfer_v1 is NOT considered reachable for file transfer.
func TestIsPeerReachable_DirectSessionWithoutCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with direct session but no file_transfer_v1 should NOT be reachable")
	}
}

// announceRouteVia is a test helper that injects a route to targetID via
// nextHopID into the service's routing table using handleAnnounceRoutes.
func announceRouteVia(svc *Service, nextHopID domain.PeerIdentity, targetID domain.PeerIdentity, hops int) {
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: string(targetID), Origin: string(nextHopID), Hops: hops, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(nextHopID, frame)
}

// TestIsPeerReachable_RouteViaFileCapableNextHop verifies that a routed peer
// is reachable when the next-hop has file_transfer_v1 capability.
func TestIsPeerReachable_RouteViaFileCapableNextHop(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	// Relay peer-B has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	// Target X is reachable via peer-B (2 hops after +1 at receiver).
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if !svc.isPeerReachable(idTargetX) {
		t.Fatal("target reachable via file-capable next-hop should be reachable")
	}
}

// TestIsPeerReachable_RouteViaNextHopWithoutFileCapability verifies that a
// routed peer is NOT reachable when the only next-hop lacks file_transfer_v1.
// This is the core regression test for the false-positive reachability bug.
func TestIsPeerReachable_RouteViaNextHopWithoutFileCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	// Relay peer-B has relay + routing but NOT file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	// Target X is reachable via peer-B.
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("target reachable only via non-file-capable next-hop should NOT be reachable")
	}
}

// TestIsPeerReachable_MultipleRoutesOneCapable verifies that when multiple
// routes exist, the peer is reachable if at least one next-hop has file_transfer_v1.
func TestIsPeerReachable_MultipleRoutesOneCapable(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	// Peer-B: relay only, no file transfer.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	// Peer-C: has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		peerIdentity: idPeerC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	// Target X announced via both peers.
	announceRouteVia(svc, idPeerB, idTargetX, 1)
	announceRouteVia(svc, idPeerC, idTargetX, 2)

	if !svc.isPeerReachable(idTargetX) {
		t.Fatal("target should be reachable via the file-capable next-hop (peer-C)")
	}
}

// TestIsPeerReachable_NoPeerNoRoute verifies that a completely unknown peer
// is reported as unreachable.
func TestIsPeerReachable_NoPeerNoRoute(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("unknown peer with no sessions or routes should NOT be reachable")
	}
}

// TestIsPeerReachable_SelfRouteSkipped verifies that routes where next_hop ==
// local identity are not considered when evaluating reachability, even when
// the local node has file_transfer_v1 capability.
func TestIsPeerReachable_SelfRouteSkipped(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	// The local node has file_transfer_v1 — so it appears in fileCapable.
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{
		peerIdentity: domain.PeerIdentity(idNodeA),
		capabilities: []domain.Capability{domain.CapFileTransferV1},
	}

	// Inject a route to targetX with NextHop == localID directly via
	// UpdateRoute. handleAnnounceRoutes rejects own-origin routes, so
	// we use the routing table API to simulate a loop entry.
	_, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idPeerB,
		NextHop:  domain.PeerIdentity(idNodeA),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].NextHop != domain.PeerIdentity(idNodeA) {
		t.Fatalf("expected self-route NextHop=%s, got %s", idNodeA, routes[0].NextHop)
	}

	// Even though local node is file-capable, sending to ourselves is a loop.
	if svc.isPeerReachable(idTargetX) {
		t.Fatal("self-route should not count as reachable")
	}
}

// TestIsPeerReachable_InboundConnectionWithCapability verifies that a peer
// connected via an inbound connection with file_transfer_v1 is reachable.
func TestIsPeerReachable_InboundConnectionWithCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(idNodeA)

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	pc := newPeerConn(connID(1), pipeLocal, Inbound, PeerConnOpts{
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	})
	svc.inboundPeerConns[pipeLocal] = pc

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with inbound file_transfer_v1 connection should be reachable")
	}
}
