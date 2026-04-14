package node

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestIsPeerReachable_DirectSessionWithCapability verifies that a peer with
// a direct outbound session carrying file_transfer_v1 is reported reachable.
func TestIsPeerReachable_DirectSessionWithCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with direct file_transfer_v1 session should be reachable")
	}
}

// TestIsPeerReachable_DirectSessionWithoutCapability verifies that a direct
// session lacking file_transfer_v1 is NOT considered reachable for file transfer.
func TestIsPeerReachable_DirectSessionWithoutCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
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
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	// Relay peer-B has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
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
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	// Relay peer-B has relay + routing but NOT file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
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
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	// Peer-B: relay only, no file transfer.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Peer-C: has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		address:      domain.PeerAddress("addr-C"),
		peerIdentity: idPeerC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-C")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
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
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	// The local node has file_transfer_v1 — so it appears in fileCapable.
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{
		address:      domain.PeerAddress("addr-A"),
		peerIdentity: domain.PeerIdentity(idNodeA),
		capabilities: []domain.Capability{domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-A")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
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
	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	pc := netcore.New(netcore.ConnID(1), pipeLocal, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("addr-B"),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	})
	svc.conns[pipeLocal] = &connEntry{core: pc}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with inbound file_transfer_v1 connection should be reachable")
	}
}

func TestIsPeerReachable_DirectSessionStalled(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("stalled direct peer should NOT be reachable for file transfer")
	}
}

func TestIsPeerReachable_RouteViaStalledNextHop(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("route via stalled next-hop should NOT be considered reachable")
	}
}

func TestFileTransferPeerUsableAtPrefersOldestHealthyConnection(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-old")] = &peerSession{
		address:      domain.PeerAddress("addr-old"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.sessions[domain.PeerAddress("addr-new")] = &peerSession{
		address:      domain.PeerAddress("addr-new"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	svc.health[domain.PeerAddress("addr-old")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-10 * time.Minute),
		LastUsefulReceiveAt: now.Add(-10 * time.Second),
	}
	svc.health[domain.PeerAddress("addr-new")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-1 * time.Minute),
		LastUsefulReceiveAt: now.Add(-5 * time.Second),
	}

	got, ok := svc.fileTransferPeerUsableAt(idPeerB)
	if !ok {
		t.Fatal("expected peer to be usable")
	}
	want := now.Add(-10 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("connectedAt = %v, want %v", got, want)
	}
}

func TestFileTransferPeerUsableAtRejectsStalledPeer(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-stalled")] = &peerSession{
		address:      domain.PeerAddress("addr-stalled"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-stalled")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-30 * time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}

	if got, ok := svc.fileTransferPeerUsableAt(idPeerB); ok {
		t.Fatalf("stalled peer must be unusable, got connectedAt=%v", got)
	}
}

func TestFileTransferPeerUsableAtRejectsPeerWithoutHealth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	svc.sessions[domain.PeerAddress("addr-no-health")] = &peerSession{
		address:      domain.PeerAddress("addr-no-health"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	if got, ok := svc.fileTransferPeerUsableAt(idPeerB); ok {
		t.Fatalf("peer without health must be unusable, got connectedAt=%v", got)
	}
}

func TestSendFrameToIdentityFallsBackToInboundWhenOutboundBufferFull(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	outboundSendCh := make(chan protocol.Frame, 1)
	outboundSendCh <- protocol.Frame{Type: "pre-filled"}
	svc.sessions[domain.PeerAddress("addr-out")] = &peerSession{
		address:      domain.PeerAddress("addr-out"),
		peerIdentity: idPeerB,
		conn:         outLocal,
		sendCh:       outboundSendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(1), inLocal, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("addr-in"),
		Identity: idPeerB,
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	})
	defer pc.Close()
	svc.conns[inLocal] = &connEntry{core: pc}
	svc.health[domain.PeerAddress("addr-in")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	frame := protocol.Frame{Type: "ping"}
	if !svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("expected inbound fallback to succeed when outbound sendCh is full")
	}

	reader := bufio.NewReader(inRemote)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	parsed, err := protocol.ParseFrameLine(line[:len(line)-1])
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if parsed.Type != "ping" {
		t.Fatalf("forwarded frame type = %q, want ping", parsed.Type)
	}
}

func TestSendFrameToIdentityFallsBackToInboundWhenOutboundSendChannelClosed(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)
	now := time.Now().UTC()

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	outboundSendCh := make(chan protocol.Frame, 1)
	close(outboundSendCh)
	svc.sessions[domain.PeerAddress("addr-out-closed")] = &peerSession{
		address:      domain.PeerAddress("addr-out-closed"),
		peerIdentity: idPeerB,
		conn:         outLocal,
		sendCh:       outboundSendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out-closed")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(2), inLocal, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("addr-in-fallback"),
		Identity: idPeerB,
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	})
	defer pc.Close()
	svc.conns[inLocal] = &connEntry{core: pc}
	svc.health[domain.PeerAddress("addr-in-fallback")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	frame := protocol.Frame{Type: "ping"}
	if !svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("expected inbound fallback to succeed when outbound sendCh is closed")
	}

	reader := bufio.NewReader(inRemote)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	parsed, err := protocol.ParseFrameLine(line[:len(line)-1])
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if parsed.Type != "ping" {
		t.Fatalf("forwarded frame type = %q, want ping", parsed.Type)
	}
}

// TestSendFrameToIdentityRejectsOutboundBeforeActivation pins the activation
// boundary: outbound bring-up inserts into s.sessions before markPeerConnected
// seeds s.health, and during that window identity-routed frames must not be
// accepted. Regression for the pre-activation visibility leak in
// sendFrameToIdentity step 1 (outbound preferred path).
func TestSendFrameToIdentityRejectsOutboundBeforeActivation(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	// Session is installed into s.sessions but markPeerConnected has NOT
	// been called yet — health is absent. This mirrors the window between
	// peer_management.go s.sessions[address] = session and the subsequent
	// markPeerConnected(...) call.
	svc.sessions[domain.PeerAddress("addr-out-preactivation")] = &peerSession{
		address:      domain.PeerAddress("addr-out-preactivation"),
		peerIdentity: idPeerB,
		conn:         outLocal,
		sendCh:       make(chan protocol.Frame, 4),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	frame := protocol.Frame{Type: "ping"}
	if svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("sendFrameToIdentity accepted outbound session before markPeerConnected; pre-activation visibility leak")
	}
}

// TestSendFrameToIdentityRejectsInboundBeforeActivation pins the same
// activation boundary for the inbound fallback branch. registerInboundConn
// installs the NetCore before hello/auth; identity/capabilities populate
// before markPeerConnected. Identity-routed frames must not land on a
// partially-handshaken inbound NetCore.
func TestSendFrameToIdentityRejectsInboundBeforeActivation(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(idNodeA)

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(7), inLocal, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("addr-in-preactivation"),
		Identity: idPeerB,
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	})
	defer pc.Close()
	svc.conns[inLocal] = &connEntry{core: pc}
	// Intentionally no svc.health entry for addr-in-preactivation.

	frame := protocol.Frame{Type: "ping"}
	if svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("sendFrameToIdentity accepted inbound NetCore before markPeerConnected; pre-activation visibility leak")
	}
}
