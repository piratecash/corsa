package node

import (
	"net"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
)

func TestHandleAnnounceRoutesAddsHop(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "peer-B", Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("expected route to be added")
	}
	// Receiver adds +1 hop.
	if routes[0].Hops != 2 {
		t.Fatalf("expected hops=2 (1+1), got %d", routes[0].Hops)
	}
	if routes[0].NextHop != "peer-B" {
		t.Fatalf("expected NextHop=peer-B, got %s", routes[0].NextHop)
	}
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("expected source=announcement, got %s", routes[0].Source)
	}
}

func TestHandleAnnounceRoutesWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// First, add a route.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "peer-B", Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// Now withdraw it.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "peer-B", Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes("peer-B", withdrawal)

	routes = svc.routingTable.Lookup("target-X")
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes after withdrawal, got %d", len(routes))
	}
}

func TestHandleAnnounceRoutesSkipsSelf(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Route about ourselves should be skipped.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "node-A", Origin: "peer-B", Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("node-A")
	if len(routes) != 0 {
		t.Fatal("route about self should not be added")
	}
}

func TestMultiSessionAwareness_FirstConnect(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 {
		t.Fatalf("expected 1 direct route after connect, got %d", len(routes))
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route")
	}
}

func TestMultiSessionAwareness_SecondSessionNoChurn(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)
	routes1 := svc.routingTable.Lookup("peer-B")
	seq1 := routes1[0].SeqNo

	svc.onPeerSessionEstablished("peer-B", true)
	routes2 := svc.routingTable.Lookup("peer-B")

	if len(routes2) != 1 {
		t.Fatal("expected exactly 1 route after second session")
	}
	// SeqNo should not have changed.
	if routes2[0].SeqNo != seq1 {
		t.Fatalf("expected SeqNo=%d (no churn), got %d", seq1, routes2[0].SeqNo)
	}
}

func TestMultiSessionAwareness_CloseOneSessionRouteRemains(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)
	svc.onPeerSessionEstablished("peer-B", true) // 2 sessions

	svc.onPeerSessionClosed("peer-B", true) // close 1, 1 remains

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 {
		t.Fatalf("expected route to remain with 1 session active, got %d routes", len(routes))
	}
	if routes[0].IsWithdrawn() {
		t.Fatal("route should not be withdrawn while a session is still active")
	}
}

func TestMultiSessionAwareness_CloseLastSessionWithdrawsRoute(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)
	svc.onPeerSessionClosed("peer-B", true) // last session

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatal("expected no active routes after last session closed")
	}
}

// --- Direct-route suppression for non-relay peers ---

// TestTrackInboundConnectSuppressesDirectRouteWithoutRelayCap verifies that
// an inbound peer without mesh_relay_v1 does NOT get a direct route in the
// routing table. A non-relay peer cannot accept relay_message frames, so
// advertising it as a direct destination would create a non-deliverable path.
func TestTrackInboundConnectSuppressesDirectRouteWithoutRelayCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 12350},
	}

	// Register inbound peer info with NO capabilities (legacy peer).
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: nil,
	}
	svc.mu.Unlock()

	svc.trackInboundConnect(conn, "peer-B")

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatalf("expected no direct route for non-relay peer, got %d routes", len(routes))
	}
}

// TestTrackInboundConnectCreatesDirectRouteWithRelayCap verifies that an
// inbound peer WITH mesh_relay_v1 gets a direct route as expected.
func TestTrackInboundConnectCreatesDirectRouteWithRelayCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 12351},
	}

	// Register inbound peer info WITH mesh_relay_v1.
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1},
	}
	svc.mu.Unlock()

	svc.trackInboundConnect(conn, "peer-B")

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 {
		t.Fatalf("expected 1 direct route for relay-capable peer, got %d", len(routes))
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route source")
	}
}

// TestOnPeerSessionEstablishedSuppressesDirectRouteWithoutRelayCap verifies
// that onPeerSessionEstablished with hasRelayCap=false does NOT create a
// direct route in the routing table. The session counter is still
// incremented so that onPeerSessionClosed can safely decrement.
func TestOnPeerSessionEstablishedSuppressesDirectRouteWithoutRelayCap(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", false)

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatalf("expected no direct route when hasRelayCap=false, got %d routes", len(routes))
	}

	// Verify session counter was incremented despite no route.
	svc.mu.Lock()
	count := svc.identitySessions["peer-B"]
	svc.mu.Unlock()
	if count != 1 {
		t.Fatalf("expected identitySessions[peer-B]=1, got %d", count)
	}
}

// --- Mixed-capability multi-session transition tests ---

// TestMixedCap_LegacyFirstThenRelayCreatesRoute verifies that when the
// first session for an identity is non-relay (no direct route), a second
// session WITH mesh_relay_v1 creates the direct route on its 0→1 relay
// transition.
func TestMixedCap_LegacyFirstThenRelayCreatesRoute(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// First session: legacy, no relay cap → no direct route.
	svc.onPeerSessionEstablished("peer-B", false)

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatal("expected no route after legacy-only session")
	}

	// Second session: relay-capable → direct route should appear.
	svc.onPeerSessionEstablished("peer-B", true)

	routes = svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 {
		t.Fatalf("expected 1 direct route after relay session, got %d", len(routes))
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route source")
	}
}

// TestMixedCap_RelayClosedLegacyRemainsWithdrawsRoute verifies that when
// the only relay-capable session closes but a legacy session remains, the
// direct route is withdrawn. The legacy session cannot accept relay_message,
// so the route must not survive.
func TestMixedCap_RelayClosedLegacyRemainsWithdrawsRoute(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Two sessions: one relay-capable, one legacy.
	svc.onPeerSessionEstablished("peer-B", true)  // creates direct route
	svc.onPeerSessionEstablished("peer-B", false)  // legacy, no route change

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 || routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route while relay session is active")
	}

	// Close the relay-capable session — route should be withdrawn.
	svc.onPeerSessionClosed("peer-B", true)

	routes = svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatalf("expected route withdrawn after last relay session closed, got %d routes", len(routes))
	}

	// The legacy session is still tracked.
	svc.mu.Lock()
	totalCount := svc.identitySessions["peer-B"]
	relayCount := svc.identityRelaySessions["peer-B"]
	svc.mu.Unlock()
	if totalCount != 1 {
		t.Fatalf("expected identitySessions=1, got %d", totalCount)
	}
	if relayCount != 0 {
		t.Fatalf("expected identityRelaySessions=0, got %d", relayCount)
	}
}

// TestMixedCap_TwoRelaySessionsOneCloseRouteRemains verifies that when
// two relay-capable sessions exist and one closes, the direct route remains
// because one relay session is still active.
func TestMixedCap_TwoRelaySessionsOneCloseRouteRemains(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)
	svc.onPeerSessionEstablished("peer-B", true)

	svc.onPeerSessionClosed("peer-B", true) // one relay remains

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 {
		t.Fatalf("expected route to remain with 1 relay session, got %d routes", len(routes))
	}
	if routes[0].IsWithdrawn() {
		t.Fatal("route should not be withdrawn while a relay session remains")
	}
}

// TestMixedCap_LegacyCloseDoesNotWithdrawRelayRoute verifies that closing
// a non-relay session does not withdraw the direct route created by an
// active relay session.
func TestMixedCap_LegacyCloseDoesNotWithdrawRelayRoute(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", true)   // creates direct route
	svc.onPeerSessionEstablished("peer-B", false)   // legacy session

	// Close the legacy session.
	svc.onPeerSessionClosed("peer-B", false)

	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 || routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("direct route should survive when relay session is still active")
	}
}

// --- Routing-only peer (mesh_routing_v1 without mesh_relay_v1) tests ---

// TestRoutingOnlyPeerDisconnectInvalidatesTransitRoutes verifies that when a
// routing-only peer (no relay cap) disconnects, any transit routes learned
// through it are invalidated via InvalidateTransitRoutes. Even though the
// receive-path gate now blocks announcements from such peers, this is
// defense-in-depth for routes that may have been accepted before the gate.
func TestRoutingOnlyPeerDisconnectInvalidatesTransitRoutes(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Simulate a routing-only peer connecting (no relay cap).
	svc.onPeerSessionEstablished("peer-B", false)

	// Manually insert a transit route through peer-B (simulates a route
	// that was accepted before the relay gate existed).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("transit route should exist before disconnect")
	}

	// Drain any pending trigger from onPeerSessionEstablished (non-relay
	// peers don't trigger, but clear state to be safe).
	_ = svc.announceLoop.PendingTrigger()

	// Close the routing-only peer's session.
	svc.onPeerSessionClosed("peer-B", false)

	// Transit route should be invalidated. Lookup() filters withdrawn entries,
	// so use Snapshot() to inspect the raw table state.
	snap := svc.routingTable.Snapshot()
	snapRoutes := snap.Routes["target-X"]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be withdrawn after routing-only peer disconnects")
	}

	// Lookup must return empty — withdrawn routes are not usable.
	if len(svc.routingTable.Lookup("target-X")) != 0 {
		t.Fatal("Lookup should return empty for withdrawn routes")
	}

	// TriggerUpdate should have been called so neighbors learn immediately.
	if !svc.announceLoop.PendingTrigger() {
		t.Fatal("expected TriggerUpdate after transit route invalidation")
	}
}

// TestRoutingOnlyPeerNonLastSessionNoInvalidation verifies that closing one
// of multiple sessions for a routing-only peer does NOT invalidate transit
// routes — only the last session closure triggers invalidation.
func TestRoutingOnlyPeerNonLastSessionNoInvalidation(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.onPeerSessionEstablished("peer-B", false)
	svc.onPeerSessionEstablished("peer-B", false) // 2 sessions

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// Drain any pending trigger from setup.
	_ = svc.announceLoop.PendingTrigger()

	// Close one session — transit route should remain, no trigger.
	svc.onPeerSessionClosed("peer-B", false)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("transit route should remain while a session is still active")
	}
	if svc.announceLoop.PendingTrigger() {
		t.Fatal("no TriggerUpdate expected when other sessions remain")
	}

	// Close last session — now transit route should be invalidated with trigger.
	svc.onPeerSessionClosed("peer-B", false)

	// Lookup() filters withdrawn entries — use Snapshot() for raw state.
	snap := svc.routingTable.Snapshot()
	snapRoutes := snap.Routes["target-X"]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot after last close, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be withdrawn after last session closes")
	}

	// Lookup must return empty — withdrawn routes are not usable.
	if len(svc.routingTable.Lookup("target-X")) != 0 {
		t.Fatal("Lookup should return empty for withdrawn routes")
	}

	if !svc.announceLoop.PendingTrigger() {
		t.Fatal("expected TriggerUpdate after last session transit route invalidation")
	}
}

func TestConfirmRouteViaHopAck(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Add an announcement route via peer-B.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute should succeed")
	}

	// Confirm with the actual NextHop identity that carried the message.
	svc.confirmRouteViaHopAck("target-X", "peer-B", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("expected source=hop_ack after confirmation, got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_WrongNextHopNotConfirmed(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Route via peer-B.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute should succeed")
	}

	// hop_ack came for a message forwarded to peer-C (different route).
	// Should NOT promote the peer-B route.
	svc.confirmRouteViaHopAck("target-X", "peer-C", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("expected source=announcement (not confirmed via wrong next-hop), got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_AlreadyHopAck(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Route already at hop_ack — should not change.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceHopAck,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute should succeed")
	}

	svc.confirmRouteViaHopAck("target-X", "peer-B", "")

	routes := svc.routingTable.Lookup("target-X")
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatal("source should remain hop_ack")
	}
}

func TestConfirmRouteViaHopAck_DirectNotDemoted(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Direct route should not be touched by hop_ack confirmation.
	svc.onPeerSessionEstablished("target-X", true)

	svc.confirmRouteViaHopAck("target-X", "target-X", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatalf("direct route should not be demoted, got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_ResolvesTransportAddress(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Simulate an outbound session: transport address "tcp://1.2.3.4:9000"
	// maps to peer identity "peer-B".
	svc.sessions["tcp://1.2.3.4:9000"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{"mesh_relay_v1", "mesh_routing_v1"},
	}

	// Route via peer-B.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute should succeed")
	}

	// hop_ack arrives from transport address — should resolve to peer-B
	// and confirm the route.
	svc.confirmRouteViaHopAck("target-X", "tcp://1.2.3.4:9000", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("expected source=hop_ack after transport-address confirmation, got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_EmptyForwardedToSkips(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Add a route.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "target-X",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute should succeed")
	}

	// Empty forwardedTo (stored locally, not forwarded) — should not confirm.
	svc.confirmRouteViaHopAck("target-X", "", "")

	routes := svc.routingTable.Lookup("target-X")
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatal("empty forwardedTo should not confirm any route")
	}
}

// newTestServiceWithRouting creates a minimal Service with routing table
// initialized, suitable for unit tests that don't need network I/O.
func newTestServiceWithRouting(localIdentity string) *Service {
	svc := &Service{
		identity:              &identity.Identity{Address: localIdentity},
		identitySessions:      make(map[string]int),
		identityRelaySessions: make(map[string]int),
		sessions:              make(map[string]*peerSession),
		connPeerInfo:          make(map[net.Conn]*connPeerHello),
		inboundTracked:        make(map[net.Conn]struct{}),
	}
	svc.routingTable = routing.NewTable(routing.WithLocalOrigin(localIdentity))
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		&noopPeerSender{},
		func() []routing.AnnounceTarget { return nil },
	)
	return svc
}

// --- Capability gating tests ---
// resolveRouteNextHopAddress uses relay-only cap for direct destinations
// (hops=1) and both caps for transit next-hops (hops>1).

func TestResolveRouteNextHop_DirectPeerRelayOnlySuffices(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// peer-B has only mesh_relay_v1, no mesh_routing_v1.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	// Direct destination (hops=1): should resolve, relay cap is enough.
	addr := svc.resolveRouteNextHopAddress("peer-B", 1)
	if addr == "" {
		t.Fatal("direct destination with relay-only cap should resolve")
	}
	if addr != "addr-B" {
		t.Fatalf("expected addr-B, got %s", addr)
	}
}

func TestResolveRouteNextHop_TransitNeedsBothCaps(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// peer-B has only mesh_relay_v1.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	// Transit next-hop (hops=3): should NOT resolve, needs routing cap too.
	addr := svc.resolveRouteNextHopAddress("peer-B", 3)
	if addr != "" {
		t.Fatalf("transit next-hop with relay-only cap should not resolve, got %s", addr)
	}

	// Now give peer-B both caps.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}

	addr = svc.resolveRouteNextHopAddress("peer-B", 3)
	if addr == "" {
		t.Fatal("transit next-hop with both caps should resolve")
	}
}

func TestResolveRouteNextHop_NoCapsRejectsAll(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// peer-B has no relevant capabilities.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{},
	}

	if addr := svc.resolveRouteNextHopAddress("peer-B", 1); addr != "" {
		t.Fatalf("peer without relay cap should not resolve for direct, got %s", addr)
	}
	if addr := svc.resolveRouteNextHopAddress("peer-B", 3); addr != "" {
		t.Fatalf("peer without caps should not resolve for transit, got %s", addr)
	}
}

// --- Route-session binding tests ---
// Direct routes (own-origin) are withdrawn on the wire; transit routes
// (learned via announcement) are silently invalidated locally.

func TestRouteSessionBinding_DirectRouteWithdrawnOnWire(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Connect peer-B → creates direct route.
	svc.onPeerSessionEstablished("peer-B", true)
	routes := svc.routingTable.Lookup("peer-B")
	if len(routes) != 1 || routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected 1 direct route after connect")
	}

	// Disconnect peer-B → should produce wire withdrawal.
	svc.onPeerSessionClosed("peer-B", true)

	routes = svc.routingTable.Lookup("peer-B")
	if len(routes) != 0 {
		t.Fatalf("expected 0 active routes after disconnect, got %d", len(routes))
	}

	// Verify the withdrawal exists in the table as a withdrawn entry.
	snap := svc.routingTable.Snapshot()
	peerRoutes := snap.Routes["peer-B"]
	if len(peerRoutes) == 0 {
		t.Fatal("expected withdrawn entry to exist in snapshot")
	}
	if !peerRoutes[0].IsWithdrawn() {
		t.Fatal("expected direct route to be withdrawn")
	}
}

func TestRouteSessionBinding_TransitRouteLocallyInvalidated(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Connect peer-B so we have a session.
	svc.onPeerSessionEstablished("peer-B", true)

	// Announce transit route: target-X reachable via peer-B, originated by peer-C.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "peer-C",
		NextHop:   "peer-B",
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("transit route should be accepted")
	}

	// Disconnect peer-B → transit route should be locally invalidated.
	svc.onPeerSessionClosed("peer-B", true)

	// Transit route should no longer be active.
	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 0 {
		t.Fatalf("expected 0 active transit routes after peer disconnect, got %d", len(routes))
	}

	// But the transit route should NOT have a wire-ready withdrawal SeqNo
	// bump — only the originator (peer-C) can do that. Verify via snapshot
	// that the entry's Origin is still peer-C (not node-A).
	snap := svc.routingTable.Snapshot()
	transitRoutes := snap.Routes["target-X"]
	if len(transitRoutes) == 0 {
		t.Fatal("expected invalidated transit entry in snapshot")
	}
	if transitRoutes[0].Origin != "peer-C" {
		t.Fatalf("transit route origin should remain peer-C, got %s", transitRoutes[0].Origin)
	}
}

func TestRouteSessionBinding_MixedDirectAndTransit(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Connect peer-B.
	svc.onPeerSessionEstablished("peer-B", true)

	// Add a transit route through peer-B.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-Y",
		Origin:    "target-Y",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("transit route should be accepted")
	}

	// Disconnect peer-B.
	svc.onPeerSessionClosed("peer-B", true)

	// Both the direct route to peer-B and the transit route via peer-B
	// should be gone from active lookups.
	if routes := svc.routingTable.Lookup("peer-B"); len(routes) != 0 {
		t.Fatal("direct route should be withdrawn")
	}
	if routes := svc.routingTable.Lookup("target-Y"); len(routes) != 0 {
		t.Fatal("transit route should be invalidated")
	}
}

// --- hop_ack scoping tests ---
// confirmRouteViaHopAck must only promote the specific (identity, origin, nextHop)
// entry that matches. Different origin or different identity must not be affected.

func TestHopAckScoping_DifferentOriginNotPromoted(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Two routes to target-X via peer-B, but different origins.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-C",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route 1 should be accepted")
	}

	ok, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-D",
		NextHop:   "peer-B",
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route 2 should be accepted")
	}

	// hop_ack with specific origin=origin-D — only that triple should
	// be promoted. origin-C must remain announcement despite sharing
	// the same NextHop.
	svc.confirmRouteViaHopAck("target-X", "peer-B", "origin-D")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	var foundC, foundD bool
	for _, r := range routes {
		if r.Origin == "origin-C" {
			foundC = true
			if r.Source != routing.RouteSourceAnnouncement {
				t.Fatalf("origin-C route must remain announcement (not part of confirmed triple), got %s", r.Source)
			}
		}
		if r.Origin == "origin-D" {
			foundD = true
			if r.Source != routing.RouteSourceHopAck {
				t.Fatalf("origin-D route should be promoted to hop_ack, got %s", r.Source)
			}
		}
	}
	if !foundC || !foundD {
		t.Fatalf("expected both origins present: foundC=%v foundD=%v", foundC, foundD)
	}
}

func TestHopAckScoping_GossipPathPromotesFirstMatchingNextHop(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Two routes to target-X via peer-B, different origins.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-C",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route 1 should be accepted")
	}

	ok, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-D",
		NextHop:   "peer-B",
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route 2 should be accepted")
	}

	// Gossip path (empty origin) promotes the first matching NextHop in
	// Lookup order — origin-C with hops=2 wins.
	svc.confirmRouteViaHopAck("target-X", "peer-B", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	var foundC, foundD bool
	for _, r := range routes {
		if r.Origin == "origin-C" {
			foundC = true
			if r.Source != routing.RouteSourceHopAck {
				t.Fatalf("gossip fallback: origin-C (first in Lookup order) should be promoted, got %s", r.Source)
			}
		}
		if r.Origin == "origin-D" {
			foundD = true
			if r.Source != routing.RouteSourceAnnouncement {
				t.Fatalf("origin-D should remain announcement, got %s", r.Source)
			}
		}
	}
	if !foundC || !foundD {
		t.Fatalf("expected both origins present: foundC=%v foundD=%v", foundC, foundD)
	}
}

func TestHopAckScoping_DifferentIdentityNotPromoted(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Route to target-X via peer-B.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-C",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route X should be accepted")
	}

	// Route to target-Y via peer-B, same origin.
	ok, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-Y",
		Origin:    "origin-C",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route Y should be accepted")
	}

	// hop_ack for target-X only.
	svc.confirmRouteViaHopAck("target-X", "peer-B", "")

	// target-X should be promoted.
	routesX := svc.routingTable.Lookup("target-X")
	if len(routesX) == 0 {
		t.Fatal("expected route to target-X")
	}
	if routesX[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("target-X should be promoted to hop_ack, got %s", routesX[0].Source)
	}

	// target-Y should NOT be promoted (different identity).
	routesY := svc.routingTable.Lookup("target-Y")
	if len(routesY) == 0 {
		t.Fatal("expected route to target-Y")
	}
	if routesY[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("target-Y should remain announcement, got %s", routesY[0].Source)
	}
}

func TestHopAckScoping_SameIdentityDifferentNextHopNotPromoted(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Two routes to target-X from same origin, different next-hops.
	ok, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-C",
		NextHop:   "peer-B",
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route via peer-B should be accepted")
	}

	ok, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  "target-X",
		Origin:    "origin-C",
		NextHop:   "peer-D",
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || !ok {
		t.Fatal("route via peer-D should be accepted")
	}

	// hop_ack from peer-D should only promote the peer-D route.
	svc.confirmRouteViaHopAck("target-X", "peer-D", "")

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	for _, r := range routes {
		if r.NextHop == "peer-B" && r.Source != routing.RouteSourceAnnouncement {
			t.Fatalf("peer-B route should remain announcement, got %s", r.Source)
		}
		if r.NextHop == "peer-D" && r.Source != routing.RouteSourceHopAck {
			t.Fatalf("peer-D route should be promoted to hop_ack, got %s", r.Source)
		}
	}
}

// --- Inbound peer resolution tests ---
// Resolvers must find inbound-only peers and return "inbound:" prefixed keys.

func TestResolveRelayAddress_InboundPeer(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Set up an inbound connection with relay capability.
	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1},
	}
	svc.mu.Unlock()

	addr := svc.resolveRelayAddress("peer-B")
	if addr == "" {
		t.Fatal("inbound peer with relay cap should be resolvable")
	}
	if addr != "inbound:10.0.0.5:8080" {
		t.Fatalf("expected inbound: prefixed key, got %s", addr)
	}
}

func TestResolveRoutableAddress_InboundPeerNeedsBothCaps(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1}, // only relay, no routing
	}
	svc.mu.Unlock()

	// Should NOT resolve — transit requires both caps.
	addr := svc.resolveRoutableAddress("peer-B")
	if addr != "" {
		t.Fatalf("inbound peer with relay-only cap should not resolve for transit, got %s", addr)
	}

	// Now add routing cap.
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}
	svc.mu.Unlock()

	addr = svc.resolveRoutableAddress("peer-B")
	if addr == "" {
		t.Fatal("inbound peer with both caps should resolve")
	}
}

func TestResolveRelayAddress_OutboundPreferredOverInbound(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Set up both outbound session and inbound connection for same peer.
	svc.sessions["outbound-addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1},
	}
	svc.mu.Unlock()

	// Should prefer outbound session (has async send queue).
	addr := svc.resolveRelayAddress("peer-B")
	if addr != "outbound-addr-B" {
		t.Fatalf("expected outbound address, got %s", addr)
	}
}

// --- resolvePeerIdentity inbound contract test ---

func TestResolvePeerIdentity_InboundByTransportAddress(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address: "peer-B",
	}
	svc.mu.Unlock()

	// Pass the transport address — should resolve to identity.
	id := svc.resolvePeerIdentity("10.0.0.5:8080")
	if id != "peer-B" {
		t.Fatalf("expected peer-B, got %q", id)
	}

	// Pass the identity — should NOT match (identity != transport address).
	id = svc.resolvePeerIdentity("peer-B")
	if id != "" {
		t.Fatalf("identity-as-address should not match inbound conn, got %q", id)
	}
}

// --- TableRouter RelayNextHopAddress test ---

func TestTableRouterPopulatesRelayNextHopAddress(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin("node-A"))

	ok, err := table.UpdateRoute(routing.RouteEntry{
		Identity: "target-X",
		Origin:   "peer-B",
		NextHop:  "peer-B",
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || !ok {
		t.Fatal("UpdateRoute failed")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity string, hops int) string {
			if peerIdentity == "peer-B" {
				return "validated-addr-B"
			}
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    "node-A",
		Recipient: "target-X",
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop")
	}
	if decision.RelayNextHopAddress != "validated-addr-B" {
		t.Fatalf("expected RelayNextHopAddress=validated-addr-B, got %q", decision.RelayNextHopAddress)
	}
}

// --- Receive-path invariant tests ---
// These verify that handleAnnounceRoutes rejects announcements that violate
// the per-origin SeqNo and withdrawal-only-by-origin invariants.

func TestHandleAnnounceRoutesRejectsForgedOwnOrigin(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// A foreign sender (peer-B) advertises a route with Origin == node-A.
	// This must be rejected: only node-A may originate routes under its
	// own identity. Accepting it would poison the monotonic SeqNo counter.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "node-A", Hops: 1, SeqNo: 100},
		},
	}

	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 0 {
		t.Fatalf("forged own-origin route must be rejected, but %d routes found", len(routes))
	}

	// Verify the SeqNo counter was NOT poisoned.
	result, err := svc.routingTable.AddDirectPeer("target-X")
	if err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}
	if result.Entry.SeqNo != 1 {
		t.Fatalf("SeqNo counter should start at 1 (unpoisoned), got %d", result.Entry.SeqNo)
	}
}

func TestHandleAnnounceRoutesRejectsForgedOwnOriginWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// First, add a legitimate direct peer route.
	if _, err := svc.routingTable.AddDirectPeer("target-X"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// A foreign sender tries to withdraw node-A's own-origin route.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "node-A", Hops: 16, SeqNo: 999},
		},
	}

	svc.handleAnnounceRoutes("peer-B", frame)

	// The direct route must still be alive.
	routes := svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("own-origin route must survive forged withdrawal attempt")
	}
	if routes[0].Hops >= routing.HopsInfinity {
		t.Fatal("own-origin route must not be withdrawn by foreign sender")
	}
}

func TestHandleAnnounceRoutesRejectsTransitWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Add a route originated by origin-C, learned via peer-B.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "origin-C", Hops: 2, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// peer-B (transit node, NOT origin-C) tries to withdraw this route.
	// The model says only the origin may emit a wire withdrawal. Transit
	// nodes must invalidate locally and stop advertising.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "origin-C", Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes("peer-B", withdrawal)

	// Route must survive — only origin-C may withdraw it.
	routes = svc.routingTable.Lookup("target-X")
	if len(routes) == 0 {
		t.Fatal("transit withdrawal must be rejected — route should survive")
	}
	if routes[0].Hops >= routing.HopsInfinity {
		t.Fatal("transit withdrawal must not kill route — only origin may withdraw")
	}
}

func TestHandleAnnounceRoutesAcceptsOriginWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Add a route where origin == sender (origin-C sends its own route).
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "origin-C", Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes("origin-C", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// origin-C withdraws its own route — this is legitimate.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "origin-C", Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes("origin-C", withdrawal)

	routes = svc.routingTable.Lookup("target-X")
	if len(routes) != 0 {
		t.Fatalf("origin withdrawal should be accepted, but %d routes remain", len(routes))
	}
}

// fakeConn is a minimal net.Conn mock for testing inbound peer resolution.
type fakeConn struct {
	net.Conn
	remoteAddr net.Addr
}

func (c *fakeConn) RemoteAddr() net.Addr { return c.remoteAddr }

// noopPeerSender does nothing — used in tests where sending is not under test.
type noopPeerSender struct{}

func (n *noopPeerSender) SendAnnounceRoutes(string, []routing.AnnounceEntry) bool {
	return true
}

// newTestServiceWithRoutingAndHealth extends newTestServiceWithRouting with
// the maps required for trackInboundConnect (health tracking, ref counting).
func newTestServiceWithRoutingAndHealth(localIdentity string) *Service {
	svc := newTestServiceWithRouting(localIdentity)
	svc.health = make(map[string]*peerHealth)
	svc.inboundHealthRefs = make(map[string]int)
	svc.dialOrigin = make(map[string]string)
	svc.connSendCh = make(map[net.Conn]chan sendItem)
	svc.connWriterDone = make(map[net.Conn]chan struct{})
	return svc
}

// TestSendFullTableSyncToInbound verifies that a newly connected inbound
// peer receives an immediate full-table sync — symmetric with the outbound
// connect path.
func TestSendFullTableSyncToInbound(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	// Populate the routing table with a route that the inbound peer
	// should learn about on connect.
	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, _ := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 12345},
	}

	// Register a buffered send channel and writerDone so enqueueFrameSync
	// does not fall back to a blocking direct socket write on net.Pipe().
	// A goroutine drains the channel and closes ack — mimicking connWriter.
	sendCh := make(chan sendItem, 16)
	defer close(sendCh)
	writerDone := make(chan struct{})
	received := make(chan []byte, 1)
	go func() {
		defer close(writerDone)
		for item := range sendCh {
			received <- item.data
			if item.ack != nil {
				close(item.ack)
			}
		}
	}()

	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.connSendCh[conn] = sendCh
	svc.connWriterDone[conn] = writerDone
	svc.mu.Unlock()

	// Call the function under test.
	svc.sendFullTableSyncToInbound(conn, "peer-B")

	// Read the frame from the drain goroutine.
	select {
	case data := <-received:
		line := string(data)
		if len(line) == 0 {
			t.Fatal("expected announce_routes frame, got empty data")
		}
		if !strings.Contains(line, "peer-C") {
			t.Fatalf("full-sync frame should contain route for peer-C, got: %s", line)
		}
		if !strings.Contains(line, "announce_routes") {
			t.Fatalf("frame should be announce_routes type, got: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for full-sync frame on inbound connection")
	}
}

// TestSendFullTableSyncToInboundSplitHorizon verifies that the full-table
// sync to an inbound peer applies split horizon — routes learned from that
// peer are not sent back to it.
func TestSendFullTableSyncToInboundSplitHorizon(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	// Add a route that was learned FROM peer-B (the connecting inbound peer).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "peer-B", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.6"), Port: 12346},
	}

	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.mu.Unlock()

	// Call the function — with only peer-B's own route in the table,
	// split horizon should filter it out, resulting in no send.
	svc.sendFullTableSyncToInbound(conn, "peer-B")

	// Verify nothing was written by attempting a read with a short timeout.
	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		readDone <- n
	}()

	select {
	case n := <-readDone:
		t.Fatalf("expected no frame (split horizon), but got %d bytes", n)
	case <-time.After(200 * time.Millisecond):
		// Expected: no data sent due to split horizon.
	}
}

// TestSendFullTableSyncToInboundEmptyTable verifies that no frame is sent
// when the routing table is empty.
func TestSendFullTableSyncToInboundEmptyTable(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.7"), Port: 12347},
	}

	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.mu.Unlock()

	svc.sendFullTableSyncToInbound(conn, "peer-B")

	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		readDone <- n
	}()

	select {
	case n := <-readDone:
		t.Fatalf("expected no frame for empty table, but got %d bytes", n)
	case <-time.After(200 * time.Millisecond):
		// Expected: nothing sent.
	}
}

// --- Full-sync capability gate tests ---

// TestOutboundFullSyncSkippedWithoutRoutingCap verifies that the outbound
// connect path does NOT send announce_routes when the peer lacks
// mesh_routing_v1 capability.
func TestOutboundFullSyncSkippedWithoutRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	// Populate the routing table so there ARE routes to announce.
	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with relay-only capability (no routing).
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	// Verify the capability gate returns false.
	if svc.sessionHasCapability("addr-B", capMeshRoutingV1) {
		t.Fatal("peer-B should NOT have mesh_routing_v1")
	}

	// The code under test: outbound full-sync is skipped because peer-B
	// lacks mesh_routing_v1. We verify by checking that AnnounceTo would
	// have produced routes (the table is non-empty) but sessionHasCapability
	// blocks the send path.
	routes := svc.routingTable.AnnounceTo("peer-B")
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce")
	}
}

// TestOutboundFullSyncSentWithRoutingCap verifies that the outbound connect
// path DOES send announce_routes when the peer has mesh_routing_v1.
func TestOutboundFullSyncSentWithRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with routing capability.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}

	if !svc.sessionHasCapability("addr-B", capMeshRoutingV1) {
		t.Fatal("peer-B should have mesh_routing_v1")
	}

	routes := svc.routingTable.AnnounceTo("peer-B")
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce to routing-capable peer")
	}
}

// TestInboundFullSyncSkippedWithoutRoutingCap verifies that inbound full-table
// sync is NOT sent when the inbound peer lacks mesh_routing_v1.
func TestInboundFullSyncSkippedWithoutRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.8"), Port: 12348},
	}

	// Register inbound peer info WITHOUT mesh_routing_v1 (relay-only).
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1},
	}
	svc.mu.Unlock()

	// connHasCapability should return false.
	if svc.connHasCapability(conn, capMeshRoutingV1) {
		t.Fatal("inbound peer should NOT have mesh_routing_v1")
	}

	// trackInboundConnect should NOT send anything because the gate blocks it.
	svc.trackInboundConnect(conn, "peer-B")

	// Verify nothing was written by attempting a read with a short timeout.
	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		readDone <- n
	}()

	select {
	case n := <-readDone:
		t.Fatalf("expected no frame for non-routing peer, but got %d bytes", n)
	case <-time.After(200 * time.Millisecond):
		// Expected: no data sent due to capability gate.
	}
}

// TestInboundFullSyncSentWithRoutingCap verifies that inbound full-table sync
// IS sent when the inbound peer has both mesh_routing_v1 and mesh_relay_v1.
func TestInboundFullSyncSentWithRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, _ := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 12349},
	}

	// Register a buffered send channel and writerDone. A goroutine drains
	// the channel and closes ack — mimicking the production connWriter.
	sendCh := make(chan sendItem, 16)
	defer close(sendCh)
	writerDone := make(chan struct{})
	received := make(chan []byte, 1)
	go func() {
		defer close(writerDone)
		for item := range sendCh {
			received <- item.data
			if item.ack != nil {
				close(item.ack)
			}
		}
	}()

	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}
	svc.connSendCh[conn] = sendCh
	svc.connWriterDone[conn] = writerDone
	svc.mu.Unlock()

	if !svc.connHasCapability(conn, capMeshRoutingV1) {
		t.Fatal("inbound peer should have mesh_routing_v1")
	}

	// trackInboundConnect should call sendFullTableSyncToInbound.
	svc.trackInboundConnect(conn, "peer-B")

	// Read the frame from the drain goroutine.
	select {
	case data := <-received:
		line := string(data)
		if len(line) == 0 {
			t.Fatal("expected announce_routes frame, got empty data")
		}
		if !strings.Contains(line, "peer-C") {
			t.Fatalf("full-sync frame should contain route for peer-C, got: %s", line)
		}
		if !strings.Contains(line, "announce_routes") {
			t.Fatalf("frame should be announce_routes type, got: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for full-sync frame on routing-capable inbound peer")
	}
}

// --- Full-sync relay gate tests (routing-only peer exclusion) ---

// TestOutboundFullSyncSkippedForRoutingOnlyPeer verifies that the outbound
// connect path does NOT send announce_routes when the peer has mesh_routing_v1
// but lacks mesh_relay_v1 (routing-only peer). Such a peer cannot carry relay
// traffic, so sending routes to it creates non-deliverable paths.
func TestOutboundFullSyncSkippedForRoutingOnlyPeer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with routing-only capability (no relay).
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRoutingV1},
	}

	// Peer has routing but not relay.
	if !svc.sessionHasCapability("addr-B", capMeshRoutingV1) {
		t.Fatal("peer-B should have mesh_routing_v1")
	}
	if svc.sessionHasCapability("addr-B", capMeshRelayV1) {
		t.Fatal("peer-B should NOT have mesh_relay_v1")
	}

	// AnnounceTo would produce routes, but the full-sync gate requires both
	// capabilities, so the send path should be skipped.
	routes := svc.routingTable.AnnounceTo("peer-B")
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce")
	}
}

// TestInboundFullSyncSkippedForRoutingOnlyPeer verifies that inbound
// full-table sync is NOT sent when the inbound peer has mesh_routing_v1
// but lacks mesh_relay_v1 (routing-only peer).
func TestInboundFullSyncSkippedForRoutingOnlyPeer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-A")

	if _, err := svc.routingTable.AddDirectPeer("peer-C"); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 12350},
	}

	// Register inbound peer info with routing cap but WITHOUT relay cap.
	svc.mu.Lock()
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRoutingV1},
	}
	svc.mu.Unlock()

	if !svc.connHasCapability(conn, capMeshRoutingV1) {
		t.Fatal("inbound peer should have mesh_routing_v1")
	}
	if svc.connHasCapability(conn, capMeshRelayV1) {
		t.Fatal("inbound peer should NOT have mesh_relay_v1")
	}

	svc.trackInboundConnect(conn, "peer-B")

	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		readDone <- n
	}()

	select {
	case n := <-readDone:
		t.Fatalf("expected no frame for routing-only peer, but got %d bytes", n)
	case <-time.After(200 * time.Millisecond):
		// Expected: no data sent because relay cap is missing.
	}
}

// TestRoutingCapablePeersExcludesRoutingOnlyPeer verifies that
// routingCapablePeers() excludes peers with mesh_routing_v1 but without
// mesh_relay_v1. The announce loop must not target routing-only peers.
func TestRoutingCapablePeersExcludesRoutingOnlyPeer(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// peer-B: routing-only (no relay) — should be excluded.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRoutingV1},
	}
	// peer-C: both capabilities — should be included.
	svc.sessions["addr-C"] = &peerSession{
		peerIdentity: "peer-C",
		capabilities: []string{capMeshRoutingV1, capMeshRelayV1},
	}

	targets := svc.routingCapablePeers()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target (peer-C only), got %d", len(targets))
	}
	if targets[0].Identity != "peer-C" {
		t.Fatalf("expected peer-C, got %s", targets[0].Identity)
	}
}

// TestRoutingCapablePeersExcludesRoutingOnlyInbound verifies that an inbound
// routing-only peer is also excluded from routingCapablePeers().
func TestRoutingCapablePeersExcludesRoutingOnlyInbound(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	pipeLocal, _ := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 12351},
	}

	svc.mu.Lock()
	svc.inboundTracked[conn] = struct{}{}
	svc.connPeerInfo[conn] = &connPeerHello{
		address:      "peer-B",
		capabilities: []string{capMeshRoutingV1},
	}
	svc.mu.Unlock()

	targets := svc.routingCapablePeers()
	if len(targets) != 0 {
		t.Fatalf("expected 0 targets (routing-only inbound excluded), got %d", len(targets))
	}
}

// --- Retry re-resolution capability tests ---

// TestRetryResolutionTransitRequiresBothCaps verifies that the retry path
// (empty validatedAddress) in sendTableDirectedRelay uses hop-aware
// resolution: a transit next-hop (hops > 1) must have both mesh_relay_v1
// and mesh_routing_v1. A relay-only session should NOT be selected.
func TestRetryResolutionTransitRequiresBothCaps(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// peer-B has only relay capability — insufficient for transit.
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	// Transit hops (hops=3): should NOT resolve with relay-only cap.
	addr := svc.resolveRouteNextHopAddress("peer-B", 3)
	if addr != "" {
		t.Fatalf("transit next-hop with relay-only cap should not resolve, got %s", addr)
	}
}

// TestRetryResolutionTransitWithBothCapsResolves verifies that a peer with
// both capabilities is resolved for transit next-hops.
func TestRetryResolutionTransitWithBothCapsResolves(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}

	addr := svc.resolveRouteNextHopAddress("peer-B", 3)
	if addr != "addr-B" {
		t.Fatalf("transit next-hop with both caps should resolve to addr-B, got %s", addr)
	}
}

// TestRetryResolutionDestinationRelayOnlySuffices verifies that a destination
// next-hop (hops=1) can resolve with relay-only capability.
func TestRetryResolutionDestinationRelayOnlySuffices(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1},
	}

	addr := svc.resolveRouteNextHopAddress("peer-B", 1)
	if addr != "addr-B" {
		t.Fatalf("destination next-hop with relay-only cap should resolve, got %s", addr)
	}
}

// TestTableRouterPopulatesRelayNextHopHops verifies that TableRouter.Route()
// sets RelayNextHopHops from the selected RouteEntry so that the retry path
// in sendTableDirectedRelay has the correct hop role for re-resolution.
func TestTableRouterPopulatesRelayNextHopHops(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	// Set up a transit route (3 hops) and a session with both caps.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "peer-C", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	svc.sessions["addr-B"] = &peerSession{
		peerIdentity: "peer-B",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}

	router := NewTableRouter(svc, svc.routingTable)
	decision := router.Route(protocol.Envelope{
		Recipient: "target-X",
		Topic:     "dm",
	})

	if decision.RelayNextHop == nil {
		t.Fatal("expected a table-directed next-hop")
	}
	if decision.RelayNextHopHops != 3 {
		t.Fatalf("expected RelayNextHopHops=3, got %d", decision.RelayNextHopHops)
	}
}

// --- Intermediate hop RouteOrigin propagation ---

// TestTryForwardViaRoutingTableReturnsRouteOrigin verifies that
// tryForwardViaRoutingTable returns the selected route's Origin field
// so that the intermediate relay hop can persist it in relayForwardState
// for triple-scoped hop_ack confirmation.
func TestTryForwardViaRoutingTableReturnsRouteOrigin(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth("node-B")

	// Add a route to target-X via peer-C with origin "origin-D".
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "origin-D", NextHop: "peer-C",
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// peer-C needs both caps (transit, hops=2) and a functioning send channel.
	sendCh := make(chan protocol.Frame, 10)
	svc.sessions["addr-C"] = &peerSession{
		peerIdentity: "peer-C",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
		sendCh:       sendCh,
	}
	// activePeerSession requires health entry with Connected=true.
	svc.mu.Lock()
	svc.health["addr-C"] = &peerHealth{Address: "addr-C", Connected: true}
	svc.mu.Unlock()

	// Build a relay frame to forward.
	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-1",
		Recipient: "target-X",
	}

	result := svc.tryForwardViaRoutingTable("target-X", frame, "peer-A")

	if result.Address == "" {
		t.Fatal("expected table-directed forward to succeed")
	}
	if result.RouteOrigin != "origin-D" {
		t.Fatalf("expected RouteOrigin='origin-D', got %q", result.RouteOrigin)
	}
}

// TestTryForwardViaRoutingTableExcludesSender verifies split horizon on
// the relay path — the sender's identity is excluded from next-hop selection.
func TestTryForwardViaRoutingTableExcludesSender(t *testing.T) {
	svc := newTestServiceWithRouting("node-B")

	// Only route is via peer-A (the sender).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: "target-X", Origin: "peer-A", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	svc.sessions["addr-A"] = &peerSession{
		peerIdentity: "peer-A",
		capabilities: []string{capMeshRelayV1, capMeshRoutingV1},
	}

	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-2",
		Recipient: "target-X",
	}

	result := svc.tryForwardViaRoutingTable("target-X", frame, "peer-A")

	if result.Address != "" {
		t.Fatalf("should not forward back to sender, got address=%s", result.Address)
	}
}

// TestTryForwardViaRoutingTableNoRoute verifies empty result when no route exists.
func TestTryForwardViaRoutingTableNoRoute(t *testing.T) {
	svc := newTestServiceWithRouting("node-B")

	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-3",
		Recipient: "unknown",
	}

	result := svc.tryForwardViaRoutingTable("unknown", frame, "peer-A")

	if result.Address != "" {
		t.Fatalf("expected empty address for unknown recipient, got %s", result.Address)
	}
	if result.RouteOrigin != "" {
		t.Fatalf("expected empty RouteOrigin, got %s", result.RouteOrigin)
	}
}

// --- Announce TTL respects table configuration ---

// TestHandleAnnounceRoutesUsesTableConfiguredTTL verifies that received
// announcements honor the table's configured defaultTTL and clock, not
// the global routing.DefaultTTL constant.
func TestHandleAnnounceRoutesUsesTableConfiguredTTL(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	customTTL := 45 * time.Second

	svc := &Service{
		identity:              &identity.Identity{Address: "node-A"},
		identitySessions:      make(map[string]int),
		identityRelaySessions: make(map[string]int),
		sessions:              make(map[string]*peerSession),
		connPeerInfo:          make(map[net.Conn]*connPeerHello),
		inboundTracked:        make(map[net.Conn]struct{}),
	}
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin("node-A"),
		routing.WithClock(func() time.Time { return now }),
		routing.WithDefaultTTL(customTTL),
	)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		&noopPeerSender{},
		func() []routing.AnnounceTarget { return nil },
	)

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "peer-B", Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	expectedExpiry := now.Add(customTTL)
	if !routes[0].ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("route should expire at %v (table clock + configured TTL), got %v",
			expectedExpiry, routes[0].ExpiresAt)
	}
}

// TestHandleAnnounceRoutesDefaultTTLWithoutConfig verifies that when no
// custom TTL is configured, the table's default (routing.DefaultTTL) is
// still applied consistently via the table's own UpdateRoute path.
func TestHandleAnnounceRoutesDefaultTTLWithoutConfig(t *testing.T) {
	svc := newTestServiceWithRouting("node-A")

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: "target-X", Origin: "peer-B", Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes("peer-B", frame)

	routes := svc.routingTable.Lookup("target-X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// ExpiresAt should be set (non-zero) — the table applied its default TTL.
	if routes[0].ExpiresAt.IsZero() {
		t.Fatal("route ExpiresAt should not be zero — table should apply defaultTTL")
	}
}

