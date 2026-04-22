package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	routingmocks "github.com/piratecash/corsa/internal/core/routing/mocks"
	"github.com/piratecash/corsa/internal/core/transport"
)

// Valid 40-char hex identity constants for tests.
const (
	idNodeA   = "aa00000000000000000000000000000000000001"
	idNodeB   = "aa00000000000000000000000000000000000002"
	idPeerA   = "bb00000000000000000000000000000000000001"
	idPeerB   = "bb00000000000000000000000000000000000002"
	idPeerC   = "bb00000000000000000000000000000000000003"
	idPeerD   = "bb00000000000000000000000000000000000004"
	idOriginC = "cc00000000000000000000000000000000000003"
	idOriginD = "cc00000000000000000000000000000000000004"
	idTargetX = "dd00000000000000000000000000000000000001"
	idTargetY = "dd00000000000000000000000000000000000002"
)

func TestHandleAnnounceRoutesAddsHop(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to be added")
	}
	// Receiver adds +1 hop.
	if routes[0].Hops != 2 {
		t.Fatalf("expected hops=2 (1+1), got %d", routes[0].Hops)
	}
	if routes[0].NextHop != idPeerB {
		t.Fatalf("expected NextHop=peer-B, got %s", routes[0].NextHop)
	}
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("expected source=announcement, got %s", routes[0].Source)
	}
}

func TestHandleAnnounceRoutesWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// First, add a route.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// Now withdraw it.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, withdrawal)

	routes = svc.routingTable.Lookup(idTargetX)
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes after withdrawal, got %d", len(routes))
	}
}

func TestHandleAnnounceRoutesSkipsSelf(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Route about ourselves should be skipped.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idNodeA, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idNodeA)
	// Lookup returns the synthetic local self-route (RouteSourceLocal) but
	// the announced route from peerB must not be stored.
	for _, r := range routes {
		if r.Source != routing.RouteSourceLocal {
			t.Fatalf("only local self-route expected, got source=%s origin=%s", r.Source, r.Origin)
		}
	}
}

func TestMultiSessionAwareness_FirstConnect(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 1 {
		t.Fatalf("expected 1 direct route after connect, got %d", len(routes))
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route")
	}
}

func TestMultiSessionAwareness_SecondSessionNoChurn(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)
	routes1 := svc.routingTable.Lookup(idPeerB)
	seq1 := routes1[0].SeqNo

	svc.onPeerSessionEstablished(idPeerB, true)
	routes2 := svc.routingTable.Lookup(idPeerB)

	if len(routes2) != 1 {
		t.Fatal("expected exactly 1 route after second session")
	}
	// SeqNo should not have changed.
	if routes2[0].SeqNo != seq1 {
		t.Fatalf("expected SeqNo=%d (no churn), got %d", seq1, routes2[0].SeqNo)
	}
}

func TestMultiSessionAwareness_CloseOneSessionRouteRemains(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)
	svc.onPeerSessionEstablished(idPeerB, true) // 2 sessions

	svc.onPeerSessionClosed(idPeerB, true) // close 1, 1 remains

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 1 {
		t.Fatalf("expected route to remain with 1 session active, got %d routes", len(routes))
	}
	if routes[0].IsWithdrawn() {
		t.Fatal("route should not be withdrawn while a session is still active")
	}
}

func TestMultiSessionAwareness_CloseLastSessionWithdrawsRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)
	svc.onPeerSessionClosed(idPeerB, true) // last session

	routes := svc.routingTable.Lookup(idPeerB)
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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 12350},
	}

	// Register inbound peer info with NO capabilities (legacy peer).
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	connID, _ := svc.connIDFor(conn)
	svc.trackInboundConnect(connID, idPeerB, idPeerB)

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 0 {
		t.Fatalf("expected no direct route for non-relay peer, got %d routes", len(routes))
	}
}

// TestTrackInboundConnectCreatesDirectRouteWithRelayCap verifies that an
// inbound peer WITH mesh_relay_v1 gets a direct route as expected.
func TestTrackInboundConnectCreatesDirectRouteWithRelayCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 12351},
	}

	// Register inbound peer info WITH mesh_relay_v1.
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	connID, _ := svc.connIDFor(conn)
	svc.trackInboundConnect(connID, idPeerB, idPeerB)

	routes := svc.routingTable.Lookup(idPeerB)
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
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, false)

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 0 {
		t.Fatalf("expected no direct route when hasRelayCap=false, got %d routes", len(routes))
	}

	// Verify session counter was incremented despite no route.
	svc.peerMu.Lock()
	count := svc.identitySessions[idPeerB]
	svc.peerMu.Unlock()
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
	svc := newTestServiceWithRouting(t, idNodeA)

	// First session: legacy, no relay cap → no direct route.
	svc.onPeerSessionEstablished(idPeerB, false)

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 0 {
		t.Fatal("expected no route after legacy-only session")
	}

	// Second session: relay-capable → direct route should appear.
	svc.onPeerSessionEstablished(idPeerB, true)

	routes = svc.routingTable.Lookup(idPeerB)
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
	svc := newTestServiceWithRouting(t, idNodeA)

	// Two sessions: one relay-capable, one legacy.
	svc.onPeerSessionEstablished(idPeerB, true)  // creates direct route
	svc.onPeerSessionEstablished(idPeerB, false) // legacy, no route change

	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 1 || routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected direct route while relay session is active")
	}

	// Close the relay-capable session — route should be withdrawn.
	svc.onPeerSessionClosed(idPeerB, true)

	routes = svc.routingTable.Lookup(idPeerB)
	if len(routes) != 0 {
		t.Fatalf("expected route withdrawn after last relay session closed, got %d routes", len(routes))
	}

	// The legacy session is still tracked.
	svc.peerMu.Lock()
	totalCount := svc.identitySessions[idPeerB]
	relayCount := svc.identityRelaySessions[idPeerB]
	svc.peerMu.Unlock()
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
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)
	svc.onPeerSessionEstablished(idPeerB, true)

	svc.onPeerSessionClosed(idPeerB, true) // one relay remains

	routes := svc.routingTable.Lookup(idPeerB)
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
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, true)  // creates direct route
	svc.onPeerSessionEstablished(idPeerB, false) // legacy session

	// Close the legacy session.
	svc.onPeerSessionClosed(idPeerB, false)

	routes := svc.routingTable.Lookup(idPeerB)
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
	svc := newTestServiceWithRouting(t, idNodeA)

	// Simulate a routing-only peer connecting (no relay cap).
	svc.onPeerSessionEstablished(idPeerB, false)

	// Manually insert a transit route through peer-B (simulates a route
	// that was accepted before the relay gate existed).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("transit route should exist before disconnect")
	}

	// Drain any pending trigger from onPeerSessionEstablished (non-relay
	// peers don't trigger, but clear state to be safe).
	_ = svc.announceLoop.PendingTrigger()

	// Close the routing-only peer's session.
	svc.onPeerSessionClosed(idPeerB, false)

	// Transit route should be invalidated. Lookup() filters withdrawn entries,
	// so use Snapshot() to inspect the raw table state.
	snap := svc.routingTable.Snapshot()
	snapRoutes := snap.Routes[idTargetX]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be withdrawn after routing-only peer disconnects")
	}

	// Lookup must return empty — withdrawn routes are not usable.
	if len(svc.routingTable.Lookup(idTargetX)) != 0 {
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
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.onPeerSessionEstablished(idPeerB, false)
	svc.onPeerSessionEstablished(idPeerB, false) // 2 sessions

	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idTargetX, NextHop: idPeerB,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// Drain any pending trigger from setup.
	_ = svc.announceLoop.PendingTrigger()

	// Close one session — transit route should remain, no trigger.
	svc.onPeerSessionClosed(idPeerB, false)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("transit route should remain while a session is still active")
	}
	if svc.announceLoop.PendingTrigger() {
		t.Fatal("no TriggerUpdate expected when other sessions remain")
	}

	// Close last session — now transit route should be invalidated with trigger.
	svc.onPeerSessionClosed(idPeerB, false)

	// Lookup() filters withdrawn entries — use Snapshot() for raw state.
	snap := svc.routingTable.Snapshot()
	snapRoutes := snap.Routes[idTargetX]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot after last close, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be withdrawn after last session closes")
	}

	// Lookup must return empty — withdrawn routes are not usable.
	if len(svc.routingTable.Lookup(idTargetX)) != 0 {
		t.Fatal("Lookup should return empty for withdrawn routes")
	}

	if !svc.announceLoop.PendingTrigger() {
		t.Fatal("expected TriggerUpdate after last session transit route invalidation")
	}
}

func TestConfirmRouteViaHopAck(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Add an announcement route via peer-B.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	// Confirm with the actual NextHop identity that carried the message.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("expected source=hop_ack after confirmation, got %s", routes[0].Source)
	}
}

// TestConfirmRouteViaHopAck_PreservesExtra verifies that hop_ack promotion
// does not strip the Extra field from the original announcement entry.
// Without this, re-announces after hop_ack confirmation would silently
// drop future wire fields (e.g. onion_box).
func TestConfirmRouteViaHopAck_PreservesExtra(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	extra := json.RawMessage(`{"onion_box":"deadbeef","future":true}`)
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
		Extra:     extra,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist after hop_ack")
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("expected source=hop_ack, got %s", routes[0].Source)
	}
	if string(routes[0].Extra) != string(extra) {
		t.Fatalf("Extra lost after hop_ack promotion: got %q, want %q", string(routes[0].Extra), string(extra))
	}

	ae := routes[0].ToAnnounceEntry()
	if string(ae.Extra) != string(extra) {
		t.Fatalf("Extra lost in ToAnnounceEntry after hop_ack: got %q", string(ae.Extra))
	}
}

func TestConfirmRouteViaHopAck_WrongNextHopNotConfirmed(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Route via peer-B.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	// hop_ack came for a message forwarded to peer-C (different route).
	// Should NOT promote the peer-B route.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerC), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("expected source=announcement (not confirmed via wrong next-hop), got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_AlreadyHopAck(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Route already at hop_ack — should not change.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceHopAck,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatal("source should remain hop_ack")
	}
}

func TestConfirmRouteViaHopAck_DirectNotDemoted(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Direct route should not be touched by hop_ack confirmation.
	svc.onPeerSessionEstablished(idTargetX, true)

	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idTargetX), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceDirect {
		t.Fatalf("direct route should not be demoted, got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_ResolvesTransportAddress(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Simulate an outbound session: transport address "tcp://1.2.3.4:9000"
	// maps to peer identity "peer-B".
	svc.sessions[domain.PeerAddress("tcp://1.2.3.4:9000")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	// Route via peer-B.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	// hop_ack arrives from transport address — should resolve to peer-B
	// and confirm the route.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress("tcp://1.2.3.4:9000"), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("expected source=hop_ack after transport-address confirmation, got %s", routes[0].Source)
	}
}

func TestConfirmRouteViaHopAck_EmptyForwardedToSkips(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Add a route.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute should succeed")
	}

	// Empty forwardedTo (stored locally, not forwarded) — should not confirm.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(""), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if routes[0].Source != routing.RouteSourceAnnouncement {
		t.Fatal("empty forwardedTo should not confirm any route")
	}
}

// newTestServiceWithRouting creates a minimal Service with routing table
// initialized, suitable for unit tests that don't need network I/O.
func newTestServiceWithRouting(t *testing.T, localIdentity string) *Service {
	t.Helper()
	svc := &Service{
		identity:              &identity.Identity{Address: localIdentity},
		identitySessions:      make(map[domain.PeerIdentity]int),
		identityRelaySessions: make(map[domain.PeerIdentity]int),
		sessions:              make(map[domain.PeerAddress]*peerSession),
		conns:                 make(map[netcore.ConnID]*connEntry),
		connIDByNetConn:       make(map[net.Conn]netcore.ConnID),
		done:                  make(chan struct{}),
		// Match the production NewService default: seed runCtx with
		// Background so that helpers deriving ctx from s.runCtx (e.g.
		// sendFrameBytesViaNetworkSync inside writeFrameToInbound) do
		// not dereference a nil interface in struct-literal fixtures.
		runCtx: context.Background(),
	}
	svc.routingTable = routing.NewTable(routing.WithLocalOrigin(routing.PeerIdentity(localIdentity)))
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)
	// Queue-state persister: MarkDirty / FlushSync are nil-safe so tests
	// that leave svc.queuePersist unset do not crash, but wiring a
	// no-op persister here keeps the helper closer to NewService's
	// defaults — tests derived from this fixture can exercise paths that
	// observe persister identity (e.g. asserting two call sites share
	// one persister) without extra setup.  Run is never started here,
	// so no disk I/O happens.  Tests that want to observe persistence
	// behaviour override svc.queuePersist themselves (see
	// persist_regression_test.go).
	svc.queuePersist = newQueueStatePersister(queueStatePersisterDeps{
		Snapshot: func() queueStateFile { return queueStateFile{} },
		Save:     func(string, queueStateFile) error { return nil },
		Wait:     realQueueStatePersistWait,
	})
	return svc
}

// --- Capability gating tests ---
// resolveRouteNextHopAddress uses relay-only cap for direct destinations
// (hops=1) and both caps for transit next-hops (hops>1).

func TestResolveRouteNextHop_DirectPeerRelayOnlySuffices(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// peer-B has only mesh_relay_v1, no mesh_routing_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	// Direct destination (hops=1): should resolve, relay cap is enough.
	addr := svc.resolveRouteNextHopAddress(idPeerB, 1)
	if addr == "" {
		t.Fatal("direct destination with relay-only cap should resolve")
	}
	if addr != "addr-B" {
		t.Fatalf("expected addr-B, got %s", addr)
	}
}

func TestResolveRouteNextHop_TransitNeedsBothCaps(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// peer-B has only mesh_relay_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	// Transit next-hop (hops=3): should NOT resolve, needs routing cap too.
	addr := svc.resolveRouteNextHopAddress(idPeerB, 3)
	if addr != "" {
		t.Fatalf("transit next-hop with relay-only cap should not resolve, got %s", addr)
	}

	// Now give peer-B both caps.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	addr = svc.resolveRouteNextHopAddress(idPeerB, 3)
	if addr == "" {
		t.Fatal("transit next-hop with both caps should resolve")
	}
}

func TestResolveRouteNextHop_NoCapsRejectsAll(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// peer-B has no relevant capabilities.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{},
	}

	if addr := svc.resolveRouteNextHopAddress(idPeerB, 1); addr != "" {
		t.Fatalf("peer without relay cap should not resolve for direct, got %s", addr)
	}
	if addr := svc.resolveRouteNextHopAddress(idPeerB, 3); addr != "" {
		t.Fatalf("peer without caps should not resolve for transit, got %s", addr)
	}
}

// --- Route-session binding tests ---
// Direct routes (own-origin) are withdrawn on the wire; transit routes
// (learned via announcement) are silently invalidated locally.

func TestRouteSessionBinding_DirectRouteWithdrawnOnWire(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Connect peer-B → creates direct route.
	svc.onPeerSessionEstablished(idPeerB, true)
	routes := svc.routingTable.Lookup(idPeerB)
	if len(routes) != 1 || routes[0].Source != routing.RouteSourceDirect {
		t.Fatal("expected 1 direct route after connect")
	}

	// Disconnect peer-B → should produce wire withdrawal.
	svc.onPeerSessionClosed(idPeerB, true)

	routes = svc.routingTable.Lookup(idPeerB)
	if len(routes) != 0 {
		t.Fatalf("expected 0 active routes after disconnect, got %d", len(routes))
	}

	// Verify the withdrawal exists in the table as a withdrawn entry.
	snap := svc.routingTable.Snapshot()
	peerRoutes := snap.Routes[idPeerB]
	if len(peerRoutes) == 0 {
		t.Fatal("expected withdrawn entry to exist in snapshot")
	}
	if !peerRoutes[0].IsWithdrawn() {
		t.Fatal("expected direct route to be withdrawn")
	}
}

func TestRouteSessionBinding_TransitRouteLocallyInvalidated(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Connect peer-B so we have a session.
	svc.onPeerSessionEstablished(idPeerB, true)

	// Announce transit route: target-X reachable via peer-B, originated by peer-C.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idPeerC,
		NextHop:   idPeerB,
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("transit route should be accepted")
	}

	// Disconnect peer-B → transit route should be locally invalidated.
	svc.onPeerSessionClosed(idPeerB, true)

	// Transit route should no longer be active.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 0 {
		t.Fatalf("expected 0 active transit routes after peer disconnect, got %d", len(routes))
	}

	// But the transit route should NOT have a wire-ready withdrawal SeqNo
	// bump — only the originator (peer-C) can do that. Verify via snapshot
	// that the entry's Origin is still peer-C (not node-A).
	snap := svc.routingTable.Snapshot()
	transitRoutes := snap.Routes[idTargetX]
	if len(transitRoutes) == 0 {
		t.Fatal("expected invalidated transit entry in snapshot")
	}
	if transitRoutes[0].Origin != idPeerC {
		t.Fatalf("transit route origin should remain peer-C, got %s", transitRoutes[0].Origin)
	}
}

func TestRouteSessionBinding_MixedDirectAndTransit(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Connect peer-B.
	svc.onPeerSessionEstablished(idPeerB, true)

	// Add a transit route through peer-B.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetY,
		Origin:    idTargetY,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("transit route should be accepted")
	}

	// Disconnect peer-B.
	svc.onPeerSessionClosed(idPeerB, true)

	// Both the direct route to peer-B and the transit route via peer-B
	// should be gone from active lookups.
	if routes := svc.routingTable.Lookup(idPeerB); len(routes) != 0 {
		t.Fatal("direct route should be withdrawn")
	}
	if routes := svc.routingTable.Lookup(idTargetY); len(routes) != 0 {
		t.Fatal("transit route should be invalidated")
	}
}

// --- hop_ack scoping tests ---
// confirmRouteViaHopAck must only promote the specific (identity, origin, nextHop)
// entry that matches. Different origin or different identity must not be affected.

func TestHopAckScoping_DifferentOriginNotPromoted(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Two routes to target-X via peer-B, but different origins.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginC,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route 1 should be accepted")
	}

	status, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginD,
		NextHop:   idPeerB,
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route 2 should be accepted")
	}

	// hop_ack with specific origin=origin-D — only that triple should
	// be promoted. origin-C must remain announcement despite sharing
	// the same NextHop.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), domain.PeerIdentity(idOriginD))

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	var foundC, foundD bool
	for _, r := range routes {
		if r.Origin == idOriginC {
			foundC = true
			if r.Source != routing.RouteSourceAnnouncement {
				t.Fatalf("origin-C route must remain announcement (not part of confirmed triple), got %s", r.Source)
			}
		}
		if r.Origin == idOriginD {
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
	svc := newTestServiceWithRouting(t, idNodeA)

	// Two routes to target-X via peer-B, different origins.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginC,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route 1 should be accepted")
	}

	status, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginD,
		NextHop:   idPeerB,
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route 2 should be accepted")
	}

	// Gossip path (empty origin) promotes the first matching NextHop in
	// Lookup order — origin-C with hops=2 wins.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	var foundC, foundD bool
	for _, r := range routes {
		if r.Origin == idOriginC {
			foundC = true
			if r.Source != routing.RouteSourceHopAck {
				t.Fatalf("gossip fallback: origin-C (first in Lookup order) should be promoted, got %s", r.Source)
			}
		}
		if r.Origin == idOriginD {
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
	svc := newTestServiceWithRouting(t, idNodeA)

	// Route to target-X via peer-B.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginC,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route X should be accepted")
	}

	// Route to target-Y via peer-B, same origin.
	status, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetY,
		Origin:    idOriginC,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route Y should be accepted")
	}

	// hop_ack for target-X only.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerB), "")

	// target-X should be promoted.
	routesX := svc.routingTable.Lookup(idTargetX)
	if len(routesX) == 0 {
		t.Fatal("expected route to target-X")
	}
	if routesX[0].Source != routing.RouteSourceHopAck {
		t.Fatalf("target-X should be promoted to hop_ack, got %s", routesX[0].Source)
	}

	// target-Y should NOT be promoted (different identity).
	routesY := svc.routingTable.Lookup(idTargetY)
	if len(routesY) == 0 {
		t.Fatal("expected route to target-Y")
	}
	if routesY[0].Source != routing.RouteSourceAnnouncement {
		t.Fatalf("target-Y should remain announcement, got %s", routesY[0].Source)
	}
}

func TestHopAckScoping_SameIdentityDifferentNextHopNotPromoted(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Two routes to target-X from same origin, different next-hops.
	status, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginC,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route via peer-B should be accepted")
	}

	status, err = svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idOriginC,
		NextHop:   idPeerD,
		Hops:      3,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("route via peer-D should be accepted")
	}

	// hop_ack from peer-D should only promote the peer-D route.
	svc.confirmRouteViaHopAck(domain.PeerIdentity(idTargetX), domain.PeerAddress(idPeerD), "")

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) < 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	for _, r := range routes {
		if r.NextHop == idPeerB && r.Source != routing.RouteSourceAnnouncement {
			t.Fatalf("peer-B route should remain announcement, got %s", r.Source)
		}
		if r.NextHop == idPeerD && r.Source != routing.RouteSourceHopAck {
			t.Fatalf("peer-D route should be promoted to hop_ack, got %s", r.Source)
		}
	}
}

// --- Inbound peer resolution tests ---
// Resolvers must find inbound-only peers and return "inbound:" prefixed keys.

func TestResolveRelayAddress_InboundPeer(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Set up an inbound connection with relay capability.
	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	addr := svc.resolveRelayAddress(idPeerB)
	if addr == "" {
		t.Fatal("inbound peer with relay cap should be resolvable")
	}
	if addr != "inbound:10.0.0.5:8080" {
		t.Fatalf("expected inbound: prefixed key, got %s", addr)
	}
}

func TestResolveRoutableAddress_InboundPeerNeedsBothCaps(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address: domain.PeerAddress(idPeerB),
		Caps:    []domain.Capability{domain.CapMeshRelayV1}, // only relay, no routing
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Should NOT resolve — transit requires both caps.
	addr := svc.resolveRoutableAddress(idPeerB)
	if addr != "" {
		t.Fatalf("inbound peer with relay-only cap should not resolve for transit, got %s", addr)
	}

	// Now add routing cap.
	svc.peerMu.Lock()
	pc.SetIdentity(domain.PeerIdentity(idPeerB))
	pc.SetCapabilities([]domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1})
	svc.peerMu.Unlock()

	addr = svc.resolveRoutableAddress(idPeerB)
	if addr == "" {
		t.Fatal("inbound peer with both caps should resolve")
	}
}

func TestResolveRelayAddress_OutboundPreferredOverInbound(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Set up both outbound session and inbound connection for same peer.
	svc.sessions[domain.PeerAddress("outbound-addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Should prefer outbound session (has async send queue).
	addr := svc.resolveRelayAddress(idPeerB)
	if addr != "outbound-addr-B" {
		t.Fatalf("expected outbound address, got %s", addr)
	}
}

// --- resolvePeerIdentity inbound contract test ---

func TestResolvePeerIdentity_InboundByTransportAddress(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 8080}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	// Pass the transport address — should resolve to identity.
	id := svc.resolvePeerIdentity("10.0.0.5:8080")
	if id != idPeerB {
		t.Fatalf("expected peer-B, got %q", id)
	}

	// Pass the identity — should NOT match (identity != transport address).
	id = svc.resolvePeerIdentity(idPeerB)
	if id != "" {
		t.Fatalf("identity-as-address should not match inbound conn, got %q", id)
	}
}

// --- TableRouter RelayNextHopAddress test ---

func TestTableRouterPopulatesRelayNextHopAddress(t *testing.T) {
	table := routing.NewTable(routing.WithLocalOrigin(idNodeA))

	status, err := table.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idPeerB,
		NextHop:  idPeerB,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil || status != routing.RouteAccepted {
		t.Fatal("UpdateRoute failed")
	}

	tr := &TableRouter{
		svc:   &Service{},
		table: table,
		sessionChecker: func(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
			if peerIdentity == domain.PeerIdentity(idPeerB) {
				return domain.PeerAddress("validated-addr-B")
			}
			return ""
		},
	}

	msg := protocol.Envelope{
		ID:        "msg-1",
		Topic:     "dm",
		Sender:    idNodeA,
		Recipient: idTargetX,
	}

	decision := tr.Route(msg)

	if decision.RelayNextHop == nil {
		t.Fatal("expected RelayNextHop")
	}
	if decision.RelayNextHopAddress != domain.PeerAddress("validated-addr-B") {
		t.Fatalf("expected RelayNextHopAddress=validated-addr-B, got %q", decision.RelayNextHopAddress)
	}
}

// --- Receive-path invariant tests ---
// These verify that handleAnnounceRoutes rejects announcements that violate
// the per-origin SeqNo and withdrawal-only-by-origin invariants.

func TestHandleAnnounceRoutesRejectsForgedOwnOrigin(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// A foreign sender (peer-B) advertises a route with Origin == node-A.
	// This must be rejected: only node-A may originate routes under its
	// own identity. Accepting it would poison the monotonic SeqNo counter.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idNodeA, Hops: 1, SeqNo: 100},
		},
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 0 {
		t.Fatalf("forged own-origin route must be rejected, but %d routes found", len(routes))
	}

	// Verify the SeqNo counter was NOT poisoned.
	result, err := svc.routingTable.AddDirectPeer(idTargetX)
	if err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}
	if result.Entry.SeqNo != 1 {
		t.Fatalf("SeqNo counter should start at 1 (unpoisoned), got %d", result.Entry.SeqNo)
	}
}

func TestHandleAnnounceRoutesRejectsForgedOwnOriginWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// First, add a legitimate direct peer route.
	if _, err := svc.routingTable.AddDirectPeer(idTargetX); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// A foreign sender tries to withdraw node-A's own-origin route.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idNodeA, Hops: 16, SeqNo: 999},
		},
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	// The direct route must still be alive.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("own-origin route must survive forged withdrawal attempt")
	}
	if routes[0].Hops >= routing.HopsInfinity {
		t.Fatal("own-origin route must not be withdrawn by foreign sender")
	}
}

func TestHandleAnnounceRoutesRejectsTransitWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Add a route originated by origin-C, learned via peer-B.
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 2, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// peer-B (transit node, NOT origin-C) tries to withdraw this route.
	// The model says only the origin may emit a wire withdrawal. Transit
	// nodes must invalidate locally and stop advertising.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, withdrawal)

	// Route must survive — only origin-C may withdraw it.
	routes = svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("transit withdrawal must be rejected — route should survive")
	}
	if routes[0].Hops >= routing.HopsInfinity {
		t.Fatal("transit withdrawal must not kill route — only origin may withdraw")
	}
}

func TestHandleAnnounceRoutesAcceptsOriginWithdrawal(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Add a route where origin == sender (origin-C sends its own route).
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idOriginC, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// origin-C withdraws its own route — this is legitimate.
	withdrawal := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 16, SeqNo: 2},
		},
	}
	svc.handleAnnounceRoutes(idOriginC, withdrawal)

	routes = svc.routingTable.Lookup(idTargetX)
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

// newNoopMockPeerSender creates a MockPeerSender that accepts any call and
// returns true — used in tests where sending is not under test.
func newNoopMockPeerSender(t *testing.T) *routingmocks.MockPeerSender {
	t.Helper()
	m := routingmocks.NewMockPeerSender(t)
	m.EXPECT().SendAnnounceRoutes(mock.Anything, mock.Anything).Return(true).Maybe()
	return m
}

// newTestServiceWithRoutingAndHealth extends newTestServiceWithRouting with
// the maps required for trackInboundConnect (health tracking, ref counting).
func newTestServiceWithRoutingAndHealth(t *testing.T, localIdentity string) *Service {
	t.Helper()
	svc := newTestServiceWithRouting(t, localIdentity)
	svc.health = make(map[domain.PeerAddress]*peerHealth)
	svc.inboundHealthRefs = make(map[domain.PeerAddress]int)
	svc.dialOrigin = make(map[domain.PeerAddress]domain.PeerAddress)
	svc.conns = make(map[netcore.ConnID]*connEntry)
	svc.connIDByNetConn = make(map[net.Conn]netcore.ConnID)
	return svc
}

// TestSendFullTableSyncToInbound verifies that a newly connected inbound
// peer receives an immediate full-table sync — symmetric with the outbound
// connect path.
func TestSendFullTableSyncToInbound(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Populate the routing table with a route that the inbound peer
	// should learn about on connect.
	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 12345},
	}

	// Register a NetCore so enqueueFrameSync routes through the writer
	// goroutine instead of falling back to a blocking direct write.
	pc := netcore.New(1, conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Start the pipe reader BEFORE calling the function under test.
	// net.Pipe() is unbuffered — NetCore.writerLoop will block on Write
	// until someone reads from the other end. If the reader goroutine starts
	// after sendFullTableSyncToInbound, the writer blocks past
	// syncFlushTimeout and enqueueFrameSync returns enqueueDropped.
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
	}()

	// Call the function under test.
	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(id, idPeerB)

	select {
	case data := <-received:
		line := string(data)
		if len(line) == 0 {
			t.Fatal("expected announce_routes frame, got empty data")
		}
		if !strings.Contains(line, idPeerC) {
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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Add a route that was learned FROM peer-B (the connecting inbound peer).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idPeerB, NextHop: idPeerB,
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

	pc := netcore.New(netcore.ConnID(2), conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Call the function — with only peer-B's own route in the table,
	// split horizon should filter it out, resulting in no send.
	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(id, idPeerB)

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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.7"), Port: 12347},
	}

	pc := netcore.New(netcore.ConnID(3), conn, netcore.Inbound, netcore.Options{})
	defer pc.Close()
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	id, _ := svc.connIDFor(conn)
	svc.sendFullTableSyncToInbound(id, idPeerB)

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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Populate the routing table so there ARE routes to announce.
	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with relay-only capability (no routing).
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	// Verify the capability gate returns false.
	if svc.sessionHasCapability(domain.PeerAddress("addr-B"), domain.CapMeshRoutingV1) {
		t.Fatal("peer-B should NOT have mesh_routing_v1")
	}

	// The code under test: outbound full-sync is skipped because peer-B
	// lacks mesh_routing_v1. We verify by checking that AnnounceTo would
	// have produced routes (the table is non-empty) but sessionHasCapability
	// blocks the send path.
	routes := svc.routingTable.AnnounceTo(idPeerB)
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce")
	}
}

// TestOutboundFullSyncSentWithRoutingCap verifies that the outbound connect
// path DOES send announce_routes when the peer has mesh_routing_v1.
func TestOutboundFullSyncSentWithRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with routing capability.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	if !svc.sessionHasCapability(domain.PeerAddress("addr-B"), domain.CapMeshRoutingV1) {
		t.Fatal("peer-B should have mesh_routing_v1")
	}

	routes := svc.routingTable.AnnounceTo(idPeerB)
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce to routing-capable peer")
	}
}

// TestInboundFullSyncSkippedWithoutRoutingCap verifies that inbound full-table
// sync is NOT sent when the inbound peer lacks mesh_routing_v1.
func TestInboundFullSyncSkippedWithoutRoutingCap(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
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
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	// connHasCapability should return false.
	connID, _ := svc.connIDFor(conn)
	if svc.connHasCapability(connID, domain.CapMeshRoutingV1) {
		t.Fatal("inbound peer should NOT have mesh_routing_v1")
	}

	// trackInboundConnect should NOT send anything because the gate blocks it.
	svc.trackInboundConnect(connID, idPeerB, idPeerB)

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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 12349},
	}

	// Register a NetCore so enqueueFrameSync routes through the writer.
	pc := netcore.New(2, conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	})
	defer pc.Close()

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	connID, _ := svc.connIDFor(conn)
	if !svc.connHasCapability(connID, domain.CapMeshRoutingV1) {
		t.Fatal("inbound peer should have mesh_routing_v1")
	}

	// Start the pipe reader BEFORE triggering the send — net.Pipe() is
	// unbuffered, so the writer goroutine will block until someone reads.
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
	}()

	// trackInboundConnect should call sendFullTableSyncToInbound.
	svc.trackInboundConnect(connID, idPeerB, idPeerB)

	select {
	case data := <-received:
		line := string(data)
		if len(line) == 0 {
			t.Fatal("expected announce_routes frame, got empty data")
		}
		if !strings.Contains(line, idPeerC) {
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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	// Create a session for peer-B with routing-only capability (no relay).
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1},
	}

	// Peer has routing but not relay.
	if !svc.sessionHasCapability(domain.PeerAddress("addr-B"), domain.CapMeshRoutingV1) {
		t.Fatal("peer-B should have mesh_routing_v1")
	}
	if svc.sessionHasCapability(domain.PeerAddress("addr-B"), domain.CapMeshRelayV1) {
		t.Fatal("peer-B should NOT have mesh_relay_v1")
	}

	// AnnounceTo would produce routes, but the full-sync gate requires both
	// capabilities, so the send path should be skipped.
	routes := svc.routingTable.AnnounceTo(idPeerB)
	if len(routes) == 0 {
		t.Fatal("routing table should have routes to announce")
	}
}

// TestInboundFullSyncSkippedForRoutingOnlyPeer verifies that inbound
// full-table sync is NOT sent when the inbound peer has mesh_routing_v1
// but lacks mesh_relay_v1 (routing-only peer).
func TestInboundFullSyncSkippedForRoutingOnlyPeer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
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
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRoutingV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	connID, _ := svc.connIDFor(conn)
	if !svc.connHasCapability(connID, domain.CapMeshRoutingV1) {
		t.Fatal("inbound peer should have mesh_routing_v1")
	}
	if svc.connHasCapability(connID, domain.CapMeshRelayV1) {
		t.Fatal("inbound peer should NOT have mesh_relay_v1")
	}

	svc.trackInboundConnect(connID, idPeerB, idPeerB)

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
	svc := newTestServiceWithRouting(t, idNodeA)

	// peer-B: routing-only (no relay) — should be excluded.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1},
	}
	// peer-C: both capabilities — should be included.
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		peerIdentity: idPeerC,
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
	}

	targets := svc.routingCapablePeers()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target (peer-C only), got %d", len(targets))
	}
	if targets[0].Identity != idPeerC {
		t.Fatalf("expected peer-C, got %s", targets[0].Identity)
	}
}

// TestRoutingCapablePeersExcludesRoutingOnlyInbound verifies that an inbound
// routing-only peer is also excluded from routingCapablePeers().
func TestRoutingCapablePeersExcludesRoutingOnlyInbound(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	pipeLocal, _ := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.11"), Port: 12351},
	}

	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(idPeerB),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRoutingV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

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
	svc := newTestServiceWithRouting(t, idNodeA)

	// peer-B has only relay capability — insufficient for transit.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	// Transit hops (hops=3): should NOT resolve with relay-only cap.
	addr := svc.resolveRouteNextHopAddress(idPeerB, 3)
	if addr != "" {
		t.Fatalf("transit next-hop with relay-only cap should not resolve, got %s", addr)
	}
}

// TestRetryResolutionTransitWithBothCapsResolves verifies that a peer with
// both capabilities is resolved for transit next-hops.
func TestRetryResolutionTransitWithBothCapsResolves(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	addr := svc.resolveRouteNextHopAddress(idPeerB, 3)
	if addr != "addr-B" {
		t.Fatalf("transit next-hop with both caps should resolve to addr-B, got %s", addr)
	}
}

// TestRetryResolutionDestinationRelayOnlySuffices verifies that a destination
// next-hop (hops=1) can resolve with relay-only capability.
func TestRetryResolutionDestinationRelayOnlySuffices(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}

	addr := svc.resolveRouteNextHopAddress(idPeerB, 1)
	if addr != "addr-B" {
		t.Fatalf("destination next-hop with relay-only cap should resolve, got %s", addr)
	}
}

// TestTableRouterPopulatesRelayNextHopHops verifies that TableRouter.Route()
// sets RelayNextHopHops from the selected RouteEntry so that the retry path
// in sendTableDirectedRelay has the correct hop role for re-resolution.
func TestTableRouterPopulatesRelayNextHopHops(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	// Set up a transit route (3 hops) and a session with both caps.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idPeerC, NextHop: idPeerB,
		Hops: 3, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	router := NewTableRouter(svc, svc.routingTable)
	decision := router.Route(protocol.Envelope{
		Recipient: idTargetX,
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
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)

	// Add a route to target-X via peer-C with origin "origin-D".
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idOriginD, NextHop: idPeerC,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// peer-C needs both caps (transit, hops=2) and a functioning send channel.
	sendCh := make(chan protocol.Frame, 10)
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		peerIdentity: idPeerC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
		sendCh:       sendCh,
	}
	// activePeerSession requires health entry with Connected=true.
	svc.peerMu.Lock()
	svc.health["addr-C"] = &peerHealth{Address: "addr-C", Connected: true}
	svc.peerMu.Unlock()

	// Build a relay frame to forward.
	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-1",
		Recipient: idTargetX,
	}

	result := svc.tryForwardViaRoutingTable(idTargetX, frame, idPeerA)

	if result.Address == "" {
		t.Fatal("expected table-directed forward to succeed")
	}
	if result.RouteOrigin != idOriginD {
		t.Fatalf("expected RouteOrigin='origin-D', got %q", result.RouteOrigin)
	}
}

// TestTryForwardViaRoutingTableExcludesSender verifies split horizon on
// the relay path — the sender's identity is excluded from next-hop selection.
func TestTryForwardViaRoutingTableExcludesSender(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeB)

	// Only route is via peer-A (the sender).
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idPeerA, NextHop: idPeerA,
		Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{
		peerIdentity: idPeerA,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}

	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-2",
		Recipient: idTargetX,
	}

	result := svc.tryForwardViaRoutingTable(idTargetX, frame, idPeerA)

	if result.Address != "" {
		t.Fatalf("should not forward back to sender, got address=%s", result.Address)
	}
}

// TestTryForwardViaRoutingTableNoRoute verifies empty result when no route exists.
func TestTryForwardViaRoutingTableNoRoute(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeB)

	frame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-3",
		Recipient: "unknown",
	}

	result := svc.tryForwardViaRoutingTable("unknown", frame, idPeerA)

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
		identity:              &identity.Identity{Address: idNodeA},
		identitySessions:      make(map[domain.PeerIdentity]int),
		identityRelaySessions: make(map[domain.PeerIdentity]int),
		sessions:              make(map[domain.PeerAddress]*peerSession),
		conns:                 make(map[netcore.ConnID]*connEntry),
		connIDByNetConn:       make(map[net.Conn]netcore.ConnID),
	}
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(idNodeA),
		routing.WithClock(func() time.Time { return now }),
		routing.WithDefaultTTL(customTTL),
	)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
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
	svc := newTestServiceWithRouting(t, idNodeA)

	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}

	svc.handleAnnounceRoutes(idPeerB, frame)

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}

	// ExpiresAt should be set (non-zero) — the table applied its default TTL.
	if routes[0].ExpiresAt.IsZero() {
		t.Fatal("route ExpiresAt should not be zero — table should apply defaultTTL")
	}
}

// --- address ≠ identity tests (NATed inbound peers) ---

// TestInboundFullSyncUsesIdentityNotAddress verifies that
// trackInboundConnect passes the peer identity (Ed25519 fingerprint)
// to sendFullTableSyncToInbound — not the transport/listen address.
// For NATed peers these differ, and split horizon must filter by
// identity to avoid sending a peer its own routes.
func TestInboundFullSyncUsesIdentityNotAddress(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	// Peer-B advertises a route for target-X. Split horizon should
	// suppress this route when syncing back to peer-B.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX, Origin: idPeerB, NextHop: idPeerB,
		Hops: 1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	// Also add a route from peer-C that SHOULD be sent to peer-B.
	if _, err := svc.routingTable.AddDirectPeer(idPeerC); err != nil {
		t.Fatalf("AddDirectPeer failed: %v", err)
	}

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.50"), Port: 9999},
	}

	// Register a NetCore so enqueueFrameSync routes through the writer.
	// NATed peer: address (listen) = "127.0.0.1:64646", identity = idPeerB.
	natListenAddr := domain.PeerAddress("127.0.0.1:64646")
	pc := netcore.New(3, conn, netcore.Inbound, netcore.Options{
		Address:  natListenAddr,
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	})
	defer pc.Close()

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	// Start the pipe reader BEFORE triggering the send — net.Pipe() is
	// unbuffered, so the writer goroutine will block until someone reads.
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := pipeRemote.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
	}()

	// trackInboundConnect must pass peerIdentity (idPeerB), not address.
	connID, _ := svc.connIDFor(conn)
	svc.trackInboundConnect(connID, natListenAddr, idPeerB)

	select {
	case data := <-received:
		line := string(data)
		// Split horizon: route learned FROM peer-B should be excluded.
		if strings.Contains(line, idPeerB) && strings.Contains(line, idTargetX) {
			t.Fatalf("full-sync should not include route learned from peer-B (split horizon broken), got: %s", line)
		}
		// Route for peer-C should be present.
		if !strings.Contains(line, idPeerC) {
			t.Fatalf("full-sync should include route for peer-C, got: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for full-sync frame")
	}
}

// TestRoutingCapablePeersUsesIdentityForInbound verifies that
// routingCapablePeers uses info.identity (not info.address) to build
// announce targets and dedup against outbound sessions.
func TestRoutingCapablePeersUsesIdentityForInbound(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	pipeLocal, _ := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()

	conn := &fakeConn{
		Conn:       pipeLocal,
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.51"), Port: 9998},
	}

	// NATed peer: address is a local listen addr, identity is the fingerprint.
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("127.0.0.1:64646"),
		Identity: domain.PeerIdentity(idPeerB),
		Caps:     []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	targets := svc.routingCapablePeers()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].Identity != idPeerB {
		t.Fatalf("target identity should be %s (fingerprint), got %s", idPeerB, targets[0].Identity)
	}
}

// TestResolveRelayAddressUsesIdentityForInbound verifies that
// resolveRelayAddress matches on info.identity, not info.address.
func TestResolveRelayAddressUsesIdentityForInbound(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.52"), Port: 8080}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("127.0.0.1:64646"), // NATed listen address
		Identity: domain.PeerIdentity(idPeerB),          // real identity
		Caps:     []domain.Capability{domain.CapMeshRelayV1},
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc, tracked: true})
	svc.peerMu.Unlock()

	// Resolve by identity — should find the inbound connection.
	addr := svc.resolveRelayAddress(idPeerB)
	if addr == "" {
		t.Fatal("resolveRelayAddress should find inbound peer by identity")
	}

	// Resolve by listen address — should NOT match (identity ≠ address).
	addr = svc.resolveRelayAddress("127.0.0.1:64646")
	if addr != "" {
		t.Fatalf("resolveRelayAddress should not match by listen address, got %s", addr)
	}
}

// TestResolvePeerIdentityReturnsIdentityNotAddress verifies that
// resolvePeerIdentity returns info.identity, not info.address.
func TestResolvePeerIdentityReturnsIdentityNotAddress(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)

	conn := &fakeConn{remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.53"), Port: 7777}}
	svc.peerMu.Lock()
	pc := netcore.New(netcore.ConnID(1), conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress("127.0.0.1:64646"), // NATed listen
		Identity: domain.PeerIdentity(idPeerB),          // fingerprint
	})
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()

	id := svc.resolvePeerIdentity("10.0.0.53:7777")
	if id != idPeerB {
		t.Fatalf("expected identity %s, got %s", idPeerB, id)
	}
}

// --- Event-driven pending queue drain tests ---

// newTestServiceWithPendingDrain creates a Service with all fields required
// by drainPendingForIdentities: pending queue, routing table, relay states,
// and a TableRouter that performs real route lookups.
func newTestServiceWithPendingDrain(t *testing.T, localIdentity string) *Service {
	t.Helper()
	svc := newTestServiceWithRouting(t, localIdentity)
	svc.pending = make(map[domain.PeerAddress][]pendingFrame)
	svc.pendingKeys = make(map[string]struct{})
	svc.outbound = make(map[string]outboundDelivery)
	svc.relayRetry = make(map[string]relayAttempt)
	svc.topics = make(map[string][]protocol.Envelope)
	svc.receipts = make(map[string][]protocol.DeliveryReceipt)
	svc.orphaned = make(map[domain.PeerAddress][]pendingFrame)
	svc.health = make(map[domain.PeerAddress]*peerHealth)
	svc.dialOrigin = make(map[domain.PeerAddress]domain.PeerAddress)
	svc.relayStates = newRelayStateStore()
	svc.router = NewTableRouter(svc, svc.routingTable)
	return svc
}

func TestDrainPendingForIdentities_SendMessageDrained(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with a relay-capable session so the router can find it.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendCh := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()

	// Add a direct route to idTargetX via peer-B.
	svc.onPeerSessionEstablished(idPeerB, true)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatal(err)
	}

	// Queue a send_message frame on a DIFFERENT peer address (peer-A offline).
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-001",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	key := pendingFrameKey(addrA, frame)
	svc.pendingKeys[key] = struct{}{}
	svc.deliveryMu.Unlock()

	// Drain for idTargetX — the frame should be routed via peer-B.
	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	// Verify the pending queue is empty.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	_, keyExists := svc.pendingKeys[key]
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected pending queue to be drained, got %d frames", remaining)
	}
	if keyExists {
		t.Fatal("expected pending key to be removed")
	}

	// Verify a relay_message was sent to peer-B.
	select {
	case relayed := <-sendCh:
		if relayed.Type != "relay_message" {
			t.Fatalf("expected relay_message, got %s", relayed.Type)
		}
		if relayed.Recipient != idTargetX {
			t.Fatalf("expected recipient %s, got %s", idTargetX, relayed.Recipient)
		}
	default:
		t.Fatal("expected relay_message frame on peer-B sendCh")
	}
}

func TestDrainPendingForIdentities_SkipsRelayMessage(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with route to idTargetX.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       make(chan protocol.Frame, 10),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// Queue a relay_message (someone else's traffic) on peer-A.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	relayFrame := protocol.Frame{
		Type:      "relay_message",
		ID:        "msg-relay-001",
		Address:   idPeerC,
		Recipient: idTargetX,
		Topic:     "dm",
		Body:      "relayed",
		CreatedAt: now.Format(time.RFC3339),
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: relayFrame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, relayFrame)] = struct{}{}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	// relay_message should NOT be drained — it stays in the queue.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected relay_message to remain in pending, got %d frames", remaining)
	}
}

func TestDrainPendingForIdentities_SkipsNonMatchingRecipient(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-002",
		Address:    idNodeA,
		Recipient:  idTargetY, // different identity
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Drain for idTargetX — but our frame is for idTargetY.
	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected frame for non-matching recipient to stay, got %d", remaining)
	}
}

func TestDrainPendingForIdentities_EmptyIdentitiesNoop(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-003",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Empty identities — should be a no-op.
	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{})

	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected frame to stay on empty identities, got %d", remaining)
	}
}

func TestDrainPendingForIdentities_ExpiredFrameRemoved(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with route.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       make(chan protocol.Frame, 10),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatal(err)
	}

	// Queue an expired send_message frame.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	expired := time.Now().UTC().Add(-10 * time.Minute)
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-expired",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "old",
		CreatedAt:  expired.Format(time.RFC3339),
		TTLSeconds: 60, // TTL = 60s, but CreatedAt is 10 min ago → expired
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: expired}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	// Expired frame should be removed from pending (marked terminal).
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected expired frame to be removed, got %d", remaining)
	}
}

func TestDrainPendingForIdentities_NoRouteFrameStays(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// No routes configured — router will return no RelayNextHop.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-noroute",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	// No route available — frame should stay in pending with zero retries.
	// No real delivery attempt was made, so the retry counter must not grow.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	var retries int
	if remaining > 0 {
		retries = svc.pending[addrA][0].Retries
	}
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected frame to stay when no route exists, got %d", remaining)
	}
	if retries != 0 {
		t.Fatalf("expected zero retries when no route exists, got %d", retries)
	}
}

// TestDrainPendingForIdentities_NoRoutePreservesOutboundState verifies that
// when no usable route exists, the outbound delivery state stays "queued" —
// no false "retrying" transition, no LastAttemptAt update. Route churn events
// must not pollute outbound state when no real send was attempted.
func TestDrainPendingForIdentities_NoRoutePreservesOutboundState(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// No routes configured — drain will return attempted=false.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-outbound-state",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}

	// Set initial outbound state to "queued" (as noteOutboundQueuedLocked would).
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.outbound[frame.ID] = outboundDelivery{
		MessageID: frame.ID,
		Recipient: frame.Recipient,
		Status:    "queued",
		QueuedAt:  now,
	}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	state := svc.outbound[frame.ID]
	svc.deliveryMu.RUnlock()

	if state.Status != "queued" {
		t.Fatalf("expected outbound status to stay 'queued' after no-route drain, got %q", state.Status)
	}
	if !state.LastAttemptAt.IsZero() {
		t.Fatalf("expected LastAttemptAt to remain zero after no-route drain, got %v", state.LastAttemptAt)
	}
}

func TestDrainPendingForIdentities_SendFailureReturnsFrame(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Add a route to idTargetX via peer-B.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatal(err)
	}

	// Override sessionChecker to return an inbound-style address.
	// This makes Route() succeed with RelayNextHop populated, but
	// sendRelayToAddress enters the inbound path where writeFrameToInbound
	// fails because no tracked connection exists for this address.
	fakeInboundAddr := domain.PeerAddress("inbound:10.0.0.99:9999")
	svc.router.(*TableRouter).sessionChecker = func(id domain.PeerIdentity, hops int) domain.PeerAddress {
		if id == idPeerB {
			return fakeInboundAddr
		}
		return ""
	}

	// Queue a send_message on offline peer-A.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-fail",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Drain: Route() returns RelayNextHop (peer-B via fake inbound address),
	// but sendRelayToAddress → writeFrameToInbound fails (no tracked conn).
	// Frame must be returned to pending queue.
	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	_, keyExists := svc.pendingKeys[pendingFrameKey(addrA, frame)]
	var retries int
	if remaining > 0 {
		retries = svc.pending[addrA][0].Retries
	}
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected frame to return to pending on send failure, got %d", remaining)
	}
	if !keyExists {
		t.Fatal("expected pending key to be restored after send failure")
	}
	// Real send failure — retry counter must have been incremented.
	if retries != 1 {
		t.Fatalf("expected 1 retry after real send failure, got %d", retries)
	}
}

// TestDrainPendingForIdentities_NoRouteDrainDoesNotExhaustRetries verifies
// that repeated drain cycles with no usable route never exhaust the retry
// budget. This is the regression test for the P1 where route churn events
// triggered drains that burned maxPendingFrameRetries without any real
// delivery attempt.
func TestDrainPendingForIdentities_NoRouteDrainDoesNotExhaustRetries(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-noroute-budget",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "hello",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Run drain cycles more times than maxPendingFrameRetries. Without the
	// fix, each cycle would increment Retries and eventually mark the frame
	// as terminal "failed". With the fix, no-route returns don't touch the
	// counter and the frame stays pending indefinitely.
	ids := map[domain.PeerIdentity]struct{}{idTargetX: {}}
	for i := 0; i < maxPendingFrameRetries+5; i++ {
		svc.drainPendingForIdentities(ids)
	}

	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	var retries int
	if remaining > 0 {
		retries = svc.pending[addrA][0].Retries
	}
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected frame to survive %d no-route drain cycles, got %d remaining", maxPendingFrameRetries+5, remaining)
	}
	if retries != 0 {
		t.Fatalf("expected zero retries after no-route drains, got %d", retries)
	}
}

// TestDrainPendingForIdentities_FailedFramesPreserveOrder verifies that when
// extracted frames fail delivery and return to the pending queue, they are
// merged back into their original positions — preserving exact interleaved
// order across recipients sharing the same peer address.
// Regression test for P2 where drain reordered DM delivery after route churn.
func TestDrainPendingForIdentities_FailedFramesPreserveOrder(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// No routes configured — all drain attempts will return false/not-attempted,
	// so extracted frames must come back to the queue.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()

	// Queue: [msg-1(X), msg-2(Y), msg-3(X)] — two recipients, interleaved.
	frames := []pendingFrame{
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-1", Address: idNodeA, Recipient: idTargetX, Topic: "dm", Body: "first", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-2", Address: idNodeA, Recipient: "other-recipient", Topic: "dm", Body: "second", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-3", Address: idNodeA, Recipient: idTargetX, Topic: "dm", Body: "third", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
	}

	svc.deliveryMu.Lock()
	svc.pending[addrA] = append([]pendingFrame(nil), frames...)
	for _, f := range frames {
		svc.pendingKeys[pendingFrameKey(addrA, f.Frame)] = struct{}{}
	}
	svc.deliveryMu.Unlock()

	// Drain for idTargetX: extracts msg-1 and msg-3, keeps msg-2.
	// Both fail (no route), so they return to pending.
	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	result := svc.pending[addrA]
	ids := make([]string, len(result))
	for i, f := range result {
		ids[i] = f.Frame.ID
	}
	svc.deliveryMu.RUnlock()

	// Expected: exact original order [msg-1, msg-2, msg-3].
	// msg-1(X) and msg-3(X) were extracted and returned; msg-2(Y) was kept.
	// The merge must interleave them back at their original positions.
	expected := []string{"msg-1", "msg-2", "msg-3"}
	if len(ids) != len(expected) {
		t.Fatalf("expected %d frames, got %d: %v", len(expected), len(ids), ids)
	}
	for i, want := range expected {
		if ids[i] != want {
			t.Fatalf("position %d: expected %s, got %s (full order: %v)", i, want, ids[i], ids)
		}
	}
}

// TestDrainPendingForIdentities_PartialDeliveryPreservesOrder verifies
// ordering when some extracted frames are delivered and others fail.
// The gaps left by delivered frames must not shift kept frames.
func TestDrainPendingForIdentities_PartialDeliveryPreservesOrder(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up a route to idTargetX via peer-B so that drain actually
	// attempts delivery.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatal(err)
	}

	// Use an inbound address so sendRelayToAddress fails (no tracked conn).
	fakeInboundAddr := domain.PeerAddress("inbound:10.0.0.99:9999")
	svc.router.(*TableRouter).sessionChecker = func(id domain.PeerIdentity, hops int) domain.PeerAddress {
		if id == idPeerB {
			return fakeInboundAddr
		}
		return ""
	}

	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()

	// Queue: [msg-1(X), msg-2(Y), msg-3(Z), msg-4(X), msg-5(Y)]
	// Drain for X extracts msg-1 and msg-4. Both fail (send failure).
	// Result must be [msg-1, msg-2, msg-3, msg-4, msg-5].
	frames := []pendingFrame{
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-1", Address: idNodeA, Recipient: idTargetX, Topic: "dm", Body: "a", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-2", Address: idNodeA, Recipient: "other-Y", Topic: "dm", Body: "b", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-3", Address: idNodeA, Recipient: "other-Z", Topic: "dm", Body: "c", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-4", Address: idNodeA, Recipient: idTargetX, Topic: "dm", Body: "d", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
		{Frame: protocol.Frame{Type: "send_message", ID: "msg-5", Address: idNodeA, Recipient: "other-Y", Topic: "dm", Body: "e", CreatedAt: now.Format(time.RFC3339), TTLSeconds: 300}, QueuedAt: now},
	}

	svc.deliveryMu.Lock()
	svc.pending[addrA] = append([]pendingFrame(nil), frames...)
	for _, f := range frames {
		svc.pendingKeys[pendingFrameKey(addrA, f.Frame)] = struct{}{}
	}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	result := svc.pending[addrA]
	ids := make([]string, len(result))
	for i, f := range result {
		ids[i] = f.Frame.ID
	}
	svc.deliveryMu.RUnlock()

	expected := []string{"msg-1", "msg-2", "msg-3", "msg-4", "msg-5"}
	if len(ids) != len(expected) {
		t.Fatalf("expected %d frames, got %d: %v", len(expected), len(ids), ids)
	}
	for i, want := range expected {
		if ids[i] != want {
			t.Fatalf("position %d: expected %s, got %s (full order: %v)", i, want, ids[i], ids)
		}
	}
}

func TestDrainPendingForIdentities_ConcurrentDrainNoDuplicate(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with a session and route.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendCh := make(chan protocol.Frame, 20)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    idTargetX,
		NextHop:   idPeerB,
		Hops:      2,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(routing.DefaultTTL),
	}); err != nil {
		t.Fatal(err)
	}

	// Queue a single send_message.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-concurrent",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "once",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Run two drains concurrently for the same identity.
	ids := map[domain.PeerIdentity]struct{}{idTargetX: {}}
	done := make(chan struct{}, 2)
	go func() { svc.drainPendingForIdentities(ids); done <- struct{}{} }()
	go func() { svc.drainPendingForIdentities(ids); done <- struct{}{} }()
	<-done
	<-done

	// Count relay_message frames on peer-B's sendCh — must be exactly 1.
	close(sendCh)
	var count int
	for f := range sendCh {
		if f.Type == "relay_message" && f.ID == "msg-concurrent" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 relay_message, got %d (concurrent double-send)", count)
	}
}

func TestDrainPendingForIdentities_SkipsReceipt(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Queue a relay_delivery_receipt targeting idTargetX.
	// Receipts are not route-recoverable — they use relayStates hop chain,
	// not the routing table. Drain must leave them untouched.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          "msg-receipt-skip",
		Address:     idNodeA,
		Recipient:   idTargetX,
		Status:      "delivered",
		DeliveredAt: now.Format(time.RFC3339),
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	svc.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{idTargetX: {}})

	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 1 {
		t.Fatalf("expected receipt to stay in pending (not route-recoverable), got %d", remaining)
	}
}

func TestHandleAnnounceRoutes_DrainsPendingForAcceptedIdentities(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with relay session.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendCh := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// Queue a send_message for idTargetX on offline peer-A.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-announce",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "waiting",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Announce a route to idTargetX via peer-B.
	announceFrame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	// Install drainDone hook to synchronize with the async goroutine
	// spawned by handleAnnounceRoutes without polling or sleep.
	var wg sync.WaitGroup
	wg.Add(1)
	svc.drainDone = wg.Done

	svc.handleAnnounceRoutes(idPeerB, announceFrame)
	wg.Wait()

	// Pending queue should be drained.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected pending to be drained after announce, got %d", remaining)
	}
}

// TestHandleAnnounceRoutes_WithdrawalWithBackupTriggersDrain verifies that
// when the best route to an identity is withdrawn but a backup route exists
// in the routing table, the event-driven drain fires so pending send_message
// frames can be delivered via the backup route immediately.
func TestHandleAnnounceRoutes_WithdrawalWithBackupTriggersDrain(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-C with a relay session (the backup route goes through C).
	addrC := domain.PeerAddress("10.0.0.3:9000")
	sendChC := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrC] = &peerSession{
		address:      addrC,
		peerIdentity: idPeerC,
		sendCh:       sendChC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrC] = &peerHealth{Address: addrC, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerC, true)

	// Two routes to idTargetX from different origins:
	//   primary: Origin=idOriginC, NextHop=idOriginC (hops=1, seqNo=1)
	//     — announced by idOriginC, will be withdrawn by idOriginC
	//   backup:  Origin=idPeerC, NextHop=idPeerC (hops=1, seqNo=1)
	//     — announced by idPeerC, survives after withdrawal
	//
	// The primary route is added first via handleAnnounceRoutes so that
	// both origin and nextHop match the sender (origin withdrawal rule).

	// We don't need a session for idOriginC — just add routes directly.
	primaryAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idOriginC, primaryAnnounce)

	backupAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerC, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerC, backupAnnounce)

	// Verify two routes exist.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes to %s, got %d", idTargetX, len(routes))
	}

	// Queue a send_message for idTargetX on offline peer-A.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-withdraw-backup",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "waiting for backup route",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// idOriginC withdraws its route (seqNo=2 > 1). Origin == sender.
	// After withdrawal, only the backup route via peer-C remains.
	withdrawalFrame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 16, SeqNo: 2},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	svc.drainDone = wg.Done

	svc.handleAnnounceRoutes(idOriginC, withdrawalFrame)
	wg.Wait()

	// Pending queue should be drained via the backup route through peer-C.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected pending to be drained via backup route after withdrawal, got %d remaining", remaining)
	}

	// Verify the frame was sent to peer-C's sendCh (the backup route).
	select {
	case sent := <-sendChC:
		if sent.ID != "msg-withdraw-backup" {
			t.Fatalf("expected msg-withdraw-backup on peer-C sendCh, got %s", sent.ID)
		}
	default:
		t.Fatal("expected frame on peer-C sendCh but channel was empty")
	}
}

// TestTTLExpiryExposesBackupAndTriggersDrain verifies the full path that
// routingTableTTLLoop follows: when a route expires and a backup survives,
// TickTTL returns the exposed identity, and the drain delivers the pending
// frame via the backup route.
func TestTTLExpiryExposesBackupAndTriggersDrain(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now

	// Build service with a routing table whose clock we control.
	svc := &Service{
		identity:              &identity.Identity{Address: idNodeA},
		identitySessions:      make(map[domain.PeerIdentity]int),
		identityRelaySessions: make(map[domain.PeerIdentity]int),
		sessions:              make(map[domain.PeerAddress]*peerSession),
		conns:                 make(map[netcore.ConnID]*connEntry),
		connIDByNetConn:       make(map[net.Conn]netcore.ConnID),
		done:                  make(chan struct{}),
		pending:               make(map[domain.PeerAddress][]pendingFrame),
		pendingKeys:           make(map[string]struct{}),
		outbound:              make(map[string]outboundDelivery),
		relayRetry:            make(map[string]relayAttempt),
		topics:                make(map[string][]protocol.Envelope),
		receipts:              make(map[string][]protocol.DeliveryReceipt),
		orphaned:              make(map[domain.PeerAddress][]pendingFrame),
		health:                make(map[domain.PeerAddress]*peerHealth),
		dialOrigin:            make(map[domain.PeerAddress]domain.PeerAddress),
		relayStates:           newRelayStateStore(),
	}
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(routing.PeerIdentity(idNodeA)),
		routing.WithClock(func() time.Time { return current }),
	)
	svc.announceLoop = routing.NewAnnounceLoop(
		svc.routingTable,
		newNoopMockPeerSender(t),
		func() []routing.AnnounceTarget { return nil },
	)
	svc.router = NewTableRouter(svc, svc.routingTable)

	// Set up peer-C with a relay session (backup route goes through C).
	addrC := domain.PeerAddress("10.0.0.3:9000")
	sendChC := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrC] = &peerSession{
		address:      addrC,
		peerIdentity: idPeerC,
		sendCh:       sendChC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrC] = &peerHealth{Address: addrC, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerC, true)

	// Two routes to idTargetX:
	//   primary: via origin-A, short TTL (10s) — will expire
	//   backup:  via peer-C, long TTL — survives
	primaryAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idOriginC, primaryAnnounce)

	backupAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerC, Hops: 2, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerC, backupAnnounce)

	// Both routes should be present.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes to %s, got %d", idTargetX, len(routes))
	}

	// Override ExpiresAt on the primary route so it expires when we advance
	// the clock. We need to manipulate the route's ExpiresAt directly — the
	// routing table created entries with DefaultTTL. We re-insert the primary
	// with a short expiry to override.
	//
	// Withdraw the primary by bumping SeqNo and re-announce with same Origin
	// so the table replaces it. But simpler: just insert it again with a
	// short ExpiresAt via UpdateRoute.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    routing.PeerIdentity(idOriginC),
		NextHop:   routing.PeerIdentity(idOriginC),
		Hops:      1,
		SeqNo:     2,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	// Queue a pending send_message for idTargetX. Use real wall-clock time
	// for CreatedAt and QueuedAt because drainPendingForIdentities checks
	// frame TTL expiry against time.Now(), not the routing table's clock.
	addrA := domain.PeerAddress("10.0.0.1:9000")
	realNow := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-ttl-backup",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "waiting for TTL expiry",
		CreatedAt:  realNow.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: realNow}}
	svc.pendingKeys[pendingFrameKey(addrA, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Advance clock past the primary's TTL.
	current = now.Add(11 * time.Second)

	// Simulate what routingTableTTLLoop does:
	//   1. Call TickTTL() — expires primary, returns exposed identity
	//   2. Build identities map
	//   3. Trigger drain
	result := svc.routingTable.TickTTL()
	if len(result.Exposed) != 1 || string(result.Exposed[0]) != idTargetX {
		t.Fatalf("expected TickTTL to expose [%s], got %v", idTargetX, result.Exposed)
	}

	identities := make(map[domain.PeerIdentity]struct{}, len(result.Exposed))
	for _, id := range result.Exposed {
		identities[domain.PeerIdentity(id)] = struct{}{}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	svc.drainDone = wg.Done

	go svc.drainPendingForIdentities(identities)
	wg.Wait()

	// Verify pending queue was drained.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected pending drained after TTL expiry, got %d remaining", remaining)
	}

	// Verify the frame was delivered via peer-C.
	select {
	case sent := <-sendChC:
		if sent.ID != "msg-ttl-backup" {
			t.Fatalf("expected msg-ttl-backup on peer-C sendCh, got %s", sent.ID)
		}
	default:
		t.Fatal("expected frame on peer-C sendCh after TTL-expiry drain")
	}
}

// TestTTLExpiryNoBackup_NoDrain verifies that when all routes for an identity
// expire (no surviving backup), TickTTL does NOT return the identity and no
// drain is triggered.
func TestTTLExpiryNoBackup_NoDrain(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now

	svc := &Service{
		identity:              &identity.Identity{Address: idNodeA},
		identitySessions:      make(map[domain.PeerIdentity]int),
		identityRelaySessions: make(map[domain.PeerIdentity]int),
		sessions:              make(map[domain.PeerAddress]*peerSession),
		conns:                 make(map[netcore.ConnID]*connEntry),
		connIDByNetConn:       make(map[net.Conn]netcore.ConnID),
		done:                  make(chan struct{}),
		pending:               make(map[domain.PeerAddress][]pendingFrame),
		pendingKeys:           make(map[string]struct{}),
		outbound:              make(map[string]outboundDelivery),
	}
	svc.routingTable = routing.NewTable(
		routing.WithLocalOrigin(routing.PeerIdentity(idNodeA)),
		routing.WithClock(func() time.Time { return current }),
	)

	// Single route — will expire.
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity:  idTargetX,
		Origin:    routing.PeerIdentity(idOriginC),
		NextHop:   routing.PeerIdentity(idOriginC),
		Hops:      1,
		SeqNo:     1,
		Source:    routing.RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	// Queue a pending message. Use real wall-clock time for QueuedAt
	// because drainPendingForIdentities checks frame TTL against time.Now().
	addrA := domain.PeerAddress("10.0.0.1:9000")
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-ttl-no-backup",
		Recipient:  idTargetX,
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrA] = []pendingFrame{{Frame: frame, QueuedAt: time.Now().UTC()}}
	svc.deliveryMu.Unlock()

	// Advance past TTL.
	current = now.Add(11 * time.Second)

	result := svc.routingTable.TickTTL()
	if len(result.Exposed) != 0 {
		t.Fatalf("no backup routes survive — expected no exposed identities, got %v", result.Exposed)
	}

	// Pending should be untouched (no drain triggered).
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrA])
	svc.deliveryMu.RUnlock()
	if remaining != 1 {
		t.Fatalf("expected pending untouched (no drain), got %d", remaining)
	}
}

// TestDisconnectWithBackupTriggersDrain verifies that when a relay-capable
// peer disconnects and RemoveDirectPeer exposes a backup route for an
// identity, drainPendingForIdentities fires and delivers the pending frame
// via the surviving backup route.
func TestDisconnectWithBackupTriggersDrain(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with a relay session (primary route goes through B).
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendChB := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendChB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// Set up peer-C with a relay session (backup route goes through C).
	addrC := domain.PeerAddress("10.0.0.3:9000")
	sendChC := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrC] = &peerSession{
		address:      addrC,
		peerIdentity: idPeerC,
		sendCh:       sendChC,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrC] = &peerHealth{Address: addrC, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerC, true)

	// Transit route to idTargetX via peer-B (will be invalidated on disconnect).
	primaryAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, primaryAnnounce)

	// Backup route to idTargetX via peer-C.
	backupAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerC, Hops: 2, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerC, backupAnnounce)

	// Verify both routes present.
	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) < 2 {
		t.Fatalf("expected at least 2 routes to %s, got %d", idTargetX, len(routes))
	}

	// Queue a pending send_message for idTargetX.
	addrStale := domain.PeerAddress("10.0.0.99:9000")
	realNow := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-disconnect-backup",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "waiting for disconnect drain",
		CreatedAt:  realNow.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrStale] = []pendingFrame{{Frame: frame, QueuedAt: realNow}}
	svc.pendingKeys[pendingFrameKey(addrStale, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Disconnect peer-B (last relay session). This triggers RemoveDirectPeer
	// which now returns ExposedBackups, and onPeerSessionClosed calls
	// triggerDrainForExposed → drainPendingForIdentities.
	var wg sync.WaitGroup
	wg.Add(1)
	svc.drainDone = wg.Done

	svc.onPeerSessionClosed(idPeerB, true)
	wg.Wait()

	// Pending queue should be drained.
	svc.deliveryMu.RLock()
	pendingRemaining := len(svc.pending[addrStale])
	svc.deliveryMu.RUnlock()
	if pendingRemaining != 0 {
		t.Fatalf("expected pending drained after disconnect, got %d remaining", pendingRemaining)
	}

	// Frame should have been delivered via peer-C (the backup route).
	// Note: onPeerSessionClosed also sends withdrawal announce_routes to all
	// routing-capable peers (including C), so we drain those first.
	found := false
	for len(sendChC) > 0 {
		sent := <-sendChC
		if sent.Type == "relay_message" && sent.ID == "msg-disconnect-backup" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected relay_message msg-disconnect-backup on peer-C sendCh after disconnect drain")
	}
}

// TestDisconnectNoBackupNoDrain verifies that when a peer disconnects and
// no backup route survives, no drain is triggered.
func TestDisconnectNoBackupNoDrain(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with a relay session.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendChB := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendChB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// Only one route to idTargetX — through peer-B (no backup).
	announce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, announce)

	// Queue a pending send_message.
	addrStale := domain.PeerAddress("10.0.0.99:9000")
	realNow := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-disconnect-no-backup",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "no backup available",
		CreatedAt:  realNow.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrStale] = []pendingFrame{{Frame: frame, QueuedAt: realNow}}
	svc.pendingKeys[pendingFrameKey(addrStale, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Disconnect peer-B. No backup → no drain should fire.
	// Don't set drainDone — if drain fires unexpectedly it won't block.
	svc.onPeerSessionClosed(idPeerB, true)

	// Pending should be untouched.
	svc.deliveryMu.RLock()
	pendingRemaining := len(svc.pending[addrStale])
	svc.deliveryMu.RUnlock()
	if pendingRemaining != 1 {
		t.Fatalf("expected pending untouched (no backup, no drain), got %d", pendingRemaining)
	}
}

// TestHandleAnnounceRoutes_UnchangedRouteTriggersDrain verifies that when a
// reconnected peer re-announces the same routing table (unchanged full-table
// sync), the drain fires for pending frames. Before this fix, only accepted
// (new/improved) routes triggered drain; unchanged reconfirmations were
// silently counted as rejected and never reached the drain path.
func TestHandleAnnounceRoutes_UnchangedRouteTriggersDrain(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with relay session.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendChB := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendChB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// First announce: route to idTargetX via peer-B — accepted.
	firstAnnounce := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(idPeerB, firstAnnounce)

	// Queue a pending send_message for idTargetX on a stale address
	// (simulates a frame that arrived while no route was available).
	addrStale := domain.PeerAddress("10.0.0.99:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-unchanged-drain",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "waiting-for-resync",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrStale] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrStale, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Second announce: identical route (same SeqNo, same hops) — unchanged.
	// Before the fix this would NOT trigger drain because UpdateRoute returned
	// ok=false and the identity was never added to drainIdentities.
	var wg sync.WaitGroup
	wg.Add(1)
	svc.drainDone = wg.Done

	svc.handleAnnounceRoutes(idPeerB, firstAnnounce)
	wg.Wait()

	// Pending queue should be drained.
	svc.deliveryMu.RLock()
	remaining := len(svc.pending[addrStale])
	svc.deliveryMu.RUnlock()

	if remaining != 0 {
		t.Fatalf("expected pending drained after unchanged re-announce, got %d", remaining)
	}

	// Verify the drained frame was sent via peer-B's sendCh.
	found := false
	for len(sendChB) > 0 {
		sent := <-sendChB
		if sent.Type == "relay_message" && sent.ID == "msg-unchanged-drain" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected relay_message for msg-unchanged-drain on peer-B sendCh")
	}
}

// TestHandleAnnounceRoutes_RejectedRouteNoDrain verifies that truly rejected
// routes (stale SeqNo, tombstone-blocked) do NOT trigger drain.
func TestHandleAnnounceRoutes_RejectedRouteNoDrain(t *testing.T) {
	svc := newTestServiceWithPendingDrain(t, idNodeA)

	// Set up peer-B with relay session.
	addrB := domain.PeerAddress("10.0.0.2:9000")
	sendChB := make(chan protocol.Frame, 10)
	svc.peerMu.Lock()
	svc.sessions[addrB] = &peerSession{
		address:      addrB,
		peerIdentity: idPeerB,
		sendCh:       sendChB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[addrB] = &peerHealth{Address: addrB, Connected: true, State: peerStateHealthy}
	svc.peerMu.Unlock()
	svc.onPeerSessionEstablished(idPeerB, true)

	// First announce: route with SeqNo=5.
	svc.handleAnnounceRoutes(idPeerB, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 5},
		},
	})

	// Queue a pending frame.
	addrStale := domain.PeerAddress("10.0.0.99:9000")
	now := time.Now().UTC()
	frame := protocol.Frame{
		Type:       "send_message",
		ID:         "msg-stale-reject",
		Address:    idNodeA,
		Recipient:  idTargetX,
		Topic:      "dm",
		Body:       "should-not-drain",
		CreatedAt:  now.Format(time.RFC3339),
		TTLSeconds: 300,
	}
	svc.deliveryMu.Lock()
	svc.pending[addrStale] = []pendingFrame{{Frame: frame, QueuedAt: now}}
	svc.pendingKeys[pendingFrameKey(addrStale, frame)] = struct{}{}
	svc.deliveryMu.Unlock()

	// Install a drainDone trap: if drain fires unexpectedly, fail the test.
	// The drain goroutine is only launched when drainIdentities is non-empty,
	// and handleAnnounceRoutes makes that decision synchronously before
	// returning — so if drainDone is ever called, the rejected route
	// incorrectly entered the drain set.
	svc.drainDone = func() {
		t.Error("drainDone called unexpectedly — rejected route should not trigger drain")
	}

	// Stale announce: lower SeqNo=3 — rejected, not unchanged.
	svc.handleAnnounceRoutes(idPeerB, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX, Origin: idPeerB, Hops: 1, SeqNo: 3},
		},
	})

	// handleAnnounceRoutes is synchronous: by the time it returns, the
	// drain/no-drain decision is already made. When all routes are rejected,
	// drainIdentities is empty and the goroutine launch is skipped entirely.
	// No sleep needed — just verify pending is untouched.
	svc.deliveryMu.RLock()
	pendingRemaining := len(svc.pending[addrStale])
	svc.deliveryMu.RUnlock()
	if pendingRemaining != 1 {
		t.Fatalf("expected pending untouched after rejected (stale) announce, got %d", pendingRemaining)
	}
}

// ---------------------------------------------------------------------------
// routingTargetsFiltered deduplication via fallback-port alias
// ---------------------------------------------------------------------------

// TestRoutingTargetsDeduplicatesFallbackPort verifies that when a session is
// connected via a fallback port (e.g. host:64646 instead of the canonical
// host:7777), the same physical host is not counted twice — once as an active
// session and again as a pending peer under its canonical address. Without
// deduplication, a fanout slot would be wasted on a duplicate target.
func TestRoutingTargetsDeduplicatesFallbackPort(t *testing.T) {
	t.Parallel()

	canonical := domain.PeerAddress("1.2.3.4:7777")
	fallback := domain.PeerAddress("1.2.3.4:64646")

	svc := &Service{
		identity: &identity.Identity{Address: idNodeA},
		cfg:      config.Node{ListenAddress: "0.0.0.0:9999", AdvertiseAddress: "10.0.0.99:9999"},
	}
	svc.initMaps()

	// Active session under fallback address, with dialOrigin mapping back.
	svc.sessions[fallback] = &peerSession{
		address:      fallback,
		peerIdentity: "peer-identity-A",
		authOK:       true,
	}
	svc.dialOrigin[fallback] = canonical
	svc.health[canonical] = &peerHealth{
		Connected:       true,
		LastPongAt:      time.Now().UTC(),
		LastConnectedAt: time.Now().UTC(),
	}
	svc.peerTypes[canonical] = domain.NodeTypeFull
	svc.peerIDs[canonical] = "peer-identity-A"

	// Canonical peer in peer list (no session under this key).
	svc.peers = []transport.Peer{
		{Address: canonical, Source: domain.PeerSourceBootstrap},
	}

	targets := svc.routingTargets()

	// Count how many times the same host appears.
	hostCount := make(map[string]int)
	for _, addr := range targets {
		host, _, _ := splitHostPort(string(addr))
		hostCount[host]++
	}
	if hostCount["1.2.3.4"] > 1 {
		t.Fatalf("host 1.2.3.4 appears %d times in routing targets — expected deduplication to 1", hostCount["1.2.3.4"])
	}
	if len(targets) != 1 {
		t.Fatalf("expected exactly 1 target, got %d: %v", len(targets), targets)
	}
}

// TestSendNoticeToPeer_BootstrapRecordsOutboundSuccess is the
// end-to-end regression guard for the raw/bootstrap outbound path.
// sendNoticeToPeer falls back to a direct TCP dial + raw hello /
// welcome / auth_session / auth_ok when neither an outbound session
// nor an inbound connection is available. Before the fix, that path
// completed the handshake but never called the advertise-convergence
// success hook, so peers reached only via push_notice fan-out diverged
// from peers reached via managed outbound sessions: they never got an
// announceable persistedMeta row and none of the hostname /
// observed-IP sweep invariants applied to them. This test drives the
// handshake end-to-end against a loopback mock peer and asserts that
// the trusted advertise triple and announce_state land on
// persistedMeta just like they would for the managed path.
func TestSendNoticeToPeer_BootstrapRecordsOutboundSuccess(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	peerID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	peerDone := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			peerDone <- fmt.Errorf("accept: %w", err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(c)

		// hello.
		if _, err := reader.ReadBytes('\n'); err != nil {
			peerDone <- fmt.Errorf("read hello: %w", err)
			return
		}
		welcomeLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "welcome",
			Challenge: "bootstrap-challenge",
			Address:   peerID.Address,
		})
		if err != nil {
			peerDone <- fmt.Errorf("marshal welcome: %w", err)
			return
		}
		if _, err := io.WriteString(c, welcomeLine); err != nil {
			peerDone <- fmt.Errorf("write welcome: %w", err)
			return
		}

		// auth_session.
		if _, err := reader.ReadBytes('\n'); err != nil {
			peerDone <- fmt.Errorf("read auth_session: %w", err)
			return
		}
		authOKLine, err := protocol.MarshalFrameLine(protocol.Frame{Type: "auth_ok"})
		if err != nil {
			peerDone <- fmt.Errorf("marshal auth_ok: %w", err)
			return
		}
		if _, err := io.WriteString(c, authOKLine); err != nil {
			peerDone <- fmt.Errorf("write auth_ok: %w", err)
			return
		}

		// push_notice. sendNoticeToPeer then does one more readFrameLine
		// — send a trivial reply so our side returns without waiting out
		// the deadline.
		if _, err := reader.ReadBytes('\n'); err != nil {
			peerDone <- fmt.Errorf("read push_notice: %w", err)
			return
		}
		replyLine, _ := protocol.MarshalFrameLine(protocol.Frame{Type: "ok"})
		_, _ = io.WriteString(c, replyLine)
		peerDone <- nil
	}()

	peerAddr := domain.PeerAddress(ln.Addr().String())
	svc.sendNoticeToPeer(peerAddr, time.Minute, "bootstrap-ciphertext")

	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatalf("peer goroutine: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("bootstrap handshake did not complete within 5s")
	}

	svc.peerMu.RLock()
	pm := svc.persistedMeta[peerAddr]
	svc.peerMu.RUnlock()
	if pm == nil {
		t.Fatalf("persistedMeta row not created by bootstrap path")
	}
	if pm.AnnounceState != announceStateAnnounceable {
		t.Fatalf("announce_state: got %q want %q", pm.AnnounceState, announceStateAnnounceable)
	}
	if pm.TrustedAdvertiseSource != trustedAdvertiseSourceOutbound {
		t.Fatalf("trusted_advertise_source: got %q want %q",
			pm.TrustedAdvertiseSource, trustedAdvertiseSourceOutbound)
	}
	if pm.TrustedAdvertiseIP != "127.0.0.1" {
		t.Fatalf("trusted_advertise_ip: got %q want %q (must be canonical IP from RemoteAddr)",
			pm.TrustedAdvertiseIP, "127.0.0.1")
	}
	_, wantPort, ok := splitHostPort(string(peerAddr))
	if !ok {
		t.Fatalf("splitHostPort failed for listener addr %q", peerAddr)
	}
	if pm.TrustedAdvertisePort != wantPort {
		t.Fatalf("trusted_advertise_port: got %q want %q", pm.TrustedAdvertisePort, wantPort)
	}
}

// TestSendNoticeToPeer_SelfIdentityNotice_AppliesCooldown pins the
// reason-aware self-loopback cooldown on the raw push_notice fan-out
// path. Before the fix, the connection_notice branch in
// sendNoticeToPeer recorded the notice via handleConnectionNotice and
// returned without calling applySelfIdentityCooldown — so a
// routingTargets() alias that slipped past the isSelfAddress() gate in
// gossipNotice and landed on our own inbound hello handler would get
// connection_notice{code=peer-banned, reason=self-identity} back and
// this branch would drop it. The next gossip tick would redial the
// same self-looping endpoint because the dial-gate reads health, not
// the persistedMeta row handleConnectionNotice writes. This test
// reproduces that wire shape against a loopback mock and asserts
// health.BannedUntil is stamped with the 24h window and
// LastErrorCode == ErrCodeSelfIdentity.
func TestSendNoticeToPeer_SelfIdentityNotice_AppliesCooldown(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Mock peer: read hello, reply with connection_notice{peer-banned,
	// reason=self-identity}. This is the exact wire shape our own
	// inbound hello handler emits when a remote hello carries our
	// identity (i.e. the self-loopback case the test is pinning).
	peerDone := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			peerDone <- fmt.Errorf("accept: %w", err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(c)

		if _, err := reader.ReadBytes('\n'); err != nil {
			peerDone <- fmt.Errorf("read hello: %w", err)
			return
		}
		// Craft the real wire shape: peer-banned code plus
		// details.reason=self-identity. NoticeErrorFromFrame on our
		// side must resolve this to ErrSelfIdentity and route through
		// applySelfIdentityCooldown — without that, handleConnectionNotice
		// would only touch persistedMeta and the regression described
		// above would reappear.
		details, err := protocol.MarshalPeerBannedDetails(time.Time{}, protocol.PeerBannedReasonSelfIdentity)
		if err != nil {
			peerDone <- fmt.Errorf("marshal details: %w", err)
			return
		}
		noticeLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:    protocol.FrameTypeConnectionNotice,
			Code:    protocol.ErrCodePeerBanned,
			Details: details,
		})
		if err != nil {
			peerDone <- fmt.Errorf("marshal notice: %w", err)
			return
		}
		if _, err := io.WriteString(c, noticeLine); err != nil {
			peerDone <- fmt.Errorf("write notice: %w", err)
			return
		}
		peerDone <- nil
	}()

	peerAddr := domain.PeerAddress(ln.Addr().String())
	svc.sendNoticeToPeer(peerAddr, time.Minute, "self-identity-ciphertext")

	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatalf("peer goroutine: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("self-identity notice exchange did not complete within 5s")
	}

	// Sanity on decode parity: the helper must resolve the crafted
	// notice the same way the production call site does. If this
	// fails, the production path is definitely broken regardless of
	// what the cooldown assertions below report.
	details, _ := protocol.MarshalPeerBannedDetails(time.Time{}, protocol.PeerBannedReasonSelfIdentity)
	decoded := protocol.NoticeErrorFromFrame(protocol.Frame{
		Type:    protocol.FrameTypeConnectionNotice,
		Code:    protocol.ErrCodePeerBanned,
		Details: details,
	})
	if !errors.Is(decoded, protocol.ErrSelfIdentity) {
		t.Fatalf("NoticeErrorFromFrame decode sanity: %v; want errors.Is(ErrSelfIdentity)", decoded)
	}

	svc.peerMu.RLock()
	health, ok := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("applySelfIdentityCooldown was not reached: no peerHealth entry for %s", peerAddr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set — sendNoticeToPeer notice branch bypassed the cooldown (the bug this test pins)")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if health.LastError == "" {
		t.Fatal("health.LastError is empty; applySelfIdentityCooldown must stamp the structured error message")
	}
	if minExpected := peerBanSelfIdentity - time.Minute; time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}

// TestSendNoticeToPeer_SelfIdentityWelcome_AppliesCooldown pins the
// defence-in-depth branch on the same raw path: if the responder is
// an older/mismatched role that does NOT emit the peer-banned notice
// but still welcomes us with our own Ed25519 address, the dial must
// abort at the welcome check and apply the same cooldown. Without
// this branch the code would sign auth_session with its own key,
// complete the raw handshake, and drop our own welcome into the
// peer caches via recordOutboundAuthSuccess — a defence-in-depth hole.
func TestSendNoticeToPeer_SelfIdentityWelcome_AppliesCooldown(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	peerDone := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			peerDone <- fmt.Errorf("accept: %w", err)
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(c)

		if _, err := reader.ReadBytes('\n'); err != nil {
			peerDone <- fmt.Errorf("read hello: %w", err)
			return
		}
		// Non-notice welcome carrying our own identity. No challenge
		// so the caller does not even attempt auth_session — the guard
		// must short-circuit right after parsing the welcome.
		welcomeLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:    "welcome",
			Address: svc.identity.Address,
			Listen:  ln.Addr().String(),
		})
		if err != nil {
			peerDone <- fmt.Errorf("marshal welcome: %w", err)
			return
		}
		if _, err := io.WriteString(c, welcomeLine); err != nil {
			peerDone <- fmt.Errorf("write welcome: %w", err)
			return
		}
		peerDone <- nil
	}()

	peerAddr := domain.PeerAddress(ln.Addr().String())
	svc.sendNoticeToPeer(peerAddr, time.Minute, "self-welcome-ciphertext")

	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatalf("peer goroutine: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("self-identity welcome exchange did not complete within 5s")
	}

	svc.peerMu.RLock()
	health, ok := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if !ok {
		t.Fatalf("applySelfIdentityCooldown was not reached: no peerHealth entry for %s", peerAddr)
	}
	if health.BannedUntil.IsZero() {
		t.Fatal("BannedUntil not set — sendNoticeToPeer welcome-address guard missing")
	}
	if got, want := health.LastErrorCode, protocol.ErrCodeSelfIdentity; got != want {
		t.Fatalf("health.LastErrorCode = %q, want %q", got, want)
	}
	if minExpected := peerBanSelfIdentity - time.Minute; time.Until(health.BannedUntil) < minExpected {
		t.Fatalf("BannedUntil too short: got %v until, want ≥ %v", time.Until(health.BannedUntil), minExpected)
	}
}
