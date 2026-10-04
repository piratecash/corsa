package node

import (
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_routing_claims_test.go pins the routing half of N1
// (docs/refactoring/n1-legacy-residual.md §2): once identity X has proved
// itself over v2 to this node, a legacy connection that merely names X — an
// impostor that connected before the proof — changes nothing about the routes
// via X, by any frame, while X's v2 session is alive.

var routingTwinsCaps = []domain.Capability{
	domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRoutingV3, domain.CapMeshRelayV1,
}

// routingTwins is a node on which X is pinned to v2, X's own v2 session has
// delivered a baseline with a route to idTargetX, and an impostor's legacy
// session naming X — opened before the pin — is still there.
type routingTwins struct {
	svc      *Service
	x        domain.PeerIdentity
	genuine  *peerSession
	impostor *peerSession
	// baseline is the route via X to idTargetX as X's v2 session made it.
	baseline routing.RouteEntry
}

func pinIdentity(t *testing.T, svc *Service, id domain.PeerIdentity) {
	t.Helper()
	svc.secureSessions = &secureSessions{
		mode:  sessionv2.ModeTransition,
		store: loadSecureSessionStore("", time.Now),
		marks: newSessionAddressMarks(time.Now),
	}
	if err := svc.secureSessions.store.noteProvenInbound(id); err != nil {
		t.Fatalf("pin: %v", err)
	}
}

func newRoutingTwins(t *testing.T) routingTwins {
	t.Helper()
	svc := newLegacyPenaltyService(t)
	peer, _ := sessionv2test.NewProvenPeer(t)
	x := peer.Identity
	svc.announceLoop.StateRegistry().MarkReconnected(x, routingTwinsCaps)
	pinIdentity(t, svc, x)

	genuine := provenOutboundSession(legacyVictimAddress, peer)
	genuine.capabilities = routingTwinsCaps
	impostor := legacyOutboundSession(legacyAttackerAddress, x)
	impostor.capabilities = routingTwinsCaps
	svc.peerMu.Lock()
	svc.sessions[genuine.address] = genuine
	svc.sessions[impostor.address] = impostor
	svc.peerMu.Unlock()

	svc.dispatchPeerSessionFrame(genuine.address, genuine, legacyBaselineFrame(idTargetX))
	tw := routingTwins{svc: svc, x: x, genuine: genuine, impostor: impostor}
	baseline, ok := tw.routeVia(idTargetX)
	if !ok {
		t.Fatal("precondition: X's v2 baseline installed the route via X")
	}
	tw.baseline = baseline
	return tw
}

func (tw routingTwins) routeVia(target domain.PeerIdentity) (routing.RouteEntry, bool) {
	for _, route := range tw.svc.routingTable.Lookup(target) {
		if route.NextHop == tw.x {
			return route, true
		}
	}
	return routing.RouteEntry{}, false
}

// RT-1: an empty baseline, a withdrawal with the largest SeqNo and a delta
// with a higher SeqNo, all from the impostor, leave X's route as X's v2
// session made it, and X's next announcement is still accepted.
func TestUnprovenClaimCannotRewriteRoutesOfAV2Identity(t *testing.T) {
	tw := newRoutingTwins(t)

	tw.svc.dispatchPeerSessionFrame(tw.impostor.address, tw.impostor, protocol.Frame{Type: "announce_routes"})
	tw.svc.dispatchPeerSessionFrame(tw.impostor.address, tw.impostor, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: tw.x.String(), Hops: routing.HopsInfinity, SeqNo: math.MaxUint64 - 1},
			{Identity: tw.x.String(), Origin: tw.x.String(), Hops: routing.HopsInfinity, SeqNo: math.MaxUint64 - 1},
		},
	})
	tw.svc.dispatchPeerSessionFrame(tw.impostor.address, tw.impostor, protocol.Frame{
		Type: "routes_update",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: idTargetX.String(), Hops: 7, SeqNo: 1 << 40},
		},
	})

	route, ok := tw.routeVia(idTargetX)
	if !ok || route.Hops != tw.baseline.Hops || route.SeqNo != tw.baseline.SeqNo {
		t.Fatalf("X's route via X is %+v (present %v), was %+v: a legacy connection that named X rewrote it", route, ok, tw.baseline)
	}
	tw.svc.dispatchPeerSessionFrame(tw.genuine.address, tw.genuine, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetY.String(), Origin: idTargetY.String(), Hops: 1, SeqNo: 1},
		},
	})
	if !routeLearnedVia(tw.svc, idTargetY, tw.x) {
		t.Fatal("X's own next announcement was refused after the impostor's frames")
	}
}

// RT-2: an unsigned poison from the impostor invalidates nothing.
func TestUnprovenClaimCannotPoisonRoutesOfAV2Identity(t *testing.T) {
	tw := newRoutingTwins(t)
	tw.svc.handleRoutePoison(sessionRoutingSender(tw.impostor), protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: idTargetX.String(),
		Reason:   protocol.RoutePoisonReasonUplinkLost,
		IssuedAt: time.Now().UTC().Format(time.RFC3339),
	})
	tw.svc.handleRoutePoisonV2(sessionRoutingSender(tw.impostor), protocol.RoutePoisonV2Frame{
		Identities: []string{idTargetX.String()},
		Reason:     protocol.RoutePoisonReasonUplinkLost,
		IssuedAt:   time.Now().UTC().Format(time.RFC3339),
	})

	if _, ok := tw.routeVia(idTargetX); !ok {
		t.Fatal("an unsigned poison from a legacy connection that named X removed X's route")
	}
}

// RT-3: a v3 frame with a far higher epoch from the impostor does not make X's
// own v3 frames stale.
func TestUnprovenClaimCannotAdvanceTheV3EpochOfAV2Identity(t *testing.T) {
	tw := newRoutingTwins(t)
	tw.svc.handleRouteAnnounceV3(sessionRoutingSender(tw.impostor), tw.impostor.address, protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: 1000,
	})
	tw.svc.handleRouteAnnounceV3(sessionRoutingSender(tw.genuine), tw.genuine.address, protocol.RouteAnnounceV3Frame{
		Kind:    protocol.RouteAnnounceV3KindFull,
		Epoch:   1,
		Entries: []protocol.RouteAnnounceV3Entry{{Identity: idTargetY.String(), Hops: 1, SeqNo: 1}},
	})
	if !routeLearnedVia(tw.svc, idTargetY, tw.x) {
		t.Fatal("X's own v3 baseline was dropped as stale: a legacy connection that named X advanced X's epoch")
	}
}

// RT-4: a relay that went out over the impostor and timed out is not charged
// to the route via X; the impostor's ack does not confirm it either.
func TestUnprovenClaimHopSignalsAreNotChargedToAV2Identity(t *testing.T) {
	tw := newRoutingTwins(t)
	for i := 0; i < routing.BlackHoleThreshold+1; i++ {
		tw.svc.onRelayHopAckTimeout(relayForwardState{
			MessageID:    "via-impostor",
			Recipient:    idTargetX,
			ForwardedTo:  tw.impostor.address,
			ForwardedVia: sessionRoutingSender(tw.impostor),
		})
	}
	if _, ok := tw.routeVia(idTargetX); !ok {
		t.Fatal("hop failures over a legacy connection that named X cooled the route via X down")
	}
}

// RT-5: a route_query_response from an accepted legacy connection naming X
// does not overwrite the route X's v2 session announced.
func TestUnprovenClaimQueryResponseCannotRewriteRoutesOfAV2Identity(t *testing.T) {
	tw := newRoutingTwins(t)
	remote := &net.TCPAddr{IP: net.ParseIP("10.0.0.67"), Port: 40100}
	connID := legacyInboundConn(t, tw.svc, netcore.ConnID(900), remote, tw.x)
	tw.svc.netCoreForID(connID).SetCapabilities(append([]domain.Capability{domain.CapMeshRouteQueryV1}, routingTwinsCaps...))

	raw, err := protocol.MarshalRouteQueryResponseFrame(protocol.RouteQueryResponseFrame{
		QueryID:        1,
		TargetIdentity: idTargetX,
		Found:          true,
		BestUplink:     idPeerC,
		BestHops:       6,
		BestSeqNo:      1 << 40,
	})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	tw.svc.dispatchNetworkFrame(connID, string(raw))

	route, ok := tw.routeVia(idTargetX)
	if !ok || route.Hops != tw.baseline.Hops || route.SeqNo != tw.baseline.SeqNo {
		t.Fatalf("X's route via X is %+v (present %v), was %+v: a query response over a legacy connection that named X rewrote it", route, ok, tw.baseline)
	}
}

// RT-8 (compatibility): an identity nobody proved over v2 keeps the legacy
// right to its routing plane — its legacy session's frames apply as before.
func TestUnprovenClaimOfAnUnpinnedIdentityKeepsItsRoutingRights(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	legacy := legacyOutboundSession(legacyAttackerAddress, idPeerB)
	svc.dispatchPeerSessionFrame(legacy.address, legacy, legacyBaselineFrame(idTargetX))
	if !routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("a legacy session of an identity that never proved v2 lost its routing rights")
	}
}

// claimsOf reports how many connections of node name id, split by kind:
// v2 (TLS) and legacy (the metered socket itself), both directions.
func claimsOf(node *Service, id domain.PeerIdentity) (v2, legacy int) {
	node.peerMu.RLock()
	defer node.peerMu.RUnlock()
	for _, session := range node.sessions {
		if session.peerIdentity != id {
			continue
		}
		if _, proven := session.provenIdentity(); proven {
			v2++
		} else {
			legacy++
		}
	}
	for _, entry := range node.conns {
		core := entry.core
		if core == nil || core.Dir() != netcore.Inbound || core.Identity() != id {
			continue
		}
		if _, proven := core.Auth().ProvenIdentity(); proven {
			v2++
		} else {
			legacy++
		}
	}
	return v2, legacy
}

// The mandatory scenario of the owner's decision on N1: a legacy impostor of
// X connects first; the real X then proves itself over v2; then X's v2
// session closes. From the proof on, the impostor has no connection to this
// node, so it neither keeps routing rights nor is a path for traffic meant
// for X — and that stays so after the last v2 session of X is gone, because
// the requirement to prove v2 is pinned.
//
// The impostor here is an old node holding X's keys: on the listener that is
// indistinguishable from a node relaying X's auth_session, which is the
// attack, and it needs no relay to set up.
func TestALegacyImpostorLosesEverythingOnceTheRealIdentityProvesV2(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	xKeys := testIdentityForNetworkConsumerTest(t)
	x := peerIdentityOf(t, &Service{identity: xKeys})

	_, stopImpostor := startTestNodeWithIdentityAndSetup(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{normalizeAddress(address)},
		Type:           domain.NodeTypeFull,
	}, xKeys, asOldNode)
	defer stopImpostor()
	waitForConditionMsg(t, 15*time.Second, "the impostor never connected over v1", func() bool {
		_, legacy := claimsOf(listener, x)
		return legacy > 0
	})

	_, stopReal := startTestNodeWithIdentity(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{normalizeAddress(address)},
		Type:           domain.NodeTypeFull,
	}, xKeys)
	waitForConditionMsg(t, 15*time.Second, "the real X never connected over v2", func() bool {
		v2, _ := claimsOf(listener, x)
		return v2 > 0
	})
	waitForConditionMsg(t, 5*time.Second, "the impostor's legacy connection survived X's v2 proof", func() bool {
		_, legacy := claimsOf(listener, x)
		return legacy == 0
	})

	stopReal()
	waitForConditionMsg(t, 15*time.Second, "X's v2 session never went away", func() bool {
		v2, _ := claimsOf(listener, x)
		return v2 == 0
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, legacy := claimsOf(listener, x); legacy > 0 {
			t.Fatal("the impostor got a connection back after X's last v2 session closed")
		}
		if targets := listener.peerSendableTargetsForTest(x); targets > 0 {
			t.Fatalf("%d connections would carry traffic meant for X", targets)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// peerSendableTargetsForTest counts the connections a send to id would pick
// from, by the same selection the send paths use, plus the relay path.
func (s *Service) peerSendableTargetsForTest(id domain.PeerIdentity) int {
	s.peerMu.RLock()
	sendable := len(s.peerSendableConnectionsLocked(id, domain.CapMeshRelayV1, time.Now()))
	s.peerMu.RUnlock()
	if s.resolveRelayAddress(id) != "" {
		sendable++
	}
	return sendable
}

// RT-6: what an impostor's legacy session wrote about X BEFORE X proved
// itself — a withdrawal with the largest SeqNo, a black-hole cooldown, a far
// higher v3 epoch — is forgotten at the proof, so X's own announcements are
// accepted at once; and the impostor's session is closed.
func TestAV2ProofForgetsWhatALegacyClaimWroteBeforeIt(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	svc.secureSessions = &secureSessions{
		mode:  sessionv2.ModeTransition,
		store: loadSecureSessionStore("", time.Now),
		marks: newSessionAddressMarks(time.Now),
	}
	peer, _ := sessionv2test.NewProvenPeer(t)
	x := peer.Identity
	svc.announceLoop.StateRegistry().MarkReconnected(x, routingTwinsCaps)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	impostor := legacyOutboundSession(legacyAttackerAddress, x)
	impostor.capabilities = routingTwinsCaps
	impostor.conn = local
	svc.peerMu.Lock()
	svc.sessions[impostor.address] = impostor
	svc.peerMu.Unlock()

	// Before the proof X is nobody's to protect: the impostor's frames apply.
	svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(idTargetX))
	svc.dispatchPeerSessionFrame(impostor.address, impostor, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: x.String(), Hops: routing.HopsInfinity, SeqNo: math.MaxUint64 - 1},
		},
	})
	svc.handleRouteAnnounceV3(sessionRoutingSender(impostor), impostor.address, protocol.RouteAnnounceV3Frame{
		Kind: protocol.RouteAnnounceV3KindFull, Epoch: 1000,
	})
	upsertRouteViaForTest(t, svc, idTargetY, x)
	for i := 0; i < routing.BlackHoleThreshold; i++ {
		svc.onRelayHopAckTimeout(relayForwardState{MessageID: "pre-proof", Recipient: idTargetY, ForwardedTo: impostor.address, ForwardedVia: sessionRoutingSender(impostor)})
	}
	if len(svc.routingTable.Lookup(idTargetY)) != 0 {
		t.Fatal("precondition: the impostor's hop failures cooled the route via X down")
	}

	// X proves itself over v2: the pin is written, then the hook runs —
	// exactly the order of openInboundTransport / provenTransport.
	if err := svc.secureSessions.store.noteProvenInbound(x); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := svc.onIdentityProvenV2(svc.runCtx, sessionv2test.Proof(t, peer)); err != nil {
		t.Fatalf("v2 proof: %v", err)
	}

	_ = remote.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := remote.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("the impostor's session is still open after X proved itself (write: %v)", err)
	}
	if len(svc.routingTable.Lookup(idTargetY)) != 0 {
		t.Fatal("the impostor's claim via X survived the purge")
	}

	genuine := provenOutboundSession(legacyVictimAddress, peer)
	genuine.capabilities = routingTwinsCaps
	svc.dispatchPeerSessionFrame(genuine.address, genuine, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: idTargetX.String(), Hops: 1, SeqNo: 1},
			{Identity: idTargetY.String(), Origin: idTargetY.String(), Hops: 1, SeqNo: 1},
		},
	})
	if !routeLearnedVia(svc, idTargetX, x) {
		t.Fatal("X's own announcement was refused by a tombstone the impostor left before the proof")
	}
	if !routeLearnedVia(svc, idTargetY, x) {
		t.Fatal("X's own route stays cooled down by failures that happened on the impostor's session")
	}
	svc.handleRouteAnnounceV3(sessionRoutingSender(genuine), genuine.address, protocol.RouteAnnounceV3Frame{
		Kind:    protocol.RouteAnnounceV3KindFull,
		Epoch:   1,
		Entries: []protocol.RouteAnnounceV3Entry{{Identity: idTargetZForTest.String(), Hops: 1, SeqNo: 1}},
	})
	if !routeLearnedVia(svc, idTargetZForTest, x) {
		t.Fatal("X's own v3 baseline is stale against the epoch the impostor sent before the proof")
	}
}

// RT-7: an ordinary v2 reconnect, with no legacy input written in X's name,
// forgets nothing.
func TestAV2ProofWithoutLegacyInputForgetsNothing(t *testing.T) {
	tw := newRoutingTwins(t)
	if err := tw.svc.onIdentityProvenV2(tw.svc.runCtx, sessionv2test.Proof(t, *tw.genuine.proven)); err != nil {
		t.Fatalf("v2 proof: %v", err)
	}
	if route, ok := tw.routeVia(idTargetX); !ok || route.Hops != tw.baseline.Hops || route.SeqNo != tw.baseline.SeqNo || route.Source != tw.baseline.Source {
		t.Fatalf("an ordinary v2 proof changed the route via X: %+v (present %v), was %+v", route, ok, tw.baseline)
	}
}

var idTargetZForTest = domain.PeerIdentityFromWire("dd00000000000000000000000000000000000003")

// upsertRouteViaForTest installs a transit claim to target via uplink the
// way an announcement would.
func upsertRouteViaForTest(t *testing.T, svc *Service, target, uplink domain.PeerIdentity) {
	t.Helper()
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: target,
		Origin:   target,
		NextHop:  uplink,
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
}
