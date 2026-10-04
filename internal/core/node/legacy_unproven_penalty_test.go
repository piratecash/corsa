package node

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_unproven_penalty_test.go pins N1 (session-security-v2 С-9): a legacy
// (v1) session never proves the identity it names, so nothing it does may
// raise punitive or budget state against that identity. Every test here plays
// the same attack — a peer on a legacy session names the victim's identity
// and misbehaves — and then asks two questions:
//
//   - is the VICTIM, arriving over a connection of its own, still served?
//   - is the ATTACKER still stopped? Moving the key must not move the
//     protection away from the peer that earned it.

var (
	legacyVictimAddress   = domain.PeerAddress("10.0.0.21:64646")
	legacyAttackerAddress = domain.PeerAddress("10.0.0.66:64646")
)

// legacyRoutingCaps is the capability set every routing-plane frame of this
// file needs on both sessions.
var legacyRoutingCaps = []domain.Capability{
	domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRelayV1,
}

// legacyOutboundSession is a dialled legacy session whose welcome named
// claimed. Nothing on this direction is proven: the address is ours, the
// identity is the peer's word.
func legacyOutboundSession(address domain.PeerAddress, claimed domain.PeerIdentity) *peerSession {
	return &peerSession{
		address:      address,
		peerIdentity: claimed,
		authOK:       true,
		capabilities: legacyRoutingCaps,
	}
}

func newLegacyPenaltyService(t *testing.T) *Service {
	t.Helper()
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)
	svc.announceLimiter = newAnnounceRateLimiter()
	svc.health = make(map[domain.PeerAddress]*peerHealth)
	svc.announceLoop.StateRegistry().MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
	return svc
}

// legacyBaselineFrame is a one-route announce_routes baseline for a target
// identity distinct per call site, so a test can tell WHOSE baseline landed.
func legacyBaselineFrame(target domain.PeerIdentity) protocol.Frame {
	return protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{{
			Identity: target.String(),
			Origin:   target.String(),
			Hops:     1,
			SeqNo:    1,
		}},
	}
}

func legacyDeltaFrame(seq int) protocol.Frame {
	return protocol.Frame{
		Type: "routes_update",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{{
			Identity: fmt.Sprintf("dd%038x", seq+1),
			Origin:   fmt.Sprintf("cc%038x", seq+1),
			Hops:     1,
			SeqNo:    uint64(seq + 1),
		}},
	}
}

func routeLearnedVia(svc *Service, target, via domain.PeerIdentity) bool {
	for _, route := range svc.routingTable.Lookup(target) {
		if route.NextHop == via && route.Hops < routing.HopsInfinity {
			return true
		}
	}
	return false
}

// chatty_routes: a delta flood over a legacy session that named the victim
// used to quarantine the victim's identity, so the victim's own baseline was
// dropped on arrival for the whole cooldown.
func TestLegacyUnprovenChattyFloodDoesNotQuarantineNamedIdentity(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	attacker := legacyOutboundSession(legacyAttackerAddress, idPeerB)
	victim := legacyOutboundSession(legacyVictimAddress, idPeerB)

	for i := 0; i <= chattyAnnounceThreshold; i++ {
		svc.dispatchPeerSessionFrame(attacker.address, attacker, legacyDeltaFrame(i))
	}

	svc.dispatchPeerSessionFrame(victim.address, victim, legacyBaselineFrame(idTargetX))
	if !routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("the victim's own baseline was dropped: a legacy session that named it armed chatty_routes against its identity")
	}
	if svc.IsPeerTransitQuarantined(idPeerB) || svc.isSubjectInRouteQuarantine(provenIdentitySubject(idPeerB)) {
		t.Fatal("an identity no session proved must not be quarantined")
	}

	svc.dispatchPeerSessionFrame(attacker.address, attacker, legacyBaselineFrame(idTargetY))
	if routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("the flooding session itself must stay muted: its baseline was applied")
	}
}

// legacyFullBaselineFrame is a baseline at the receive-side frame cap whose
// routes all lead to identities beginning with prefix. At that size the frame
// costs more than the bucket refills during a test, so "was it applied" reads
// the bucket rather than the clock.
func legacyFullBaselineFrame(prefix string) protocol.Frame {
	routes := make([]protocol.AnnounceRouteFrame, maxRoutesPerAnnounceFrame)
	for i := range routes {
		target := fmt.Sprintf("%s%038x", prefix, i+1)
		routes[i] = protocol.AnnounceRouteFrame{Identity: target, Origin: target, Hops: 1, SeqNo: 1}
	}
	return protocol.Frame{Type: "announce_routes", AnnounceRoutes: routes}
}

// announceLimiter: a legacy session that named the victim used to spend the
// victim's route bucket, so the victim's own baseline was throttled.
func TestLegacyUnprovenAnnounceFloodDoesNotSpendNamedIdentityBudget(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	attacker := legacyOutboundSession(legacyAttackerAddress, idPeerB)
	victim := legacyOutboundSession(legacyVictimAddress, idPeerB)

	for spent := 0; spent < announceBurstRoutesPerPeer; spent += maxRoutesPerAnnounceFrame {
		svc.dispatchPeerSessionFrame(attacker.address, attacker, legacyFullBaselineFrame("dd"))
	}

	svc.dispatchPeerSessionFrame(victim.address, victim, legacyFullBaselineFrame("ee"))
	if !routeLearnedVia(svc, domain.PeerIdentityFromWire(fmt.Sprintf("ee%038x", 1)), idPeerB) {
		t.Fatal("the victim's own baseline was throttled: a legacy session that named it spent its announce budget")
	}

	svc.dispatchPeerSessionFrame(attacker.address, attacker, legacyFullBaselineFrame("ef"))
	if routeLearnedVia(svc, domain.PeerIdentityFromWire(fmt.Sprintf("ef%038x", 1)), idPeerB) {
		t.Fatal("the flooding session itself must stay throttled: its baseline was applied")
	}
}

// request_resync debounce: a legacy session that named the victim used to
// stamp the victim's debounce, so the victim's own request — the one that
// asks for the baseline it is missing — was dropped for the whole window.
func TestLegacyUnprovenResyncDoesNotDebounceNamedIdentity(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	attacker := legacyOutboundSession(legacyAttackerAddress, idPeerB)
	victim := legacyOutboundSession(legacyVictimAddress, idPeerB)
	registry := svc.announceLoop.StateRegistry()

	svc.dispatchPeerSessionFrame(attacker.address, attacker, protocol.Frame{Type: "request_resync"})
	if !registry.Get(idPeerB).View().NeedsFullResync {
		t.Fatal("precondition: a legacy session keeps its right to ask for a resync")
	}

	// The forced full sync the attacker asked for went out; the victim then
	// desyncs for real and asks on its own connection.
	registry.Get(idPeerB).RecordFullSyncSuccess(0, time.Now())
	svc.dispatchPeerSessionFrame(victim.address, victim, protocol.Frame{Type: "request_resync"})
	if !registry.Get(idPeerB).View().NeedsFullResync {
		t.Fatal("the victim's own request_resync was debounced by a legacy session that named it")
	}

	registry.Get(idPeerB).RecordFullSyncSuccess(0, time.Now())
	svc.dispatchPeerSessionFrame(attacker.address, attacker, protocol.Frame{Type: "request_resync"})
	if registry.Get(idPeerB).View().NeedsFullResync {
		t.Fatal("the asking session itself must stay debounced: its second request inside the window was accepted")
	}
}

// legacyInboundConn registers an accepted legacy connection from remote whose
// hello named claimed and passed auth_session — the relayable proof.
func legacyInboundConn(t *testing.T, svc *Service, id netcore.ConnID, remote *net.TCPAddr, claimed domain.PeerIdentity) domain.ConnID {
	t.Helper()
	local, peer := net.Pipe()
	t.Cleanup(func() { _ = local.Close() })
	t.Cleanup(func() { _ = peer.Close() })
	conn := &fakeConn{Conn: local, remoteAddr: remote}
	pc := netcore.New(id, conn, netcore.Inbound, netcore.Options{
		Address:  domain.PeerAddress(remote.String()),
		Identity: claimed,
		Caps:     legacyRoutingCaps,
	})
	pc.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: claimed.String()}})
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(conn, &connEntry{core: pc})
	svc.peerMu.Unlock()
	connID, ok := svc.connIDFor(conn)
	if !ok {
		t.Fatal("the test connection is not registered")
	}
	return connID
}

func dispatchInboundLegacyFrame(t *testing.T, svc *Service, connID domain.ConnID, frame protocol.Frame) {
	t.Helper()
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal %s: %v", frame.Type, err)
	}
	svc.dispatchNetworkFrame(connID, line)
}

// disconnect_storm: accepted legacy connections that named the victim and
// kept dropping used to quarantine the victim's identity — its announcements
// dropped and transit through it refused for the whole cooldown.
func TestLegacyUnprovenDisconnectStormDoesNotQuarantineNamedIdentity(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	svc.eventBus = newStormBus(t)
	svc.routeWithdrawalGracePeriodTest = -1
	attackerIP := net.ParseIP("10.0.0.66")

	for i := 0; i < quarantineDisconnectThreshold; i++ {
		remote := &net.TCPAddr{IP: attackerIP, Port: 40000 + i}
		connID := legacyInboundConn(t, svc, netcore.ConnID(100+i), remote, idPeerB)
		overlay := domain.PeerAddress(remote.String())
		svc.trackInboundConnect(connID, overlay, idPeerB)
		svc.trackInboundDisconnectWithPresenceEvidence(connID, overlay, nil)
	}

	if svc.IsPeerTransitQuarantined(idPeerB) {
		t.Fatal("transit through the victim is refused: accepted legacy connections that named it armed disconnect_storm against its identity")
	}
	victim := legacyOutboundSession(legacyVictimAddress, idPeerB)
	svc.dispatchPeerSessionFrame(victim.address, victim, legacyBaselineFrame(idTargetX))
	if !routeLearnedVia(svc, idTargetX, idPeerB) {
		t.Fatal("the victim's own baseline was dropped: accepted legacy connections that named it armed disconnect_storm against its identity")
	}

	again := legacyInboundConn(t, svc, netcore.ConnID(200), &net.TCPAddr{IP: attackerIP, Port: 41000}, idPeerB)
	dispatchInboundLegacyFrame(t, svc, again, legacyBaselineFrame(idTargetY))
	if routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("the flapping host itself must stay quarantined: its baseline was applied")
	}
}

// setup_failure_cycle: an address whose dialled legacy sessions kept failing
// setup used to quarantine whatever identity the welcome of the
// threshold-crossing dial named — any identity, chosen by the dialled peer.
func TestLegacyUnprovenSetupFailureCycleDoesNotQuarantineNamedIdentity(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	victimID := domain.PeerIdentityFromWire(testIdentityForNetworkConsumerTest(t).Address)

	for i := 0; i < setupFailureBanThreshold; i++ {
		local, peer := net.Pipe()
		t.Cleanup(func() { _ = local.Close() })
		t.Cleanup(func() { _ = peer.Close() })
		// No NetCore: initPeerSession's first request fails at once, which is
		// the setup failure under test.
		session := legacyOutboundSession(legacyAttackerAddress, victimID)
		session.conn = local
		svc.onCMSessionEstablished(SessionInfo{
			Address:        legacyAttackerAddress,
			Session:        session,
			SlotGeneration: uint64(i + 1),
		})
		svc.runLoopsWg.Wait()
	}

	if !svc.IsSetupFailureBanned(legacyAttackerAddress) {
		t.Fatal("precondition: the failing address itself is in setup-failure cooldown")
	}
	if svc.IsPeerTransitQuarantined(victimID) {
		t.Fatal("transit through the victim is refused: a dialled legacy address armed setup_failure_cycle against the identity its welcome named")
	}
}

// non-DM key-sync hop suppression: a dialled legacy session whose welcome
// named the victim and that pushed mostly unknown authors used to suppress
// the victim's identity as a hop, so for ten minutes no message the victim
// forwarded could buy this node the key it was missing.
func TestLegacyUnprovenUnattributedPushesDoNotSuppressNamedIdentityHop(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	// One pass permanently in flight: every admission answers busy, so no key
	// sync starts and dials, while the attribution under test is still
	// recorded on every arrival.
	limiter := newNonDMKeySyncLimiter(time.Now)
	limiter.inFlight["held-by-the-test"] = struct{}{}
	svc.nonDMKeySync = limiter
	attacker := legacyOutboundSession(legacyAttackerAddress, idPeerB)

	for i := 0; i < nonDMAttributionMinSample; i++ {
		svc.dispatchPeerSessionFrame(attacker.address, attacker, protocol.Frame{
			Type:  "push_message",
			Topic: "global",
			Item: &protocol.MessageFrame{
				ID:         fmt.Sprintf("unattributed-%d", i),
				Sender:     fabricatedAuthor(i),
				Recipient:  "*",
				Flag:       string(protocol.MessageFlagImmutable),
				CreatedAt:  time.Now().UTC().Format(time.RFC3339),
				TTLSeconds: 300,
				Body:       "noise",
			},
		})
	}

	svc.senderKeySyncMu.Lock()
	defer svc.senderKeySyncMu.Unlock()
	now := time.Now()
	suppressed := 0
	for hop := range limiter.hops {
		if !limiter.hopSuppressedLocked(hop, now) {
			continue
		}
		suppressed++
		if strings.Contains(hop, idPeerB.String()) {
			t.Fatalf("hop %q is suppressed: a legacy session that named the victim suppressed the victim's identity", hop)
		}
	}
	if suppressed == 0 {
		t.Fatal("the pushing session itself must stay suppressed: no hop is")
	}
}

// provenRoutingSender is a sender whose identity a v2 session proved — what
// the routing-plane tests that predate the penalty subject were written
// against.
func provenRoutingSender(id domain.PeerIdentity) routingSender {
	return routingSender{identity: id, penalty: provenIdentitySubject(id)}
}

// The inbound fixture above must be able to land a baseline at all, or "the
// flapping host stays quarantined" would hold on a dispatch that applies
// nothing.
func TestLegacyUnprovenInboundFixtureAppliesBaselines(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	svc.eventBus = newStormBus(t)
	connID := legacyInboundConn(t, svc, netcore.ConnID(300), &net.TCPAddr{IP: net.ParseIP("10.0.0.66"), Port: 42000}, idPeerB)

	dispatchInboundLegacyFrame(t, svc, connID, legacyBaselineFrame(idTargetY))
	if !routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("a legacy inbound baseline must reach the table when nothing is quarantined")
	}
}

// Onion peers all arrive from loopback. A flapping one is charged by its
// connection, so it cannot mute the next onion peer — which the source IP,
// shared by all of them, would.
func TestLegacyUnprovenLoopbackStormDoesNotMuteOtherLoopbackPeers(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	svc.eventBus = newStormBus(t)
	svc.routeWithdrawalGracePeriodTest = -1

	for i := 0; i < quarantineDisconnectThreshold; i++ {
		remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 43000 + i}
		connID := legacyInboundConn(t, svc, netcore.ConnID(400+i), remote, idPeerC)
		overlay := domain.PeerAddress(remote.String())
		svc.trackInboundConnect(connID, overlay, idPeerC)
		svc.trackInboundDisconnectWithPresenceEvidence(connID, overlay, nil)
	}

	other := legacyInboundConn(t, svc, netcore.ConnID(500), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 44000}, idPeerB)
	dispatchInboundLegacyFrame(t, svc, other, legacyBaselineFrame(idTargetY))
	if !routeLearnedVia(svc, idTargetY, idPeerB) {
		t.Fatal("another onion peer was muted by a flapping one: loopback must not be one subject")
	}
}

// provenOutboundSession is a dialled session whose peer proved its identity
// over v2: here the identity IS the subject. The proof comes from a real
// handshake (sessionv2test), the only place one can come from.
func provenOutboundSession(address domain.PeerAddress, peer sessionv2.Peer) *peerSession {
	session := legacyOutboundSession(address, peer.Identity)
	session.proven = &peer
	return session
}

// provenInboundAuth is the auth state of an accepted connection on which peer
// proved its identity over v2.
func provenInboundAuth(t *testing.T, peer sessionv2.Peer) *connauth.State {
	t.Helper()
	return connauth.ProvenBySessionV2(protocol.Frame{Address: peer.Identity.String()}, sessionv2test.Proof(t, peer))
}

// A proven identity keeps every protection it had: its chatty flood follows
// it to any other connection it opens, and its disconnect storm blocks
// transit through it.
func TestProvenIdentityKeepsIdentityKeyedProtection(t *testing.T) {
	t.Run("chatty flood mutes the identity on every connection", func(t *testing.T) {
		svc := newLegacyPenaltyService(t)
		peer, _ := sessionv2test.NewProvenPeer(t)
		flooding := provenOutboundSession(legacyAttackerAddress, peer)
		elsewhere := provenOutboundSession(legacyVictimAddress, peer)

		for i := 0; i <= chattyAnnounceThreshold; i++ {
			svc.dispatchPeerSessionFrame(flooding.address, flooding, legacyDeltaFrame(i))
		}
		svc.dispatchPeerSessionFrame(elsewhere.address, elsewhere, legacyBaselineFrame(idTargetX))
		if routeLearnedVia(svc, idTargetX, peer.Identity) {
			t.Fatal("a proven identity's quarantine must follow it to its other connections")
		}
	})

	t.Run("disconnect storm blocks transit through the identity", func(t *testing.T) {
		svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.routeWithdrawalGracePeriodTest = -1
		peer, _ := sessionv2test.NewProvenPeer(t)
		for i := 0; i < quarantineDisconnectThreshold; i++ {
			remote := &net.TCPAddr{IP: net.ParseIP("10.0.0.21"), Port: 45000 + i}
			connID := legacyInboundConn(t, svc, netcore.ConnID(600+i), remote, peer.Identity)
			svc.netCoreForID(connID).SetAuth(provenInboundAuth(t, peer))
			overlay := domain.PeerAddress(remote.String())
			svc.trackInboundConnect(connID, overlay, peer.Identity)
			svc.trackInboundDisconnectWithPresenceEvidence(connID, overlay, nil)
		}
		if !svc.IsPeerTransitQuarantined(peer.Identity) {
			t.Fatal("a proven identity's disconnect storm must block transit through it")
		}
	})
}

func TestPenaltySubjectOfAcceptedLegacyConnection(t *testing.T) {
	cases := []struct {
		name   string
		remote string
		want   penaltySubject
	}{
		{"external IPv4 drops the port", "10.0.0.66:40001", penaltySubject{space: penaltySubjectInboundHost, address: "10.0.0.66"}},
		{"external IPv6 drops the port", "[2001:db8::1]:40001", penaltySubject{space: penaltySubjectInboundHost, address: "2001:db8::1"}},
		{"loopback is the connection", "127.0.0.1:40001", penaltySubject{space: penaltySubjectConnection, conn: 7}},
		{"IPv6 loopback is the connection", "[::1]:40001", penaltySubject{space: penaltySubjectConnection, conn: 7}},
		{"an unreadable address is the connection", "pipe", penaltySubject{space: penaltySubjectConnection, conn: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := acceptedLegacySubject(7, tc.remote); got != tc.want {
				t.Fatalf("acceptedLegacySubject(%q) = %s, want %s", tc.remote, got, tc.want)
			}
		})
	}
	if !acceptedLegacySubject(0, "pipe").IsZero() {
		t.Fatal("no connection and no address names nobody")
	}
}

func TestPenaltySubjectOfCoreReadsTheProofNotTheHello(t *testing.T) {
	peer, _ := sessionv2test.NewProvenPeer(t)
	local, other := net.Pipe()
	t.Cleanup(func() { _ = local.Close() })
	t.Cleanup(func() { _ = other.Close() })
	conn := &fakeConn{Conn: local, remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.66"), Port: 40001}}
	core := netcore.New(netcore.ConnID(9), conn, netcore.Inbound, netcore.Options{Identity: peer.Identity})
	t.Cleanup(core.Close)
	legacy := penaltySubject{space: penaltySubjectInboundHost, address: "10.0.0.66"}

	if got := penaltySubjectOfCore(9, core); got != legacy {
		t.Fatalf("before auth = %s, want %s", got, legacy)
	}
	core.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: peer.Identity.String()}})
	if got := penaltySubjectOfCore(9, core); got != legacy {
		t.Fatalf("after a relayable auth_session = %s, want %s", got, legacy)
	}
	core.SetAuth(provenInboundAuth(t, peer))
	if got := penaltySubjectOfCore(9, core); got != provenIdentitySubject(peer.Identity) {
		t.Fatalf("after a v2 proof = %s, want %s", got, provenIdentitySubject(peer.Identity))
	}
}

func TestPenaltySubjectOfDialledSession(t *testing.T) {
	peer, _ := sessionv2test.NewProvenPeer(t)
	if got := legacyOutboundSession(legacyAttackerAddress, peer.Identity).penaltySubject(); got != dialledAddressSubject(legacyAttackerAddress) {
		t.Fatalf("legacy session = %s, want the dialled address", got)
	}
	if got := provenOutboundSession(legacyAttackerAddress, peer).penaltySubject(); got != provenIdentitySubject(peer.Identity) {
		t.Fatalf("v2 session = %s, want the proven identity", got)
	}
	described := legacyOutboundSession(legacyAttackerAddress, peer.Identity)
	described.proven = &sessionv2.Peer{Identity: peer.Identity}
	if got := described.penaltySubject(); got != dialledAddressSubject(legacyAttackerAddress) {
		t.Fatalf("a peer description no handshake produced = %s, want the dialled address", got)
	}
}
