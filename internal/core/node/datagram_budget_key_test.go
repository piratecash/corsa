package node

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_budget_key_test.go pins WHO the per-neighbour budget of §5 is
// charged to, and WHEN.
//
// Two defects live here, and they are one defect seen from two sides — a
// budget that the sender can steer:
//
//   - the key. On an outbound session the identity comes from welcome.Address
//     (applyWelcomeMetadata), which the REMOTE side writes: an empty one made
//     the ingress return before the charge and left the whole session
//     unbudgeted, and a new one per reconnect opened a new bucket with a full
//     burst. Nothing on that direction is proven — authenticatePeerSession
//     signs OUR reply to THEIR challenge — so the only key this node can defend
//     is the host:port it dialled;
//   - the moment. The capability gate stood ABOVE the charge on both
//     directions, so a neighbour that authenticated without mesh_datagram_v1
//     got the cheapest verdict on the node for free, at line rate.

// datagramTestPeerIP is the source address the inbound fixture of this file
// reports. It has to be a real host:port, which is the point of routableConn
// below.
const datagramTestPeerIP = "10.0.0.90"

// datagramTestSecondDstHex is a SECOND valid identity, used where a test needs
// the peer to rename itself between two connections.
const datagramTestSecondDstHex = "11a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4"

// routableConn gives a net.Pipe end a routable RemoteAddr.
//
// It exists to keep the ban-score assertions of this file HONEST. addBanScore
// keys on net.SplitHostPort(NetCore.RemoteAddr()), and a bare pipe reports
// "pipe", which fails the split and makes the whole ban path a no-op — so
// "the neighbour was not scored" would hold on such a fixture no matter what
// the code did. With a real host:port the assertion has a live subject, which
// the positive control in TestCapabilityDropChargesTheNeighbourSilently proves
// by making the very same counter move.
type routableConn struct {
	net.Conn
	remote net.Addr
}

func (c routableConn) RemoteAddr() net.Addr { return c.remote }

// hostPortAddr is a net.Addr that is a host:port string and nothing else.
type hostPortAddr string

func (a hostPortAddr) Network() string { return "tcp" }
func (a hostPortAddr) String() string  { return string(a) }

// newRoutableDatagramInboundFixture is newLayeredDatagramInboundFixture with a
// routable remote address, so the ban surface is reachable.
func newRoutableDatagramInboundFixture(t *testing.T, caps ...domain.Capability) (*Service, *netcoretest.Backend, domain.ConnID) {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:    "127.0.0.1:0",
		Type:             config.NodeTypeFull,
		TrustStorePath:   t.TempDir() + "/trust.json",
		EnableDatagramV1: true,
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must build the conveyor: the charge under test lives beside it")
	}
	requireDatagramPlane(t, svc)
	registerFixtureDatagramTypes(t, svc)

	connID := netcore.ConnID(7810)
	remote := datagramTestPeerIP + ":64646"
	backend.Register(connID, netcore.Inbound, remote)

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(connID, routableConn{Conn: serverPipe, remote: hostPortAddr(remote)}, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)
	// The VERIFIED hello is where the proof lives: connauth.VerifyAuthSession
	// checked a signature over a challenge this node generated against
	// Hello.Address, so that address — and nothing the peer says afterwards —
	// is what the ingress may key a budget on.
	pc.SetAuth(&connauth.State{Verified: true, Hello: protocol.Frame{Address: datagramTestDstHex}})
	pc.SetCapabilities(caps)
	pc.SetIdentity(domain.PeerIdentityFromWire(datagramTestDstHex))

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
	svc.peerMu.Unlock()

	return svc, backend, connID
}

// newDatagramSessionService builds a node with the conveyor open and no
// sessions, ready for welcomedOutboundSession below.
func newDatagramSessionService(t *testing.T) *Service {
	t.Helper()
	svc := newDatagramLayerService(t, true)
	if svc.datagramLayer() == nil {
		t.Fatal("the fixture must build the conveyor")
	}
	svc.runCtx = context.Background()
	requireDatagramPlane(t, svc)
	registerFixtureDatagramTypes(t, svc)
	return svc
}

// welcomedOutboundSession builds the session a dial to address would have
// produced from this welcome, through the PRODUCTION metadata path.
//
// It calls applyWelcomeMetadata rather than assigning peerIdentity by hand,
// because the defect under test IS that derivation: welcome.Address is the
// remote's own claim, and a test that set the field directly could not show
// what an empty or renamed claim does to the budget.
func welcomedOutboundSession(
	t *testing.T,
	svc *Service,
	address domain.PeerAddress,
	welcomeAddress string,
	advertised ...domain.Capability,
) *peerSession {
	t.Helper()
	session := &peerSession{
		address: address,
		// The datagram ingress keys its channel-relative state on this id and
		// refuses an arrival without one.
		connID:  domain.ConnID(7301),
		sendCh:  make(chan peerSendItem, 4),
		inboxCh: make(chan protocol.Frame, 8),
		errCh:   make(chan error, 4),
		authOK:  true,
	}
	names := make([]string, 0, len(advertised))
	for _, capability := range advertised {
		names = append(names, string(capability))
	}
	welcome := protocol.Frame{
		Address:      welcomeAddress,
		Capabilities: names,
	}
	// Resolved outside peerMu, exactly as openPeerSession resolves it.
	advertise := svc.localDatagramAdvertise()
	if !advertise.Endpoint {
		t.Fatal("the node under test does not advertise mesh_datagram_v1, so no session can negotiate it")
	}
	svc.peerMu.Lock()
	applyWelcomeMetadata(session, welcome, svc.cfg.EnableMeshRoutingV3, advertise)
	svc.sessions[address] = session
	svc.health[address] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()
	return session
}

// datagramAdmissionStats is the §4.1 step 1 ledger — the only place a charge is
// observable, and the observable every test in this file reads.
func datagramAdmissionStats(svc *Service) datagram.AdmissionStats {
	return svc.datagramLayer().admission.Stats()
}

// ---------------------------------------------------------------------------
// The capability drop is charged, and stays silent
// ---------------------------------------------------------------------------

// TestCapabilityDropChargesTheNeighbourSilently is the finding on BOTH
// directions: a neighbour that never negotiated mesh_datagram_v1 must pay for
// the frames it sends and get nothing else — no error frame, no tear-down, no
// ban score, whatever the dtype or class.
//
// The mutation this kills: restoring the `if !HasCapability { return }` guard
// above handleDatagramFrame on either dispatcher. The frames are then dropped
// just as silently, and charged to nobody — which is the whole defect, and
// which the Admitted/AdmittedBytes assertions below are the only witnesses to.
func TestCapabilityDropChargesTheNeighbourSilently(t *testing.T) {
	const frames = 5

	t.Run("inbound", func(t *testing.T) {
		svc, backend, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshRelayV1)
		line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

		for i := 0; i < frames; i++ {
			if !svc.dispatchNetworkFrame(connID, line) {
				t.Fatalf("frame %d tore the connection down: this plane drops silently (§2)", i)
			}
		}

		stats := datagramAdmissionStats(svc)
		if stats.Admitted != frames {
			t.Fatalf("charged %d frames, want %d — a gate above the budget is a free load channel", stats.Admitted, frames)
		}
		if stats.AdmittedBytes != uint64(frames*len(line)) {
			t.Fatalf("charged %d bytes, want %d (the whole wire line, %d times)",
				stats.AdmittedBytes, frames*len(line), frames)
		}
		if got := datagramObservedCount(svc); got != 0 {
			t.Fatalf("the conveyor decided on %d frames from a connection that never negotiated the plane", got)
		}
		select {
		case data := <-backend.Outbound(connID):
			t.Fatalf("the drop must be silent on the wire; got %q", data)
		default:
		}
		if score := banScoreForIP(svc, datagramTestPeerIP); score != 0 {
			t.Fatalf("ban score = %d: a missing capability is not misbehaviour (§4.4)", score)
		}

		// The ban observable is LIVE on this fixture — without this control the
		// assertion above would hold on any fixture whose RemoteAddr cannot be
		// split, and would prove nothing.
		banned, _, bannedID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		decoy := injectDuplicateJSONKey(t, line, `"ttl":9`)
		if !banned.dispatchNetworkFrame(bannedID, decoy) {
			t.Fatal("a header violation must not tear the connection down either")
		}
		if score := banScoreForIP(banned, datagramTestPeerIP); score == 0 {
			t.Fatal("the ban surface is unreachable on this fixture, so the assertion above is vacuous")
		}
	})

	t.Run("inbound_with_the_capability_is_still_served", func(t *testing.T) {
		svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
		line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

		for i := 0; i < frames; i++ {
			if !svc.dispatchNetworkFrame(connID, line) {
				t.Fatalf("frame %d tore the connection down", i)
			}
		}
		if stats := datagramAdmissionStats(svc); stats.Admitted != frames {
			t.Fatalf("a legitimate neighbour was charged for %d frames, want %d", stats.Admitted, frames)
		}
		if got := datagramObservedCount(svc); got != frames {
			t.Fatalf("the conveyor decided on %d frames, want %d — the fix must not have turned a working receive path into a drop", got, frames)
		}
	})

	t.Run("outbound_session", func(t *testing.T) {
		svc := newDatagramSessionService(t)
		line := mustDatagramLine(t, newNodeDatagram(t, nil))

		// Negotiates mesh_relay_v1 and nothing of this plane.
		session := welcomedOutboundSession(t, svc,
			domain.PeerAddress("198.51.100.11:64646"), datagramTestDstHex, domain.CapMeshRelayV1)
		if svc.sessionHasCapability(session, domain.CapMeshDatagramV1) {
			t.Fatal("the fixture negotiated mesh_datagram_v1: it cannot exercise the capability drop")
		}

		for i := 0; i < frames; i++ {
			svc.dispatchSessionDatagramLine(session, line)
		}

		stats := datagramAdmissionStats(svc)
		if stats.Admitted != frames {
			t.Fatalf("charged %d frames, want %d", stats.Admitted, frames)
		}
		if stats.AdmittedBytes != uint64(frames*len(line)) {
			t.Fatalf("charged %d bytes, want %d", stats.AdmittedBytes, frames*len(line))
		}
		if got := datagramObservedCount(svc); got != 0 {
			t.Fatalf("the conveyor decided on %d frames from a session that never negotiated the plane", got)
		}

		// Positive control on the same node: a session that DID negotiate the
		// plane is still served.
		served := welcomedOutboundSession(t, svc,
			domain.PeerAddress("198.51.100.12:64646"), datagramTestDstHex, domain.CapMeshDatagramV1)
		svc.dispatchSessionDatagramLine(served, line)
		if got := datagramObservedCount(svc); got != 1 {
			t.Fatalf("the conveyor decided on %d frames from a capable session, want 1", got)
		}
	})
}

// TestCapabilityDropIsCountedUnderItsOwnReason is the OTHER half of the same
// refusal: it must be silent on the wire and loud in the ledger.
//
// The charge alone is not enough to see it. AdmissionStats.Admitted moves for
// every frame this node reads, served or refused, so a neighbour off the plane
// pushing frames at line rate looked exactly like a busy healthy one — the only
// difference was a Debug line, which is not an observable in production. §10
// asks for "dropped by reason", and this refusal had no reason to be dropped
// under.
//
// What it must NOT do is become an inbound outcome: Metrics.Observed means
// "frames the conveyor decided on", and the conveyor never saw these. That is
// why the counter goes through ObserveDrop, and why the Observed assertion
// below is as much of the contract as the drop count.
//
// The mutation this kills: removing the ObserveDrop call from
// dropDatagramOffThePlane, or routing it through refuseDatagram instead — the
// first leaves the drop count at zero, the second moves Observed.
func TestCapabilityDropIsCountedUnderItsOwnReason(t *testing.T) {
	const frames = 3

	t.Run("inbound", func(t *testing.T) {
		svc, backend, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshRelayV1)
		line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

		for i := 0; i < frames; i++ {
			// A true here IS "the connection is alive": the dispatcher's bool is
			// what decides whether the reader keeps the socket (§2).
			if !svc.dispatchNetworkFrame(connID, line) {
				t.Fatalf("frame %d tore the connection down; this plane drops silently (§2)", i)
			}
		}

		if got := datagramDropCount(svc, datagram.DropPlaneNotNegotiated); got != frames {
			t.Fatalf("plane_not_negotiated drops = %d, want %d: the role gate leaves no trace in the ledger", got, frames)
		}
		if got := datagramObservedCount(svc); got != 0 {
			t.Fatalf("Observed = %d, want 0: a frame the conveyor never saw was counted as one it decided on", got)
		}
		// Still silent, still unpunished — the counter is for the operator, not
		// for the neighbour.
		select {
		case data := <-backend.Outbound(connID):
			t.Fatalf("the drop must be silent on the wire; got %q", data)
		default:
		}
		if score := banScoreForIP(svc, datagramTestPeerIP); score != 0 {
			t.Fatalf("ban score = %d: a missing capability is not misbehaviour (§4.4)", score)
		}
	})

	t.Run("outbound_session", func(t *testing.T) {
		svc := newDatagramSessionService(t)
		line := mustDatagramLine(t, newNodeDatagram(t, nil))

		session := welcomedOutboundSession(t, svc,
			domain.PeerAddress("198.51.100.31:64646"), datagramTestDstHex, domain.CapMeshRelayV1)
		for i := 0; i < frames; i++ {
			svc.dispatchSessionDatagramLine(session, line)
		}

		if got := datagramDropCount(svc, datagram.DropPlaneNotNegotiated); got != frames {
			t.Fatalf("plane_not_negotiated drops = %d, want %d on the dialled direction", got, frames)
		}
		if got := datagramObservedCount(svc); got != 0 {
			t.Fatalf("Observed = %d, want 0", got)
		}

		// The positive control on the same node: a session that DID negotiate
		// the plane is served and moves the OTHER counter, so neither assertion
		// above can hold because this node counts nothing or counts everything.
		served := welcomedOutboundSession(t, svc,
			domain.PeerAddress("198.51.100.32:64646"), datagramTestDstHex, domain.CapMeshDatagramV1)
		svc.dispatchSessionDatagramLine(served, line)
		if got := datagramObservedCount(svc); got != 1 {
			t.Fatalf("the conveyor decided on %d frames from a capable session, want 1", got)
		}
		if got := datagramDropCount(svc, datagram.DropPlaneNotNegotiated); got != frames {
			t.Fatalf("a negotiated session was counted as off the plane: drops = %d, want %d", got, frames)
		}
	})
}

// banScoreForIP reads the ban ledger of one IP. The ledger lives in the
// IP/advertise domain, so the read takes ipStateMu.
func banScoreForIP(svc *Service, ip string) int {
	svc.ipStateMu.RLock()
	defer svc.ipStateMu.RUnlock()
	return svc.bans[ip].Score
}

// ---------------------------------------------------------------------------
// A session whose peer named no identity is still billed
// ---------------------------------------------------------------------------

// TestOutboundSessionWithoutAnIdentityIsStillCharged is the first half of the
// key defect: welcome.Address is the REMOTE's claim, so a peer that sends a
// blank or unparseable one used to produce a zero identity, and the ingress
// returned on it BEFORE the charge — an unbudgeted session for as long as it
// stayed open, bought with an empty string.
//
// The mutation this kills: moving the identity check back above the charge in
// handleDatagramFrame, or keying the outbound budget on the identity again
// (the charge then lands on the zero key, which Admit refuses, and
// AdmittedBytes stays at zero).
func TestOutboundSessionWithoutAnIdentityIsStillCharged(t *testing.T) {
	const frames = 4

	for name, welcomeAddress := range map[string]string{
		"blank_welcome_address":   "",
		"garbage_welcome_address": "not-a-fingerprint",
	} {
		t.Run(name, func(t *testing.T) {
			svc := newDatagramSessionService(t)
			line := mustDatagramLine(t, newNodeDatagram(t, nil))
			address := domain.PeerAddress("198.51.100.21:64646")

			session := welcomedOutboundSession(t, svc, address, welcomeAddress, domain.CapMeshDatagramV1)
			// The fixture is only a probe if the production derivation really
			// produced no identity from this welcome.
			if !session.peerIdentity.IsZero() {
				t.Fatalf("welcome %q produced identity %s: the fixture cannot exercise the defect",
					welcomeAddress, session.peerIdentity)
			}

			for i := 0; i < frames; i++ {
				svc.dispatchSessionDatagramLine(session, line)
			}

			stats := datagramAdmissionStats(svc)
			if stats.Admitted != frames {
				t.Fatalf("charged %d frames, want %d — an unnamed peer bought an unbudgeted session", stats.Admitted, frames)
			}
			if stats.AdmittedBytes != uint64(frames*len(line)) {
				t.Fatalf("charged %d bytes, want %d", stats.AdmittedBytes, frames*len(line))
			}
			if stats.TrackedPeers != 1 {
				t.Fatalf("tracked %d buckets for one dialled address, want 1", stats.TrackedPeers)
			}
			// And the frames themselves are refused: the conveyor keys its
			// state on an identity, and this neighbour named none.
			if got := datagramDropCount(svc, datagram.DropMalformed); got != frames {
				t.Fatalf("malformed drops = %d, want %d", got, frames)
			}

			// Positive control: a session on another dial address whose welcome
			// DOES name an identity is served, so the assertions above cannot
			// pass on a node that refuses everything.
			named := welcomedOutboundSession(t, svc,
				domain.PeerAddress("198.51.100.22:64646"), datagramTestDstHex, domain.CapMeshDatagramV1)
			svc.dispatchSessionDatagramLine(named, line)
			if got := datagramDropCount(svc, datagram.DropMalformed); got != frames {
				t.Fatalf("a well-formed datagram from a named peer was counted malformed (drops = %d)", got)
			}
			if stats := datagramAdmissionStats(svc); stats.Admitted != frames+1 {
				t.Fatalf("the named peer's frame was charged %d times in total, want %d", stats.Admitted, frames+1)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// A reconnect under a new name finds the same bucket
// ---------------------------------------------------------------------------

// TestOutboundBudgetSurvivesAReconnectUnderANewName is the second half: the
// budget must not be resettable by the neighbour.
//
// The fixture runs TWO connections in sequence over one dial address, because
// that is the only shape that can tell a stable key from a steerable one: with
// the identity as the key the second connection opens a second bucket with a
// full burst, and the flood continues.
//
// The mutation this kills: keying the outbound charge on
// session.peerIdentity — the second dispatch then lands on a fresh bucket, is
// admitted, and both the DropAdmission and the TrackedPeers assertions fail.
func TestOutboundBudgetSurvivesAReconnectUnderANewName(t *testing.T) {
	svc := newDatagramSessionService(t)
	line := mustDatagramLine(t, newNodeDatagram(t, nil))
	address := domain.PeerAddress("203.0.113.5:64646")
	admission := svc.datagramLayer().admission

	// Connection one, calling itself datagramTestDstHex.
	first := welcomedOutboundSession(t, svc, address, datagramTestDstHex, domain.CapMeshDatagramV1)
	svc.dispatchSessionDatagramLine(first, line)
	if stats := datagramAdmissionStats(svc); stats.Admitted != 1 || stats.TrackedPeers != 1 {
		t.Fatalf("the first frame did not open exactly one bucket: %+v", stats)
	}

	// Connection two: the SAME dial address, a DIFFERENT claimed identity.
	// Registering it replaces the first session exactly as a reconnect does.
	second := welcomedOutboundSession(t, svc, address, datagramTestSecondDstHex, domain.CapMeshDatagramV1)
	if second.peerIdentity == first.peerIdentity {
		t.Fatal("the two connections claim the same identity: the fixture cannot show the key did not change")
	}

	// Drain the frame bucket of the DIALLED ADDRESS. The drain and the frame
	// that follows are microseconds apart and the bucket refills at
	// FramesPerSecond, so nothing here races the refill.
	key := datagram.DialedAddressKey(address)
	for i := 0; i < svc.datagramLayer().limits.Peer.FrameBurst; i++ {
		admission.Admit(key, 0)
	}
	if admission.Admit(key, 0) {
		t.Fatal("the drain did not empty the frame bucket")
	}

	before := datagramDropCount(svc, datagram.DropAdmission)
	svc.dispatchSessionDatagramLine(second, line)
	if got := datagramDropCount(svc, datagram.DropAdmission) - before; got != 1 {
		t.Fatalf("admission drops after the reconnect = %d, want 1: a new welcome.Address bought a fresh budget", got)
	}
	if stats := datagramAdmissionStats(svc); stats.TrackedPeers != 1 {
		t.Fatalf("tracked %d buckets for one dialled address across two connections, want 1", stats.TrackedPeers)
	}

	// Positive control: a session on a DIFFERENT dial address is a different
	// neighbour and is served, so the refusal above is about the budget and not
	// about the node having stopped accepting datagrams.
	other := welcomedOutboundSession(t, svc,
		domain.PeerAddress("203.0.113.6:64646"), datagramTestDstHex, domain.CapMeshDatagramV1)
	admitted := datagramAdmissionStats(svc).Admitted
	dropped := datagramDropCount(svc, datagram.DropAdmission)
	svc.dispatchSessionDatagramLine(other, line)
	if got := datagramAdmissionStats(svc).Admitted; got != admitted+1 {
		t.Fatalf("a neighbour on another dialled address was charged %d frames, want %d", got, admitted+1)
	}
	if got := datagramDropCount(svc, datagram.DropAdmission); got != dropped {
		t.Fatal("a neighbour on another dialled address was refused on the flooder's budget")
	}
}
