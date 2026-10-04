package node

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// legacy_datagram_budget_test.go pins the datagram half of N1
// (docs/refactoring/n1-legacy-residual.md §1): a legacy connection that names
// identity X — on an accepted connection by a relayable auth_session — is
// charged and owned by what this node observed of the socket, never by X, and
// it never reads as proven. Only a v2 session yields the proven-identity key.

// datagramTwinsFixture is a node with two accepted connections naming the same
// identity X: an impostor that passed a v1 auth_session from one host, and
// X's own v2 session from another.
type datagramTwinsFixture struct {
	svc      *Service
	peer     sessionv2.Peer
	impostor domain.ConnID
	genuine  domain.ConnID
}

func newDatagramTwinsFixture(t *testing.T) datagramTwinsFixture {
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
	requireDatagramPlane(t, svc)
	registerFixtureDatagramTypes(t, svc)

	peer, _ := sessionv2test.NewProvenPeer(t)
	legacy := &connauth.State{Verified: true, Hello: protocol.Frame{Address: peer.Identity.String()}}
	return datagramTwinsFixture{
		svc:      svc,
		peer:     peer,
		impostor: registerDatagramInboundConn(t, svc, backend, 7801, "10.0.0.66:40001", peer.Identity, legacy),
		genuine:  registerDatagramInboundConn(t, svc, backend, 7802, "10.0.0.21:40002", peer.Identity, provenInboundAuth(t, peer)),
	}
}

// registerDatagramInboundConn registers an accepted connection with a
// routable remote address, the given auth state and mesh_datagram_v1.
func registerDatagramInboundConn(
	t *testing.T,
	svc *Service,
	backend *netcoretest.Backend,
	id uint64,
	remote string,
	claimed domain.PeerIdentity,
	auth *connauth.State,
) domain.ConnID {
	t.Helper()
	connID := netcore.ConnID(id)
	backend.Register(connID, netcore.Inbound, remote)
	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	pc := netcore.New(connID, routableConn{Conn: serverPipe, remote: hostPortAddr(remote)}, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)
	pc.SetAuth(auth)
	pc.SetCapabilities([]domain.Capability{domain.CapMeshDatagramV1})
	pc.SetIdentity(claimed)
	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: pc})
	svc.peerMu.Unlock()
	return connID
}

// DG-1: the impostor spends its own budget to the last frame; X's v2 session
// is still admitted. A frozen clock keeps refill out of the verdict, so the
// assertion reads the buckets and not the speed of the machine.
func TestLegacyDatagramFloodDoesNotSpendTheProvenIdentitysBudget(t *testing.T) {
	fx := newDatagramTwinsFixture(t)
	frozen := time.Unix(1780000000, 0)
	fx.svc.datagramLayer().admission = datagram.NewPeerAdmission(datagram.AdmissionConfig{
		Clock: func() time.Time { return frozen },
	})
	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

	burst := datagram.DefaultLimits().Normalized().Peer.FrameBurst
	for i := 0; i <= burst; i++ {
		fx.svc.dispatchNetworkFrame(fx.impostor, line)
	}
	before := datagramAdmissionStats(fx.svc)
	if before.RefusedFrames == 0 {
		t.Fatal("precondition: the impostor exhausted its frame budget")
	}

	fx.svc.dispatchNetworkFrame(fx.genuine, line)
	after := datagramAdmissionStats(fx.svc)
	if after.Admitted != before.Admitted+1 || after.RefusedFrames != before.RefusedFrames {
		t.Fatalf("X's v2 frame was refused (admitted %d→%d, refused %d→%d): a legacy connection that named X spent X's budget",
			before.Admitted, after.Admitted, before.RefusedFrames, after.RefusedFrames)
	}

	fx.svc.dispatchNetworkFrame(fx.impostor, line)
	if again := datagramAdmissionStats(fx.svc); again.RefusedFrames != after.RefusedFrames+1 {
		t.Fatal("the impostor itself must stay throttled")
	}
}

// DG-2: only a v2 session yields the proven-identity key, in either
// direction; a legacy connection never does.
func TestDatagramProvenKeyComesOnlyFromV2(t *testing.T) {
	fx := newDatagramTwinsFixture(t)

	if got := fx.svc.inboundDatagramBudgetKey(fx.impostor).Space(); got == datagram.AdmissionKeySpaceProvenIdentity {
		t.Fatal("an accepted v1 connection is keyed as a proven identity: its auth_session can be relayed")
	}
	if got := fx.svc.inboundDatagramBudgetKey(fx.genuine).Space(); got != datagram.AdmissionKeySpaceProvenIdentity {
		t.Fatalf("an accepted v2 connection is keyed in %s, want proven_identity", got)
	}
	if fx.svc.inboundDatagramNeighbour(fx.impostor).budgetKey != fx.svc.inboundDatagramBudgetKey(fx.impostor) {
		t.Fatal("the pre-parse charges and the ingress must bill one key per connection")
	}

	legacy := legacyOutboundSession(domain.PeerAddress("198.51.100.30:64646"), fx.peer.Identity)
	legacy.connID = domain.ConnID(7811)
	if got := fx.svc.sessionDatagramNeighbour(legacy).budgetKey.Space(); got != datagram.AdmissionKeySpaceDialedAddress {
		t.Fatalf("a dialled v1 session is keyed in %s, want dialed_address", got)
	}
	proven := provenOutboundSession(domain.PeerAddress("198.51.100.31:64646"), fx.peer)
	proven.connID = domain.ConnID(7812)
	if got := fx.svc.sessionDatagramNeighbour(proven).budgetKey.Space(); got != datagram.AdmissionKeySpaceProvenIdentity {
		t.Fatalf("a dialled v2 session is keyed in %s, want proven_identity", got)
	}
}

// DG-5: the legacy key is the source host for an external address — two
// connections from one host share it, so a reconnect does not refill it —
// and the connection itself for loopback, where every onion peer arrives.
func TestLegacyDatagramKeyIsTheHostOrTheLoopbackConnection(t *testing.T) {
	fx := newDatagramTwinsFixture(t)
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	legacy := func() *connauth.State {
		return &connauth.State{Verified: true, Hello: protocol.Frame{Address: fx.peer.Identity.String()}}
	}
	sameHost := registerDatagramInboundConn(t, fx.svc, backend, 7821, "10.0.0.66:40999", fx.peer.Identity, legacy())
	onionA := registerDatagramInboundConn(t, fx.svc, backend, 7822, "127.0.0.1:41001", fx.peer.Identity, legacy())
	onionB := registerDatagramInboundConn(t, fx.svc, backend, 7823, "127.0.0.1:41002", fx.peer.Identity, legacy())

	if fx.svc.inboundDatagramBudgetKey(fx.impostor) != fx.svc.inboundDatagramBudgetKey(sameHost) {
		t.Fatal("two legacy connections from one external host must share one budget")
	}
	if fx.svc.inboundDatagramBudgetKey(onionA) == fx.svc.inboundDatagramBudgetKey(onionB) {
		t.Fatal("two loopback connections must not share a budget: one onion peer would spend every other's")
	}
	if fx.svc.inboundDatagramBudgetKey(onionA).IsZero() {
		t.Fatal("an authenticated legacy connection must stay billable")
	}
}

// DG-4 (compatibility): no type this node registers requires a proven
// neighbour, so moving legacy connections out of the proven namespace makes
// none of them unavailable to old nodes.
func TestNoRegisteredDatagramTypeRequiresAProvenNeighbour(t *testing.T) {
	svc := newDatagramLayerService(t, true)
	registered := 0
	for _, dtype := range []domain.DType{
		domain.DTypeGetIdentity, domain.DTypePostIdentity, domain.DTypePushIdentity, domain.DTypeDMControl,
	} {
		entry, ok := svc.datagramLayer().types.Lookup(dtype)
		if !ok {
			t.Fatalf("%s is not registered", dtype)
		}
		registered++
		if entry.RequiresProvenPeer() {
			t.Fatalf("%s requires a proven neighbour: legacy nodes would lose it", dtype)
		}
	}
	if got := len(svc.datagramLayer().types.DTypes()); got != registered {
		t.Fatalf("%d types registered, this list knows %d: review the new one against legacy availability", got, registered)
	}
}
