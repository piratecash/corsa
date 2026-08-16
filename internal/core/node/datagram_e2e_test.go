package node

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_e2e_test.go drives the whole plane over real TCP sessions: a
// datagram created on node A leaves through the layer's scheduler, crosses the
// wire, passes the strict parser, the §5 admission, the signature check and
// the conveyor on the far side, and reaches a registered handler.
//
// Two topologies, because they exercise different halves of §4.3:
//
//   - A → B: the direct-session branch of step 1, including the role gate and
//     the last-hop dtype gate;
//   - A → C → B: the ranked candidate list, the transit gate on C, the ttl
//     decrement and the second hop's direct branch.
//
// The dtype has to be DECLARED by the destination for either topology to
// deliver: §6.1 credits a peer with no type it did not list, so both nodes
// register the same fixture kit and their `dtypes` field carries it in full.
//
// A third topology, A → C → B with an EMPTY registry on C, covers the state
// PR-0 really ships: no handler for any type, and still a lawful relay.

// datagramE2EDType is the type the handler under test is registered for. A
// name of its own: the discovery kit (push_identity et al.) is registered
// by production wiring now, so the sink must not collide with it.
const datagramE2EDType = domain.DType("e2e_sink")

// registerDatagramSink registers a handler for datagramE2EDType on the node
// and returns a channel that receives the payload of every delivered frame.
//
// The registry is the layer's declared extension point, so wiring a type from
// a test is the same operation the first real protocol PR performs — not a
// back door around the production path.
//
// It also fills in the REST of the fixture kit, so every node in this file
// declares the same set and no topology fails on a type its destination merely
// happens not to have registered. The set reaches the wire as an explicit list
// either way: an endpoint always states its `dtypes` (§6.1).
func registerDatagramSink(t *testing.T, svc *Service) <-chan []byte {
	t.Helper()
	if svc.datagramLayer() == nil {
		t.Fatal("the node has no datagram layer to register a type on")
	}
	delivered := make(chan []byte, 4)
	err := svc.datagramLayer().types.Register(datagram.TypeRegistration{
		DType:   datagramE2EDType,
		Modes:   []domain.DatagramMode{domain.DatagramModeRouted},
		Classes: []domain.DatagramClass{domain.DatagramClassControl},
		Payload: datagram.PayloadSchema{Name: "datagram_e2e", Version: 1},
		// The sender of a ROUTED frame is authenticated by the frame's own
		// signature (header.SignedSrc), so this type reads nothing about the
		// NEIGHBOUR — which is what the declaration says. Leaving it at the
		// strict default would refuse the transit topology below at its last
		// hop: B reaches C by dialling it, so C's frames arrive at B on a
		// direction that proves nothing about C, and B would drop a frame whose
		// real sender it can verify itself.
		SenderProof: datagram.SenderProvenInPayload,
		Handler: datagram.HandlerFunc(func(
			_ context.Context,
			_ datagram.DeliveryContext,
			payload []byte,
		) datagram.HandlerResult {
			select {
			case delivered <- append([]byte(nil), payload...):
			default:
			}
			return datagram.AcceptDelivery()
		}),
	})
	if err != nil {
		t.Fatalf("register the datagram sink: %v", err)
	}
	registerFixtureDatagramTypes(t, svc)
	return delivered
}

// startDatagramNode starts a full node with the plane enabled and its fixture
// kit registered BEFORE the first handshake.
//
// The order is load-bearing, not tidiness: §6.1 fixes the declared type set
// for the lifetime of a session, so a type registered after the peers
// connected would never reach the wire.
func startDatagramNode(t *testing.T, listen string, bootstrap ...string) (*Service, <-chan []byte, func()) {
	t.Helper()
	var delivered <-chan []byte
	svc, stop := startDatagramNodeWithRegistry(t, listen, func(svc *Service) {
		delivered = registerDatagramSink(t, svc)
	}, bootstrap...)
	return svc, delivered, stop
}

// startEmptyRegistryDatagramNode starts a full node with the plane enabled and
// NOTHING registered — the exact state PR-0 ships. Such a node handles no
// dtype and says so with an explicitly empty `dtypes` (§6.1), while still
// advertising both capabilities of §6: it understands the envelope, and it
// forwards other people's frames.
func startEmptyRegistryDatagramNode(t *testing.T, listen string, bootstrap ...string) (*Service, func()) {
	t.Helper()
	return startDatagramNodeWithRegistry(t, listen, nil, bootstrap...)
}

// startDatagramNodeWithRegistry is the shared body of the two: build the node,
// let the caller fill its type registry, then start it.
func startDatagramNodeWithRegistry(
	t *testing.T,
	listen string,
	register func(*Service),
	bootstrap ...string,
) (*Service, func()) {
	t.Helper()
	cfg := deriveTestAdvertisePort(config.Node{
		ListenAddress:     listen,
		BootstrapPeers:    bootstrap,
		Type:              config.NodeTypeFull,
		EnableDatagramV1:  true,
		AllowPrivatePeers: true,
		PeersStatePath:    filepath.Join(t.TempDir(), "peers.json"),
	})
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate test identity: %v", err)
	}
	svc := NewService(cfg, id, nil)
	svc.disableRateLimiting = true
	svc.markPeerStateIntervalTest = -1
	if svc.datagramLayer() == nil {
		t.Fatal("the datagram layer was not constructed although the flag is on")
	}
	if register != nil {
		register(svc)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started, stop := startTestService(t, ctx, cancel, svc)
	return started, stop
}

// sendDatagramTo builds a routed datagram from svc to dst and places it
// through the layer's own local-send path — the same path §4.3 defines, with
// the same gates, the same reservation and the same candidate walk a real
// protocol would use.
func sendDatagramTo(t *testing.T, svc *Service, dst domain.PeerIdentity, payload string) datagram.SendOutcome {
	t.Helper()
	frame := newServiceDatagram(t, svc, dst, datagramE2EDType, domain.RoutePolicyBest)
	frame.Payload = []byte(payload)
	signed, err := protocol.SignDatagram(frame, domain.NetworkID(networkName), svc.identity.PrivateKey)
	if err != nil {
		t.Fatalf("SignDatagram: %v", err)
	}
	return svc.datagramLayer().pipeline.SendLocal(context.Background(), datagram.LocalSendOpts{Frame: signed})
}

// waitForDatagram waits for the sink to receive the expected payload.
func waitForDatagram(t *testing.T, delivered <-chan []byte, want string, budget time.Duration) {
	t.Helper()
	deadline := time.After(budget)
	for {
		select {
		case got := <-delivered:
			if string(got) == want {
				return
			}
			t.Fatalf("handler received %q, want %q", string(got), want)
		case <-deadline:
			t.Fatalf("no datagram reached the handler within %s", budget)
		}
	}
}

// TestDatagramDeliveredToDirectNeighbour is the two-node end-to-end case: A
// creates a datagram for B, the scheduler picks the direct session, and B's
// registered handler receives the payload.
func TestDatagramDeliveredToDirectNeighbour(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)

	nodeB, delivered, stopB := startDatagramNode(t, addrB)
	defer stopB()
	nodeA, _, stopA := startDatagramNode(t, addrA, normalizeAddress(addrB))
	defer stopA()

	dst := domain.PeerIdentityFromWire(nodeB.Address())

	// The send must not race the handshake: until the capability is
	// negotiated the peer is not a candidate at all, and the probe is the
	// exact predicate the send uses.
	waitForCondition(t, 15*time.Second, func() bool {
		return datagramReachableNow(t, nodeA, dst, datagramE2EDType)
	})

	outcome := sendDatagramTo(t, nodeA, dst, "direct-hop-payload")
	if outcome.Kind() != datagram.SendQueued {
		t.Fatalf("SendLocal outcome = %s, want queued", outcome)
	}
	if nextHop, ok := outcome.NextHop(); !ok || nextHop != dst {
		t.Fatalf("queued next hop = %v (ok=%v), want the destination itself", nextHop, ok)
	}
	waitForDatagram(t, delivered, "direct-hop-payload", 15*time.Second)

	// The receiving side must count the delivery on the SAME counter series
	// the wire-level refusals use — one series, not two. The counter is
	// written after the handler returns, so this is a wait, not a read: the
	// handler firing is the earlier event of the two.
	waitForCondition(t, 5*time.Second, func() bool {
		return nodeB.datagramMetrics.Snapshot().Delivered > 0
	})
}

// TestDatagramDeliveredThroughTransit is the three-node case: A and B share no
// session, C relays. It exercises the ranked candidate list on A, the transit
// gate and forward path on C, and the direct branch again on C → B.
func TestDatagramDeliveredThroughTransit(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)
	addrC := freeAddress(t)

	// C is the only node either end dials, so A and B never become
	// neighbours and the direct branch cannot answer for them.
	nodeC, _, stopC := startDatagramNode(t, addrC)
	defer stopC()
	nodeA, _, stopA := startDatagramNode(t, addrA, normalizeAddress(addrC))
	defer stopA()
	nodeB, delivered, stopB := startDatagramNode(t, addrB, normalizeAddress(addrC))
	defer stopB()

	dst := domain.PeerIdentityFromWire(nodeB.Address())

	// Wait for the mesh to converge far enough that A knows a route to B
	// through C. The probe is the right predicate again: it reads the fresh
	// lookup and applies the same candidate filters the send does.
	waitForCondition(t, 25*time.Second, func() bool {
		return datagramReachableNow(t, nodeA, dst, datagramE2EDType)
	})

	// Baseline AFTER convergence: by now the initial push_identity frames of
	// the discovery kit — legitimately addressed to C — have been delivered
	// at C, so only a DELTA during the transit hop would be a violation.
	deliveredAtRelayBefore := nodeC.datagramMetrics.Snapshot().Delivered
	forwardedBefore := nodeC.datagramMetrics.Snapshot().Forwarded

	outcome := sendDatagramTo(t, nodeA, dst, "transit-hop-payload")
	if outcome.Kind() != datagram.SendQueued {
		t.Fatalf("SendLocal outcome = %s, want queued", outcome)
	}
	nextHop, ok := outcome.NextHop()
	if !ok {
		t.Fatal("a queued outcome carried no next hop")
	}
	if nextHop == dst {
		t.Fatal("the frame went straight to the destination; the transit topology did not hold")
	}
	if nextHop != domain.PeerIdentityFromWire(nodeC.Address()) {
		t.Fatalf("next hop = %s, want the relay %s", nextHop, nodeC.Address())
	}

	waitForDatagram(t, delivered, "transit-hop-payload", 20*time.Second)

	// C must have counted a FORWARD, not a delivery: it is not the
	// destination and it has no handler for the type.
	waitForCondition(t, 5*time.Second, func() bool {
		return nodeC.datagramMetrics.Snapshot().Forwarded > forwardedBefore
	})
	if got := nodeC.datagramMetrics.Snapshot().Delivered; got != deliveredAtRelayBefore {
		t.Fatalf("the relay delivered a datagram addressed to somebody else (delivered %d → %d)", deliveredAtRelayBefore, got)
	}
}

// TestEmptyRegistryRelayCarriesSomebodyElsesDatagram is the case the plane
// could not do at all before the §6 amendment, and the reason it exists.
//
// C is a PR-0 node in its shipping state: the layer wired, the type registry
// EMPTY. It handles no dtype and declares exactly that (`"dtypes": []`), yet
// it understands the envelope and forwards, so it advertises both capabilities
// of §6 — and A may therefore pick it as the next hop for a frame addressed to
// B.
//
// With the endpoint capability withheld until a type kit is registered,
// C failed the candidate filter, which demands mesh_datagram_v1 from EVERY
// candidate including a purely transit one (§2.2 rule 2). A found no
// admissible hop and the whole plane carried nothing.
func TestEmptyRegistryRelayCarriesSomebodyElsesDatagram(t *testing.T) {
	t.Parallel()

	addrA := freeAddress(t)
	addrB := freeAddress(t)
	addrC := freeAddress(t)

	nodeC, stopC := startEmptyRegistryDatagramNode(t, addrC)
	defer stopC()
	nodeA, _, stopA := startDatagramNode(t, addrA, normalizeAddress(addrC))
	defer stopA()
	nodeB, delivered, stopB := startDatagramNode(t, addrB, normalizeAddress(addrC))
	defer stopB()

	// The relay really is in the state under test: both capabilities, and no
	// handler for the type it is about to carry (its declared set is the
	// production discovery kit and nothing else — the sink type is absent).
	advertise := nodeC.localDatagramAdvertise()
	if !advertise.Endpoint || !advertise.Transit {
		t.Fatalf("the relay advertises %+v, want both capabilities", advertise)
	}
	field := nodeC.localDTypeStrings(advertise)
	if field == nil || containsString(*field, datagramE2EDType.String()) {
		t.Fatalf("the relay declares %v, want a set without %s", field, datagramE2EDType)
	}

	dst := domain.PeerIdentityFromWire(nodeB.Address())
	waitForCondition(t, 25*time.Second, func() bool {
		return datagramReachableNow(t, nodeA, dst, datagramE2EDType)
	})

	// Baseline AFTER convergence: the initial push_identity frames addressed
	// to C are legitimate deliveries; the transit hop below must add none.
	deliveredAtRelayBefore := nodeC.datagramMetrics.Snapshot().Delivered
	forwardedBefore := nodeC.datagramMetrics.Snapshot().Forwarded

	outcome := sendDatagramTo(t, nodeA, dst, "empty-registry-relay-payload")
	if outcome.Kind() != datagram.SendQueued {
		t.Fatalf("SendLocal outcome = %s, want queued", outcome)
	}
	nextHop, ok := outcome.NextHop()
	if !ok {
		t.Fatal("a queued outcome carried no next hop")
	}
	if nextHop != domain.PeerIdentityFromWire(nodeC.Address()) {
		t.Fatalf("next hop = %s, want the empty-registry relay %s", nextHop, nodeC.Address())
	}

	waitForDatagram(t, delivered, "empty-registry-relay-payload", 20*time.Second)

	// The relay forwarded the sink frame and delivered nothing new: it has
	// no handler for the carried type, and it was not the destination.
	waitForCondition(t, 5*time.Second, func() bool {
		return nodeC.datagramMetrics.Snapshot().Forwarded > forwardedBefore
	})
	if got := nodeC.datagramMetrics.Snapshot().Delivered; got != deliveredAtRelayBefore {
		t.Fatalf("a relay without the type delivered somebody else's datagram (delivered %d → %d)", deliveredAtRelayBefore, got)
	}
}

// datagramReachableNow decodes the probe's JSON answer down to the one bit the
// e2e tests wait on. The answer carries its reason as well (§6.1), which the
// unit tests assert; here only the bit matters.
func datagramReachableNow(t *testing.T, svc *Service, dst domain.PeerIdentity, dtype domain.DType) bool {
	t.Helper()
	data, err := svc.DatagramReachable(context.Background(), dst, dtype)
	if err != nil {
		return false
	}
	var answer struct {
		Reachable bool `json:"reachable"`
	}
	if err := json.Unmarshal(data, &answer); err != nil {
		t.Fatalf("decode the reachability answer: %v", err)
	}
	return answer.Reachable
}
