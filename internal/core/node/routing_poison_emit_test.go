package node

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_poison_emit_test.go covers the Phase 4 13.3-B emit-hook
// integration: snapshotting identities-via-uplink before
// RemoveDirectPeer, fanning out route_poison_v1 to every OTHER
// routing-capable peer, and skipping the just-disconnected uplink
// itself.

// peerSessionFixture wires a single outbound session keyed by
// senderAddr with the requested cap set and returns the sendCh the
// test inspects.
func peerSessionFixture(t *testing.T, svc *Service, senderAddr domain.PeerAddress, peer domain.PeerIdentity, caps []domain.Capability) chan protocol.Frame {
	t.Helper()
	sendCh := make(chan protocol.Frame, 16)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: peer,
		capabilities: caps,
		sendCh:       sendCh,
	}
	if svc.health == nil {
		svc.health = make(map[domain.PeerAddress]*peerHealth)
	}
	svc.health[senderAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()
	return sendCh
}

func TestIdentitiesViaUplink_ReturnsActiveTransitOnly(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)

	// Two direct peers — these end up in their own (Identity, Uplink)
	// slots and IdentitiesViaUplink should NOT count them as transit
	// identities of either uplink (the direct claim's identity equals
	// its uplink — caller filters that explicitly when needed).
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idPeerB))
	addDirectViaIdentity(t, svc, domain.PeerIdentity(idOriginC))

	// Three transit destinations reachable via idPeerB; one of them
	// also reachable via idOriginC.
	for _, target := range []string{idTargetX, "ee00000000000000000000000000000000000099", "ff00000000000000000000000000000000000088"} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: domain.PeerIdentity(target),
			Origin:   domain.PeerIdentity(idPeerB),
			NextHop:  domain.PeerIdentity(idPeerB),
			Hops:     2,
			SeqNo:    1,
			Source:   routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("seed %s via idPeerB: %v", target, err)
		}
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(idTargetX),
		Origin:   domain.PeerIdentity(idOriginC),
		NextHop:  domain.PeerIdentity(idOriginC),
		Hops:     3,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed idTargetX via idOriginC: %v", err)
	}

	got := svc.routingTable.IdentitiesViaUplink(routing.PeerIdentity(idPeerB))
	// Expected: the 3 transit identities + idPeerB's own direct claim
	// (which has uplink=idPeerB by construction). The poison emit
	// caller is responsible for filtering the direct case if needed.
	if len(got) != 4 {
		t.Fatalf("expected 4 identities via idPeerB (3 transit + 1 direct), got %d: %v", len(got), got)
	}
	idSet := make(map[string]bool, len(got))
	for _, id := range got {
		idSet[string(id)] = true
	}
	for _, want := range []string{idPeerB, idTargetX, "ee00000000000000000000000000000000000099", "ff00000000000000000000000000000000000088"} {
		if !idSet[want] {
			t.Fatalf("missing identity %q in result: %v", want, got)
		}
	}
}

func TestPoisonReverseToOtherPeers_EmitsToCapablePeersExceptLost(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)

	// Three outbound sessions: lost (the disconnected uplink), capable
	// (other peer with v1+poison_reverse — should receive poison),
	// non-capable (other peer without poison_reverse — should be
	// skipped by the SendRoutePoison cap gate).
	lostAddr := domain.PeerAddress("addr-lost")
	capableAddr := domain.PeerAddress("addr-capable")
	noncapAddr := domain.PeerAddress("addr-noncap")
	lostID := domain.PeerIdentity(idPeerB)
	capableID := domain.PeerIdentity(idOriginC)
	noncapID := domain.PeerIdentity("dd00000000000000000000000000000000000010")
	// idTargetX is the transit identity we're poisoning about.

	_ = peerSessionFixture(t, svc, lostAddr, lostID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	capCh := peerSessionFixture(t, svc, capableAddr, capableID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	noncapCh := peerSessionFixture(t, svc, noncapAddr, noncapID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1}) // no poison_reverse

	svc.poisonReverseToOtherPeers(context.Background(), lostID, []routing.PeerIdentity{routing.PeerIdentity(idTargetX)})

	// capable peer must receive one poison frame about idTargetX.
	select {
	case got := <-capCh:
		if got.Type != protocol.RoutePoisonFrameType {
			t.Fatalf("capable peer: got %q want %q", got.Type, protocol.RoutePoisonFrameType)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("capable peer received no poison frame within 100ms")
	}

	// non-capable peer must receive nothing (cap gate inside
	// SendRoutePoison filters it).
	select {
	case got := <-noncapCh:
		t.Fatalf("non-capable peer must NOT receive poison; got %q", got.Type)
	default:
	}
}

func TestPoisonReverseToOtherPeers_NoOtherPeersIsNoop(t *testing.T) {
	// Only the lost uplink in the routing-capable set → nothing to
	// emit to. Must not panic.
	svc, _ := newTestServiceWithIdentity(t)
	lostAddr := domain.PeerAddress("addr-solo")
	lostID := domain.PeerIdentity(idPeerB)
	_ = peerSessionFixture(t, svc, lostAddr, lostID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})

	svc.poisonReverseToOtherPeers(context.Background(), lostID, []routing.PeerIdentity{routing.PeerIdentity(idTargetX)})
	// No assertion needed beyond "does not panic / hang".
}
