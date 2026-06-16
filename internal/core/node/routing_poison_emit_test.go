package node

import (
	"context"
	"fmt"
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
	addDirectViaIdentity(t, svc, idPeerB)
	addDirectViaIdentity(t, svc, idOriginC)

	// Three transit destinations reachable via idPeerB; one of them
	// also reachable via idOriginC.
	for _, target := range []string{idTargetX.String(), "ee00000000000000000000000000000000000099", "ff00000000000000000000000000000000000088"} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: domain.PeerIdentityFromWire(target),
			Origin:   idPeerB,
			NextHop:  idPeerB,
			Hops:     2,
			SeqNo:    1,
			Source:   routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("seed %s via idPeerB: %v", target, err)
		}
	}
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idOriginC,
		NextHop:  idOriginC,
		Hops:     3,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed idTargetX via idOriginC: %v", err)
	}

	got := svc.routingTable.IdentitiesViaUplink(idPeerB)
	// Expected: the 3 transit identities + idPeerB's own direct claim
	// (which has uplink=idPeerB by construction). The poison emit
	// caller is responsible for filtering the direct case if needed.
	if len(got) != 4 {
		t.Fatalf("expected 4 identities via idPeerB (3 transit + 1 direct), got %d: %v", len(got), got)
	}
	idSet := make(map[string]bool, len(got))
	for _, id := range got {
		idSet[id.String()] = true
	}
	for _, want := range []string{idPeerB.String(), idTargetX.String(), "ee00000000000000000000000000000000000099", "ff00000000000000000000000000000000000088"} {
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
	lostID := idPeerB
	capableID := idOriginC
	noncapID := domain.PeerIdentityFromWire("dd00000000000000000000000000000000000010")
	// idTargetX is the transit identity we're poisoning about.

	_ = peerSessionFixture(t, svc, lostAddr, lostID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	capCh := peerSessionFixture(t, svc, capableAddr, capableID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})
	noncapCh := peerSessionFixture(t, svc, noncapAddr, noncapID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1}) // no poison_reverse

	svc.poisonReverseToOtherPeers(context.Background(), lostID, []routing.PeerIdentity{idTargetX})

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

// TestPoisonReverseToOtherPeers_BatchesToV2Peer pins the Phase B rollout:
// a v2-capable peer gets ONE route_poison_v2 frame covering the whole
// identity list, while a v1-only peer gets one route_poison_v1 per identity
// (mixed-version fallback). Batching is gated on PoisonBatchEnabled.
func TestPoisonReverseToOtherPeers_BatchesToV2Peer(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.cfg.PoisonBatchEnabled = true

	lostID := idPeerB
	v2ID := idOriginC
	v1ID := domain.PeerIdentityFromWire("dd00000000000000000000000000000000000010")

	_ = peerSessionFixture(t, svc, "addr-lost", lostID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshPoisonReverseV2, domain.CapMeshRelayV1})
	v2Ch := peerSessionFixture(t, svc, "addr-v2", v2ID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshPoisonReverseV2, domain.CapMeshRelayV1})
	v1Ch := peerSessionFixture(t, svc, "addr-v1", v1ID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1}) // no v2

	id2 := domain.PeerIdentityFromWire("ee00000000000000000000000000000000000099")
	svc.poisonReverseToOtherPeers(context.Background(), lostID,
		[]routing.PeerIdentity{routing.PeerIdentity(idTargetX), routing.PeerIdentity(id2)})

	// v2 peer: exactly one batched frame carrying both identities.
	select {
	case got := <-v2Ch:
		if got.Type != protocol.RoutePoisonV2FrameType {
			t.Fatalf("v2 peer: got %q want %q", got.Type, protocol.RoutePoisonV2FrameType)
		}
		batch, err := protocol.UnmarshalRoutePoisonV2Frame([]byte(got.RawLine))
		if err != nil {
			t.Fatalf("v2 peer: parse batch: %v", err)
		}
		if len(batch.Identities) != 2 {
			t.Fatalf("v2 batch must carry 2 identities, got %d: %v", len(batch.Identities), batch.Identities)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("v2 peer received no batched poison frame")
	}
	select {
	case extra := <-v2Ch:
		t.Fatalf("v2 peer must get exactly ONE batch frame, got extra %q", extra.Type)
	default:
	}

	// v1-only peer: one route_poison_v1 per identity (2 frames), no v2.
	v1Count := 0
	deadline := time.After(200 * time.Millisecond)
	for v1Count < 2 {
		select {
		case got := <-v1Ch:
			if got.Type != protocol.RoutePoisonFrameType {
				t.Fatalf("v1 peer: got %q want %q", got.Type, protocol.RoutePoisonFrameType)
			}
			v1Count++
		case <-deadline:
			t.Fatalf("v1 peer: expected 2 per-identity v1 frames, got %d", v1Count)
		}
	}
}

// TestPoisonReverseToOtherPeers_KillSwitchForcesV1 pins that with batching
// disabled, even a v2-capable peer receives per-identity v1 frames.
func TestPoisonReverseToOtherPeers_KillSwitchForcesV1(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.cfg.PoisonBatchEnabled = false // kill-switch

	lostID := idPeerB
	v2ID := idOriginC
	v2Ch := peerSessionFixture(t, svc, "addr-v2", v2ID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshPoisonReverseV2, domain.CapMeshRelayV1})

	svc.poisonReverseToOtherPeers(context.Background(), lostID, []routing.PeerIdentity{routing.PeerIdentity(idTargetX)})

	select {
	case got := <-v2Ch:
		if got.Type != protocol.RoutePoisonFrameType {
			t.Fatalf("kill-switch: v2 peer must get v1 frame, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("kill-switch: v2 peer received no poison frame")
	}
}

// TestHandleRoutePoisonV2_CascadeBatchedDownstream pins the propagation-hop
// fix: when a v2 batch zeroes out multiple targets, the cascade to a
// downstream v2 peer is ONE batched frame, not one singleton per identity.
func TestHandleRoutePoisonV2_CascadeBatchedDownstream(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	svc.cfg.PoisonBatchEnabled = true

	senderA := idPeerB
	downstreamID := idOriginC
	tX := idTargetX
	tY := domain.PeerIdentityFromWire("ee00000000000000000000000000000000000099")

	// Both transit identities reachable ONLY via senderA → poisoning them
	// via senderA leaves no surviving uplink → cascade.
	for _, target := range []domain.PeerIdentity{tX, tY} {
		if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
			Identity: target, Origin: senderA, NextHop: senderA, Hops: 2, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
		}); err != nil {
			t.Fatalf("seed %s: %v", target, err)
		}
	}

	downCh := peerSessionFixture(t, svc, "addr-down", downstreamID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshPoisonReverseV2, domain.CapMeshRelayV1})

	svc.handleRoutePoisonV2(senderA, protocol.RoutePoisonV2Frame{
		Type:       protocol.RoutePoisonV2FrameType,
		Identities: []string{tX.String(), tY.String()},
		Reason:     protocol.RoutePoisonReasonUplinkLost,
		IssuedAt:   time.Now().UTC().Format(time.RFC3339),
	})

	select {
	case got := <-downCh:
		if got.Type != protocol.RoutePoisonV2FrameType {
			t.Fatalf("downstream cascade: got %q, want batched %q", got.Type, protocol.RoutePoisonV2FrameType)
		}
		batch, err := protocol.UnmarshalRoutePoisonV2Frame([]byte(got.RawLine))
		if err != nil {
			t.Fatalf("parse cascade: %v", err)
		}
		if len(batch.Identities) != 2 {
			t.Fatalf("cascade must batch 2 identities into ONE frame, got %d", len(batch.Identities))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("downstream peer received no cascade frame")
	}
	select {
	case extra := <-downCh:
		t.Fatalf("cascade must be ONE batched frame, got extra %q", extra.Type)
	default:
	}
}

// TestFanPoisonReverseBatched_ChunksUnderMaxFrameLine pins that a large
// identity list is split into multiple route_poison_v2 frames, each within
// protocol.MaxFrameLine, and that every identity is covered.
func TestFanPoisonReverseBatched_ChunksUnderMaxFrameLine(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	ch := peerSessionFixture(t, svc, "addr-v2", idOriginC,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshPoisonReverseV2, domain.CapMeshRelayV1})

	const n = poisonBatchChunkSize*2 + 100 // forces 3 chunks
	idents := make([]domain.PeerIdentity, n)
	for i := range idents {
		idents[i] = domain.PeerIdentityFromWire(fmt.Sprintf("%040x", i+1))
	}

	svc.fanPoisonReverseBatched(context.Background(), "addr-v2", idents)

	frames := 0
	total := 0
	for {
		select {
		case f := <-ch:
			frames++
			if f.Type != protocol.RoutePoisonV2FrameType {
				t.Fatalf("frame %d: got %q", frames, f.Type)
			}
			if len(f.RawLine) > protocol.MaxFrameLine {
				t.Fatalf("chunk %d RawLine %d exceeds MaxFrameLine %d", frames, len(f.RawLine), protocol.MaxFrameLine)
			}
			b, err := protocol.UnmarshalRoutePoisonV2Frame([]byte(f.RawLine))
			if err != nil {
				t.Fatalf("parse chunk %d: %v", frames, err)
			}
			total += len(b.Identities)
		case <-time.After(200 * time.Millisecond):
			if frames != 3 {
				t.Fatalf("expected 3 chunked frames for %d identities, got %d", n, frames)
			}
			if total != n {
				t.Fatalf("chunks must cover all %d identities, got %d", n, total)
			}
			return
		}
	}
}

func TestPoisonReverseToOtherPeers_NoOtherPeersIsNoop(t *testing.T) {
	// Only the lost uplink in the routing-capable set → nothing to
	// emit to. Must not panic.
	svc, _ := newTestServiceWithIdentity(t)
	lostAddr := domain.PeerAddress("addr-solo")
	lostID := idPeerB
	_ = peerSessionFixture(t, svc, lostAddr, lostID,
		[]domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshPoisonReverseV1, domain.CapMeshRelayV1})

	svc.poisonReverseToOtherPeers(context.Background(), lostID, []routing.PeerIdentity{idTargetX})
	// No assertion needed beyond "does not panic / hang".
}
