package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_announce_v3_test.go covers the Phase 4 compact-announce receive
// path handleRouteAnnounceV3 (phase-4 §3.1, overview §7.1): full-kind
// applies entries and establishes the baseline; delta-before-baseline asks
// for a resync; the dropped Origin is synthesised as the sender; the epoch
// watermark drops stale replays and triggers a resync on a reset.

// v3PeerSession wires an outbound session keyed by senderAddr so the
// handler's request_resync replies route through enqueuePeerFrame, and
// returns the send channel the test inspects.
func v3PeerSession(t *testing.T, svc *Service, senderAddr domain.PeerAddress, peer domain.PeerIdentity) chan protocol.Frame {
	t.Helper()
	sendCh := make(chan protocol.Frame, 4)
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: peer,
		// Realistic deployed cap set: a node that supports v3 also supports
		// v2 (localCapabilities advertises both). The v3 recovery fast-path
		// reuses the v2-gated request_resync escape hatch, so the session
		// must carry v2 for the resync reply to dispatch.
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
		sendCh:       sendCh,
	}
	svc.health = map[domain.PeerAddress]*peerHealth{senderAddr: {Connected: true}}
	svc.peerMu.Unlock()
	return sendCh
}

func TestHandleRouteAnnounceV3_FullAppliesEntriesAndSynthesisesOrigin(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: 1,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
		},
	}

	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"), frame)

	got := svc.routingTable.Lookup(idTargetX)
	if len(got) == 0 {
		t.Fatalf("v3 full frame did not land in the routing table")
	}
	// Hops = wire 1 + receiver convention 1 = 2; NextHop is the sender.
	if got[0].NextHop != idPeerB {
		t.Fatalf("NextHop synthesised wrong: got %q want %q", got[0].NextHop, idPeerB)
	}
	if got[0].Hops != 2 {
		t.Fatalf("hops: got %d want 2 (wire 1 + receiver +1)", got[0].Hops)
	}
	// Full kind must establish the receive-side baseline.
	if state := registry.Get(idPeerB); state == nil || !state.HasReceivedBaseline() {
		t.Fatalf("v3 full must establish receive baseline")
	}
}

func TestHandleRouteAnnounceV3_DeltaBeforeBaselineRequestsResync(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := v3PeerSession(t, svc, senderAddr, idPeerB)

	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindDelta,
		Epoch: 1,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
		},
	}

	svc.handleRouteAnnounceV3(idPeerB, senderAddr, frame)

	if got := svc.routingTable.Lookup(idTargetX); len(got) > 0 {
		t.Fatalf("delta before baseline must NOT be applied (entries=%d)", len(got))
	}
	select {
	case got := <-sendCh:
		if got.Type != "request_resync" {
			t.Fatalf("expected request_resync, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("delta-before-baseline did not emit request_resync")
	}
}

// TestHandleRouteAnnounceV3_QuarantinedSender_DropsSilentlyNoResync is the
// v3 counterpart of the v2 quarantine-gate regression test. The delta-
// before-baseline branch would otherwise emit SendRequestResync back to
// the quarantined peer (line ~1726 of routing_announce.go), violating
// the "silently dropped" contract in
// docs/refactoring/route-withdrawal-grace-period.md. Also asserts no
// AnnouncePeerState is created (top-level gate runs before
// GetOrCreate/ObserveV3Epoch).
func TestHandleRouteAnnounceV3_QuarantinedSender_DropsSilentlyNoResync(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	// Arm route quarantine BEFORE state creation. Critical: we are
	// asserting the handler does not even reach GetOrCreate, so the
	// registry must start empty for this peer.
	svc.peerMu.Lock()
	svc.armRouteQuarantineLocked(idPeerB, "test", time.Now())
	svc.peerMu.Unlock()

	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := v3PeerSession(t, svc, senderAddr, idPeerB)

	// Delta frame — the worst-case for our gate. Without the top-level
	// quarantine check, the baseline-gate branch would call
	// SendRequestResync(senderAddr).
	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindDelta,
		Epoch: 1,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
		},
	}

	svc.handleRouteAnnounceV3(idPeerB, senderAddr, frame)

	// 1. Delta must NOT be applied.
	if got := svc.routingTable.Lookup(idTargetX); len(got) > 0 {
		t.Fatalf("quarantined v3 sender bypassed gate; entries applied: %d", len(got))
	}
	// 2. No wire frame must have been emitted on the sender's send
	//    channel — the leak we are guarding against.
	select {
	case got := <-sendCh:
		t.Fatalf("quarantined v3 sender received routing-control traffic, got %q", got.Type)
	default:
	}
	// 3. AnnouncePeerState must not exist — top-level gate fires
	//    before GetOrCreate/ObserveV3Epoch.
	if state := svc.announceLoop.StateRegistry().Get(idPeerB); state != nil {
		t.Fatalf("quarantine gate must short-circuit before GetOrCreate; per-peer state was created")
	}
}

func TestHandleRouteAnnounceV3_StaleEpochDropped(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	// Advance the watermark to epoch 5 with a full frame (also applies).
	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"),
		protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: 5})

	// A later frame with a lower epoch is a stale-process replay: dropped
	// before any table mutation regardless of its entries.
	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"),
		protocol.RouteAnnounceV3Frame{
			Kind:  protocol.RouteAnnounceV3KindFull,
			Epoch: 2,
			Entries: []protocol.RouteAnnounceV3Entry{
				{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
			},
		})

	if got := svc.routingTable.Lookup(idTargetX); len(got) > 0 {
		t.Fatalf("stale-epoch frame must be dropped, but %d entries were applied", len(got))
	}
}

// TestHandleRouteAnnounceV3_EpochResetFullRebaselinesWithoutResync pins that
// a kind=full frame at a higher epoch is self-sufficient: it re-establishes
// the baseline for the new epoch and is applied, with NO redundant
// request_resync (full already carries the fresh table).
func TestHandleRouteAnnounceV3_EpochResetFullRebaselinesWithoutResync(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := v3PeerSession(t, svc, senderAddr, idPeerB)

	// Seed watermark + baseline at epoch 1.
	svc.handleRouteAnnounceV3(idPeerB, senderAddr,
		protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: 1})

	// Higher epoch + full → re-baseline and apply, no resync.
	svc.handleRouteAnnounceV3(idPeerB, senderAddr,
		protocol.RouteAnnounceV3Frame{
			Kind:  protocol.RouteAnnounceV3KindFull,
			Epoch: 9,
			Entries: []protocol.RouteAnnounceV3Entry{
				{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
			},
		})

	if got := svc.routingTable.Lookup(idTargetX); len(got) == 0 {
		t.Fatalf("epoch-reset full frame must be applied")
	}
	select {
	case got := <-sendCh:
		t.Fatalf("epoch-reset full must NOT emit a wire frame, got %q", got.Type)
	default:
	}
}

// TestHandleRouteAnnounceV3_EpochResetDeltaRequestsResync pins that a
// kind=delta frame at a higher epoch cannot be applied — the prior baseline
// described the old table — so the receiver asks the peer to resend a full
// baseline and drops the delta.
func TestHandleRouteAnnounceV3_EpochResetDeltaRequestsResync(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
	senderAddr := domain.PeerAddress("addr-peerB")
	sendCh := v3PeerSession(t, svc, senderAddr, idPeerB)

	// Seed watermark + baseline at epoch 1.
	svc.handleRouteAnnounceV3(idPeerB, senderAddr,
		protocol.RouteAnnounceV3Frame{Kind: protocol.RouteAnnounceV3KindFull, Epoch: 1})

	// Higher epoch + delta → baseline invalid → request_resync, delta dropped.
	svc.handleRouteAnnounceV3(idPeerB, senderAddr,
		protocol.RouteAnnounceV3Frame{
			Kind:  protocol.RouteAnnounceV3KindDelta,
			Epoch: 9,
			Entries: []protocol.RouteAnnounceV3Entry{
				{Identity: idTargetX.String(), Hops: 1, SeqNo: 1},
			},
		})

	if got := svc.routingTable.Lookup(idTargetX); len(got) > 0 {
		t.Fatalf("epoch-reset delta must NOT be applied (entries=%d)", len(got))
	}
	select {
	case got := <-sendCh:
		if got.Type != "request_resync" {
			t.Fatalf("expected request_resync on epoch-reset delta, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("epoch-reset delta did not emit request_resync")
	}
}
