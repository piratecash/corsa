package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_route_quarantine_handler_wiring_test.go pins the HANDLER-level
// wiring of the deltas-only chatty_routes contract: the chatty trigger
// must be fed by DELTA frames (routes_update, v3 kind="delta") and NOT
// by full baselines (announce_routes, v3 kind="full") or the
// request_resync control frame. The state-machine unit tests in
// routing_route_quarantine_test.go drive recordInboundAnnounceAndMaybeArm
// directly; this file proves the actual receive handlers route only the
// right frame types into it.
//
// Method: pre-seed the per-peer announce window to one BELOW the chatty
// threshold, then deliver exactly ONE frame through the real handler.
// If the handler counts the frame toward chatty, threshold-1 + 1 crosses
// the line and the peer is quarantined; if it does not, the peer stays
// clean. (recordInboundAnnounceAndMaybeArm sits at the very top of each
// handler, before the quarantine gate, so the arm decision is observable
// even though a delta frame that DOES arm is then dropped by that gate.)

// seedChattyHistoryNearThreshold fills peerAnnounceHistory[peer] with
// chattyAnnounceThreshold-1 recent timestamps, so a single additional
// counted frame tips the sliding window over the trigger.
func seedChattyHistoryNearThreshold(svc *Service, peer domain.PeerIdentity) {
	now := time.Now()
	hist := make([]time.Time, chattyAnnounceThreshold-1)
	for i := range hist {
		hist[i] = now
	}
	svc.peerMu.Lock()
	if svc.peerAnnounceHistory == nil {
		svc.peerAnnounceHistory = make(map[domain.PeerIdentity][]time.Time)
	}
	svc.peerAnnounceHistory[peer] = hist
	svc.peerMu.Unlock()
}

func TestChattyWiring_OnlyDeltaFramesArmQuarantine(t *testing.T) {
	peer := idPeerB

	// --- Baselines must NOT arm chatty_routes ---

	t.Run("announce_routes baseline does NOT arm", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.announceLoop.StateRegistry().MarkReconnected(peer,
			[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
		seedChattyHistoryNearThreshold(svc, peer)

		svc.handleAnnounceRoutes(peer, protocol.Frame{
			Type:           "announce_routes",
			AnnounceRoutes: buildAnnounceRouteFrames(1),
		})

		if svc.IsPeerInRouteQuarantine(peer) {
			t.Fatal("full baseline (announce_routes) must NOT count toward chatty_routes")
		}
	})

	t.Run("v3 kind=full baseline does NOT arm", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.announceLoop.StateRegistry().MarkReconnected(peer,
			[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
		seedChattyHistoryNearThreshold(svc, peer)

		svc.handleRouteAnnounceV3(peer, domain.PeerAddress("addr-peerB"),
			protocol.RouteAnnounceV3Frame{
				Kind:    protocol.RouteAnnounceV3KindFull,
				Epoch:   1,
				Entries: buildRouteAnnounceV3Entries(1),
			})

		if svc.IsPeerInRouteQuarantine(peer) {
			t.Fatal("v3 kind=full must NOT count toward chatty_routes")
		}
	})

	// --- Deltas MUST arm chatty_routes ---

	t.Run("routes_update delta DOES arm", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.announceLoop.StateRegistry().MarkReconnected(peer,
			[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
		seedChattyHistoryNearThreshold(svc, peer)

		svc.handleRoutesUpdate(peer, domain.PeerAddress("addr-peerB"), protocol.Frame{
			Type:           "routes_update",
			AnnounceRoutes: buildAnnounceRouteFrames(1),
		})

		if !svc.IsPeerInRouteQuarantine(peer) {
			t.Fatal("v2 delta (routes_update) MUST count toward chatty_routes")
		}
	})

	t.Run("v3 kind=delta DOES arm", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.announceLoop.StateRegistry().MarkReconnected(peer,
			[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
		seedChattyHistoryNearThreshold(svc, peer)

		svc.handleRouteAnnounceV3(peer, domain.PeerAddress("addr-peerB"),
			protocol.RouteAnnounceV3Frame{
				Kind:    protocol.RouteAnnounceV3KindDelta,
				Epoch:   1,
				Entries: buildRouteAnnounceV3Entries(1),
			})

		if !svc.IsPeerInRouteQuarantine(peer) {
			t.Fatal("v3 kind=delta MUST count toward chatty_routes")
		}
	})

	// --- request_resync (control frame) must NOT arm chatty_routes ---

	t.Run("request_resync does NOT arm", func(t *testing.T) {
		svc := newTestServiceWithRouting(t, idNodeA)
		svc.eventBus = newStormBus(t)
		svc.announceLoop.StateRegistry().MarkReconnected(peer,
			[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2})
		seedChattyHistoryNearThreshold(svc, peer)

		svc.handleRequestResync(peer)

		if svc.IsPeerInRouteQuarantine(peer) {
			t.Fatal("request_resync (control frame) must NOT count toward chatty_routes")
		}
	})
}
