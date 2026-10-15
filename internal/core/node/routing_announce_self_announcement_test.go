package node

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// selfAnnouncementSeqNo is far above the SeqNo our own Direct claim to
// idPeerB carries (1), so a self-announcement would win every SeqNo
// comparison against it if it ever reached the table.
const selfAnnouncementSeqNo = 1000

// A neighbour never announces a route to itself: its own identity has no
// bucket in its table, so its projection carries no entry for it. An entry
// with Identity == sender therefore describes the one slot only we own —
// the (sender, sender) Direct claim bound to the live session. Whatever the
// wire generation, live or withdrawal, it must neither replace that claim
// (an Announcement in its place is invalidated silently on disconnect, so
// the own-direct withdrawal never leaves) nor tombstone it (a withdrawn
// Direct claim is re-emitted to every other neighbour as our own withdrawal
// of a peer that is still connected).
func TestAnnounceIngest_SenderAnnouncingItselfLeavesDirectClaimIntact(t *testing.T) {
	sendLegacy := func(svc *Service, wireHops int) {
		svc.handleAnnounceRoutes(provenRoutingSender(idPeerB), legacyAnnounce(idPeerB, idPeerB, wireHops, selfAnnouncementSeqNo))
	}
	sendV2 := func(svc *Service, wireHops int) {
		// routes_update applies only on top of a baseline; mark it so the
		// delta reaches the per-entry checks instead of the resync gate.
		svc.announceLoop.StateRegistry().GetOrCreate(idPeerB).MarkBaselineReceived()
		frame := legacyAnnounce(idPeerB, idPeerB, wireHops, selfAnnouncementSeqNo)
		frame.Type = "routes_update"
		svc.handleRoutesUpdate(provenRoutingSender(idPeerB), domain.PeerAddress("addr-peerB"), frame)
	}
	sendV3 := func(svc *Service, wireHops int) {
		sendV3Full(svc, idPeerB, uint8(wireHops), selfAnnouncementSeqNo)
	}
	cases := []struct {
		name     string
		send     func(svc *Service, wireHops int)
		wireHops int
	}{
		{name: "legacy live", send: sendLegacy, wireHops: 0},
		{name: "legacy withdrawal", send: sendLegacy, wireHops: routing.HopsInfinity},
		{name: "v2 live", send: sendV2, wireHops: 0},
		{name: "v2 withdrawal", send: sendV2, wireHops: routing.HopsInfinity},
		{name: "v3 live", send: sendV3, wireHops: 0},
		{name: "v3 withdrawal", send: sendV3, wireHops: routing.HopsInfinity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := newSeqOrderFixture(t)
			direct := admitDirectPeerB(t, svc)

			tc.send(svc, tc.wireHops)

			assertDirectClaimToPeerBIntact(t, svc, direct)
		})
	}
}

// A route poison is the sender's statement that its own route to the
// named identity died. About the sender itself that statement is
// meaningless, and the slot it would hit is our live Direct claim:
// tombstoning it would make us advertise the withdrawal of a connected peer
// and fan a poison-reverse about it across the mesh.
func TestRoutePoison_SenderPoisoningItselfLeavesDirectClaimIntact(t *testing.T) {
	const issuedAt = "2026-10-01T12:00:00Z"
	cases := []struct {
		name   string
		poison func(svc *Service)
	}{
		{name: "route_poison_v1", poison: func(svc *Service) {
			svc.handleRoutePoison(provenRoutingSender(idPeerB), protocol.RoutePoisonFrame{
				Type:     protocol.RoutePoisonFrameType,
				Identity: idPeerB.String(),
				Reason:   protocol.RoutePoisonReasonUplinkLost,
				IssuedAt: issuedAt,
			})
		}},
		{name: "route_poison_v2", poison: func(svc *Service) {
			svc.handleRoutePoisonV2(provenRoutingSender(idPeerB), protocol.RoutePoisonV2Frame{
				Type:       protocol.RoutePoisonV2FrameType,
				Identities: []string{idPeerB.String()},
				Reason:     protocol.RoutePoisonReasonUplinkLost,
				IssuedAt:   issuedAt,
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := newSeqOrderFixture(t)
			direct := admitDirectPeerB(t, svc)

			tc.poison(svc)

			assertDirectClaimToPeerBIntact(t, svc, direct)
		})
	}
}

// Without a Direct claim (a routing peer that never got one, e.g. no
// mesh_relay_v1) a self-announcement must not create a route to the sender
// through the sender either.
func TestAnnounceIngest_SenderAnnouncingItselfCreatesNoRoute(t *testing.T) {
	svc, _ := newSeqOrderFixture(t)

	svc.handleAnnounceRoutes(provenRoutingSender(idPeerB), legacyAnnounce(idPeerB, idPeerB, 0, selfAnnouncementSeqNo))
	sendV3Full(svc, idPeerB, 1, selfAnnouncementSeqNo)

	if claim := svc.routingTable.InspectUplinkClaim(idPeerB, idPeerB); claim != nil {
		t.Fatalf("self-announcement stored a claim to the sender through the sender: %+v", *claim)
	}
}

// The refusal follows the other sender-controlled refusals: every refused
// entry is counted, and the reason gets one Warn per frame.
func TestAnnounceIngest_SelfAnnouncementRefusalLogsOneWarnPerFrame(t *testing.T) {
	// NOT parallel: it inspects the global zerolog output.
	var buf syncWriter
	origLogger := log.Logger
	log.Logger = zerolog.New(&buf).With().Logger()
	defer func() { log.Logger = origLogger }()

	svc, _ := newSeqOrderFixture(t)
	self := idPeerB.String()
	svc.handleAnnounceRoutes(provenRoutingSender(idPeerB), protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: self, Origin: self, Hops: 0, SeqNo: selfAnnouncementSeqNo},
			{Identity: self, Origin: self, Hops: routing.HopsInfinity, SeqNo: selfAnnouncementSeqNo + 1},
			{Identity: idTargetX.String(), Origin: self, Hops: 1, SeqNo: 1},
		},
	})

	got := warnSummaries(t, buf.String())["announce_rejected_self_announcement"]
	if len(got) != 1 {
		t.Fatalf("announce_rejected_self_announcement: %d Warn lines for one frame, want exactly 1", len(got))
	}
	if got[0].Count != 2 {
		t.Fatalf("announce_rejected_self_announcement: Warn reports count=%d, want 2", got[0].Count)
	}
	if routes := svc.routingTable.Lookup(idTargetX); len(routes) != 1 {
		t.Fatalf("the honest entry next to the refused ones must still land, got %+v", routes)
	}
}

// admitDirectPeerB registers idPeerB as a direct neighbour and returns the
// Direct claim it got.
func admitDirectPeerB(t *testing.T, svc *Service) routing.RouteEntry {
	t.Helper()
	if _, err := svc.routingTable.AddDirectPeer(idPeerB); err != nil {
		t.Fatalf("AddDirectPeer(idPeerB): %v", err)
	}
	claim := svc.routingTable.InspectUplinkClaim(idPeerB, idPeerB)
	if claim == nil || claim.Source != routing.RouteSourceDirect {
		t.Fatalf("AddDirectPeer did not store a Direct claim: %+v", claim)
	}
	return *claim
}

// assertDirectClaimToPeerBIntact checks the three observable consequences of
// the (idPeerB, idPeerB) Direct claim: it is stored unchanged, it is
// advertised live to another neighbour, and the disconnect of idPeerB still
// produces our own wire withdrawal for it.
func assertDirectClaimToPeerBIntact(t *testing.T, svc *Service, want routing.RouteEntry) {
	t.Helper()
	claim := svc.routingTable.InspectUplinkClaim(idPeerB, idPeerB)
	if claim == nil {
		t.Fatal("the Direct claim to idPeerB disappeared")
	}
	if claim.Source != want.Source || claim.IsWithdrawn() || claim.Hops != want.Hops || claim.SeqNo != want.SeqNo {
		t.Fatalf("Direct claim to idPeerB = {Source:%s Hops:%d SeqNo:%d Withdrawn:%v}, want {Source:%s Hops:%d SeqNo:%d Withdrawn:false}",
			claim.Source, claim.Hops, claim.SeqNo, claim.IsWithdrawn(), want.Source, want.Hops, want.SeqNo)
	}

	assertAdvertisedLive(t, svc.routingTable.AnnounceTo(idPeerC), idPeerB)

	removed, err := svc.routingTable.RemoveDirectPeer(idPeerB)
	if err != nil {
		t.Fatalf("RemoveDirectPeer(idPeerB): %v", err)
	}
	if len(removed.Withdrawals) != 1 || removed.Withdrawals[0].Identity != idPeerB {
		t.Fatalf("disconnect of idPeerB must emit our own withdrawal for it, got %+v", removed.Withdrawals)
	}
}

func assertAdvertisedLive(t *testing.T, announced []routing.AnnounceEntry, identity domain.PeerIdentity) {
	t.Helper()
	for _, entry := range announced {
		if entry.Identity != identity {
			continue
		}
		if entry.Hops != 1 {
			t.Fatalf("%s advertised with hops=%d, want the live direct hops=1", identity, entry.Hops)
		}
		return
	}
	t.Fatalf("%s is not advertised at all: %+v", identity, announced)
}
