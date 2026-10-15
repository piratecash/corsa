package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// relay_hop_ack_stale_test.go covers the Phase 3 review fix: a late
// relay_hop_ack from a downstream that has been SUPERSEDED by a failover
// must not flip HopAckObserved on the state now tracking the current
// downstream. The stale-sender guard therefore runs BEFORE
// markHopAckObserved, not only before route confirmation. Otherwise the
// current attempt's budget timer is silenced and never fails over.

// TestHandleRelayHopAck_StaleTableRoutedSenderDoesNotSuppressTimer — the
// table-routed reroute case (relayNextHopRoutingTable, ForwardedTo rotated to B).
// A late ack from the old next-hop A (identity != ForwardedTo's identity)
// must be ignored for suppression; a subsequent ack from the current
// downstream B marks observed.
func TestHandleRelayHopAck_StaleTableRoutedSenderDoesNotSuppressTimer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Sessions so resolvePeerIdentity maps addresses → identities.
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{peerIdentity: idPeerC, address: "addr-B"}

	// Currently forwarded to B (table-routed); A is the
	// superseded prior next-hop. Budget armed, not yet observed.
	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-stale-table",
		ForwardedTo:          domain.PeerAddress("addr-B"),
		Recipient:            idTargetX,
		NextHopSelection:     relayNextHopRoutingTable,
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		HopAckRemainingTicks: 5,
		HopAckObserved:       false,
	})

	// Late ack from the superseded downstream A.
	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), sessionRoutingSender(svc.sessions["addr-A"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-table", Status: "forwarded",
	})
	if observedHopAck(t, svc, "m-stale-table") {
		t.Fatal("stale table-routed ack from superseded A suppressed the current timer")
	}

	// Ack from the current downstream B.
	svc.handleRelayHopAck(domain.PeerAddress("addr-B"), sessionRoutingSender(svc.sessions["addr-B"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-table", Status: "forwarded",
	})
	if !observedHopAck(t, svc, "m-stale-table") {
		t.Fatal("current ack from B did not mark HopAckObserved")
	}
}

// TestHandleRelayHopAck_StaleGossipFallbackSenderDoesNotSuppressTimer —
// the table→gossip fallback case (selection overwritten, prior next-hop in
// AbandonedForwardedTo). A late ack from the abandoned next-hop A must be
// ignored for suppression; an ack from the current gossip target marks
// observed.
func TestHandleRelayHopAck_StaleGossipFallbackSenderDoesNotSuppressTimer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-G")] = &peerSession{peerIdentity: idPeerC, address: "addr-G"}

	// The gossip resend overwrote the selection and pushed A onto AbandonedForwardedTo;
	// the message is now out via gossip target G.
	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-stale-gossip",
		ForwardedTo:          domain.PeerAddress("addr-G"),
		Recipient:            idTargetX,
		NextHopSelection:     relayNextHopGossipFanOut,
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		HopAckRemainingTicks: 5,
		HopAckObserved:       false,
	})

	// Late ack from the abandoned next-hop A.
	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), sessionRoutingSender(svc.sessions["addr-A"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-gossip", Status: "forwarded",
	})
	if observedHopAck(t, svc, "m-stale-gossip") {
		t.Fatal("stale gossip-fallback ack from abandoned A suppressed the current timer")
	}

	// Ack from the current gossip target G.
	svc.handleRelayHopAck(domain.PeerAddress("addr-G"), sessionRoutingSender(svc.sessions["addr-G"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-gossip", Status: "forwarded",
	})
	if !observedHopAck(t, svc, "m-stale-gossip") {
		t.Fatal("current ack from gossip target G did not mark HopAckObserved")
	}
}

// observedHopAck reads HopAckObserved for a message under the relay state
// store lock.
func observedHopAck(t *testing.T, svc *Service, messageID string) bool {
	t.Helper()
	svc.relayStates.mu.Lock()
	defer svc.relayStates.mu.Unlock()
	st := svc.relayStates.states[messageID]
	if st == nil {
		t.Fatalf("relay state for %q missing", messageID)
	}
	return st.HopAckObserved
}

// TestHandleRelayHopAck_TableRoutedAttemptIgnoresAckFromAnyOtherIdentity
// pins what distinguishes a table-routed attempt in the stale-ack guard:
// the routing table named exactly one next hop, so an ack from ANY other
// identity — even one that was never a previous next hop of this message
// — must neither suppress the current timer nor confirm a route.
func TestHandleRelayHopAck_TableRoutedAttemptIgnoresAckFromAnyOtherIdentity(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{peerIdentity: idPeerC, address: "addr-B"}

	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-table-other",
		ForwardedTo:          domain.PeerAddress("addr-B"),
		Recipient:            idTargetX,
		NextHopSelection:     relayNextHopRoutingTable,
		HopAckRemainingTicks: 5,
	})

	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), sessionRoutingSender(svc.sessions["addr-A"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-table-other", Status: "forwarded",
	})
	if observedHopAck(t, svc, "m-table-other") {
		t.Fatal("ack from an identity the routing table did not pick suppressed the table-routed timer")
	}
}

// TestHandleRelayHopAck_UntabledAttemptRejectsOnlyAbandonedHops pins the
// other side of the guard: an attempt whose next hop was not pinned by a
// routing-table decision (gossip fan-out, direct session, failover) can
// only prove an ack stale through the abandoned-next-hop list, so an ack
// from an identity that was never abandoned is treated as current.
func TestHandleRelayHopAck_UntabledAttemptRejectsOnlyAbandonedHops(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-G")] = &peerSession{peerIdentity: idPeerC, address: "addr-G"}

	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-gossip-other",
		ForwardedTo:          domain.PeerAddress("addr-G"),
		Recipient:            idTargetX,
		NextHopSelection:     relayNextHopGossipFanOut,
		HopAckRemainingTicks: 5,
	})

	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), sessionRoutingSender(svc.sessions["addr-A"]), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-gossip-other", Status: "forwarded",
	})
	if !observedHopAck(t, svc, "m-gossip-other") {
		t.Fatal("ack from a never-abandoned identity was rejected on an attempt the routing table did not pin")
	}
}
