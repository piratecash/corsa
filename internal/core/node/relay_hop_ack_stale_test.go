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
// table-routed reroute case (RouteOrigin set, ForwardedTo rotated to B).
// A late ack from the old next-hop A (identity != ForwardedTo's identity)
// must be ignored for suppression; a subsequent ack from the current
// downstream B marks observed.
func TestHandleRelayHopAck_StaleTableRoutedSenderDoesNotSuppressTimer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	// Sessions so resolvePeerIdentity maps addresses → identities.
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{peerIdentity: idPeerC, address: "addr-B"}

	// Currently forwarded to B (table-routed: RouteOrigin set); A is the
	// superseded prior next-hop. Budget armed, not yet observed.
	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-stale-table",
		ForwardedTo:          domain.PeerAddress("addr-B"),
		Recipient:            domain.PeerIdentity(idTargetX),
		RouteOrigin:          domain.PeerIdentity(idTargetX), // non-empty → table-routed stale branch
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		HopAckRemainingTicks: 5,
		HopAckObserved:       false,
	})

	// Late ack from the superseded downstream A.
	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-table", Status: "forwarded",
	})
	if observedHopAck(t, svc, "m-stale-table") {
		t.Fatal("stale table-routed ack from superseded A suppressed the current timer")
	}

	// Ack from the current downstream B.
	svc.handleRelayHopAck(domain.PeerAddress("addr-B"), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-table", Status: "forwarded",
	})
	if !observedHopAck(t, svc, "m-stale-table") {
		t.Fatal("current ack from B did not mark HopAckObserved")
	}
}

// TestHandleRelayHopAck_StaleGossipFallbackSenderDoesNotSuppressTimer —
// the table→gossip fallback case (RouteOrigin cleared, prior next-hop in
// AbandonedForwardedTo). A late ack from the abandoned next-hop A must be
// ignored for suppression; an ack from the current gossip target marks
// observed.
func TestHandleRelayHopAck_StaleGossipFallbackSenderDoesNotSuppressTimer(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)
	svc.relayStates = newRelayStateStore()

	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{peerIdentity: idPeerA, address: "addr-A"}
	svc.sessions[domain.PeerAddress("addr-G")] = &peerSession{peerIdentity: idPeerC, address: "addr-G"}

	// Failover cleared RouteOrigin and pushed A onto AbandonedForwardedTo;
	// the message is now out via gossip target G.
	svc.relayStates.store(&relayForwardState{
		MessageID:            "m-stale-gossip",
		ForwardedTo:          domain.PeerAddress("addr-G"),
		Recipient:            domain.PeerIdentity(idTargetX),
		RouteOrigin:          "", // empty → table→gossip fallback stale branch
		AbandonedForwardedTo: []domain.PeerAddress{"addr-A"},
		HopAckRemainingTicks: 5,
		HopAckObserved:       false,
	})

	// Late ack from the abandoned next-hop A.
	svc.handleRelayHopAck(domain.PeerAddress("addr-A"), protocol.Frame{
		Type: "relay_hop_ack", ID: "m-stale-gossip", Status: "forwarded",
	})
	if observedHopAck(t, svc, "m-stale-gossip") {
		t.Fatal("stale gossip-fallback ack from abandoned A suppressed the current timer")
	}

	// Ack from the current gossip target G.
	svc.handleRelayHopAck(domain.PeerAddress("addr-G"), protocol.Frame{
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
