package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestRelayStatesKeepCollidingMessageRefsAndExactReceiptPaths(t *testing.T) {
	a, b, c := domaintest.ID("relay-ref-a"), domaintest.ID("relay-ref-b"), domaintest.ID("relay-ref-c")
	refs := []domain.MessageRef{
		{Topic: "dm", ID: "shared", Sender: a.String(), Recipient: b.String()},
		{Topic: "dm", ID: "shared", Sender: c.String(), Recipient: b.String()},
		{Topic: "dm", ID: "shared", Sender: a.String(), Recipient: c.String()},
		{Topic: protocol.TopicControlDM, ID: "shared", Sender: a.String(), Recipient: b.String()},
	}
	rs := newRelayStateStore()
	for i, ref := range refs {
		if !rs.tryReserve(ref, domain.PeerAddress(ref.Sender)) || rs.tryReserve(ref, "other-upstream") {
			t.Fatalf("exact reservation %d failed to distinguish duplicate and neighbour", i)
		}
		rs.updateState(&relayForwardState{Message: ref, PreviousHop: domain.PeerAddress(ref.Sender), ReceiptForwardTo: domain.PeerAddress(ref.Sender), ForwardedTo: domain.PeerAddress(ref.Recipient), NextHopSelection: relayNextHopRoutingTable, RemainingTTL: 2})
	}
	if rs.count() != len(refs) || len(rs.byID["shared"]) != len(refs) {
		t.Fatal("wire-ID collision merged primary or legacy candidate state")
	}
	s := &Service{relayStates: rs}
	for _, ref := range refs[:3] {
		receipt := protocol.DeliveryReceipt{MessageID: protocol.MessageID(ref.ID), Sender: ref.Recipient, Recipient: ref.Sender, Status: protocol.ReceiptStatusDelivered}
		if got := s.relayHopForReceipt(receipt); got != domain.PeerAddress(ref.Sender) {
			t.Fatalf("reverse receipt selected another author: %v = %s", ref, got)
		}
		receipt.Status = protocol.ReceiptStatusSeenAck
		receipt.Sender, receipt.Recipient = ref.Sender, ref.Recipient
		if got := s.relayHopForReceipt(receipt); got != domain.PeerAddress(ref.Recipient) {
			t.Fatalf("seen ACK selected another recipient: %v = %s", ref, got)
		}
	}
	unknown := protocol.DeliveryReceipt{MessageID: "shared", Sender: b.String(), Recipient: domaintest.ID("relay-ref-unrelated").String(), Status: protocol.ReceiptStatusDelivered}
	if got := s.relayHopForReceipt(unknown); got != "" {
		t.Fatal("unknown receipt parties borrowed a reverse path", got)
	}
	rs.release(refs[0])
	if rs.hasSeen(refs[0]) || !rs.hasSeen(refs[1]) || len(rs.byID["shared"]) != len(refs)-1 {
		t.Fatal("exact release lost a neighbour or retained its legacy index")
	}
	rs.decrementTTLs()
	rs.decrementTTLs()
	if rs.count() != 0 || len(rs.byID) != 0 || len(rs.perPeer) != 0 {
		t.Fatal("TTL retirement leaked exact state, legacy candidate index, or quota")
	}
}

func TestLegacyRelayHopACKRefusesAmbiguityWithoutConsumingEitherState(t *testing.T) {
	a, b, c := domaintest.ID("relay-ack-a"), domaintest.ID("relay-ack-b"), domaintest.ID("relay-ack-c")
	hop, otherHop := domaintest.ID("relay-ack-hop"), domaintest.ID("relay-ack-other-hop")
	refs := []domain.MessageRef{{Topic: "dm", ID: "same-wire-id", Sender: a.String(), Recipient: b.String()}, {Topic: "dm", ID: "same-wire-id", Sender: c.String(), Recipient: b.String()}}
	for _, distinctHop := range []bool{false, true} {
		name := "ambiguous-same-hop"
		if distinctHop {
			name = "one-qualified-hop"
		}
		t.Run(name, func(t *testing.T) {
			rs := newRelayStateStore()
			for i, ref := range refs {
				via := hop
				if distinctHop && i == 1 {
					via = otherHop
				}
				if !rs.store(&relayForwardState{Message: ref, PreviousHop: "upstream", ForwardedTo: domain.PeerAddress(via.String()), ForwardedVia: routingSender{identity: via}, Recipient: b, NextHopSelection: relayNextHopRoutingTable, RemainingTTL: 2, HopAckRemainingTicks: 9, FrameLine: ref.Key()}) {
					t.Fatal("collision fixture did not store both exact messages")
				}
			}
			recipient, accepted := rs.consumeHopAck("same-wire-id", hop)
			if accepted != distinctHop || (accepted && recipient != b) || (!accepted && !recipient.IsZero()) {
				t.Fatalf("legacy ACK = (%s,%v), unique qualifying ref=%v", recipient, accepted, distinctHop)
			}
			for i, ref := range refs {
				state := rs.states[ref]
				wantConsumed := distinctHop && i == 0
				if state.HopAckObserved != wantConsumed || (state.FrameLine == "") != wantConsumed || state.HopAckRemainingTicks != 9 {
					t.Fatalf("legacy ACK consumed another state or mutated a refused ambiguity: %v %+v", ref, state)
				}
			}
		})
	}
}
