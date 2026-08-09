package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_write_ticket_test.go pins the hand-over between the layer and the
// node's writer:
//
//   - every frame reaches the writer with its class contract — the write grace
//     and the send deadline. That is the property the whole walk rests on, so
//     it is pinned here rather than left to a comment;
//   - the candidate walk carries that one contract to every connection it tries
//     and stops at the one that takes the frame, so a peer with several sockets
//     writes the frame under the same deadline whichever socket accepts it.

// candidateSend is a scripted stand-in for the two tracked senders: it records
// the ticket each candidate was offered and answers accept or refuse from a
// script.
type candidateSend struct {
	accepts []bool
	tickets []*netcore.WriteTicket
	calls   int
}

func (c *candidateSend) send(_ datagramSendTarget, _ protocol.Frame, ticket *netcore.WriteTicket) bool {
	index := c.calls
	c.calls++
	c.tickets = append(c.tickets, ticket)
	return index < len(c.accepts) && c.accepts[index]
}

// threeTargets is two outbound sessions and one accepted connection — the two
// tiers the walk crosses. The sessions are bare objects: nothing here reaches a
// queue, because the sender is scripted.
func threeTargets() []datagramSendTarget {
	return []datagramSendTarget{
		{session: &peerSession{address: domain.PeerAddress("a")}, connID: domain.ConnID(1)},
		{session: &peerSession{address: domain.PeerAddress("b")}, connID: domain.ConnID(2)},
		{connID: domain.ConnID(3)},
	}
}

// TestEmitToStopsAtTheConnectionThatTookTheFrame is the walk's whole contract:
// the bytes are offered to one socket at a time and the first acceptance ends
// it. Walking on after an acceptance would put the same frame on a second
// connection of the peer.
func TestEmitToStopsAtTheConnectionThatTookTheFrame(t *testing.T) {
	sender := &candidateSend{accepts: []bool{false, false, true}}

	if !emitOverCandidates(
		threeTargets(),
		protocol.Frame{Type: protocol.DatagramFrameType},
		netcore.OutboundWrite{WriteGrace: time.Second},
		sender.send,
	) {
		t.Fatal("the third candidate accepted the frame, EmitTo must report true")
	}
	if sender.calls != 3 {
		t.Fatalf("the walk offered the frame to %d candidates, want 3", sender.calls)
	}
}

// Every candidate refusing is the other half: the walk reports false, and the
// frame provably never left.
func TestEmitToReportsFalseWhenEveryCandidateRefuses(t *testing.T) {
	sender := &candidateSend{accepts: []bool{false, false, false}}

	if emitOverCandidates(
		threeTargets(),
		protocol.Frame{Type: protocol.DatagramFrameType},
		netcore.OutboundWrite{WriteGrace: time.Second},
		sender.send,
	) {
		t.Fatal("no candidate accepted, EmitTo must report false")
	}
}

// TestEmitToCarriesOneContractToEveryCandidate pins what the walk owes each
// connection it tries: the class contract, unchanged.
//
// The ticket is the only thing carrying that contract — the send deadline and
// the write grace — into the writer, and it is a read-only carrier: the writer
// asks it two questions before the socket write and never writes to it (see
// netcore/write_ticket.go). So the whole send needs exactly one, and every
// candidate must be offered that one: a walk that minted a fresh ticket per
// refused socket would pay an allocation for each fallback and, worse, invite a
// future field whose value differs between candidates of the SAME frame.
func TestEmitToCarriesOneContractToEveryCandidate(t *testing.T) {
	sender := &candidateSend{accepts: []bool{false, false, true}}

	emitOverCandidates(
		threeTargets(),
		protocol.Frame{Type: protocol.DatagramFrameType},
		netcore.OutboundWrite{WriteGrace: time.Second},
		sender.send,
	)

	if len(sender.tickets) == 0 {
		t.Fatal("the walk offered the frame to nobody")
	}
	for i, ticket := range sender.tickets {
		if ticket == nil {
			t.Fatalf("candidate %d got no ticket, so its write carries no class contract", i)
		}
		if ticket != sender.tickets[0] {
			t.Fatalf("candidate %d was offered a different ticket than candidate 0: one send "+
				"has one contract, and minting a second one only pays for a fallback", i)
		}
	}
}

// A frame with an EMPTY contract must not allocate a ticket at all: the
// contract of NewWriteTicket is that an empty one costs nothing.
func TestEmitToWithAnEmptyContractStaysTicketless(t *testing.T) {
	sender := &candidateSend{accepts: []bool{false, true}}
	if !emitOverCandidates(
		threeTargets(),
		protocol.Frame{Type: protocol.DatagramFrameType},
		netcore.OutboundWrite{},
		sender.send,
	) {
		t.Fatal("the second candidate accepted the frame")
	}
	for i, ticket := range sender.tickets {
		if ticket != nil {
			t.Fatalf("candidate %d received a ticket for an empty contract", i)
		}
	}
}

// ---------------------------------------------------------------------------
// The write contract of an ordinary frame
// ---------------------------------------------------------------------------

// TestOutboundWriteCarriesTheClassContract pins what the emitter builds for
// every frame the layer hands it: the write grace and the send deadline of the
// frame's class. An unknown class is refused rather than written with the
// connection's default.
func TestOutboundWriteCarriesTheClassContract(t *testing.T) {
	emitter := datagramFrameEmitter{}

	write, err := emitter.outboundWrite(datagram.OutboundFrame{Class: domain.DatagramClassBulk})
	if err != nil {
		t.Fatalf("outboundWrite: %v", err)
	}
	if write.WriteGrace <= 0 {
		t.Fatal("the class write grace is owed to every frame")
	}

	deadline := time.Now().UTC().Add(time.Minute)
	dated, err := emitter.outboundWrite(datagram.OutboundFrame{
		Class:     domain.DatagramClassControl,
		SendUntil: deadline,
	})
	if err != nil {
		t.Fatalf("outboundWrite: %v", err)
	}
	if !dated.SendUntil.Valid() || !dated.SendUntil.Time().Equal(deadline) {
		t.Fatalf("send deadline = %v (valid %t), want %s",
			dated.SendUntil.Time(), dated.SendUntil.Valid(), deadline)
	}

	if _, err := emitter.outboundWrite(
		datagram.OutboundFrame{Class: domain.DatagramClass("nonsense")},
	); err == nil {
		t.Fatal("an unknown class must still be refused")
	}
}
