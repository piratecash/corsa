package datagram

import (
	"context"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// pipeline_sender_proof_test.go pins the ONE fact an outbound session cannot
// deliver: proof of who the neighbour is.
//
// The handshake proves the INITIATOR's identity to the RESPONDER — the
// challenge of that exchange travels one way — so on a session THIS node
// dialled, the address in the welcome is a label the remote chose for itself.
// Every fixture below therefore carries TWO neighbours: with one, "the frame
// was admitted under a borrowed name" and "the frame was admitted" are the same
// observation.

var errNotTheTrustedNeighbour = errors.New("the authorizer does not trust this sender")

// trustOnly is an authorization hook of the shape §7 exists for: it answers
// "who sent this" and admits exactly one neighbour.
func trustOnly(trusted domain.PeerIdentity) AuthorizerFunc {
	return func(_ context.Context, delivery DeliveryContext, _ []byte) AuthorizationDecision {
		peer, remote := delivery.IncomingPeer().Identity()
		if remote && peer == trusted {
			return Accept()
		}
		return Reject(errNotTheTrustedNeighbour)
	}
}

// dialedSession is the admission key of a session THIS node dialled: the only
// key that direction can defend, and the discriminator the conveyor reads to
// learn that nothing about the remote is proven.
func dialedSession() AdmissionKey {
	return DialedAddressKey(domain.PeerAddress("203.0.113.7:64646"))
}

// askFor builds a request addressed to this node, which is the shortest path
// to the §7 hook: no signature, no anti-replay, no reverse state.
func askFor(t *testing.T, node *pipelineNode, seed string, dtype domain.DType) protocol.DatagramFrame {
	t.Helper()
	return requestFrame(t, requestOpts{label: newLabel(t, seed), dst: node.id, dtype: dtype})
}

// TestADialedSessionCannotBorrowAnotherPeersIdentity is the finding itself, on
// the plane where it is cheapest to reach: a request addressed to this node.
//
// The mutation this kills: deleting senderProofGate from HandleInbound, or
// making inboundFrame.authority answer `proven` for anything other than a
// budget key that names THIS peer in the proven namespace.
func TestADialedSessionCannotBorrowAnotherPeersIdentity(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "endpoint"})

	trusted := domaintest.ID("trusted-neighbour")
	stranger := domaintest.ID("stranger")

	handler := acceptingHandler()
	registration := requestType(dtypeQuery, handler)
	registration.Authorizer = trustOnly(trusted)
	registerType(t, node, registration)

	// POSITIVE CONTROL. On an ACCEPTED connection the identity is proven —
	// connauth checked a signature over a challenge this node generated — so the
	// hook decides on a fact and the legitimate frame goes through exactly as
	// before. Without this the test would be indistinguishable from a layer that
	// refuses everything.
	requireOutcome(t, node.deliver(t, trusted, askFor(t, node, "proven-trusted", dtypeQuery)), InboundDelivered)
	if handler.callCount() != 1 {
		t.Fatalf("the handler ran %d times for one authorized frame", handler.callCount())
	}

	// NEGATIVE CONTROL. The hook really answers "WHO sent this": the second,
	// equally proven neighbour is refused by the hook itself, under its own
	// reason. This is what makes "it borrowed the first one's name" a
	// demonstrable statement rather than a tautology.
	requireDrop(t, node.deliver(t, stranger, askFor(t, node, "proven-stranger", dtypeQuery)), DropUnauthorized)
	if handler.callCount() != 1 {
		t.Fatalf("a rejected frame reached the handler: %d calls", handler.callCount())
	}

	// THE FINDING. Same frame, same claimed identity, on a session THIS node
	// dialled: nothing about the sender is proven, so the hook must not be asked
	// at all — and the refusal must be NAMED, so that "we would not let anybody
	// answer this question here" cannot be read as "the trust list said no".
	borrowed := node.deliverBilledTo(t, trusted, dialedSession(), askFor(t, node, "borrowed", dtypeQuery))
	requireDrop(t, borrowed, DropUnprovenSender)
	if handler.callCount() != 1 {
		t.Fatalf("a frame under a borrowed name reached the handler: %d calls", handler.callCount())
	}
	if borrowed.BanWorthy() {
		t.Fatal("naming yourself in your own welcome is what the handshake asks for; §4.4 charges no ban for it")
	}
}

// TestAProvenKeyThatNamesAnotherNeighbourIsNotProof closes the drift the
// discriminator exists to prevent: the proof is the AGREEMENT of the two facts
// the caller supplies, not the namespace of the key on its own.
//
// The mutation this kills: reducing inboundFrame.authority to
// `budgetKey.Space() == AdmissionKeySpaceProvenIdentity`. A receive path that
// then paired a proven key with somebody else's claimed identity — one line in
// a future dispatcher — would re-open the whole finding silently.
func TestAProvenKeyThatNamesAnotherNeighbourIsNotProof(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "endpoint"})

	trusted := domaintest.ID("trusted-neighbour")
	stranger := domaintest.ID("stranger")

	handler := acceptingHandler()
	registration := requestType(dtypeQuery, handler)
	registration.Authorizer = trustOnly(trusted)
	registerType(t, node, registration)

	requireDrop(t, node.deliverBilledTo(
		t, trusted, ProvenIdentityKey(stranger), askFor(t, node, "mismatched", dtypeQuery),
	), DropUnprovenSender)
	if handler.callCount() != 0 {
		t.Fatalf("a self-contradicting arrival reached the handler: %d calls", handler.callCount())
	}
}

// TestADialedSessionStillServesATypeThatDeclaredItNeedsNoProof is the
// blast-radius control.
//
// The gate refuses a DECLARED REQUIREMENT, not a direction: a type that states
// its sender is authenticated inside the payload (§7) has nothing to be misled
// about, and refusing it would take the whole request/response plane off every
// session this node dialled — which is most of a client node's traffic.
//
// The mutation this kills: hoisting the refusal to "any claimed ingress", or
// dropping the registry lookup from the gate.
func TestADialedSessionStillServesATypeThatDeclaredItNeedsNoProof(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "endpoint"})

	handler := acceptingHandler()
	registration := requestType(dtypeCached, handler)
	registration.SenderProof = SenderProvenInPayload
	registerType(t, node, registration)

	requireOutcome(t, node.deliverBilledTo(
		t, domaintest.ID("whoever-it-says-it-is"), dialedSession(),
		askFor(t, node, "no-authorizer", dtypeCached),
	), InboundDelivered)
	if handler.callCount() != 1 {
		t.Fatalf("the handler ran %d times, want 1", handler.callCount())
	}
}

// TestADialedSessionCannotBorrowAnIdentityOnTheResponsePlane is the same
// finding on the plane a CLIENT reaches first: the answer to a request this
// node originated arrives on the session this node dialled, and its
// authorization hook is handed the same unprovable name.
//
// The record is checked afterwards on purpose: a refusal before the CAS must
// leave the exchange pending, so the genuine answer can still arrive (§4.2).
func TestADialedSessionCannotBorrowAnIdentityOnTheResponsePlane(t *testing.T) {
	t.Parallel()

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "initiator"})
	target := newPipelineNode(t, net, nodeOpts{name: "target"})
	link(node, target, false, false)
	route(node, target.id, target.id, 1)

	handler := acceptingHandler()
	registration := responseType(dtypeAnswer, dtypeQuery, handler)
	registration.Authorizer = trustOnly(target.id)
	registerType(t, node, registration)

	openExchange := func(seed string) Label {
		label := newLabel(t, seed)
		if outcome := node.pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame: requestFrame(t, requestOpts{label: label, dst: target.id}),
		}); outcome.Kind() != SendQueued {
			t.Fatalf("SendLocal(%s): %s", seed, outcome)
		}
		return label
	}

	// POSITIVE CONTROL: the answer over a connection whose peer is proven.
	proven := openExchange("response-proven")
	requireOutcome(t, node.deliver(t, target.id, responseFrame(t, responseOpts{
		label: proven, subject: target.id,
	})), InboundDelivered)
	if handler.callCount() != 1 {
		t.Fatalf("the handler ran %d times for one authorized answer", handler.callCount())
	}

	// THE FINDING: the same answer on a session this node dialled, from a
	// neighbour that merely CALLS itself target.
	borrowed := openExchange("response-borrowed")
	requireDrop(t, node.deliverBilledTo(t, target.id, dialedSession(), responseFrame(t, responseOpts{
		label: borrowed, subject: target.id,
	})), DropUnprovenSender)
	if handler.callCount() != 1 {
		t.Fatalf("an answer under a borrowed name reached the handler: %d calls", handler.callCount())
	}

	record, live := node.reverse.Lookup(borrowed)
	if !live {
		t.Fatal("a refusal before the CAS must leave the exchange alive")
	}
	if record.State() != ReverseSlotPending {
		t.Fatalf("record is %s, want pending: the genuine answer has not been spent", record.State())
	}
}
