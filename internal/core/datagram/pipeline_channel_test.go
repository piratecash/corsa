package datagram

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// pipeline_channel_test.go pins the three facts this round is about, and every
// one of them needs the SAME fixture shape: TWO neighbours and TWO channels.
//
// With one neighbour, "the frame was admitted under a borrowed name" and "the
// frame was admitted" are the same observation; with one channel, "the answer
// went back where the question came from" and "the answer went to the node whose
// name was on the question" are the same observation too. Both collapses are
// exactly what the previous rounds' fixtures suffered from.

// borrowedName is the identity a dialled neighbour writes into its own welcome:
// somebody else's. Nothing on that direction can contradict it, which is the
// whole premise of every test below.
func borrowedName(t *testing.T) (victim domain.PeerIdentity, victimChannel, attackerChannel ChannelID) {
	t.Helper()
	return domaintest.ID("victim-B"), testChannel("victim-B"), testChannel("attacker")
}

// ---------------------------------------------------------------------------
// (a) A borrowed name buys neither the victim's reverse quota nor its sessions
// ---------------------------------------------------------------------------

// TestABorrowedNameGetsNeitherTheVictimsQuotaNorItsChannel is finding P1-A on
// its two observable consequences at once.
//
// The bucket is keyed on the ADMISSION KEY the arrival was billed to, and in
// this fixture the two directions produce two different keys for one name: the
// accepted connection is billed to the victim's PROVEN identity, the dialled one
// to the host:port this node dialled. That is what separates the buckets here;
// the per-owner accounting itself — one neighbour across reconnects is one
// bucket — is pinned in reverse_quota_owner_test.go.
//
// The mutations this kills:
//
//   - keying ReverseReserveOpts.Upstream on the arrival's IDENTITY again
//     (ChannelUpstream(..., ProvenIdentityKey(arrival.peer), ...)): the
//     attacker's record then lands in the victim's bucket, and the per-upstream
//     tally (byUpstream) holds 3 against one of them and 0 against the other;
//   - addressing the answer with nextHopEgress(arrival.peer) instead of
//     channelEgress(arrival.channel, ...): the hand-over then carries no
//     channel at all and the transport resolves it through the identity map,
//     which is the borrowed name.
func TestABorrowedNameGetsNeitherTheVictimsQuotaNorItsChannel(t *testing.T) {
	t.Parallel()

	victim, victimChannel, attackerChannel := borrowedName(t)

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "transit", transit: true})
	target := newPipelineNode(t, net, nodeOpts{name: "target"})
	link(node, target, true, false)
	route(node, target.id, target.id, 1)

	// --- the reverse quota ---

	// The two arrivals as the receive path bills them: the accepted connection
	// is charged to the victim's proven identity, the dialled one to the
	// host:port this node dialled. Both are read off the SAME helper the fixture
	// hands the conveyor, so the test cannot assert against a key no arrival
	// produces.
	proven := ingressOpts{peer: victim, channel: victimChannel, authority: AuthorityProven}
	borrowed := ingressOpts{peer: victim, channel: attackerChannel, authority: AuthorityClaimed}
	victimBucket := ChannelUpstream(victimChannel, proven.budgetKey(), victim)
	attackerBucket := ChannelUpstream(attackerChannel, borrowed.budgetKey(), victim)

	// POSITIVE CONTROL: two requests over the SAME session share ONE bucket.
	// Without it, "the two ended up in different buckets" would also be true of
	// a table that gives every record a bucket of its own.
	requireOutcome(t, node.deliverOn(t, proven,
		requestFrame(t, requestOpts{label: newLabel(t, "proven-1"), dst: target.id})), InboundForwarded)
	requireOutcome(t, node.deliverOn(t, proven,
		requestFrame(t, requestOpts{label: newLabel(t, "proven-2"), dst: target.id})), InboundForwarded)

	if load := upstreamLoad(node.reverse, victimBucket); load != 2 {
		t.Fatalf("the victim's own bucket holds %d records, want 2: one neighbour is one bucket", load)
	}

	// THE FINDING: the same name on a session THIS node dialled.
	requireOutcome(t, node.deliverOn(t, borrowed,
		requestFrame(t, requestOpts{label: newLabel(t, "borrowed-1"), dst: target.id})), InboundForwarded)

	if load := upstreamLoad(node.reverse, victimBucket); load != 2 {
		t.Fatalf("the victim's bucket moved to %d: a borrowed name shares the victim's reverse quota", load)
	}
	if load := upstreamLoad(node.reverse, attackerBucket); load != 1 {
		t.Fatalf("the attacker's own bucket holds %d records, want 1", load)
	}

	// --- the answer's channel ---

	answering := newPipelineNode(t, net, nodeOpts{name: "endpoint"})
	registration := requestType(dtypeQuery, answeringHandler(dtypeAnswer, []byte("answer")))
	registration.SenderProof = SenderProvenInPayload
	registerType(t, answering, registration)
	registerType(t, answering, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

	// POSITIVE CONTROL: over a proven channel the answer is pinned to that same
	// channel, so the pin is not "the layer refuses to name any channel".
	requireOutcome(t, answering.deliverOn(t, ingressOpts{
		peer: victim, channel: victimChannel, authority: AuthorityProven,
	}, requestFrame(t, requestOpts{label: newLabel(t, "answer-proven"), dst: answering.id})), InboundAnswered)
	requireAnswerChannel(t, net, victimChannel)

	// THE FINDING: the answer to a question that arrived on the ATTACKER's
	// channel goes back on the attacker's channel, never on the victim's.
	requireOutcome(t, answering.deliverOn(t, ingressOpts{
		peer: victim, channel: attackerChannel, authority: AuthorityClaimed,
	}, requestFrame(t, requestOpts{label: newLabel(t, "answer-borrowed"), dst: answering.id})), InboundAnswered)
	requireAnswerChannel(t, net, attackerChannel)
}

// requireAnswerChannel asserts the channel the LAST hand-over named.
func requireAnswerChannel(t *testing.T, net *fakeNetwork, want ChannelID) {
	t.Helper()
	events := net.journal()
	if len(events) == 0 {
		t.Fatal("nothing was handed to the transport")
	}
	last := events[len(events)-1]
	if last.channel != want {
		t.Fatalf("the answer was handed over on %s, want %s: an answer belongs to the channel "+
			"the question arrived on, not to the identity the question NAMED", last.channel, want)
	}
}

// ---------------------------------------------------------------------------
// (b) Foreign code is never shown a claim as if it were proof
// ---------------------------------------------------------------------------

// TestForeignCodeSeesTheAuthorityOfTheNeighbourItIsToldAbout is finding P1-B,
// checked on what the FOREIGN code sees rather than on what the conveyor knows.
//
// The handler and the authorizer are the two seams that receive the value, and
// if either is handed a claimed identity with nothing to say so, an
// impersonation is one `if peer == trusted` away.
//
// The mutations this kill:
//
//   - making IngressPeer.Identity answer for a claimed ingress. The claim must
//     leave the layer only together with its level (PresentedIdentity);
//   - dropping the authority out of IngressPeer so Authority() answers the zero
//     value everywhere: the PROVEN half of the test then fails, which is why the
//     positive control is not optional here.
func TestForeignCodeSeesTheAuthorityOfTheNeighbourItIsToldAbout(t *testing.T) {
	t.Parallel()

	victim, victimChannel, attackerChannel := borrowedName(t)

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "transit", transit: true})
	target := newPipelineNode(t, net, nodeOpts{name: "target"})
	link(node, target, true, false)
	route(node, target.id, target.id, 1)

	seen := &ingressWitness{}
	transiting := requestType(dtypeQuery, acceptingHandler())
	transiting.SenderProof = SenderProvenInPayload
	transiting.Authorizer = AuthorizerFunc(
		func(_ context.Context, delivery DeliveryContext, _ []byte) AuthorizationDecision {
			seen.record(delivery.IncomingPeer())
			return Accept()
		})
	registerType(t, node, transiting)

	// POSITIVE CONTROL: an accepted connection really does reach the hook as
	// proven, so the negative half below is about the DIRECTION and not about a
	// hook that is never told anything.
	requireOutcome(t, node.deliverOn(t, ingressOpts{
		peer: victim, channel: victimChannel, authority: AuthorityProven,
	}, requestFrame(t, requestOpts{label: newLabel(t, "authorize-proven"), dst: node.id})), InboundDelivered)
	seen.requireProven(t, victim, victimChannel)

	// THE FINDING: on the dialled direction the hook is told a name and the
	// level behind it, and the identity-shaped accessor answers false.
	requireOutcome(t, node.deliverOn(t, ingressOpts{
		peer: victim, channel: attackerChannel, authority: AuthorityClaimed,
	}, requestFrame(t, requestOpts{label: newLabel(t, "authorize-claimed"), dst: node.id})), InboundDelivered)
	seen.requireClaimed(t, victim, attackerChannel)

	// The HANDLER is the second seam of §7 that receives the value, and it sees
	// the same thing: a claim marked as one.
	endpoint := newPipelineNode(t, net, nodeOpts{name: "endpoint"})
	handler := acceptingHandler()
	local := requestType(dtypeCached, handler)
	local.SenderProof = SenderProvenInPayload
	registerType(t, endpoint, local)

	requireOutcome(t, endpoint.deliverOn(t, ingressOpts{
		peer: victim, channel: attackerChannel, authority: AuthorityClaimed,
	}, requestFrame(t, requestOpts{
		label: newLabel(t, "handler-claimed"), dst: endpoint.id, dtype: dtypeCached,
	})), InboundDelivered)

	delivery, ran := handler.lastContext()
	if !ran {
		t.Fatal("the handler never ran")
	}
	if _, proven := delivery.IncomingPeer().Identity(); proven {
		t.Fatal("the handler was handed a claimed identity through the PROVEN accessor")
	}
	if name, level := delivery.IncomingPeer().PresentedIdentity(); name != victim || level != AuthorityClaimed {
		t.Fatalf("the handler was shown (%s, %s), want (%s, claimed)", name, level, victim)
	}
}

// ingressWitness records what a hook was told about the neighbour.
type ingressWitness struct {
	last IngressPeer
	runs int
}

func (w *ingressWitness) record(peer IngressPeer) {
	w.last = peer
	w.runs++
}

func (w *ingressWitness) requireProven(t *testing.T, want domain.PeerIdentity, channel ChannelID) {
	t.Helper()
	if w.runs == 0 {
		t.Fatal("the hook never ran")
	}
	got, proven := w.last.Identity()
	if !proven || got != want {
		t.Fatalf("the hook saw Identity() = (%s, %t), want (%s, true)", got, proven, want)
	}
	if w.last.Authority() != AuthorityProven {
		t.Fatalf("authority = %s, want proven", w.last.Authority())
	}
	if got, ok := w.last.Channel(); !ok || got != channel {
		t.Fatalf("channel = %s, want %s", got, channel)
	}
}

func (w *ingressWitness) requireClaimed(t *testing.T, want domain.PeerIdentity, channel ChannelID) {
	t.Helper()
	if _, proven := w.last.Identity(); proven {
		t.Fatal("the hook was handed a claimed identity through the PROVEN accessor")
	}
	if name, level := w.last.PresentedIdentity(); name != want || level != AuthorityClaimed {
		t.Fatalf("the hook was shown (%s, %s), want (%s, claimed)", name, level, want)
	}
	if got, ok := w.last.Channel(); !ok || got != channel {
		t.Fatalf("channel = %s, want %s: a claimed ingress still HAS a channel", got, channel)
	}
}

// ---------------------------------------------------------------------------
// (c) The requirement is a DECLARATION of the type, not a guess about its hooks
// ---------------------------------------------------------------------------

// TestSenderProofIsDeclaredByTheTypeAndNotInferredFromItsHooks is finding P2-C.
//
// The inference it replaces was "the type declares an Authorizer, therefore it
// depends on knowing who the neighbour is". That is false in both directions:
// §7 describes a sender authenticated by a signature INSIDE the payload, and
// such a type has an Authorizer that never touches IncomingPeer and must keep
// working on every session this node dialled — while a type with no Authorizer
// at all can perfectly well build its handler on the neighbour's name.
//
// The mutations this kills:
//
//   - restoring asksWhoSent(entry) — "has an Authorizer" — as the gate's
//     condition: the payload-authenticated type below is then refused;
//   - making SenderProvenInPayload the zero value: the type that declares
//     nothing is then served on a direction that proved nothing, which is the
//     failure the declaration exists to make impossible to reach by omission.
func TestSenderProofIsDeclaredByTheTypeAndNotInferredFromItsHooks(t *testing.T) {
	t.Parallel()

	victim, victimChannel, attackerChannel := borrowedName(t)

	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "endpoint"})

	// A type that DECLARES NOTHING. The zero value is the strict end, so it is
	// served only where the neighbour is proven — even though it has no
	// Authorizer at all, which the old inference read as "asks nobody".
	silent := acceptingHandler()
	registerType(t, node, requestType(dtypeQuery, silent))

	// A type that declares its sender is authenticated INSIDE the payload, AND
	// carries an Authorizer. Under the old inference the hook alone condemned
	// it; under the declaration it is served.
	payloadAuthenticated := acceptingHandler()
	declared := requestType(dtypeCached, payloadAuthenticated)
	declared.SenderProof = SenderProvenInPayload
	declared.Authorizer = AuthorizerFunc(func(context.Context, DeliveryContext, []byte) AuthorizationDecision {
		// The shape §7 describes: the decision is taken on the payload, and the
		// neighbour's name is never read.
		return Accept()
	})
	registerType(t, node, declared)

	proven := ingressOpts{peer: victim, channel: victimChannel, authority: AuthorityProven}
	dialed := ingressOpts{peer: victim, channel: attackerChannel, authority: AuthorityClaimed}

	// POSITIVE CONTROL: the strict type is served where the proof exists.
	requireOutcome(t, node.deliverOn(t, proven, requestFrame(t, requestOpts{
		label: newLabel(t, "strict-proven"), dst: node.id, dtype: dtypeQuery,
	})), InboundDelivered)
	if silent.callCount() != 1 {
		t.Fatalf("the strict type ran %d times on a proven channel, want 1", silent.callCount())
	}

	// THE DEFAULT: the strict type is refused on the dialled direction, under
	// its own named reason.
	requireDrop(t, node.deliverOn(t, dialed, requestFrame(t, requestOpts{
		label: newLabel(t, "strict-dialed"), dst: node.id, dtype: dtypeQuery,
	})), DropUnprovenSender)
	if silent.callCount() != 1 {
		t.Fatalf("the strict type ran %d times, want 1: a dialled arrival reached it", silent.callCount())
	}

	// THE DECLARATION: the payload-authenticated type is served on exactly the
	// direction the old inference took it off.
	requireOutcome(t, node.deliverOn(t, dialed, requestFrame(t, requestOpts{
		label: newLabel(t, "payload-dialed"), dst: node.id, dtype: dtypeCached,
	})), InboundDelivered)
	if payloadAuthenticated.callCount() != 1 {
		t.Fatalf("the payload-authenticated type ran %d times on a dialled channel, want 1",
			payloadAuthenticated.callCount())
	}
}

// TestTheUndeclaredSenderProofPolicyIsTheStrictOne states the default on the
// registration itself, so a change of the constant's ORDER — which would flip
// every type that declares nothing — fails here and not three tests away.
func TestTheUndeclaredSenderProofPolicyIsTheStrictOne(t *testing.T) {
	t.Parallel()

	if (SenderProofPolicy(0)) != RequiresProvenPeer {
		t.Fatal("the zero SenderProofPolicy must be the STRICT one: a type that forgot to " +
			"declare must lose availability on dialled sessions, never gain an unprovable sender")
	}
	registry := NewTypeRegistry()
	registerTypeInto(t, registry, requestType(dtypeQuery, acceptingHandler()))
	entry, known := registry.Lookup(dtypeQuery)
	if !known {
		t.Fatal("the type was not registered")
	}
	if !entry.RequiresProvenPeer() {
		t.Fatal("a registration that declared no sender-proof policy does not require a proven peer")
	}
}
