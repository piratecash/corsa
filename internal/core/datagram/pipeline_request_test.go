package datagram

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// pipeline_request_test.go covers the request fork of §4.1 and the two-phase
// reservation of §4.2 as the conveyor drives it.

// requestFixture is origin → relay → target with a route only at the relay.
type requestFixture struct {
	net    *fakeNetwork
	origin *pipelineNode
	relay  *pipelineNode
	target *pipelineNode
}

func newRequestFixture(t *testing.T) *requestFixture {
	t.Helper()
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	return &requestFixture{net: net, origin: nodes[0], relay: nodes[1], target: nodes[2]}
}

// TestRequestAtDestinationAnswersWithoutReverseState is §9: a request with
// dst == self reaches the handler and its answer goes back to the neighbour it
// came from, with NO reverse record created — there is nowhere else to answer.
func TestRequestAtDestinationAnswersWithoutReverseState(t *testing.T) {
	fixture := newRequestFixture(t)
	handler := answeringHandler(dtypeAnswer, []byte("answer"))
	registerType(t, fixture.target, requestType(dtypeQuery, handler))

	label := newLabel(t, "direct")
	result := fixture.target.deliver(t, fixture.relay.id, requestFrame(t, requestOpts{
		label: label, dst: fixture.target.id,
	}))
	requireOutcome(t, result, InboundAnswered)

	if fixture.target.reverse.Len() != 0 {
		t.Fatal("the endpoint creates no reverse state")
	}
	journal := fixture.net.journal()
	if len(journal) != 1 {
		t.Fatalf("want exactly one answer on the wire, got %d", len(journal))
	}
	answer := journal[0]
	if answer.to != fixture.relay.id {
		t.Fatalf("the answer went to %v, want the neighbour the request came from", answer.to)
	}
	if answer.frame.Mode != domain.DatagramModeResponse {
		t.Fatalf("answer mode %s", answer.frame.Mode)
	}
	if answer.frame.Dst != label.Raw() {
		t.Fatal("the answer must echo the request's label in dst")
	}
	if answer.frame.Src != fixture.target.id {
		t.Fatal("the answer's src is the address the question was ADDRESSED TO (§2.1.1)")
	}
	if answer.frame.Auth != nil {
		t.Fatal("the request/response plane carries no auth (§2.1)")
	}
	if !answer.frame.RoutePolicy.IsNone() {
		t.Fatal("route_policy is forbidden in a response (§2.1)")
	}
	if answer.frame.TTL != ResponseTTL() {
		t.Fatalf("answer ttl %d, want %d", answer.frame.TTL, ResponseTTL())
	}
	// The send deadline is computed locally: arrival + queue_residence(control).
	if want := ResponseSendDeadline(fixture.target.clock()); !answer.sendUntil.Equal(want) {
		t.Fatalf("answer deadline %s, want %s", answer.sendUntil, want)
	}
}

// TestAnswerOnlyOnAccepted is §9: `rejected` and `failed` are a silent drop of
// the request WITHOUT an answer — answering on a refusal would disguise it as
// success.
func TestAnswerOnlyOnAccepted(t *testing.T) {
	cases := []struct {
		name    string
		handler *recordingHandler
		reason  DropReason
	}{
		{"rejected", refusingHandler(), DropHandlerRejected},
		{"failed", failingHandler(), DropHandlerFailed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newRequestFixture(t)
			registerType(t, fixture.target, requestType(dtypeQuery, testCase.handler))

			result := fixture.target.deliver(t, fixture.relay.id, requestFrame(t, requestOpts{
				label: newLabel(t, testCase.name), dst: fixture.target.id,
			}))
			requireDrop(t, result, testCase.reason)
			if len(fixture.net.journal()) != 0 {
				t.Fatal("a refusal must not put an answer on the wire")
			}
		})
	}
}

// TestARequestWithNowhereToGoIsASilentDrop pins what a relay does with a
// request it cannot place: it drops it, and it leaves no reverse state behind.
//
// There used to be a step between: an interceptor could ANSWER the request from
// the relay's own cache, in the destination's name, without a route to the
// destination at all. It is gone — a relay that answers for somebody else is a
// participant in a protocol neither endpoint can tell it apart from.
func TestARequestWithNowhereToGoIsASilentDrop(t *testing.T) {
	net := newFakeNetwork()
	origin := newPipelineNode(t, net, nodeOpts{name: "origin"})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	link(origin, relay, false, true)
	// Deliberately NO route to the destination at the relay.
	registerType(t, relay, requestType(dtypeQuery, acceptingHandler()))

	result := relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "nowhere"), dst: domaintest.ID("unreachable"),
	}))
	requireDrop(t, result, DropNoCandidates)
	if relay.reverse.Len() != 0 {
		t.Fatal("a dropped request must not leave reverse state")
	}
	if len(net.journal()) != 0 {
		t.Fatal("a relay answers nothing on behalf of a destination it cannot reach")
	}
}

// TestRequestPlaneNeverTouchesTheRoutedReplayCache is the §4.1 separation of
// the planes, in both directions.
func TestRequestPlaneNeverTouchesTheRoutedReplayCache(t *testing.T) {
	fixture := newRequestFixture(t)
	registerType(t, fixture.target, requestType(dtypeQuery, acceptingHandler()))

	requireOutcome(t, fixture.relay.deliver(t, fixture.origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "plane"), dst: fixture.target.id,
	})), InboundForwarded)

	if fixture.relay.replay.Len() != 0 {
		t.Fatalf("the request plane occupied %d anti-replay records", fixture.relay.replay.Len())
	}
	// And the reverse table is the state it DOES use.
	if fixture.relay.reverse.Len() != 1 {
		t.Fatalf("want one reverse record, got %d", fixture.relay.reverse.Len())
	}
}

// TestDownstreamIsFixedBeforePublication is §4.2 phase 3: the chosen candidate
// is in the record before the frame is published, which is what lets a fast
// answer arrive re-entrantly and still find its way home.
func TestDownstreamIsFixedBeforePublication(t *testing.T) {
	fixture := newRequestFixture(t)
	label := newLabel(t, "phase3")

	var (
		observedDownstream Downstream
		observed           bool
	)
	// The target answers synchronously from inside the relay's own EmitTo, so
	// the assertion below runs while the relay is still publishing.
	registerType(t, fixture.target, requestType(dtypeQuery, HandlerFunc(
		func(context.Context, DeliveryContext, []byte) HandlerResult {
			record, live := fixture.relay.reverse.Lookup(label)
			if live {
				observedDownstream, observed = record.Downstream()
			}
			return AcceptWithAnswer(dtypeAnswer, []byte("answer"))
		})))
	registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))
	registerType(t, fixture.relay, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

	requireOutcome(t, fixture.relay.deliver(t, fixture.origin.id, requestFrame(t, requestOpts{
		label: label, dst: fixture.target.id,
	})), InboundForwarded)

	if !observed || observedDownstream.Channel() != testChannel(fixture.target.id.String()) {
		t.Fatalf("downstream was %v/%v while the frame was still being published", observedDownstream, observed)
	}
	// The answer that came back during publication was forwarded to the origin.
	journal := fixture.net.journal()
	if len(journal) != 3 {
		t.Fatalf("want request, answer and forwarded answer, got %d events", len(journal))
	}
	if last := journal[2]; last.to != fixture.origin.id || last.frame.Mode != domain.DatagramModeResponse {
		t.Fatalf("the answer did not reach the origin: %+v", last)
	}
	if fixture.relay.reverse.Len() != 0 {
		t.Fatal("a completed exchange frees its record")
	}
}

// TestRepeatedRequestDoesNotRepointDownstream is §4.2 phase 2 at the conveyor
// level, and it explains why: the answer to the FIRST forward would otherwise
// lose its way back.
func TestRepeatedRequestDoesNotRepointDownstream(t *testing.T) {
	net := newFakeNetwork()
	origin := newPipelineNode(t, net, nodeOpts{name: "origin"})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	first := newPipelineNode(t, net, nodeOpts{name: "hop-a", transit: true})
	second := newPipelineNode(t, net, nodeOpts{name: "hop-b", transit: true})
	target := domaintest.ID("target")
	link(origin, relay, false, true)
	link(relay, first, true, true)
	link(relay, second, true, true)
	relay.routes.set(target, RouteHint{NextHop: first.id, Hops: 2})

	label := newLabel(t, "loop")
	frame := requestFrame(t, requestOpts{label: label, dst: target})
	requireOutcome(t, relay.deliver(t, origin.id, frame), InboundForwarded)

	// The route changes, and the same label comes round again — a loop, or a
	// duplicate. Neither may re-point the record.
	relay.routes.set(target, RouteHint{NextHop: second.id, Hops: 2})
	requireDrop(t, relay.deliver(t, origin.id, frame), DropReverseSlotBusy)

	record, live := relay.reverse.Lookup(label)
	if !live {
		t.Fatal("the record must survive the repeat")
	}
	downstream, _ := record.Downstream()
	if downstream.Channel() != testChannel(first.id.String()) {
		t.Fatalf("downstream is now %v, want the original %v", downstream, first.id)
	}
	if relay.reverse.Len() != 1 {
		t.Fatal("a repeat must not create a second record")
	}
}

// TestEnqueueFailureRewritesDownstreamThenRollsBack is §4.2 phase 4: a refused
// queue means the frame never left towards that candidate, so downstream moves
// to the next one — and when the candidates run out the slot goes entirely.
func TestEnqueueFailureRewritesDownstreamThenRollsBack(t *testing.T) {
	net := newFakeNetwork()
	origin := newPipelineNode(t, net, nodeOpts{name: "origin"})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	first := newPipelineNode(t, net, nodeOpts{name: "hop-a", transit: true})
	second := newPipelineNode(t, net, nodeOpts{name: "hop-b", transit: true})
	target := domaintest.ID("target")
	link(origin, relay, false, true)
	link(relay, first, true, true)
	link(relay, second, true, true)
	relay.routes.set(target,
		RouteHint{NextHop: first.id, Hops: 2},
		RouteHint{NextHop: second.id, Hops: 3},
	)
	net.refuseQueue(first.id)

	label := newLabel(t, "rollback")
	requireOutcome(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: label, dst: target,
	})), InboundForwarded)

	record, live := relay.reverse.Lookup(label)
	if !live {
		t.Fatal("the record must exist after a successful second candidate")
	}
	downstream, _ := record.Downstream()
	if downstream.Channel() != testChannel(second.id.String()) {
		t.Fatalf("downstream %v, want the candidate that actually took the frame (%v)", downstream, second.id)
	}

	// Now refuse both: the slot is released entirely.
	net.refuseQueue(second.id)
	requireDrop(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "rollback-2"), dst: target,
	})), DropForwardFailed)
	if relay.reverse.Len() != 1 {
		t.Fatalf("the rolled back record must be gone, %d records left", relay.reverse.Len())
	}
}

// TestRequestIsRefusedWhenTheCandidateNamesNoChannel is the closed direction of
// the pin.
//
// A resolver that ranks a connection without saying WHICH connection it is
// leaves the reverse record nothing to store, and the two things the layer could
// do instead are both the defect: publish with no return path recorded, or
// record the peer's NAME and let any session answering to it take the slot. So
// the hop is refused, the walk moves on, and an exhausted walk rolls the record
// back like any other.
func TestRequestIsRefusedWhenTheCandidateNamesNoChannel(t *testing.T) {
	net := newFakeNetwork()
	origin := newPipelineNode(t, net, nodeOpts{name: "origin"})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	hop := newPipelineNode(t, net, nodeOpts{name: "hop", transit: true})
	target := domaintest.ID("target")
	link(origin, relay, false, true)
	link(relay, hop, true, true)
	relay.routes.set(target, RouteHint{NextHop: hop.id, Hops: 2})

	// The ONE thing the resolver stops saying: which connection it ranked.
	// Everything else about the hop stays exactly as the positive control has it.
	nameless := hop.connection(true)
	nameless.Channel = NoChannel()
	relay.peers.set(hop.id, nameless)

	requireDrop(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "nameless"), dst: target,
	})), DropForwardFailed)
	if relay.reverse.Len() != 0 {
		t.Fatalf("an exhausted walk must roll the record back, %d records left", relay.reverse.Len())
	}

	// POSITIVE CONTROL: the same topology with the channel named forwards.
	relay.peers.set(hop.id, hop.connection(true))
	requireOutcome(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "named"), dst: target,
	})), InboundForwarded)
}

// TestRequestTransitGateDropsBeforeTheReservation pins that the unsigned plane
// obeys the same rule: an endpoint-only node spends no reverse state on
// somebody else's request.
func TestRequestTransitGateDropsBeforeTheReservation(t *testing.T) {
	net := newFakeNetwork()
	origin := newPipelineNode(t, net, nodeOpts{name: "origin"})
	relay := newPipelineNode(t, net, nodeOpts{name: "endpoint-only"})
	hop := newPipelineNode(t, net, nodeOpts{name: "hop", transit: true})
	target := domaintest.ID("target")
	link(origin, relay, false, false)
	link(relay, hop, false, true)
	route(relay, target, hop.id, 2)

	requireDrop(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: newLabel(t, "no-transit"), dst: target,
	})), DropTransitGate)
	if relay.reverse.Len() != 0 {
		t.Fatal("the transit gate stands before the reservation")
	}
}

// TestLocalRequestTakesTheLocalUpstreamMarker is §4.2: our own request marks
// the record with the LOCAL marker, not with our address.
func TestLocalRequestTakesTheLocalUpstreamMarker(t *testing.T) {
	fixture := newRequestFixture(t)
	registerType(t, fixture.target, requestType(dtypeQuery, acceptingHandler()))

	label := newLabel(t, "mine")
	outcome := fixture.origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: requestFrame(t, requestOpts{label: label, dst: fixture.target.id}),
	})
	if outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal: %s", outcome)
	}
	record, live := fixture.origin.reverse.Lookup(label)
	if !live {
		t.Fatal("the origin keeps a reverse record for its own request")
	}
	if !record.Upstream().IsLocal() {
		t.Fatalf("upstream %s, want the local marker", record.Upstream())
	}
	if _, addressable := record.Upstream().Peer(); addressable {
		t.Fatal("the local marker must not be addressable")
	}
	if record.DType() != dtypeQuery {
		t.Fatalf("the record must store the REQUEST dtype, got %q", record.DType())
	}
}

// TestRequestPlaneRunsTheSameLocalDeliveryGates closes the last of the three
// planes for §9's "the authorization hook is called on local delivery in all
// three modes", and it is the regression test behind the de-duplication of the
// gate sequence: the routed plane, the local response resolver and this one
// used to carry three copies of
//
//	types.Lookup → observeUnknownDType → admitRegisteredFrame →
//	NewDeliveryContext → authorizeLocalDelivery
//
// and three copies of a security gate is three chances for one plane to lose a
// check without anybody noticing.
func TestRequestPlaneRunsTheSameLocalDeliveryGates(t *testing.T) {
	tests := map[string]struct {
		register func(t *testing.T, target *pipelineNode, handler *recordingHandler)
		dtype    domain.DType
		reason   DropReason
	}{
		"unknown dtype": {
			register: func(*testing.T, *pipelineNode, *recordingHandler) {},
			dtype:    dtypeQuery,
			reason:   DropUnknownDType,
		},
		"mode not allowed for the type": {
			register: func(t *testing.T, target *pipelineNode, handler *recordingHandler) {
				registerType(t, target, routedType(dtypeQuery, handler))
			},
			dtype:  dtypeQuery,
			reason: DropModeNotAllowedForType,
		},
		"authorization reject": {
			register: func(t *testing.T, target *pipelineNode, handler *recordingHandler) {
				registration := requestType(dtypeQuery, handler)
				registration.Authorizer = AuthorizerFunc(
					func(context.Context, DeliveryContext, []byte) AuthorizationDecision {
						return Reject(errTestRefused)
					})
				registerType(t, target, registration)
			},
			dtype:  dtypeQuery,
			reason: DropUnauthorized,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRequestFixture(t)
			handler := answeringHandler(dtypeAnswer, []byte("answer"))
			test.register(t, fixture.target, handler)

			result := fixture.target.deliver(t, fixture.relay.id, requestFrame(t, requestOpts{
				label: newLabel(t, "gated-"+name), dst: fixture.target.id, dtype: test.dtype,
			}))
			requireDrop(t, result, test.reason)
			if handler.callCount() != 0 {
				t.Fatal("a gate refusal must not reach the handler")
			}
			if len(fixture.net.journal()) != 0 {
				t.Fatal("a refused request must not produce an answer")
			}
		})
	}
}
