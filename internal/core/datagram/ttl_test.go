package datagram

import (
	"context"
	"fmt"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// ttl_test.go covers the whole ttl block of §9: the five rules of §4.1.1 as
// pure functions, and the end-to-end maximum-path test that only a multi-node
// topology can express.

func TestDefaultMaxHopsMatchesTheFileCommandBudget(t *testing.T) {
	// §9: defaultMaxHops equals 10 and equals today's fileCommandDefaultMaxTTL,
	// so the file transport's migration does not change behaviour by one bit.
	if domain.DatagramDefaultMaxHops != 10 {
		t.Fatalf("defaultMaxHops = %d, want 10", domain.DatagramDefaultMaxHops)
	}
	if OriginTTL() != domain.DatagramDefaultMaxHops || ResponseTTL() != domain.DatagramDefaultMaxHops {
		t.Fatalf("origin ttl %d and response ttl %d must both be defaultMaxHops", OriginTTL(), ResponseTTL())
	}
}

func TestTTLRulesOperateOnTheRawValue(t *testing.T) {
	// Rule 1: the zero check is on the RAW value, before the clamp — a clamp
	// first would resurrect the datagram.
	if !TTLExhausted(0) {
		t.Fatal("a raw ttl of zero must be exhausted")
	}
	if TTLExhausted(ClampTTL(0)) != true {
		t.Fatal("clamping must not revive a zero ttl")
	}

	// Rule 2: `ttl <= max_ttl` is also on the RAW value. 255 against a signed
	// budget of 10 is refused; checking the clamped value would have admitted
	// it, because the clamp itself rewrites 255 to 10.
	if TTLWithinBudget(255, 10) {
		t.Fatal("ttl 255 must exceed a signed max_ttl of 10")
	}
	if !TTLWithinBudget(ClampTTL(255), 10) {
		t.Fatal("this is exactly the mistake the raw check prevents")
	}
	if !TTLWithinBudget(10, 10) {
		t.Fatal("the boundary is within budget")
	}

	// Rule 3: the clamp.
	if got := ClampTTL(255); got != domain.DatagramDefaultMaxHops {
		t.Fatalf("clamp(255) = %d, want %d", got, domain.DatagramDefaultMaxHops)
	}
	if got := ClampTTL(3); got != 3 {
		t.Fatalf("clamp must not raise a small ttl, got %d", got)
	}

	// Rule 4: exactly one decrement, and no underflow.
	if got, ok := DecrementTTL(10); !ok || got != 9 {
		t.Fatalf("decrement(10) = %d/%v", got, ok)
	}
	if _, ok := DecrementTTL(0); ok {
		t.Fatal("a zero ttl has no hop left to spend")
	}
	if got, ok := ClampAndDecrement(255); !ok || got != domain.DatagramDefaultMaxHops-1 {
		t.Fatalf("clampAndDecrement(255) = %d/%v, want %d", got, ok, domain.DatagramDefaultMaxHops-1)
	}
}

// ---------------------------------------------------------------------------
// The maximum path, end to end
// ---------------------------------------------------------------------------

// lineTopology builds origin → transit × (hops−1) → target and returns the
// nodes in path order.
func lineTopology(t *testing.T, net *fakeNetwork, hops int) []*pipelineNode {
	t.Helper()
	nodes := make([]*pipelineNode, 0, hops+1)
	for i := 0; i <= hops; i++ {
		// Only the intermediate nodes advertise transit; the two ends are
		// endpoints, exactly as an endpoint-only client would be.
		transit := i != 0 && i != hops
		nodes = append(nodes, newPipelineNode(t, net, nodeOpts{
			name:    fmt.Sprintf("node-%02d", i),
			transit: transit,
		}))
	}
	for i := 0; i < hops; i++ {
		link(nodes[i], nodes[i+1], i != 0, i+1 != hops)
	}
	target := nodes[hops].id
	for i := 0; i < hops; i++ {
		route(nodes[i], target, nodes[i+1].id, hops-i)
	}
	return nodes
}

// TestMaximumPathRoundTrip is the end-to-end test of §9: a request across the
// full ten hops and an answer back across the same ten, with the ttl never
// reaching zero prematurely.
//
// It also pins the two rules a single-node test cannot see: the ORIGIN does
// not decrement (the first hop receives the full budget), and LOCAL DELIVERY
// does not either — at the target, and at the initiator whose upstream is
// local.
func TestMaximumPathRoundTrip(t *testing.T) {
	net := newFakeNetwork()
	hops := int(domain.DatagramDefaultMaxHops)
	nodes := lineTopology(t, net, hops)
	origin, target := nodes[0], nodes[hops]

	answers := answeringHandler(dtypeAnswer, []byte("record"))
	resolver := acceptingHandler()
	for _, node := range nodes {
		registerType(t, node, requestType(dtypeQuery, answers))
		registerType(t, node, responseType(dtypeAnswer, dtypeQuery, resolver))
	}

	label := newLabel(t, "max-path")
	outcome := origin.pipeline.SendLocal(context.Background(), LocalSendOpts{
		Frame: requestFrame(t, requestOpts{label: label, dst: target.id}),
	})
	if outcome.Kind() != SendQueued {
		t.Fatalf("SendLocal: %s (%v)", outcome, outcome.Err())
	}

	journal := net.journal()
	if len(journal) != 2*hops {
		t.Fatalf("want %d wire events (%d out, %d back), got %d", 2*hops, hops, hops, len(journal))
	}

	// The request: the origin does not decrement, every relay decrements once.
	for i := 0; i < hops; i++ {
		event := journal[i]
		wantTTL := uint8(hops - i)
		if event.frame.Mode != domain.DatagramModeRequest {
			t.Fatalf("event %d: mode %s, want request", i, event.frame.Mode)
		}
		if event.frame.TTL != wantTTL {
			t.Fatalf("request hop %d: ttl %d, want %d", i, event.frame.TTL, wantTTL)
		}
		if event.frame.TTL == 0 {
			t.Fatalf("request hop %d reached ttl 0 prematurely", i)
		}
	}

	// The answer starts at defaultMaxHops whoever produced it, and decreases on
	// the way back — a reply never outlives the request that caused it.
	for i := 0; i < hops; i++ {
		event := journal[hops+i]
		wantTTL := uint8(hops - i)
		if event.frame.Mode != domain.DatagramModeResponse {
			t.Fatalf("event %d: mode %s, want response", hops+i, event.frame.Mode)
		}
		if event.frame.TTL != wantTTL {
			t.Fatalf("response hop %d: ttl %d, want %d", i, event.frame.TTL, wantTTL)
		}
	}

	if answers.callCount() != 1 {
		t.Fatalf("the target handler ran %d times, want 1", answers.callCount())
	}
	if resolver.callCount() != 1 {
		t.Fatalf("the initiator's resolver ran %d times, want 1", resolver.callCount())
	}
}

// TestUnsignedRequestWithInflatedTTLIsClampedInThePipeline covers §9: an
// unsigned request with ttl = 255 is clamped to defaultMaxHops in the
// CONVEYOR — this plane has no auth.max_ttl — and therefore does not live
// longer than the reverse state is sized for.
func TestUnsignedRequestWithInflatedTTLIsClampedInThePipeline(t *testing.T) {
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	relay, target := nodes[1], nodes[2]
	registerType(t, target, requestType(dtypeQuery, acceptingHandler()))

	label := newLabel(t, "inflated")
	result := relay.deliver(t, nodes[0].id, requestFrame(t, requestOpts{
		label: label, dst: target.id, ttl: 255,
	}))
	requireOutcome(t, result, InboundForwarded)

	journal := net.journal()
	if len(journal) != 1 {
		t.Fatalf("want one forwarded frame, got %d", len(journal))
	}
	if got := journal[0].frame.TTL; got != domain.DatagramDefaultMaxHops-1 {
		t.Fatalf("forwarded ttl %d, want %d (clamp then one decrement)", got, domain.DatagramDefaultMaxHops-1)
	}
}

// TestUnsignedResponseWithInflatedTTLIsClamped is the same rule on the way
// back (§9).
func TestUnsignedResponseWithInflatedTTLIsClamped(t *testing.T) {
	net := newFakeNetwork()
	nodes := lineTopology(t, net, 2)
	origin, relay, target := nodes[0], nodes[1], nodes[2]
	registerType(t, target, requestType(dtypeQuery, acceptingHandler()))
	registerType(t, origin, responseType(dtypeAnswer, dtypeQuery, acceptingHandler()))

	label := newLabel(t, "inflated-back")
	requireOutcome(t, relay.deliver(t, origin.id, requestFrame(t, requestOpts{
		label: label, dst: target.id,
	})), InboundForwarded)

	result := relay.deliver(t, target.id, responseFrame(t, responseOpts{
		label: label, subject: target.id, ttl: 255,
	}))
	requireOutcome(t, result, InboundForwarded)

	journal := net.journal()
	last := journal[len(journal)-1]
	if last.frame.Mode != domain.DatagramModeResponse {
		t.Fatalf("last event is %s, want a response", last.frame.Mode)
	}
	if got := last.frame.TTL; got != domain.DatagramDefaultMaxHops-1 {
		t.Fatalf("response ttl %d, want %d", got, domain.DatagramDefaultMaxHops-1)
	}
}

// TestRoutedLocalDeliveryDoesNotDecrement pins the "no decrement on local
// delivery" half of rule 4 for the signed plane.
func TestRoutedLocalDeliveryDoesNotDecrement(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := acceptingHandler()
	registerType(t, receiver, routedType(dtypePush, handler))

	frame := signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(), ttl: 7,
	})
	requireOutcome(t, receiver.deliver(t, sender.id, frame), InboundDelivered)

	delivered, ok := handler.lastContext()
	if !ok {
		t.Fatal("the handler was not called")
	}
	if got := delivered.Header().TTL(); got != 7 {
		t.Fatalf("the handler saw ttl %d, want the undecremented 7", got)
	}
}

// TestDecrementRefusesTheLastHop pins the stop point §4.1.1 implies but no
// call site used to enforce: a frame whose budget the decrement would bring to
// zero is dropped HERE, not serialized and handed to a neighbour who is
// obliged to drop it at step 3 of §4.1.
//
// The decrement pays for the hop we are about to make, so `ttl = 1` at a relay
// buys nothing: the neighbour receives a raw zero and drops it. Publishing it
// anyway costs a serialization, a socket write and one frame of the
// neighbour's inbound budget per frame that reaches the end of its path.
func TestDecrementRefusesTheLastHop(t *testing.T) {
	if _, ok := DecrementTTL(1); ok {
		t.Fatal("a frame that would arrive with ttl = 0 must not be forwarded")
	}
	if got, ok := DecrementTTL(2); !ok || got != 1 {
		t.Fatalf("DecrementTTL(2) = %d,%v — the last USEFUL hop must still be made", got, ok)
	}
	if _, ok := ClampAndDecrement(1); ok {
		t.Fatal("the unsigned planes stop at the same place")
	}
}
