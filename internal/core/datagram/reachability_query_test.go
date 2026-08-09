package datagram

import (
	"context"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// reachability_query_test.go pins the shape contract of the layer's two
// READ-ONLY surfaces.
//
// The finding: they took open structs and validated nothing, so a consumer
// could ask about a datagram that cannot exist — a malformed dtype, a policy
// no send accepts — and get a route back. RoutedFrameBuilder would refuse that
// very send, so the probe answered about a frame the layer would never build.
//
// The tests drive the LAYER, not an RPC surface above it: PR-0's whole point
// is that service adapters call this seam directly.

// queryDst is a destination that is fine in every case below, so a failure is
// unambiguously about the field under test.
func queryDst() domain.PeerIdentity { return domaintest.ID("query-dst") }

// TestAReachabilityQueryIsRefusedUnlessARealSendCouldBuildIt walks the wire
// rules of §2.2 and §2.1 that a probe silently ignored.
func TestAReachabilityQueryIsRefusedUnlessARealSendCouldBuildIt(t *testing.T) {
	cases := []struct {
		name string
		opts ReachabilityQueryOpts
	}{
		{"no destination", ReachabilityQueryOpts{DType: dtypeQuery}},
		{"no dtype", ReachabilityQueryOpts{Dst: queryDst()}},
		{"malformed dtype", ReachabilityQueryOpts{Dst: queryDst(), DType: domain.DType("Not A DType")}},
		{"uppercase dtype", ReachabilityQueryOpts{Dst: queryDst(), DType: domain.DType("Get_Identity")}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := NewReachabilityQuery(tc.opts)
			if err == nil {
				t.Fatalf("the query was accepted: %+v", query)
			}
			if !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("err = %v, want an ErrInvalidQuery", err)
			}
		})
	}
}

// TestAValidReachabilityQueryIsAccepted is the other direction, so the table
// above cannot be satisfied by a constructor that refuses everything.
func TestAValidReachabilityQueryIsAccepted(t *testing.T) {
	query, err := NewReachabilityQuery(ReachabilityQueryOpts{Dst: queryDst(), DType: dtypeQuery})
	if err != nil {
		t.Fatalf("NewReachabilityQuery: %v", err)
	}
	if query.Dst() != queryDst() || query.DType() != dtypeQuery {
		t.Fatalf("the query lost its own fields: %+v", query)
	}
}

// TestARoutePlanQueryRefusesAPolicyNoSendWouldAccept covers the field the plan
// adds. The zero value is refused rather than defaulted for the same reason
// RoutedFrameOpts refuses it: "which policy did this diagnostic use" must not
// be a guess.
func TestARoutePlanQueryRefusesAPolicyNoSendWouldAccept(t *testing.T) {
	valid := ReachabilityQueryOpts{Dst: queryDst(), DType: dtypeQuery}

	for _, policy := range []domain.RoutePolicy{"", domain.RoutePolicy("sideways")} {
		if _, err := NewRoutePlanQuery(RoutePlanQueryOpts{
			ReachabilityQueryOpts: valid, RoutePolicy: policy,
		}); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("policy %q was accepted, err = %v", policy, err)
		}
	}
	if _, err := NewRoutePlanQuery(RoutePlanQueryOpts{
		ReachabilityQueryOpts: valid, RoutePolicy: domain.RoutePolicyBest,
	}); err != nil {
		t.Fatalf("a lawful plan query was refused: %v", err)
	}
}

// TestTheProbeRefusesAQueryThatDidNotComeFromItsConstructor is the second half
// of "unvalidated is not constructible": Go will always let a caller write
// ReachabilityQuery{}, so the entry point refuses anything the constructor did
// not stamp instead of probing on a zero destination.
func TestTheProbeRefusesAQueryThatDidNotComeFromItsConstructor(t *testing.T) {
	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "query-node"})

	if _, err := node.pipeline.scheduler.Reachable(context.Background(), ReachabilityQuery{}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("Reachable accepted a zero query, err = %v", err)
	}
	if _, err := node.pipeline.scheduler.ExplainRoute(context.Background(), RoutePlanQuery{}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("ExplainRoute accepted a zero query, err = %v", err)
	}
}

// TestAValidatedQueryStillAnswersTheSameRoute proves the validation did not
// change what the surfaces DO: a lawful query over a live topology reaches the
// same verdict it always did.
func TestAValidatedQueryStillAnswersTheSameRoute(t *testing.T) {
	net := newFakeNetwork()
	node := newPipelineNode(t, net, nodeOpts{name: "origin"})
	peer := newPipelineNode(t, net, nodeOpts{name: "peer"})
	link(node, peer, true, true)

	query, err := NewReachabilityQuery(ReachabilityQueryOpts{Dst: peer.id, DType: dtypeQuery})
	if err != nil {
		t.Fatalf("NewReachabilityQuery: %v", err)
	}
	result, err := node.pipeline.scheduler.Reachable(context.Background(), query)
	if err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	if !result.Reachable() {
		t.Fatalf("a live neighbour is unreachable: %+v", result)
	}

	plan, err := NewRoutePlanQuery(RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: peer.id, DType: dtypeQuery},
		RoutePolicy:           domain.RoutePolicyBest,
	})
	if err != nil {
		t.Fatalf("NewRoutePlanQuery: %v", err)
	}
	explained, err := node.pipeline.scheduler.ExplainRoute(context.Background(), plan)
	if err != nil {
		t.Fatalf("ExplainRoute: %v", err)
	}
	if len(explained.Entries()) == 0 {
		t.Fatal("the plan is empty for a live neighbour")
	}
}
