package node

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_query_validation_test.go pins the read-only diagnostics at the
// SERVICE level rather than at the RPC edge.
//
// The layer now refuses a reachability or route-plan query that no real send
// could have built, and the refusal has to reach every consumer — not only the
// ones that arrive through the console. An in-process caller (the desktop UI,
// a future artifact owner, anything holding a *Service) bypasses the RPC
// surface entirely, so a validation that lived there would leave exactly those
// callers able to ask about an unsendable datagram and be handed a verdict
// about it. A "reachable: false" for a query the gates could never evaluate is
// worse than an error: it names a destination as unreachable when it is nothing
// of the sort.

func datagramQueryService(t *testing.T) (*Service, domain.PeerIdentity) {
	t.Helper()
	svc := newDatagramLayerService(t, true)
	return svc, domain.PeerIdentityFromWire(strings.Repeat("7", 40))
}

// TestReachabilityRefusesAQueryNoSendCouldBuild is the finding at Service
// scope: every refusal is the LAYER's, reached through the constructor, and
// every one of them surfaces as an error rather than as a verdict.
//
// The mutation this kills: building the query with a struct literal again (now
// impossible — the fields are unexported), or dropping the constructor's error
// on the floor and probing with whatever it returned.
func TestReachabilityRefusesAQueryNoSendCouldBuild(t *testing.T) {
	t.Parallel()

	svc, peer := datagramQueryService(t)
	const dtype = domain.DType("push_identity")

	cases := []struct {
		name  string
		dst   domain.PeerIdentity
		dtype domain.DType
	}{
		{name: "no destination", dtype: dtype},
		{name: "no dtype", dst: peer},
		{
			// The last-hop gate is decided by the dtype, so a name the wire
			// would refuse is not a question about reachability.
			name:  "malformed dtype",
			dst:   peer,
			dtype: domain.DType("Push Identity"),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			answer, err := svc.DatagramReachable(
				context.Background(), testCase.dst, testCase.dtype)
			if err == nil {
				t.Fatalf("DatagramReachable answered %s for a query no send could build: an in-process caller is handed a verdict about an unsendable datagram", answer)
			}
			if !errors.Is(err, datagram.ErrInvalidQuery) {
				t.Fatalf("DatagramReachable error = %v, want one wrapping ErrInvalidQuery so a caller can tell a bad question from a bad answer", err)
			}
		})
	}
}

// TestRoutePlanRefusesAQueryNoSendCouldBuild covers the plan, whose extra input
// is the policy — and the ZERO policy is refused rather than defaulted, because
// `best` and `explore` answer different questions and a plan that quietly
// picked one would describe a send the caller never asked about.
func TestRoutePlanRefusesAQueryNoSendCouldBuild(t *testing.T) {
	t.Parallel()

	svc, peer := datagramQueryService(t)
	const dtype = domain.DType("push_identity")

	cases := []struct {
		name   string
		dst    domain.PeerIdentity
		dtype  domain.DType
		policy domain.RoutePolicy
	}{
		{name: "no destination", dtype: dtype, policy: domain.RoutePolicyBest},
		{name: "no dtype", dst: peer, policy: domain.RoutePolicyBest},
		{name: "no route policy", dst: peer, dtype: dtype},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			plan, err := svc.ExplainDatagramRoute(
				context.Background(), testCase.dst, testCase.dtype, testCase.policy)
			if err == nil {
				t.Fatalf("ExplainDatagramRoute answered %s for a query no send could build", plan)
			}
			if !errors.Is(err, datagram.ErrInvalidQuery) {
				t.Fatalf("ExplainDatagramRoute error = %v, want one wrapping ErrInvalidQuery", err)
			}
		})
	}
}

// TestAValidQueryStillAnswers is the negative control: the refusals above must
// come from the query being unbuildable, not from the diagnostics having become
// unusable.
func TestAValidQueryStillAnswers(t *testing.T) {
	t.Parallel()

	svc, peer := datagramQueryService(t)
	const dtype = domain.DType("push_identity")

	if _, err := svc.DatagramReachable(context.Background(), peer, dtype); err != nil {
		t.Fatalf("DatagramReachable refused a well-formed query: %v", err)
	}
	if _, err := svc.ExplainDatagramRoute(
		context.Background(), peer, dtype, domain.RoutePolicyBest); err != nil {
		t.Fatalf("ExplainDatagramRoute refused a well-formed query: %v", err)
	}
}
