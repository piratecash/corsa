package node

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_route_attribution_test.go is step 02's exit criterion at the surface
// an operator actually reads: both axes have to survive the whole way from the
// node's own resolver, through the composite, the ranking and the plan, into
// the JSON — and be rendered as two independent facts.
//
// It is a node-level test on purpose. The layer's own tests drive fakes; this
// one drives the PRODUCTION wiring, which is where the composite resolver, the
// mesh attribution of the routing table and the plane of a live connection are
// really joined.
//
// Reference: docs/refactoring/dht/02-route-source.md §5.

// datagramPlanCandidate is the wire shape ExplainDatagramRoute renders, read
// back through the same JSON a console would.
type datagramPlanCandidate struct {
	NextHop        string `json:"next_hop"`
	RouteSource    string `json:"route_source"`
	DiscoveryPlane string `json:"discovery_plane"`
}

func datagramPlanCandidates(t *testing.T, raw json.RawMessage) []datagramPlanCandidate {
	t.Helper()
	var plan struct {
		Candidates []datagramPlanCandidate `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatalf("unmarshal route plan: %v", err)
	}
	return plan.Candidates
}

// TestRoutePlanRendersBothAttributionAxes pins the two fields and, more
// importantly, that they are TWO: a reader has to be able to tell "proven by a
// live session" from "found by the mesh", because on the day a second plane
// exists those stop coinciding.
//
// The mutation this kills: rendering one field derived from the other — a
// `discovery_plane` computed from the route source, or a `route_source`
// inferred from the plane — which would look correct today and would lose
// exactly the overlay-found direct session step 09 has to observe.
func TestRoutePlanRendersBothAttributionAxes(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerServiceOfType(t, domain.NodeTypeFull)
	registerFixtureDatagramTypes(t, svc)

	const dtype = domain.DType("push_identity")
	neighbour := domain.PeerIdentityFromWire(strings.Repeat("4", 40))
	installDatagramPeer(t, svc, neighbour, datagramPeerConn{
		version:     domain.ProtocolVersion(config.ProtocolVersion),
		connectedAt: time.Now().UTC().Add(-time.Hour),
	})

	raw, err := svc.ExplainDatagramRoute(
		context.Background(), neighbour, dtype, domain.RoutePolicyBest)
	if err != nil {
		t.Fatalf("ExplainDatagramRoute: %v", err)
	}
	candidates := datagramPlanCandidates(t, raw)
	if len(candidates) == 0 {
		t.Fatalf("plan has no candidates for a live neighbour: %s", raw)
	}

	best := candidates[0]
	if best.NextHop != neighbour.String() {
		t.Fatalf("best candidate = %s, want the neighbour itself", best.NextHop)
	}
	// Direct because a live session proves it; mesh because that is the plane
	// this build opens sessions through. Two statements, two fields.
	if best.RouteSource != "direct" {
		t.Fatalf("route_source = %q, want %q: a live session is the strongest proof the table has",
			best.RouteSource, "direct")
	}
	if best.DiscoveryPlane != "mesh" {
		t.Fatalf("discovery_plane = %q, want %q: this build finds every connection through the mesh",
			best.DiscoveryPlane, "mesh")
	}
}
