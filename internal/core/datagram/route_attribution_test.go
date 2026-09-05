package datagram

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// route_attribution_test.go covers the layer's half of step 02: the two axes
// reach the candidate and the plan intact, and neither of them reaches the
// ranking.
//
// Reference: docs/refactoring/dht/02-route-source.md §4, §5.

// planFor builds the read-only plan for one destination, which is the surface
// an operator and the node's JSON both read.
func planFor(t *testing.T, fixture *schedFixture, dst domain.PeerIdentity) RoutePlan {
	t.Helper()
	query, err := NewRoutePlanQuery(RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
		RoutePolicy:           domain.RoutePolicyBest,
	})
	if err != nil {
		t.Fatalf("NewRoutePlanQuery: %v", err)
	}
	plan, err := fixture.scheduler.ExplainRoute(context.Background(), query)
	if err != nil {
		t.Fatalf("ExplainRoute: %v", err)
	}
	return plan
}

// requireAttribution asserts both axes of one plan entry at once. Both or
// neither: an assertion that checked one axis would pass on exactly the bug
// the split exists to prevent.
func requireAttribution(
	t *testing.T,
	entry RoutePlanEntry,
	wantSource domain.RouteSource,
	wantPlane domain.DiscoveryPlane,
) {
	t.Helper()
	source, attributed := entry.Attribution.Source()
	if !attributed {
		t.Fatalf("next hop %s carries no attribution at all", entry.NextHop)
	}
	if source != wantSource {
		t.Fatalf("next hop %s: route source = %v, want %v", entry.NextHop, source, wantSource)
	}
	plane, named := entry.Attribution.Plane()
	if !named || plane != wantPlane {
		t.Fatalf("next hop %s: discovery plane = %v/%v, want %v", entry.NextHop, plane, named, wantPlane)
	}
}

// TestOverlayDiscoveredDirectSessionKeepsBothFacts is the §5 criterion at the
// layer's own surface, and it is about the branch that has no RouteHint at all.
//
// Step 1 of §4.3 promotes a live direct session ahead of the routing table. A
// session opened because the overlay answered a lookup is BOTH: direct by
// proof, overlay by discovery. The plan has to show both — the trust axis
// alone would erase the overlay's contribution exactly where it is the whole
// point, and the plane alone would deny that a live session is a live session.
//
// The mutation this kills: deriving one axis from the other — reporting `mesh`
// for anything direct, or `announcement` for anything the overlay produced.
func TestOverlayDiscoveredDirectSessionKeepsBothFacts(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("overlay-found-neighbour")

	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{string(schedDType)}),
		Discovery:               domain.DiscoveryPlaneOverlay,
		ReportedProtocolVersion: schedLocalVersion,
	})

	plan := planFor(t, fixture, dst)
	if plan.Len() != 1 {
		t.Fatalf("plan has %d entries, want the single direct candidate", plan.Len())
	}
	entry := plan.Entries()[0]
	if entry.NextHop != dst {
		t.Fatalf("first candidate = %s, want the destination itself", entry.NextHop)
	}
	requireAttribution(t, entry, domain.RouteSourceDirect, domain.DiscoveryPlaneOverlay)
}

// TestDirectSessionOfTheMeshIsAttributedToTheMesh is the negative control of
// the test above: the same branch, the other plane. Without it, an
// implementation that hard-coded `overlay` on the direct branch would pass.
func TestDirectSessionOfTheMeshIsAttributedToTheMesh(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("mesh-neighbour")
	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Hour, string(schedDType)))

	entry := planFor(t, fixture, dst).Entries()[0]
	requireAttribution(t, entry, domain.RouteSourceDirect, domain.DiscoveryPlaneMesh)
}

// TestUnattributedConnectionIsNotDefaultedToMesh pins the direction the layer
// refuses to guess in.
//
// The layer does not know which plane its resolver is, so a connection that
// names none produces no attribution rather than a mesh one. Defaulting would
// put a claim in an operator's console that no plane ever made — and on the day
// a second plane exists, it would attribute that plane's routes to the mesh.
func TestUnattributedConnectionIsNotDefaultedToMesh(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("plane-less-neighbour")
	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{string(schedDType)}),
		ReportedProtocolVersion: schedLocalVersion,
	})

	entry := planFor(t, fixture, dst).Entries()[0]
	if entry.Attribution.Known() {
		t.Fatalf("a connection that named no plane was attributed %s", entry.Attribution)
	}
}

// TestRoutedHintAttributionSurvivesToThePlan covers the other producer: the
// resolver's own attribution travels through ranking to the diagnostic
// unchanged, including a source the mesh alone could never claim.
func TestRoutedHintAttributionSurvivesToThePlan(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("remote-destination")
	relay := domaintest.ID("relay-hop")
	fixture.datagramPeer(relay, time.Hour, string(schedDType))

	hint := fixture.route(relay, 3)
	hint.Attribution = domain.OverlayRouteAttribution(domain.RouteSourceHopAck)
	fixture.routes.set(dst, hint)

	entry := planFor(t, fixture, dst).Entries()[0]
	if entry.NextHop != relay {
		t.Fatalf("first candidate = %s, want the relay", entry.NextHop)
	}
	requireAttribution(t, entry, domain.RouteSourceHopAck, domain.DiscoveryPlaneOverlay)
}

// TestDiscoveryPlaneDoesNotRankCandidates is the §4 boundary: this step adds
// attribution, it does not move ranking.
//
// Both directions are checked with the SAME topology and only the planes
// swapped, so the assertion is about the plane and nothing else. Under a
// comparator that had learned to prefer a plane, one of the two orders would
// flip; under the real comparator both are decided by hops alone.
func TestDiscoveryPlaneDoesNotRankCandidates(t *testing.T) {
	t.Parallel()

	near := domaintest.ID("near-relay")
	far := domaintest.ID("far-relay")

	cases := map[string]struct {
		nearPlane domain.RouteAttribution
		farPlane  domain.RouteAttribution
	}{
		"near hop found by the mesh": {
			nearPlane: domain.MeshRouteAttribution(domain.RouteSourceAnnouncement),
			farPlane:  domain.OverlayRouteAttribution(domain.RouteSourceAnnouncement),
		},
		"near hop found by the overlay": {
			nearPlane: domain.OverlayRouteAttribution(domain.RouteSourceAnnouncement),
			farPlane:  domain.MeshRouteAttribution(domain.RouteSourceAnnouncement),
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fixture := newSchedFixture(t, schedFixtureOpts{})
			dst := domaintest.ID("remote-destination")
			// Identical connections: same version, same uptime. Hops is the only
			// key left that can separate them.
			fixture.datagramPeer(near, time.Hour, string(schedDType))
			fixture.datagramPeer(far, time.Hour, string(schedDType))

			nearHint := fixture.route(near, 2)
			nearHint.Attribution = testCase.nearPlane
			farHint := fixture.route(far, 5)
			farHint.Attribution = testCase.farPlane
			// Offered far-first so a comparator that ignored hops and kept
			// insertion order would also fail.
			fixture.routes.set(dst, farHint, nearHint)

			entries := planFor(t, fixture, dst).Entries()
			if len(entries) != 2 {
				t.Fatalf("plan has %d entries, want 2", len(entries))
			}
			if entries[0].NextHop != near {
				t.Fatalf("best candidate = %s, want the 2-hop relay %s: the discovery plane must not rank",
					entries[0].NextHop, near)
			}
		})
	}
}
