package domain

import "testing"

// route_attribution_test.go guards the ONE thing this step must not do while
// adding attribution: change how routes are ranked.
//
// The trust order lives in RouteSource.TrustRank and is read by the routing
// table's admission, eviction and update paths. The discovery plane is a
// second, orthogonal axis, and the failure mode the step's §1 names outright is
// folding it into the first — which would look like a new enum value and would
// silently re-rank every route in the tree.

// TestRouteSourceNumberingAndTrustRankArePinned fixes both the values and the
// ranks by number.
//
// The exhaustive sweep is the part that matters. A future value appended to the
// enum would not fail a table of the four known ones — it would simply not be
// listed — so the test asks the opposite question: which of the 256 possible
// values does TrustRank admit into the order at all? Exactly four may, and they
// must be these four with these ranks.
func TestRouteSourceNumberingAndTrustRankArePinned(t *testing.T) {
	t.Parallel()

	type expectation struct {
		label string
		rank  int
	}
	want := map[RouteSource]expectation{
		RouteSourceAnnouncement: {label: "announcement", rank: 0},
		RouteSourceHopAck:       {label: "hop_ack", rank: 1},
		RouteSourceDirect:       {label: "direct", rank: 2},
		RouteSourceLocal:        {label: "local", rank: 3},
	}

	// The numbering itself: the iota order IS the wire-free contract every
	// stored claim and every comparison was written against.
	if RouteSourceAnnouncement != 0 || RouteSourceHopAck != 1 || RouteSourceDirect != 2 || RouteSourceLocal != 3 {
		t.Fatalf("RouteSource numbering changed: %d/%d/%d/%d",
			RouteSourceAnnouncement, RouteSourceHopAck, RouteSourceDirect, RouteSourceLocal)
	}

	for source, expected := range want {
		if got := source.TrustRank(); got != expected.rank {
			t.Fatalf("%s.TrustRank() = %d, want %d: the trust order decides route admission and eviction",
				expected.label, got, expected.rank)
		}
		if got := source.String(); got != expected.label {
			t.Fatalf("RouteSource(%d).String() = %q, want %q", source, got, expected.label)
		}
	}

	for value := 0; value <= 255; value++ {
		source := RouteSource(value)
		_, known := want[source]
		ranked := source.TrustRank() >= 0
		if ranked != known {
			t.Fatalf("RouteSource(%d) is %v in the trust order, want %v: a new value in this enum re-ranks every existing route, which is why the discovery plane is a SEPARATE type",
				value, ranked, known)
		}
	}
}

// TestDiscoveryPlaneIsNotATrustRank is the guard on the other half of the split.
//
// The plane must stay a poll-order fact. A TrustRank method on it would be the
// exact mistake in a new spelling: it would put "found by the overlay" on the
// same scale as "proven by a live session", and the composite resolver's order
// would quietly become a statement about how much a route is believed.
func TestDiscoveryPlaneIsNotATrustRank(t *testing.T) {
	t.Parallel()

	if _, ranked := any(DiscoveryPlaneMesh).(interface{ TrustRank() int }); ranked {
		t.Fatal("DiscoveryPlane must not have a TrustRank: the plane decides who is ASKED first, not what is believed")
	}

	if DiscoveryPlaneUnset.Valid() {
		t.Fatal("the zero plane must not be valid: a route nobody attributed is not mesh by default")
	}
	for plane, label := range map[DiscoveryPlane]string{
		DiscoveryPlaneUnset:   "unset",
		DiscoveryPlaneMesh:    "mesh",
		DiscoveryPlaneOverlay: "overlay",
	} {
		if got := plane.String(); got != label {
			t.Fatalf("DiscoveryPlane(%d).String() = %q, want %q", plane, got, label)
		}
	}
	if !DiscoveryPlaneMesh.Valid() || !DiscoveryPlaneOverlay.Valid() {
		t.Fatal("both real planes must be valid")
	}
}

// TestRouteAttributionKeepsBothFactsAtOnce is the step's §5 criterion in its
// smallest form: a route can be a direct session AND an overlay discovery, and
// neither half may be derivable from — or destroyed by — the other.
func TestRouteAttributionKeepsBothFactsAtOnce(t *testing.T) {
	t.Parallel()

	overlayDirect := OverlayRouteAttribution(RouteSourceDirect)
	source, attributed := overlayDirect.Source()
	if !attributed || source != RouteSourceDirect {
		t.Fatalf("Source() = %v/%v, want direct: the overlay must not erase that this is a live session",
			source, attributed)
	}
	plane, named := overlayDirect.Plane()
	if !named || plane != DiscoveryPlaneOverlay {
		t.Fatalf("Plane() = %v/%v, want overlay: being a direct session must not erase who found it",
			plane, named)
	}
	if got := overlayDirect.String(); got != "direct/overlay" {
		t.Fatalf("String() = %q, want %q", got, "direct/overlay")
	}

	meshAnnounced := MeshRouteAttribution(RouteSourceAnnouncement)
	if got := meshAnnounced.String(); got != "announcement/mesh" {
		t.Fatalf("String() = %q, want %q", got, "announcement/mesh")
	}
}

// TestUnattributedRouteInventsNeitherAxis pins the reason the two axes travel
// inside a type instead of as two bare fields: the zero RouteSource is
// RouteSourceAnnouncement, a REAL value, so a struct nobody filled in would
// claim an origin it was never given.
func TestUnattributedRouteInventsNeitherAxis(t *testing.T) {
	t.Parallel()

	for name, attribution := range map[string]RouteAttribution{
		"named absence": UnattributedRoute(),
		"zero value":    {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if attribution.Known() {
				t.Fatal("an unfilled attribution must not claim to be known")
			}
			if source, attributed := attribution.Source(); attributed {
				t.Fatalf("Source() answered %v: an unfilled attribution must not invent an origin", source)
			}
			if plane, named := attribution.Plane(); named {
				t.Fatalf("Plane() answered %v: an unfilled attribution must not invent a plane", plane)
			}
			if got := attribution.String(); got != "unattributed" {
				t.Fatalf("String() = %q, want %q", got, "unattributed")
			}
		})
	}
}
