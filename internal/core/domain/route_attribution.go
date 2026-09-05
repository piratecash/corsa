package domain

import "fmt"

// route_attribution.go answers TWO questions about one route and keeps them
// apart on purpose:
//
//   - RouteSource — how the route was learned and what proves it: an
//     announcement, a relay hop-ack, a live direct session, or the node
//     itself. This is the TRUST axis, and TrustRank orders it.
//   - DiscoveryPlane — WHICH plane found it: today's distance-vector mesh, or
//     the structured overlay. This axis carries no trust and takes part in no
//     ranking whatsoever.
//
// They are orthogonal because the two facts are not mutually exclusive. A
// session established thanks to an overlay lookup is SIMULTANEOUSLY a direct
// connection (RouteSourceDirect) and an overlay discovery; a single
// enumeration would keep one of those facts and silently lose the other,
// whichever value we picked. Folding the overlay into RouteSource would also
// have inserted a new value into TrustRank's ordering — that is, it would have
// re-ranked every existing route as a side effect of adding observability.
//
// RouteSource lives here rather than in internal/core/routing because both
// planes must be able to name it: the datagram layer's RouteResolver is the
// seam behind which the control plane is replaceable, and a hint shape that
// forced the future overlay resolver to import the mesh routing package would
// defeat exactly that seam. routing re-exports the type and its values, so the
// distance-vector plane's own call sites are unchanged.
//
// Reference: docs/refactoring/dht/02-route-source.md §1, §2.

// RouteSource indicates how a route was learned. The trust hierarchy is:
// direct > hop_ack > announcement. A route learned through a more trusted
// source is preferred over one with the same (identity, origin, nextHop)
// triple learned through a less trusted source.
//
// The numbering and the ordering are FIXED. They are read by the routing
// table's admission, eviction and update paths, so a new value or a reordered
// one changes route selection everywhere at once — see
// routing/route_store_admission.go and routing/route_store_mutation.go.
type RouteSource uint8

const (
	RouteSourceAnnouncement RouteSource = iota // learned via announce_routes frame
	RouteSourceHopAck                          // confirmed by relay_hop_ack
	RouteSourceDirect                          // directly connected peer
	RouteSourceLocal                           // synthetic: the node itself (hops=0, never expires)
)

// String returns a human-readable representation for logging and debugging.
func (s RouteSource) String() string {
	switch s {
	case RouteSourceLocal:
		return "local"
	case RouteSourceDirect:
		return "direct"
	case RouteSourceHopAck:
		return "hop_ack"
	case RouteSourceAnnouncement:
		return "announcement"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// TrustRank returns a numeric rank for comparison. Higher rank means
// more trusted. This avoids relying on iota ordering.
func (s RouteSource) TrustRank() int {
	switch s {
	case RouteSourceLocal:
		return 3
	case RouteSourceDirect:
		return 2
	case RouteSourceHopAck:
		return 1
	case RouteSourceAnnouncement:
		return 0
	default:
		return -1
	}
}

// DiscoveryPlane names the plane that produced a route.
//
// It has NO TrustRank twin, and that absence is the point. The composite
// resolver decides which plane is asked FIRST, which is a question of poll
// order; how much a route is believed is RouteSource's question. An earlier
// draft of this step put both on one scale and thereby turned "we now also
// look in the overlay" into a silent re-ranking of every mesh route.
type DiscoveryPlane uint8

const (
	// DiscoveryPlaneUnset is the zero value and never a valid answer: a route
	// nobody attributed is not "mesh by default". It exists so that "nobody
	// filled this in" stays distinguishable from a real plane.
	DiscoveryPlaneUnset DiscoveryPlane = iota
	// DiscoveryPlaneMesh is the distance-vector plane — today the only
	// producer of routes in the tree.
	DiscoveryPlaneMesh
	// DiscoveryPlaneOverlay is the structured overlay. No production code
	// produces it yet; it exists now because the fact it records — an overlay
	// lookup that ended in a direct session — is precisely the pair of facts
	// this type must be shown not to lose (02-route-source.md §5).
	DiscoveryPlaneOverlay
)

// String returns the metric and log label of the plane.
func (p DiscoveryPlane) String() string {
	switch p {
	case DiscoveryPlaneMesh:
		return "mesh"
	case DiscoveryPlaneOverlay:
		return "overlay"
	case DiscoveryPlaneUnset:
		return "unset"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

// Valid reports whether the plane names a real one. The unset zero value does
// not.
func (p DiscoveryPlane) Valid() bool {
	return p == DiscoveryPlaneMesh || p == DiscoveryPlaneOverlay
}

// RouteAttribution carries BOTH axes of one route and is the only shape in
// which they travel together.
//
// Its fields are unexported and its zero value is "nobody attributed this",
// which is a distinguishable state rather than a default. That is not
// decoration: the zero RouteSource is RouteSourceAnnouncement — a real value —
// so a bare pair of fields would make an unfilled attribution claim an origin
// it was never given, and RouteSource's numbering may not be changed to open a
// zero slot because that numbering IS the trust order.
//
// There is deliberately no constructor that takes a DiscoveryPlane argument:
// the two named constructors below make an invalid plane unrepresentable
// instead of refusable, so no call site has an error to forget.
type RouteAttribution struct {
	source RouteSource
	plane  DiscoveryPlane
	known  bool
}

// MeshRouteAttribution attributes a route to the distance-vector plane.
func MeshRouteAttribution(source RouteSource) RouteAttribution {
	return RouteAttribution{source: source, plane: DiscoveryPlaneMesh, known: true}
}

// OverlayRouteAttribution attributes a route to the structured overlay.
func OverlayRouteAttribution(source RouteSource) RouteAttribution {
	return RouteAttribution{source: source, plane: DiscoveryPlaneOverlay, known: true}
}

// UnattributedRoute is the explicit "nobody said which plane or which proof"
// value. It is a named constructor rather than a bare zero literal so that a
// producer choosing it says so.
func UnattributedRoute() RouteAttribution { return RouteAttribution{} }

// Known reports whether either axis was ever filled in.
func (a RouteAttribution) Known() bool { return a.known }

// Source returns the trust axis. The bool is false for an unattributed route:
// returning the bare zero would invent RouteSourceAnnouncement as an origin.
func (a RouteAttribution) Source() (RouteSource, bool) {
	if !a.known {
		return RouteSourceAnnouncement, false
	}
	return a.source, true
}

// Plane returns the discovery axis. The bool is false for an unattributed
// route, mirroring Source.
func (a RouteAttribution) Plane() (DiscoveryPlane, bool) {
	if !a.known {
		return DiscoveryPlaneUnset, false
	}
	return a.plane, true
}

// String renders both axes for a log line, as "<source>/<plane>".
func (a RouteAttribution) String() string {
	if !a.known {
		return "unattributed"
	}
	return a.source.String() + "/" + a.plane.String()
}
