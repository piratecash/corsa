package routing

// RoutePlane names which plane answers "how do I reach this identity".
//
// It exists as a declared switch rather than as something inferred from the
// code, and that is deliberate. Two earlier attempts to INFER "is the mesh
// still primary" both failed: a protocol-version floor does not imply anything
// about the routing table, and probing for the presence of Table.Lookup cannot
// notice the day that method starts answering from a bounded overlay cache
// while keeping its signature. A property that cannot be observed cannot guard
// anything.
//
// So it is stated. The structural-overlay work has to flip this constant to
// function at all — it is the switch that says which plane a lookup belongs to
// — and anything gated on it fires at that moment. The residual risk is honest
// and small: somebody could build the overlay and forget to flip the switch,
// in which case the overlay would be running as a secondary plane, which is
// what the earlier rollout steps do anyway.
type RoutePlane uint8

const (
	// RoutePlaneMesh: this node holds a route to every identity it cares
	// about — the full path-vector table.
	RoutePlaneMesh RoutePlane = iota
	// RoutePlaneOverlay: routes are resolved through the structural
	// overlay and the table is bounded. Anything that assumed a full table
	// is invalid from here.
	RoutePlaneOverlay
)

// ActiveRoutePlane is the plane this build resolves routes on.
//
// TODO(presence-route-fallback-removal): the overlay rollout flips this to
// RoutePlaneOverlay. Everything gated on it — starting with the presence route
// fallback in internal/core/node/presence_route_fallback.go — must be deleted
// in that same change; its guard test fails here to say so.
const ActiveRoutePlane = RoutePlaneMesh
