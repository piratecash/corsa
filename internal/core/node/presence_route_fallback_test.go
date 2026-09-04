package node

import (
	"os"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestPresenceRouteFallbackIsStillNeeded is a guard, not a behaviour test.
//
// It fails on the day routing.ActiveRoutePlane becomes RoutePlaneOverlay — the
// day this node stops holding a route to every identity it cares about, and the
// fallback has nothing left to fall back TO. The correct response to this test
// failing is to DELETE presence_route_fallback.go, the
// PresenceSourceRouteFallback member, the striped avatar and this test; not to
// flip the switch back.
//
// Written this way round because a temporary bridge with no alarm on it is a
// permanent one. This codebase already carries two of these
// (TODO(receipt-sender-ack-gate-removal), TODO(transit-age-restamp-removal)),
// and both are found by their guards rather than by anyone remembering.
func TestPresenceRouteFallbackIsStillNeeded(t *testing.T) {
	if !presenceRouteFallbackStillNeeded() {
		t.Fatal(
			"routing.ActiveRoutePlane is the overlay, so this node no longer holds " +
				"a route to every identity it cares about. The route " +
				"fallback has nothing to fall back TO — it is not 'worse' at that " +
				"point, it is meaningless. Delete presence_route_fallback.go, " +
				"domain.PresenceSourceRouteFallback, the striped avatar in the " +
				"desktop contact list and this test " +
				"(TODO(presence-route-fallback-removal)).")
	}
}

// TestFallbackGuardWatchesTheRoutePlane pins WHAT the guard is gated on.
//
// Not a protocol floor (proves nothing about the table) and not the existence
// of Table.Lookup (a bounded overlay can keep that method and its signature).
// It watches routing.ActiveRoutePlane — the switch the overlay work has to flip
// to function — so the guard fires on the change that actually invalidates the
// fallback.
func TestFallbackGuardWatchesTheRoutePlane(t *testing.T) {
	if routing.ActiveRoutePlane != routing.RoutePlaneMesh {
		t.Fatalf("route plane is %v: see TestPresenceRouteFallbackIsStillNeeded",
			routing.ActiveRoutePlane)
	}
	// The guard must be a function OF that switch, not a constant true. If
	// the two ever disagree the guard has stopped watching anything.
	if presenceRouteFallbackStillNeeded() != (routing.ActiveRoutePlane == routing.RoutePlaneMesh) {
		t.Fatal("the fallback guard no longer follows routing.ActiveRoutePlane: " +
			"it cannot fire when the overlay becomes the primary plane")
	}
}

// TestFallbackRetirementIsNotGatedOnTheProtocolFloor pins WHY the trigger is
// what it is.
//
// The first version compared MinimumProtocolVersion against a constant, and
// that is wrong in both directions: every peer can speak the newest version
// while this node still holds a full routing table (the fallback still works,
// and the guard would demand its removal early), and the overlay can become
// primary with no floor bump at all (the fallback becomes meaningless, and the
// guard would say nothing). The property that matters is whether a full routing
// table exists, not what version the network speaks.
func TestFallbackRetirementIsNotGatedOnTheProtocolFloor(t *testing.T) {
	source, err := os.ReadFile("presence_route_fallback.go")
	if err != nil {
		t.Fatalf("reading presence_route_fallback.go: %v", err)
	}
	if strings.Contains(string(source), "config.MinimumProtocolVersion") {
		t.Fatalf("the fallback's retirement is gated on the protocol floor again "+
			"(current floor %d). A floor bump neither proves the full routing "+
			"table is gone nor is required for the overlay to take over; gate on "+
			"the route plane instead.", config.MinimumProtocolVersion)
	}
}

// TestFallbackPresenceIsNeverProven pins the property that makes the fallback
// acceptable at all: it is online, and it never claims to be evidence. If this
// ever passes as proven, the interface would draw an assumption exactly like a
// signature, and the honesty the source distinction buys is gone.
func TestFallbackPresenceIsNeverProven(t *testing.T) {
	presence := presenceFromRouteFallback()

	if presence.State != domain.PresenceOnline {
		t.Fatalf("fallback state: got %s, want online", presence.State)
	}
	if presence.Source != domain.PresenceSourceRouteFallback {
		t.Fatalf("fallback source: got %s", presence.Source)
	}
	if presence.IsProven() {
		t.Fatal("the route fallback must never read as proven")
	}
	if !presence.IsInferred() {
		t.Fatal("the route fallback must read as inferred")
	}
}
