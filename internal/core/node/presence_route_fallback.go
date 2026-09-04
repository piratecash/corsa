package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TODO(presence-route-fallback-removal): DELETE THIS FILE, its test, the
// domain.PresenceSourceRouteFallback member and the interface's outlined
// indicator, once presence no longer has a full routing table to fall back to.
//
// # What this is
//
// A contact that cannot answer a liveness probe — an old build with no datagram
// layer, a node with the layer disabled, a contact whose identity record we have
// never resolved — can still be SEEN by routing. This file is the decision to
// show that, labelled as the weaker claim it is, instead of reporting ignorance.
//
// # Why it exists, given that the document says otherwise
//
// The design note this came from prescribes `unknown` for a
// contact that is not probeable, and that is the right answer in a network where
// probing is normal. It is the wrong answer TODAY, when almost nobody can answer
// a probe yet: it would replace a presence that is sometimes wrong with no
// presence at all, and a user reads that as the feature having broken rather
// than as the software being careful. The owner decided for the fallback with
// that trade named (2026-09-03).
//
// The honesty the document was protecting is kept somewhere better: in the
// SOURCE. A fallback presence is domain.PresenceSourceRouteFallback, it answers
// false to IsProven, and the interface draws it as a STRIPED green avatar
// against the plain green of a witnessed one. The user is told the difference
// between "they answered" and "we believe so"; they are simply not told
// nothing.
//
// # What is deliberately NOT done here
//
// The fallback is for contacts that CANNOT be probed. A probeable contact that
// merely has not answered stays on the honest path and goes offline after
// presenceDetectMult silent probes, even while a route to them is still in the
// table. Extending the fallback to them would be the comfortable choice and it
// would undo the entire point: a stale route lives up to ten minutes, so honest
// offline would simply never happen. TestProbeableContactNeverUsesTheFallback
// holds that line.
//
// It also does not override a suppression of ours: a route WE removed says
// nothing about the contact whether or not they can be probed
// (TestFallbackStillRespectsOurOwnSuppression).
//
// # When it goes
//
// The fallback rests on this node holding a route to every identity it cares
// about. That is exactly the property the structural overlay removes: the mesh
// becomes repair-only and the table becomes bounded. At that point there is no
// full table to fall back TO, and the fallback is not "worse", it is
// meaningless.
//
// # The tripwire, and an honest account of what it can and cannot do
//
// Two earlier versions of this guard were fake, and it is worth saying why so
// the third one is not replaced by a fourth.
//
//  1. A MinimumProtocolVersion floor. Wrong in both directions: a floor bump
//     proves nothing about the routing table, and the overlay can take over
//     without one.
//  2. Probing for the existence of Table.Lookup. That only detects the method
//     being deleted or re-signed. The likely shape of the change is neither:
//     Lookup keeps its signature and starts answering from a bounded overlay
//     cache, and the guard stays green at exactly the moment the fallback
//     stops making sense.
//
// The property "is the routing table still full" is not observable from here.
// So it is DECLARED, once, next to the table that owns it:
// routing.ActiveRoutePlane.
//
// And it is not merely declared. The constant gates the real path:
// routing.Table's full-table candidate collection asserts on it, so a build
// that sets the plane to the overlay and still asks for every claim about an
// arbitrary identity fails at that line rather than silently returning a
// partial answer. That is what makes this a tripwire rather than a note — the
// switch is load-bearing, so it cannot be left un-flipped by a working
// overlay, and it cannot be flipped without this guard firing.

// presenceRouteFallbackStillNeeded reports whether this node still has a full
// routing table to fall back TO.
//
// The guard test asserts it, and the fix when it fails is to delete this file
// rather than to update the test, which is the point of writing it this way
// round.
func presenceRouteFallbackStillNeeded() bool {
	return routing.ActiveRoutePlane == routing.RoutePlaneMesh
}

// presenceFromRouteFallback is the presence of a contact that routing can see
// and a probe cannot reach.
//
// Online, because a route to them exists and our own connectivity is healthy
// and we have not suppressed anything — the same inference the whole product
// ran on until now. Attributed to the fallback, because that inference is not
// evidence about the person, and every reader downstream is able to tell.
func presenceFromRouteFallback() domain.Presence {
	return domain.OnlinePresence(domain.PresenceSourceRouteFallback)
}
