package datagram

import (
	"context"
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
)

// composite_resolver.go is the place a second route plane is plugged in.
//
// RouteResolver has always been declared as the seam behind which the control
// plane is replaceable (§4.3), but it had exactly one implementation and every
// caller was wired straight to it. Introducing the composite now — with a
// single source — is what makes step 09 an addition to ONE literal instead of
// a change to the scheduler, the node wiring and every test that builds them.
//
// Reference: docs/refactoring/dht/02-route-source.md §2,
// docs/refactoring/dht-dualstack-migration.md §4.2.

// ErrNoRouteSources marks a composite built with nothing to ask.
var ErrNoRouteSources = errors.New("datagram: composite route resolver requires at least one source")

// CompositeRouteResolver asks several route sources IN A FIXED ORDER and
// concatenates what they answer.
//
// # The order is a poll order, never a trust rank
//
// Which source is asked first decides only who gets to name a next hop FIRST,
// and therefore which attribution survives when two planes offer the same next
// hop and the ranking comparator cannot separate them. It says nothing about
// how much a route is believed: that is RouteSource.TrustRank's question, it
// lives in the routing table, and this type deliberately has no say in it.
// Ranking stays entirely in candidates.go, over keys that do not include the
// plane.
//
// # The order comes from the release, not from configuration
//
// The sources are fixed when the resolver is constructed and there is no
// setter. A deployment cannot reorder the planes, because a node whose poll
// order differs from its neighbours' is a node whose behaviour cannot be
// reasoned about from its version — see dht-dualstack-migration.md §4.2, which
// makes the same call about the roll-out switch.
//
// # One source changes nothing, structurally
//
// With a single source the composite forwards that source's own answer,
// untouched and uncopied: "introducing the composite did not change a single
// next-hop decision" is then a property of this code rather than a claim a
// test has to keep re-establishing. The test exists anyway (§4 of the step),
// but it confirms the property instead of carrying it.
type CompositeRouteResolver struct {
	// sources is the poll order. Never empty — the constructor refuses that —
	// and never mutated after construction.
	sources []RouteResolver
}

// NewCompositeRouteResolver fixes the poll order at construction.
//
// An empty list and a nil source are refused rather than skipped: a composite
// that silently dropped a plane would answer with half the network and look
// exactly like a topology with no route to give, which is the failure mode the
// dual-stack phases spend their whole soak window trying to detect.
func NewCompositeRouteResolver(sources ...RouteResolver) (*CompositeRouteResolver, error) {
	if len(sources) == 0 {
		return nil, ErrNoRouteSources
	}
	ordered := make([]RouteResolver, 0, len(sources))
	for i, source := range sources {
		if isNilValue(source) {
			return nil, errNilRouteSource(i)
		}
		ordered = append(ordered, source)
	}
	return &CompositeRouteResolver{sources: ordered}, nil
}

// errNilRouteSource names the position so a wiring mistake in a list of planes
// says which entry is missing rather than only that one is.
func errNilRouteSource(position int) error {
	return fmt.Errorf("datagram: composite route resolver source %d is nil", position)
}

// FreshRoutes asks every source for the per-destination lookup, in poll order.
func (c *CompositeRouteResolver) FreshRoutes(ctx context.Context, dst domain.PeerIdentity) []RouteHint {
	return c.collect(func(source RouteResolver) []RouteHint {
		return source.FreshRoutes(ctx, dst)
	})
}

// CachedRoutes asks every source for the coalesced snapshot, in poll order.
//
// The freshness contract of §4.3 is per SOURCE, not per composite: each plane
// answers this call from whatever it treats as its cheap read, exactly as it
// would if the scheduler held it directly.
func (c *CompositeRouteResolver) CachedRoutes(ctx context.Context, dst domain.PeerIdentity) []RouteHint {
	return c.collect(func(source RouteResolver) []RouteHint {
		return source.CachedRoutes(ctx, dst)
	})
}

// collect walks the sources in poll order and joins their answers.
//
// The single-source branch is not an optimisation. It is what makes the
// no-behaviour-change guarantee structural: with one plane the caller receives
// the very slice that plane returned, so there is no reallocation, no
// reordering and no nil-versus-empty difference for anything downstream to
// notice.
func (c *CompositeRouteResolver) collect(ask func(RouteResolver) []RouteHint) []RouteHint {
	if len(c.sources) == 1 {
		return ask(c.sources[0])
	}
	var hints []RouteHint
	for _, source := range c.sources {
		hints = append(hints, ask(source)...)
	}
	return hints
}
