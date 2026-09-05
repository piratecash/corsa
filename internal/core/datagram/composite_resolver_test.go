package datagram

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// composite_resolver_test.go carries the §4 acceptance criterion of step 02:
// "the composite with one element must not change behaviour, and that is
// proved by a test rather than argued".
//
// Reference: docs/refactoring/dht/02-route-source.md §4, §5.

// staticRouteSource is a resolver that answers from a fixed pair of tables and
// records what it was asked. Two of them make the poll order observable.
type staticRouteSource struct {
	fresh  []RouteHint
	cached []RouteHint
	// asked counts calls of both methods, so a test can show that a source was
	// consulted rather than merely that its hints are absent from the answer.
	asked int
}

func (s *staticRouteSource) FreshRoutes(context.Context, domain.PeerIdentity) []RouteHint {
	s.asked++
	return s.fresh
}

func (s *staticRouteSource) CachedRoutes(context.Context, domain.PeerIdentity) []RouteHint {
	s.asked++
	return s.cached
}

func hintVia(nextHop domain.PeerIdentity, hops int, attribution domain.RouteAttribution) RouteHint {
	return RouteHint{NextHop: nextHop, Hops: hops, Attribution: attribution}
}

// TestCompositeRefusesAPlaneItCannotAsk keeps a half-wired composite from
// answering with part of the network.
//
// A dropped source and an empty topology are indistinguishable from the
// outside: both look like "no route". That is precisely the signal the
// dual-stack phases spend their soak windows watching, so the failure has to
// happen at construction, where it names itself.
func TestCompositeRefusesAPlaneItCannotAsk(t *testing.T) {
	t.Parallel()

	if _, err := NewCompositeRouteResolver(); !errors.Is(err, ErrNoRouteSources) {
		t.Fatalf("empty composite error = %v, want ErrNoRouteSources", err)
	}
	if _, err := NewCompositeRouteResolver(&staticRouteSource{}, nil); err == nil {
		t.Fatal("a nil source must be refused: a silently dropped plane answers with half the network")
	}
	// A TYPED nil satisfies `!= nil`, which is how a nil check written the
	// obvious way lets exactly the wiring mistake it exists for through.
	var typedNil *staticRouteSource
	if _, err := NewCompositeRouteResolver(typedNil); err == nil {
		t.Fatal("a typed-nil source must be refused")
	}
}

// TestCompositeWithOneSourceForwardsThatSourcesAnswer is the structural half of
// the no-behaviour-change guarantee: with a single plane the caller receives
// the very slice that plane returned — not a reordering, not a copy, not an
// empty slice where the source said nil.
func TestCompositeWithOneSourceForwardsThatSourcesAnswer(t *testing.T) {
	t.Parallel()

	relay := domaintest.ID("relay")
	source := &staticRouteSource{
		fresh:  []RouteHint{hintVia(relay, 2, domain.MeshRouteAttribution(domain.RouteSourceAnnouncement))},
		cached: []RouteHint{hintVia(relay, 4, domain.MeshRouteAttribution(domain.RouteSourceHopAck))},
	}
	composite, err := NewCompositeRouteResolver(source)
	if err != nil {
		t.Fatalf("NewCompositeRouteResolver: %v", err)
	}

	dst := domaintest.ID("destination")
	fresh := composite.FreshRoutes(context.Background(), dst)
	if len(fresh) != 1 || &fresh[0] != &source.fresh[0] {
		t.Fatal("FreshRoutes must forward the single source's own slice untouched")
	}
	cached := composite.CachedRoutes(context.Background(), dst)
	if len(cached) != 1 || &cached[0] != &source.cached[0] {
		t.Fatal("CachedRoutes must forward the single source's own slice untouched")
	}

	// nil must survive as nil: an empty non-nil slice is a different statement
	// to anything downstream that distinguishes them.
	empty, err := NewCompositeRouteResolver(&staticRouteSource{})
	if err != nil {
		t.Fatalf("NewCompositeRouteResolver: %v", err)
	}
	if got := empty.FreshRoutes(context.Background(), dst); got != nil {
		t.Fatalf("FreshRoutes of a silent source = %v, want nil", got)
	}
}

// TestCompositeAsksEverySourceInPollOrder covers the shape step 09 will use:
// every plane is asked, and their answers are joined in the order the release
// fixed — which is what decides whose attribution a duplicate next hop keeps.
func TestCompositeAsksEverySourceInPollOrder(t *testing.T) {
	t.Parallel()

	first := domaintest.ID("first-plane-hop")
	second := domaintest.ID("second-plane-hop")
	mesh := &staticRouteSource{
		fresh:  []RouteHint{hintVia(first, 1, domain.MeshRouteAttribution(domain.RouteSourceDirect))},
		cached: []RouteHint{hintVia(first, 1, domain.MeshRouteAttribution(domain.RouteSourceDirect))},
	}
	overlay := &staticRouteSource{
		fresh:  []RouteHint{hintVia(second, 3, domain.OverlayRouteAttribution(domain.RouteSourceAnnouncement))},
		cached: []RouteHint{hintVia(second, 3, domain.OverlayRouteAttribution(domain.RouteSourceAnnouncement))},
	}
	composite, err := NewCompositeRouteResolver(mesh, overlay)
	if err != nil {
		t.Fatalf("NewCompositeRouteResolver: %v", err)
	}

	dst := domaintest.ID("destination")
	got := composite.FreshRoutes(context.Background(), dst)
	want := []RouteHint{mesh.fresh[0], overlay.fresh[0]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FreshRoutes = %v, want the sources joined in poll order %v", got, want)
	}
	if mesh.asked != 1 || overlay.asked != 1 {
		t.Fatalf("sources asked %d/%d times, want each exactly once", mesh.asked, overlay.asked)
	}

	got = composite.CachedRoutes(context.Background(), dst)
	if !reflect.DeepEqual(got, []RouteHint{mesh.cached[0], overlay.cached[0]}) {
		t.Fatalf("CachedRoutes = %v, want the sources joined in poll order", got)
	}
}

// TestCompositeWithOneSourceDecidesIdenticallyToTheBareResolver is the
// behavioural half of the criterion, and it compares the CANDIDATES rather than
// a rendered plan: RouteCandidate holds every ranking key the send walks, so a
// deep comparison of the two selections is the strongest available reading of
// "the same next-hop decisions on the same input".
//
// The topology is deliberately one the ranking has to work on — a direct
// session, several relays of different hop counts and versions, plus a
// withdrawn and an expired entry — so the comparison covers the filters, the
// dedup and the sort rather than a single trivial hop.
func TestCompositeWithOneSourceDecidesIdenticallyToTheBareResolver(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("remote-destination")

	near := domaintest.ID("near-relay")
	far := domaintest.ID("far-relay")
	stale := domaintest.ID("expired-relay")
	gone := domaintest.ID("withdrawn-relay")
	fixture.datagramPeer(near, 2*time.Hour, string(schedDType))
	fixture.datagramPeer(far, time.Hour, string(schedDType))
	fixture.datagramPeer(stale, time.Hour, string(schedDType))
	fixture.datagramPeer(gone, time.Hour, string(schedDType))
	// The destination is also a live neighbour, so the direct branch of §4.3
	// contributes element 0 and the ranked list must not duplicate it.
	fixture.direct.set(dst, fixture.datagramPeer(dst, 3*time.Hour, string(schedDType)))

	expired := fixture.route(stale, 2)
	expired.ExpiresAt = fixture.clock().Add(-time.Minute)
	withdrawn := fixture.route(gone, 2)
	withdrawn.Withdrawn = true
	fixture.routes.set(dst,
		fixture.route(far, 5),
		fixture.route(near, 2),
		// A second hint for the same next hop: the dedup branch has to pick one.
		fixture.route(near, 4),
		expired,
		withdrawn,
	)

	composite, err := NewCompositeRouteResolver(fixture.routes)
	if err != nil {
		t.Fatalf("NewCompositeRouteResolver: %v", err)
	}
	wrapped, err := NewScheduler(SchedulerConfig{
		Routes:               composite,
		Peers:                fixture.peers,
		Direct:               fixture.direct,
		Secret:               schedSecret{secret: []byte("node-local-secret")},
		Clock:                fixture.clock,
		LocalID:              fixture.local,
		LocalProtocolVersion: schedLocalVersion,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	frame := protocol.DatagramFrame{Dst: dst, DType: schedDType}
	opts := selectionOpts{incomingPeer: LocalIngress(), avoid: NoAvoidedNextHop()}

	bare := fixture.scheduler.ordinaryCandidates(context.Background(), frame, opts)
	through := wrapped.ordinaryCandidates(context.Background(), frame, opts)

	if len(bare.candidates) < 3 {
		t.Fatalf("fixture produced %d candidates: too few to prove the ranking was exercised", len(bare.candidates))
	}
	if !reflect.DeepEqual(bare, through) {
		t.Fatalf("composite changed the selection:\n bare = %+v\n composite = %+v", bare, through)
	}
}
