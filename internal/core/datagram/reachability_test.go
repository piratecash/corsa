package datagram

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
)

// reachability_test.go covers the two read-only surfaces: the probe and the
// plan. The centrepiece is the parity test against the file router's
// ExplainRoute — the behaviour §4.3 says is carried over unchanged, checked
// entry by entry on a set of topologies rather than described in prose.

// ---------------------------------------------------------------------------
// Parity with the file router's ExplainRoute
// ---------------------------------------------------------------------------

// parityPeer describes one peer once, so both routers are built from the
// same facts and a divergence cannot be an artefact of two fixtures.
type parityPeer struct {
	id       domain.PeerIdentity
	age      time.Duration
	reported domain.ProtocolVersion
	usable   bool
}
type parityRoute struct {
	nextHop   domain.PeerIdentity
	hops      int
	withdrawn bool
}
type parityTopology struct {
	name   string
	dst    domain.PeerIdentity
	peers  []parityPeer
	routes []parityRoute
}

// parityDType is the type both plans are asked about. Every parity peer
// declares it, so the last-hop gate never fires and the comparison stays about
// candidate ORDER — the property §9 requires to be carried over unchanged.
const parityDType domain.DType = "file_transfer"

// parityNonceCache is the trivial NonceCache the file router requires; the
// plan never touches it.
type parityNonceCache struct{ seen map[string]struct{} }

func (c *parityNonceCache) Has(nonce string) bool {
	_, ok := c.seen[nonce]
	return ok
}

func (c *parityNonceCache) TryAdd(nonce string) bool {
	if c.Has(nonce) {
		return false
	}
	c.seen[nonce] = struct{}{}
	return true
}

// TestRoutePlanMatchesFileRouterExplainRoute is the §9 requirement: the
// plan of the datagram layer agrees with today's ExplainRoute across a set
// of topologies, entry by entry and field by field. The two implementations
// are wired from one description of the same network, and the version cap —
// which today lives in the node layer and now lives in the layer itself —
// is exercised by the inflated peer in the last topology.
func TestRoutePlanMatchesFileRouterExplainRoute(t *testing.T) {
	t.Parallel()

	dst := domaintest.ID("destination-identity")
	relayClose := domaintest.ID("relay-close")
	relayFar := domaintest.ID("relay-far")
	stalled := domaintest.ID("relay-stalled")

	topologies := []parityTopology{
		{
			name: "ranked relays",
			dst:  dst,
			peers: []parityPeer{
				{id: relayClose, age: time.Hour, reported: schedLocalVersion, usable: true},
				{id: relayFar, age: time.Minute, reported: schedLocalVersion, usable: true},
			},
			routes: []parityRoute{{relayClose, 1, false}, {relayFar, 2, false}},
		},
		{
			name: "direct session promoted above a relay",
			dst:  dst,
			peers: []parityPeer{
				{id: dst, age: time.Hour, reported: schedLocalVersion, usable: true},
				{id: relayFar, age: time.Minute, reported: schedLocalVersion, usable: true},
			},
			routes: []parityRoute{{relayFar, 2, false}},
		},
		{
			name: "direct session deduplicated against the table's own direct entry",
			dst:  dst,
			peers: []parityPeer{
				{id: dst, age: time.Hour, reported: schedLocalVersion, usable: true},
				{id: relayFar, age: time.Minute, reported: schedLocalVersion, usable: true},
			},
			routes: []parityRoute{{dst, 1, false}, {relayFar, 2, false}},
		},
		{
			name:   "no route at all",
			dst:    dst,
			peers:  nil,
			routes: nil,
		},
		{
			name: "withdrawn and stalled entries dropped",
			dst:  dst,
			peers: []parityPeer{
				{id: relayClose, age: time.Hour, reported: schedLocalVersion, usable: true},
				{id: relayFar, age: time.Minute, reported: schedLocalVersion, usable: true},
				{id: stalled, age: time.Hour, reported: schedLocalVersion, usable: false},
			},
			routes: []parityRoute{{relayClose, 3, true}, {stalled, 1, false}, {relayFar, 2, false}},
		},
		{
			name: "inflated version does not win the primary key",
			dst:  dst,
			peers: []parityPeer{
				{id: relayClose, age: time.Hour, reported: schedLocalVersion, usable: true},
				{id: relayFar, age: time.Minute, reported: 999, usable: true},
			},
			routes: []parityRoute{{relayClose, 2, false}, {relayFar, 1, false}},
		},
	}

	for _, topology := range topologies {
		t.Run(topology.name, func(t *testing.T) {
			// One clock for both routers: the file router reads
			// time.Now() itself, so the topology's timestamps have to be
			// anchored to the same instant or ConnectedAt would differ by
			// the microseconds between two fixture builds.
			now := time.Now().UTC()
			want := fileRouterPlan(t, topology, now)
			got := datagramPlan(t, topology, now)
			if len(got) != len(want) {
				t.Fatalf("plan length = %d, want %d\n got: %+v\nwant: %+v", len(got), len(want), got, want)
			}
			for i := range want {
				if got[i].NextHop != want[i].NextHop ||
					got[i].Hops != want[i].Hops ||
					got[i].ProtocolVersion != want[i].ProtocolVersion ||
					!got[i].ConnectedAt.Equal(want[i].ConnectedAt) {
					t.Fatalf("entry %d = %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// fileRouterPlan renders the topology through today's implementation.
func fileRouterPlan(t *testing.T, topology parityTopology, now time.Time) []filerouter.RoutePlanEntry {
	t.Helper()
	meta := make(map[domain.PeerIdentity]filerouter.PeerRouteMeta, len(topology.peers))
	for _, peer := range topology.peers {
		if !peer.usable {
			continue
		}
		// The node layer caps the ranking key and keeps the raw value —
		// the behaviour the datagram layer now performs itself.
		ranking := peer.reported
		if ranking > schedLocalVersion {
			ranking = schedLocalVersion
		}
		meta[peer.id] = filerouter.PeerRouteMeta{
			ConnectedAt:        now.Add(-peer.age),
			ProtocolVersion:    ranking,
			RawProtocolVersion: peer.reported,
		}
	}
	entries := make([]routing.RouteEntry, 0, len(topology.routes))
	for _, route := range topology.routes {
		hops := route.hops
		if route.withdrawn {
			hops = routing.HopsInfinity
		}
		entries = append(entries, routing.RouteEntry{
			Identity:  topology.dst,
			NextHop:   route.nextHop,
			Hops:      hops,
			ExpiresAt: now.Add(time.Hour),
		})
	}
	router := filerouter.NewRouter(filerouter.RouterConfig{
		NonceCache: &parityNonceCache{seen: make(map[string]struct{})},
		LocalID:    domaintest.ID("local-node"),
		IsFullNode: func() bool { return true },
		RouteSnap: func() routing.Snapshot {
			return routing.Snapshot{TakenAt: now, Routes: map[domain.PeerIdentity][]routing.RouteEntry{topology.dst: entries}}
		},
		RouteLookup: func(peer domain.PeerIdentity) []routing.RouteEntry {
			if peer != topology.dst {
				return nil
			}
			return entries
		},
		PeerRouteMeta: func(peer domain.PeerIdentity) (filerouter.PeerRouteMeta, bool) {
			found, ok := meta[peer]
			return found, ok
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})
	return router.ExplainRoute(topology.dst)
}

// datagramPlan renders the same topology through the datagram scheduler.
func datagramPlan(t *testing.T, topology parityTopology, now time.Time) []RoutePlanEntry {
	t.Helper()
	fixture := newSchedFixture(t, schedFixtureOpts{})
	fixture.setNow(now)
	for _, peer := range topology.peers {
		if !peer.usable {
			fixture.peers.stall(peer.id)
			continue
		}
		conn := PeerConnection{
			ConnectedAt:             fixture.clock().Add(-peer.age),
			Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
			DTypes:                  declaredDTypesOf([]string{parityDType.String()}),
			ReportedProtocolVersion: peer.reported,
		}
		fixture.peers.set(peer.id, conn)
		if peer.id == topology.dst {
			fixture.direct.set(peer.id, conn)
		}
	}
	hints := make([]RouteHint, 0, len(topology.routes))
	for _, route := range topology.routes {
		hints = append(hints, RouteHint{
			NextHop:   route.nextHop,
			Hops:      route.hops,
			Withdrawn: route.withdrawn,
			ExpiresAt: fixture.clock().Add(time.Hour),
		})
	}
	fixture.routes.set(topology.dst, hints...)

	// Every peer DECLARES the dtype under test, which keeps the last-hop gate
	// out of the comparison: the file router has no such gate, and the parity
	// claim is about the candidate ORDER, not about the new refusal.
	return mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: topology.dst, DType: parityDType},
		RoutePolicy:           domain.RoutePolicyBest,
	})).Entries()
}

// ---------------------------------------------------------------------------
// The probe
// ---------------------------------------------------------------------------

// TestProbeAgreesWithTheSendOverAnUnchangedTable is the one-way guarantee:
// with the table unchanged between the two calls, "reachable" followed by
// an immediate send yields neither no_route nor rejected, and "unreachable"
// means the send would not have been queued.
func TestProbeAgreesWithTheSendOverAnUnchangedTable(t *testing.T) {
	t.Parallel()

	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	scenarios := map[string]struct {
		setup func(*schedFixture)
		dtype domain.DType
	}{
		"direct session, declared type": {
			setup: func(f *schedFixture) {
				f.direct.set(dst, f.datagramPeer(dst, time.Hour, schedDType.String()))
			},
			dtype: schedDType,
		},
		"relay only": {
			setup: func(f *schedFixture) {
				f.datagramPeer(relay, time.Hour)
				f.routes.set(dst, f.route(relay, 2))
			},
			dtype: schedDType,
		},
		"no route": {
			setup: func(*schedFixture) {},
			dtype: schedDType,
		},
		"destination did not declare the type": {
			setup: func(f *schedFixture) {
				f.direct.set(dst, f.datagramPeer(dst, time.Hour))
			},
			dtype: schedDType,
		},
		"only next hop is stalled": {
			setup: func(f *schedFixture) {
				f.datagramPeer(relay, time.Hour)
				f.peers.stall(relay)
				f.routes.set(dst, f.route(relay, 2))
			},
			dtype: schedDType,
		},
		"only next hop will not relay": {
			setup: func(f *schedFixture) {
				f.peers.set(relay, PeerConnection{
					ConnectedAt:             f.clock().Add(-time.Hour),
					Advertised:              advertising(CapabilityDatagramV1),
					ReportedProtocolVersion: schedLocalVersion,
				})
				f.routes.set(dst, f.route(relay, 2))
			},
			dtype: schedDType,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			fixture := newSchedFixture(t, schedFixtureOpts{})
			scenario.setup(fixture)

			probe := mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{
				Dst: dst, DType: scenario.dtype,
			}))
			outcome := fixture.send(t, dst, withDType(scenario.dtype))
			queued := outcome.Kind() == SendQueued
			if probe.Reachable() != queued {
				t.Fatalf("probe said %s, send answered %s", probe, outcome)
			}
			if !probe.Reachable() && outcome.Kind() != SendNoRoute && outcome.Kind() != SendRejected {
				t.Fatalf("unreachable must mean no_route or rejected, got %s", outcome)
			}
			// The probe names the SAME refusal the send would: they read one
			// verdict, so a caller acting on the reason (§6.1) acts on what
			// the send would really have answered.
			probeReason, probeRefused := probe.Rejection()
			sendReason, sendRefused := outcome.Rejection()
			if probeRefused != sendRefused || probeReason != sendReason {
				t.Fatalf("probe reason %v/%v, send reason %v/%v",
					probeReason, probeRefused, sendReason, sendRefused)
			}
		})
	}
}

// TestProbeReservesNothingAndSpendsNoBudget pins the §4.3 boundary: the probe
// is read-only. It occupies no anti-replay slot and hands nothing to a writer.
func TestProbeReservesNothingAndSpendsNoBudget(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 1))

	if !mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("expected the destination to be reachable")
	}
	mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	if fixture.replay.Len() != 0 {
		t.Fatalf("the probe and the plan occupied %d anti-replay records", fixture.replay.Len())
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("the probe and the plan enqueued frames: %v", tried)
	}
}

// TestProbeReadsTheFreshLookup pins the freshness half: a user action taken
// right after a route appears must not be answered with "unreachable" while
// the send would already work. The cached snapshot is deliberately empty.
func TestProbeReadsTheFreshLookup(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.setFresh(dst, fixture.route(relay, 1))

	if !mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("the probe must read the fresh lookup, like a locally originated send")
	}
	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendQueued {
		t.Fatalf("the send disagreed with the probe: %s", outcome)
	}
}

// TestProbeDoesNotTakeAvoidNextHop pins the lawful pair of §4.3: the probe
// answers about sends WITHOUT an exclusion, so "reachable" together with
// no_route from a send-with-avoid is not a contradiction.
func TestProbeDoesNotTakeAvoidNextHop(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	only := domaintest.ID("relay-only")
	fixture.datagramPeer(only, time.Hour)
	fixture.routes.set(dst, fixture.route(only, 1))

	if !mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("expected reachable")
	}
	outcome := fixture.sendAvoiding(t, dst, AvoidNextHop(only))
	if outcome.Kind() != SendNoRoute {
		t.Fatalf("send-with-avoid outcome = %s, want no_route", outcome)
	}
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

// TestPlanForBestMatchesTheSendOrder pins that the plan and the send build
// ONE list from ONE source: with every hop refusing the enqueue, the order
// the send walked must equal the plan, element for element.
func TestPlanForBestMatchesTheSendOrder(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relays := []domain.PeerIdentity{
		domaintest.ID("relay-a"), domaintest.ID("relay-b"), domaintest.ID("relay-c"),
	}
	hints := make([]RouteHint, 0, len(relays))
	for i, relay := range relays {
		fixture.datagramPeer(relay, time.Duration(i+1)*time.Hour)
		hints = append(hints, fixture.route(relay, i+1))
		fixture.sender.refuseHop(relay)
	}
	fixture.direct.set(dst, fixture.datagramPeer(dst, 10*time.Hour, schedDType.String()))
	fixture.sender.refuseHop(dst)
	fixture.routes.set(dst, hints...)

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
		RoutePolicy:           domain.RoutePolicyBest,
	}))
	if plan.Entries()[0].NextHop != dst {
		t.Fatalf("the gated direct session must head the plan, got %s", plan.Entries()[0].NextHop)
	}
	fixture.send(t, dst)
	requireHops(t, fixture.sender.tried(), planNextHops(plan)...)
}

// TestPlanForExploreShowsComparatorOrderAndLeavesTheCounterAlone pins both
// halves of the read-only contract: the plan renders the comparator order
// rather than guessing the rotation, and it does not move the counter — a
// send after any number of plan calls behaves as if none had happened.
func TestPlanForExploreShowsComparatorOrderAndLeavesTheCounterAlone(t *testing.T) {
	t.Parallel()

	relays := []domain.PeerIdentity{
		domaintest.ID("relay-a"), domaintest.ID("relay-b"), domaintest.ID("relay-c"),
	}
	planned, dst := exploreFixture(t, schedFixtureOpts{}, relays...)
	untouched, _ := exploreFixture(t, schedFixtureOpts{}, relays...)

	query := RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
		RoutePolicy:           domain.RoutePolicyExplore,
	}
	best := mustExplainRoute(t, planned.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: query.ReachabilityQueryOpts,
		RoutePolicy:           domain.RoutePolicyBest,
	}))
	for i := 0; i < 5; i++ {
		explore := mustExplainRoute(t, planned.scheduler, mustRoutePlanQuery(t, query))
		requireHops(t, planNextHops(explore), planNextHops(best)...)
	}

	if hop, other := exploreSend(t, planned, dst), exploreSend(t, untouched, dst); hop != other {
		t.Fatalf("plan calls moved the rotation counter: %s vs %s", hop, other)
	}
}

// TestPlanIsEmptyWhenTheLastHopGateRefuses keeps the plan aligned with the
// probe: a destination that cannot receive this type has no plan to show.
func TestPlanIsEmptyWhenTheLastHopGateRefuses(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")
	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Hour))
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 2))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	if plan.Len() != 0 {
		t.Fatalf("plan = %+v, want empty for a type the destination cannot handle", plan.Entries())
	}
	if mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("the probe must agree with the plan")
	}
}

// TestPlanEntriesAreACopy keeps a diagnostic caller from editing the
// scheduler's view of the network.
func TestPlanEntriesAreACopy(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 1))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	entries := plan.Entries()
	entries[0].NextHop = domaintest.ID("tampered")
	if plan.Entries()[0].NextHop != relay {
		t.Fatal("Entries must hand out a copy")
	}
}

// The plan ACCEPTS route_policy, so it has to say what the policy changes
// about it: §4.3 promises that the first element is the send's first
// candidate only for `best`. A silently ignored input is a diagnostic that
// looks authoritative and is not.
func TestPlanReportsWhetherItsFirstElementIsGuaranteed(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	for i, hop := range []domain.PeerIdentity{domaintest.ID("relay-a"), domaintest.ID("relay-b")} {
		fixture.datagramPeer(hop, time.Duration(i+1)*time.Hour)
		fixture.routes.set(dst, fixture.route(hop, i+1))
	}
	fixture.routes.set(dst,
		fixture.route(domaintest.ID("relay-a"), 1),
		fixture.route(domaintest.ID("relay-b"), 2),
	)

	query := RoutePlanQueryOpts{ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType}}

	query.RoutePolicy = domain.RoutePolicyBest
	best := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, query))
	if best.Policy() != domain.RoutePolicyBest {
		t.Fatalf("policy = %q, want best", best.Policy())
	}
	if !best.FirstCandidateGuaranteed() {
		t.Fatal("best promises that element 0 is the send's first candidate")
	}

	query.RoutePolicy = domain.RoutePolicyExplore
	explore := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, query))
	if explore.FirstCandidateGuaranteed() {
		t.Fatal("explore shows the comparator order, not the future rotation")
	}
	if explore.Len() != best.Len() {
		t.Fatalf("plan lengths differ (%d vs %d): the policy must not change the SET", explore.Len(), best.Len())
	}
}

// ---------------------------------------------------------------------------
// The two negatives (§6.1)
// ---------------------------------------------------------------------------

// TestProbeSeparatesTheGateRefusalFromTheMissingRoute is the ШЕРОХОВАТОСТЬ-4
// regression.
//
// §6.1 says a NEGATIVE LIVE ANSWER cancels a positive cached confirmation and
// clears it immediately. That rule is about SUPPORT: the peer told us in its
// handshake that it has no handler for this dtype. A destination that is
// simply off the routing table this second says nothing about support, and
// invalidating on that would wipe a good confirmation on every route flap.
//
// While the probe answered a bare bool the two were indistinguishable, so a
// caller could only either implement the rule wrong or not at all — which is
// why the migration prototype grew a SECOND copy of the node's session seam
// just to ask "does this peer declare this dtype".
func TestProbeSeparatesTheGateRefusalFromTheMissingRoute(t *testing.T) {
	t.Parallel()

	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	tests := map[string]struct {
		setup      func(*schedFixture)
		wantReason RejectionReason
		wantGate   bool
		unsupport  bool
		wantMissed domain.CapabilityName
	}{
		"the destination declared no handler for the type": {
			setup: func(f *schedFixture) {
				// A LIVE direct session whose declared dtype set omits the
				// type: the last-hop gate refuses, and this is the one
				// negative §6.1 acts on.
				f.direct.set(dst, f.datagramPeer(dst, time.Hour))
				f.datagramPeer(relay, time.Hour)
				f.routes.set(dst, f.route(relay, 2))
			},
			wantReason: RejectionUnsupportedDType,
			wantGate:   true,
			unsupport:  true,
		},
		"the only candidate will not relay": {
			setup: func(f *schedFixture) {
				f.peers.set(relay, PeerConnection{
					ConnectedAt:             f.clock().Add(-time.Hour),
					Advertised:              advertising(CapabilityDatagramV1),
					ReportedProtocolVersion: schedLocalVersion,
				})
				f.routes.set(dst, f.route(relay, 2))
			},
			wantReason: RejectionMissingCapability,
			wantGate:   true,
			wantMissed: CapabilityDatagramTransitV1,
		},
		"there is no route at all": {
			setup:    func(f *schedFixture) {},
			wantGate: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newSchedFixture(t, schedFixtureOpts{})
			test.setup(fixture)

			query := ReachabilityQueryOpts{Dst: dst, DType: schedDType}
			probe := mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, query))
			if probe.Reachable() {
				t.Fatalf("every case here must be unreachable, got %s", probe)
			}
			reason, refused := probe.Rejection()
			if refused != test.wantGate {
				t.Fatalf("gate refusal = %v, want %v (probe: %s)", refused, test.wantGate, probe)
			}
			if refused && reason != test.wantReason {
				t.Fatalf("reason = %s, want %s", reason, test.wantReason)
			}
			if got := probe.UnsupportedDType(); got != test.unsupport {
				t.Fatalf("UnsupportedDType = %v, want %v — §6.1 acts on exactly this", got, test.unsupport)
			}
			missing, named := probe.MissingCapability()
			if named != (test.wantMissed != "") || (named && missing != test.wantMissed) {
				t.Fatalf("missing capability = %q/%v, want %q", missing, named, test.wantMissed)
			}

			// The plan reports the same distinction, so a diagnostic and a
			// retry engine never disagree about why there is no path.
			plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
				ReachabilityQueryOpts: query,
			}))
			if plan.Len() != 0 {
				t.Fatalf("plan = %+v, want empty", plan.Entries())
			}
			planReason, planRefused := plan.GateRefusal()
			if planRefused != test.wantGate || (planRefused && planReason != test.wantReason) {
				t.Fatalf("plan refusal = %s/%v, want %s/%v",
					planReason, planRefused, test.wantReason, test.wantGate)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Validated read-only queries
// ---------------------------------------------------------------------------

// mustReachabilityQuery builds a VALIDATED probe query or fails the test. The
// surfaces refuse anything the constructor did not stamp, so every test asks
// the same way a service adapter has to.
func mustReachabilityQuery(t *testing.T, opts ReachabilityQueryOpts) ReachabilityQuery {
	t.Helper()
	query, err := NewReachabilityQuery(opts)
	if err != nil {
		t.Fatalf("NewReachabilityQuery(%+v): %v", opts, err)
	}
	return query
}

// mustRoutePlanQuery is the same for the plan. A test that does not care about
// the policy asks for `best`, which is the only one that promises anything
// about element 0 (§4.3).
func mustRoutePlanQuery(t *testing.T, opts RoutePlanQueryOpts) RoutePlanQuery {
	t.Helper()
	if opts.RoutePolicy == "" {
		opts.RoutePolicy = domain.RoutePolicyBest
	}
	query, err := NewRoutePlanQuery(opts)
	if err != nil {
		t.Fatalf("NewRoutePlanQuery(%+v): %v", opts, err)
	}
	return query
}

// mustReachable runs the probe and fails on a malformed query, so a test
// asserting about routes never silently asserts about its own typo.
func mustReachable(t *testing.T, s *Scheduler, query ReachabilityQuery) ReachabilityResult {
	t.Helper()
	result, err := s.Reachable(context.Background(), query)
	if err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	return result
}

// mustExplainRoute is the same for the plan.
func mustExplainRoute(t *testing.T, s *Scheduler, query RoutePlanQuery) RoutePlan {
	t.Helper()
	plan, err := s.ExplainRoute(context.Background(), query)
	if err != nil {
		t.Fatalf("ExplainRoute: %v", err)
	}
	return plan
}
