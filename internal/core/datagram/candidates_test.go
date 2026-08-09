package datagram

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// candidates_test.go covers the candidate half of §4.3: the capability
// filter before the sort, the exclusions, the dedup that shares the
// comparator with the sort, the sort itself, wall-clock expiry and the
// version normalization.

func planNextHops(plan RoutePlan) []domain.PeerIdentity {
	hops := make([]domain.PeerIdentity, 0, plan.Len())
	for _, entry := range plan.Entries() {
		hops = append(hops, entry.NextHop)
	}
	return hops
}

func requireHops(t *testing.T, got []domain.PeerIdentity, want ...domain.PeerIdentity) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("next hops = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("next hops = %v, want %v", got, want)
		}
	}
}

// TestComparatorOrderIsVersionHopsUptimeIdentity pins the four keys of §4.3
// in priority order, each decided by a pair that ties on every earlier key.
func TestComparatorOrderIsVersionHopsUptimeIdentity(t *testing.T) {
	t.Parallel()

	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	newer := RouteCandidate{nextHop: domaintest.ID("a"), hops: 9, protocolVersion: 27, connectedAt: base}
	older := RouteCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 26, connectedAt: base}
	if !routeCandidateLess(newer, older) {
		t.Fatal("protocolVersion DESC must outrank hops")
	}

	near := RouteCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 27, connectedAt: base}
	far := RouteCandidate{nextHop: domaintest.ID("a"), hops: 2, protocolVersion: 27, connectedAt: base.Add(-time.Hour)}
	if !routeCandidateLess(near, far) {
		t.Fatal("hops ASC must outrank uptime")
	}

	long := RouteCandidate{nextHop: domaintest.ID("b"), hops: 1, protocolVersion: 27, connectedAt: base.Add(-time.Hour)}
	short := RouteCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 27, connectedAt: base}
	if !routeCandidateLess(long, short) {
		t.Fatal("older connectedAt must win the uptime key, ahead of the identity tie-break")
	}

	unknown := RouteCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 27}
	known := RouteCandidate{nextHop: domaintest.ID("z"), hops: 1, protocolVersion: 27, connectedAt: base}
	if !routeCandidateLess(known, unknown) {
		t.Fatal("a zero connectedAt means unknown and must sort last")
	}

	first := RouteCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 27, connectedAt: base}
	second := RouteCandidate{nextHop: domaintest.ID("b"), hops: 1, protocolVersion: 27, connectedAt: base}
	if !routeCandidateLess(first, second) {
		t.Fatal("identity is the final deterministic tie-break")
	}
}

// TestCapabilityFilterRunsBeforeRanking pins §6: the filter is applied
// BEFORE the sort and is not a penalty. The peer that would win every
// ranking key is the one without mesh_datagram_transit_v1, so a scoring
// implementation would still surface it — and this test would fail.
func TestCapabilityFilterRunsBeforeRanking(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	endpointOnly := domaintest.ID("relay-endpoint-only")
	transit := domaintest.ID("relay-transit")

	fixture.peers.set(endpointOnly, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-10 * time.Hour),
		Advertised:              advertising(CapabilityDatagramV1),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.datagramPeer(transit, time.Minute)
	fixture.routes.set(dst,
		fixture.route(endpointOnly, 1),
		fixture.route(transit, 5),
	)

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	requireHops(t, planNextHops(plan), transit)
}

// TestExtraAdvertisedNamesDoNotFilterCandidates pins the absence of the
// path-wide gate. A relay that advertises only the two role names carries the
// frame exactly as one that also advertises a name this build never heard of,
// because the
// envelope no longer lets a sender demand anything of the path — and the
// comparator, not a capability list, decides which of the two wins.
func TestExtraAdvertisedNamesDoNotFilterCandidates(t *testing.T) {
	t.Parallel()

	const unknownName = domain.CapabilityName("some_future_extension_v9")
	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	plain := domaintest.ID("relay-plain")
	capable := domaintest.ID("relay-capable")

	fixture.datagramPeer(plain, 10*time.Hour)
	fixture.peers.set(capable, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1, unknownName),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.routes.set(dst, fixture.route(plain, 1), fixture.route(capable, 4))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	// Both are admitted; the fewer-hops key puts the plain relay first.
	requireHops(t, planNextHops(plan), plain, capable)
}

// TestStalledNextHopIsNotACandidate pins the liveness half of the
// exclusions: a next hop with no connection the send could use is not a
// candidate, or the plan would promise hops nothing can be sent into.
func TestStalledNextHopIsNotACandidate(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	stalled := domaintest.ID("relay-stalled")
	healthy := domaintest.ID("relay-healthy")

	fixture.datagramPeer(stalled, 10*time.Hour)
	fixture.peers.stall(stalled)
	fixture.datagramPeer(healthy, time.Minute)
	fixture.routes.set(dst, fixture.route(stalled, 1), fixture.route(healthy, 6))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	requireHops(t, planNextHops(plan), healthy)
}

// TestSelfRoutesAndWithdrawnRoutesAreExcluded covers the remaining
// structural exclusions of §4.3 item 2.
func TestSelfRoutesAndWithdrawnRoutesAreExcluded(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	withdrawn := domaintest.ID("relay-withdrawn")
	alive := domaintest.ID("relay-alive")

	fixture.datagramPeer(fixture.local, time.Hour)
	fixture.datagramPeer(withdrawn, time.Hour)
	fixture.datagramPeer(alive, time.Hour)
	fixture.routes.set(dst,
		fixture.route(fixture.local, 1),
		RouteHint{NextHop: withdrawn, Hops: 1, Withdrawn: true, ExpiresAt: fixture.clock().Add(time.Hour)},
		fixture.route(alive, 3),
	)

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	requireHops(t, planNextHops(plan), alive)
}

// TestWallClockExpiryAfterSnapshotPublish is the §4.3 rule that the cached
// snapshot cannot express: the snapshot republishes on a dirty flag, so a
// finite-TTL route that aged out between publishes still looks alive in it.
// Expiry is judged by the clock AT SELECTION TIME.
func TestWallClockExpiryAfterSnapshotPublish(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	aging := domaintest.ID("relay-aging")
	fixture.datagramPeer(aging, time.Hour)
	fixture.routes.set(dst, RouteHint{
		NextHop:   aging,
		Hops:      1,
		ExpiresAt: fixture.clock().Add(30 * time.Second),
	})

	if outcome := fixture.send(t, dst); outcome.Kind() != SendQueued {
		t.Fatalf("before expiry: outcome = %s, want queued", outcome)
	}

	// The snapshot is untouched — exactly the situation the rule is about.
	fixture.advance(31 * time.Second)
	fixture.sender.reset()
	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendNoRoute {
		t.Fatalf("after wall-clock expiry: outcome = %s, want no_route", outcome)
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("a route dead by the wall clock must not be attempted, tried %v", tried)
	}
}

// TestDedupUsesTheSortComparator pins §4.3 item 3. Two routing entries
// point at the same next hop with different hop counts; the surviving one
// must be the better one BY THE COMPARATOR, and the dedup must not change
// the peer's ranking key relative to a second candidate.
func TestDedupUsesTheSortComparator(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	twoWay := domaintest.ID("relay-two-entries")
	rival := domaintest.ID("relay-rival")

	fixture.datagramPeer(twoWay, time.Hour)
	fixture.datagramPeer(rival, time.Hour)
	fixture.routes.set(dst,
		fixture.route(twoWay, 7),
		fixture.route(rival, 4),
		fixture.route(twoWay, 2), // the better entry arrives second
	)

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	entries := plan.Entries()
	if len(entries) != 2 {
		t.Fatalf("dedup must collapse the pair into one entry, got %d", len(entries))
	}
	if entries[0].NextHop != twoWay || entries[0].Hops != 2 {
		t.Fatalf("dedup kept the wrong entry: %+v", entries[0])
	}
	if entries[1].NextHop != rival {
		t.Fatalf("dedup and sort disagreed about the order: %+v", entries)
	}
}

// TestVersionNormalizationCapsInsteadOfZeroing pins both halves of §4.3:
// a peer claiming v999 must not WIN the primary key over a legitimate peer
// at the local version, and a peer one version ahead of this build must not
// be pushed to the back — that earlier "clamp to zero" behaviour starved
// staged rollouts.
func TestVersionNormalizationCapsInsteadOfZeroing(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	inflated := domaintest.ID("relay-inflated")
	legit := domaintest.ID("relay-legit")
	ahead := domaintest.ID("relay-one-ahead")

	// The inflated peer is closer AND newer on the wire; only the cap
	// keeps it from taking the head of the plan.
	fixture.peers.set(inflated, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: 999,
	})
	fixture.peers.set(legit, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.peers.set(ahead, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-2 * time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion + 1,
	})
	fixture.routes.set(dst,
		fixture.route(inflated, 1),
		fixture.route(legit, 1),
		fixture.route(ahead, 1),
	)

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	entries := plan.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected all three peers in the plan, got %d", len(entries))
	}
	for i, entry := range entries {
		if entry.ProtocolVersion > schedLocalVersion {
			t.Fatalf("entry %d exposes an uncapped ranking key %d", i, entry.ProtocolVersion)
		}
	}
	// All three collapse to the same version tier, so uptime decides:
	// the peer one version ahead is the OLDEST connection and therefore
	// first — it is not exiled to the back, and the liar does not win.
	requireHops(t, planNextHops(plan), ahead, legit, inflated)
}

// TestCandidateKeysComeFromTheConnectionTheSendWillUse is the regression
// the file router already paid for: ranking promised an inbound path of a
// newer version while the bytes left over an outbound session of an older
// one. PeerMetadata answers with ONE connection — the one the send picks —
// so an aggregating implementation would rank relayA first and fail here.
func TestCandidateKeysComeFromTheConnectionTheSendWillUse(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relayA := domaintest.ID("relay-a-split-versions")
	relayB := domaintest.ID("relay-b-single")

	// relayA also holds an inbound session at schedLocalVersion. It is
	// deliberately NOT registered: the contract says the resolver answers
	// with the connection the send would try first (outbound), and this
	// fixture models that helper.
	fixture.peers.set(relayA, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion - 3, // outbound, older
	})
	fixture.peers.set(relayB, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion - 1,
	})
	fixture.routes.set(dst, fixture.route(relayA, 1), fixture.route(relayB, 9))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
	}))
	entries := plan.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].NextHop != relayB {
		t.Fatalf("ranking must follow the outbound socket: head = %s", entries[0].NextHop)
	}
	if entries[1].ProtocolVersion != schedLocalVersion-3 {
		t.Fatalf("relayA must rank by its outbound version, got %d", entries[1].ProtocolVersion)
	}

	// And the send goes where the plan said it would.
	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != relayB {
		t.Fatalf("send disagreed with the plan: %s", outcome)
	}
}

// TestRawVersionKeptForDiagnostics pins that the cap is a ranking
// projection, not a rewrite of what the peer said: the raw value stays
// available for the audit log.
func TestRawVersionKeptForDiagnostics(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	inflated := domaintest.ID("relay-inflated")
	fixture.peers.set(inflated, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: 999,
	})

	selector := candidateSelector{
		clock:    fixture.clock,
		peers:    fixture.peers,
		localID:  fixture.local,
		versions: versionNormalizer{local: schedLocalVersion},
	}
	candidates, _ := selector.rank(
		context.Background(),
		protocol.DatagramFrame{Dst: dst, DType: schedDType},
		[]RouteHint{fixture.route(inflated, 1)},
		selectionOpts{incomingPeer: LocalIngress()},
	)
	if len(candidates) != 1 {
		t.Fatalf("expected one candidate, got %d", len(candidates))
	}
	if candidates[0].ProtocolVersion() != schedLocalVersion {
		t.Fatalf("ranking key = %d, want the local cap", candidates[0].ProtocolVersion())
	}
	if candidates[0].RawProtocolVersion() != 999 {
		t.Fatalf("raw version = %d, want the reported 999", candidates[0].RawProtocolVersion())
	}
}

// §4.3 item 2 excludes "next_hop == dst ALREADY TRIED by the direct session",
// not every route through the destination. While DirectSession and
// PeerMetadata answer from one helper the two readings agree; the moment they
// disagree, the unconditional form makes the destination unreachable by BOTH
// paths — the direct branch has no session and the routing branch throws its
// route away.
func TestRouteThroughTheDestinationSurvivesWhenTheDirectBranchDidNot(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")

	// The routing table knows a one-hop route through dst itself and the peer
	// table can send into it — but the direct-session source does not see it.
	fixture.datagramPeer(dst, time.Hour, schedDType.String())
	fixture.routes.set(dst, fixture.route(dst, 1))

	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendQueued {
		t.Fatalf("outcome = %s, want the frame queued to the destination", outcome)
	}
	hop, _ := outcome.NextHop()
	if hop != dst {
		t.Fatalf("next hop = %s, want the destination itself", hop)
	}
	if !mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("the probe must agree with the send")
	}
}

// The other half: when the direct branch DID try the destination, the routing
// entry through it must not produce a second candidate — one place decides
// whether the destination may receive the frame.
func TestRouteThroughTheDestinationIsDroppedOnceTheDirectBranchTriedIt(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")

	conn := fixture.datagramPeer(dst, time.Hour, schedDType.String())
	fixture.direct.set(dst, conn)
	fixture.routes.set(dst, fixture.route(dst, 1))

	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
		ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
		RoutePolicy:           domain.RoutePolicyBest,
	}))
	if plan.Len() != 1 {
		t.Fatalf("plan = %+v, want the destination exactly once", plan.Entries())
	}
}

// A destination the direct branch never saw still faces the last-hop dtype
// gate on the routing path: the gate belongs to the hop, not to the branch.
func TestRouteThroughTheDestinationStillFacesTheLastHopGate(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")

	// Connected and capable, but it never declared this dtype.
	fixture.datagramPeer(dst, time.Hour)
	fixture.routes.set(dst, fixture.route(dst, 1))

	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}
	if reason, _ := outcome.Rejection(); reason != RejectionUnsupportedDType {
		t.Fatalf("reason = %s, want unsupported_dtype", reason)
	}
}

// TestPeerSurvivesWhenOnlyItsHeadConnectionFailsTheGates is the other side of
// the §4.3 line 574 rule: "the metadata describes the connection the send will
// try" has to hold in BOTH directions.
//
// What it caught: PeerMetadata.SendableConnection answered head-of-list without
// knowing the frame, so a peer whose FIRST connection failed a gate — no
// transit capability, no declared dtype — was discarded whole, although its
// second connection passed every gate and the emitter was ready to hand the
// frame to it. A working route became unreachable, and the probe and the plan
// agreed with the loss because all three read the same frame-blind answer.
//
// The mutation this kills lives in the fake: make firstSendableConnection
// return conns[0] regardless of the frame, and every assertion below turns red
// at once — which is exactly the property under test, because the layer can
// only ask for the right connection if it passes the frame at all.
func TestPeerSurvivesWhenOnlyItsHeadConnectionFailsTheGates(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay-two-sockets")

	// The head socket is endpoint-only: it may receive what is addressed to it
	// and must not be asked to forward somebody else's frame (§6).
	incapable := PeerConnection{
		ConnectedAt:             fixture.clock().Add(-10 * time.Hour),
		Advertised:              advertising(CapabilityDatagramV1),
		ReportedProtocolVersion: schedLocalVersion,
	}
	capable := PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion,
	}
	fixture.peers.setAll(relay, incapable, capable)
	fixture.routes.set(dst, fixture.route(relay, 2))

	query := ReachabilityQueryOpts{Dst: dst, DType: schedDType}

	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != relay {
		t.Fatalf("outcome = %s (%v), want queued(%s)", outcome, outcome.Err(), relay)
	}
	requireHops(t, fixture.sender.tried(), relay)

	// The candidate must be ranked by the connection the send really used —
	// otherwise the plan would publish the incapable socket's uptime and
	// version, which is the aggregate §4.3 forbids.
	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{ReachabilityQueryOpts: query}))
	requireHops(t, planNextHops(plan), relay)
	if got := plan.Entries()[0].ConnectedAt; !got.Equal(capable.ConnectedAt) {
		t.Fatalf("plan ranked by connectedAt %v, want the admissible connection's %v", got, capable.ConnectedAt)
	}

	if result := mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, query)); !result.Reachable() {
		t.Fatalf("probe = %s, want reachable: it must agree with the send it exists to predict", result)
	}
}

// TestDirectDestinationSurvivesWhenOnlyItsHeadConnectionFailsTheDTypeGate is
// the same defect on step 1 of §4.3, where it is worse: the direct branch is
// tried FIRST, and losing it demotes a neighbour to the routing table — or,
// when the head failed the last-hop dtype gate, aborts the whole selection as
// a hard policy stop.
func TestDirectDestinationSurvivesWhenOnlyItsHeadConnectionFailsTheDTypeGate(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")

	noHandler := PeerConnection{
		ConnectedAt:             fixture.clock().Add(-10 * time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{dtypeQuery.String()}),
		ReportedProtocolVersion: schedLocalVersion,
	}
	handler := PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{schedDType.String()}),
		ReportedProtocolVersion: schedLocalVersion,
	}
	fixture.direct.setAll(dst, noHandler, handler)

	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != dst {
		t.Fatalf("outcome = %s (%v), want queued(%s)", outcome, outcome.Err(), dst)
	}
	requireHops(t, fixture.sender.tried(), dst)

	query := ReachabilityQueryOpts{Dst: dst, DType: schedDType}
	if result := mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, query)); !result.Reachable() {
		t.Fatalf("probe = %s, want reachable", result)
	}
	plan := mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{ReachabilityQueryOpts: query}))
	requireHops(t, planNextHops(plan), dst)
}
