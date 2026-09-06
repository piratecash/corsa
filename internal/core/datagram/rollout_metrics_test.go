package datagram

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// Admission-refusal counters (docs/refactoring/dht/05-rollout-metrics.md).
//
// Two properties, and the second is the one that is easy to lose: a refusal is
// counted where a SEND was refused, and NOT where a read-only projection
// reached the same verdict. The plan and the reachability probe exist to
// predict refusals; counting them would inflate the metric with sends that
// never happened, and the inflation would look exactly like a worsening
// network.

// TestRefusedSendIsCountedByWhichGateSpoke pins that the last-hop dtype gate
// lands in its own slot rather than in a generic "refused" total.
func TestRefusedSendIsCountedByWhichGateSpoke(t *testing.T) {
	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	// The destination has a live session and speaks datagrams, but does not
	// implement this dtype: the last-hop gate refuses, and relaying around it
	// would only move the silent drop to the destination.
	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{"some_other_type"}),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 2))

	if outcome := fixture.send(t, dst); outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}

	refusals := fixture.metrics.Snapshot().SendRefusals
	if got := refusals["unsupported_dtype"]; got != 1 {
		t.Fatalf("unsupported_dtype = %d, want 1 (all refusals: %v)", got, refusals)
	}
	if len(refusals) != 1 {
		t.Fatalf("one refusal must move exactly one counter, moved %v", refusals)
	}
}

// TestMissingEndpointCapabilityIsCountedApartFromMissingTransit pins the split
// that makes the metric actionable: "the peer does not speak datagrams at all"
// and "the peer speaks them but will not carry other people's" are different
// upgrades, and one counter for both would say neither.
func TestMissingEndpointCapabilityIsCountedApartFromMissingTransit(t *testing.T) {
	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	// The only route is through a relay that never advertised the transit
	// capability, so the role gate refuses it as a transit candidate.
	fixture.peers.set(relay, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1),
		DTypes:                  declaredDTypesOf(nil),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.routes.set(dst, fixture.route(relay, 2))

	if outcome := fixture.send(t, dst); outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}

	refusals := fixture.metrics.Snapshot().SendRefusals
	if got := refusals["missing_transit_capability"]; got != 1 {
		t.Fatalf("missing_transit_capability = %d, want 1 (all refusals: %v)", got, refusals)
	}
	if got := refusals["missing_endpoint_capability"]; got != 0 {
		t.Fatalf("missing_endpoint_capability = %d, want 0 — the peer does speak datagrams", got)
	}
}

// TestTheRouteExplanationDoesNotMoveRefusalCounters pins the boundary between
// a send and a projection.
//
// ExplainRoute answers the same question the send asks and reaches the same
// verdict on purpose. If it moved the counters, an operator opening a
// diagnostic would raise the very number they came to read — and the metric
// would measure how often somebody looked.
func TestTheRouteExplanationDoesNotMoveRefusalCounters(t *testing.T) {
	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	fixture.peers.set(relay, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1),
		DTypes:                  declaredDTypesOf(nil),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.routes.set(dst, fixture.route(relay, 2))

	// One REAL send first, so the counter is non-zero and the assertion below
	// is about a number that exists. Asserting "still empty" would pass just as
	// well on a build where nothing is ever counted.
	if outcome := fixture.send(t, dst); outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}
	before := fixture.metrics.Snapshot().SendRefusals
	if before["missing_transit_capability"] != 1 {
		t.Fatalf("setup did not produce a counted refusal: %v", before)
	}

	for i := 0; i < 3; i++ {
		mustExplainRoute(t, fixture.scheduler, mustRoutePlanQuery(t, RoutePlanQueryOpts{
			ReachabilityQueryOpts: ReachabilityQueryOpts{Dst: dst, DType: schedDType},
		}))
	}
	after := fixture.metrics.Snapshot().SendRefusals

	if len(after) != len(before) {
		t.Fatalf("a read-only route plan moved refusal counters: before %v, after %v", before, after)
	}
	for name, count := range after {
		if before[name] != count {
			t.Fatalf("a read-only route plan moved %q: %d → %d", name, before[name], count)
		}
	}
}

// TestMetricsSnapshotBoundsItsPeriod pins that the datagram counters say WHEN
// they started and WHEN they were read.
//
// Without both, a pair of readings cannot be compared at all: the counters are
// cumulative and in-memory, so 100 before a restart and 120 after look like a
// difference of 20 while describing two unrelated runs. Comparing a counter to
// itself cannot detect that — the second run may have passed the first — which
// is why the START of the period is the field that matters here, not just the
// read time.
func TestMetricsSnapshotBoundsItsPeriod(t *testing.T) {
	started := time.Now().UTC().Add(-time.Hour)
	metrics := NewMetricsStartedAt(started)
	metrics.ObserveSendRefusal(RejectionMissingCapability, CapabilityDatagramTransitV1)

	snapshot := metrics.Snapshot()
	if snapshot.StartedAt == nil {
		t.Fatal("started_at missing: a cumulative counter without its period start cannot be compared across reads")
	}
	if !snapshot.StartedAt.Equal(started) {
		t.Fatalf("started_at = %v, want %v", *snapshot.StartedAt, started)
	}
	if snapshot.ReadAt == nil {
		t.Fatal("read_at missing: a rate has no denominator without it")
	}
	if snapshot.ReadAt.Before(started) {
		t.Fatalf("read_at %v precedes started_at %v", *snapshot.ReadAt, started)
	}

	// A NEW instance is a new period, and it says so. This is the restart the
	// counter values themselves cannot reveal.
	fresh := NewMetricsStartedAt(started.Add(30 * time.Minute))
	freshSnapshot := fresh.Snapshot()
	if freshSnapshot.StartedAt == nil || freshSnapshot.StartedAt.Equal(started) {
		t.Fatalf("a fresh counter set must report its own period start, got %v", freshSnapshot.StartedAt)
	}
	if freshSnapshot.SendRefusals["missing_transit_capability"] != 0 {
		t.Fatal("a fresh counter set must start empty — otherwise the period stamp describes nothing")
	}
}

// TestMetricsSnapshotReportsUnknownPeriodAsNull pins that a counter set built
// without a start stamp says "unknown" instead of the year 1: a dashboard
// subtracting 0001-01-01 reports two thousand years of uptime rather than an
// obvious gap.
func TestMetricsSnapshotReportsUnknownPeriodAsNull(t *testing.T) {
	snapshot := (&Metrics{}).Snapshot()
	if snapshot.StartedAt != nil {
		t.Fatalf("started_at = %v, want nil for a counter set with no period", *snapshot.StartedAt)
	}
	if snapshot.ReadAt == nil {
		t.Fatal("read_at must be stamped even when the period start is unknown")
	}
}
