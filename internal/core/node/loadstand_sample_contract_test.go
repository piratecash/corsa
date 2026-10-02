package node

import (
	"context"
	"errors"
	"maps"
	"math"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// TestLoadStandSample pins what a stand sample is and what may be computed
// from two of them. All but one subtest are synthetic: the rules are about
// pairs of numbers and are checked on numbers whose answer is known.
func TestLoadStandSample(t *testing.T) {
	// Serial parent: the one integration subtest builds real stand nodes,
	// and stand nodes refuse a CORSA_* environment.
	isolateFromCorsaEnvironment(t)

	parallel := map[string]func(*testing.T){
		"WindowSubtractsWithinOneIncarnation":       testLoadStandWindowSubtractsWithinOneIncarnation,
		"WindowRefusesAnotherIncarnation":           testLoadStandWindowRefusesAnotherIncarnation,
		"WindowRefusesAPairNotReadForward":          testLoadStandWindowRefusesAPairNotReadForward,
		"WindowRefusesAFamilyNotReadForward":        testLoadStandWindowRefusesAFamilyNotReadForward,
		"WindowRefusesAnotherNode":                  testLoadStandWindowRefusesAnotherNode,
		"WindowRefusesACounterReset":                testLoadStandWindowRefusesACounterReset,
		"WindowRefusesADatagramPlaneAtOneEdge":      testLoadStandWindowRefusesADatagramPlaneAtOneEdge,
		"WindowRefusesADecreasedCounter":            testLoadStandWindowRefusesADecreasedCounter,
		"WindowRefusesAnUnboundedDatagramPeriod":    testLoadStandWindowRefusesAnUnboundedDatagramPeriod,
		"WindowWithoutDatagramPlaneHasNoDelta":      testLoadStandWindowWithoutDatagramPlaneHasNoDelta,
		"SampleReadsEverythingThroughItsSource":     testLoadStandSampleReadsEverythingThroughItsSource,
		"SampleFailsWithAnyOfItsSources":            testLoadStandSampleFailsWithAnyOfItsSources,
		"ByteBalanceHoldsOnAClosedSystem":           testLoadStandByteBalanceHoldsOnAClosedSystem,
		"ByteBalanceToleranceIsExact":               testLoadStandByteBalanceToleranceIsExact,
		"ByteBalanceAtTheThresholdIsJudged":         testLoadStandByteBalanceAtTheThresholdIsJudged,
		"ByteBalanceBlindInASmallWindow":            testLoadStandByteBalanceBlindInASmallWindow,
		"ByteBalanceCatchesDoubleCountInAWideOne":   testLoadStandByteBalanceCatchesDoubleCountInAWideOne,
		"ByteBalanceOverALongSpanSeesAgain":         testLoadStandByteBalanceOverALongSpanSeesAgain,
		"ByteBalanceUndefinedWithoutEveryWindow":    testLoadStandByteBalanceUndefinedWithoutEveryWindow,
		"ByteBalanceSumsLivesAcrossAFlap":           testLoadStandByteBalanceSumsLivesAcrossAFlap,
		"ByteBalanceReadsTheWholeLifeHistory":       testLoadStandByteBalanceReadsTheWholeLifeHistory,
		"FinalReadingIsTakenAfterTheStop":           testLoadStandFinalReadingIsTakenAfterTheStop,
		"ByteBalanceAllowsForEveryStop":             testLoadStandByteBalanceAllowsForEveryStop,
		"ByteBalanceUndefinedOverEmptySweeps":       testLoadStandByteBalanceUndefinedOverEmptySweeps,
		"FloorAboveHeapIsADefect":                   testLoadStandFloorAboveHeapIsADefect,
		"BansAreADefect":                            testLoadStandBansAreADefect,
		"ImplausibleRSSIsADefect":                   testLoadStandImplausibleRSSIsADefect,
		"ImplausibleCPUOverAWindowIsADefect":        testLoadStandImplausibleCPUOverAWindowIsADefect,
		"AggregatesByRoleWithHubsApart":             testLoadStandAggregatesByRoleWithHubsApart,
		"AggregateRefusesAValueWithoutAGroup":       testLoadStandAggregateRefusesAValueWithoutAGroup,
		"ProcessSampleReadsTheRuntime":              testLoadStandProcessSampleReadsTheRuntime,
		"RuntimeMetricsRefuseWhatTheyCannotRead":    testLoadStandRuntimeMetricsRefuseWhatTheyCannotRead,
		"RuntimeMetricsFeedTheirFields":             testLoadStandRuntimeMetricsFeedTheirFields,
		"DescriptorCountRefusesAStaticListing":      testLoadStandDescriptorCountRefusesAStaticListing,
		"RedialIsASessionNotAmongTheCut":            testLoadStandRedialIsASessionNotAmongTheCut,
		"TwoRealNodesGiveAWindowUntilEdgeRestarted": testLoadStandTwoRealNodesGiveAWindowUntilEdgeRestarted,
	}
	for name, test := range parallel {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			test(t)
		})
	}
}

// loadStandEpoch anchors every synthetic instant; only differences matter.
var loadStandEpoch = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

func loadStandAt(offset time.Duration) time.Time { return loadStandEpoch.Add(offset) }

// syntheticLoadStandSample is a sample of node in incarnation life, read at
// offset into that life, whose every counter family starts with the
// incarnation and whose transport counters stand at traffic.
func syntheticLoadStandSample(node loadStandNodeID, life loadStandLife, offset time.Duration, traffic loadStandTrafficTotals) loadStandNodeSample {
	startedAt := loadStandAt(time.Duration(life) * time.Hour)
	readAt := startedAt.Add(offset)
	datagramStarted, datagramRead := startedAt, readAt
	return loadStandNodeSample{
		Node:        node,
		Role:        loadStandRoleFull,
		Incarnation: loadStandIncarnationID{Life: life, StartedAt: startedAt},
		ReadAt:      readAt,
		Traffic: domain.TransportTrafficStats{
			StartedAt: startedAt, ReadAt: readAt, BytesSent: uint64(traffic.Sent), BytesReceived: uint64(traffic.Received),
		},
		Datagram: loadStandReadingOf(loadStandDatagramSample{
			Metrics: datagram.MetricsSnapshot{
				DropsByReason: map[string]uint64{},
				SendRefusals:  map[string]uint64{},
				StartedAt:     &datagramStarted,
				ReadAt:        &datagramRead,
			},
			Reverse: datagram.ReverseDiagnostics{LocalRefusals: map[string]uint64{}},
		}),
		Sessions: domain.SessionOutcomeStats{StartedAt: startedAt, ReadAt: readAt},
		Modes:    routing.ModeSelectionStats{StartedAt: startedAt, ReadAt: readAt},
	}
}

// withDatagramCounters replaces the datagram counters of a synthetic sample,
// keeping its period.
func withDatagramCounters(sample loadStandNodeSample, metrics datagram.MetricsSnapshot, refusals map[string]uint64) loadStandNodeSample {
	current, _ := sample.Datagram.Get()
	metrics.StartedAt, metrics.ReadAt = current.Metrics.StartedAt, current.Metrics.ReadAt
	sample.Datagram = loadStandReadingOf(loadStandDatagramSample{
		Metrics: metrics,
		Reverse: datagram.ReverseDiagnostics{LocalRefusals: refusals},
	})
	return sample
}

func testLoadStandWindowSubtractsWithinOneIncarnation(t *testing.T) {
	from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{Sent: 1_000, Received: 400})
	from.Sessions.Attempts, from.Sessions.Succeeded, from.Sessions.ErrorsConnect = 5, 4, 1
	from.Modes.Decisions = []routing.ModeDecision{{Operation: 1, Mode: 1, Reason: 1, Count: 7}}
	from = withDatagramCounters(from, datagram.MetricsSnapshot{
		Observed: 10, Dropped: 2, DropsByReason: map[string]uint64{"no_route": 2}, SendRefusals: map[string]uint64{},
	}, map[string]uint64{"get_identity": 1})

	to := syntheticLoadStandSample("full-000", 1, 3*time.Minute, loadStandTrafficTotals{Sent: 1_600, Received: 900})
	to.Sessions.Attempts, to.Sessions.Succeeded, to.Sessions.ErrorsConnect, to.Sessions.ErrorsOther = 9, 6, 2, 1
	to.Modes.Decisions = []routing.ModeDecision{{Operation: 1, Mode: 1, Reason: 1, Count: 10}, {Operation: 2, Mode: 1, Reason: 3, Count: 4}}
	to = withDatagramCounters(to, datagram.MetricsSnapshot{
		Observed: 25, Dropped: 5, UnknownDType: 1,
		DropsByReason: map[string]uint64{"no_route": 3, "unknown_dtype": 2}, SendRefusals: map[string]uint64{"budget": 4},
	}, map[string]uint64{"get_identity": 3})
	// Each family is read at its own instant; its window is its own.
	to.Traffic.ReadAt = to.ReadAt.Add(-3 * time.Second)
	to.Sessions.ReadAt = to.ReadAt.Add(-time.Second)
	to.Modes.ReadAt = to.ReadAt.Add(-2 * time.Second)

	window, err := newLoadStandNodeWindow(from, to)
	if err != nil {
		t.Fatalf("newLoadStandNodeWindow: %v", err)
	}
	periods := map[string][2]time.Duration{
		"node":     {window.Period.Length(), 2 * time.Minute},
		"bytes":    {window.Bytes.Period.Length(), 2*time.Minute - 3*time.Second},
		"sessions": {window.Sessions.Period.Length(), 2*time.Minute - time.Second},
		"modes":    {window.Modes.Period.Length(), 2*time.Minute - 2*time.Second},
	}
	for family, lengths := range periods {
		if lengths[0] != lengths[1] {
			t.Errorf("%s window lasts %s, want %s", family, lengths[0], lengths[1])
		}
	}
	if window.Bytes.Sent != 600 || window.Bytes.Received != 500 {
		t.Errorf("bytes %+v, want 600 sent and 500 received", window.Bytes)
	}
	if want := (loadStandSessionDelta{Attempts: 4, Succeeded: 2, ErrorsConnect: 1, ErrorsOther: 1}); window.Sessions.Delta != want {
		t.Errorf("sessions %+v, want %+v", window.Sessions.Delta, want)
	}
	wantModes := map[loadStandModeKey]loadStandEventCount{{Operation: 1, Mode: 1, Reason: 1}: 3, {Operation: 2, Mode: 1, Reason: 3}: 4}
	if !reflect.DeepEqual(window.Modes.Counts, wantModes) {
		t.Errorf("modes %v, want %v", window.Modes.Counts, wantModes)
	}
	delta, err := window.Datagram.Get()
	if err != nil {
		t.Fatalf("datagram delta: %v", err)
	}
	wantDelta := loadStandDatagramDelta{
		Period:   loadStandPeriod{From: from.ReadAt, To: to.ReadAt},
		Observed: 15, Dropped: 3, UnknownDType: 1,
		DropsByReason: map[string]loadStandEventCount{"no_route": 1, "unknown_dtype": 2},
		SendRefusals:  map[string]loadStandEventCount{"budget": 4},
		LocalRefusals: map[string]loadStandEventCount{"get_identity": 2},
	}
	if !reflect.DeepEqual(delta, wantDelta) {
		t.Errorf("datagram %+v, want %+v", delta, wantDelta)
	}
}

// The node's samples moving forward is not enough: §5.1.1 asks it of every
// family's own read_at, which is the edge its rate is computed over.
func testLoadStandWindowRefusesAFamilyNotReadForward(t *testing.T) {
	stalls := map[string]func(from, to *loadStandNodeSample){
		"transport traffic": func(from, to *loadStandNodeSample) { to.Traffic.ReadAt = from.Traffic.ReadAt },
		"session outcomes":  func(from, to *loadStandNodeSample) { to.Sessions.ReadAt = from.Sessions.ReadAt },
		"mode selection":    func(from, to *loadStandNodeSample) { to.Modes.ReadAt = from.Modes.ReadAt.Add(-time.Second) },
		"datagram": func(from, to *loadStandNodeSample) {
			current, _ := to.Datagram.Get()
			earlier, _ := from.Datagram.Get()
			current.Metrics.ReadAt = earlier.Metrics.ReadAt
			to.Datagram = loadStandReadingOf(current)
		},
	}
	for name, stall := range stalls {
		from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
		to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
		stall(&from, &to)
		if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandWindowNotForward) {
			t.Errorf("%s: newLoadStandNodeWindow = %v, want errLoadStandWindowNotForward", name, err)
		}
	}
}

// Every counter family carries its own period; within one incarnation they
// must all still be the period that began with it.
func testLoadStandWindowRefusesACounterReset(t *testing.T) {
	resets := map[string]func(*loadStandNodeSample){
		"transport traffic": func(s *loadStandNodeSample) { s.Traffic.StartedAt = s.Traffic.StartedAt.Add(time.Second) },
		"session outcomes":  func(s *loadStandNodeSample) { s.Sessions.StartedAt = s.Sessions.StartedAt.Add(time.Second) },
		"mode selection":    func(s *loadStandNodeSample) { s.Modes.StartedAt = s.Modes.StartedAt.Add(time.Second) },
		"datagram": func(s *loadStandNodeSample) {
			current, _ := s.Datagram.Get()
			restarted := current.Metrics.StartedAt.Add(time.Second)
			current.Metrics.StartedAt = &restarted
			s.Datagram = loadStandReadingOf(current)
		},
	}
	for name, reset := range resets {
		from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
		to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
		reset(&to)
		if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandWindowCountersReset) {
			t.Errorf("%s: newLoadStandNodeWindow = %v, want errLoadStandWindowCountersReset", name, err)
		}
	}
}

// Every cumulative counter the stand subtracts only grows within an
// incarnation — the transport counters included: they are counted at the
// socket and never persisted, so no session teardown or health eviction
// moves them. One that fell refuses the window rather than being reported
// as a negative or wrapped-around number.
func testLoadStandWindowRefusesADecreasedCounter(t *testing.T) {
	decreases := map[string]func(from, to *loadStandNodeSample){
		"transport bytes received": func(from, to *loadStandNodeSample) { from.Traffic.BytesReceived, to.Traffic.BytesReceived = 500, 499 },
		"transport bytes sent":     func(from, to *loadStandNodeSample) { from.Traffic.BytesSent, to.Traffic.BytesSent = 500, 499 },
		"sessions":                 func(from, to *loadStandNodeSample) { from.Sessions.Succeeded, to.Sessions.Succeeded = 3, 2 },
		"mode decision gone": func(from, to *loadStandNodeSample) {
			from.Modes.Decisions = []routing.ModeDecision{{Operation: 1, Mode: 1, Reason: 1, Count: 2}}
		},
		"datagram reason gone": func(from, to *loadStandNodeSample) {
			*from = withDatagramCounters(*from, datagram.MetricsSnapshot{DropsByReason: map[string]uint64{"no_route": 1}}, map[string]uint64{})
		},
	}
	for name, decrease := range decreases {
		from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
		to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
		decrease(&from, &to)
		if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandCounterDecreased) {
			t.Errorf("%s: newLoadStandNodeWindow = %v, want errLoadStandCounterDecreased", name, err)
		}
	}
}

// loadStandScriptedNodeSource answers what it was given, and fails the methods
// named in failing — a node in another process, seen through its RPC, is
// exactly this to the sample.
type loadStandScriptedNodeSource struct {
	sample  loadStandNodeSample
	failing map[string]error
}

func (s loadStandScriptedNodeSource) TransportTraffic(context.Context) (domain.TransportTrafficStats, error) {
	return s.sample.Traffic, s.failing["TransportTraffic"]
}

func (s loadStandScriptedNodeSource) DatagramSummary(context.Context) (loadStandReading[loadStandDatagramSample], error) {
	return s.sample.Datagram, s.failing["DatagramSummary"]
}

func (s loadStandScriptedNodeSource) SessionOutcomes(context.Context) (domain.SessionOutcomeStats, error) {
	return s.sample.Sessions, s.failing["SessionOutcomes"]
}

func (s loadStandScriptedNodeSource) ModeSelection(context.Context) (routing.ModeSelectionStats, error) {
	return s.sample.Modes, s.failing["ModeSelection"]
}

func (s loadStandScriptedNodeSource) Neighbours(context.Context) (domain.NeighbourComposition, error) {
	return s.sample.Neighbours, s.failing["Neighbours"]
}

func (s loadStandScriptedNodeSource) RouteEntries(context.Context) (int, error) {
	return s.sample.RouteEntries, s.failing["RouteEntries"]
}

func (s loadStandScriptedNodeSource) Resources(context.Context) (domain.ResourceBreakdown, error) {
	return s.sample.Resources, s.failing["Resources"]
}

func (s loadStandScriptedNodeSource) Bans(context.Context, time.Time, loadStandBanPolicy) ([]loadStandBanFinding, error) {
	return s.sample.Bans, s.failing["Bans"]
}

// loadStandFixedClock reads one instant, so a sample's ReadAt is known.
type loadStandFixedClock struct{ at time.Time }

func (c loadStandFixedClock) Now() time.Time { return c.at }

// A sample is assembled from its source and the stand's own knowledge of
// the node, and from nothing else: the incarnation's start is the one the
// node reports with its transport counters, the life is the stand's count.
func testLoadStandSampleReadsEverythingThroughItsSource(t *testing.T) {
	want := syntheticLoadStandSample("edge-003", 2, time.Minute, loadStandTrafficTotals{Sent: 70, Received: 30})
	want.Role = loadStandRoleEdge
	want.Neighbours = domain.NeighbourComposition{Ready: true, Connections: 2}
	want.RouteEntries = 5
	want.Bans = []loadStandBanFinding{{Kind: loadStandBanScore, Key: "127.0.0.1"}}
	clock := loadStandFixedClock{at: want.ReadAt}

	subject := loadStandSampleSubject{Node: "edge-003", Role: loadStandRoleEdge, Life: 2}
	got, err := takeLoadStandNodeSample(t.Context(), clock, subject, loadStandScriptedNodeSource{sample: want}, loadStandBanPolicy{})
	if err != nil {
		t.Fatalf("takeLoadStandNodeSample: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sample %+v, want %+v", got, want)
	}
}

// A source that fails fails the sample: one missing family would make every
// window built on the sample silently partial.
func testLoadStandSampleFailsWithAnyOfItsSources(t *testing.T) {
	methods := []string{"TransportTraffic", "DatagramSummary", "SessionOutcomes", "ModeSelection", "Neighbours", "RouteEntries", "Resources", "Bans"}
	refusal := errors.New("rpc: connection refused")
	subject := loadStandSampleSubject{Node: "full-001", Role: loadStandRoleFull, Life: 1}
	for _, method := range methods {
		source := loadStandScriptedNodeSource{sample: syntheticLoadStandSample("full-001", 1, time.Minute, loadStandTrafficTotals{}), failing: map[string]error{method: refusal}}
		_, err := takeLoadStandNodeSample(t.Context(), runjournal.SystemClock{}, subject, source, loadStandBanPolicy{})
		if !errors.Is(err, errLoadStandSampleUnreadable) || !errors.Is(err, refusal) {
			t.Errorf("%s failing: takeLoadStandNodeSample = %v, want errLoadStandSampleUnreadable carrying the refusal", method, err)
		}
	}
}

// 05 §5.1.1: a pair across a restart is DISCARDED, not subtracted. For the
// byte totals this is not pedantry: a restarted node's totals include what
// peers.json persisted, so they do not even restart from zero, and a
// difference across the restart would look plausible.
func testLoadStandWindowRefusesAnotherIncarnation(t *testing.T) {
	from := syntheticLoadStandSample("edge-000", 1, time.Minute, loadStandTrafficTotals{})
	nextLife := syntheticLoadStandSample("edge-000", 2, time.Minute, loadStandTrafficTotals{})
	sameLifeOtherStart := syntheticLoadStandSample("edge-000", 1, 2*time.Minute, loadStandTrafficTotals{})
	sameLifeOtherStart.Incarnation.StartedAt = from.Incarnation.StartedAt.Add(time.Second)

	for name, to := range map[string]loadStandNodeSample{"next life": nextLife, "same life, other start": sameLifeOtherStart} {
		if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandWindowAcrossIncarnations) {
			t.Errorf("%s: newLoadStandNodeWindow = %v, want errLoadStandWindowAcrossIncarnations", name, err)
		}
	}
}

func testLoadStandWindowRefusesAPairNotReadForward(t *testing.T) {
	earlier := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
	later := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
	simultaneous := later
	simultaneous.ReadAt = earlier.ReadAt

	pairs := map[string][2]loadStandNodeSample{
		"reversed":     {later, earlier},
		"simultaneous": {earlier, simultaneous},
	}
	for name, pair := range pairs {
		if _, err := newLoadStandNodeWindow(pair[0], pair[1]); !errors.Is(err, errLoadStandWindowNotForward) {
			t.Errorf("%s: newLoadStandNodeWindow = %v, want errLoadStandWindowNotForward", name, err)
		}
	}
}

func testLoadStandWindowRefusesAnotherNode(t *testing.T) {
	from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
	to := syntheticLoadStandSample("full-001", 1, 2*time.Minute, loadStandTrafficTotals{})
	if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandWindowOtherNode) {
		t.Fatalf("newLoadStandNodeWindow = %v, want errLoadStandWindowOtherNode", err)
	}
}

func testLoadStandWindowRefusesADatagramPlaneAtOneEdge(t *testing.T) {
	from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
	to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
	to.Datagram = loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled)
	_, err := newLoadStandNodeWindow(from, to)
	if !errors.Is(err, errLoadStandWindowDatagramOneEdge) || errors.Is(err, errLoadStandWindowCountersReset) {
		t.Fatalf("newLoadStandNodeWindow = %v, want errLoadStandWindowDatagramOneEdge only", err)
	}
}

func testLoadStandWindowRefusesAnUnboundedDatagramPeriod(t *testing.T) {
	from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
	to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
	current, _ := to.Datagram.Get()
	current.Metrics.StartedAt = nil
	to.Datagram = loadStandReadingOf(current)
	if _, err := newLoadStandNodeWindow(from, to); !errors.Is(err, errLoadStandWindowUnboundedCounters) {
		t.Fatalf("newLoadStandNodeWindow = %v, want errLoadStandWindowUnboundedCounters", err)
	}
}

// A node without the datagram plane has no datagram window, and says so —
// it does not report a window of zeros.
func testLoadStandWindowWithoutDatagramPlaneHasNoDelta(t *testing.T) {
	from := syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{})
	to := syntheticLoadStandSample("full-000", 1, 2*time.Minute, loadStandTrafficTotals{})
	from.Datagram = loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled)
	to.Datagram = loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled)
	window, err := newLoadStandNodeWindow(from, to)
	if err != nil {
		t.Fatalf("newLoadStandNodeWindow: %v", err)
	}
	if _, err := window.Datagram.Get(); !errors.Is(err, errDatagramNotEnabled) {
		t.Fatalf("datagram delta error = %v, want errDatagramNotEnabled", err)
	}
}

// loadStandSyntheticNode is one node of a synthetic balance: read readDelay
// after each sweep begins, sending and receiving at a steady rate (bytes per
// second of its life), plus a one-off carried between the first two sweeps.
type loadStandSyntheticNode struct {
	id        loadStandNodeID
	readDelay time.Duration
	sendRate  loadStandByteCount
	recvRate  loadStandByteCount
	burst     loadStandTrafficTotals
}

// syntheticLoadStandSweepsAt reads every node once per sweep offset. The
// burst lands between the first sweep and the second.
func syntheticLoadStandSweepsAt(offsets []time.Duration, nodes ...loadStandSyntheticNode) []loadStandSweep {
	sweeps := make([]loadStandSweep, len(offsets))
	for i, offset := range offsets {
		for _, node := range nodes {
			at := offset + node.readDelay
			seconds := loadStandByteCount(at / time.Second)
			totals := loadStandTrafficTotals{Sent: node.sendRate * seconds, Received: node.recvRate * seconds}
			if i > 0 {
				totals.Sent += node.burst.Sent
				totals.Received += node.burst.Received
			}
			sweeps[i].Nodes = append(sweeps[i].Nodes, syntheticLoadStandSample(node.id, 1, at, totals))
		}
		sweeps[i] = syntheticLoadStandSweepOf(sweeps[i].Nodes...)
	}
	return sweeps
}

// syntheticLoadStandSweepOf is a sweep of samples that began with its first
// transport read and ended with its last.
func syntheticLoadStandSweepOf(samples ...loadStandNodeSample) loadStandSweep {
	sweep := loadStandSweep{Nodes: samples}
	sweep.Begin, sweep.End = loadStandTrafficReadRange(sweep)
	return sweep
}

// syntheticLoadStandBalanceSweeps is two sweeps interval apart.
func syntheticLoadStandBalanceSweeps(interval time.Duration, nodes ...loadStandSyntheticNode) (loadStandSweep, loadStandSweep) {
	sweeps := syntheticLoadStandSweepsAt([]time.Duration{time.Hour, time.Hour + interval}, nodes...)
	return sweeps[0], sweeps[1]
}

// Every byte one node sent another received: read at one instant, with no
// slack, a balanced stand passes exactly.
func testLoadStandByteBalanceHoldsOnAClosedSystem(t *testing.T) {
	earlier, later := syntheticLoadStandBalanceSweeps(10*time.Second,
		loadStandSyntheticNode{id: "full-000", burst: loadStandTrafficTotals{Sent: 1_000, Received: 100}},
		loadStandSyntheticNode{id: "edge-000", burst: loadStandTrafficTotals{Sent: 60, Received: 700}},
		loadStandSyntheticNode{id: "edge-001", burst: loadStandTrafficTotals{Sent: 40, Received: 300}},
	)
	balance, err := checkLoadStandByteBalance(earlier, later, nil, loadStandBalanceTolerance{MaxAllowanceShare: 1})
	if err != nil {
		t.Fatalf("a balanced closed system with zero tolerance: %v", err)
	}
	if balance.Nodes != 3 || balance.Volume != 1_100 || balance.Imbalance != 0 {
		t.Fatalf("balance %+v, want 3 nodes, volume 1100, no imbalance", balance)
	}
}

// Two nodes read 1 s apart, sweeps 10 s apart: the interval inside both
// windows is 9 s and each sweep spreads over 1 s. Volume 9 000 with burst
// factor 2 gives 2 × 9 000 × 2 s / 9 s = 4 000; in-flight 50 per node adds
// 100. An imbalance of 4 100 passes, 4 101 is a defect, and the share
// returned is 4 100 / 9 000 either way.
func testLoadStandByteBalanceToleranceIsExact(t *testing.T) {
	tolerance := loadStandBalanceTolerance{BurstFactor: 2, PerNodeInFlight: 50, MaxAllowanceShare: 1}
	cases := map[loadStandByteCount]bool{3_900: true, 3_899: false}
	for received, holds := range cases {
		earlier, later := syntheticLoadStandBalanceSweeps(10*time.Second,
			loadStandSyntheticNode{id: "full-000", burst: loadStandTrafficTotals{Sent: 8_000, Received: 1_000}},
			loadStandSyntheticNode{id: "edge-000", readDelay: time.Second, burst: loadStandTrafficTotals{Sent: 1_000, Received: received}},
		)
		balance, err := checkLoadStandByteBalance(earlier, later, nil, tolerance)
		var defect *loadStandDefect
		switch {
		case holds && err != nil:
			t.Errorf("received %d: imbalance %d refused: %v", received, balance.Imbalance, err)
		case !holds && (!errors.As(err, &defect) || defect.Kind != loadStandDefectByteBalance):
			t.Errorf("received %d: imbalance %d gave %v, want a byte_balance defect", received, balance.Imbalance, err)
		}
		if balance.Allowance != 4_100 || balance.AllowanceShare != loadStandShare(4_100.0/9_000.0) {
			t.Errorf("received %d: allowance %d at share %v, want 4100 at %v", received, balance.Allowance, balance.AllowanceShare, 4_100.0/9_000.0)
		}
	}
}

// An allowance of exactly the threshold share still judges: the check is
// blind only ABOVE it. Volume 10 000, in-flight 500 per node on two nodes,
// no skew: the share is exactly 10 %.
func testLoadStandByteBalanceAtTheThresholdIsJudged(t *testing.T) {
	tolerance := loadStandBalanceTolerance{PerNodeInFlight: 500, MaxAllowanceShare: 0.10}
	for received, wantDefect := range map[loadStandByteCount]bool{9_000: false, 8_999: true} {
		earlier, later := syntheticLoadStandBalanceSweeps(time.Minute,
			loadStandSyntheticNode{id: "full-000", burst: loadStandTrafficTotals{Sent: 10_000}},
			loadStandSyntheticNode{id: "edge-000", burst: loadStandTrafficTotals{Received: received}},
		)
		balance, err := checkLoadStandByteBalance(earlier, later, nil, tolerance)
		if balance.AllowanceShare != 0.10 || errors.Is(err, errLoadStandBalanceUndefined) {
			t.Errorf("received %d: share %v gave %v, want a judged balance at exactly 10%%", received, balance.AllowanceShare, err)
		}
		if gotDefect := errors.Is(err, errLoadStandDefect); gotDefect != wantDefect {
			t.Errorf("received %d: defect=%v, want %v (%v)", received, gotDefect, wantDefect, err)
		}
	}
}

// In a window short against its read skew the allowance swallows the volume:
// a node counting every received byte twice is not a pass, it is undefined —
// and the share says why.
func testLoadStandByteBalanceBlindInASmallWindow(t *testing.T) {
	earlier, later := syntheticLoadStandBalanceSweeps(3*time.Second,
		loadStandSyntheticNode{id: "full-000", burst: loadStandTrafficTotals{Sent: 10_000}},
		loadStandSyntheticNode{id: "edge-000", readDelay: time.Second, burst: loadStandTrafficTotals{Received: 20_000}},
	)
	balance, err := checkLoadStandByteBalance(earlier, later, nil, loadStandDefaultBalanceTolerance())
	if !errors.Is(err, errLoadStandBalanceUndefined) || errors.Is(err, errLoadStandDefect) {
		t.Fatalf("a double count under a %.0f%% allowance gave %v, want undefined", 100*float64(balance.AllowanceShare), err)
	}
	if balance.AllowanceShare <= loadStandDefaultBalanceTolerance().MaxAllowanceShare || balance.Volume != 20_000 {
		t.Fatalf("balance %+v: want the blind share and the volume returned with the refusal", balance)
	}
}

// The same double count over a window the allowance is small against is a
// defect — whichever side counts twice.
func testLoadStandByteBalanceCatchesDoubleCountInAWideOne(t *testing.T) {
	cases := map[string][2]loadStandTrafficTotals{
		"receipts counted twice": {{Sent: 1_000_000}, {Received: 2_000_000}},
		"sends counted twice":    {{Sent: 2_000_000}, {Received: 1_000_000}},
	}
	for name, bursts := range cases {
		earlier, later := syntheticLoadStandBalanceSweeps(time.Minute,
			loadStandSyntheticNode{id: "full-000", burst: bursts[0]},
			loadStandSyntheticNode{id: "edge-000", burst: bursts[1]},
		)
		balance, err := checkLoadStandByteBalance(earlier, later, nil, loadStandDefaultBalanceTolerance())
		var defect *loadStandDefect
		if !errors.As(err, &defect) || defect.Kind != loadStandDefectByteBalance {
			t.Errorf("%s: a double count under a %.1f%% allowance gave %v, want a byte_balance defect", name, 100*float64(balance.AllowanceShare), err)
		}
	}
}

// The two sweeps of a balance need not be adjacent: over a long span the
// same read skew is a small share of the volume, and a stand whose adjacent
// windows are all too short to judge is judged across them. Nodes read 1 s
// apart, both carrying 500 B/s each way, burst factor 1: 3 s apart the share
// is 100 %, 100 s apart it is 2 %.
func testLoadStandByteBalanceOverALongSpanSeesAgain(t *testing.T) {
	sweeps := syntheticLoadStandSweepsAt([]time.Duration{time.Hour, time.Hour + 3*time.Second, time.Hour + 100*time.Second},
		loadStandSyntheticNode{id: "full-000", sendRate: 500, recvRate: 500},
		loadStandSyntheticNode{id: "edge-000", readDelay: time.Second, sendRate: 500, recvRate: 500},
	)
	tolerance := loadStandBalanceTolerance{BurstFactor: 1, MaxAllowanceShare: 0.10}
	if _, err := checkLoadStandByteBalance(sweeps[0], sweeps[1], nil, tolerance); !errors.Is(err, errLoadStandBalanceUndefined) {
		t.Fatalf("adjacent sweeps 3 s apart: %v, want undefined", err)
	}
	balance, err := checkLoadStandByteBalance(sweeps[0], sweeps[2], nil, tolerance)
	if err != nil {
		t.Fatalf("sweeps 100 s apart: %v", err)
	}
	if balance.AllowanceShare > 0.05 {
		t.Fatalf("over 100 s the share is %v, want about 2%%", balance.AllowanceShare)
	}
}

// The balance is a statement about the WHOLE stand over one interval; a node
// restarted, stopped or started in between — or nothing carried, or no
// interval inside every window — leaves it undefined, which is not a defect.
// The stand keeps traffic elsewhere in every case, so the refusal comes from
// the rule, not from an empty stand.
func testLoadStandByteBalanceUndefinedWithoutEveryWindow(t *testing.T) {
	changes := map[string]func(earlier, later *loadStandSweep){
		"a node restarted": func(earlier, later *loadStandSweep) {
			later.Nodes[1] = syntheticLoadStandSample(later.Nodes[1].Node, 2, time.Second, loadStandTrafficTotals{})
		},
		"a node stopped": func(earlier, later *loadStandSweep) { later.Nodes = later.Nodes[:2] },
		"a node started": func(earlier, later *loadStandSweep) { earlier.Nodes = earlier.Nodes[:2] },
		"nothing carried": func(earlier, later *loadStandSweep) {
			for i := range later.Nodes {
				later.Nodes[i].Traffic.BytesSent, later.Nodes[i].Traffic.BytesReceived = earlier.Nodes[i].Traffic.BytesSent, earlier.Nodes[i].Traffic.BytesReceived
			}
		},
		"no interval inside every window": func(earlier, later *loadStandSweep) {
			late := earlier.Nodes[2].ReadAt.Add(2 * time.Minute)
			later.Nodes[2].ReadAt, later.Nodes[2].Traffic.ReadAt = late.Add(time.Minute), late.Add(time.Minute)
			earlier.Nodes[2].ReadAt, earlier.Nodes[2].Traffic.ReadAt = late, late
			earlier.Nodes[2].Sessions.ReadAt, earlier.Nodes[2].Modes.ReadAt = late, late
			later.Nodes[2].Sessions.ReadAt, later.Nodes[2].Modes.ReadAt = late.Add(time.Minute), late.Add(time.Minute)
			earlier.Nodes[2].Datagram = loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled)
			later.Nodes[2].Datagram = loadStandReadingRefused[loadStandDatagramSample](errDatagramNotEnabled)
		},
	}
	for name, change := range changes {
		earlier, later := syntheticLoadStandBalanceSweeps(time.Minute,
			loadStandSyntheticNode{id: "full-000", burst: loadStandTrafficTotals{Sent: 2_000_000}},
			loadStandSyntheticNode{id: "edge-000", burst: loadStandTrafficTotals{Received: 1_000_000}},
			loadStandSyntheticNode{id: "edge-001", burst: loadStandTrafficTotals{Received: 1_000_000}},
		)
		if _, err := checkLoadStandByteBalance(earlier, later, nil, loadStandDefaultBalanceTolerance()); err != nil {
			t.Fatalf("the unchanged stand does not balance: %v", err)
		}
		change(&earlier, &later)
		_, err := checkLoadStandByteBalance(earlier, later, nil, loadStandDefaultBalanceTolerance())
		if !errors.Is(err, errLoadStandBalanceUndefined) || errors.Is(err, errLoadStandDefect) {
			t.Errorf("%s: checkLoadStandByteBalance = %v, want errLoadStandBalanceUndefined and no defect", name, err)
		}
	}
}

func syntheticLoadStandFloorSweep(heapObjects loadStandByteCount) loadStandSweep {
	sweep := loadStandSweep{Begin: loadStandAt(0), End: loadStandAt(time.Second)}
	for _, node := range []loadStandNodeID{"full-000", "edge-000"} {
		sample := syntheticLoadStandSample(node, 1, time.Second, loadStandTrafficTotals{})
		// 10 × 50 = 500 bytes per node, 1 000 for the sweep.
		sample.Resources = domain.NewResourceBreakdown(loadStandAt(0),
			domain.NewSubsystemUsage(domain.ResourceSubsystemRoutePlane, domain.NewResourceGauge("claims", 10, 50)))
		sweep.Nodes = append(sweep.Nodes, sample)
	}
	sweep.Process = loadStandProcessSample{HeapObjects: heapObjects}
	return sweep
}

// The subsystem figures are floors of what each node holds, and every node
// lives in this one heap — so their sum above the heap means the floors are
// not floors.
func testLoadStandFloorAboveHeapIsADefect(t *testing.T) {
	if err := checkLoadStandSweep(syntheticLoadStandFloorSweep(1_000)); err != nil {
		t.Fatalf("floors equal to the heap: %v", err)
	}
	err := checkLoadStandSweep(syntheticLoadStandFloorSweep(999))
	var defect *loadStandDefect
	if !errors.As(err, &defect) || defect.Kind != loadStandDefectFloorAboveHeap {
		t.Fatalf("floors of 1000 over a heap of 999 gave %v, want a floor_above_heap defect", err)
	}
}

func testLoadStandBansAreADefect(t *testing.T) {
	sweep := syntheticLoadStandFloorSweep(1_000)
	sweep.Nodes[1].Bans = []loadStandBanFinding{{Kind: loadStandBanScore, Key: "127.0.0.1"}}
	err := checkLoadStandSweep(sweep)
	var defect *loadStandDefect
	if !errors.As(err, &defect) || defect.Kind != loadStandDefectBansPresent {
		t.Fatalf("a banned node gave %v, want a bans_present defect", err)
	}
}

// A peak RSS beyond everything the Go runtime mapped (times a margin) is a
// unit error.
func testLoadStandImplausibleRSSIsADefect(t *testing.T) {
	plausible := loadStandProcessSample{
		GoMemory: 100 << 20,
		Rusage:   loadStandReadingOf(loadStandRusage{UserCPU: 3 * time.Second, SystemCPU: time.Second, MaxRSS: 300 << 20}),
	}
	if err := checkLoadStandProcess(plausible); err != nil {
		t.Fatalf("a plausible process: %v", err)
	}
	usage, _ := plausible.Rusage.Get()
	usage.MaxRSS *= 1024
	implausible := plausible
	implausible.Rusage = loadStandReadingOf(usage)
	var defect *loadStandDefect
	if err := checkLoadStandProcess(implausible); !errors.As(err, &defect) || defect.Kind != loadStandDefectProcessImplausible {
		t.Errorf("RSS in the wrong unit: checkLoadStandProcess = %v, want a process_implausible defect", err)
	}

	// A heap far above the floors, so the only defect is the process's.
	inSweep := syntheticLoadStandFloorSweep(0)
	inSweep.Process = implausible
	inSweep.Process.HeapObjects = 1 << 40
	if err := checkLoadStandSweep(inSweep); !errors.As(err, &defect) || defect.Kind != loadStandDefectProcessImplausible {
		t.Errorf("checkLoadStandSweep over an implausible process = %v, want a process_implausible defect", err)
	}

	withoutRusage := plausible
	withoutRusage.Rusage = loadStandReadingRefused[loadStandRusage](errors.New("no getrusage here"))
	if err := checkLoadStandProcess(withoutRusage); err != nil {
		t.Fatalf("a platform without rusage has nothing to contradict, got %v", err)
	}
}

// The kernel's CPU over a window is bounded by GOMAXPROCS × the window's wall
// time, both measured by the stand: 4 processors over 10 s allow 40 s, twice
// that with the margin, plus the accounting jitter.
func testLoadStandImplausibleCPUOverAWindowIsADefect(t *testing.T) {
	process := func(at time.Duration, cpu time.Duration) loadStandProcessSample {
		return loadStandProcessSample{
			ReadAt: loadStandAt(at),
			Procs:  4,
			Rusage: loadStandReadingOf(loadStandRusage{UserCPU: cpu / 2, SystemCPU: cpu / 2}),
		}
	}
	earlier := process(0, time.Second)
	ceiling := time.Second + 2*40*time.Second + loadStandKernelCPUJitter
	if err := checkLoadStandProcessWindow(earlier, process(10*time.Second, ceiling)); err != nil {
		t.Fatalf("CPU at the ceiling: %v", err)
	}
	var defect *loadStandDefect
	if err := checkLoadStandProcessWindow(earlier, process(10*time.Second, ceiling+2*time.Millisecond)); !errors.As(err, &defect) || defect.Kind != loadStandDefectProcessImplausible {
		t.Errorf("CPU above the ceiling: %v, want a process_implausible defect", err)
	}
	if err := checkLoadStandProcessWindow(earlier, process(0, time.Second)); !errors.Is(err, errLoadStandWindowNotForward) {
		t.Errorf("a window of no wall time: %v, want errLoadStandWindowNotForward", err)
	}
	withoutRusage := process(10*time.Second, time.Hour)
	withoutRusage.Rusage = loadStandReadingRefused[loadStandRusage](errors.New("no getrusage here"))
	if err := checkLoadStandProcessWindow(earlier, withoutRusage); err != nil {
		t.Errorf("rusage refused at the later edge only: nothing to contradict, got %v", err)
	}
	refusedEarlier := earlier
	refusedEarlier.Rusage = loadStandReadingRefused[loadStandRusage](errors.New("no getrusage here"))
	if err := checkLoadStandProcessWindow(refusedEarlier, process(10*time.Second, time.Hour)); err != nil {
		t.Errorf("rusage refused at the earlier edge only: nothing to contradict, got %v", err)
	}
	// GOMAXPROCS raised inside the window: the larger count bounds it, so a
	// window that used the new processors is not a defect.
	raised := process(10*time.Second, time.Second+2*80*time.Second)
	raised.Procs = 8
	if err := checkLoadStandProcessWindow(earlier, raised); err != nil {
		t.Errorf("GOMAXPROCS 4 → 8 with 160 s of CPU over 10 s: %v", err)
	}
}

// The process sample reads this very process, so its figures have floors
// that hold on any platform, and the kernel's figures must agree with what
// the stand itself measured. It holds with GOGC=off too, where the runtime's
// CPU classes never move: nothing here reads them for a check.
func testLoadStandProcessSampleReadsTheRuntime(t *testing.T) {
	first, err := takeLoadStandProcessSample(runjournal.SystemClock{})
	if err != nil {
		t.Fatalf("takeLoadStandProcessSample: %v", err)
	}
	if first.HeapObjects == 0 || first.HeapAllocs < first.HeapObjects || first.GoMemory < first.HeapObjects {
		t.Errorf("heap objects %d, allocs %d, Go memory %d: want objects > 0, allocs and Go memory at least objects",
			first.HeapObjects, first.HeapAllocs, first.GoMemory)
	}
	if first.Goroutines == 0 || first.ReadAt.IsZero() {
		t.Errorf("goroutines %d, read at %s", first.Goroutines, first.ReadAt)
	}
	if want := loadStandProcCount(runtime.GOMAXPROCS(0)); first.Procs != want {
		t.Errorf("sample records %d processors, GOMAXPROCS is %d", first.Procs, want)
	}
	if descriptors, err := first.Descriptors.Get(); err == nil && descriptors < 3 {
		t.Errorf("%d open descriptors: a process holds at least stdin, stdout and stderr", descriptors)
	}
	if usage, err := first.Rusage.Get(); err == nil && (usage.MaxRSS < first.HeapObjects || usage.UserCPU <= 0) {
		t.Errorf("rusage %+v: max RSS below the heap (%d) or no user CPU", usage, first.HeapObjects)
	}
	if err := checkLoadStandProcess(first); err != nil {
		t.Errorf("the real process is implausible: %v", err)
	}

	busyUntil := time.Now().Add(20 * time.Millisecond)
	for time.Now().Before(busyUntil) {
		runtime.Gosched()
	}
	second, err := takeLoadStandProcessSample(runjournal.SystemClock{})
	if err != nil {
		t.Fatalf("takeLoadStandProcessSample: %v", err)
	}
	if err := checkLoadStandProcessWindow(first, second); err != nil {
		t.Errorf("the real process over a window is implausible: %v", err)
	}
	if got := loadStandCPUSeconds(1.5); got != 1500*time.Millisecond {
		t.Errorf("1.5 CPU seconds read as %s", got)
	}
}

// Nearest-rank statistics per group, hubs apart from the other full nodes.
// Edges 1..10: median 5, p90 9, max 10. Full nodes 100, 300, 200: median
// 200, p90 300, max 300. One hub: 7 everywhere.
func testLoadStandAggregatesByRoleWithHubsApart(t *testing.T) {
	var values []loadStandRoleValue[loadStandByteCount]
	for i := range 10 {
		values = append(values, loadStandRoleValue[loadStandByteCount]{Node: loadStandEdgeNodeID(i), Role: loadStandRoleEdge, Value: loadStandByteCount(10 - i)})
	}
	for i, value := range []loadStandByteCount{100, 300, 200} {
		values = append(values, loadStandRoleValue[loadStandByteCount]{Node: loadStandFullNodeID(i + 1), Role: loadStandRoleFull, Value: value})
	}
	values = append(values, loadStandRoleValue[loadStandByteCount]{Node: loadStandFullNodeID(0), Role: loadStandRoleFull, Hub: true, Value: 7})

	got, err := aggregateLoadStandByRole(values)
	if err != nil {
		t.Fatalf("aggregateLoadStandByRole: %v", err)
	}
	want := map[loadStandRoleGroup]loadStandRoleStats[loadStandByteCount]{
		loadStandGroupEdge: {Count: 10, Median: 5, P90: 9, Max: 10},
		loadStandGroupFull: {Count: 3, Median: 200, P90: 300, Max: 300},
		loadStandGroupHub:  {Count: 1, Median: 7, P90: 7, Max: 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregate %+v, want %+v", got, want)
	}
}

func testLoadStandAggregateRefusesAValueWithoutAGroup(t *testing.T) {
	invalid := map[string]loadStandRoleValue[loadStandByteCount]{
		"an edge hub":  {Node: "edge-000", Role: loadStandRoleEdge, Hub: true},
		"unknown role": {Node: "odd-000", Role: "relay"},
	}
	for name, value := range invalid {
		if _, err := aggregateLoadStandByRole([]loadStandRoleValue[loadStandByteCount]{value}); !errors.Is(err, errLoadStandAggregateInvalid) {
			t.Errorf("%s: aggregateLoadStandByRole = %v, want errLoadStandAggregateInvalid", name, err)
		}
	}
}

// A metric the release does not know, or one read as the wrong kind, is
// refused — not stored as a zero, and not read through a panicking accessor.
func testLoadStandRuntimeMetricsRefuseWhatTheyCannotRead(t *testing.T) {
	tables := map[string][]loadStandRuntimeMetric{
		"unknown metric": {{name: "/loadstand/no-such-metric:bytes", kind: metrics.KindUint64, store: func(s *loadStandProcessSample, v metrics.Value) {
			s.HeapLive = loadStandByteCount(v.Uint64())
		}}},
		"wrong kind": {{name: "/gc/heap/live:bytes", kind: metrics.KindFloat64, store: func(s *loadStandProcessSample, v metrics.Value) {
			s.CPUTotal = loadStandCPUSeconds(v.Float64())
		}}},
	}
	for name, table := range tables {
		var sample loadStandProcessSample
		if err := readLoadStandRuntimeMetrics(&sample, table); !errors.Is(err, errLoadStandRuntimeMetricUnsupported) {
			t.Errorf("%s: readLoadStandRuntimeMetrics = %v, want errLoadStandRuntimeMetricUnsupported", name, err)
		}
	}
}

// Which runtime metric feeds which field is the contract the self-checks
// rest on: the floor check needs the heap's objects NOW, not the live heap
// of the last GC, and the CPU check needs the CPU made available.
func testLoadStandRuntimeMetricsFeedTheirFields(t *testing.T) {
	var sample loadStandProcessSample
	want := map[string]any{
		"/gc/heap/live:bytes":                &sample.HeapLive,
		"/memory/classes/heap/objects:bytes": &sample.HeapObjects,
		"/gc/heap/allocs:bytes":              &sample.HeapAllocs,
		"/memory/classes/total:bytes":        &sample.GoMemory,
		"/gc/cycles/total:gc-cycles":         &sample.GCCycles,
		"/sched/goroutines:goroutines":       &sample.Goroutines,
		"/cpu/classes/total:cpu-seconds":     &sample.CPUTotal,
		"/cpu/classes/user:cpu-seconds":      &sample.CPUUser,
		"/cpu/classes/gc/total:cpu-seconds":  &sample.CPUGC,
		"/cpu/classes/idle:cpu-seconds":      &sample.CPUIdle,
	}
	got := make(map[string]any, len(loadStandRuntimeMetrics))
	for _, metric := range loadStandRuntimeMetrics {
		got[metric.name] = metric.field(&sample)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime metrics feed %v, want %v", got, want)
	}
}

// A directory that lists the same entries whatever the process opens — as
// /dev/fd does on FreeBSD without fdescfs — is refused, so the count is
// skipped rather than reported. A plain directory stands in for it here.
func testLoadStandDescriptorCountRefusesAStaticListing(t *testing.T) {
	static := t.TempDir()
	if _, err := descriptorCountIn(static); !errors.Is(err, errDescriptorListingStatic) {
		t.Fatalf("descriptorCountIn(static directory) = %v, want errDescriptorListingStatic", err)
	}
}

// The one integration subtest: a full node and an edge dialling it.
//
//   - Two sweeps of one incarnation make a window.
//   - The edge's session is cut twice, as a flapping link would cut it: every
//     window between the cuts is defined (100 % within one incarnation, which
//     the earlier ledger-based windows could not promise), and the windows
//     add up to the window across all of them.
//   - The stand's balance over the whole span is never a defect.
//   - After the edge restarts, the pair across the restart is refused.
func testLoadStandTwoRealNodesGiveAWindowUntilEdgeRestarted(t *testing.T) {
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	full := newFullNodeForTest(t, ports, "full-000", loopbackOpts())
	address, _ := full.DialAddress()
	edgeOpts := loopbackOpts()
	edgeOpts.BootstrapPeers = []domain.PeerAddress{address}
	edge := newEdgeNodeForTest(t, "edge-000", edgeOpts)
	startLoadStandNodeForTest(t, full)
	startLoadStandNodeForTest(t, edge)

	clock := runjournal.SystemClock{}
	nodes := []*loadStandNode{full, edge}
	awaitLoadStandRedial(t, edge, nil)
	first := takeLoadStandSweepForTest(t, clock, nodes)
	second := takeLoadStandSweepForTest(t, clock, nodes)
	if err := checkLoadStandSweep(second); err != nil {
		t.Fatalf("self-checks on a fresh two-node stand: %v", err)
	}
	for i := range first.Nodes {
		window, err := newLoadStandNodeWindow(first.Nodes[i], second.Nodes[i])
		if err != nil {
			t.Fatalf("window of %s within one incarnation: %v", first.Nodes[i].Node, err)
		}
		if window.Incarnation.Life != 1 || window.Role != first.Nodes[i].Role {
			t.Errorf("window of %s: life %d role %s", window.Node, window.Incarnation.Life, window.Role)
		}
	}

	edgeSamples := []loadStandNodeSample{second.Nodes[1]}
	for range 2 {
		cut := cutLoadStandSessions(t, edge)
		awaitLoadStandRedial(t, edge, cut)
		edgeSamples = append(edgeSamples, takeLoadStandSweepForTest(t, clock, nodes).Nodes[1])
	}
	var adjacent loadStandByteWindow
	for i := 1; i < len(edgeSamples); i++ {
		window, err := newLoadStandNodeWindow(edgeSamples[i-1], edgeSamples[i])
		if err != nil {
			t.Fatalf("window %d across a cut session: %v", i, err)
		}
		adjacent.Sent += window.Bytes.Sent
		adjacent.Received += window.Bytes.Received
	}
	whole, err := newLoadStandNodeWindow(edgeSamples[0], edgeSamples[len(edgeSamples)-1])
	if err != nil {
		t.Fatalf("window across both cuts: %v", err)
	}
	if whole.Bytes.Sent != adjacent.Sent || whole.Bytes.Received != adjacent.Received || whole.Bytes.Sent == 0 {
		t.Errorf("across the cuts the edge carried %d/%d bytes, its windows add up to %d/%d",
			whole.Bytes.Sent, whole.Bytes.Received, adjacent.Sent, adjacent.Received)
	}

	// The edge's only peer is the full node and the full node's only peer is
	// the edge, both in their first life: once the stand is quiet, each
	// node's cumulative counters are EXACTLY the other's. A cut closes a
	// socket whose bytes were all read — the far end reads to EOF — so it
	// strands nothing on an idle stand, and any tolerance here would be room
	// for a socket counted twice (a doubled meter moves a direction by its
	// whole volume, a few kilobytes on this stand).
	quiet := awaitLoadStandQuiet(t, full, edge)
	requireLoadStandMirrored(t, "after the cuts", quiet[0], quiet[1])

	last := takeLoadStandSweepForTest(t, clock, nodes)
	balance, err := checkLoadStandByteBalance(first, last, nil, loadStandDefaultBalanceTolerance())
	if errors.Is(err, errLoadStandDefect) {
		t.Fatalf("the two-node stand does not balance: %v (%+v)", err, balance)
	}
	t.Logf("two-node balance over %d cuts: %+v (%v)", len(edgeSamples)-1, balance, err)

	stoppedService := runningServiceForTest(t, edge)
	stopLoadStandNodeForTest(t, edge)
	// The final reading is taken after the stop, so nothing the stop itself
	// carried is missing from it: it is what the stopped Service still reads.
	recorded := edge.EndedIncarnations()
	afterStop := stoppedService.TransportTrafficStats()
	if final := recorded[len(recorded)-1].Final; final.BytesSent != afterStop.BytesSent || final.BytesReceived != afterStop.BytesReceived {
		t.Fatalf("the final reading %d/%d is not the counters after the stop, %d/%d", final.BytesSent, final.BytesReceived, afterStop.BytesSent, afterStop.BytesReceived)
	}
	if err := edge.Start(t.Context()); err != nil {
		t.Fatalf("restart edge: %v", err)
	}
	awaitLoadStandRedial(t, edge, nil)
	awaitLoadStandQuiet(t, full, edge)
	third := takeLoadStandSweepForTest(t, clock, nodes)
	edgeBefore, edgeAfter := last.Nodes[1], third.Nodes[1]
	if edgeAfter.Incarnation.Life != 2 || edgeAfter.Incarnation.StartedAt.Equal(edgeBefore.Incarnation.StartedAt) {
		t.Fatalf("restarted edge reports incarnation %+v after %+v", edgeAfter.Incarnation, edgeBefore.Incarnation)
	}
	if _, err := newLoadStandNodeWindow(edgeBefore, edgeAfter); !errors.Is(err, errLoadStandWindowAcrossIncarnations) {
		t.Fatalf("window across the edge's restart = %v, want errLoadStandWindowAcrossIncarnations", err)
	}
	if _, err := newLoadStandNodeWindow(last.Nodes[0], third.Nodes[0]); err != nil {
		t.Fatalf("the full node did not restart, yet its window was refused: %v", err)
	}

	// Across the restart the balance is still defined: the edge's first life
	// is counted to the reading taken after its stop, its second from zero.
	// Both sweeps are taken on a quiet stand, so nothing is on its way at
	// either edge and the balance must hold with no allowance at all — the
	// same exactness as above, now across a stop.
	exact := loadStandBalanceTolerance{MaxAllowanceShare: loadStandShare(math.Inf(1))}
	balance, err = checkLoadStandByteBalance(last, third, edge.EndedIncarnations(), exact)
	if err != nil || balance.Stops != 1 || balance.Imbalance != 0 {
		t.Fatalf("balance across the edge's restart = %+v, %v; want a defined, exact balance over one stop", balance, err)
	}
}

// awaitLoadStandRedial waits until node holds an outbound session that is
// not one of cut — after start-up (cut empty), and after a cut until the
// connection manager has dialled again. A cut session leaves s.sessions
// asynchronously, so "any session" would be satisfied by the one just cut.
func awaitLoadStandRedial(t *testing.T, node *loadStandNode, cut map[domain.ConnID]struct{}) {
	t.Helper()

	svc := runningServiceForTest(t, node)
	deadline := time.Now().Add(loadStandSampleWait)
	for {
		live := loadStandSessionIDs(svc)
		if loadStandHoldsSessionOutside(live, cut) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s held no session beyond the cut %v within %s (live: %v)", node.id, slices.Collect(maps.Keys(cut)), loadStandSampleWait, live)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func loadStandSessionIDs(svc *Service) []domain.ConnID {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	ids := make([]domain.ConnID, 0, len(svc.sessions))
	for _, session := range svc.sessions {
		ids = append(ids, session.connID)
	}
	return ids
}

func takeLoadStandSweepForTest(t *testing.T, clock runjournal.Clock, nodes []*loadStandNode) loadStandSweep {
	t.Helper()

	sweep, err := takeLoadStandSweep(t.Context(), clock, nodes, loadStandBanPolicy{})
	if err != nil {
		t.Fatalf("takeLoadStandSweep: %v", err)
	}
	if len(sweep.Nodes) != len(nodes) {
		t.Fatalf("sweep sampled %d of %d running nodes", len(sweep.Nodes), len(nodes))
	}
	return sweep
}

// cutLoadStandSessions closes every outbound session of node through its
// Network, as a dropped TCP connection would end it.
func cutLoadStandSessions(t *testing.T, node *loadStandNode) map[domain.ConnID]struct{} {
	t.Helper()

	svc := runningServiceForTest(t, node)
	ids := loadStandSessionIDs(svc)
	if len(ids) == 0 {
		t.Fatal("node has no session to cut")
	}
	cut := make(map[domain.ConnID]struct{}, len(ids))
	for _, id := range ids {
		if err := svc.Network().Close(t.Context(), id); err != nil {
			t.Fatalf("close session %d: %v", id, err)
		}
		cut[id] = struct{}{}
	}
	return cut
}

// requireLoadStandMirrored checks two nodes that are each other's only peer:
// what one sent is exactly what the other received, both ways.
func requireLoadStandMirrored(t *testing.T, stage string, full, edge domain.TransportTrafficStats) {
	t.Helper()

	directions := map[string][2]uint64{
		"edge→full": {edge.BytesSent, full.BytesReceived},
		"full→edge": {full.BytesSent, edge.BytesReceived},
	}
	for direction, pair := range directions {
		if pair[0] != pair[1] {
			t.Errorf("%s, %s: %d sent, %d received — a quiet two-node stand must agree exactly", stage, direction, pair[0], pair[1])
		}
	}
}

// awaitLoadStandQuiet waits until two readings of every node's transport
// counters, 100 ms apart, are identical, and returns them in nodes' order.
func awaitLoadStandQuiet(t *testing.T, nodes ...*loadStandNode) []domain.TransportTrafficStats {
	t.Helper()

	read := func() []domain.TransportTrafficStats {
		stats := make([]domain.TransportTrafficStats, len(nodes))
		for i, node := range nodes {
			stats[i] = runningServiceForTest(t, node).TransportTrafficStats()
			stats[i].ReadAt = time.Time{}
		}
		return stats
	}
	deadline := time.Now().Add(loadStandSampleWait)
	previous := read()
	for {
		time.Sleep(100 * time.Millisecond)
		current := read()
		if reflect.DeepEqual(current, previous) {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stand did not go quiet within %s: %+v then %+v", loadStandSampleWait, previous, current)
		}
		previous = current
	}
}

// loadStandSampleWait bounds how long a sample is awaited; start-up and a
// session drop take milliseconds, and the bound is for a loaded machine.
const loadStandSampleWait = 10 * time.Second

// A cut session leaves s.sessions asynchronously, so right after the cut the
// node still lists it. Waiting for "any session" returns at once and samples
// before the redial; only a session the cut did not close is the redial.
func testLoadStandRedialIsASessionNotAmongTheCut(t *testing.T) {
	cut := map[domain.ConnID]struct{}{7: {}}
	cases := map[string]struct {
		live []domain.ConnID
		want bool
	}{
		"nothing live":              {live: nil, want: false},
		"only the cut one, still":   {live: []domain.ConnID{7}, want: false},
		"the redial beside the cut": {live: []domain.ConnID{7, 9}, want: true},
		"only the redial":           {live: []domain.ConnID{9}, want: true},
	}
	for name, c := range cases {
		if got := loadStandHoldsSessionOutside(c.live, cut); got != c.want {
			t.Errorf("%s: loadStandHoldsSessionOutside(%v) = %v, want %v", name, c.live, got, c.want)
		}
	}
}

// loadStandHoldsSessionOutside reports whether live holds a session other
// than those in cut.
func loadStandHoldsSessionOutside(live []domain.ConnID, cut map[domain.ConnID]struct{}) bool {
	for _, id := range live {
		if _, wasCut := cut[id]; !wasCut {
			return true
		}
	}
	return false
}

// syntheticLoadStandEnded is node's life ending endOffset into it with final
// counters.
func syntheticLoadStandEnded(node loadStandNodeID, life loadStandLife, endOffset time.Duration, final loadStandTrafficTotals) loadStandEndedIncarnation {
	startedAt := loadStandAt(time.Duration(life) * time.Hour)
	return loadStandEndedIncarnation{
		Node: node,
		Life: life,
		Final: domain.TransportTrafficStats{
			StartedAt: startedAt, ReadAt: startedAt.Add(endOffset), BytesSent: uint64(final.Sent), BytesReceived: uint64(final.Received),
		},
	}
}

// loadStandFlapFixture: the full node runs throughout (life 1); the edge is
// sampled in life 1, stops, lives a whole life 2 inside the span, and is
// sampled again in life 3. Each node life starts on its hour; the earlier
// sweep reads 10 min into hour 1, the later 10 min into hour 3.
//
//	full   life 1: 100/100 → 1 100/1 050            +1 000 sent  +950 received
//	edge   life 1: 100/200 → final 400/600           +300         +400
//	       life 2: final 250/300 (from zero)         +250         +300
//	       life 3: 400/300 at the later sweep         +400         +300
//
// Σ sent = Σ received = 1 950, and no life balances on its own — leaving
// any of them out shows.
func loadStandFlapFixture() (loadStandSweep, loadStandSweep, []loadStandEndedIncarnation) {
	earlier := syntheticLoadStandSweepOf(
		syntheticLoadStandSample("full-000", 1, 10*time.Minute, loadStandTrafficTotals{Sent: 100, Received: 100}),
		syntheticLoadStandSample("edge-000", 1, 10*time.Minute, loadStandTrafficTotals{Sent: 100, Received: 200}),
	)
	later := syntheticLoadStandSweepOf(
		syntheticLoadStandSample("full-000", 1, 2*time.Hour+10*time.Minute, loadStandTrafficTotals{Sent: 1_100, Received: 1_050}),
		syntheticLoadStandSample("edge-000", 3, 10*time.Minute, loadStandTrafficTotals{Sent: 400, Received: 300}),
	)
	ended := []loadStandEndedIncarnation{
		syntheticLoadStandEnded("edge-000", 1, 50*time.Minute, loadStandTrafficTotals{Sent: 400, Received: 600}),
		syntheticLoadStandEnded("edge-000", 2, 50*time.Minute, loadStandTrafficTotals{Sent: 250, Received: 300}),
	}
	return earlier, later, ended
}

// Under flap a node restarts inside every span; the balance sums its bytes
// over the lives the span holds instead of giving up. Without a final
// reading for a life that ended inside the span, it does give up.
func testLoadStandByteBalanceSumsLivesAcrossAFlap(t *testing.T) {
	earlier, later, ended := loadStandFlapFixture()
	exact := loadStandBalanceTolerance{MaxAllowanceShare: 1}
	balance, err := checkLoadStandByteBalance(earlier, later, ended, exact)
	if err != nil {
		t.Fatalf("a balanced flap: %v", err)
	}
	if balance.Sent != 1_950 || balance.Received != 1_950 || balance.Stops != 2 || balance.Nodes != 2 {
		t.Fatalf("balance %+v, want 1950 both ways over 2 nodes and 2 stops", balance)
	}

	// The edge stopped for good before the later sweep: its last life ends
	// in a final reading instead of a sample.
	gone := syntheticLoadStandSweepOf(later.Nodes[0])
	goneEnded := append(slices.Clone(ended), syntheticLoadStandEnded("edge-000", 3, 5*time.Minute, loadStandTrafficTotals{Sent: 400, Received: 300}))
	if _, err := checkLoadStandByteBalance(earlier, gone, goneEnded, exact); err != nil {
		t.Errorf("an edge that stopped for good inside the span: %v", err)
	}

	// A node that started inside the span counts from zero.
	born := syntheticLoadStandSweepOf(append(slices.Clone(later.Nodes), syntheticLoadStandSample("edge-001", 3, 10*time.Minute, loadStandTrafficTotals{}))...)
	if _, err := checkLoadStandByteBalance(earlier, born, ended, exact); err != nil {
		t.Errorf("a node born inside the span with nothing carried: %v", err)
	}

	undefined := map[string][]loadStandEndedIncarnation{
		"a life ended without a final reading": ended[:1],
		"no final readings at all":             nil,
		"a life recorded twice":                append(slices.Clone(ended), ended[1]),
		"a final read after the later sweep began": {
			ended[0], syntheticLoadStandEnded("edge-000", 2, 2*time.Hour, loadStandTrafficTotals{Sent: 250, Received: 300}),
		},
	}
	otherStart := slices.Clone(ended)
	otherStart[0].Final.StartedAt = otherStart[0].Final.StartedAt.Add(time.Second)
	undefined["a final reading of another start of the life"] = otherStart
	fell := slices.Clone(ended)
	fell[0] = syntheticLoadStandEnded("edge-000", 1, 50*time.Minute, loadStandTrafficTotals{Sent: 50, Received: 600})
	undefined["a life whose counter fell"] = fell
	for name, lives := range undefined {
		_, err := checkLoadStandByteBalance(earlier, later, lives, exact)
		if !errors.Is(err, errLoadStandBalanceUndefined) || errors.Is(err, errLoadStandDefect) {
			t.Errorf("%s: %v, want errLoadStandBalanceUndefined", name, err)
		}
	}

	// A life that over-reports what it sent is a defect, flap or not.
	inflated := slices.Clone(ended)
	inflated[1] = syntheticLoadStandEnded("edge-000", 2, 50*time.Minute, loadStandTrafficTotals{Sent: 900, Received: 300})
	var defect *loadStandDefect
	if _, err := checkLoadStandByteBalance(earlier, later, inflated, exact); !errors.As(err, &defect) || defect.Kind != loadStandDefectByteBalance {
		t.Errorf("an inflated life: %v, want a byte_balance defect", err)
	}
}

// Every stop strands what was on its way to the stopping node, and a stop
// read before it happened carries its tail: the flap fixture with in-flight
// 10 per node running at the later edge (2) and per stop (2), and a tail of
// 5 on one stop, allows 45.
func testLoadStandByteBalanceAllowsForEveryStop(t *testing.T) {
	tolerance := loadStandBalanceTolerance{PerNodeInFlight: 10, MaxAllowanceShare: 1}
	for received, holds := range map[loadStandByteCount]bool{1_005: true, 1_004: false} {
		earlier, later, ended := loadStandFlapFixture()
		ended[0].TailAllowance = 5
		later.Nodes[0].Traffic.BytesReceived = uint64(received)
		balance, err := checkLoadStandByteBalance(earlier, later, ended, tolerance)
		if balance.Allowance != 45 {
			t.Errorf("received %d: allowance %d, want 45", received, balance.Allowance)
		}
		if gotDefect := errors.Is(err, errLoadStandDefect); gotDefect == holds || (holds && err != nil) {
			t.Errorf("received %d (imbalance %d): %v, want holds=%v", received, balance.Imbalance, err, holds)
		}
	}
}

// A sweep of no node has no edge to measure from: undefined, not a panic.
func testLoadStandByteBalanceUndefinedOverEmptySweeps(t *testing.T) {
	someone := syntheticLoadStandSweepOf(syntheticLoadStandSample("full-000", 1, time.Minute, loadStandTrafficTotals{}))
	for name, pair := range map[string][2]loadStandSweep{
		"both empty":    {{}, {}},
		"earlier empty": {{}, someone},
		"later empty":   {someone, {}},
	} {
		if _, err := checkLoadStandByteBalance(pair[0], pair[1], nil, loadStandDefaultBalanceTolerance()); !errors.Is(err, errLoadStandBalanceUndefined) {
			t.Errorf("%s: %v, want errLoadStandBalanceUndefined", name, err)
		}
	}
}

// The ended lives the stand hands the balance are its WHOLE history: lives
// that ended before the span and the life sampled at the later edge, which
// ended after it, are in the list too. The balance takes from it exactly the
// lives inside the span — a life ended before the span is not counted, and
// the life sampled at the later edge is counted to that sample, not to a
// final reading taken after the span.
//
//	full  life 1: 1 000/1 000 → 1 750/1 650              +750 sent  +650 received
//	edge  life 1: ended before the span (999/999)        not counted
//	      life 2: 100/100 → final 300/400 inside         +200       +300
//	      life 3: final 150/250 inside (from zero)        +150       +250
//	      life 4: 300/200 at the later sweep (final 500/500 after it)  +300  +200
//
// Σ sent = Σ received = 1 400.
func testLoadStandByteBalanceReadsTheWholeLifeHistory(t *testing.T) {
	earlier := syntheticLoadStandSweepOf(
		syntheticLoadStandSample("full-000", 1, time.Hour+10*time.Minute, loadStandTrafficTotals{Sent: 1_000, Received: 1_000}),
		syntheticLoadStandSample("edge-000", 2, 10*time.Minute, loadStandTrafficTotals{Sent: 100, Received: 100}),
	)
	later := syntheticLoadStandSweepOf(
		syntheticLoadStandSample("full-000", 1, 3*time.Hour+10*time.Minute, loadStandTrafficTotals{Sent: 1_750, Received: 1_650}),
		syntheticLoadStandSample("edge-000", 4, 10*time.Minute, loadStandTrafficTotals{Sent: 300, Received: 200}),
	)
	history := []loadStandEndedIncarnation{
		syntheticLoadStandEnded("edge-000", 1, 50*time.Minute, loadStandTrafficTotals{Sent: 999, Received: 999}),
		syntheticLoadStandEnded("edge-000", 2, 50*time.Minute, loadStandTrafficTotals{Sent: 300, Received: 400}),
		syntheticLoadStandEnded("edge-000", 3, 50*time.Minute, loadStandTrafficTotals{Sent: 150, Received: 250}),
		syntheticLoadStandEnded("edge-000", 4, 50*time.Minute, loadStandTrafficTotals{Sent: 500, Received: 500}),
	}
	balance, err := checkLoadStandByteBalance(earlier, later, history, loadStandBalanceTolerance{MaxAllowanceShare: 1})
	if err != nil {
		t.Fatalf("a balanced span inside a longer history: %v", err)
	}
	if balance.Sent != 1_400 || balance.Received != 1_400 || balance.Stops != 2 {
		t.Fatalf("balance %+v, want 1400 both ways over the 2 lives that ended inside the span", balance)
	}
}

// loadStandStopAttempts is how many stops must see bytes arrive while they
// ran before the test believes it exercised the final reading at all.
const loadStandStopAttempts = 3

// The final reading of a life is taken AFTER its stop: bytes the node still
// read while stopping are in it. The full node writes to the edge throughout
// the edge's Stop; a stop whose counters grew while it ran must have a final
// reading equal to the counters after it — a reading taken when Stop began
// would miss that growth. Stops that happened to see no growth prove
// nothing and are repeated.
func testLoadStandFinalReadingIsTakenAfterTheStop(t *testing.T) {
	ports := newLoadStandPortRegistry(reserveLoopbackAddress)
	full := newFullNodeForTest(t, ports, "full-000", loopbackOpts())
	address, _ := full.DialAddress()
	edgeOpts := loopbackOpts()
	edgeOpts.BootstrapPeers = []domain.PeerAddress{address}
	edge := newEdgeNodeForTest(t, "edge-000", edgeOpts)
	startLoadStandNodeForTest(t, full)
	startLoadStandNodeForTest(t, edge)

	deadline := time.Now().Add(3 * loadStandSampleWait)
	for grown := 0; grown < loadStandStopAttempts; {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d stops saw bytes arrive while they ran", grown, loadStandStopAttempts)
		}
		awaitLoadStandRedial(t, edge, nil)
		stopping := runningServiceForTest(t, edge)
		stopFlood := floodLoadStandInbound(t, runningServiceForTest(t, full))
		before := stopping.TransportTrafficStats()
		stopLoadStandNodeForTest(t, edge)
		after := stopping.TransportTrafficStats()
		stopFlood()

		ended := edge.EndedIncarnations()
		final := ended[len(ended)-1].Final
		if final.BytesSent != after.BytesSent || final.BytesReceived != after.BytesReceived {
			t.Fatalf("final reading %d/%d, counters after the stop %d/%d (before it %d/%d)",
				final.BytesSent, final.BytesReceived, after.BytesSent, after.BytesReceived, before.BytesSent, before.BytesReceived)
		}
		if after.BytesReceived > before.BytesReceived {
			grown++
		}
		if err := edge.Start(t.Context()); err != nil {
			t.Fatalf("restart edge: %v", err)
		}
	}
}

// floodLoadStandInbound has svc write pings to its inbound connections as
// fast as the writers take them, until the returned stop is called, which
// waits for the writer to finish. A stop closes the edge's sockets in well
// under a millisecond, so only a continuous stream is reliably arriving
// while it does.
func floodLoadStandInbound(t *testing.T, svc *Service) func() {
	t.Helper()

	ping := []byte("{\"type\":\"ping\"}\n")
	ids := loadStandInboundConnIDs(svc)
	if len(ids) == 0 {
		t.Fatal("the full node has no inbound connection to flood")
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-done:
				return
			default:
			}
			for _, id := range ids {
				// A send to a connection the stopping edge just closed, or to
				// a queue the flood has filled, fails; that is the point of
				// the flood, not a fault in it.
				_ = svc.Network().SendFrame(context.Background(), id, ping)
			}
			runtime.Gosched()
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}

func loadStandInboundConnIDs(svc *Service) []domain.ConnID {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	var ids []domain.ConnID
	svc.forEachInboundConnIDLocked(func(id domain.ConnID, _ domain.PeerAddress, _ bool) bool {
		ids = append(ids, id)
		return true
	})
	return ids
}
