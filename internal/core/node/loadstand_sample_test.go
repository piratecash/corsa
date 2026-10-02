package node

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"runtime/metrics"
	"slices"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/testutil/runjournal"
)

// A load-stand sample is what one node, or the process hosting every node,
// says about itself at one instant. A sample alone is a cumulative number
// whose period is "since this incarnation started"; what the stand reports is
// a WINDOW — the difference of two samples — and a window is admitted only
// under the pair rules of docs/refactoring/dht/05-rollout-metrics.md §5.1.1:
// the same incarnation at both edges, the later read strictly after the
// earlier, and every counter family still in the period it began with. Each
// family's window spans that family's own read_at pair, not the node's.
//
// Samples are taken by the stand's one goroutine. Nothing here panics or
// fails a test: a sample that contradicts the stand's own invariants is
// returned as a *loadStandDefect for the owner to record.

var (
	errLoadStandSampleUnreadable = errors.New("load stand sample: a source did not answer")

	errLoadStandWindowOtherNode          = errors.New("load stand window: the two samples are of different nodes")
	errLoadStandWindowAcrossIncarnations = errors.New("load stand window: the two samples are of different incarnations")
	errLoadStandWindowNotForward         = errors.New("load stand window: the later sample was not read after the earlier one")
	errLoadStandWindowCountersReset      = errors.New("load stand window: a counter family restarted within one incarnation")
	errLoadStandWindowUnboundedCounters  = errors.New("load stand window: a counter family does not say which period it covers")
	// errLoadStandWindowDatagramOneEdge: the datagram plane answered at one
	// edge and not the other. The plane is fixed by configuration for an
	// incarnation, so this is a source that failed, not a counter reset.
	errLoadStandWindowDatagramOneEdge = errors.New("load stand window: the datagram plane answered at one edge only")
	errLoadStandCounterDecreased      = errors.New("load stand window: a cumulative counter decreased")

	// errLoadStandBalanceUndefined is not a defect: the balance is a
	// statement about the whole stand over one interval, and an interval in
	// which some node restarted, stopped, started or rearranged its ledger —
	// or one the allowance would make blind — has no such statement.
	errLoadStandBalanceUndefined = errors.New("load stand byte balance: undefined for these two sweeps")

	errLoadStandDefect                   = errors.New("load stand defect")
	errLoadStandAggregateInvalid         = errors.New("load stand aggregate: a value has no role group")
	errLoadStandRuntimeMetricUnsupported = errors.New("runtime metric is not supported by this Go release")
)

// loadStandByteCount is an amount of memory or traffic in bytes.
type loadStandByteCount uint64

// loadStandEventCount is how many times something happened.
type loadStandEventCount uint64

// loadStandGoroutineCount is how many goroutines exist.
type loadStandGoroutineCount uint64

// loadStandProcCount is a number of logical processors (GOMAXPROCS).
type loadStandProcCount int

// loadStandLife numbers the incarnations of one stand node: 1 for the first
// Start, 2 after the first restart, and so on.
type loadStandLife int

// loadStandIncarnationID is one life of one node. The life number is the
// stand's count; StartedAt is the Service's own (every counter family of
// the Service starts there), so both the stand and the node agree that a
// restart happened.
type loadStandIncarnationID struct {
	Life      loadStandLife
	StartedAt time.Time
}

func (id loadStandIncarnationID) sameAs(other loadStandIncarnationID) bool {
	return id.Life == other.Life && id.StartedAt.Equal(other.StartedAt)
}

// loadStandPeriod is the interval a window describes.
type loadStandPeriod struct {
	From time.Time
	To   time.Time
}

func (p loadStandPeriod) Length() time.Duration { return p.To.Sub(p.From) }

// loadStandReading is a value a source may decline to give — a platform
// without getrusage, a node without the datagram plane. The refusal is kept beside the absent value, so
// "not measured" never reads as zero.
type loadStandReading[T any] struct {
	value T
	err   error
}

func loadStandReadingOf[T any](value T) loadStandReading[T] {
	return loadStandReading[T]{value: value}
}

func loadStandReadingRefused[T any](err error) loadStandReading[T] {
	return loadStandReading[T]{err: err}
}

func (r loadStandReading[T]) Get() (T, error) { return r.value, r.err }

// --- node sample ---

type loadStandTrafficTotals struct {
	Sent     loadStandByteCount
	Received loadStandByteCount
}

// loadStandDatagramSample is the part of FetchDatagramSummary the stand
// reads: the pipeline counters with their period, and the reverse table's
// local quota.
type loadStandDatagramSample struct {
	Metrics datagram.MetricsSnapshot    `json:"metrics"`
	Reverse datagram.ReverseDiagnostics `json:"reverse"`
}

// loadStandNodeSample is one node's state at ReadAt. Readiness is
// Neighbours.Connections together with RouteEntries; Bans are the findings
// the policy treats as invalidating, and their presence is a defect.
type loadStandNodeSample struct {
	Node        loadStandNodeID
	Role        loadStandRole
	Incarnation loadStandIncarnationID
	// ReadAt is stamped when the last source answered — the closing edge of
	// what the sample describes.
	ReadAt       time.Time
	Traffic      domain.TransportTrafficStats
	Datagram     loadStandReading[loadStandDatagramSample]
	Sessions     domain.SessionOutcomeStats
	Modes        routing.ModeSelectionStats
	Neighbours   domain.NeighbourComposition
	RouteEntries int
	Resources    domain.ResourceBreakdown
	Bans         []loadStandBanFinding
}

// loadStandNodeSource is where a node's sample is read from. The sample does
// not care whether the node runs in this process: in-process it is the
// Service itself (loadStandServiceSource); a stand whose nodes run as
// separate processes — which the owner requires for acceptance measurements
// (decision №36) — implements it over RPC (fetchRouteSummary,
// getResourceBreakdown, fetchDatagramSummary). Every method may therefore do
// I/O, so each takes a context and can fail.
//
// What an RPC source can serve TODAY (input to T5, recorded so it is not
// rediscovered):
//
//   - TransportTraffic, SessionOutcomes, Neighbours, RouteEntries — yes,
//     fetchRouteSummary carries them (transport_traffic among them).
//   - DatagramSummary — yes, fetchDatagramSummary is the same JSON this
//     in-process source decodes.
//   - ModeSelection — partly: the RPC renders the enums with String() and
//     nothing parses them back, so a source needs its own name → enum map.
//   - Resources — partly: getResourceBreakdown answers, but
//     domain.ResourceBreakdown has no UnmarshalJSON; the source decodes into
//     a stand DTO of its own.
//   - Bans — partly: no RPC exposes ban_score / blacklist, the IP-wide
//     remote ban or the per-peer remote ban; the check is weaker over RPC
//     until one does.
//   - The process sample — no RPC at all. For a node in its own process it
//     is read from outside: /proc/<pid> on Linux, proc_pid_rusage and
//     proc_pidinfo on darwin.
//   - The floor check then compares each node's floors with that node's own
//     heap, not a shared one.
//   - A life's final transport reading can only be taken before its stop;
//     the source states the stop's tail as
//     loadStandEndedIncarnation.TailAllowance.
type loadStandNodeSource interface {
	// TransportTraffic also carries the incarnation's start: its StartedAt is
	// the node's own, the instant every counter family begins.
	TransportTraffic(ctx context.Context) (domain.TransportTrafficStats, error)
	// DatagramSummary refuses with errDatagramNotEnabled — as a reading, not
	// an error — when the node runs without the plane.
	DatagramSummary(ctx context.Context) (loadStandReading[loadStandDatagramSample], error)
	SessionOutcomes(ctx context.Context) (domain.SessionOutcomeStats, error)
	ModeSelection(ctx context.Context) (routing.ModeSelectionStats, error)
	Neighbours(ctx context.Context) (domain.NeighbourComposition, error)
	RouteEntries(ctx context.Context) (int, error)
	Resources(ctx context.Context) (domain.ResourceBreakdown, error)
	Bans(ctx context.Context, now time.Time, policy loadStandBanPolicy) ([]loadStandBanFinding, error)
}

// loadStandSampleSubject is what the stand knows about the node it samples;
// everything else comes from the node.
type loadStandSampleSubject struct {
	Node loadStandNodeID
	Role loadStandRole
	Life loadStandLife
}

// takeLoadStandNodeSample reads every source in turn and stamps the sample
// when the last one answered. A source that fails fails the sample: a sample
// missing a family would make every window built on it silently partial.
func takeLoadStandNodeSample(ctx context.Context, clock runjournal.Clock, subject loadStandSampleSubject, source loadStandNodeSource, policy loadStandBanPolicy) (loadStandNodeSample, error) {
	var errs []error
	collect := func(err error) { errs = append(errs, err) }

	traffic, err := source.TransportTraffic(ctx)
	collect(err)
	datagramSample, err := source.DatagramSummary(ctx)
	collect(err)
	sessions, err := source.SessionOutcomes(ctx)
	collect(err)
	modes, err := source.ModeSelection(ctx)
	collect(err)
	neighbours, err := source.Neighbours(ctx)
	collect(err)
	routeEntries, err := source.RouteEntries(ctx)
	collect(err)
	resources, err := source.Resources(ctx)
	collect(err)
	bans, err := source.Bans(ctx, clock.Now(), policy)
	collect(err)
	if err := errors.Join(errs...); err != nil {
		return loadStandNodeSample{}, fmt.Errorf("%w: %s: %w", errLoadStandSampleUnreadable, subject.Node, err)
	}
	return loadStandNodeSample{
		Node:         subject.Node,
		Role:         subject.Role,
		Incarnation:  loadStandIncarnationID{Life: subject.Life, StartedAt: traffic.StartedAt},
		ReadAt:       clock.Now(),
		Traffic:      traffic,
		Datagram:     datagramSample,
		Sessions:     sessions,
		Modes:        modes,
		Neighbours:   neighbours,
		RouteEntries: routeEntries,
		Resources:    resources,
		Bans:         bans,
	}, nil
}

// Sample reads the running incarnation through its Service.
func (n *loadStandNode) Sample(ctx context.Context, clock runjournal.Clock, policy loadStandBanPolicy) (loadStandNodeSample, error) {
	if n.current == nil {
		return loadStandNodeSample{}, fmt.Errorf("load stand node %s: %w", n.id, errLoadStandNodeNotRunning)
	}
	subject := loadStandSampleSubject{Node: n.id, Role: n.role, Life: n.current.life}
	return takeLoadStandNodeSample(ctx, clock, subject, loadStandServiceSource{svc: n.current.running.svc}, policy)
}

// loadStandServiceSource reads a Service of this process through its own
// readers. They are not all lock-free: the breakdown takes each domain read
// lock in turn and the ban check reads under peerMu and ipStateMu. None
// holds one lock across another, so sampling adds read contention on those
// mutexes — a writer queued behind it waits, and with it every new reader —
// and nothing more. The context is unused: nothing here waits on I/O.
type loadStandServiceSource struct {
	svc *Service
}

func (s loadStandServiceSource) TransportTraffic(context.Context) (domain.TransportTrafficStats, error) {
	return s.svc.TransportTrafficStats(), nil
}

func (s loadStandServiceSource) DatagramSummary(context.Context) (loadStandReading[loadStandDatagramSample], error) {
	return readLoadStandDatagram(s.svc)
}

func (s loadStandServiceSource) SessionOutcomes(context.Context) (domain.SessionOutcomeStats, error) {
	return s.svc.SessionOutcomeStats(), nil
}

func (s loadStandServiceSource) ModeSelection(context.Context) (routing.ModeSelectionStats, error) {
	return s.svc.ModeSelectionStats(), nil
}

func (s loadStandServiceSource) Neighbours(context.Context) (domain.NeighbourComposition, error) {
	return s.svc.NeighbourComposition(), nil
}

func (s loadStandServiceSource) RouteEntries(context.Context) (int, error) {
	return s.svc.RoutingSnapshot().TotalEntries, nil
}

func (s loadStandServiceSource) Resources(context.Context) (domain.ResourceBreakdown, error) {
	return s.svc.ResourceBreakdown(), nil
}

func (s loadStandServiceSource) Bans(_ context.Context, now time.Time, policy loadStandBanPolicy) ([]loadStandBanFinding, error) {
	return invalidatingLoadStandBanFindings(s.svc, now, policy), nil
}

// readLoadStandDatagram reads the summary through its JSON form, the one an
// operator gets, so the stand measures what the RPC reports.
func readLoadStandDatagram(svc *Service) (loadStandReading[loadStandDatagramSample], error) {
	raw, err := svc.FetchDatagramSummary()
	switch {
	case errors.Is(err, errDatagramNotEnabled):
		return loadStandReadingRefused[loadStandDatagramSample](err), nil
	case err != nil:
		return loadStandReading[loadStandDatagramSample]{}, fmt.Errorf("%w: datagram summary: %w", errLoadStandSampleUnreadable, err)
	}
	var summary loadStandDatagramSample
	if err := json.Unmarshal(raw, &summary); err != nil {
		return loadStandReading[loadStandDatagramSample]{}, fmt.Errorf("%w: decode datagram summary: %w", errLoadStandSampleUnreadable, err)
	}
	return loadStandReadingOf(summary), nil
}

// --- process sample ---

type loadStandRusage struct {
	UserCPU   time.Duration
	SystemCPU time.Duration
	MaxRSS    loadStandByteCount
}

// loadStandProcessSample is the process hosting every node.
//
// HeapLive is the live heap as the PREVIOUS collection marked it; HeapObjects
// is what the heap holds now — live objects AND garbage not yet swept. The
// floor check compares against HeapObjects, which makes it a COARSE upper
// bound: a live figure from the last GC can lag behind memory a node acquired
// since, so it cannot be used, and the price of the one that can is that
// unswept garbage widens the bound. The check catches floors that are not
// floors by a margin, not by a byte.
type loadStandProcessSample struct {
	ReadAt      time.Time
	HeapLive    loadStandByteCount
	HeapObjects loadStandByteCount
	HeapAllocs  loadStandByteCount
	GoMemory    loadStandByteCount
	GCCycles    loadStandEventCount
	Goroutines  loadStandGoroutineCount
	// CPUTotal is the CPU time AVAILABLE to the process (GOMAXPROCS × wall
	// time), not CPU spent; spent is CPUTotal − CPUIdle, of which CPUUser
	// ran Go code and CPUGC collected. The runtime brings these up to date
	// only when a GC cycle completes — before the first one they are zero —
	// so they describe the last GC, not ReadAt, and nothing here compares
	// them with the kernel's count.
	CPUTotal time.Duration
	CPUUser  time.Duration
	CPUGC    time.Duration
	CPUIdle  time.Duration
	// Procs is GOMAXPROCS when the sample was read: with ReadAt, it is the
	// stand's own measure of the CPU the process could use.
	Procs       loadStandProcCount
	Rusage      loadStandReading[loadStandRusage]
	Descriptors loadStandReading[int]
}

// loadStandRuntimeMetric is one runtime metric and the sample field it
// feeds. field names the destination as a value, so which metric feeds which
// field is a fact a test can read, not only a statement inside a closure.
type loadStandRuntimeMetric struct {
	name  string
	kind  metrics.ValueKind
	field func(*loadStandProcessSample) any
	store func(*loadStandProcessSample, metrics.Value)
}

// loadStandCountMetric feeds an unsigned runtime metric into a count field.
func loadStandCountMetric[T ~uint64](name string, field func(*loadStandProcessSample) *T) loadStandRuntimeMetric {
	return loadStandRuntimeMetric{
		name:  name,
		kind:  metrics.KindUint64,
		field: func(s *loadStandProcessSample) any { return field(s) },
		store: func(s *loadStandProcessSample, v metrics.Value) { *field(s) = T(v.Uint64()) },
	}
}

// loadStandCPUMetric feeds a cpu-seconds runtime metric into a duration.
func loadStandCPUMetric(name string, field func(*loadStandProcessSample) *time.Duration) loadStandRuntimeMetric {
	return loadStandRuntimeMetric{
		name:  name,
		kind:  metrics.KindFloat64,
		field: func(s *loadStandProcessSample) any { return field(s) },
		store: func(s *loadStandProcessSample, v metrics.Value) { *field(s) = loadStandCPUSeconds(v.Float64()) },
	}
}

var loadStandRuntimeMetrics = []loadStandRuntimeMetric{
	loadStandCountMetric("/gc/heap/live:bytes", func(s *loadStandProcessSample) *loadStandByteCount { return &s.HeapLive }),
	loadStandCountMetric("/memory/classes/heap/objects:bytes", func(s *loadStandProcessSample) *loadStandByteCount { return &s.HeapObjects }),
	loadStandCountMetric("/gc/heap/allocs:bytes", func(s *loadStandProcessSample) *loadStandByteCount { return &s.HeapAllocs }),
	loadStandCountMetric("/memory/classes/total:bytes", func(s *loadStandProcessSample) *loadStandByteCount { return &s.GoMemory }),
	loadStandCountMetric("/gc/cycles/total:gc-cycles", func(s *loadStandProcessSample) *loadStandEventCount { return &s.GCCycles }),
	loadStandCountMetric("/sched/goroutines:goroutines", func(s *loadStandProcessSample) *loadStandGoroutineCount { return &s.Goroutines }),
	loadStandCPUMetric("/cpu/classes/total:cpu-seconds", func(s *loadStandProcessSample) *time.Duration { return &s.CPUTotal }),
	loadStandCPUMetric("/cpu/classes/user:cpu-seconds", func(s *loadStandProcessSample) *time.Duration { return &s.CPUUser }),
	loadStandCPUMetric("/cpu/classes/gc/total:cpu-seconds", func(s *loadStandProcessSample) *time.Duration { return &s.CPUGC }),
	loadStandCPUMetric("/cpu/classes/idle:cpu-seconds", func(s *loadStandProcessSample) *time.Duration { return &s.CPUIdle }),
}

func loadStandCPUSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// takeLoadStandProcessSample reads the runtime, then the platform. A runtime
// metric this release does not know is an error — the stand was written
// against it — while a platform reading that is refused is kept as a refusal.
func takeLoadStandProcessSample(clock runjournal.Clock) (loadStandProcessSample, error) {
	var sample loadStandProcessSample
	if err := readLoadStandRuntimeMetrics(&sample, loadStandRuntimeMetrics); err != nil {
		return loadStandProcessSample{}, err
	}
	usage, usageErr := readLoadStandRusage()
	// ReadAt pairs with the kernel's CPU count: the CPU window divides one by
	// the other, so nothing slower — the descriptor listing — goes between.
	sample.ReadAt = clock.Now()
	sample.Rusage = loadStandReading[loadStandRusage]{value: usage, err: usageErr}
	sample.Procs = loadStandProcCount(runtime.GOMAXPROCS(0))
	descriptors, descriptorsErr := openDescriptorCount()
	sample.Descriptors = loadStandReading[int]{value: descriptors, err: descriptorsErr}
	return sample, nil
}

// readLoadStandRuntimeMetrics reads table into sample. The table is a
// parameter so the refusal of an unknown metric can be exercised.
func readLoadStandRuntimeMetrics(sample *loadStandProcessSample, table []loadStandRuntimeMetric) error {
	samples := make([]metrics.Sample, len(table))
	for i, metric := range table {
		samples[i].Name = metric.name
	}
	metrics.Read(samples)
	for i, metric := range table {
		if samples[i].Value.Kind() != metric.kind {
			return fmt.Errorf("%w: %s", errLoadStandRuntimeMetricUnsupported, metric.name)
		}
		metric.store(sample, samples[i].Value)
	}
	return nil
}

// --- sweep ---

// loadStandSweep is every running node sampled one after another between
// Begin and End, and the process sampled right after: the heap it reports
// then includes everything the node samples reported.
type loadStandSweep struct {
	Begin   time.Time
	End     time.Time
	Nodes   []loadStandNodeSample
	Process loadStandProcessSample
}

// takeLoadStandSweep samples the nodes that are running; a stopped node has
// nothing to say and is absent from the sweep rather than present as zeros.
func takeLoadStandSweep(ctx context.Context, clock runjournal.Clock, nodes []*loadStandNode, policy loadStandBanPolicy) (loadStandSweep, error) {
	sweep := loadStandSweep{Begin: clock.Now()}
	for _, node := range nodes {
		if _, running := node.Service(); !running {
			continue
		}
		sample, err := node.Sample(ctx, clock, policy)
		if err != nil {
			return loadStandSweep{}, err
		}
		sweep.Nodes = append(sweep.Nodes, sample)
	}
	sweep.End = clock.Now()
	process, err := takeLoadStandProcessSample(clock)
	if err != nil {
		return loadStandSweep{}, err
	}
	sweep.Process = process
	return sweep, nil
}

// --- windows ---

type loadStandSessionDelta struct {
	Attempts      loadStandEventCount
	Succeeded     loadStandEventCount
	ErrorsConnect loadStandEventCount
	ErrorsCompat  loadStandEventCount
	ErrorsOther   loadStandEventCount
}

type loadStandSessionWindow struct {
	Period loadStandPeriod
	Delta  loadStandSessionDelta
}

type loadStandModeKey struct {
	Operation routing.AnnounceOperation
	Mode      routing.AnnounceMode
	Reason    routing.ModeReason
}

type loadStandModeWindow struct {
	Period loadStandPeriod
	Counts map[loadStandModeKey]loadStandEventCount
}

type loadStandDatagramDelta struct {
	Period        loadStandPeriod
	Observed      loadStandEventCount
	Accepted      loadStandEventCount
	Dropped       loadStandEventCount
	UnknownDType  loadStandEventCount
	DropsByReason map[string]loadStandEventCount
	SendRefusals  map[string]loadStandEventCount
	LocalRefusals map[string]loadStandEventCount
}

// loadStandNodeWindow is what one node did between two samples of one
// incarnation. Period is the node samples' own edges; every family carries
// its own period, and a rate is that family's delta over its own period.
type loadStandNodeWindow struct {
	Node        loadStandNodeID
	Role        loadStandRole
	Incarnation loadStandIncarnationID
	Period      loadStandPeriod
	Bytes       loadStandByteWindow
	Sessions    loadStandSessionWindow
	Modes       loadStandModeWindow
	Datagram    loadStandReading[loadStandDatagramDelta]
}

// newLoadStandNodeWindow subtracts from from to, or refuses the pair.
func newLoadStandNodeWindow(from, to loadStandNodeSample) (loadStandNodeWindow, error) {
	if err := checkLoadStandWindowEdges(from, to); err != nil {
		return loadStandNodeWindow{}, err
	}
	deltas := loadStandDeltaReader{node: to.Node}
	window := loadStandNodeWindow{
		Node:        to.Node,
		Role:        to.Role,
		Incarnation: to.Incarnation,
		Period:      loadStandPeriod{From: from.ReadAt, To: to.ReadAt},
		Bytes:       deltas.bytes(from.Traffic, to.Traffic),
		Sessions: loadStandSessionWindow{
			Period: loadStandPeriod{From: from.Sessions.ReadAt, To: to.Sessions.ReadAt},
			Delta:  deltas.sessions(from.Sessions, to.Sessions),
		},
		Modes: loadStandModeWindow{
			Period: loadStandPeriod{From: from.Modes.ReadAt, To: to.Modes.ReadAt},
			Counts: deltas.modes(from.Modes, to.Modes),
		},
	}
	datagramDelta, err := deltas.datagram(from.Datagram, to.Datagram)
	if err != nil {
		return loadStandNodeWindow{}, err
	}
	window.Datagram = datagramDelta
	if deltas.err != nil {
		return loadStandNodeWindow{}, deltas.err
	}
	return window, nil
}

func checkLoadStandWindowEdges(from, to loadStandNodeSample) error {
	switch {
	case from.Node != to.Node:
		return fmt.Errorf("%w: %s and %s", errLoadStandWindowOtherNode, from.Node, to.Node)
	case !from.Incarnation.sameAs(to.Incarnation):
		return fmt.Errorf("%w: %s life %d started %s, then life %d started %s", errLoadStandWindowAcrossIncarnations,
			to.Node, from.Incarnation.Life, from.Incarnation.StartedAt.Format(time.RFC3339Nano),
			to.Incarnation.Life, to.Incarnation.StartedAt.Format(time.RFC3339Nano))
	case !to.ReadAt.After(from.ReadAt):
		return fmt.Errorf("%w: %s read at %s, then at %s", errLoadStandWindowNotForward,
			to.Node, from.ReadAt.Format(time.RFC3339Nano), to.ReadAt.Format(time.RFC3339Nano))
	}
	for _, period := range []loadStandCounterPeriod{
		{family: "transport_traffic", startedFrom: from.Traffic.StartedAt, startedTo: to.Traffic.StartedAt, readFrom: from.Traffic.ReadAt, readTo: to.Traffic.ReadAt},
		{family: "session_outcomes", startedFrom: from.Sessions.StartedAt, startedTo: to.Sessions.StartedAt, readFrom: from.Sessions.ReadAt, readTo: to.Sessions.ReadAt},
		{family: "mode_selection", startedFrom: from.Modes.StartedAt, startedTo: to.Modes.StartedAt, readFrom: from.Modes.ReadAt, readTo: to.Modes.ReadAt},
	} {
		if err := period.check(to.Node); err != nil {
			return err
		}
	}
	return nil
}

// loadStandCounterPeriod is one counter family's own period at the two
// edges; §5.1.1 asks both conditions of every family, not only of the node.
type loadStandCounterPeriod struct {
	family      string
	startedFrom time.Time
	startedTo   time.Time
	readFrom    time.Time
	readTo      time.Time
}

func (p loadStandCounterPeriod) check(node loadStandNodeID) error {
	switch {
	case !p.startedFrom.Equal(p.startedTo):
		return fmt.Errorf("%w: %s %s started at %s, then at %s", errLoadStandWindowCountersReset, node, p.family,
			p.startedFrom.Format(time.RFC3339Nano), p.startedTo.Format(time.RFC3339Nano))
	case !p.readTo.After(p.readFrom):
		return fmt.Errorf("%w: %s %s read at %s, then at %s", errLoadStandWindowNotForward, node, p.family,
			p.readFrom.Format(time.RFC3339Nano), p.readTo.Format(time.RFC3339Nano))
	default:
		return nil
	}
}

// loadStandDeltaReader subtracts cumulative counters and collects every one
// that fell, so a window reports all of them at once.
type loadStandDeltaReader struct {
	node loadStandNodeID
	err  error
}

func (r *loadStandDeltaReader) count(name string, before, after uint64) loadStandEventCount {
	if after < before {
		r.err = errors.Join(r.err, fmt.Errorf("%w: %s %s fell from %d to %d", errLoadStandCounterDecreased, r.node, name, before, after))
		return 0
	}
	return loadStandEventCount(after - before)
}

// countMap subtracts per key over the union of keys: a key present before
// and gone after is a counter that fell to zero.
func (r *loadStandDeltaReader) countMap(name string, before, after map[string]uint64) map[string]loadStandEventCount {
	deltas := make(map[string]loadStandEventCount, len(after))
	for key := range loadStandKeyUnion(before, after) {
		deltas[key] = r.count(name+"/"+key, before[key], after[key])
	}
	return deltas
}

func (r *loadStandDeltaReader) sessions(before, after domain.SessionOutcomeStats) loadStandSessionDelta {
	return loadStandSessionDelta{
		Attempts:      r.count("session_attempts", before.Attempts, after.Attempts),
		Succeeded:     r.count("session_succeeded", before.Succeeded, after.Succeeded),
		ErrorsConnect: r.count("session_errors_connect", before.ErrorsConnect, after.ErrorsConnect),
		ErrorsCompat:  r.count("session_errors_compat", before.ErrorsCompat, after.ErrorsCompat),
		ErrorsOther:   r.count("session_errors_other", before.ErrorsOther, after.ErrorsOther),
	}
}

func (r *loadStandDeltaReader) modes(before, after routing.ModeSelectionStats) map[loadStandModeKey]loadStandEventCount {
	counts := func(stats routing.ModeSelectionStats) map[loadStandModeKey]uint64 {
		byKey := make(map[loadStandModeKey]uint64, len(stats.Decisions))
		for _, decision := range stats.Decisions {
			byKey[loadStandModeKey{Operation: decision.Operation, Mode: decision.Mode, Reason: decision.Reason}] = decision.Count
		}
		return byKey
	}
	beforeCounts, afterCounts := counts(before), counts(after)
	deltas := make(map[loadStandModeKey]loadStandEventCount, len(afterCounts))
	for key := range loadStandKeyUnion(beforeCounts, afterCounts) {
		deltas[key] = r.count(fmt.Sprintf("mode_selection/%d/%d/%d", key.Operation, key.Mode, key.Reason), beforeCounts[key], afterCounts[key])
	}
	return deltas
}

// datagram subtracts the datagram counters when the plane answered at both
// edges, and reports no window when it answered at neither.
func (r *loadStandDeltaReader) datagram(from, to loadStandReading[loadStandDatagramSample]) (loadStandReading[loadStandDatagramDelta], error) {
	before, beforeErr := from.Get()
	after, afterErr := to.Get()
	switch {
	case beforeErr != nil && afterErr != nil:
		return loadStandReadingRefused[loadStandDatagramDelta](afterErr), nil
	case beforeErr != nil || afterErr != nil:
		return loadStandReading[loadStandDatagramDelta]{}, fmt.Errorf("%w: %s (%v / %v)", errLoadStandWindowDatagramOneEdge, r.node, beforeErr, afterErr)
	}
	period, err := r.datagramPeriod(before.Metrics, after.Metrics)
	if err != nil {
		return loadStandReading[loadStandDatagramDelta]{}, err
	}
	return loadStandReadingOf(loadStandDatagramDelta{
		Period:        period,
		Observed:      r.count("datagram_observed", before.Metrics.Observed, after.Metrics.Observed),
		Accepted:      r.count("datagram_accepted", before.Metrics.Accepted, after.Metrics.Accepted),
		Dropped:       r.count("datagram_dropped", before.Metrics.Dropped, after.Metrics.Dropped),
		UnknownDType:  r.count("datagram_unknown_dtype", before.Metrics.UnknownDType, after.Metrics.UnknownDType),
		DropsByReason: r.countMap("datagram_drops", before.Metrics.DropsByReason, after.Metrics.DropsByReason),
		SendRefusals:  r.countMap("datagram_send_refusals", before.Metrics.SendRefusals, after.Metrics.SendRefusals),
		LocalRefusals: r.countMap("datagram_local_refusals", before.Reverse.LocalRefusals, after.Reverse.LocalRefusals),
	}), nil
}

func (r *loadStandDeltaReader) datagramPeriod(before, after datagram.MetricsSnapshot) (loadStandPeriod, error) {
	if before.StartedAt == nil || before.ReadAt == nil || after.StartedAt == nil || after.ReadAt == nil {
		return loadStandPeriod{}, fmt.Errorf("%w: %s datagram metrics", errLoadStandWindowUnboundedCounters, r.node)
	}
	period := loadStandCounterPeriod{
		family:      "datagram_metrics",
		startedFrom: *before.StartedAt,
		startedTo:   *after.StartedAt,
		readFrom:    *before.ReadAt,
		readTo:      *after.ReadAt,
	}
	if err := period.check(r.node); err != nil {
		return loadStandPeriod{}, err
	}
	return loadStandPeriod{From: *before.ReadAt, To: *after.ReadAt}, nil
}

func loadStandKeyUnion[K comparable, V any](maps ...map[K]V) map[K]struct{} {
	union := make(map[K]struct{})
	for _, m := range maps {
		for key := range m {
			union[key] = struct{}{}
		}
	}
	return union
}

// --- self-checks ---

type loadStandDefectKind string

const (
	// loadStandDefectByteBalance — the two ends of the stand's connections
	// disagree by more than in-flight bytes and read skew can explain.
	loadStandDefectByteBalance loadStandDefectKind = "byte_balance"
	// loadStandDefectFloorAboveHeap — the subsystem floors of all nodes add
	// up to more than the heap they all live in.
	loadStandDefectFloorAboveHeap loadStandDefectKind = "floor_above_heap"
	// loadStandDefectBansPresent — a node holds a ban that cuts it off on a
	// one-address stand; its sample measures the ban.
	loadStandDefectBansPresent loadStandDefectKind = "bans_present"
	// loadStandDefectProcessImplausible — the process sample contradicts
	// itself: a unit or a scale is wrong somewhere between the kernel, the
	// runtime and the stand.
	loadStandDefectProcessImplausible loadStandDefectKind = "process_implausible"
)

// loadStandDefect is a sample contradicting an invariant the stand relies
// on. It is a value the stand's goroutine records — never a panic, never a
// t.Fatal from a goroutine that does not own the test.
type loadStandDefect struct {
	Kind   loadStandDefectKind
	Detail string
}

func (d *loadStandDefect) Error() string {
	return fmt.Sprintf("%s: %s: %s", errLoadStandDefect, d.Kind, d.Detail)
}

func (d *loadStandDefect) Is(target error) bool { return target == errLoadStandDefect }

// checkLoadStandSweep runs the checks one sweep can answer alone.
func checkLoadStandSweep(sweep loadStandSweep) error {
	return errors.Join(checkLoadStandFloor(sweep), checkLoadStandBans(sweep), checkLoadStandProcess(sweep.Process))
}

// checkLoadStandFloor: every node's breakdown is a floor of what it holds,
// all nodes share this process's heap, and the process was sampled after
// them — so the floors cannot add up to more than the heap's objects (a
// coarse bound: see loadStandProcessSample).
func checkLoadStandFloor(sweep loadStandSweep) error {
	var floor loadStandByteCount
	for _, node := range sweep.Nodes {
		floor += loadStandByteCount(node.Resources.FloorBytes())
	}
	if floor <= sweep.Process.HeapObjects {
		return nil
	}
	return &loadStandDefect{
		Kind:   loadStandDefectFloorAboveHeap,
		Detail: fmt.Sprintf("subsystem floors of %d nodes add up to %d bytes, the heap holds %d", len(sweep.Nodes), floor, sweep.Process.HeapObjects),
	}
}

func checkLoadStandBans(sweep loadStandSweep) error {
	var defects []error
	for _, node := range sweep.Nodes {
		if len(node.Bans) == 0 {
			continue
		}
		defects = append(defects, &loadStandDefect{
			Kind:   loadStandDefectBansPresent,
			Detail: fmt.Sprintf("%s (life %d): %v", node.Node, node.Incarnation.Life, node.Bans),
		})
	}
	return errors.Join(defects...)
}

const (
	// loadStandRSSGoMemoryFactor and loadStandRSSNonGoBytes bound the peak
	// resident set by what the Go runtime has mapped: everything the runtime
	// mapped may be resident, and the rest of a test binary — its text, C
	// libraries, thread stacks — is tens of megabytes. A unit error is a
	// factor of 1024 and lands far outside.
	loadStandRSSGoMemoryFactor loadStandByteCount = 8
	loadStandRSSNonGoBytes     loadStandByteCount = 512 << 20
	// loadStandKernelCPUFactor bounds the CPU the kernel counted over a
	// window by GOMAXPROCS × the window's wall time, both measured by the
	// stand. Threads blocked in syscalls run outside GOMAXPROCS, so the
	// kernel can count somewhat more; a scale error is a factor of 1000.
	loadStandKernelCPUFactor = 2
	// loadStandKernelCPUJitter absorbs the kernel's accounting granularity,
	// which on a short window is comparable to the window itself.
	loadStandKernelCPUJitter = 50 * time.Millisecond
)

// checkLoadStandProcess checks what one process sample can answer alone: the
// kernel's peak resident set against what the Go runtime mapped (both
// current). Where the platform gave no rusage there is nothing to compare.
func checkLoadStandProcess(process loadStandProcessSample) error {
	usage, err := process.Rusage.Get()
	if err != nil {
		// An absent platform reading has nothing to contradict; the refusal
		// stays in the sample for the report.
		return nil
	}
	if ceiling := process.GoMemory*loadStandRSSGoMemoryFactor + loadStandRSSNonGoBytes; usage.MaxRSS > ceiling {
		return &loadStandDefect{
			Kind:   loadStandDefectProcessImplausible,
			Detail: fmt.Sprintf("peak RSS %d bytes exceeds %d (%d× the %d bytes Go mapped, plus %d)", usage.MaxRSS, ceiling, loadStandRSSGoMemoryFactor, process.GoMemory, loadStandRSSNonGoBytes),
		}
	}
	return nil
}

// checkLoadStandProcessWindow checks the kernel's CPU count over a window
// against the CPU the process could have used in it — GOMAXPROCS times the
// wall time, both read by the stand itself. The runtime's own CPU classes
// are not used: they move only when a GC completes.
func checkLoadStandProcessWindow(earlier, later loadStandProcessSample) error {
	before, beforeErr := earlier.Rusage.Get()
	after, afterErr := later.Rusage.Get()
	if beforeErr != nil || afterErr != nil {
		// Nothing to compare without the kernel's count at both edges.
		return nil
	}
	wall := later.ReadAt.Sub(earlier.ReadAt)
	if wall <= 0 {
		return fmt.Errorf("%w: process sampled at %s, then at %s", errLoadStandWindowNotForward,
			earlier.ReadAt.Format(time.RFC3339Nano), later.ReadAt.Format(time.RFC3339Nano))
	}
	spent := (after.UserCPU + after.SystemCPU) - (before.UserCPU + before.SystemCPU)
	available := time.Duration(max(earlier.Procs, later.Procs)) * wall
	if spent <= loadStandKernelCPUFactor*available+loadStandKernelCPUJitter {
		return nil
	}
	return &loadStandDefect{
		Kind: loadStandDefectProcessImplausible,
		Detail: fmt.Sprintf("the kernel counts %s of CPU in %s of wall time, more than %d× the %s %d processors allow",
			spent, wall, loadStandKernelCPUFactor, available, max(earlier.Procs, later.Procs)),
	}
}

// --- aggregation by role ---

// loadStandRoleGroup is what a per-node figure is summarised over. Hubs are
// apart from the other full nodes: they carry every edge's uplink, and
// folding them in would hide a hub cost in a full-node median.
type loadStandRoleGroup string

const (
	loadStandGroupHub  loadStandRoleGroup = "hub"
	loadStandGroupFull loadStandRoleGroup = "full"
	loadStandGroupEdge loadStandRoleGroup = "edge"
)

type loadStandRoleValue[V cmp.Ordered] struct {
	Node  loadStandNodeID
	Role  loadStandRole
	Hub   bool
	Value V
}

// loadStandRoleStats are nearest-rank statistics: each is a value some node
// actually had, so no interpolation invents a figure nobody measured.
type loadStandRoleStats[V cmp.Ordered] struct {
	Count  int
	Median V
	P90    V
	Max    V
}

func loadStandRoleGroupOf[V cmp.Ordered](value loadStandRoleValue[V]) (loadStandRoleGroup, error) {
	switch {
	case value.Role == loadStandRoleFull && value.Hub:
		return loadStandGroupHub, nil
	case value.Role == loadStandRoleFull:
		return loadStandGroupFull, nil
	case value.Role == loadStandRoleEdge && !value.Hub:
		return loadStandGroupEdge, nil
	default:
		return "", fmt.Errorf("%w: %s has role %q with hub=%v", errLoadStandAggregateInvalid, value.Node, value.Role, value.Hub)
	}
}

// aggregateLoadStandByRole summarises per-node values by role group.
func aggregateLoadStandByRole[V cmp.Ordered](values []loadStandRoleValue[V]) (map[loadStandRoleGroup]loadStandRoleStats[V], error) {
	grouped := make(map[loadStandRoleGroup][]V)
	for _, value := range values {
		group, err := loadStandRoleGroupOf(value)
		if err != nil {
			return nil, err
		}
		grouped[group] = append(grouped[group], value.Value)
	}
	stats := make(map[loadStandRoleGroup]loadStandRoleStats[V], len(grouped))
	for group, members := range grouped {
		slices.Sort(members)
		stats[group] = loadStandRoleStats[V]{
			Count:  len(members),
			Median: loadStandNearestRank(members, 50),
			P90:    loadStandNearestRank(members, 90),
			Max:    members[len(members)-1],
		}
	}
	return stats, nil
}

// loadStandNearestRank is the value at rank ⌈percentile·n/100⌉ of sorted.
func loadStandNearestRank[V cmp.Ordered](sorted []V, percentile int) V {
	rank := (percentile*len(sorted) + 99) / 100
	return sorted[max(rank, 1)-1]
}
