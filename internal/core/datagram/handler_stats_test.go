package datagram

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// handler_stats_test.go pins the per-type handler accounting
// (docs/rpc/datagram.md, `handlers`): one record per handler call, on every
// plane that calls a handler, after every gate, with the outcome the handler
// returned and the time it took on the pipeline's clock.

// scriptedHandler answers each call with the next step of its script and
// counts the calls itself — the spy the layer's numbers are compared against.
// A step may panic or move the clock; the count is taken before the step runs,
// so a panicking call is still a call.
type scriptedHandler struct {
	mu     sync.Mutex
	calls  int
	script []func() HandlerResult
}

func scriptOf(steps ...func() HandlerResult) *scriptedHandler {
	return &scriptedHandler{script: steps}
}

func (h *scriptedHandler) Handle(context.Context, DeliveryContext, []byte) HandlerResult {
	h.mu.Lock()
	h.calls++
	if len(h.script) == 0 {
		h.mu.Unlock()
		return AcceptDelivery()
	}
	next := h.script[0]
	h.script = h.script[1:]
	h.mu.Unlock()
	return next()
}

func (h *scriptedHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func acceptStep() HandlerResult { return AcceptDelivery() }
func rejectStep() HandlerResult { return RejectDelivery(errors.New("not wanted")) }
func failStep() HandlerResult   { return FailDelivery(errors.New("disk full")) }
func unsetStep() HandlerResult  { return HandlerResult{} }
func panicStep() HandlerResult  { panic("handler exploded") }

// handlerCountsOf reads one (dtype, mode) row, failing when the row is absent:
// a registered type that has been called must have one.
func handlerCountsOf(t *testing.T, node *pipelineNode, dtype domain.DType, mode domain.DatagramMode) HandlerCounts {
	t.Helper()
	byMode, ok := node.types.HandlerStats()[dtype.String()]
	if !ok {
		t.Fatalf("no handler stats for %s", dtype)
	}
	counts, ok := byMode[mode.String()]
	if !ok {
		t.Fatalf("no handler stats for %s in mode %s", dtype, mode)
	}
	return counts
}

func requireHandlerCounts(t *testing.T, got, want HandlerCounts) {
	t.Helper()
	got.WallNs, want.WallNs = 0, 0
	if got != want {
		t.Fatalf("handler counts = %+v, want %+v", got, want)
	}
	if got.Calls != got.Accepted+got.Rejected+got.Failed {
		t.Fatalf("calls %d != accepted %d + rejected %d + failed %d", got.Calls, got.Accepted, got.Rejected, got.Failed)
	}
	if got.Panics > got.Failed {
		t.Fatalf("panics %d exceed failed %d: a panic is a failed call, not a separate one", got.Panics, got.Failed)
	}
}

// TestHandlerStatsCountEveryOutcomeOnTheRoutedPlane walks every handler
// outcome — the unset result and a panic included — and then a duplicate.
//
// The duplicate is what pins the accounting AFTER the gates: the anti-replay
// sieve drops it before the handler, so a count taken any earlier would move.
func TestHandlerStatsCountEveryOutcomeOnTheRoutedPlane(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	handler := scriptOf(acceptStep, rejectStep, failStep, unsetStep, panicStep)
	registerType(t, receiver, routedType(dtypePush, handler))

	for i := 0; i < 5; i++ {
		frame := signedRouted(t, routedOpts{private: private, src: signer, dst: receiver.id, now: sender.clock()})
		result := receiver.deliver(t, sender.id, frame)
		if i == 0 {
			requireOutcome(t, result, InboundDelivered)
			requireDrop(t, receiver.deliver(t, sender.id, frame), DropReplayDuplicate)
		}
	}

	if handler.callCount() != 5 {
		t.Fatalf("the spy saw %d calls, want 5", handler.callCount())
	}
	requireHandlerCounts(t, handlerCountsOf(t, receiver, dtypePush, domain.DatagramModeRouted), HandlerCounts{
		Calls: 5, Accepted: 1, Rejected: 1, Failed: 3, Panics: 1,
	})
}

// TestHandlerStatsCountTheRequestPlane is the case that fails if the count
// lives in the routed plane's runLocalDelivery instead of the one wrapper all
// three planes call.
func TestHandlerStatsCountTheRequestPlane(t *testing.T) {
	fixture := newRequestFixture(t)
	handler := scriptOf(
		func() HandlerResult { return AcceptWithAnswer(dtypeAnswer, []byte("answer")) },
		rejectStep,
		panicStep,
	)
	registerType(t, fixture.target, requestType(dtypeQuery, handler))

	for i, seed := range []string{"request-1", "request-2", "request-3"} {
		result := fixture.target.deliver(t, fixture.relay.id, requestFrame(t, requestOpts{
			label: newLabel(t, seed), dst: fixture.target.id,
		}))
		if i == 0 {
			requireOutcome(t, result, InboundAnswered)
		}
	}

	if handler.callCount() != 3 {
		t.Fatalf("the spy saw %d calls, want 3", handler.callCount())
	}
	requireHandlerCounts(t, handlerCountsOf(t, fixture.target, dtypeQuery, domain.DatagramModeRequest), HandlerCounts{
		Calls: 3, Accepted: 1, Rejected: 1, Failed: 1, Panics: 1,
	})
}

// TestHandlerStatsCountTheResponsePlane covers the third caller: an answer
// delivered to this node's own request.
func TestHandlerStatsCountTheResponsePlane(t *testing.T) {
	fixture := newLocalResponseFixture(t, newLabel(t, "stats-response"))
	handler := scriptOf(acceptStep)
	registerType(t, fixture.origin, responseType(dtypeAnswer, dtypeQuery, handler))

	requireOutcome(t, fixture.answer(t, dtypeAnswer), InboundDelivered)

	if handler.callCount() != 1 {
		t.Fatalf("the spy saw %d calls, want 1", handler.callCount())
	}
	requireHandlerCounts(t, handlerCountsOf(t, fixture.origin, dtypeAnswer, domain.DatagramModeResponse), HandlerCounts{
		Calls: 1, Accepted: 1,
	})
}

// TestHandlerStatsHaveNoKeyForAnUnknownDType pins the closed key set: a dtype
// this build does not implement is counted by UnknownDType and nowhere else,
// so a sender cannot grow the stats map by inventing names.
func TestHandlerStatsHaveNoKeyForAnUnknownDType(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	registerType(t, receiver, routedType(dtypePush, acceptingHandler()))

	requireDrop(t, receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(), dtype: dtypeCached,
	})), DropUnknownDType)

	if got := receiver.metrics.unknownTypes(); len(got) != 1 || got[0] != dtypeCached {
		t.Fatalf("unknown dtypes observed %v, want [%s]", got, dtypeCached)
	}
	stats := receiver.types.HandlerStats()
	if _, present := stats[dtypeCached.String()]; present {
		t.Fatalf("an unknown dtype grew a stats key: %v", stats)
	}
	if counts := stats[dtypePush.String()][domain.DatagramModeRouted.String()]; counts.Calls != 0 {
		t.Fatalf("a refused unknown type was charged to %s: %+v", dtypePush, counts)
	}
}

// TestHandlerStatsMeasureElapsedTimeOnTheDurationSource moves the injected
// duration source inside the handler (in this fixture it is the node clock)
// and expects exactly that much: elapsed time, whatever the handler waited on
// included.
func TestHandlerStatsMeasureElapsedTimeOnTheDurationSource(t *testing.T) {
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver"})
	const spent = 1500 * time.Microsecond
	registerType(t, receiver, routedType(dtypePush, scriptOf(func() HandlerResult {
		receiver.advance(spent)
		return AcceptDelivery()
	})))

	requireOutcome(t, receiver.deliver(t, sender.id, signedRouted(t, routedOpts{
		private: private, src: signer, dst: receiver.id, now: sender.clock(),
	})), InboundDelivered)

	if got := handlerCountsOf(t, receiver, dtypePush, domain.DatagramModeRouted).WallNs; got != uint64(spent) {
		t.Fatalf("wall_ns = %d, want exactly %d", got, uint64(spent))
	}
}

// TestHandlerStatsListEveryDeclaredModeBeforeAnyCall pins the shape a reader
// relies on: a registered type has a row per declared mode from the start, so
// "zero calls" is a reading and not a missing key.
func TestHandlerStatsListEveryDeclaredModeBeforeAnyCall(t *testing.T) {
	registry := NewTypeRegistry()
	registerTypeInto(t, registry, requestType(dtypeQuery, acceptingHandler()))

	byMode, ok := registry.HandlerStats()[dtypeQuery.String()]
	if !ok {
		t.Fatal("a registered type has no stats row")
	}
	if len(byMode) != 1 {
		t.Fatalf("rows %v, want exactly the declared request mode", byMode)
	}
	if counts := byMode[domain.DatagramModeRequest.String()]; counts != (HandlerCounts{}) {
		t.Fatalf("a type never called reports %+v", counts)
	}
}

// TestHandlerStatsAgreeWithTheModeCounters pins how the `handlers` block and
// the per-mode `metrics` relate: the same handler calls, cut two ways, never
// summed.
//
// What the three callers of runHandler make exact:
//
//   - every handler_rejected and handler_failed drop is produced right after a
//     handler returned that outcome, and nowhere else, so Σ rejected and
//     Σ failed over all (dtype, mode) rows equal those two drop reasons;
//   - on the routed plane the only `delivered` is an accepted handler, so
//     accepted(routed) == ByMode[routed].Delivered;
//   - on the request plane an accepted call ends as `delivered` (no answer),
//     `answered`, or a refused answer whose drop reason other branches share —
//     so accepted(request) >= Delivered + Answered, with equality when no
//     answer was refused, as here.
func TestHandlerStatsAgreeWithTheModeCounters(t *testing.T) {
	metrics := NewMetrics()
	net := newFakeNetwork()
	private, signer := newSigner(t)
	sender := newPipelineNode(t, net, nodeOpts{id: signer})
	relay := newPipelineNode(t, net, nodeOpts{name: "relay", transit: true})
	receiver := newPipelineNode(t, net, nodeOpts{name: "receiver", metrics: metrics})
	link(relay, receiver, true, false)

	registerType(t, receiver, routedType(dtypePush, scriptOf(acceptStep, acceptStep, rejectStep, failStep, panicStep)))
	registerType(t, receiver, requestType(dtypeQuery, scriptOf(
		func() HandlerResult { return AcceptWithAnswer(dtypeAnswer, []byte("answer")) },
		acceptStep,
		rejectStep,
		unsetStep,
	)))

	for i := 0; i < 5; i++ {
		receiver.deliver(t, sender.id, signedRouted(t, routedOpts{private: private, src: signer, dst: receiver.id, now: sender.clock()}))
	}
	for i := 0; i < 4; i++ {
		receiver.deliver(t, relay.id, requestFrame(t, requestOpts{label: newLabel(t, "agree-"+strconv.Itoa(i)), dst: receiver.id}))
	}

	snapshot := metrics.Snapshot()
	accepted := map[string]uint64{}
	var rejected, failed uint64
	for _, byMode := range receiver.types.HandlerStats() {
		for mode, counts := range byMode {
			accepted[mode] += counts.Accepted
			rejected += counts.Rejected
			failed += counts.Failed
		}
	}
	if rejected != snapshot.DropsByReason[DropHandlerRejected.String()] {
		t.Fatalf("Σ rejected = %d, handler_rejected drops = %d", rejected, snapshot.DropsByReason[DropHandlerRejected.String()])
	}
	if failed != snapshot.DropsByReason[DropHandlerFailed.String()] {
		t.Fatalf("Σ failed = %d, handler_failed drops = %d", failed, snapshot.DropsByReason[DropHandlerFailed.String()])
	}
	routed := snapshot.ByMode[domain.DatagramModeRouted.String()]
	if accepted[domain.DatagramModeRouted.String()] != routed.Delivered || routed.Delivered != 2 {
		t.Fatalf("accepted(routed) = %d, delivered(routed) = %d, want both 2",
			accepted[domain.DatagramModeRouted.String()], routed.Delivered)
	}
	request := snapshot.ByMode[domain.DatagramModeRequest.String()]
	if accepted[domain.DatagramModeRequest.String()] != request.Delivered+request.Answered || request.Answered != 1 {
		t.Fatalf("accepted(request) = %d, delivered + answered = %d + %d",
			accepted[domain.DatagramModeRequest.String()], request.Delivered, request.Answered)
	}
}

// fakeStopwatch is a duration source a test moves by hand, independent of the
// fixture's wall clock.
type fakeStopwatch struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeStopwatch() *fakeStopwatch {
	return &fakeStopwatch{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (s *fakeStopwatch) read() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

func (s *fakeStopwatch) advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

// stopwatchFixture is a sender and a receiver whose handler durations come
// from a fake stopwatch rather than from the wall clock.
type stopwatchFixture struct {
	private   ed25519.PrivateKey
	signer    domain.PeerIdentity
	sender    *pipelineNode
	receiver  *pipelineNode
	stopwatch *fakeStopwatch
}

func newStopwatchFixture(t *testing.T) *stopwatchFixture {
	t.Helper()
	net := newFakeNetwork()
	private, signer := newSigner(t)
	stopwatch := newFakeStopwatch()
	return &stopwatchFixture{
		private:   private,
		signer:    signer,
		sender:    newPipelineNode(t, net, nodeOpts{id: signer}),
		receiver:  newPipelineNode(t, net, nodeOpts{name: "receiver", monotonic: stopwatch.read}),
		stopwatch: stopwatch,
	}
}

func (f *stopwatchFixture) deliverOne(t *testing.T) InboundResult {
	t.Helper()
	return f.receiver.deliver(t, f.sender.id, signedRouted(t, routedOpts{
		private: f.private, src: f.signer, dst: f.receiver.id, now: f.sender.clock(),
	}))
}

// TestHandlerWallTimeIgnoresAWallClockStep is the NTP case: the node's wall
// clock steps back an hour while the handler runs, and the handler still took
// exactly what the monotonic source says. Measured on the wall clock the call
// would read as negative and be lost.
func TestHandlerWallTimeIgnoresAWallClockStep(t *testing.T) {
	fixture := newStopwatchFixture(t)
	const spent = 2 * time.Millisecond
	registerType(t, fixture.receiver, routedType(dtypePush, scriptOf(func() HandlerResult {
		fixture.receiver.advance(-time.Hour)
		fixture.stopwatch.advance(spent)
		return AcceptDelivery()
	})))

	requireOutcome(t, fixture.deliverOne(t), InboundDelivered)

	if got := handlerCountsOf(t, fixture.receiver, dtypePush, domain.DatagramModeRouted).WallNs; got != uint64(spent) {
		t.Fatalf("wall_ns = %d, want exactly the %d the monotonic source moved", got, uint64(spent))
	}
}

// TestHandlerWallTimeNeverWrapsOnABackwardReading pins the clamp: a duration
// source that answers an earlier instant at the end of a call (an injected
// one can) charges nothing, instead of adding an unsigned wrap-around of
// ~1.8e19 ns to the sum.
func TestHandlerWallTimeNeverWrapsOnABackwardReading(t *testing.T) {
	fixture := newStopwatchFixture(t)
	registerType(t, fixture.receiver, routedType(dtypePush, scriptOf(func() HandlerResult {
		fixture.stopwatch.advance(-time.Second)
		return AcceptDelivery()
	})))

	requireOutcome(t, fixture.deliverOne(t), InboundDelivered)

	counts := handlerCountsOf(t, fixture.receiver, dtypePush, domain.DatagramModeRouted)
	if counts.WallNs != 0 || counts.Calls != 1 {
		t.Fatalf("counts = %+v, want one call charged zero time", counts)
	}
}

// TestPanicReportIsNotChargedToTheHandler pins where the clock stops on a
// panic: before the crash report is written. The report carries a stack trace
// and can take longer than the handler did; charging it to the type would make
// a crashing handler look slow rather than broken.
func TestPanicReportIsNotChargedToTheHandler(t *testing.T) {
	fixture := newStopwatchFixture(t)
	const spent = 3 * time.Millisecond
	registerType(t, fixture.receiver, routedType(dtypePush, scriptOf(func() HandlerResult {
		fixture.stopwatch.advance(spent)
		panic("handler exploded")
	})))

	// Writing the crash report costs a whole second on the stopwatch. The
	// global logger is swapped in a test that is not parallel, so no other
	// test writes through it meanwhile.
	previous := log.Logger
	log.Logger = zerolog.New(stopwatchWriter{stopwatch: fixture.stopwatch, cost: time.Second})
	t.Cleanup(func() { log.Logger = previous })

	requireDrop(t, fixture.deliverOne(t), DropHandlerFailed)

	counts := handlerCountsOf(t, fixture.receiver, dtypePush, domain.DatagramModeRouted)
	if counts.Panics != 1 {
		t.Fatalf("counts = %+v, want one panic", counts)
	}
	if counts.WallNs != uint64(spent) {
		t.Fatalf("wall_ns = %d, want the handler's own %d without the crash report", counts.WallNs, uint64(spent))
	}
}

// stopwatchWriter is a log sink whose every write costs time on a stopwatch.
type stopwatchWriter struct {
	stopwatch *fakeStopwatch
	cost      time.Duration
}

func (w stopwatchWriter) Write(line []byte) (int, error) {
	w.stopwatch.advance(w.cost)
	return len(line), nil
}

// TestDefaultDurationSourceIsMonotonic pins the production default: without an
// injected source the pipeline measures durations on readings that carry
// Go's monotonic clock, which a wall-clock step does not move.
func TestDefaultDurationSourceIsMonotonic(t *testing.T) {
	node := newPipelineNode(t, newFakeNetwork(), nodeOpts{name: "default"})
	pipeline, err := NewPipeline(PipelineConfig{
		Clock:       node.clock,
		Types:       node.types,
		ReplayCache: node.replay,
		Reverse:     node.reverse,
		Scheduler:   node.scheduler,
		Emitter:     nodeEmitter{net: node.net, from: node.id, sender: node},
		Network:     testNetwork,
		LocalID:     node.id,
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	// time.Time.String appends " m=±<seconds>" exactly when the value carries
	// a monotonic reading — the one public way to observe it.
	if reading := pipeline.monotonic(); !strings.Contains(reading.String(), " m=") {
		t.Fatal("the default duration source carries no monotonic reading: a wall-clock step would distort every handler time")
	}
}
