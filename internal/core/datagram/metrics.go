package datagram

import (
	"sync/atomic"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// metrics.go is the counting end of the layer (§10): accepted, forwarded,
// dropped BY REASON, unknown types, refused answers, plus the reverse-state
// events of §4.2.
//
// Everything the layer refuses is refused SILENTLY on the wire — no error
// frame, no connection tear-down — so these counters are the only place a
// drop is observable at all. That is why the drop dimension is the closed
// DropReason enum declared in pipeline.go and not a string: a metric that
// cannot be enumerated cannot be alerted on, and a second, parallel list of
// reasons would drift from the one the pipeline actually decides by.
//
// Style follows routing.RouteCapStats: atomic counters, lock-free reads, one
// snapshot value published to callers. No external dependency, no Prometheus,
// no registry — M9 renders the snapshot into an RPC diagnostic, and a metric
// system that has to be initialized before the layer works is a metric system
// that gets disabled in tests and then rots.
//
// Reference: docs/refactoring/datagram-transport.md §4.2, §5, §10, §11.6.

// enumSlots sizes the counter arrays. All three enumerations counted here are
// uint8, so one slot per possible value is 2 KiB per array and the array can
// never be indexed out of range — including by a value a future milestone
// adds without touching this file.
const enumSlots = 256

// modeSlots is one slot per datagram mode plus one for anything the parser
// could not have produced. The extra slot is not defensive clutter: it is the
// difference between "no such mode was seen" and a silent miscount.
const modeSlots = 4

const (
	modeSlotRouted = iota
	modeSlotRequest
	modeSlotResponse
	modeSlotOther
)

var modeSlotNames = [modeSlots]string{
	modeSlotRouted:   domain.DatagramModeRouted.String(),
	modeSlotRequest:  domain.DatagramModeRequest.String(),
	modeSlotResponse: domain.DatagramModeResponse.String(),
	modeSlotOther:    "other",
}

func modeSlot(mode domain.DatagramMode) int {
	switch mode {
	case domain.DatagramModeRouted:
		return modeSlotRouted
	case domain.DatagramModeRequest:
		return modeSlotRequest
	case domain.DatagramModeResponse:
		return modeSlotResponse
	default:
		return modeSlotOther
	}
}

// refusedAnswerReasons is the set of drop reasons that mean "an answer was
// refused" (§10). They are listed once, here, rather than derived from a
// naming convention: DropAnswerNotDelivered is a refused answer and does not
// start with DropReverse, and DropReverseSlotBusy is a refused REQUEST and
// does, so a prefix rule would get both wrong.
var refusedAnswerReasons = [...]DropReason{
	DropReverseUnknownLabel,
	DropReverseWrongDownstream,
	DropReverseSubjectMismatch,
	DropReverseNotPending,
	DropReversePairing,
	DropReverseProbeExhausted,
	DropReverseRecordStale,
	DropReverseClaimLost,
	DropAnswerNotDelivered,
}

// Metrics is the layer's counter set. Safe for concurrent use from every
// receive goroutine at once; readers never block writers.
type Metrics struct {
	// startedAt is when THIS counter set began accumulating — the life of the
	// instance, not of the process.
	//
	// It is what makes a pair of readings comparable. Every counter here is
	// cumulative and in-memory, so two readings taken across a restart differ
	// by "120 minus 100 = 20" while describing two unrelated runs; comparing a
	// counter to itself cannot detect that, because the second run may well
	// have passed the first. The stamp changes when the instance does, so the
	// pair is discarded instead of subtracted. Immutable after construction.
	startedAt time.Time

	outcomes [modeSlots][enumSlots]atomic.Uint64
	drops    [enumSlots]atomic.Uint64
	reverse  [enumSlots]atomic.Uint64
	unknown  atomic.Uint64
	// sendRefusals counts sends the POLICY GATES refused, split by which gate
	// spoke. Until this existed the three verdicts of §4.3
	// (RejectionMissingCapability with either advertised capability, and
	// RejectionUnsupportedDType) were per-call values that reached a JSON
	// field of one request and nothing cumulative — a capability rollout could
	// not be watched from a running node, which is what
	// docs/refactoring/dht/05-rollout-metrics.md is about.
	sendRefusals [sendRefusalSlots]atomic.Uint64
}

// sendRefusalSlot names WHICH admission gate refused a send.
//
// A slot, not a map keyed by the capability name: the name is ours today, and
// a counter map whose keys could ever come from a frame is the defect
// ObserveUnknownDType refuses to have. Four fixed slots also make the totals
// addable — every refusal lands in exactly one.
type sendRefusalSlot uint8

const (
	// sendRefusalMissingEndpoint — the peer never advertised mesh_datagram_v1,
	// so the datagram plane does not exist for it at all.
	sendRefusalMissingEndpoint sendRefusalSlot = iota
	// sendRefusalMissingTransit — the peer speaks datagrams but does not
	// forward other nodes' frames, and this frame needed a transit.
	sendRefusalMissingTransit
	// sendRefusalUnsupportedDType — the destination itself does not implement
	// this dtype. Not a rollout signal about the transport, and kept apart for
	// that reason.
	sendRefusalUnsupportedDType
	// sendRefusalOther — a refusal this build cannot attribute. Present so the
	// four slots sum to every refusal: a gate added later without a slot shows
	// up here instead of vanishing.
	sendRefusalOther

	sendRefusalSlots
)

var sendRefusalNames = [sendRefusalSlots]string{
	sendRefusalMissingEndpoint:  "missing_endpoint_capability",
	sendRefusalMissingTransit:   "missing_transit_capability",
	sendRefusalUnsupportedDType: "unsupported_dtype",
	sendRefusalOther:            "other",
}

// Compile-time proof that one type serves both counting seams: the
// pipeline's metricsSink and the reverse table's reverseMetrics. They are
// declared separately because the table is usable on its own, but there is
// exactly one implementation and therefore one place the numbers live.
var (
	_ metricsSink    = (*Metrics)(nil)
	_ reverseMetrics = (*Metrics)(nil)
)

// NewMetrics builds an empty counter set. There is nothing to configure: a
// counter with a knob is a counter that means different things on two nodes.
func NewMetrics() *Metrics { return NewMetricsStartedAt(time.Now().UTC()) }

// NewMetricsStartedAt returns a counter set whose accumulation period begins at
// startedAt.
//
// Separate from NewMetrics so a caller that already knows when the node began —
// and every reading of these counters is anchored to that instant — does not
// have to accept a second, slightly different "now".
func NewMetricsStartedAt(startedAt time.Time) *Metrics {
	return &Metrics{startedAt: startedAt}
}

// ObserveInbound counts one processed inbound frame. reason is
// DropReasonUnset for every non-drop outcome, and the outcome is counted
// regardless — "accepted" and "dropped" are two values of one dimension, so
// they can never disagree about how many frames were seen.
func (m *Metrics) ObserveInbound(mode domain.DatagramMode, outcome InboundOutcome, reason DropReason) {
	if m == nil {
		return
	}
	m.outcomes[modeSlot(mode)][outcome].Add(1)
	if reason != DropReasonUnset {
		m.drops[reason].Add(1)
	}
}

// ObserveDrop counts a frame the layer dropped OUTSIDE the inbound conveyor —
// today the one case is a frame the writer refused after it had already left
// the class queue (§5).
//
// It touches only the reason breakdown and not the inbound totals, and that is
// the point: such a frame was already counted once as an accepted inbound
// frame (or was created locally and never inbound at all), so adding it to
// Observed a second time would make "observed" stop meaning "frames seen".
// §10 asks for "dropped by reason", and until this existed a frame lost
// between the queue and the socket appeared in no counter at all.
func (m *Metrics) ObserveDrop(reason DropReason) {
	if m == nil || reason == DropReasonUnset {
		return
	}
	m.drops[reason].Add(1)
}

// ObserveUnknownDType counts a frame addressed here whose dtype this build
// does not implement: a silent drop on a live connection, with no ban (§2).
//
// Only the TOTAL is kept. Naming the types would make an attacker-controlled
// string the key of a map this node grows, and §11.6 has not decided whether
// the per-type breakdown is worth publishing at all; the total is what §10
// asks for and is enough to see "the network moved on without us".
func (m *Metrics) ObserveUnknownDType(_ domain.DType) {
	if m == nil {
		return
	}
	m.unknown.Add(1)
}

// ObserveReverseState counts one reverse-state transition (§4.2).
// ObserveSendRefusal records that the admission gates refused a send.
//
// The UNIT is one refused SEND, not one rejected candidate. A send walks
// several candidates and may reject most of them before handing the frame to
// one that passes — counting per candidate would make refusals track the
// length of the candidate list, and a healthy node with many neighbours would
// look like a failing one. It is also not a failed delivery: a refusal means
// the frame was never handed to anybody, which is a different fact from a
// frame that went out and did not arrive.
//
// Called only where a real send ended in a refusal. The read-only route plan
// and the reachability probe reach the same verdict by design — that is what
// they are for — and counting them would inflate the metric with predictions
// of sends that never happened.
func (m *Metrics) ObserveSendRefusal(reason RejectionReason, missing domain.CapabilityName) {
	if m == nil {
		return
	}
	m.sendRefusals[sendRefusalSlotOf(reason, missing)].Add(1)
}

// sendRefusalSlotOf maps a verdict onto its slot.
func sendRefusalSlotOf(reason RejectionReason, missing domain.CapabilityName) sendRefusalSlot {
	switch reason {
	case RejectionUnsupportedDType:
		return sendRefusalUnsupportedDType
	case RejectionMissingCapability:
		switch missing {
		case CapabilityDatagramV1:
			return sendRefusalMissingEndpoint
		case CapabilityDatagramTransitV1:
			return sendRefusalMissingTransit
		}
	}
	return sendRefusalOther
}

func (m *Metrics) ObserveReverseState(event ReverseEvent) {
	if m == nil {
		return
	}
	m.reverse[event].Add(1)
}

// ModeCounts is the outcome breakdown of one mode.
type ModeCounts struct {
	// Observed is every frame the pipeline decided on in this mode.
	Observed uint64
	// Delivered counts frames a local handler accepted.
	Delivered uint64
	// Forwarded counts frames queued towards a next hop.
	Forwarded uint64
	// Answered counts requests answered here.
	Answered uint64
	// Dropped counts frames that ended here; DropsByReason says why.
	Dropped uint64
}

// MetricsSnapshot is the published view of the counters, shaped for an RPC
// diagnostic: totals first, then the breakdowns, with the zero-valued
// reasons and events omitted so the document stays readable on a node that
// has never hit them.
//
// Consistency: every counter is read at its own Load point, exactly as
// routing.RouteCapStats does. Under live traffic two fields may be observed
// one increment apart; once traffic stops the snapshot is exact, and the
// totals are computed FROM the same reads that produced the breakdowns, so
// the two can never disagree inside one snapshot.
type MetricsSnapshot struct {
	// Observed is every inbound frame the pipeline decided on.
	Observed uint64
	// Accepted is the frames that were not dropped: delivered, forwarded or
	// answered — the "accepted and forwarded" pair of §10 read as one number.
	Accepted uint64
	// Delivered, Forwarded, Answered and Dropped are the totals across all
	// three modes.
	Delivered uint64
	// Forwarded counts frames queued towards a next hop, all modes.
	Forwarded uint64
	// Answered counts answers produced here, all modes.
	Answered uint64
	// Dropped counts frames refused, all modes and all reasons.
	Dropped uint64
	// UnknownDType counts frames addressed here whose type this build does
	// not implement (§10, §11.6).
	UnknownDType uint64
	// RefusedAnswers counts answers the response plane refused — a pairing
	// mismatch, a wrong downstream, an exhausted probe budget, a lost claim
	// or a write queue that would not take the answer (§4.2, §10).
	RefusedAnswers uint64
	// ByMode is the per-mode outcome breakdown, keyed by the wire name of
	// the mode.
	ByMode map[string]ModeCounts
	// DropsByReason is the drop breakdown, keyed by the metric label of
	// DropReason. Zero-valued reasons are omitted.
	DropsByReason map[string]uint64
	// ReverseEvents is the reverse-state breakdown, keyed by the metric
	// label of ReverseEvent. Zero-valued events are omitted.
	ReverseEvents map[string]uint64
	// SendRefusals is the admission-gate breakdown of refused sends, keyed by
	// which gate spoke. Zero-valued slots are omitted. One refused send, one
	// increment — see ObserveSendRefusal for what the unit is and is not.
	SendRefusals map[string]uint64
	// StartedAt and ReadAt bound the period EVERY counter here describes.
	//
	// Pointers so an unknown instant is `null` rather than the year 1: a
	// dashboard subtracting "0001-01-01" produces two thousand years of uptime
	// instead of an obvious gap.
	//
	// Both are required to compare two readings. ReadAt gives the rate a
	// denominator; StartedAt tells whether the two readings even belong to the
	// same run — across a restart the counters reset, and a plain difference
	// silently reports the second run's total as the growth since the first.
	StartedAt *time.Time
	ReadAt    *time.Time
}

// Snapshot publishes the counters. Lock-free: nothing here takes a mutex, so
// a diagnostic RPC cannot slow the receive path down.
func (m *Metrics) Snapshot() MetricsSnapshot {
	snapshot := MetricsSnapshot{
		ByMode:        make(map[string]ModeCounts, modeSlots),
		DropsByReason: make(map[string]uint64),
		ReverseEvents: make(map[string]uint64),
		SendRefusals:  make(map[string]uint64),
	}
	if m == nil {
		return snapshot
	}

	for slot := 0; slot < modeSlots; slot++ {
		counts := m.modeCounts(slot)
		if counts.Observed == 0 {
			continue
		}
		snapshot.ByMode[modeSlotNames[slot]] = counts
		snapshot.Observed += counts.Observed
		snapshot.Delivered += counts.Delivered
		snapshot.Forwarded += counts.Forwarded
		snapshot.Answered += counts.Answered
		snapshot.Dropped += counts.Dropped
	}
	snapshot.Accepted = snapshot.Delivered + snapshot.Forwarded + snapshot.Answered

	for reason := 0; reason < enumSlots; reason++ {
		count := m.drops[reason].Load()
		if count == 0 {
			continue
		}
		snapshot.DropsByReason[DropReason(reason).String()] = count
	}
	for event := 0; event < enumSlots; event++ {
		count := m.reverse[event].Load()
		if count == 0 {
			continue
		}
		snapshot.ReverseEvents[ReverseEvent(event).String()] = count
	}

	for _, reason := range refusedAnswerReasons {
		snapshot.RefusedAnswers += m.drops[reason].Load()
	}
	for slot := sendRefusalSlot(0); slot < sendRefusalSlots; slot++ {
		count := m.sendRefusals[slot].Load()
		if count == 0 {
			continue
		}
		snapshot.SendRefusals[sendRefusalNames[slot]] = count
	}
	snapshot.UnknownDType = m.unknown.Load()
	if !m.startedAt.IsZero() {
		startedAt := m.startedAt
		snapshot.StartedAt = &startedAt
	}
	readAt := time.Now().UTC()
	snapshot.ReadAt = &readAt
	return snapshot
}

func (m *Metrics) modeCounts(slot int) ModeCounts {
	counts := ModeCounts{
		Delivered: m.outcomes[slot][InboundDelivered].Load(),
		Forwarded: m.outcomes[slot][InboundForwarded].Load(),
		Answered:  m.outcomes[slot][InboundAnswered].Load(),
		Dropped:   m.outcomes[slot][InboundDropped].Load(),
	}
	counts.Observed = counts.Delivered + counts.Forwarded +
		counts.Answered + counts.Dropped +
		m.outcomes[slot][InboundOutcomeUnset].Load()
	return counts
}

// Diagnostics is the whole §5 / §10 picture of one layer instance in one
// value: what the counters saw, what the budgets did, what the queue holds
// and the numbers all three were configured with.
//
// It exists because the three components are deliberately independent — the
// queue does not know about the budget, the budget does not know about the
// metrics — and a diagnostic that had to be assembled by its caller would be
// assembled differently by each of them. M9 renders this; nothing here knows
// what an RPC is.
type Diagnostics struct {
	// Limits are the numbers this instance runs on, so a reader never has
	// to guess which build the counters came from.
	Limits Limits
	// Metrics is the pipeline's decision breakdown.
	Metrics MetricsSnapshot
	// Admission is the per-neighbour budget's counter set.
	Admission AdmissionStats
	// Queue is the weighted queue's counter set and current depths.
	Queue QueueStats
	// Replay is the base anti-replay cache's counter set and occupancy.
	//
	// It is here because the cache is the one component whose OVERFLOW is a
	// fairness decision rather than a queue depth: RejectedNoisyPeer,
	// EvictedNoisyPeer and RejectedCapacity say that this node refused or
	// evicted a record under pressure, and AbandonedReservations says a
	// pipeline branch was lost. §5 requires that pressure to be observable,
	// and until this field existed the counters had no reader outside the
	// tests — a rule with no way to see it fire.
	Replay ReplayDiagnostics
	// Reverse is the reverse-state table's occupancy and, above all, who its
	// shared local quota refused.
	//
	// The table used to be absent from this snapshot entirely, which made it
	// the one component of the plane whose pressure could not be seen — and
	// the only one whose overflow REFUSES instead of evicting. A refusal there
	// does not slow a subsystem down, it stops it from asking at all.
	Reverse ReverseDiagnostics
}

// ReverseDiagnostics is what the reverse-state table reports.
//
// The three fields answer one question in three parts, and none of them
// answers it alone: how many exchanges are open, how much of the shared local
// quota is spoken for, and which of this node's own subsystems that quota has
// turned away. Occupancy without refusals cannot tell a busy node from a
// blocked one; refusals without occupancy cannot tell a tight quota from a
// burst.
type ReverseDiagnostics struct {
	// LocalRefusals counts capped refusals of this node's own requests, by
	// dtype. Keys are dtype names; an empty map means the shared quota has
	// never turned away a local request.
	LocalRefusals map[string]uint64
	// Held is the number of reverse records at this instant.
	Held int
	// LocalSlots is how many of the shared local-request slots are occupied.
	// Read it against Limits.Reverse.PerUpstreamCap.
	LocalSlots int
}

// ReplayDiagnostics is what the base anti-replay cache reports: what its
// counters saw, and how full it is right now.
//
// The occupancy travels beside the counters because neither answers an
// operator's question alone — "records refused for capacity" means one thing
// against a cache at its ceiling and another against a nearly empty one, and
// the ceiling itself is in Limits.
type ReplayDiagnostics struct {
	// Counters is the cache's lifetime counter set.
	Counters BaseReplayCacheMetrics
	// Held is the number of records the cache holds at this instant,
	// expired-but-retained ones included.
	Held int
}

// CollectDiagnostics assembles the snapshot. Every component is optional: a
// node that wired no queue reports a zero queue rather than refusing to
// answer, because a diagnostic that fails when a subsystem is absent is a
// diagnostic nobody calls.
func CollectDiagnostics(
	limits Limits,
	metrics *Metrics,
	admission *PeerAdmission,
	queue *WeightedQueue,
	replay *BaseReplayCache,
	reverse *ReverseTable,
) Diagnostics {
	diagnostics := Diagnostics{Limits: limits.Normalized(), Metrics: metrics.Snapshot()}
	if admission != nil {
		diagnostics.Admission = admission.Stats()
	}
	if queue != nil {
		diagnostics.Queue = queue.Stats()
	}
	if replay != nil {
		diagnostics.Replay = ReplayDiagnostics{Counters: replay.Metrics(), Held: replay.Len()}
	}
	if reverse != nil {
		diagnostics.Reverse = ReverseDiagnostics{
			Held:          reverse.Len(),
			LocalSlots:    reverse.LocalSlots(),
			LocalRefusals: reverseRefusalNames(reverse.LocalRefusals()),
		}
	}
	return diagnostics
}

// reverseRefusalNames renders the refusal tally for a reader. Always a non-nil
// map: "no local request has ever been refused" is a real and expected state,
// and `null` would read as "this build does not know".
func reverseRefusalNames(refusals map[domain.DType]uint64) map[string]uint64 {
	named := make(map[string]uint64, len(refusals))
	for dtype, count := range refusals {
		named[dtype.String()] = count
	}
	return named
}
