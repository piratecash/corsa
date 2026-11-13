package datagram

import (
	"crypto/rand"
	"time"

	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// QueueGrowthBoundary brackets a change or read under the queue mutex in the
// local since-boot time domain.
type QueueGrowthBoundary struct {
	Earliest, Latest time.Duration
}

// QueueLaneGrowth retains the current nondecreasing run and bounds for the
// longest run with at least one increase since the queue's baseline. The lower
// and upper maxima may come from different runs.
type QueueLaneGrowth struct {
	Depth          int
	Sequence       uint64
	RunStart       QueueGrowthBoundary
	RunHasIncrease bool
	HasGrowingRun  bool
	LongestLower   time.Duration
	LongestUpper   time.Duration
}

// QueueGrowthSnapshot covers the whole prefix from Baseline to End for this
// queue instance. It has no UTC, process or artifact attribution. A nonempty
// UnknownReason invalidates the prefix. It cannot recover arbitrary subwindows.
type QueueGrowthSnapshot struct {
	Instance      [16]byte
	Baseline, End QueueGrowthBoundary
	Control, Bulk QueueLaneGrowth
	UnknownReason string
}

type queueGrowthObservation struct {
	clock func() (time.Duration, error)
	state QueueGrowthSnapshot
}

// NewWeightedQueueWithGrowthObservation opts into the inactive K12 source.
// A clock failure leaves the readout unknown and the queue available.
func NewWeightedQueueWithGrowthObservation(cfg WeightedQueueConfig) *WeightedQueue {
	return newWeightedQueueWithGrowthObservation(cfg, trustedclock.SinceBoot)
}

func newWeightedQueueWithGrowthObservation(cfg WeightedQueueConfig, clock func() (time.Duration, error)) *WeightedQueue {
	q := NewWeightedQueue(cfg)
	o := &queueGrowthObservation{clock: clock}
	q.growth = o
	if _, err := rand.Read(o.state.Instance[:]); err != nil {
		o.state.UnknownReason = "source_identity_unavailable"
		return q
	}
	start, err := clock()
	if !o.boundary(start, err) {
		return q
	}
	o.state.Baseline = o.state.End
	o.state.Control.RunStart = o.state.Baseline
	o.state.Bulk.RunStart = o.state.Baseline
	return q
}

func (o *queueGrowthObservation) boundary(start time.Duration, startErr error) bool {
	if o.state.UnknownReason != "" {
		return false
	}
	end, endErr := o.clock()
	if startErr != nil || endErr != nil || start < 0 {
		o.state.UnknownReason = "duration_read_failed"
		return false
	}
	if start < o.state.End.Latest || end < start {
		o.state.UnknownReason = "duration_regressed"
		return false
	}
	o.state.End = QueueGrowthBoundary{Earliest: start, Latest: end}
	return true
}

// Each owner transaction can only add to one lane or remove from the lanes.
// Comparing actual depths under mu therefore preserves every decrease even
// when the expiry and dispatch in one Dequeue share a boundary.
func (q *WeightedQueue) recordGrowthLocked(start time.Duration, err error) {
	o := q.growth
	control, bulk := len(q.lanes[0].items), len(q.lanes[1].items)
	if control == o.state.Control.Depth && bulk == o.state.Bulk.Depth {
		return
	}
	if !o.boundary(start, err) {
		return
	}
	if !o.state.Control.record(control, o.state.End) || !o.state.Bulk.record(bulk, o.state.End) {
		o.state.UnknownReason = "sequence_overflow"
	}
}

func (l *QueueLaneGrowth) record(depth int, end QueueGrowthBoundary) bool {
	if depth == l.Depth {
		return true
	}
	if l.Sequence == ^uint64(0) {
		return false
	}
	l.Sequence++
	if depth < l.Depth {
		l.extend(end)
		l.RunStart, l.RunHasIncrease = end, false
	} else {
		l.RunHasIncrease = true
	}
	l.Depth = depth
	return true
}

func (l *QueueLaneGrowth) extend(end QueueGrowthBoundary) {
	if !l.RunHasIncrease {
		return
	}
	l.HasGrowingRun = true
	l.LongestLower = max(l.LongestLower, end.Earliest-l.RunStart.Latest)
	l.LongestUpper = max(l.LongestUpper, end.Latest-l.RunStart.Earliest)
}

// GrowthObservation reads both lanes at one boundary. Ordinary queues report
// source_unavailable. This readout is separate from the diagnostic RPC.
func (q *WeightedQueue) GrowthObservation() QueueGrowthSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	o := q.growth
	if o == nil {
		return QueueGrowthSnapshot{UnknownReason: "source_unavailable"}
	}
	if o.state.UnknownReason == "" &&
		(o.state.Control.Depth != len(q.lanes[0].items) || o.state.Bulk.Depth != len(q.lanes[1].items)) {
		o.state.UnknownReason = "actual_depth_mismatch"
	}
	start, err := o.clock()
	if !o.boundary(start, err) {
		return o.state
	}
	out := o.state
	out.Control.extend(out.End)
	out.Bulk.extend(out.End)
	return out
}
