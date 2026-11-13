package node

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// GossipQueueSnapshot reports two independent cuts. Taken counts removal by
// a worker even if cancellation prevents the worker from running the job.
type GossipQueueSnapshot struct {
	Send, Notice  GossipQueueLaneSnapshot
	UnknownReason string
}

type GossipQueueLaneSnapshot = QueueDepthSnapshot

// QueueDepthSnapshot describes the accounting for one diagnostic queue instance.
type QueueDepthSnapshot struct {
	Instance                 [16]byte
	OpenedAt, At             time.Duration
	Depth                    int
	Enqueued, Taken, Drained uint64
	Stopped                  bool
	UnknownReason            string
	Growth                   GossipQueueGrowth
}

type GossipQueueBoundary struct {
	Earliest, Latest time.Duration
}

// GossipQueueGrowth covers one lane from its creation. A growing run is
// nondecreasing and contains at least one increase. The longest lower and
// upper bounds may come from different runs.
type GossipQueueGrowth struct {
	RunStart, Cut              GossipQueueBoundary
	CurrentHasIncrease         bool
	HasGrowingRun              bool
	LongestLower, LongestUpper time.Duration
}

func (g *GossipQueueGrowth) extend() {
	if g.CurrentHasIncrease {
		g.HasGrowingRun = true
		g.LongestLower = max(g.LongestLower, g.Cut.Earliest-g.RunStart.Latest)
		g.LongestUpper = max(g.LongestUpper, g.Cut.Latest-g.RunStart.Earliest)
	}
}

type gossipQueueObservation struct {
	send, notice gossipQueueLane
}

// All observed channel operations are nonblocking under mu. A worker waits
// outside mu on the shared notification for an empty lane. The pool lock,
// when needed, is taken before mu. Job functions never run under either lock.
type gossipQueueLane struct {
	mu      sync.Mutex
	changed chan struct{}
	queueDepthEvidence
}

// The queue owner serializes both its channel operations and these updates.
type queueDepthEvidence struct {
	state QueueDepthSnapshot
	clock func() (time.Duration, error)
}

func (o *queueDepthEvidence) start() {
	if _, err := rand.Read(o.state.Instance[:]); err != nil || o.state.Instance == ([16]byte{}) {
		o.invalidateLocked("source_identity_unavailable")
	}
	if o.clock == nil {
		o.clock = trustedclock.SinceBoot
	}
	at, err := o.clock()
	if err != nil || at < 0 {
		o.invalidateLocked("clock_unavailable")
		return
	}
	o.state.OpenedAt, o.state.At = at, at
	baseline := GossipQueueBoundary{Earliest: at, Latest: at}
	o.state.Growth.RunStart, o.state.Growth.Cut = baseline, baseline
}

func (o *queueDepthEvidence) invalidateLocked(reason string) {
	if o.state.UnknownReason == "" {
		o.state.UnknownReason = reason
	}
}

func (o *queueDepthEvidence) recordLocked(counter *uint64) {
	if *counter == ^uint64(0) {
		o.invalidateLocked("counter_overflow")
		return
	}
	(*counter)++
}

func (o *queueDepthEvidence) cutLocked(before time.Duration, beforeErr error) bool {
	if o.state.UnknownReason != "" {
		return false
	}
	after, err := o.clock()
	if beforeErr != nil || err != nil || before < o.state.At || after < before {
		o.invalidateLocked("clock_unavailable_or_regressed")
		return false
	}
	o.state.At = after
	o.state.Growth.Cut = GossipQueueBoundary{Earliest: before, Latest: after}
	return true
}

func (o *queueDepthEvidence) changeLocked(depth int, before time.Duration, beforeErr error) {
	old := o.state.Depth
	o.state.Depth = depth
	if !o.cutLocked(before, beforeErr) {
		return
	}
	g := &o.state.Growth
	if depth < old {
		g.extend()
		g.RunStart, g.CurrentHasIncrease = g.Cut, false
	} else if depth > old {
		g.CurrentHasIncrease = true
	}
}

func (o *gossipQueueLane) offer(jobs chan<- func(), job func()) gossipEnqueueResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(jobs) == cap(jobs) {
		return gossipQueueFull
	}
	before, err := o.clock()
	select {
	case jobs <- job:
		o.recordLocked(&o.state.Enqueued)
		o.changeLocked(len(jobs), before, err)
		if o.changed != nil {
			close(o.changed)
			o.changed = nil
		}
		return gossipEnqueued
	default:
		return gossipQueueFull
	}
}

func (o *gossipQueueLane) take(ctx context.Context, jobs <-chan func()) (func(), bool) {
	for {
		o.mu.Lock()
		if ctx.Err() != nil || o.state.Stopped {
			o.mu.Unlock()
			return nil, false
		}
		if len(jobs) > 0 {
			before, err := o.clock()
			job := <-jobs
			o.recordLocked(&o.state.Taken)
			o.changeLocked(len(jobs), before, err)
			o.mu.Unlock()
			return job, true
		}
		if o.changed == nil {
			o.changed = make(chan struct{})
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-changed:
		}
	}
}

func (o *gossipQueueLane) stop(jobs <-chan func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// The supervisor excludes enqueues. Workers share mu with the drain.
	before, err := o.clock()
	for len(jobs) != 0 {
		<-jobs
		o.recordLocked(&o.state.Drained)
	}
	o.changeLocked(0, before, err)
	o.state.Stopped = true
}

func (o *gossipQueueLane) snapshot(jobs <-chan func()) GossipQueueLaneSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.snapshotLocked(len(jobs))
}

func (o *queueDepthEvidence) snapshotLocked(depth int) QueueDepthSnapshot {
	v := o.state
	if v.Taken > v.Enqueued || v.Drained > v.Enqueued-v.Taken ||
		v.Enqueued-v.Taken-v.Drained != uint64(v.Depth) || v.Depth != depth || v.Stopped && v.Depth != 0 {
		o.invalidateLocked("queue_coverage_mismatch")
	}
	if !v.Stopped && o.state.UnknownReason == "" {
		before, err := o.clock()
		o.cutLocked(before, err)
	}
	out := o.state
	out.Growth.extend()
	return out
}
