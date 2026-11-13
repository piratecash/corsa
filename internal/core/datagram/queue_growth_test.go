package datagram

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestQueueGrowthTracksEachLaneAndRemovalPath(t *testing.T) {
	const control = domain.DatagramClassControl
	const bulk = domain.DatagramClassBulk
	var elapsed time.Duration
	wall := func() time.Time { return time.Unix(0, int64(elapsed)) }
	queue := newWeightedQueueWithGrowthObservation(WeightedQueueConfig{Clock: wall}, func() (time.Duration, error) {
		return elapsed, nil
	})
	read := func() QueueGrowthSnapshot {
		t.Helper()
		s := queue.GrowthObservation()
		if s.UnknownReason != "" || s.Instance == ([16]byte{}) {
			t.Fatalf("queue observation unavailable: %+v", s)
		}
		return s
	}
	put := func(class domain.DatagramClass, until time.Time) {
		t.Helper()
		if !queue.Enqueue(queuedFrame(class, 128, until)) {
			t.Fatalf("enqueue %v refused", class)
		}
	}

	elapsed = time.Second
	put(control, wall().Add(time.Second))
	put(bulk, time.Time{})
	elapsed = 3 * time.Second
	if queue.Enqueue(queuedFrame(bulk, 0, time.Time{})) {
		t.Fatal("accepted invalid frame")
	}
	if s := read(); s.Control.Sequence != 1 || s.Bulk.Sequence != 1 || s.Control.LongestLower != elapsed || s.Bulk.LongestLower != elapsed {
		t.Fatalf("before dequeue: %+v", s)
	}
	// One dequeue removes the expired control head and dispatches bulk.
	if frame, ok := queue.Dequeue(); !ok || frame.Class != bulk {
		t.Fatalf("dequeue = %v, %t", frame.Class, ok)
	}
	if s := read(); s.Control.Depth != 0 || s.Bulk.Depth != 0 || s.Control.Sequence != 2 || s.Bulk.Sequence != 2 ||
		s.Control.RunHasIncrease || s.Bulk.RunHasIncrease || !s.Control.HasGrowingRun || s.Control.LongestLower != elapsed {
		t.Fatalf("after dequeue: %+v", s)
	}

	elapsed = 4 * time.Second
	put(control, time.Time{})
	put(control, wall().Add(time.Second)) // Expiry behind a live head.
	put(bulk, time.Time{})
	elapsed = 6 * time.Second
	if n := queue.DropExpired(); n != 1 {
		t.Fatalf("sweep removed %d frames", n)
	}
	if s := read(); s.Control.Depth != 1 || s.Control.Sequence != 5 || s.Control.RunHasIncrease ||
		s.Bulk.Depth != 1 || s.Bulk.Sequence != 3 || !s.Bulk.RunHasIncrease {
		t.Fatalf("after sweep: %+v", s)
	}
}

func TestQueueGrowthObservationClockFailureIsSticky(t *testing.T) {
	t.Run("ordinary queue", func(t *testing.T) {
		queue := NewWeightedQueue(WeightedQueueConfig{})
		if s := queue.GrowthObservation(); s.UnknownReason != "source_unavailable" || s.Instance != ([16]byte{}) {
			t.Fatalf("ordinary queue observation = %+v", s)
		}
	})
	for _, tc := range []struct {
		name   string
		now    time.Duration
		err    error
		reason string
	}{
		{"read failure", 2 * time.Second, errors.New("clock unavailable"), "duration_read_failed"},
		{"regression", 0, nil, "duration_regressed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Second
			var clockErr error
			queue := newWeightedQueueWithGrowthObservation(WeightedQueueConfig{}, func() (time.Duration, error) {
				return now, clockErr
			})
			now, clockErr = tc.now, tc.err
			if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 128, time.Time{})) {
				t.Fatal("clock failure affected queue admission")
			}
			now, clockErr = 3*time.Second, nil
			if s := queue.GrowthObservation(); s.UnknownReason != tc.reason {
				t.Fatalf("observation = %+v, want %s", s, tc.reason)
			}
			if _, ok := queue.Dequeue(); !ok {
				t.Fatal("clock failure affected dispatch")
			}
		})
	}
}

func TestQueueGrowthBracketsDurationAndSeparatesInstances(t *testing.T) {
	var now time.Duration
	clock := func() (time.Duration, error) {
		now += time.Second
		return now, nil
	}
	queue := newWeightedQueueWithGrowthObservation(WeightedQueueConfig{}, clock)
	if !queue.Enqueue(queuedFrame(domain.DatagramClassControl, 128, time.Time{})) {
		t.Fatal("enqueue refused")
	}
	s := queue.GrowthObservation()
	if s.UnknownReason != "" || s.Baseline != (QueueGrowthBoundary{time.Second, 2 * time.Second}) ||
		s.End != (QueueGrowthBoundary{5 * time.Second, 6 * time.Second}) ||
		s.Control.LongestLower != 3*time.Second || s.Control.LongestUpper != 5*time.Second || s.Bulk.HasGrowingRun {
		t.Fatalf("bracketed observation = %+v", s)
	}
	next := newWeightedQueueWithGrowthObservation(WeightedQueueConfig{}, clock).GrowthObservation()
	if next.UnknownReason != "" || next.Instance == s.Instance || next.Control.HasGrowingRun || next.Control.Depth != 0 {
		t.Fatalf("new instance = %+v, previous = %+v", next, s)
	}
}
