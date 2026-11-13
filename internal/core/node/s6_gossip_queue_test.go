package node

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestS6GossipQueueObservationCountsBothLanesThroughDrain(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	svc, readouts, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &key, GossipQueueDepth: true})
	if err != nil {
		t.Fatal(err)
	}
	if reason := readouts.Pending.GossipQueues().UnknownReason; reason != "pool_not_started" {
		t.Fatalf("before start: %q", reason)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	svc.startGossipDispatch(ctx)
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		cancel()
		release()
		svc.WaitBackground()
	})
	occupy := func(workers int, dispatch func(func())) {
		started := make(chan struct{}, workers)
		for range workers {
			dispatch(func() { started <- struct{}{}; <-gate })
		}
		for range workers {
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("workers did not take their jobs")
			}
		}
	}
	occupy(gossipSendWorkers, svc.dispatchGossipSend)
	occupy(gossipNoticeWorkers, svc.dispatchGossipNoticeSend)

	abandoned := func() { t.Error("job ran after cancellation") }
	for range 3 {
		svc.dispatchGossipSend(abandoned)
	}
	for range 2 {
		svc.dispatchGossipNoticeSend(abandoned)
	}
	before := readouts.Pending.GossipQueues()
	if before.Send.Depth != 3 || before.Notice.Depth != 2 ||
		before.Send.Taken != gossipSendWorkers || before.Notice.Taken != gossipNoticeWorkers ||
		before.Send.Instance == before.Notice.Instance ||
		before.Send.UnknownReason != "" || before.Notice.UnknownReason != "" {
		t.Fatalf("before drain: %+v", before)
	}
	cancel()
	release()
	svc.WaitBackground()
	svc.dispatchGossipSend(abandoned)
	svc.dispatchGossipNoticeSend(abandoned)
	after := readouts.Pending.GossipQueues()
	for _, pair := range [][2]GossipQueueLaneSnapshot{{before.Send, after.Send}, {before.Notice, after.Notice}} {
		prev, final := pair[0], pair[1]
		if !final.Stopped || final.Depth != 0 || final.UnknownReason != "" ||
			final.Instance != prev.Instance || final.At < prev.At ||
			final.Enqueued != prev.Enqueued || final.Taken != prev.Taken || final.Drained != uint64(prev.Depth) {
			t.Fatalf("drain lost queue accounting: before=%+v after=%+v", prev, final)
		}
	}
}

func TestS6GossipQueueObservationOptInAndSaturation(t *testing.T) {
	cfg, id := pendingConstructionInputs(t)
	key := pendingConstructionKey()
	ordinary, readouts, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{Pending: &key})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ordinary.WaitBackground)
	if ordinary.gossipQueueObservation != nil || readouts.Pending.GossipQueues().UnknownReason != "source_unavailable" {
		t.Fatal("gossip observation active without explicit opt-in")
	}
	if _, _, err := NewServiceWithObservations(cfg, id, nil, ObservationOptions{GossipQueueDepth: true}); err == nil {
		t.Fatal("gossip observation accepted without a pending observation key")
	}
	var lane gossipQueueLane
	lane.start()
	jobs := make(chan func(), 1)
	job := func() {}
	if lane.offer(jobs, job) != gossipEnqueued || lane.offer(jobs, job) != gossipQueueFull {
		t.Fatal("expected one admission and one saturated offer")
	}
	if cut := lane.snapshot(jobs); cut.Depth != 1 || cut.Enqueued != 1 || cut.UnknownReason != "" {
		t.Fatalf("refusal changed depth: %+v", cut)
	}
	<-jobs // Unaccounted removal must invalidate continuity.
	if cut := lane.snapshot(jobs); cut.UnknownReason != "queue_coverage_mismatch" {
		t.Fatalf("untracked removal accepted: %+v", cut)
	}
}

func TestS6GossipQueueGrowthKeepsDecreaseTimeBetweenCuts(t *testing.T) {
	history := func(decreaseAt time.Duration) GossipQueueLaneSnapshot {
		t.Helper()
		var now time.Duration
		lane := gossipQueueLane{queueDepthEvidence: queueDepthEvidence{clock: func() (time.Duration, error) { return now, nil }}}
		lane.start()
		jobs := make(chan func(), 2)
		job := func() {}
		now = time.Hour
		if lane.offer(jobs, job) != gossipEnqueued {
			t.Fatal("first job was refused")
		}
		first := lane.snapshot(jobs)
		now = decreaseAt
		if _, ok := lane.take(t.Context(), jobs); !ok {
			t.Fatal("job was not taken")
		}
		now = 26 * time.Hour
		if lane.offer(jobs, job) != gossipEnqueued {
			t.Fatal("second job was refused")
		}
		now = 27 * time.Hour
		final := lane.snapshot(jobs)
		if first.Depth != 1 || final.Depth != 1 || final.Enqueued != 2 || final.Taken != 1 ||
			final.Growth.RunStart.Earliest != decreaseAt || final.UnknownReason != "" {
			t.Fatalf("unexpected cuts: first=%+v final=%+v", first, final)
		}
		lane.stop(jobs)
		stopped := lane.snapshot(jobs)
		now += time.Hour
		if next := lane.snapshot(jobs); next != stopped || !next.Stopped {
			t.Fatalf("stopped cut extended: before=%+v after=%+v", stopped, next)
		}
		return final
	}
	long := history(25 * time.Hour)
	short := history(10 * time.Hour)
	if long.Growth.LongestLower != 25*time.Hour || long.Growth.LongestUpper != 25*time.Hour ||
		short.Growth.LongestLower != 17*time.Hour || short.Growth.LongestUpper != 17*time.Hour {
		t.Fatalf("decrease timing lost: long=%+v short=%+v", long.Growth, short.Growth)
	}
}

func TestS6GossipQueueGrowthRejectsInvalidClockWithoutLosingQueue(t *testing.T) {
	for _, mode := range []string{"failed", "regressed"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Hour
			var fault error
			lane := gossipQueueLane{queueDepthEvidence: queueDepthEvidence{clock: func() (time.Duration, error) { return now, fault }}}
			lane.start()
			jobs := make(chan func(), 1)
			if mode == "failed" {
				fault = errors.New("duration provider unavailable")
			} else {
				now--
			}
			if lane.offer(jobs, func() {}) != gossipEnqueued {
				t.Fatal("observer error affected the queue")
			}
			fault, now = nil, 2*time.Hour
			snapshot := lane.snapshot(jobs)
			if snapshot.Depth != 1 || snapshot.Enqueued != 1 ||
				snapshot.UnknownReason != "clock_unavailable_or_regressed" {
				t.Fatalf("invalid clock acquired coverage: %+v", snapshot)
			}
		})
	}
}
