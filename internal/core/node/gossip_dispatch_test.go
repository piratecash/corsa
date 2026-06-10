package node

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Gossip-dispatch pool — the contract pinned here:
//
//   - pool down (gossipPoolUp false): dispatchGossipSend preserves the
//     historical goroutine-per-send behaviour (goBackground), so unit
//     tests and partially-wired Services are unaffected.
//   - pool up: jobs execute on the bounded workers, all of them.
//   - lane saturated: dispatch NEVER blocks the caller and NEVER falls
//     back to a goroutine (peers control both DM and notice volume);
//     the job is dropped and counted in gossipSendsDropped /
//     gossipNoticesDropped per lane.
//   - notices ride a dedicated lane, so a saturated DM lane does not
//     shed them.
//   - ctx cancelled: queued jobs are abandoned WITHOUT running (a job
//     can block up to a dial timeout — running the backlog would hold
//     WaitBackground hostage), both lanes are drained so abandoned
//     closures stop pinning frames, and late dispatchers drop instead
//     of spawning fallback goroutines.
//
// Panic semantics are intentionally NOT softened by the pool: the send
// helpers defer crashlog.DeferRecover, which logs and re-panics
// (crash-on-panic policy) — identical to the per-goroutine era.
// ---------------------------------------------------------------------------

func TestDispatchGossipSend_FallbackWithoutPool(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	done := make(chan struct{})
	svc.dispatchGossipSend(func() { close(done) })

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback path did not execute the job")
	}
	// goBackground tracks the job on backgroundWg — Wait must return.
	svc.backgroundWg.Wait()
}

func TestDispatchGossipSend_PoolExecutesAllJobs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc := &Service{}
	svc.startGossipDispatch(ctx)

	const jobs = 500
	var ran atomic.Int64
	executed := make(chan struct{}, jobs)
	for i := 0; i < jobs; i++ {
		svc.dispatchGossipSend(func() {
			ran.Add(1)
			executed <- struct{}{}
		})
	}

	deadline := time.After(5 * time.Second)
	for i := 0; i < jobs; i++ {
		select {
		case <-executed:
		case <-deadline:
			t.Fatalf("only %d/%d jobs executed before deadline", ran.Load(), jobs)
		}
	}
	if got := svc.gossipSendsDropped.Load(); got != 0 {
		t.Fatalf("expected no drops below queue capacity, got %d", got)
	}
}

func TestDispatchGossipSend_DropsWhenSaturatedWithoutBlocking(t *testing.T) {
	t.Parallel()

	// Pool "up" but with a zero-capacity queue and NO workers: every
	// dispatch must take the default branch immediately.
	svc := &Service{gossipJobs: make(chan func())}
	svc.gossipPoolUp.Store(true)

	doneCh := make(chan struct{})
	go func() {
		svc.dispatchGossipSend(func() { t.Error("dropped job must not run") })
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchGossipSend blocked on a saturated queue")
	}
	if got := svc.gossipSendsDropped.Load(); got != 1 {
		t.Fatalf("expected 1 dropped send, got %d", got)
	}
}

// Notices ride their own lane: a saturated DM lane must not shed them,
// and a saturated notice lane drops (counted separately) WITHOUT a
// goroutine fallback — inbound notices re-gossip, so an unbounded
// escape hatch would hand peers the goroutine storm back.
func TestDispatchGossipNoticeSend_DedicatedLaneAndNoGoroutineFallback(t *testing.T) {
	t.Parallel()

	// DM lane saturated (cap 0), notice lane has room.
	svc := &Service{
		gossipJobs:       make(chan func()),
		gossipNoticeJobs: make(chan func(), 1),
	}
	svc.gossipPoolUp.Store(true)

	svc.dispatchGossipNoticeSend(func() {})
	if n := len(svc.gossipNoticeJobs); n != 1 {
		t.Fatalf("notice not enqueued on its own lane despite DM saturation (lane len %d)", n)
	}
	if got := svc.gossipNoticesDropped.Load(); got != 0 {
		t.Fatalf("notice dropped with a non-full lane: %d", got)
	}

	// Notice lane now full: overflow is dropped, counted, and NOT run
	// on a fallback goroutine.
	svc.dispatchGossipNoticeSend(func() { t.Error("overflowed notice must not run") })
	if got := svc.gossipNoticesDropped.Load(); got != 1 {
		t.Fatalf("expected 1 dropped notice, got %d", got)
	}
	if got := svc.gossipSendsDropped.Load(); got != 0 {
		t.Fatalf("notice drop counted against the DM lane: %d", got)
	}
	svc.backgroundWg.Wait() // would catch a goroutine fallback
}

func TestGossipDispatch_ShutdownAbandonsQueuedJobsAndDrains(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{}
	svc.startGossipDispatch(ctx)

	// Occupy every worker with a job parked on a gate so subsequently
	// dispatched jobs are guaranteed to sit in the queue.
	gate := make(chan struct{})
	var occupied sync.WaitGroup
	occupied.Add(gossipSendWorkers)
	for i := 0; i < gossipSendWorkers; i++ {
		svc.dispatchGossipSend(func() {
			occupied.Done()
			<-gate
		})
	}
	occupied.Wait()

	var ran atomic.Int64
	const queued = 100
	for i := 0; i < queued; i++ {
		svc.dispatchGossipSend(func() { ran.Add(1) })
	}
	// Feed the notice lane as well so the post-shutdown assertions
	// cover the supervisor draining BOTH lanes. (Notice workers are
	// idle and may run some of these no-ops before cancel — fine; the
	// contract is only that the lane is EMPTY once teardown finishes.)
	for i := 0; i < 10; i++ {
		svc.dispatchGossipNoticeSend(func() {})
	}

	cancel()
	close(gate)

	exited := make(chan struct{})
	go func() {
		svc.backgroundWg.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("workers/supervisor did not exit after cancel — shutdown executed the backlog?")
	}

	if got := ran.Load(); got != 0 {
		t.Fatalf("%d queued jobs ran after shutdown; want 0 (abandoned)", got)
	}
	if n := len(svc.gossipJobs); n != 0 {
		t.Fatalf("DM lane not drained at shutdown: %d closures still pinned", n)
	}
	if n := len(svc.gossipNoticeJobs); n != 0 {
		t.Fatalf("notice lane not drained at shutdown: %d closures still pinned", n)
	}

	// Late dispatch after teardown: dropped — neither enqueued nor run
	// on a fallback goroutine, on either lane.
	svc.dispatchGossipSend(func() { t.Error("post-shutdown job must not run") })
	svc.dispatchGossipNoticeSend(func() { t.Error("post-shutdown notice must not run") })
	if n := len(svc.gossipJobs) + len(svc.gossipNoticeJobs); n != 0 {
		t.Fatalf("post-shutdown dispatch was enqueued (%d) instead of dropped", n)
	}
	svc.backgroundWg.Wait() // would catch a goBackground fallback
}
