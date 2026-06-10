package node

import (
	"context"

	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Bounded gossip-dispatch pool — replaces goroutine-per-target fan-out.
//
// Background: executeGossipTargets / gossipNotice / gossipReceipt used to
// spawn one goroutine PER TARGET per message, and retryRelayDeliveries
// (every ~2s) additionally spawned one goroutine PER RETRYABLE MESSAGE.
// With a relay backlog of N messages and T gossip targets a single retry
// tick created N×(1+T) concurrent goroutines — six figures under storm.
// The goroutines finish, but the Go runtime never frees goroutine
// descriptors (they parked in the scheduler free-list forever) and keeps
// the stack high-water mark, so every storm permanently ratcheted
// mem_sys/heap (observed: ~83 MB of runtime.malg and 2.1 GB mem_sys on a
// node with 48 live goroutines). See heap pprof from 2026-06-10.
//
// The pool bounds peak goroutine count to gossipSendWorkers +
// gossipNoticeWorkers — there is deliberately NO goroutine fallback on
// saturation anywhere: peers control both DM volume (relay traffic) and
// notice volume (inbound authenticated push_notice re-gossips through
// handleInboundPushNotice → gossipNotice, up to the command limiter), so
// any unbounded escape hatch hands remote peers the goroutine storm
// back. Every job is fire-and-forget (sendGossipFrameToPeer /
// sendNoticeToPeer / sendReceiptToPeer — all non-blocking enqueues or
// deadline-bounded writes). Saturation policy per traffic class:
//
//   - DM gossip and receipts (dispatchGossipSend, gossipJobs lane):
//     DROPPED on overflow. Both are re-covered by the 2s
//     retryRelayDeliveries cycle, which re-routes every
//     still-undelivered message/receipt — a dropped send is a 2-second
//     delay, not a loss.
//   - push_notice fan-out (dispatchGossipNoticeSend, gossipNoticeJobs
//     lane): notices have NO retry path of their own (fetch_notices is
//     a passive pull), so instead of a lossy shared queue they get a
//     DEDICATED lane that DM storms cannot saturate. Overflowing the
//     notice lane itself takes a notice flood — peer-driven abuse
//     traffic — and shedding under abuse is the same posture the
//     command limiter already takes. Drops are counted in
//     gossipNoticesDropped.
// ---------------------------------------------------------------------------

const (
	// gossipSendWorkers bounds peak concurrency of DM/receipt gossip
	// sends. Sends are short (non-blocking session enqueue,
	// inbound-direct write under the writer deadline, or in-memory
	// pending-ring append), so a small pool sustains far more
	// throughput than peers can absorb.
	gossipSendWorkers = 32

	// gossipSendQueueCap bounds the DM/receipt dispatch backlog. Sized
	// to absorb a full retry tick on a healthy node (hundreds of
	// messages × ~10 targets) while capping worst-case pinned memory to
	// a few hundred KB of closures. Overflow is dropped (file header).
	gossipSendQueueCap = 8192

	// gossipNoticeWorkers serves the dedicated notice lane. Notice
	// fan-out is routingTargets-wide but event-scale in frequency;
	// a handful of workers clears it faster than the command limiter
	// admits new inbound notices.
	gossipNoticeWorkers = 4

	// gossipNoticeQueueCap bounds the notice lane backlog: ~a hundred
	// concurrent notices × a typical target count. Overflow means a
	// notice flood (abuse) and is shed, counted in
	// gossipNoticesDropped.
	gossipNoticeQueueCap = 1024
)

// startGossipDispatch launches both worker lanes. Called once from Run
// before bootstrapLoop starts (the first retryRelayDeliveries tick must
// find the pool up). Workers are goBackground-tracked and exit on ctx
// cancellation; jobs still queued at shutdown are abandoned, which is
// safe for fire-and-forget traffic.
func (s *Service) startGossipDispatch(ctx context.Context) {
	s.gossipJobs = make(chan func(), gossipSendQueueCap)
	s.gossipNoticeJobs = make(chan func(), gossipNoticeQueueCap)
	s.startGossipWorkers(ctx, gossipSendWorkers, s.gossipJobs)
	s.startGossipWorkers(ctx, gossipNoticeWorkers, s.gossipNoticeJobs)

	// Shutdown supervisor: flip the pool to "down" so late dispatchers
	// drop instead of enqueueing, then drain the abandoned closures so
	// they stop pinning frames/ciphertexts for as long as the Service
	// value outlives ctx.
	//
	// The flag flip AND the drains happen under gossipPoolMu.Lock while
	// every enqueue holds gossipPoolMu.RLock: any dispatcher that read
	// shutdown=false has finished its enqueue before the drain starts,
	// and every dispatcher arriving after the Lock observes
	// shutdown=true and drops. No closure can be parked post-drain.
	s.goBackground(func() {
		<-ctx.Done()
		s.gossipPoolMu.Lock()
		defer s.gossipPoolMu.Unlock()
		s.gossipPoolShutdown.Store(true)
		drainGossipJobs(s.gossipJobs)
		drainGossipJobs(s.gossipNoticeJobs)
	})

	// Publish AFTER the channels exist and workers are launched so a
	// concurrent dispatcher can never see the flag without the
	// channels.
	s.gossipPoolUp.Store(true)
}

// startGossipWorkers launches n identical workers draining jobs until
// ctx is cancelled.
func (s *Service) startGossipWorkers(ctx context.Context, n int, jobs <-chan func()) {
	for i := 0; i < n; i++ {
		s.goBackground(func() {
			for {
				// Shutdown takes priority over more work: a bare
				// two-case select picks RANDOMLY when both cases are
				// ready, so under a saturated queue workers could keep
				// executing jobs long after cancellation.
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case job := <-jobs:
					if ctx.Err() != nil {
						// Drew a job in the same instant the pool was
						// cancelled: abandon it WITHOUT running.
						// Gossip is fire-and-forget, and a send can
						// block up to a dial/handshake timeout
						// (sendNoticeToPeer), which would hold
						// WaitBackground hostage at shutdown.
						return
					}
					// No recover wrapper here on purpose: the send
					// helpers defer crashlog.DeferRecover themselves,
					// and DeferRecover logs + RE-PANICS (crash-on-
					// panic policy), exactly as it did when each send
					// ran on its own goroutine.
					job()
				}
			}
		})
	}
}

// drainGossipJobs empties a job lane without running anything. Caller
// must hold gossipPoolMu.Lock with gossipPoolShutdown already set.
func drainGossipJobs(ch chan func()) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// gossipEnqueueResult is the outcome of tryEnqueueGossipJob; the two
// dispatch variants differ only in which lane they use and how they
// account for overflow.
type gossipEnqueueResult int

const (
	gossipEnqueued gossipEnqueueResult = iota
	gossipQueueFull
	gossipPoolShutdownDrop
)

// dispatchGossipSend runs a fire-and-forget DM/receipt gossip send on
// the bounded pool, shedding the job when the lane is saturated (the
// relay retry cycle re-covers it). While the pool is down (unit tests,
// partially-wired Services, pre-Run) it preserves the historical
// goroutine-per-send behaviour via goBackground, so existing test
// contracts — including backgroundWg tracking — are unchanged outside
// Run.
func (s *Service) dispatchGossipSend(job func()) {
	switch s.tryEnqueueGossipJob(s.gossipJobs, job) {
	case gossipQueueFull:
		dropped := s.gossipSendsDropped.Add(1)
		log.Debug().Uint64("dropped_total", dropped).Msg("gossip_dispatch_saturated_send_dropped")
	case gossipPoolShutdownDrop:
		// Teardown: silently abandoned, matching the historical
		// behaviour of cancelling the per-send goroutine.
	}
}

// dispatchGossipNoticeSend routes push_notice fan-out through the
// dedicated notice lane, which DM storms cannot saturate. Overflow
// means a notice flood and is shed — deliberately NOT a goroutine
// fallback: inbound authenticated notices re-gossip through this path,
// so an unbounded escape hatch would hand remote peers the goroutine
// storm this file exists to prevent.
func (s *Service) dispatchGossipNoticeSend(job func()) {
	switch s.tryEnqueueGossipJob(s.gossipNoticeJobs, job) {
	case gossipQueueFull:
		dropped := s.gossipNoticesDropped.Add(1)
		log.Warn().Uint64("dropped_total", dropped).Msg("gossip_dispatch_notice_lane_saturated_send_dropped")
	case gossipPoolShutdownDrop:
		// Teardown: never spawn fresh goroutines after shutdown began.
	}
}

// tryEnqueueGossipJob hands job to the given lane, or to the pre-pool
// goBackground fallback when the pool never started (unit tests,
// partially-wired Services — historical per-send goroutine, tracked on
// backgroundWg, reported as gossipEnqueued). Always non-blocking.
func (s *Service) tryEnqueueGossipJob(lane chan func(), job func()) gossipEnqueueResult {
	if !s.gossipPoolUp.Load() {
		s.goBackground(job)
		return gossipEnqueued
	}
	// RLock pairs with the supervisor's Lock: enqueues are excluded
	// from the shutdown flag-flip + drain critical section, so nothing
	// can be parked in a lane after its drain (see supervisor).
	s.gossipPoolMu.RLock()
	defer s.gossipPoolMu.RUnlock()
	if s.gossipPoolShutdown.Load() {
		return gossipPoolShutdownDrop
	}
	select {
	case lane <- job:
		return gossipEnqueued
	default:
		return gossipQueueFull
	}
}
