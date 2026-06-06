package service

import (
	"context"
	"sync/atomic"
	"time"
)

// defaultStatusNotifyCoalesceInterval bounds how often the coalesced status
// notify fires. NotifyStatusChanged deep-copies the entire NodeStatus, so on a
// large mesh (hundreds of nodes) where peer counts / health / traffic change
// continuously, forwarding every NodeStatusMonitor change straight through
// would deep-copy on every event and stall the UI (profiling flagged
// deepCopyNodeStatus as the top allocator). 100 ms collapses a burst into one
// rebuild while staying well below human-perceptible latency for a status pane.
const defaultStatusNotifyCoalesceInterval = 100 * time.Millisecond

// StatusNotifyCoalescer collapses a burst of status-change signals into at most
// one downstream notify per interval. Signal() is cheap, non-blocking and
// safe to call from any ebus handler goroutine (it only sets a flag and a
// non-blocking wake); a single Run loop invokes fn at the bounded rate.
//
// Trailing-edge debounce: the first Signal after an idle period arms the
// window, fn fires once when the window elapses (covering every Signal that
// landed in it), and the final state is never dropped. Under sustained
// signalling fn fires at most once per interval.
type StatusNotifyCoalescer struct {
	fn       func()
	interval time.Duration
	wake     chan struct{}
	dirty    atomic.Bool
}

// NewStatusNotifyCoalescer builds a coalescer that calls fn at most once per
// interval. A non-positive interval falls back to the default.
func NewStatusNotifyCoalescer(interval time.Duration, fn func()) *StatusNotifyCoalescer {
	if interval <= 0 {
		interval = defaultStatusNotifyCoalesceInterval
	}
	return &StatusNotifyCoalescer{
		fn:       fn,
		interval: interval,
		wake:     make(chan struct{}, 1),
	}
}

// Signal records that the status changed and wakes the Run loop. Never blocks:
// the wake channel has capacity 1 and extra sends fall through the default.
func (c *StatusNotifyCoalescer) Signal() {
	c.dirty.Store(true)
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Run drives the coalescing loop until ctx is cancelled. Exactly one goroutine
// should call Run. On each wake it waits one interval (coalescing any further
// Signals that land in the window) and then fires fn once if anything is
// pending. fn runs on this goroutine, so a slow fn (the NodeStatus deep-copy)
// throttles itself rather than fanning out one goroutine per event.
func (c *StatusNotifyCoalescer) Run(ctx context.Context) {
	timer := time.NewTimer(c.interval)
	timer.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		// Coalesce: wait out the quiet window so a burst becomes one fire.
		timer.Reset(c.interval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if c.dirty.Swap(false) {
			c.fn()
		}
	}
}
