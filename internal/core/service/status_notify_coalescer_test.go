package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestStatusNotifyCoalescer_CollapsesBurst verifies that a burst of Signal()
// calls within one window collapses into a single fn invocation — the core
// UI-freeze guard: NotifyStatusChanged (a full NodeStatus deep-copy) must not
// run per event under a status-change storm.
func TestStatusNotifyCoalescer_CollapsesBurst(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	const interval = 30 * time.Millisecond
	c := NewStatusNotifyCoalescer(interval, func() { calls.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	// Fire a tight burst far larger than any plausible per-window event count.
	for i := 0; i < 200; i++ {
		c.Signal()
	}

	// Wait out a couple of windows so the (single) coalesced fire lands.
	time.Sleep(4 * interval)

	if got := calls.Load(); got < 1 {
		t.Fatalf("coalescer never fired for a burst of 200 signals")
	} else if got > 2 {
		t.Fatalf("burst of 200 signals fired fn %d times; expected ~1 (coalescing broken)", got)
	}
}

// TestStatusNotifyCoalescer_DeliversFinalState verifies a single isolated
// Signal still fires exactly once — the trailing state is never dropped.
func TestStatusNotifyCoalescer_DeliversFinalState(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	const interval = 30 * time.Millisecond
	c := NewStatusNotifyCoalescer(interval, func() { calls.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.Signal()
	time.Sleep(4 * interval)

	if got := calls.Load(); got != 1 {
		t.Fatalf("single Signal fired fn %d times, want 1", got)
	}
}

// TestStatusNotifyCoalescer_StopsOnCtxCancel verifies Run returns when the
// context is cancelled and does not fire afterward.
func TestStatusNotifyCoalescer_StopsOnCtxCancel(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	const interval = 20 * time.Millisecond
	c := NewStatusNotifyCoalescer(interval, func() { calls.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	// Signals after shutdown must not fire fn (loop has exited).
	before := calls.Load()
	c.Signal()
	time.Sleep(3 * interval)
	if got := calls.Load(); got != before {
		t.Fatalf("fn fired after ctx cancel: before=%d after=%d", before, got)
	}
}
