package ebus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishAsync(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	var called atomic.Bool
	done := make(chan struct{})

	b.Subscribe("test.event", func(msg string, count int) {
		if msg != "hello" || count != 42 {
			t.Errorf("unexpected args: %q, %d", msg, count)
		}
		called.Store(true)
		close(done)
	})

	b.Publish("test.event", "hello", 42)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within timeout")
	}

	if !called.Load() {
		t.Fatal("handler was not called")
	}
}

func TestPublishSync(t *testing.T) {
	t.Parallel()
	b := New()

	var called bool
	b.Subscribe("sync.event", func(val int) {
		called = true
		if val != 7 {
			t.Errorf("expected 7, got %d", val)
		}
	}, WithSync())

	b.Publish("sync.event", 7)

	// Sync handler runs inline — must be called before Publish returns.
	if !called {
		t.Fatal("sync handler was not called before Publish returned")
	}
}

func TestMultipleSubscribers(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	var count atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		b.Subscribe("multi", func() {
			count.Add(1)
			wg.Done()
		})
	}

	b.Publish("multi")
	wg.Wait()

	if count.Load() != 5 {
		t.Fatalf("expected 5 calls, got %d", count.Load())
	}
}

func TestNoSubscribers(t *testing.T) {
	t.Parallel()
	b := New()

	// Should not panic.
	b.Publish("nobody.listens", "data")
}

func TestHandlerPanicDoesNotCrashPublisher(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	done := make(chan struct{})

	b.Subscribe("panic.event", func() {
		panic("boom")
	})
	b.Subscribe("panic.event", func() {
		close(done)
	})

	b.Publish("panic.event")

	select {
	case <-done:
		// Second handler ran despite first panicking.
	case <-time.After(2 * time.Second):
		t.Fatal("second handler was not called after first panicked")
	}
}

func TestShutdownWaitsForHandlers(t *testing.T) {
	t.Parallel()
	b := New()

	var finished atomic.Bool

	b.Subscribe("slow", func() {
		time.Sleep(100 * time.Millisecond)
		finished.Store(true)
	})

	b.Publish("slow")
	b.Shutdown()

	if !finished.Load() {
		t.Fatal("Shutdown returned before handler finished")
	}
}

func TestDifferentTopicsIsolated(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	var aCalled, bCalled atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)

	b.Subscribe("topic.a", func() {
		aCalled.Store(true)
		wg.Done()
	})
	b.Subscribe("topic.b", func() {
		bCalled.Store(true)
	})

	b.Publish("topic.a")
	wg.Wait()

	if !aCalled.Load() {
		t.Fatal("topic.a handler was not called")
	}
	if bCalled.Load() {
		t.Fatal("topic.b handler should not have been called")
	}
}

func TestNilBusSafe(t *testing.T) {
	t.Parallel()
	var b *Bus

	// None of these should panic on nil receiver.
	b.Subscribe("any", func() {})
	b.Publish("any")
	b.Shutdown()
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	b := New()

	var count atomic.Int32
	id := b.Subscribe("unsub", func() {
		count.Add(1)
	}, WithSync())

	b.Publish("unsub")
	if count.Load() != 1 {
		t.Fatalf("expected 1 call before unsubscribe, got %d", count.Load())
	}

	b.Unsubscribe(id)
	b.Publish("unsub")
	if count.Load() != 1 {
		t.Fatalf("expected 1 call after unsubscribe, got %d", count.Load())
	}
}

func TestUnsubscribeAsync(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	var count atomic.Int32
	done := make(chan struct{})

	id := b.Subscribe("unsub.async", func() {
		count.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
	})

	b.Publish("unsub.async")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called")
	}

	b.Unsubscribe(id)
	b.Publish("unsub.async")

	// Give a moment for any spurious delivery.
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected 1 call after unsubscribe, got %d", count.Load())
	}
}

func TestUnsubscribeAll(t *testing.T) {
	t.Parallel()
	b := New()

	var count atomic.Int32
	id1 := b.Subscribe("topic.a", func() { count.Add(1) }, WithSync())
	id2 := b.Subscribe("topic.b", func() { count.Add(1) }, WithSync())
	// Third handler on topic.a should survive the batch removal.
	b.Subscribe("topic.a", func() { count.Add(10) }, WithSync())

	b.Publish("topic.a")
	b.Publish("topic.b")
	if count.Load() != 12 {
		t.Fatalf("expected 12 before UnsubscribeAll, got %d", count.Load())
	}

	count.Store(0)
	b.UnsubscribeAll([]SubscriptionID{id1, id2})

	b.Publish("topic.a")
	b.Publish("topic.b")
	// Only the surviving topic.a handler (+10) should fire.
	if count.Load() != 10 {
		t.Fatalf("expected 10 after UnsubscribeAll, got %d", count.Load())
	}
}

func TestUnsubscribeZeroAndDoubleRemove(t *testing.T) {
	t.Parallel()
	b := New()

	// Zero ID is a no-op.
	b.Unsubscribe(0)

	id := b.Subscribe("x", func() {}, WithSync())
	b.Unsubscribe(id)
	// Double remove should not panic.
	b.Unsubscribe(id)
}

func TestNilBusUnsubscribeSafe(t *testing.T) {
	t.Parallel()
	var b *Bus
	// None of these should panic on nil receiver.
	b.Unsubscribe(42)
	b.UnsubscribeAll([]SubscriptionID{1, 2})
}

func TestInboxDropOnFull(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	// Block the handler so the inbox fills up.
	gate := make(chan struct{})
	var received atomic.Int32

	b.Subscribe("flood", func() {
		received.Add(1)
		<-gate
	})

	// First event enters the handler and blocks on gate.
	b.Publish("flood")
	// Wait for handler to pick up the first event.
	time.Sleep(50 * time.Millisecond)

	// Fill the inbox (defaultInboxSize = 64).
	for i := 0; i < defaultInboxSize; i++ {
		b.Publish("flood")
	}

	// This one should be dropped — inbox is full and handler is blocked.
	b.Publish("flood")

	// Unblock all.
	close(gate)

	// Wait for drainer to process buffered events.
	time.Sleep(200 * time.Millisecond)

	// We expect 1 (in handler) + 64 (buffered) = 65, not 66.
	got := received.Load()
	if got > int32(defaultInboxSize+1) {
		t.Fatalf("expected at most %d events, got %d (drop-on-full failed)", defaultInboxSize+1, got)
	}
	if got < int32(defaultInboxSize+1) {
		t.Fatalf("expected at least %d events, got %d", defaultInboxSize+1, got)
	}
}

// TestConcurrentPublishUnsubscribe verifies that Publish and Unsubscribe
// running concurrently do not panic (no "send on closed channel").
// Run with -race to verify no data races.
func TestConcurrentPublishUnsubscribe(t *testing.T) {
	t.Parallel()
	b := New()
	defer b.Shutdown()

	// Subscribe many handlers and concurrently publish + unsubscribe.
	const n = 50
	ids := make([]SubscriptionID, n)
	for i := 0; i < n; i++ {
		ids[i] = b.Subscribe("race.topic", func(v int) {
			// noop — just receiving is enough
		})
	}

	var wg sync.WaitGroup

	// Publishers: blast events as fast as possible.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.Publish("race.topic", j)
			}
		}()
	}

	// Unsubscribers: remove handlers while publishers are running.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id SubscriptionID) {
			defer wg.Done()
			// Small jitter so unsubscribes interleave with publishes.
			time.Sleep(time.Duration(id%5) * time.Millisecond)
			b.Unsubscribe(id)
		}(ids[i])
	}

	wg.Wait()
	// If we reach here without a panic, the test passes.
}

// TestConcurrentPublish verifies that Publish is safe to call from many
// goroutines simultaneously. Uses WithSync() so every event is delivered
// inline — no inbox buffer means no drop-on-full, letting us assert exact
// delivery count.
func TestConcurrentPublish(t *testing.T) {
	t.Parallel()
	b := New()

	var count atomic.Int64

	b.Subscribe("concurrent", func(n int) {
		count.Add(int64(n))
	}, WithSync())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish("concurrent", 1)
		}()
	}

	wg.Wait()

	if count.Load() != 100 {
		t.Fatalf("expected 100, got %d", count.Load())
	}
}
