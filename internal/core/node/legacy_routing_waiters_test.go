package node

import (
	"context"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestCancelledDrainWaiterDoesNotAccumulateOrConsumePurge(t *testing.T) {
	var writers unprovenRoutingWriters
	id := domain.PeerIdentity{0xd7, 1}
	writers.enter(id)
	for range 300 {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- writers.waitDrained(ctx, id) }()
		waitContactRecordCondition(t, "drain waiter registration", func() bool {
			writers.mu.Lock()
			defer writers.mu.Unlock()
			return len(writers.drained[id]) > 0
		})
		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatalf("cancel result: %v", err)
		}
		if writers.owePurge(id) {
			t.Fatal("held writer unexpectedly drained")
		}
		writers.mu.Lock()
		retained := len(writers.drained[id])
		_, owed := writers.owed[id]
		writers.mu.Unlock()
		if retained != 0 || !owed {
			t.Fatalf("cancel retained %d waiters, purge owed=%v", retained, owed)
		}
	}
	if !writers.leave(id) {
		t.Fatal("last writer lost its purge handoff")
	}
	if writers.leave(id) {
		t.Fatal("purge writer was charged twice")
	}
}

func TestCancelledDrainRemovesOnlyItsSubscriptionAndNilsTail(t *testing.T) {
	var writers unprovenRoutingWriters
	id := domain.PeerIdentity{0xd7, 2}
	writers.enter(id)
	first := make(chan error, 1)
	go func() { first <- writers.waitDrained(t.Context(), id) }()
	waitContactRecordCondition(t, "first waiter", func() bool { writers.mu.Lock(); defer writers.mu.Unlock(); return len(writers.drained[id]) == 1 })
	ctx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	go func() { cancelled <- writers.waitDrained(ctx, id) }()
	waitContactRecordCondition(t, "second waiter", func() bool { writers.mu.Lock(); defer writers.mu.Unlock(); return len(writers.drained[id]) == 2 })
	writers.mu.Lock()
	backing := writers.drained[id]
	writers.mu.Unlock()
	cancel()
	if err := <-cancelled; err != context.Canceled {
		t.Fatal(err)
	}
	writers.mu.Lock()
	if len(writers.drained[id]) != 1 || backing[1] != nil {
		writers.mu.Unlock()
		t.Fatal("cancel removed another waiter or retained a tail channel")
	}
	writers.mu.Unlock()
	if writers.owePurge(id) || !writers.leave(id) {
		t.Fatal("lost purge handoff")
	}
	select {
	case <-first:
		t.Fatal("survivor woke before actual owed purge finished")
	default:
	}
	writers.leave(id)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	writers.mu.Lock()
	defer writers.mu.Unlock()
	if len(writers.drained[id]) != 0 || writers.inflight[id] != 0 {
		t.Fatal("final writer retained waiters or inflight")
	}
}

func TestDrainCancellationConcurrentWithLastLeave(t *testing.T) {
	for range 200 {
		var writers unprovenRoutingWriters
		id := domain.PeerIdentity{0xd7, 3}
		writers.enter(id)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- writers.waitDrained(ctx, id) }()
		waitContactRecordCondition(t, "registered race waiter", func() bool { writers.mu.Lock(); defer writers.mu.Unlock(); return len(writers.drained[id]) == 1 })
		left := make(chan struct{})
		go func() { writers.leave(id); close(left) }()
		cancel()
		err := <-done
		<-left
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
		writers.mu.Lock()
		retained := len(writers.drained[id])
		writers.mu.Unlock()
		if retained != 0 {
			t.Fatal("cancel/leave retained subscription")
		}
	}
}
