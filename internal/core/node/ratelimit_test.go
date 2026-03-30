package node

import (
	"testing"
	"time"
)

func TestRelayRateLimiter_BurstAllowed(t *testing.T) {
	t.Parallel()
	rl := newRelayRateLimiter()

	for i := 0; i < relayBurstPerPeer; i++ {
		if !rl.allow("peer-1") {
			t.Fatalf("burst request %d should be allowed", i)
		}
	}
}

func TestRelayRateLimiter_ExceedsBurstRejected(t *testing.T) {
	t.Parallel()
	rl := newRelayRateLimiter()

	// Exhaust burst.
	for i := 0; i < relayBurstPerPeer; i++ {
		rl.allow("peer-1")
	}

	if rl.allow("peer-1") {
		t.Fatal("request after burst exhaustion should be rejected")
	}
}

func TestRelayRateLimiter_IndependentPeers(t *testing.T) {
	t.Parallel()
	rl := newRelayRateLimiter()

	// Exhaust peer-1.
	for i := 0; i < relayBurstPerPeer; i++ {
		rl.allow("peer-1")
	}

	// peer-2 should still have full burst.
	if !rl.allow("peer-2") {
		t.Fatal("peer-2 should have independent bucket")
	}
}

func TestRelayRateLimiter_RefillOverTime(t *testing.T) {
	t.Parallel()
	rl := newRelayRateLimiter()

	// Exhaust burst.
	for i := 0; i < relayBurstPerPeer; i++ {
		rl.allow("peer-1")
	}
	if rl.allow("peer-1") {
		t.Fatal("should be empty after burst")
	}

	// Manually adjust lastRefill to simulate time passing.
	rl.mu.Lock()
	b := rl.buckets["peer-1"]
	b.lastRefill = b.lastRefill.Add(-1 * time.Second)
	rl.mu.Unlock()

	// After 1 second, refillRate tokens should be available.
	if !rl.allow("peer-1") {
		t.Fatal("should be allowed after refill")
	}
}

func TestRelayRateLimiter_CleanupRemovesStaleBuckets(t *testing.T) {
	t.Parallel()
	rl := newRelayRateLimiter()

	rl.allow("stale-peer")
	// Simulate stale bucket.
	rl.mu.Lock()
	rl.buckets["stale-peer"].lastRefill = time.Now().Add(-10 * time.Minute)
	rl.mu.Unlock()

	rl.allow("fresh-peer")

	rl.cleanup(5 * time.Minute)

	rl.mu.Lock()
	_, staleExists := rl.buckets["stale-peer"]
	_, freshExists := rl.buckets["fresh-peer"]
	rl.mu.Unlock()

	if staleExists {
		t.Fatal("stale bucket should be cleaned up")
	}
	if !freshExists {
		t.Fatal("fresh bucket should be kept")
	}
}
