package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Every relay limiter here runs on a hand-driven clock that only the test
// moves, so "no time has passed" and "exactly one refill period has passed"
// are facts of the test rather than hopes about scheduler latency.

func newTestRelayLimiter() (*relayRateLimiter, *manualTestClock) {
	clock := newManualTestClock()
	return newRelayRateLimiter(clock.now), clock
}

func relayLimiterHasBucket(rl *relayRateLimiter, address domain.PeerAddress) bool {
	_, ok := rl.buckets.tokensOf(address)
	return ok
}

func TestRelayRateLimiter_BurstAllowed(t *testing.T) {
	t.Parallel()
	rl, _ := newTestRelayLimiter()

	for i := 0; i < relayBurstPerPeer; i++ {
		if !rl.allow("peer-1") {
			t.Fatalf("burst request %d should be allowed", i)
		}
	}
}

func TestRelayRateLimiter_ExceedsBurstRejected(t *testing.T) {
	t.Parallel()
	rl, _ := newTestRelayLimiter()

	if got := countAllowed(relayBurstPerPeer+1, func() bool { return rl.allow("peer-1") }); got != relayBurstPerPeer {
		t.Fatalf("a fresh bucket must grant exactly the burst; granted %d, want %d", got, relayBurstPerPeer)
	}
}

func TestRelayRateLimiter_IndependentPeers(t *testing.T) {
	t.Parallel()
	rl, _ := newTestRelayLimiter()

	// Exhaust peer-1.
	for i := 0; i < relayBurstPerPeer; i++ {
		rl.allow("peer-1")
	}

	// peer-2 should still have full burst.
	if !rl.allow("peer-2") {
		t.Fatal("peer-2 should have independent bucket")
	}
}

func TestRelayRateLimiter_RefillFollowsInjectedClock(t *testing.T) {
	t.Parallel()
	rl, clock := newTestRelayLimiter()
	allowPeer := func() bool { return rl.allow("peer-1") }

	if got := countAllowed(relayBurstPerPeer+1, allowPeer); got != relayBurstPerPeer {
		t.Fatalf("precondition: fresh bucket grants the burst; granted %d", got)
	}

	clock.advance(time.Second / relayRefillRate)
	if got := countAllowed(2, allowPeer); got != 1 {
		t.Fatalf("one refill period must restore exactly one token; granted %d", got)
	}

	clock.advance(time.Second)
	if got := countAllowed(relayRefillRate+1, allowPeer); got != relayRefillRate {
		t.Fatalf("one second must restore exactly relayRefillRate tokens; granted %d, want %d", got, int(relayRefillRate))
	}

	clock.advance(time.Hour)
	if got := countAllowed(relayBurstPerPeer+1, allowPeer); got != relayBurstPerPeer {
		t.Fatalf("a long idle period must refill to the burst and no further; granted %d, want %d", got, relayBurstPerPeer)
	}
}

// A refused call still advances the bucket to now and keeps the fraction it
// earned. Dropping the fraction would make a steady stream slower than the
// configured rate; not moving lastRefill would credit the same interval again
// on the next call and make it faster.
func TestRelayRateLimiter_FractionalRefillAccumulatesAcrossRefusals(t *testing.T) {
	t.Parallel()
	rl, clock := newTestRelayLimiter()
	allowPeer := func() bool { return rl.allow("peer-1") }
	countAllowed(relayBurstPerPeer, allowPeer)

	halfPeriod := time.Second / relayRefillRate / 2
	clock.advance(halfPeriod)
	if allowPeer() {
		t.Fatal("half a refill period must not yield a whole token")
	}
	clock.advance(halfPeriod)
	if got := countAllowed(2, allowPeer); got != 1 {
		t.Fatalf("two half periods must add up to exactly one token; granted %d", got)
	}
}

func TestRelayRateLimiter_SweepEvictsOnlyBucketsIdlePastCleanupAge(t *testing.T) {
	t.Parallel()
	rl, clock := newTestRelayLimiter()

	rl.allow("stale-peer")
	clock.advance(relayLimiterCleanupAge)
	rl.allow("fresh-peer")

	rl.sweepIdle()
	if !relayLimiterHasBucket(rl, "stale-peer") {
		t.Fatal("a bucket idle for exactly the cleanup age must be kept")
	}

	clock.advance(time.Nanosecond)
	rl.sweepIdle()
	if relayLimiterHasBucket(rl, "stale-peer") {
		t.Fatal("a bucket idle for longer than the cleanup age must be evicted")
	}
	if !relayLimiterHasBucket(rl, "fresh-peer") {
		t.Fatal("a bucket used inside the cleanup age must be kept")
	}
}
