package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// relayRateLimiter enforces per-peer relay fan-out rate limits using a
// token bucket algorithm. Each peer gets a bucket that refills at a fixed
// rate up to a maximum burst. When a peer's bucket is empty, relay frames
// to that peer are dropped (not queued).
//
// This prevents a single peer from being flooded with relay_message frames
// during gossip fan-out, and limits the total relay bandwidth a node can
// push to any single neighbor per time window.
//
// Time is read only through the injected clock (the wall clock in
// production), so refill and stale-bucket cleanup can be driven
// deterministically in tests.
type relayRateLimiter struct {
	buckets *tokenBucketSet[domain.PeerAddress]
	// idleAge is how long a bucket may go untouched before sweepIdle evicts
	// it. The constructor fixes it next to the bucket policy instead of the
	// sweep's caller choosing it, so "never swept sooner than a full refill"
	// is checked against exactly what production wires.
	idleAge time.Duration
}

// relayBurstPerPeer is the maximum number of relay frames that can be sent
// to a single peer in a burst (bucket capacity).
const relayBurstPerPeer = 50

// relayRefillRate is the number of relay tokens added per second per peer.
// 20 tokens/s × 50 burst allows sustained ~20 relay/s with short spikes.
const relayRefillRate = 20.0

// relayLimiterCleanupAge is the inactivity window after which a per-peer
// relay bucket is evicted by sweepIdle (run on every bootstrapLoop tick). It
// must stay at least as long as a drained bucket takes to refill
// (relayBurstPerPeer / relayRefillRate = 2.5 s): an eviction hands the peer a
// full bucket, so a shorter horizon would be a refill shortcut.
const relayLimiterCleanupAge = 5 * time.Minute

// newRelayRateLimiter builds a limiter whose buckets refill and age by clock.
// Production passes time.Now; nil falls back to it (see newTokenBucketSet).
func newRelayRateLimiter(clock func() time.Time) *relayRateLimiter {
	return &relayRateLimiter{
		buckets: newTokenBucketSet[domain.PeerAddress](tokenBucketPolicy{
			burst:           relayBurstPerPeer,
			refillPerSecond: relayRefillRate,
		}, clock),
		idleAge: relayLimiterCleanupAge,
	}
}

// allow checks whether a relay frame to the given peer address should be
// permitted. Returns true and decrements one token on success. Returns
// false when the bucket is empty.
func (rl *relayRateLimiter) allow(address domain.PeerAddress) bool {
	return rl.buckets.take(address, 1)
}

// sweepIdle evicts buckets idle for longer than the limiter's idle age.
// Called periodically to prevent unbounded map growth.
func (rl *relayRateLimiter) sweepIdle() {
	rl.buckets.evictIdle(rl.idleAge)
}
