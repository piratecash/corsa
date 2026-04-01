package node

import (
	"sync"
	"time"

	"corsa/internal/core/domain"
)

// relayRateLimiter enforces per-peer relay fan-out rate limits using a
// token bucket algorithm. Each peer gets a bucket that refills at a fixed
// rate up to a maximum burst. When a peer's bucket is empty, relay frames
// to that peer are dropped (not queued).
//
// This prevents a single peer from being flooded with relay_message frames
// during gossip fan-out, and limits the total relay bandwidth a node can
// push to any single neighbor per time window.
type relayRateLimiter struct {
	mu      sync.Mutex
	buckets map[domain.PeerAddress]*tokenBucket
}

// tokenBucket tracks per-peer relay allowance.
type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// relayBurstPerPeer is the maximum number of relay frames that can be sent
// to a single peer in a burst (bucket capacity).
const relayBurstPerPeer = 50

// relayRefillRate is the number of relay tokens added per second per peer.
// 20 tokens/s × 50 burst allows sustained ~20 relay/s with short spikes.
const relayRefillRate = 20.0

func newRelayRateLimiter() *relayRateLimiter {
	return &relayRateLimiter{
		buckets: make(map[domain.PeerAddress]*tokenBucket),
	}
}

// allow checks whether a relay frame to the given peer address should be
// permitted. Returns true and decrements one token on success. Returns
// false when the bucket is empty.
func (rl *relayRateLimiter) allow(address domain.PeerAddress) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[address]
	now := time.Now()
	if !ok {
		b = &tokenBucket{
			tokens:     relayBurstPerPeer,
			lastRefill: now,
		}
		rl.buckets[address] = b
	}

	// Refill tokens based on elapsed time.
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * relayRefillRate
	if b.tokens > relayBurstPerPeer {
		b.tokens = relayBurstPerPeer
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// cleanup removes stale buckets for peers that haven't been seen recently.
// Called periodically to prevent unbounded map growth.
func (rl *relayRateLimiter) cleanup(maxAge time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for addr, b := range rl.buckets {
		if b.lastRefill.Before(cutoff) {
			delete(rl.buckets, addr)
		}
	}
}
