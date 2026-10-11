package node

import (
	"sync"
	"time"
)

// tokenBucketPolicy is the shape every bucket of one tokenBucketSet shares.
type tokenBucketPolicy struct {
	// burst is how many tokens a full bucket holds; a fresh key starts full.
	burst float64
	// refillPerSecond is how many tokens an idle bucket regains per second.
	refillPerSecond float64
}

// tokenBucket is one key's allowance. Tokens are fractional so a bucket
// polled faster than its refill period still accumulates the time it is owed.
type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// tokenBucketSet is a keyed set of token buckets that refill and age by an
// injected clock. It is the only place the package's per-key token-bucket math
// lives; relayRateLimiter, announceRateLimiter and commandRateLimiter each own
// one and add only what is specific to them (key type, sizing, exempt keys,
// when to forget a key, how long an idle bucket is kept).
//
// mu is a leaf: nothing is called under it except the clock, which is a pure
// time source and never re-enters the caller.
type tokenBucketSet[K comparable] struct {
	mu      sync.Mutex
	buckets map[K]*tokenBucket
	// policy and clock are set once in newTokenBucketSet and never written
	// again, which is why they need no protection from mu.
	policy tokenBucketPolicy
	clock  func() time.Time
}

// newTokenBucketSet builds an empty set. Production passes time.Now; a nil
// clock falls back to it, the same as the package's other clock-driven
// structures, so a missing argument cannot turn into a nil-func call on a
// receive path.
func newTokenBucketSet[K comparable](policy tokenBucketPolicy, clock func() time.Time) *tokenBucketSet[K] {
	if clock == nil {
		clock = time.Now
	}
	return &tokenBucketSet[K]{
		buckets: make(map[K]*tokenBucket),
		policy:  policy,
		clock:   clock,
	}
}

// take refills key's bucket up to now and spends cost tokens from it.
//
// The reservation is all-or-nothing: when the bucket holds fewer than cost
// tokens nothing is spent, so a sender cannot drip-drain a bucket with
// requests that never fit. A refused call still refills and restamps the
// bucket, so the fraction it earned is carried to the next call and a bucket
// that keeps being refused never counts as idle for evictIdle.
//
// A cost below 1 is charged as 1. The clamp lives here rather than in each
// limiter because the invariant it protects belongs to the bucket: a take
// never mints tokens. Without it a cost of 0 would pass for free forever and
// a negative cost would raise the balance above the burst.
func (s *tokenBucketSet[K]) take(key K, cost int) bool {
	if cost < 1 {
		cost = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	bucket := s.bucketLocked(key, now)
	bucket.refill(s.policy, now)
	if bucket.tokens < float64(cost) {
		return false
	}
	bucket.tokens -= float64(cost)
	return true
}

// forget drops key's bucket, so the next take for it starts from a full one.
func (s *tokenBucketSet[K]) forget(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buckets, key)
}

// evictIdle drops every bucket not refilled for longer than maxAge. A bucket
// idle for exactly maxAge is kept.
func (s *tokenBucketSet[K]) evictIdle(maxAge time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.clock().Add(-maxAge)
	for key, bucket := range s.buckets {
		if bucket.lastRefill.Before(cutoff) {
			delete(s.buckets, key)
		}
	}
}

// bucketLocked returns key's bucket, creating a full one stamped now on first
// use. Caller holds s.mu.
func (s *tokenBucketSet[K]) bucketLocked(key K, now time.Time) *tokenBucket {
	bucket, ok := s.buckets[key]
	if !ok {
		bucket = &tokenBucket{tokens: s.policy.burst, lastRefill: now}
		s.buckets[key] = bucket
	}
	return bucket
}

// refill credits the time since the last refill, capped at the burst.
func (b *tokenBucket) refill(policy tokenBucketPolicy, now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * policy.refillPerSecond
	if b.tokens > policy.burst {
		b.tokens = policy.burst
	}
	b.lastRefill = now
}
