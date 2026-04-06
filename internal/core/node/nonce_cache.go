package node

import (
	"sync"
	"time"

	"corsa/internal/core/domain"
)

// nonceEntry tracks a single nonce in the anti-replay cache.
type nonceEntry struct {
	nonce    string
	addedAt  time.Time
	listPrev *nonceEntry // doubly-linked for O(1) LRU eviction
	listNext *nonceEntry
}

// nonceCache is a bounded, TTL-aware, LRU-evicting anti-replay cache for
// FileCommandFrame nonce. It prevents replay attacks by rejecting frames
// whose nonce has been seen before within the TTL window.
//
// Concurrency: all methods are safe for concurrent use.
//
// Eviction: entries expire after FileCommandNonceTTL. If the cache reaches
// FileCommandNonceCacheSize before TTL expiry, the oldest entry is evicted
// (LRU). This bounds memory even under sustained traffic.
type nonceCache struct {
	mu       sync.Mutex
	entries  map[string]*nonceEntry // nonce → entry
	head     *nonceEntry            // most recently added
	tail     *nonceEntry            // oldest (eviction candidate)
	capacity int
	ttl      time.Duration
}

// newNonceCache creates a nonce cache with the given capacity and TTL.
func newNonceCache(capacity int, ttl time.Duration) *nonceCache {
	return &nonceCache{
		entries:  make(map[string]*nonceEntry, capacity),
		capacity: capacity,
		ttl:      ttl,
	}
}

// newDefaultNonceCache creates a nonce cache with default file command limits.
func newDefaultNonceCache() *nonceCache {
	return newNonceCache(domain.FileCommandNonceCacheSize, domain.FileCommandNonceTTL)
}

// Has checks whether the nonce has been seen before. Does NOT insert it.
// Use this for the early anti-replay check before expensive validation;
// call Add only after the frame passes all authenticity checks.
func (c *nonceCache) Has(nonce string) bool {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictExpiredLocked(now)

	_, exists := c.entries[nonce]
	return exists
}

// Add inserts a nonce into the cache. Should only be called after the frame
// has passed nonce binding, freshness, and signature verification. If the
// nonce already exists (benign race between concurrent deliveries of the
// same legitimate frame), the call is a no-op.
//
// Prefer TryAdd when the caller must distinguish "I won the insert" from
// "another goroutine already inserted" — e.g. for anti-replay commit.
func (c *nonceCache) Add(nonce string) {
	c.TryAdd(nonce)
}

// TryAdd atomically checks whether the nonce is already cached and, if not,
// inserts it. Returns true if this call performed the insert (the caller
// "won" the race), false if the nonce was already present.
//
// This is the commit step of the anti-replay pipeline: after the frame has
// passed all authenticity checks (nonce binding, freshness, signature),
// TryAdd ensures that exactly one concurrent goroutine proceeds to
// deliver/forward the frame. Losers observe false and drop the duplicate.
func (c *nonceCache) TryAdd(nonce string) bool {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Already present — another goroutine committed first.
	if _, exists := c.entries[nonce]; exists {
		return false
	}

	// Capacity enforcement: evict oldest if full.
	if len(c.entries) >= c.capacity {
		c.evictOldestLocked()
	}

	entry := &nonceEntry{
		nonce:   nonce,
		addedAt: now,
	}
	c.insertHeadLocked(entry)
	c.entries[nonce] = entry
	return true
}

// Len returns the current number of entries in the cache.
func (c *nonceCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// evictExpiredLocked removes entries older than TTL from the tail.
// Must be called with c.mu held.
func (c *nonceCache) evictExpiredLocked(now time.Time) {
	cutoff := now.Add(-c.ttl)
	for c.tail != nil && c.tail.addedAt.Before(cutoff) {
		c.removeTailLocked()
	}
}

// evictOldestLocked removes the single oldest entry (tail).
// Must be called with c.mu held.
func (c *nonceCache) evictOldestLocked() {
	if c.tail != nil {
		c.removeTailLocked()
	}
}

// insertHeadLocked inserts an entry at the head of the linked list.
// Must be called with c.mu held.
func (c *nonceCache) insertHeadLocked(e *nonceEntry) {
	e.listPrev = nil
	e.listNext = c.head
	if c.head != nil {
		c.head.listPrev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

// removeTailLocked removes and deletes the tail entry.
// Must be called with c.mu held.
func (c *nonceCache) removeTailLocked() {
	if c.tail == nil {
		return
	}
	old := c.tail
	delete(c.entries, old.nonce)

	c.tail = old.listPrev
	if c.tail != nil {
		c.tail.listNext = nil
	} else {
		c.head = nil
	}
	old.listPrev = nil
	old.listNext = nil
}
