package node

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNonceCacheBasic(t *testing.T) {
	t.Parallel()

	cache := newNonceCache(100, 5*time.Minute)

	// New nonce: Has returns false.
	if cache.Has("nonce-1") {
		t.Error("Has should return false for unseen nonce")
	}

	// Add it.
	cache.Add("nonce-1")

	// Now Has returns true.
	if !cache.Has("nonce-1") {
		t.Error("Has should return true after Add")
	}

	// Different nonce: still unseen.
	if cache.Has("nonce-2") {
		t.Error("Has should return false for different nonce")
	}

	cache.Add("nonce-2")

	if cache.Len() != 2 {
		t.Errorf("Len() = %d, want 2", cache.Len())
	}
}

func TestNonceCacheAddIdempotent(t *testing.T) {
	t.Parallel()

	cache := newNonceCache(100, 5*time.Minute)

	cache.Add("nonce-dup")
	cache.Add("nonce-dup") // should be no-op

	if cache.Len() != 1 {
		t.Errorf("Len() = %d, want 1 after duplicate Add", cache.Len())
	}
}

func TestNonceCacheCapacityEviction(t *testing.T) {
	t.Parallel()

	cache := newNonceCache(5, 1*time.Hour)

	for i := 0; i < 5; i++ {
		cache.Add(fmt.Sprintf("nonce-%d", i))
	}

	if cache.Len() != 5 {
		t.Errorf("Len() = %d, want 5", cache.Len())
	}

	// Adding one more should evict the oldest.
	cache.Add("nonce-5")
	if cache.Len() != 5 {
		t.Errorf("Len() = %d, want 5 after eviction", cache.Len())
	}

	// nonce-0 (oldest) should have been evicted.
	if cache.Has("nonce-0") {
		t.Error("nonce-0 should have been evicted")
	}

	// nonce-4 (recent) should still be present.
	if !cache.Has("nonce-4") {
		t.Error("nonce-4 should still be in cache")
	}
}

func TestNonceCacheTTLEviction(t *testing.T) {
	t.Parallel()

	// Very short TTL for testing.
	cache := newNonceCache(100, 50*time.Millisecond)

	cache.Add("nonce-ttl")
	if !cache.Has("nonce-ttl") {
		t.Error("nonce should be present immediately after Add")
	}

	// Wait for TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// After TTL, the nonce should be evicted on next Has.
	if cache.Has("nonce-ttl") {
		t.Error("nonce should have been evicted after TTL")
	}
}

func TestNonceCacheDefaultCreation(t *testing.T) {
	t.Parallel()

	cache := newDefaultNonceCache()
	if cache == nil {
		t.Fatal("newDefaultNonceCache returned nil")
	}
	if cache.capacity != 10000 {
		t.Errorf("capacity = %d, want 10000", cache.capacity)
	}
}

// TestNonceCacheTryAddReturnsTrue verifies that TryAdd returns true on the
// first insert and false on subsequent attempts for the same nonce.
func TestNonceCacheTryAddReturnsTrue(t *testing.T) {
	t.Parallel()

	cache := newNonceCache(100, 5*time.Minute)

	if !cache.TryAdd("nonce-1") {
		t.Error("first TryAdd should return true")
	}
	if cache.TryAdd("nonce-1") {
		t.Error("second TryAdd for same nonce should return false")
	}
	if cache.Len() != 1 {
		t.Errorf("Len() = %d, want 1", cache.Len())
	}
}

// TestNonceCacheTryAddConcurrentExactlyOneWins verifies that when N goroutines
// race to TryAdd the same nonce, exactly one returns true — the anti-replay
// guarantee that closes the Has/Add TOCTOU race.
func TestNonceCacheTryAddConcurrentExactlyOneWins(t *testing.T) {
	t.Parallel()

	cache := newNonceCache(10000, 5*time.Minute)
	const goroutines = 100

	var winners atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // synchronize all goroutines
			if cache.TryAdd("race-nonce") {
				winners.Add(1)
			}
		}()
	}

	close(start) // release all goroutines simultaneously
	wg.Wait()

	if w := winners.Load(); w != 1 {
		t.Errorf("exactly 1 goroutine should win TryAdd, got %d", w)
	}
	if cache.Len() != 1 {
		t.Errorf("cache should contain exactly 1 entry, got %d", cache.Len())
	}
}
