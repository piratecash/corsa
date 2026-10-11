package node

import (
	"testing"
	"time"
)

// tokenBucketSet carries the refill math every per-key limiter in the package
// relies on, so its contract is pinned here once on a hand-driven clock; the
// limiter tests then only pin what each limiter adds on top.

// tokensOf reports key's current balance without refilling it, and whether
// the key has a bucket at all.
func (s *tokenBucketSet[K]) tokensOf(key K) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, ok := s.buckets[key]
	if !ok {
		return 0, false
	}
	return bucket.tokens, true
}

// testBucketPolicy refills one token every 100ms, so whole and half refill
// periods are exact durations and exact float products.
var testBucketPolicy = tokenBucketPolicy{burst: 5, refillPerSecond: 10}

const testBucketRefillPeriod = 100 * time.Millisecond

func newTestBucketSet() (*tokenBucketSet[string], *manualTestClock) {
	clock := newManualTestClock()
	return newTokenBucketSet[string](testBucketPolicy, clock.now), clock
}

func takeOne(s *tokenBucketSet[string], key string) func() bool {
	return func() bool { return s.take(key, 1) }
}

func TestTokenBucketSet_FreshKeyGrantsExactlyTheBurst(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()

	if got := countAllowed(10, takeOne(s, "a")); got != int(testBucketPolicy.burst) {
		t.Fatalf("fresh key granted %d tokens, want the burst %v", got, testBucketPolicy.burst)
	}
}

func TestTokenBucketSet_FrozenClockNeverRefills(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	for i := 0; i < 1000; i++ {
		if s.take("a", 1) {
			t.Fatalf("call %d on an exhausted bucket passed without the clock moving", i)
		}
	}
}

func TestTokenBucketSet_RefillIsElapsedTimesRate(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	const periods = 3
	clock.advance(periods * testBucketRefillPeriod)
	if got := countAllowed(10, takeOne(s, "a")); got != periods {
		t.Fatalf("%d refill periods granted %d tokens, want %d", periods, got, periods)
	}
}

func TestTokenBucketSet_RefusedCallCarriesItsFraction(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	clock.advance(testBucketRefillPeriod / 2)
	if s.take("a", 1) {
		t.Fatal("half a refill period must not yield a whole token")
	}
	if got, _ := s.tokensOf("a"); got != 0.5 {
		t.Fatalf("a refused call must keep the half token it earned; tokens = %v, want 0.5", got)
	}

	clock.advance(testBucketRefillPeriod / 2)
	if got := countAllowed(10, takeOne(s, "a")); got != 1 {
		t.Fatalf("two half periods split by a refusal granted %d tokens, want exactly 1", got)
	}
}

func TestTokenBucketSet_RefillIsCappedAtTheBurst(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	s.take("a", 1)

	clock.advance(time.Hour)
	if got := countAllowed(10, takeOne(s, "a")); got != int(testBucketPolicy.burst) {
		t.Fatalf("a long idle period granted %d tokens, want the burst %v", got, testBucketPolicy.burst)
	}
}

func TestTokenBucketSet_CostIsAllOrNothing(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()
	burst := int(testBucketPolicy.burst)

	if s.take("a", burst+1) {
		t.Fatal("a cost above the balance must be refused")
	}
	if got, _ := s.tokensOf("a"); got != testBucketPolicy.burst {
		t.Fatalf("a refused cost must not drain the bucket; tokens = %v, want %v", got, testBucketPolicy.burst)
	}
	if !s.take("a", burst) {
		t.Fatal("a cost equal to the balance must pass")
	}
	if got, _ := s.tokensOf("a"); got != 0 {
		t.Fatalf("a cost equal to the balance must spend all of it; tokens = %v", got)
	}
}

func TestTokenBucketSet_KeysAreIsolated(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	if got := countAllowed(10, takeOne(s, "b")); got != int(testBucketPolicy.burst) {
		t.Fatalf("an untouched key granted %d tokens after another key was drained, want the burst", got)
	}
}

func TestTokenBucketSet_ForgetRestartsTheKeyFull(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	s.forget("a")
	if _, ok := s.tokensOf("a"); ok {
		t.Fatal("forget must drop the bucket")
	}
	if got := countAllowed(10, takeOne(s, "a")); got != int(testBucketPolicy.burst) {
		t.Fatalf("a forgotten key granted %d tokens, want a full burst", got)
	}
}

func TestTokenBucketSet_EvictIdleKeepsExactlyMaxAgeAndDropsOlder(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	const maxAge = time.Minute

	s.take("old", 1)
	clock.advance(maxAge)
	s.take("new", 1)

	s.evictIdle(maxAge)
	if _, ok := s.tokensOf("old"); !ok {
		t.Fatal("a bucket idle for exactly maxAge must be kept")
	}

	clock.advance(time.Nanosecond)
	s.evictIdle(maxAge)
	if _, ok := s.tokensOf("old"); ok {
		t.Fatal("a bucket idle for longer than maxAge must be evicted")
	}
	if _, ok := s.tokensOf("new"); !ok {
		t.Fatal("a bucket used inside maxAge must be kept")
	}
}

// A refused take restamps the bucket like a granted one, so a key that keeps
// knocking on an empty bucket is never idle and never evicted into a fresh one.
func TestTokenBucketSet_RefusedTakeKeepsTheBucketFromAgingOut(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	const maxAge = time.Minute
	countAllowed(10, takeOne(s, "flooder"))

	clock.advance(maxAge)
	if s.take("flooder", int(testBucketPolicy.burst)+1) {
		t.Fatal("precondition: a cost above the burst is always refused")
	}
	clock.advance(time.Nanosecond)
	s.evictIdle(maxAge)
	if _, ok := s.tokensOf("flooder"); !ok {
		t.Fatal("a bucket refused a moment ago must not be evicted as idle")
	}
}
