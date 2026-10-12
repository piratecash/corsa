package node

import (
	"testing"
	"time"
)

// tokenBucketSet carries the refill math relayRateLimiter, announceRateLimiter
// and commandRateLimiter rely on, so its contract is pinned here once on a
// hand-driven clock; the limiter tests then only pin what each limiter adds on
// top.

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

// countAllowed calls allow until it refuses or attempts runs out and returns
// how many calls passed. A limiter that should grant exactly n is checked
// with attempts > n, so a surplus grant shows up as a wrong count.
func countAllowed(attempts int, allow func() bool) int {
	for i := 0; i < attempts; i++ {
		if !allow() {
			return i
		}
	}
	return attempts
}

// The test policy refills one token every 100ms, so whole and half refill
// periods are exact durations and exact float products.
const (
	testBucketBurst           = 5
	testBucketRefillPerSecond = 10
	testBucketRefillPeriod    = time.Second / testBucketRefillPerSecond
)

// testBucketIdleAge is well above the test policy's half-second full refill,
// so it is the horizon the set sweeps at, unraised.
const testBucketIdleAge = time.Minute

func testBucketPolicy() tokenBucketPolicy {
	return tokenBucketPolicy{burst: testBucketBurst, refillPerSecond: testBucketRefillPerSecond, idleAge: testBucketIdleAge}
}

func newTestBucketSet() (*tokenBucketSet[string], *manualTestClock) {
	clock := newManualTestClock()
	return newTokenBucketSet[string](testBucketPolicy(), clock.now), clock
}

func takeOne(s *tokenBucketSet[string], key string) func() bool {
	return func() bool { return s.take(key, 1) }
}

func TestTokenBucketSet_FreshKeyGrantsExactlyTheBurst(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()

	if got := countAllowed(10, takeOne(s, "a")); got != testBucketBurst {
		t.Fatalf("fresh key granted %d tokens, want the burst %d", got, testBucketBurst)
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
	if got := countAllowed(10, takeOne(s, "a")); got != testBucketBurst {
		t.Fatalf("a long idle period granted %d tokens, want the burst %d", got, testBucketBurst)
	}
}

func TestTokenBucketSet_CostIsAllOrNothing(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()

	if s.take("a", testBucketBurst+1) {
		t.Fatal("a cost above the balance must be refused")
	}
	if got, _ := s.tokensOf("a"); got != testBucketBurst {
		t.Fatalf("a refused cost must not drain the bucket; tokens = %v, want %d", got, testBucketBurst)
	}
	if !s.take("a", testBucketBurst) {
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

	if got := countAllowed(10, takeOne(s, "b")); got != testBucketBurst {
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
	if got := countAllowed(10, takeOne(s, "a")); got != testBucketBurst {
		t.Fatalf("a forgotten key granted %d tokens, want a full burst", got)
	}
}

func TestTokenBucketSet_SweepIdleKeepsExactlyIdleAgeAndDropsOlder(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()

	s.take("old", 1)
	clock.advance(testBucketIdleAge)
	s.take("new", 1)

	s.sweepIdle()
	if _, ok := s.tokensOf("old"); !ok {
		t.Fatal("a bucket idle for exactly the idle age must be kept")
	}

	clock.advance(time.Nanosecond)
	s.sweepIdle()
	if _, ok := s.tokensOf("old"); ok {
		t.Fatal("a bucket idle for longer than the idle age must be evicted")
	}
	if _, ok := s.tokensOf("new"); !ok {
		t.Fatal("a bucket used inside the idle age must be kept")
	}
}

// A policy that declares an idle age shorter than a full refill is swept only
// after the full refill: the set, not each limiter, keeps a sweep from handing
// a still-draining key a full bucket. Up to that instant the drained bucket
// keeps its exact balance; one nanosecond past it, it is gone and would have
// been full.
func TestTokenBucketSet_SweepNeverRunsAheadOfAFullRefill(t *testing.T) {
	t.Parallel()
	policy := testBucketPolicy()
	policy.idleAge = 0
	clock := newManualTestClock()
	s := newTokenBucketSet[string](policy, clock.now)
	countAllowed(testBucketBurst, takeOne(s, "drained"))

	clock.advance(policy.fullRefill())
	s.sweepIdle()
	if _, ok := s.tokensOf("drained"); !ok {
		t.Fatalf("a bucket idle for exactly a full refill (%v) must be kept although the declared idle age is 0", policy.fullRefill())
	}

	clock.advance(time.Nanosecond)
	s.sweepIdle()
	if _, ok := s.tokensOf("drained"); ok {
		t.Fatal("a bucket idle for longer than a full refill must be evicted")
	}
}

// A policy that never refills never gets a drained bucket back to its burst,
// so the set never sweeps it, whatever idle age the policy declares: dropping
// it would hand the key a full bucket it would never have earned.
func TestTokenBucketSet_PolicyThatNeverRefillsIsNeverSwept(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{0, -1} {
		policy := tokenBucketPolicy{burst: testBucketBurst, refillPerSecond: rate, idleAge: testBucketIdleAge}
		if got := policy.fullRefill(); got != neverRefills {
			t.Fatalf("rate %v: fullRefill = %v, want neverRefills", rate, got)
		}
		clock := newManualTestClock()
		s := newTokenBucketSet[string](policy, clock.now)
		countAllowed(testBucketBurst, takeOne(s, "drained"))

		// Four centuries, in steps the clock's own Duration can hold: past the
		// ~292 years a Duration spans, so "never" cannot hide behind the
		// largest idle age the type can express.
		const century = 100 * 365 * 24 * time.Hour
		for step := 1; step <= 4; step++ {
			clock.advance(century)
			s.sweepIdle()
			if _, ok := s.tokensOf("drained"); !ok {
				t.Fatalf("rate %v: after %d00 years a drained bucket that can never refill was swept into a fresh one", rate, step)
			}
		}
	}
}

// fullRefill rounds up, so a bucket idle for longer than it holds the whole
// burst even when burst/rate is not a whole number of nanoseconds (100/30 s).
func TestTokenBucketPolicy_FullRefillRoundsUp(t *testing.T) {
	t.Parallel()
	policy := tokenBucketPolicy{burst: 100, refillPerSecond: 30}
	if got, want := policy.fullRefill(), 3333333334*time.Nanosecond; got != want {
		t.Fatalf("fullRefill = %v, want %v", got, want)
	}
}

// A refused take restamps the bucket like a granted one, so a key that keeps
// knocking on an empty bucket is never idle and never evicted into a fresh one.
func TestTokenBucketSet_RefusedTakeKeepsTheBucketFromAgingOut(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	countAllowed(10, takeOne(s, "flooder"))

	clock.advance(testBucketIdleAge)
	if s.take("flooder", testBucketBurst+1) {
		t.Fatal("precondition: a cost above the burst is always refused")
	}
	clock.advance(time.Nanosecond)
	s.sweepIdle()
	if _, ok := s.tokensOf("flooder"); !ok {
		t.Fatal("a bucket refused a moment ago must not be evicted as idle")
	}
}

// A refused take restamps lastRefill, so two refusals in a row at the same
// instant credit the elapsed interval once, not twice.
func TestTokenBucketSet_RefusedTakeCreditsAnIntervalOnlyOnce(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	clock.advance(testBucketRefillPeriod / 2)
	// Both takes run unconditionally: the second is the one the mutation
	// (no restamp on refusal) would let through.
	firstGranted := s.take("a", 1)
	secondGranted := s.take("a", 1)
	if firstGranted || secondGranted {
		t.Fatalf("half a refill period must refuse both takes at the same instant; granted first=%v second=%v", firstGranted, secondGranted)
	}
	if got, _ := s.tokensOf("a"); got != 0.5 {
		t.Fatalf("tokens = %v, want exactly 0.5: the second refusal must not credit the half period again", got)
	}
}

// A clock that steps backwards is not guarded (time.Now is monotonic in
// production), and this pins the exact arithmetic the refill has always done
// rather than any guard: the backward step is debited at the refill rate and
// restamped, and the forward step back credits exactly that debit again, so
// the round trip mints nothing.
func TestTokenBucketSet_BackwardsClockIsDebitedAndCreditedBack(t *testing.T) {
	t.Parallel()
	s, clock := newTestBucketSet()
	countAllowed(10, takeOne(s, "a"))

	const rewind = time.Hour
	wantDebited := -rewind.Seconds() * testBucketRefillPerSecond
	clock.advance(-rewind)
	if s.take("a", 1) {
		t.Fatal("a take after the clock stepped back must be refused")
	}
	if got, _ := s.tokensOf("a"); got != wantDebited {
		t.Fatalf("tokens after a %v backward step = %v, want exactly %v (the step debited at the refill rate)", rewind, got, wantDebited)
	}

	clock.advance(rewind)
	if s.take("a", 1) {
		t.Fatal("returning to the original instant must not grant the tokens the rewind skipped")
	}
	if got, _ := s.tokensOf("a"); got != 0 {
		t.Fatalf("tokens after the round trip = %v, want exactly the 0 the bucket was drained to", got)
	}
}

// A cost below 1 is charged as 1: a miscounting caller neither passes for
// free nor, with a negative cost, raises the balance.
func TestTokenBucketSet_CostBelowOneIsChargedAsOne(t *testing.T) {
	t.Parallel()
	s, _ := newTestBucketSet()

	if !s.take("a", 0) {
		t.Fatal("cost 0 must be accepted while the bucket holds tokens")
	}
	if got, _ := s.tokensOf("a"); got != testBucketBurst-1 {
		t.Fatalf("cost 0 left %v tokens, want %d: it must be charged exactly 1", got, testBucketBurst-1)
	}
	if !s.take("a", -42) {
		t.Fatal("a negative cost must be accepted while the bucket holds tokens")
	}
	if got, _ := s.tokensOf("a"); got != testBucketBurst-2 {
		t.Fatalf("cost -42 left %v tokens, want %d: it must be charged exactly 1", got, testBucketBurst-2)
	}
	countAllowed(10, takeOne(s, "a"))
	if s.take("a", 0) {
		t.Fatal("cost 0 must be refused by an empty bucket")
	}
}

// Construction with a nil clock falls back to the wall clock instead of
// leaving a nil func to be called on the first take.
func TestTokenBucketSet_NilClockFallsBackToWallClock(t *testing.T) {
	t.Parallel()
	s := newTokenBucketSet[string](testBucketPolicy(), nil)
	if !s.take("a", 1) {
		t.Fatal("the first take on a set built with a nil clock must pass")
	}
}

// A limiter is never swept sooner than a drained bucket takes to refill to
// the burst on its own: an evicted key comes back full, so a shorter idle age
// would be a refill shortcut. The policy and the idle age are read from the
// production constructors, so a constructor wired to other values is caught.
func TestRateLimiterSweepNeverOutrunsAFullRefill(t *testing.T) {
	t.Parallel()
	relay := newRelayRateLimiter(nil)
	announce := newAnnounceRateLimiter(nil)
	command := newCommandRateLimiter(nil)
	cases := []struct {
		name   string
		policy tokenBucketPolicy
	}{
		{"relay", relay.buckets.policy},
		{"announce", announce.buckets.policy},
		{"command", command.buckets.policy},
	}
	for _, c := range cases {
		if c.policy.idleAge < c.policy.fullRefill() {
			t.Errorf("%s: declared idle age %v is shorter than the %v a drained bucket needs to refill", c.name, c.policy.idleAge, c.policy.fullRefill())
		}
	}
}
