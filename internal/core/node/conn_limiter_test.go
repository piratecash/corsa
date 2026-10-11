package node

import (
	"testing"
	"time"
)

// Both limiters here run on a hand-driven clock that only the test moves:
// window expiry, refill and cleanup are asserted at exact instants instead
// of after a sleep with timing headroom.

// ---------------------------------------------------------------------------
// connRateLimiter tests
// ---------------------------------------------------------------------------

func newTestConnLimiter() (*connRateLimiter, *manualTestClock) {
	clock := newManualTestClock()
	return newConnRateLimiter(clock.now), clock
}

func connLimiterHasWindow(cl *connRateLimiter, ip string) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	_, ok := cl.windows[ip]
	return ok
}

func TestConnRateLimiterAllowsNormalRate(t *testing.T) {
	t.Parallel()
	cl, _ := newTestConnLimiter()

	// First connection from an IP should always be allowed.
	if !cl.allowConnect("192.168.1.1") {
		t.Fatal("first connection should be allowed")
	}
}

func TestConnRateLimiterBlocksFlood(t *testing.T) {
	t.Parallel()
	cl, _ := newTestConnLimiter()

	ip := "10.0.0.1"
	// Exhaust the per-window limit.
	for i := 0; i < defaultConnRateLimit; i++ {
		if !cl.allowConnect(ip) {
			t.Fatalf("connection %d should be allowed", i+1)
		}
	}

	// Next connection should be rejected.
	if cl.allowConnect(ip) {
		t.Fatal("connection beyond rate limit should be rejected")
	}
}

func TestConnRateLimiterAllowsAfterWindowReset(t *testing.T) {
	t.Parallel()
	cl, clock := newTestConnLimiter()
	ip := "10.0.0.2"
	allowIP := func() bool { return cl.allowConnect(ip) }

	if got := countAllowed(defaultConnRateLimit+1, allowIP); got != defaultConnRateLimit {
		t.Fatalf("precondition: one window admits exactly the limit; admitted %d", got)
	}

	clock.advance(defaultConnRateWindow - time.Nanosecond)
	if allowIP() {
		t.Fatal("a connection before the window ends must stay blocked")
	}

	clock.advance(time.Nanosecond)
	if got := countAllowed(defaultConnRateLimit+1, allowIP); got != defaultConnRateLimit {
		t.Fatalf("a window that has run its full length must reset to the whole limit; admitted %d, want %d", got, defaultConnRateLimit)
	}
}

func TestConnRateLimiterIsolatesIPs(t *testing.T) {
	t.Parallel()
	cl, _ := newTestConnLimiter()

	// Exhaust limit for one IP.
	for i := 0; i < defaultConnRateLimit; i++ {
		cl.allowConnect("10.0.0.1")
	}

	// Different IP should not be affected.
	if !cl.allowConnect("10.0.0.2") {
		t.Fatal("different IP should not be affected by rate limit on another IP")
	}
}

// An address the listener could not parse is not counted against any window:
// on a frozen clock, well past the per-window limit, every attempt passes.
func TestConnRateLimiterEmptyIPAllowed(t *testing.T) {
	t.Parallel()
	cl, _ := newTestConnLimiter()
	attempts := defaultConnRateLimit + 5
	if got := countAllowed(attempts, func() bool { return cl.allowConnect("") }); got != attempts {
		t.Fatalf("empty IP admitted %d of %d attempts; it must always be allowed", got, attempts)
	}
}

// Construction with a nil clock falls back to the wall clock instead of
// leaving a nil func to be called on the first attempt.
func TestConnRateLimiterNilClockFallsBackToWallClock(t *testing.T) {
	t.Parallel()
	cl := newConnRateLimiter(nil)
	if !cl.allowConnect("10.0.0.9") {
		t.Fatal("the first connection on a limiter built with a nil clock must pass")
	}
}

func TestConnRateLimiterCleanupEvictsOnlyWindowsOlderThanMaxAge(t *testing.T) {
	t.Parallel()
	cl, clock := newTestConnLimiter()
	const maxAge = time.Minute

	cl.allowConnect("10.0.0.1")
	clock.advance(maxAge)
	cl.allowConnect("10.0.0.2")

	cl.cleanup(maxAge)
	if !connLimiterHasWindow(cl, "10.0.0.1") {
		t.Fatal("a window that started exactly maxAge ago must be kept")
	}

	clock.advance(time.Nanosecond)
	cl.cleanup(maxAge)
	if connLimiterHasWindow(cl, "10.0.0.1") {
		t.Fatal("a window that started more than maxAge ago must be evicted")
	}
	if !connLimiterHasWindow(cl, "10.0.0.2") {
		t.Fatal("a window that started inside maxAge must be kept")
	}
}

// ---------------------------------------------------------------------------
// commandRateLimiter tests
// ---------------------------------------------------------------------------

func newTestCommandLimiter() (*commandRateLimiter, *manualTestClock) {
	clock := newManualTestClock()
	return newCommandRateLimiter(clock.now), clock
}

func commandLimiterHasBucket(rl *commandRateLimiter, connKey string) bool {
	_, ok := rl.buckets.tokensOf(connKey)
	return ok
}

func TestCommandRateLimiterAllowsNormalRate(t *testing.T) {
	t.Parallel()
	rl, _ := newTestCommandLimiter()

	for i := 0; i < 50; i++ {
		if !rl.allowCommand("conn1") {
			t.Fatalf("command %d should be allowed", i+1)
		}
	}
}

func TestCommandRateLimiterBlocksFlood(t *testing.T) {
	t.Parallel()
	rl, _ := newTestCommandLimiter()

	if got := countAllowed(cmdBurstPerConn+1, func() bool { return rl.allowCommand("conn1") }); got != cmdBurstPerConn {
		t.Fatalf("a fresh bucket must grant exactly the burst; granted %d, want %d", got, cmdBurstPerConn)
	}
}

func TestCommandRateLimiterIsolatesConnections(t *testing.T) {
	t.Parallel()
	rl, _ := newTestCommandLimiter()

	// Exhaust burst for conn1.
	for i := 0; i < cmdBurstPerConn; i++ {
		rl.allowCommand("conn1")
	}

	// conn2 should not be affected.
	if !rl.allowCommand("conn2") {
		t.Fatal("different connection should not be affected")
	}
}

func TestCommandRateLimiterRefillFollowsInjectedClock(t *testing.T) {
	t.Parallel()
	rl, clock := newTestCommandLimiter()
	allowConn := func() bool { return rl.allowCommand("conn1") }

	if got := countAllowed(cmdBurstPerConn+1, allowConn); got != cmdBurstPerConn {
		t.Fatalf("precondition: fresh bucket grants the burst; granted %d", got)
	}

	clock.advance(time.Second / 2)
	if got, want := countAllowed(cmdBurstPerConn, allowConn), int(cmdRefillRate/2); got != want {
		t.Fatalf("half a second must restore exactly half the per-second rate; granted %d, want %d", got, want)
	}

	clock.advance(time.Second)
	if got := countAllowed(cmdBurstPerConn, allowConn); got != int(cmdRefillRate) {
		t.Fatalf("one second must restore exactly cmdRefillRate tokens; granted %d, want %d", got, int(cmdRefillRate))
	}

	clock.advance(time.Hour)
	if got := countAllowed(cmdBurstPerConn+1, allowConn); got != cmdBurstPerConn {
		t.Fatalf("a long idle period must refill to the burst and no further; granted %d, want %d", got, cmdBurstPerConn)
	}
}

func TestCommandRateLimiterRemoveConn(t *testing.T) {
	t.Parallel()
	rl, _ := newTestCommandLimiter()
	rl.allowCommand("conn1")
	rl.removeConn("conn1")

	if commandLimiterHasBucket(rl, "conn1") {
		t.Fatal("bucket should be removed after removeConn")
	}
}

func TestCommandRateLimiterCleanupEvictsOnlyBucketsIdlePastMaxAge(t *testing.T) {
	t.Parallel()
	rl, clock := newTestCommandLimiter()
	const maxAge = time.Minute

	rl.allowCommand("conn1")
	clock.advance(maxAge)
	rl.allowCommand("conn2")

	rl.cleanup(maxAge)
	if !commandLimiterHasBucket(rl, "conn1") {
		t.Fatal("a bucket idle for exactly maxAge must be kept")
	}

	clock.advance(time.Nanosecond)
	rl.cleanup(maxAge)
	if commandLimiterHasBucket(rl, "conn1") {
		t.Fatal("a bucket idle for longer than maxAge must be evicted")
	}
	if !commandLimiterHasBucket(rl, "conn2") {
		t.Fatal("a bucket used inside maxAge must be kept")
	}
}
