package node

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// connRateLimiter tests
// ---------------------------------------------------------------------------

func TestConnRateLimiterAllowsNormalRate(t *testing.T) {
	t.Parallel()
	cl := newConnRateLimiter()

	// First connection from an IP should always be allowed.
	if !cl.allowConnect("192.168.1.1") {
		t.Fatal("first connection should be allowed")
	}
}

func TestConnRateLimiterBlocksFlood(t *testing.T) {
	t.Parallel()
	cl := newConnRateLimiter()

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
	cl := &connRateLimiter{
		windows:      make(map[string]*connWindow),
		maxPerWindow: 2,
		windowSize:   50 * time.Millisecond,
	}

	ip := "10.0.0.2"
	cl.allowConnect(ip)
	cl.allowConnect(ip)
	if cl.allowConnect(ip) {
		t.Fatal("should be blocked within window")
	}

	// Wait for window to expire.
	time.Sleep(60 * time.Millisecond)

	if !cl.allowConnect(ip) {
		t.Fatal("should be allowed after window reset")
	}
}

func TestConnRateLimiterIsolatesIPs(t *testing.T) {
	t.Parallel()
	cl := newConnRateLimiter()

	// Exhaust limit for one IP.
	for i := 0; i < defaultConnRateLimit; i++ {
		cl.allowConnect("10.0.0.1")
	}

	// Different IP should not be affected.
	if !cl.allowConnect("10.0.0.2") {
		t.Fatal("different IP should not be affected by rate limit on another IP")
	}
}

func TestConnRateLimiterEmptyIPAllowed(t *testing.T) {
	t.Parallel()
	cl := newConnRateLimiter()
	if !cl.allowConnect("") {
		t.Fatal("empty IP should always be allowed")
	}
}

func TestConnRateLimiterCleanup(t *testing.T) {
	t.Parallel()
	cl := &connRateLimiter{
		windows:      make(map[string]*connWindow),
		maxPerWindow: defaultConnRateLimit,
		windowSize:   defaultConnRateWindow,
	}

	cl.allowConnect("10.0.0.1")
	cl.allowConnect("10.0.0.2")
	if len(cl.windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(cl.windows))
	}

	// Cleanup with zero maxAge should remove everything.
	cl.cleanup(0)
	if len(cl.windows) != 0 {
		t.Fatalf("expected 0 windows after cleanup, got %d", len(cl.windows))
	}
}

// ---------------------------------------------------------------------------
// commandRateLimiter tests
// ---------------------------------------------------------------------------

func TestCommandRateLimiterAllowsNormalRate(t *testing.T) {
	t.Parallel()
	rl := newCommandRateLimiter()

	for i := 0; i < 50; i++ {
		if !rl.allowCommand("conn1") {
			t.Fatalf("command %d should be allowed", i+1)
		}
	}
}

func TestCommandRateLimiterBlocksFlood(t *testing.T) {
	t.Parallel()
	rl := newCommandRateLimiter()

	// Exhaust the burst.
	for i := 0; i < cmdBurstPerConn; i++ {
		rl.allowCommand("conn1")
	}

	// Next command should be rejected.
	if rl.allowCommand("conn1") {
		t.Fatal("command beyond burst should be rejected")
	}
}

func TestCommandRateLimiterIsolatesConnections(t *testing.T) {
	t.Parallel()
	rl := newCommandRateLimiter()

	// Exhaust burst for conn1.
	for i := 0; i < cmdBurstPerConn; i++ {
		rl.allowCommand("conn1")
	}

	// conn2 should not be affected.
	if !rl.allowCommand("conn2") {
		t.Fatal("different connection should not be affected")
	}
}

func TestCommandRateLimiterRemoveConn(t *testing.T) {
	t.Parallel()
	rl := newCommandRateLimiter()
	rl.allowCommand("conn1")
	rl.removeConn("conn1")

	rl.mu.Lock()
	_, exists := rl.buckets["conn1"]
	rl.mu.Unlock()
	if exists {
		t.Fatal("bucket should be removed after removeConn")
	}
}

func TestCommandRateLimiterCleanup(t *testing.T) {
	t.Parallel()
	rl := newCommandRateLimiter()
	rl.allowCommand("conn1")
	rl.allowCommand("conn2")

	rl.cleanup(0)

	rl.mu.Lock()
	count := len(rl.buckets)
	rl.mu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 buckets after cleanup, got %d", count)
	}
}
