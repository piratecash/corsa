package rpc

import (
	"testing"
	"time"
)

func TestAuthRateLimiterAllowsInitially(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()
	if al.isLockedOut("10.0.0.1") {
		t.Fatal("new IP should not be locked out")
	}
}

func TestAuthRateLimiterLocksOutAfterThreshold(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()

	ip := "10.0.0.1"
	for i := 0; i < authMaxAttempts; i++ {
		al.recordFailure(ip)
	}

	if !al.isLockedOut(ip) {
		t.Fatal("IP should be locked out after max attempts")
	}
}

func TestAuthRateLimiterIsolatesIPs(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()

	for i := 0; i < authMaxAttempts; i++ {
		al.recordFailure("10.0.0.1")
	}

	if al.isLockedOut("10.0.0.2") {
		t.Fatal("different IP should not be locked out")
	}
}

func TestAuthRateLimiterResetClearsFailures(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()

	ip := "10.0.0.1"
	for i := 0; i < authMaxAttempts-1; i++ {
		al.recordFailure(ip)
	}
	al.resetFailures(ip)
	al.recordFailure(ip)

	if al.isLockedOut(ip) {
		t.Fatal("IP should not be locked out after reset + single failure")
	}
}

func TestAuthRateLimiterWindowPruning(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()
	ip := "10.0.0.1"

	// Manually inject old failures that fall outside the window.
	al.mu.Lock()
	rec := &authFailureRecord{}
	oldTime := time.Now().Add(-authWindowDuration - time.Minute)
	for i := 0; i < authMaxAttempts; i++ {
		rec.attempts = append(rec.attempts, oldTime)
	}
	al.failures[ip] = rec
	al.mu.Unlock()

	// New failure should not trigger lockout (old ones should be pruned).
	al.recordFailure(ip)
	if al.isLockedOut(ip) {
		t.Fatal("old failures should be pruned from window")
	}
}

func TestAuthRateLimiterCleanup(t *testing.T) {
	t.Parallel()
	al := newAuthRateLimiter()
	al.recordFailure("10.0.0.1")

	al.cleanup()

	al.mu.Lock()
	// After cleanup, entry with a single recent attempt should remain.
	_, exists := al.failures["10.0.0.1"]
	al.mu.Unlock()
	if !exists {
		t.Fatal("recent failure should survive cleanup")
	}
}
