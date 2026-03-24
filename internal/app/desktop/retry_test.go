package desktop

import (
	"testing"
	"time"
)

func TestRetryWithBackoff_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	result, ok := retryWithBackoff(3, time.Millisecond, func() (string, bool) {
		calls++
		return "hello", true
	})

	if !ok || result != "hello" {
		t.Fatalf("expected (hello, true), got (%s, %v)", result, ok)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryWithBackoff_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	result, ok := retryWithBackoff(4, time.Millisecond, func() (int, bool) {
		calls++
		if calls < 3 {
			return 0, false
		}
		return 42, true
	})

	if !ok || result != 42 {
		t.Fatalf("expected (42, true), got (%d, %v)", result, ok)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryWithBackoff_ExhaustsAttempts(t *testing.T) {
	calls := 0
	result, ok := retryWithBackoff(3, time.Millisecond, func() (string, bool) {
		calls++
		return "", false
	})

	if ok {
		t.Fatal("expected ok=false after exhausting attempts")
	}
	if result != "" {
		t.Fatalf("expected zero value, got %q", result)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryWithBackoff_BackoffIncreases(t *testing.T) {
	var timestamps []time.Time
	retryWithBackoff(3, 50*time.Millisecond, func() (bool, bool) {
		timestamps = append(timestamps, time.Now())
		return false, false
	})

	if len(timestamps) != 3 {
		t.Fatalf("expected 3 timestamps, got %d", len(timestamps))
	}

	// First gap should be ~50ms, second ~100ms.
	gap1 := timestamps[1].Sub(timestamps[0])
	gap2 := timestamps[2].Sub(timestamps[1])

	if gap1 < 30*time.Millisecond || gap1 > 150*time.Millisecond {
		t.Fatalf("first backoff gap %v out of expected range [30ms, 150ms]", gap1)
	}
	if gap2 < 60*time.Millisecond || gap2 > 300*time.Millisecond {
		t.Fatalf("second backoff gap %v out of expected range [60ms, 300ms]", gap2)
	}
	if gap2 <= gap1 {
		t.Fatalf("expected increasing backoff: gap1=%v gap2=%v", gap1, gap2)
	}
}
