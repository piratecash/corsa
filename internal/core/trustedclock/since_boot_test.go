package trustedclock

import (
	"errors"
	"testing"
	"time"
)

// sinceBootOrSkip reads SinceBoot once and skips the test when this platform
// has no such clock — measured by the call, not predicted from GOOS.
func sinceBootOrSkip(t *testing.T) time.Duration {
	t.Helper()
	reading, err := SinceBoot()
	if errors.Is(err, ErrSinceBootUnsupported) {
		t.Skipf("no clock since boot here: %v", err)
	}
	if err != nil {
		t.Fatalf("SinceBoot: %v", err)
	}
	return reading
}

// The clock since boot never goes back between two readings.
func TestSinceBootNeverGoesBack(t *testing.T) {
	t.Parallel()
	previous := sinceBootOrSkip(t)
	if previous <= 0 {
		t.Fatalf("SinceBoot = %v; a running system has been up for some time", previous)
	}
	for range 10000 {
		reading, err := SinceBoot()
		if err != nil {
			t.Fatalf("SinceBoot: %v", err)
		}
		if reading < previous {
			t.Fatalf("SinceBoot went back from %v to %v", previous, reading)
		}
		previous = reading
	}
}

// Across a pause the clock since boot moves as far as the process's own
// monotonic clock, within the coarsest resolution among the platforms
// (GetTickCount64 ticks at about 16 ms).
func TestSinceBootMovesWithElapsedTime(t *testing.T) {
	t.Parallel()
	const pause, tolerance = 300 * time.Millisecond, 100 * time.Millisecond
	before := sinceBootOrSkip(t)
	started := time.Now()
	time.Sleep(pause)
	after, err := SinceBoot()
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("SinceBoot: %v", err)
	}
	moved := after - before
	if moved < pause-tolerance {
		t.Fatalf("SinceBoot moved %v across a pause of %v", moved, pause)
	}
	if diff := (moved - elapsed).Abs(); diff > tolerance {
		t.Fatalf("SinceBoot moved %v while the monotonic clock moved %v: off by %v", moved, elapsed, diff)
	}
}
