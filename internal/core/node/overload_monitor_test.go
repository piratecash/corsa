package node

import (
	"runtime"
	"testing"
)

// TestOverloadMonitor_DisabledByZeroThreshold pins the rollout-default
// behaviour: with threshold <= 0 the monitor never reports overload,
// so existing deployments observe pre-Phase-0 behaviour exactly until
// they explicitly set CORSA_OVERLOAD_GOROUTINE_THRESHOLD.
func TestOverloadMonitor_DisabledByZeroThreshold(t *testing.T) {
	cases := []struct {
		name      string
		threshold int
	}{
		{"zero_threshold_disables", 0},
		{"negative_threshold_disables", -100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newOverloadMonitor(tc.threshold)
			// Even if NumGoroutine is large, disabled monitor must
			// always return false.
			for i := 0; i < 5; i++ {
				if m.IsOverloaded() {
					t.Fatalf("disabled monitor must not report overloaded (call %d)", i)
				}
			}
			if got := m.EngagedTotal(); got != 0 {
				t.Fatalf("disabled monitor must not increment EngagedTotal, got %d", got)
			}
		})
	}
}

// TestOverloadMonitor_EngagesAboveThreshold pins the trip-point
// behaviour. We pick a threshold below the current goroutine count so
// the monitor MUST report overloaded; and a threshold well above so
// it MUST NOT. EngagedTotal counts true-returns only.
func TestOverloadMonitor_EngagesAboveThreshold(t *testing.T) {
	current := runtime.NumGoroutine()

	t.Run("threshold_below_current_engages", func(t *testing.T) {
		m := newOverloadMonitor(current - 1) // current goroutines exceed threshold
		if !m.IsOverloaded() {
			t.Fatalf("expected IsOverloaded=true when threshold(%d) < NumGoroutine(%d)",
				current-1, runtime.NumGoroutine())
		}
		if got := m.EngagedTotal(); got != 1 {
			t.Fatalf("EngagedTotal should advance to 1 on engagement, got %d", got)
		}
		// Second call should also engage and advance counter.
		if !m.IsOverloaded() {
			t.Fatalf("expected sustained engagement on follow-up call")
		}
		if got := m.EngagedTotal(); got != 2 {
			t.Fatalf("EngagedTotal should advance to 2, got %d", got)
		}
	})

	t.Run("threshold_above_current_does_not_engage", func(t *testing.T) {
		m := newOverloadMonitor(current + 1_000_000) // unreachable in tests
		if m.IsOverloaded() {
			t.Fatalf("expected IsOverloaded=false when threshold >> NumGoroutine")
		}
		if got := m.EngagedTotal(); got != 0 {
			t.Fatalf("EngagedTotal must stay at 0 when not engaging, got %d", got)
		}
	})
}
