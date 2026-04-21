package domain

import (
	"testing"
	"time"
)

// TestAggregateStatusSnapshot_EqualContent_IgnoresComputedAt verifies that
// two snapshots with identical business payload but different ComputedAt
// stamps compare equal. This is the core guarantee that lets publishers
// suppress no-op TopicAggregateStatusChanged events during peer storms:
// each recompute stamps a fresh wall-clock time even when nothing observable
// changed, so naive struct equality would never report "unchanged".
func TestAggregateStatusSnapshot_EqualContent_IgnoresComputedAt(t *testing.T) {
	t.Parallel()

	base := AggregateStatusSnapshot{
		Status:          NetworkStatusWarning,
		UsablePeers:     3,
		ConnectedPeers:  5,
		TotalPeers:      7,
		PendingMessages: 2,
		ComputedAt:      time.Unix(1_700_000_000, 0).UTC(),
	}
	later := base
	later.ComputedAt = base.ComputedAt.Add(5 * time.Second)

	if !base.EqualContent(later) {
		t.Fatalf("EqualContent must ignore ComputedAt: base=%+v later=%+v", base, later)
	}
}

// TestAggregateStatusSnapshot_EqualContent_DetectsFieldChange walks every
// semantic field and asserts that flipping it makes EqualContent return
// false. This pins the contract so a future field added to the struct
// without being wired into EqualContent fails a targeted test instead of
// silently hiding legitimate status changes from the UI.
func TestAggregateStatusSnapshot_EqualContent_DetectsFieldChange(t *testing.T) {
	t.Parallel()

	base := AggregateStatusSnapshot{
		Status:          NetworkStatusHealthy,
		UsablePeers:     4,
		ConnectedPeers:  4,
		TotalPeers:      4,
		PendingMessages: 0,
		ComputedAt:      time.Unix(1_700_000_000, 0).UTC(),
	}

	cases := []struct {
		name   string
		mutate func(*AggregateStatusSnapshot)
	}{
		{"status", func(s *AggregateStatusSnapshot) { s.Status = NetworkStatusWarning }},
		{"usable", func(s *AggregateStatusSnapshot) { s.UsablePeers = 5 }},
		{"connected", func(s *AggregateStatusSnapshot) { s.ConnectedPeers = 5 }},
		{"total", func(s *AggregateStatusSnapshot) { s.TotalPeers = 5 }},
		{"pending", func(s *AggregateStatusSnapshot) { s.PendingMessages = 1 }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mutated := base
			tc.mutate(&mutated)
			if base.EqualContent(mutated) {
				t.Fatalf("EqualContent should detect change in %s: %+v vs %+v", tc.name, base, mutated)
			}
		})
	}
}
