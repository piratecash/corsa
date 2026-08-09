package datagram_test

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// baseTime is a fixed instant; every test below reasons in offsets from it
// so no assertion depends on the machine clock.
var baseTime = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

// replayKey builds a distinct, non-zero replay key per test case.
func replayKey(seed byte) domain.ReplayKey {
	var key domain.ReplayKey
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// manualClock is the injectable time source used across these tests.
type manualClock struct {
	now time.Time
}

func newClock(now time.Time) *manualClock      { return &manualClock{now: now} }
func (c *manualClock) Now() time.Time          { return c.now }
func (c *manualClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
func (c *manualClock) Set(t time.Time)         { c.now = t }

// TestBaseReplayDeadlineMeasuresTheWindowFromSignedAuthTime pins the ONE clamp
// the layer has for base_until, which ComputeDeadlines applies on the receive
// path and nothing else restates.
//
// The window is not an argument and cannot be one: it is wire-normative, so
// every node on the path reaches the same answer from the signed header alone.
// The moment of ARRIVAL is not an argument either, and that is the whole reason
// a frame held back in transit cannot occupy a slot longer than the same frame
// delivered at once — the rule is stated by the signature and needs no case of
// its own.
//
// The clamp as the RECEIVE PATH applies it is pinned where that path is tested:
// deadlines_test.go asserts both that replay_until never outlives auth.time plus
// the base window and its exact value. ComputeDeadlines reaches it through this
// same function, so the two cannot answer differently — which is the whole
// reason the cache no longer carries a window of its own.
func TestBaseReplayDeadlineMeasuresTheWindowFromSignedAuthTime(t *testing.T) {
	t.Parallel()

	authTime := baseTime
	window := domain.DatagramBaseReplayWindow

	tests := []struct {
		name        string
		replayUntil time.Time
		want        time.Time
	}{
		{
			name:        "long replay_until is clamped to auth.time + window",
			replayUntil: authTime.Add(time.Hour),
			want:        authTime.Add(window),
		},
		{
			name:        "short replay_until wins over the window",
			replayUntil: authTime.Add(time.Minute),
			want:        authTime.Add(time.Minute),
		},
		{
			name:        "equal values collapse to the same deadline",
			replayUntil: authTime.Add(window),
			want:        authTime.Add(window),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := datagram.BaseReplayDeadline(authTime, test.replayUntil)
			if !got.Equal(test.want) {
				t.Fatalf("base_until = %s, want %s", got, test.want)
			}
		})
	}
}

// The boundary is inclusive on both sides of the pipeline: at
// now == replay_until the key is still alive, so Validity and anti-replay
// cannot leave a one-second re-delivery window between them.
func TestReplayRetentionBoundaryIsInclusive(t *testing.T) {
	t.Parallel()

	until := baseTime.Add(domain.DatagramBaseReplayWindow)
	retention := datagram.NewReplayRetention(until)

	if !retention.AliveAt(until) {
		t.Fatal("key must still be alive at now == replay_until")
	}
	if retention.CleanupOnlyAt(until) {
		t.Fatal("cleanup-only phase must start strictly after replay_until")
	}
	if retention.AliveAt(until.Add(time.Nanosecond)) {
		t.Fatal("key must be dead strictly after replay_until")
	}
	if !retention.CleanupOnlyAt(until.Add(time.Nanosecond)) {
		t.Fatal("record must enter the cleanup-only phase after replay_until")
	}
}

// Semantic expiry and physical removal are two different events: a record
// that still owes something is kept past its deadline.
func TestReplayRetentionRemovableOnlyWhenNothingIsOwed(t *testing.T) {
	t.Parallel()

	until := baseTime.Add(time.Minute)
	retention := datagram.NewReplayRetention(until)
	after := until.Add(time.Second)

	if retention.RemovableAt(until, 0) {
		t.Fatal("a live record must not be removable")
	}
	if retention.RemovableAt(after, 1) {
		t.Fatal("an expired record with an obligation must not be removable")
	}
	if !retention.RemovableAt(after, 0) {
		t.Fatal("an expired record owing nothing must be removable")
	}
}
