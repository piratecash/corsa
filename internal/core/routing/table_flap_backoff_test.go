package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// triggerFlapBurst drives N consecutive add/remove cycles within the
// flap window so that recordWithdrawalLocked crosses the configured
// flapThreshold and arms hold-down. The advance step keeps each cycle
// inside the window, and the helper returns the wall-clock time after
// the burst so callers can compute the expected hold-down expiry.
//
// The helper does NOT advance the clock after the final remove —
// callers expect *current to equal the wall-clock at which the last
// hold-down was armed, so they can compare holdDownUntil against a
// known offset.
func triggerFlapBurst(t *testing.T, tbl *Table, peerID PeerIdentity, current *time.Time, threshold int) time.Time {
	t.Helper()
	for i := 0; i < threshold; i++ {
		mustAddDirect(t, tbl, peerID)
		mustRemoveDirect(t, tbl, peerID)
		if i < threshold-1 {
			*current = current.Add(2 * time.Second)
		}
	}
	return *current
}

// holdDownLeft returns how much hold-down time remains for peerID at
// the table's current clock. Zero or negative means hold-down has
// expired or was never armed.
func holdDownLeft(t *testing.T, tbl *Table, peerID PeerIdentity) time.Duration {
	t.Helper()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	fs := tbl.flap.state[peerID]
	if fs == nil {
		return 0
	}
	return fs.holdDownUntil.Sub(tbl.clock())
}

// consecutiveFlapsFor returns the per-peer streak counter for assertions.
func consecutiveFlapsFor(t *testing.T, tbl *Table, peerID PeerIdentity) int {
	t.Helper()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	fs := tbl.flap.state[peerID]
	if fs == nil {
		return 0
	}
	return fs.consecutiveFlaps
}

// TestRecordWithdrawal_ExpBackoff verifies that successive flap-bursts
// inside the stable window double the hold-down duration: 30s, 60s,
// 120s, … capped at MaxHoldDownDuration. Without exp-backoff a peer
// that keeps flapping would burn the same short hold-down in a tight
// loop, which is precisely the reconnect-storm amplifier this patch
// is closing.
func TestRecordWithdrawal_ExpBackoff(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	const base = 30 * time.Second
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(base),
	)

	// Burst 1: hold-down = base.
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 1 {
		t.Fatalf("after burst 1 consecutiveFlaps=%d, want 1", got)
	}
	if got := holdDownLeft(t, tbl, domaintest.ID("flappy")); got != base {
		t.Fatalf("after burst 1 hold-down=%v, want %v", got, base)
	}

	// Advance past hold-down but stay within the stable window so
	// consecutiveFlaps does NOT reset implicitly.
	current = current.Add(base + time.Second)

	// Burst 2: hold-down = 2*base.
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 2 {
		t.Fatalf("after burst 2 consecutiveFlaps=%d, want 2", got)
	}
	if got := holdDownLeft(t, tbl, domaintest.ID("flappy")); got != 2*base {
		t.Fatalf("after burst 2 hold-down=%v, want %v", got, 2*base)
	}

	// Burst 3: hold-down = 4*base = 120s, still under cap.
	current = current.Add(2*base + time.Second)
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 3 {
		t.Fatalf("after burst 3 consecutiveFlaps=%d, want 3", got)
	}
	if got := holdDownLeft(t, tbl, domaintest.ID("flappy")); got != 4*base {
		t.Fatalf("after burst 3 hold-down=%v, want %v", got, 4*base)
	}
}

// TestRecordWithdrawal_ExpBackoffCapsAtMax pins the upper bound so a
// chronically flapping peer cannot drive the hold-down past
// MaxHoldDownDuration. Without the cap, a long-running flapping
// session would compute multi-hour hold-downs that block legitimate
// recovery long after the peer stabilizes.
//
// We use a wide flapWindow (and therefore a wide stable window) so
// the gap between bursts can stay above the previous hold-down
// without crossing the implicit-reset threshold — that is the only
// way to keep consecutiveFlaps growing burst after burst.
func TestRecordWithdrawal_ExpBackoffCapsAtMax(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	const base = 60 * time.Second
	// flapWindow chosen so FlapStableWindowMultiplier×flapWindow
	// comfortably exceeds MaxHoldDownDuration. With multiplier=2 and
	// max=600s, anything ≥ 600s works; we pick 1h to leave headroom.
	const flapWindow = 1 * time.Hour
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(flapWindow),
		WithFlapThreshold(3),
		WithHoldDownDuration(base),
	)

	// Drive enough bursts that the unbounded multiplier would push
	// past MaxHoldDownDuration: 60 * 2^FlapBackoffShiftCap = 1920s,
	// well above the 600s cap.
	for i := 0; i < FlapBackoffShiftCap+3; i++ {
		triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
		// Stay inside the stable window so consecutiveFlaps keeps
		// growing, but past the previous hold-down so the next burst
		// is a real "new" burst rather than a no-op double-arm.
		current = current.Add(MaxHoldDownDuration + time.Second)
	}

	tbl.mu.Lock()
	fs := tbl.flap.state[domaintest.ID("flappy")]
	tbl.mu.Unlock()
	if fs == nil {
		t.Fatal("flap state must exist after bursts")
	}
	armed := fs.holdDownUntil.Sub(fs.lastFlapAt)
	if armed > MaxHoldDownDuration {
		t.Fatalf("armed hold-down=%v exceeds MaxHoldDownDuration=%v", armed, MaxHoldDownDuration)
	}
	if armed < base {
		t.Fatalf("armed hold-down=%v must be at least base=%v", armed, base)
	}
}

// TestFlapBackoff_ResetAfterStableWindow verifies the implicit reset
// path: a fresh withdrawal that lands more than
// FlapStableWindowMultiplier × flapWindow after the previous burst
// must be treated as a brand-new streak (consecutiveFlaps==1, hold
// down=base). Without this reset, a peer that flapped once a year
// ago would still see a multi-burst hold-down on its first new flap.
func TestFlapBackoff_ResetAfterStableWindow(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	const base = 30 * time.Second
	const flapWindow = 60 * time.Second
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(flapWindow),
		WithFlapThreshold(3),
		WithHoldDownDuration(base),
	)

	// Burst 1.
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 1 {
		t.Fatalf("after burst 1 consecutiveFlaps=%d, want 1", got)
	}

	// Advance past FlapStableWindowMultiplier × flapWindow + slack so
	// the implicit reset inside recordWithdrawalLocked must clear the
	// streak when the next burst lands.
	current = current.Add(time.Duration(FlapStableWindowMultiplier)*flapWindow + 5*time.Second)

	// Burst 2 starts fresh — first withdrawal alone won't arm hold-down,
	// so we run another full burst and check that the resulting streak
	// counter is 1 (reset), not 2 (preserved).
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 1 {
		t.Fatalf("after stable gap + burst consecutiveFlaps=%d, want 1 (reset)", got)
	}
	if got := holdDownLeft(t, tbl, domaintest.ID("flappy")); got != base {
		t.Fatalf("after stable gap + burst hold-down=%v, want %v (reset to base)", got, base)
	}
}

// TestRecordSuccessfulRouteAdd_ResetsStreak proves the explicit reset
// path that node.Service uses after a successful announce-plane
// round-trip: RecordSuccessfulRouteAdd clears consecutiveFlaps and
// lastFlapAt without touching withdrawTimes. The next flap-burst
// therefore starts at base hold-down again.
func TestRecordSuccessfulRouteAdd_ResetsStreak(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	const base = 30 * time.Second
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(base),
	)

	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 1 {
		t.Fatalf("after burst consecutiveFlaps=%d, want 1", got)
	}

	tbl.RecordSuccessfulRouteAdd(domaintest.ID("flappy"))
	if got := consecutiveFlapsFor(t, tbl, domaintest.ID("flappy")); got != 0 {
		t.Fatalf("after RecordSuccessfulRouteAdd consecutiveFlaps=%d, want 0", got)
	}

	// Advance past the previous hold-down so withdrawTimes are
	// outside the flap window and a fresh burst arms hold-down at
	// base again.
	current = current.Add(2 * time.Minute)
	triggerFlapBurst(t, tbl, domaintest.ID("flappy"), &current, 3)
	if got := holdDownLeft(t, tbl, domaintest.ID("flappy")); got != base {
		t.Fatalf("after reset + new burst hold-down=%v, want %v", got, base)
	}
}

// TestRecordSuccessfulRouteAdd_EmptyIdentityNoOp documents the
// no-allocation guard: an empty identity must not insert a fresh
// flap-state entry. Without this guard a misbehaving caller could
// pollute the flap-state map by passing zero-value identities.
func TestRecordSuccessfulRouteAdd_EmptyIdentityNoOp(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))
	tbl.RecordSuccessfulRouteAdd(domain.PeerIdentity{})
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if len(tbl.flap.state) != 0 {
		t.Fatalf("flap.state must remain empty after RecordSuccessfulRouteAdd(\"\"), got %d entries", len(tbl.flap.state))
	}
}

// TestHoldDownDurationForBurst_TableDriven covers the helper directly
// so future regressions to the multiplier table are caught even if
// the caller path is refactored.
func TestHoldDownDurationForBurst_TableDriven(t *testing.T) {
	const base = 30 * time.Second
	cases := []struct {
		consecutive int
		want        time.Duration
	}{
		{consecutive: 0, want: base},
		{consecutive: 1, want: base},
		{consecutive: 2, want: 2 * base},
		{consecutive: 3, want: 4 * base},
		{consecutive: 4, want: 8 * base},
		{consecutive: 5, want: 16 * base},
		// 32*base = 960s, exceeds MaxHoldDownDuration=600s.
		{consecutive: 6, want: MaxHoldDownDuration},
		{consecutive: 99, want: MaxHoldDownDuration},
	}
	for _, c := range cases {
		got := holdDownDurationForBurst(base, c.consecutive)
		if got != c.want {
			t.Errorf("holdDownDurationForBurst(base=%v, consecutive=%d) = %v, want %v",
				base, c.consecutive, got, c.want)
		}
	}
}

// TestHoldDownDurationForBurst_ZeroBaseDisables pins the contract that
// WithHoldDownDuration(0) disables hold-down for every burst index,
// not just the first. Without the base<=0 short-circuit in
// holdDownDurationForBurst, exp-backoff would push base*2^shift through
// the scaled<=0 branch on consecutiveFlaps>=2 and inflate a deliberately
// disabled hold-down into the maximum penalty. We assert across the
// full burst-index range so a regression on any single index fails
// the test loudly.
func TestHoldDownDurationForBurst_ZeroBaseDisables(t *testing.T) {
	cases := []int{0, 1, 2, 3, 4, 5, 6, FlapBackoffShiftCap + 5, 99}
	for _, consecutive := range cases {
		got := holdDownDurationForBurst(0, consecutive)
		if got != 0 {
			t.Errorf("holdDownDurationForBurst(base=0, consecutive=%d) = %v, want 0 (disabled)",
				consecutive, got)
		}
	}
}

// TestHoldDownDurationForBurst_NegativeBaseDisables guards the same
// short-circuit for any caller that hands in a negative duration —
// e.g. a config layer that uses a negative sentinel to mean "off".
// Negative bases must round-trip unchanged for every burst index so
// that downstream code can treat the result with the same now+d
// arithmetic without a special case.
func TestHoldDownDurationForBurst_NegativeBaseDisables(t *testing.T) {
	const base = -1 * time.Second
	for _, consecutive := range []int{0, 1, 2, 5, FlapBackoffShiftCap + 5} {
		got := holdDownDurationForBurst(base, consecutive)
		if got != base {
			t.Errorf("holdDownDurationForBurst(base=%v, consecutive=%d) = %v, want %v",
				base, consecutive, got, base)
		}
	}
}
