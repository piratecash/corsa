package routing

import (
	"testing"
	"time"
)

// Bad-hops hysteresis regression suite (fast-invalidation recidivism
// hold-down). Field data showed single Identities cycling
// invalidate→re-learn→invalidate hundreds of times per hour through
// the P3 branch — every cycle a storage mutation, a dirty-snapshot
// rebuild and a warn log. The hysteresis arms a per-Identity
// hold-down once accepted fast-invalidations exceed the per-window
// budget; while armed, further bad-hops claims are dropped before
// any storage mutation, while sane-hops claims (the recovery path)
// pass untouched. Re-arms inside the recidivism window escalate the
// hold-down exponentially up to DefaultBadHopsHoldDownMax.

// hysteresisFixture returns a table with fast invalidation enabled
// (MaxSaneHops=8) and the given hysteresis knobs, plus a mutable
// clock anchor.
func hysteresisFixture(opts ...TableOption) (*Table, *time.Time) {
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	base := []TableOption{
		WithClock(func() time.Time { return now }),
		WithLocalOrigin("node-A"),
		WithMaxSaneHops(8),
	}
	return NewTable(append(base, opts...)...), &now
}

// ingestBadHops sends one bad-hops (Hops=10) announcement for dest
// via the given uplink at the given SeqNo and returns the status.
func ingestBadHops(t *testing.T, tbl *Table, dest, uplink PeerIdentity, seq uint64) RouteUpdateStatus {
	t.Helper()
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: "src-x", NextHop: uplink,
		Hops: 10, SeqNo: seq, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute(seq=%d): %v", seq, err)
	}
	return status
}

// TestBadHopsHysteresis_ArmsAndDropsAfterBudget pins the core
// contract: the first budget-worth of bad-hops claims are processed
// normally (tombstone churn, FastInvalidations bumps), the arming
// event is counted once, and every further bad-hops claim during
// hold-down is rejected WITHOUT touching the FastInvalidations
// counter (no storage mutation happened).
func TestBadHopsHysteresis_ArmsAndDropsAfterBudget(t *testing.T) {
	tbl, _ := hysteresisFixture()
	const dest, uplink PeerIdentity = "dest", "u-1"

	// Budget is DefaultMaxBadHopsPerWindow (3): events 1-3 stay
	// under the arm condition (count > budget), event 4 arms.
	for seq := uint64(1); seq <= 4; seq++ {
		if got := ingestBadHops(t, tbl, dest, uplink, seq); got != RouteAccepted {
			t.Fatalf("pre-hold-down bad-hops claim seq=%d must be accepted as tombstone, got %v", seq, got)
		}
	}
	stats := tbl.CapStats()
	if stats.FastInvalidations != 4 {
		t.Fatalf("FastInvalidations=%d after 4 accepted invalidations, want 4", stats.FastInvalidations)
	}
	if stats.BadHopsHoldowns != 1 {
		t.Fatalf("BadHopsHoldowns=%d after budget exceeded, want 1 (edge-triggered arm)", stats.BadHopsHoldowns)
	}

	// During hold-down: dropped cheaply, no counter movement.
	if got := ingestBadHops(t, tbl, dest, uplink, 5); got != RouteRejected {
		t.Fatalf("bad-hops claim during hold-down must be rejected, got %v", got)
	}
	if got := ingestBadHops(t, tbl, dest, "u-2", 6); got != RouteRejected {
		t.Fatalf("bad-hops claim via OTHER uplink during hold-down must be rejected too, got %v", got)
	}
	stats = tbl.CapStats()
	if stats.FastInvalidations != 4 {
		t.Fatalf("FastInvalidations=%d after hold-down drops, want 4 (drops must not mutate storage)", stats.FastInvalidations)
	}
	if stats.BadHopsHoldowns != 1 {
		t.Fatalf("BadHopsHoldowns=%d, want 1 — no re-arm during active hold-down", stats.BadHopsHoldowns)
	}
}

// TestBadHopsHysteresis_SaneRecoveryPassesDuringHoldDown pins the
// safety property that makes the hysteresis safe to enable by
// default: hold-down suppresses only Hops > MaxSaneHops claims. A
// legitimate loop-free recovery announce (sane hops, strictly newer
// SeqNo) is admitted through the standard branches and becomes
// selectable immediately.
func TestBadHopsHysteresis_SaneRecoveryPassesDuringHoldDown(t *testing.T) {
	tbl, _ := hysteresisFixture()
	const dest, uplink PeerIdentity = "dest", "u-1"

	for seq := uint64(1); seq <= 4; seq++ {
		ingestBadHops(t, tbl, dest, uplink, seq)
	}
	if tbl.CapStats().BadHopsHoldowns != 1 {
		t.Fatal("setup: hold-down must be armed")
	}

	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: "src-x", NextHop: uplink,
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if err != nil || status != RouteAccepted {
		t.Fatalf("sane-hops recovery during hold-down must be accepted: status=%v err=%v", status, err)
	}
	routes := tbl.Lookup(dest)
	if len(routes) != 1 {
		t.Fatalf("recovered route must be selectable, Lookup returned %d entries", len(routes))
	}
	if routes[0].Hops != 2 {
		t.Fatalf("recovered route Hops=%d, want 2", routes[0].Hops)
	}
}

// TestBadHopsHysteresis_EscalatesForRecidivists pins the exponential
// escalation: a second arm inside the recidivism window doubles the
// hold-down duration.
func TestBadHopsHysteresis_EscalatesForRecidivists(t *testing.T) {
	const holdBase = time.Minute
	tbl, now := hysteresisFixture(
		WithBadHopsWindow(10*time.Second),
		WithBadHopsHoldDown(holdBase),
	)
	const dest, uplink PeerIdentity = "dest", "u-1"

	// Strike 1.
	for seq := uint64(1); seq <= 4; seq++ {
		ingestBadHops(t, tbl, dest, uplink, seq)
	}
	armedAt := *now
	tbl.mu.Lock()
	bs := tbl.flap.badHops[dest]
	tbl.mu.Unlock()
	if bs == nil || bs.strikes != 1 {
		t.Fatalf("first arm must record strikes=1, got %+v", bs)
	}
	if got := bs.holdUntil; !got.Equal(armedAt.Add(holdBase)) {
		t.Fatalf("first-strike hold-down = %v, want base %v", got.Sub(armedAt), holdBase)
	}

	// Release: advance past hold-down (but inside the recidivism
	// window) and run the tick so holdUntil is cleared.
	*now = armedAt.Add(holdBase + time.Second)
	tbl.TickTTL()
	tbl.mu.Lock()
	bs = tbl.flap.badHops[dest]
	tbl.mu.Unlock()
	if bs == nil {
		t.Fatal("recidivism anchor must survive release inside the window")
	}
	if !bs.holdUntil.IsZero() {
		t.Fatalf("tick must clear expired hold-down, got holdUntil=%v", bs.holdUntil)
	}

	// Strike 2: re-offend immediately — duration must double.
	reArmedAt := *now
	for seq := uint64(10); seq <= 13; seq++ {
		ingestBadHops(t, tbl, dest, uplink, seq)
	}
	tbl.mu.Lock()
	bs = tbl.flap.badHops[dest]
	tbl.mu.Unlock()
	if bs.strikes != 2 {
		t.Fatalf("second arm inside recidivism window must record strikes=2, got %d", bs.strikes)
	}
	if got := bs.holdUntil; !got.Equal(reArmedAt.Add(2 * holdBase)) {
		t.Fatalf("second-strike hold-down = %v, want doubled %v", got.Sub(reArmedAt), 2*holdBase)
	}
	if tbl.CapStats().BadHopsHoldowns != 2 {
		t.Fatalf("BadHopsHoldowns=%d, want 2", tbl.CapStats().BadHopsHoldowns)
	}
}

// TestBadHopsHysteresis_StrikesResetAfterQuietPeriod pins the decay
// side of recidivism: an Identity that stays quiet past
// DefaultBadHopsRecidivismWindow is forgotten entirely (entry
// purged by the tick) and a later relapse starts over at strike 1 /
// base duration.
func TestBadHopsHysteresis_StrikesResetAfterQuietPeriod(t *testing.T) {
	const holdBase = time.Minute
	tbl, now := hysteresisFixture(
		WithBadHopsWindow(10*time.Second),
		WithBadHopsHoldDown(holdBase),
	)
	const dest, uplink PeerIdentity = "dest", "u-1"

	for seq := uint64(1); seq <= 4; seq++ {
		ingestBadHops(t, tbl, dest, uplink, seq)
	}

	// Quiet stretch: past hold-down AND past the recidivism window.
	*now = now.Add(DefaultBadHopsRecidivismWindow + time.Minute)
	tbl.TickTTL()
	tbl.mu.Lock()
	_, survived := tbl.flap.badHops[dest]
	tbl.mu.Unlock()
	if survived {
		t.Fatal("cold entry (no events, no hold-down, recidivism window elapsed) must be purged by the tick")
	}

	// Relapse: starts over at strike 1 with the base duration.
	relapseAt := *now
	for seq := uint64(20); seq <= 23; seq++ {
		ingestBadHops(t, tbl, dest, uplink, seq)
	}
	tbl.mu.Lock()
	bs := tbl.flap.badHops[dest]
	tbl.mu.Unlock()
	if bs == nil || bs.strikes != 1 {
		t.Fatalf("relapse after quiet period must reset to strikes=1, got %+v", bs)
	}
	if got := bs.holdUntil; !got.Equal(relapseAt.Add(holdBase)) {
		t.Fatalf("relapse hold-down = %v, want base %v", got.Sub(relapseAt), holdBase)
	}
}

// TestBadHopsHysteresis_DisabledKeepsPerEventBehaviour pins the
// kill-switch: WithMaxBadHopsPerWindow(0) disables the hysteresis
// and every bad-hops claim keeps flowing through the per-event P3
// guard (tombstone + counter), no matter the volume.
func TestBadHopsHysteresis_DisabledKeepsPerEventBehaviour(t *testing.T) {
	tbl, _ := hysteresisFixture(WithMaxBadHopsPerWindow(0))
	const dest, uplink PeerIdentity = "dest", "u-1"

	for seq := uint64(1); seq <= 10; seq++ {
		if got := ingestBadHops(t, tbl, dest, uplink, seq); got != RouteAccepted {
			t.Fatalf("with hysteresis disabled every strictly-newer bad-hops claim must be accepted, seq=%d got %v", seq, got)
		}
	}
	stats := tbl.CapStats()
	if stats.FastInvalidations != 10 {
		t.Fatalf("FastInvalidations=%d, want 10", stats.FastInvalidations)
	}
	if stats.BadHopsHoldowns != 0 {
		t.Fatalf("BadHopsHoldowns=%d with hysteresis disabled, want 0", stats.BadHopsHoldowns)
	}
}
