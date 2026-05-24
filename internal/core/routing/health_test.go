package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestRouteHealth_StringRoundTrip — RouteHealth.String returns stable
// lower-case wire labels used by RPC observability and structured
// logs. The labels are part of the surface contract for fetchRouteHealth
// (PR 11.5) and must not silently change.
func TestRouteHealth_StringRoundTrip(t *testing.T) {
	cases := []struct {
		h    RouteHealth
		want string
	}{
		{HealthGood, "good"},
		{HealthQuestionable, "questionable"},
		{HealthBad, "bad"},
		{HealthDead, "dead"},
		{RouteHealth(255), "unknown"},
	}
	for _, c := range cases {
		if got := c.h.String(); got != c.want {
			t.Fatalf("RouteHealth(%d).String() = %q, want %q", c.h, got, c.want)
		}
	}
}

// TestRouteHealth_GoodToQuestionableTransitionAt60Seconds verifies the
// passive-timeline transition: a Good pair with 60 s of hop_ack idle
// transitions to Questionable. The boundary at exactly 60 s qualifies —
// applyIdleTick uses idle >= threshold semantics, so 60.000s and above
// transition.
func TestRouteHealth_GoodToQuestionableTransitionAt60Seconds(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     "id-target",
		Uplink:       "id-uplink",
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}

	// 59s idle — still Good.
	state.applyIdleTick(base.Add(59 * time.Second))
	if state.Health != HealthGood {
		t.Fatalf("after 59s idle: Health = %s, want good", state.Health)
	}

	// 60s idle — transitions to Questionable.
	now := base.Add(60 * time.Second)
	state.applyIdleTick(now)
	if state.Health != HealthQuestionable {
		t.Fatalf("after 60s idle: Health = %s, want questionable", state.Health)
	}
	if !state.TransitionAt.Equal(now) {
		t.Fatalf("TransitionAt = %v, want %v", state.TransitionAt, now)
	}
}

// TestRouteHealth_QuestionableToBadAtHopAckTimeout verifies the
// passive Bad transition at 122 s idle, independent of probe failures.
func TestRouteHealth_QuestionableToBadAtHopAckTimeout(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     "id-target",
		Uplink:       "id-uplink",
		Health:       HealthQuestionable,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(122 * time.Second))
	if state.Health != HealthBad {
		t.Fatalf("after 122s idle: Health = %s, want bad", state.Health)
	}
}

// TestRouteHealth_BadToDeadAt182s verifies the terminal passive
// transition. Dead pairs are excluded from selection by CompositeScore
// and locally invalidated by the caller.
func TestRouteHealth_BadToDeadAt182s(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     "id-target",
		Uplink:       "id-uplink",
		Health:       HealthBad,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(182 * time.Second))
	if state.Health != HealthDead {
		t.Fatalf("after 182s idle: Health = %s, want dead", state.Health)
	}
}

// TestRouteHealth_IdleTickSkipsBadWhenLatentToDead verifies that a
// single late tick at >=182s correctly promotes Questionable straight
// to Dead, in case the ticker missed an intermediate run (e.g.,
// process-pause). The reverse-severity check inside applyIdleTick
// matches the Dead branch first.
func TestRouteHealth_IdleTickSkipsBadWhenLatentToDead(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     "id-target",
		Uplink:       "id-uplink",
		Health:       HealthQuestionable,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(300 * time.Second))
	if state.Health != HealthDead {
		t.Fatalf("after 300s idle: Health = %s, want dead", state.Health)
	}
}

// TestRouteHealth_HopAckRestoresGoodFromAnyState verifies that a
// hop_ack received at any (Identity, Uplink) pair fully restores Good
// — passive confirmation is the strongest signal in the state
// machine. ProbeFailures is reset to zero.
func TestRouteHealth_HopAckRestoresGoodFromAnyState(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	for _, from := range []RouteHealth{HealthQuestionable, HealthBad, HealthDead} {
		state := &RouteHealthState{
			Health:        from,
			ProbeFailures: 5,
			LastHopAck:    base,
			TransitionAt:  base,
		}
		now := base.Add(100 * time.Second)
		state.applyHopAck(now)
		if state.Health != HealthGood {
			t.Fatalf("from=%s: Health after hop_ack = %s, want good", from, state.Health)
		}
		if state.ProbeFailures != 0 {
			t.Fatalf("from=%s: ProbeFailures after hop_ack = %d, want 0", from, state.ProbeFailures)
		}
		if !state.LastHopAck.Equal(now) {
			t.Fatalf("from=%s: LastHopAck = %v, want %v", from, state.LastHopAck, now)
		}
	}
}

// TestRouteHealth_ProbeAckReachableRestoresGood — a probe response
// with reachable=true behaves like hop_ack: Good + reset
// ProbeFailures. LastHopAck is refreshed because the ack proves the
// uplink can serve the target.
func TestRouteHealth_ProbeAckReachableRestoresGood(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:        HealthBad,
		ProbeFailures: 4,
		LastHopAck:    base,
		TransitionAt:  base,
	}
	now := base.Add(200 * time.Second)
	state.applyProbeAck(true, now)
	if state.Health != HealthGood {
		t.Fatalf("Health after probe_ack(true) = %s, want good", state.Health)
	}
	if state.ProbeFailures != 0 {
		t.Fatalf("ProbeFailures = %d, want 0", state.ProbeFailures)
	}
	if !state.LastHopAck.Equal(now) {
		t.Fatalf("LastHopAck = %v, want %v (probe_ack(true) refreshes LastHopAck)", state.LastHopAck, now)
	}
}

// TestRouteHealth_ProbeAckUnreachableIncrementsFailures verifies the
// non-reachable path: ProbeFailures grows, threshold crossing forces
// Bad. Bad/Dead are pulled back to Questionable first because the
// remote uplink demonstrably acked the probe.
func TestRouteHealth_ProbeAckUnreachableIncrementsFailures(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:       HealthQuestionable,
		LastHopAck:   base,
		TransitionAt: base,
	}
	// First two failures — stay Questionable.
	state.applyProbeAck(false, base.Add(15*time.Second))
	if state.Health != HealthQuestionable || state.ProbeFailures != 1 {
		t.Fatalf("after 1 fail: Health=%s, ProbeFailures=%d", state.Health, state.ProbeFailures)
	}
	state.applyProbeAck(false, base.Add(30*time.Second))
	if state.Health != HealthQuestionable || state.ProbeFailures != 2 {
		t.Fatalf("after 2 fails: Health=%s, ProbeFailures=%d", state.Health, state.ProbeFailures)
	}
	// Third failure — crosses HealthProbeFailureThreshold=3 → Bad.
	state.applyProbeAck(false, base.Add(45*time.Second))
	if state.Health != HealthBad {
		t.Fatalf("after 3 fails: Health=%s, want bad", state.Health)
	}
	if state.ProbeFailures != 3 {
		t.Fatalf("ProbeFailures = %d, want 3", state.ProbeFailures)
	}
}

// TestRouteHealth_ProbeAckFromBadRewakensToQuestionable — the
// asymmetric rewake path: a Bad pair that receives any probe_ack
// (even reachable=false) is first lifted to Questionable because the
// uplink proved it can answer. Threshold check then re-evaluates.
func TestRouteHealth_ProbeAckFromBadRewakensToQuestionable(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:        HealthBad,
		ProbeFailures: 0, // pretend Bad was entered via passive idle, not probe
		LastHopAck:    base,
		TransitionAt:  base,
	}
	state.applyProbeAck(false, base.Add(125*time.Second))
	if state.Health != HealthQuestionable {
		t.Fatalf("Bad + 1 probe_ack(false): Health=%s, want questionable (ProbeFailures=1 below threshold)", state.Health)
	}
	if state.ProbeFailures != 1 {
		t.Fatalf("ProbeFailures = %d, want 1", state.ProbeFailures)
	}
}

// TestRouteHealth_ProbeFailureCrossesThreshold — timeout path: 3
// applyProbeFailure calls on a Questionable pair transitions to Bad
// independently of the passive 122 s hop_ack timeline.
func TestRouteHealth_ProbeFailureCrossesThreshold(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:       HealthQuestionable,
		LastHopAck:   base,
		TransitionAt: base,
	}
	for i := 1; i <= HealthProbeFailureThreshold; i++ {
		state.applyProbeFailure(base.Add(time.Duration(i) * 15 * time.Second))
		if i < HealthProbeFailureThreshold {
			if state.Health != HealthQuestionable {
				t.Fatalf("after %d failures: Health=%s, want questionable", i, state.Health)
			}
		}
	}
	if state.Health != HealthBad {
		t.Fatalf("after %d failures: Health=%s, want bad", HealthProbeFailureThreshold, state.Health)
	}
}

// TestRouteHealth_ProbeFailureDoesNotResurrectDead verifies that
// applyProbeFailure on an already-Dead pair leaves it Dead. Probe
// failures cannot "demote" Dead back to Bad — only a successful
// confirmation (hop_ack or probe_ack reachable=true) can transition
// out of Dead.
func TestRouteHealth_ProbeFailureDoesNotResurrectDead(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:       HealthDead,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyProbeFailure(base.Add(200 * time.Second))
	state.applyProbeFailure(base.Add(215 * time.Second))
	state.applyProbeFailure(base.Add(230 * time.Second))
	if state.Health != HealthDead {
		t.Fatalf("Health after probe failures on Dead = %s, want dead", state.Health)
	}
}

// TestRouteHealth_TransitionAtStampedOnEveryChange verifies that
// TransitionAt is updated only on actual state transitions, not on
// every applyHopAck. This matters for RPC observability: "transitioned
// 12s ago" should reflect the actual state change, not the most recent
// confirmation.
func TestRouteHealth_TransitionAtStampedOnEveryChange(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}
	// Repeated hop_ack while Good — no transition, TransitionAt
	// stays at base.
	state.applyHopAck(base.Add(30 * time.Second))
	state.applyHopAck(base.Add(45 * time.Second))
	if !state.TransitionAt.Equal(base) {
		t.Fatalf("TransitionAt = %v, want %v (no state change should not bump TransitionAt)", state.TransitionAt, base)
	}
	if !state.LastHopAck.Equal(base.Add(45 * time.Second)) {
		t.Fatalf("LastHopAck = %v, want %v", state.LastHopAck, base.Add(45*time.Second))
	}
	// Actual transition Good→Questionable via tick.
	transitionTime := base.Add(70 * time.Second)
	// Force the transition: stamp LastHopAck back to base for the
	// idle-since calculation to trigger.
	state.LastHopAck = base
	state.applyIdleTick(transitionTime)
	if state.Health != HealthQuestionable {
		t.Fatalf("state did not transition, got %s", state.Health)
	}
	if !state.TransitionAt.Equal(transitionTime) {
		t.Fatalf("TransitionAt = %v, want %v on actual transition", state.TransitionAt, transitionTime)
	}
}

// TestHealthStore_EnsureLockedCreatesGood — fresh upsert creates a
// HealthGood entry stamped at `now` with LastHopAck=now (the
// applyIdleTick timeline reference) and Confirmed=false (no real
// positive evidence yet). ensureLocked is the writer-side upsert
// used by hop_ack and probe_ack handlers as well as the
// UpdateRoute admission path.
//
// The two-field split (LastHopAck as a timer reference vs.
// Confirmed as the user-visible "ever confirmed?" signal) is
// what lets the RPC layer omit last_hop_ack for never-confirmed
// pairs while keeping the passive-timeline machinery
// (applyIdleTick) anchored at the creation moment. See PR 11.15
// P3 / 11.16 doc-comments on the Confirmed field and on
// ensureLocked for the regression history.
func TestHealthStore_EnsureLockedCreatesGood(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	state := store.ensureLocked("id-a", "id-uplink-1", now)
	if state.Health != HealthGood {
		t.Fatalf("new state Health = %s, want good", state.Health)
	}
	if !state.LastHopAck.Equal(now) {
		t.Fatalf("LastHopAck = %v, want %v (applyIdleTick timer reference)", state.LastHopAck, now)
	}
	if state.Confirmed {
		t.Fatal("fresh entry has Confirmed=true; want false (no real evidence yet — PR 11.15 P3 / 11.16)")
	}
	if !state.TransitionAt.Equal(now) {
		t.Fatalf("TransitionAt = %v, want %v", state.TransitionAt, now)
	}
	if state.Identity != domain.PeerIdentity("id-a") {
		t.Fatalf("Identity = %q, want id-a", state.Identity)
	}
	if state.Uplink != domain.PeerIdentity("id-uplink-1") {
		t.Fatalf("Uplink = %q, want id-uplink-1", state.Uplink)
	}
}

// TestHealthStore_EnsureLockedReturnsExisting verifies that a second
// ensureLocked call for the same pair returns the existing state
// pointer — not a fresh one. This keeps caller-side mutations
// (apply* methods) visible across writer paths.
func TestHealthStore_EnsureLockedReturnsExisting(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	first := store.ensureLocked("id-a", "id-uplink-1", now)
	first.RTT = 42 * time.Millisecond

	second := store.ensureLocked("id-a", "id-uplink-1", now.Add(time.Minute))
	if first != second {
		t.Fatal("ensureLocked returned a fresh state for an existing pair")
	}
	if second.RTT != 42*time.Millisecond {
		t.Fatalf("RTT lost across ensureLocked: got %v, want 42ms", second.RTT)
	}
}

// TestHealthStore_ScopedToUplink_HopAckForUplinkADoesNotAffectUplinkB —
// the critical Phase 2 invariant: per-uplink scoping. A hop_ack
// observed for (Identity X, Uplink A) does NOT update health for
// (Identity X, Uplink B). This is what makes RouteHealthState useful
// in the post-Phase-1 multi-uplink-per-Identity model.
func TestHealthStore_ScopedToUplink_HopAckForUplinkADoesNotAffectUplinkB(t *testing.T) {
	store := newHealthStore()
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	stateA := store.ensureLocked("id-target", "id-uplink-A", base)
	stateB := store.ensureLocked("id-target", "id-uplink-B", base)

	// Force B into Questionable via a passive tick after A's recent
	// hop_ack.
	stateB.applyIdleTick(base.Add(70 * time.Second))
	if stateB.Health != HealthQuestionable {
		t.Fatalf("setup: stateB.Health = %s, want questionable", stateB.Health)
	}

	// Apply a hop_ack to A — must not touch B.
	stateA.applyHopAck(base.Add(80 * time.Second))

	if stateA.Health != HealthGood {
		t.Fatalf("stateA.Health = %s, want good", stateA.Health)
	}
	if stateB.Health != HealthQuestionable {
		t.Fatalf("stateB.Health = %s, want questionable (untouched by uplink-A activity)", stateB.Health)
	}
}

// TestHealthStore_GetLockedReturnsNilForUnknownPair — read-side
// behaviour on a cold pair. Lookup-path callers rely on this to fall
// back to nil-health CompositeScore (by-hops + source bonus).
func TestHealthStore_GetLockedReturnsNilForUnknownPair(t *testing.T) {
	store := newHealthStore()
	if got := store.getLocked("id-a", "id-uplink-1"); got != nil {
		t.Fatalf("getLocked on cold pair = %v, want nil", got)
	}
}

// TestHealthStore_EvictUplinkLocked verifies tight-sync eviction of a
// single uplink — used when one peer withdraws its claim to Identity
// while other uplinks remain. Other pairs untouched.
func TestHealthStore_EvictUplinkLocked(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store.ensureLocked("id-x", "id-uplink-1", now)
	store.ensureLocked("id-x", "id-uplink-2", now)
	store.ensureLocked("id-y", "id-uplink-1", now)

	store.evictUplinkLocked("id-x", "id-uplink-1")

	if store.getLocked("id-x", "id-uplink-1") != nil {
		t.Fatal("evicted pair (id-x, id-uplink-1) still tracked")
	}
	if store.getLocked("id-x", "id-uplink-2") == nil {
		t.Fatal("untouched pair (id-x, id-uplink-2) lost")
	}
	if store.getLocked("id-y", "id-uplink-1") == nil {
		t.Fatal("untouched pair (id-y, id-uplink-1) lost — eviction must not cascade across identities")
	}
}

// TestHealthStore_EvictIdentityLocked verifies full-identity eviction —
// invoked when all uplinks for an identity withdrew and routeStore
// dropped the identity bucket entirely.
func TestHealthStore_EvictIdentityLocked(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store.ensureLocked("id-x", "id-uplink-1", now)
	store.ensureLocked("id-x", "id-uplink-2", now)
	store.ensureLocked("id-y", "id-uplink-1", now)

	store.evictIdentityLocked("id-x")

	if got := store.lenLocked(); got != 1 {
		t.Fatalf("lenLocked after evictIdentity = %d, want 1", got)
	}
	if store.getLocked("id-y", "id-uplink-1") == nil {
		t.Fatal("untouched identity id-y lost")
	}
}

// TestHealthStore_SnapshotLocked_DeepCopy — the snapshot returned
// by snapshotLocked is the data plane behind Table.HealthSnapshot
// and the synchronous fetchRouteHealth RPC handler (no
// atomic.Pointer cache, see snapshotLocked's and
// Table.HealthSnapshot's doc-comments). It must be a deep copy:
// mutating a returned RouteHealthState must not affect the live
// store state, so concurrent RPC callers cannot accidentally
// corrupt each other or the writers.
func TestHealthStore_SnapshotLocked_DeepCopy(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := store.ensureLocked("id-x", "id-uplink-1", now)
	state.RTT = 50 * time.Millisecond

	snap := store.snapshotLocked()
	if len(snap) != 1 {
		t.Fatalf("snapshotLocked() len = %d, want 1", len(snap))
	}

	// Mutate the snapshot copy.
	snap[0].RTT = 999 * time.Millisecond

	// Live state must remain unchanged.
	live := store.getLocked("id-x", "id-uplink-1")
	if live.RTT != 50*time.Millisecond {
		t.Fatalf("snapshot mutation leaked into live state: RTT = %v, want 50ms", live.RTT)
	}
}

// TestHealthStore_SnapshotLocked_EmptyReturnsNil — empty stores return
// nil from snapshot to let callers compare cheaply.
func TestHealthStore_SnapshotLocked_EmptyReturnsNil(t *testing.T) {
	store := newHealthStore()
	if got := store.snapshotLocked(); got != nil {
		t.Fatalf("snapshotLocked() on empty store = %v, want nil", got)
	}
}

// TestNewTable_InitialisesHealthStore — the Phase 2 PR 11.1 wire-up:
// every routing.Table built via NewTable has a non-nil health field
// ready for PR 11.2 to start writing into.
func TestNewTable_InitialisesHealthStore(t *testing.T) {
	tbl := NewTable()
	if tbl.health == nil {
		t.Fatal("NewTable() left health field nil — PR 11.1 must initialise it")
	}
	if got := tbl.health.lenLocked(); got != 0 {
		t.Fatalf("freshly initialised health store has %d entries, want 0", got)
	}
}
