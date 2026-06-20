package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// TestRouteHealth_BackoffEarnedThreshold pins that the back-off is earned only
// after stabilityStreakForBackoff consecutive confirmations.
func TestRouteHealth_BackoffEarnedThreshold(t *testing.T) {
	s := &RouteHealthState{}
	for i := 0; i < stabilityStreakForBackoff-1; i++ {
		s.StabilityStreak = i
		if s.backoffEarned() {
			t.Fatalf("streak %d must NOT earn back-off (threshold %d)", i, stabilityStreakForBackoff)
		}
	}
	s.StabilityStreak = stabilityStreakForBackoff
	if !s.backoffEarned() {
		t.Fatalf("streak %d must earn back-off", stabilityStreakForBackoff)
	}
}

// TestRouteHealth_ProbeBackoffDelaysQuestionable verifies a proven-stable route
// stays Good past 60s (delayed to 90s) when backoff is on, while Bad/Dead are
// UNCHANGED — a stable route that dies is still detected at the base timeline.
func TestRouteHealth_ProbeBackoffDelaysQuestionable(t *testing.T) {
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity: domaintest.ID("id-target"), Uplink: domaintest.ID("id-uplink"),
		Health: HealthGood, LastHopAck: base, TransitionAt: base,
	}
	for i := 0; i < maxStabilityStreak; i++ {
		state.applyHopAck(base) // earn the back-off (also pins LastHopAck=base)
	}
	if !state.backoffEarned() {
		t.Fatalf("after %d confirmations back-off must be earned", maxStabilityStreak)
	}

	// At 60s: a base route would be Questionable, but the stable route stays
	// Good until the extended 90s threshold.
	state.applyIdleTick(base.Add(60*time.Second), true)
	if state.Health != HealthGood {
		t.Fatalf("backoff: stable route must stay Good at 60s, got %s", state.Health)
	}
	// Just past the extended threshold (90s).
	want := HealthQuestionableAfter + stableQuestionableExtension
	state.applyIdleTick(base.Add(want+time.Second), true)
	if state.Health != HealthQuestionable {
		t.Fatalf("backoff: past %s a stable route must be Questionable, got %s", want, state.Health)
	}

	// Sanity: the extension stays below the (unchanged) Bad threshold so
	// ordering is preserved and death detection is not slowed.
	if want >= HealthBadAfter {
		t.Fatalf("extended Questionable %s must stay below HealthBadAfter %s", want, HealthBadAfter)
	}
}

// TestRouteHealth_ProbeBackoffResetsOnFailure verifies that any negative
// evidence drops the route back to the fast base timeline immediately.
func TestRouteHealth_ProbeBackoffResetsOnFailure(t *testing.T) {
	base := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity: domaintest.ID("id-target"), Uplink: domaintest.ID("id-uplink"),
		Health: HealthGood, LastHopAck: base, TransitionAt: base,
	}
	for i := 0; i < maxStabilityStreak; i++ {
		state.applyHopAck(base)
	}
	state.applyProbeFailure(base)
	if state.StabilityStreak != 0 {
		t.Fatalf("streak must reset to 0 on probe failure, got %d", state.StabilityStreak)
	}
	if state.backoffEarned() {
		t.Fatal("back-off must be lost after the streak resets")
	}
}

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
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}

	// 59s idle — still Good.
	state.applyIdleTick(base.Add(59*time.Second), false)
	if state.Health != HealthGood {
		t.Fatalf("after 59s idle: Health = %s, want good", state.Health)
	}

	// 60s idle — transitions to Questionable.
	now := base.Add(60 * time.Second)
	state.applyIdleTick(now, false)
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
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthQuestionable,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(122*time.Second), false)
	if state.Health != HealthBad {
		t.Fatalf("after 122s idle: Health = %s, want bad", state.Health)
	}
}

// TestRouteHealth_BadToDeadAt182s verifies the terminal passive
// transition for a CONFIRMED pair. Dead pairs are excluded from
// selection by CompositeScore and locally invalidated by the caller.
// Confirmed=true is required: passive idle may only assert Dead for a
// pair that once had real reachability evidence and then went silent
// (see applyIdleTick's Confirmed gate).
func TestRouteHealth_BadToDeadAt182s(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthBad,
		Confirmed:    true,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(182*time.Second), false)
	if state.Health != HealthDead {
		t.Fatalf("after 182s idle: Health = %s, want dead", state.Health)
	}
}

// TestRouteHealth_IdleTickSkipsBadWhenLatentToDead verifies that a
// single late tick at >=182s correctly promotes a CONFIRMED Questionable
// pair straight to Dead, in case the ticker missed an intermediate run
// (e.g., process-pause). The reverse-severity check inside applyIdleTick
// matches the Dead branch first.
func TestRouteHealth_IdleTickSkipsBadWhenLatentToDead(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthQuestionable,
		Confirmed:    true,
		LastHopAck:   base,
		TransitionAt: base,
	}
	state.applyIdleTick(base.Add(300*time.Second), false)
	if state.Health != HealthDead {
		t.Fatalf("after 300s idle: Health = %s, want dead", state.Health)
	}
}

// TestRouteHealth_UnconfirmedIdleCapsAtBad — a pair that was NEVER
// confirmed (Confirmed=false: no hop_ack and no reachable probe_ack ever
// observed) must NOT be passively aged to Dead. 182s of idle on such a
// pair is not evidence the route died — only evidence we never had a
// local confirmation channel for it (the probe loop cannot cover every
// transit pair at mesh scale). Capping at Bad keeps it penalised in
// Lookup and still emitted on the wire, but invisible to
// healthProjectionSig — so it produces no JournalCauseHealthAging flip
// and no false withdrawal, breaking the Dead<->Questionable re-announce
// flap that dominated announce/alloc churn. Genuinely-gone routes are
// reclaimed by TTL expiry, not by passive Dead.
func TestRouteHealth_UnconfirmedIdleCapsAtBad(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthQuestionable,
		Confirmed:    false,
		LastHopAck:   base,
		TransitionAt: base,
	}
	// Past the Dead threshold: an unconfirmed pair caps at Bad.
	state.applyIdleTick(base.Add(182*time.Second), false)
	if state.Health != HealthBad {
		t.Fatalf("unconfirmed after 182s idle: Health = %s, want bad (Dead reserved for confirmed pairs)", state.Health)
	}
	// A later/latent tick must keep it at Bad, never escalating to Dead
	// while it remains unconfirmed.
	state.applyIdleTick(base.Add(600*time.Second), false)
	if state.Health != HealthBad {
		t.Fatalf("unconfirmed after 600s idle: Health = %s, want bad (still no confirmation)", state.Health)
	}
}

// TestRouteHealth_UnconfirmedThenConfirmedAgesToDead — once a pair earns
// its first real reachability evidence (Confirmed flips sticky-true), the
// passive Dead timeline applies normally: a confirmed pair that later
// goes silent IS evidence of a route that died, and must reach Dead.
func TestRouteHealth_UnconfirmedThenConfirmedAgesToDead(t *testing.T) {
	base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Identity:     domaintest.ID("id-target"),
		Uplink:       domaintest.ID("id-uplink"),
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}
	// First positive evidence confirms the pair and refreshes the timer.
	state.applyProbeAck(true, base.Add(10*time.Second))
	if !state.Confirmed {
		t.Fatal("applyProbeAck(reachable=true) must set Confirmed=true")
	}
	// 182s of idle since that confirmation ages the now-confirmed pair to Dead.
	state.applyIdleTick(base.Add(10*time.Second).Add(182*time.Second), false)
	if state.Health != HealthDead {
		t.Fatalf("confirmed pair after 182s idle: Health = %s, want dead", state.Health)
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
	state.applyIdleTick(transitionTime, false)
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

	state := store.ensureLocked(domaintest.ID("id-a"), domaintest.ID("id-uplink-1"), now)
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
	if state.Identity != domaintest.ID("id-a") {
		t.Fatalf("Identity = %q, want id-a", state.Identity)
	}
	if state.Uplink != domaintest.ID("id-uplink-1") {
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

	first := store.ensureLocked(domaintest.ID("id-a"), domaintest.ID("id-uplink-1"), now)
	first.RTT = 42 * time.Millisecond

	second := store.ensureLocked(domaintest.ID("id-a"), domaintest.ID("id-uplink-1"), now.Add(time.Minute))
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

	stateA := store.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink-A"), base)
	stateB := store.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink-B"), base)

	// Force B into Questionable via a passive tick after A's recent
	// hop_ack.
	stateB.applyIdleTick(base.Add(70*time.Second), false)
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
	if got := store.getLocked(domaintest.ID("id-a"), domaintest.ID("id-uplink-1")); got != nil {
		t.Fatalf("getLocked on cold pair = %v, want nil", got)
	}
}

// TestHealthStore_EvictUplinkLocked verifies tight-sync eviction of a
// single uplink — used when one peer withdraws its claim to Identity
// while other uplinks remain. Other pairs untouched.
func TestHealthStore_EvictUplinkLocked(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store.ensureLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1"), now)
	store.ensureLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-2"), now)
	store.ensureLocked(domaintest.ID("id-y"), domaintest.ID("id-uplink-1"), now)

	store.evictUplinkLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1"))

	if store.getLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1")) != nil {
		t.Fatal("evicted pair (id-x, id-uplink-1) still tracked")
	}
	if store.getLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-2")) == nil {
		t.Fatal("untouched pair (id-x, id-uplink-2) lost")
	}
	if store.getLocked(domaintest.ID("id-y"), domaintest.ID("id-uplink-1")) == nil {
		t.Fatal("untouched pair (id-y, id-uplink-1) lost — eviction must not cascade across identities")
	}
}

// TestHealthStore_EvictIdentityLocked verifies full-identity eviction —
// invoked when all uplinks for an identity withdrew and routeStore
// dropped the identity bucket entirely.
func TestHealthStore_EvictIdentityLocked(t *testing.T) {
	store := newHealthStore()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store.ensureLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1"), now)
	store.ensureLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-2"), now)
	store.ensureLocked(domaintest.ID("id-y"), domaintest.ID("id-uplink-1"), now)

	store.evictIdentityLocked(domaintest.ID("id-x"))

	if got := store.lenLocked(); got != 1 {
		t.Fatalf("lenLocked after evictIdentity = %d, want 1", got)
	}
	if store.getLocked(domaintest.ID("id-y"), domaintest.ID("id-uplink-1")) == nil {
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
	state := store.ensureLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1"), now)
	state.RTT = 50 * time.Millisecond

	snap := store.snapshotLocked()
	if len(snap) != 1 {
		t.Fatalf("snapshotLocked() len = %d, want 1", len(snap))
	}

	// Mutate the snapshot copy.
	snap[0].RTT = 999 * time.Millisecond

	// Live state must remain unchanged.
	live := store.getLocked(domaintest.ID("id-x"), domaintest.ID("id-uplink-1"))
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

// ---------------------------------------------------------------------
// Phase 3 PR 12.1 — reputation primitives (hop-ack reliability + black-hole
// cooldown). All tests below exercise apply* methods on RouteHealthState
// directly; no production call site yet feeds them — that wire-up lands
// in PR 12.2 (Table.MarkHopAck / Table.MarkHopFailure plumbing).
// Architectural anchor: docs/cluster-mesh/phase-3-multipath-reputation.md §4.1.
// ---------------------------------------------------------------------

// TestRouteReputation_ColdStartUntilWarmupSamples — until the pair has
// accumulated ReliabilityWarmupSamples positive/negative observations,
// CompositeScore must NOT fold the reliability term: the EMA is too
// noisy on a sample size of 1-2 to outweigh the strict-tier health
// invariant. The §4.1 contract is "cold-start ignored".
//
// The check is structural: CompositeScore with a fresh pair (Attempts=2)
// should equal CompositeScore with HopAckAttempts=0 — same shape, no
// reliability bonus added.
func TestRouteReputation_ColdStartUntilWarmupSamples(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	cold := &RouteHealthState{
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}
	warming := &RouteHealthState{
		Health:           HealthGood,
		LastHopAck:       base,
		TransitionAt:     base,
		HopAckAttempts:   ReliabilityWarmupSamples - 1,
		HopAckSuccesses:  ReliabilityWarmupSamples - 1,
		ReliabilityScore: 1.0,
	}

	coldScore := CompositeScore(1, RouteSourceAnnouncement, cold, false)
	warmingScore := CompositeScore(1, RouteSourceAnnouncement, warming, false)
	if !almostEqual(coldScore, warmingScore) {
		t.Fatalf("reliability bonus leaked during warmup: cold=%f, warming=%f", coldScore, warmingScore)
	}

	// One more observation crosses the threshold — bonus kicks in.
	warmed := *warming
	warmed.HopAckAttempts = ReliabilityWarmupSamples
	warmed.HopAckSuccesses = ReliabilityWarmupSamples
	warmedScore := CompositeScore(1, RouteSourceAnnouncement, &warmed, false)
	if warmedScore <= warmingScore {
		t.Fatalf("crossing warmup threshold did not boost score: warming=%f, warmed=%f", warmingScore, warmedScore)
	}
}

// TestRouteReputation_SuccessIncrementsAttemptsAndScore — every
// applyHopAckSuccess bumps both attempts and successes, recomputes the
// ratio, and resets ConsecutiveFailures. ReliabilityScore is the EMA
// of binary outcomes (1.0 for success) — the first call seeds at 1.0,
// subsequent successes keep it at 1.0.
func TestRouteReputation_SuccessIncrementsAttemptsAndScore(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:              HealthGood,
		LastHopAck:          base,
		TransitionAt:        base,
		ConsecutiveFailures: 4,
	}
	state.applyHopAckSuccess(base.Add(1 * time.Second))
	if state.HopAckAttempts != 1 || state.HopAckSuccesses != 1 {
		t.Fatalf("after 1 success: Attempts=%d, Successes=%d, want 1,1", state.HopAckAttempts, state.HopAckSuccesses)
	}
	if !almostEqual(state.ReliabilityScore, 1.0) {
		t.Fatalf("after 1 success: ReliabilityScore=%f, want 1.0 (cold-start seed)", state.ReliabilityScore)
	}
	if state.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 (success must reset)", state.ConsecutiveFailures)
	}

	state.applyHopAckSuccess(base.Add(2 * time.Second))
	state.applyHopAckSuccess(base.Add(3 * time.Second))
	if state.HopAckAttempts != 3 || state.HopAckSuccesses != 3 {
		t.Fatalf("after 3 successes: Attempts=%d, Successes=%d, want 3,3", state.HopAckAttempts, state.HopAckSuccesses)
	}
	if !almostEqual(state.ReliabilityScore, 1.0) {
		t.Fatalf("after 3 successes: ReliabilityScore=%f, want 1.0", state.ReliabilityScore)
	}
}

// TestRouteReputation_FailureIncrementsConsecutiveAndDecaysScore —
// applyHopAckFailure bumps attempts only (NOT successes), increments
// the consecutive counter, and blends the EMA toward 0 at the
// canonical alpha. Documents the §4.1 invariant "failure = outcome 0.0".
func TestRouteReputation_FailureIncrementsConsecutiveAndDecaysScore(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:           HealthGood,
		LastHopAck:       base,
		TransitionAt:     base,
		ReliabilityScore: 1.0, // pretend pair was steady-state reliable
		HopAckAttempts:   10,
		HopAckSuccesses:  10,
	}
	state.applyHopAckFailure(base.Add(1 * time.Second))
	if state.HopAckAttempts != 11 {
		t.Fatalf("HopAckAttempts = %d, want 11 (failure bumps attempts)", state.HopAckAttempts)
	}
	if state.HopAckSuccesses != 10 {
		t.Fatalf("HopAckSuccesses = %d, want 10 (failure must NOT bump successes)", state.HopAckSuccesses)
	}
	if state.ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d, want 1", state.ConsecutiveFailures)
	}
	// EMA blend at alpha=0.2: 1.0*0.8 + 0.0*0.2 = 0.8.
	wantScore := 1.0*(1-ReliabilityEWMAFactor) + 0.0*ReliabilityEWMAFactor
	if !almostEqual(state.ReliabilityScore, wantScore) {
		t.Fatalf("ReliabilityScore = %f, want %f (alpha=%.2f blend)", state.ReliabilityScore, wantScore, ReliabilityEWMAFactor)
	}
}

// TestRouteReputation_BlackHoleArmedAfter5ConsecutiveFailures — when
// ConsecutiveFailures crosses BlackHoleThreshold the apply method must
// arm CooldownUntil = now + BlackHoleCooldown. Below the threshold the
// cooldown stays zero. Overview §9.5 lists this as a release-blocking
// test name.
func TestRouteReputation_BlackHoleArmedAfter5ConsecutiveFailures(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:       HealthGood,
		LastHopAck:   base,
		TransitionAt: base,
	}
	// First 4 failures — no cooldown armed.
	for i := 1; i <= BlackHoleThreshold-1; i++ {
		state.applyHopAckFailure(base.Add(time.Duration(i) * time.Second))
		if !state.CooldownUntil.IsZero() {
			t.Fatalf("CooldownUntil armed prematurely after %d failures: %v", i, state.CooldownUntil)
		}
	}
	// 5-th failure — arms cooldown.
	armAt := base.Add(time.Duration(BlackHoleThreshold) * time.Second)
	state.applyHopAckFailure(armAt)
	wantUntil := armAt.Add(BlackHoleCooldown)
	if !state.CooldownUntil.Equal(wantUntil) {
		t.Fatalf("CooldownUntil = %v, want %v (now + BlackHoleCooldown)", state.CooldownUntil, wantUntil)
	}
	if state.ConsecutiveFailures != BlackHoleThreshold {
		t.Fatalf("ConsecutiveFailures = %d, want %d", state.ConsecutiveFailures, BlackHoleThreshold)
	}
}

// TestRouteReputation_CooldownExpiryRestoresSelectability —
// applyCooldownExpiryLocked is the helper TickHealth calls every probe
// cadence: an armed pair whose CooldownUntil has elapsed gets cleared
// AND its ConsecutiveFailures reset to 0 (second chance). A pair whose
// CooldownUntil is still in the future is left untouched.
func TestRouteReputation_CooldownExpiryRestoresSelectability(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	armAt := base
	cooledUntil := armAt.Add(BlackHoleCooldown)

	// Still inside the cooldown window — no clear.
	stillInside := &RouteHealthState{
		Health:              HealthGood,
		LastHopAck:          armAt,
		TransitionAt:        armAt,
		ConsecutiveFailures: BlackHoleThreshold,
		CooldownUntil:       cooledUntil,
	}
	stillInside.applyCooldownExpiryLocked(cooledUntil.Add(-time.Second))
	if stillInside.CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil cleared before window elapsed")
	}
	if stillInside.ConsecutiveFailures != BlackHoleThreshold {
		t.Fatalf("ConsecutiveFailures = %d during cooldown, want %d", stillInside.ConsecutiveFailures, BlackHoleThreshold)
	}

	// Past expiry — cleared and counter reset.
	elapsed := &RouteHealthState{
		Health:              HealthGood,
		LastHopAck:          armAt,
		TransitionAt:        armAt,
		ConsecutiveFailures: BlackHoleThreshold,
		CooldownUntil:       cooledUntil,
	}
	elapsed.applyCooldownExpiryLocked(cooledUntil.Add(time.Second))
	if !elapsed.CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil = %v after expiry, want zero", elapsed.CooldownUntil)
	}
	if elapsed.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d after expiry, want 0", elapsed.ConsecutiveFailures)
	}

	// Defensive: no-op on never-armed pairs (CooldownUntil zero
	// already).
	never := &RouteHealthState{Health: HealthGood, LastHopAck: armAt, TransitionAt: armAt}
	never.applyCooldownExpiryLocked(armAt.Add(time.Hour))
	if !never.CooldownUntil.IsZero() {
		t.Fatalf("applyCooldownExpiryLocked invented CooldownUntil on a fresh pair: %v", never.CooldownUntil)
	}
}

// TestRouteReputation_SuccessDuringCooldownClearsCooldown — a late
// hop_ack (organic relay traffic landing during the cooldown window)
// is positive evidence and overrides the black-hole signal: the pair
// is immediately selectable again. Documents §4.1 contract that
// applyHopAckSuccess clears CooldownUntil.
func TestRouteReputation_SuccessDuringCooldownClearsCooldown(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:              HealthGood,
		LastHopAck:          base,
		TransitionAt:        base,
		ConsecutiveFailures: BlackHoleThreshold,
		CooldownUntil:       base.Add(BlackHoleCooldown),
	}
	state.applyHopAckSuccess(base.Add(30 * time.Second))
	if !state.CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil = %v after success during cooldown, want zero", state.CooldownUntil)
	}
	if state.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 after success", state.ConsecutiveFailures)
	}
}

// TestRouteReputation_ScoreClampedTo01 — ReliabilityScore must stay
// within [0, 1]. Run an adversarial sequence of pure-failure inputs
// to verify the EMA never goes negative, and pure-success inputs to
// verify it never exceeds 1.
func TestRouteReputation_ScoreClampedTo01(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	allFail := &RouteHealthState{Health: HealthGood, LastHopAck: base, TransitionAt: base, ReliabilityScore: 1.0}
	for i := 1; i <= 50; i++ {
		allFail.applyHopAckFailure(base.Add(time.Duration(i) * time.Second))
		if allFail.ReliabilityScore < 0 || allFail.ReliabilityScore > 1 {
			t.Fatalf("after %d failures: ReliabilityScore=%f, want [0,1]", i, allFail.ReliabilityScore)
		}
	}

	allOK := &RouteHealthState{Health: HealthGood, LastHopAck: base, TransitionAt: base}
	for i := 1; i <= 50; i++ {
		allOK.applyHopAckSuccess(base.Add(time.Duration(i) * time.Second))
		if allOK.ReliabilityScore < 0 || allOK.ReliabilityScore > 1 {
			t.Fatalf("after %d successes: ReliabilityScore=%f, want [0,1]", i, allOK.ReliabilityScore)
		}
	}
	if !almostEqual(allOK.ReliabilityScore, 1.0) {
		t.Fatalf("after 50 successes: ReliabilityScore=%f, want 1.0", allOK.ReliabilityScore)
	}
}

// TestRouteReputation_FreshFailureDoesNotArmCooldownBelowThreshold —
// guards the threshold semantics specifically: 4 failures must leave
// CooldownUntil zero. A small off-by-one in the apply method (>= vs >)
// would arm cooldown one failure too early.
func TestRouteReputation_FreshFailureDoesNotArmCooldownBelowThreshold(t *testing.T) {
	if BlackHoleThreshold <= 1 {
		t.Skipf("threshold %d makes below-threshold test meaningless", BlackHoleThreshold)
	}
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{Health: HealthGood, LastHopAck: base, TransitionAt: base}
	for i := 1; i < BlackHoleThreshold; i++ {
		state.applyHopAckFailure(base.Add(time.Duration(i) * time.Second))
	}
	if !state.CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil armed after %d failures: %v", BlackHoleThreshold-1, state.CooldownUntil)
	}
}

// TestRouteReputation_ApplyHopAckSuccessLeavesHealthMachineUntouched —
// reputation primitives are orthogonal to the Phase 2 state machine:
// applyHopAckSuccess MUST NOT mutate Health / LastHopAck /
// TransitionAt / ProbeFailures. The Phase 2 applyHopAck remains the
// only path that touches those fields. PR 12.2 wires both together at
// the Table level (MarkHopAck calls applyHopAck then
// applyHopAckSuccess inside one t.mu.Lock).
//
// Without this separation a single failed PR 12.2 wire-up could let
// reputation primitives drift the health state machine — guarding
// the contract here means the test breaks loud if someone "helpfully"
// merges the two.
func TestRouteReputation_ApplyHopAckSuccessLeavesHealthMachineUntouched(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:        HealthQuestionable,
		LastHopAck:    base,
		TransitionAt:  base,
		ProbeFailures: 2,
	}
	state.applyHopAckSuccess(base.Add(30 * time.Second))
	if state.Health != HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (reputation primitives must not touch health state machine)", state.Health)
	}
	if !state.LastHopAck.Equal(base) {
		t.Fatalf("LastHopAck moved: got %v, want %v", state.LastHopAck, base)
	}
	if !state.TransitionAt.Equal(base) {
		t.Fatalf("TransitionAt moved: got %v, want %v", state.TransitionAt, base)
	}
	if state.ProbeFailures != 2 {
		t.Fatalf("ProbeFailures = %d, want 2 (must be untouched)", state.ProbeFailures)
	}
}

// TestRouteReputation_ApplyHopAckFailureLeavesHealthMachineUntouched —
// symmetric guard for the failure path: reputation primitives must
// not bump Health to Bad / Dead, nor touch ProbeFailures (which is
// owned by the probe path).
func TestRouteReputation_ApplyHopAckFailureLeavesHealthMachineUntouched(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	state := &RouteHealthState{
		Health:        HealthGood,
		LastHopAck:    base,
		TransitionAt:  base,
		ProbeFailures: 1,
	}
	state.applyHopAckFailure(base.Add(30 * time.Second))
	if state.Health != HealthGood {
		t.Fatalf("Health = %s, want good (reputation primitives must not touch health state machine)", state.Health)
	}
	if state.ProbeFailures != 1 {
		t.Fatalf("ProbeFailures = %d, want 1 (probe path owns this counter)", state.ProbeFailures)
	}
}

// Compile-time sanity: the new fields are addressable as documented.
// If the field set drifts (rename / removal), this test stops
// compiling — useful as a quick canary in the otherwise behaviour-
// only suite above. Uses domaintest.ID to keep the import
// honest after PR 12.1; remove if the import becomes unused in
// future cleanups.
func TestRouteReputation_FieldShapeCompiles(t *testing.T) {
	_ = RouteHealthState{
		Identity:            domaintest.ID("id"),
		Uplink:              domaintest.ID("u"),
		HopAckAttempts:      0,
		HopAckSuccesses:     0,
		ReliabilityScore:    0,
		ConsecutiveFailures: 0,
		CooldownUntil:       time.Time{},
	}
}
