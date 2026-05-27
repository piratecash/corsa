package routing

import (
	"testing"
	"time"
)

// table_health_reputation_test.go covers Phase 3 PR 12.2 — the wire-up
// of the reputation primitives introduced in PR 12.1
// (applyHopAckSuccess / applyHopAckFailure / applyCooldownExpiryLocked
// on RouteHealthState) into the Table-level writer surface
// (MarkHopAck / ConfirmHopAck / new MarkHopFailure).
//
// Test ownership rationale: the apply-method semantics are exhaustively
// covered in health_test.go; the Table-level MarkProbe* contract
// (anti-orphan guards, per-pair scoping, kicking the dirty flag) is
// covered in table_health_probe_test.go. This file owns the equivalent
// contract for MarkHopAck reputation bumps and the new MarkHopFailure
// entry point — that is, "Phase 2 health side effects PLUS Phase 3
// reputation side effects all under one t.mu.Lock".
//
// Fixed-clock date convention: every test uses 2026-05-23 (matching
// the Phase 2 test suites). The choice is deliberate — the upsertClaim
// helper hardcodes ExpiresAt = time.Now().Add(time.Hour) (real clock,
// not table clock), so the fixed clock MUST be pinned in the past
// relative to real test execution. Otherwise MarkHopFailure's
// IsExpired guard fires (claim looks expired from the table's view)
// and masks the reputation path under the anti-stale guard.

// TestMarkHopAck_BumpsReputationAttemptsAndSuccesses verifies the
// PR 12.2 extension: a successful hop_ack now ALSO records a
// reputation outcome via applyHopAckSuccess inside the same
// t.mu.Lock critical section the Phase 2 health update runs in.
// Without folding both calls under one lock, a concurrent
// MarkHopFailure could interleave and corrupt the consecutive-failure
// counter.
func TestMarkHopAck_BumpsReputationAttemptsAndSuccesses(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink", 25*time.Millisecond)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].HopAckAttempts != 1 || snap[0].HopAckSuccesses != 1 {
		t.Fatalf("after MarkHopAck: Attempts=%d, Successes=%d, want 1,1", snap[0].HopAckAttempts, snap[0].HopAckSuccesses)
	}
	if !almostEqualReputationTest(snap[0].ReliabilityScore, 1.0) {
		t.Fatalf("ReliabilityScore = %f, want 1.0 (cold-start seed)", snap[0].ReliabilityScore)
	}
}

// TestMarkHopAck_BumpsReputationClearsCooldown — a late hop_ack on a
// pair whose cooldown has been armed by a prior streak of failures
// must immediately clear the arm: positive evidence overrides the
// black-hole signal. Documents the cross-method coupling between
// MarkHopFailure (PR 12.2 below) and MarkHopAck through the shared
// reputation surface.
func TestMarkHopAck_BumpsReputationClearsCooldown(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink")
	}
	// Sanity precondition: cooldown armed by the failure streak.
	snap := tbl.HealthSnapshot()
	if len(snap) != 1 || snap[0].CooldownUntil.IsZero() {
		t.Fatalf("precondition: expected cooldown armed, got snap=%v", snap)
	}

	tbl.MarkHopAck("id-target", "id-uplink", 0)

	snap = tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if !snap[0].CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil = %v after late hop_ack, want zero", snap[0].CooldownUntil)
	}
	if snap[0].ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 after success", snap[0].ConsecutiveFailures)
	}
}

// TestMarkHopFailure_BumpsAttemptsAndConsecutiveFailures — the basic
// writer contract: an organic relay hop_ack timeout reaches
// Table.MarkHopFailure (the wire-up lives in routing_relay.go's
// onRelayHopAckTimeout callback), the call lands the reputation
// primitive without touching Phase 2 health / probe state.
func TestMarkHopFailure_BumpsAttemptsAndConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	// Seed reputation through a prior success so HopAckSuccesses is
	// non-zero and the next-failure delta is observable.
	tbl.MarkHopAck("id-target", "id-uplink", 25*time.Millisecond)

	tbl.MarkHopFailure("id-target", "id-uplink")

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].HopAckAttempts != 2 {
		t.Fatalf("HopAckAttempts = %d, want 2 (success + failure)", snap[0].HopAckAttempts)
	}
	if snap[0].HopAckSuccesses != 1 {
		t.Fatalf("HopAckSuccesses = %d, want 1 (failure must not bump successes)", snap[0].HopAckSuccesses)
	}
	if snap[0].ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d, want 1", snap[0].ConsecutiveFailures)
	}
	// Health state machine MUST stay untouched — MarkHopFailure is a
	// reputation-side signal, not a probe-side or hop-ack-side
	// observation. PR 12.4 reads CooldownUntil from Lookup; the
	// health label is independent.
	if snap[0].Health != HealthGood {
		t.Fatalf("Health = %s, want good (MarkHopFailure must not move state machine)", snap[0].Health)
	}
}

// TestMarkHopFailure_ArmsCooldownOn5thConsecutiveFailure — release-
// blocking integration with PR 12.4 (Lookup filter): the cooldown
// arm fires through Table.MarkHopFailure exactly once at the
// BlackHoleThreshold-th call. Overview §9.5 names this scenario.
func TestMarkHopFailure_ArmsCooldownOn5thConsecutiveFailure(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold-1; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink")
	}
	if snap := tbl.HealthSnapshot(); len(snap) == 1 && !snap[0].CooldownUntil.IsZero() {
		t.Fatalf("CooldownUntil armed before threshold: %v", snap[0].CooldownUntil)
	}
	tbl.MarkHopFailure("id-target", "id-uplink")
	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	want := now.Add(BlackHoleCooldown)
	if !snap[0].CooldownUntil.Equal(want) {
		t.Fatalf("CooldownUntil = %v, want %v (now + BlackHoleCooldown)", snap[0].CooldownUntil, want)
	}
}

// TestMarkHopFailure_OnAbsentPairIsNoop — storage-side guard: a
// timeout fired against a (Identity, Uplink) pair the routing
// table has never observed (no UpdateRoute admission, no
// AddDirectPeer) must not create any state. InspectTriple returns
// nil and MarkHopFailure short-circuits before touching health.
func TestMarkHopFailure_OnAbsentPairIsNoop(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))
	tbl.MarkHopFailure("id-never-seen", "id-never-uplink")
	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil after MarkHopFailure on absent pair", got)
	}
}

// TestMarkHopFailure_OnAbsentHealthEntryIsNoop — anti-orphan guard
// symmetric to Phase 2 MarkProbeAck(reachable=false) /
// MarkProbeFailure: when storage carries the claim but health has
// been evicted (reconcileHealthLocked after TickTTL / cap pressure),
// a late hop-ack timeout MUST NOT resurrect the pair as a fresh
// Good-labelled entry with ConsecutiveFailures=1. The Phase 3 plan
// §4.2 spells this out as "если pair не tracked — no-op (нет previous
// evidence — не создаём synthetic Bad-сигнал)".
//
// In production Phase 2's UpdateRoute admission seeds Questionable
// health on every newly-learned (Identity, Uplink) pair, so this
// guard fires for the eviction-then-late-timeout race specifically.
func TestMarkHopFailure_OnAbsentHealthEntryIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	// upsertClaim seeded a Questionable health entry via UpdateRoute.
	// Simulate reconcile/eviction so storage stays but health goes
	// away — the exact race the guard protects against.
	tbl.mu.Lock()
	tbl.health.evictUplinkLocked("id-target", "id-uplink")
	tbl.mu.Unlock()
	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("precondition: expected health snapshot empty after eviction, got %v", got)
	}

	tbl.MarkHopFailure("id-target", "id-uplink")

	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil (failure must not resurrect evicted pair)", got)
	}
}

// TestMarkHopFailure_OnWithdrawnClaimIsNoop — anti-orphan extension
// of the absent-pair guard: a withdrawn claim still appears in
// storage (as a tombstone) but is no longer selectable; firing
// MarkHopFailure against it must not create or touch a health entry
// because withdraw is the authoritative "this route is gone" signal
// that reputation should follow, not contradict.
func TestMarkHopFailure_OnWithdrawnClaimIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	// Establish a real health entry so this test exercises the
	// "withdrawn ⇒ no-op" branch, not the "absent ⇒ no-op" branch.
	tbl.MarkHopAck("id-target", "id-uplink", 0)
	// upsertClaim seeds SeqNo=1; withdraw needs strictly newer SeqNo.
	if !tbl.WithdrawRoute("id-target", "id-target", "id-uplink", 2) {
		t.Fatal("WithdrawRoute returned false")
	}

	before := tbl.HealthSnapshot()
	tbl.MarkHopFailure("id-target", "id-uplink")
	after := tbl.HealthSnapshot()

	if len(after) != len(before) {
		t.Fatalf("HealthSnapshot len changed after MarkHopFailure on withdrawn claim: before=%d after=%d", len(before), len(after))
	}
	if len(after) > 0 && after[0].ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d on withdrawn claim, want 0 (no-op)", after[0].ConsecutiveFailures)
	}
}

// TestMarkHopFailure_OnExpiredClaimIsNoop — same contract as the
// withdrawn-claim guard, but exercising the IsExpired branch of the
// storage check. A claim whose ExpiresAt is in the past is not
// selectable by Lookup; reputation must not paint it red retroactively.
func TestMarkHopFailure_OnExpiredClaimIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Inject an entry whose ExpiresAt is in the past via UpdateRoute.
	entry := RouteEntry{
		Identity:  "id-target",
		Origin:    "id-target",
		NextHop:   "id-uplink",
		Hops:      2,
		SeqNo:     1,
		Source:    RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Hour),
	}
	if _, err := tbl.UpdateRoute(entry); err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	tbl.MarkHopFailure("id-target", "id-uplink")

	snap := tbl.HealthSnapshot()
	for _, s := range snap {
		if s.Identity == "id-target" && s.Uplink == "id-uplink" && s.ConsecutiveFailures != 0 {
			t.Fatalf("ConsecutiveFailures = %d on expired claim, want 0 (no-op)", s.ConsecutiveFailures)
		}
	}
}

// TestMarkHopFailure_EmptyIdentityIsNoop — defensive guard mirrors
// MarkProbeFailure / MarkHopAck: empty identity or uplink does not
// create or touch any health entry.
func TestMarkHopFailure_EmptyIdentityIsNoop(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))
	tbl.MarkHopFailure("", "id-uplink")
	tbl.MarkHopFailure("id-target", "")
	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil after MarkHopFailure with empty id/uplink", got)
	}
}

// TestMarkHopFailure_ScopedToUplink_DoesNotAffectOtherUplink —
// per-pair scoping invariant on the failure-writer side, symmetric to
// the MarkHopAck / MarkProbeFailure equivalents.
func TestMarkHopFailure_ScopedToUplink_DoesNotAffectOtherUplink(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink-A", 0)
	tbl.MarkHopAck("id-target", "id-uplink-B", 0)

	tbl.MarkHopFailure("id-target", "id-uplink-A")

	snap := tbl.HealthSnapshot()
	var gotA, gotB *RouteHealthState
	for i := range snap {
		s := &snap[i]
		switch s.Uplink {
		case "id-uplink-A":
			gotA = s
		case "id-uplink-B":
			gotB = s
		}
	}
	if gotA == nil || gotA.ConsecutiveFailures != 1 {
		t.Fatalf("uplink-A: ConsecutiveFailures = %v, want 1", gotA)
	}
	if gotB == nil || gotB.ConsecutiveFailures != 0 {
		t.Fatalf("uplink-B: ConsecutiveFailures = %v, want 0 (untouched by uplink-A failure)", gotB)
	}
}

// TestMarkHopFailure_MarksDirty — TickHealth uses the dirty flag to
// skip rebuilds when nothing changed. MarkHopFailure mutates the
// published snapshot (HopAckAttempts / ReliabilityScore /
// ConsecutiveFailures / CooldownUntil are surfaced through
// fetchRouteReputation in PR 12.7), so the writer must set dirty —
// otherwise the snapshot publisher would leave RPC reads reporting
// stale counters until some unrelated mutation arrived. Mirrors the
// PR 11.28 P2#1 dirty contract enforced by every Phase 2 health
// writer.
func TestMarkHopFailure_MarksDirty(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink", 0)
	// Drain dirty: ConsumeDirty CAS true→false.
	tbl.ConsumeDirty()

	tbl.MarkHopFailure("id-target", "id-uplink")
	if !tbl.ConsumeDirty() {
		t.Fatalf("MarkHopFailure did not mark table dirty")
	}
}

// TestMarkHopAck_BumpsReputation_LocalPenaltiesOnly — release-
// blocking trust-budget invariant from the Phase 3 plan §2.3:
// reputation must update ONLY through local hop_ack writers
// (MarkHopAck / ConfirmHopAck / MarkHopFailure). Ingesting a
// foreign claim through the normal UpdateRoute path MUST NOT touch
// HopAckAttempts / HopAckSuccesses / ReliabilityScore /
// ConsecutiveFailures / CooldownUntil.
//
// The zero-trust budget (overview §4.2) forbids third-party
// reputation gossip; if a future change accidentally couples
// reputation primitives to wire-frame ingest the test breaks loud.
func TestMarkHopAck_BumpsReputation_LocalPenaltiesOnly(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)

	// Re-ingest the same claim multiple times — simulates wire-frame
	// announce / routes_update traffic. Each call goes through
	// UpdateRoute (the wire ingest entry point), NOT through
	// MarkHopAck. Reputation MUST stay untouched.
	for i := 0; i < 10; i++ {
		entry := RouteEntry{
			Identity:  "id-target",
			Origin:    "id-target",
			NextHop:   "id-uplink",
			Hops:      2,
			SeqNo:     uint64(i + 2), // strictly newer each time
			Source:    RouteSourceAnnouncement,
			ExpiresAt: now.Add(time.Hour),
		}
		if _, err := tbl.UpdateRoute(entry); err != nil {
			t.Fatalf("UpdateRoute iteration %d: %v", i, err)
		}
	}

	snap := tbl.HealthSnapshot()
	for _, s := range snap {
		if s.Identity == "id-target" && s.Uplink == "id-uplink" {
			if s.HopAckAttempts != 0 || s.HopAckSuccesses != 0 {
				t.Fatalf("UpdateRoute leaked into reputation: Attempts=%d, Successes=%d, want both 0", s.HopAckAttempts, s.HopAckSuccesses)
			}
			if s.ReliabilityScore != 0 {
				t.Fatalf("UpdateRoute leaked into ReliabilityScore: got %f, want 0", s.ReliabilityScore)
			}
			if s.ConsecutiveFailures != 0 {
				t.Fatalf("UpdateRoute leaked into ConsecutiveFailures: got %d, want 0", s.ConsecutiveFailures)
			}
			if !s.CooldownUntil.IsZero() {
				t.Fatalf("UpdateRoute leaked into CooldownUntil: got %v, want zero", s.CooldownUntil)
			}
		}
	}
}

// almostEqualReputationTest avoids depending on score_test.go's
// floatEpsilon constant; this file does not import math and the
// epsilon is local-only.
func almostEqualReputationTest(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}
