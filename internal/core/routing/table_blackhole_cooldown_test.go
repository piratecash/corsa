package routing

import (
	"testing"
	"time"
)

// table_blackhole_cooldown_test.go covers Phase 3 PR 12.4 — the
// black-hole cooldown filter that hides (Identity, Uplink) pairs
// from Lookup and Announce projections while CooldownUntil is in
// the future, plus the TickHealth integration that clears expired
// cooldowns on the same cadence as passive idle aging.
//
// PR 12.1 wrote the cooldown ARM (applyHopAckFailure on the 5th
// consecutive failure); PR 12.2 wired the failure into the
// production hop-ack timeout path. PR 12.4 closes the loop by
// making Lookup / AnnounceTargetFor / AnnounceableFor /
// AnnounceProjectionFor respect the arm and TickHealth clear it.
//
// Fixed-clock date convention follows the rest of the routing
// suite (2026-05-23, pinned in the past relative to real test
// execution — see table_health_reputation_test.go for the
// rationale tied to upsertClaim's time.Now()-anchored ExpiresAt).

// TestBlackHole_DetectionAfter5ConsecutiveFailures — overview
// §9.5 release-blocking scenario. Five MarkHopFailure calls in a
// row arm the cooldown; Lookup must immediately drop the pair
// from the ranked result. Mirrors the corresponding
// reputation-side test in table_health_reputation_test.go but
// pivots to the Lookup-filter contract.
func TestBlackHole_DetectionAfter5ConsecutiveFailures(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)

	// Bring A through the failure streak — the 5th call arms cooldown.
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink-A")
	}

	got := tbl.Lookup("id-target")
	if len(got) != 1 {
		t.Fatalf("Lookup returned %d entries, want 1 (A must be filtered)", len(got))
	}
	if got[0].NextHop != "id-uplink-B" {
		t.Fatalf("Lookup[0].NextHop = %q, want id-uplink-B (A is in cooldown)", got[0].NextHop)
	}
}

// TestBlackHole_TickHealthClearsExpiredCooldowns — the
// applyCooldownExpiryLocked helper introduced in PR 12.1 must be
// driven by TickHealth so an armed cooldown auto-releases without
// requiring an organic success to clear it. With the fixed clock
// advancing past now+BlackHoleCooldown the next tick lifts the
// arm and the pair re-enters Lookup.
func TestBlackHole_TickHealthClearsExpiredCooldowns(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clockMu := &mutableClock{now: now}
	tbl := NewTable(WithLocalOrigin("self"), WithClock(clockMu.nowFunc))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink")
	}
	if got := tbl.Lookup("id-target"); len(got) != 0 {
		t.Fatalf("Lookup returned %d entries while cooled down, want 0", len(got))
	}

	// Advance past the cooldown window and tick — applyCooldown-
	// ExpiryLocked clears CooldownUntil and the pair becomes
	// selectable again.
	clockMu.advance(BlackHoleCooldown + time.Second)
	tbl.TickHealth()

	got := tbl.Lookup("id-target")
	if len(got) != 1 {
		t.Fatalf("post-expiry Lookup returned %d entries, want 1", len(got))
	}
	if got[0].NextHop != "id-uplink" {
		t.Fatalf("post-expiry Lookup[0].NextHop = %q, want id-uplink", got[0].NextHop)
	}
}

// TestBlackHole_DirectClaimNotExempt — release-blocking invariant
// from Phase 3 §4.4. The Phase 2 Dead filter has a Direct
// exemption (session-driven lifecycle), but the cooldown filter
// does NOT — a Direct claim whose (Identity, Uplink) pair
// reliably misses hop_acks IS a black hole regardless of the
// session being live. Suppressing it from Lookup matches the
// "this peer can't actually deliver" signal reputation just
// recorded. The test must fail loud if the filter ever inherits
// the Direct exemption.
func TestBlackHole_DirectClaimNotExempt(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// AddDirectPeer creates a Direct claim with (Identity == Uplink).
	if _, err := tbl.AddDirectPeer("id-direct"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	// Drive the cooldown arm via MarkHopFailure. AddDirectPeer
	// seeded HealthGood, so the reputation primitive has an entry
	// to mutate.
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-direct", "id-direct")
	}

	got := tbl.Lookup("id-direct")
	if len(got) != 0 {
		t.Fatalf("Direct cooldown not respected: Lookup returned %d entries, want 0 (no Direct exemption for black-hole)", len(got))
	}
}

// TestBlackHole_AnnounceProjectionForRespectsCooldown — the
// announce projection (AnnounceTo) must also suppress
// cooled-down claims so neighbours stop refreshing a path we
// know is a black hole. Mirror of the Lookup-side filter; the
// suppression is symmetric so local forwarding and outbound
// announce semantics agree (Phase 2 PR 11.23 P2 documented the
// same invariant for HealthDead).
func TestBlackHole_AnnounceProjectionForRespectsCooldown(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink-A")
	}

	// AnnounceTo to some unrelated peer — the per-Identity live
	// winner picker should choose B (A is cooled down).
	entries := tbl.AnnounceTo("id-some-other-peer")
	for _, e := range entries {
		if e.Identity == "id-target" {
			// The winner for id-target should reflect B's path
			// (3 hops) since A is suppressed. The AnnounceEntry
			// carries the SENDER's hops (= claim.Hops here),
			// so B's 3 should show up.
			if e.Hops != 3 {
				t.Fatalf("AnnounceTo for id-target: Hops=%d, want 3 (A suppressed by cooldown)", e.Hops)
			}
			return
		}
	}
	t.Fatal("id-target missing from AnnounceTo result while B should still be advertised")
}

// TestBlackHole_AnnounceableForRespectsCooldown — Announceable
// (read-only projection used by RPC observability) likewise
// drops cooled-down claims. Without the filter operator tooling
// would surface a route the relay path is intentionally avoiding.
func TestBlackHole_AnnounceableForRespectsCooldown(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink-A")
	}

	got := tbl.Announceable("id-some-peer")
	var sawA, sawB bool
	for _, r := range got {
		if r.Identity == "id-target" {
			switch r.NextHop {
			case "id-uplink-A":
				sawA = true
			case "id-uplink-B":
				sawB = true
			}
		}
	}
	if sawA {
		t.Fatal("cooled-down A appeared in Announceable; cooldown filter not applied")
	}
	if !sawB {
		t.Fatal("B missing from Announceable; non-cooled alternative must remain")
	}
}

// TestBlackHole_AnnounceTargetForRespectsCooldown — the per-
// (target, requester) picker used by handleRouteQuery (Phase 2
// route_query_response_v1 building) must not return a cooled-
// down uplink in response to a peer's targeted query. The query
// asks "do you know a route to X?" and answering with a
// known-bad path would defeat the recovery mechanism the query
// exists to provide.
func TestBlackHole_AnnounceTargetForRespectsCooldown(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink-A")
	}

	entry, uplink, found := tbl.AnnounceTargetFor("id-target", "id-requester")
	if !found {
		t.Fatal("AnnounceTargetFor returned found=false, want B as live winner")
	}
	if uplink != "id-uplink-B" {
		t.Fatalf("AnnounceTargetFor uplink = %q, want id-uplink-B (A is cooled down)", uplink)
	}
	if entry.Identity != "id-target" {
		t.Fatalf("AnnounceTargetFor entry.Identity = %q, want id-target", entry.Identity)
	}
}

// TestBlackHole_AllUplinksCooledDown_LookupReturnsNil — when
// every alternative for an identity is in cooldown the Lookup
// result is empty. Callers (relay path, route query trigger)
// then fall back to gossip / route_query — exactly the recovery
// the black-hole signal is designed to surface.
func TestBlackHole_AllUplinksCooledDown_LookupReturnsNil(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink-A")
		tbl.MarkHopFailure("id-target", "id-uplink-B")
	}

	if got := tbl.Lookup("id-target"); len(got) != 0 {
		t.Fatalf("Lookup returned %d entries when all uplinks cooled, want 0", len(got))
	}
}

// TestBlackHole_SuccessClearsCooldownImmediately_LookupRestores —
// integration with PR 12.2 success path. A late hop_ack arriving
// during the cooldown window clears the arm via
// applyHopAckSuccess (PR 12.1) and the pair becomes immediately
// selectable in Lookup without waiting for the 2-minute window.
func TestBlackHole_SuccessClearsCooldownImmediately_LookupRestores(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-target", "id-uplink")
	}
	if got := tbl.Lookup("id-target"); len(got) != 0 {
		t.Fatalf("precondition: Lookup must be empty during cooldown, got %d entries", len(got))
	}

	// Late hop_ack lands inside the cooldown window.
	tbl.MarkHopAck("id-target", "id-uplink", 0)

	got := tbl.Lookup("id-target")
	if len(got) != 1 {
		t.Fatalf("Lookup post-success returned %d entries, want 1", len(got))
	}
	if got[0].NextHop != "id-uplink" {
		t.Fatalf("Lookup[0].NextHop = %q, want id-uplink", got[0].NextHop)
	}
}

// TestBlackHole_TickHealthClearsExpiredCooldownForDirectPair —
// even though TickHealth skips Direct pairs for IDLE aging (PR
// 11.27 P2#2 — session-driven lifecycle owns the Direct Health
// label), it MUST still clear the cooldown arm for them. Direct
// pairs in cooldown should release on the same 2-minute timer
// as transit pairs; the skip in PR 11.27 was specifically about
// the Good→Questionable→Bad→Dead idle promotion, not about the
// reputation primitive's cooldown lifecycle.
func TestBlackHole_TickHealthClearsExpiredCooldownForDirectPair(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clockMu := &mutableClock{now: now}
	tbl := NewTable(WithLocalOrigin("self"), WithClock(clockMu.nowFunc))

	if _, err := tbl.AddDirectPeer("id-direct"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-direct", "id-direct")
	}
	if got := tbl.Lookup("id-direct"); len(got) != 0 {
		t.Fatalf("precondition: Direct pair cooldown not applied; Lookup returned %d", len(got))
	}

	clockMu.advance(BlackHoleCooldown + time.Second)
	tbl.TickHealth()

	got := tbl.Lookup("id-direct")
	if len(got) != 1 {
		t.Fatalf("Direct pair cooldown did not clear via TickHealth: Lookup returned %d entries, want 1", len(got))
	}
}

// TestBlackHole_AddDirectPeerClearsCooldownOnReconnect — session
// re-establishment is positive liveness evidence on par with an
// organic late hop-ack, so AddDirectPeer (the reconnect path that
// re-admits / reactivates the direct claim) must lift an armed
// cooldown for the (peer, peer) pair. Without the clear, a peer
// whose downtime accumulated 5+ hop-ack failures stays hidden
// from Lookup / fetchRouteLookup for up to BlackHoleCooldown
// AFTER the new handshake completes — the "peer online, route
// count=0" field bug this clear exists to fix.
func TestBlackHole_AddDirectPeerClearsCooldownOnReconnect(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	if _, err := tbl.AddDirectPeer("id-direct"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-direct", "id-direct")
	}
	if got := tbl.Lookup("id-direct"); len(got) != 0 {
		t.Fatalf("precondition: Direct pair cooldown not applied; Lookup returned %d", len(got))
	}

	// Reconnect: storage-idempotent re-add, but the cooldown must lift.
	if _, err := tbl.AddDirectPeer("id-direct"); err != nil {
		t.Fatalf("AddDirectPeer (reconnect): %v", err)
	}

	got := tbl.Lookup("id-direct")
	if len(got) != 1 {
		t.Fatalf("post-reconnect Lookup returned %d entries, want 1 (cooldown must be cleared by AddDirectPeer)", len(got))
	}
	var found bool
	for _, st := range tbl.HealthSnapshot() {
		if st.Identity != "id-direct" || st.Uplink != "id-direct" {
			continue
		}
		found = true
		if !st.CooldownUntil.IsZero() {
			t.Fatalf("CooldownUntil = %v after reconnect, want zero", st.CooldownUntil)
		}
		if st.ConsecutiveFailures != 0 {
			t.Fatalf("ConsecutiveFailures = %d after reconnect, want 0", st.ConsecutiveFailures)
		}
		// Shaping grace must engage exactly as on the organic-recovery
		// and expiry-sweep clears.
		if st.LastCooldownClearedAt.IsZero() {
			t.Fatal("LastCooldownClearedAt not stamped on session-establish clear")
		}
		// Honest history: the EMA and attempt counters are NOT reset.
		if st.HopAckAttempts != uint64(BlackHoleThreshold) {
			t.Fatalf("HopAckAttempts = %d, want %d (reputation history must survive the clear)", st.HopAckAttempts, BlackHoleThreshold)
		}
	}
	if !found {
		t.Fatal("health entry for (id-direct, id-direct) missing from snapshot")
	}
}

// TestBlackHole_ClearDirectPairCooldown — the standalone clear used
// by the withdrawal-grace reconnect branch (tryCancelPendingWithdrawal
// == true), where AddDirectPeer is intentionally skipped because the
// direct route never left the table. Same semantics as the inline
// AddDirectPeer clear: armed cooldown lifts, untracked / already-clean
// pairs and empty identities are no-ops returning false.
func TestBlackHole_ClearDirectPairCooldown(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	if _, err := tbl.AddDirectPeer("id-direct"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure("id-direct", "id-direct")
	}
	if got := tbl.Lookup("id-direct"); len(got) != 0 {
		t.Fatalf("precondition: Direct pair cooldown not applied; Lookup returned %d", len(got))
	}

	if !tbl.ClearDirectPairCooldown("id-direct") {
		t.Fatal("ClearDirectPairCooldown returned false on an armed cooldown, want true")
	}
	got := tbl.Lookup("id-direct")
	if len(got) != 1 {
		t.Fatalf("post-clear Lookup returned %d entries, want 1", len(got))
	}

	// Idempotency / guard branches.
	if tbl.ClearDirectPairCooldown("id-direct") {
		t.Fatal("second ClearDirectPairCooldown returned true, want false (nothing left to clear)")
	}
	if tbl.ClearDirectPairCooldown("") {
		t.Fatal("ClearDirectPairCooldown(\"\") returned true, want false")
	}
	if tbl.ClearDirectPairCooldown("id-untracked") {
		t.Fatal("ClearDirectPairCooldown on untracked pair returned true, want false")
	}
}

// mutableClock helper is shared with table_dirty_test.go — see the
// declaration there. The method `nowFunc` matches WithClock's
// expected signature.
