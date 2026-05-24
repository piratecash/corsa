package routing

import (
	"testing"
	"time"
)

// table_lookup_phase2_test.go covers the Phase 2 changes to
// Table.Lookup: CompositeScore-based ranking, HealthDead exclusion,
// nil-health fallback, and MarkHopAck side effects.
//
// Pre-Phase-2 tests live in table_lookup_test.go (legacy
// hops+source-priority ordering) and stay green because the
// CompositeScore in cold-start (no health data) collapses to
// base − hops×10 + sourceBonus, which preserves the relative ordering
// for any case the legacy tests assert.

// upsertClaim is a thin test helper that injects a raw uplink claim
// into the routing table via UpdateRoute, bypassing the wire-frame
// ingest path. Returns the resulting RouteEntry as Lookup would
// surface it.
func upsertClaim(t *testing.T, tbl *Table, identity, uplink PeerIdentity, hops int, source RouteSource) {
	t.Helper()
	entry := RouteEntry{
		Identity:  identity,
		Origin:    identity, // post-A1 wire metadata; storage drops it
		NextHop:   uplink,
		Hops:      hops,
		SeqNo:     1,
		Source:    source,
		ExpiresAt: time.Now().Add(time.Hour),
	}
	if _, err := tbl.UpdateRoute(entry); err != nil {
		t.Fatalf("UpdateRoute(%q via %q): %v", identity, uplink, err)
	}
}

// TestLookup_BackwardCompat_NilHealthFallsBackToByHops verifies that
// a freshly-built table with no health observations ranks routes the
// same way the pre-Phase-2 Lookup did: lower hops first, source-trust
// tie-break. This is the explicit backward-compatibility contract.
func TestLookup_BackwardCompat_NilHealthFallsBackToByHops(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-far", 3, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-near", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-mid", 2, RouteSourceAnnouncement)

	got := tbl.Lookup("id-target")
	if len(got) != 3 {
		t.Fatalf("Lookup returned %d entries, want 3", len(got))
	}
	wantOrder := []PeerIdentity{"id-uplink-near", "id-uplink-mid", "id-uplink-far"}
	for i, want := range wantOrder {
		if got[i].NextHop != want {
			t.Fatalf("Lookup[%d].NextHop = %q, want %q (full=%v)", i, got[i].NextHop, want, got)
		}
	}
}

// TestLookup_CompositeScoreRanking_LowRTTBeatsHighRTTSameHops — the
// key Phase 2 motivation rendered as a Lookup-path test: two
// same-hop uplinks with different RTT EWMA's must rank by RTT.
func TestLookup_CompositeScoreRanking_LowRTTBeatsHighRTTSameHops(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-slow", 2, RouteSourceAnnouncement)

	tbl.MarkHopAck("id-target", "id-uplink-fast", 10*time.Millisecond)
	tbl.MarkHopAck("id-target", "id-uplink-slow", 200*time.Millisecond)

	got := tbl.Lookup("id-target")
	if len(got) != 2 {
		t.Fatalf("Lookup returned %d entries, want 2", len(got))
	}
	if got[0].NextHop != "id-uplink-fast" {
		t.Fatalf("Lookup[0].NextHop = %q, want id-uplink-fast (low-RTT path must rank first)", got[0].NextHop)
	}
}

// TestLookup_CompositeScoreRanking_LocalRTTBeatsHigherHops — the
// stronger Phase 2 case: 3 hops with sub-20ms RTT can beat 2 hops
// with no RTT data, because the RTT bonus exceeds the per-hop
// penalty. This is what the iter-1.5 roadmap text called
// "RTT-weighted route selection".
func TestLookup_CompositeScoreRanking_LocalRTTBeatsHigherHops(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-local-3h", 3, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-distant-2h", 2, RouteSourceAnnouncement)

	// local-3h: RTT 5ms → +30 RTT bonus → base 70 + 30 = 100
	// distant-2h: no RTT data → base 80 + 0 = 80
	tbl.MarkHopAck("id-target", "id-uplink-local-3h", 5*time.Millisecond)

	got := tbl.Lookup("id-target")
	if got[0].NextHop != "id-uplink-local-3h" {
		t.Fatalf("Lookup[0].NextHop = %q, want id-uplink-local-3h (low-RTT 3-hop must beat 2-hop unconfirmed)", got[0].NextHop)
	}
}

// TestLookup_DeadExcludedFromResult — the strongest invariant of the
// Phase 2 health gate: HealthDead claims are filtered out entirely,
// even if they would otherwise have the lowest hop count.
func TestLookup_DeadExcludedFromResult(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-dead", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-good", 3, RouteSourceAnnouncement)

	// Drive the (id-target, id-uplink-dead) pair into HealthDead by
	// reaching deep into the health store under t.mu — production
	// callers use the wire path, but the test fast-forwards the
	// state machine.
	tbl.mu.Lock()
	state := tbl.health.ensureLocked("id-target", "id-uplink-dead", now)
	state.Health = HealthDead
	tbl.mu.Unlock()

	got := tbl.Lookup("id-target")
	for _, entry := range got {
		if entry.NextHop == "id-uplink-dead" {
			t.Fatalf("Lookup returned the Dead claim: %v", entry)
		}
	}
	if len(got) != 1 || got[0].NextHop != "id-uplink-good" {
		t.Fatalf("Lookup returned %v, want exactly [id-uplink-good]", got)
	}
}

// TestLookup_DirectClaimExemptFromDeadFilter — PR 11.25 P2.
// Direct (own-origin, hops=1) claims are EXEMPT from the
// HealthDead exclusion. Direct routes are session-bound: their
// storage lifecycle is driven by AddDirectPeer / RemoveDirectPeer,
// so a live storage claim implies a live session. A Dead label
// on a Direct claim signals "no organic hop_ack lately", not
// "session is gone". Excluding the claim from Lookup would leave
// the relay path falling back to gossip while AnnounceTo keeps
// advertising the same Direct route (PR 11.24 P2#1 exempted the
// announce side); local forwarding and outbound announce
// semantics must stay consistent.
func TestLookup_DirectClaimExemptFromDeadFilter(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// AddDirectPeer is the production path for own-origin direct
	// claims (Hops=1, NextHop==Identity, ExpiresAt=zero, Source=Direct).
	if _, err := tbl.AddDirectPeer("id-peer-direct"); err != nil {
		t.Fatalf("setup: AddDirectPeer failed: %v", err)
	}
	tbl.ForceHealthForTest("id-peer-direct", "id-peer-direct", HealthDead)

	got := tbl.Lookup("id-peer-direct")
	foundDirect := false
	for _, entry := range got {
		if entry.Source == RouteSourceDirect && entry.NextHop == "id-peer-direct" {
			foundDirect = true
			break
		}
	}
	if !foundDirect {
		t.Fatalf("Lookup excluded Dead Direct claim; want Direct exempt from Dead filter (PR 11.25 P2). Entries: %v", got)
	}
}

// TestLookup_BadDeprioritizedButNotExcluded — HealthBad still keeps
// the claim in the result (Lookup is "last resort before gossip
// fallback"), but it ranks behind any Good alternative.
func TestLookup_BadDeprioritizedButNotExcluded(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-bad", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-good", 2, RouteSourceAnnouncement)

	tbl.mu.Lock()
	bad := tbl.health.ensureLocked("id-target", "id-uplink-bad", now)
	bad.Health = HealthBad
	tbl.mu.Unlock()

	got := tbl.Lookup("id-target")
	if len(got) != 2 {
		t.Fatalf("Lookup returned %d entries, want 2 (Bad must still be selectable as last resort)", len(got))
	}
	if got[0].NextHop != "id-uplink-good" {
		t.Fatalf("Lookup[0].NextHop = %q, want id-uplink-good (Good must outrank Bad)", got[0].NextHop)
	}
	if got[1].NextHop != "id-uplink-bad" {
		t.Fatalf("Lookup[1].NextHop = %q, want id-uplink-bad", got[1].NextHop)
	}
}

// TestLookup_SelfRouteAlwaysFirst — the synthetic self-route
// invariant survives Phase 2 ranking. When localOrigin matches the
// queried identity, the synthetic Hops=0 RouteSourceLocal entry must
// land at index 0 regardless of any other claims for that identity.
func TestLookup_SelfRouteAlwaysFirst(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Inject a transit announcement claim to our own identity. Even
	// though no rational peer would advertise this, we want to
	// confirm that the self-route ranking invariant survives an
	// adversarial claim that has the same Identity as localOrigin.
	upsertClaim(t, tbl, "self", "id-other", 1, RouteSourceAnnouncement)

	got := tbl.Lookup("self")
	if len(got) == 0 {
		t.Fatal("Lookup for self returned nothing — synthetic self-route missing")
	}
	if got[0].Source != RouteSourceLocal {
		t.Fatalf("Lookup[0].Source = %s, want local (self-route must be first)", got[0].Source)
	}
	if got[0].Hops != 0 {
		t.Fatalf("Lookup[0].Hops = %d, want 0", got[0].Hops)
	}
}

// TestMarkHopAck_UpdatesHealthAndRTT — the writer-side contract:
// MarkHopAck transitions the (identity, uplink) state to Good and
// folds rtt>0 into the EWMA estimate.
func TestMarkHopAck_UpdatesHealthAndRTT(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Use an announcement claim — Direct requires NextHop == Identity
	// per UplinkClaim.Validate, but the health entry is keyed by the
	// (Identity, NextHop) pair regardless of source.
	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink", 30*time.Millisecond)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != HealthGood {
		t.Fatalf("Health = %s, want good", snap[0].Health)
	}
	if snap[0].RTT != 30*time.Millisecond {
		t.Fatalf("RTT = %v, want 30ms (cold-start sample)", snap[0].RTT)
	}
	if !snap[0].LastHopAck.Equal(now) {
		t.Fatalf("LastHopAck = %v, want %v", snap[0].LastHopAck, now)
	}
}

// TestMarkHopAck_ScopedToUplink_DoesNotAffectOtherUplink — the
// per-pair scoping invariant. A hop_ack for (X, uplink-A) must NOT
// touch the state of (X, uplink-B).
func TestMarkHopAck_ScopedToUplink_DoesNotAffectOtherUplink(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 2, RouteSourceAnnouncement)

	// Drive B into Questionable so we can detect any unintended touch.
	tbl.mu.Lock()
	stateB := tbl.health.ensureLocked("id-target", "id-uplink-B", now)
	stateB.Health = HealthQuestionable
	tbl.mu.Unlock()

	tbl.MarkHopAck("id-target", "id-uplink-A", 25*time.Millisecond)

	tbl.mu.RLock()
	gotA := tbl.health.getLocked("id-target", "id-uplink-A")
	gotB := tbl.health.getLocked("id-target", "id-uplink-B")
	tbl.mu.RUnlock()

	if gotA == nil || gotA.Health != HealthGood {
		t.Fatalf("stateA.Health = %v, want good", gotA)
	}
	if gotB == nil || gotB.Health != HealthQuestionable {
		t.Fatalf("stateB.Health = %v, want questionable (untouched by uplink-A hop_ack)", gotB)
	}
}

// TestMarkHopAck_ZeroRTTPreservesEWMA — when caller passes rtt=0
// ("no measurement available"), the prior RTT estimate stays intact.
// This is the relay-path contract: hop_ack receive does not currently
// have send-timestamp threaded through, so it passes rtt=0 and the
// health bump still applies without corrupting RTT.
func TestMarkHopAck_ZeroRTTPreservesEWMA(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink", 50*time.Millisecond)
	tbl.MarkHopAck("id-target", "id-uplink", 0) // simulate hop_ack without rtt sample

	tbl.mu.RLock()
	state := tbl.health.getLocked("id-target", "id-uplink")
	tbl.mu.RUnlock()
	if state.RTT != 50*time.Millisecond {
		t.Fatalf("RTT after rtt=0 mark: %v, want 50ms (zero must not overwrite EWMA)", state.RTT)
	}
}

// TestMarkHopAck_RecoversFromBadAndDead — the most important state-
// machine integration test: a Bad pair plus a hop_ack returns to
// Good and ProbeFailures is zeroed.
func TestMarkHopAck_RecoversFromBadAndDead(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)

	tbl.mu.Lock()
	state := tbl.health.ensureLocked("id-target", "id-uplink", now)
	state.Health = HealthBad
	state.ProbeFailures = 7
	tbl.mu.Unlock()

	tbl.MarkHopAck("id-target", "id-uplink", 0)

	tbl.mu.RLock()
	state = tbl.health.getLocked("id-target", "id-uplink")
	tbl.mu.RUnlock()
	if state.Health != HealthGood {
		t.Fatalf("Health = %s, want good", state.Health)
	}
	if state.ProbeFailures != 0 {
		t.Fatalf("ProbeFailures = %d, want 0", state.ProbeFailures)
	}
}

// TestMarkHopAck_EmptyIdentityIsNoop — defensive: empty identity or
// uplink strings are silently ignored. Production code should not
// construct empty values, but the helper exposes the writer mutex,
// so the guard avoids creating garbage entries.
func TestMarkHopAck_EmptyIdentityIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	tbl.MarkHopAck("", "id-uplink", 10*time.Millisecond)
	tbl.MarkHopAck("id-target", "", 10*time.Millisecond)

	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil (empty identity/uplink must not create state)", got)
	}
}

// TestUpdateRoute_FreshAnnouncement_LeavesConfirmedFalse — the
// PR 11.15 P3 / 11.16 regression. A newly admitted Announcement
// route produces a health entry seeded by ensureLocked: LastHopAck
// is stamped at `now` (the applyIdleTick timeline reference, so
// the pair ages on the documented Good→Questionable→Bad→Dead
// schedule from creation forward), Health is downgraded by
// UpdateRoute to Questionable (no confirmation evidence yet),
// and Confirmed is false. The fetchRouteHealth RPC gates
// emission of last_hop_ack on Confirmed==true, so this entry
// will correctly appear without last_hop_ack on the wire even
// though the internal field is non-zero.
func TestUpdateRoute_FreshAnnouncement_LeavesConfirmedFalse(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	status, err := tbl.UpdateRoute(RouteEntry{
		Identity:  "id-target",
		Origin:    "id-target",
		NextHop:   "id-uplink",
		Hops:      2,
		SeqNo:     1,
		Source:    RouteSourceAnnouncement,
		ExpiresAt: now.Add(DefaultTTL),
	})
	if err != nil || status != RouteAccepted {
		t.Fatalf("setup: UpdateRoute status=%v err=%v", status, err)
	}

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Health != HealthQuestionable {
		t.Fatalf("fresh announcement entry Health = %s, want questionable", snap[0].Health)
	}
	if snap[0].Confirmed {
		t.Fatal("fresh announcement entry Confirmed = true, want false (no positive evidence yet — PR 11.15 P3 / 11.16)")
	}
	// LastHopAck IS stamped (timer reference), but the RPC layer
	// uses Confirmed to decide emission. Asserting LastHopAck=now
	// here pins the internal contract; the RPC-side gating test
	// lives in routing_commands_test.go (PR 11.16).
	if !snap[0].LastHopAck.Equal(now) {
		t.Fatalf("LastHopAck = %v, want %v (applyIdleTick timer reference must be set so the passive timeline ages from creation)", snap[0].LastHopAck, now)
	}
}

// TestAddDirectPeerReset_PreservesLastHopAckOnConfirmedPair — PR
// 11.30 P3 regression. The AddDirectPeer reset path is allowed
// to refresh Health/TransitionAt/ProbeFailures on a degraded
// direct claim, but it MUST NOT overwrite LastHopAck — that
// timestamp is the operator-facing "last real positive evidence"
// signal, gated on the wire by Confirmed. Without this guard,
// a historically-confirmed peer that disconnects → reconnects
// would expose a synthetic fresh last_hop_ack in
// fetchRouteHealth even though no new ack happened on the new
// session, contradicting both the Confirmed flag's contract
// (RouteHealthState.Confirmed doc-comment in health.go) and
// the RPC gate (Confirmed && !LastHopAck.IsZero() in
// routing_commands.go).
func TestAddDirectPeerReset_PreservesLastHopAckOnConfirmedPair(t *testing.T) {
	t0 := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	clk := t0
	tbl := NewTable(WithLocalOrigin("self"), WithClock(func() time.Time { return clk }))

	// Bring the peer up and mark it confirmed via an organic
	// hop_ack at t0. This stamps Confirmed=true and LastHopAck=t0
	// on the (peer-X, peer-X) direct pair.
	if _, err := tbl.AddDirectPeer("peer-X"); err != nil {
		t.Fatalf("setup: AddDirectPeer initial: %v", err)
	}
	tbl.MarkHopAck("peer-X", "peer-X", 0)
	originalLastHopAck := t0

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 || !snap[0].Confirmed || !snap[0].LastHopAck.Equal(originalLastHopAck) {
		t.Fatalf("setup: expected Confirmed=true and LastHopAck=%v, got %+v", originalLastHopAck, snap)
	}

	// Force-degrade the pair, advance the clock, then re-add to
	// simulate a session bounce. The reset path runs because
	// Health != Good.
	tbl.ForceHealthForTest("peer-X", "peer-X", HealthBad)
	clk = t0.Add(45 * time.Second)
	if _, err := tbl.AddDirectPeer("peer-X"); err != nil {
		t.Fatalf("re-add: AddDirectPeer: %v", err)
	}

	snap = tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if !snap[0].Confirmed {
		t.Fatal("Confirmed flipped to false on reset; want sticky-true (Confirmed is monotonic-up by design)")
	}
	if !snap[0].LastHopAck.Equal(originalLastHopAck) {
		t.Fatalf("LastHopAck=%v after reset, want preserved %v (synthetic fresh stamp would lie about real evidence on the wire — PR 11.30 P3)", snap[0].LastHopAck, originalLastHopAck)
	}
	if !snap[0].TransitionAt.Equal(clk) {
		t.Fatalf("TransitionAt=%v, want %v (session bounce moment); reset must still mark the transition", snap[0].TransitionAt, clk)
	}
	if snap[0].Health != HealthGood {
		t.Fatalf("Health=%s after reset, want good", snap[0].Health)
	}
}

// TestApplyHopAck_FlipsConfirmed — pins the PR 11.16 contract
// that real positive evidence (hop_ack) flips Confirmed=true
// even on a fresh placeholder.
func TestApplyHopAck_FlipsConfirmed(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	// Fresh placeholder is Confirmed=false.
	if snap := tbl.HealthSnapshot(); len(snap) != 1 || snap[0].Confirmed {
		t.Fatalf("setup: HealthSnapshot Confirmed should start false: %+v", snap)
	}

	tbl.MarkHopAck("id-target", "id-uplink", 0)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if !snap[0].Confirmed {
		t.Fatal("Confirmed = false after MarkHopAck, want true (positive evidence must flip the flag)")
	}
}

// TestApplyProbeAck_ReachableFlipsConfirmed — symmetric to the
// hop_ack test: a probe ack with reachable=true is the other
// positive-evidence path and must also flip Confirmed.
func TestApplyProbeAck_ReachableFlipsConfirmed(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkProbeAck("id-target", "id-uplink", true, 25*time.Millisecond)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if !snap[0].Confirmed {
		t.Fatal("Confirmed = false after MarkProbeAck(reachable=true), want true")
	}
}

// TestApplyProbeAck_UnreachableDoesNotFlipConfirmed — negative
// probe evidence is NOT a confirmation; the flag stays false.
// Once Confirmed is true elsewhere, negative evidence MUST NOT
// reset it back to false (Confirmed is monotonic-up for the
// lifetime of the state entry — see RouteHealthState's
// Confirmed doc-comment).
func TestApplyProbeAck_UnreachableDoesNotFlipConfirmed(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)

	// Negative ack on a never-confirmed pair: Confirmed stays
	// false. (Production semantics: the responder answered with
	// "I can't reach the target", which is evidence about the
	// route but NOT about Uplink → us reachability.)
	tbl.MarkProbeAck("id-target", "id-uplink", false, 0)
	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("HealthSnapshot len = %d, want 1", len(snap))
	}
	if snap[0].Confirmed {
		t.Fatal("Confirmed = true after MarkProbeAck(reachable=false), want false")
	}

	// Flip Confirmed via positive evidence, then send another
	// negative ack: Confirmed must stay true (sticky).
	tbl.MarkHopAck("id-target", "id-uplink", 0)
	tbl.MarkProbeAck("id-target", "id-uplink", false, 0)
	snap = tbl.HealthSnapshot()
	if !snap[0].Confirmed {
		t.Fatal("Confirmed reverted to false after negative ack — flag must be monotonic-up (sticky)")
	}
}

// TestAnnounceTargetFor_AgreesWithLookupOnCompositeScore — PR
// 11.34 P2 regression. route_query response (handleRouteQuery
// via Table.AnnounceTargetFor) must advertise the SAME winner
// Table.Lookup would select — otherwise the response is useless
// for recovery: a peer that asked "what's your best route to X"
// gets a short Bad path while the responder's relay code would
// itself pick a longer Good path. Earlier the picker used
// isBetterLiveClaim (Hops + SeqNo + Extra + Uplink), which
// disagreed with CompositeScore when health / RTT / source-trust
// shifted the ranking.
//
// Setup: two claims for target. The "short Bad" claim has Hops=1
// but HealthBad. The "long Good" claim has Hops=3 and
// HealthGood. CompositeScore picks the long Good one
// (HealthBad's strict-tier penalty puts every Bad candidate
// below every Good candidate, regardless of the +20 Hops
// difference). isBetterLiveClaim would have picked the short
// Bad one. AnnounceTargetFor must return the long Good Uplink.
func TestAnnounceTargetFor_AgreesWithLookupOnCompositeScore(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-short", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-long", 3, RouteSourceAnnouncement)

	// Drive the short pair into HealthBad so CompositeScore
	// deprioritises it below the long Good pair.
	tbl.ForceHealthForTest("id-target", "id-uplink-short", HealthBad)

	// Sanity: Lookup's winner is the long Good pair.
	lookupResult := tbl.Lookup("id-target")
	if len(lookupResult) == 0 {
		t.Fatal("setup: Lookup returned no results")
	}
	if lookupResult[0].NextHop != "id-uplink-long" {
		t.Fatalf("setup: Lookup winner = %q, want id-uplink-long (CompositeScore should rank Good above Bad)", lookupResult[0].NextHop)
	}

	// AnnounceTargetFor must agree.
	entry, uplink, ok := tbl.AnnounceTargetFor("id-target", "id-requester")
	if !ok {
		t.Fatal("AnnounceTargetFor returned ok=false; want a live winner")
	}
	if uplink != "id-uplink-long" {
		t.Fatalf("AnnounceTargetFor winner uplink = %q, want id-uplink-long (must agree with Lookup's CompositeScore winner — PR 11.34 P2)", uplink)
	}
	if entry.Hops != 3 {
		t.Fatalf("AnnounceTargetFor entry.Hops = %d, want 3 (the long Good path)", entry.Hops)
	}
}

// TestAnnounceable_FiltersDeadPair — PR 11.23 P2 regression.
// A locally-Dead (Identity, Uplink) pair must not appear in
// Announceable output. Without this filter the announce loop
// would keep re-advertising routes that Lookup has already
// filtered out, and neighbours would keep refreshing a path we
// already gave up on.
func TestAnnounceable_FiltersDeadPair(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Two live claims for the same target via different uplinks.
	upsertClaim(t, tbl, "id-target", "id-uplink-good", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-dead", 2, RouteSourceAnnouncement)

	// Drive one pair into Dead.
	tbl.ForceHealthForTest("id-target", "id-uplink-dead", HealthDead)

	got := tbl.Announceable("peer-X")
	for _, entry := range got {
		if entry.NextHop == "id-uplink-dead" {
			t.Fatalf("Announceable returned the Dead claim: %+v (PR 11.23 P2: Dead pairs must be suppressed on the announce plane)", entry)
		}
	}
	// The non-Dead claim must still be present.
	foundGood := false
	for _, entry := range got {
		if entry.NextHop == "id-uplink-good" {
			foundGood = true
			break
		}
	}
	if !foundGood {
		t.Fatalf("Announceable suppressed the healthy claim too: %v", got)
	}
}

// TestAnnounceTo_FiltersDeadFromLiveWinner — PR 11.23 P2 for the
// wire-projection path. AnnounceProjectionFor picks one live
// winner per Identity; the Dead pair must not be eligible. If
// the Dead pair would have been the winner (better hops), the
// next-best non-Dead claim takes its place.
//
// AnnounceEntry strips NextHop on the wire (the receiver derives
// it locally as the sender's identity), so the assertion uses
// Hops as the discriminator: the Dead pair has Hops=1 and the
// non-Dead pair has Hops=3. With the Dead filter active, the
// emitted entry must carry Hops=3 — proof that the non-Dead
// claim won the live-winner pick.
func TestAnnounceTo_FiltersDeadFromLiveWinner(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-dead", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-alt", 3, RouteSourceAnnouncement)

	tbl.ForceHealthForTest("id-target", "id-uplink-dead", HealthDead)

	got := tbl.AnnounceTo("peer-X")
	var targetEntries int
	for _, entry := range got {
		if entry.Identity != "id-target" {
			continue
		}
		targetEntries++
		if entry.Hops != 3 {
			t.Fatalf("AnnounceTo emitted Hops=%d for id-target, want 3 (Dead pair has Hops=1 — Hops=1 here would mean the Dead pair leaked through): %+v", entry.Hops, entry)
		}
	}
	if targetEntries == 0 {
		t.Fatal("AnnounceTo emitted no entry for id-target; expected the non-Dead claim to win")
	}
}

// TestAnnounceTo_DirectClaimExemptFromDeadFilter — PR 11.24 P2#1
// regression. The HealthDead constant's doc-comment promises a
// wire withdrawal for own-origin Dead routes; transit routes
// silently drop. A Direct (own-origin) claim that ends up
// labelled HealthDead must therefore NOT be silently suppressed
// from the announce projection — the absence of an announce
// entry is not a withdrawal, and neighbours would keep the prior
// live route until TTL. The fix exempts Direct claims from the
// Dead filter: Direct lifecycle is session-driven (AddDirectPeer
// / RemoveDirectPeer emit real withdrawals on disconnect), so a
// Dead label on a Direct claim only means "no organic hop_ack
// traffic", not "route is gone".
func TestAnnounceTo_DirectClaimExemptFromDeadFilter(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// AddDirectPeer is the production path for own-origin direct
	// claims (Hops=1, NextHop==Identity, ExpiresAt=zero).
	if _, err := tbl.AddDirectPeer("id-peer-direct"); err != nil {
		t.Fatalf("setup: AddDirectPeer failed: %v", err)
	}
	// Force the Direct pair to Dead in the health store. Pure
	// pathological state — production direct peers shouldn't age
	// to Dead because session-disconnect drives RemoveDirectPeer.
	tbl.ForceHealthForTest("id-peer-direct", "id-peer-direct", HealthDead)

	got := tbl.AnnounceTo("peer-X")
	foundDirect := false
	for _, entry := range got {
		if entry.Identity == "id-peer-direct" && entry.Hops == 1 {
			foundDirect = true
			break
		}
	}
	if !foundDirect {
		t.Fatalf("AnnounceTo suppressed Dead Direct claim; want Direct claims exempt from the Dead filter (PR 11.24 P2#1). Entries: %v", got)
	}
}

// TestAnnounceTo_AllDeadEmitsNothing — when EVERY claim for an
// Identity is Dead, the live-winner branch finds no candidate
// and the projection falls through to the tombstone branch. For
// foreign-origin Dead claims (Source != Direct) the tombstone
// branch suppresses transit tombstones, so no wire entry is
// emitted at all. This matches the HealthDead contract: transit
// routes are silently dropped, only own-origin withdrawals
// hit the wire.
func TestAnnounceTo_AllDeadEmitsNothing(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-A", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-B", 3, RouteSourceAnnouncement)

	tbl.ForceHealthForTest("id-target", "id-uplink-A", HealthDead)
	tbl.ForceHealthForTest("id-target", "id-uplink-B", HealthDead)

	got := tbl.AnnounceTo("peer-X")
	for _, entry := range got {
		if entry.Identity == "id-target" {
			t.Fatalf("all-Dead target produced wire entry: %+v (transit-tombstone fall-through should not emit)", entry)
		}
	}
}

// TestHealthSnapshot_DeepCopy — the hot-read path contract: snapshot
// mutations do not leak into the live store.
func TestHealthSnapshot_DeepCopy(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 2, RouteSourceAnnouncement)
	tbl.MarkHopAck("id-target", "id-uplink", 40*time.Millisecond)

	snap := tbl.HealthSnapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	snap[0].RTT = 999 * time.Millisecond

	tbl.mu.RLock()
	live := tbl.health.getLocked("id-target", "id-uplink")
	tbl.mu.RUnlock()
	if live.RTT != 40*time.Millisecond {
		t.Fatalf("snapshot mutation leaked into live store: RTT = %v, want 40ms", live.RTT)
	}
}
