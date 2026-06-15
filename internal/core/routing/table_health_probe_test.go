package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// table_health_probe_test.go covers the Phase 2 writer methods
// Table.MarkProbeAck and Table.MarkProbeFailure introduced in
// PR 11.3a. The state-machine semantics themselves live on
// RouteHealthState (health.go) and are exhaustively tested in
// health_test.go; here we focus on the Table-level entry points
// and their interaction with the EWMA RTT estimate.

// TestMarkProbeAck_ReachableRestoresGoodAndUpdatesRTT — the happy
// path: probe_ack with reachable=true behaves like a hop_ack and
// folds the sender-measured rtt sample into the EWMA estimate.
func TestMarkProbeAck_ReachableRestoresGoodAndUpdatesRTT(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)

	// Drive the pair into Bad so the ack must actively restore it.
	tbl.mu.Lock()
	state := tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"), now)
	state.Health = HealthBad
	state.ProbeFailures = 4
	tbl.mu.Unlock()

	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), true, 25*time.Millisecond)

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()

	if got.Health != HealthGood {
		t.Fatalf("Health = %s, want good", got.Health)
	}
	if got.ProbeFailures != 0 {
		t.Fatalf("ProbeFailures = %d, want 0", got.ProbeFailures)
	}
	if got.RTT != 25*time.Millisecond {
		t.Fatalf("RTT = %v, want 25ms (cold-start sample)", got.RTT)
	}
	if !got.LastProbe.Equal(now) {
		t.Fatalf("LastProbe = %v, want %v", got.LastProbe, now)
	}
}

// TestMarkProbeAck_UnreachableIncrementsFailures — reachable=false
// path: the responder is alive (it answered) but has no route to the
// target. Pair stays in Questionable until ProbeFailures crosses the
// threshold, then Bad.
func TestMarkProbeAck_UnreachableIncrementsFailures(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)
	tbl.mu.Lock()
	tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"), now).Health = HealthQuestionable
	tbl.mu.Unlock()

	// First two failures keep the pair Questionable.
	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), false, 0)
	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), false, 0)

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthQuestionable {
		t.Fatalf("after 2 unreachable acks: Health = %s, want questionable", got.Health)
	}
	if got.ProbeFailures != 2 {
		t.Fatalf("ProbeFailures = %d, want 2", got.ProbeFailures)
	}

	// Third failure crosses HealthProbeFailureThreshold=3 → Bad.
	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), false, 0)
	tbl.mu.RLock()
	got = tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthBad {
		t.Fatalf("after 3 unreachable acks: Health = %s, want bad", got.Health)
	}
}

// TestMarkProbeAck_ZeroRTTPreservesEWMA — same contract as
// MarkHopAck: an ack arriving with no useful RTT measurement (rtt=0)
// keeps the prior EWMA value. This typically happens when the
// responder set rtt_ms=0 but the sender's own measured round-trip is
// implausible (e.g., clock skew handled outside) and the caller opts
// out of the RTT fold. In normal probe flow the sender computes
// rtt=now()-probeSent and rtt is always non-zero.
func TestMarkProbeAck_ZeroRTTPreservesEWMA(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)
	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), true, 40*time.Millisecond)
	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), true, 0)

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.RTT != 40*time.Millisecond {
		t.Fatalf("RTT after rtt=0 ack: %v, want 40ms (zero must not overwrite EWMA)", got.RTT)
	}
}

// TestMarkProbeAck_ScopedToUplink_DoesNotAffectOtherUplink — per-pair
// scoping invariant on the writer side: a probe_ack arriving for
// (X, uplink-A) must NOT change health for (X, uplink-B).
func TestMarkProbeAck_ScopedToUplink_DoesNotAffectOtherUplink(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink-A"), 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink-B"), 2, RouteSourceAnnouncement)

	tbl.mu.Lock()
	stateB := tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink-B"), now)
	stateB.Health = HealthQuestionable
	tbl.mu.Unlock()

	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink-A"), true, 20*time.Millisecond)

	tbl.mu.RLock()
	gotA := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink-A"))
	gotB := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink-B"))
	tbl.mu.RUnlock()

	if gotA.Health != HealthGood {
		t.Fatalf("uplink-A: Health = %s, want good", gotA.Health)
	}
	if gotB.Health != HealthQuestionable {
		t.Fatalf("uplink-B: Health = %s, want questionable (untouched by uplink-A ack)", gotB.Health)
	}
}

// TestMarkProbeAck_EmptyIdentityIsNoop — defensive guard mirrors
// MarkHopAck: empty identity or uplink does not create stray health
// entries.
func TestMarkProbeAck_EmptyIdentityIsNoop(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	tbl.MarkProbeAck(domain.PeerIdentity{}, domaintest.ID("id-uplink"), true, 10*time.Millisecond)
	tbl.MarkProbeAck(domaintest.ID("id-target"), domain.PeerIdentity{}, true, 10*time.Millisecond)

	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil after MarkProbeAck with empty id/uplink", got)
	}
}

// TestMarkProbeFailure_IncrementsFailuresAndCrossesThreshold — three
// timeouts in a row push a Questionable pair into Bad without any
// hop_ack or reachable=false ack ever arriving.
func TestMarkProbeFailure_IncrementsFailuresAndCrossesThreshold(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)
	tbl.mu.Lock()
	tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"), now).Health = HealthQuestionable
	tbl.mu.Unlock()

	tbl.MarkProbeFailure(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.MarkProbeFailure(domaintest.ID("id-target"), domaintest.ID("id-uplink"))

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthQuestionable {
		t.Fatalf("after 2 failures: Health = %s, want questionable", got.Health)
	}
	if got.ProbeFailures != 2 {
		t.Fatalf("ProbeFailures = %d, want 2", got.ProbeFailures)
	}

	tbl.MarkProbeFailure(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RLock()
	got = tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthBad {
		t.Fatalf("after 3 failures: Health = %s, want bad", got.Health)
	}
}

// TestMarkProbeFailure_DoesNotResurrectDead — a Dead pair stays Dead
// under probe failures. Only a successful confirmation (hop_ack or
// reachable=true probe_ack) can transition out of Dead, per the
// state machine.
func TestMarkProbeFailure_DoesNotResurrectDead(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)
	tbl.mu.Lock()
	tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"), now).Health = HealthDead
	tbl.mu.Unlock()

	for i := 0; i < 5; i++ {
		tbl.MarkProbeFailure(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	}

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthDead {
		t.Fatalf("after 5 failures on Dead pair: Health = %s, want dead", got.Health)
	}
}

// TestMarkProbeFailure_EmptyIdentityIsNoop — defensive guard.
func TestMarkProbeFailure_EmptyIdentityIsNoop(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	tbl.MarkProbeFailure(domain.PeerIdentity{}, domaintest.ID("id-uplink"))
	tbl.MarkProbeFailure(domaintest.ID("id-target"), domain.PeerIdentity{})
	if got := tbl.HealthSnapshot(); got != nil {
		t.Fatalf("HealthSnapshot() = %v, want nil after MarkProbeFailure with empty id/uplink", got)
	}
}

// TestMarkProbeAck_RewakesFromBadOnUnreachable — the asymmetric
// rewake path: a Bad pair entered via passive timeline (no probe
// failures yet) receives reachable=false → first lifts to Questionable
// because the responder demonstrably acked, then re-evaluates.
func TestMarkProbeAck_RewakesFromBadOnUnreachable(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, domaintest.ID("id-target"), domaintest.ID("id-uplink"), 2, RouteSourceAnnouncement)
	tbl.mu.Lock()
	state := tbl.health.ensureLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"), now)
	state.Health = HealthBad
	state.ProbeFailures = 0 // entered Bad via passive timeline
	tbl.mu.Unlock()

	tbl.MarkProbeAck(domaintest.ID("id-target"), domaintest.ID("id-uplink"), false, 0)

	tbl.mu.RLock()
	got := tbl.health.getLocked(domaintest.ID("id-target"), domaintest.ID("id-uplink"))
	tbl.mu.RUnlock()
	if got.Health != HealthQuestionable {
		t.Fatalf("Health = %s, want questionable (Bad rewakes on any ack)", got.Health)
	}
	if got.ProbeFailures != 1 {
		t.Fatalf("ProbeFailures = %d, want 1", got.ProbeFailures)
	}
}
