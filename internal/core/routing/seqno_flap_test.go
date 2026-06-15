package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// Phase 1 P2 — SeqNo flap cap regression suite.
//
// These tests exercise the per-Identity outbound-SeqNo velocity
// tracker on FlapDetector and the resulting hold-down behaviour on
// AnnounceProjectionFor (wire emit suppression) plus ApplyUpdate
// (outbound-projected-SeqNo ingest guard). The shape mirrors the
// connection-flap regression suite in table_flap_backoff_test.go —
// fixed-clock fixtures so the assertions are deterministic, no real
// wall-clock sleeps.
//
// See docs/routing.md "SeqNo flap cap" for the permanent
// invariants this suite pins and flap.go::FlapDetector for the
// detector-side contract.

// TestSeqNoFlap_HoldDownEngagesAfterThreshold drives the velocity
// tracker through 1 over-threshold cycles and verifies:
//   - AnnounceProjectionFor stops emitting for the over-active
//     Identity once the threshold is crossed;
//   - RouteCapStats.SeqNoFlapHoldowns bumps exactly once per engage;
//   - peers unrelated to the over-active Identity keep receiving
//     emits for their own destinations.
//
// Fixture: window=1min, threshold=3, hold-down=2min. The synthetic
// upstream uses a distinct uplink per cycle so every fresh emit
// hits a cache miss and counts as an outbound-SeqNo advance.
func TestSeqNoFlap_HoldDownEngagesAfterThreshold(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		quiet   = domaintest.ID("quiet")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(3),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Seed a stable claim for `quiet` so the second arm of the
	// assertion (other Identities keep flowing) has something to
	// match against.
	mustUpdate(t, tbl, RouteEntry{
		Identity: quiet, Origin: domaintest.ID("src-quiet"), NextHop: domaintest.ID("uplink-q"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// Drive 4 distinct-content advances for `victim`. Each ingest
	// uses a different uplink with a strictly larger SeqNo so the
	// per-Identity AnnounceProjectionFor winner shifts each cycle
	// (Hops are equal → SeqNo tiebreak picks the latest uplink),
	// forcing a fresh outbound SeqNo allocation (cache miss). With
	// threshold=3 and 4 advances, the 4th tips the velocity ring
	// over the threshold and engages hold-down.
	uplinks := []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3"), domaintest.ID("u4")}
	for i, up := range uplinks {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		// Each AnnounceTo() triggers a fresh outbound SeqNo
		// allocation for victim (different sig per cycle, the
		// winner uplink changes), recording one advance against
		// the velocity ring.
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}

	stats := tbl.CapStats()
	if stats.SeqNoFlapHoldowns != 1 {
		t.Fatalf("SeqNoFlapHoldowns=%d after 4 advances over threshold=3, want 1", stats.SeqNoFlapHoldowns)
	}

	// Wire emit must now suppress victim. quiet's emit must still
	// land — hold-down is per-Identity, not global.
	emits := tbl.AnnounceTo(viewer)
	for _, e := range emits {
		if e.Identity == victim {
			t.Fatalf("victim emit must be suppressed during hold-down, got entry=%+v", e)
		}
	}
	foundQuiet := false
	for _, e := range emits {
		if e.Identity == quiet && e.Hops < HopsInfinity {
			foundQuiet = true
			break
		}
	}
	if !foundQuiet {
		t.Fatalf("hold-down on victim must not affect quiet's live emit, got entries=%+v", emits)
	}
}

// TestSeqNoFlap_AnnounceTargetFor_RespectsHoldDown — PR 11.33 P2
// regression. AnnounceTargetFor is the per-identity projection
// helper used by handleRouteQuery to build route_query_response.
// It MUST honour the SeqNo flap-cap hold-down the same way
// AnnounceProjectionFor does — otherwise a peer could receive a
// route_query answer for a target that the announce path is
// explicitly NOT advertising, leaking the over-active Identity
// past the cap.
//
// Setup mirrors TestSeqNoFlap_HoldDownEngagesAfterThreshold:
// drive 4 advances against threshold=3 to engage hold-down on
// `victim`, then verify (1) AnnounceTargetFor(victim, ...)
// returns ok=false during hold-down, (2) AnnounceTargetFor for
// an unrelated `quiet` Identity still returns ok=true (per-
// Identity scope, not global).
func TestSeqNoFlap_AnnounceTargetFor_RespectsHoldDown(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		quiet   = domaintest.ID("quiet")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(3),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Seed a stable quiet Identity so the per-Identity scope
	// assertion has something to match.
	mustUpdate(t, tbl, RouteEntry{
		Identity: quiet, Origin: domaintest.ID("src-quiet"), NextHop: domaintest.ID("uplink-q"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// Drive 4 distinct-content advances for `victim` via
	// AnnounceTo cycles; the 4th tips velocity over threshold=3
	// and engages hold-down.
	uplinks := []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3"), domaintest.ID("u4")}
	for i, up := range uplinks {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("setup: SeqNoFlapHoldowns=%d, want 1 (hold-down should have engaged)", tbl.CapStats().SeqNoFlapHoldowns)
	}

	// AnnounceTargetFor on victim must return ok=false (held).
	if _, _, ok := tbl.AnnounceTargetFor(victim, viewer); ok {
		t.Fatal("AnnounceTargetFor returned ok=true for held-down Identity; route_query would leak past the flap cap")
	}

	// Quiet Identity unaffected — hold-down is per-Identity.
	if _, _, ok := tbl.AnnounceTargetFor(quiet, viewer); !ok {
		t.Fatal("AnnounceTargetFor returned ok=false for quiet Identity; hold-down is per-Identity, must not affect siblings")
	}
}

// TestSeqNoFlap_AnnounceTargetFor_SuppressesArmingEmit — second
// arm of the PR 11.33 P2 contract. When AnnounceTargetFor itself
// engages hold-down inside the call (the over-threshold advance
// is THIS call's emit), the response must still be suppressed —
// not just the next call. Mirrors AnnounceProjectionFor's
// `if armed { ... continue }` branch.
func TestSeqNoFlap_AnnounceTargetFor_SuppressesArmingEmit(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(3),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Drive 3 advances via AnnounceTo (within threshold). The
	// 4th advance will tip over — but instead of issuing it via
	// AnnounceTo, we issue it via AnnounceTargetFor. The arm
	// happens INSIDE that call; the result must be suppressed.
	uplinks := []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")}
	for i, up := range uplinks {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	// Pre-condition: hold-down has NOT yet engaged.
	if tbl.CapStats().SeqNoFlapHoldowns != 0 {
		t.Fatalf("setup: SeqNoFlapHoldowns=%d, want 0 (only 3 advances so far)", tbl.CapStats().SeqNoFlapHoldowns)
	}

	// Land the 4th distinct-content claim, then call
	// AnnounceTargetFor — this is the call that pushes velocity
	// over the threshold.
	mustUpdate(t, tbl, RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u4"),
		Hops: 2, SeqNo: 14, Source: RouteSourceAnnouncement,
	})
	_, _, ok := tbl.AnnounceTargetFor(victim, viewer)
	if ok {
		t.Fatal("AnnounceTargetFor returned ok=true on the arming call; the engage-this-call emit must be suppressed (mirrors AnnounceProjectionFor's armed branch)")
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("SeqNoFlapHoldowns=%d after arming call, want 1", tbl.CapStats().SeqNoFlapHoldowns)
	}
}

// TestSeqNoFlap_HoldDownReleasesAfterDuration walks the clock past
// hold-down and verifies the suppression lifts: the next cycle
// resumes wire emit for the previously-held Identity, and
// TickTTL's clearExpiredSeqHoldDownsLocked drops the stale entry
// from the velocity map.
func TestSeqNoFlap_HoldDownReleasesAfterDuration(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(30*time.Second),
	)

	// Cross the threshold (3 advances against threshold=2). Hops
	// kept equal so the SeqNo tiebreaker rotates the winner across
	// uplinks and every cycle's sig differs (cache miss → advance).
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("expected SeqNoFlapHoldowns=1 after crossing threshold, got %d", tbl.CapStats().SeqNoFlapHoldowns)
	}

	// Within hold-down: emit suppressed.
	for _, e := range tbl.AnnounceTo(viewer) {
		if e.Identity == victim {
			t.Fatalf("victim emit must be suppressed during hold-down, got entry=%+v", e)
		}
	}

	// Walk past hold-down (30s + slack) and trigger TickTTL — the
	// velocity entry's holdUntil should be cleared, advance ring
	// trimmed, and the next AnnounceTo emits for victim resume.
	now = now.Add(45 * time.Second)
	tbl.TickTTL()

	tbl.mu.Lock()
	sv := tbl.flap.seqVelocities[victim]
	tbl.mu.Unlock()
	if sv != nil && !sv.holdUntil.IsZero() {
		t.Fatalf("clearExpiredSeqHoldDownsLocked must zero holdUntil for victim, got %v", sv.holdUntil)
	}

	resumed := false
	for _, e := range tbl.AnnounceTo(viewer) {
		if e.Identity == victim && e.Hops < HopsInfinity {
			resumed = true
			break
		}
	}
	if !resumed {
		t.Fatalf("victim emit must resume after hold-down expires")
	}
}

// TestSeqNoFlap_IngestGuardRejectsRunawayDuringHoldDown verifies the
// outbound-projected-SeqNo guard in ApplyUpdate: while Identity is
// in hold-down, an incoming announcement whose SeqNo would push
// outboundMax forward by >1 is rejected, preventing the upstream
// from continuing to drive our outbound SeqNo namespace forward
// during suppression. Same-SeqNo reconfirmations and incremental
// SeqNo bumps stay accepted.
func TestSeqNoFlap_IngestGuardRejectsRunawayDuringHoldDown(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Engage hold-down: 3 distinct fresh advances against threshold=2.
	// Same Hops, climbing SeqNo, so each cycle's winner shifts to the
	// newest uplink and produces a cache miss → advance.
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("hold-down must arm; SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}

	tbl.mu.RLock()
	currentOutboundMax := tbl.store.outboundMax[victim]
	tbl.mu.RUnlock()

	// Runaway: ingest a SeqNo far beyond outboundMax+1. Must be
	// rejected by the ingest guard.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u-runaway"),
		Hops: 4, SeqNo: currentOutboundMax + 100, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("runaway ingest during hold-down must be rejected, got status=%v", status)
	}

	// Same-SeqNo reconfirmation against an existing claim must
	// still pass through (refreshes receive-side TTL). Use uplink
	// u1's stored claim.
	status, err = tbl.UpdateRoute(RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u1"),
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute reconfirmation err: %v", err)
	}
	if status == RouteRejected {
		t.Fatalf("same-SeqNo reconfirmation during hold-down must not be rejected by the runaway guard, got %v", status)
	}
}

// TestSeqNoFlap_IngestGuardFrontsFastInvalidation pins the
// review-flagged regression: when the P2 SeqNo hold-down is
// engaged for an Identity, a bad-hops announcement (hops >
// MaxSaneHops) with `SeqNo > outboundMax+1` MUST be rejected by
// the P2 ingest guard BEFORE reaching P3 fast invalidation. The
// pre-fix order ran P3 first, so the bad-hops claim was accepted
// as a tombstone at the runaway SeqNo, poisoning the SeqNo-
// resurrection guard for the (Identity, Uplink) pair: any later
// legitimate recovery from the upstream had to advance past that
// huge SeqNo value before being admitted.
//
// Fixture: arm hold-down on the Identity, then ingest a bad-hops
// claim with a runaway SeqNo. Verify:
//   - status is RouteRejected (P2 guard fired);
//   - FastInvalidations counter stays at zero (P3 was NOT reached);
//   - storage does NOT contain a tombstone at the runaway SeqNo
//     for the bad uplink (so legit recovery can still advance
//     from the pre-runaway high-water).
func TestSeqNoFlap_IngestGuardFrontsFastInvalidation(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
		WithMaxSaneHops(8),
	)

	// Engage hold-down: 3 advances against threshold=2.
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("hold-down must arm; got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}

	tbl.mu.RLock()
	outboundMaxBefore := tbl.store.outboundMax[victim]
	tbl.mu.RUnlock()
	runawaySeqNo := outboundMaxBefore + 100

	// Bad-hops announcement on a new uplink with runaway SeqNo
	// during hold-down. Pre-fix: P3 fires first → tombstone at
	// runaway SeqNo gets appended for u-bad. Post-fix: P2 ingest
	// guard rejects before P3 runs → no tombstone added.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u-bad"),
		Hops: 12, SeqNo: runawaySeqNo, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("bad-hops runaway during hold-down must be rejected by P2 ingest guard BEFORE P3, got status=%v", status)
	}

	// P3 must not have run — its counter stays at zero.
	if got := tbl.CapStats().FastInvalidations; got != 0 {
		t.Fatalf("FastInvalidations must stay at 0 — P2 guard should reject before P3 runs; got %d", got)
	}

	// No tombstone (or any other claim) for u-bad must be stored.
	// Pre-fix this would be a withdrawn UplinkClaim at runaway SeqNo,
	// which would poison the SeqNo-resurrection guard for the
	// (victim, u-bad) pair so that legitimate recovery from the
	// real upstream has to exceed runawaySeqNo to be admitted.
	tbl.mu.RLock()
	bucket := tbl.store.buckets[victim]
	tbl.mu.RUnlock()
	for _, c := range bucket {
		if c.Uplink == domaintest.ID("u-bad") {
			t.Fatalf("P2 ingest guard must not let P3 store a poison tombstone for u-bad; got claim=%+v", c)
		}
	}
}

// TestSeqNoFlap_IngestGuardRejectsClampedHopsInfinityRunaway pins
// the combined-failure-mode regression: P2 hold-down guard must
// reject a wire-15-clamped-to-HopsInfinity bad-hops claim with a
// runaway SeqNo during hold-down, BEFORE the P3 fast-invalidation
// branch stores a tombstone at the runaway SeqNo. Without this,
// `incoming.Withdrawn == true` (derived by toUplinkClaim from
// `entry.Hops == HopsInfinity`) used to exempt the case from the
// P2 guard — the runaway peer then slipped past P2, hit P3, and
// poisoned the SeqNo-resurrection guard for the (Identity, Uplink)
// pair at the runaway SeqNo.
//
// Genuine wire withdrawals never reach this guard — they route
// through `Table.WithdrawRoute`, not `UpdateRoute`. Any
// `UpdateRoute` call with `Hops == HopsInfinity` is by construction
// a receive-path-clamped loop boundary (wire.Hops was 15+), so the
// guard must apply regardless of whether `incoming.Withdrawn` was
// derived from the sentinel.
func TestSeqNoFlap_IngestGuardRejectsClampedHopsInfinityRunaway(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
		WithMaxSaneHops(8),
	)

	// Engage hold-down: 3 advances against threshold=2.
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("hold-down must arm; got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}

	tbl.mu.RLock()
	outboundMaxBefore := tbl.store.outboundMax[victim]
	tbl.mu.RUnlock()
	runawaySeqNo := outboundMaxBefore + 100

	// Wire-15-clamped boundary + runaway SeqNo during hold-down.
	// `Hops = HopsInfinity` mirrors what the receive path produces
	// after `wire.Hops + 1 > HopsInfinity` clamp.
	// `toUplinkClaim` will derive `incoming.Withdrawn = true` from
	// the sentinel. Pre-fix the P2 guard's `!incoming.Withdrawn`
	// exempt let this slip past, P3 then stored a tombstone at
	// `runawaySeqNo` and any future legit recovery from the real
	// upstream would have to advance past that runaway value.
	// Post-fix the P2 guard rejects regardless of `Withdrawn`.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u-bad"),
		Hops: HopsInfinity, SeqNo: runawaySeqNo, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("clamped-HopsInfinity runaway during hold-down must be rejected by P2 guard BEFORE P3, got status=%v", status)
	}

	// P3 must NOT have run — its counter stays at zero (the rest of
	// the suite's P3 paths bump it; this one must not).
	if got := tbl.CapStats().FastInvalidations; got != 0 {
		t.Fatalf("FastInvalidations must stay at 0 — P2 guard should reject before P3 runs on clamped-Hops boundary; got %d", got)
	}

	// No poison tombstone for u-bad — the (victim, u-bad) bucket
	// slot must remain empty so a future legit announce from the
	// real upstream can land normally without crossing a runaway
	// SeqNo resurrection guard.
	tbl.mu.RLock()
	bucket := tbl.store.buckets[victim]
	tbl.mu.RUnlock()
	for _, c := range bucket {
		if c.Uplink == domaintest.ID("u-bad") {
			t.Fatalf("P2 ingest guard must not let P3 store a poison tombstone for u-bad on the clamped-Hops boundary; got claim=%+v", c)
		}
	}
}

// TestSeqNoFlap_HopAckPromotionDoesNotAdvanceCounter verifies that
// HopAck trust uplift at the same wire SeqNo does NOT count as an
// outbound-SeqNo advance — the cap must not engage purely on
// legitimate hop_ack confirmation traffic. The mechanism: HopAck
// promotion goes through ApplyUpdate's same-SeqNo replace path,
// which never reaches nextOutboundSeqLockedPerPeer (no wire emit
// is produced as a side effect of the promotion itself; outbound
// SeqNo only advances when AnnounceTo actually runs on a content
// change, and the trust uplift does not change the live winner's
// (Hops, SeqNo, Extra) sig).
func TestSeqNoFlap_HopAckPromotionDoesNotAdvanceCounter(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(1),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Initial announcement → fresh advance.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src"), NextHop: uplink,
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	_ = tbl.AnnounceTo(viewer)

	// Same-SeqNo HopAck promotion (trust uplift). MUST NOT advance
	// the outbound counter — no wire emit happens, no fresh
	// outbound SeqNo allocation.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: localID, NextHop: uplink,
		Hops: 2, SeqNo: 10, Source: RouteSourceHopAck,
	})
	_ = tbl.AnnounceTo(viewer)

	if tbl.CapStats().SeqNoFlapHoldowns != 0 {
		t.Fatalf("HopAck promotion at same SeqNo must NOT trigger hold-down, got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}
}

// TestSeqNoFlap_ThresholdCrossingCycleSuppressesEmit pins the
// review-flagged regression: when AnnounceTo's projection call is
// the one that CROSSES the SeqNo flap-cap threshold (records the
// advance that engages hold-down), the peer being announced to
// MUST NOT receive the over-threshold emit. The pre-fix code
// engaged hold-down inside nextOutboundSeqLockedPerPeer but still
// appended the entry to the result, so the engage peer got the
// emit and only subsequent peers' cycles observed the suppression
// — contradicting the "wire emit suppressed to EVERY peer"
// contract on the SeqNo flap cap.
//
// Setup: threshold=2, run two below-threshold cycles to prime the
// velocity ring, then a third cycle that records advance #3 (>2 →
// engage). The third AnnounceTo call must return an entry slice
// WITHOUT the victim Identity, even though THIS call is the one
// that engaged.
func TestSeqNoFlap_ThresholdCrossingCycleSuppressesEmit(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	findX := func(entries []AnnounceEntry) *AnnounceEntry {
		for i := range entries {
			if entries[i].Identity == victim {
				return &entries[i]
			}
		}
		return nil
	}

	// Cycles 1 and 2: below threshold (1, 2). Each crosses-into-cache
	// for victim, so each records an advance, but threshold=2 means
	// neither engages (1>2 false, 2>2 false).
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		emits := tbl.AnnounceTo(viewer)
		if findX(emits) == nil {
			t.Fatalf("cycle %d: victim emit expected (still below threshold)", i+1)
		}
		now = now.Add(time.Second)
	}

	// Cycle 3: advance #3 → 3>2 → engages hold-down INSIDE this
	// AnnounceTo call. The contract demands suppression for THIS
	// peer's emit too, not just subsequent peers.
	mustUpdate(t, tbl, RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u3"),
		Hops: 2, SeqNo: 12, Source: RouteSourceAnnouncement,
	})
	cycle3 := tbl.AnnounceTo(viewer)

	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("threshold-crossing AnnounceTo must engage hold-down, got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}
	if got := findX(cycle3); got != nil {
		t.Fatalf("threshold-crossing AnnounceTo MUST suppress emit for the engage peer; got entry=%+v", got)
	}
}

// TestSeqNoFlap_HoldDownArmingMarksDirty pins the dirty-flag
// propagation contract: when AnnounceTo's projection call engages
// a hold-down (RouteCapStats.SeqNoFlapHoldowns bumps inside the
// published Snapshot), Table.dirty must be set so the snapshot
// publisher's ConsumeDirty fast-path republishes the new counter
// value immediately. Without this propagation,
// fetch_route_summary.cap_admission.seqno_flap_holdowns would lag
// the actual engage event until some unrelated mutation/expiry
// triggered a republish — an operator-visible regression for a
// counter that exists specifically to surface the engage moment.
func TestSeqNoFlap_HoldDownArmingMarksDirty(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(1*time.Minute),
		WithSeqHoldDownDuration(2*time.Minute),
	)

	// Run two cycles below the threshold and drain the dirty flag —
	// we want the engage cycle to be the only thing producing a
	// dirty signal.
	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}
	// Drain any dirty signal that accumulated through ingest+emit.
	_ = tbl.ConsumeDirty()
	if tbl.IsDirty() {
		t.Fatalf("dirty flag must be clean after ConsumeDirty + no further mutations")
	}

	// Third advance trips the cap (3>2 → engage). The AnnounceTo
	// call ITSELF must mark dirty so a subsequent snapshot
	// republish sees SeqNoFlapHoldowns=1.
	mustUpdate(t, tbl, RouteEntry{
		Identity: victim, Origin: domaintest.ID("src-v"), NextHop: domaintest.ID("u3"),
		Hops: 2, SeqNo: 12, Source: RouteSourceAnnouncement,
	})
	// mustUpdate marks dirty for the storage mutation — drain again
	// so the assertion isolates AnnounceTo's contribution.
	_ = tbl.ConsumeDirty()

	_ = tbl.AnnounceTo(viewer)

	if tbl.CapStats().SeqNoFlapHoldowns != 1 {
		t.Fatalf("hold-down must engage on threshold-crossing AnnounceTo, got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}
	if !tbl.IsDirty() {
		t.Fatalf("AnnounceTo that armed hold-down must mark Table.dirty so the snapshot publisher republishes the SeqNoFlapHoldowns bump")
	}
}

// TestSeqNoFlap_WindowZeroDisablesCap pins the symmetric
// "window=0 disables" contract documented in config.go for
// CORSA_SEQNO_ADVANCE_WINDOW_SECONDS=0. Both knobs (threshold AND
// window) are operator-facing kill switches; the detector
// short-circuit must honour either being non-positive, otherwise a
// fixed-clock test fixture or a single-second high-rate burst
// would arm hold-down purely on advance count, contradicting the
// "per window" half of the cap contract.
func TestSeqNoFlap_WindowZeroDisablesCap(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(2),
		WithSeqAdvanceWindow(0),
	)

	for i, up := range []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3"), domaintest.ID("u4"), domaintest.ID("u5")} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"), NextHop: up,
			Hops: 2, SeqNo: uint64(10 + i), Source: RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
	}

	if tbl.CapStats().SeqNoFlapHoldowns != 0 {
		t.Fatalf("window=0 must disable the cap; got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}
}

// TestSeqNoFlap_DisabledCapShortCircuits pins the
// maxSeqAdvancePerWindow<=0 contract: the bare-Table default (no
// WithMaxSeqAdvancePerWindow) leaves the cap disabled, so
// recordSeqAdvanceLocked is a no-op even under storm-frequency
// ingest. RouteCapStats.SeqNoFlapHoldowns stays at zero. This is
// the deterministic-test default that mirrors the existing
// maxNextHopsPerOrigin pattern.
func TestSeqNoFlap_DisabledCapShortCircuits(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		victim  = domaintest.ID("victim")
		viewer  = domaintest.ID("peer-Z")
	)

	// No WithMaxSeqAdvancePerWindow → cap disabled at the bare
	// Table level. Override Production's DefaultMaxSeqAdvancePerWindow
	// by explicitly passing 0.
	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSeqAdvancePerWindow(0),
	)

	for i := 0; i < 50; i++ {
		mustUpdate(t, tbl, RouteEntry{
			Identity: victim, Origin: domaintest.ID("src-v"),
			NextHop: domaintest.ID("u-" + string(rune('a'+i%26))),
			Hops:    2 + (i % 10),
			SeqNo:   uint64(100 + i),
			Source:  RouteSourceAnnouncement,
		})
		_ = tbl.AnnounceTo(viewer)
		now = now.Add(time.Second)
	}

	if tbl.CapStats().SeqNoFlapHoldowns != 0 {
		t.Fatalf("disabled cap must not engage hold-down, got SeqNoFlapHoldowns=%d", tbl.CapStats().SeqNoFlapHoldowns)
	}
}
