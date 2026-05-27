package routing

import (
	"testing"
	"time"
)

// table_lookup_shaping_test.go covers Phase 3 PR 12.6 — the
// LookupForRelay wrapper that occasionally rotates the top-
// ranked candidate onto an alternative uplink to give an under-
// observed or just-recovered shaping candidate fewer of the
// "winner takes all" forwards.
//
// Fixed-clock convention follows the rest of the routing suite
// (2026-05-23) for the same upsertClaim ExpiresAt reason
// documented in table_health_reputation_test.go.

// markPairFullyAttested seeds a (Identity, Uplink) health entry
// with enough successful hop-acks to push it past the
// ReliabilityShapingMinAttempts threshold. Used by tests that
// need a "non-shaping" alternative — the plain MarkHopAck would
// stop at one observation, leaving the alternative ALSO inside
// the warmup window.
func markPairFullyAttested(t *testing.T, tbl *Table, identity, uplink PeerIdentity) {
	t.Helper()
	for i := uint64(0); i < ReliabilityShapingMinAttempts; i++ {
		tbl.MarkHopAck(identity, uplink, 0)
	}
}

// setShapingTestPair manipulates the health entry for the
// (Identity, Uplink) pair directly so the shaping-criterion
// tests can fix CompositeScore inputs without firing the apply-
// method side effects (state transitions, reliability EMA
// updates). Using MarkHopAck instead would let the reliability
// term flip the ranking and make the "shaping rotated the top
// slot" assertion pass for the wrong reason.
//
// The pair lands at Health=Good, ReliabilityScore=0.5 (the
// neutral mid-point that produces zero reliability bonus —
// see score.go's reliabilityBonus), HopAckAttempts set to
// `attempts` and HopAckSuccesses to half so the success ratio
// matches the neutral score. Suitable for shaping tests where
// only the warmup threshold (attempts < ReliabilityShapingMinAttempts)
// matters and the rank order MUST come from hops alone.
func setShapingTestPair(t *testing.T, tbl *Table, identity, uplink PeerIdentity, attempts uint64) {
	t.Helper()
	now := tbl.clock()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	state := tbl.health.ensureLocked(identity, uplink, now)
	state.Health = HealthGood
	state.Confirmed = true
	state.LastHopAck = now
	state.TransitionAt = now
	state.HopAckAttempts = attempts
	state.HopAckSuccesses = attempts / 2
	state.ReliabilityScore = 0.5
}

// setNonShapingTierPair stamps a fully-attested (past warmup) pair at a
// specific health tier, so the shaping tier-gate tests can build a
// non-shaping Bad/Dead alternative deterministically. Past the warmup
// floor and with no recent cooldown the pair is NOT a shaping candidate;
// the only thing distinguishing it is its health tier.
func setNonShapingTierPair(t *testing.T, tbl *Table, identity, uplink PeerIdentity, tier RouteHealth) {
	t.Helper()
	now := tbl.clock()
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	state := tbl.health.ensureLocked(identity, uplink, now)
	state.Health = tier
	state.LastHopAck = now
	state.TransitionAt = now
	state.HopAckAttempts = ReliabilityShapingMinAttempts + 5
	state.HopAckSuccesses = (ReliabilityShapingMinAttempts + 5) / 2
	state.ReliabilityScore = 0.5
}

// TestLookupForRelay_BadAlternativeNotPromotedOverShapingCandidate —
// the only non-shaping alternative is HealthBad. Bad pairs survive
// Lookup (merely low-scored, not filtered), and a Bad pair is NOT a
// shaping candidate, so a naive `!isShapingCandidate` rotation would
// divert relay traffic from a merely-warming Good candidate onto a
// known-bad path. The tier gate must reject it: no rotation, the
// shaping candidate stays on top (Phase 3 §4.6).
func TestLookupForRelay_BadAlternativeNotPromotedOverShapingCandidate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-bad", 2, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setNonShapingTierPair(t, tbl, "id-target", "id-uplink-bad", HealthBad)

	// Precondition: the Good shaping candidate still outranks the Bad
	// alternative (the −500 Bad penalty sinks it).
	plain := tbl.Lookup("id-target")
	if len(plain) != 2 || plain[0].NextHop != "id-uplink-fast" {
		t.Fatalf("precondition: plain Lookup order = %v, want fast first", plain)
	}

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 2 {
		t.Fatalf("LookupForRelay length = %d, want 2", len(shaped))
	}
	if shaped[0].NextHop != "id-uplink-fast" {
		t.Fatalf("shaped[0].NextHop = %q, want id-uplink-fast (Bad alternative must not be promoted)", shaped[0].NextHop)
	}
}

// TestLookupForRelay_DirectDeadAlternativeNotPromoted — a Direct
// HealthDead pair survives Lookup via the direct exemption and is not a
// shaping candidate, so without the tier gate it would be promoted over
// a healthy warmup candidate. The gate must reject Dead alternatives.
func TestLookupForRelay_DirectDeadAlternativeNotPromoted(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Direct route to id-target (NextHop == id-target, hops=1, Direct):
	// kept in Lookup even when Dead. Plus a 2-hop announcement path that
	// is a Good shaping candidate and outranks the Dead direct (Dead
	// scores the −1 sentinel).
	if _, err := tbl.AddDirectPeer("id-target"); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 2, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setNonShapingTierPair(t, tbl, "id-target", "id-target", HealthDead)

	plain := tbl.Lookup("id-target")
	if len(plain) < 2 || plain[0].NextHop != "id-uplink-fast" {
		t.Fatalf("precondition: plain Lookup order = %v, want fast (Good) first", plain)
	}

	shaped := tbl.LookupForRelay("id-target", 0)
	if shaped[0].NextHop != "id-uplink-fast" {
		t.Fatalf("shaped[0].NextHop = %q, want id-uplink-fast (Dead direct must not be promoted)", shaped[0].NextHop)
	}
}

// TestLookupForRelay_HintZeroRotatesWhenTopIsShapingCandidate —
// the canonical happy path. hint=0 (0 % ShapingProbeRatio == 0)
// AND the top entry is a shaping candidate (no hop_acks yet)
// AND the alternative is past the warmup floor → the
// alternative moves to index 0 and the shaping candidate to
// index 1.
func TestLookupForRelay_HintZeroRotatesWhenTopIsShapingCandidate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Two uplinks for the target. The 1-hop path normally wins
	// CompositeScore by hops; we set it up as a warmup-stage
	// shaping candidate (HopAckAttempts below the threshold) and
	// the 2-hop path as a past-warmup non-shaping alternative
	// with neutral reliability (so the reliability term does not
	// flip the plain ranking against the hop-count advantage).
	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-stable", 2, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-stable", ReliabilityShapingMinAttempts+5)

	// Sanity precondition: plain Lookup ranks the 1-hop on top.
	plain := tbl.Lookup("id-target")
	if len(plain) != 2 || plain[0].NextHop != "id-uplink-fast" {
		t.Fatalf("precondition: plain Lookup order = %v, want fast first", plain)
	}

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 2 {
		t.Fatalf("LookupForRelay length = %d, want 2", len(shaped))
	}
	if shaped[0].NextHop != "id-uplink-stable" {
		t.Fatalf("shaped[0].NextHop = %q, want id-uplink-stable (shaping rotation)", shaped[0].NextHop)
	}
	if shaped[1].NextHop != "id-uplink-fast" {
		t.Fatalf("shaped[1].NextHop = %q, want id-uplink-fast (demoted shaping candidate)", shaped[1].NextHop)
	}
}

// TestLookupForRelay_HintNonZeroDoesNotRotate — for any hint
// that is NOT a multiple of ShapingProbeRatio the wrapper must
// return the plain Lookup result unmodified.
func TestLookupForRelay_HintNonZeroDoesNotRotate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-stable", 2, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-stable", ReliabilityShapingMinAttempts+5)

	for hint := uint64(1); hint < ShapingProbeRatio; hint++ {
		shaped := tbl.LookupForRelay("id-target", hint)
		if len(shaped) != 2 || shaped[0].NextHop != "id-uplink-fast" {
			t.Fatalf("hint=%d: shaped[0] = %q, want fast (no rotation for non-multiple hint)", hint, shaped[0].NextHop)
		}
	}
}

// TestLookupForRelay_NoRotationWhenTopIsNotShapingCandidate —
// even at hint=0, if the top pair has already crossed
// ReliabilityShapingMinAttempts the rotation is suppressed.
// The shaping pass exists to accelerate sample accrual on
// under-observed pairs; a well-observed pair should not be
// downgraded.
func TestLookupForRelay_NoRotationWhenTopIsNotShapingCandidate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-stable", 2, RouteSourceAnnouncement)
	// Both pairs are fully attested — neither qualifies for
	// shaping, so even hint=0 leaves the ranking alone.
	markPairFullyAttested(t, tbl, "id-target", "id-uplink-fast")
	markPairFullyAttested(t, tbl, "id-target", "id-uplink-stable")

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 2 || shaped[0].NextHop != "id-uplink-fast" {
		t.Fatalf("shaped[0] = %q, want fast (no shaping candidate on top)", shaped[0].NextHop)
	}
}

// TestLookupForRelay_NoRotationWhenNoNonShapingAlternative —
// hint=0 AND the top is a shaping candidate, but ALL
// alternatives are also shaping candidates. Rotating among
// shaping candidates only moves the warmup gap around; the
// helper leaves the order alone.
func TestLookupForRelay_NoRotationWhenNoNonShapingAlternative(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-mid", 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-far", 3, RouteSourceAnnouncement)
	// No MarkHopAck on any of them — all three are shaping
	// candidates by the warmup criterion.

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 3 || shaped[0].NextHop != "id-uplink-fast" {
		t.Fatalf("shaped[0] = %q, want fast (no non-shaping alternative)", shaped[0].NextHop)
	}
}

// TestLookupForRelay_SingleUplinkNeverRotates — a one-uplink
// result has nothing to rotate to. Returns the single entry
// regardless of hint.
func TestLookupForRelay_SingleUplinkNeverRotates(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink", 1, RouteSourceAnnouncement)

	for hint := uint64(0); hint < 8; hint++ {
		shaped := tbl.LookupForRelay("id-target", hint)
		if len(shaped) != 1 || shaped[0].NextHop != "id-uplink" {
			t.Fatalf("hint=%d single-uplink: got %v, want [id-uplink]", hint, shaped)
		}
	}
}

// TestLookupForRelay_EmptyLookupReturnsNil — when the underlying
// Lookup returns nil (no routes), shaping is a no-op.
func TestLookupForRelay_EmptyLookupReturnsNil(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("self"))
	if got := tbl.LookupForRelay("id-never-seen", 0); got != nil {
		t.Fatalf("LookupForRelay on empty table = %v, want nil", got)
	}
}

// TestLookupForRelay_PicksHighestRankedNonShapingAlternative —
// when multiple alternatives exist and one is non-shaping, the
// rotation MUST pick the highest-ranked non-shaping one (not
// the first index in the routes slice). Validates that
// LookupForRelay walks routes[1:] in CompositeScore order
// rather than picking routes[1] blindly.
func TestLookupForRelay_PicksHighestRankedNonShapingAlternative(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	// Top is shaping (1 hop, warmup attempts). routes[1] is
	// ALSO a shaping candidate (3 hops, warmup attempts).
	// routes[2] is the past-warmup non-shaping alternative
	// (4 hops). Without proper "skip shaping alternatives"
	// logic the rotation would land on routes[1].
	//
	// All three pairs are stamped to Health=Good with neutral
	// reliability so the rank order comes from hops alone —
	// otherwise the Questionable penalty (from the
	// upsertClaim-seeded Health) would put far-attested above
	// fast in plain ranking and the rotation path would never
	// fire.
	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-mid-shaping", 3, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-far-attested", 4, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-mid-shaping", ReliabilityShapingMinAttempts-1)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-far-attested", ReliabilityShapingMinAttempts+5)

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 3 {
		t.Fatalf("LookupForRelay length = %d, want 3", len(shaped))
	}
	if shaped[0].NextHop != "id-uplink-far-attested" {
		t.Fatalf("shaped[0].NextHop = %q, want id-uplink-far-attested", shaped[0].NextHop)
	}
}

// TestLookupForRelay_PostCooldownPairIsShapingCandidate —
// after applyCooldownExpiryLocked clears a cooldown (e.g. via
// TickHealth past the 2-minute window), the recovered pair
// stays a shaping candidate for ShapingRecentCooldownWindow.
//
// Going through MarkHopFailure × 5 + clock advance + TickHealth
// to set up "post-cooldown shaping" muddies the test:
// reputation legitimately drops the fast pair's
// CompositeScore below the stable alternative even before
// shaping fires, so the assertion would pass for the wrong
// reason. Instead the test stamps the relevant fields
// directly via ForceHealthForTest and a manual field set under
// t.mu, isolating the criterion under test.
func TestLookupForRelay_PostCooldownPairIsShapingCandidate(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-stable", 2, RouteSourceAnnouncement)
	// Fully attest BOTH pairs so the warmup criterion never
	// matches; only the post-cooldown criterion is left to
	// trigger shaping.
	markPairFullyAttested(t, tbl, "id-target", "id-uplink-fast")
	markPairFullyAttested(t, tbl, "id-target", "id-uplink-stable")

	// Stamp LastCooldownClearedAt directly on the fast pair so
	// it qualifies as a shaping candidate by the recency
	// criterion. ForceHealthForTest is the public test seam for
	// this sort of state manipulation; we use it together with a
	// brief t.mu.Lock to touch the reputation field without
	// going through the apply-method machinery.
	tbl.mu.Lock()
	state := tbl.health.getLocked("id-target", "id-uplink-fast")
	if state == nil {
		tbl.mu.Unlock()
		t.Fatal("fast pair health entry missing after attestation")
	}
	state.LastCooldownClearedAt = now
	tbl.mu.Unlock()

	shaped := tbl.LookupForRelay("id-target", 0)
	if len(shaped) != 2 {
		t.Fatalf("LookupForRelay length = %d, want 2", len(shaped))
	}
	if shaped[0].NextHop != "id-uplink-stable" {
		t.Fatalf("shaped[0].NextHop = %q, want id-uplink-stable (post-cooldown rotation)", shaped[0].NextHop)
	}
}

// TestLookupForRelay_RatioRotationFires1InN — over a large
// number of hints, exactly 1-in-ShapingProbeRatio LookupForRelay
// calls rotate (when the conditions hold). Pins the documented
// rotation ratio numerically so a future change to
// ShapingProbeRatio doesn't accidentally make rotation
// monotonic or constant.
func TestLookupForRelay_RatioRotationFires1InN(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin("self"), WithClock(fixedClock(now)))

	upsertClaim(t, tbl, "id-target", "id-uplink-fast", 1, RouteSourceAnnouncement)
	upsertClaim(t, tbl, "id-target", "id-uplink-stable", 2, RouteSourceAnnouncement)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-fast", ReliabilityShapingMinAttempts-1)
	setShapingTestPair(t, tbl, "id-target", "id-uplink-stable", ReliabilityShapingMinAttempts+5)

	const totalCalls uint64 = 100
	rotated := 0
	for hint := uint64(1); hint <= totalCalls; hint++ {
		shaped := tbl.LookupForRelay("id-target", hint)
		if shaped[0].NextHop == "id-uplink-stable" {
			rotated++
		}
	}
	wantRotated := int(totalCalls / ShapingProbeRatio)
	if rotated != wantRotated {
		t.Fatalf("over %d calls: rotated %d times, want %d (1 in %d)", totalCalls, rotated, wantRotated, ShapingProbeRatio)
	}
}
