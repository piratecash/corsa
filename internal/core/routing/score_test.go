package routing

import (
	"math"
	"testing"
	"time"
)

const floatEpsilon = 1e-9

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < floatEpsilon
}

// TestUpdateRTT_ColdStartReturnsSample — the cold-start contract:
// prev=0 → return sample verbatim. This is the documented behaviour
// for the first observation, accepting outlier risk in exchange for
// simple code.
func TestUpdateRTT_ColdStartReturnsSample(t *testing.T) {
	cases := []struct {
		sample time.Duration
		want   time.Duration
	}{
		{50 * time.Millisecond, 50 * time.Millisecond},
		{1 * time.Millisecond, 1 * time.Millisecond},
		{2 * time.Second, 2 * time.Second},
	}
	for _, c := range cases {
		got := UpdateRTT(0, c.sample)
		if got != c.want {
			t.Fatalf("UpdateRTT(0, %v) = %v, want %v", c.sample, got, c.want)
		}
	}
}

// TestUpdateRTT_NonPositiveSampleIgnored — defensive guard: a
// non-positive sample (zero or negative duration) is treated as
// "invalid measurement" and the running average is preserved
// unchanged.
func TestUpdateRTT_NonPositiveSampleIgnored(t *testing.T) {
	prev := 50 * time.Millisecond
	if got := UpdateRTT(prev, 0); got != prev {
		t.Fatalf("UpdateRTT(50ms, 0) = %v, want %v", got, prev)
	}
	if got := UpdateRTT(prev, -10*time.Millisecond); got != prev {
		t.Fatalf("UpdateRTT(50ms, -10ms) = %v, want %v", got, prev)
	}
}

// TestUpdateRTT_BlendsAtAlpha verifies the EWMA blend with the
// canonical alpha=0.3: new = prev*0.7 + sample*0.3.
func TestUpdateRTT_BlendsAtAlpha(t *testing.T) {
	prev := 100 * time.Millisecond
	sample := 200 * time.Millisecond
	got := UpdateRTT(prev, sample)
	want := time.Duration(float64(prev)*0.7 + float64(sample)*0.3)
	if got != want {
		t.Fatalf("UpdateRTT(100ms, 200ms) = %v, want %v", got, want)
	}
}

// TestUpdateRTT_ConvergesToNewBaseline — running a stable stream of
// samples through UpdateRTT converges the running average toward the
// sample value. With alpha=0.3, ~5 samples bring the estimate within
// 17% of the target — fast enough for route quality tracking.
func TestUpdateRTT_ConvergesToNewBaseline(t *testing.T) {
	rtt := time.Duration(0)
	const sample = 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		rtt = UpdateRTT(rtt, sample)
	}

	// After 10 samples convergence is within ~3%.
	delta := math.Abs(float64(rtt) - float64(sample))
	rel := delta / float64(sample)
	if rel > 0.05 {
		t.Fatalf("after 10 stable samples: rtt=%v, sample=%v (relative drift %.4f, want < 0.05)", rtt, sample, rel)
	}
}

// TestUpdateRTT_OutlierSeedDecaysQuickly verifies the cold-start
// trade-off: if the first sample is an outlier, the running estimate
// recovers toward the stable baseline within a handful of samples.
// This is the rationale for not adding a warm-up window in Phase 2.
func TestUpdateRTT_OutlierSeedDecaysQuickly(t *testing.T) {
	// First sample is a 500ms outlier; subsequent samples are 50ms.
	rtt := UpdateRTT(0, 500*time.Millisecond)
	if rtt != 500*time.Millisecond {
		t.Fatalf("cold start did not seed with sample, got %v", rtt)
	}
	for i := 0; i < 7; i++ {
		rtt = UpdateRTT(rtt, 50*time.Millisecond)
	}
	// After 7 stable samples, estimate should be well below the
	// outlier seed — within 2× the true baseline.
	if rtt > 100*time.Millisecond {
		t.Fatalf("after outlier seed + 7 stable samples: rtt=%v, want < 100ms", rtt)
	}
}

// TestCompositeScore_NilHealthFallsBackToByHops verifies the
// backward-compat path: passing health=nil returns base − hops×10 +
// sourceBonus. PR 11.2 integrates CompositeScore into Lookup; callers
// that have not yet wired health (or are testing edge cases) rely on
// this fallback.
func TestCompositeScore_NilHealthFallsBackToByHops(t *testing.T) {
	cases := []struct {
		hops   uint8
		source RouteSource
		want   float64
	}{
		// 100 − hops*10 + sourceBonus
		{1, RouteSourceDirect, 100 - 10 + 20},
		{2, RouteSourceHopAck, 100 - 20 + 10},
		{3, RouteSourceAnnouncement, 100 - 30 + 0},
		{5, RouteSourceAnnouncement, 100 - 50 + 0},
	}
	for _, c := range cases {
		got := CompositeScore(c.hops, c.source, nil)
		if !almostEqual(got, c.want) {
			t.Fatalf("CompositeScore(hops=%d, source=%s, nil) = %f, want %f", c.hops, c.source, got, c.want)
		}
	}
}

// TestCompositeScore_DeadExcluded — Dead health forces the result
// to the scoreExcluded sentinel regardless of other inputs. The
// sentinel is the value-level "do not select" marker for
// diagnostics; production Dead filtering is done by the caller on
// Health == HealthDead BEFORE invoking CompositeScore (see
// Table.Lookup), not by checking score sign — under PR 11.35 P2
// strict-tier penalties a Questionable / Bad / very-long-Good
// score can also be negative, so sign-based detection cannot tell
// Dead apart from a legitimately-selectable last-resort claim.
func TestCompositeScore_DeadExcluded(t *testing.T) {
	health := &RouteHealthState{Health: HealthDead, RTT: 10 * time.Millisecond}
	got := CompositeScore(1, RouteSourceDirect, health)
	if !almostEqual(got, scoreExcluded) {
		t.Fatalf("CompositeScore with HealthDead = %f, want %f (scoreExcluded sentinel)", got, scoreExcluded)
	}
}

// TestCompositeScore_RTTBonusFavorsLocalPath — the key Phase 2
// motivation: a 3-hop path with low RTT can beat a 2-hop path with
// high RTT. Without the RTT bonus, hop count alone would always pick
// the 2-hop route.
func TestCompositeScore_RTTBonusFavorsLocalPath(t *testing.T) {
	// Path A: 3 hops, 10ms RTT, Good health, Announcement source.
	healthA := &RouteHealthState{Health: HealthGood, RTT: 10 * time.Millisecond}
	scoreA := CompositeScore(3, RouteSourceAnnouncement, healthA)

	// Path B: 2 hops, 150ms RTT, Good health, Announcement source.
	healthB := &RouteHealthState{Health: HealthGood, RTT: 150 * time.Millisecond}
	scoreB := CompositeScore(2, RouteSourceAnnouncement, healthB)

	if scoreA <= scoreB {
		t.Fatalf("expected A (3hops/10ms) > B (2hops/150ms): scoreA=%f, scoreB=%f", scoreA, scoreB)
	}
}

// TestCompositeScore_HealthPenaltyDeprioritizes — a healthy 2-hop
// route beats a Bad 1-hop direct route, demonstrating that health
// penalty outweighs even the Direct trust bonus when the path is
// known-broken.
func TestCompositeScore_HealthPenaltyDeprioritizes(t *testing.T) {
	// Healthy 2-hop announcement path.
	healthGood := &RouteHealthState{Health: HealthGood, RTT: 30 * time.Millisecond}
	scoreGood := CompositeScore(2, RouteSourceAnnouncement, healthGood)

	// Bad 1-hop direct path.
	healthBad := &RouteHealthState{Health: HealthBad, RTT: 30 * time.Millisecond}
	scoreBad := CompositeScore(1, RouteSourceDirect, healthBad)

	if scoreGood <= scoreBad {
		t.Fatalf("expected Good 2-hop > Bad 1-hop direct: scoreGood=%f, scoreBad=%f", scoreGood, scoreBad)
	}
}

// TestCompositeScore_RTTBonusBoundaries verifies the RTT bonus
// piecewise function:
//   - At/below 20ms: full +30 bonus.
//   - At 60ms (midpoint of taper): +15 bonus.
//   - At/above 100ms: 0 bonus.
//
// Boundary semantics: <=20ms gives full bonus, >=100ms gives zero;
// the (20ms, 100ms) interval is linearly interpolated.
func TestCompositeScore_RTTBonusBoundaries(t *testing.T) {
	healthBelow := &RouteHealthState{Health: HealthGood, RTT: 20 * time.Millisecond}
	healthMid := &RouteHealthState{Health: HealthGood, RTT: 60 * time.Millisecond}
	healthAbove := &RouteHealthState{Health: HealthGood, RTT: 100 * time.Millisecond}
	healthCold := &RouteHealthState{Health: HealthGood, RTT: 0}

	scoreBelow := CompositeScore(1, RouteSourceAnnouncement, healthBelow)
	scoreMid := CompositeScore(1, RouteSourceAnnouncement, healthMid)
	scoreAbove := CompositeScore(1, RouteSourceAnnouncement, healthAbove)
	scoreCold := CompositeScore(1, RouteSourceAnnouncement, healthCold)

	// 1 hop = base 100 − 10 = 90, plus source 0 for Announcement.
	const base = 90.0
	if !almostEqual(scoreBelow, base+30) {
		t.Fatalf("RTT=20ms: score=%f, want base+30=%f", scoreBelow, base+30)
	}
	if !almostEqual(scoreMid, base+15) {
		t.Fatalf("RTT=60ms: score=%f, want base+15=%f (midpoint of taper)", scoreMid, base+15)
	}
	if !almostEqual(scoreAbove, base) {
		t.Fatalf("RTT=100ms: score=%f, want base=%f (zero bonus)", scoreAbove, base)
	}
	if !almostEqual(scoreCold, base) {
		t.Fatalf("RTT=0 (no estimate yet): score=%f, want base=%f (skip RTT term)", scoreCold, base)
	}
}

// TestCompositeScore_SourceBonusOrdering verifies the source trust
// ordering: Direct > HopAck > Announcement. Each level adds a
// distinct bonus that matches the existing RouteSource.TrustRank()
// hierarchy used elsewhere in the codebase.
func TestCompositeScore_SourceBonusOrdering(t *testing.T) {
	health := &RouteHealthState{Health: HealthGood, RTT: 10 * time.Millisecond}
	direct := CompositeScore(1, RouteSourceDirect, health)
	hopAck := CompositeScore(1, RouteSourceHopAck, health)
	announce := CompositeScore(1, RouteSourceAnnouncement, health)

	if !(direct > hopAck && hopAck > announce) {
		t.Fatalf("expected direct > hopAck > announce, got %f %f %f", direct, hopAck, announce)
	}
	if !almostEqual(direct-hopAck, scoreSourceDirect-scoreSourceHopAck) {
		t.Fatalf("direct−hopAck = %f, want %f", direct-hopAck, scoreSourceDirect-scoreSourceHopAck)
	}
	if !almostEqual(hopAck-announce, scoreSourceHopAck) {
		t.Fatalf("hopAck−announce = %f, want %f", hopAck-announce, scoreSourceHopAck)
	}
}
