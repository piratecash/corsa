package routing

import "time"

// CompositeScore is the unified ranking function for uplink claims,
// introduced in Phase 2 (docs/protocol/route_health.md
// §2.2). It replaces the pure-by-hops sort that Lookup used pre-Phase-2
// and serves as the single source of truth that Phase 3 will extend
// with a reliability term (overview §10.5).
//
// Score components (higher is better):
//
//   - base: 100 − hops × 10. Prefers shorter paths but does not
//     dominate other factors — a 3-hop low-RTT path can beat a
//     2-hop high-RTT path by ~20 points.
//   - RTT bonus: +30 when RTT < 20 ms, linear taper to 0 at 100 ms,
//     0 above 100 ms. Nudges selection toward local-network paths.
//   - health penalty: 0 (Good), −250 (Questionable), −500 (Bad).
//     Dead forces the result to −1 (scoreExcluded sentinel) to mark
//     the claim "excluded from selection".
//
//     The penalty magnitudes are deliberately large relative to the
//     base/RTT/source spread (max ≈ 190: hops ∈ [1,15] gives a 140-unit
//     base swing, RTT contributes up to +30, source up to +20). With
//     Pq=250 and Pb=500 the tiers are strictly ordered: every Good
//     candidate outranks every Questionable, and every Questionable
//     outranks every Bad, regardless of how favourably hop count / RTT
//     / source-trust line up inside a tier. This is a release-blocking
//     invariant — TableRouter / routing_relay use a "selected Bad
//     route ⇒ no Good/Questionable alternative" structural assumption
//     to fire route_query_v1 recovery on every Bad-route send. If a
//     Bad route could outscore some Good alternative the recovery
//     signal would fire on a path the selector already preferred,
//     which is meaningless. Within a tier the composite still ranks
//     by hops / RTT / source as before.
//   - source bonus: +20 (Direct), +10 (HopAck), 0 (Announcement).
//     Mirrors the existing RouteSource.TrustRank() ordering but in
//     score units.
//
// nil-health safe: passing health=nil returns base + sourceBonus and
// skips RTT/health terms. This preserves backward compatibility with
// callers that have not yet wired health tracking into Lookup (PR
// 11.2 integration).
//
// The function is pure — no clock, no state mutation, no I/O — so it
// runs lock-free and is safe to call under either t.mu.RLock or
// t.mu.Lock.
const (
	scoreBase             = 100.0
	scoreHopPenalty       = 10.0
	scoreRTTBonusMax      = 30.0
	scoreRTTLowThreshold  = 20 * time.Millisecond
	scoreRTTHighThreshold = 100 * time.Millisecond
	// scoreHealthQPenalty / scoreHealthBadPenalty are sized to strictly
	// dominate the within-tier spread (≈190 units across hops/RTT/source)
	// so the tiers cannot interleave. See the CompositeScore doc-comment
	// above for the invariant and its release-blocking rationale.
	scoreHealthQPenalty   = 250.0
	scoreHealthBadPenalty = 500.0
	scoreSourceDirect     = 20.0
	scoreSourceHopAck     = 10.0
	// scoreExcluded is the sentinel returned for Dead health (or any
	// other "do not select" condition). Filtering Dead is done by
	// the caller inspecting Health == HealthDead BEFORE calling
	// CompositeScore (see Table.Lookup) — never by checking score
	// sign. Under the strict-tier penalty sizing above a raw
	// Questionable score (min ≈ −300), a raw Bad score (min ≈
	// −550), and even a high-hops Good score (min ≈ −50) can all
	// be negative, so sign alone cannot tell Dead apart from any
	// other legitimately-selectable claim. The sentinel value
	// stays as a defence-in-depth signal for diagnostics paths
	// that bypass the Health check; it must not be used as the
	// primary Dead filter.
	scoreExcluded = -1.0
)

// CompositeScore ranks an uplink claim characterised by its hop count
// and learning source against an optional health state. Pass health=nil
// to fall back to the by-hops + source semantic used prior to Phase 2.
func CompositeScore(hops uint8, source RouteSource, health *RouteHealthState) float64 {
	if health != nil && health.Health == HealthDead {
		return scoreExcluded
	}

	score := scoreBase - float64(hops)*scoreHopPenalty

	if health != nil && health.RTT > 0 {
		score += rttBonus(health.RTT)
	}

	if health != nil {
		switch health.Health {
		case HealthGood:
			// no penalty — Good is the top tier
		case HealthQuestionable:
			// −250: strictly below every Good candidate regardless
			// of base / RTT / source within-tier spread.
			score -= scoreHealthQPenalty
		case HealthBad:
			// −500: strictly below every Questionable, and therefore
			// strictly below every Good. The TableRouter Bad-route
			// recovery trigger relies on this invariant: selecting
			// a Bad route at Lookup time means no Good or
			// Questionable alternative exists.
			score -= scoreHealthBadPenalty
		case HealthDead:
			// already returned above
		}
	}

	score += sourceBonus(source)

	return score
}

// rttBonus computes the RTT-component of CompositeScore.
//
//   - RTT < scoreRTTLowThreshold (20 ms): full +30 bonus.
//   - scoreRTTLowThreshold ≤ RTT < scoreRTTHighThreshold (20-100 ms):
//     linear taper from +30 down to 0.
//   - RTT ≥ scoreRTTHighThreshold (100 ms): 0 bonus.
//
// The linear taper makes the score sensitive to RTT improvements in
// the range typical for direct internet routes (sub-100 ms) without
// over-weighting LAN paths that are already faster than anything
// across the WAN.
func rttBonus(rtt time.Duration) float64 {
	if rtt <= scoreRTTLowThreshold {
		return scoreRTTBonusMax
	}
	if rtt >= scoreRTTHighThreshold {
		return 0
	}
	// rtt is between scoreRTTLowThreshold and scoreRTTHighThreshold.
	// Linear interpolation: bonus = max × (high − rtt) / (high − low).
	span := float64(scoreRTTHighThreshold - scoreRTTLowThreshold)
	remaining := float64(scoreRTTHighThreshold - rtt)
	return scoreRTTBonusMax * remaining / span
}

// sourceBonus translates the route learning source into score units.
// The ordering matches RouteSource.TrustRank() but uses score-unit
// constants so the composite formula stays a single expression.
//
// RouteSourceLocal (synthetic self-route, hops=0) is mapped to the
// same +20 as Direct — neither value reaches the Lookup-ranking path
// because self-routes are returned by Lookup as a short-circuit
// before composite scoring runs, but keeping the case explicit guards
// against future code paths that pass it in by accident.
func sourceBonus(source RouteSource) float64 {
	switch source {
	case RouteSourceLocal, RouteSourceDirect:
		return scoreSourceDirect
	case RouteSourceHopAck:
		return scoreSourceHopAck
	case RouteSourceAnnouncement:
		return 0
	default:
		return 0
	}
}

// EWMASmoothingFactor is the alpha for UpdateRTT's exponentially-
// weighted moving average. Picked from the roadmap iter-1.5 §1.5c
// recommendation: 0.3 weighs recent samples enough to track route
// quality changes within ~5 samples while remaining stable against
// per-sample outliers.
const EWMASmoothingFactor = 0.3

// UpdateRTT folds a fresh RTT sample into the running EWMA estimate
// and returns the new value. The caller is responsible for
// persisting it back into RouteHealthState.RTT under the appropriate
// lock.
//
// Cold-start contract: when prev == 0 the function returns sample
// verbatim — the first observation becomes the baseline. Outlier
// risk is acceptable because EWMASmoothingFactor=0.3 makes the
// running estimate forget that seed within ~5 subsequent samples;
// a warm-up window was considered and deferred to Phase 3 if field
// telemetry shows recurring problems
// (docs/protocol/route_health.md).
//
// The function is pure (no time/state side effects) and safe to call
// under either lock mode.
func UpdateRTT(prev, sample time.Duration) time.Duration {
	if sample <= 0 {
		return prev
	}
	if prev == 0 {
		return sample
	}
	blended := float64(prev)*(1-EWMASmoothingFactor) + float64(sample)*EWMASmoothingFactor
	return time.Duration(blended)
}
