package routing

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// RouteHealth describes the active reachability state of a route to a
// specific identity via a specific direct-peer uplink. The state
// machine is driven by hop_ack timestamps and probe results.
//
// Health is a Phase 2 primitive (docs/protocol/route_health.md):
// it answers "is identity X currently reachable through uplink U?",
// which is a different question from peerHealth — "is the direct peer
// itself alive?". A direct peer can be peerHealth.Score>0 (TCP session
// alive, pongs flowing) while a specific route through that peer is
// Dead because the relay chain beyond the peer fell over.
type RouteHealth uint8

const (
	// HealthGood — uplink confirmed via recent hop_ack or
	// probe_ack(reachable=true). Route used normally, full TTL.
	HealthGood RouteHealth = iota

	// HealthQuestionable — no recent confirmation for this pair.
	// Probes are sent every HealthProbeInterval; route deprioritized
	// in CompositeScore but still selectable when no better path
	// exists.
	HealthQuestionable

	// HealthBad — unresponsive: no hop_ack for HealthBadAfter OR
	// ProbeFailures crossed HealthProbeFailureThreshold. Route
	// excluded from selection unless no other uplinks remain (last
	// resort before gossip fallback).
	HealthBad

	// HealthDead — fully timed out (no response for HealthDeadAfter).
	// Route locally invalidated and stop being announced. Wire
	// withdrawal sent only if this node is the origin of the route;
	// transit routes are silently dropped and converge via TTL expiry
	// or origin's own withdrawal — the zero-trust budget forbids
	// transit nodes from authoritatively withdrawing on origin's
	// behalf.
	HealthDead
)

// String returns the lower-case wire-format label used by RPC
// observability and structured logs.
func (h RouteHealth) String() string {
	switch h {
	case HealthGood:
		return "good"
	case HealthQuestionable:
		return "questionable"
	case HealthBad:
		return "bad"
	case HealthDead:
		return "dead"
	default:
		return "unknown"
	}
}

// State machine timings. The probe-driven Bad transition can fire
// before the passive hop_ack-driven one — 3 probe failures × 30 s
// probe cadence = 90 s, while passive hop_ack idle reaches Bad at
// 122 s. The asymmetry is intentional: active checks (probes) should
// detect failure faster than passive observation (hop_ack idle), so
// that an active prober gets a fast quality signal while a node that
// hasn't probed yet still has the slower passive timeline as a
// fallback. See docs/protocol/route_health.md.
const (
	// HealthQuestionableAfter — hop_ack idle period before
	// Good→Questionable.
	HealthQuestionableAfter = 60 * time.Second

	// HealthBadAfter — total hop_ack idle period before
	// Questionable→Bad via the passive timeline.
	HealthBadAfter = 122 * time.Second

	// HealthDeadAfter — total hop_ack idle period before Bad→Dead.
	HealthDeadAfter = 182 * time.Second

	// HealthProbeFailureThreshold — number of consecutive probe
	// failures (timeout or reachable=false) that transitions a
	// non-Good state to Bad regardless of the hop_ack timeline.
	HealthProbeFailureThreshold = 3

	// HealthProbeInterval — cadence at which Questionable pairs are
	// probed. Used by the Phase 2 probe sender (PR 11.3); declared
	// here so the state machine and the sender share one constant.
	//
	// Raised from 15s to 30s to halve active-probe frame volume across
	// a large mesh (a 1000-node node may hold many Questionable pairs).
	// The probe-driven Bad timeline (3 × 30s = 90s) stays below the
	// passive hop_ack one (122s), preserving the "active detects faster
	// than passive" invariant above. Combined with the per-tick probe
	// budget (maxProbesPerTick), this bounds probe load by both rate and
	// burst size.
	HealthProbeInterval = 30 * time.Second

	// The OPT-IN probe back-off (CORSA_PROBE_BACKOFF) delays ONLY the
	// Good→Questionable transition for proven-stable routes, leaving the Bad
	// (122s) and Dead (182s) thresholds UNCHANGED. Since Questionable is what
	// triggers active probing, a stable route is probed ~stableQuestionable
	// (90s) instead of 60s — but if it then dies silently it is still detected
	// at the SAME 122s/182s as any other route. The extension is deliberately
	// kept below HealthBadAfter (with one probe-interval of margin) so the
	// Good≻Questionable≻Bad ordering is never inverted — that is why this is an
	// additive +30s step rather than a multiplier on the whole timeline.
	//
	// A route earns the back-off once its StabilityStreak (incremented per
	// positive confirmation, reset on any negative evidence) reaches
	// stabilityStreakForBackoff, so a new or recently-flapping route keeps the
	// fast 60s timeline.
	stabilityStreakForBackoff   = 3
	stableQuestionableExtension = HealthProbeInterval // +30s → 90s, < 122s Bad
	// maxStabilityStreak caps the counter so it cannot grow unbounded.
	maxStabilityStreak = stabilityStreakForBackoff

	// HealthProbeTimeout — per-probe send-to-ack budget. After this
	// elapses without a probe_ack, the pair gets one
	// applyProbeFailure tick. Hooked up by PR 11.3.
	HealthProbeTimeout = 5 * time.Second
)

// Phase 3 PR 12.1 — reputation primitives. The constants below
// configure the local hop-ack success/failure book-keeping that
// docs/cluster-mesh/phase-3-multipath-reputation.md §4.1 describes.
// They are NOT operator-tunable knobs — Phase 3 §4.9 decision #3 keeps
// them as code constants until field telemetry shows a concrete need.
//
// The reliability term is deliberately small compared with the health-
// state penalty: docs/cluster-mesh/phase-3-multipath-reputation.md §4.1
// fixes the strict-tier ordering Good ≻ Questionable ≻ Bad as a
// release-blocking invariant — see also the doc-comment on
// scoreHealthQPenalty / scoreHealthBadPenalty in score.go. Bumping
// ReliabilityBonusMax above scoreSourceDirect would let a high-
// reliability Bad pair outrank a low-reliability Good pair and break
// every Bad-path recovery trigger downstream.
const (
	// ReliabilityWarmupSamples is the minimum number of observed
	// hop-ack attempts before CompositeScore folds in the reliability
	// term. Below the threshold a single failure could swing the EMA
	// across the whole [0, 1] range — too noisy to act on. Three
	// observations let the term carry useful signal without delaying
	// Bad-path detection past Phase 2's passive 122 s timeline.
	ReliabilityWarmupSamples uint64 = 3

	// ReliabilityEWMAFactor is the alpha of the per-pair success-ratio
	// EMA. Smaller than the RTT alpha (EWMASmoothingFactor = 0.3) on
	// purpose: relay reliability changes more slowly than network RTT,
	// and we want a single timeout to nudge the score, not dominate it.
	ReliabilityEWMAFactor = 0.2

	// BlackHoleThreshold is the number of consecutive hop-ack failures
	// at which applyHopAckFailure arms a cooldown. Five matches the
	// roadmap iter-2 §3e value the Phase 3 plan absorbs verbatim.
	BlackHoleThreshold = 5

	// BlackHoleCooldown is how long an armed pair stays out of
	// Lookup / Announceable selection after the threshold is crossed.
	// Two minutes is the original iter-2 §3e value: long enough for a
	// transient relay-side failure (queue backlog, GC pause) to clear,
	// short enough that a permanently-broken alternative is retried
	// soon. The PR 12.4 filter consumes CooldownUntil; Phase 3 §4.4
	// describes the integration. applyCooldownExpiryLocked clears the
	// arm when the window elapses (called from Table.TickHealth).
	BlackHoleCooldown = 2 * time.Minute

	// ReliabilityShapingMinAttempts is the per-(Identity, Uplink)
	// attempt count below which the pair is considered "new /
	// under-observed" by the Phase 3 PR 12.6 multi-path traffic
	// shaping helper. Until the counter reaches this floor the
	// shaping pass occasionally rotates traffic onto an alternative
	// uplink so the new pair's reliability statistics accumulate
	// against a real, non-degenerate sample size before
	// CompositeScore's reliability term starts to dominate the
	// ranking. Phase 3 §4.6 documents the chosen value (10).
	ReliabilityShapingMinAttempts uint64 = 10

	// ShapingRecentCooldownWindow bounds how long after a
	// black-hole cooldown was cleared the pair still counts as a
	// shaping candidate. Mirrors ReliabilityShapingMinAttempts on
	// the recovery side: a pair that JUST exited cooldown deserves
	// a brief grace period where alternative paths absorb some
	// traffic, so a recurrence of the failure pattern is detected
	// faster (the alternative also sees enough samples to
	// recompute its own reliability) and a degenerate single-
	// success-then-relapse cycle does not lock in the recovering
	// pair too quickly.
	ShapingRecentCooldownWindow = 1 * time.Minute

	// ShapingProbeRatio is the denominator of the Phase 3 PR 12.6
	// hint-driven shaping rotation. LookupForRelay reorders the
	// ranked result so a shaping candidate yields the top slot to
	// an alternative on every (hint % ShapingProbeRatio == 0) call.
	// Ratio 4 = roughly one in four relay forwards goes through
	// the alternative when a shaping candidate would otherwise
	// win, balancing reliability-sample accrual against the
	// per-message latency cost of routing through a longer path.
	// Phase 3 §4.6 records the choice; the constant is not
	// operator-tunable (Phase 3 §4.9 keeps reputation knobs out
	// of the operator surface until field telemetry demonstrates
	// a concrete need).
	ShapingProbeRatio uint64 = 4
)

// RouteHealthState tracks the active reachability of a
// (target identity, uplink peer) pair. Key shape is per-pair —
// adapted from the original iter-1.5 (Identity, Origin, NextHop)
// triple to match the per-uplink storage introduced in Phase 1
// (overview §4.1). Origin field was dropped from the routing storage
// dedup key, so per-Origin health was dropped along with it.
//
// Ownership: lives inside healthStore inside routing.Table under
// t.mu. Mutating methods (apply*) must be called with t.mu held in
// W mode; read methods may be called under R mode. The atomic.Pointer
// snapshot path used by fetchRouteHealth RPC reads via snapshotLocked
// under R mode and stores a deep copy outside the lock.
type RouteHealthState struct {
	// Identity is the destination identity this route targets.
	Identity domain.PeerIdentity

	// Uplink is the direct-peer fingerprint through which we learned
	// (or use) this route. The Phase 1 per-(Identity, Uplink) storage
	// guarantees each pair has at most one claim, so a single
	// RouteHealthState per pair is the natural shape.
	Uplink domain.PeerIdentity

	// Health is the current state-machine state. Transitions are
	// always paired with a TransitionAt update.
	Health RouteHealth

	// LastHopAck is the timeline reference used by applyIdleTick to
	// age the pair through Good→Questionable→Bad→Dead. It is
	// stamped at creation (ensureLocked) so the passive timeline has
	// a well-defined origin even before any confirmation arrives,
	// and re-stamped on every real confirmation (applyHopAck /
	// applyProbeAck(reachable=true)).
	//
	// Note: this field is NOT user-visible as a "last confirmation"
	// signal. The wire / RPC contract (docs/protocol/route_health.md
	// — last_hop_ack must be omitted for never-confirmed pairs) is
	// driven by the Confirmed flag below: fetchRouteHealth gates
	// emission of last_hop_ack on Confirmed==true, not on the
	// timestamp itself. Keeping the timer reference and the
	// "have we ever been confirmed?" semantic in separate fields
	// avoids the PR 11.15 / 11.16 regression where stamping
	// LastHopAck=zero for unconfirmed pairs forced applyIdleTick
	// into a TransitionAt-fallback that re-anchored on every
	// state transition.
	LastHopAck time.Time

	// Confirmed flips from false to true the first time the pair
	// receives positive evidence of reachability: a relay_hop_ack
	// (applyHopAck) or a route_probe_ack_v1 with reachable=true
	// (applyProbeAck). Negative evidence (probe timeouts,
	// probe_ack(reachable=false)) and passive idle ticks never
	// flip this back to false — once a confirmation is on record,
	// the historical fact persists for the lifetime of the entry.
	// Eviction (evictUplinkLocked) drops the whole state and
	// effectively resets Confirmed because a brand-new state is
	// created on the next admission.
	//
	// PR 11.15 P3 / 11.16: fetchRouteHealth uses this flag to
	// decide whether to emit last_hop_ack on the wire. Pairs
	// seeded by UpdateRoute (Announcement → Questionable) have
	// Confirmed=false until probe activity or real hop_ack
	// traffic arrives, so they correctly appear as never-confirmed
	// in operator tooling.
	Confirmed bool

	// LastProbe is the timestamp of the most recent probe
	// observation for this pair — either an ack arrival
	// (applyProbeAck) or a probe-failure tick (applyProbeFailure
	// for a timeout that elapsed without an ack). Send-side
	// timestamps live on the outstanding-probe registry
	// (probeRegistry.outstandingProbe.SentAt in
	// internal/core/node/routing_probe_loop.go) and are NOT
	// reflected here — the registry resolves them on ack ingest
	// to compute RTT, but the health state only records that an
	// observation arrived. fetchRouteHealth surfaces LastProbe so
	// operators can distinguish "no probe activity yet" (zero
	// value) from "probe activity is ongoing but no recent ack"
	// (non-zero but stale).
	LastProbe time.Time

	// ProbeFailures is the consecutive failure counter. Reset to
	// zero on any successful confirmation (hop_ack or
	// probe_ack(reachable=true)); incremented on probe timeout or
	// probe_ack(reachable=false). Crosses HealthProbeFailureThreshold
	// to force Bad even when LastHopAck is still recent.
	ProbeFailures int

	// StabilityStreak counts consecutive positive confirmations (hop_ack or
	// reachable probe_ack), capped at maxStabilityStreak. Once it reaches
	// stabilityStreakForBackoff (backoffEarned) the opt-in probe back-off
	// extends the Good→Questionable threshold by stableQuestionableExtension.
	// Reset to 0 on any negative evidence (probe timeout / reachable=false) or
	// on a transition to Bad/Dead, so a route that begins flapping immediately
	// loses its slow-age privilege.
	StabilityStreak int

	// RTT is the EWMA estimate of round-trip latency for this pair.
	// Updated by UpdateRTT (see score.go) using samples from hop_ack
	// turnaround (PR 11.2) and probe round-trip timing (PR 11.3).
	// Sender-measured, never trusts payload RTT supplied by the
	// remote — zero-trust budget §4.2.
	RTT time.Duration

	// TransitionAt is the wall-clock time of the most recent
	// state change. Used by RPC observability ("transitioned 12s
	// ago") and by debugging tooling.
	TransitionAt time.Time

	// Phase 3 PR 12.1 — reputation primitives. The fields below are
	// owned by the same RouteHealthState entry, mutated under the
	// same routing.Table.t.mu (W mode for apply*, R mode for read),
	// and snapshotted by the same healthStore.snapshotLocked path.
	// docs/cluster-mesh/phase-3-multipath-reputation.md §2.5 fixes
	// the locking decision; placing the fields here (rather than in
	// a parallel reputationStore) keeps Lookup-path reads atomic
	// without cross-mutex coordination.
	//
	// Production hop-ack call sites do NOT yet write into these
	// fields — PR 12.2 wires Table.MarkHopAck / Table.MarkHopFailure
	// to call applyHopAckSuccess / applyHopAckFailure. PR 12.4
	// consumes CooldownUntil as a Lookup / Announceable filter.
	// PR 12.7 surfaces the snapshot through fetchRouteReputation.

	// HopAckAttempts is the total number of hop-ack outcomes
	// observed for this pair — successes and failures combined.
	// Used as the cold-start gate for the reliability term in
	// CompositeScore (term ignored while
	// HopAckAttempts < ReliabilityWarmupSamples). Saturates
	// implicitly at math.MaxUint64; in practice traffic volume
	// per (Identity, Uplink) pair never approaches the limit.
	HopAckAttempts uint64

	// HopAckSuccesses is the subset of HopAckAttempts where
	// applyHopAckSuccess was called. Kept alongside Attempts so
	// observability can report the raw counter pair, not just the
	// derived ratio.
	HopAckSuccesses uint64

	// ReliabilityScore is the EWMA-smoothed hop-ack success ratio,
	// in [0, 1]. Updated by applyHopAckSuccess (outcome 1.0) and
	// applyHopAckFailure (outcome 0.0) at ReliabilityEWMAFactor.
	// The very first apply seeds the score with its outcome
	// directly to avoid biasing the long-run estimate toward 0.5.
	// CompositeScore consumes this via reliabilityBonus once the
	// pair is past the warmup gate.
	ReliabilityScore float64

	// ConsecutiveFailures counts hop-ack failures since the most
	// recent success. applyHopAckSuccess resets it to zero. When
	// the counter reaches BlackHoleThreshold the apply method arms
	// CooldownUntil — the local "this uplink is misbehaving"
	// signal that PR 12.4 turns into a Lookup-time filter.
	ConsecutiveFailures int

	// CooldownUntil is the wall-clock deadline before which the
	// pair is excluded from Lookup / Announceable / AnnounceTargetFor
	// (PR 12.4). Zero means "not armed". A success during the
	// cooldown window clears the arm immediately (see
	// applyHopAckSuccess); otherwise applyCooldownExpiryLocked
	// (called from Table.TickHealth) clears it once the deadline
	// elapses and gives the uplink a second chance.
	CooldownUntil time.Time

	// LastCooldownClearedAt records the wall-clock time the most
	// recent cooldown arm was cleared (by either organic success
	// via applyHopAckSuccess or by the TickHealth-driven
	// applyCooldownExpiryLocked sweep). Zero before any cooldown
	// has ever been armed.
	//
	// Used by the Phase 3 PR 12.6 multi-path traffic shaping
	// helper (isShapingCandidate) so a pair that JUST exited
	// cooldown still rotates partial traffic onto an alternative
	// uplink for ShapingRecentCooldownWindow afterwards. Without
	// this field a recovered-from-cooldown pair would immediately
	// resume monopolising relay forwards and a recurrence of the
	// failure pattern would take another full 5 hop-acks to
	// detect.
	LastCooldownClearedAt time.Time
}

// applyHopAck advances the state machine on relay_hop_ack reception
// for the (Identity, Uplink) pair. Always restores Good and resets
// ProbeFailures — hop_ack is the strongest passive confirmation.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyHopAck(now time.Time) {
	s.LastHopAck = now
	s.Confirmed = true // first real positive evidence; sticky thereafter.
	s.ProbeFailures = 0
	s.bumpStability()
	if s.Health != HealthGood {
		s.TransitionAt = now
		s.Health = HealthGood
	}
}

// bumpStability records one positive confirmation, capped at
// maxStabilityStreak. resetStability clears it on negative evidence. Both are
// no-ops for the back-off when CORSA_PROBE_BACKOFF is off (backoffEarned is
// only consulted by applyIdleTick when the Table passes backoff=true).
func (s *RouteHealthState) bumpStability() {
	if s.StabilityStreak < maxStabilityStreak {
		s.StabilityStreak++
	}
}

func (s *RouteHealthState) resetStability() { s.StabilityStreak = 0 }

// backoffEarned reports whether the route has confirmed itself stable enough
// to earn the delayed Good→Questionable transition. Pure; the caller decides
// whether the back-off is enabled at all.
func (s *RouteHealthState) backoffEarned() bool {
	return s.StabilityStreak >= stabilityStreakForBackoff
}

// applyProbeAck advances the state machine on route_probe_ack
// reception.
//
//   - reachable=true behaves like hop_ack: restore Good, reset
//     ProbeFailures, and refresh LastHopAck (the ack proves the
//     uplink is currently reachable for this target).
//   - reachable=false is negative evidence about the route, NOT
//     positive evidence about the uplink itself. ProbeFailures is
//     incremented; if it crosses HealthProbeFailureThreshold the
//     pair transitions to Bad. A pair in Bad is pulled back to
//     Questionable (active probe activity beats passive idle
//     bookkeeping). A pair in Dead STAYS Dead — review concern
//     P2#3: Lookup filters out Dead claims, and a "I can't reach
//     X through this uplink" answer should not silently re-enable
//     a route that was previously excluded from selection. Only
//     genuine reachability evidence (hop_ack or reachable=true
//     probe ack) can resurrect Dead.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyProbeAck(reachable bool, now time.Time) {
	s.LastProbe = now
	if reachable {
		s.LastHopAck = now
		s.Confirmed = true // first real positive evidence; sticky thereafter.
		s.ProbeFailures = 0
		s.bumpStability()
		if s.Health != HealthGood {
			s.TransitionAt = now
			s.Health = HealthGood
		}
		return
	}
	s.ProbeFailures++
	s.resetStability()
	if s.Health == HealthDead {
		// Dead is sticky against negative evidence. The
		// counter still bumps so observability sees the
		// failure, but the state machine does not regress.
		return
	}
	if s.Health == HealthBad {
		s.TransitionAt = now
		s.Health = HealthQuestionable
	}
	if s.ProbeFailures >= HealthProbeFailureThreshold && s.Health != HealthBad {
		s.TransitionAt = now
		s.Health = HealthBad
	}
}

// applyProbeFailure advances the state machine on probe timeout — no
// route_probe_ack arrived within HealthProbeTimeout. Behaves the same
// as applyProbeAck(reachable=false): increments ProbeFailures and
// bumps LastProbe so observability can distinguish "no probe activity
// has ever been observed" (LastProbe zero) from "probes are being
// fired but none acks back" (LastProbe non-zero, no LastHopAck
// refresh). Dead remains sticky — see applyProbeAck's doc-comment for
// the rationale.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyProbeFailure(now time.Time) {
	s.LastProbe = now
	s.ProbeFailures++
	s.resetStability()
	if s.ProbeFailures >= HealthProbeFailureThreshold && s.Health != HealthBad && s.Health != HealthDead {
		s.TransitionAt = now
		s.Health = HealthBad
	}
}

// applyIdleTick performs the periodic passive-timeline transition:
// 60 s of hop_ack idle promotes Good→Questionable, 122 s promotes to
// Bad, and 182 s promotes to Dead — but the Dead step applies only to
// pairs that have been Confirmed at least once. A never-confirmed pair
// (no hop_ack / reachable probe_ack ever observed) caps at Bad, because
// passive idle on a pair with no confirmation channel is probe
// starvation, not evidence the route died; promoting it to Dead would
// emit a false withdrawal and trigger the re-announce reseed flap. See
// the Dead branch below for the full rationale. Probe-driven transitions
// are handled separately by applyProbeAck / applyProbeFailure.
//
// The transitions are evaluated in reverse-severity order (Dead first)
// because a single tick may have to skip Bad if the pair was sitting
// in Questionable for far too long without any tick in between.
//
// LastHopAck is the timeline reference and is ALWAYS non-zero for
// any state created by ensureLocked — the placeholder is stamped
// at creation time so the passive timeline measures "age since
// creation" until a real confirmation arrives. The user-visible
// "have we ever been confirmed?" distinction is carried by the
// separate Confirmed flag (see RouteHealthState doc-comment), so
// the RPC layer can omit last_hop_ack for never-confirmed pairs
// while the timer logic here stays simple and correct.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyIdleTick(now time.Time, backoff bool) {
	idle := now.Sub(s.LastHopAck)
	// Probe back-off (opt-in): a proven-stable route delays ONLY its
	// Good→Questionable transition by one probe interval (60s → 90s), so it is
	// actively probed less often. Bad (122s) and Dead (182s) are UNCHANGED, so
	// a stable route that dies silently is detected just as fast as any other
	// — and 90s stays below HealthBadAfter with a full probe-interval of margin
	// so the Good≻Questionable≻Bad ordering is never inverted.
	questionableAfter := HealthQuestionableAfter
	if backoff && s.backoffEarned() {
		questionableAfter += stableQuestionableExtension
	}
	switch {
	case idle >= HealthDeadAfter && s.Health != HealthDead:
		// Passive idle may assert Dead only for a pair that once held real
		// reachability evidence (Confirmed) and then went silent. A pair that
		// was NEVER confirmed has no hop_ack / probe_ack(reachable) channel, so
		// 182s of idle is not proof the route died — only proof we never
		// managed to confirm it locally. At mesh scale the probe loop cannot
		// cover every transit pair within the timeline, so most unconfirmed
		// transit pairs would otherwise hit Dead purely from probe starvation.
		// Marking such a pair Dead is doubly harmful: projectIdentityLocked
		// drops Dead transit from the announce projection (a false withdrawal
		// the mesh immediately contradicts), and the next accepted re-announce
		// evicts+reseeds it to Questionable (table_mutation.go), restarting the
		// 182s clock — an infinite Dead<->Questionable flap synced to the
		// announce cadence that drives the JournalCauseHealthAging churn and its
		// downstream announce/alloc cost. Cap unconfirmed pairs at Bad: still
		// penalised in Lookup (Bad score tier) and still emitted on the wire,
		// but invisible to healthProjectionSig, so no journal flip and no
		// withdrawal. Genuinely-gone routes are reclaimed by TTL expiry. Once a
		// pair is confirmed the flag is sticky, so a real route that later dies
		// still ages to Dead on the normal timeline.
		if !s.Confirmed {
			if s.Health != HealthBad {
				s.TransitionAt = now
				s.Health = HealthBad
				s.resetStability()
			}
			return
		}
		s.TransitionAt = now
		s.Health = HealthDead
		s.resetStability()
	case idle >= HealthBadAfter && s.Health != HealthBad && s.Health != HealthDead:
		s.TransitionAt = now
		s.Health = HealthBad
		s.resetStability()
	case idle >= questionableAfter && s.Health == HealthGood:
		s.TransitionAt = now
		s.Health = HealthQuestionable
	}
}

// applyHopAckSuccess records a positive hop-ack outcome on the
// reputation primitives. Distinct from applyHopAck (Phase 2 health
// state machine) by design: the state-machine side and the reputation
// side are independent surfaces over the same pair so a future PR
// could change one without disturbing the other. PR 12.2 wires both
// at the Table level (MarkHopAck / ConfirmHopAck call applyHopAck
// then applyHopAckSuccess inside one t.mu.Lock).
//
// Cold-start contract: the very first observation seeds the EMA with
// its outcome verbatim — for a success that means ReliabilityScore=1.
// This matches UpdateRTT's documented cold-start behaviour (score.go)
// and avoids the "every fresh pair starts at 0.5" bias that an
// uninitialised EMA would otherwise introduce.
//
// Side effects beyond the EMA: HopAckAttempts and HopAckSuccesses
// increment; ConsecutiveFailures resets to zero; an armed
// CooldownUntil is cleared (a late hop-ack is positive evidence and
// must immediately restore selectability — see §4.1 contract in
// docs/cluster-mesh/phase-3-multipath-reputation.md).
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyHopAckSuccess(now time.Time) {
	s.HopAckAttempts++
	s.HopAckSuccesses++
	if s.HopAckAttempts == 1 {
		s.ReliabilityScore = 1.0
	} else {
		s.ReliabilityScore = (1-ReliabilityEWMAFactor)*s.ReliabilityScore + ReliabilityEWMAFactor*1.0
	}
	if s.ReliabilityScore > 1.0 {
		s.ReliabilityScore = 1.0
	}
	s.ConsecutiveFailures = 0
	// Phase 3 PR 12.6 — record the cooldown clear so the
	// shaping helper keeps the pair on a partial-traffic regime
	// for ShapingRecentCooldownWindow even after a successful
	// recovery. We stamp ONLY when we actually clear an armed
	// cooldown so a steady-state success path (no cooldown
	// ever armed) does not falsely trigger shaping.
	if !s.CooldownUntil.IsZero() {
		s.LastCooldownClearedAt = now
	}
	s.CooldownUntil = time.Time{}
}

// applyHopAckFailure records a negative hop-ack outcome on the
// reputation primitives. Symmetric to applyHopAckSuccess on the
// counter / EMA side, but only HopAckAttempts bumps —
// HopAckSuccesses stays put so the observable success ratio is
// honest. The 0.0 outcome blends into the EMA at
// ReliabilityEWMAFactor, the consecutive-failure counter
// increments, and on the BlackHoleThreshold-th consecutive failure
// the apply method arms CooldownUntil = now + BlackHoleCooldown so
// PR 12.4's Lookup filter can suppress the pair until the window
// elapses.
//
// Cold-start contract: a first-ever apply call seeds ReliabilityScore
// at its outcome (0.0 for a failure). This is symmetric with
// applyHopAckSuccess's first-call branch and is documented in §4.1.
//
// The apply method does NOT clear or shorten an already-armed
// cooldown — repeated failures inside the window keep the deadline
// where the threshold crossing left it. A success (organic late
// hop-ack landing during the window) is the only event that lifts
// the arm early.
//
// Caller must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applyHopAckFailure(now time.Time) {
	s.HopAckAttempts++
	if s.HopAckAttempts == 1 {
		s.ReliabilityScore = 0.0
	} else {
		s.ReliabilityScore = (1-ReliabilityEWMAFactor)*s.ReliabilityScore + ReliabilityEWMAFactor*0.0
	}
	if s.ReliabilityScore < 0.0 {
		s.ReliabilityScore = 0.0
	}
	s.ConsecutiveFailures++
	if s.ConsecutiveFailures >= BlackHoleThreshold && s.CooldownUntil.IsZero() {
		s.CooldownUntil = now.Add(BlackHoleCooldown)
	}
}

// applyCooldownExpiryLocked is the per-tick helper Table.TickHealth
// (Phase 3 PR 12.4) calls on every tracked pair to release a
// black-hole cooldown whose deadline has elapsed. Clearing the
// CooldownUntil deadline AND the ConsecutiveFailures counter gives
// the uplink a clean slate — the next observed hop-ack (success or
// failure) starts from "no consecutive failures yet" so a single
// stray retry timeout does NOT immediately re-arm the cooldown.
//
// The "Locked" suffix mirrors the rest of healthStore's contract:
// callers must hold the owning routing.Table's t.mu in W mode. The
// method is a no-op for pairs without an armed cooldown (zero
// CooldownUntil) or whose deadline is still in the future.
func (s *RouteHealthState) applyCooldownExpiryLocked(now time.Time) {
	if s.CooldownUntil.IsZero() {
		return
	}
	if now.Before(s.CooldownUntil) {
		return
	}
	s.CooldownUntil = time.Time{}
	s.ConsecutiveFailures = 0
	// Phase 3 PR 12.6 — stamp the clear so the shaping helper
	// keeps the pair on partial-traffic mode briefly even after
	// the cooldown auto-released. Mirror of the equivalent stamp
	// on applyHopAckSuccess's organic-recovery branch.
	s.LastCooldownClearedAt = now
}

// applySessionEstablishedLocked clears the black-hole braking state
// (ConsecutiveFailures counter and an armed CooldownUntil) when a
// direct session to the uplink has just been (re-)established and
// the peer's identity confirmed by the transport handshake.
//
// Rationale: hop-ack failures accumulated while the peer was down
// (typically inside the withdrawal grace window, when the direct
// route is still in the table and relay attempts keep timing out)
// arm the black-hole cooldown for the (peer, peer) pair. The
// cooldown is empirical "cannot deliver" evidence — but a freshly
// completed handshake is strictly newer evidence of the opposite,
// equivalent in strength to the organic late hop-ack that
// applyHopAckSuccess treats as an immediate cooldown lift. Without
// this clear, the reconnected direct peer stays hidden from
// Lookup / fetchRouteLookup for up to BlackHoleCooldown even though
// the session is alive ("peer online, route count=0").
//
// What is deliberately NOT touched: ReliabilityScore / HopAckAttempts
// / HopAckSuccesses stay as-is — the EMA is honest history and the
// session handshake is not a hop-ack outcome; and Health-state-machine
// fields are owned by AddDirectPeer's existing reset. LastCooldownClearedAt
// is stamped only when an armed cooldown was actually lifted, so the
// PR 12.6 partial-traffic shaping grace engages exactly as it does on
// the organic-recovery and expiry-sweep paths.
//
// Returns true when any field changed (caller marks the snapshot
// dirty). Callers must hold the owning routing.Table's t.mu in W mode.
func (s *RouteHealthState) applySessionEstablishedLocked(now time.Time) bool {
	changed := false
	if s.ConsecutiveFailures != 0 {
		s.ConsecutiveFailures = 0
		changed = true
	}
	if !s.CooldownUntil.IsZero() {
		s.CooldownUntil = time.Time{}
		s.LastCooldownClearedAt = now
		changed = true
	}
	return changed
}

// isShapingCandidate reports whether the (Identity, Uplink) pair
// should still be considered for multi-path traffic shaping by
// the Phase 3 PR 12.6 LookupForRelay rotation. Two conditions
// qualify a pair as a shaping candidate; either is sufficient:
//
//   - Warmup: HopAckAttempts is below ReliabilityShapingMinAttempts.
//     The reliability EMA needs more samples before its signal
//     becomes ranking-grade, so the shaping pass rotates traffic
//     onto alternative uplinks to accelerate sample accrual.
//   - Post-cooldown grace: LastCooldownClearedAt is non-zero and
//     `now` is within ShapingRecentCooldownWindow of it. A pair
//     that just exited cooldown receives a brief partial-traffic
//     regime so a recurrence of the failure pattern is detected
//     faster (the alternative uplink also builds up enough
//     samples to be reliably preferred if the recovered pair
//     misbehaves again).
//
// Returns false for nil state — the caller (LookupForRelay) treats
// "no health entry tracked yet" as "no shaping needed" because the
// nil-health CompositeScore branch already produces the safe by-
// hops fallback ranking.
//
// Pure: no clock, no I/O, no mutation. Safe to call under either
// t.mu mode (callers hold R for Lookup-path reads).
func isShapingCandidate(state *RouteHealthState, now time.Time) bool {
	if state == nil {
		return false
	}
	if state.HopAckAttempts < ReliabilityShapingMinAttempts {
		return true
	}
	if !state.LastCooldownClearedAt.IsZero() && now.Sub(state.LastCooldownClearedAt) <= ShapingRecentCooldownWindow {
		return true
	}
	return false
}

// isShapingAlternativeAcceptable reports whether a route's health makes
// it a valid target to promote OVER a shaping candidate in
// Table.LookupForRelay. Two conditions must hold:
//
//   - it must NOT itself be a shaping candidate — swapping one warmup /
//     just-cooled-down pair for another only moves the sample gap
//     around without accelerating reliability accrual;
//   - it must be in a healthy tier (HealthGood or HealthQuestionable).
//     A HealthBad or HealthDead alternative must never win the slot:
//     shaping exists to gather samples on a merely-under-observed path,
//     not to divert traffic onto a known-worse one. Lookup already
//     drops cooled-down pairs, but a fully-observed Bad pair is NOT a
//     shaping candidate and a Direct HealthDead pair survives Lookup's
//     direct exemption, so without this tier gate either could be
//     promoted over a healthy warmup candidate (Phase 3 §4.6:
//     "Bad / Dead / cooled-down alternatives never chosen over a
//     shaping candidate — the alternative must be Good / Questionable").
//
// A nil state (no health recorded yet) carries no negative signal and
// is treated as acceptable, mirroring CompositeScore's nil-health
// handling.
func isShapingAlternativeAcceptable(state *RouteHealthState, now time.Time) bool {
	if isShapingCandidate(state, now) {
		return false
	}
	if state == nil {
		return true
	}
	return state.Health == HealthGood || state.Health == HealthQuestionable
}

// healthKey identifies a tracked (Identity, Uplink) pair.
type healthKey struct {
	Identity domain.PeerIdentity
	Uplink   domain.PeerIdentity
}

// HealthProbeTarget is a lean (Identity, Uplink) pair handed to the probe
// scheduler. It exists so probeTick can enumerate the Questionable pairs
// it needs to probe without deep-copying the entire RouteHealthState set
// (see healthStore.questionableTargetsLocked).
type HealthProbeTarget struct {
	Identity domain.PeerIdentity
	Uplink   domain.PeerIdentity
}

// healthStore owns the RouteHealthState map. It is a leaf data
// structure with no own mutex — locking is provided by the owning
// routing.Table.t.mu. The "Locked" suffix on every method documents
// the lock-ownership contract: callers must hold t.mu in the
// indicated mode before invoking.
type healthStore struct {
	// states maps (Identity, Uplink) → state. Owned by the parent
	// Table; never accessed without t.mu held.
	states map[healthKey]*RouteHealthState
}

// newHealthStore returns an empty health store ready for use as a
// Table leaf.
func newHealthStore() *healthStore {
	return &healthStore{
		states: make(map[healthKey]*RouteHealthState),
	}
}

// isDeadLocked reports whether the (Identity, Uplink) pair is
// currently marked HealthDead. Returns false for absent entries
// (cold pairs / never-seen claims) and for any non-Dead label.
//
// Backs the Phase 2 announce-side suppression contract on the
// HealthDead constant: a Dead pair is filtered out of
// AnnounceProjectionFor / AnnounceableFor so locally-dead routes
// are no longer advertised to neighbours. The Lookup-side filter
// (table_lookup.go::Lookup) and this announce-side filter share
// the same per-pair signal — both consult the health store under
// t.mu without taking the lock themselves; callers must already
// hold t.mu in R mode.
//
// Returns false (not a Dead signal) when the parent healthStore
// is nil — defensive check that mirrors the lookupHealthLocked
// pattern used by the Lookup path.
func (s *healthStore) isDeadLocked(identity, uplink PeerIdentity) bool {
	if s == nil {
		return false
	}
	state := s.states[healthKey{Identity: identity, Uplink: uplink}]
	return state != nil && state.Health == HealthDead
}

// isCooledDownLocked reports whether the (Identity, Uplink) pair
// is currently inside an armed black-hole cooldown window — that
// is, CooldownUntil is non-zero AND `now` is before that deadline.
// Returns false for absent entries (cold pairs / never-seen
// claims) and for entries whose cooldown has expired but TickHealth
// has not yet cleared the field.
//
// Phase 3 PR 12.4 contract: the cooldown filter is applied
// symmetrically on Lookup (table_lookup.go::Lookup), the
// per-target picker (AnnounceTargetFor), and the announce
// projection (AnnounceableFor / AnnounceProjectionFor). Unlike
// the Dead filter, there is NO Direct exemption — a Direct pair
// that reliably misses hop_acks IS a black hole even though the
// underlying session is alive, and suppressing it from Lookup
// matches the "this peer can't actually deliver" signal that
// the 5 consecutive failures already recorded on the reputation
// surface. The release-blocking invariant is pinned by
// TestBlackHole_DirectClaimNotExempt.
//
// The `now` parameter exists because TickHealth's
// applyCooldownExpiryLocked sweep runs at HealthProbeInterval
// (30 s) cadence, so there can be up to one tick of lag between
// "CooldownUntil elapsed" and "CooldownUntil field cleared".
// Reading the live `now` against the stored deadline closes that
// gap on every Lookup / Announce projection without needing the
// sweep to land first.
//
// Caller must hold t.mu in R mode; this helper performs only a
// map read and a time comparison.
func (s *healthStore) isCooledDownLocked(identity, uplink PeerIdentity, now time.Time) bool {
	if s == nil {
		return false
	}
	state := s.states[healthKey{Identity: identity, Uplink: uplink}]
	if state == nil {
		return false
	}
	if state.CooldownUntil.IsZero() {
		return false
	}
	return now.Before(state.CooldownUntil)
}

// getLocked returns the state for the pair, or nil when no state is
// tracked yet. Callers must hold t.mu in R mode.
func (s *healthStore) getLocked(identity, uplink domain.PeerIdentity) *RouteHealthState {
	return s.states[healthKey{Identity: identity, Uplink: uplink}]
}

// ensureLocked returns the state for the pair, creating a fresh
// HealthGood entry stamped at `now` when none exists. Used by writer
// paths (hop_ack ingest, probe ack ingest, UpdateRoute admission)
// that need to upsert without a separate exists-check. Callers
// must hold t.mu in W mode.
//
// The fresh entry stamps LastHopAck=now so the passive-timeline
// machinery (applyIdleTick) has a well-defined idle origin. The
// "have we ever been really confirmed?" distinction is carried by
// the separate Confirmed flag: ensureLocked leaves it false, and
// only applyHopAck / applyProbeAck(reachable=true) flip it true.
// Wire / RPC layers (fetchRouteHealth — see
// internal/core/rpc/routing_commands.go) gate emission of
// last_hop_ack on Confirmed==true so a placeholder created by
// UpdateRoute (Announcement → Questionable, Confirmed=false) is
// correctly reported as never-confirmed.
//
// PR 11.15 P3 / 11.16 (regression history): an earlier iteration
// tried to leave LastHopAck=zero on fresh ensure and fall back to
// TransitionAt inside applyIdleTick. That broke the passive
// timeline because TransitionAt is rewritten on every transition,
// so a never-confirmed pair would re-anchor at each step and
// never escalate Questionable → Bad on the documented timeline.
// The Confirmed-flag split keeps applyIdleTick's reference stable
// (LastHopAck is monotonic-or-stays) and moves the user-visible
// "confirmed?" question to its own field.
func (s *healthStore) ensureLocked(identity, uplink domain.PeerIdentity, now time.Time) *RouteHealthState {
	k := healthKey{Identity: identity, Uplink: uplink}
	if state, ok := s.states[k]; ok {
		return state
	}
	state := &RouteHealthState{
		Identity:     identity,
		Uplink:       uplink,
		Health:       HealthGood,
		LastHopAck:   now,
		TransitionAt: now,
		// Confirmed left at false — fresh placeholder has no
		// positive evidence yet. applyHopAck /
		// applyProbeAck(reachable=true) flip it true on the
		// first real confirmation.
	}
	s.states[k] = state
	return state
}

// evictUplinkLocked drops the state for the (Identity, Uplink) pair.
// Invoked when the routeStore drops the corresponding UplinkClaim
// (single uplink withdrew its route to Identity, others remain).
// Callers must hold t.mu in W mode.
func (s *healthStore) evictUplinkLocked(identity, uplink domain.PeerIdentity) {
	delete(s.states, healthKey{Identity: identity, Uplink: uplink})
}

// evictIdentityLocked drops every state for the given identity.
// Invoked when the routeStore drops the identity entirely (last
// uplink withdrew). Iterates the full map; the operation is rare
// enough (only happens on full identity teardown) that O(N) cost is
// acceptable. Callers must hold t.mu in W mode.
func (s *healthStore) evictIdentityLocked(identity domain.PeerIdentity) {
	for k := range s.states {
		if k.Identity == identity {
			delete(s.states, k)
		}
	}
}

// lenLocked returns the number of tracked pairs. Test- and
// observability-only.
func (s *healthStore) lenLocked() int {
	return len(s.states)
}

// snapshotLocked returns a deep copy of every tracked state.
// Backs Table.HealthSnapshot, which is called synchronously per
// fetchRouteHealth AND fetchRouteLookup RPC request (the latter joined
// this path once Snapshot.Health was narrowed to the Dead∪cooled subset;
// see Table.HealthSnapshot's doc-comment and docs/locking.md for the
// locking contract).
// There is no atomic.Pointer cache between this method and the
// RPC handler — the handler takes t.mu.RLock() on each request,
// walks the healthStore here, and releases. Returns nil for an
// empty store so callers can compare to nil cheaply.
//
// Callers must hold t.mu in R mode.
func (s *healthStore) snapshotLocked() []RouteHealthState {
	if len(s.states) == 0 {
		return nil
	}
	out := make([]RouteHealthState, 0, len(s.states))
	for _, state := range s.states {
		out = append(out, *state)
	}
	return out
}

// questionableTargetsLocked returns the (Identity, Uplink) pairs whose
// state is currently HealthQuestionable — the only states the probe
// scheduler acts on. It deliberately does NOT deep-copy the full
// RouteHealthState set the way snapshotLocked does: on a dense node the
// health store holds hundreds of thousands of pairs, almost all Good,
// and probeTick ran every HealthProbeInterval copying the entire set
// just to pick out the Questionable handful — a dominant alloc_space
// source. Allocating only for the Questionable subset (typically a few
// pairs) removes that churn. Returns nil when none are Questionable.
//
// Callers must hold t.mu in R mode.
func (s *healthStore) questionableTargetsLocked() []HealthProbeTarget {
	var out []HealthProbeTarget
	for _, state := range s.states {
		if state.Health == HealthQuestionable {
			out = append(out, HealthProbeTarget{Identity: state.Identity, Uplink: state.Uplink})
		}
	}
	return out
}

// routingRelevantStatesLocked returns only the health states the PUBLISHED
// routing snapshot's consumers act on: Dead pairs (Snapshot.BestRoute excludes
// them and fetchRouteSummary's reachability filter drops them) and pairs inside
// an armed black-hole cooldown (fetchRouteSummary excludes those too). The
// healthy majority is omitted — Snapshot.Health is a routing-relevance view,
// NOT a full dump.
//
// This is what stops SnapshotIncremental from deep-copying the entire health
// set (hundreds of thousands of Good pairs) into every 2s rebuild — the
// dominant alloc_space source on a dense node. The Dead/cooled subset is a
// handful of pairs, so the copy is cheap. Consumers that need the complete
// per-pair tiers (the fetchRouteLookup CompositeScore diagnostic, and
// fetchRouteHealth) call Table.HealthSnapshot directly at request time
// instead of reading Snapshot.Health. Returns nil when nothing is
// routing-relevant.
//
// Callers must hold t.mu in R mode.
func (s *healthStore) routingRelevantStatesLocked() []RouteHealthState {
	var out []RouteHealthState
	for _, state := range s.states {
		if state.Health == HealthDead || !state.CooldownUntil.IsZero() {
			out = append(out, *state)
		}
	}
	return out
}
