package routing

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// FlapDetector tracks per-peer disconnect history and applies the
// flap-detection / hold-down state machine. It is consumed exclusively
// through *Table — public observability (FlapSnapshot,
// RecordSuccessfulRouteAdd) and the write-side mutators that arm /
// query / clear hold-down (RemoveDirectPeer, AddDirectPeer, TickTTL)
// all go through thin Table wrappers that hold t.mu before calling
// the *Locked methods below.
//
// The detector has NO mutex of its own. Every *Locked method requires
// the owning Table's t.mu to be held in the documented mode (writer
// or reader). This keeps the lock contract identical to the
// pre-FlapDetector state where flap fields lived directly on Table:
// a single t.mu acquisition protects both the routes map and the
// flap state, so Table.Snapshot / Table.TickTTL / Table.RemoveDirectPeer
// can observe / mutate them as one consistent unit. Splitting flap
// onto its own mutex would require auditing every caller for nested
// locking — not a Phase 1 R2 goal; if a future phase needs that, do
// it as a separate refactor.
//
// The detector also does not own a clock. Callers pass the wall-clock
// `now` they already read from t.clock() on entry to t.mu, which
// guarantees the routes-pass and flap-pass of a single Table operation
// agree on the timeline.
type FlapDetector struct {
	// state maps peerIdentity → its tracked disconnect history /
	// hold-down state. Mutated only with the owning Table's t.mu
	// held (writer). Read with t.mu (reader or writer).
	state map[PeerIdentity]*peerFlapState

	// window is the time window for counting disconnect events.
	// A peer that disconnects threshold+ times within window enters
	// hold-down.
	window time.Duration

	// threshold is the number of disconnects within window that arms
	// hold-down.
	threshold int

	// holdDown is the base hold-down duration. Successive flap-bursts
	// grow it via holdDownDurationForBurst exponential backoff,
	// capped at MaxHoldDownDuration.
	holdDown time.Duration

	// penalizedTTL is the shortened TTL applied to routes created
	// during hold-down. Set via WithPenalizedTTL. Currently unused on
	// the production write path — direct routes use ExpiresAt=zero
	// regardless of hold-down state, the only effect of hold-down on
	// AddDirectPeer is the Penalized flag in AddDirectPeerResult —
	// but the field is preserved for API compatibility with callers
	// that read it via WithPenalizedTTL and any future use of the
	// dampening value on transit routes.
	penalizedTTL time.Duration

	// seqVelocities tracks per-Identity outbound-SeqNo advance
	// velocity for Phase 1 P2 (SeqNo flap cap). Each entry is a
	// sliding-window ring of advance timestamps; when the count
	// inside seqAdvanceWindow exceeds maxSeqAdvancePerWindow the
	// entry's holdUntil is armed to now+seqHoldDown, which
	// AnnounceProjectionFor consults via isInSeqHoldDownLocked to
	// suppress wire emit for the entire window.
	//
	// Lives on the same FlapDetector struct (and under the same
	// Table.t.mu writer-lock contract) as the connection-flap
	// `state` map but feeds an INDEPENDENT decision tree: connection
	// flap suppresses AddDirectPeer / RemoveDirectPeer churn, SeqNo
	// flap suppresses per-Identity wire emit. The two maps are keyed
	// differently (connection flap keys on the peer that disconnected;
	// SeqNo flap keys on the destination Identity whose outbound
	// timeline is advancing) and never share entries even when both
	// dimensions happen to use the same PeerIdentity value.
	//
	// Lazy-allocated: empty map shared across all newFlapDetector()
	// instances costs nothing until the first advance lands. Mutated
	// only under t.mu (writer).
	seqVelocities map[PeerIdentity]*seqVelocity

	// seqAdvanceWindow is the sliding-window length for the SeqNo
	// flap-cap detector. Outbound-SeqNo advances older than
	// `now - seqAdvanceWindow` are trimmed on every record /
	// clearExpired pass. Default DefaultSeqAdvanceWindow (5 min);
	// operator override via WithSeqAdvanceWindow.
	seqAdvanceWindow time.Duration

	// maxSeqAdvancePerWindow is the threshold at which a single
	// Identity's velocity tracker engages hold-down. Default
	// DefaultMaxSeqAdvancePerWindow (10); operator override via
	// WithMaxSeqAdvancePerWindow. Zero (or negative) disables the
	// cap entirely — recordSeqAdvanceLocked short-circuits and
	// isInSeqHoldDownLocked returns false unconditionally.
	maxSeqAdvancePerWindow int

	// seqHoldDown is the duration for which a single Identity's
	// wire emit is suppressed once the velocity threshold is
	// crossed. Default DefaultSeqHoldDownDuration (DefaultTTL/2).
	// NOT operator/env-tunable: the value preserves the refresh-
	// interval invariant EffectiveForcedFullSyncInterval <=
	// DefaultTTL/2, so a hold-down is always at least as long as
	// the receiver-side TTL window the suppression must outlast.
	// The package exposes `WithSeqHoldDownDuration(d)` as a
	// Table-option strictly for regression coverage — there is no
	// corresponding env var (`config.Node` doesn't carry the
	// knob), so production Services constructed via config.Default()
	// always run the derived default. If a future operator-facing
	// override is genuinely needed, route it through config first
	// so the invariant check has a single place to live.
	seqHoldDown time.Duration

	// badHops tracks per-Identity ACCEPTED fast-invalidation
	// recidivism for the bad-hops hysteresis (see the constant block
	// in types.go for the full rationale). Keyed by destination
	// Identity — the same keying as seqVelocities, and a third
	// independent decision tree on this detector: connection flap
	// dampens direct-route churn, SeqNo flap dampens wire emit,
	// bad-hops hysteresis dampens P3 tombstone churn. Lazy-allocated;
	// mutated only under the owning Table's t.mu (writer).
	badHops map[PeerIdentity]*badHopsState

	// badHopsWindow / maxBadHopsPerWindow configure the recidivism
	// detector: more than maxBadHopsPerWindow accepted
	// fast-invalidations for one Identity within badHopsWindow arms
	// hold-down. Either knob <= 0 disables the hysteresis (the P3
	// guard itself keeps working per-event). Defaults
	// DefaultBadHopsWindow / DefaultMaxBadHopsPerWindow; test
	// overrides via WithBadHopsWindow / WithMaxBadHopsPerWindow.
	badHopsWindow       time.Duration
	maxBadHopsPerWindow int

	// badHopsHoldDownBase is the first-strike hold-down duration;
	// re-arms within DefaultBadHopsRecidivismWindow double it up to
	// DefaultBadHopsHoldDownMax. Test override via
	// WithBadHopsHoldDown.
	badHopsHoldDownBase time.Duration
}

// badHopsState is the per-Identity record for the bad-hops
// hysteresis: a sliding ring of accepted fast-invalidation
// timestamps plus the hold-down / recidivism bookkeeping.
type badHopsState struct {
	// events is the ring of ACCEPTED fast-invalidation timestamps,
	// trimmed to the trailing badHopsWindow on every record pass —
	// bounded by maxBadHopsPerWindow+1 in steady state.
	events []time.Time

	// holdUntil is non-zero while the Identity is in bad-hops
	// hold-down; ApplyUpdate's P3 branch drops bad-hops claims for
	// the Identity without touching storage while now < holdUntil.
	holdUntil time.Time

	// trigger is the wire-frame Origin observed when hold-down was
	// most recently armed — surfaced in the engage/release logs so
	// operators can locate the looping segment.
	trigger PeerIdentity

	// strikes counts consecutive arms within the recidivism window;
	// drives the exponential hold-down escalation. Reset to 1 when
	// the Identity stays quiet past DefaultBadHopsRecidivismWindow.
	strikes int

	// lastArmed is the wall-clock of the most recent arm — the
	// recidivism anchor.
	lastArmed time.Time
}

// seqVelocity records the per-Identity outbound-SeqNo advance
// timeline used by the Phase 1 P2 SeqNo flap cap. One instance per
// Identity that has issued at least one outbound advance.
type seqVelocity struct {
	// advances is a ring of accepted outbound-SeqNo advance
	// timestamps for this Identity. Trimmed to the trailing
	// seqAdvanceWindow on every record / clearExpired pass so the
	// slice stays bounded by maxSeqAdvancePerWindow+epsilon under
	// steady load.
	advances []time.Time

	// holdUntil is non-zero while the Identity is in hold-down.
	// AnnounceProjectionFor skips Identities for which
	// now.Before(holdUntil) is true; ApplyUpdate's outbound-
	// projected-SeqNo guard also reads this to reject incoming
	// announces that would advance outbound past observed+1.
	holdUntil time.Time

	// holdTrigger names the wire-frame Origin most recently
	// observed at the moment hold-down was armed. Surfaced in the
	// engage warn-log so operators can identify the upstream
	// lineage driving the storm. Cleared together with holdUntil
	// on hold-down expiry.
	holdTrigger PeerIdentity
}

// newFlapDetector constructs a detector with package defaults. Options
// applied to the owning Table (WithFlapWindow / WithFlapThreshold /
// WithHoldDownDuration / WithPenalizedTTL) mutate the returned
// detector's fields directly.
func newFlapDetector() *FlapDetector {
	return &FlapDetector{
		state:         make(map[PeerIdentity]*peerFlapState),
		window:        DefaultFlapWindow,
		threshold:     DefaultFlapThreshold,
		holdDown:      DefaultHoldDownDuration,
		penalizedTTL:  DefaultPenalizedTTL,
		seqVelocities: make(map[PeerIdentity]*seqVelocity),
		// Phase 1 P2 defaults: the SeqNo flap cap is DISABLED on the
		// bare Table (maxSeqAdvancePerWindow=0 — recordSeqAdvanceLocked
		// short-circuits). This mirrors the maxNextHopsPerOrigin
		// pattern: bare Table built by unit tests and any caller that
		// wires a Table directly without going through config keeps
		// the pre-cap behaviour deterministically; production Service
		// constructed via config.Default() activates the cap at
		// DefaultMaxSeqAdvancePerWindow (10) via the env reader plus
		// WithMaxSeqAdvancePerWindow.
		//
		// seqAdvanceWindow and seqHoldDown are seeded with the
		// production defaults even when the cap itself is disabled —
		// callers that enable the cap mid-test via
		// WithMaxSeqAdvancePerWindow only need to override the value
		// they care about, not the three together.
		seqAdvanceWindow:       DefaultSeqAdvanceWindow,
		maxSeqAdvancePerWindow: 0,
		seqHoldDown:            DefaultSeqHoldDownDuration,
		// Bad-hops hysteresis defaults are ACTIVE (unlike the SeqNo
		// flap cap, which defaults disabled): the hysteresis is
		// reachable only from the P3 fast-invalidation branch, and
		// that branch is itself gated on maxSaneHops > 0 — disabled
		// on the bare Table. A Table that opted into fast
		// invalidation (WithMaxSaneHops) gets the recidivism damping
		// with it; the per-event P3 tests that need raw behaviour
		// stay under the threshold or override via
		// WithMaxBadHopsPerWindow(0).
		badHops:             make(map[PeerIdentity]*badHopsState),
		badHopsWindow:       DefaultBadHopsWindow,
		maxBadHopsPerWindow: DefaultMaxBadHopsPerWindow,
		badHopsHoldDownBase: DefaultBadHopsHoldDownBase,
	}
}

// isInHoldDownLocked reports whether the peer is currently in flap
// hold-down at the given wall-clock instant. Caller must hold the
// owning Table's t.mu (reader OK).
func (f *FlapDetector) isInHoldDownLocked(peer PeerIdentity, now time.Time) bool {
	fs := f.state[peer]
	if fs == nil {
		return false
	}
	return now.Before(fs.holdDownUntil)
}

// recordWithdrawalLocked tracks a disconnect event for flap detection.
// If the withdrawal count within window crosses threshold, hold-down
// is activated. Caller must hold the owning Table's t.mu (writer).
//
// Exponential backoff: every consecutive flap-burst that lands while a
// previous burst is still within FlapStableWindowMultiplier × window
// doubles the hold-down duration, capped at MaxHoldDownDuration. The
// growth is bounded by FlapBackoffShiftCap so the multiplier never
// overflows int64 ns. Stable peers reset implicitly inside this same
// helper when the gap from the last burst exceeds the stable window;
// callers can also reset explicitly via Table.RecordSuccessfulRouteAdd
// once they observe convergence on the announce plane.
func (f *FlapDetector) recordWithdrawalLocked(peer PeerIdentity, now time.Time) {
	fs := f.state[peer]
	if fs == nil {
		fs = &peerFlapState{}
		f.state[peer] = fs
	}

	fs.withdrawTimes = append(fs.withdrawTimes, now)

	// Trim events outside the window.
	cutoff := now.Add(-f.window)
	trimmed := fs.withdrawTimes[:0]
	for _, wt := range fs.withdrawTimes {
		if !wt.Before(cutoff) {
			trimmed = append(trimmed, wt)
		}
	}
	fs.withdrawTimes = trimmed

	// Implicit stable-window reset: if the previous burst is older than
	// the stable window, treat this withdrawal as a fresh streak.
	stableWindow := time.Duration(FlapStableWindowMultiplier) * f.window
	if !fs.lastFlapAt.IsZero() && now.Sub(fs.lastFlapAt) > stableWindow {
		fs.consecutiveFlaps = 0
	}

	if len(fs.withdrawTimes) >= f.threshold {
		// Bump consecutiveFlaps only on the *transition* from
		// "hold-down expired/never armed" to "hold-down active". A
		// single flap-burst that drops the connection threshold+N
		// times must arm exactly one hold-down — without this guard
		// the counter would grow on every withdrawal beyond the
		// threshold and the next burst would already start at a
		// multi-step exp-backoff slot.
		if fs.holdDownUntil.IsZero() || !now.Before(fs.holdDownUntil) {
			fs.consecutiveFlaps++
		}
		fs.lastFlapAt = now
		fs.holdDownUntil = now.Add(holdDownDurationForBurst(f.holdDown, fs.consecutiveFlaps))
	}
}

// clearStableLocked clears the consecutive-flap counter for a peer
// that has demonstrated stability. withdrawTimes is intentionally
// left untouched so a single successful add does not erase the
// flap-rate evidence inside the current window — that history trims
// itself naturally as the window slides forward.
//
// Returns true when state actually changed (callers use this to
// decide whether to mark Table dirty). Caller must hold the owning
// Table's t.mu (writer).
func (f *FlapDetector) clearStableLocked(peer PeerIdentity) bool {
	fs := f.state[peer]
	if fs == nil {
		return false
	}
	mutated := fs.consecutiveFlaps != 0 || !fs.lastFlapAt.IsZero()
	fs.consecutiveFlaps = 0
	fs.lastFlapAt = time.Time{}
	return mutated
}

// snapshotLocked returns FlapEntry projections of all peers with
// either recent withdrawals (within window of `now`) or an active
// hold-down. Stale entries are filtered: withdrawals outside the
// flap window are not counted, and peers with no recent withdrawals
// and no active hold-down are excluded.
//
// HoldDownUntil is normalized to zero whenever InHoldDown is false —
// fs.holdDownUntil may still be a past timestamp here (TickTTL clears
// it on its own schedule); without normalization the FlapEntry would
// carry a stale deadline that contradicts the InHoldDown=false flag.
//
// Caller must hold the owning Table's t.mu (reader OK).
func (f *FlapDetector) snapshotLocked(now time.Time) []FlapEntry {
	cutoff := now.Add(-f.window)
	var entries []FlapEntry
	for peer, fs := range f.state {
		recentCount := 0
		for _, wt := range fs.withdrawTimes {
			if !wt.Before(cutoff) {
				recentCount++
			}
		}
		inHoldDown := now.Before(fs.holdDownUntil)
		if recentCount == 0 && !inHoldDown {
			continue
		}

		holdDownUntil := fs.holdDownUntil
		if !inHoldDown {
			holdDownUntil = time.Time{}
		}

		entries = append(entries, FlapEntry{
			PeerIdentity:      peer,
			RecentWithdrawals: recentCount,
			InHoldDown:        inHoldDown,
			HoldDownUntil:     holdDownUntil,
		})
	}
	return entries
}

// tickLocked drops peers from state where both hold-down has expired
// and all withdrawal timestamps are outside the window. Returns true
// when any state was mutated (callers use this to decide dirty
// marking). Caller must hold the owning Table's t.mu (writer).
func (f *FlapDetector) tickLocked(now time.Time) bool {
	// Phase 1 P2: also clear expired SeqNo flap-cap hold-downs and
	// trim stale advance timestamps. Identical lock contract — t.mu
	// (writer) — so the two sub-passes share a single tick cycle.
	mutated := f.clearExpiredSeqHoldDownsLocked(now)
	// Bad-hops hysteresis cleanup — same lock contract, same tick.
	if f.clearExpiredBadHopsHoldDownsLocked(now) {
		mutated = true
	}
	cutoff := now.Add(-f.window)
	for peer, fs := range f.state {
		if !now.Before(fs.holdDownUntil) {
			// Hold-down has expired. Clear the timestamp so the
			// published snapshot reflects InHoldDown=false on the
			// next refresh — without this, FlapEntry.InHoldDown
			// stays true in the cached snapshot until withdrawTimes
			// drain past window (up to ~90 s after the hold-down
			// actually ended on defaults: 30 s hold-down + 120 s
			// flap window). Hold-down expiry is time-derived flap
			// state — see docs/routing.md "Snapshot freshness" — so
			// it is bounded by TickTTL_interval + one refresh, not
			// by a single refresh tick. TickTTL is the schedule that
			// converts the wall-clock transition into a writer event;
			// without the clear here, even running TickTTL would not
			// flip InHoldDown until withdrawTimes drained, breaking
			// even that looser bound.
			if !fs.holdDownUntil.IsZero() {
				fs.holdDownUntil = time.Time{}
				mutated = true
			}

			// Trim stale withdrawal events.
			n := 0
			for _, wt := range fs.withdrawTimes {
				if !wt.Before(cutoff) {
					fs.withdrawTimes[n] = wt
					n++
				}
			}
			if n != len(fs.withdrawTimes) {
				mutated = true
			}
			fs.withdrawTimes = fs.withdrawTimes[:n]

			if n == 0 {
				delete(f.state, peer)
				mutated = true
			}
		}
	}
	return mutated
}

// recordSeqAdvanceLocked accounts a single accepted outbound-SeqNo
// advance for `identity`. Callers (routeStore.nextOutboundSeqLocked*
// helpers) invoke this on every FRESH allocation — i.e. when the
// outboundMax for the Identity moved forward. Cache hits do NOT
// advance and MUST NOT call this method, otherwise reused-SeqNo
// emits would inflate the velocity counter and engage hold-down
// against stable content.
//
// trigger names the wire-frame Origin most recently observed for
// the Identity (or the local origin for own-direct broadcasts);
// surfaced through holdTrigger when hold-down arms so the engage
// warn-log can identify the lineage driving the storm. Empty
// trigger is acceptable for synthetic test fixtures and degrades
// the log entry's diagnostic value but is otherwise harmless.
//
// When the velocity ring (entries within seqAdvanceWindow trailing
// `now`) crosses maxSeqAdvancePerWindow, hold-down is armed:
// `holdUntil = now + seqHoldDown`. While hold-down is already
// active the threshold-crossing path is short-circuited (no
// re-arm, no counter bump) so a runaway upstream cannot increment
// SeqNoFlapHoldowns once per advance — the counter measures
// engagement events, not advance events.
//
// Caller must hold the owning Table's t.mu (writer). When the cap
// is disabled (maxSeqAdvancePerWindow <= 0) the method is a no-op
// — neither map allocation nor counter mutation occurs. Returns
// whether hold-down was newly armed by this advance (callers
// outside the cap-stats counter use this to log the engage event
// at warn level; the atomic counter is bumped inside).
func (f *FlapDetector) recordSeqAdvanceLocked(identity, trigger PeerIdentity, now time.Time, holdownCounter *atomic.Uint64) bool {
	// Cap is disabled when EITHER knob is non-positive. The window
	// gate matters as much as the threshold gate: with window<=0 the
	// trim cutoff at `now - window` is `now` (or later), so the ring
	// retains every same-timestamp advance and a fixed-clock test
	// fixture (or a single-second high-rate burst on a real clock)
	// would arm hold-down purely on advance count, ignoring the
	// "per window" half of the contract. config.go documents
	// CORSA_SEQNO_ADVANCE_WINDOW_SECONDS=0 as a disable signal —
	// honouring it here keeps the operator-facing knob behaviour
	// consistent with both env vars (`CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW=0`
	// and `CORSA_SEQNO_ADVANCE_WINDOW_SECONDS=0`).
	if f == nil || f.maxSeqAdvancePerWindow <= 0 || f.seqAdvanceWindow <= 0 || identity == "" {
		return false
	}

	sv := f.seqVelocities[identity]
	if sv == nil {
		sv = &seqVelocity{}
		f.seqVelocities[identity] = sv
	}

	sv.advances = append(sv.advances, now)

	// Trim entries outside the velocity window. The slice is bounded
	// by maxSeqAdvancePerWindow+1 in steady state (the +1 is the
	// current advance that just landed), so the copy cost is constant.
	cutoff := now.Add(-f.seqAdvanceWindow)
	trimmed := sv.advances[:0]
	for _, t := range sv.advances {
		if !t.Before(cutoff) {
			trimmed = append(trimmed, t)
		}
	}
	sv.advances = trimmed

	// Threshold check fires only on the EDGE — hold-down is armed once
	// per engage, not once per advance during an active window. The
	// caller's holdownCounter (typically &routeStore.capStats.seqNoFlapHoldowns)
	// is bumped here so SeqNoFlapHoldowns matches the count of
	// armed-events 1:1.
	if len(sv.advances) > f.maxSeqAdvancePerWindow && (sv.holdUntil.IsZero() || !now.Before(sv.holdUntil)) {
		sv.holdUntil = now.Add(f.seqHoldDown)
		sv.holdTrigger = trigger
		if holdownCounter != nil {
			holdownCounter.Add(1)
		}
		// Operator-facing engage event. zerolog writes are
		// non-blocking on stderr; safe to emit under t.mu (writer)
		// — same pattern as announce.go's mode-divergence warn.
		// One log per engage event (not per advance during an
		// active hold-down) so the burst rate matches the counter.
		log.Warn().
			Str("identity", string(identity)).
			Str("trigger_origin", string(trigger)).
			Int("window_advances", len(sv.advances)).
			Int("max_per_window", f.maxSeqAdvancePerWindow).
			Dur("window", f.seqAdvanceWindow).
			Dur("hold_down", f.seqHoldDown).
			Time("hold_until", sv.holdUntil).
			Msg("routing_seqno_flap_hold_down_engaged")
		return true
	}
	return false
}

// isInSeqHoldDownLocked reports whether the Identity is currently in
// SeqNo flap-cap hold-down at the given wall-clock instant. Used by
// routeStore.AnnounceProjectionFor (to skip wire emit for Identities
// under hold-down) and by routeStore.ApplyUpdate (to enforce the
// outbound-projected-SeqNo guard).
//
// Caller must hold the owning Table's t.mu (reader OK). Returns
// false unconditionally when the cap is disabled or no advance has
// ever been recorded for `identity`.
func (f *FlapDetector) isInSeqHoldDownLocked(identity PeerIdentity, now time.Time) bool {
	// Symmetric with recordSeqAdvanceLocked: a disabled cap (either
	// knob non-positive) reports "not in hold-down" unconditionally.
	// Without the window guard here, a leftover seqVelocities entry
	// from a window>0 test could continue to suppress wire emit after
	// the operator switched the env to "0" — surprising behaviour
	// for a documented disable signal.
	if f == nil || f.maxSeqAdvancePerWindow <= 0 || f.seqAdvanceWindow <= 0 || identity == "" {
		return false
	}
	sv := f.seqVelocities[identity]
	if sv == nil {
		return false
	}
	return now.Before(sv.holdUntil)
}

// clearExpiredSeqHoldDownsLocked is the SeqNo-flap-cap sibling of
// tickLocked: it drops entries whose advance ring is empty AND whose
// hold-down has expired, and zeroes holdUntil / holdTrigger on
// entries where hold-down has expired but advances within the window
// remain (so a fresh advance reuses the entry without inheriting
// stale hold-down state). Returns whether any field changed
// (callers use this to decide dirty marking, mirroring tickLocked).
// Caller must hold t.mu (writer).
func (f *FlapDetector) clearExpiredSeqHoldDownsLocked(now time.Time) bool {
	if f == nil {
		return false
	}
	cutoff := now.Add(-f.seqAdvanceWindow)
	mutated := false
	for id, sv := range f.seqVelocities {
		// Clear expired hold-down state. Hold-down expiry is a
		// time-derived transition — AnnounceProjectionFor's gate
		// gets the right answer either way, but zeroing the
		// timestamp here lets future advances re-arm cleanly
		// without comparing against a stale deadline.
		if !sv.holdUntil.IsZero() && !now.Before(sv.holdUntil) {
			// Symmetric release log paired with the engage log in
			// recordSeqAdvanceLocked: dashboards built on log
			// scraping (rather than the SeqNoFlapHoldowns counter
			// delta) can pair the two timestamps for a hold-down's
			// duration. The release log carries the same Identity
			// + trigger payload as the engage log so the two lines
			// are joinable by Identity in operator tooling. See
			// docs/routing.md "SeqNo flap cap" for the
			// operator-facing observability contract.
			log.Warn().
				Str("identity", string(id)).
				Str("trigger_origin", string(sv.holdTrigger)).
				Int("retained_advances", len(sv.advances)).
				Msg("routing_seqno_flap_hold_down_released")
			sv.holdUntil = time.Time{}
			sv.holdTrigger = ""
			mutated = true
		}

		// Trim stale advance timestamps.
		n := 0
		for _, t := range sv.advances {
			if !t.Before(cutoff) {
				sv.advances[n] = t
				n++
			}
		}
		if n != len(sv.advances) {
			mutated = true
		}
		sv.advances = sv.advances[:n]

		if n == 0 && sv.holdUntil.IsZero() {
			delete(f.seqVelocities, id)
			mutated = true
		}
	}
	return mutated
}

// recordBadHopsInvalidationLocked feeds the bad-hops hysteresis with
// one ACCEPTED fast-invalidation for the Identity. When the count
// inside badHopsWindow exceeds maxBadHopsPerWindow, hold-down is
// armed (edge-triggered — once per engage, not per event) with
// exponential escalation for re-arms inside
// DefaultBadHopsRecidivismWindow. The caller's counter (typically
// &routeStore.capStats.badHopsHoldowns) is bumped on each arm so the
// observability counter matches engage events 1:1.
//
// Caller must hold the owning Table's t.mu (writer). No-op when
// either knob disables the hysteresis. Returns whether hold-down was
// newly armed by this event.
func (f *FlapDetector) recordBadHopsInvalidationLocked(identity, trigger PeerIdentity, now time.Time, holdownCounter *atomic.Uint64) bool {
	if f == nil || f.maxBadHopsPerWindow <= 0 || f.badHopsWindow <= 0 || identity == "" {
		return false
	}

	bs := f.badHops[identity]
	if bs == nil {
		bs = &badHopsState{}
		f.badHops[identity] = bs
	}

	bs.events = append(bs.events, now)
	cutoff := now.Add(-f.badHopsWindow)
	trimmed := bs.events[:0]
	for _, t := range bs.events {
		if !t.Before(cutoff) {
			trimmed = append(trimmed, t)
		}
	}
	bs.events = trimmed

	if len(bs.events) > f.maxBadHopsPerWindow && (bs.holdUntil.IsZero() || !now.Before(bs.holdUntil)) {
		// Recidivism: strikes escalate while re-arms stay inside the
		// window, reset to 1 after a quiet stretch — same shape as
		// the node-layer quarantine escalation.
		if bs.lastArmed.IsZero() || now.Sub(bs.lastArmed) > DefaultBadHopsRecidivismWindow || bs.strikes < 1 {
			bs.strikes = 1
		} else {
			bs.strikes++
		}
		dur := badHopsHoldDownForStrikes(f.badHopsHoldDownBase, bs.strikes)
		bs.holdUntil = now.Add(dur)
		bs.lastArmed = now
		bs.trigger = trigger
		if holdownCounter != nil {
			holdownCounter.Add(1)
		}
		log.Warn().
			Str("identity", string(identity)).
			Str("trigger_origin", string(trigger)).
			Int("window_events", len(bs.events)).
			Int("max_per_window", f.maxBadHopsPerWindow).
			Dur("window", f.badHopsWindow).
			Dur("hold_down", dur).
			Int("strikes", bs.strikes).
			Time("hold_until", bs.holdUntil).
			Msg("routing_bad_hops_hold_down_engaged")
		return true
	}
	return false
}

// isInBadHopsHoldDownLocked reports whether the Identity's bad-hops
// claims are currently being dropped by the hysteresis. Consulted at
// the top of ApplyUpdate's P3 branch. Caller must hold the owning
// Table's t.mu (reader OK).
func (f *FlapDetector) isInBadHopsHoldDownLocked(identity PeerIdentity, now time.Time) bool {
	if f == nil || f.maxBadHopsPerWindow <= 0 || f.badHopsWindow <= 0 || identity == "" {
		return false
	}
	bs := f.badHops[identity]
	if bs == nil {
		return false
	}
	return now.Before(bs.holdUntil)
}

// clearExpiredBadHopsHoldDownsLocked is the bad-hops sibling of
// clearExpiredSeqHoldDownsLocked: zeroes expired hold-downs (with the
// paired release log), trims stale event timestamps, and drops
// entries that carry no signal anymore — empty ring, no active
// hold-down, AND recidivism anchor colder than
// DefaultBadHopsRecidivismWindow (kept until then so strikes survive
// across consecutive hold-downs). Returns whether anything changed.
// Caller must hold t.mu (writer).
func (f *FlapDetector) clearExpiredBadHopsHoldDownsLocked(now time.Time) bool {
	if f == nil {
		return false
	}
	cutoff := now.Add(-f.badHopsWindow)
	mutated := false
	for id, bs := range f.badHops {
		if !bs.holdUntil.IsZero() && !now.Before(bs.holdUntil) {
			log.Warn().
				Str("identity", string(id)).
				Str("trigger_origin", string(bs.trigger)).
				Int("strikes", bs.strikes).
				Msg("routing_bad_hops_hold_down_released")
			bs.holdUntil = time.Time{}
			bs.trigger = ""
			mutated = true
		}

		n := 0
		for _, t := range bs.events {
			if !t.Before(cutoff) {
				bs.events[n] = t
				n++
			}
		}
		if n != len(bs.events) {
			mutated = true
		}
		bs.events = bs.events[:n]

		recidivismCold := bs.lastArmed.IsZero() ||
			now.Sub(bs.lastArmed) > DefaultBadHopsRecidivismWindow
		if n == 0 && bs.holdUntil.IsZero() && recidivismCold {
			delete(f.badHops, id)
			mutated = true
		}
	}
	return mutated
}

// badHopsHoldDownForStrikes returns the hold-down duration for the
// given strike count: base × 2^(strikes-1), capped at
// DefaultBadHopsHoldDownMax. The shift is bounded by the cap check
// inside the loop, so the multiplier cannot overflow.
func badHopsHoldDownForStrikes(base time.Duration, strikes int) time.Duration {
	if strikes <= 1 {
		return base
	}
	d := base
	for i := 1; i < strikes; i++ {
		d *= 2
		if d >= DefaultBadHopsHoldDownMax {
			return DefaultBadHopsHoldDownMax
		}
	}
	return d
}

// holdDownDurationForBurst returns the hold-down duration for the
// consecutiveFlaps-th burst. The first burst (consecutiveFlaps == 1)
// uses the configured base duration; each subsequent burst doubles
// it, capped at MaxHoldDownDuration and bounded by
// FlapBackoffShiftCap so the bit-shift cannot overflow.
//
// A non-positive base disables hold-down entirely and short-circuits
// to base. Without this guard, exp-backoff would push base*2^shift
// down the scaled <= 0 branch starting on the second burst and turn a
// deliberately disabled hold-down into MaxHoldDownDuration — the
// opposite of what WithHoldDownDuration(0) requests.
//
// Stays as a package-level function rather than a FlapDetector method
// because (a) it is stateless — pure function of base + burst count —
// and (b) table_flap_backoff_test.go calls it directly to pin the
// exp-backoff curve, which would otherwise need a detector instance
// to test.
func holdDownDurationForBurst(base time.Duration, consecutiveFlaps int) time.Duration {
	if base <= 0 {
		return base
	}
	if consecutiveFlaps <= 1 {
		if base > MaxHoldDownDuration {
			return MaxHoldDownDuration
		}
		return base
	}
	shift := consecutiveFlaps - 1
	if shift > FlapBackoffShiftCap {
		shift = FlapBackoffShiftCap
	}
	scaled := base * (1 << shift)
	if scaled > MaxHoldDownDuration || scaled <= 0 {
		return MaxHoldDownDuration
	}
	return scaled
}
