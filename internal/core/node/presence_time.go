package node

import "time"

// presence_time.go is how presence tells the time, and it is a TYPE rather than
// a convention because the convention kept leaking.
//
// Presence does exactly three things with a moment, and each of them has a rule
// that is wrong to get wrong:
//
//   - it MEASURES an interval (a validity window, a probe timeout, a cadence, a
//     cooldown). The rule is presenceInstant.Since: whichever clock saw more
//     time, monotonic or wall.
//   - it ORDERS two observations (a close against a proof). The rule is
//     presenceInstant.ObservedAfter: the monotonic reading, which no clock step
//     can invert. Suspend does not matter here — it shifts nothing relative.
//   - it hands a moment OUT of the process, to be persisted or serialised. The
//     rule is presenceInstant.Wall, and the name is the point: that value has
//     left presence and none of the above applies to it any more.
//
// Every one of those rules was, at some point, written correctly in one place
// and forgotten in another — a `.UTC()` at the clock, a `now.Sub(last)` in a
// cooldown, a `Before` on a deadline. Each leak was found by review rather than
// by the compiler, and the last one was found after a source-scanning test had
// already been written to prevent exactly it, because that test read a list of
// files somebody had to remember to extend.
//
// So the moment is no longer a time.Time. presenceInstant has no Sub, no
// Before and no After: the wrong spelling does not compile, and the reviewer
// does not have to be the type checker.

// presenceInstant is a moment as presence measures it.
type presenceInstant struct{ t time.Time }

// presenceInstantAt wraps a time.Time. It is how a moment ENTERS presence —
// from the clock, or from a test — and the only place a bare time.Time is
// accepted.
func presenceInstantAt(t time.Time) presenceInstant { return presenceInstant{t: t} }

// IsZero reports whether nothing was ever recorded here.
func (i presenceInstant) IsZero() bool { return i.t.IsZero() }

// Add moves an instant forward, which is how a schedule is built.
func (i presenceInstant) Add(d time.Duration) presenceInstant {
	return presenceInstant{t: i.t.Add(d)}
}

// Since is how much time has passed from start to this instant, and it is the
// ONE way presence measures a duration.
//
// It takes the LARGER of the two answers the clock can give, and each of them
// covers the other's blind spot:
//
//   - the MONOTONIC delta is immune to the wall clock being stepped. On its own
//     it is also blind to SUSPEND: Go documents that the monotonic clock stops
//     while the machine sleeps on some systems, so a laptop closed for three
//     hours wakes up believing no time has passed — a proof still inside its
//     450 s window, an open probe that never times out, a cadence resuming as
//     if nothing happened. That is exactly the long false green this whole
//     feature exists to remove, and on a laptop it is the NORMAL case.
//   - the WALL delta sees the suspend, and is the one a clock step corrupts.
//
// The larger of the two is therefore right in both directions. The cost is
// stated rather than hidden: a wall clock jumped FORWARD spuriously expires a
// window early — one probe and one round trip, against 450 s of claiming
// somebody is there who is not.
func (i presenceInstant) Since(start presenceInstant) time.Duration {
	// Time.Sub returns the MONOTONIC delta when both values carry a reading,
	// and falls back to the wall clock when either does not. Round(0) strips
	// the reading, so the second subtraction is always wall (time.Time docs).
	return longerElapsed(i.t.Sub(start.t), i.t.Round(0).Sub(start.t.Round(0)))
}

// Reached reports whether a scheduled moment has arrived, on the same measure.
func (i presenceInstant) Reached(deadline presenceInstant) bool {
	return i.Since(deadline) >= 0
}

// ObservedAfter orders two OBSERVATIONS, which is a different question from how
// much time has passed between them.
//
// It uses the monotonic reading (Time.After does, when both values carry one),
// because what must not happen here is a clock step inverting two events. A
// suspend cannot: it delays both sides equally and changes no order.
//
// Ordering is only ever a last resort. Two events that pass through one lock
// are ordered by a counter minted under it — see Service.nextSessionTransitionLocked
// — and this is for the pairs that share no such point, of which there is
// exactly one: an attributable session close against evidence of life.
func (i presenceInstant) ObservedAfter(other presenceInstant) bool {
	return i.t.After(other.t)
}

// Wall is the moment as a calendar time, and calling it means the value is
// LEAVING presence — persistence, serialisation, a log line. The monotonic
// reading does not survive that and means nothing after it, so nothing that
// comes back from here may be measured or ordered against a presenceInstant.
func (i presenceInstant) Wall() time.Time { return i.t }

func (i presenceInstant) String() string { return i.t.String() }

// longerElapsed is the rule of Since with the clock reading taken out, so that
// it can be tested.
//
// The two deltas cannot be made to disagree from inside a test — the runtime is
// what produces a divergence, by suspending or by having its wall clock moved —
// so the rule is asserted here, over durations, and the reading that feeds it is
// asserted separately.
func longerElapsed(monotonic, wall time.Duration) time.Duration {
	if wall > monotonic {
		return wall
	}
	return monotonic
}
