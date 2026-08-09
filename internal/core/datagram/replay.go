package datagram

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// replay.go holds the two time rules of the replay cache: how long a key is
// worth keeping, and when its record may finally be deleted. They are separate
// rules on purpose, and they are kept apart from replay_base.go because the
// SEPARATION is the invariant — an implementation that computed both from one
// instant would delete a record while an operation still addressed it.
//
// Reference: docs/refactoring/datagram-transport.md §2.2, §4.1.

// BaseReplayDeadline computes `base_until` — how long the BASE cache holds a
// key:
//
//	base_until = min(replay_until, auth.time + domain.DatagramBaseReplayWindow)
//
// The window is measured from the SIGNED auth.time and never from the moment
// of arrival: otherwise a frame delayed in transit would occupy a slot
// longer than the same frame that arrived immediately, and an attacker could
// stretch the cache by simply holding frames back. It cannot be passed in,
// which is the rule and not an omission — the value is wire-normative, so every
// node on the path has to reach the same answer from the signed header alone,
// and a per-instance window would make that answer local.
//
// This is the ONE place the clamp lives. ComputeDeadlines used to restate it
// inline while the cache restated it again with a window of its own, so one
// wire rule had three expressions and only one of them was on the receive path.
func BaseReplayDeadline(authTime, replayUntil time.Time) time.Time {
	capped := authTime.Add(domain.DatagramBaseReplayWindow)
	if replayUntil.Before(capped) {
		return replayUntil
	}
	return capped
}

// ReplayRetention separates the two events that must never be merged: the
// SEMANTIC expiry of the anti-replay key and the PHYSICAL removal of its
// record (§4.1).
//
// Strictly after replay_until the key stops meaning "replay" — and that is
// safe, because any frame with this transcript carries valid_until ≤
// replay_until and is already dead by Validity at that point, whatever the
// store answers. But the record may still OWE something. There is exactly one
// obligation a replay record can have left: an unfinished reservation, held by
// a pipeline branch that has not reached Commit or Release yet.
//
// Deleting on the deadline would hand that key to a second instance of the same
// frame while the first one is still running; keeping the record as a LIVE one
// would mean answering "replay" to a frame that is no longer a replay. So the
// record enters a cleanup-only phase instead: dead for reception, physically
// retained, and removed once it owes nothing.
//
// The phase is bounded, and by the cache rather than by this rule: an
// uncommitted record is reclaimed a whole hop budget past replay_until
// (baseHeldReservationGrace), because after that no live operation can still be
// holding it and what is left is a lost branch, not a quarantine.
type ReplayRetention struct {
	replayUntil time.Time
}

// NewReplayRetention builds the retention rule of one record. The deadline
// passed here is base_until — see BaseReplayDeadline, which is what the cache
// clamps Reserve's replay_until to.
func NewReplayRetention(replayUntil time.Time) ReplayRetention {
	return ReplayRetention{replayUntil: replayUntil}
}

// Until returns the deadline this retention was built with.
func (r ReplayRetention) Until() time.Time { return r.replayUntil }

// AliveAt reports whether the key still carries anti-replay meaning. The
// boundary is INCLUSIVE — the key is alive while now ≤ replay_until (§2.2) —
// so that Validity and anti-replay both treat the boundary instant as live
// and no re-delivery window exists in any single second.
func (r ReplayRetention) AliveAt(now time.Time) bool {
	return !now.After(r.replayUntil)
}

// CleanupOnlyAt reports whether the record has entered the cleanup-only
// phase: semantically expired, physically retained until its obligations are
// played out.
func (r ReplayRetention) CleanupOnlyAt(now time.Time) bool {
	return now.After(r.replayUntil)
}

// RemovableAt reports whether the record may be deleted for good. Both
// conditions are required, and the obligation count is what the caller derives
// from the state it already holds (baseEntry.obligations: an uncommitted record
// owes its reservation, a committed one owes nothing) — never a separate
// refcount, which would be a second source of truth about the same fact.
func (r ReplayRetention) RemovableAt(now time.Time, obligations int) bool {
	return r.CleanupOnlyAt(now) && obligations == 0
}
