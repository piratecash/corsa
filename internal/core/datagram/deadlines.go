package datagram

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// deadlines.go is the WHOLE timing contract of the routed plane.
//
// It used to be the mandatory HALF of it: an auth profile stated what it
// wanted and this file decided what the frame actually got. The profile side is
// gone, and with it the last reason for the split — a stateless forwarder has
// exactly one temporal policy, it is a pure function of the SIGNED header, and
// every node on the path has to compute the same answer or the same frame is
// alive on one relay and dead on the next.
//
// The order of operations below is normative and the reverse order was a bug:
// clamping after the `now` check produces a frame that passed one boundary and
// lives by another, with send_until beyond the boundary it was admitted under.
//
// Reference: docs/protocol/datagram.md §2.2, §3.3, §4.2.

// sendGrace is the margin subtracted from the end of the validity window so a
// frame is not handed to the writer at the very edge of its life (§3.3).
//
// It is deliberately NOT the same number as the per-class write grace: this one
// bounds how late a frame may still be queued, while write_grace bounds the
// socket write itself, and both are subtracted.
const sendGrace = time.Minute

// DeadlineOutcome is the result of the timing computation.
type DeadlineOutcome uint8

const (
	// DeadlineOutcomeUnset is the zero value.
	DeadlineOutcomeUnset DeadlineOutcome = iota
	// DeadlinesComputed means the frame is alive and has time left to be
	// written.
	DeadlinesComputed
	// DeadlinesNotYetValid means now < valid_from — a frame from the
	// future.
	DeadlinesNotYetValid
	// DeadlinesStale means now > valid_until — the frame is dead. The
	// boundary itself is ALIVE.
	DeadlinesStale
	// DeadlinesExpired means the frame is still valid, but the clamped
	// send_until is already behind now: there is no time left to write it,
	// so it is not enqueued at all. Local delivery is unaffected — this
	// outcome is about the send path (§2.2).
	DeadlinesExpired
)

var deadlineOutcomeNames = map[DeadlineOutcome]string{
	DeadlineOutcomeUnset: "unset",
	DeadlinesComputed:    "computed",
	DeadlinesNotYetValid: "not_yet_valid",
	DeadlinesStale:       "stale",
	DeadlinesExpired:     "expired",
}

// String returns the drop-metric label of the outcome.
func (o DeadlineOutcome) String() string { return enumName(deadlineOutcomeNames, o) }

// Deadlines are the instants the pipeline runs on, after clamping. They are
// read-only by construction: the whole point of the type is that downstream
// code cannot recompute a deadline on its own and get a different answer.
type Deadlines struct {
	validFrom   time.Time
	validUntil  time.Time
	replayUntil time.Time
	sendUntil   time.Time
}

// ValidFrom is the lower bound of the validity interval, INCLUSIVE for life.
// It is never clamped: "too far in the future" is a refusal of its own, and
// clamping it would turn a future frame into a live one.
func (d Deadlines) ValidFrom() time.Time { return d.validFrom }

// ValidUntil is the clamped upper bound of the validity interval, INCLUSIVE
// for life. Everything else is computed from THIS value.
func (d Deadlines) ValidUntil() time.Time { return d.validUntil }

// ReplayUntil is the retention deadline of the replay key, INCLUSIVE for life.
// It is never below ValidUntil: a node that accepted a frame for a day and kept
// its key for an hour would let an exact copy through every hour, forever.
func (d Deadlines) ReplayUntil() time.Time { return d.replayUntil }

// SendUntil is the deadline the writer checks immediately before the socket
// write, and the queue checks on admission (§3.3 rule 5).
func (d Deadlines) SendUntil() time.Time { return d.sendUntil }

// DeadlineDecision pairs the outcome with the deadlines, when there are any.
type DeadlineDecision struct {
	outcome   DeadlineOutcome
	deadlines Deadlines
}

// Outcome reports what the layer decided about the frame's timing.
func (d DeadlineDecision) Outcome() DeadlineOutcome { return d.outcome }

// Deadlines returns the computed instants. The bool is false for the two
// refusals, where no consistent set of deadlines exists; it is TRUE for
// DeadlinesExpired, because the frame is alive and only the send window is
// gone — a caller delivering it locally still needs replay_until.
func (d DeadlineDecision) Deadlines() (Deadlines, bool) {
	switch d.outcome {
	case DeadlinesComputed, DeadlinesExpired:
		return d.deadlines, true
	default:
		return Deadlines{}, false
	}
}

// ComputeDeadlines is the timing rule of §3.3 as a pure function of (header,
// now): no clock, no store, no cryptography — which is why a refusal here costs
// the sender nothing but a parse, and costs this node neither a crypto token
// nor a ban decision.
//
// # Everything is bounded by the base anti-replay window
//
// The node's only anti-replay state is the bounded in-memory cache, so the
// window that cache holds a key for is also the window in which this node may
// CARRY the frame. A longer validity would mean forwarding a copy this node can
// no longer recognise as a repeat — and once the key had aged out, the frame
// would still be valid, so this node would admit it and forward it a second
// time, and so would every node on the path, once per window for the whole of
// the validity.
//
// The price is named rather than hidden, and it is the honest price of keeping
// no copy: a frame still valid at its destination is dropped as STALE by a node
// it reaches later than the base window from auth.time.
func ComputeDeadlines(header Header, now time.Time) DeadlineDecision {
	signed := header.AuthTime()
	baseWindowEnd := signed.Add(domain.DatagramBaseReplayWindow)

	freshnessEnd := signed.Add(domain.DatagramFreshnessWindow)
	validFrom := signed.Add(-domain.DatagramFreshnessWindow)
	// The clamp comes FIRST. Checking the raw bound and clamping afterwards
	// would admit a frame by one boundary and run it by another (§2.2).
	validUntil := earlier(freshnessEnd, baseWindowEnd)

	// Both boundaries are INCLUSIVE for life — one invariant across the whole
	// spec: at now == valid_until the frame is still alive, and death is
	// strictly past the bound.
	switch {
	case now.Before(validFrom):
		return DeadlineDecision{outcome: DeadlinesNotYetValid}
	case now.After(validUntil):
		return DeadlineDecision{outcome: DeadlinesStale}
	}

	// replay_until is never below valid_until: the key outlives every frame it
	// can still recognise. The clamp against the base window is BaseReplayDeadline
	// and not a second copy of it here: the cache and this function stating one
	// wire rule twice is how they get to disagree about it.
	replayUntil := BaseReplayDeadline(signed, later(freshnessEnd, validUntil))

	sendUntil := sendWindowEnd(freshnessEnd, validUntil, now, header.Class())

	deadlines := Deadlines{
		validFrom:   validFrom,
		validUntil:  validUntil,
		replayUntil: replayUntil,
		sendUntil:   sendUntil,
	}
	if sendUntil.Before(now) {
		return DeadlineDecision{outcome: DeadlinesExpired, deadlines: deadlines}
	}
	return DeadlineDecision{outcome: DeadlinesComputed, deadlines: deadlines}
}

// sendWindowEnd is the last instant at which the frame may still be handed to
// the writer. It is the earliest of THREE independent bounds, and each answers
// a different question:
//
//   - freshness_end − send_grace: how late the frame may still be QUEUED, so
//     nothing enters a lane at the very edge of its life (§3.3);
//   - now + queue_residence(class): how long the frame's class lets it SIT in
//     that lane;
//   - valid_until − write_grace(class): the room the socket WRITE itself needs,
//     so a write that started in time still finishes inside the validity.
//
// The third bound is the one that binds nothing while the freshness window and
// the base replay window hold the same value: valid_until is then the later
// bound of the two and send_grace the larger margin. It stays because those are
// two knobs and not one (domain.DatagramFreshnessWindow says so in as many
// words), and with a base window shorter than the freshness window it is the
// ONLY term that keeps the write inside valid_until — every other term is
// measured from the unclamped freshness end or from now.
//
// It takes its bounds as arguments rather than recomputing them so the caller's
// clamp and this function's clamp cannot become two different answers, and so
// the write-grace term is reachable from a test without a build whose domain
// constants differ.
func sendWindowEnd(freshnessEnd, validUntil, now time.Time, class domain.DatagramClass) time.Time {
	return earlier(
		earlier(freshnessEnd.Add(-sendGrace), now.Add(queueResidence(class))),
		validUntil.Add(-writeGrace(class)),
	)
}

// writeGrace keeps the subtraction total. A Header carries a class the wire
// layer has already validated, so the error branch is unreachable; zero grace
// would only mean "no room reserved for the write", which is the conservative
// reading rather than an unbounded one.
func writeGrace(class domain.DatagramClass) time.Duration {
	grace, err := domain.WriteGrace(class)
	if err != nil {
		return 0
	}
	return grace
}

// queueResidence keeps the send window total, on the same argument as
// writeGrace: zero residence yields "no time left to queue", which is the safe
// answer rather than an unbounded one.
func queueResidence(class domain.DatagramClass) time.Duration {
	residence, err := domain.QueueResidence(class)
	if err != nil {
		return 0
	}
	return residence
}

// earlier returns the earlier of two instants.
func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// later returns the later of two instants.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
