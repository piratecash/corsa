package findnode

import (
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
	"sync/atomic"
)

type OwnedReceivedAnswer struct{ answer *ownedReceivedAnswerState }
type ownedReceivedAnswerState struct {
	owner    *dormantIntakeState
	target   Session
	stamp    findnodebridge.CaptureStamp
	received findnodebridge.OwnedInstant
	body     protocol.FindNodeAnswer
	consumed atomic.Bool
}

func (sink ObservationSink) SealAnswer(stamp findnodebridge.CaptureStamp, received findnodebridge.OwnedInstant, target Session, raw []byte) (OwnedReceivedAnswer, error) {
	owner := sink.observationOwner
	if owner == nil || !owner.consumer.Matches(stamp, received) || !target.valid() || target.state.opened > Instant(received.Elapsed()) {
		return OwnedReceivedAnswer{}, ErrUnauthenticated
	}
	answer, err := protocol.ParseFindNodeAnswer(raw)
	if err != nil {
		return OwnedReceivedAnswer{}, err
	}
	return OwnedReceivedAnswer{answer: &ownedReceivedAnswerState{owner: owner, target: target, stamp: stamp, received: received, body: answer}}, nil
}

// Correlate only the SAME original target/forwarded ID and retained target
// identity. The relay copies the signed body without verifying it and rewrites
// only its local envelope ID to the retained original source request ID.
func (intake DormantIntake) MatchOwnedAnswer(input OwnedReceivedAnswer) (OwnedAnswerWork, error) {
	owner, cell := intake.dormantOwner, input.answer
	if owner == nil || cell == nil || cell.owner != owner || !owner.consumer.Matches(cell.stamp, cell.received) {
		return OwnedAnswerWork{}, ErrUnauthenticated
	}
	if !cell.consumed.CompareAndSwap(false, true) {
		return OwnedAnswerWork{}, ErrStaleWork
	}
	o := owner.coordinator
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if err := o.advanceLocked(o.last); err != nil {
		return OwnedAnswerWork{}, err
	}
	entry := o.pending.targetEntryLocked(pendingKey{cell.target, cell.body.ID})
	if entry == nil || entry.intake == nil || entry.request.Target != cell.body.Target || !entry.source.valid() || !cell.target.valid() || (entry.intake.phase != candidateDispatching && entry.intake.phase != candidateAwaitingAnswer) || entry.intake.calendar == nil {
		return OwnedAnswerWork{}, ErrUnmatchedAnswer
	}
	source, target, request, forwarded, expires := entry.source, entry.target, entry.request, entry.forwarded, entry.expires
	phase := entry.intake.phase
	reservation := o.handle(entry)
	answer := cell.body
	answer.ID = request.ID
	// Allocation happens before R; this private cell does not escape on refusal.
	result := mintOwnedAnswerWork(owner, source, answer, expires)
	calendar := entry.intake.calendar
	calendar.mu.Lock()
	defer calendar.mu.Unlock()
	stamp, lease, err := owner.consumer.ReserveAcceptedClock()
	if err != nil {
		return OwnedAnswerWork{}, err
	}
	defer lease.Release()
	at, err := lease.Sample()
	if err != nil {
		return OwnedAnswerWork{}, err
	}
	f := Instant(at.Elapsed())
	if !owner.consumer.Matches(stamp, at) || f < o.last || f < Instant(cell.received.Elapsed()) || f >= expires || !o.currentLocked(reservation) || entry.source != source || entry.target != target || entry.request != request || entry.forwarded != forwarded || entry.expires != expires || entry.intake.phase != phase || target != cell.target || !source.valid() || !target.valid() || target.state.opened > Instant(cell.received.Elapsed()) || source.state.opened > f {
		return OwnedAnswerWork{}, ErrUnmatchedAnswer
	}
	if _, err := o.sourceRowLocked(entry); err != nil {
		return OwnedAnswerWork{}, ErrUnmatchedAnswer
	}
	// Diagnostic qualification only. Missing/inconsistent ingress or legacy
	// starts leave UNKNOWN; they never change this already successful Match.
	state := entry.intake
	identity := state.ownedIdentity
	if state.forwardEndpointKnown && state.ownedReceived != nil && state.generation == reservation.generation &&
		identity.owner == owner && identity.source == source && identity.request == request && ownedReceivedMatches(owner, entry) {
		observation := ownedRelayEndpointObservation{
			relayKnown: true, generation: reservation.generation,
			sourceConnID: uint64(source.state.connID), targetConnID: uint64(target.state.connID),
			sourceOpenedNS: int64(source.state.opened), targetOpenedNS: int64(target.state.opened),
			sourceIdentity: source.state.identity, targetIdentity: target.state.identity,
			request: request, forwarded: forwarded, expiresNS: int64(expires),
			receivedNS: int64(identity.received.Elapsed()), forwardNS: state.forwardEndpointNS,
			answerReceivedNS: int64(cell.received.Elapsed()),
		}
		if owner.consumer.IsRuntime() {
			observation.clock = ownedForwardObservationClockRuntime
		} else if owner.consumer.IsFixture() {
			observation.clock = ownedForwardObservationClockFixture
		}
		observation.receivedKnown = observation.clock != ownedForwardObservationClockUnknown && observation.receivedNS >= 0
		observation.forwardKnown = observation.clock != ownedForwardObservationClockUnknown && observation.forwardNS >= 0
		observation.answerReceivedKnown = observation.clock != ownedForwardObservationClockUnknown && observation.answerReceivedNS >= 0
		result.answer.relayObservation = observation
	}
	o.last = f
	o.count(&o.counters.answered)
	if phase == candidateDispatching {
		entry.intake.phase = candidateTerminalDispatch
	} else {
		o.releaseLocked(entry)
	}
	return result, nil
}
