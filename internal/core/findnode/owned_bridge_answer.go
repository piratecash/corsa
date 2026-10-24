package findnode

import (
	"github.com/piratecash/corsa/internal/core/protocol"
	"sync/atomic"
)

// Typed delivery retains the exact original source, not a later ConnID route.
// Local signing completion/cache-copy and relayed pending correlation are the
// finite private mint sites. Mint only before R or after its release; the cell
// allocation must not enter any bounded admission suffix.
type OwnedAnswerWork struct{ answer *ownedAnswerState }
type OwnedQueuedAnswer struct{ answer *ownedAnswerState }
type OwnedAnswerPermit struct{ answer *ownedAnswerState }
type ownedAnswerState struct {
	owner   *dormantIntakeState
	source  Session
	body    protocol.FindNodeAnswer
	expires Instant
	payload []byte
	phase   atomic.Uint32
	// Immutable relay scalars are frozen before this answer escapes Match.
	relayObservation ownedRelayEndpointObservation
	// One original Take writer; publish the plain value before the known bit.
	answerEndpointNS    int64
	answerEndpointKnown atomic.Bool
	writeObservation    atomic.Uint32
}

// Plain historical values only: no owner, Session, clock, receipt or row pointer.
// Numeric identity fields are testimony, never lookup or admission authority.
type ownedRelayEndpointObservation struct {
	relayKnown                                                         bool
	clock                                                              ownedForwardObservationClock
	receivedKnown, forwardKnown, answerReceivedKnown, answerStartKnown bool
	receivedNS, forwardNS, answerReceivedNS, answerStartNS             int64
	chainKnown                                                         bool
	spanNS                                                             int64
	generation                                                         uint64
	sourceConnID, targetConnID                                         uint64
	sourceOpenedNS, targetOpenedNS                                     int64
	sourceIdentity, targetIdentity                                     [20]byte
	request, forwarded                                                 protocol.FindNodeRequest
	expiresNS                                                          int64
	writeOutcome                                                       ownedAnswerWriteObservation
}

type ownedAnswerWriteObservation uint32

const (
	ownedAnswerWriteUnreported ownedAnswerWriteObservation = iota
	ownedAnswerWriteReportedOK
	ownedAnswerWriteReportedFailed
)

// This reader never samples, locks, looks up a numeric ID or requires liveness.
// u is the successful original Take sample, not raw-write/byte completion.
// The outcome is only the original writer's report after successful Finish CAS.
func (work OwnedAnswerWork) relayEndpointObservation() ownedRelayEndpointObservation {
	a := work.answer
	if a == nil || a.owner == nil {
		return ownedRelayEndpointObservation{}
	}
	out := a.relayObservation
	if !out.relayKnown {
		if a.owner.consumer.IsRuntime() {
			out.clock = ownedForwardObservationClockRuntime
		} else if a.owner.consumer.IsFixture() {
			out.clock = ownedForwardObservationClockFixture
		}
	}
	if a.answerEndpointKnown.Load() {
		out.answerStartNS = a.answerEndpointNS
		out.answerStartKnown = out.clock != ownedForwardObservationClockUnknown && out.answerStartNS >= 0
		out.writeOutcome = ownedAnswerWriteObservation(a.writeObservation.Load())
	}
	if out.relayKnown && out.receivedKnown && out.forwardKnown && out.answerReceivedKnown && out.answerStartKnown &&
		out.receivedNS <= out.forwardNS && out.forwardNS <= out.answerReceivedNS && out.answerReceivedNS <= out.answerStartNS {
		out.chainKnown = true
		// Ordered nonnegative int64 endpoints make subtraction non-overflowing.
		out.spanNS = out.answerStartNS - out.receivedNS
	}
	return out
}

// OwnedRelayEndpointClock reports provenance, not physical calibration or a
// process/boot/domain identity. It grants no clock or admission authority.
type OwnedRelayEndpointClock uint8

const (
	OwnedRelayEndpointClockUnknown OwnedRelayEndpointClock = iota
	OwnedRelayEndpointClockFixture
	OwnedRelayEndpointClockRuntime
)

// OwnedRelayWriteOutcome is the original writer's report after Finish, not
// proof of byte completion or delivery. Unreported is distinct from failure.
type OwnedRelayWriteOutcome uint8

const (
	OwnedRelayWriteUnreported OwnedRelayWriteOutcome = iota
	OwnedRelayWriteReportedOK
	OwnedRelayWriteReportedFailed
)

// OwnedRelayEndpointObservation contains historical values for one exact
// original answer work. Session numbers and generation are local testimony,
// never lookup keys or cross-process connection identity. Request and forwarded
// fields remain distinct. A local/cache/coalesced answer may report its own u
// and writer outcome, but has no relayed a/f/h or known relay chain.
type OwnedRelayEndpointObservation struct {
	Clock                                                              OwnedRelayEndpointClock
	RelayKnown                                                         bool
	ReceivedKnown, ForwardKnown, AnswerReceivedKnown, AnswerStartKnown bool
	ReceivedNS, ForwardNS, AnswerReceivedNS, AnswerStartNS             int64
	ChainKnown                                                         bool
	SpanNS                                                             int64
	Generation                                                         uint64
	SourceConnID, TargetConnID                                         uint64
	SourceOpenedNS, TargetOpenedNS                                     int64
	SourceIdentity, TargetIdentity                                     [20]byte
	RequestID, ForwardedID                                             uint64
	RequestTarget, ForwardedTarget                                     [20]byte
	RequestChallenge, ForwardedChallenge                               [32]byte
	ExpiresNS                                                          int64
	WriteOutcome                                                       OwnedRelayWriteOutcome
}

// RelayEndpointObservation copies the existing private reader once. It adds
// no Sample, lock, state, owner/Session/domain handle or numeric-ID lookup.
// f/u are successful Take samples, not write completion; h is original answer
// receipt, not the later Match validation sample. Runtime is provenance only.
func (work OwnedAnswerWork) RelayEndpointObservation() OwnedRelayEndpointObservation {
	observed := work.relayEndpointObservation()
	out := OwnedRelayEndpointObservation{
		RelayKnown:    observed.relayKnown,
		ReceivedKnown: observed.receivedKnown, ForwardKnown: observed.forwardKnown,
		AnswerReceivedKnown: observed.answerReceivedKnown, AnswerStartKnown: observed.answerStartKnown,
		ReceivedNS: observed.receivedNS, ForwardNS: observed.forwardNS,
		AnswerReceivedNS: observed.answerReceivedNS, AnswerStartNS: observed.answerStartNS,
		ChainKnown: observed.chainKnown, SpanNS: observed.spanNS,
		Generation:   observed.generation,
		SourceConnID: observed.sourceConnID, TargetConnID: observed.targetConnID,
		SourceOpenedNS: observed.sourceOpenedNS, TargetOpenedNS: observed.targetOpenedNS,
		SourceIdentity: observed.sourceIdentity, TargetIdentity: observed.targetIdentity,
		RequestID: uint64(observed.request.ID), ForwardedID: uint64(observed.forwarded.ID),
		RequestTarget: observed.request.Target, ForwardedTarget: observed.forwarded.Target,
		RequestChallenge: observed.request.Challenge, ForwardedChallenge: observed.forwarded.Challenge,
		ExpiresNS: observed.expiresNS,
	}
	switch observed.clock {
	case ownedForwardObservationClockFixture:
		out.Clock = OwnedRelayEndpointClockFixture
	case ownedForwardObservationClockRuntime:
		out.Clock = OwnedRelayEndpointClockRuntime
	}
	switch observed.writeOutcome {
	case ownedAnswerWriteReportedOK:
		out.WriteOutcome = OwnedRelayWriteReportedOK
	case ownedAnswerWriteReportedFailed:
		out.WriteOutcome = OwnedRelayWriteReportedFailed
	}
	return out
}

const (
	ownedAnswerReady uint32 = iota
	ownedAnswerPreparing
	ownedAnswerQueued
	ownedAnswerTaking
	ownedAnswerStarted
	ownedAnswerDone
	ownedAnswerCancelled
)

func mintOwnedAnswerWork(owner *dormantIntakeState, source Session, answer protocol.FindNodeAnswer, expires Instant) OwnedAnswerWork {
	return OwnedAnswerWork{answer: &ownedAnswerState{owner: owner, source: source, body: answer, expires: expires}}
}

// Routing metadata only. A queued handle must still obtain a genuine fresh
// original writer start after its deadline syscall/application waits.
func (work OwnedAnswerWork) SourceSession() Session {
	if work.answer == nil || work.answer.owner == nil {
		return Session{}
	}
	return work.answer.source
}
func (work OwnedAnswerWork) QueueAnswerForOriginal(original Session) (OwnedQueuedAnswer, error) {
	a := work.answer
	if a == nil || a.owner == nil || a.source != original || a.source.state == nil || a.expires < 0 {
		return OwnedQueuedAnswer{}, ErrUnauthenticated
	}
	if !a.phase.CompareAndSwap(ownedAnswerReady, ownedAnswerPreparing) {
		return OwnedQueuedAnswer{}, ErrStaleWork
	}
	raw, err := protocol.MarshalFindNodeAnswer(a.body)
	if err != nil {
		a.phase.Store(ownedAnswerCancelled)
		return OwnedQueuedAnswer{}, err
	}
	a.payload = append(raw, '\n')
	a.phase.Store(ownedAnswerQueued)
	return OwnedQueuedAnswer{answer: a}, nil
}
func (work OwnedAnswerWork) CancelAnswer() error { return cancelOwnedAnswer(work.answer) }
func (queued OwnedQueuedAnswer) Cancel() error   { return cancelOwnedAnswer(queued.answer) }
func cancelOwnedAnswer(a *ownedAnswerState) error {
	if a == nil || a.owner == nil {
		return ErrUnauthenticated
	}
	for {
		phase := a.phase.Load()
		if phase != ownedAnswerReady && phase != ownedAnswerQueued {
			return ErrStaleWork
		}
		if a.phase.CompareAndSwap(phase, ownedAnswerCancelled) {
			return nil
		}
	}
}

// Answer delivery grants no forwarding debit or local AdmittedAtT. It checks
// exact original source liveness and original request expiry under genuine R.
func (queued OwnedQueuedAnswer) Take() ([]byte, OwnedAnswerPermit, error) {
	a := queued.answer
	if a == nil || a.owner == nil || a.source.state == nil {
		return nil, OwnedAnswerPermit{}, ErrUnauthenticated
	}
	if !a.phase.CompareAndSwap(ownedAnswerQueued, ownedAnswerTaking) {
		return nil, OwnedAnswerPermit{}, ErrStaleWork
	}
	defer func() {
		if a.phase.Load() == ownedAnswerTaking {
			a.phase.Store(ownedAnswerCancelled)
		}
	}()
	stamp, lease, err := a.owner.consumer.ReserveAcceptedClock()
	if err != nil {
		return nil, OwnedAnswerPermit{}, err
	}
	defer lease.Release()
	at, err := lease.Sample()
	if err != nil {
		return nil, OwnedAnswerPermit{}, err
	}
	if !a.owner.consumer.Matches(stamp, at) || !a.source.valid() || a.source.state.opened > Instant(at.Elapsed()) {
		return nil, OwnedAnswerPermit{}, ErrUnauthenticated
	}
	if Instant(at.Elapsed()) >= a.expires {
		return nil, OwnedAnswerPermit{}, ErrNotReady
	}
	a.answerEndpointNS = int64(at.Elapsed())
	a.answerEndpointKnown.Store(true)
	a.phase.Store(ownedAnswerStarted)
	return a.payload, OwnedAnswerPermit{answer: a}, nil
}

// No accounting row remains attached to an answer delivery. Finish retires
// only this exact in-flight cell, even after original End, with no fake clock,
// accepted-debt refund, callback or requeue. Byte outcome is capture metadata.
func (permit OwnedAnswerPermit) Finish(writeOK bool) error {
	a := permit.answer
	if a == nil || a.owner == nil {
		return ErrUnauthenticated
	}
	if !a.phase.CompareAndSwap(ownedAnswerStarted, ownedAnswerDone) {
		return ErrStaleWork
	}
	if writeOK {
		a.writeObservation.Store(uint32(ownedAnswerWriteReportedOK))
	} else {
		a.writeObservation.Store(uint32(ownedAnswerWriteReportedFailed))
	}
	return nil
}
