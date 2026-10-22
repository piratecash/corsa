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
	return nil
}
