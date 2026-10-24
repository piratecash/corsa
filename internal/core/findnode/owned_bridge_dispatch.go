package findnode

import (
	"github.com/piratecash/corsa/internal/core/protocol"
	"sync/atomic"
	"time"
)

// Original accepted C3a receipt, not a caller-created reservation or timestamp.
// Every handle aliases this one state machine. Payload is allocated exactly
// once before queue publication; only the actual original writer receives it.
type OwnedForwardWork struct{ work *ownedForwardState }
type OwnedQueuedForward struct{ work *ownedForwardState }
type OwnedWritePermit struct{ work *ownedForwardState }
type ownedForwardState struct {
	owner              *dormantIntakeState
	cut                *ownedCutState
	receipt            ownedRoundSuccessReceipt
	source, target     Session
	request, forwarded protocol.FindNodeRequest
	phase              atomic.Uint32
	payload            []byte
	// Diagnostic only: written/read under owner.coordinator.mu.
	forwardEndpointNS    int64
	forwardEndpointKnown bool
}

const (
	ownedForwardReady uint32 = iota
	ownedForwardPreparing
	ownedForwardQueued
	ownedForwardTaking
	ownedForwardStarted
	ownedForwardFinishing
	ownedForwardDone
	ownedForwardCancelled
)

func ownedForwardOriginal(w *ownedForwardState) bool {
	if w == nil || w.owner == nil || w.cut == nil || w.cut.owner != w.owner {
		return false
	}
	r := w.receipt
	e := r.execution
	if e == nil || e.claim == nil || e.claim.owner != w.owner || e.plan == nil || r.ordinal < 0 || r.ordinal >= e.plan.count || r != e.receipts[r.ordinal] || !e.done[r.ordinal] {
		return false
	}
	row := e.plan.rows[e.plan.order[r.ordinal]]
	forwarded := row.request
	forwarded.ID = r.forwarded
	return r.reservation == row.reservation && r.target == row.target && w.source == row.source && w.target == row.target && w.request == row.request && w.forwarded == forwarded
}

// Private historical endpoints of this exact original work, not permission to
// start/finish/write. ForwardNS is the successful Take SAMPLE, not a timer wake,
// projected slot, raw Write or delivery. Finish/error/End never turn this a/f
// pair into a completion claim. No domain/Session/receipt handle escapes.
type ownedForwardObservationClock uint8

const (
	ownedForwardObservationClockUnknown ownedForwardObservationClock = iota
	ownedForwardObservationClockFixture
	ownedForwardObservationClockRuntime
)

type ownedForwardEndpointObservation struct {
	clock                         ownedForwardObservationClock
	receivedKnown, forwardKnown   bool
	pairKnown                     bool
	receivedNS, forwardNS, spanNS int64
}

func (work OwnedForwardWork) endpointObservation() ownedForwardEndpointObservation {
	w := work.work
	if !ownedForwardOriginal(w) {
		return ownedForwardEndpointObservation{}
	}
	o := w.owner.coordinator
	o.mu.Lock()
	defer o.mu.Unlock()
	// The accepted execution plan retains this original immutable ingress.
	// Matches is immutable provenance only; neither a new Sample nor liveness.
	r := w.receipt
	identity := r.execution.plan.rows[r.execution.plan.order[r.ordinal]].receivedIdentity
	if identity.owner != w.owner || identity.source != w.source || identity.request != w.request || !w.owner.consumer.Matches(identity.stamp, identity.received) {
		return ownedForwardEndpointObservation{}
	}
	out := ownedForwardEndpointObservation{receivedNS: int64(identity.received.Elapsed()), forwardNS: w.forwardEndpointNS}
	if w.owner.consumer.IsRuntime() {
		out.clock = ownedForwardObservationClockRuntime
	} else if w.owner.consumer.IsFixture() {
		out.clock = ownedForwardObservationClockFixture
	}
	out.receivedKnown = out.clock != ownedForwardObservationClockUnknown && out.receivedNS >= 0
	out.forwardKnown = out.clock != ownedForwardObservationClockUnknown && w.forwardEndpointKnown && out.forwardNS >= 0
	if out.receivedKnown && out.forwardKnown && out.forwardNS >= out.receivedNS {
		out.pairKnown = true
		out.spanNS = out.forwardNS - out.receivedNS
	}
	return out
}

// RouteTarget is immutable routing metadata, never a writer-start permit.
func (work OwnedForwardWork) RouteTarget() Session {
	if !ownedForwardOriginal(work.work) {
		return Session{}
	}
	return work.work.target
}

func (work OwnedForwardWork) QueueForOriginal(original Session) (OwnedQueuedForward, error) {
	w := work.work
	if !ownedForwardOriginal(w) || original != w.target {
		return OwnedQueuedForward{}, ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedForwardReady, ownedForwardPreparing) {
		return OwnedQueuedForward{}, ErrStaleWork
	}
	raw, err := protocol.MarshalFindNodeRequest(w.forwarded)
	if err != nil {
		w.phase.Store(ownedForwardCancelled)
		retireOwnedForward(w)
		return OwnedQueuedForward{}, err
	}
	w.payload = append(raw, '\n')
	w.phase.Store(ownedForwardQueued)
	return OwnedQueuedForward{work: w}, nil
}

// WaitDelay is only a wake hint for an already accepted future calendar slot.
// It grants no admission and is always followed by the genuine fresh Take.
func (queued OwnedQueuedForward) WaitDelay() (time.Duration, error) {
	w := queued.work
	if !ownedForwardOriginal(w) || w.phase.Load() != ownedForwardQueued || w.receipt.alias == nil {
		return 0, ErrStaleWork
	}
	clock, err := w.owner.finishObserver.ReserveClock()
	if err != nil {
		return 0, err
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		return 0, err
	}
	if !w.owner.consumer.Matches(w.cut.stamp, at) {
		return 0, ErrUnauthenticated
	}
	delta := w.receipt.scheduled - Instant(at.Elapsed())
	if delta <= 0 {
		return 0, nil
	}
	return time.Duration(delta), nil
}

func (work OwnedForwardWork) Cancel() error     { return cancelOwnedForward(work.work) }
func (queued OwnedQueuedForward) Cancel() error { return cancelOwnedForward(queued.work) }
func cancelOwnedForward(w *ownedForwardState) error {
	if !ownedForwardOriginal(w) {
		return ErrUnauthenticated
	}
	for {
		phase := w.phase.Load()
		if phase != ownedForwardReady && phase != ownedForwardQueued {
			return ErrStaleWork
		}
		if w.phase.CompareAndSwap(phase, ownedForwardCancelled) {
			retireOwnedForward(w)
			return nil
		}
	}
}
func retireOwnedForward(w *ownedForwardState) {
	if !ownedForwardOriginal(w) {
		return
	}
	o := w.owner.coordinator
	r := w.receipt.reservation
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	// Drop only this accepted original generation before writer start. A writer already
	// dispatching is never cancelled by producer post-offer/gate cleanup.
	if o.currentLocked(r) && r.entry.intake.phase == candidateReserved && r.entry.source == w.source && r.entry.target == w.target && r.entry.forwarded == w.forwarded {
		o.endLocked(r.entry)
	}
}

// Take is called by the ACTUAL queued writer after its last external wait and
// socket-deadline preparation. This Take admits forwarding writer start, not responder AdmittedAtT. The immediate next writer
// action after return is rawConn.Write; no Node lookup or queue hop may follow.
func (queued OwnedQueuedForward) Take() ([]byte, OwnedWritePermit, error) {
	w := queued.work
	if !ownedForwardOriginal(w) {
		return nil, OwnedWritePermit{}, ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedForwardQueued, ownedForwardTaking) {
		return nil, OwnedWritePermit{}, ErrStaleWork
	}
	defer func() {
		if w.phase.Load() == ownedForwardTaking {
			w.phase.Store(ownedForwardCancelled)
			retireOwnedForward(w)
		}
	}()
	owner, cut := w.owner, w.cut
	o := owner.coordinator
	r := w.receipt.reservation
	o.mu.Lock()
	defer o.mu.Unlock()
	// Successful writer start must return directly to raw Write. Diagnostics
	// would reacquire other calendars; publish success only in postwrite Finish.
	defer func() {
		if w.phase.Load() != ownedForwardStarted {
			o.publishLocked()
		}
	}()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	housekeeping := max(Instant(cut.at.Elapsed()), o.last)
	if err := o.advanceLocked(housekeeping); err != nil {
		return nil, OwnedWritePermit{}, err
	}
	if !o.currentLocked(r) || r.entry.intake.phase != candidateReserved {
		return nil, OwnedWritePermit{}, ErrStaleWork
	}
	entry := r.entry
	state := entry.intake
	calendar := state.calendar
	if calendar == nil || state.alias == nil {
		return nil, OwnedWritePermit{}, ErrStaleWork
	}
	calendar.mu.Lock()
	defer calendar.mu.Unlock()
	stamp, lease, err := owner.consumer.ReserveAcceptedClock()
	if err != nil {
		return nil, OwnedWritePermit{}, err
	}
	defer lease.Release()
	at, err := lease.Sample()
	if err != nil {
		return nil, OwnedWritePermit{}, err
	}
	f := Instant(at.Elapsed())
	if !owner.consumer.Matches(stamp, at) || f < w.receipt.at || f < o.last || !w.source.valid() || !w.target.valid() || w.source.state.opened > f || w.target.state.opened > f || !ownedReceivedMatches(owner, entry) || entry.source != w.source || entry.target != w.target || entry.request != w.request || entry.forwarded != w.forwarded || state.alias != w.receipt.alias || state.alias.at != w.receipt.scheduled || state.alias.latest != w.receipt.latest || state.latest != w.receipt.latest || state.source != w.receipt.source {
		return nil, OwnedWritePermit{}, ErrStaleWork
	}
	if f > state.latest {
		o.count(&o.counters.violations)
		return nil, OwnedWritePermit{}, errCandidateAssumption
	}
	if f >= entry.expires || f < state.alias.at {
		return nil, OwnedWritePermit{}, ErrNotReady
	}
	// Unrelated cleanup debt cannot revoke an already accepted exact row.
	row, err := o.sourceRowLocked(entry)
	if err != nil {
		return nil, OwnedWritePermit{}, err
	}
	pair, err := row.actual.candidatePairTrial(f)
	actual := calendar.actual
	actual.refill(f)
	if err != nil || actual.tokens < forwardToken {
		return nil, OwnedWritePermit{}, errCandidateAssumption
	}
	// All fallible checks precede the bounded actual-debt/start suffix under R.
	actual.tokens -= forwardToken
	row.actual = pair
	calendar.actual = actual
	calendar.last = f
	calendar.forwardDropAliasLocked(state.alias)
	state.alias = nil
	o.last = f
	state.phase = candidateDispatching
	o.count(&o.counters.taken)
	state.forwardEndpointNS, state.forwardEndpointKnown = int64(f), true
	w.forwardEndpointNS, w.forwardEndpointKnown = int64(f), true
	w.phase.Store(ownedForwardStarted)
	return w.payload, OwnedWritePermit{work: w}, nil
}

// Finish is mandatory exactly once after either socket-write outcome. This
// genuine clock-only completion is NOT admission or current-snapshot authority. The
// original Observer was retained before construction escape. A changed End
// generation cannot make original in-flight work impossible to finish.
func (permit OwnedWritePermit) Finish(writeOK bool) error {
	w := permit.work
	if !ownedForwardOriginal(w) {
		return ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedForwardStarted, ownedForwardFinishing) {
		return ErrStaleWork
	}
	defer w.phase.Store(ownedForwardDone)
	owner := w.owner
	o := owner.coordinator
	r := w.receipt.reservation
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if !o.currentLocked(r) || r.entry.source != w.source || r.entry.target != w.target || r.entry.forwarded != w.forwarded {
		return ErrStaleWork
	}
	entry := r.entry
	state := entry.intake
	if state.phase != candidateDispatching && state.phase != candidateTerminalDispatch {
		return ErrStaleWork
	}
	if state.calendar == nil {
		return ErrStaleWork
	}
	state.calendar.mu.Lock()
	defer state.calendar.mu.Unlock()
	// All Node/application waits are over; no W/common/R is held. Release the
	// completion clock immediately after its ONE genuine Sample.
	clock, err := owner.finishObserver.ReserveClock()
	if err != nil {
		o.releaseLocked(entry)
		return err
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		o.releaseLocked(entry)
		return err
	}
	if !owner.consumer.Matches(w.cut.stamp, at) {
		o.releaseLocked(entry)
		return ErrUnauthenticated
	}
	f := Instant(at.Elapsed())
	if f < o.last {
		o.releaseLocked(entry)
		return ErrClock
	}
	row, err := o.sourceRowLocked(entry)
	if err != nil {
		o.releaseLocked(entry)
		return err
	}
	expiry, err := f.plus(candidateGroupWindow)
	if err != nil {
		o.releaseLocked(entry)
		return err
	}
	nextSource := *row
	nextSource.expires = max(nextSource.expires, expiry)
	if err := o.groups.finish(state.cell, f); err != nil {
		o.releaseLocked(entry)
		return err
	}
	*row = nextSource
	o.last = f
	if f > state.latest {
		o.count(&o.counters.violations)
	}
	if state.phase == candidateTerminalDispatch || !writeOK || f >= entry.expires || !entry.source.valid() || !entry.target.valid() {
		o.releaseLocked(entry)
	} else {
		state.phase = candidateAwaitingAnswer
	}
	return nil
}
