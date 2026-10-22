package findnode

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"sync/atomic"
)

// These values describe named storage, never admission, heap liveness or a
// resource acceptance threshold. Resolve type sizes once, outside every R/T.
// The opaque source-owner token payload is omitted from this partial named
// storage floor; no authority token is constructed merely for sizing.
var ownedResourceSizes = struct {
	owner, pending, calendar, forward, answer, batch, round, cut, member, class, received uint64
	local, cacheEntry, recipient, coordinated, signing, publicKey                         uint64
}{
	owner:    domain.SizeOfAll(dormantIntakeState{}, intakeCoordinator{}, candidatePendingBook{}, candidateCapacityEpoch{}),
	pending:  domain.SizeOfAll(pendingEntry{}, candidatePending{}, ownedReceivedState{}),
	calendar: domain.SizeOfAll(ForwardCalendar{}),
	forward:  domain.SizeOfAll(ownedForwardState{}), answer: domain.SizeOfAll(ownedAnswerState{}),
	batch: domain.SizeOfAll(ownedRoundBatchState{}),
	round: domain.SizeOfAll(ownedAttemptClaim{}, ownedRoundExecution{}, ownedRoundPlanState{}),
	cut:   domain.SizeOfAll(ownedCutState{}), member: domain.SizeOfAll(Session{}), class: domain.SizeOfAll(SessionClass(0)),
	received:    domain.SizeOfAll(ownedReceivedState{}, pendingEntry{}, candidatePending{}),
	local:       domain.SizeOfAll(ownedLocalTargetState{}, TargetCoordinator{}, TargetCache{}, SigningCalendar{}),
	cacheEntry:  domain.SizeOfAll(domain.FindNodeChallenge{}, targetEntry{}),
	recipient:   domain.SizeOfAll(Session{}, domain.FindNodeRequestID(0)),
	coordinated: domain.SizeOfAll(coordinatedSigning{}), signing: domain.SizeOfAll(signingSlot{}, (*signingSlot)(nil), struct{}{}), publicKey: 32,
}

// WorkResourceShape is immutable metadata of an already marshalled one-use
// cell. NamedRoundEstimateBytes counts this cell's named reachable round/cut/
// plan/receipt storage WITHOUT deduplicating shared objects. It is intentionally
// excluded from memory floors: several cells may share one round, and distinct
// rounds may each retain their large fixed arrays. It is not a heap upper bound.
type WorkResourceShape struct {
	CellBytes, PayloadBytes, NamedRoundEstimateBytes uint64
}

func ownedForwardResourceShape(w *ownedForwardState) WorkResourceShape {
	if !ownedForwardOriginal(w) {
		return WorkResourceShape{}
	}
	e := w.receipt.execution
	cut := w.cut
	estimate := ownedResourceSizes.round + ownedResourceSizes.cut
	estimate += uint64(cap(cut.originals.members))*ownedResourceSizes.member + uint64(cap(cut.classes))*ownedResourceSizes.class
	estimate += uint64(e.plan.count) * ownedResourceSizes.received
	return WorkResourceShape{CellBytes: ownedResourceSizes.forward, PayloadBytes: uint64(len(w.payload)), NamedRoundEstimateBytes: estimate}
}
func (queued OwnedQueuedForward) ResourceShape() WorkResourceShape {
	return ownedForwardResourceShape(queued.work)
}
func (queued OwnedQueuedAnswer) ResourceShape() WorkResourceShape {
	a := queued.answer
	if a == nil || a.owner == nil {
		return WorkResourceShape{}
	}
	// The body may share its PublicKey backing with cache/other answers. Only
	// the cell and this unique marshalled payload belong in the item floor.
	return WorkResourceShape{CellBytes: ownedResourceSizes.answer, PayloadBytes: uint64(len(a.payload))}
}

type BatchResourceShape struct {
	BatchBytes, Works, NamedRoundEstimateBytes uint64
}

func (batch OwnedRoundBatch) ResourceShape() BatchResourceShape {
	b := batch.batch
	if b == nil {
		return BatchResourceShape{}
	}
	result := BatchResourceShape{BatchBytes: ownedResourceSizes.batch, Works: uint64(b.count)}
	if b.count > 0 {
		result.NamedRoundEstimateBytes = ownedForwardResourceShape(b.works[0].work).NamedRoundEstimateBytes
	}
	return result
}

// This metric-only value has one operational owner: the existing typed
// NetCore item. Its linear hold/release sites are closed by Source guards.
// Copies grant no admission/clock/budget authority. Do not release on logical
// work cancellation/completion; the item still physically retains its fields.
type OwnedResourceCustody struct {
	owner  *dormantIntakeState
	shape  WorkResourceShape
	answer bool
}
type ownedWriterResourceCounters struct{ forward, answer, cellBytes, payloadBytes, roundEstimate atomic.Int64 }

// Each field is an independent atomic load, not one coherent tuple. During
// hold/release a reader may see a count and bytes from different instants.
// This diagnostic cannot establish count-times-size equality or heap liveness.
type WriterResourceSnapshot struct{ ForwardItems, AnswerItems, CellBytes, PayloadBytes, NamedRoundEstimateBytes uint64 }

func holdOwnedResourceCustody(owner *dormantIntakeState, shape WorkResourceShape, answer bool) OwnedResourceCustody {
	if owner == nil {
		return OwnedResourceCustody{}
	}
	c := &owner.resources
	if answer {
		c.answer.Add(1)
	} else {
		c.forward.Add(1)
	}
	c.cellBytes.Add(int64(shape.CellBytes))
	c.payloadBytes.Add(int64(shape.PayloadBytes))
	c.roundEstimate.Add(int64(shape.NamedRoundEstimateBytes))
	return OwnedResourceCustody{owner: owner, shape: shape, answer: answer}
}
func (queued OwnedQueuedForward) HoldResources() OwnedResourceCustody {
	if queued.work == nil {
		return OwnedResourceCustody{}
	}
	return holdOwnedResourceCustody(queued.work.owner, queued.ResourceShape(), false)
}
func (queued OwnedQueuedAnswer) HoldResources() OwnedResourceCustody {
	if queued.answer == nil {
		return OwnedResourceCustody{}
	}
	return holdOwnedResourceCustody(queued.answer.owner, queued.ResourceShape(), true)
}
func (held OwnedResourceCustody) Release() {
	if held.owner == nil {
		return
	}
	c := &held.owner.resources
	s := held.shape
	if held.answer {
		c.answer.Add(-1)
	} else {
		c.forward.Add(-1)
	}
	c.cellBytes.Add(-int64(s.CellBytes))
	c.payloadBytes.Add(-int64(s.PayloadBytes))
	c.roundEstimate.Add(-int64(s.NamedRoundEstimateBytes))
}
func ownedResourceNonnegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
func (c *ownedWriterResourceCounters) snapshot() WriterResourceSnapshot {
	return WriterResourceSnapshot{ForwardItems: ownedResourceNonnegative(c.forward.Load()), AnswerItems: ownedResourceNonnegative(c.answer.Load()), CellBytes: ownedResourceNonnegative(c.cellBytes.Load()), PayloadBytes: ownedResourceNonnegative(c.payloadBytes.Load()), NamedRoundEstimateBytes: ownedResourceNonnegative(c.roundEstimate.Load())}
}

type IntakeDiagnosticCounters struct {
	Offered, Refused, Replaced, Committed, Taken, Answered, Violations uint64
	Saturated                                                          bool
}

// ResourceSnapshot reads existing immutable atomic publications. It adds no
// application lock, publication or new mutable snapshot to a writer-start
// suffix. Phase counts refer to the last published intake sample; successful
// Take intentionally waits until completion to publish diagnostic state.
type ResourceSnapshot struct {
	Enabled                                                                                                                            bool
	FixedBytes, PendingEntryBytes, CalendarBytes, LocalFixedBytes, CacheEntryBytes, RecipientBytes, CoordinatedBytes, SigningSlotBytes uint64
	SampleAt                                                                                                                           Instant
	Round, Generation                                                                                                                  uint64
	Pending, Provisional, Reserved, Dispatching, Awaiting, Terminal, Calendars, GroupRecords, TargetNames, Aliases                     int
	Intake                                                                                                                             IntakeDiagnosticCounters
	Writer                                                                                                                             WriterResourceSnapshot
	LocalEnabled                                                                                                                       bool
	Local                                                                                                                              LocalTargetSnapshot
}

func (intake DormantIntake) ResourceSnapshot() ResourceSnapshot {
	owner := intake.dormantOwner
	if owner == nil || owner.coordinator == nil {
		return ResourceSnapshot{}
	}
	s := owner.coordinator.snapshot()
	result := ResourceSnapshot{Enabled: true, Writer: owner.resources.snapshot(), FixedBytes: ownedResourceSizes.owner, PendingEntryBytes: ownedResourceSizes.pending, CalendarBytes: ownedResourceSizes.calendar,
		SampleAt: s.sampleAt, Round: s.round, Generation: s.generation, Pending: s.pending, Provisional: s.provisional, Reserved: s.reserved, Dispatching: s.dispatching, Awaiting: s.awaiting, Terminal: s.terminal, Calendars: s.calendars, GroupRecords: s.records, TargetNames: s.names, Aliases: s.aliases,
		Intake: IntakeDiagnosticCounters{Offered: s.offered, Refused: s.refused, Replaced: s.replaced, Committed: s.committed, Taken: s.taken, Answered: s.answered, Violations: s.violations, Saturated: s.saturated}}
	// localTarget is installed only during construction before Service escape.
	if local := owner.localTarget; local != nil && local.target != nil {
		result.LocalEnabled = true
		result.Local = local.target.Snapshot()
		result.LocalFixedBytes = ownedResourceSizes.local + ownedResourceSizes.publicKey
		result.CacheEntryBytes = ownedResourceSizes.cacheEntry
		result.RecipientBytes = ownedResourceSizes.recipient
		result.CoordinatedBytes = ownedResourceSizes.coordinated
		result.SigningSlotBytes = ownedResourceSizes.signing
	}
	return result
}
