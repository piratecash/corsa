package findnode

import (
	"math"

	"github.com/piratecash/corsa/internal/core/findnodebridge"
)

// finalizeOwnedCandidate is an unused one-candidate joint-commit fragment.
// It does not choose round order, close a round, authenticate received time,
// grant AdmittedAtR/T, or enable the Constructing production bridge.
func (attempt ownedAttempt) finalizeOwnedCandidate(r candidateReservation) error {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner {
		return ErrInvalid
	}
	owner, cut := c.owner, c.cut
	o := owner.coordinator
	if o == nil || r.owner != o || r.entry == nil || !owner.consumer.Matches(cut.stamp, cut.at) {
		return ErrUnauthenticated
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return ErrStaleWork
		}
		return ErrNotReady
	}
	defer c.phase.Store(ownedAttemptLive)
	_, err := attempt.finalizeOwnedCandidateBusy(r, nil)
	return err
}

// Shared C3a body. Only an original BUSY claim enters here; the round driver
// retains that claim across all candidates. A standalone candidate passes nil.
func (attempt ownedAttempt) finalizeOwnedCandidateBusy(r candidateReservation, execution *ownedRoundExecution) (ownedRoundSuccessReceipt, error) {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner || c.phase.Load() != ownedAttemptBusy {
		return ownedRoundSuccessReceipt{}, ErrStaleWork
	}
	if execution != nil && (execution.claim != c || execution.plan == nil || execution.plan != c.roundPlan || execution.next < 0 || execution.next >= execution.plan.count || execution.done[execution.next] || execution.plan.order[execution.next] < 0 || execution.plan.order[execution.next] >= execution.plan.count || execution.plan.rows[execution.plan.order[execution.next]].reservation != r) {
		return ownedRoundSuccessReceipt{}, ErrStaleWork
	}
	owner, cut := c.owner, c.cut
	o := owner.coordinator
	if o == nil || r.owner != o || r.entry == nil || !owner.consumer.Matches(cut.stamp, cut.at) {
		return ownedRoundSuccessReceipt{}, ErrUnauthenticated
	}
	if !c.projected || !owner.scratch.borrowed.Load() || cut.originals.members == nil || len(cut.originals.members) > owner.maxMembers || len(cut.classes) != len(cut.originals.members) || owner.scratch.targetCount < 0 || owner.scratch.targetCount > len(owner.scratch.targets) || owner.scratch.offeredCount < 0 || owner.scratch.offeredCount > len(owner.scratch.offered) {
		return ownedRoundSuccessReceipt{}, ErrNotReady
	}
	q := Instant(cut.at.Elapsed())
	// Registered before every application-lock defer: diagnostics reacquire the
	// coordinator only AFTER the final lease and all application locks release.
	defer o.publishOwnedAttempt()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if execution != nil {
		if err := validateOwnedRoundExecutionLocked(execution); err != nil {
			return ownedRoundSuccessReceipt{}, err
		}
	}
	if !o.currentLocked(r) || r.entry.intake.phase != candidateProvisional || !o.open || r.entry.intake.round != o.round {
		return ownedRoundSuccessReceipt{}, ErrStaleWork
	}
	if q < 0 {
		return ownedRoundSuccessReceipt{}, ErrClock
	}
	// q is historical preparation/cleanup authority, never the final sample f.
	// A prior candidate may already have advanced last past q. Never backdate
	// cleanup: skip it in that case and validate actual f below. Expired debt
	// found by final trials conservatively refuses, without an in-lease sweep.
	if q >= o.last {
		if err := o.advanceLocked(q); err != nil {
			return ownedRoundSuccessReceipt{}, err
		}
	}
	if !o.currentLocked(r) || r.entry.intake.phase != candidateProvisional || !o.open || r.entry.intake.round != o.round {
		return ownedRoundSuccessReceipt{}, ErrStaleWork
	}
	if execution != nil {
		if err := validateOwnedRoundExecutionLocked(execution); err != nil {
			return ownedRoundSuccessReceipt{}, err
		}
	}
	entry := r.entry
	if entry.source.state == nil || entry.target.state == nil || entry.source == entry.target || entry.target.Identity() != entry.request.Target {
		return ownedRoundSuccessReceipt{}, ErrPrioritySnapshot
	}
	// The exact source and target handles, not equal IDs, must be originals.
	sourcePresent, targetPresent := false, false
	for _, original := range cut.originals.members {
		sourcePresent = sourcePresent || original == entry.source
		targetPresent = targetPresent || original == entry.target
	}
	if !sourcePresent || !targetPresent {
		return ownedRoundSuccessReceipt{}, ErrPrioritySnapshot
	}
	priority := Priority(0)
	class := SessionClass(0)
	for index := 0; index < owner.scratch.offeredCount; index++ {
		if owner.scratch.offered[index].session == entry.source {
			class = owner.scratch.offered[index].class
			break
		}
	}
	targetIndex := -1
	for index := 0; index < owner.scratch.targetCount; index++ {
		if owner.scratch.targets[index].identity == entry.request.Target {
			targetIndex = index
			break
		}
	}
	if targetIndex < 0 {
		return ownedRoundSuccessReceipt{}, ErrPrioritySnapshot
	}
	switch class {
	case ClassOrdinary, ClassContact, ClassSelected, ClassIncomingPinned:
	default:
		return ownedRoundSuccessReceipt{}, ErrPrioritySnapshot
	}
	priority = PriorityNew
	if class == ClassContact || class == ClassSelected {
		priority = PriorityProtected
	} else {
		for _, original := range owner.scratch.targets[targetIndex].oldest {
			if original == entry.source {
				priority = PriorityAged
				break
			}
		}
	}
	if priority == PriorityProtected && entry.source.ConnID() > o.watermark {
		return ownedRoundSuccessReceipt{}, ErrBudget
	}
	// These unpublished objects are prepared before any final reservation.
	prepared := &candidateSourceWindows{owner: o.sourceOwner}
	slot := &forwardSlot{}
	var capacityCuts [1]candidateCapacityCut
	calendar := o.calendarLocked(entry.request.Target)
	fresh := calendar == nil
	calendarIndex := -1
	var retired *ForwardCalendar
	if fresh {
		for index, old := range o.calendars {
			if old == nil {
				calendarIndex = index
				break
			}
		}
		if calendarIndex < 0 {
			for index, old := range o.calendars {
				if o.pending.targetCountLocked(old.target) != 0 {
					continue
				}
				old.mu.Lock()
				safe := old.retireableLocked(q)
				if safe {
					calendarIndex = index
					retired = old
					// Hold the donor through replacement. The new calendar is
					// unpublished, so this remains ONE held calendar mutex.
					defer old.mu.Unlock()
					break
				}
				old.mu.Unlock()
			}
			if calendarIndex < 0 {
				return ownedRoundSuccessReceipt{}, ErrCapacity
			}
		}
		var err error
		calendar, err = newOwnedForwardCalendar(entry.request.Target, q, o.profile.initialTokens)
		if err != nil {
			return ownedRoundSuccessReceipt{}, err
		}
	}
	if retired == nil {
		calendar.mu.Lock()
		defer calendar.mu.Unlock()
	}
	if !calendar.ownedAliases || calendar.slots != nil || !o.aliasesMatchLocked(calendar) {
		return ownedRoundSuccessReceipt{}, ErrInvalid
	}
	entry.target.state.mu.Lock()
	defer entry.target.state.mu.Unlock()
	lease, err := owner.consumer.TryReadClock(cut.stamp)
	if err != nil {
		return ownedRoundSuccessReceipt{}, err
	}
	err = attempt.commitOwnedFinalLocked(r, calendar, retired, calendarIndex, fresh, prepared, slot, &capacityCuts, priority, lease, execution)
	lease.Release()
	if err != nil {
		return ownedRoundSuccessReceipt{}, err
	}
	if execution != nil {
		return execution.receipt, nil
	}
	return ownedRoundSuccessReceipt{}, nil
}

// commitOwnedFinalLocked is called only by finalizeOwnedCandidate with its
// original coordinator/pending/one-calendar/target locks, BUSY claim and SAME
// live reservation. It neither releases nor retains that reservation. Every
// return goes directly to the caller's unconditional Release; no callback,
// defer registration, allocation, application wait or clock resample follows.
func (attempt ownedAttempt) commitOwnedFinalLocked(r candidateReservation, calendar, retired *ForwardCalendar, calendarIndex int, fresh bool, prepared *candidateSourceWindows, slot *forwardSlot, capacityCuts *[1]candidateCapacityCut, priority Priority, lease *findnodebridge.CommitReservation, execution *ownedRoundExecution) error {
	c := attempt.claim
	owner, cut := c.owner, c.cut
	o := owner.coordinator
	entry := r.entry
	q := Instant(cut.at.Elapsed())
	at, err := cut.originals.sampleAndCheckOriginals(lease)
	if err != nil {
		return err
	}
	if !ownedReceivedMatches(owner, r.entry) {
		return ErrUnauthenticated
	}
	if !owner.consumer.Matches(cut.stamp, at) || !at.NotBefore(cut.at) || !cut.validBefore.Contains(at) {
		return ErrStaleWork
	}
	f := Instant(at.Elapsed())
	if f < o.last {
		return ErrClock
	}
	if !o.currentLocked(r) || entry.intake.phase != candidateProvisional || entry.intake.round != o.round || !o.open {
		return ErrStaleWork
	}
	last, err := o.cut.plus(o.profile.slack)
	if err != nil {
		return err
	}
	if f < o.cut {
		return ErrNotReady
	}
	if f > last || f >= entry.expires || f > entry.intake.latest {
		return ErrBudget
	}
	if retired != nil && (o.calendars[calendarIndex] != retired || o.pending.targetCountLocked(retired.target) != 0 || !retired.retireableLocked(f)) {
		return ErrCapacity
	}
	if o.capacity == nil || o.capacity.owner != o || o.capacity.domain == nil {
		return ErrInvalid
	}
	boundSource := entry.source.state.sourceWindows.Load()
	if boundSource != nil && boundSource.owner != o.sourceOwner {
		return ErrStaleWork
	}
	// Genuine same-owner cut conversion. No external completeness/degree bool.
	capacityCuts[0] = candidateCapacityCut{owner: o, domain: o.capacity.domain, at: q, degree: cut.degree.Window, complete: true}
	capacity, err := o.selectCapacityLocked(f, capacityCuts[:])
	if err != nil {
		return err
	}
	// Honest time/epoch housekeeping may survive a later budget refusal.
	o.last = f
	if fresh {
		// Allocation was early, creation accounting is at actual f. Do not
		// earn refill credit between the preparatory q and this final point.
		bucket := forwardBucket{at: f, tokens: Instant(o.profile.initialTokens) * forwardToken}
		calendar.projected, calendar.actual, calendar.last = bucket, bucket, f
	}
	source, err := o.planSourceLocked(entry, f, prepared)
	if err != nil {
		return err
	}
	plan, err := calendar.planCombinedFloorLocked(f, entry.intake.latest, source.floor)
	if err != nil {
		return err
	}
	source, err = source.finalizeSourceLocked(plan.slot.at)
	if err != nil {
		return err
	}
	group, err := o.groups.plan(entry.source.state.group, entry.request.Target, plan.slot.at, capacity)
	if err != nil {
		return err
	}
	record, _ := o.groups.record(group.key)
	if record.refs == math.MaxInt {
		return ErrCapacity
	}
	id, err := entry.target.peekNextIDLocked()
	if err != nil {
		return err
	}
	if o.pending.targetEntryLocked(pendingKey{entry.target, id}) != nil {
		return ErrDuplicateID
	}
	slot.at, slot.latest = plan.slot.at, plan.slot.latest
	forwarded := entry.request
	forwarded.ID = id
	// LAST fallible ownership claim. Nothing below may fail or acquire a lock.
	if source.fresh && !entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows) {
		return ErrStaleWork
	}
	source.windows.rows[source.index] = source.row
	entry.intake.source = candidateSourceRef{windows: source.windows, index: source.index, generation: source.row.generation}
	entry.target.commitNextIDLocked(id)
	if fresh {
		o.calendars[calendarIndex] = calendar
	}
	o.groups.commit(group, f)
	calendar.applyCombinedLocked(plan, slot, f)
	entry.forwarded = forwarded
	entry.intake.calendar = calendar
	entry.intake.alias = slot
	entry.intake.cell = group.key
	entry.intake.priority = priority
	entry.intake.phase = candidateReserved
	o.count(&o.counters.committed)
	if execution != nil {
		recordOwnedRoundSuccessLocked(execution, r, f)
	}
	return nil
}

// publishOwnedAttempt is called by a defer registered before the final lock
// scope. It reacquires ordinary ownership only after that scope has unwound.
func (o *intakeCoordinator) publishOwnedAttempt() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.publishLocked()
}
