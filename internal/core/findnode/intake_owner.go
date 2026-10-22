package findnode

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

var errCandidateAssumption = errors.New("find node: candidate dispatch assumption violated")

type candidatePhase uint8

const (
	candidateProvisional candidatePhase = iota + 1
	candidateReserved
	candidateDispatching
	candidateAwaitingAnswer
	candidateTerminalDispatch
)

type candidateProfile struct {
	timing        TimingProfile
	round, slack  Instant
	nodeCap       int
	initialTokens uint32
}
type candidatePending struct {
	phase             candidatePhase
	generation, round uint64
	latest            Instant
	priority          Priority
	calendar          *ForwardCalendar
	alias             *forwardSlot
	cell              candidateRecordKey
	source            candidateSourceRef
	ownedReceived     *ownedReceivedState
	ownedIdentity     ownedReceivedIdentity
}
type candidateReservation struct {
	owner      *intakeCoordinator
	entry      *pendingEntry
	generation uint64
}
type candidateForwardPermit struct{ reservation candidateReservation }
type candidateWriteOutcome uint8

const (
	candidateWriteFinished candidateWriteOutcome = iota + 1
	candidateWriteFailed
)

type immutableForwardInput struct {
	source, target Session
	request        protocol.FindNodeRequest
}
type candidateRoundResult struct{ reserved, refused int }
type candidateSnapshot struct {
	sampleAt                                                                                            Instant
	round, generation                                                                                   uint64
	cursor, watermark                                                                                   domain.ConnID
	pending, provisional, reserved, dispatching, awaiting, terminal, calendars, records, names, aliases int
	offered, refused, replaced, committed, taken, answered, violations                                  uint64
	saturated                                                                                           bool
}

// intakeCoordinator is a private, disconnected A-core, not full AdmittedAtR.
// Lock order: mu -> pending.mu -> ONE calendar.mu -> target Session.state.mu.
// No I/O, crypto or external callback runs within this ownership domain.
type intakeCoordinator struct {
	mu                sync.Mutex
	pending           *candidatePendingBook
	calendars         [MaxPendingTotal]*ForwardCalendar
	groups            candidateGroups
	sourceOwner       *candidateSourceOwnerToken
	profile           candidateProfile
	capacity          *candidateCapacityEpoch
	last              Instant
	generation, round uint64
	open              bool
	cut               Instant
	cursor, watermark domain.ConnID
	sweep             bool
	rrCell            int
	rrSession         [candidateGroupCells]domain.ConnID
	counters          candidateSnapshot
	published         atomic.Pointer[candidateSnapshot]
}

func newIntakeCoordinator(profile candidateProfile, start Instant) (*intakeCoordinator, error) {
	if start < 0 || profile.round <= 0 || profile.slack < 0 || profile.nodeCap < 512 || profile.nodeCap > 3840 || profile.initialTokens > 4 {
		return nil, ErrInvalid
	}
	total, err := profile.round.plus(profile.slack)
	if err != nil || total > Instant(profile.timing.Runtime) {
		return nil, ErrInvalid
	}
	if _, err = profile.timing.LatestForward(start); err != nil {
		return nil, err
	}
	o := &intakeCoordinator{pending: newCandidatePendingBook(), groups: newCandidateGroups(), sourceOwner: &candidateSourceOwnerToken{}, profile: profile, last: start}
	o.publishLocked()
	return o, nil
}

// Both read helpers require o.mu or sole constructor ownership. No table
// index, view or copied mutex escapes; target belongs to the original object.
func (o *intakeCoordinator) calendarLocked(target domain.PeerIdentity) *ForwardCalendar {
	for _, calendar := range o.calendars {
		if calendar != nil && calendar.target == target {
			return calendar
		}
	}
	return nil
}
func (o *intakeCoordinator) calendarCountLocked() int {
	count := 0
	for _, calendar := range o.calendars {
		if calendar != nil {
			count++
		}
	}
	return count
}
func (o *intakeCoordinator) snapshot() candidateSnapshot {
	if s := o.published.Load(); s != nil {
		return *s
	}
	return candidateSnapshot{}
}
func (o *intakeCoordinator) count(counter *uint64) {
	if *counter < math.MaxUint64 {
		*counter++
	} else {
		o.counters.saturated = true
	}
}
func (o *intakeCoordinator) currentLocked(r candidateReservation) bool {
	if r.owner != o || r.entry == nil || r.entry.intake == nil || r.generation != r.entry.intake.generation {
		return false
	}
	index := o.pending.sourceIndexLocked(pendingKey{r.entry.source, r.entry.request.ID})
	if index < 0 || o.pending.entries[index] != r.entry {
		return false
	}
	if r.entry.intake.phase != candidateProvisional {
		_, err := o.sourceRowLocked(r.entry)
		return err == nil
	}
	return true
}
func (o *intakeCoordinator) handle(entry *pendingEntry) candidateReservation {
	return candidateReservation{o, entry, entry.intake.generation}
}

func (o *intakeCoordinator) offer(source, target Session, request protocol.FindNodeRequest, view candidateMembership, received Instant) (candidateReservation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	o.count(&o.counters.offered)
	if err := o.advanceLocked(received); err != nil {
		return candidateReservation{}, err
	}
	priority, err := view.priority(o, source, request.Target, received)
	if err != nil || !target.valid() || target.Identity() != request.Target || source == target {
		o.count(&o.counters.refused)
		return candidateReservation{}, ErrPrioritySnapshot
	}
	latest, err := o.profile.timing.LatestForward(received)
	if err != nil {
		return candidateReservation{}, err
	}
	expires, err := received.plus(responderTimeout)
	if err != nil {
		return candidateReservation{}, err
	}
	if o.generation == math.MaxUint64 {
		return candidateReservation{}, ErrCapacity
	}
	var cut Instant
	if !o.open {
		if o.round == math.MaxUint64 {
			return candidateReservation{}, ErrCapacity
		}
		cut, err = received.plus(o.profile.round)
		if err != nil {
			return candidateReservation{}, err
		}
	} else if received >= o.cut {
		return candidateReservation{}, ErrNotReady
	}
	if priority == PriorityProtected && o.sweep && source.ConnID() > o.watermark {
		// A newcomer cannot join this sweep, but must not prevent an empty
		// fixed collection interval from eventually renewing its watermark.
		if !o.open {
			o.round++
			o.cut = cut
			o.open = true
		}
		return candidateReservation{}, ErrNotReady
	}
	key := pendingKey{source, request.ID}
	if o.pending.sourceIndexLocked(key) >= 0 {
		return candidateReservation{}, ErrDuplicateID
	}
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if entry.source == source && entry.intake.phase == candidateProvisional {
			return candidateReservation{}, ErrCapacity
		}
	}
	if o.pending.sourceCountLocked(source) >= MaxPendingPerSession {
		return candidateReservation{}, ErrCapacity
	}
	fullTarget := o.pending.targetCountLocked(request.Target) >= MaxPendingPerTarget
	index := o.pending.vacancyLocked()
	if fullTarget || o.pending.countLocked() >= MaxPendingTotal {
		var victim *pendingEntry
		for candidateIndex, entry := range o.pending.entries {
			if entry == nil || entry.intake.phase != candidateProvisional || (fullTarget && entry.request.Target != request.Target) {
				continue
			}
			if victim == nil || o.less(victim.source, victim.intake.priority, entry.source, entry.intake.priority) {
				victim = entry
				index = candidateIndex
			}
		}
		if victim == nil || !o.less(source, priority, victim.source, victim.intake.priority) {
			return candidateReservation{}, ErrCapacity
		}
		o.pending.release(victim)
		o.count(&o.counters.replaced)
	}
	if !o.open {
		o.round++
		o.cut = cut
		o.open = true
		if !o.sweep {
			o.watermark = view.maximum
			o.sweep = true
		}
	}
	o.generation++
	entry := &pendingEntry{source: source, target: target, request: request, expires: expires, intake: &candidatePending{phase: candidateProvisional, generation: o.generation, round: o.round, latest: latest, priority: priority}}
	o.pending.entries[index] = entry
	return o.handle(entry), nil
}

// less requires o.mu. It is a stable total order, not group-inherited priority.
func (o *intakeCoordinator) less(a Session, ap Priority, b Session, bp Priority) bool {
	if ap != bp {
		return ap < bp
	}
	if ap == PriorityProtected {
		aw, bw := a.ConnID() <= o.cursor, b.ConnID() <= o.cursor
		if aw != bw {
			return !aw
		}
		return a.ConnID() < b.ConnID()
	}
	if ap == PriorityAged {
		return candidateOlder(a, b)
	}
	ac, bc := o.groups.orderCell(a.state.group), o.groups.orderCell(b.state.group)
	ad, bd := (ac-o.rrCell+candidateGroupCells)%candidateGroupCells, (bc-o.rrCell+candidateGroupCells)%candidateGroupCells
	if ad != bd {
		return ad < bd
	}
	aw, bw := a.ConnID() <= o.rrSession[ac], b.ConnID() <= o.rrSession[bc]
	if aw != bw {
		return !aw
	}
	return a.ConnID() < b.ConnID()
}
func (o *intakeCoordinator) sealRound(view candidateMembership, now Instant, cuts ...candidateCapacityCut) (candidateRoundResult, error) {
	prepared := o.prepareSources()
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if now < 0 || now < o.last {
		return candidateRoundResult{}, ErrClock
	}
	if !o.open {
		return candidateRoundResult{}, nil
	}
	if now < o.cut {
		return candidateRoundResult{}, ErrNotReady
	}
	last, err := o.cut.plus(o.profile.slack)
	if err != nil {
		return candidateRoundResult{}, err
	}
	if view.owner != o || view.at != now {
		return candidateRoundResult{}, ErrPrioritySnapshot
	}
	if err = o.advanceLocked(now); err != nil {
		return candidateRoundResult{}, err
	}
	// A refused new-window cut closes/refuses provisional work through the
	// normal round path. It never resets or blocks accepted obligations.
	_, capacityErr := o.selectCapacityLocked(now, cuts)
	rows := make([]*pendingEntry, 0, MaxPendingTotal)
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if entry.intake.phase == candidateProvisional {
			rows = append(rows, entry)
		}
	}
	result := candidateRoundResult{}
	oldCursor := o.cursor
	wrapped := false
	l1 := false
	for _, entry := range rows {
		priority, e := view.priority(o, entry.source, entry.request.Target, now)
		if e != nil {
			entry.intake.priority = 0
		} else {
			entry.intake.priority = priority
			if priority == PriorityProtected {
				l1 = true
			}
		}
	}
	// Freeze prospective group order before any successful commit allocates names.
	order := make(map[*pendingEntry]int, len(rows))
	for _, entry := range rows {
		order[entry] = o.groups.orderCell(entry.source.state.group)
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.intake.priority != b.intake.priority {
			return a.intake.priority < b.intake.priority
		}
		if a.intake.priority != PriorityNew {
			return o.less(a.source, a.intake.priority, b.source, b.intake.priority)
		}
		ac, bc := order[a], order[b]
		ad, bd := (ac-o.rrCell+candidateGroupCells)%candidateGroupCells, (bc-o.rrCell+candidateGroupCells)%candidateGroupCells
		if ad != bd {
			return ad < bd
		}
		aw, bw := a.source.ConnID() <= o.rrSession[ac], b.source.ConnID() <= o.rrSession[bc]
		if aw != bw {
			return !aw
		}
		return a.source.ConnID() < b.source.ConnID()
	})
	// The lower level gives each accounting cell one turn before its second.
	// These transient references alias the same <=512 physical pending rows.
	var cells [candidateGroupCells][]*pendingEntry
	ordered := make([]*pendingEntry, 0, len(rows))
	for _, entry := range rows {
		if entry.intake.priority != PriorityNew {
			ordered = append(ordered, entry)
			continue
		}
		cell := order[entry]
		cells[cell] = append(cells[cell], entry)
	}
	for len(ordered) < len(rows) {
		for offset := range candidateGroupCells {
			cell := (o.rrCell + offset) % candidateGroupCells
			if len(cells[cell]) == 0 {
				continue
			}
			ordered = append(ordered, cells[cell][0])
			cells[cell][0] = nil
			cells[cell] = cells[cell][1:]
		}
	}
	rows = ordered
	for _, entry := range rows {
		invalid := now > last || entry.intake.priority == 0 || !entry.source.valid() || !entry.target.valid() || (entry.intake.priority == PriorityProtected && entry.source.ConnID() > o.watermark)
		if capacityErr != nil {
			err = capacityErr
		} else if invalid {
			err = ErrBudget
		} else {
			err = o.commitLocked(entry, now, prepared[entry.source])
		}
		if err != nil {
			o.pending.release(entry)
			result.refused++
			o.count(&o.counters.refused)
			continue
		}
		result.reserved++
		o.count(&o.counters.committed)
		switch entry.intake.priority {
		case PriorityProtected:
			wrapped = wrapped || entry.source.ConnID() <= oldCursor
			o.cursor = entry.source.ConnID()
		case PriorityNew:
			cell := order[entry]
			o.rrSession[cell] = entry.source.ConnID()
			o.rrCell = (cell + 1) % candidateGroupCells
		}
	}
	if now > last {
		o.count(&o.counters.violations)
	}
	if wrapped {
		o.watermark = view.maximum
	} else if !l1 {
		o.cursor = 0
		o.watermark = view.maximum
	}
	o.open = false
	return result, nil
}

// commitLocked requires o.mu and pending.mu; acquires ONE calendar then ID lock.
func (o *intakeCoordinator) commitLocked(entry *pendingEntry, now Instant, prepared *candidateSourceWindows) error {
	capacity, err := o.frozenCapacityLocked(now)
	if err != nil {
		return err
	}
	source, err := o.planSourceLocked(entry, now, prepared)
	if err != nil {
		return err
	}
	calendar := o.calendarLocked(entry.request.Target)
	fresh := calendar == nil
	calendarIndex := -1
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
				safe := old.retireableLocked(now)
				old.mu.Unlock()
				if safe {
					calendarIndex = index
					break
				}
			}
			if calendarIndex < 0 {
				return ErrCapacity
			}
		}
		var err error
		calendar, err = newOwnedForwardCalendar(entry.request.Target, now, o.profile.initialTokens)
		if err != nil {
			return err
		}
	}
	calendar.mu.Lock()
	defer calendar.mu.Unlock()
	if !calendar.ownedAliases || calendar.slots != nil {
		return ErrInvalid
	}
	if !o.aliasesMatchLocked(calendar) {
		return ErrInvalid
	}
	plan, err := calendar.planCombinedFloorLocked(now, entry.intake.latest, source.floor)
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
	entry.target.state.mu.Lock()
	defer entry.target.state.mu.Unlock()
	id, err := entry.target.peekNextIDLocked()
	if err != nil {
		return err
	}
	sourceIndex := o.pending.sourceIndexLocked(pendingKey{entry.source, entry.request.ID})
	if !entry.source.valid() || sourceIndex < 0 || o.pending.entries[sourceIndex] != entry {
		return ErrStaleWork
	}
	if o.pending.targetEntryLocked(pendingKey{entry.target, id}) != nil {
		return ErrDuplicateID
	}
	if currentCapacity, capacityErr := o.frozenCapacityLocked(now); capacityErr != nil || currentCapacity != capacity {
		return ErrNotReady
	}
	slot := &forwardSlot{at: plan.slot.at, latest: plan.slot.latest}
	forwarded := entry.request
	forwarded.ID = id
	// Last fallible ownership claim: no source charge has been published yet.
	if source.fresh && !entry.source.state.sourceWindows.CompareAndSwap(nil, source.windows) {
		return ErrStaleWork
	}
	// All validation and checked arithmetic precede this uninterrupted commit.
	source.windows.rows[source.index] = source.row
	entry.intake.source = candidateSourceRef{windows: source.windows, index: source.index, generation: source.row.generation}
	entry.target.commitNextIDLocked(id)
	if fresh {
		o.calendars[calendarIndex] = calendar
	}
	o.groups.commit(group, now)
	calendar.applyCombinedLocked(plan, slot, now)
	entry.forwarded = forwarded
	entry.intake.calendar = calendar
	entry.intake.alias = slot
	entry.intake.cell = group.key
	entry.intake.phase = candidateReserved
	return nil
}

// aliasesMatchLocked requires o.mu, pending.mu and calendar.mu.
func (o *intakeCoordinator) aliasesMatchLocked(calendar *ForwardCalendar) bool {
	n := 0
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if entry.intake.calendar == calendar && entry.intake.alias != nil {
			if !calendar.forwardHasAliasLocked(entry.intake.alias) || entry.intake.phase != candidateReserved || entry.request.Target != calendar.target || entry.intake.alias.latest != entry.intake.latest {
				return false
			}
			n++
		}
	}
	return n == calendar.forwardAliasCountLocked()
}

func (o *intakeCoordinator) takeForward(r candidateReservation, now Instant) (candidateForwardPermit, immutableForwardInput, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if err := o.advanceLocked(now); err != nil {
		return candidateForwardPermit{}, immutableForwardInput{}, err
	}
	if !o.currentLocked(r) || r.entry.intake.phase != candidateReserved {
		return candidateForwardPermit{}, immutableForwardInput{}, ErrStaleWork
	}
	entry := r.entry
	state := entry.intake
	calendar := state.calendar
	calendar.mu.Lock()
	defer calendar.mu.Unlock()
	if now < state.alias.at {
		return candidateForwardPermit{}, immutableForwardInput{}, ErrNotReady
	}
	row, err := o.sourceRowLocked(entry)
	if err != nil {
		return candidateForwardPermit{}, immutableForwardInput{}, err
	}
	pair, pairErr := row.actual.candidatePairTrial(now)
	actual := calendar.actual
	actual.refill(now)
	if pairErr != nil || actual.tokens < forwardToken {
		o.count(&o.counters.violations)
		return candidateForwardPermit{}, immutableForwardInput{}, errCandidateAssumption
	}
	actual.tokens -= forwardToken
	row.actual = pair
	calendar.actual = actual
	calendar.last = now
	calendar.forwardDropAliasLocked(state.alias)
	state.alias = nil
	state.phase = candidateDispatching
	o.count(&o.counters.taken)
	return candidateForwardPermit{r}, immutableForwardInput{source: entry.source, target: entry.target, request: entry.forwarded}, nil
}
func (o *intakeCoordinator) finishForward(permit candidateForwardPermit, now Instant, outcome candidateWriteOutcome) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if outcome != candidateWriteFinished && outcome != candidateWriteFailed {
		return ErrInvalid
	}
	if now < o.last || now < 0 {
		return ErrClock
	}
	if !o.currentLocked(permit.reservation) {
		return ErrStaleWork
	}
	entry := permit.reservation.entry
	state := entry.intake
	if state.phase != candidateDispatching && state.phase != candidateTerminalDispatch {
		return ErrStaleWork
	}
	row, err := o.sourceRowLocked(entry)
	if err != nil {
		return err
	}
	expiry, err := now.plus(candidateGroupWindow)
	if err != nil {
		return err
	}
	nextSource := *row
	nextSource.expires = max(nextSource.expires, expiry)
	if err := o.groups.finish(state.cell, now); err != nil {
		return err
	}
	*row = nextSource
	o.last = now
	if now > state.latest {
		// Completion is an upper bound on actual writer start, not proof of it.
		// Record the conservative overrun without resetting any commitment.
		o.count(&o.counters.violations)
	}
	if state.phase == candidateTerminalDispatch || outcome == candidateWriteFailed || now >= entry.expires || !entry.source.valid() || !entry.target.valid() {
		state.calendar.mu.Lock()
		o.releaseLocked(entry)
		state.calendar.mu.Unlock()
	} else {
		state.phase = candidateAwaitingAnswer
	}
	return nil
}
func (o *intakeCoordinator) answer(target Session, answer protocol.FindNodeAnswer, now Instant) (Delivery, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if err := o.advanceLocked(now); err != nil {
		return Delivery{}, err
	}
	entry := o.pending.targetEntryLocked(pendingKey{target, answer.ID})
	if entry == nil || entry.request.Target != answer.Target || !entry.source.valid() || !target.valid() || (entry.intake.phase != candidateDispatching && entry.intake.phase != candidateAwaitingAnswer) {
		return Delivery{}, ErrUnmatchedAnswer
	}
	if _, err := o.sourceRowLocked(entry); err != nil {
		return Delivery{}, ErrUnmatchedAnswer
	}
	answer.ID = entry.request.ID
	delivery := Delivery{Session: entry.source, Answer: answer}
	o.count(&o.counters.answered)
	if entry.intake.phase == candidateDispatching {
		entry.intake.phase = candidateTerminalDispatch
	} else {
		entry.intake.calendar.mu.Lock()
		o.releaseLocked(entry)
		entry.intake.calendar.mu.Unlock()
	}
	return delivery, nil
}
func (o *intakeCoordinator) cancel(r candidateReservation, now Instant) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	err := o.advanceLocked(now)
	if !o.currentLocked(r) {
		return false, err
	}
	o.endLocked(r.entry)
	return true, err
}
func (o *intakeCoordinator) closeSession(source Session, now Instant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	err := o.advanceLocked(now)
	source.close()
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if entry.source == source || entry.target == source {
			o.endLocked(entry)
		}
	}
	return err
}
func (o *intakeCoordinator) expire(now Instant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	return o.advanceLocked(now)
}

// endLocked requires o.mu and pending.mu; acquires at most one calendar.mu.
func (o *intakeCoordinator) endLocked(entry *pendingEntry) {
	state := entry.intake
	if state.phase == candidateDispatching || state.phase == candidateTerminalDispatch {
		state.phase = candidateTerminalDispatch
		return
	}
	if state.calendar != nil {
		state.calendar.mu.Lock()
		defer state.calendar.mu.Unlock()
	}
	o.releaseLocked(entry)
}

// releaseLocked requires o.mu, pending.mu and the row's calendar.mu if nonnil.
func (o *intakeCoordinator) releaseLocked(entry *pendingEntry) {
	state := entry.intake
	if state.calendar != nil {
		if state.alias != nil {
			state.calendar.forwardDropAliasLocked(state.alias)
			state.alias = nil
		}
		o.groups.release(state.cell)
	}
	o.releaseSourceLocked(entry)
	o.pending.release(entry)
}

// advanceLocked requires o.mu and pending.mu; no invalid-time expiry authority.
func (o *intakeCoordinator) advanceLocked(now Instant) error {
	if now < 0 || now < o.last {
		for _, entry := range o.pending.entries {
			if entry == nil {
				continue
			}
			if !entry.source.valid() || !entry.target.valid() {
				o.endLocked(entry)
			}
		}
		return ErrClock
	}
	o.last = now
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if now >= entry.expires || !entry.source.valid() || !entry.target.valid() || (entry.intake.phase == candidateReserved && now > entry.intake.latest) {
			o.endLocked(entry)
		}
	}
	o.groups.expire(now)
	return nil
}
func (o *intakeCoordinator) resetGroups(now Instant) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	if err := o.advanceLocked(now); err != nil {
		return false
	}
	return o.groups.reset(now, o.pending.countLocked())
}

// publishLocked requires o.mu, or sole constructor ownership. Call only after
// releasing pending/calendar/Session locks; it takes them in the same order.
func (o *intakeCoordinator) publishLocked() {
	snapshot := o.counters
	snapshot.sampleAt = o.last
	snapshot.round = o.round
	snapshot.generation = o.groups.generation
	snapshot.cursor = o.cursor
	snapshot.watermark = o.watermark
	snapshot.calendars = o.calendarCountLocked()
	snapshot.names, snapshot.records = o.groups.counts()
	o.pending.mu.Lock()
	snapshot.pending = o.pending.countLocked()
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		switch entry.intake.phase {
		case candidateProvisional:
			snapshot.provisional++
		case candidateReserved:
			snapshot.reserved++
		case candidateDispatching:
			snapshot.dispatching++
		case candidateAwaitingAnswer:
			snapshot.awaiting++
		case candidateTerminalDispatch:
			snapshot.terminal++
		}
	}
	for _, calendar := range o.calendars {
		if calendar == nil {
			continue
		}
		calendar.mu.Lock()
		snapshot.aliases += calendar.forwardAliasCountLocked()
		calendar.mu.Unlock()
	}
	o.pending.mu.Unlock()
	o.published.Store(&snapshot)
}
