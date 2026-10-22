package findnode

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Historical preparation only. No admission, lease, shared round mutation or
// iterable public view is returned. Copies retain the same original claim.
type ownedRoundPlan struct{ claim *ownedAttemptClaim }
type ownedRoundPlanRow struct {
	reservation               candidateReservation
	source, target            Session
	request                   protocol.FindNodeRequest
	receipt                   *ownedReceivedState
	receivedIdentity          ownedReceivedIdentity
	expires, latest, opened   Instant
	connID                    domain.ConnID
	priority, offeredPriority Priority
	cell, cellRank            int
	beyondWatermark           bool
}
type ownedRoundPlanState struct {
	rows                               [MaxPendingTotal]ownedRoundPlanRow
	order                              [MaxPendingTotal]int
	count                              int
	round, generation, groupGeneration uint64
	consumed                           bool
	cut                                Instant
	cursor, watermark, maximum         domain.ConnID
	sweep                              bool
	rrCell                             int
	rrSession                          [candidateGroupCells]domain.ConnID
	cellRanks                          [candidateGroupCells]int
}

func (attempt ownedAttempt) prepareOwnedRoundPlan() (ownedRoundPlan, error) {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner {
		return ownedRoundPlan{}, ErrInvalid
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return ownedRoundPlan{}, ErrStaleWork
		}
		return ownedRoundPlan{}, ErrNotReady
	}
	defer c.phase.Store(ownedAttemptLive)
	owner, cut := c.owner, c.cut
	if !c.projected || !owner.scratch.borrowed.Load() {
		return ownedRoundPlan{}, ErrNotReady
	}
	if c.roundPlan != nil {
		return ownedRoundPlan{}, ErrInvalid
	}
	if owner.coordinator == nil || !owner.consumer.Matches(cut.stamp, cut.at) {
		return ownedRoundPlan{}, ErrUnauthenticated
	}
	// One fixed local workbook is allocated before application locks, not a new
	// authoritative book. Failed preparation never exposes its row references.
	plan := &ownedRoundPlanState{}
	o := owner.coordinator
	o.mu.Lock()
	o.pending.mu.Lock()
	err := attempt.copyOwnedRoundPlanLocked(plan)
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if err != nil {
		return ownedRoundPlan{}, err
	}
	orderOwnedRoundPlan(plan)
	c.roundPlan = plan
	return ownedRoundPlan{claim: c}, nil
}

// Caller holds this claim BUSY and original coordinator.mu -> pending.mu.
// No cleanup, counter, generation, priority, debt or clock is changed here.
func (attempt ownedAttempt) copyOwnedRoundPlanLocked(plan *ownedRoundPlanState) error {
	c := attempt.claim
	owner, cut := c.owner, c.cut
	o := owner.coordinator
	s := &owner.scratch
	if !o.open {
		return ErrNotReady
	}
	q := Instant(cut.at.Elapsed())
	if q < 0 || o.cut < 0 {
		return ErrClock
	}
	if q < o.cut {
		return ErrNotReady
	}
	if s.targetCount < 0 || s.targetCount > len(s.targets) || s.offeredCount < 0 || s.offeredCount > len(s.offered) || cut.originals.members == nil || len(cut.originals.members) > owner.maxMembers || len(cut.classes) != len(cut.originals.members) || o.rrCell < 0 || o.rrCell >= candidateGroupCells {
		return ErrInvalid
	}
	plan.round, plan.cut = o.round, o.cut
	plan.generation, plan.groupGeneration = o.generation, o.groups.generation
	plan.cursor, plan.watermark, plan.maximum = o.cursor, o.watermark, s.maximum
	plan.sweep, plan.rrCell, plan.rrSession = o.sweep, o.rrCell, o.rrSession
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if entry.intake == nil {
			return ErrStaleWork
		}
		if entry.intake.phase != candidateProvisional {
			continue
		}
		if !ownedReceivedMatches(owner, entry) {
			return ErrUnauthenticated
		}
		r := o.handle(entry)
		if !o.currentLocked(r) || entry.intake.round != o.round {
			return ErrStaleWork
		}
		if plan.count >= len(plan.rows) {
			return ErrCapacity
		}
		if entry.source.state == nil || entry.target.state == nil || entry.source == entry.target || entry.target.Identity() != entry.request.Target {
			return ErrPrioritySnapshot
		}
		sourcePresent, targetPresent := false, false
		for _, original := range cut.originals.members {
			sourcePresent = sourcePresent || original == entry.source
			targetPresent = targetPresent || original == entry.target
		}
		if !sourcePresent || !targetPresent {
			return ErrPrioritySnapshot
		}
		class := SessionClass(0)
		for index := 0; index < s.offeredCount; index++ {
			if s.offered[index].session == entry.source {
				class = s.offered[index].class
				break
			}
		}
		targetIndex := -1
		for index := 0; index < s.targetCount; index++ {
			if s.targets[index].identity == entry.request.Target {
				targetIndex = index
				break
			}
		}
		if targetIndex < 0 {
			return ErrPrioritySnapshot
		}
		switch class {
		case ClassOrdinary, ClassContact, ClassSelected, ClassIncomingPinned:
		default:
			return ErrPrioritySnapshot
		}
		priority := PriorityNew
		if class == ClassContact || class == ClassSelected {
			priority = PriorityProtected
		} else {
			for _, original := range s.targets[targetIndex].oldest {
				if original == entry.source {
					priority = PriorityAged
					break
				}
			}
		}
		cell := o.groups.orderCell(entry.source.state.group)
		if cell < 0 || cell >= candidateGroupCells {
			return ErrInvalid
		}
		plan.rows[plan.count] = ownedRoundPlanRow{reservation: r, source: entry.source, target: entry.target, request: entry.request, receipt: entry.intake.ownedReceived, receivedIdentity: entry.intake.ownedIdentity, expires: entry.expires, latest: entry.intake.latest, opened: entry.source.state.opened, connID: entry.source.ConnID(), priority: priority, offeredPriority: entry.intake.priority, cell: cell, beyondWatermark: priority == PriorityProtected && entry.source.ConnID() > plan.watermark}
		plan.order[plan.count] = plan.count
		plan.count++
	}
	return nil
}

// Frozen old comparator before the old per-cell interleave. Only copied values
// are inspected; no methods, mutable lookup, callback or clock participates.
func ownedRoundRowLess(plan *ownedRoundPlanState, a, b ownedRoundPlanRow) bool {
	if a.priority != b.priority {
		return a.priority < b.priority
	}
	if a.priority == PriorityProtected {
		aw, bw := a.connID <= plan.cursor, b.connID <= plan.cursor
		if aw != bw {
			return !aw
		}
		return a.connID < b.connID
	}
	if a.priority == PriorityAged {
		if a.opened != b.opened {
			return a.opened < b.opened
		}
		return a.connID < b.connID
	}
	ad, bd := (a.cell-plan.rrCell+candidateGroupCells)%candidateGroupCells, (b.cell-plan.rrCell+candidateGroupCells)%candidateGroupCells
	if ad != bd {
		return ad < bd
	}
	aw, bw := a.connID <= plan.rrSession[a.cell], b.connID <= plan.rrSession[b.cell]
	if aw != bw {
		return !aw
	}
	return a.connID < b.connID
}

func orderOwnedRoundPlan(plan *ownedRoundPlanState) {
	for i := 1; i < plan.count; i++ {
		v, j := plan.order[i], i
		for j > 0 && ownedRoundRowLess(plan, plan.rows[v], plan.rows[plan.order[j-1]]) {
			plan.order[j] = plan.order[j-1]
			j--
		}
		plan.order[j] = v
	}
	firstNew := plan.count
	for i := 0; i < plan.count; i++ {
		row := &plan.rows[plan.order[i]]
		if row.priority != PriorityNew {
			continue
		}
		if firstNew == plan.count {
			firstNew = i
		}
		row.cellRank = plan.cellRanks[row.cell]
		plan.cellRanks[row.cell]++
	}
	// Sorting by rank then rotated cell is exactly one turn per nonempty cell
	// before its second. The old within-cell Session rotation is already frozen.
	for i := firstNew + 1; i < plan.count; i++ {
		v, j := plan.order[i], i
		a := plan.rows[v]
		ad := (a.cell - plan.rrCell + candidateGroupCells) % candidateGroupCells
		for j > firstNew {
			b := plan.rows[plan.order[j-1]]
			bd := (b.cell - plan.rrCell + candidateGroupCells) % candidateGroupCells
			if a.cellRank > b.cellRank || (a.cellRank == b.cellRank && ad >= bd) {
				break
			}
			plan.order[j] = plan.order[j-1]
			j--
		}
		plan.order[j] = v
	}
}
