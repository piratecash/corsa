package findnode

import "github.com/piratecash/corsa/internal/core/domain"

// Private execution state belongs to one original BUSY claim. A plan is still
// historical; only each candidate's existing C3a final reservation admits work.
type ownedRoundExecution struct {
	claim           *ownedAttemptClaim
	plan            *ownedRoundPlanState
	done            [MaxPendingTotal]bool
	next            int
	cursor          domain.ConnID
	rrCell          int
	rrSession       [candidateGroupCells]domain.ConnID
	groupGeneration uint64
	oldCursor       domain.ConnID
	l1, wrapped     bool
	result          candidateRoundResult
	receipt         ownedRoundSuccessReceipt
	receipts        [MaxPendingTotal]ownedRoundSuccessReceipt
}

// No receipt input, admission conversion, getter or production root exists.
// The value is minted only inside the successful C3a fixed suffix under SAME R.
type ownedRoundSuccessReceipt struct {
	execution         *ownedRoundExecution
	ordinal           int
	reservation       candidateReservation
	round             uint64
	target            Session
	forwarded         domain.FindNodeRequestID
	alias             *forwardSlot
	source            candidateSourceRef
	at                Instant
	scheduled, latest Instant
}

func (attempt ownedAttempt) consumeOwnedRoundPlan(ticket ownedRoundPlan) (candidateRoundResult, error) {
	c := attempt.claim
	if c == nil || c.owner == nil || c.cut == nil || c.cut.owner != c.owner || ticket.claim != c {
		return candidateRoundResult{}, ErrInvalid
	}
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		phase := c.phase.Load()
		if phase == ownedAttemptClosed || phase == ownedAttemptClosing {
			return candidateRoundResult{}, ErrStaleWork
		}
		return candidateRoundResult{}, ErrNotReady
	}
	defer c.phase.Store(ownedAttemptLive)
	plan := c.roundPlan
	if plan == nil || plan.consumed || !c.projected || !c.owner.scratch.borrowed.Load() {
		return candidateRoundResult{}, ErrStaleWork
	}
	if plan.count < 0 || plan.count > MaxPendingTotal {
		return candidateRoundResult{}, ErrInvalid
	}
	// Fixed execution storage and all preparation precede application waits/R.
	execution := &ownedRoundExecution{claim: c, plan: plan, cursor: plan.cursor, rrCell: plan.rrCell, rrSession: plan.rrSession, groupGeneration: plan.groupGeneration, oldCursor: plan.cursor}
	c.execution = execution
	for i := 0; i < plan.count; i++ {
		index := plan.order[i]
		if index < 0 || index >= plan.count {
			return candidateRoundResult{}, ErrInvalid
		}
		for j := 0; j < i; j++ {
			if plan.order[j] == index {
				return candidateRoundResult{}, ErrInvalid
			}
		}
		execution.l1 = execution.l1 || plan.rows[index].priority == PriorityProtected
	}
	plan.consumed = true
	o := c.owner.coordinator
	if o == nil {
		return candidateRoundResult{}, ErrInvalid
	}
	// This defer runs after every per-candidate R and application-lock scope.
	defer o.publishOwnedAttempt()
	o.mu.Lock()
	o.pending.mu.Lock()
	err := validateOwnedRoundExecutionLocked(execution)
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if err != nil {
		refuseOwnedRoundRemainder(execution)
		return execution.result, err
	}
	for ordinal := 0; ordinal < plan.count; ordinal++ {
		execution.next = ordinal
		row := plan.rows[plan.order[ordinal]]
		receipt, finalErr := attempt.finalizeOwnedCandidateBusy(row.reservation, execution)
		if finalErr == nil {
			if err := acceptOwnedRoundReceipt(execution, receipt); err != nil {
				refuseOwnedRoundRemainder(execution)
				return execution.result, err
			}
			continue
		}
		// Refusal cannot roll back a successful fixed suffix. Exact original
		// provisional generations alone may be unlinked; accepted phases survive.
		refuseOwnedRoundRow(execution, ordinal)
		if finalErr != ErrBudget && finalErr != ErrCapacity && finalErr != ErrDuplicateID {
			refuseOwnedRoundRemainder(execution)
			return execution.result, finalErr
		}
	}
	o.mu.Lock()
	o.pending.mu.Lock()
	err = validateOwnedRoundExecutionLocked(execution)
	if err == nil {
		last, e := o.cut.plus(o.profile.slack)
		if e != nil {
			err = e
		} else {
			if execution.wrapped {
				o.watermark = plan.maximum
			} else if !execution.l1 {
				o.cursor, o.watermark = 0, plan.maximum
			}
			if o.last > last {
				o.count(&o.counters.violations)
			}
			o.open = false
		}
	}
	o.pending.mu.Unlock()
	o.mu.Unlock()
	return execution.result, err
}

// Requires original coordinator.mu -> pending.mu, original BUSY claim.
// Compare COMPLETE remaining row set, not just candidate key or row count.
// Accepted rows already covered by exact receipts may progress their lifecycle.
func validateOwnedRoundExecutionLocked(execution *ownedRoundExecution) error {
	if execution == nil || execution.claim == nil || execution.plan == nil {
		return ErrInvalid
	}
	c, plan := execution.claim, execution.plan
	o := c.owner.coordinator
	if c.phase.Load() != ownedAttemptBusy || c.roundPlan != plan || !plan.consumed || !c.projected || !c.owner.scratch.borrowed.Load() {
		return ErrStaleWork
	}
	if !o.open || o.round != plan.round || o.cut != plan.cut || o.generation != plan.generation || o.groups.generation != execution.groupGeneration || o.cursor != execution.cursor || o.watermark != plan.watermark || o.sweep != plan.sweep || o.rrCell != execution.rrCell || o.rrSession != execution.rrSession {
		return ErrStaleWork
	}
	remaining := 0
	for ordinal := 0; ordinal < plan.count; ordinal++ {
		if execution.done[ordinal] {
			continue
		}
		row := plan.rows[plan.order[ordinal]]
		r, entry := row.reservation, row.reservation.entry
		if !o.currentLocked(r) || entry.intake.phase != candidateProvisional || entry.intake.round != plan.round || entry.source != row.source || entry.target != row.target || entry.request != row.request || entry.intake.ownedReceived != row.receipt || entry.intake.ownedIdentity != row.receivedIdentity || !ownedReceivedMatches(c.owner, entry) || entry.expires != row.expires || entry.intake.latest != row.latest || entry.intake.priority != row.offeredPriority || entry.source.state == nil || entry.source.state.opened != row.opened || entry.source.ConnID() != row.connID {
			return ErrStaleWork
		}
		remaining++
	}
	actual := 0
	for _, entry := range o.pending.entries {
		if entry == nil || entry.intake == nil || entry.intake.phase != candidateProvisional {
			continue
		}
		actual++
		found := false
		for ordinal := 0; ordinal < plan.count; ordinal++ {
			if !execution.done[ordinal] && plan.rows[plan.order[ordinal]].reservation.entry == entry {
				found = true
				break
			}
		}
		if !found {
			return ErrStaleWork
		}
	}
	if actual != remaining {
		return ErrStaleWork
	}
	return nil
}

// SAME R and original coordinator/pending/calendar/target locks remain held.
// No fallible check, allocation, lock, callback, clock or diagnostic occurs.
func recordOwnedRoundSuccessLocked(execution *ownedRoundExecution, r candidateReservation, f Instant) {
	row := execution.plan.rows[execution.plan.order[execution.next]]
	o := execution.claim.owner.coordinator
	switch row.priority {
	case PriorityProtected:
		execution.wrapped = execution.wrapped || row.connID <= execution.oldCursor
		o.cursor = row.connID
	case PriorityNew:
		o.rrSession[row.cell] = row.connID
		o.rrCell = (row.cell + 1) % candidateGroupCells
	}
	execution.cursor, execution.rrCell, execution.rrSession = o.cursor, o.rrCell, o.rrSession
	execution.groupGeneration = o.groups.generation
	execution.done[execution.next] = true
	execution.result.reserved++
	execution.receipt = ownedRoundSuccessReceipt{execution: execution, ordinal: execution.next, reservation: r, round: execution.plan.round, target: r.entry.target, forwarded: r.entry.forwarded.ID, alias: r.entry.intake.alias, source: r.entry.intake.source, at: f, scheduled: r.entry.intake.alias.at, latest: r.entry.intake.latest}
	execution.receipts[execution.next] = execution.receipt
}

// Compare only sealed local values minted in that suffix. Never reinterpret a
// mutable row's later phase, alias, correlation or lifecycle after unlocking.
func acceptOwnedRoundReceipt(execution *ownedRoundExecution, receipt ownedRoundSuccessReceipt) error {
	if execution == nil || execution.plan == nil || receipt.execution != execution || receipt != execution.receipt || receipt.ordinal != execution.next || receipt.ordinal < 0 || receipt.ordinal >= execution.plan.count || !execution.done[receipt.ordinal] || receipt.reservation != execution.plan.rows[execution.plan.order[receipt.ordinal]].reservation || receipt.round != execution.plan.round {
		return ErrStaleWork
	}
	return nil
}

func refuseOwnedRoundRow(execution *ownedRoundExecution, ordinal int) {
	o := execution.claim.owner.coordinator
	o.mu.Lock()
	o.pending.mu.Lock()
	refuseOwnedRoundRowLocked(execution, ordinal)
	o.pending.mu.Unlock()
	o.mu.Unlock()
}

// This is cancellation of original provisional work, never end/release of an
// accepted row. Equal-key replacement/new generation and writer phases survive.
func refuseOwnedRoundRowLocked(execution *ownedRoundExecution, ordinal int) {
	if execution.done[ordinal] {
		return
	}
	r := execution.plan.rows[execution.plan.order[ordinal]].reservation
	o := execution.claim.owner.coordinator
	if o.currentLocked(r) && r.entry.intake.phase == candidateProvisional && r.entry.intake.round == execution.plan.round {
		o.pending.release(r.entry)
		o.count(&o.counters.refused)
		execution.result.refused++
	}
	execution.done[ordinal] = true
}

func refuseOwnedRoundRemainder(execution *ownedRoundExecution) {
	o := execution.claim.owner.coordinator
	o.mu.Lock()
	o.pending.mu.Lock()
	// A stale scheduling/row snapshot never gains round-close authority merely
	// because cancelling its own old rows made the pending book look empty.
	for ordinal := 0; ordinal < execution.plan.count; ordinal++ {
		refuseOwnedRoundRowLocked(execution, ordinal)
	}
	o.pending.mu.Unlock()
	o.mu.Unlock()
}
