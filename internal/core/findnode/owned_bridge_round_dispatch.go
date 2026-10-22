package findnode

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"sync/atomic"
)

// These factories retain the ORIGINAL completion clock role before escape.
// Completion is not admission; ordinary dormant factories remain unchanged.
func NewExecutingIntake(consumer findnodebridge.Consumer, observer findnodebridge.Observer, profile DormantIntakeProfile, maxMembers int) (DormantIntake, ObservationSink, error) {
	intake, sink, err := NewDormantIntake(consumer, profile, maxMembers)
	if err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	intake.dormantOwner.finishObserver = observer
	return intake, sink, nil
}
func NewExecutingFixtureIntake(consumer findnodebridge.Consumer, observer findnodebridge.Observer, profile DormantIntakeProfile, maxMembers int) (DormantIntake, ObservationSink, error) {
	intake, sink, err := NewDormantFixtureIntake(consumer, profile, maxMembers)
	if err != nil {
		return DormantIntake{}, ObservationSink{}, err
	}
	intake.dormantOwner.finishObserver = observer
	return intake, sink, nil
}

type OwnedRoundBatch struct{ batch *ownedRoundBatchState }
type ownedRoundBatchState struct {
	owner *dormantIntakeState
	works [MaxPendingTotal]OwnedForwardWork
	count uint32
	next  atomic.Uint32
}

// Next transfers each opaque original work cell at most once. Routing metadata
// is not a T permit; only the actual original writer may attempt Take.
func (batch OwnedRoundBatch) Next() (OwnedForwardWork, bool) {
	b := batch.batch
	if b == nil {
		return OwnedForwardWork{}, false
	}
	for {
		n := b.next.Load()
		if n >= b.count {
			return OwnedForwardWork{}, false
		}
		if b.next.CompareAndSwap(n, n+1) {
			return b.works[n], true
		}
	}
}

// CloseOwnedRound drives the actual C3b snapshot and C3c consumer under the
// original sole owner lane. It also returns all earlier exact successes when a
// later candidate refuses; those successful reservations must never be lost.
func (intake DormantIntake) CloseOwnedRound(cut OwnedCut) (OwnedRoundBatch, error) {
	attempt, err := intake.tryOwnedAttempt(cut)
	if err != nil {
		return OwnedRoundBatch{}, err
	}
	// This exclusive claim returns to Live before this final cleanup; its result
	// cannot alter admission, accepted work, or the already computed return value.
	defer func() { _ = attempt.releaseOwnedAttempt() }()
	owner := attempt.claim.owner
	o := owner.coordinator
	var targets [MaxPendingTotal]domain.PeerIdentity
	var sources [MaxPendingTotal]Session
	count := 0
	o.mu.Lock()
	o.pending.mu.Lock()
	err = o.advanceLocked(Instant(cut.sealedCut.at.Elapsed()))
	if err == nil {
		for _, entry := range o.pending.entries {
			if entry == nil || entry.intake == nil || entry.intake.phase != candidateProvisional {
				continue
			}
			if count >= MaxPendingTotal {
				err = ErrCapacity
				break
			}
			targets[count], sources[count] = entry.request.Target, entry.source
			count++
		}
	}
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if err != nil {
		return OwnedRoundBatch{}, err
	}
	if err = attempt.projectOwnedMembership(targets[:count], sources[:count]); err != nil {
		return OwnedRoundBatch{}, err
	}
	plan, err := attempt.prepareOwnedRoundPlan()
	if err != nil {
		return OwnedRoundBatch{}, err
	}
	result, consumeErr := attempt.consumeOwnedRoundPlan(plan)
	execution := attempt.claim.execution
	if execution == nil || result.reserved < 0 || result.reserved > MaxPendingTotal {
		return OwnedRoundBatch{}, ErrStaleWork
	}
	b := &ownedRoundBatchState{owner: owner}
	for ordinal := 0; ordinal < execution.plan.count; ordinal++ {
		receipt := execution.receipts[ordinal]
		if receipt.execution == nil {
			continue
		}
		if receipt.execution != execution || receipt.ordinal != ordinal || !execution.done[ordinal] || receipt.reservation != execution.plan.rows[execution.plan.order[ordinal]].reservation {
			return OwnedRoundBatch{batch: b}, ErrStaleWork
		}
		original := execution.plan.rows[execution.plan.order[ordinal]]
		forwarded := original.request
		forwarded.ID = receipt.forwarded
		b.works[b.count] = OwnedForwardWork{work: &ownedForwardState{owner: owner, cut: cut.sealedCut, receipt: receipt, source: original.source, target: original.target, request: original.request, forwarded: forwarded}}
		b.count++
	}
	if int(b.count) != result.reserved {
		return OwnedRoundBatch{batch: b}, ErrStaleWork
	}
	return OwnedRoundBatch{batch: b}, consumeErr
}

// Scheduler hint only. It authorizes no sample/admission/round mutation.
func (intake DormantIntake) OwnedRoundPending() bool {
	if intake.dormantOwner == nil {
		return false
	}
	o := intake.dormantOwner.coordinator
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.open
}

// The original Node End published Session.closed under its existing W first.
// Called only AFTER W and common unlock. Keep in-flight writer generations for
// mandatory Finish; no caller-supplied now or debt refund exists.
func (intake DormantIntake) EndOriginalSession(session Session) error {
	owner := intake.dormantOwner
	if owner == nil || session.state == nil || session.valid() {
		return ErrUnauthenticated
	}
	o := owner.coordinator
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	for _, entry := range o.pending.entries {
		if entry != nil && (entry.source == session || entry.target == session) {
			o.endLocked(entry)
		}
	}
	return nil
}
