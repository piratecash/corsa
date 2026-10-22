package findnode

import (
	"math"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Immutable historical ingress identity, not a present R/T permit. The only
// mutable member of its separately allocated original cell is consumed.
type ownedReceivedIdentity struct {
	owner    *dormantIntakeState
	source   Session
	stamp    findnodebridge.CaptureStamp
	received findnodebridge.OwnedInstant
	request  protocol.FindNodeRequest
}
type ownedReceivedState struct {
	identity ownedReceivedIdentity
	consumed atomic.Bool
}
type OwnedReceivedRequest struct{ receivedRequest *ownedReceivedState }

// The strict Node ingress is the sole production caller. It supplies the
// original raw frame and original point sampled before its Node waits.
func (sink ObservationSink) SealReceived(stamp findnodebridge.CaptureStamp, received findnodebridge.OwnedInstant, source Session, raw []byte) (OwnedReceivedRequest, error) {
	owner := sink.observationOwner
	if owner == nil || !owner.consumer.Matches(stamp, received) || !source.valid() || source.state.opened < 0 || source.state.opened > Instant(received.Elapsed()) {
		return OwnedReceivedRequest{}, ErrUnauthenticated
	}
	request, err := protocol.ParseFindNodeRequest(raw)
	if err != nil {
		return OwnedReceivedRequest{}, err
	}
	return OwnedReceivedRequest{receivedRequest: &ownedReceivedState{identity: ownedReceivedIdentity{owner: owner, source: source, stamp: stamp, received: received, request: request}}}, nil
}

// OfferReceived queues original provisional work only. No wire output, source
// debit, target ID, alias, final reservation or mutable receipt getter exists.
func (intake DormantIntake) OfferReceived(input OwnedReceivedRequest, target Session, cut OwnedCut) error {
	owner, cell, sealed := intake.dormantOwner, input.receivedRequest, cut.sealedCut
	if owner == nil || cell == nil || sealed == nil || sealed.owner != owner || cell.identity.owner != owner {
		return ErrUnauthenticated
	}
	identity := cell.identity
	if !owner.consumer.Matches(identity.stamp, identity.received) || !owner.consumer.Matches(sealed.stamp, sealed.at) || sealed.at.Elapsed() < identity.received.Elapsed() || !identity.source.valid() || identity.source.state.opened > Instant(identity.received.Elapsed()) || !target.valid() || target.Identity() != identity.request.Target || target == identity.source {
		return ErrUnauthenticated
	}
	if !cell.consumed.CompareAndSwap(false, true) {
		return ErrStaleWork
	}
	sourceFound, targetFound := false, false
	for _, original := range sealed.originals.members {
		sourceFound = sourceFound || original == identity.source
		targetFound = targetFound || original == target
	}
	if !sourceFound || !targetFound {
		return ErrPrioritySnapshot
	}
	attempt, err := intake.tryOwnedAttempt(cut)
	if err != nil {
		return err
	}
	// This exclusive claim returns to Live before this final cleanup; its result
	// cannot alter admission, accepted work, or the already computed return value.
	defer func() { _ = attempt.releaseOwnedAttempt() }()
	if err = attempt.projectOwnedMembership([]domain.PeerIdentity{identity.request.Target}, []Session{identity.source}); err != nil {
		return err
	}
	priority, err := attempt.ownedHistoricalPriority(identity.source, identity.request.Target)
	if err != nil {
		return err
	}
	c := attempt.claim
	if !c.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		return ErrNotReady
	}
	defer c.phase.Store(ownedAttemptLive)
	// All allocations and application waits precede this offer's genuine R/f.
	state := &candidatePending{priority: priority, ownedReceived: cell, ownedIdentity: identity}
	entry := &pendingEntry{source: identity.source, target: target, request: identity.request, intake: state}
	received := Instant(identity.received.Elapsed())
	o := owner.coordinator
	latest, err := o.profile.timing.LatestForward(received)
	if err != nil {
		return err
	}
	expires, err := received.plus(responderTimeout)
	if err != nil {
		return err
	}
	fixedCut, err := received.plus(o.profile.round)
	if err != nil {
		return err
	}
	state.latest, entry.expires = latest, expires
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	o.count(&o.counters.offered)
	// Historical complete q (or an already authenticated later owner point) may
	// perform existing mandatory housekeeping BEFORE R, including calendar waits.
	housekeeping := max(Instant(sealed.at.Elapsed()), o.last)
	if err = o.advanceLocked(housekeeping); err != nil {
		return err
	}
	lease, err := owner.consumer.TryReadClock(sealed.stamp)
	if err != nil {
		return err
	}
	defer lease.Release()
	point, err := sealed.originals.sampleAndCheckOriginals(lease)
	if err != nil {
		return err
	}
	f := Instant(point.Elapsed())
	if !owner.consumer.Matches(sealed.stamp, point) || point.Elapsed() < sealed.at.Elapsed() || !sealed.validBefore.Contains(point) || f < o.last || cell.identity != identity || !identity.source.valid() || !target.valid() {
		return ErrStaleWork
	}
	// received NEVER changes. An offer cannot extend a collection because its
	// strict parser, class capture or coordinator/pending lock waits were slow.
	if f >= fixedCut || f >= expires || f >= latest || (o.open && (received >= o.cut || f >= o.cut)) {
		return ErrNotReady
	}
	// Refuse conservatively if any original row became cleanup-due after q.
	// Cleanup can acquire calendar.mu, so it must not run under this R. No cap
	// or victim decision may count an expired row, and there is no hidden retry.
	for _, prior := range o.pending.entries {
		if prior != nil && (f >= prior.expires || !prior.source.valid() || !prior.target.valid() || (prior.intake.phase == candidateReserved && f > prior.intake.latest)) {
			return ErrNotReady
		}
	}
	o.last = f
	if o.generation == math.MaxUint64 || (!o.open && o.round == math.MaxUint64) {
		return ErrCapacity
	}
	if priority == PriorityProtected && o.sweep && identity.source.ConnID() > o.watermark {
		if !o.open {
			o.round++
			o.cut = fixedCut
			o.open = true
		}
		return ErrNotReady
	}
	if o.pending.sourceIndexLocked(pendingKey{identity.source, identity.request.ID}) >= 0 {
		return ErrDuplicateID
	}
	for _, prior := range o.pending.entries {
		if prior != nil && prior.source == identity.source && prior.intake.phase == candidateProvisional {
			return ErrCapacity
		}
	}
	if o.pending.sourceCountLocked(identity.source) >= MaxPendingPerSession {
		return ErrCapacity
	}
	fullTarget := o.pending.targetCountLocked(identity.request.Target) >= MaxPendingPerTarget
	index := o.pending.vacancyLocked()
	if fullTarget || o.pending.countLocked() >= MaxPendingTotal {
		var victim *pendingEntry
		for candidateIndex, prior := range o.pending.entries {
			if prior == nil || prior.intake.phase != candidateProvisional || (fullTarget && prior.request.Target != identity.request.Target) {
				continue
			}
			if victim == nil || o.less(victim.source, victim.intake.priority, prior.source, prior.intake.priority) {
				victim = prior
				index = candidateIndex
			}
		}
		if victim == nil || !o.less(identity.source, priority, victim.source, victim.intake.priority) {
			return ErrCapacity
		}
		o.pending.release(victim)
		o.count(&o.counters.replaced)
	}
	if index < 0 || index >= MaxPendingTotal {
		return ErrCapacity
	}
	if !o.open {
		o.round++
		o.cut = fixedCut
		o.open = true
		if !o.sweep {
			o.watermark = owner.scratch.maximum
			o.sweep = true
		}
	}
	o.generation++
	state.phase, state.generation, state.round = candidateProvisional, o.generation, o.round
	o.pending.entries[index] = entry
	return nil
}

// All fields are immutable after construction; pending stores a second exact
// copy, and C3b/c freeze/compare both the original pointer and this full tuple.
func ownedReceivedMatches(owner *dormantIntakeState, entry *pendingEntry) bool {
	if owner == nil || entry == nil || entry.intake == nil {
		return false
	}
	cell, identity := entry.intake.ownedReceived, entry.intake.ownedIdentity
	if cell == nil {
		return owner.consumer.IsFixture()
	}
	return cell.consumed.Load() && cell.identity == identity && identity.owner == owner && identity.source == entry.source && identity.request == entry.request && owner.consumer.Matches(identity.stamp, identity.received) && entry.source.state != nil && entry.source.state.opened <= Instant(identity.received.Elapsed())
}
