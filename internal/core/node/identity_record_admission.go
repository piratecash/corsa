package node

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog/log"
)

const relationAdmissionCapacity = 256
const relationAdmissionRateCapacity = 30

type relationObservedTime struct {
	wall    time.Time
	elapsed time.Duration
}
type relationAdmissionLifetime struct{ origin relationObservedTime }

func (l relationAdmissionLifetime) live(now relationObservedTime) bool {
	return now.elapsed >= l.origin.elapsed && now.elapsed-l.origin.elapsed <= identityLookupTaskLifetime && !now.wall.Round(0).After(l.origin.wall.Round(0).Add(identityLookupTaskLifetime))
}

type relationAdmissionPhase uint8

const (
	relationAdmissionFree relationAdmissionPhase = iota
	relationAdmissionProspective
	relationAdmissionVerifiedAwaitSend
	relationAdmissionQueued
	relationAdmissionEarnedOwed
	relationAdmissionStoppedOwed
)

type relationAdmissionRef struct {
	owner      *relationRecordAdmission
	index      uint16
	generation uint64
}
type relationAdmissionRate struct {
	events [relationAdmissionRateCapacity]time.Duration
	count  int
}

type relationAdmissionSlot struct {
	generation        uint64
	phase             relationAdmissionPhase
	processing        bool
	cancelled         bool
	protectionHeld    bool
	label             domain.PeerIdentity
	attemptGeneration uint64
	resolution        *identityResolution
	action            identityLookupActionStamp
	observed          pinOwnerObservation
	lifetime          relationAdmissionLifetime
	registered        time.Duration
	queuedAt          time.Duration
	queueAccepted     bool
	verifiedAt        time.Duration
	verified          verifiedRelationPinRecord
	fifo              uint64
	body              [2048]byte
	bodyLen           int
	signature         [64]byte
	proof             [64]byte
	owed              owedRelationProtection
	completion        relationRecordCompletion
}

// All mutable fields belong exclusively to resolver.mu. Processing pins the
// physical slot until the one joined worker actually returns, even on cancel.
type relationRecordAdmission struct {
	openFiles    func() (identityPinDurableFiles, error)
	clock        func() relationObservedTime
	lastObserved time.Duration
	clockFailed  bool
	started      *relationObservedTime
	closing      bool
	slots        [relationAdmissionCapacity]relationAdmissionSlot
	rate         relationAdmissionRate
	fifo         uint64
	wake         chan struct{}
}

// This private opt-in is deliberately absent from normal resolver construction.
func newIdentityResolverWithRecordAdmission(svc *Service, intents *identityIntentStore, network domain.NetworkID) *identityResolver {
	r := newIdentityResolver(svc, intents, network)
	epoch := time.Now()
	r.recordAdmission = &relationRecordAdmission{clock: func() relationObservedTime {
		now := time.Now()
		return relationObservedTime{wall: now, elapsed: now.Sub(epoch)}
	}, wake: make(chan struct{}, 1), openFiles: svc.localContactPinStore().openIdentityPinFiles}
	return r
}

// sampleLocked requires resolver.mu. Default and injected clocks pass through
// this shared wall-only boundary: strip Go's monotonic reading before retaining
// startup/original/current samples or the opt-in resolution createdAt. Elapsed
// startup/rate/lifetime still use the separate observed elapsed value.
func (a *relationRecordAdmission) sampleLocked() (relationObservedTime, bool) {
	now := a.clock()
	now.wall = now.wall.Round(0)
	if now.elapsed < 0 || now.elapsed < a.lastObserved || now.elapsed == time.Duration(math.MaxInt64) {
		a.clockFailed = true
	}
	if a.clockFailed {
		return now, false
	}
	a.lastObserved = now.elapsed
	return now, true
}
func (a *relationRecordAdmission) rateTrial(now relationObservedTime) (relationAdmissionRate, bool) {
	var next relationAdmissionRate
	if a.started == nil || a.closing || a.clockFailed || now.elapsed < a.started.elapsed || now.elapsed-a.started.elapsed < identityLookupAttemptWindow {
		return next, false
	}
	for i := 0; i < a.rate.count; i++ {
		event := a.rate.events[i]
		if now.elapsed < event {
			return next, false
		}
		if now.elapsed-event < identityLookupAttemptWindow {
			next.events[next.count] = event
			next.count++
		}
	}
	if next.count == len(next.events) {
		return next, false
	}
	next.events[next.count] = now.elapsed
	next.count++
	return next, true
}
func (r *identityResolver) beginRecordAdmissionRun() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.recordAdmission
	if a == nil || a.started != nil || a.closing || r.unavailableLocked() {
		return false
	}
	now, ok := a.sampleLocked()
	if !ok {
		return false
	}
	a.started = &now
	return true
}

// slotLocked requires resolver.mu. A recycled index never accepts an old ref.
func (r *identityResolver) relationSlotLocked(ref *relationAdmissionRef) *relationAdmissionSlot {
	a := r.recordAdmission
	if ref == nil || a == nil || ref.owner != a || int(ref.index) >= len(a.slots) {
		return nil
	}
	slot := &a.slots[ref.index]
	if slot.phase == relationAdmissionFree || slot.generation != ref.generation {
		return nil
	}
	return slot
}

// clearRelationSlotLocked requires resolver.mu and an actually joined slot.
func (r *identityResolver) clearRelationSlotLocked(slot *relationAdmissionSlot) {
	if slot.processing {
		slot.cancelled = true
		return
	}
	if slot.protectionHeld {
		r.svc.releaseRecordProtection(slot.observed.identity)
	}
	generation := slot.generation
	*slot = relationAdmissionSlot{generation: generation}
}
func (r *identityResolver) discardRelationAttemptLocked(entry identityAttemptEntry) {
	if slot := r.relationSlotLocked(entry.recordAdmission); slot != nil && slot.phase < relationAdmissionEarnedOwed {
		r.clearRelationSlotLocked(slot)
	}
}
func (r *identityResolver) expireRelationSlotsLocked(now relationObservedTime) {
	a := r.recordAdmission
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase == relationAdmissionFree || slot.phase >= relationAdmissionEarnedOwed {
			continue
		}
		expired := !slot.lifetime.live(now)
		if slot.phase != relationAdmissionQueued {
			expired = expired || now.elapsed < slot.registered || now.elapsed-slot.registered > identityLookupAttemptWindow
		}
		if expired {
			r.clearRelationSlotLocked(slot)
		}
	}
}
func (r *identityResolver) stopRecordAdmissionLocked() {
	a := r.recordAdmission
	if a == nil {
		return
	}
	a.closing = true
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase == relationAdmissionFree {
			continue
		}
		if slot.processing {
			slot.cancelled = true
			continue
		}
		if slot.phase >= relationAdmissionEarnedOwed {
			r.retainStoppedRelationSlotLocked(slot)
			continue
		}
		r.clearRelationSlotLocked(slot)
	}
}
func (r *identityResolver) wakeRecordAdmissionLocked() {
	select {
	case r.recordAdmission.wake <- struct{}{}:
	default:
	}
}

// selectRelationSlotLocked requires resolver.mu. Only qualified FIFO heads run;
// unverified prospectives share capacity but cannot block a qualified head.
func (r *identityResolver) selectRelationSlotLocked() *relationAdmissionSlot {
	a := r.recordAdmission
	now, ok := a.sampleLocked()
	if !ok || a.closing || r.unavailableLocked() {
		return nil
	}
	r.expireRelationSlotsLocked(now)
	var head *relationAdmissionSlot
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase != relationAdmissionQueued && slot.phase != relationAdmissionEarnedOwed {
			continue
		}
		if head == nil || slot.fifo < head.fifo {
			head = slot
		}
	}
	if head != nil && head.phase == relationAdmissionQueued {
		if _, ready := a.rateTrial(now); !ready {
			return nil
		}
	}
	if head != nil {
		head.processing = true
	}
	return head
}
func (r *identityResolver) runRecordAdmission(ctx context.Context) {
	timer := time.NewTimer(peerSweepInterval)
	defer timer.Stop()
	defer func() { r.mu.Lock(); r.stopRecordAdmissionLocked(); r.mu.Unlock() }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r.mu.Lock()
		slot := r.selectRelationSlotLocked()
		r.mu.Unlock()
		if slot != nil {
			attemptCtx, cancel := context.WithTimeout(ctx, contactRecordAttemptLimit)
			err := r.processRelationSlot(attemptCtx, slot)
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Debug().Err(err).Msg("identity_record_admission_attempt_deferred")
			}
		}
		// A wake never bypasses a whole-pass retry cadence. No detached I/O owner.
		select {
		case <-r.recordAdmission.wake:
		default:
		}
		timer.Reset(peerSweepInterval)
	}
}
func (r *identityResolver) processRelationSlot(ctx context.Context, slot *relationAdmissionSlot) (resultErr error) {
	terminal := false
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		slot.processing = false
		if terminal {
			r.clearRelationSlotLocked(slot)
			return
		}
		if slot.phase >= relationAdmissionEarnedOwed {
			if r.recordAdmission.closing || r.unavailableLocked() {
				r.retainStoppedRelationSlotLocked(slot)
			}
			return
		}
		if slot.cancelled || r.recordAdmission.closing || r.unavailableLocked() {
			r.clearRelationSlotLocked(slot)
		}
	}()
	store := r.svc.localContactPinStore()
	if store == nil {
		terminal = true
		return domain.ErrIdentityPinStoreUnavailable
	}
	files, err := r.recordAdmission.openFiles()
	if err != nil {
		terminal = errors.Is(err, secretfile.ErrDurabilityUnsupported)
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, files.Close()) }()
	if err := files.Preflight(ctx); err != nil {
		terminal = errors.Is(err, secretfile.ErrDurabilityUnsupported)
		return err
	}
	r.mu.Lock()
	earned := slot.phase >= relationAdmissionEarnedOwed
	r.mu.Unlock()
	if !earned {
		disposition, err := r.stageRelationAdmission(ctx, slot)
		if err != nil {
			terminal = !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
			return err
		}
		switch disposition {
		case relationAdmissionWait:
			return nil
		case relationAdmissionContactPath:
			terminal = true
			return nil
		}
	}
	r.mu.Lock()
	owed, previous := slot.owed, slot.completion
	r.mu.Unlock()
	completion, err := r.svc.completeOwedRelationRecord(ctx, owed, previous, files)
	r.mu.Lock()
	slot.completion = completion
	r.mu.Unlock()
	terminal = completion.state == relationRecordHistoricalCompleted || completion.state == relationRecordStaleOwner
	return err
}

// retainStoppedRelationSlotLocked requires resolver.mu AFTER actual worker
// return. Retain only earned history in this same slot; no replay authority.
func (r *identityResolver) retainStoppedRelationSlotLocked(slot *relationAdmissionSlot) {
	if slot.protectionHeld {
		r.svc.releaseRecordProtection(slot.observed.identity)
		slot.protectionHeld = false
	}
	slot.phase = relationAdmissionStoppedOwed
	slot.action = identityLookupActionStamp{}
	slot.resolution = nil
	clear(slot.body[:])
	clear(slot.signature[:])
	clear(slot.proof[:])
	slot.bodyLen = 0
	slot.verified = verifiedRelationPinRecord{}
}
