package node

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/rs/zerolog/log"
)

const relationAdmissionCapacity = 256
const relationAdmissionRateCapacity = 30

// Immutable class authority; presence of the shared owner enables no class.
type recordAdmissionClasses uint8

const (
	recordAdmissionNone       recordAdmissionClasses = 0
	recordAdmissionChargedR   recordAdmissionClasses = 1 << 0
	recordAdmissionImmediateK recordAdmissionClasses = 1 << 1
)

func (classes recordAdmissionClasses) valid() bool {
	return classes & ^(recordAdmissionChargedR|recordAdmissionImmediateK) == 0
}
func (a *relationRecordAdmission) admitsR() bool {
	return a != nil && a.classes.valid() && a.classes&recordAdmissionChargedR != 0
}
func (a *relationRecordAdmission) admitsK() bool {
	return a != nil && a.classes.valid() && a.classes&recordAdmissionImmediateK != 0
}

// K provenance is immutable construction authority. Zero preserves the existing
// push-only private factory; reconciliation cannot mint transport push handles.
type contactRecordAdmissionSource uint8

const (
	contactRecordPushSource contactRecordAdmissionSource = iota
	contactRecordReconciliationSource
)

func (source contactRecordAdmissionSource) valid() bool {
	return source == contactRecordPushSource || source == contactRecordReconciliationSource
}
func (a *relationRecordAdmission) admitsContactPush() bool {
	return a.admitsK() && a.kSource == contactRecordPushSource
}
func (a *relationRecordAdmission) admitsContactReconciliation() bool {
	return a.admitsK() && a.kSource == contactRecordReconciliationSource
}

// Private construction dependency, not stage authority. Every production
// constructor supplies nil borrowedKFiles; only a named fixture supplies files.
type recordAdmissionConstruction struct {
	classes        recordAdmissionClasses
	kSource        contactRecordAdmissionSource
	borrowedKFiles identityPinDurableFiles
}

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
	relationAdmissionImmediateKProcessing
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
	kAuthority        *contactRecordStageAuthority
	kReconciliation   *contactRecordReconciliationAuthority
	r2Claim           *ownDMRelationClaim
	r2Role            uint8
	r2Verified        verifiedOwnDMRelationRecord
}

// Classes/service/resolver/network are immutable. kStageMu owns K readiness and
// authority verdicts; remaining mutable fields belong to resolver.mu. Processing pins the
// physical slot until the one joined worker actually returns, even on cancel.
type relationRecordAdmission struct {
	classes         recordAdmissionClasses
	kSource         contactRecordAdmissionSource
	service         *Service
	resolver        *identityResolver
	network         domain.NetworkID
	kStageMu        sync.Mutex
	kReady          bool
	kClosing        bool
	kFiles          identityPinDurableFiles // immutable borrowed fixture dependency; nil in every production constructor
	openFiles       func() (identityPinDurableFiles, error)
	clock           func() relationObservedTime
	lastObserved    time.Duration
	clockFailed     bool
	started         *relationObservedTime
	closing         bool
	slots           [relationAdmissionCapacity]relationAdmissionSlot
	rate            relationAdmissionRate
	fifo            uint64
	r2PendingCursor uint16
	wake            chan struct{}
}

// This private opt-in is deliberately absent from normal resolver construction.
func newIdentityResolverWithRecordAdmission(svc *Service, intents *identityIntentStore, network domain.NetworkID) *identityResolver {
	r := newIdentityResolver(svc, intents, network)
	epoch := time.Now()
	r.recordAdmission = &relationRecordAdmission{classes: recordAdmissionChargedR, service: svc, resolver: r, network: network, clock: func() relationObservedTime {
		now := time.Now()
		return relationObservedTime{wall: now, elapsed: now.Sub(epoch)}
	}, wake: make(chan struct{}, 1), openFiles: svc.localContactPinStore().openIdentityPinFiles}
	return r
}

// Constructor identity only: transport proof custody must not vary with Run
// readiness or closing between the original dial and its proof binding.
func (s *Service) hasContactRecordPushTransportCapability() bool {
	if s == nil || s.identityResolver == nil {
		return false
	}
	r := s.identityResolver
	a := r.recordAdmission
	return a.admitsContactPush() && r.svc == s && a.service == s && a.resolver == r && a.network == r.network && a.network == domain.NetworkID(networkName)
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
	if !a.admitsR() || a.started != nil || a.closing || r.unavailableLocked() {
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
	if ref == nil || !a.admitsR() || ref.owner != a || int(ref.index) >= len(a.slots) {
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
		if slot.r2Claim != nil {
			if !slot.r2Claim.stamp.prepared.r2Residence.liveLocked(r, now) {
				r.clearRelationSlotLocked(slot)
			}
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
	r.revokeContactRecordPushesLocked()
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
	if !r.recordAdmission.admitsR() {
		return
	}
	select {
	case r.recordAdmission.wake <- struct{}{}:
	default:
	}
}

// selectRelationSlotLocked requires resolver.mu. Only qualified FIFO heads run;
// unverified prospectives share capacity but cannot block a qualified head.
func (r *identityResolver) selectRelationSlotLocked() *relationAdmissionSlot {
	a := r.recordAdmission
	if !a.admitsR() {
		return nil
	}
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
			head = nil
		}
	}
	if head == nil {
		for n := 0; n < len(a.slots); n++ {
			i := (int(a.r2PendingCursor) + n) % len(a.slots)
			candidate := &a.slots[i]
			if candidate.r2Claim != nil && candidate.phase == relationAdmissionProspective && !candidate.processing {
				head = candidate
				a.r2PendingCursor = uint16((i + 1) % len(a.slots))
				break
			}
		}
	}
	if head != nil {
		head.processing = true
	}
	return head
}
func (r *identityResolver) runRecordAdmission(ctx context.Context) {
	if !r.recordAdmission.admitsR() {
		return
	}
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
	if !r.recordAdmission.admitsR() {
		return errRelationAdmissionStale
	}
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
	if slot.r2Claim != nil && slot.phase == relationAdmissionProspective {
		terminal, resultErr = r.prepareOwnDMRelation(ctx, slot)
		return resultErr
	}
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
	slot.r2Claim = nil
	slot.r2Verified = verifiedOwnDMRelationRecord{}
}

// One authority is owned by one synchronous push invocation. Immutable bindings
// precede escape. Only kStageMu changes candidate/verdict. No retained payload queue.
type contactRecordStageAuthority struct {
	admission  *relationRecordAdmission
	resolver   *identityResolver
	service    *Service
	network    domain.NetworkID
	index      uint16
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	candidate  *contactRecordCandidate
	revoked    bool
	finished   bool
	staged     bool
}

// This constructor has no live caller. Unlike the R-only constructor it touches
// neither the store/backend receiver nor any R clock/timer/wake channel.
func newIdentityResolverWithContactRecordAdmission(svc *Service, intents *identityIntentStore, network domain.NetworkID, borrowedFiles identityPinDurableFiles, source contactRecordAdmissionSource) *identityResolver {
	r := newIdentityResolver(svc, intents, network)
	r.recordAdmission = &relationRecordAdmission{classes: recordAdmissionImmediateK, kSource: source, service: svc, resolver: r, network: network, kFiles: borrowedFiles}
	return r
}

func (r *identityResolver) beginContactRecordPushRun() {
	r.mu.Lock()
	a := r.recordAdmission
	ready := false
	if a.admitsK() && a.kSource.valid() && !a.closing && !r.unavailableLocked() {
		a.kStageMu.Lock()
		if !a.kClosing {
			a.kReady = true
			ready = true
		}
		a.kStageMu.Unlock()
	}
	r.mu.Unlock()
	if ready && r.svc.contactPins != nil {
		r.svc.contactPins.notify()
	}
}

// Caller owns resolver.mu. No cancellation callback runs under either lock.
func (r *identityResolver) revokeContactRecordPushesLocked() {
	a := r.recordAdmission
	if !a.admitsK() {
		return
	}
	a.kStageMu.Lock()
	defer a.kStageMu.Unlock()
	a.kClosing, a.kReady = true, false
	for i := range a.slots {
		if g := a.slots[i].kAuthority; g != nil {
			g.revoked = true
		}
		if g := a.slots[i].kReconciliation; g != nil {
			g.revoked = true
		}
	}
}
func (r *identityResolver) closeContactRecordPushes() {
	var cancels [relationAdmissionCapacity]context.CancelFunc
	r.mu.Lock()
	r.revokeContactRecordPushesLocked()
	if a := r.recordAdmission; a.admitsK() {
		for i := range a.slots {
			if g := a.slots[i].kAuthority; g != nil {
				cancels[i] = g.cancel
			}
			if g := a.slots[i].kReconciliation; g != nil {
				cancels[i] = g.cancel
			}
		}
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		if cancel != nil {
			cancel()
		}
	}
}

// A successful reservation pins its generation until finishContactRecordPush,
// including preflight/Close that has ignored cancellation and really not returned.
func (r *identityResolver) reserveContactRecordPush(ctx context.Context) *contactRecordStageAuthority {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.recordAdmission
	if !a.admitsContactPush() || !a.classes.valid() || a.closing || r.unavailableLocked() || ctx.Err() != nil {
		return nil
	}
	a.kStageMu.Lock()
	if !a.kReady || a.kClosing || a.resolver != r || a.service != r.svc || a.network != r.network {
		a.kStageMu.Unlock()
		return nil
	}
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase != relationAdmissionFree || slot.generation == math.MaxUint64 {
			continue
		}
		generation := slot.generation + 1
		*slot = relationAdmissionSlot{generation: generation, phase: relationAdmissionImmediateKProcessing, processing: true}
		// Reserve physical ownership FIRST. Even the standard child-context linking
		// is outside the leaf. resolver.mu prevents close/selector/reuse in this gap.
		a.kStageMu.Unlock()
		child, cancel := context.WithCancel(ctx)
		g := &contactRecordStageAuthority{admission: a, resolver: r, service: r.svc, network: r.network, index: uint16(i), generation: generation, ctx: child, cancel: cancel}
		a.kStageMu.Lock()
		slot.kAuthority = g
		a.kStageMu.Unlock()
		return g
	}
	a.kStageMu.Unlock()
	return nil
}
func (r *identityResolver) finishContactRecordPush(g *contactRecordStageAuthority) {
	if g == nil {
		return
	}
	g.cancel()
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.recordAdmission
	if a != g.admission || g.resolver != r || int(g.index) >= len(a.slots) {
		return
	}
	slot := &a.slots[g.index]
	if slot.kAuthority != g || slot.generation != g.generation || slot.phase != relationAdmissionImmediateKProcessing {
		return
	}
	a.kStageMu.Lock()
	g.finished = true
	g.candidate = nil
	// These K identity fields are written under BOTH locks and read by the leaf.
	// Actual synchronous owner has returned. Cancellation alone never reaches here.
	*slot = relationAdmissionSlot{generation: g.generation}
	a.kStageMu.Unlock()
}

// The registered handler captures this port once; no lookup of a replacement
// svc.identityResolver can manufacture authority for an original event.
func (r *identityResolver) prepareContactRecordPush(ctx context.Context, owner *proofTransportOwner, network domain.NetworkID, id domain.PeerIdentity, record protocol.SignedIdentityRecord) *contactRecordStageAuthority {
	if r == nil || r.network != network || owner == nil || owner.life == nil {
		return nil
	}
	owner.life.mu.Lock()
	live := owner.matchesTransportLocked(r.svc, owner.id)
	owner.life.mu.Unlock()
	if !live {
		return nil
	}
	g := r.reserveContactRecordPush(ctx)
	if g == nil {
		return nil
	}
	candidate, err := r.svc.observeContactRecord(g.ctx, id, record)
	if err == nil {
		_, err = verifyContactMinSessionRecord(candidate)
	}
	if err != nil {
		r.finishContactRecordPush(g)
		return nil
	}
	a := g.admission
	a.kStageMu.Lock()
	if g.revoked || g.finished || a.kClosing || !a.kReady || g.ctx.Err() != nil {
		a.kStageMu.Unlock()
		r.finishContactRecordPush(g)
		return nil
	}
	g.candidate = candidate
	a.kStageMu.Unlock()
	return g
}
func (r *identityResolver) settleContactRecordPush(g *contactRecordStageAuthority) {
	if g == nil {
		return
	}
	// Candidate association was published once under the leaf, and this SAME
	// invocation owns it until finish. Shutdown never changes that association.
	_, _ = r.svc.stageContactRecordPush(verifiedContactMinSessionRecord{candidate: g.candidate}, g)
	r.svc.contactPins.notify()
}
