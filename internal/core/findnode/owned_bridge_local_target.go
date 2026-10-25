package findnode

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"sync/atomic"
	"time"
)

// OwnedLocalTarget is the sole local responder bound to an original intake.
// Its state never accepts caller-created Instants, reservations or sign permits.
type OwnedLocalTarget struct{ local *ownedLocalTargetState }
type ownedLocalTargetState struct {
	owner  *dormantIntakeState
	target *TargetCoordinator
	timed  bool
	timing OwnedTargetTimingProfile
}
type OwnedLocalSignWork struct{ work *ownedLocalSignState }
type OwnedLocalSignPermit struct{ work *ownedLocalSignState }
type ownedLocalSignState struct {
	local    *ownedLocalTargetState
	identity ownedReceivedIdentity
	entry    *targetEntry
	phase    atomic.Uint32
	timing   ownedLocalTimingEndpoints // diagnostic only; guarded by target.mu
}

// These endpoints belong only to this exact original work. No history/ring or
// authority escapes; Take and Complete retain their existing clock samples.
type ownedLocalTimingEndpoints struct {
	takeNS, completeNS       int64
	takeKnown, completeKnown bool
}

type ownedLocalTimingClock uint8

const (
	ownedLocalTimingClockUnknown ownedLocalTimingClock = iota
	ownedLocalTimingClockFixture
	ownedLocalTimingClockRuntime
)

// Package-private value-copy diagnostic. Runtime marks clock provenance only,
// never calibration, boot identity, physical accuracy, or a latency guarantee.
// CompleteNS is the successful Complete SAMPLE before its cache/publication
// tail, not function return, writer Take, Write completion or delivery.
type ownedLocalTimingObservation struct {
	clock                               ownedLocalTimingClock
	takeKnown, completeKnown, pairKnown bool
	takeNS, completeNS, spanNS          int64
	observedReserveExceeded             bool
	// Immutable ingress of the original fresh signing work, never a recipient lookup.
	receivedKnown                  bool
	receivedNS                     int64
	requestID, sourceConnID        uint64
	sourceOpenedNS                 int64
	sourceIdentity, targetIdentity [20]byte
	challenge                      [32]byte
}

func (work OwnedLocalSignWork) timingObservation() ownedLocalTimingObservation {
	w := work.work
	if !ownedLocalOriginal(w) || !w.local.timed {
		return ownedLocalTimingObservation{}
	}
	o := w.local.target
	o.mu.Lock()
	defer o.mu.Unlock()
	identity := w.identity
	// Historical immutable provenance only; End/current generation is not a
	// permission check on retained evidence. Never sample or look up a ConnID.
	if identity.source.state == nil || identity.source.state.connID == 0 || identity.source.state.identity.IsZero() || identity.source.state.opened < 0 || identity.source.state.opened > Instant(identity.received.Elapsed()) || !w.local.owner.consumer.Matches(identity.stamp, identity.received) {
		return ownedLocalTimingObservation{}
	}
	endpoints := w.timing
	out := ownedLocalTimingObservation{takeKnown: endpoints.takeKnown, completeKnown: endpoints.completeKnown, takeNS: endpoints.takeNS, completeNS: endpoints.completeNS,
		receivedNS: int64(identity.received.Elapsed()), requestID: uint64(identity.request.ID),
		sourceConnID: uint64(identity.source.state.connID), sourceOpenedNS: int64(identity.source.state.opened),
		sourceIdentity: identity.source.state.identity, targetIdentity: identity.request.Target, challenge: identity.request.Challenge}
	if w.local.owner.consumer.IsRuntime() {
		out.clock = ownedLocalTimingClockRuntime
	} else if w.local.owner.consumer.IsFixture() {
		out.clock = ownedLocalTimingClockFixture
	}
	out.receivedKnown = out.clock != ownedLocalTimingClockUnknown && out.receivedNS >= 0
	if out.clock != ownedLocalTimingClockUnknown && out.takeKnown && out.completeKnown && out.takeNS >= 0 && out.completeNS >= out.takeNS {
		out.pairKnown = true
		out.spanNS = out.completeNS - out.takeNS
		out.observedReserveExceeded = out.spanNS > int64(w.local.timing.RuntimeReserve)
	}
	return out
}

// OwnedTargetEndpointClock reports provenance only, not calibration or a
// process/boot/domain identity. No clock capability is returned.
type OwnedTargetEndpointClock uint8

const (
	OwnedTargetEndpointClockUnknown OwnedTargetEndpointClock = iota
	OwnedTargetEndpointClockFixture
	OwnedTargetEndpointClockRuntime
)

// OwnedTargetEndpointObservation contains finite historical scalar copies of
// one original fresh timed signing work. Tuple equality is not lookup, cross-
// process identity or authority. Waiting/cache recipients have no new Work.
// TakeNS is the existing Take sample, retained only after successful budget debit.
// SpanNS remains CompleteNS-ReceivedNS; CompleteNS-TakeNS is a separate reserve span.
// CompleteNS is the successful Complete SAMPLE before cache/mint/publication,
// not Complete return, writer start, byte completion or delivery.
type OwnedTargetEndpointObservation struct {
	Clock                                              OwnedTargetEndpointClock
	ReceivedKnown, TakeKnown, CompleteKnown, PairKnown bool
	ReceivedNS, TakeNS, CompleteNS, SpanNS             int64
	RequestID, SourceConnID                            uint64
	SourceOpenedNS                                     int64
	SourceIdentity, TargetIdentity                     [20]byte
	Challenge                                          [32]byte
}

// TargetEndpointObservation reads the existing target-mutex snapshot once.
// It never samples, mutates, returns an owner/Session/permit/domain handle or
// resolves a numeric identifier. Untimed/zero/foreign input remains UNKNOWN.
func (work OwnedLocalSignWork) TargetEndpointObservation() OwnedTargetEndpointObservation {
	observed := work.timingObservation()
	if !observed.receivedKnown {
		return OwnedTargetEndpointObservation{}
	}
	out := OwnedTargetEndpointObservation{
		ReceivedKnown: true, ReceivedNS: observed.receivedNS,
		TakeKnown: observed.takeKnown && observed.takeNS >= 0, TakeNS: observed.takeNS,
		CompleteKnown: observed.completeKnown && observed.takeKnown && observed.completeNS >= 0,
		CompleteNS:    observed.completeNS,
		RequestID:     observed.requestID, SourceConnID: observed.sourceConnID,
		SourceOpenedNS: observed.sourceOpenedNS, SourceIdentity: observed.sourceIdentity,
		TargetIdentity: observed.targetIdentity, Challenge: observed.challenge,
	}
	switch observed.clock {
	case ownedLocalTimingClockFixture:
		out.Clock = OwnedTargetEndpointClockFixture
	case ownedLocalTimingClockRuntime:
		out.Clock = OwnedTargetEndpointClockRuntime
	}
	if out.CompleteKnown && out.CompleteNS >= out.ReceivedNS {
		out.PairKnown = true
		// Both points nonnegative and ordered, so int64 subtraction is safe.
		out.SpanNS = out.CompleteNS - out.ReceivedNS
	}
	return out
}

const (
	ownedLocalSignReady uint32 = iota
	ownedLocalSignTaking
	ownedLocalSignConsumed
	ownedLocalSignCompleting
	ownedLocalSignDone
	ownedLocalSignCancelled
)

// Answers use the shared opaque original-writer companion, never bare Delivery.
type OwnedLocalResult struct {
	Kind LocalTargetResultKind
	Work OwnedLocalSignWork
	Copy OwnedAnswerWork
}

// NewLocalTarget is called during dedicated construction before Service escape.
// A second call cannot mint a second per-identity calendar or reset source debt.
func (intake DormantIntake) NewLocalTarget(self identity.PublicKey, network domain.NetworkID, profile SigningProfile) (OwnedLocalTarget, error) {
	return intake.newLocalTarget(self, network, profile, OwnedTargetTimingProfile{}, false)
}

// Explicit owned-only construction: no setter or implicit timing default.
func (intake DormantIntake) NewLocalTargetWithTiming(self identity.PublicKey, network domain.NetworkID, profile SigningProfile, timing OwnedTargetTimingProfile) (OwnedLocalTarget, error) {
	if !timing.valid() {
		return OwnedLocalTarget{}, ErrInvalid
	}
	return intake.newLocalTarget(self, network, profile, timing, true)
}

func (intake DormantIntake) newLocalTarget(self identity.PublicKey, network domain.NetworkID, profile SigningProfile, timing OwnedTargetTimingProfile, timed bool) (OwnedLocalTarget, error) {
	owner := intake.dormantOwner
	if owner == nil {
		return OwnedLocalTarget{}, ErrUnauthenticated
	}
	owner.coordinator.mu.Lock()
	defer owner.coordinator.mu.Unlock()
	if owner.localTarget != nil {
		return OwnedLocalTarget{}, ErrInvalid
	}
	clock, err := owner.finishObserver.ReserveClock()
	if err != nil {
		return OwnedLocalTarget{}, err
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		return OwnedLocalTarget{}, err
	}
	if !owner.consumer.MatchesPoint(at) {
		return OwnedLocalTarget{}, ErrUnauthenticated
	}
	target, err := NewTargetCoordinator(self, network, profile, Instant(at.Elapsed()))
	if err != nil {
		return OwnedLocalTarget{}, err
	}
	local := &ownedLocalTargetState{owner: owner, target: target, timing: timing, timed: timed}
	owner.localTarget = local
	return OwnedLocalTarget{local: local}, nil
}

// ReserveReceived reserves a projected signing alias, never actual T. Strict
// input and cut both belong to the SAME original intake and source lifetime.
func (local OwnedLocalTarget) ReserveReceived(input OwnedReceivedRequest, cut OwnedCut) (OwnedLocalResult, error) {
	l, c, cell := local.local, cut.sealedCut, input.receivedRequest
	if l == nil || l.owner == nil || l.target == nil || l.owner.localTarget != l || c == nil || c.owner != l.owner || cell == nil || cell.identity.owner != l.owner {
		return OwnedLocalResult{}, ErrUnauthenticated
	}
	id := cell.identity
	o := l.target
	if id.request.Target != o.cache.self || !l.owner.consumer.Matches(id.stamp, id.received) || !l.owner.consumer.Matches(c.stamp, c.at) || c.at.Elapsed() < id.received.Elapsed() || !id.source.valid() || id.source.state.opened > Instant(id.received.Elapsed()) {
		return OwnedLocalResult{}, ErrUnauthenticated
	}
	if !cell.consumed.CompareAndSwap(false, true) {
		return OwnedLocalResult{}, ErrStaleWork
	}
	found := false
	members := make([]Session, 0, len(c.originals.members))
	classes := make(map[domain.ConnID]SessionClass, len(c.originals.members))
	for index, source := range c.originals.members {
		if source.Identity() == o.cache.self {
			continue
		}
		found = found || source == id.source
		members = append(members, source)
		classes[source.ConnID()] = c.classes[index]
	}
	if !found {
		return OwnedLocalResult{}, ErrPrioritySnapshot
	}
	priorities, err := NewPrioritySnapshot(o.cache.self, members, Instant(c.at.Elapsed()), func(info SessionInfo) (SessionClass, error) {
		class, ok := classes[info.ConnID]
		if !ok {
			return 0, ErrPrioritySnapshot
		}
		return class, nil
	})
	if err != nil {
		return OwnedLocalResult{}, err
	}
	deadline, err := Instant(id.received.Elapsed()).plus(responderTimeout)
	if err != nil {
		return OwnedLocalResult{}, err
	}
	work := &ownedLocalSignState{local: l, identity: id}
	var serviceWindow ownedTargetServiceWindow
	var refusal *OwnedTargetTimingRefusal
	if l.timed {
		readyBy, err := Instant(id.received.Elapsed()).plus(Instant(l.timing.ServiceBound))
		if err != nil {
			return OwnedLocalResult{}, err
		}
		serviceWindow = ownedTargetServiceWindow{enabled: true, latestSignStart: readyBy - Instant(l.timing.RuntimeReserve), signatureReadyBy: readyBy}
		// The bounded diagnostic is allocated before owner locks and final R.
		// It contains numeric observations only, never owner/receipt authority.
		refusal = &OwnedTargetTimingRefusal{}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	// Mandatory cleanup and all application locks precede current R/f.
	if Instant(c.at.Elapsed()) < o.last {
		return OwnedLocalResult{}, ErrStaleWork
	}
	if err = o.advanceLocked(Instant(c.at.Elapsed())); err != nil {
		return OwnedLocalResult{}, err
	}
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	id.source.state.mu.Lock()
	defer id.source.state.mu.Unlock()
	// Out-of-order ingress can precede a retained shared job's first receive.
	// Refuse that extra recipient rather than extend its original response limit
	// or retime an already accepted signing alias owned by the first source.
	if existing := o.cache.entries[id.request.Challenge]; existing != nil && existing.phase == targetReserved && existing.coordinated != nil && deadline < existing.coordinated.signBy {
		return OwnedLocalResult{}, ErrBudget
	}
	// Stage ALL capacity selection, mutable maps, rows and output cells before R.
	// The detached cache cannot alias a live recipient map or coordinated state.
	plan, err := stageOwnedLocalAdmissionLocked(o, id, priorities, deadline)
	if err != nil {
		return OwnedLocalResult{}, err
	}
	// Ready expiry retains the SAME successful Complete sample plus60s.
	// Admission staging is detached: preserve its original refusal precedence,
	// then check recipient-relative availability before publishing any copy.
	if l.timed && plan.result.Kind == TargetAnswerReady {
		ready := o.cache.entries[id.request.Challenge]
		if ready == nil || ready.phase != targetReady || ready.expires < cacheLifetime {
			return OwnedLocalResult{}, ErrStaleWork
		}
		completed := ready.expires - cacheLifetime
		if max(Instant(id.received.Elapsed()), completed) >= serviceWindow.signatureReadyBy {
			return OwnedLocalResult{}, ErrBudget
		}
	}
	work.entry = plan.result.Work.entry
	copyResult := mintOwnedAnswerWork(l.owner, id.source, plan.result.Answer, deadline)
	preparedSlot := &signingSlot{source: id.source}
	nextSlots := make(map[*signingSlot]struct{}, len(o.calendar.slots)+1)
	for slot := range o.calendar.slots {
		nextSlots[slot] = struct{}{}
	}
	for index := 0; index < plan.changes.count; index++ {
		delete(nextSlots, plan.changes.aliases[index].slot)
	}
	if plan.result.Kind == TargetNeedsSignature {
		nextSlots[preparedSlot] = struct{}{}
	}
	preparedSigning := &coordinatedSigning{phase: signingAliasPending, alias: SigningReservation{calendar: o.calendar, slot: preparedSlot}, signBy: deadline, serviceWindow: serviceWindow}
	if priorities.priorities[id.source] == PriorityProtected {
		preparedSigning.protected[0] = id.source
	}
	lease, err := l.owner.consumer.TryReadClock(c.stamp)
	if err != nil {
		return OwnedLocalResult{}, err
	}
	defer lease.Release()
	at, err := c.originals.sampleAndCheckOriginals(lease)
	if err != nil {
		return OwnedLocalResult{}, err
	}
	f := Instant(at.Elapsed())
	if !l.owner.consumer.Matches(c.stamp, at) || !at.NotBefore(c.at) || !c.validBefore.Contains(at) || f < o.last || f >= deadline || cell.identity != id {
		return OwnedLocalResult{}, ErrStaleWork
	}
	// The detached q capacity/victim decision is valid only while every row and
	// strict class boundary remains unchanged. A due cleanup refuses untouched.
	for _, entry := range o.cache.entries {
		if f >= entry.expires {
			return OwnedLocalResult{}, ErrNotReady
		}
	}
	for source := range o.cache.sessions {
		if !source.valid() {
			return OwnedLocalResult{}, ErrNotReady
		}
	}
	var projected, actual forwardBucket
	var slotAt, nextProjected Instant
	if plan.result.Kind == TargetNeedsSignature {
		if len(nextSlots)-1 >= o.calendar.limit {
			return OwnedLocalResult{}, ErrCapacity
		}
		if id.source.state.signer != nil && id.source.state.signer != o.calendar {
			return OwnedLocalResult{}, ErrInvalid
		}
		projected = id.source.state.signingProjected
		actual = id.source.state.signingActual
		if id.source.state.signer == nil {
			projected = forwardBucket{at: f, tokens: forwardBurst}
			actual = projected
		}
		projected, slotAt, err = projected.project(max(f, o.calendar.nextProjected))
		if err != nil {
			return OwnedLocalResult{}, err
		}
		if serviceWindow.enabled && (f >= serviceWindow.latestSignStart || slotAt >= serviceWindow.latestSignStart) {
			refusal.ReceivedNS = int64(id.received.Elapsed())
			refusal.SampledNS = int64(f)
			refusal.ProjectedNS = int64(slotAt)
			refusal.LatestStartNS = int64(serviceWindow.latestSignStart)
			refusal.SignatureReadyNS = int64(serviceWindow.signatureReadyBy)
			refusal.AnswerDeadlineNS = int64(deadline)
			return OwnedLocalResult{}, refusal
		}
		if slotAt >= deadline {
			return OwnedLocalResult{}, ErrBudget
		}
		nextProjected, err = slotAt.plus(o.calendar.spacing)
		if err != nil {
			return OwnedLocalResult{}, err
		}
	}
	// Final bounded suffix: only preallocated state and authenticated f arithmetic.
	// Existing live entry pointers survive unchanged, including unrelated permits.
	plan.install(o.cache, f)
	o.calendar.slots = nextSlots
	o.last = f
	switch plan.result.Kind {
	case TargetAnswerReady:
		o.counters.ReadyCopy++
		return OwnedLocalResult{Kind: LocalReadyCopy, Copy: copyResult}, nil
	case TargetWaiting:
		o.counters.Waiting++
		return OwnedLocalResult{Kind: LocalWaitingExisting}, nil
	}
	id.source.state.signer = o.calendar
	id.source.state.signingProjected = projected
	id.source.state.signingActual = actual
	preparedSlot.at, preparedSlot.latest = slotAt, deadline
	o.calendar.nextProjected = nextProjected
	o.calendar.last = f
	work.entry.coordinated = preparedSigning
	o.counters.Reserved++
	return OwnedLocalResult{Kind: LocalReserved, Work: OwnedLocalSignWork{work: work}}, nil
}

func drainOwnedLocalAliasesLocked(calendar *SigningCalendar, changes *targetChanges) {
	for i := 0; i < changes.count; i++ {
		r := changes.aliases[i]
		if r.calendar == calendar && r.slot != nil {
			delete(calendar.slots, r.slot)
		}
		changes.aliases[i] = SigningReservation{}
	}
	changes.count = 0
}
func ownedLocalOriginal(w *ownedLocalSignState) bool {
	return w != nil && w.local != nil && w.local.owner != nil && w.local.target != nil && w.local.owner.localTarget == w.local && w.identity.owner == w.local.owner && w.entry != nil && w.identity.request.Target == w.local.target.cache.self && w.identity.request.Challenge == w.entry.challenge
}
func (work OwnedLocalSignWork) WaitDelay() (time.Duration, error) {
	w := work.work
	if !ownedLocalOriginal(w) || w.phase.Load() != ownedLocalSignReady {
		return 0, ErrStaleWork
	}
	o := w.local.target
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	if !o.currentLocked(w.entry, signingAliasPending) {
		return 0, ErrStaleWork
	}
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	w.identity.source.state.mu.Lock()
	defer w.identity.source.state.mu.Unlock()
	clock, err := w.local.owner.finishObserver.ReserveClock()
	if err != nil {
		return 0, err
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		return 0, err
	}
	if !w.local.owner.consumer.MatchesPoint(at) {
		return 0, ErrUnauthenticated
	}
	f := Instant(at.Elapsed())
	if f < o.last {
		return 0, ErrClock
	}
	actual := w.identity.source.state.signingActual
	actual.refill(f)
	sourceAt, err := f.plus(max(0, forwardToken-actual.tokens))
	if err != nil {
		return 0, err
	}
	earliest := max(f, w.entry.coordinated.alias.At(), o.calendar.nextActual, sourceAt)
	if earliest >= w.entry.coordinated.startBoundary() {
		return 0, ErrBudget
	}
	return time.Duration(earliest - f), nil
}

// Take is the actual local AdmittedAtT, not forwarding writer Take. All waits
// are before its ONE genuine current Sample; both actual budgets and ticket
// consumption share the same R. Historical selection cuts apply only to Reserve.
// The caller then signs outside locks.
func (work OwnedLocalSignWork) Take() (OwnedLocalSignPermit, PublicSigningInput, error) {
	w := work.work
	if !ownedLocalOriginal(w) {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedLocalSignReady, ownedLocalSignTaking) {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrStaleWork
	}
	terminal := false
	// A refused attempt has not consumed T. Keep the accepted exact alias and
	// ticket usable after a lawful not-yet-ready/cleanup/clock refusal. Actual T
	// consumes it once; exact stale generation, End or deadline retires it.
	defer func() {
		if w.phase.Load() == ownedLocalSignTaking {
			if terminal {
				w.phase.Store(ownedLocalSignCancelled)
				cancelOwnedLocalOriginal(w)
			} else {
				w.phase.Store(ownedLocalSignReady)
			}
		}
	}()
	o := w.local.target
	owner := w.local.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.advanceLocked(o.last); err != nil {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, err
	}
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	w.identity.source.state.mu.Lock()
	defer w.identity.source.state.mu.Unlock()
	// Accepted original work uses current clock->authR custody after ALL waits.
	// The reservation's old selection cut is neither reacquired nor retained.
	stamp, lease, err := owner.consumer.ReserveAcceptedClock()
	if err != nil {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, err
	}
	defer lease.Release()
	at, err := lease.Sample()
	if err != nil {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, err
	}
	f := Instant(at.Elapsed())
	entry := w.entry
	if !owner.consumer.Matches(stamp, at) || f < o.last {
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrClock
	}
	if !w.identity.source.valid() || !o.currentLocked(entry, signingAliasPending) || entry.recipients[w.identity.source] != w.identity.request.ID {
		terminal = true
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrStaleWork
	}
	if f >= entry.coordinated.startBoundary() {
		terminal = true
		o.last = f
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrStaleWork
	}
	alias := entry.coordinated.alias
	if alias.slot == nil || alias.slot.source != w.identity.source {
		terminal = true
		return OwnedLocalSignPermit{}, PublicSigningInput{}, ErrStaleWork
	}
	// Historical cachewide rows do not re-admit accepted original work. Their
	// ordinary cleanup remains outside this R and never gates this exact T.
	o.last = f
	if err = o.calendar.consumeSourceLocked(alias, f); err != nil {
		if err == ErrStaleWork || err == ErrUnauthenticated || err == ErrBudget {
			terminal = true
		}
		return OwnedLocalSignPermit{}, PublicSigningInput{}, err
	}
	// No fallible operation or lock acquisition after actual budget debit.
	entry.coordinated.alias = SigningReservation{}
	entry.coordinated.phase = signingConsumed
	o.counters.Consumed++
	w.phase.Store(ownedLocalSignConsumed)
	if w.local.timed {
		w.timing.takeNS, w.timing.takeKnown = int64(f), true
	}
	return OwnedLocalSignPermit{work: w}, PublicSigningInput{network: o.network, target: o.cache.self, challenge: entry.challenge}, nil
}

func (work OwnedLocalSignWork) Cancel() error {
	w := work.work
	if !ownedLocalOriginal(w) {
		return ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedLocalSignReady, ownedLocalSignCancelled) {
		return ErrStaleWork
	}
	cancelOwnedLocalOriginal(w)
	return nil
}
func cancelOwnedLocalOriginal(w *ownedLocalSignState) {
	o := w.local.target
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	var changes targetChanges
	o.cache.cancelLocked(SignWork{cache: o.cache, entry: w.entry}, &changes)
	drainOwnedLocalAliasesLocked(o.calendar, &changes)
}

// Complete is mandatory after the external signing attempt. Genuine clock-only
// completion remains available after End; no current-generation R is requested.
// signature nil means signing failed/panicked: retire exact work, refund nothing.
func (permit OwnedLocalSignPermit) Complete(signature *domain.FindNodeSignature) ([]OwnedAnswerWork, error) {
	w := permit.work
	if !ownedLocalOriginal(w) {
		return nil, ErrUnauthenticated
	}
	if !w.phase.CompareAndSwap(ownedLocalSignConsumed, ownedLocalSignCompleting) {
		return nil, ErrStaleWork
	}
	defer w.phase.Store(ownedLocalSignDone)
	o := w.local.target
	owner := w.local.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	var changes targetChanges
	retire := func() {
		o.cache.cancelLocked(SignWork{cache: o.cache, entry: w.entry}, &changes)
		drainOwnedLocalAliasesLocked(o.calendar, &changes)
	}
	if !o.currentLocked(w.entry, signingConsumed) {
		return nil, ErrStaleWork
	}
	answerExpires := w.entry.coordinated.signBy // completeLocked retires this exact coordinated state
	clock, err := owner.finishObserver.ReserveClock()
	if err != nil {
		retire()
		return nil, err
	}
	at, err := clock.Sample()
	clock.Release()
	if err != nil {
		retire()
		return nil, err
	}
	f := Instant(at.Elapsed())
	if !owner.consumer.MatchesPoint(at) || f < o.last {
		retire()
		return nil, ErrClock
	}
	if signature == nil || f >= w.entry.coordinated.readyBoundary() {
		retire()
		return nil, ErrStaleWork
	}
	o.last = f
	// Cache completion applies the same exact entry pointer and each original
	// recipient/requestID. A closed original is skipped, never looked up by ConnID.
	deliveries, err := o.cache.completeLocked(SignWork{cache: o.cache, entry: w.entry}, AnswerBody{Target: o.cache.self, PublicKey: o.key, Signature: *signature}, f, &changes)
	drainOwnedLocalAliasesLocked(o.calendar, &changes)
	if err != nil {
		retire()
		return nil, err
	}
	out := make([]OwnedAnswerWork, 0, len(deliveries))
	for _, delivery := range deliveries {
		out = append(out, mintOwnedAnswerWork(owner, delivery.Session, delivery.Answer, answerExpires))
	}
	o.counters.Completed++
	if w.local.timed && w.timing.takeKnown {
		w.timing.completeNS, w.timing.completeKnown = int64(f), true
	}
	return out, nil
}

// EndOriginalSession follows existing W/common publication, never holds them.
// It complements forwarding retirement; a consumed job still allows completion.
func (local OwnedLocalTarget) EndOriginalSession(source Session) error {
	l := local.local
	if l == nil || source.state == nil || source.valid() {
		return ErrUnauthenticated
	}
	o := l.target
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	o.cache.mu.Lock()
	defer o.cache.mu.Unlock()
	o.calendar.mu.Lock()
	defer o.calendar.mu.Unlock()
	var changes targetChanges
	o.cache.closeSessionLocked(source, &changes)
	drainOwnedLocalAliasesLocked(o.calendar, &changes)
	for slot := range o.calendar.slots {
		if slot.source == source {
			delete(o.calendar.slots, slot)
		}
	}
	return nil
}
