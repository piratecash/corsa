package findnode

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type coordinatedSigningPhase uint8

const (
	signingAliasPending coordinatedSigningPhase = iota + 1
	signingConsumed
	signingUnsignable
)

// This optional record belongs to an existing targetEntry, never a second job
// inventory. Only a pending alias retains its original source Session.
type coordinatedSigning struct {
	phase         coordinatedSigningPhase
	alias         SigningReservation
	signBy        Instant // Original response deadline; owned service cutoffs are separate.
	serviceWindow ownedTargetServiceWindow
	// Owned accepted L1 promises, bounded by the existing three recipients.
	// Legacy inactive compositions leave this metadata empty.
	protected [MaxChallengeSessions]Session
}

// targetChanges is one bounded cleanup phase. Callers release cache.mu before
// draining it and reset it before admission can create another alias.
type targetChanges struct {
	aliases [MaxTargetEntries]SigningReservation
	count   int
}

func (c *targetChanges) retireAlias(state *coordinatedSigning) {
	if state.alias.slot == nil {
		return
	}
	if c.count >= len(c.aliases) {
		panic("find node: target cleanup phase exceeded cache bound")
	}
	c.aliases[c.count] = state.alias
	c.count++
	state.alias = SigningReservation{}
}

func (c *targetChanges) retireEntry(entry *targetEntry) {
	if entry.coordinated != nil {
		c.retireAlias(entry.coordinated)
		entry.coordinated = nil
	}
}

func (c *targetChanges) drain(calendar *SigningCalendar) {
	for i := range c.count {
		calendar.Cancel(c.aliases[i])
		c.aliases[i] = SigningReservation{}
	}
	c.count = 0
}

func (c *targetChanges) assertEmpty() {
	if c.count != 0 {
		panic("find node: standalone cache acquired composed signing state")
	}
}

// LocalReservation can cancel its exact generation, but cannot complete it.
type LocalReservation struct {
	owner *TargetCoordinator
	entry *targetEntry
}

// ConsumedSigningPermit is issued only after both actual budgets were debited.
// Its entry must still be current, Reserved and Consumed at completion.
type ConsumedSigningPermit struct {
	owner *TargetCoordinator
	entry *targetEntry
}

type LocalTargetResultKind uint8

const (
	LocalReadyCopy LocalTargetResultKind = iota + 1
	LocalWaitingExisting
	LocalReserved
)

type LocalTargetResult struct {
	Kind        LocalTargetResultKind
	Reservation LocalReservation
	Answer      protocol.FindNodeAnswer
}

// PublicSigningInput contains immutable public values. External crypto executes
// after TakeSignWork returns, without any state-owner mutex held.
type PublicSigningInput struct {
	network   domain.NetworkID
	target    domain.PeerIdentity
	challenge domain.FindNodeChallenge
}

func (in PublicSigningInput) Network() domain.NetworkID           { return in.network }
func (in PublicSigningInput) Target() domain.PeerIdentity         { return in.target }
func (in PublicSigningInput) Challenge() domain.FindNodeChallenge { return in.challenge }
func (in PublicSigningInput) Transcript() ([]byte, error) {
	return protocol.BuildFindNodeAnswerTranscript(in.network, in.target, in.challenge)
}

type LocalTargetCounters struct {
	Reserved, Waiting, ReadyCopy, Refused, Consumed, Completed uint64
}

// LocalTargetSnapshot is the last composed diagnostic sample, not a network
// admission/response acknowledgement or automatic observation of external close.
type LocalTargetSnapshot struct {
	SampleAt                           Instant
	Sequence                           uint64
	SequenceSaturated                  bool
	Cache                              TargetSnapshot
	Local                              LocalTargetCounters
	AliasPending, Consumed, Unsignable int
	CalendarPending                    int
}

// TargetCoordinator is an inactive local composition. Lock order is mu, then
// cache.mu OR calendar.mu; calendar may take sessionState.mu. No reverse edge,
// callback, crypto, I/O, worker or mutable subowner escapes this object.
type TargetCoordinator struct {
	mu        sync.Mutex
	key       identity.PublicKey
	network   domain.NetworkID
	cache     *TargetCache
	calendar  *SigningCalendar
	last      Instant
	sequence  uint64
	counters  LocalTargetCounters
	published atomic.Pointer[LocalTargetSnapshot]
}

func NewTargetCoordinator(self identity.PublicKey, network domain.NetworkID, profile SigningProfile, start Instant) (*TargetCoordinator, error) {
	key, err := identity.ParsePublicKey(self.Bytes())
	if err != nil {
		return nil, ErrInvalid
	}
	target, err := domain.ParsePeerIdentity(key.Fingerprint())
	if err != nil {
		return nil, ErrInvalid
	}
	if _, err := protocol.BuildFindNodeAnswerTranscript(network, target, domain.FindNodeChallenge{}); err != nil {
		return nil, ErrInvalid
	}
	calendar, err := NewSigningCalendar(profile, start)
	if err != nil {
		return nil, err
	}
	cache, err := NewTargetCache(target)
	if err != nil {
		return nil, err
	}
	cache.last = start
	o := &TargetCoordinator{key: key, network: network, cache: cache, calendar: calendar, last: start}
	o.publishLocked()
	return o, nil
}

func (o *TargetCoordinator) ReserveLocalTarget(source Session, request protocol.FindNodeRequest, priorities PrioritySnapshot, received, signBy Instant) (LocalTargetResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	if err := o.advanceLocked(received); err != nil {
		o.counters.Refused++
		return LocalTargetResult{}, err
	}
	var changes targetChanges
	o.cache.mu.Lock()
	result, err := o.cache.admitLocked(source, request, received, signBy, priorities, &changes)
	o.cache.mu.Unlock()
	// An evicted victim's physical slot must be released BEFORE new Reserve.
	changes.drain(o.calendar)
	if err != nil {
		o.counters.Refused++
		return LocalTargetResult{}, err
	}
	switch result.Kind {
	case TargetAnswerReady:
		o.counters.ReadyCopy++
		return LocalTargetResult{Kind: LocalReadyCopy, Answer: result.Answer}, nil
	case TargetWaiting:
		o.counters.Waiting++
		return LocalTargetResult{Kind: LocalWaitingExisting}, nil
	}
	alias, err := o.calendar.Reserve(source, received, signBy)
	o.cache.mu.Lock()
	if err != nil {
		o.cache.cancelLocked(result.Work, &changes)
	} else {
		result.Work.entry.coordinated = &coordinatedSigning{phase: signingAliasPending, alias: alias, signBy: signBy}
	}
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	if err != nil {
		o.counters.Refused++
		return LocalTargetResult{}, err
	}
	o.counters.Reserved++
	return LocalTargetResult{Kind: LocalReserved, Reservation: LocalReservation{owner: o, entry: result.Work.entry}}, nil
}

func (o *TargetCoordinator) TakeSignWork(reservation LocalReservation, now Instant) (ConsumedSigningPermit, PublicSigningInput, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	if err := o.advanceLocked(now); err != nil {
		return ConsumedSigningPermit{}, PublicSigningInput{}, err
	}
	o.cache.mu.Lock()
	entry := reservation.entry
	if reservation.owner != o || !o.currentLocked(entry, signingAliasPending) || now >= entry.coordinated.signBy {
		o.cache.mu.Unlock()
		return ConsumedSigningPermit{}, PublicSigningInput{}, ErrStaleWork
	}
	alias := entry.coordinated.alias
	o.cache.mu.Unlock()
	err := o.calendar.Consume(alias, now)
	if err == ErrNotReady {
		return ConsumedSigningPermit{}, PublicSigningInput{}, err
	}
	var changes targetChanges
	o.cache.mu.Lock()
	if err != nil {
		changes.retireAlias(entry.coordinated)
		entry.coordinated.phase = signingUnsignable
		o.reconcileClosedLocked(&changes)
	} else {
		entry.coordinated.alias = SigningReservation{}
		entry.coordinated.phase = signingConsumed
	}
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	if err != nil {
		return ConsumedSigningPermit{}, PublicSigningInput{}, err
	}
	o.counters.Consumed++
	return ConsumedSigningPermit{owner: o, entry: entry}, PublicSigningInput{network: o.network, target: o.cache.self, challenge: entry.challenge}, nil
}

// CompleteSigned checks consumed local authority, not cryptographic validity of
// arbitrary signature bytes. The future initiator must verify the full answer.
func (o *TargetCoordinator) CompleteSigned(permit ConsumedSigningPermit, signature domain.FindNodeSignature, now Instant) ([]Delivery, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	if err := o.advanceLocked(now); err != nil {
		return nil, err
	}
	var changes targetChanges
	o.cache.mu.Lock()
	entry := permit.entry
	if permit.owner != o || !o.currentLocked(entry, signingConsumed) || now >= entry.coordinated.signBy {
		o.cache.mu.Unlock()
		return nil, ErrStaleWork
	}
	deliveries, err := o.cache.completeLocked(SignWork{cache: o.cache, entry: entry}, AnswerBody{Target: o.cache.self, PublicKey: o.key, Signature: signature}, now, &changes)
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	if err == nil {
		o.counters.Completed++
	}
	return deliveries, err
}

func (o *TargetCoordinator) Cancel(reservation LocalReservation, now Instant) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	err := o.advanceLocked(now)
	var changes targetChanges
	o.cache.mu.Lock()
	if err != nil {
		o.reconcileClosedLocked(&changes)
	}
	cancelled := false
	if reservation.owner == o {
		cancelled = o.cache.cancelLocked(SignWork{cache: o.cache, entry: reservation.entry}, &changes)
	}
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	return cancelled, err
}

func (o *TargetCoordinator) CloseSession(source Session, now Instant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	err := o.advanceLocked(now)
	var changes targetChanges
	o.cache.mu.Lock()
	if err != nil {
		o.reconcileClosedLocked(&changes)
	}
	o.cache.closeSessionLocked(source, &changes)
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	o.calendar.CloseSession(source)
	return err
}

func (o *TargetCoordinator) Expire(now Instant) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	defer o.publishLocked()
	err := o.advanceLocked(now)
	if err != nil {
		var changes targetChanges
		o.cache.mu.Lock()
		o.reconcileClosedLocked(&changes)
		o.cache.mu.Unlock()
		changes.drain(o.calendar)
	}
	return err
}

// advanceLocked requires o.mu. Invalid time grants no authority or time expiry.
func (o *TargetCoordinator) advanceLocked(now Instant) error {
	if now < 0 || now < o.last {
		return ErrClock
	}
	o.last = now
	var changes targetChanges
	o.cache.mu.Lock()
	err := o.cache.advanceLocked(now, &changes)
	o.reconcileClosedLocked(&changes)
	o.cache.mu.Unlock()
	changes.drain(o.calendar)
	if err != nil {
		return err
	}
	return o.calendar.Expire(now)
}

// reconcileClosedLocked requires o.mu and cache.mu. The existing session map
// has at most 256*3 references; it is not a second persistent close inventory.
func (o *TargetCoordinator) reconcileClosedLocked(changes *targetChanges) {
	for source := range o.cache.sessions {
		if !source.valid() {
			o.cache.closeSessionLocked(source, changes)
		}
	}
}

// currentLocked requires o.mu and cache.mu. Pointer membership plus phase is
// authority, independently of the saturating diagnostic sequence.
func (o *TargetCoordinator) currentLocked(entry *targetEntry, phase coordinatedSigningPhase) bool {
	return entry != nil && o.cache.entries[entry.challenge] == entry && entry.phase == targetReserved && entry.coordinated != nil && entry.coordinated.phase == phase
}

// publishLocked requires o.mu (or sole constructor ownership). Subowners are
// sampled separately: cache.mu is never held while calendar methods execute.
func (o *TargetCoordinator) publishLocked() {
	if o.sequence < math.MaxUint64 {
		o.sequence++
	}
	snapshot := &LocalTargetSnapshot{SampleAt: o.last, Sequence: o.sequence, SequenceSaturated: o.sequence == math.MaxUint64, Local: o.counters}
	o.cache.mu.Lock()
	snapshot.Cache = o.cache.snapshotLocked()
	for _, entry := range o.cache.entries {
		if state := entry.coordinated; state != nil {
			switch state.phase {
			case signingAliasPending:
				snapshot.AliasPending++
			case signingConsumed:
				snapshot.Consumed++
			case signingUnsignable:
				snapshot.Unsignable++
			}
		}
	}
	o.cache.mu.Unlock()
	snapshot.CalendarPending = o.calendar.Pending()
	if snapshot.AliasPending != snapshot.CalendarPending || snapshot.CalendarPending > o.calendar.limit || snapshot.CalendarPending > MaxTargetEntries {
		panic("find node: target row/signing alias ownership mismatch")
	}
	o.published.Store(snapshot)
}

func (o *TargetCoordinator) Snapshot() LocalTargetSnapshot {
	if snapshot := o.published.Load(); snapshot != nil {
		return *snapshot
	}
	return LocalTargetSnapshot{}
}
