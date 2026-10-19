package findnode

import (
	"sync"
	"time"
)

// SigningProfile is the conservative proposal in d1-admission-budget.md, not an
// approved G2 default. Every constructor requires it. No extra node-wide burst
// is added; the established per-session 1/s, burst 4 is kept independently.
type SigningProfile struct {
	NodeSpacing     time.Duration
	MaxReservations int
}
type signingSlot struct {
	source     Session
	at, latest Instant
}
type SigningReservation struct {
	calendar *SigningCalendar
	slot     *signingSlot
}

func (r SigningReservation) At() Instant {
	if r.slot == nil {
		return 0
	}
	return r.slot.at
}

// SigningCalendar stores bounded budget aliases, not signing jobs or replies.
// The future caller must pair each alias with ONE existing Reserved cache row,
// cancel on eviction/close, and execute crypto outside all state-owner locks.
// All R and direct sessions of this target share this ONE calendar. Per-session
// credits live on Session until connection close, never reset when a job ends.
type SigningCalendar struct {
	mu                              sync.Mutex
	spacing                         Instant
	limit                           int
	nextProjected, nextActual, last Instant
	slots                           map[*signingSlot]struct{}
}

func NewSigningCalendar(profile SigningProfile, start Instant) (*SigningCalendar, error) {
	if start < 0 || profile.NodeSpacing < 250*time.Millisecond || profile.NodeSpacing > 6*time.Second || profile.MaxReservations < 1 || profile.MaxReservations > MaxTargetEntries {
		return nil, ErrInvalid
	}
	return &SigningCalendar{spacing: Instant(profile.NodeSpacing), limit: profile.MaxReservations, nextProjected: start, nextActual: start, last: start, slots: make(map[*signingSlot]struct{})}, nil
}
func (c *SigningCalendar) Reserve(source Session, now, latest Instant) (SigningReservation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return SigningReservation{}, ErrClock
	}
	c.last = now
	c.expireSlots(now)
	if !source.valid() {
		return SigningReservation{}, ErrUnauthenticated
	}
	if latest < now || latest-now > responderTimeout {
		return SigningReservation{}, ErrBudget
	}
	if len(c.slots) >= c.limit {
		return SigningReservation{}, ErrCapacity
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	if source.state.signer != nil && source.state.signer != c {
		return SigningReservation{}, ErrInvalid
	}
	projected := source.state.signingProjected
	if source.state.signer == nil {
		projected = forwardBucket{at: now, tokens: forwardBurst}
	}
	earliest := max(now, c.nextProjected)
	projected, at, err := projected.project(earliest)
	if err != nil {
		return SigningReservation{}, err
	}
	if at > latest {
		return SigningReservation{}, ErrBudget
	}
	next, err := at.plus(c.spacing)
	if err != nil {
		return SigningReservation{}, err
	}
	if source.state.signer == nil {
		source.state.signer = c
		source.state.signingActual = forwardBucket{at: now, tokens: forwardBurst}
	}
	source.state.signingProjected = projected
	c.nextProjected = next
	slot := &signingSlot{source: source, at: at, latest: latest}
	c.slots[slot] = struct{}{}
	return SigningReservation{calendar: c, slot: slot}, nil
}
func (c *SigningCalendar) Consume(r SigningReservation, now Instant) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return ErrClock
	}
	c.last = now
	c.expireSlots(now)
	if r.calendar != c || r.slot == nil {
		return ErrStaleWork
	}
	if _, ok := c.slots[r.slot]; !ok {
		return ErrStaleWork
	}
	if now > r.slot.latest {
		delete(c.slots, r.slot)
		return ErrBudget
	}
	if !r.slot.source.valid() {
		delete(c.slots, r.slot)
		return ErrUnauthenticated
	}
	if now < r.slot.at || now < c.nextActual {
		return ErrNotReady
	}
	next, err := now.plus(c.spacing)
	if err != nil {
		return err
	}
	state := r.slot.source.state
	state.mu.Lock()
	defer state.mu.Unlock()
	state.signingActual.refill(now)
	if state.signingActual.tokens < forwardToken {
		return ErrNotReady
	}
	state.signingActual.tokens -= forwardToken
	c.nextActual = next
	delete(c.slots, r.slot)
	return nil
}
func (c *SigningCalendar) Cancel(r SigningReservation) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.calendar != c || r.slot == nil {
		return false
	}
	if _, ok := c.slots[r.slot]; !ok {
		return false
	}
	delete(c.slots, r.slot)
	return true
}
func (c *SigningCalendar) CloseSession(source Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	source.close()
	for slot := range c.slots {
		if slot.source == source {
			delete(c.slots, slot)
		}
	}
}
func (c *SigningCalendar) Pending() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.slots) }

// Expire releases overdue aliases without refunding projected or actual tokens.
func (c *SigningCalendar) Expire(now Instant) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return ErrClock
	}
	c.last = now
	c.expireSlots(now)
	return nil
}
func (c *SigningCalendar) expireSlots(now Instant) {
	for slot := range c.slots {
		if now > slot.latest {
			delete(c.slots, slot)
		}
	}
}
