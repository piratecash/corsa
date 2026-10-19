package findnode

import (
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// PairKey keeps one source SESSION and an exact target IDENTITY, not a target
// connection. A future bounded registry MUST use this key across target links.
type PairKey struct {
	source Session
	target domain.PeerIdentity
}

func NewPairKey(source Session, target domain.PeerIdentity) (PairKey, error) {
	if !source.valid() || target.IsZero() || source.Identity() == target {
		return PairKey{}, ErrInvalid
	}
	return PairKey{source: source, target: target}, nil
}

// PairWindow is one caller-owned (proven source session, exact target) gate.
// It has three timestamps and no attacker-keyed map. Registry lifetime and
// bounded distinct-target accounting belong to the future enclosing admission.
type PairWindow struct {
	key   PairKey
	mu    sync.Mutex
	times [3]Instant
	count int
	last  Instant
}

func NewPairWindow(key PairKey) (*PairWindow, error) {
	if !key.source.valid() || key.target.IsZero() {
		return nil, ErrInvalid
	}
	return &PairWindow{key: key}, nil
}
func (p *PairWindow) Allow(now Instant) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if now < 0 || now < p.last || !p.key.source.valid() || p.key.target.IsZero() {
		return false
	}
	p.last = now
	kept := 0
	for i := 0; i < p.count; i++ {
		if now-p.times[i] < Instant(30*time.Second) {
			p.times[kept] = p.times[i]
			kept++
		}
	}
	p.count = kept
	if p.count == len(p.times) {
		return false
	}
	p.times[p.count] = now
	p.count++
	return true
}

// TimingProfile is a proposal input, with no production default. Its values
// must include admission-round/runtime delay and be approved/measured before
// G2. R's reservation cannot reserve T's separate, contended signing budget.
type TimingProfile struct{ Target, Network, Runtime time.Duration }

func (p TimingProfile) LatestForward(received Instant) (Instant, error) {
	if p.Target <= 0 || p.Network <= 0 || p.Runtime <= 0 {
		return 0, ErrInvalid
	}
	if p.Target >= time.Duration(responderTimeout) || p.Network >= time.Duration(responderTimeout) || p.Runtime >= time.Duration(responderTimeout) {
		return 0, ErrInvalid
	}
	sum := p.Target + p.Network + p.Runtime
	if sum >= time.Duration(responderTimeout) {
		return 0, ErrInvalid
	}
	return received.plus(responderTimeout - Instant(sum))
}

type forwardBucket struct{ at, tokens Instant }

const forwardToken = Instant(time.Second)
const forwardBurst = 4 * forwardToken

func (b *forwardBucket) refill(now Instant) {
	if now <= b.at {
		return
	}
	delta := now - b.at
	if delta >= forwardBurst-b.tokens {
		b.tokens = forwardBurst
	} else {
		b.tokens += delta
	}
	b.at = now
}
func (b forwardBucket) project(now Instant) (forwardBucket, Instant, error) {
	b.refill(now)
	if b.tokens >= forwardToken {
		b.tokens -= forwardToken
		return b, b.at, nil
	}
	at, err := b.at.plus(forwardToken - b.tokens)
	if err != nil {
		return b, 0, err
	}
	b.at = at
	b.tokens = 0
	return b, at, nil
}

type forwardSlot struct{ at, latest Instant }
type Reservation struct {
	calendar *ForwardCalendar
	slot     *forwardSlot
}

func (r Reservation) At() Instant {
	if r.slot == nil {
		return 0
	}
	return r.slot.at
}

// ForwardCalendar is a conservative append-only reservation witness for ONE
// R-to-target identity. All connections to that identity share this object. All levels share it, including their existing spent
// tokens and reservations. It does not implement online fair intake rounds.
// Cancel releases physical occupancy but never refunds the projected token.
// Consume separately charges actual-time tokens, so delayed jobs cannot bunch
// into more than the existing 1/s, burst-4 allowance.
type ForwardCalendar struct {
	target            domain.PeerIdentity
	mu                sync.Mutex
	projected, actual forwardBucket
	last              Instant
	slots             map[*forwardSlot]struct{}
}

func NewForwardCalendar(target domain.PeerIdentity, now Instant, initialTokens uint32) (*ForwardCalendar, error) {
	if target.IsZero() || now < 0 || initialTokens > 4 {
		return nil, ErrInvalid
	}
	bucket := forwardBucket{at: now, tokens: Instant(initialTokens) * forwardToken}
	return &ForwardCalendar{target: target, projected: bucket, actual: bucket, last: now, slots: make(map[*forwardSlot]struct{})}, nil
}
func (c *ForwardCalendar) Reserve(now, latest Instant) (Reservation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return Reservation{}, ErrClock
	}
	c.last = now
	c.expireSlots(now)
	if latest < now || latest-now > responderTimeout {
		return Reservation{}, ErrBudget
	}
	if len(c.slots) >= MaxPendingPerTarget {
		return Reservation{}, ErrCapacity
	}
	projected, at, err := c.projected.project(now)
	if err != nil {
		return Reservation{}, err
	}
	if at > latest {
		return Reservation{}, ErrBudget
	}
	slot := &forwardSlot{at: at, latest: latest}
	c.slots[slot] = struct{}{}
	c.projected = projected
	return Reservation{calendar: c, slot: slot}, nil
}
func (c *ForwardCalendar) Cancel(r Reservation) bool {
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
func (c *ForwardCalendar) Consume(r Reservation, now Instant) error {
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
	if now < r.slot.at {
		return ErrNotReady
	}
	c.actual.refill(now)
	if c.actual.tokens < forwardToken {
		return ErrNotReady
	}
	c.actual.tokens -= forwardToken
	delete(c.slots, r.slot)
	return nil
}
func (c *ForwardCalendar) Pending() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.slots) }

// Expire releases overdue aliases without refunding projected or actual tokens.
func (c *ForwardCalendar) Expire(now Instant) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return ErrClock
	}
	c.last = now
	c.expireSlots(now)
	return nil
}
func (c *ForwardCalendar) expireSlots(now Instant) {
	for slot := range c.slots {
		if now > slot.latest {
			delete(c.slots, slot)
		}
	}
}

func (c *ForwardCalendar) Target() domain.PeerIdentity { return c.target }

type combinedForwardPlan struct {
	projected forwardBucket
	slot      forwardSlot
}

// planCombinedLocked requires c.mu. It never publishes virtual actual debt.
// All remaining aliases belong to the private enclosing owner's pending rows.
func (c *ForwardCalendar) planCombinedLocked(now, latest Instant) (combinedForwardPlan, error) {
	if now < 0 || now < c.last {
		return combinedForwardPlan{}, ErrClock
	}
	if latest < now || latest-now > responderTimeout {
		return combinedForwardPlan{}, ErrBudget
	}
	if len(c.slots) >= MaxPendingPerTarget {
		return combinedForwardPlan{}, ErrCapacity
	}
	horizon := now
	for slot := range c.slots {
		if slot.latest < now || slot.at > slot.latest {
			return combinedForwardPlan{}, ErrInvalid
		}
		horizon = max(horizon, slot.latest)
	}
	actual := c.actual
	for range len(c.slots) {
		var err error
		actual, _, err = actual.project(horizon)
		if err != nil {
			return combinedForwardPlan{}, err
		}
	}
	_, floor, err := actual.project(horizon)
	if err != nil {
		return combinedForwardPlan{}, err
	}
	projected, at, err := c.projected.project(max(now, floor))
	if err != nil {
		return combinedForwardPlan{}, err
	}
	if at > latest {
		return combinedForwardPlan{}, ErrBudget
	}
	return combinedForwardPlan{projected: projected, slot: forwardSlot{at: at, latest: latest}}, nil
}

// applyCombinedLocked requires c.mu and the unchanged successful trial state.
func (c *ForwardCalendar) applyCombinedLocked(plan combinedForwardPlan, slot *forwardSlot, now Instant) {
	c.last = now
	c.projected = plan.projected
	c.slots[slot] = struct{}{}
}

// retireableLocked requires c.mu. Physical alias release never refunds debt.
func (c *ForwardCalendar) retireableLocked(now Instant) bool {
	if len(c.slots) != 0 || now < c.last || now < c.projected.at || now < c.actual.at {
		return false
	}
	projected, actual := c.projected, c.actual
	projected.refill(now)
	actual.refill(now)
	return projected.tokens == forwardBurst && actual.tokens == forwardBurst
}
