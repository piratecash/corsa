package findnode

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Frozen pre-C2c full map implementation follows below, with only identifiers
// mechanically renamed. It never calls the new representation helpers.
type aliasOldBucket struct{ at, tokens Instant }

const aliasOldToken = Instant(time.Second)
const aliasOldBurst = 4 * aliasOldToken

func (b *aliasOldBucket) refill(now Instant) {
	if now <= b.at {
		return
	}
	delta := now - b.at
	if delta >= aliasOldBurst-b.tokens {
		b.tokens = aliasOldBurst
	} else {
		b.tokens += delta
	}
	b.at = now
}
func (b aliasOldBucket) project(now Instant) (aliasOldBucket, Instant, error) {
	b.refill(now)
	if b.tokens >= aliasOldToken {
		b.tokens -= aliasOldToken
		return b, b.at, nil
	}
	at, err := b.at.plus(aliasOldToken - b.tokens)
	if err != nil {
		return b, 0, err
	}
	b.at = at
	b.tokens = 0
	return b, at, nil
}

type aliasOldSlot struct{ at, latest Instant }
type aliasOldReservation struct {
	calendar *aliasOldCalendar
	slot     *aliasOldSlot
}

func (r aliasOldReservation) At() Instant {
	if r.slot == nil {
		return 0
	}
	return r.slot.at
}

// aliasOldCalendar is a conservative append-only reservation witness for ONE
// R-to-target identity. All connections to that identity share this object. All levels share it, including their existing spent
// tokens and reservations. It does not implement online fair intake rounds.
// Cancel releases physical occupancy but never refunds the projected token.
// Consume separately charges actual-time tokens, so delayed jobs cannot bunch
// into more than the existing 1/s, burst-4 allowance.
type aliasOldCalendar struct {
	target            domain.PeerIdentity
	mu                sync.Mutex
	projected, actual aliasOldBucket
	last              Instant
	slots             map[*aliasOldSlot]struct{}
}

func aliasOldNewCalendar(target domain.PeerIdentity, now Instant, initialTokens uint32) (*aliasOldCalendar, error) {
	if target.IsZero() || now < 0 || initialTokens > 4 {
		return nil, ErrInvalid
	}
	bucket := aliasOldBucket{at: now, tokens: Instant(initialTokens) * aliasOldToken}
	return &aliasOldCalendar{target: target, projected: bucket, actual: bucket, last: now, slots: make(map[*aliasOldSlot]struct{})}, nil
}
func (c *aliasOldCalendar) Reserve(now, latest Instant) (aliasOldReservation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return aliasOldReservation{}, ErrClock
	}
	c.last = now
	c.expireSlots(now)
	if latest < now || latest-now > responderTimeout {
		return aliasOldReservation{}, ErrBudget
	}
	if len(c.slots) >= MaxPendingPerTarget {
		return aliasOldReservation{}, ErrCapacity
	}
	projected, at, err := c.projected.project(now)
	if err != nil {
		return aliasOldReservation{}, err
	}
	if at > latest {
		return aliasOldReservation{}, ErrBudget
	}
	slot := &aliasOldSlot{at: at, latest: latest}
	c.slots[slot] = struct{}{}
	c.projected = projected
	return aliasOldReservation{calendar: c, slot: slot}, nil
}
func (c *aliasOldCalendar) Cancel(r aliasOldReservation) bool {
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
func (c *aliasOldCalendar) Consume(r aliasOldReservation, now Instant) error {
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
	if c.actual.tokens < aliasOldToken {
		return ErrNotReady
	}
	c.actual.tokens -= aliasOldToken
	delete(c.slots, r.slot)
	return nil
}
func (c *aliasOldCalendar) Pending() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.slots) }

// Expire releases overdue aliases without refunding projected or actual tokens.
func (c *aliasOldCalendar) Expire(now Instant) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now < 0 || now < c.last {
		return ErrClock
	}
	c.last = now
	c.expireSlots(now)
	return nil
}
func (c *aliasOldCalendar) expireSlots(now Instant) {
	for slot := range c.slots {
		if now > slot.latest {
			delete(c.slots, slot)
		}
	}
}

func (c *aliasOldCalendar) Target() domain.PeerIdentity { return c.target }

type aliasOldCombinedPlan struct {
	projected aliasOldBucket
	slot      aliasOldSlot
}

// planCombinedFloorLocked preserves the real clock/deadline checks. A source
// floor is only a projected lower bound, never the timestamp of a charge.
func (c *aliasOldCalendar) planCombinedFloorLocked(now, latest, sourceFloor Instant) (aliasOldCombinedPlan, error) {
	if sourceFloor < now {
		return aliasOldCombinedPlan{}, ErrInvalid
	}
	if now < 0 || now < c.last {
		return aliasOldCombinedPlan{}, ErrClock
	}
	if latest < now || latest-now > responderTimeout {
		return aliasOldCombinedPlan{}, ErrBudget
	}
	if len(c.slots) >= MaxPendingPerTarget {
		return aliasOldCombinedPlan{}, ErrCapacity
	}
	horizon := now
	for slot := range c.slots {
		if slot.latest < now || slot.at > slot.latest {
			return aliasOldCombinedPlan{}, ErrInvalid
		}
		horizon = max(horizon, slot.latest)
	}
	actual := c.actual
	for range len(c.slots) {
		var err error
		actual, _, err = actual.project(horizon)
		if err != nil {
			return aliasOldCombinedPlan{}, err
		}
	}
	_, floor, err := actual.project(horizon)
	if err != nil {
		return aliasOldCombinedPlan{}, err
	}
	projected, at, err := c.projected.project(max(now, floor, sourceFloor))
	if err != nil {
		return aliasOldCombinedPlan{}, err
	}
	if at > latest {
		return aliasOldCombinedPlan{}, ErrBudget
	}
	return aliasOldCombinedPlan{projected: projected, slot: aliasOldSlot{at: at, latest: latest}}, nil
}

// applyCombinedLocked requires c.mu and the unchanged successful trial state.
func (c *aliasOldCalendar) applyCombinedLocked(plan aliasOldCombinedPlan, slot *aliasOldSlot, now Instant) {
	c.last = now
	c.projected = plan.projected
	c.slots[slot] = struct{}{}
}

// Tickets preserve pointer multiplicity across two separately allocated books.
type aliasFixedTicket struct {
	current   Reservation
	reference aliasOldReservation
}
type aliasFixedPair struct {
	current   *ForwardCalendar
	reference *aliasOldCalendar
	tickets   []aliasFixedTicket
	owned     bool
}

func aliasFixedNewPair(t *testing.T, owned bool, tokens uint32) *aliasFixedPair {
	t.Helper()
	target := candidateTarget(920001)
	var c *ForwardCalendar
	var err error
	if owned {
		c, err = newOwnedForwardCalendar(target, 0, tokens)
	} else {
		c, err = NewForwardCalendar(target, 0, tokens)
	}
	old, oldErr := aliasOldNewCalendar(target, 0, tokens)
	if err != nil || oldErr != nil {
		t.Fatal("calendar pair constructor", err, oldErr)
	}
	p := &aliasFixedPair{current: c, reference: old, owned: owned}
	p.check(t)
	return p
}
func (p *aliasFixedPair) check(t *testing.T) {
	t.Helper()
	c, old := p.current, p.reference
	c.mu.Lock()
	defer c.mu.Unlock()
	old.mu.Lock()
	defer old.mu.Unlock()
	if c.target != old.target || c.last != old.last || c.projected.at != old.projected.at || c.projected.tokens != old.projected.tokens || c.actual.at != old.actual.at || c.actual.tokens != old.actual.tokens {
		t.Fatal("calendar scalar/debt differs from frozen map")
	}
	active := map[*forwardSlot]bool{}
	if p.owned {
		if !c.ownedAliases || c.slots != nil || c.aliasCount < 0 || c.aliasCount > MaxPendingPerTarget {
			t.Fatal("owned calendar is not a single fixed prefix")
		}
		for i := 0; i < c.aliasCount; i++ {
			if c.aliases[i] == nil || active[c.aliases[i]] {
				t.Fatal("nil/duplicate fixed alias pointer")
			}
			active[c.aliases[i]] = true
		}
		for i := c.aliasCount; i < len(c.aliases); i++ {
			if c.aliases[i] != nil {
				t.Fatal("inactive fixed alias tail retains pointer")
			}
		}
	} else {
		if c.ownedAliases || c.aliasCount != 0 || c.aliases != ([MaxPendingPerTarget]*forwardSlot{}) {
			t.Fatal("public map mode converted or gained fixed aliases")
		}
		for slot := range c.slots {
			active[slot] = true
		}
	}
	if len(active) != len(old.slots) {
		t.Fatal("alias count differs from frozen map")
	}
	seen := 0
	for _, ticket := range p.tickets {
		_, want := old.slots[ticket.reference.slot]
		if active[ticket.current.slot] != want {
			t.Fatal("exact pointer membership differs from frozen map")
		}
		if want {
			seen++
			if ticket.current.slot.at != ticket.reference.slot.at || ticket.current.slot.latest != ticket.reference.slot.latest {
				t.Fatal("alias value differs from frozen map")
			}
		}
	}
	if seen != len(active) {
		t.Fatal("unpaired alias escaped independent ticket inventory")
	}
}
func (p *aliasFixedPair) reserve(t *testing.T, now, latest Instant) (int, error) {
	t.Helper()
	r, err := p.current.Reserve(now, latest)
	old, oldErr := p.reference.Reserve(now, latest)
	if err != oldErr || r.At() != old.At() {
		t.Fatal("reserve return differs from frozen map", err, oldErr, r.At(), old.At())
	}
	id := -1
	if err == nil {
		id = len(p.tickets)
		p.tickets = append(p.tickets, aliasFixedTicket{r, old})
	}
	p.check(t)
	return id, err
}
func (p *aliasFixedPair) cancel(t *testing.T, id int) bool {
	t.Helper()
	ticket := p.tickets[id]
	got, want := p.current.Cancel(ticket.current), p.reference.Cancel(ticket.reference)
	if got != want {
		t.Fatal("cancel result differs from frozen map")
	}
	p.check(t)
	return got
}
func (p *aliasFixedPair) consume(t *testing.T, id int, now Instant) error {
	t.Helper()
	ticket := p.tickets[id]
	got, want := p.current.Consume(ticket.current, now), p.reference.Consume(ticket.reference, now)
	if got != want {
		t.Fatal("consume result differs from frozen map", got, want)
	}
	p.check(t)
	return got
}
func (p *aliasFixedPair) expire(t *testing.T, now Instant) {
	t.Helper()
	got, want := p.current.Expire(now), p.reference.Expire(now)
	if got != want {
		t.Fatal("expire result differs from frozen map", got, want)
	}
	p.check(t)
}
func aliasFixedMustReserve(t *testing.T, p *aliasFixedPair, now, latest Instant) int {
	t.Helper()
	id, err := p.reserve(t, now, latest)
	if err != nil {
		t.Fatal("positive reserve", err)
	}
	return id
}
func aliasFixedSixteen(t *testing.T, p *aliasFixedPair) {
	t.Helper()
	for range 10 {
		aliasFixedMustReserve(t, p, 0, Instant(6*time.Second))
	}
	for second := 1; second <= 6; second++ {
		now := Instant(time.Duration(second) * time.Second)
		aliasFixedMustReserve(t, p, now, now+Instant(6*time.Second))
	}
	if p.current.Pending() != 16 || p.reference.Pending() != 16 {
		t.Fatal("reachable sixteen aliases not retained at equal deadline")
	}
}
func TestOwnedForwardAliasesPublicParity(t *testing.T) {
	for _, name := range []string{"invalid-constructor", "burst-no-refund", "exact-deadline", "delayed-actual", "sixteen-existing-limit", "foreign-reservation", "clock-and-invalid-latest-effects", "overflow"} {
		t.Run(name, func(t *testing.T) {
			for _, owned := range []bool{false, true} {
				p := aliasFixedNewPair(t, owned, 4)
				switch name {
				case "invalid-constructor":
					for _, tc := range []struct {
						target domain.PeerIdentity
						at     Instant
						tokens uint32
					}{{domain.PeerIdentity{}, 0, 4}, {candidateTarget(1), -1, 4}, {candidateTarget(1), 0, 5}} {
						var got *ForwardCalendar
						var err error
						if owned {
							got, err = newOwnedForwardCalendar(tc.target, tc.at, tc.tokens)
						} else {
							got, err = NewForwardCalendar(tc.target, tc.at, tc.tokens)
						}
						old, oldErr := aliasOldNewCalendar(tc.target, tc.at, tc.tokens)
						if got != nil || old != nil || !errors.Is(err, ErrInvalid) || err != oldErr {
							t.Fatal("invalid constructor differs from frozen map", err, oldErr)
						}
					}
				case "burst-no-refund":
					for range 4 {
						id := aliasFixedMustReserve(t, p, 0, Instant(6*time.Second))
						if !p.cancel(t, id) {
							t.Fatal("positive burst cancel")
						}
					}
					id := aliasFixedMustReserve(t, p, 0, Instant(6*time.Second))
					if p.tickets[id].current.At() != Instant(time.Second) {
						t.Fatal("cancel refunded projected spend")
					}
				case "exact-deadline":
					id := aliasFixedMustReserve(t, p, 0, Instant(time.Second))
					p.expire(t, Instant(time.Second))
					if p.current.Pending() != 1 {
						t.Fatal("equal deadline expired")
					}
					if err := p.consume(t, id, Instant(time.Second)); err != nil {
						t.Fatal(err)
					}
				case "delayed-actual":
					ids := []int{}
					for range 4 {
						ids = append(ids, aliasFixedMustReserve(t, p, Instant(100*time.Millisecond), Instant(750*time.Millisecond)))
					}
					for _, id := range ids {
						if err := p.consume(t, id, Instant(600*time.Millisecond)); err != nil {
							t.Fatal(err)
						}
					}
					id := aliasFixedMustReserve(t, p, Instant(700*time.Millisecond), Instant(1350*time.Millisecond))
					if err := p.consume(t, id, p.tickets[id].current.At()); !errors.Is(err, ErrNotReady) {
						t.Fatal("delayed actual spend escaped bucket", err)
					}
				case "sixteen-existing-limit":
					aliasFixedSixteen(t, p)
					if _, err := p.reserve(t, Instant(6*time.Second), Instant(12*time.Second)); !errors.Is(err, ErrCapacity) {
						t.Fatal("seventeenth alias accepted", err)
					}
				case "foreign-reservation":
					id := aliasFixedMustReserve(t, p, 0, Instant(time.Second))
					other := aliasFixedNewPair(t, owned, 4)
					if other.current.Cancel(p.tickets[id].current) || other.reference.Cancel(p.tickets[id].reference) {
						t.Fatal("foreign reservation cancelled")
					}
					if got, want := other.current.Consume(p.tickets[id].current, 0), other.reference.Consume(p.tickets[id].reference, 0); got != want || !errors.Is(got, ErrStaleWork) {
						t.Fatal("foreign consume", got, want)
					}
					other.check(t)
					p.check(t)
				case "clock-and-invalid-latest-effects":
					aliasFixedMustReserve(t, p, 0, Instant(time.Second))
					p.expire(t, Instant(2*time.Second))
					if _, err := p.reserve(t, Instant(3*time.Second), Instant(2*time.Second)); !errors.Is(err, ErrBudget) {
						t.Fatal(err)
					}
					if p.current.last != Instant(3*time.Second) {
						t.Fatal("invalid latest incorrectly rolled back prior clock/expiry effects")
					}
					if _, err := p.reserve(t, Instant(time.Second), Instant(2*time.Second)); !errors.Is(err, ErrClock) {
						t.Fatal(err)
					}
				case "overflow":
					now := Instant(math.MaxInt64 - 2)
					for range 4 {
						aliasFixedMustReserve(t, p, now, Instant(math.MaxInt64-1))
					}
					if _, err := p.reserve(t, now, Instant(math.MaxInt64-1)); err == nil {
						t.Fatal("overflow reservation accepted")
					}
				}
			}
		})
	}
}
func TestOwnedForwardAliasesDenseMoves(t *testing.T) {
	for _, name := range []string{"first", "middle", "last", "swap-expired", "deadline-equal", "retained-slot-pointer"} {
		t.Run(name, func(t *testing.T) {
			p := aliasFixedNewPair(t, true, 4)
			for _, latest := range []Instant{Instant(time.Second), Instant(3 * time.Second), Instant(2 * time.Second)} {
				aliasFixedMustReserve(t, p, 0, latest)
			}
			switch name {
			case "first", "middle", "last":
				index := map[string]int{"first": 0, "middle": 1, "last": 2}[name]
				before := *p.tickets[index].current.slot
				if !p.cancel(t, index) {
					t.Fatal("live pointer could not cancel")
				}
				if *p.tickets[index].current.slot != before || p.cancel(t, index) {
					t.Fatal("removed slot object rewritten or reused")
				}
			case "swap-expired":
				p.expire(t, Instant(2500*time.Millisecond))
				if p.current.Pending() != 1 {
					t.Fatal("swapped expired alias survived")
				}
			case "deadline-equal":
				p.expire(t, Instant(time.Second))
				if p.current.Pending() != 3 {
					t.Fatal("equality removed an alias")
				}
			case "retained-slot-pointer":
				p = aliasFixedNewPair(t, true, 4)
				first := aliasFixedMustReserve(t, p, 0, Instant(3*time.Second))
				second := aliasFixedMustReserve(t, p, 0, Instant(3*time.Second))
				if p.tickets[first].current.slot == p.tickets[second].current.slot {
					t.Fatal("fixture aliases share pointer")
				}
				if !p.cancel(t, second) {
					t.Fatal("second equal-valued pointer missing")
				}
				if !p.cancel(t, first) {
					t.Fatal("equal-valued first pointer was accidentally removed")
				}
			}
		})
	}
}
func TestOwnedForwardAliasesCombinedParity(t *testing.T) {
	for _, name := range []string{"tokens-zero", "tokens-four", "future-alias-floor", "source-floor", "full-sixteen", "invalid-time"} {
		t.Run(name, func(t *testing.T) {
			for _, owned := range []bool{false, true} {
				tokens := uint32(4)
				if name == "tokens-zero" {
					tokens = 0
				}
				p := aliasFixedNewPair(t, owned, tokens)
				now, latest, floor := Instant(0), Instant(6*time.Second), Instant(0)
				switch name {
				case "future-alias-floor":
					for range 4 {
						aliasFixedMustReserve(t, p, Instant(500*time.Millisecond), Instant(2*time.Second))
					}
					now = Instant(600 * time.Millisecond)
					floor = now
					latest = Instant(3 * time.Second)
				case "source-floor":
					now = Instant(time.Second)
					floor = Instant(3 * time.Second)
				case "full-sixteen":
					aliasFixedSixteen(t, p)
					now = Instant(6 * time.Second)
					floor = now
					latest = Instant(12 * time.Second)
				case "invalid-time":
					now = -1
					floor = 0
				}
				before := calendarOldRead(p.current)
				p.current.mu.Lock()
				got, err := p.current.planCombinedFloorLocked(now, latest, floor)
				p.current.mu.Unlock()
				p.reference.mu.Lock()
				want, werr := p.reference.planCombinedFloorLocked(now, latest, floor)
				p.reference.mu.Unlock()
				if err != werr || got.slot.at != Instant(want.slot.at) || got.slot.latest != Instant(want.slot.latest) || got.projected.at != want.projected.at || got.projected.tokens != want.projected.tokens {
					t.Fatal("combined query differs from frozen map", name, err, werr, got, want)
				}
				if !reflect.DeepEqual(calendarOldRead(p.current), before) {
					t.Fatal("combined trial mutated original calendar")
				}
				p.check(t)
				if owned && err == nil {
					slot := &forwardSlot{at: got.slot.at, latest: got.slot.latest}
					oldSlot := &aliasOldSlot{at: want.slot.at, latest: want.slot.latest}
					p.current.mu.Lock()
					p.current.applyCombinedLocked(got, slot, now)
					p.current.mu.Unlock()
					p.reference.mu.Lock()
					p.reference.applyCombinedLocked(want, oldSlot, now)
					p.reference.mu.Unlock()
					p.tickets = append(p.tickets, aliasFixedTicket{Reservation{calendar: p.current, slot: slot}, aliasOldReservation{calendar: p.reference, slot: oldSlot}})
					p.check(t)
				}
			}
		})
	}
}
func aliasFixedOriginalOffer(t *testing.T, o *intakeCoordinator) (candidateReservation, Session, Session) {
	t.Helper()
	source, _ := provenFixture(t, 1, "alias-source", 0)
	target, _ := provenFixture(t, 2, "alias-target", 0)
	r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if err != nil {
		t.Fatal(err)
	}
	return r, source, target
}
func TestOwnedForwardAliasesOriginalOwnerAtomicity(t *testing.T) {
	for _, name := range []string{"constructor-owned-no-map", "exhausted-id-no-effects", "joint-deadline-no-effects", "public-mode-refused-before-effects"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			if name == "constructor-owned-no-map" {
				c, e := newOwnedForwardCalendar(candidateTarget(920002), 0, 4)
				if e != nil || c == nil || !c.ownedAliases || c.slots != nil || c.aliasCount != 0 {
					t.Fatal("owned constructor allocated public map storage", e)
				}
			}
			if name == "joint-deadline-no-effects" {
				o.profile.timing = TimingProfile{Target: time.Second, Network: time.Second, Runtime: 3250 * time.Millisecond}
			}
			r, source, target := aliasFixedOriginalOffer(t, o)
			if name == "exhausted-id-no-effects" {
				target.state.exhausted = true
			}
			if name == "joint-deadline-no-effects" || name == "public-mode-refused-before-effects" {
				var c *ForwardCalendar
				var err error
				if name == "public-mode-refused-before-effects" {
					c, err = NewForwardCalendar(target.Identity(), 0, 4)
				} else {
					c, err = newOwnedForwardCalendar(target.Identity(), 0, 0)
				}
				if err != nil {
					t.Fatal(err)
				}
				calendarFixedSeed(t, o, c)
			}
			prepared := o.prepareSources()
			before := calendarFixedAccounting(o, []Session{source, target})
			err := calendarFixedCommitDirect(o, r, o.profile.round, prepared[source])
			if name == "constructor-owned-no-map" {
				if err != nil {
					t.Fatal(err)
				}
				c := r.entry.intake.calendar
				if c == nil || !c.ownedAliases || c.slots != nil || c.aliasCount != 1 || c.aliases[0] != r.entry.intake.alias {
					t.Fatal("owner constructor did not bind fixed alias prefix")
				}
			} else {
				want := error(ErrCapacity)
				if name == "exhausted-id-no-effects" {
					want = ErrStaleWork
				}
				if name == "joint-deadline-no-effects" {
					want = ErrBudget
				}
				if name == "public-mode-refused-before-effects" {
					want = ErrInvalid
				}
				if !errors.Is(err, want) {
					t.Fatal("owner preclaim refusal did not hold", name, err, want)
				}
				if !reflect.DeepEqual(calendarFixedAccounting(o, []Session{source, target}), before) {
					t.Fatal("owner preclaim refusal changed accounting")
				}
			}
		})
	}
}
func TestOwnedForwardAliasesRetirementAndRetainedPublicCalls(t *testing.T) {
	for _, name := range []string{"old-retained-reserve", "At-after-cancel", "replacement-pointer-target", "old-debt-preserved"} {
		t.Run(name, func(t *testing.T) {
			if name == "At-after-cancel" {
				p := aliasFixedNewPair(t, true, 4)
				id := aliasFixedMustReserve(t, p, Instant(2*time.Second), Instant(3*time.Second))
				r := p.tickets[id].current
				if !p.cancel(t, id) || r.At() != Instant(2*time.Second) {
					t.Fatal("retained At changed after cancel")
				}
				return
			}
			o := candidateTestOwner(t)
			calendarFixedFill(t, o, 1)
			old := o.calendars[0]
			oldTarget := old.Target()
			ended, err := old.Reserve(0, Instant(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if !old.Cancel(ended) {
				t.Fatal("retirement debt fixture cancel")
			}
			before := calendarOldRead(old)
			source, _ := provenFixture(t, 1, "replacement-source", 0)
			target, _ := provenFixture(t, 2, "replacement-target", 0)
			r, out := candidateSourceOfferSeal(t, o, source, target, 1, Instant(5*time.Second))
			if out.reserved != 1 {
				t.Fatal("genuine replacement did not reserve", out)
			}
			next := r.entry.intake.calendar
			if next == old || next.Target() != target.Identity() || old.Target() != oldTarget || !reflect.DeepEqual(calendarOldRead(old), before) {
				t.Fatal("retired original calendar reused or rewritten")
			}
			if ended.At() != 0 || old.Cancel(ended) || next.Cancel(ended) {
				t.Fatal("retained old reservation acquired replacement authority")
			}
			if name == "old-retained-reserve" {
				newBefore := calendarOldRead(next)
				late, err := old.Reserve(Instant(6*time.Second), Instant(7*time.Second))
				if err != nil || late.At() != Instant(6*time.Second) {
					t.Fatal("retained owned public Reserve failed", err)
				}
				if !reflect.DeepEqual(calendarOldRead(next), newBefore) || !old.ownedAliases || old.slots != nil {
					t.Fatal("old retained public call mutated canonical replacement")
				}
			}
			if name == "old-debt-preserved" && old.projected != (forwardBucket{at: 0, tokens: 3 * forwardToken}) {
				t.Fatal("retirement erased nonrefundable old debt")
			}
		})
	}
}

// These traces use real operation locks and causal channels; timeouts are only
// bounded failure containment, never accepted functional mutation evidence.
func aliasFixedWait(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatal("alias fixture worker did not join")
	}
}
func TestOwnedForwardAliasesConcurrentOwnerOperations(t *testing.T) {
	t.Run("take-cancel", func(t *testing.T) {
		o := candidateTestOwner(t)
		r, source, target := aliasFixedOriginalOffer(t, o)
		cut := o.profile.round
		out, err := o.sealRound(candidateView(t, o, cut, []Session{source}, []Session{source}, target), cut)
		if err != nil || out.reserved != 1 {
			t.Fatal(out, err)
		}
		type taken struct {
			permit candidateForwardPermit
			err    error
		}
		result := make(chan taken, 1)
		finish := make(chan struct{})
		joined := make(chan struct{})
		finished := make(chan error, 1)
		var once sync.Once
		release := func() { once.Do(func() { close(finish) }) }
		t.Cleanup(func() { release(); aliasFixedWait(t, joined) })
		go func() {
			defer close(joined)
			p, _, e := o.takeForward(r, cut)
			result <- taken{p, e}
			<-finish
			if e == nil {
				e = o.finishForward(p, cut, candidateWriteFinished)
			}
			finished <- e
		}()
		var got taken
		select {
		case got = <-result:
		case <-time.After(2 * time.Second):
			t.Fatal("take did not complete")
		}
		if got.err != nil {
			t.Fatal(got.err)
		}
		cancelDone := make(chan struct{})
		cancelResult := make(chan error, 1)
		t.Cleanup(func() { aliasFixedWait(t, cancelDone) })
		go func() {
			defer close(cancelDone)
			ok, e := o.cancel(r, cut)
			if e == nil && !ok {
				e = ErrStaleWork
			}
			cancelResult <- e
		}()
		aliasFixedWait(t, cancelDone)
		if e := <-cancelResult; e != nil {
			t.Fatal(e)
		}
		if s := o.snapshot(); s.terminal != 1 || s.aliases != 0 || s.pending != 1 {
			t.Fatal("cancel lost genuine dispatch obligation", s)
		}
		release()
		aliasFixedWait(t, joined)
		if e := <-finished; e != nil {
			t.Fatal(e)
		}
		if s := o.snapshot(); s.pending != 0 {
			t.Fatal("sole Finish did not release terminal row", s)
		}
	})
	t.Run("reserve-expire", func(t *testing.T) {
		p := aliasFixedNewPair(t, true, 4)
		entered := make(chan struct{})
		reserved := make(chan Reservation, 1)
		errs := make(chan error, 1)
		joined := make(chan struct{})
		p.current.mu.Lock()
		var unlock sync.Once
		releaseMutex := func() { unlock.Do(p.current.mu.Unlock) }
		t.Cleanup(func() { releaseMutex(); aliasFixedWait(t, joined) })
		go func() {
			defer close(joined)
			close(entered)
			r, e := p.current.Reserve(0, Instant(time.Second))
			reserved <- r
			errs <- e
		}()
		aliasFixedWait(t, entered)
		releaseMutex()
		aliasFixedWait(t, joined)
		r, err := <-reserved, <-errs
		old, oldErr := p.reference.Reserve(0, Instant(time.Second))
		if err != nil || oldErr != nil {
			t.Fatal(err, oldErr)
		}
		p.tickets = append(p.tickets, aliasFixedTicket{r, old})
		p.check(t)
		expired := make(chan struct{})
		expiryErr := make(chan error, 1)
		t.Cleanup(func() { aliasFixedWait(t, expired) })
		go func() { defer close(expired); expiryErr <- p.current.Expire(Instant(2 * time.Second)) }()
		aliasFixedWait(t, expired)
		if e := <-expiryErr; e != nil {
			t.Fatal(e)
		}
		if e := p.reference.Expire(Instant(2 * time.Second)); e != nil {
			t.Fatal(e)
		}
		p.check(t)
		if p.current.Pending() != 0 {
			t.Fatal("joined expiry retained old alias")
		}
	})
}
