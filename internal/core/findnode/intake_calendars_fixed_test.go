package findnode

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// FROZEN scalar arithmetic from the accepted budget.go, mechanically renamed.
// No fixed-table read/count helper or production retireable method is called.
type calendarOldBucket struct{ at, tokens Instant }

const calendarOldToken = Instant(time.Second)
const calendarOldBurst = 4 * calendarOldToken

func (b *calendarOldBucket) refill(now Instant) {
	if now <= b.at {
		return
	}
	delta := now - b.at
	if delta >= calendarOldBurst-b.tokens {
		b.tokens = calendarOldBurst
	} else {
		b.tokens += delta
	}
	b.at = now
}
func (b calendarOldBucket) project(now Instant) (calendarOldBucket, Instant, error) {
	b.refill(now)
	if b.tokens >= calendarOldToken {
		b.tokens -= calendarOldToken
		return b, b.at, nil
	}
	at, err := b.at.plus(calendarOldToken - b.tokens)
	if err != nil {
		return b, 0, err
	}
	b.at = at
	b.tokens = 0
	return b, at, nil
}

type calendarOldState struct {
	target            domain.PeerIdentity
	projected, actual calendarOldBucket
	last              Instant
	slots             map[*forwardSlot]forwardSlot
}
type calendarOldCombinedPlan struct {
	projected calendarOldBucket
	slot      forwardSlot
}

func (c *calendarOldState) retireable(now Instant) bool {
	if len(c.slots) != 0 || now < c.last || now < c.projected.at || now < c.actual.at {
		return false
	}
	projected, actual := c.projected, c.actual
	projected.refill(now)
	actual.refill(now)
	return projected.tokens == calendarOldBurst && actual.tokens == calendarOldBurst
}

func (c *calendarOldState) plan(now, latest, sourceFloor Instant) (calendarOldCombinedPlan, error) {
	if sourceFloor < now {
		return calendarOldCombinedPlan{}, ErrInvalid
	}
	if now < 0 || now < c.last {
		return calendarOldCombinedPlan{}, ErrClock
	}
	if latest < now || latest-now > responderTimeout {
		return calendarOldCombinedPlan{}, ErrBudget
	}
	if len(c.slots) >= MaxPendingPerTarget {
		return calendarOldCombinedPlan{}, ErrCapacity
	}
	horizon := now
	for _, slot := range c.slots {
		if slot.latest < now || slot.at > slot.latest {
			return calendarOldCombinedPlan{}, ErrInvalid
		}
		horizon = max(horizon, slot.latest)
	}
	actual := c.actual
	for range len(c.slots) {
		var err error
		actual, _, err = actual.project(horizon)
		if err != nil {
			return calendarOldCombinedPlan{}, err
		}
	}
	_, floor, err := actual.project(horizon)
	if err != nil {
		return calendarOldCombinedPlan{}, err
	}
	projected, at, err := c.projected.project(max(now, floor, sourceFloor))
	if err != nil {
		return calendarOldCombinedPlan{}, err
	}
	if at > latest {
		return calendarOldCombinedPlan{}, ErrBudget
	}
	return calendarOldCombinedPlan{projected: projected, slot: forwardSlot{at: at, latest: latest}}, nil
}

func calendarOldNew(target domain.PeerIdentity, now Instant, tokens uint32) calendarOldState {
	b := calendarOldBucket{at: now, tokens: Instant(tokens) * calendarOldToken}
	return calendarOldState{target: target, projected: b, actual: b, last: now, slots: map[*forwardSlot]forwardSlot{}}
}
func calendarOldRead(c *ForwardCalendar) calendarOldState {
	c.mu.Lock()
	defer c.mu.Unlock()
	slots := map[*forwardSlot]forwardSlot{}
	if c.ownedAliases {
		for i := 0; i < c.aliasCount; i++ {
			slot := c.aliases[i]
			slots[slot] = *slot
		}
	} else {
		for slot := range c.slots {
			slots[slot] = *slot
		}
	}
	return calendarOldState{target: c.target, projected: calendarOldBucket{c.projected.at, c.projected.tokens}, actual: calendarOldBucket{c.actual.at, c.actual.tokens}, last: c.last, slots: slots}
}

// This map registry preserves old lookup/install/delete semantics. For a full
// map its result is a SET of permitted victims, never a guessed iteration order.
type calendarOldRegistry struct {
	pointers map[domain.PeerIdentity]*ForwardCalendar
	states   map[domain.PeerIdentity]calendarOldState
}

func calendarOldCapture(t *testing.T, o *intakeCoordinator) calendarOldRegistry {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	m := calendarOldRegistry{pointers: map[domain.PeerIdentity]*ForwardCalendar{}, states: map[domain.PeerIdentity]calendarOldState{}}
	for _, c := range o.calendars {
		if c == nil {
			continue
		}
		if _, exists := m.pointers[c.target]; exists {
			t.Fatal("duplicate table identity")
		}
		m.pointers[c.target] = c
		m.states[c.target] = calendarOldRead(c)
	}
	return m
}
func (m calendarOldRegistry) permitted(target domain.PeerIdentity, now Instant, pending map[domain.PeerIdentity]int) map[domain.PeerIdentity]bool {
	result := map[domain.PeerIdentity]bool{}
	if m.pointers[target] != nil || len(m.pointers) < MaxPendingTotal {
		return result
	}
	for target, state := range m.states {
		if pending[target] == 0 && state.retireable(now) {
			result[target] = true
		}
	}
	return result
}
func (m calendarOldRegistry) compare(t *testing.T, o *intakeCoordinator) {
	t.Helper()
	actual := calendarOldCapture(t, o)
	if len(actual.pointers) != len(m.pointers) {
		t.Fatal("calendar table differs from frozen map state")
	}
	for target, pointer := range m.pointers {
		if actual.pointers[target] != pointer {
			t.Fatal("calendar table changed original pointer identity")
		}
	}
	if !reflect.DeepEqual(actual.states, m.states) {
		t.Fatal("calendar table differs from frozen map state")
	}
}
func calendarFixedRemoved(t *testing.T, before calendarOldRegistry, o *intakeCoordinator) domain.PeerIdentity {
	t.Helper()
	after := calendarOldCapture(t, o)
	var removed domain.PeerIdentity
	count := 0
	for target := range before.pointers {
		if after.pointers[target] == nil {
			removed = target
			count++
		}
	}
	if count != 1 || len(after.pointers) != MaxPendingTotal {
		t.Fatal("replacement did not remove exactly one original pointer", count)
	}
	return removed
}
func (m *calendarOldRegistry) commit(t *testing.T, target domain.PeerIdentity, pointer *ForwardCalendar, slot *forwardSlot, now, latest, floor Instant, tokens uint32, allowed map[domain.PeerIdentity]bool, removed domain.PeerIdentity) {
	t.Helper()
	state, exists := m.states[target]
	if !exists {
		if len(m.pointers) >= MaxPendingTotal {
			if !allowed[removed] {
				t.Fatal("replacement outside independently permitted victim set")
			}
			delete(m.pointers, removed)
			delete(m.states, removed)
		} else if !removed.IsZero() {
			t.Fatal("retirement despite free table cell")
		}
		state = calendarOldNew(target, now, tokens)
		m.pointers[target] = pointer
	} else if m.pointers[target] != pointer {
		t.Fatal("same identity received another calendar pointer")
	}
	expected, err := state.plan(now, latest, floor)
	if err != nil || slot == nil || *slot != expected.slot {
		t.Fatal("calendar plan differs from independent scalar reference", err)
	}
	state.projected = expected.projected
	state.last = now
	state.slots[slot] = expected.slot
	m.states[target] = state
}

// Adapters are test-only. Seeding is under the owner lock or sole fixture
// ownership and rejects invalid/duplicate identities and occupied/no free cells.
func calendarFixedSeedInto(o *intakeCoordinator, c *ForwardCalendar, index int) error {
	if c == nil || c.target.IsZero() || index < 0 || index >= MaxPendingTotal || o.calendars[index] != nil {
		return ErrInvalid
	}
	for _, old := range o.calendars {
		if old != nil && old.target == c.target {
			return ErrDuplicateID
		}
	}
	o.calendars[index] = c
	return nil
}
func calendarFixedSeed(t *testing.T, o *intakeCoordinator, c *ForwardCalendar) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	index := -1
	for i, old := range o.calendars {
		if old == nil {
			index = i
			break
		}
	}
	if err := calendarFixedSeedInto(o, c, index); err != nil {
		t.Fatal("invalid calendar fixture seed", err)
	}
}
func calendarFixedRead(t *testing.T, o *intakeCoordinator, target domain.PeerIdentity) *ForwardCalendar {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calendarLocked(target)
}
func calendarFixedCount(t *testing.T, o *intakeCoordinator) int {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calendarCountLocked()
}
func calendarFixedNew(t *testing.T, target domain.PeerIdentity, at Instant, tokens uint32) *ForwardCalendar {
	t.Helper()
	c, e := newOwnedForwardCalendar(target, at, tokens)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func calendarFixedFill(t *testing.T, o *intakeCoordinator, safe int) {
	t.Helper()
	for i := 0; i < MaxPendingTotal; i++ {
		at := Instant(10000 * time.Second)
		if i < safe {
			at = 0
		}
		calendarFixedSeed(t, o, calendarFixedNew(t, candidateTarget(100000+i), at, 4))
	}
}
func calendarFixedPending(o *intakeCoordinator) map[domain.PeerIdentity]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	m := map[domain.PeerIdentity]int{}
	for key, n := range pendingTestTargetCountsLocked(o.pending) {
		m[key] = n
	}
	return m
}
func calendarFixedAccounting(o *intakeCoordinator, members []Session) any {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	calendars := map[domain.PeerIdentity]any{}
	for _, c := range o.calendars {
		if c != nil {
			calendars[c.target] = []any{c, calendarOldRead(c)}
		}
	}
	names := map[domain.SourceGroup]int{}
	for i := 0; i < o.groups.namedCount; i++ {
		names[o.groups.named[i]] = i
	}
	records := map[candidateRecordKey]candidateGroupRecord{}
	for i := 0; i < o.groups.recordCount; i++ {
		row := o.groups.records[i]
		records[row.key] = row.record
	}
	bySource := map[pendingKey]any{}
	for key, e := range pendingTestSourceMapLocked(o.pending) {
		bySource[key] = []any{e, e.source, e.target, e.request, e.forwarded, e.expires, *e.intake}
	}
	byTarget := map[pendingKey]*pendingEntry{}
	for key, e := range pendingTestTargetMapLocked(o.pending) {
		byTarget[key] = e
	}
	sources := map[Session]int{}
	for key, n := range pendingTestSourceCountsLocked(o.pending) {
		sources[key] = n
	}
	targets := map[domain.PeerIdentity]int{}
	for key, n := range pendingTestTargetCountsLocked(o.pending) {
		targets[key] = n
	}
	sessions := []any{}
	for _, member := range members {
		w := member.state.sourceWindows.Load()
		var history any
		if w != nil {
			history = []any{w, w.owner, w.rows}
		}
		sessions = append(sessions, []any{member, member.state.nextID, member.state.exhausted, history})
	}
	return []any{calendars, names, records, o.groups.generation, o.groups.lastUse, o.groups.used, o.sourceOwner, o.capacity, bySource, byTarget, sources, targets, sessions, o.counters, o.last, o.generation, o.round, o.open, o.cut, o.cursor, o.watermark, o.sweep, o.rrCell, o.rrSession}
}
func calendarFixedCommitDirect(o *intakeCoordinator, r candidateReservation, now Instant, prepared *candidateSourceWindows) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	return o.commitLocked(r.entry, now, prepared)
}

func TestCandidateCalendarTableLookupAndSnapshot(t *testing.T) {
	for _, name := range []string{"empty", "sparse", "full", "last-cell", "exact-target", "missing", "seed-rejection"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			switch name {
			case "sparse", "last-cell":
				c := calendarFixedNew(t, candidateTarget(1), 0, 4)
				o.mu.Lock()
				e := calendarFixedSeedInto(o, c, 511)
				o.mu.Unlock()
				if e != nil {
					t.Fatal(e)
				}
			case "full":
				calendarFixedFill(t, o, 0)
			case "exact-target":
				for i := 0; i < 2; i++ {
					id := candidateTarget(i)
					id[0] = 7
					calendarFixedSeed(t, o, calendarFixedNew(t, id, 0, 4))
				}
			case "seed-rejection":
				c := calendarFixedNew(t, candidateTarget(1), 0, 4)
				calendarFixedSeed(t, o, c)
				o.mu.Lock()
				a := calendarFixedSeedInto(o, c, 1)
				b := calendarFixedSeedInto(o, nil, 2)
				d := calendarFixedSeedInto(o, calendarFixedNew(t, candidateTarget(2), 0, 4), 0)
				o.mu.Unlock()
				if !errors.Is(a, ErrDuplicateID) || !errors.Is(b, ErrInvalid) || !errors.Is(d, ErrInvalid) {
					t.Fatal("unsafe fixture seed accepted")
				}
			}
			reference := calendarOldCapture(t, o)
			if calendarFixedCount(t, o) != len(reference.pointers) {
				t.Fatal("calendar occupancy used physical array length")
			}
			for target, pointer := range reference.pointers {
				if calendarFixedRead(t, o, target) != pointer {
					t.Fatal("calendar lookup lost exact target identity or final cell")
				}
			}
			absent := candidateTarget(900000)
			if calendarFixedRead(t, o, absent) != nil {
				t.Fatal("absent lookup returned foreign calendar")
			}
			o.mu.Lock()
			o.publishLocked()
			o.mu.Unlock()
			snapshot := o.snapshot()
			if snapshot.calendars != len(reference.pointers) || snapshot.aliases != 0 {
				t.Fatal("sparse snapshot counted nil cells or invented aliases", snapshot)
			}
			reference.compare(t, o)
		})
	}
}
func TestCandidateCalendarTableRetirementDifferential(t *testing.T) {
	for _, name := range []string{"before-refill", "at-refill", "after-refill", "projected-future", "actual-future", "last-future", "projected-not-full", "actual-not-full", "slots", "pending", "all-safe"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			calendarFixedFill(t, o, 0)
			victim := o.calendars[0]
			now := Instant(4 * time.Second)
			victim.last = 0
			victim.projected = forwardBucket{}
			victim.actual = forwardBucket{}
			switch name {
			case "before-refill":
				now--
			case "after-refill":
				now++
			case "projected-future":
				victim.projected = forwardBucket{at: now + 1, tokens: forwardBurst}
				victim.actual = forwardBucket{tokens: forwardBurst}
			case "actual-future":
				victim.actual = forwardBucket{at: now + 1, tokens: forwardBurst}
				victim.projected = forwardBucket{tokens: forwardBurst}
			case "last-future":
				victim.last = now + 1
			case "projected-not-full":
				victim.projected = forwardBucket{at: now, tokens: forwardBurst - 1}
				victim.actual = forwardBucket{tokens: forwardBurst}
			case "actual-not-full":
				victim.actual = forwardBucket{at: now, tokens: forwardBurst - 1}
				victim.projected = forwardBucket{tokens: forwardBurst}
			case "slots":
				if !victim.ownedAliases || victim.slots != nil || victim.aliasCount != 0 {
					t.Fatal("invalid owned fixed-prefix fixture seed")
				}
				victim.aliases[0] = &forwardSlot{at: 0, latest: now}
				victim.aliasCount = 1
			case "pending":
				pendingTestRetirementWithActualTerminal(t)
				return
			case "all-safe":
				for _, c := range o.calendars {
					c.last = 0
					c.projected = forwardBucket{tokens: forwardBurst}
					c.actual = forwardBucket{tokens: forwardBurst}
				}
			}
			reference := calendarOldCapture(t, o)
			pending := calendarFixedPending(o)
			safe := reference.permitted(candidateTarget(900000), now, pending)
			for target, c := range reference.pointers {
				before := calendarOldRead(c)
				c.mu.Lock()
				got := c.retireableLocked(now)
				c.mu.Unlock()
				expected := reference.states[target]
				if got != expected.retireable(now) || !reflect.DeepEqual(calendarOldRead(c), before) {
					t.Fatal("retirement predicate differs from frozen scalar witness")
				}
			}
			source, _ := provenFixture(t, 1, "source", 0)
			target, _ := provenFixture(t, 2, "target", 0)
			received := now - o.profile.round
			r, out := candidateSourceOfferSeal(t, o, source, target, 1, received)
			if len(safe) == 0 {
				if out.reserved != 0 || out.refused != 1 {
					t.Fatal("full debt table retired an unsafe calendar", name, out)
				}
				reference.compare(t, o)
			} else {
				if out.reserved != 1 {
					t.Fatal("lawful full table retirement refused", name, out)
				}
				removed := calendarFixedRemoved(t, reference, o)
				reference.commit(t, target.Identity(), r.entry.intake.calendar, r.entry.intake.alias, now, r.entry.intake.latest, now, o.profile.initialTokens, safe, removed)
				reference.compare(t, o)
			}
		})
	}
}
func TestCandidateCalendarTableFailedCommitIsAtomic(t *testing.T) {
	for _, name := range []string{"id-exhausted", "duplicate-correlation", "group-refusal"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			calendarFixedFill(t, o, 1)
			source, _ := provenFixture(t, 1, "source", 0)
			target, _ := provenFixture(t, 2, "target", 0)
			received := Instant(0)
			var correlated candidateReservation
			if name == "duplicate-correlation" {
				prior, _ := provenFixture(t, 3, "prior", 0)
				correlated, _ = candidateSourceOfferSeal(t, o, prior, target, 9, 0)
				if correlated.entry.intake.phase != candidateReserved || o.pending.targetEntryLocked(pendingKey{target, correlated.entry.forwarded.ID}) != correlated.entry {
					t.Fatal("duplicate fixture lacks separate genuine correlation")
				}
				received = o.profile.round
			}
			r, e := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, received, []Session{source}, []Session{source}, target), received)
			if e != nil {
				t.Fatal(e)
			}
			switch name {
			case "id-exhausted":
				target.state.exhausted = true
			case "duplicate-correlation":
				// Defensive ID corruption only: a second genuine correlation,
				// not a provisional row forged under an unrelated target key.
				target.state.mu.Lock()
				target.state.nextID = correlated.entry.forwarded.ID
				target.state.mu.Unlock()
			case "group-refusal":
				fixedSeedName(t, &o.groups, source.state.group, 0)
				for i := 0; i < 512; i++ {
					fixedSeedRecord(t, &o.groups, candidateRecordKey{0, candidateTarget(i)}, candidateGroupRecord{expires: candidateGroupWindow})
				}
			}
			prepared := o.prepareSources()
			before := calendarFixedAccounting(o, []Session{source, target})
			now := received + o.profile.round
			err := calendarFixedCommitDirect(o, r, now, prepared[source])
			want := error(ErrCapacity)
			switch name {
			case "id-exhausted":
				want = ErrStaleWork
			case "duplicate-correlation":
				want = ErrDuplicateID
			}
			if !errors.Is(err, want) {
				t.Fatal("checked refusal reached wrong gate", name, err)
			}
			if !reflect.DeepEqual(calendarFixedAccounting(o, []Session{source, target}), before) {
				t.Fatal("failed commit published table pointer or changed original accounting")
			}
		})
	}
}
func TestCandidateCalendarTableLawfulVictimsAndOriginalPointers(t *testing.T) {
	for _, name := range []string{"unique-victim", "multiple-safe"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			n := 1
			if name == "multiple-safe" {
				n = 3
			}
			calendarFixedFill(t, o, n)
			retained := o.calendars[0]
			ended, e := retained.Reserve(0, Instant(time.Second))
			if e != nil || !retained.Cancel(ended) {
				t.Fatal("genuine ended reservation fixture", e)
			}
			source, _ := provenFixture(t, 1, "source", 0)
			target, signer := provenFixture(t, 2, "target", 0)
			reference := calendarOldCapture(t, o)
			now := Instant(5 * time.Second)
			safe := reference.permitted(target.Identity(), now+o.profile.round, calendarFixedPending(o))
			r, out := candidateSourceOfferSeal(t, o, source, target, 1, now)
			if out.reserved != 1 {
				t.Fatal(out)
			}
			removed := calendarFixedRemoved(t, reference, o)
			old := reference.pointers[removed]
			oldState := reference.states[removed]
			if !safe[removed] {
				t.Fatal("replacement outside independently permitted victim set")
			}
			if r.entry.intake.calendar == old || r.entry.intake.calendar == retained || !reflect.DeepEqual(calendarOldRead(old), oldState) {
				t.Fatal("retired calendar object was reused or rewritten")
			}
			reference.commit(t, target.Identity(), r.entry.intake.calendar, r.entry.intake.alias, now+o.profile.round, r.entry.intake.latest, now+o.profile.round, 4, safe, removed)
			reference.compare(t, o)
			if retained.Cancel(ended) || r.entry.intake.calendar.Cancel(ended) {
				t.Fatal("ended old handle acquired replacement authority")
			}
			reference.compare(t, o)
			if name == "unique-victim" {
				// Retire a genuinely completed owner reservation, retaining its exact old
				// forward permit. No pointer/phase is fabricated to manufacture authority.
				cut := now + o.profile.round
				alias := r.entry.intake.alias
				oldPointer := r.entry.intake.calendar
				permit, input, e := o.takeForward(r, cut)
				if e != nil {
					t.Fatal(e)
				}
				state := reference.states[target.Identity()]
				state.actual.refill(cut)
				state.actual.tokens -= calendarOldToken
				state.last = cut
				delete(state.slots, alias)
				reference.states[target.Identity()] = state
				reference.compare(t, o)
				if e = o.finishForward(permit, cut, candidateWriteFinished); e != nil {
					t.Fatal(e)
				}
				reference.compare(t, o)
				if _, e = o.answer(target, bodyFor(t, signer).Answer(input.request.ID), cut); e != nil {
					t.Fatal(e)
				}
				reference.compare(t, o)
				oldState := calendarOldRead(oldPointer)
				nextTarget, _ := provenFixture(t, 3, "next-target", 0)
				next := now + Instant(5*time.Second)
				nextCut := next + o.profile.round
				allowed := reference.permitted(nextTarget.Identity(), nextCut, calendarFixedPending(o))
				if len(allowed) != 1 || !allowed[target.Identity()] {
					t.Fatal("completed genuine permit is not the unique safe victim")
				}
				fresh, out := candidateSourceOfferSeal(t, o, source, nextTarget, 2, next)
				if out.reserved != 1 {
					t.Fatal(out)
				}
				retired := calendarFixedRemoved(t, reference, o)
				reference.commit(t, nextTarget.Identity(), fresh.entry.intake.calendar, fresh.entry.intake.alias, nextCut, fresh.entry.intake.latest, nextCut, 4, allowed, retired)
				reference.compare(t, o)
				if retired != target.Identity() || fresh.entry.intake.calendar == oldPointer || !reflect.DeepEqual(calendarOldRead(oldPointer), oldState) {
					t.Fatal("retired genuine calendar was recycled")
				}
				if ok, _ := o.cancel(r, nextCut); ok {
					t.Fatal("retired genuine reservation acquired replacement authority")
				}
				if e = o.finishForward(permit, nextCut, candidateWriteFinished); !errors.Is(e, ErrStaleWork) {
					t.Fatal("retired genuine permit acquired replacement authority", e)
				}
				reference.compare(t, o)
				if !reflect.DeepEqual(calendarOldRead(oldPointer), oldState) {
					t.Fatal("stale permit rewrote externally retained calendar")
				}
			}
		})
	}
}
func TestCandidateCalendarTablePendingPhasesBlockRetirement(t *testing.T) {
	for _, name := range []string{"reserved", "dispatching", "awaiting", "terminal"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			source, _ := provenFixture(t, 1, "source", 0)
			oldTarget, signer := provenFixture(t, 2, "old-target", 0)
			newTarget, _ := provenFixture(t, 3, "new-target", 0)
			r, out := candidateSourceOfferSeal(t, o, source, oldTarget, 1, 0)
			if out.reserved != 1 {
				t.Fatal(out)
			}
			at := o.profile.round
			if name != "reserved" {
				permit, input, e := o.takeForward(r, at)
				if e != nil {
					t.Fatal(e)
				}
				if name == "awaiting" {
					if e = o.finishForward(permit, at, candidateWriteFinished); e != nil {
						t.Fatal(e)
					}
				}
				if name == "terminal" {
					if _, e = o.answer(oldTarget, bodyFor(t, signer).Answer(input.request.ID), at); e != nil {
						t.Fatal(e)
					}
				}
			}
			for i := 0; i < 511; i++ {
				calendarFixedSeed(t, o, calendarFixedNew(t, candidateTarget(100000+i), Instant(10000*time.Second), 4))
			}
			reference := calendarOldCapture(t, o)
			now := Instant(1200 * time.Millisecond)
			safe := reference.permitted(newTarget.Identity(), now+o.profile.round, calendarFixedPending(o))
			if len(safe) != 0 {
				t.Fatal("pending phase omitted from independent victim set")
			}
			_, out = candidateSourceOfferSeal(t, o, source, newTarget, 2, now)
			if out.reserved != 0 || out.refused != 1 {
				t.Fatal("pending or terminal target retired", name, out)
			}
			reference.compare(t, o)
		})
	}
}
func TestCandidateCalendarTableSharedIdentityAndLifecycle(t *testing.T) {
	for _, name := range []string{"cancel", "close", "expire", "answer-terminal"} {
		t.Run(name, func(t *testing.T) {
			o := candidateTestOwner(t)
			originalOwner, originalPending, groupGeneration := o.sourceOwner, o.pending, o.groups.generation
			a, _ := provenFixture(t, 1, "selected", 0)
			b, _ := provenFixture(t, 2, "incoming", 0)
			first, signer := provenFixture(t, 3, "target-a", 0)
			second, _ := provenFixture(t, 4, "target-b", 0, signer)
			sources := []Session{a, b}
			now := candidateGroupWindow
			view := func(at Instant) candidateMembership {
				v, e := captureCandidateMembership(o, []domain.PeerIdentity{first.Identity()}, sources, sources, at, func(in SessionInfo) (SessionClass, error) {
					if in.ConnID == a.ConnID() {
						return ClassSelected, nil
					}
					return ClassIncomingPinned, nil
				})
				if e != nil {
					t.Fatal(e)
				}
				return v
			}
			v := view(now)
			if p, e := v.priority(o, a, first.Identity(), now); e != nil || p != PriorityProtected {
				t.Fatal(e)
			}
			if p, e := v.priority(o, b, first.Identity(), now); e != nil || p != PriorityAged {
				t.Fatal(e)
			}
			r1, e := o.offer(a, first, request(1, first.Identity(), 1), v, now)
			if e != nil {
				t.Fatal(e)
			}
			r2, e := o.offer(b, second, request(2, second.Identity(), 2), v, now)
			if e != nil {
				t.Fatal(e)
			}
			cut := now + o.profile.round
			out, e := o.sealRound(view(cut), cut)
			if e != nil || out.reserved != 2 {
				t.Fatal(out, e)
			}
			c := r1.entry.intake.calendar
			if c != r2.entry.intake.calendar || calendarFixedCount(t, o) != 1 {
				t.Fatal("connections or priority levels split shared identity debt")
			}
			// The real seal order follows existing priority. Independent scalar planning
			// includes the first alias's full latest deadline before the second plan.
			reference := calendarOldRegistry{pointers: map[domain.PeerIdentity]*ForwardCalendar{}, states: map[domain.PeerIdentity]calendarOldState{}}
			reference.commit(t, first.Identity(), c, r1.entry.intake.alias, cut, r1.entry.intake.latest, cut, 4, nil, domain.PeerIdentity{})
			reference.commit(t, first.Identity(), c, r2.entry.intake.alias, cut, r2.entry.intake.latest, cut, 4, nil, domain.PeerIdentity{})
			reference.compare(t, o)
			alias1, alias2 := r1.entry.intake.alias, r2.entry.intake.alias
			state := reference.states[first.Identity()]
			final := cut
			switch name {
			case "cancel":
				if ok, e := o.cancel(r1, cut); !ok || e != nil {
					t.Fatal(e)
				}
				if ok, e := o.cancel(r2, cut); !ok || e != nil {
					t.Fatal(e)
				}
			case "close":
				if e = o.closeSession(a, cut); e != nil {
					t.Fatal(e)
				}
				if e = o.closeSession(b, cut); e != nil {
					t.Fatal(e)
				}
			case "expire":
				final = now + responderTimeout
				if e = o.expire(final); e != nil {
					t.Fatal(e)
				}
			case "answer-terminal":
				permit, input, e := o.takeForward(r1, cut)
				if e != nil {
					t.Fatal(e)
				}
				state.actual.refill(cut)
				state.actual.tokens -= calendarOldToken
				state.last = cut
				if _, e = o.answer(first, bodyFor(t, signer).Answer(input.request.ID), cut); e != nil {
					t.Fatal(e)
				}
				if r1.entry.intake.phase != candidateTerminalDispatch || o.pending.targetCountLocked(first.Identity()) != 2 {
					t.Fatal("answer released outstanding writer obligation")
				}
				if ok, e := o.cancel(r2, cut); !ok || e != nil {
					t.Fatal(e)
				}
				if e = o.finishForward(permit, cut, candidateWriteFinished); e != nil {
					t.Fatal(e)
				}
				if e = o.finishForward(permit, cut, candidateWriteFinished); !errors.Is(e, ErrStaleWork) {
					t.Fatal("old permit acquired new authority", e)
				}
			}
			delete(state.slots, alias1)
			delete(state.slots, alias2)
			reference.states[first.Identity()] = state
			reference.compare(t, o)
			if o.sourceOwner != originalOwner || o.pending != originalPending || o.groups.generation != groupGeneration || calendarFixedRead(t, o, first.Identity()) != c || o.pending.targetCountLocked(first.Identity()) != 0 {
				t.Fatal("lifecycle reconstructed authority or canonical calendar")
			}
			if ok, _ := o.cancel(r1, final); ok {
				t.Fatal("old ended reservation became current")
			}
			reference.compare(t, o)
		})
	}
}
func TestCandidateCalendarTableCapacityAndReuseTrace(t *testing.T) {
	for _, name := range []string{"unique-victim", "multiple-safe"} {
		t.Run(name, func(t *testing.T) {
			o := capacityOwner(t, candidateCapacityHint{value: 64, present: true})
			n := 1
			if name == "multiple-safe" {
				n = 3
			}
			calendarFixedFill(t, o, n)
			reference := calendarOldCapture(t, o)
			before := calendarOldCapture(t, o)
			if cap, e := capacitySelect(o, candidateCapacityWindow, capacityCut(o, candidateCapacityWindow, 0)); e != nil || cap != 512 {
				t.Fatal(cap, e)
			}
			before.compare(t, o)
			source, _ := provenFixture(t, 1, "source", 0)
			targets := []Session{}
			for i := 0; i < 3; i++ {
				s, _ := provenFixture(t, domain.ConnID(i+2), domain.SourceGroup(fmt.Sprintf("target%d", i)), 0)
				targets = append(targets, s)
			}
			owner, generation := o.sourceOwner, o.groups.generation
			for step := 0; step < 9; step++ {
				now := candidateCapacityWindow + Instant(step)*Instant(5*time.Second)
				target := targets[step%len(targets)]
				cut := now + o.profile.round
				safe := reference.permitted(target.Identity(), cut, calendarFixedPending(o))
				existing := reference.pointers[target.Identity()]
				r, out := candidateSourceOfferSeal(t, o, source, target, domain.FindNodeRequestID(step+1), now)
				if out.reserved != 1 {
					t.Fatal("lawful deterministic reuse trace refused", step, out)
				}
				var removed domain.PeerIdentity
				if existing == nil {
					removed = calendarFixedRemoved(t, reference, o)
				}
				reference.commit(t, target.Identity(), r.entry.intake.calendar, r.entry.intake.alias, cut, r.entry.intake.latest, cut, 4, safe, removed)
				reference.compare(t, o)
				alias := r.entry.intake.alias
				if ok, e := o.cancel(r, cut); !ok || e != nil {
					t.Fatal(e)
				}
				state := reference.states[target.Identity()]
				delete(state.slots, alias)
				reference.states[target.Identity()] = state
				reference.compare(t, o)
				if o.sourceOwner != owner || o.groups.generation != generation || o.capacity.cap != 512 || o.snapshot().committed != uint64(step+1) || o.snapshot().refused != 0 {
					t.Fatal("reuse trace erased authority/debt or changed exact outcomes")
				}
			}
		})
	}
}
func TestCandidateCalendarTableCompetingSourceClaims(t *testing.T) {
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	owners := []*intakeCoordinator{candidateTestOwner(t), candidateTestOwner(t)}
	references := []calendarOldRegistry{}
	views := []candidateMembership{}
	for _, o := range owners {
		calendarFixedFill(t, o, 1)
		r, e := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
		if e != nil || r.entry == nil {
			t.Fatal(e)
		}
		references = append(references, calendarOldCapture(t, o))
		views = append(views, candidateView(t, o, o.profile.round, []Session{source}, []Session{source}, target))
	}
	type result struct {
		out candidateRoundResult
		err error
	}
	results := make([]result, 2)
	start := make(chan struct{})
	var joined sync.WaitGroup
	for i, o := range owners {
		joined.Add(1)
		go func(i int, o *intakeCoordinator) {
			defer joined.Done()
			<-start
			results[i].out, results[i].err = o.sealRound(views[i], o.profile.round)
		}(i, o)
	}
	close(start)
	joined.Wait()
	winners := 0
	for i, o := range owners {
		r := results[i]
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.out.reserved == 0 {
			if r.out.refused != 1 || o.snapshot().committed != 0 || o.snapshot().records != 0 || o.snapshot().names != 0 || o.snapshot().pending != 0 {
				t.Fatal("losing source claim changed owner accounting", r.out)
			}
			references[i].compare(t, o)
		} else {
			winners++
			if r.out.reserved != 1 || r.out.refused != 0 || source.state.sourceWindows.Load().owner != o.sourceOwner {
				t.Fatal("source claim winner has foreign authority")
			}
			removed := calendarFixedRemoved(t, references[i], o)
			safe := references[i].permitted(target.Identity(), o.profile.round, map[domain.PeerIdentity]int{})
			if !safe[removed] {
				t.Fatal("competing claim removed unsafe victim")
			}
			entry := pendingTestSourceMapLocked(o.pending)[pendingKey{source, 1}]
			if entry == nil || entry.intake.source.windows != source.state.sourceWindows.Load() || o.snapshot().committed != 1 || o.snapshot().pending != 1 {
				t.Fatal("winner lost original source/pending authority")
			}
			references[i].commit(t, target.Identity(), entry.intake.calendar, entry.intake.alias, o.profile.round, entry.intake.latest, o.profile.round, 4, safe, removed)
			references[i].compare(t, o)
		}
	}
	if winners != 1 || target.state.nextID != 1 {
		t.Fatal("competing source claims duplicated outgoing authority", winners, target.state.nextID)
	}
	// No hook/sleep asserts which losing path ran: a loser may observe the other
	// owner before planSourceLocked OR lose the final CAS. Both preserve its book.
}
