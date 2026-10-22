package findnode

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// SOURCE-frozen old four-map storage. Only LAWFUL owner events feed these
// methods: release requires bySource[key] == entry. The old raw release itself
// is deliberately unchanged and is NOT a defensive detached-pointer API.
type pendingOldBook struct {
	bySource map[pendingKey]*pendingEntry
	byTarget map[pendingKey]*pendingEntry
	sources  map[Session]int
	targets  map[domain.PeerIdentity]int
}

func pendingOldNew() *pendingOldBook {
	return &pendingOldBook{bySource: make(map[pendingKey]*pendingEntry), byTarget: make(map[pendingKey]*pendingEntry), sources: make(map[Session]int), targets: make(map[domain.PeerIdentity]int)}
}
func (p *pendingOldBook) offer(entry *pendingEntry) {
	p.bySource[pendingKey{entry.source, entry.request.ID}] = entry
	p.sources[entry.source]++
	p.targets[entry.request.Target]++
}
func (p *pendingOldBook) commit(entry *pendingEntry) {
	p.byTarget[pendingKey{entry.target, entry.forwarded.ID}] = entry
}
func (p *pendingOldBook) terminal(entry *pendingEntry) {
	delete(p.byTarget, pendingKey{entry.target, entry.forwarded.ID})
}
func (p *pendingOldBook) release(entry *pendingEntry) {
	delete(p.bySource, pendingKey{entry.source, entry.request.ID})
	if entry.intake == nil || entry.intake.phase != candidateProvisional {
		delete(p.byTarget, pendingKey{entry.target, entry.forwarded.ID})
	}
	p.sources[entry.source]--
	if p.sources[entry.source] == 0 {
		delete(p.sources, entry.source)
	}
	p.targets[entry.request.Target]--
	if p.targets[entry.request.Target] == 0 {
		delete(p.targets, entry.request.Target)
	}
}
func pendingTestSourceMapLocked(p *candidatePendingBook) map[pendingKey]*pendingEntry {
	out := map[pendingKey]*pendingEntry{}
	for _, entry := range p.entries {
		if entry != nil {
			out[pendingKey{entry.source, entry.request.ID}] = entry
		}
	}
	return out
}
func pendingTestTargetMapLocked(p *candidatePendingBook) map[pendingKey]*pendingEntry {
	out := map[pendingKey]*pendingEntry{}
	for _, entry := range p.entries {
		if entry == nil || entry.intake == nil {
			continue
		}
		switch entry.intake.phase {
		case candidateReserved, candidateDispatching, candidateAwaitingAnswer:
			out[pendingKey{entry.target, entry.forwarded.ID}] = entry
		}
	}
	return out
}
func pendingTestSourceCountsLocked(p *candidatePendingBook) map[Session]int {
	out := map[Session]int{}
	for _, entry := range p.entries {
		if entry != nil {
			out[entry.source]++
		}
	}
	return out
}
func pendingTestTargetCountsLocked(p *candidatePendingBook) map[domain.PeerIdentity]int {
	out := map[domain.PeerIdentity]int{}
	for _, entry := range p.entries {
		if entry != nil {
			out[entry.request.Target]++
		}
	}
	return out
}
func pendingTestCompare(t *testing.T, p *candidatePendingBook, old *pendingOldBook) {
	t.Helper()
	if !reflect.DeepEqual(pendingTestSourceMapLocked(p), old.bySource) || !reflect.DeepEqual(pendingTestTargetMapLocked(p), old.byTarget) || !reflect.DeepEqual(pendingTestSourceCountsLocked(p), old.sources) || !reflect.DeepEqual(pendingTestTargetCountsLocked(p), old.targets) || p.countLocked() != len(old.bySource) {
		t.Fatal("fixed physical pending book differs from frozen four maps")
	}
	for key, entry := range old.bySource {
		index := p.sourceIndexLocked(key)
		if index < 0 || p.entries[index] != entry {
			t.Fatal("fixed source lookup detached original entry")
		}
	}
	for key, entry := range old.byTarget {
		if p.targetEntryLocked(key) != entry {
			t.Fatal("fixed target lookup detached original correlation")
		}
	}
	for _, entry := range p.entries {
		if entry == nil {
			continue
		}
		key := pendingKey{entry.target, entry.forwarded.ID}
		if p.targetEntryLocked(key) != old.byTarget[key] {
			t.Fatal("fixed target lookup invented provisional or terminal correlation")
		}
		if p.sourceCountLocked(entry.source) != old.sources[entry.source] || p.targetCountLocked(entry.request.Target) != old.targets[entry.request.Target] {
			t.Fatal("fixed counts omitted physical noncorrelated obligation")
		}
	}
}

// Whitebox storage fixture only, never a claim that512 rows from one Session
// passed the owner one-provisional or16/session admission gates.
func pendingTestEntry(source, target Session, id domain.FindNodeRequestID) *pendingEntry {
	return &pendingEntry{source: source, target: target, request: request(id, target.Identity(), 1), forwarded: request(id, target.Identity(), 1), expires: 100, intake: &candidatePending{phase: candidateProvisional, generation: uint64(id) + 1}}
}
func TestCandidatePendingBookFrozenMapDifferential(t *testing.T) {
	source, _ := provenFixture(t, 1, "source", 0)
	target, remote := provenFixture(t, 2, "target", 0)
	sameIdentity, _ := provenFixture(t, 3, "other-target-session", 0, remote)
	p, old := newCandidatePendingBook(), pendingOldNew()
	identity := p
	pendingTestCompare(t, p, old)
	if p.vacancyLocked() != 0 || p.sourceIndexLocked(pendingKey{source, 0}) != -1 || p.targetEntryLocked(pendingKey{target, 0}) != nil {
		t.Fatal("empty fixed book invented authority")
	}
	for i := 0; i < MaxPendingTotal; i++ {
		e := pendingTestEntry(source, target, domain.FindNodeRequestID(i))
		p.entries[i] = e
		old.offer(e)
	}
	pendingTestCompare(t, p, old)
	if p.countLocked() != 512 || p.vacancyLocked() != -1 {
		t.Fatal("fixed book physical512 boundary differs")
	}
	first := p.entries[0]
	first.forwarded.ID = domain.FindNodeRequestID(math.MaxUint64)
	first.intake.phase = candidateReserved
	old.commit(first)
	pendingTestCompare(t, p, old)
	if p.targetEntryLocked(pendingKey{sameIdentity, first.forwarded.ID}) != nil || p.sourceIndexLocked(pendingKey{sameIdentity, 0}) != -1 {
		t.Fatal("fixed pending lookup confused exact Session with identity")
	}
	if p.targetEntryLocked(pendingKey{target, first.forwarded.ID - 1}) != nil {
		t.Fatal("fixed pending lookup ignored exact forwarded ID")
	}
	// Original event trace, independent expected map writes: Dispatch and normal
	// Finish keep correlation, then answer/end deletes correlation only.
	first.intake.phase = candidateDispatching
	pendingTestCompare(t, p, old)
	first.intake.phase = candidateAwaitingAnswer
	pendingTestCompare(t, p, old)
	first.intake.phase = candidateTerminalDispatch
	old.terminal(first)
	pendingTestCompare(t, p, old)
	if p.countLocked() != 512 || p.targetCountLocked(target.Identity()) != 512 {
		t.Fatal("terminal physical row freed count before Finish")
	}
	for _, index := range []int{0, 1, 2, 255, 511} {
		entry := p.entries[index]
		if old.bySource[pendingKey{entry.source, entry.request.ID}] != entry {
			t.Fatal("reference release not lawful")
		}
		old.release(entry)
		p.release(entry)
		pendingTestCompare(t, p, old)
	}
	if p.vacancyLocked() != 0 || p.countLocked() != 507 || p != identity {
		t.Fatal("sparse deletion changed book identity or skipped rows")
	}
	// The same forwarded ID on a distinct target Session is lawful correlation.
	next := pendingTestEntry(source, sameIdentity, 1000)
	next.forwarded.ID = 7
	next.intake.phase = candidateReserved
	p.entries[p.vacancyLocked()] = next
	old.offer(next)
	old.commit(next)
	peer := pendingTestEntry(sameIdentity, target, 1001)
	peer.forwarded.ID = 7
	peer.intake.phase = candidateReserved
	p.entries[p.vacancyLocked()] = peer
	old.offer(peer)
	old.commit(peer)
	pendingTestCompare(t, p, old)
	if p.targetEntryLocked(pendingKey{target, 7}) != peer || p.targetEntryLocked(pendingKey{sameIdentity, 7}) != next {
		t.Fatal("same ID on distinct target Sessions collided")
	}
	unknown := p.entries[3]
	unknown.intake.phase = candidatePhase(255)
	if p.targetEntryLocked(pendingKey{unknown.target, unknown.forwarded.ID}) != nil {
		t.Fatal("unknown phase minted target correlation")
	}
	unknown.intake.phase = candidateProvisional
	// Actual lawful owner offers exercise a hole followed by adjacent expirations;
	// the old reference release is fed only admitted current entries.
	o := candidateTestOwner(t)
	live := []Session{}
	for i := 0; i < 3; i++ {
		s, _ := provenFixture(t, domain.ConnID(10+i), domain.SourceGroup(fmt.Sprint("expiry", i)), 0)
		live = append(live, s)
	}
	expiryTarget, _ := provenFixture(t, 20, "expiry-target", 0)
	view := candidateView(t, o, 0, live, live, expiryTarget)
	handles := []candidateReservation{}
	expiryReference := pendingOldNew()
	for i, s := range live {
		r, e := o.offer(s, expiryTarget, request(domain.FindNodeRequestID(i+1), expiryTarget.Identity(), 1), view, 0)
		if e != nil {
			t.Fatal(e)
		}
		handles = append(handles, r)
		expiryReference.offer(r.entry)
	}
	if ok, e := o.cancel(handles[0], 0); !ok || e != nil {
		t.Fatal(e)
	}
	expiryReference.release(handles[0].entry)
	if o.pending.entries[0] != nil || o.pending.countLocked() != 2 {
		t.Fatal("sparse expiry fixture did not create first-cell hole")
	}
	if e := o.expire(handles[1].entry.expires); e != nil {
		t.Fatal(e)
	}
	expiryReference.release(handles[1].entry)
	expiryReference.release(handles[2].entry)
	if o.pending.countLocked() != 0 || expiryTarget.state.nextID != 0 {
		t.Fatal("expired neighbors survived sparse full sweep")
	}
	pendingTestCompare(t, o.pending, expiryReference)
}
func TestCandidatePendingBookOwnerLifecycle(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 10, "source", 0)
	target, signer := provenFixture(t, 20, "target", 0)
	old := pendingOldNew()
	original := o.pending
	r, out := candidateSourceOfferSeal(t, o, source, target, 7, 0)
	if out.reserved != 1 {
		t.Fatal("genuine pending prefix was not reserved")
	}
	// Capture the LAWFUL offer and commit events, never infer expected target map
	// by invoking the new target query or rebuilding from its current phase.
	old.offer(r.entry)
	old.commit(r.entry)
	pendingTestCompare(t, o.pending, old)
	at := o.profile.round
	permit, input, e := o.takeForward(r, at)
	if e != nil {
		t.Fatal(e)
	}
	if r.entry.intake.phase != candidateDispatching {
		t.Fatal("Take lost dispatching phase")
	}
	pendingTestCompare(t, o.pending, old)
	body := bodyFor(t, signer).Answer(input.request.ID)
	delivery, e := o.answer(target, body, at)
	if e != nil || delivery.Session != source || delivery.Answer.ID != 7 {
		t.Fatal("genuine dispatch answer lost original source envelope", e)
	}
	old.terminal(r.entry)
	pendingTestCompare(t, o.pending, old)
	if r.entry.intake.phase != candidateTerminalDispatch || o.pending.countLocked() != 1 || o.pending.targetCountLocked(target.Identity()) != 1 {
		t.Fatal("dispatch answer removed outstanding physical writer")
	}
	if _, e = o.answer(target, body, at); !errors.Is(e, ErrUnmatchedAnswer) {
		t.Fatal("terminal row accepted a second answer", e)
	}
	projected, actual := r.entry.intake.calendar.projected, r.entry.intake.calendar.actual
	if e = o.finishForward(permit, at, candidateWriteFinished); e != nil {
		t.Fatal(e)
	}
	old.release(r.entry)
	pendingTestCompare(t, o.pending, old)
	if o.pending != original || r.entry.intake.calendar.projected != projected || r.entry.intake.calendar.actual != actual {
		t.Fatal("physical release reconstructed book or refunded calendar debt")
	}
	if e = o.finishForward(permit, at, candidateWriteFinished); !errors.Is(e, ErrStaleWork) {
		t.Fatal("writer Finish had a second terminal authority", e)
	}
	// Normal Finish -> AwaitingAnswer keeps pending and target correlation.
	r, out = candidateSourceOfferSeal(t, o, source, target, 8, Instant(time.Second))
	if out.reserved != 1 {
		t.Fatal("second round refused")
	}
	old.offer(r.entry)
	old.commit(r.entry)
	at = Instant(time.Second) + o.profile.round
	permit, input, e = o.takeForward(r, at)
	if e != nil {
		t.Fatal(e)
	}
	if e = o.finishForward(permit, at, candidateWriteFinished); e != nil {
		t.Fatal(e)
	}
	if r.entry.intake.phase != candidateAwaitingAnswer {
		t.Fatal("normal Finish did not retain awaiting answer")
	}
	pendingTestCompare(t, o.pending, old)
	if _, e = o.answer(target, bodyFor(t, signer).Answer(input.request.ID), at); e != nil {
		t.Fatal(e)
	}
	old.release(r.entry)
	pendingTestCompare(t, o.pending, old)
}

// Produce one real retained terminal for the pre-existing calendar retirement
// parent. No count-only or provisional byTarget corruption is used.
func pendingTestRetirementWithActualTerminal(t *testing.T) {
	t.Helper()
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 10, "held-source", 0)
	target, signer := provenFixture(t, 20, "held-target", 0)
	r, out := candidateSourceOfferSeal(t, o, source, target, 1, 0)
	if out.reserved != 1 {
		t.Fatal("retirement fixture was not genuinely committed")
	}
	at := o.profile.round
	permit, input, e := o.takeForward(r, at)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = o.answer(target, bodyFor(t, signer).Answer(input.request.ID), at); e != nil {
		t.Fatal(e)
	}
	held := r.entry.intake.calendar
	for i := 0; i < MaxPendingTotal-1; i++ {
		calendarFixedSeed(t, o, calendarFixedNew(t, candidateTarget(200000+i), Instant(10000*time.Second), 4))
	}
	now := Instant(4*time.Second) + at
	held.mu.Lock()
	scalarSafe := held.retireableLocked(now)
	held.mu.Unlock()
	if !scalarSafe || r.entry.intake.phase != candidateTerminalDispatch || o.pending.targetEntryLocked(pendingKey{target, input.request.ID}) != nil || o.pending.targetCountLocked(target.Identity()) != 1 {
		t.Fatal("retirement fixture lacks real physical-only terminal witness")
	}
	reference := calendarOldCapture(t, o)
	expectedCounts := map[domain.PeerIdentity]int{target.Identity(): 1}
	if !reflect.DeepEqual(calendarFixedPending(o), expectedCounts) || len(reference.permitted(candidateTarget(900000), now, expectedCounts)) != 0 {
		t.Fatal("independent retirement reference ignored retained terminal")
	}
	nextSource, _ := provenFixture(t, 1, "new-source", 0)
	nextTarget, _ := provenFixture(t, 2, "new-target", 0)
	_, out = candidateSourceOfferSeal(t, o, nextSource, nextTarget, 2, now-o.profile.round)
	if out.reserved != 0 || out.refused != 1 || calendarFixedRead(t, o, target.Identity()) != held {
		t.Fatal("full calendar table retired a physical terminal obligation", out)
	}
	reference.compare(t, o)
	projected, actual := held.projected, held.actual
	if e = o.finishForward(permit, now, candidateWriteFinished); e != nil {
		t.Fatal(e)
	}
	if o.pending.countLocked() != 0 || held.projected != projected || held.actual != actual {
		t.Fatal("terminal Finish refunded debt or failed physical release")
	}
	// The now-unblocked original scalar-safe calendar is the sole lawful victim.
	nextNow := now + o.profile.round
	safe := reference.permitted(nextTarget.Identity(), nextNow, map[domain.PeerIdentity]int{})
	if len(safe) != 1 || !safe[target.Identity()] {
		t.Fatal("finished terminal did not restore sole lawful retirement candidate")
	}
	r2, out := candidateSourceOfferSeal(t, o, nextSource, nextTarget, 3, now)
	if out.reserved != 1 {
		t.Fatal("lawful retirement after real Finish refused", out)
	}
	removed := calendarFixedRemoved(t, reference, o)
	reference.commit(t, nextTarget.Identity(), r2.entry.intake.calendar, r2.entry.intake.alias, nextNow, r2.entry.intake.latest, nextNow, o.profile.initialTokens, safe, removed)
	reference.compare(t, o)
}
func TestCandidatePendingBookBoundsReplacementAndDebt(t *testing.T) {
	// Both16 boundaries are reached with real admitted TerminalDispatch rows.
	// Logical test times are explicit; no assumption that expiry releases writer
	// ownership, no timers, real dispatch workers or numeric runtime profile.
	for _, mode := range []string{"target16", "source16"} {
		t.Run(mode, func(t *testing.T) {
			o := candidateTestOwner(t)
			commonSource, _ := provenFixture(t, 1, "source", 0)
			commonTarget, commonSigner := provenFixture(t, 2, "target", 0)
			permits := []candidateForwardPermit{}
			sources := []Session{}
			for i := 0; i < 16; i++ {
				source, target, signer := commonSource, commonTarget, commonSigner
				if mode == "target16" {
					source, _ = provenFixture(t, domain.ConnID(100-i), domain.SourceGroup(fmt.Sprint("s", i)), 0)
				} else {
					target, signer = provenFixture(t, domain.ConnID(10+i), domain.SourceGroup(fmt.Sprint("t", i)), 0)
				}
				received := Instant(i) * Instant(time.Second)
				r, out := candidateSourceOfferSeal(t, o, source, target, domain.FindNodeRequestID(i+1), received)
				if out.reserved != 1 {
					t.Fatal("exact16 retained fixture failed lawful prefix", i, out)
				}
				now := received + o.profile.round
				permit, input, e := o.takeForward(r, now)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = o.answer(target, bodyFor(t, signer).Answer(input.request.ID), now); e != nil {
					t.Fatal(e)
				}
				permits = append(permits, permit)
				sources = append(sources, source)
			}
			source, target := commonSource, commonTarget
			if mode == "target16" {
				// The admitted protected sources descended from100 through85,
				// renewing the completed sweep watermark to85. A distinct84
				// remains eligible;100 would test the earlier newcomer gate.
				source, _ = provenFixture(t, 84, "seventeenth-source", 0)
			} else {
				target, _ = provenFixture(t, 100, "seventeenth-target", 0)
			}
			before := calendarFixedAccounting(o, append(sources, target)).([]any)
			expectedCounters := o.counters
			expectedCounters.offered++ // exact documented attempted-offer effect; test-owned copy only
			before[13] = expectedCounters
			at := Instant(15*time.Second) + o.profile.round
			if at < o.last || o.open || !o.sweep || source.ConnID() > o.watermark {
				t.Fatal("pending cap fixture does not reach capacity gate", at, o.last, o.open, o.sweep, source.ConnID(), o.watermark)
			}
			beforeWindows := source.state.sourceWindows.Load()
			if mode == "target16" && (beforeWindows != nil || o.pending.sourceCountLocked(source) != 0) {
				t.Fatal("seventeenth source already owns pending debt")
			}
			if mode == "target16" && o.pending.targetCountLocked(target.Identity()) != 16 {
				t.Fatal("terminal target16 cap lost")
			}
			if mode == "source16" && o.pending.sourceCountLocked(source) != 16 {
				t.Fatal("terminal source16 cap lost")
			}
			r, e := o.offer(source, target, request(99, target.Identity(), 1), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
			if !errors.Is(e, ErrCapacity) || r != (candidateReservation{}) || o.pending.countLocked() != 16 || source.state.sourceWindows.Load() != beforeWindows {
				t.Fatal("seventeenth retained pending obligation admitted", e)
			}
			// Full comparison changes only the independently predicted attempted-offer
			// counter in the test-owned expected snapshot. Actual owner state is untouched.
			if !reflect.DeepEqual(calendarFixedAccounting(o, append(sources, target)), before) {
				t.Fatal("capacity refusal changed original debt or rows")
			}
			for _, permit := range permits {
				if e = o.finishForward(permit, at, candidateWriteFinished); e != nil {
					t.Fatal(e)
				}
			}
			if o.pending.countLocked() != 0 {
				t.Fatal("terminal finishes did not release exact physical16")
			}
		})
	}
}
func TestCandidatePendingBookFailedJointCommitAtomic(t *testing.T) {
	// Existing three-case calendar test retains exact ErrStaleWork/ErrDuplicateID/
	// ErrCapacity and full DeepEqual. Here source ownership conflict is independent.
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	r, e := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if e != nil {
		t.Fatal(e)
	}
	prepared := o.prepareSources()
	foreign := &candidateSourceWindows{owner: &candidateSourceOwnerToken{}}
	source.state.sourceWindows.Store(foreign) // explicit original companion conflict, not a claimed final-CAS race
	before := calendarFixedAccounting(o, []Session{source, target})
	if e = calendarFixedCommitDirect(o, r, o.profile.round, prepared[source]); !errors.Is(e, ErrStaleWork) {
		t.Fatal("foreign source companion did not refuse exact stale gate", e)
	}
	if !reflect.DeepEqual(calendarFixedAccounting(o, []Session{source, target}), before) {
		t.Fatal("failed source ownership trial changed pending or joint accounting")
	}
}

// pendingTestSweepInert establishes a completed lawful same-now sweep before
// a stale-authority no-change baseline. Stale APIs themselves advance first.
func pendingTestSweepInert(t *testing.T, o *intakeCoordinator, members []Session, now Instant) any {
	t.Helper()
	for _, member := range members {
		if !member.valid() {
			t.Fatal("stale fixture requires live original Sessions")
		}
	}
	if err := o.expire(now); err != nil {
		t.Fatal("stale fixture initial lawful sweep", err)
	}
	o.mu.Lock()
	o.pending.mu.Lock()
	inert := o.last == now
	for _, entry := range o.pending.entries {
		if entry == nil {
			continue
		}
		if !entry.source.valid() || !entry.target.valid() || now >= entry.expires || (entry.intake.phase == candidateReserved && now > entry.intake.latest) {
			inert = false
		}
	}
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if !inert {
		t.Fatal("stale fixture left eligible expiry or invalid original Session")
	}
	before := calendarFixedAccounting(o, members)
	if err := o.expire(now); err != nil {
		t.Fatal("stale fixture repeated lawful sweep", err)
	}
	if !reflect.DeepEqual(calendarFixedAccounting(o, members), before) {
		t.Fatal("same-now sweep is not inert before stale authority check")
	}
	return before
}

func TestCandidatePendingBookStaleHandlesAndConcurrentTerminals(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, signer := provenFixture(t, 2, "target", 0)
	old, e := o.offer(source, target, request(7, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if e != nil {
		t.Fatal(e)
	}
	cell := o.pending.sourceIndexLocked(pendingKey{source, 7})
	if ok, e := o.cancel(old, 0); !ok || e != nil {
		t.Fatal(e)
	}
	next, e := o.offer(source, target, request(7, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if e != nil {
		t.Fatal(e)
	}
	if next.entry == old.entry || next.entry.intake == old.entry.intake || o.pending.sourceIndexLocked(pendingKey{source, 7}) != cell {
		t.Fatal("vacancy reuse did not allocate distinct retained objects")
	}
	before := pendingTestSweepInert(t, o, []Session{source, target}, 0)
	if ok, e := o.cancel(old, 0); ok || e != nil {
		t.Fatal("stale source-key reservation cancelled replacement", e)
	}
	// Defensive raw fixed release is outside frozen legacy release's lawful
	// invocation domain; it must NOT erase an equal-key replacement.
	o.pending.release(old.entry)
	if !reflect.DeepEqual(calendarFixedAccounting(o, []Session{source, target}), before) {
		t.Fatal("detached raw fixed release or stale handle changed replacement")
	}
	if out, e := o.sealRound(candidateView(t, o, o.profile.round, []Session{source}, []Session{source}, target), o.profile.round); e != nil || out.reserved != 1 {
		t.Fatal(e)
	}
	permit, input, e := o.takeForward(next, o.profile.round)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = o.answer(target, bodyFor(t, signer).Answer(input.request.ID), o.profile.round); e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	results := make([]error, 2)
	var joined sync.WaitGroup
	for i := range results {
		joined.Add(1)
		go func(i int) {
			defer joined.Done()
			<-start
			results[i] = o.finishForward(permit, o.profile.round, candidateWriteFinished)
		}(i)
	}
	close(start)
	joined.Wait()
	wins := 0
	for _, e := range results {
		if e == nil {
			wins++
		} else if !errors.Is(e, ErrStaleWork) {
			t.Fatal("concurrent Finish reached unrelated refusal", e)
		}
	}
	if wins != 1 || o.pending.countLocked() != 0 {
		t.Fatal("copied writer permits released physical ownership more than once", wins)
	}
	before = pendingTestSweepInert(t, o, []Session{source, target}, o.profile.round)
	if _, _, e = o.takeForward(old, o.profile.round); !errors.Is(e, ErrStaleWork) {
		t.Fatal("old permit/reservation revived after cell reuse", e)
	}
	if !reflect.DeepEqual(calendarFixedAccounting(o, []Session{source, target}), before) {
		t.Fatal("stale Take changed current original accounting")
	}
}
func TestCandidatePendingBookPublicLegacyCompatibility(t *testing.T) {
	// Public implementation remains independent; it still publishes counters and
	// accepts a plain Forward, unlike the private dormant owner storage book.
	source, _ := provenFixture(t, 1, "source", 0)
	target, signer := provenFixture(t, 2, "target", 0)
	p := NewPendingTable()
	f, e := p.Reserve(source, target, request(7, target.Identity(), 1), 0)
	if e != nil {
		t.Fatal(e)
	}
	if p.bySource[pendingKey{source, 7}] != f.entry || p.Snapshot().Pending != 1 || p.Snapshot().Accepted != 1 {
		t.Fatal("public pending constructor/API/counters changed")
	}
	if _, e = p.Answer(target, bodyFor(t, signer).Answer(f.Request.ID), 0); e != nil {
		t.Fatal(e)
	}
	if p.Snapshot().Pending != 0 || p.Snapshot().Completed != 1 || p.Cancel(f) {
		t.Fatal("public pending answer/cancel semantics changed")
	}
}
