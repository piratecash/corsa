package findnode

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func candidateTestOwner(t *testing.T) *intakeCoordinator {
	t.Helper()
	o, err := newIntakeCoordinator(candidateProfile{timing: TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, round: Instant(100 * time.Millisecond), nodeCap: 512, initialTokens: 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func candidateView(t *testing.T, o *intakeCoordinator, at Instant, sources, live []Session, targets ...Session) candidateMembership {
	t.Helper()
	ids := make([]domain.PeerIdentity, len(targets))
	for i, target := range targets {
		ids[i] = target.Identity()
	}
	view, err := captureCandidateMembership(o, ids, sources, live, at, func(SessionInfo) (SessionClass, error) { return ClassSelected, nil })
	if err != nil {
		t.Fatal(err)
	}
	return view
}
func TestCandidateCombinedDelayedActualAndFutureAliases(t *testing.T) {
	target, _ := provenFixture(t, 1, "target", 0)
	calendar, err := NewForwardCalendar(target.Identity(), 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	// Exact legacy-calendar state: four projected0.1 aliases, actual Takes0.6.
	aliases := make([]Reservation, 4)
	for i := range aliases {
		aliases[i], err = calendar.Reserve(Instant(100*time.Millisecond), Instant(750*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, alias := range aliases {
		if err = calendar.Consume(alias, Instant(600*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	calendar.mu.Lock()
	oldP, oldA := calendar.projected, calendar.actual
	_, err = calendar.planCombinedLocked(Instant(700*time.Millisecond), Instant(1350*time.Millisecond))
	if !errors.Is(err, ErrBudget) {
		t.Errorf("projected-only false promise accepted: %v", err)
	}
	if calendar.projected != oldP || calendar.actual != oldA {
		t.Error("failed trial mutated buckets")
	}
	calendar.mu.Unlock()
	future, err := NewForwardCalendar(target.Identity(), 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err = future.Reserve(Instant(500*time.Millisecond), Instant(2*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	future.mu.Lock()
	if _, err = future.planCombinedLocked(Instant(600*time.Millisecond), Instant(1750*time.Millisecond)); !errors.Is(err, ErrBudget) {
		t.Errorf("future aliases ignored: %v", err)
	}
	plan, err := future.planCombinedLocked(Instant(600*time.Millisecond), Instant(3*time.Second))
	future.mu.Unlock()
	if err != nil || plan.slot.at != Instant(3*time.Second) {
		t.Fatalf("conservative actual floor=%v err=%v", plan.slot.at, err)
	}
}
func TestCandidateTrialRefusalNoEffectsAndIDExhaustion(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 10, "source", 0)
	target, _ := provenFixture(t, 20, "target", 0)
	received := Instant(0)
	v := candidateView(t, o, received, []Session{source}, []Session{source}, target)
	r, err := o.offer(source, target, request(1, target.Identity(), 1), v, received)
	if err != nil {
		t.Fatal(err)
	}
	target.state.mu.Lock()
	target.state.exhausted = true
	next := target.state.nextID
	target.state.mu.Unlock()
	seal := Instant(100 * time.Millisecond)
	out, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal)
	if err != nil || out.reserved != 0 || out.refused != 1 {
		t.Fatalf("exhaustion result %+v/%v", out, err)
	}
	if s := o.snapshot(); s.names != 0 || s.records != 0 || s.calendars != 0 || s.pending != 0 || s.cursor != 0 {
		t.Fatalf("partial effects %+v", s)
	}
	if target.state.nextID != next {
		t.Fatal("ID consumed on refusal")
	}
	if _, _, err = o.takeForward(r, seal); !errors.Is(err, ErrStaleWork) {
		t.Fatal("refused handle remained live")
	}
}
func TestCandidateLifecycleNoRefundAndRetainedWriter(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 10, "source", 0)
	target, _ := provenFixture(t, 20, "target", 0)
	r, err := o.offer(source, target, request(7, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if err != nil {
		t.Fatal(err)
	}
	at := Instant(100 * time.Millisecond)
	if out, e := o.sealRound(candidateView(t, o, at, []Session{source}, []Session{source}, target), at); e != nil || out.reserved != 1 {
		t.Fatalf("seal %+v/%v", out, e)
	}
	calendar := r.entry.intake.calendar
	debt := calendar.projected
	permit, input, err := o.takeForward(r, at)
	if err != nil {
		t.Fatal(err)
	}
	if input.request.Challenge != r.entry.request.Challenge {
		t.Fatal("changed challenge")
	}
	if _, _, err = o.takeForward(r, at); !errors.Is(err, ErrStaleWork) {
		t.Fatal("double take")
	}
	if err = o.closeSession(source, at); err != nil {
		t.Fatal(err)
	}
	if s := o.snapshot(); s.pending != 1 || s.terminal != 1 || s.records != 1 {
		t.Fatalf("writer prematurely freed %+v", s)
	}
	if err = o.expire(Instant(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if o.snapshot().pending != 1 {
		t.Fatal("nominal expiry freed active writer")
	}
	if err = o.finishForward(permit, Instant(21*time.Second), candidateWriteFailed); err != nil {
		t.Fatal(err)
	}
	if s := o.snapshot(); s.pending != 0 || s.records != 1 || s.violations != 1 {
		t.Fatalf("finish %+v", s)
	}
	if calendar.projected != debt || calendar.actual.tokens != forwardBurst-forwardToken {
		t.Fatal("refunded reservation/actual token")
	}
	if err = o.finishForward(permit, Instant(22*time.Second), candidateWriteFinished); !errors.Is(err, ErrStaleWork) {
		t.Fatal("replayed Finish")
	}
	if o.resetGroups(Instant(620 * time.Second)) {
		t.Fatal("reset before completion+window")
	}
	if !o.resetGroups(Instant(621 * time.Second)) {
		t.Fatal("idle generation did not reset")
	}
	if _, _, err = o.takeForward(r, Instant(621*time.Second)); !errors.Is(err, ErrStaleWork) {
		t.Fatal("stale ABA handle")
	}
}
func TestCandidateOneProvisionalFixedCutAndSuccessfulCursor(t *testing.T) {
	o := candidateTestOwner(t)
	a, _ := provenFixture(t, 1, "shared", 0)
	b, _ := provenFixture(t, 2, "shared", 0)
	target, _ := provenFixture(t, 3, "target", 0)
	sources := []Session{a, b}
	v := candidateView(t, o, 0, sources, sources, target)
	if _, err := o.offer(a, target, request(1, target.Identity(), 1), v, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := o.offer(a, target, request(2, target.Identity(), 2), v, 0); !errors.Is(err, ErrCapacity) {
		t.Fatal("second provisional")
	}
	if _, err := o.offer(b, target, request(3, target.Identity(), 3), v, 0); err != nil {
		t.Fatal(err)
	}
	if o.snapshot().records != 0 {
		t.Fatal("pre-admission debit")
	}
	cut := o.cut
	if _, err := o.sealRound(candidateView(t, o, cut-1, sources, sources, target), cut-1); !errors.Is(err, ErrNotReady) {
		t.Fatal("early flush")
	}
	if _, err := o.offer(a, target, request(4, target.Identity(), 4), candidateView(t, o, cut, sources, sources, target), cut); !errors.Is(err, ErrNotReady) {
		t.Fatal("late offer entered old round")
	}
	if o.cut != cut {
		t.Fatal("round extended")
	}
	out, err := o.sealRound(candidateView(t, o, cut, sources, sources, target), cut)
	if err != nil || out.reserved != 2 {
		t.Fatalf("seal %+v/%v", out, err)
	}
	if o.snapshot().records != 1 || o.snapshot().cursor != b.ConnID() {
		t.Fatalf("shared repeat/cursor %+v", o.snapshot())
	}
}
func TestCandidateTrueL2MembershipAndWatermark(t *testing.T) {
	o := candidateTestOwner(t)
	target, _ := provenFixture(t, 100, "target", 0)
	live := make([]Session, 6)
	for i := range live {
		live[i], _ = provenFixture(t, domain.ConnID(i+1), "shared", Instant(i))
	}
	at := candidateGroupWindow + 100
	ids := []domain.PeerIdentity{target.Identity()}
	view, err := captureCandidateMembership(o, ids, live[4:], live, at, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if p, e := view.priority(o, live[4], target.Identity(), at); e != nil || p != PriorityNew {
		t.Fatalf("non-row oldest excluded %v/%v", p, e)
	}
	view, err = captureCandidateMembership(o, ids, live, live, at, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range live {
		p, e := view.priority(o, s, target.Identity(), at)
		if e != nil || (i < 4 && p != PriorityAged) || (i >= 4 && p != PriorityNew) {
			t.Fatalf("top4 %d/%v/%v", i, p, e)
		}
	}
	// A protected source above a frozen membership watermark waits for a sweep.
	o.sweep = true
	o.watermark = 2
	selected := candidateView(t, o, at, live, live, target)
	if _, err = o.offer(live[5], target, request(1, target.Identity(), 1), selected, at); !errors.Is(err, ErrNotReady) {
		t.Fatal("new high ID bypassed watermark")
	}
	forged := view
	forged.owner = candidateTestOwner(t)
	if _, err = o.offer(live[0], target, request(1, target.Identity(), 1), forged, at); !errors.Is(err, ErrPrioritySnapshot) {
		t.Fatal("cross-owner membership")
	}
}
func TestCandidateConcurrentIDAndClose(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0)
	if err != nil {
		t.Fatal(err)
	}
	at := Instant(100 * time.Millisecond)
	view := candidateView(t, o, at, []Session{source}, []Session{source}, target)
	target.state.mu.Lock()
	var wg sync.WaitGroup
	wg.Add(2)
	ids := make(chan domain.FindNodeRequestID, 1)
	errs := make(chan error, 2)
	go func() { defer wg.Done(); id, e := target.allocateID(); ids <- id; errs <- e }()
	go func() { defer wg.Done(); _, e := o.sealRound(view, at); errs <- e }()
	target.state.mu.Unlock()
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if r.entry.forwarded.ID == <-ids {
		t.Fatal("peeked ID stolen by allocator")
	}
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = o.cancel(r, at) }()
	go func() { defer wg.Done(); _ = o.closeSession(source, at) }()
	wg.Wait()
	if o.snapshot().pending != 0 || o.snapshot().records != 1 {
		t.Fatalf("double release/refund %+v", o.snapshot())
	}
}
func TestCandidateClockGenerationAndIDOverflow(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	o.generation = math.MaxUint64
	if _, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, 0, []Session{source}, []Session{source}, target), 0); !errors.Is(err, ErrCapacity) {
		t.Fatal("generation wrapped")
	}
	if o.snapshot().pending != 0 {
		t.Fatal("overflow effects")
	}
	o.generation = 0
	if err := o.expire(100); err != nil {
		t.Fatal(err)
	}
	if err := o.expire(99); !errors.Is(err, ErrClock) {
		t.Fatal("clock rollback")
	}
	target.state.mu.Lock()
	target.state.nextID = math.MaxUint64
	target.state.mu.Unlock()
	id, err := target.allocateID()
	if err != nil || uint64(id) != math.MaxUint64 {
		t.Fatal("final ID unavailable")
	}
	if _, err = target.allocateID(); !errors.Is(err, ErrStaleWork) {
		t.Fatal("ID wrapped")
	}
}

func TestCandidateL1RotationAndLowerCellRoundRobin(t *testing.T) {
	o := candidateTestOwner(t)
	target, _ := provenFixture(t, 100, "target", 0)
	sources := make([]Session, 10)
	for i := range sources {
		sources[i], _ = provenFixture(t, domain.ConnID(i+1), "shared", 0)
	}
	counts := make(map[Session]int)
	for round := range 5 {
		at := Instant(round) * Instant(10*time.Second)
		view := candidateView(t, o, at, sources, sources, target)
		handles := make([]candidateReservation, 0, len(sources))
		for i, s := range sources {
			r, err := o.offer(s, target, request(domain.FindNodeRequestID(round), target.Identity(), byte(i)), view, at)
			if err != nil {
				t.Fatal(err)
			}
			handles = append(handles, r)
		}
		cut := at + o.profile.round
		result, err := o.sealRound(candidateView(t, o, cut, sources, sources, target), cut)
		if err != nil || result.reserved != 4 {
			t.Fatalf("conservative slots %+v/%v", result, err)
		}
		for _, r := range handles {
			if r.entry.intake.phase != candidateReserved {
				continue
			}
			counts[r.entry.source]++
			projected := r.entry.intake.calendar.projected
			if ok, e := o.cancel(r, cut); !ok || e != nil {
				t.Fatal("cancel")
			}
			if r.entry.intake.calendar.projected != projected {
				t.Fatal("cancel refunded projected debt")
			}
		}
	}
	for _, s := range sources {
		if counts[s] != 2 {
			t.Fatalf("starved L1 %d: %d", s.ConnID(), counts[s])
		}
	}
	lower := candidateTestOwner(t)
	groupCommit(t, &lower.groups, "A", candidateTarget(1), 0, 512)
	groupCommit(t, &lower.groups, "B", candidateTarget(2), 0, 512)
	sources = sources[:6]
	// Construct fresh actual sessions with the two authoritative fixture groups.
	for i := range sources {
		group := domain.SourceGroup("A")
		if i >= 3 {
			group = "B"
		}
		sources[i], _ = provenFixture(t, domain.ConnID(i+1), group, 0)
	}
	ids := []domain.PeerIdentity{target.Identity()}
	view, err := captureCandidateMembership(lower, ids, sources, sources, 0, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	handles := make([]candidateReservation, 0, 6)
	for i, s := range sources {
		r, e := lower.offer(s, target, request(domain.FindNodeRequestID(i), target.Identity(), byte(i)), view, 0)
		if e != nil {
			t.Fatal(e)
		}
		handles = append(handles, r)
	}
	cut := lower.profile.round
	view, err = captureCandidateMembership(lower, ids, sources, sources, cut, ordinary)
	if err != nil {
		t.Fatal(err)
	}
	result, err := lower.sealRound(view, cut)
	if err != nil || result.reserved != 4 {
		t.Fatalf("lower slots %+v/%v", result, err)
	}
	byGroup := make(map[domain.SourceGroup]int)
	for _, r := range handles {
		if r.entry.intake.phase == candidateReserved {
			byGroup[r.entry.source.state.group]++
		}
	}
	if byGroup["A"] != 2 || byGroup["B"] != 2 {
		t.Fatalf("group batching replaced RR: %+v", byGroup)
	}
}

func TestCandidateEmptySweepRenewsOnlyAfterFixedInterval(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 99, "new", 0)
	target, _ := provenFixture(t, 100, "target", 0)
	o.sweep = true
	o.watermark = 3
	o.cursor = 2
	view := candidateView(t, o, 0, []Session{source}, []Session{source}, target)
	if _, err := o.offer(source, target, request(1, target.Identity(), 1), view, 0); !errors.Is(err, ErrNotReady) {
		t.Fatal("newcomer entered old sweep")
	}
	if !o.open || o.snapshot().pending != 0 || o.watermark != 3 {
		t.Fatal("newcomer bypassed fixed empty collection")
	}
	if _, err := o.sealRound(candidateView(t, o, o.cut-1, []Session{source}, []Session{source}, target), o.cut-1); !errors.Is(err, ErrNotReady) {
		t.Fatal("empty sweep flushed early")
	}
	cut := o.cut
	if _, err := o.sealRound(candidateView(t, o, cut, []Session{source}, []Session{source}, target), cut); err != nil {
		t.Fatal(err)
	}
	if o.watermark != 99 {
		t.Fatal("empty sweep permanently excluded new IDs")
	}
	if _, err := o.offer(source, target, request(2, target.Identity(), 2), candidateView(t, o, cut, []Session{source}, []Session{source}, target), cut); err != nil {
		t.Fatal(err)
	}
}
func TestCandidateComposedAnswerCorrelationAndABA(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, key := provenFixture(t, 2, "target", 0)
	impostor, _ := provenFixture(t, 3, "impostor", 0)
	at := Instant(0)
	r, err := o.offer(source, target, request(42, target.Identity(), 9), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
	if err != nil {
		t.Fatal(err)
	}
	cut := o.cut
	if _, err = o.sealRound(candidateView(t, o, cut, []Session{source}, []Session{source}, target), cut); err != nil {
		t.Fatal(err)
	}
	permit, input, err := o.takeForward(r, cut)
	if err != nil {
		t.Fatal(err)
	}
	answer := bodyFor(t, key).Answer(input.request.ID)
	if _, err = o.answer(impostor, answer, cut); !errors.Is(err, ErrUnmatchedAnswer) {
		t.Fatal("wrong actual target link")
	}
	delivery, err := o.answer(target, answer, cut)
	if err != nil || delivery.Session != source || delivery.Answer.ID != 42 {
		t.Fatalf("source restore %+v/%v", delivery, err)
	}
	if s := o.snapshot(); s.pending != 1 || s.terminal != 1 {
		t.Fatal("answer lost writer obligation")
	}
	if _, err = o.answer(target, answer, cut); !errors.Is(err, ErrUnmatchedAnswer) {
		t.Fatal("answer replay")
	}
	if err = o.finishForward(permit, cut, candidateWriteFinished); err != nil {
		t.Fatal(err)
	}
	at = Instant(10 * time.Second)
	fresh, err := o.offer(source, target, request(42, target.Identity(), 10), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := o.cancel(r, at); ok || err != nil {
		t.Fatal("old exact ID cancelled recreated row")
	}
	if ok, err := o.cancel(fresh, at); !ok || err != nil {
		t.Fatal("fresh authority unavailable")
	}
}
