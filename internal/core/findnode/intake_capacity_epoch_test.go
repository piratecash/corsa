package findnode

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func capacityOwner(t *testing.T, hint candidateCapacityHint) *intakeCoordinator {
	t.Helper()
	o, err := newCapacityIntakeCoordinator(candidateProfile{timing: TimingProfile{Target: time.Second, Network: time.Second, Runtime: time.Second}, round: Instant(100 * time.Millisecond), nodeCap: 512, initialTokens: 4}, 0, hint)
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func capacityCut(o *intakeCoordinator, at Instant, degree int) candidateCapacityCut {
	return candidateCapacityCut{owner: o, domain: o.capacity.domain, at: at, degree: degree, complete: true}
}
func capacitySelect(o *intakeCoordinator, at Instant, cuts ...candidateCapacityCut) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.selectCapacityLocked(at, cuts)
}
func TestCandidateCapacityBootstrapAndCopiedInputs(t *testing.T) {
	for _, c := range []struct {
		name string
		hint candidateCapacityHint
		want int
	}{
		{"absent", candidateCapacityHint{}, 512},
		{"zero", candidateCapacityHint{present: true}, 512},
		{"fresh", candidateCapacityHint{value: 10, age: candidateCapacityHintAge - 1, present: true}, 600},
		{"exact24h", candidateCapacityHint{value: 64, age: candidateCapacityHintAge, present: true}, 512},
		{"future", candidateCapacityHint{value: 64, age: -1, present: true}, 512},
		{"negative", candidateCapacityHint{value: -1, present: true}, 512},
		{"clip", candidateCapacityHint{value: 65, present: true}, 3840},
		{"maxint", candidateCapacityHint{value: math.MaxInt, present: true}, 3840},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := capacityOwner(t, c.hint)
			c.hint.value = 0
			cap, err := capacitySelect(o, 0)
			if err != nil || cap != c.want {
				t.Fatal("bootstrap hint boundary or clipping changed", cap, c.want, err)
			}
			cap, err = capacitySelect(o, candidateCapacityWindow-1, capacityCut(o, candidateCapacityWindow-1, 0))
			if err != nil || cap != c.want || o.profile.nodeCap != 512 {
				t.Fatal("first-window bootstrap was overwritten", cap, err)
			}
			if o.capacity.owner != o || o.capacity.domain == nil || o.sourceOwner == nil {
				t.Fatal("capacity constructor lost original owner/domain")
			}
		})
	}
}
func TestCandidateCapacityCompleteCurrentEpochAndOwnership(t *testing.T) {
	for _, kind := range []string{"missing", "incomplete", "foreign_owner", "foreign_domain", "future", "stale_epoch", "negative_degree", "multiple"} {
		t.Run(kind, func(t *testing.T) {
			o := capacityOwner(t, candidateCapacityHint{})
			now := candidateCapacityWindow
			cut := capacityCut(o, now, 64)
			switch kind {
			case "incomplete":
				cut.complete = false
			case "foreign_owner":
				cut.owner = capacityOwner(t, candidateCapacityHint{})
			case "foreign_domain":
				cut.domain = &candidateCapacityDomain{}
			case "future":
				cut.at = now + 1
			case "stale_epoch":
				cut.at = now - 1
			case "negative_degree":
				cut.degree = -1
			}
			cuts := []candidateCapacityCut{cut}
			if kind == "missing" {
				cuts = nil
			}
			if kind == "multiple" {
				cuts = append(cuts, cut)
			}
			if _, err := capacitySelect(o, now, cuts...); err == nil {
				t.Fatal("invalid or incomplete boundary cut accepted", kind)
			}
			if o.capacity.epoch != 0 || o.capacity.cap != 512 || fixedGroupRecords(&o.groups) != 0 {
				t.Fatal("refused cut changed frozen cap or debt", kind)
			}
		})
	}
	o := capacityOwner(t, candidateCapacityHint{})
	now := candidateCapacityWindow + 17
	if cap, err := capacitySelect(o, now, capacityCut(o, now-10, 64)); err != nil || cap != 3840 {
		t.Fatal("complete delayed current cut refused", cap, err)
	}
	if cap, err := capacitySelect(o, now+1, capacityCut(o, now+1, 0)); err != nil || cap != 3840 {
		t.Fatal("same epoch cap was revised", cap, err)
	}
	late := Instant(math.MaxInt64)
	if cap, err := capacitySelect(o, late, capacityCut(o, late, 1)); err != nil || cap != 512 {
		t.Fatal("skipped epochs overflowed or replayed", cap, err)
	}
	if _, err := capacitySelect(o, late-1); !errors.Is(err, ErrClock) {
		t.Fatal("logical regression accepted", err)
	}
}
func TestCandidateCapacityForeignStateAndFixedDefault(t *testing.T) {
	a, b := capacityOwner(t, candidateCapacityHint{}), capacityOwner(t, candidateCapacityHint{})
	if a.capacity.domain == b.capacity.domain {
		t.Fatal("nonzero capacity domains alias")
	}
	a.capacity = b.capacity // private corruption fixture, not a production setter
	if _, err := capacitySelect(a, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign capacity owner accepted", err)
	}
	fixed := candidateTestOwner(t)
	if fixed.capacity != nil {
		t.Fatal("ordinary fixed-cap constructor enabled epochs")
	}
	if cap, err := capacitySelect(fixed, 0); err != nil || cap != fixed.profile.nodeCap {
		t.Fatal("fixed-cap default changed", cap, err)
	}
	if _, err := capacitySelect(fixed, 0, capacityCut(b, 0, 64)); !errors.Is(err, ErrInvalid) {
		t.Fatal("fixed owner accepted epoch input", err)
	}
}
func TestCandidateCapacityRefusedBoundaryKeepsAcceptedWriter(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{value: 64, present: true})
	source, _ := provenFixture(t, 10, "capacity-source", 0)
	target, _ := provenFixture(t, 20, "capacity-target", 0)
	at := Instant(599 * time.Second)
	r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
	if err != nil {
		t.Fatal(err)
	}
	seal := at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal); err != nil || result.reserved != 1 {
		t.Fatal("bootstrap genuine round refused", result, err)
	}
	permit, _, err := o.takeForward(r, seal)
	if err != nil {
		t.Fatal(err)
	}
	windows := source.state.sourceWindows.Load()
	before := *windows
	owner, generation, calendar := o.sourceOwner, o.groups.generation, r.entry.intake.calendar
	projected, actual := calendar.projected, calendar.actual
	at = candidateCapacityWindow
	if _, err = o.offer(source, target, request(2, target.Identity(), 2), candidateView(t, o, at, []Session{source}, []Session{source}, target), at); err != nil {
		t.Fatal(err)
	}
	seal = at + Instant(100*time.Millisecond)
	result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal)
	if err != nil || result.refused != 1 || result.reserved != 0 {
		t.Fatal("missing boundary cut admitted or left provisional work", result, err)
	}
	if *windows != before || o.sourceOwner != owner || o.groups.generation != generation || r.entry.intake.calendar != calendar || calendar.projected != projected || calendar.actual != actual {
		t.Fatal("boundary refusal reset or charged old ledger")
	}
	if o.snapshot().dispatching != 1 || o.snapshot().pending != 1 {
		t.Fatal("boundary refusal destroyed accepted writer")
	}
	finish := seal + Instant(100*time.Millisecond)
	key := r.entry.intake.cell
	if err := o.finishForward(permit, finish, candidateWriteFailed); err != nil {
		t.Fatal("accepted completion blocked by missing new cut", err)
	}
	if fixedMustRecord(t, &o.groups, key).expires != finish+candidateGroupWindow || calendar.projected != projected || calendar.actual != actual {
		t.Fatal("writer finish lost expiry debt or refunded tokens")
	}
	if fixedMustRecord(t, &o.groups, key).refs != 0 || o.snapshot().pending != 0 {
		t.Fatal("actual writer return not released")
	}
	at = finish + Instant(100*time.Millisecond)
	if _, err := o.offer(source, target, request(3, target.Identity(), 3), candidateView(t, o, at, []Session{source}, []Session{source}, target), at); err != nil {
		t.Fatal(err)
	}
	seal = at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal, capacityCut(o, seal, 0)); err != nil || result.reserved != 1 || o.capacity.cap != 512 {
		t.Fatal("complete current cut did not restore new admission", result, err)
	}
	if o.sourceOwner != owner || o.groups.generation != generation || source.state.sourceWindows.Load() != windows {
		t.Fatal("successful shrink rebound ownership or source history")
	}
}
func TestCandidateCapacityShrinkRetainsConservativeGlobalDebt(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{value: 64, present: true})
	at := Instant(599 * time.Second)
	// Historical group debt is assembled through actual pure group plans,
	// not invented network proofs or thousands of socket handshakes.
	for cell := 0; cell <= 50; cell++ {
		n := 16
		if cell == 0 {
			n = 3840
		}
		for i := 0; i < n; i++ {
			target := domain.PeerIdentity{byte(cell + 1), byte(i >> 8), byte(i)}
			plan, err := o.groups.plan(domain.SourceGroup(fmt.Sprintf("g%d", cell)), target, at, 3840)
			if err != nil {
				t.Fatal("historical valid group plan failed", cell, i, err)
			}
			o.groups.commit(plan, at)
		}
	}
	count, names, generation, owner := fixedGroupRecords(&o.groups), fixedGroupNames(&o.groups), o.groups.generation, o.sourceOwner
	if count != 4640 || count <= 512+16*candidateGroupCells || count > candidateMaxRecords {
		t.Fatal("shrink debt fixture not above new admission ceiling", count)
	}
	if cap, err := capacitySelect(o, candidateCapacityWindow, capacityCut(o, candidateCapacityWindow, 0)); err != nil || cap != 512 {
		t.Fatal(err, cap)
	}
	if fixedGroupRecords(&o.groups) != count || fixedGroupNames(&o.groups) != names || o.groups.generation != generation || o.sourceOwner != owner {
		t.Fatal("capacity shrink evicted or reset old debt")
	}
	if _, err := o.groups.plan("late", domain.PeerIdentity{254, 1}, candidateCapacityWindow, 512); !errors.Is(err, ErrCapacity) {
		t.Fatal("hard retained ceiling allowed new distinct debt after shrink", err)
	}
	key := candidateRecordKey{cell: 0, target: domain.PeerIdentity{1, 0, 0}}
	refs := fixedMustRecord(t, &o.groups, key).refs
	plan, err := o.groups.plan("g0", key.target, candidateCapacityWindow, 512)
	if err != nil {
		t.Fatal("shrink changed existing-record reference policy", err)
	}
	o.groups.commit(plan, candidateCapacityWindow)
	if fixedMustRecord(t, &o.groups, key).refs != refs+1 || fixedGroupRecords(&o.groups) != count {
		t.Fatal("existing record lost retained reference")
	}
}
func TestCandidateCapacityForwardSlotKeepsFutureDebtAndOldPermit(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{})
	target, _ := provenFixture(t, 100, "capacity-target", 0)
	// Sources 1 and 5 share the original target calendar. The middle three
	// use independent genuine targets so their reservations do not consume
	// that calendar's equal-deadline signing budget. The fifth original-target
	// slot can project across 600s at its unchanged latest boundary; this does
	// not assert five simultaneous same-target admissions.
	targets := []Session{target}
	for i := 0; i < 3; i++ {
		middle, _ := provenFixture(t, domain.ConnID(101+i), domain.SourceGroup(fmt.Sprintf("capacity-target-%d", i+1)), 0)
		targets = append(targets, middle)
	}
	var sources []Session
	for i := 0; i < 5; i++ {
		s, _ := provenFixture(t, domain.ConnID(i+1), domain.SourceGroup(fmt.Sprintf("capacity-%d", i)), 0)
		sources = append(sources, s)
	}
	at := Instant(599 * time.Second)
	var reservations []candidateReservation
	for i, source := range sources {
		selectedTarget := target
		if i > 0 && i < 4 {
			selectedTarget = targets[i]
		}
		r, err := o.offer(source, selectedTarget, request(domain.FindNodeRequestID(i+1), selectedTarget.Identity(), byte(i+1)), candidateView(t, o, at, sources, sources, targets...), at)
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, r)
	}
	seal := at + Instant(100*time.Millisecond)
	result, err := o.sealRound(candidateView(t, o, seal, sources, sources, targets...), seal)
	if err != nil || result.reserved != 5 || result.refused != 0 {
		t.Fatal("genuine projected future-slot fixture refused", result, err)
	}
	last := reservations[len(reservations)-1]
	slot := last.entry.intake.alias.at
	if slot < candidateCapacityWindow || o.capacity.epoch != 0 || o.capacity.cap != 512 {
		t.Fatal("future forwarding slot selected future degree epoch", slot, o.capacity.epoch)
	}
	key := last.entry.intake.cell
	if fixedMustRecord(t, &o.groups, key).expires != slot+candidateGroupWindow || fixedMustRecord(t, &o.groups, key).expires <= seal+candidateGroupWindow {
		t.Fatal("future slot debt shortened to admission time")
	}
	permit, _, err := o.takeForward(last, slot)
	if err != nil {
		t.Fatal("old future permit blocked without new capacity cut", err)
	}
	if err := o.finishForward(permit, slot, candidateWriteFailed); err != nil {
		t.Fatal(err)
	}
}
func TestCandidateCapacityMembershipIsNotTimestampRelabel(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{})
	source, _ := provenFixture(t, 1, "capacity-source", 0)
	target, _ := provenFixture(t, 2, "capacity-target", 0)
	at := Instant(599 * time.Second)
	if _, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, at, []Session{source}, []Session{source}, target), at); err != nil {
		t.Fatal(err)
	}
	stale := candidateView(t, o, at, []Session{source}, []Session{source}, target)
	if _, err := o.sealRound(stale, candidateCapacityWindow, capacityCut(o, candidateCapacityWindow, 64)); !errors.Is(err, ErrPrioritySnapshot) {
		t.Fatal("old membership was relabeled as post-wait observation", err)
	}
	if source.state.sourceWindows.Load() != nil || o.capacity.epoch != 0 {
		t.Fatal("stale membership charged or froze a cut")
	}
}

func TestCandidateCapacityBoundaryKeepsAnswerCancelAndExpiry(t *testing.T) {
	for _, kind := range []string{"answer", "cancel", "expire"} {
		t.Run(kind, func(t *testing.T) {
			o := capacityOwner(t, candidateCapacityHint{})
			source, _ := provenFixture(t, 1, "capacity-source", 0)
			target, key := provenFixture(t, 2, "capacity-target", 0)
			at := Instant(599 * time.Second)
			r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
			if err != nil {
				t.Fatal(err)
			}
			seal := at + Instant(100*time.Millisecond)
			if result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal); err != nil || result.reserved != 1 {
				t.Fatal(result, err)
			}
			calendar := r.entry.intake.calendar
			debt := calendar.projected
			switch kind {
			case "answer":
				permit, input, err := o.takeForward(r, seal)
				if err != nil {
					t.Fatal(err)
				}
				if err = o.finishForward(permit, candidateCapacityWindow, candidateWriteFinished); err != nil {
					t.Fatal(err)
				}
				if delivery, err := o.answer(target, bodyFor(t, key).Answer(input.request.ID), candidateCapacityWindow); err != nil || delivery.Session != source {
					t.Fatal("missing new cut blocked old answer", err)
				}
			case "cancel":
				if removed, err := o.cancel(r, candidateCapacityWindow); err != nil || !removed {
					t.Fatal("missing new cut blocked cancellation", removed, err)
				}
			case "expire":
				if err := o.expire(at + responderTimeout); err != nil {
					t.Fatal("missing new cut blocked expiry", err)
				}
			}
			if o.snapshot().pending != 0 || calendar.projected != debt || o.capacity.epoch != 0 {
				t.Fatal("accepted operation reset debt or required fresh cap", kind)
			}
		})
	}
}
func TestCandidateCapacityMissingCutStillRenewsEmptySweep(t *testing.T) {
	o := capacityOwner(t, candidateCapacityHint{})
	source, _ := provenFixture(t, 10, "capacity-source", 0)
	newcomer, _ := provenFixture(t, 30, "capacity-newcomer", 0)
	target, _ := provenFixture(t, 20, "capacity-target", 0)
	at := Instant(599 * time.Second)
	r, err := o.offer(source, target, request(1, target.Identity(), 1), candidateView(t, o, at, []Session{source}, []Session{source}, target), at)
	if err != nil {
		t.Fatal(err)
	}
	seal := at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal); err != nil || result.reserved != 1 {
		t.Fatal(result, err)
	}
	if _, err := o.cancel(r, seal); err != nil {
		t.Fatal(err)
	}
	at = candidateCapacityWindow
	if _, err := o.offer(source, target, request(2, target.Identity(), 2), candidateView(t, o, at, []Session{source}, []Session{source}, target), at); err != nil {
		t.Fatal(err)
	}
	seal = at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, []Session{source}, []Session{source}, target), seal); err != nil || result.refused != 1 {
		t.Fatal("missing cut round did not close", result, err)
	}
	at = seal + Instant(100*time.Millisecond)
	live := []Session{source, newcomer}
	if _, err := o.offer(newcomer, target, request(3, target.Identity(), 3), candidateView(t, o, at, []Session{newcomer}, live, target), at); !errors.Is(err, ErrNotReady) {
		t.Fatal("new protected session bypassed genuine empty round", err)
	}
	seal = at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, nil, live, target), seal); err != nil || result.reserved != 0 || result.refused != 0 {
		t.Fatal("empty boundary round failed", result, err)
	}
	if o.watermark != newcomer.ConnID() || o.open {
		t.Fatal("refused capacity blocked empty sweep renewal")
	}
	at = seal + Instant(100*time.Millisecond)
	if _, err := o.offer(newcomer, target, request(4, target.Identity(), 4), candidateView(t, o, at, []Session{newcomer}, live, target), at); err != nil {
		t.Fatal(err)
	}
	seal = at + Instant(100*time.Millisecond)
	if result, err := o.sealRound(candidateView(t, o, seal, []Session{newcomer}, live, target), seal, capacityCut(o, seal, 0)); err != nil || result.reserved != 1 {
		t.Fatal("newcomer not admitted after genuine empty sweep and complete cut", result, err)
	}
}
