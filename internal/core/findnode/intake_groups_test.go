package findnode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func candidateTarget(number int) domain.PeerIdentity {
	var id domain.PeerIdentity
	binary.BigEndian.PutUint64(id[len(id)-8:], uint64(number+1))
	return id
}
func groupCommit(t *testing.T, g *candidateGroups, group domain.SourceGroup, target domain.PeerIdentity, at Instant, cap int) {
	t.Helper()
	p, e := g.plan(group, target, at, cap)
	if e != nil {
		t.Fatal(e)
	}
	g.commit(p, at)
	g.release(p.key)
}
func TestCandidateGroupsHardBoundOrdinaryPartialLedgerCommits(t *testing.T) {
	g := newCandidateGroups()
	count := 0
	for cell, size := range []int{3840, 1920, 1280, 912} {
		for target := range size {
			groupCommit(t, &g, domain.SourceGroup(fmt.Sprintf("g%d", cell)), candidateTarget(target), Instant(count)*Instant(time.Second)/32, 3840)
			count++
		}
	}
	if fixedGroupRecords(&g) != candidateMaxRecords {
		t.Fatalf("hard reach=%d", fixedGroupRecords(&g))
	}
	oldNames := fixedGroupNames(&g)
	if _, err := g.plan("g3", candidateTarget(912), Instant(300*time.Second), 3840); !errors.Is(err, ErrCapacity) {
		t.Fatal("hard H admitted below share")
	}
	if fixedGroupNames(&g) != oldNames || fixedGroupRecords(&g) != 7952 {
		t.Fatal("refusal effects")
	}
	groupCommit(t, &g, "g0", candidateTarget(0), Instant(300*time.Second), 3840)
	if fixedGroupRecords(&g) != 7952 {
		t.Fatal("repeat grew records")
	}
	// This exercises the real ledger but not absent source/pair/degree gates.
}
func TestCandidateStickyFirst256SustainedAndLateOverflowCompetition(t *testing.T) {
	o := candidateTestOwner(t)
	for i := range 256 {
		groupCommit(t, &o.groups, domain.SourceGroup(fmt.Sprintf("named%d", i)), candidateTarget(i), 0, 512)
	}
	for target := range 16 {
		groupCommit(t, &o.groups, "overflow-early", candidateTarget(1000+target), 0, 512)
	}
	for at := Instant(10 * time.Second); at <= Instant(40*time.Second); at += Instant(10 * time.Second) {
		for i := range 256 {
			groupCommit(t, &o.groups, domain.SourceGroup(fmt.Sprintf("named%d", i)), candidateTarget(i), at, 512)
		}
	}
	if fixedGroupNames(&o.groups) != 256 || o.groups.prospective("overflow-late") != 256 {
		t.Fatal("registry promoted overflow/changed names")
	}
	a, _ := provenFixture(t, 1, "overflow-late", 0)
	b, _ := provenFixture(t, 2, "another-late", 0)
	target, _ := provenFixture(t, 3, "target", 0)
	now := Instant(40 * time.Second)
	sources := []Session{a, b}
	view, err := captureCandidateMembership(o, []domain.PeerIdentity{target.Identity()}, sources, sources, now, func(in SessionInfo) (SessionClass, error) {
		if in.ConnID == 1 {
			return ClassSelected, nil
		}
		return ClassIncomingPinned, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if p, e := view.priority(o, a, target.Identity(), now); e != nil || p != PriorityProtected {
		t.Fatal("L1 lost in overflow")
	}
	if p, e := view.priority(o, b, target.Identity(), now); e != nil || p != PriorityAged {
		t.Fatal("late L2 lost session priority")
	}
	for i, s := range sources {
		if _, err = o.offer(s, target, request(domain.FindNodeRequestID(i), target.Identity(), byte(i)), view, now); err != nil {
			t.Fatal(err)
		}
	}
	cut := now + o.profile.round
	view, err = captureCandidateMembership(o, []domain.PeerIdentity{target.Identity()}, sources, sources, cut, func(in SessionInfo) (SessionClass, error) {
		if in.ConnID == 1 {
			return ClassSelected, nil
		}
		return ClassIncomingPinned, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := o.sealRound(view, cut)
	if err != nil || result.reserved != 0 || result.refused != 2 {
		t.Fatalf("invented independent overflow floor %+v/%v", result, err)
	}
	if o.snapshot().names != 256 || o.snapshot().calendars != 0 {
		t.Fatal("failed late requests published name/calendar")
	}
}
func TestCandidateGroupsRefusalRefsExpiryAndSingleGeneration(t *testing.T) {
	g := newCandidateGroups()
	p, e := g.plan("g", candidateTarget(0), 0, 512)
	if e != nil {
		t.Fatal(e)
	}
	g.commit(p, 0)
	g.expire(candidateGroupWindow)
	if g.reset(candidateGroupWindow, 0) {
		t.Fatal("reset live reference")
	}
	g.release(p.key)
	g.expire(candidateGroupWindow)
	generation := g.generation
	if !g.reset(candidateGroupWindow, 0) || g.generation != generation+1 || fixedGroupNames(&g) != 0 {
		t.Fatal("reset failed")
	}
	next, e := g.plan("other", candidateTarget(0), candidateGroupWindow, 512)
	if e != nil {
		t.Fatal(e)
	}
	if next.cell != 0 || !next.name {
		t.Fatal("old cell grant substituted for rederivation")
	}
}
func TestCandidatePhysicalRowsAllPhasesAndCanonicalIdentity(t *testing.T) {
	o := candidateTestOwner(t)
	sources := make([]Session, 32)
	targets := make([]Session, 128)
	for i := range sources {
		sources[i], _ = provenFixture(t, domain.ConnID(i+1), "shared", 0)
	}
	for i := range targets {
		targets[i], _ = provenFixture(t, domain.ConnID(i+1000), "target", 0)
	}
	handles := make([]candidateReservation, 0, 512)
	// Each source visits16 distinct identities; every target still owns4 rows.
	// The old stride32 revisited only4 pairs four times inside30s.
	for round := range 16 {
		at := Instant(round) * Instant(100*time.Millisecond)
		view := candidateView(t, o, at, sources, sources, targets...)
		for i, source := range sources {
			target := targets[(round*8+i)%len(targets)]
			h, err := o.offer(source, target, request(domain.FindNodeRequestID(round), target.Identity(), byte(round)), view, at)
			if err != nil {
				t.Fatalf("round%d/source%d: %v", round, i, err)
			}
			handles = append(handles, h)
		}
		if s := o.snapshot(); s.pending != len(handles) || s.provisional != 32 {
			t.Fatalf("physical rows %+v", s)
		}
		cut := at + o.profile.round
		result, err := o.sealRound(candidateView(t, o, cut, sources, sources, targets...), cut)
		if err != nil || result.reserved != 32 {
			t.Fatalf("round%d %+v/%v", round, result, err)
		}
	}
	if s := o.snapshot(); s.pending != 512 || s.aliases != 512 || s.calendars != 128 {
		t.Fatalf("physical full %+v", s)
	}
	at := Instant(1600 * time.Millisecond)
	if _, err := o.offer(sources[0], targets[0], request(99, targets[0].Identity(), 99), candidateView(t, o, at, sources, sources, targets...), at); !errors.Is(err, ErrCapacity) {
		t.Fatal("source16/node512 bypass")
	}
	if ok, err := o.cancel(handles[0], at); err != nil || !ok {
		t.Fatal("cancel")
	}
	if o.snapshot().pending != 511 || o.snapshot().aliases != 511 {
		t.Fatal("alias/row release mismatch")
	}
}
func TestCandidateTarget16ProvisionalAndCalendarDebtRetirement(t *testing.T) {
	o := candidateTestOwner(t)
	target, _ := provenFixture(t, 100, "target", 0)
	sources := make([]Session, 17)
	for i := range sources {
		sources[i], _ = provenFixture(t, domain.ConnID(i+1), "shared", 0)
	}
	view := candidateView(t, o, 0, sources, sources, target)
	for i, s := range sources {
		_, err := o.offer(s, target, request(domain.FindNodeRequestID(i), target.Identity(), byte(i)), view, 0)
		if (i < 16 && err != nil) || (i == 16 && !errors.Is(err, ErrCapacity)) {
			t.Fatalf("target16/%d: %v", i, err)
		}
	}
	debt := candidateTestOwner(t)
	// Explicit bounded registry fixture with no rows but nonrefundable debt.
	for i := range MaxPendingTotal {
		c, err := newOwnedForwardCalendar(candidateTarget(i), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		calendarFixedSeed(t, debt, c)
	}
	source := sources[0]
	if _, err := debt.offer(source, target, request(1, target.Identity(), 1), candidateView(t, debt, 0, []Session{source}, []Session{source}, target), 0); err != nil {
		t.Fatal(err)
	}
	cut := debt.profile.round
	out, err := debt.sealRound(candidateView(t, debt, cut, []Session{source}, []Session{source}, target), cut)
	if err != nil || out.reserved != 0 || calendarFixedCount(t, debt) != 512 {
		t.Fatalf("minted fresh calendar %+v/%v", out, err)
	}
	// After genuine refill, an unused canonical entry may retire.
	at := Instant(5 * time.Second)
	if _, err = debt.offer(source, target, request(2, target.Identity(), 2), candidateView(t, debt, at, []Session{source}, []Session{source}, target), at); err != nil {
		t.Fatal(err)
	}
	cut = at + debt.profile.round
	out, err = debt.sealRound(candidateView(t, debt, cut, []Session{source}, []Session{source}, target), cut)
	if err != nil || out.reserved != 1 || calendarFixedCount(t, debt) != 512 {
		t.Fatalf("honest retirement %+v/%v", out, err)
	}
}
