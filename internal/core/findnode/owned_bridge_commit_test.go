package findnode

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type ownedFinalFixture struct {
	domain      findnodebridge.FixtureDomain
	clock       *atomic.Int64
	stop        func()
	intake      DormantIntake
	cut         OwnedCut
	attempt     ownedAttempt
	r           candidateReservation
	members     []Session
	q, received Instant
}

// Proofs/roles are genuine. ConnID/opened, classes, receive times and profile
// are PRIVATE LOGICAL fixture inputs, not current Node provenance or G2.
func ownedFinalNew(t *testing.T, received, q Instant, boundary time.Duration, initialTokens ...uint32) ownedFinalFixture {
	t.Helper()
	d, clock, stop := ownedOriginalDomain(t)
	profile := ownedBridgeProfile()
	profile.Round, profile.Slack = 100*time.Millisecond, 300*time.Millisecond
	if len(initialTokens) > 1 {
		t.Fatal("one explicit fixture token profile")
	}
	if len(initialTokens) == 1 {
		profile.InitialTokens = initialTokens[0]
	}
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), profile, 3)
	if err != nil {
		t.Fatal(err)
	}
	members := []Session{ownedOriginalSession(t, 1, 0), ownedOriginalSession(t, 2, 0), ownedOriginalSession(t, 3, 0)}
	o := intake.dormantOwner.coordinator
	view := candidateView(t, o, received, members[:1], members, members[1])
	r, err := o.offer(members[0], members[1], protocol.FindNodeRequest{ID: 7, Target: members[1].Identity()}, view, received)
	if err != nil {
		t.Fatal("original logical offer", err)
	}
	clock.Store(int64(q))
	stamp, at := ownedBridgePoint(t, d)
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, boundary), []ObservedMember{{members[0], ClassSelected}, {members[1], ClassSelected}, {members[2], ClassSelected}}, ObservedDegree{})
	if err != nil {
		t.Fatal("original sealed cut", err)
	}
	attempt, err := intake.tryOwnedAttempt(cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.projectOwnedMembership([]domain.PeerIdentity{members[1].Identity()}, members[:1]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attempt.releaseOwnedAttempt() })
	return ownedFinalFixture{d, clock, stop, intake, cut, attempt, r, members, q, received}
}

// Reuse the frozen full accounting reader, excluding only documented time/
// capacity housekeeping and diagnostic counters. Keep every admission field.
func ownedFinalAdmissionState(t *testing.T, f ownedFinalFixture) any {
	t.Helper()
	all, ok := calendarFixedAccounting(f.intake.dormantOwner.coordinator, f.members).([]any)
	if !ok || len(all) != 24 {
		t.Fatal("full accounting fixture schema changed")
	}
	out := append([]any(nil), all[:7]...)
	out = append(out, all[8:13]...)
	out = append(out, all[15:]...)
	return out
}

func ownedFinalNoCharge(t *testing.T, f ownedFinalFixture, before any, committed uint64) {
	t.Helper()
	o := f.intake.dormantOwner.coordinator
	if !reflect.DeepEqual(before, ownedFinalAdmissionState(t, f)) || o.snapshot().committed != committed {
		t.Fatal("refused final candidate changed admission/debt/ID/original pointers")
	}
	if f.attempt.claim.phase.Load() != ownedAttemptLive {
		t.Fatal("refusal lost same LIVE claim")
	}
}

func TestOwnedFinalCandidateSameOwnerCommitAndRelease(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
	o := f.intake.dormantOwner.coordinator
	at := Instant(10150 * time.Millisecond)
	f.clock.Store(int64(at))
	if err := f.attempt.finalizeOwnedCandidate(f.r); err != nil {
		t.Fatal("original final joint admission", err)
	}
	entry := f.r.entry
	w := f.members[0].state.sourceWindows.Load()
	if entry.intake.phase != candidateReserved || entry.intake.alias == nil || entry.intake.alias.at < at || entry.forwarded.ID != 0 || f.members[1].state.nextID != 1 || w == nil || w.owner != o.sourceOwner || entry.intake.source.windows != w || o.snapshot().committed != 1 {
		t.Fatal("joint commit did not retain original source/calendar/ID/correlation")
	}
	calendar := entry.intake.calendar
	if calendar == nil || calendar != calendarFixedRead(t, o, f.members[1].Identity()) || calendar.Pending() != 1 {
		t.Fatal("joint commit replaced original calendar or omitted fixed alias")
	}
	lease, err := f.domain.Consumer().TryReadClock(f.cut.sealedCut.stamp)
	if err != nil {
		t.Fatal("final reservation was not released", err)
	}
	lease.Release()
	before := ownedFinalAdmissionState(t, f)
	if err := f.attempt.finalizeOwnedCandidate(f.r); !errors.Is(err, ErrStaleWork) {
		t.Fatal("same committed row replay accepted", err)
	}
	ownedFinalNoCharge(t, f, before, 1)
	debt := calendar.projected
	if ended, err := o.cancel(f.r, at); err != nil || !ended {
		t.Fatal("same original cancel", ended, err)
	}
	if calendar.projected != debt || f.members[0].state.sourceWindows.Load() != w || calendar.Pending() != 0 || o.snapshot().pending != 0 {
		t.Fatal("cancel reset debt/book/source or retained physical row")
	}
}

func TestOwnedFinalCandidateTemporalAndIdentityRefusals(t *testing.T) {
	for _, name := range []string{"bound-equality", "late-slack", "before-round", "stale-row-generation", "foreign-source-owner", "id-exhausted", "cancelled-domain", "clock-regression"} {
		t.Run(name, func(t *testing.T) {
			q := Instant(10100 * time.Millisecond)
			boundary := 10 * time.Second
			if name == "before-round" {
				q = Instant(10050 * time.Millisecond)
			}
			if name == "bound-equality" {
				boundary = 50 * time.Millisecond
			}
			f := ownedFinalNew(t, Instant(10*time.Second), q, boundary)
			o := f.intake.dormantOwner.coordinator
			at := Instant(10150 * time.Millisecond)
			var want error
			switch name {
			case "bound-equality":
				want = ErrStaleWork
			case "late-slack":
				at = Instant(10401 * time.Millisecond)
				want = ErrBudget
			case "before-round":
				at = Instant(10075 * time.Millisecond)
				want = ErrNotReady
			case "stale-row-generation":
				f.r.generation++
				want = ErrStaleWork
			case "foreign-source-owner":
				f.members[0].state.sourceWindows.Store(&candidateSourceWindows{owner: &candidateSourceOwnerToken{}})
				want = ErrStaleWork
			case "id-exhausted":
				f.members[1].state.mu.Lock()
				f.members[1].state.exhausted = true
				f.members[1].state.mu.Unlock()
				want = ErrStaleWork
			case "cancelled-domain":
				f.stop()
			case "clock-regression":
				at = q - 1
			}
			f.clock.Store(int64(at))
			before := ownedFinalAdmissionState(t, f)
			committed := o.snapshot().committed
			err := f.attempt.finalizeOwnedCandidate(f.r)
			if err == nil || (want != nil && !errors.Is(err, want)) {
				t.Fatal("final refusal reached wrong gate or accepted", name, err)
			}
			ownedFinalNoCharge(t, f, before, committed)
		})
	}
}

func TestOwnedFinalCandidateEveryOriginalSamplePrecedesClosedRefusal(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
	f.members[2].End() // Neither offered nor target nor selected oldest-four.
	before := ownedFinalAdmissionState(t, f)
	f.clock.Store(int64(10150 * time.Millisecond))
	if err := f.attempt.finalizeOwnedCandidate(f.r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("excluded original closure was ignored", err)
	}
	ownedFinalNoCharge(t, f, before, 0)
	// Independent HWM witness: Sample must have consumed 10.15 before refusal.
	f.clock.Store(int64(10125 * time.Millisecond))
	clock, err := f.domain.Observer().ReserveClock()
	if err != nil {
		t.Fatal("closed Session must not itself poison clock", err)
	}
	_, err = clock.Sample()
	clock.Release()
	if err == nil {
		t.Fatal("all-original refusal happened before actual Sample/HWM")
	}
}

func TestOwnedFinalCandidateCapacityFreezeIsObservationNotAdmission(t *testing.T) {
	for _, name := range []string{"valid-id-refusal", "invalid-original", "foreign-source-owner"} {
		t.Run(name, func(t *testing.T) {
			f := ownedFinalNew(t, Instant(600*time.Second), Instant(600100*time.Millisecond), 10*time.Second)
			o := f.intake.dormantOwner.coordinator
			f.members[1].state.mu.Lock()
			f.members[1].state.exhausted = true
			f.members[1].state.mu.Unlock()
			if name == "invalid-original" {
				f.members[2].End()
			}
			if name == "foreign-source-owner" {
				f.members[0].state.sourceWindows.Store(&candidateSourceWindows{owner: &candidateSourceOwnerToken{}})
			}
			before := ownedFinalAdmissionState(t, f)
			f.clock.Store(int64(600150 * time.Millisecond))
			err := f.attempt.finalizeOwnedCandidate(f.r)
			if name == "invalid-original" && !errors.Is(err, ErrUnauthenticated) || name != "invalid-original" && !errors.Is(err, ErrStaleWork) {
				t.Fatal("capacity observation refusal gate", err)
			}
			ownedFinalNoCharge(t, f, before, 0)
			wantEpoch := uint64(1)
			if name != "valid-id-refusal" {
				wantEpoch = 0
			}
			if o.capacity.epoch != wantEpoch || o.capacity.cap != 512 {
				t.Fatal("capacity freeze used invalid evidence or required successful admission")
			}
		})
	}
}

func TestOwnedFinalCandidateFreshAccountingStartsAtFinalSample(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second, 0)
	at := Instant(10150 * time.Millisecond)
	f.clock.Store(int64(at))
	if err := f.attempt.finalizeOwnedCandidate(f.r); err != nil {
		t.Fatal("zero-initial-token final trial", err)
	}
	calendar := f.r.entry.intake.calendar
	if f.r.entry.intake.alias.at != at+forwardToken || calendar.actual.at != at || calendar.actual.tokens != 0 {
		t.Fatal("unpublished calendar earned preparatory q-to-f refill credit")
	}
}

func TestOwnedFinalCandidateSameCutCanServeLaterCandidate(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
	o := f.intake.dormantOwner.coordinator
	view := candidateView(t, o, f.received, f.members[2:], f.members, f.members[1])
	other, err := o.offer(f.members[2], f.members[1], protocol.FindNodeRequest{ID: 8, Target: f.members[1].Identity()}, view, f.received)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.attempt.releaseOwnedAttempt(); err != nil {
		t.Fatal(err)
	}
	f.attempt, err = f.intake.tryOwnedAttempt(f.cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.attempt.projectOwnedMembership([]domain.PeerIdentity{f.members[1].Identity()}, []Session{f.members[0], f.members[2]}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.attempt.releaseOwnedAttempt() })
	f.clock.Store(int64(10150 * time.Millisecond))
	if err := f.attempt.finalizeOwnedCandidate(f.r); err != nil {
		t.Fatal("first candidate from original cut", err)
	}
	first := f.r.entry.intake.calendar
	f.clock.Store(int64(10200 * time.Millisecond))
	if err := f.attempt.finalizeOwnedCandidate(other); err != nil {
		t.Fatal("later candidate backdated cleanup or replaced original cut", err)
	}
	if other.entry.intake.calendar != first || f.r.entry.intake.phase != candidateReserved || other.entry.intake.phase != candidateReserved || o.snapshot().committed != 2 || f.members[1].state.nextID != 2 {
		t.Fatal("later candidate reset earlier accepted calendar/debt/ID")
	}
}

func TestOwnedFinalCandidateRetiresOnlyOriginalSafeCalendar(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
	o := f.intake.dormantOwner.coordinator
	calendarFixedFill(t, o, MaxPendingTotal)
	old := o.calendars[0]
	oldTarget := old.target
	oldState := calendarOldRead(old)
	f.clock.Store(int64(10150 * time.Millisecond))
	if err := f.attempt.finalizeOwnedCandidate(f.r); err != nil {
		t.Fatal("safe original donor final commit", err)
	}
	if o.calendars[0] != f.r.entry.intake.calendar || o.calendars[0] == old || old.target != oldTarget || !reflect.DeepEqual(oldState, calendarOldRead(old)) || calendarFixedCount(t, o) != MaxPendingTotal {
		t.Fatal("retirement retagged/reset old object or grew physical table")
	}
}

func TestOwnedFinalCandidateRetainsExistingCorrelationAndDebt(t *testing.T) {
	f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
	o := f.intake.dormantOwner.coordinator
	view := candidateView(t, o, f.received, f.members[2:], f.members, f.members[1])
	other, err := o.offer(f.members[2], f.members[1], protocol.FindNodeRequest{ID: 8, Target: f.members[1].Identity()}, view, f.received)
	if err != nil {
		t.Fatal(err)
	}
	// Real old joint commit, explicitly logical-time fixture, not a forged row.
	if err := calendarFixedCommitDirect(o, other, f.q, &candidateSourceWindows{owner: o.sourceOwner}); err != nil {
		t.Fatal("old accepted fixture", err)
	}
	f.members[1].state.mu.Lock()
	f.members[1].state.nextID = other.entry.forwarded.ID
	f.members[1].state.mu.Unlock()
	before := ownedFinalAdmissionState(t, f)
	f.clock.Store(int64(10150 * time.Millisecond))
	if err := f.attempt.finalizeOwnedCandidate(f.r); !errors.Is(err, ErrDuplicateID) {
		t.Fatal("existing target correlation not preserved", err)
	}
	ownedFinalNoCharge(t, f, before, 0)
	if other.entry.intake.phase != candidateReserved || other.entry.intake.alias == nil {
		t.Fatal("refusal rolled back earlier accepted row")
	}
}
