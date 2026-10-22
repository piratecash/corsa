package findnode

import (
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TLS proofs and tagged FixtureDomain are genuine. Receive/profile/classes are
// explicit private logical inputs, never production receive/Ready provenance.
func ownedRoundExecNew(t *testing.T) ownedFinalFixture {
	t.Helper()
	d, clock, stop := ownedOriginalDomain(t)
	profile := ownedBridgeProfile()
	profile.Round, profile.Slack = 100*time.Millisecond, 300*time.Millisecond
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), profile, 3)
	if err != nil {
		t.Fatal(err)
	}
	members := []Session{ownedOriginalSession(t, 1, 0), ownedOriginalSession(t, 2, 0), ownedOriginalSession(t, 3, 0)}
	received, q := Instant(10*time.Second), Instant(10100*time.Millisecond)
	o := intake.dormantOwner.coordinator
	view, err := captureCandidateMembership(o, []domain.PeerIdentity{members[1].Identity()}, members[:1], members, received, func(SessionInfo) (SessionClass, error) { return ClassOrdinary, nil })
	if err != nil {
		t.Fatal(err)
	}
	r, err := o.offer(members[0], members[1], request(7, members[1].Identity(), 1), view, received)
	if err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(q))
	stamp, at := ownedBridgePoint(t, d)
	facts := []ObservedMember{{members[0], ClassOrdinary}, {members[1], ClassOrdinary}, {members[2], ClassOrdinary}}
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 10*time.Second), facts, ObservedDegree{})
	if err != nil {
		t.Fatal(err)
	}
	attempt := ownedScratchBorrow(t, intake, cut)
	if err := attempt.projectOwnedMembership([]domain.PeerIdentity{members[1].Identity()}, members[:1]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attempt.releaseOwnedAttempt() })
	return ownedFinalFixture{d, clock, stop, intake, cut, attempt, r, members, q, received}
}

func TestOwnedRoundExecuteSuccessReceiptAndSameLockProgress(t *testing.T) {
	f := ownedRoundExecNew(t)
	o := f.intake.dormantOwner.coordinator
	ticket, err := f.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	plan := ticket.claim.roundPlan
	cell := plan.rows[plan.order[0]].cell
	f.clock.Store(int64(10150 * time.Millisecond))
	result, err := f.attempt.consumeOwnedRoundPlan(ticket)
	if err != nil || result != (candidateRoundResult{reserved: 1}) || o.open || o.rrSession[cell] != f.r.entry.source.ConnID() || o.rrCell != (cell+1)%candidateGroupCells || f.r.entry.intake.phase != candidateReserved {
		t.Fatal("round did not publish exact successful frozen-cell progress", result, err)
	}
	if f.attempt.claim.phase.Load() != ownedAttemptLive || !plan.consumed {
		t.Fatal("execution did not restore LIVE separately from consumed ticket")
	}
	before := ownedFinalAdmissionState(t, f)
	if _, err := f.attempt.consumeOwnedRoundPlan(ticket); !errors.Is(err, ErrStaleWork) || !reflect.DeepEqual(before, ownedFinalAdmissionState(t, f)) {
		t.Fatal("consumed success ticket replay", err)
	}
	lease, err := f.domain.Consumer().TryReadClock(f.cut.sealedCut.stamp)
	if err != nil {
		t.Fatal("per-candidate R not released", err)
	}
	lease.Release()
}

func TestOwnedRoundExecuteStaleSnapshotBeforeReservation(t *testing.T) {
	for _, name := range []string{"generation", "round", "cursor", "RR", "raw-priority", "request", "row-generation"} {
		t.Run(name, func(t *testing.T) {
			f := ownedRoundExecNew(t)
			o := f.intake.dormantOwner.coordinator
			ticket, err := f.attempt.prepareOwnedRoundPlan()
			if err != nil {
				t.Fatal(err)
			}
			o.mu.Lock()
			o.pending.mu.Lock()
			switch name {
			case "generation":
				o.generation++
			case "round":
				o.round++
			case "cursor":
				o.cursor++
			case "RR":
				o.rrSession[0]++
			case "raw-priority":
				f.r.entry.intake.priority = PriorityProtected
			case "request":
				f.r.entry.request.Challenge[0]++
			case "row-generation":
				f.r.entry.intake.generation++
			}
			o.pending.mu.Unlock()
			o.mu.Unlock()
			f.clock.Store(int64(10150 * time.Millisecond))
			if _, err := f.attempt.consumeOwnedRoundPlan(ticket); !errors.Is(err, ErrStaleWork) {
				t.Fatal("stale snapshot reached admission", name, err)
			}
			if !o.open || o.snapshot().committed != 0 || f.attempt.claim.phase.Load() != ownedAttemptLive {
				t.Fatal("stale cleanup closed changed round, charged or retained BUSY")
			}
			// Independent high-water witness: 10.15 must not have been Sampled. 10.125
			// remains after original sealed q=10.1 and succeeds only without that Sample.
			f.clock.Store(int64(10125 * time.Millisecond))
			clock, err := f.domain.Observer().ReserveClock()
			if err != nil {
				t.Fatal(err)
			}
			_, err = clock.Sample()
			clock.Release()
			if err != nil {
				t.Fatal("stale snapshot acquired/sampled R before refusal", err)
			}
		})
	}
}

func TestOwnedRoundExecuteInterleavingNewOriginalSurvives(t *testing.T) {
	f := ownedRoundExecNew(t)
	o := f.intake.dormantOwner.coordinator
	ticket, err := f.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	view, err := captureCandidateMembership(o, []domain.PeerIdentity{f.members[1].Identity()}, f.members[2:], f.members, f.received, func(SessionInfo) (SessionClass, error) { return ClassOrdinary, nil })
	if err != nil {
		t.Fatal(err)
	}
	other, err := o.offer(f.members[2], f.members[1], request(8, f.members[1].Identity(), 1), view, f.received)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(10150 * time.Millisecond))
	if _, err := f.attempt.consumeOwnedRoundPlan(ticket); !errors.Is(err, ErrStaleWork) {
		t.Fatal(err)
	}
	o.mu.Lock()
	o.pending.mu.Lock()
	oldCurrent, newCurrent := o.currentLocked(f.r), o.currentLocked(other)
	open := o.open
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if oldCurrent || !newCurrent || !open || other.entry.intake.phase != candidateProvisional {
		t.Fatal("cleanup acquired new row/generation or closed foreign work")
	}
}

func TestOwnedRoundExecuteReceiptRejectsWrongExecutionAndTuple(t *testing.T) {
	// Values alone are not admission proof: the receiver requires exact execution,
	// ordinal, original reservation/generation and the stored successful receipt.
	e := &ownedRoundExecution{plan: &ownedRoundPlanState{count: 1, round: 7}}
	e.plan.order[0] = 0
	e.done[0] = true
	e.receipt = ownedRoundSuccessReceipt{execution: e, round: 7}
	if err := acceptOwnedRoundReceipt(e, e.receipt); err != nil {
		t.Fatal("exact private value fixture", err)
	}
	for _, name := range []string{"zero", "execution", "ordinal", "generation", "round", "forwarded"} {
		t.Run(name, func(t *testing.T) {
			got := e.receipt
			switch name {
			case "zero":
				got = ownedRoundSuccessReceipt{}
			case "execution":
				got.execution = &ownedRoundExecution{}
			case "ordinal":
				got.ordinal = 1
			case "generation":
				got.reservation.generation++
			case "round":
				got.round++
			case "forwarded":
				got.forwarded++
			}
			if err := acceptOwnedRoundReceipt(e, got); !errors.Is(err, ErrStaleWork) {
				t.Fatal("forged/stale receipt value accepted", name, err)
			}
		})
	}
}

func TestOwnedRoundExecuteRefusalPreservesAcceptedDebtAndWriter(t *testing.T) {
	for _, phase := range []string{"reserved", "dispatching", "awaiting", "terminal-writer"} {
		t.Run(phase, func(t *testing.T) {
			f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
			o := f.intake.dormantOwner.coordinator
			view := candidateView(t, o, f.received, f.members[2:], f.members, f.members[1])
			other, err := o.offer(f.members[2], f.members[1], request(8, f.members[1].Identity(), 1), view, f.received)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.attempt.releaseOwnedAttempt(); err != nil {
				t.Fatal(err)
			}
			f.attempt = ownedScratchBorrow(t, f.intake, f.cut)
			if err := f.attempt.projectOwnedMembership([]domain.PeerIdentity{f.members[1].Identity()}, []Session{f.members[0], f.members[2]}); err != nil {
				t.Fatal(err)
			}
			f.clock.Store(int64(10150 * time.Millisecond))
			if err := f.attempt.finalizeOwnedCandidate(f.r); err != nil {
				t.Fatal(err)
			}
			if phase != "reserved" {
				permit, _, e := o.takeForward(f.r, Instant(10150*time.Millisecond))
				if e != nil {
					t.Fatal("original writer take", e)
				}
				if phase == "awaiting" {
					if e := o.finishForward(permit, Instant(10150*time.Millisecond), candidateWriteFinished); e != nil {
						t.Fatal("original writer finish", e)
					}
				}
				if phase == "terminal-writer" {
					if ok, e := o.cancel(f.r, Instant(10150*time.Millisecond)); !ok || e != nil {
						t.Fatal("original outstanding writer terminal", e)
					}
				}
			}
			acceptedPhase := f.r.entry.intake.phase
			ticket, err := f.attempt.prepareOwnedRoundPlan()
			if err != nil {
				t.Fatal(err)
			}
			calendar := f.r.entry.intake.calendar
			debt := calendar.projected
			alias := f.r.entry.intake.alias
			source := f.r.entry.intake.source
			forwarded := f.r.entry.forwarded
			nextID := f.members[1].state.nextID
			// Exhaustion is an independent real target-ID gate after final Sample. It
			// refuses remaining provisional work without touching the accepted original.
			f.members[1].state.mu.Lock()
			f.members[1].state.exhausted = true
			f.members[1].state.mu.Unlock()
			f.clock.Store(int64(10200 * time.Millisecond))
			if _, err := f.attempt.consumeOwnedRoundPlan(ticket); !errors.Is(err, ErrStaleWork) {
				t.Fatal("target exhaustion gate", err)
			}
			if f.r.entry.intake.phase != acceptedPhase || f.r.entry.intake.alias != alias || f.r.entry.intake.source != source || f.r.entry.forwarded != forwarded || calendar.projected != debt || f.members[1].state.nextID != nextID {
				t.Fatal("refusal rolled back accepted debt/correlation/obligation")
			}
			o.mu.Lock()
			o.pending.mu.Lock()
			accepted, provisional := o.currentLocked(f.r), o.currentLocked(other)
			o.pending.mu.Unlock()
			o.mu.Unlock()
			if !accepted || provisional || f.attempt.claim.phase.Load() != ownedAttemptLive {
				t.Fatal("refusal lost accepted row or left provisional/BUSY")
			}
		})
	}
}

func TestOwnedRoundExecuteOriginalBusyExcludesCopiedRelease(t *testing.T) {
	f := ownedRoundExecNew(t)
	o := f.intake.dormantOwner.coordinator
	ticket, err := f.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Store(int64(10150 * time.Millisecond))
	o.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := f.attempt.consumeOwnedRoundPlan(ticket); done <- err }()
	deadline := time.After(5 * time.Second)
	timedOut := false
	for f.attempt.claim.phase.Load() != ownedAttemptBusy {
		select {
		case <-deadline:
			timedOut = true
		default:
		}
		if timedOut {
			break
		}
		runtime.Gosched()
	}
	var releaseErr, projectErr error
	if !timedOut {
		releaseErr = f.attempt.releaseOwnedAttempt()
		projectErr = f.attempt.projectOwnedMembership(nil, nil)
	}
	o.mu.Unlock()
	err = <-done
	if timedOut || err != nil || !errors.Is(releaseErr, ErrNotReady) || !errors.Is(projectErr, ErrNotReady) || f.attempt.claim.phase.Load() != ownedAttemptLive {
		t.Fatal("original BUSY lane was released/reprojected or not restored", err, releaseErr, projectErr)
	}
}

func TestOwnedRoundExecuteProtectedCursorAndEmpty(t *testing.T) {
	t.Run("protected-success", func(t *testing.T) {
		f := ownedFinalNew(t, Instant(10*time.Second), Instant(10100*time.Millisecond), 10*time.Second)
		ticket, err := f.attempt.prepareOwnedRoundPlan()
		if err != nil {
			t.Fatal(err)
		}
		f.clock.Store(int64(10150 * time.Millisecond))
		result, err := f.attempt.consumeOwnedRoundPlan(ticket)
		o := f.intake.dormantOwner.coordinator
		if err != nil || result.reserved != 1 || o.cursor != f.r.entry.source.ConnID() || o.open {
			t.Fatal("protected successful cursor not in same successful round", result, err)
		}
	})
	t.Run("empty-open", func(t *testing.T) {
		f := ownedRoundNew(t, 0, Instant(600*time.Second))
		ticket, err := f.attempt.prepareOwnedRoundPlan()
		if err != nil {
			t.Fatal(err)
		}
		maximum := ticket.claim.roundPlan.maximum
		result, err := f.attempt.consumeOwnedRoundPlan(ticket)
		o := f.intake.dormantOwner.coordinator
		if err != nil || result != (candidateRoundResult{}) || o.open || o.cursor != 0 || o.watermark != maximum || f.attempt.claim.phase.Load() != ownedAttemptLive {
			t.Fatal("empty original round did not renew exact watermark without candidate admission", result, err)
		}
	})
}

func TestOwnedRoundExecuteForeignExecutionRefusedBeforeReservation(t *testing.T) {
	f := ownedRoundExecNew(t)
	other := ownedRoundExecNew(t)
	ticket, err := other.attempt.prepareOwnedRoundPlan()
	if err != nil {
		t.Fatal(err)
	}
	foreign := &ownedRoundExecution{claim: other.attempt.claim, plan: ticket.claim.roundPlan}
	before := ownedFinalAdmissionState(t, f)
	if !f.attempt.claim.phase.CompareAndSwap(ownedAttemptLive, ownedAttemptBusy) {
		t.Fatal("original private BUSY fixture")
	}
	f.clock.Store(int64(10150 * time.Millisecond))
	_, err = f.attempt.finalizeOwnedCandidateBusy(f.r, foreign)
	f.attempt.claim.phase.Store(ownedAttemptLive)
	if !errors.Is(err, ErrStaleWork) || !reflect.DeepEqual(before, ownedFinalAdmissionState(t, f)) {
		t.Fatal("foreign execution used original application locks/reservation", err)
	}
	f.clock.Store(int64(10125 * time.Millisecond))
	clock, err := f.domain.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	_, err = clock.Sample()
	clock.Release()
	if err != nil {
		t.Fatal("foreign execution sampled original clock before tuple refusal", err)
	}
}

func ownedRoundExecTwo(t *testing.T) (ownedFinalFixture, candidateReservation) {
	t.Helper()
	d, clock, stop := ownedOriginalDomain(t)
	profile := ownedBridgeProfile()
	profile.Round, profile.Slack = 100*time.Millisecond, 300*time.Millisecond
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), profile, 5)
	if err != nil {
		t.Fatal(err)
	}
	members := []Session{ownedOriginalSession(t, 1, 0), ownedOriginalSession(t, 2, 0), ownedOriginalSession(t, 3, 0), ownedOriginalSession(t, 4, 0), ownedOriginalSession(t, 5, 0)}
	received, q := Instant(10*time.Second), Instant(10100*time.Millisecond)
	o := intake.dormantOwner.coordinator
	targets := []domain.PeerIdentity{members[1].Identity(), members[3].Identity()}
	offered := []Session{members[0], members[2]}
	view, err := captureCandidateMembership(o, targets, offered, members, received, func(SessionInfo) (SessionClass, error) { return ClassSelected, nil })
	if err != nil {
		t.Fatal(err)
	}
	first, err := o.offer(members[0], members[1], request(7, members[1].Identity(), 1), view, received)
	if err != nil {
		t.Fatal(err)
	}
	second, err := o.offer(members[2], members[3], request(8, members[3].Identity(), 1), view, received)
	if err != nil {
		t.Fatal(err)
	}
	clock.Store(int64(q))
	stamp, at := ownedBridgePoint(t, d)
	facts := make([]ObservedMember, len(members))
	for i, s := range members {
		facts[i] = ObservedMember{s, ClassSelected}
	}
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, 10*time.Second), facts, ObservedDegree{})
	if err != nil {
		t.Fatal(err)
	}
	attempt := ownedScratchBorrow(t, intake, cut)
	if err := attempt.projectOwnedMembership(targets, offered); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attempt.releaseOwnedAttempt() })
	return ownedFinalFixture{d, clock, stop, intake, cut, attempt, first, members, q, received}, second
}

func TestOwnedRoundExecuteSameCutFreshSamplesAfterEachWait(t *testing.T) {
	for _, closeOriginal := range []bool{false, true} {
		t.Run(map[bool]string{false: "two-accepted", true: "second-checks-nonoffered-original"}[closeOriginal], func(t *testing.T) {
			f, second := ownedRoundExecTwo(t)
			o := f.intake.dormantOwner.coordinator
			ticket, err := f.attempt.prepareOwnedRoundPlan()
			if err != nil {
				t.Fatal(err)
			}
			plan := ticket.claim.roundPlan
			if plan.count != 2 || plan.rows[plan.order[0]].reservation != f.r || plan.rows[plan.order[1]].reservation != second {
				t.Fatal("directed two-original protected order fixture")
			}
			firstAt, secondAt := Instant(10150*time.Millisecond), Instant(10200*time.Millisecond)
			// Distinct original targets make this a genuine second application wait:
			// candidate1 can commit while candidate2 cannot pass its target mutex.
			second.entry.target.state.mu.Lock()
			f.clock.Store(int64(firstAt))
			type completed struct {
				result candidateRoundResult
				err    error
			}
			done := make(chan completed, 1)
			go func() { r, e := f.attempt.consumeOwnedRoundPlan(ticket); done <- completed{r, e} }()
			deadline := time.After(5 * time.Second)
			firstObserved := false
			timedOut := false
			for !firstObserved && !timedOut {
				firstObserved = o.snapshot().committed == 1
				if !firstObserved {
					select {
					case <-deadline:
						timedOut = true
					default:
					}
					runtime.Gosched()
				}
			}
			// Time advances ONLY while the second target wait is still blocked. With
			// one Sample per round or reused first f, its fresh calendar starts too early.
			f.clock.Store(int64(secondAt))
			if closeOriginal {
				f.members[4].End()
			} // not offered, target or oldest-four
			second.entry.target.state.mu.Unlock()
			var got completed
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("round driver did not finish after releasing original target wait")
			}
			if timedOut || !firstObserved {
				t.Fatal("first admission did not complete before the second application wait", got.err)
			}
			if f.r.entry.intake.phase != candidateReserved || f.r.entry.intake.alias.at != firstAt || f.r.entry.intake.calendar.actual.at != firstAt {
				t.Fatal("first candidate did not retain its own sampled f")
			}
			if closeOriginal {
				if !errors.Is(got.err, ErrUnauthenticated) || got.result != (candidateRoundResult{reserved: 1, refused: 1}) {
					t.Fatal("second candidate skipped rechecking every original after its wait", got.result, got.err)
				}
				o.mu.Lock()
				o.pending.mu.Lock()
				current := o.currentLocked(second)
				o.pending.mu.Unlock()
				o.mu.Unlock()
				if current {
					t.Fatal("refused second provisional generation survived")
				}
			} else {
				if got.err != nil || got.result != (candidateRoundResult{reserved: 2}) || second.entry.intake.phase != candidateReserved || second.entry.intake.alias.at != secondAt || second.entry.intake.calendar.actual.at != secondAt {
					t.Fatal("second candidate reused first f or Sampled before its own application wait", got.result, got.err)
				}
				if o.open {
					t.Fatal("complete two-success original round remained open")
				}
			}
			if f.attempt.claim.phase.Load() != ownedAttemptLive {
				t.Fatal("success/refusal did not restore original LIVE claim")
			}
			// Independent clock high-water witness in BOTH branches: a fresh Sample at
			// secondAt must precede even the nonoffered-original closed refusal. Reusing
			// firstAt or checking originals before Sample would permit this regression.
			f.clock.Store(int64(10175 * time.Millisecond))
			clock, e := f.domain.Observer().ReserveClock()
			if e != nil {
				t.Fatal(e)
			}
			_, e = clock.Sample()
			clock.Release()
			if e == nil {
				t.Fatal("second candidate did not Sample fresh f before all-original checks")
			}
		})
	}
}
