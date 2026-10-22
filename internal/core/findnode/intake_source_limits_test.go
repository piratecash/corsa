package findnode

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// candidateSourceOfferSeal uses genuine canonical Sessions and fresh membership
// at both operation times. Its caller chooses the explicit synthetic clock.
func candidateSourceOfferSeal(t *testing.T, o *intakeCoordinator, source, target Session, id domain.FindNodeRequestID, received Instant) (candidateReservation, candidateRoundResult) {
	t.Helper()
	sources := []Session{source}
	r, err := o.offer(source, target, request(id, target.Identity(), byte(id)), candidateView(t, o, received, sources, sources, target), received)
	if err != nil {
		t.Fatal(err)
	}
	cut := received + o.profile.round
	out, err := o.sealRound(candidateView(t, o, cut, sources, sources, target), cut)
	if err != nil {
		t.Fatal(err)
	}
	return r, out
}

func candidateSourceRow(t *testing.T, o *intakeCoordinator, r candidateReservation) candidateSourceTarget {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending.mu.Lock()
	defer o.pending.mu.Unlock()
	row, err := o.sourceRowLocked(r.entry)
	if err != nil {
		t.Fatal(err)
	}
	return *row
}

func TestCandidateSourcePairIdentityAndExactWindow(t *testing.T) {
	o := candidateTestOwner(t)
	source, sourcePeer := provenFixture(t, 10, "source", 0)
	target, remote := provenFixture(t, 20, "target", 0)
	secondLink, _ := provenFixture(t, 21, "target", 0, remote)
	var first candidateReservation
	for i := range 3 {
		at := Instant(i) * Instant(100*time.Millisecond)
		link := target
		if i == 1 {
			link = secondLink
		}
		r, out := candidateSourceOfferSeal(t, o, source, link, domain.FindNodeRequestID(i+1), at)
		if out.reserved != 1 {
			t.Fatalf("three pair commitments: %d %+v", i, out)
		}
		if i == 0 {
			first = r
		}
		permit, _, err := o.takeForward(r, at+o.profile.round)
		if err != nil {
			t.Fatal(err)
		}
		if err = o.finishForward(permit, at+o.profile.round, candidateWriteFinished); err != nil {
			t.Fatal(err)
		}
		ref := r.entry.intake.source
		row := ref.windows.rows[ref.index]
		if row.refs != 1 {
			t.Fatal("normal Finish dropped pending reference", row.refs)
		}
		if ok, err := o.cancel(r, at+o.profile.round); err != nil || !ok {
			t.Fatal("cancel", err)
		}
	}
	windows := source.state.sourceWindows.Load()
	if windows == nil || windows.rows[0].projected.count != 3 || windows.rows[0].actual.count != 3 || windows.rows[0].refs != 0 || !windows.rows[1].target.IsZero() {
		t.Fatal("target links split pair history")
	}
	before := windows.rows
	next := target.state.nextID
	_, out := candidateSourceOfferSeal(t, o, source, target, 4, Instant(300*time.Millisecond))
	if out.reserved != 0 || out.refused != 1 || windows.rows != before || target.state.nextID != next {
		t.Fatal("fourth pair admitted or charged")
	}
	// A fresh Session for the SAME proven identity has independent pair history,
	// but its higher ConnID must first wait for the old protected sweep to renew.
	fresh, _ := provenFixture(t, 30, "source", 0, sourcePeer)
	if fresh.Identity() != source.Identity() || fresh == source {
		t.Fatal("fresh source did not preserve identity with a distinct Session")
	}
	members := []Session{source, fresh}
	arrived := Instant(400 * time.Millisecond)
	if _, err := o.offer(fresh, target, request(1, target.Identity(), 1), candidateView(t, o, arrived, members, members, target), arrived); !errors.Is(err, ErrNotReady) {
		t.Fatal("new protected source bypassed old sweep", err)
	}
	if o.snapshot().pending != 0 || o.snapshot().watermark != source.ConnID() || windows.rows != before || fresh.state.sourceWindows.Load() != nil || target.state.nextID != next {
		t.Fatal("deferred newcomer charged state or advanced the watermark")
	}
	cut := arrived + o.profile.round
	empty, err := o.sealRound(candidateView(t, o, cut, members, members, target), cut)
	if err != nil || empty.reserved != 0 || empty.refused != 0 || o.snapshot().watermark != fresh.ConnID() || windows.rows != before || fresh.state.sourceWindows.Load() != nil || target.state.nextID != next {
		t.Fatal("empty fixed round did not renew membership without debit", empty, err)
	}
	r, out := candidateSourceOfferSeal(t, o, fresh, target, 1, cut)
	if out.reserved != 1 || fresh.state.sourceWindows.Load() == windows {
		t.Fatal("independent source Session inherited history before30s", out)
	}
	freshAt := cut + o.profile.round
	permit, _, err := o.takeForward(r, freshAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.finishForward(permit, freshAt, candidateWriteFinished); err != nil {
		t.Fatal(err)
	}
	if ok, err := o.cancel(r, freshAt); !ok || err != nil {
		t.Fatal("fresh source cancel", err)
	}
	freshWindows := fresh.state.sourceWindows.Load()
	if freshWindows == nil || freshWindows.rows[0].projected.count != 1 || freshWindows.rows[0].actual.count != 1 || freshWindows.rows[0].refs != 0 || windows.rows != before {
		t.Fatal("fresh Session changed original history or cancellation refunded debit")
	}
	// Exactly30s after the first projected AND actual event, both old events age out.
	r, out = candidateSourceOfferSeal(t, o, source, secondLink, 5, Instant(30*time.Second))
	if out.reserved != 1 {
		t.Fatal("30s equality refused", out)
	}
	if ok, err := o.cancel(r, Instant(30100*time.Millisecond)); !ok || err != nil {
		t.Fatal(err)
	}
	if ok, _ := o.cancel(first, Instant(30100*time.Millisecond)); ok {
		t.Fatal("old handle revived")
	}
}

func TestCandidateSource128DistinctAndExactReuse(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	targets := make([]Session, 129)
	for i := range targets {
		targets[i], _ = provenFixture(t, domain.ConnID(i+100), "target", 0)
	}
	var stale candidateReservation
	for i := range 128 {
		at := Instant(i) * Instant(100*time.Millisecond)
		r, out := candidateSourceOfferSeal(t, o, source, targets[i], domain.FindNodeRequestID(i+1), at)
		if out.reserved != 1 {
			t.Fatalf("distinct%d %+v", i, out)
		}
		if i == 0 {
			stale = r
		}
		if ok, err := o.cancel(r, at+o.profile.round); !ok || err != nil {
			t.Fatal(err)
		}
	}
	windows := source.state.sourceWindows.Load()
	before := windows.rows
	next := targets[128].state.nextID
	_, out := candidateSourceOfferSeal(t, o, source, targets[128], 129, Instant(12800*time.Millisecond))
	if out.reserved != 0 || out.refused != 1 || windows.rows != before || targets[128].state.nextID != next {
		t.Fatal("129th distinct target admitted or mutated history")
	}
	// Row0's original final slot=.1s, so its exact distinct boundary is600.1s.
	r, out := candidateSourceOfferSeal(t, o, source, targets[128], 130, candidateGroupWindow)
	if out.reserved != 1 {
		t.Fatal("expired unreferenced row not reused", out)
	}
	row := candidateSourceRow(t, o, r)
	if r.entry.intake.source.index != 0 || row.generation != before[0].generation+1 {
		t.Fatal("reuse generation not advanced")
	}
	snapshot := windows.rows
	if ok, err := o.cancel(stale, candidateGroupWindow+o.profile.round); ok || err != nil || windows.rows != snapshot {
		t.Fatal("stale handle mutated reused row", err)
	}
	if ok, err := o.cancel(r, candidateGroupWindow+o.profile.round); !ok || err != nil {
		t.Fatal(err)
	}
}

func TestCandidateSourceOutstandingDispatchBlocksReuse(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	r, out := candidateSourceOfferSeal(t, o, source, target, 1, 0)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	at := o.profile.round
	permit, _, err := o.takeForward(r, at)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := o.cancel(r, at); !ok || err != nil {
		t.Fatal(err)
	}
	if r.entry.intake.phase != candidateTerminalDispatch || candidateSourceRow(t, o, r).refs != 1 {
		t.Fatal("terminal dispatch released source ref")
	}
	late := 2 * candidateGroupWindow
	if err := o.expire(late); err != nil {
		t.Fatal(err)
	}
	row := candidateSourceRow(t, o, r)
	if row.refs != 1 {
		t.Fatal("nominal deadlines evicted unresolved dispatch")
	}
	// Boundary injection isolates row reuse without pretending128 real commits ran.
	windows := source.state.sourceWindows.Load()
	for i := 1; i < len(windows.rows); i++ {
		windows.rows[i] = candidateSourceTarget{target: candidateTarget(i + 10), expires: late + candidateGroupWindow, generation: 1}
	}
	replacement, _ := provenFixture(t, 3, "target", 0)
	_, out = candidateSourceOfferSeal(t, o, source, replacement, 2, late)
	if out.reserved != 0 || out.refused != 1 || windows.rows[0] != row {
		t.Fatal("referenced expired row reused")
	}
	finish := late + o.profile.round
	if err := o.finishForward(permit, finish, candidateWriteFinished); err != nil {
		t.Fatal(err)
	}
	after := windows.rows[0]
	if after.refs != 0 || after.expires != finish+candidateGroupWindow {
		t.Fatal("sole late Finish did not extend/release", after)
	}
	if err := o.finishForward(permit, finish, candidateWriteFinished); !errors.Is(err, ErrStaleWork) || windows.rows[0] != after {
		t.Fatal("repeat Finish changed history", err)
	}
}

func TestCandidateSourceOwnerClaimAndCheckedRefusals(t *testing.T) {
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	first, second := candidateTestOwner(t), candidateTestOwner(t)
	if first.sourceOwner == second.sourceOwner {
		t.Fatal("owner tokens alias")
	}
	r, out := candidateSourceOfferSeal(t, first, source, target, 1, 0)
	if out.reserved != 1 {
		t.Fatal(out)
	}
	windows := source.state.sourceWindows.Load()
	before := windows.rows
	next := target.state.nextID
	_, out = candidateSourceOfferSeal(t, second, source, target, 2, 0)
	if out.reserved != 0 || out.refused != 1 || windows.rows != before || target.state.nextID != next || second.snapshot().records != 0 {
		t.Fatal("second owner spent source history")
	}
	if ok, err := first.cancel(r, first.profile.round); !ok || err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"generation", "refs", "expiry"} {
		t.Run(kind, func(t *testing.T) {
			o := candidateTestOwner(t)
			s, _ := provenFixture(t, 10, "source", 0)
			target, _ := provenFixture(t, 20, "target", 0)
			prepared := &candidateSourceWindows{owner: o.sourceOwner}
			switch kind {
			case "generation":
				for i := range prepared.rows {
					prepared.rows[i] = candidateSourceTarget{target: candidateTarget(i + 50), generation: math.MaxUint64}
				}
			case "refs":
				prepared.rows[0] = candidateSourceTarget{target: target.Identity(), generation: 1, refs: math.MaxInt, expires: candidateGroupWindow}
			case "expiry":
				// Direct checked-addition trial; no deployment profile uses this boundary.
				p := candidateSourcePlan{windows: prepared, row: candidateSourceTarget{target: target.Identity(), generation: 1}}
				if _, err := p.finalizeSourceLocked(Instant(math.MaxInt64) - candidateGroupWindow + 1); err == nil {
					t.Fatal("expiry overflow accepted")
				}
				if s.state.sourceWindows.Load() != nil || prepared.rows != ([candidateSourceTargets]candidateSourceTarget{}) {
					t.Fatal("overflow bound source")
				}
				return
			}
			received := Instant(0)
			r, err := o.offer(s, target, request(1, target.Identity(), 1), candidateView(t, o, received, []Session{s}, []Session{s}, target), received)
			if err != nil {
				t.Fatal(err)
			}
			before := prepared.rows
			o.mu.Lock()
			o.pending.mu.Lock()
			err = o.commitLocked(r.entry, o.profile.round, prepared)
			o.pending.mu.Unlock()
			o.mu.Unlock()
			if err == nil || s.state.sourceWindows.Load() != nil || prepared.rows != before || target.state.nextID != 0 || calendarFixedCount(t, o) != 0 || fixedGroupRecords(&o.groups) != 0 || fixedGroupNames(&o.groups) != 0 {
				t.Fatal("checked trial published effects", err)
			}
		})
	}
}

func TestCandidateSourceActualTakeAtomicityAndStaleGeneration(t *testing.T) {
	for _, failed := range []string{"pair", "target"} {
		t.Run(failed, func(t *testing.T) {
			o := candidateTestOwner(t)
			source, _ := provenFixture(t, 1, "source", 0)
			target, _ := provenFixture(t, 2, "target", 0)
			r, out := candidateSourceOfferSeal(t, o, source, target, 1, 0)
			if out.reserved != 1 {
				t.Fatal(out)
			}
			at := o.profile.round
			state := r.entry.intake
			row := &source.state.sourceWindows.Load().rows[state.source.index]
			if failed == "pair" {
				row.actual = candidatePairHistory{times: [3]Instant{at, at, at}, count: 3}
			} else {
				state.calendar.actual = forwardBucket{at: at, tokens: 0}
			}
			beforeRow, beforeTarget, beforeAlias := *row, state.calendar.actual, state.alias
			if _, _, err := o.takeForward(r, at); !errors.Is(err, errCandidateAssumption) {
				t.Fatal("assumption failure missing", err)
			}
			if *row != beforeRow || state.calendar.actual != beforeTarget || state.alias != beforeAlias || state.calendar.forwardAliasCountLocked() != 1 || state.phase != candidateReserved {
				t.Fatal("failed Take partially spent pair/target or alias")
			}
			state.source.generation++ // Explicit detached-reference fault, not a reachable reuse.
			if _, _, err := o.takeForward(r, at); !errors.Is(err, ErrStaleWork) || *row != beforeRow {
				t.Fatal("stale row generation mutated replacement", err)
			}
		})
	}
}

func TestCandidateSourceConcurrentPairPermitsBounded(t *testing.T) {
	o := candidateTestOwner(t)
	source, _ := provenFixture(t, 1, "source", 0)
	target, _ := provenFixture(t, 2, "target", 0)
	// Keep the same pair's commitments, but retire each alias at its valid time.
	for i := range 3 {
		at := Instant(i) * Instant(100*time.Millisecond)
		r, out := candidateSourceOfferSeal(t, o, source, target, domain.FindNodeRequestID(i+1), at)
		if out.reserved != 1 {
			t.Fatal(out)
		}
		var wg sync.WaitGroup
		var permits [2]candidateForwardPermit
		var errs [2]error
		for j := range 2 {
			wg.Add(1)
			go func(j int) { defer wg.Done(); permits[j], _, errs[j] = o.takeForward(r, at+o.profile.round) }(j)
		}
		wg.Wait()
		successes := 0
		for j, err := range errs {
			if err == nil {
				successes++
				if err = o.finishForward(permits[j], at+o.profile.round, candidateWriteFailed); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrStaleWork) {
				t.Fatal(err)
			}
		}
		if successes != 1 {
			t.Fatal("duplicate permit", successes)
		}
	}
	_, out := candidateSourceOfferSeal(t, o, source, target, 4, Instant(300*time.Millisecond))
	if out.reserved != 0 || out.refused != 1 {
		t.Fatal("concurrent pair history exceeded three", out)
	}
}

func TestCandidateSourcePureWindowClocksAndAllReuseConditions(t *testing.T) {
	original := candidatePairHistory{times: [3]Instant{0, 1, 2}, count: 3}
	for _, at := range []Instant{-1, 1, 3} {
		if _, err := original.candidatePairTrial(at); err == nil {
			t.Fatal("invalid/full trial admitted", at)
		}
		if original != (candidatePairHistory{times: [3]Instant{0, 1, 2}, count: 3}) {
			t.Fatal("value trial changed original")
		}
	}
	if next, err := original.candidatePairTrial(candidatePairWindow); err != nil || next.count != 3 || next.times[0] != 1 || next.times[2] != candidatePairWindow {
		t.Fatal("exact rolling boundary", next, err)
	}
	for _, blocked := range []string{"refs", "distinct", "projected", "actual", "generation"} {
		t.Run(blocked, func(t *testing.T) {
			o := candidateTestOwner(t)
			source, _ := provenFixture(t, 1, "source", 0)
			target, _ := provenFixture(t, 2, "target", 0)
			prepared := &candidateSourceWindows{owner: o.sourceOwner}
			at := 2 * candidateGroupWindow
			for i := range prepared.rows {
				prepared.rows[i] = candidateSourceTarget{target: candidateTarget(i + 100), generation: 1, refs: 1, expires: at + candidateGroupWindow}
			}
			candidate := candidateSourceTarget{target: candidateTarget(1), generation: 1, expires: at}
			switch blocked {
			case "refs":
				candidate.refs = 1
			case "distinct":
				candidate.expires = at + 1
			case "projected":
				candidate.projected = candidatePairHistory{times: [3]Instant{at}, count: 1}
			case "actual":
				candidate.actual = candidatePairHistory{times: [3]Instant{at}, count: 1}
			case "generation":
				candidate.generation = math.MaxUint64
			}
			prepared.rows[0] = candidate
			entry := &pendingEntry{source: source, target: target, request: request(1, target.Identity(), 1), intake: &candidatePending{}}
			before := prepared.rows
			if _, err := o.planSourceLocked(entry, at, prepared); !errors.Is(err, ErrCapacity) || prepared.rows != before || source.state.sourceWindows.Load() != nil {
				t.Fatal("incomplete reuse predicate accepted", err)
			}
			prepared.rows[0] = candidateSourceTarget{target: candidateTarget(1), generation: 1, expires: at}
			plan, err := o.planSourceLocked(entry, at, prepared)
			if err != nil || plan.index != 0 || plan.row.generation != 2 {
				t.Fatal("all reuse predicates failed at equality", plan, err)
			}
		})
	}
}
