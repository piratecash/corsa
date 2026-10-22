package findnode

import (
	"errors"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
	"sync/atomic"
	"testing"
	"time"
)

type executingFixture struct {
	intake  DormantIntake
	sink    ObservationSink
	domain  findnodebridge.FixtureDomain
	clock   *atomic.Int64
	stop    func()
	members []Session
}

func executingNew(t *testing.T, count int, tokens uint32, foreignObserver bool) executingFixture {
	t.Helper()
	d, clock, stop := ownedOriginalDomain(t)
	observer := d.Observer()
	if foreignObserver {
		other, _, _ := ownedOriginalDomain(t)
		observer = other.Observer()
	}
	profile := ownedBridgeProfile()
	profile.InitialTokens = tokens
	intake, sink, err := NewExecutingFixtureIntake(d.Consumer(), observer, profile, 8)
	if err != nil {
		t.Fatal(err)
	}
	f := executingFixture{intake: intake, sink: sink, domain: d, clock: clock, stop: stop}
	for id := 1; id <= count+1; id++ {
		f.members = append(f.members, ownedOriginalSession(t, domain.ConnID(id), 0))
	}
	return f
}
func (f executingFixture) cut(t *testing.T) OwnedCut {
	t.Helper()
	stamp, at := ownedBridgePoint(t, f.domain)
	members := make([]ObservedMember, len(f.members))
	for i, s := range f.members {
		members[i] = ObservedMember{Session: s, Class: ClassOrdinary}
	}
	cut, err := f.sink.Seal(stamp, at, ownedBridgeBoundary(t, at, time.Second), members, ObservedDegree{Incoming: len(members), Window: len(members)})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}
func (f executingFixture) admit(t *testing.T, count int) OwnedRoundBatch {
	t.Helper()
	target := f.members[len(f.members)-1]
	for i := 0; i < count; i++ {
		if err := f.intake.OfferReceived(receivedRequest(t, f.sink, f.domain, f.members[i], target, 17), target, f.cut(t)); err != nil {
			t.Fatal("original receive offer", i, err)
		}
	}
	f.clock.Store(int64(ownedBridgeProfile().Round))
	batch, err := f.intake.CloseOwnedRound(f.cut(t))
	if err != nil {
		t.Fatal("actual C3b/C3c consumer", err)
	}
	return batch
}
func executingOne(t *testing.T, foreign bool) (executingFixture, OwnedForwardWork, OwnedQueuedForward) {
	t.Helper()
	f := executingNew(t, 1, 4, foreign)
	batch := f.admit(t, 1)
	work, ok := batch.Next()
	if !ok || work.work == nil {
		t.Fatal("actual success receipt absent")
	}
	if _, ok := batch.Next(); ok {
		t.Fatal("one original receipt repeated")
	}
	queued, err := work.QueueForOriginal(f.members[1])
	if err != nil {
		t.Fatal(err)
	}
	return f, work, queued
}

func TestOwnedExecutingBatchTakeFinishSameOriginalOnce(t *testing.T) {
	f, work, queued := executingOne(t, false)
	if delay, err := queued.WaitDelay(); err != nil || delay != 0 {
		t.Fatal("initial original slot", delay, err)
	}
	data, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := protocol.ParseFindNodeRequest(data)
	if err != nil || parsed != work.work.forwarded {
		t.Fatal("queued immutable forwarded bytes", parsed, err)
	}
	if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) {
		t.Fatal("queued copy took twice", err)
	}
	f.clock.Add(1)
	if err := permit.Finish(true); err != nil {
		t.Fatal("original genuine completion", err)
	}
	if err := permit.Finish(true); !errors.Is(err, ErrStaleWork) {
		t.Fatal("writer permit finished twice", err)
	}
	entry := work.work.receipt.reservation.entry
	if entry.intake.phase != candidateAwaitingAnswer || f.intake.dormantOwner.coordinator.snapshot().taken != 1 {
		t.Fatal("successful original lifecycle absent")
	}
}
func TestOwnedExecutingCancelRetainsCommittedDebtAndTargetID(t *testing.T) {
	f, work, queued := executingOne(t, false)
	entry := work.work.receipt.reservation.entry
	calendar := entry.intake.calendar
	projected := calendar.projected
	id := f.members[1].state.nextID
	if err := queued.Cancel(); err != nil {
		t.Fatal(err)
	}
	if calendar.projected != projected || f.members[1].state.nextID != id || f.intake.dormantOwner.coordinator.snapshot().pending != 0 {
		t.Fatal("prewriter cancellation refunded accepted commitment")
	}
	if _, _, err := queued.Take(); !errors.Is(err, ErrStaleWork) {
		t.Fatal("cancelled work became writer authority", err)
	}
}
func TestOwnedExecutingEndBeforeWriterRefusesOriginal(t *testing.T) {
	f, work, queued := executingOne(t, false)
	entry := work.work.receipt.reservation.entry
	projected := entry.intake.calendar.projected
	publication := f.domain.Publisher().BeginPublication()
	f.members[0].End()
	publication.Complete()
	if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := queued.Take(); err == nil {
		t.Fatal("ended original acquired writer start")
	}
	if entry.intake.calendar.projected != projected || f.members[1].state.nextID != 1 {
		t.Fatal("End refunded accepted debt/ID")
	}
}
func TestOwnedExecutingEndAfterStartStillFinishesAfterShutdown(t *testing.T) {
	f, work, queued := executingOne(t, false)
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal(err)
	}
	publication := f.domain.Publisher().BeginPublication()
	f.members[0].End()
	publication.Complete()
	if err := f.intake.EndOriginalSession(f.members[0]); err != nil {
		t.Fatal(err)
	}
	entry := work.work.receipt.reservation.entry
	if entry.intake.phase != candidateTerminalDispatch || f.intake.dormantOwner.coordinator.snapshot().pending != 1 {
		t.Fatal("End discarded original in-flight writer")
	}
	f.clock.Add(17)
	f.stop()
	if err := permit.Finish(true); err != nil {
		t.Fatal("changed epoch/shutdown blocked genuine original completion", err)
	}
	if f.intake.dormantOwner.coordinator.snapshot().pending != 0 {
		t.Fatal("mandatory Finish left terminal row")
	}
}
func TestOwnedExecutingForeignCompletionObserverRefusesBeforeWrite(t *testing.T) {
	f, _, queued := executingOne(t, true)
	if _, err := queued.WaitDelay(); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("foreign observer supplied original writer clock", err)
	}
	if err := queued.Cancel(); err != nil {
		t.Fatal(err)
	}
	if f.intake.dormantOwner.coordinator.snapshot().pending != 0 || f.members[1].state.nextID != 1 {
		t.Fatal("foreign role refusal forged lifecycle or refunded ID")
	}
}
func TestOwnedExecutingFutureSlotUsesWakeOnlyThenFreshTake(t *testing.T) {
	f := executingNew(t, 2, 1, false)
	batch := f.admit(t, 2)
	var future OwnedForwardWork
	for {
		work, ok := batch.Next()
		if !ok {
			break
		}
		if work.work.receipt.scheduled > Instant(f.clock.Load()) {
			future = work
		} else {
			_ = work.Cancel()
		}
	}
	if future.work == nil {
		t.Fatal("fixture did not exercise actual future C3a calendar slot")
	}
	queued, err := future.QueueForOriginal(f.members[2])
	if err != nil {
		t.Fatal(err)
	}
	delay, err := queued.WaitDelay()
	if err != nil || delay <= 0 {
		t.Fatal("future original reservation lost schedule", delay, err)
	}
	f.clock.Store(int64(future.work.receipt.scheduled))
	if delay, err := queued.WaitDelay(); err != nil || delay != 0 {
		t.Fatal("genuine due point", delay, err)
	}
	if _, permit, err := queued.Take(); err != nil {
		t.Fatal("due original slot failed Take", err)
	} else if err := permit.Finish(false); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedExecutingSuccessfulTakeDoesNotPublishOtherCalendar(t *testing.T) {
	f, _, queued := executingOne(t, false)
	o := f.intake.dormantOwner.coordinator
	unrelated, err := newOwnedForwardCalendar(ownedOriginalSession(t, 8, 0).Identity(), Instant(f.clock.Load()), 4)
	if err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	index := -1
	for i, c := range o.calendars {
		if c == nil {
			index = i
			break
		}
	}
	if index < 0 {
		o.mu.Unlock()
		t.Fatal("no calendar slot")
	}
	o.calendars[index] = unrelated
	o.mu.Unlock()
	unrelated.mu.Lock()
	type result struct {
		permit OwnedWritePermit
		err    error
	}
	done := make(chan result, 1)
	go func() { _, permit, err := queued.Take(); done <- result{permit, err} }()
	select {
	case got := <-done:
		unrelated.mu.Unlock()
		if got.err != nil {
			t.Fatal(got.err)
		}
		if err := got.permit.Finish(false); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		unrelated.mu.Unlock()
		select {
		case got := <-done:
			if got.err == nil {
				_ = got.permit.Finish(false)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Take child did not join")
		}
		t.Fatal("admitted writer start waited for unrelated diagnostic calendar")
	}
}

func TestOwnedExecutingFailedAndPoisonFinishReleaseWithoutRefund(t *testing.T) {
	for _, poison := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "poison"}[poison], func(t *testing.T) {
			f, work, queued := executingOne(t, false)
			_, permit, err := queued.Take()
			if err != nil {
				t.Fatal(err)
			}
			calendar := work.work.receipt.reservation.entry.intake.calendar
			projected := calendar.projected
			actual := calendar.actual
			if poison {
				f.clock.Store(-1)
			} else {
				f.clock.Add(1)
			}
			err = permit.Finish(false)
			if poison && err == nil {
				t.Fatal("poisoned original completion invented a time")
			}
			if !poison && err != nil {
				t.Fatal(err)
			}
			if f.intake.dormantOwner.coordinator.snapshot().pending != 0 || calendar.projected != projected || calendar.actual != actual || f.members[1].state.nextID != 1 {
				t.Fatal("failed completion retained row or refunded original debt/ID")
			}
			if err := permit.Finish(true); !errors.Is(err, ErrStaleWork) {
				t.Fatal("failed completion permit replayed", err)
			}
		})
	}
}
