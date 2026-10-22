package findnode

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOwnedAcceptedWriterSurvivesUnrelatedPublication(t *testing.T) {
	f, work, queued := executingOne(t, false)
	old := work.work.cut.stamp
	publication := f.domain.Publisher().BeginPublication()
	publication.Complete()
	if _, err := f.domain.Consumer().TryReadClock(old); err == nil {
		t.Fatal("fixture did not revoke historical class cut")
	}
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal("unrelated W revoked accepted original obligation", err)
	}
	if err := permit.Finish(false); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedAcceptedWriterDoesNotRevalidateUnrelatedOldMember(t *testing.T) {
	f := executingNew(t, 2, 4, false)
	batch := f.admit(t, 1)
	work, ok := batch.Next()
	if !ok {
		t.Fatal("actual receipt absent")
	}
	queued, err := work.QueueForOriginal(f.members[2])
	if err != nil {
		t.Fatal(err)
	}
	publication := f.domain.Publisher().BeginPublication()
	f.members[1].End()
	publication.Complete()
	if err := f.intake.EndOriginalSession(f.members[1]); err != nil {
		t.Fatal(err)
	}
	_, permit, err := queued.Take()
	if err != nil {
		t.Fatal("unrelated historical member End revoked accepted original", err)
	}
	if err := permit.Finish(false); err != nil {
		t.Fatal(err)
	}
}
func TestOwnedAcceptedWriterLateLatestCountsViolationWithoutRefund(t *testing.T) {
	f, work, queued := executingOne(t, false)
	entry := work.work.receipt.reservation.entry
	calendar := entry.intake.calendar
	projected, actual := calendar.projected, calendar.actual
	f.clock.Store(int64(entry.intake.latest + 1))
	if _, _, err := queued.Take(); !errors.Is(err, errCandidateAssumption) {
		t.Fatal("late original start lost explicit assumption violation", err)
	}
	snapshot := f.intake.dormantOwner.coordinator.snapshot()
	if snapshot.violations != 1 || snapshot.pending != 0 || calendar.projected != projected || calendar.actual != actual || f.members[1].state.nextID != 1 {
		t.Fatal("late start did not retire exact row conservatively", snapshot)
	}
}

func ownedAcceptedWaitForPort(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw := make([]byte, 1<<20)
		n := runtime.Stack(raw, true)
		if n == len(raw) {
			t.Fatal("accepted wait fixture requires complete bounded stacks")
		}
		for _, stack := range strings.Split(string(raw[:n]), "\n\n") {
			if strings.Contains(stack, "findnodebridge.Consumer.ReserveAcceptedClock") && strings.Contains(stack, "findnode.OwnedQueuedForward.Take") {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("original writer did not reach accepted clock/auth wait")
}
func TestOwnedAcceptedWriterIgnoresUnrelatedEndAfterCleanup(t *testing.T) {
	f := executingNew(t, 3, 4, false)
	for _, pair := range [][2]Session{{f.members[0], f.members[3]}, {f.members[1], f.members[2]}} {
		if err := f.intake.OfferReceived(receivedRequest(t, f.sink, f.domain, pair[0], pair[1], 17), pair[1], f.cut(t)); err != nil {
			t.Fatal(err)
		}
	}
	f.clock.Store(int64(ownedBridgeProfile().Round))
	batch, err := f.intake.CloseOwnedRound(f.cut(t))
	if err != nil {
		t.Fatal(err)
	}
	var current, other OwnedForwardWork
	for {
		work, ok := batch.Next()
		if !ok {
			break
		}
		if work.RouteTarget() == f.members[3] {
			current = work
		} else {
			other = work
		}
	}
	if current.work == nil || other.work == nil {
		t.Fatal("two actual distinct-calendar receipts absent")
	}
	queued, err := current.QueueForOriginal(f.members[3])
	if err != nil {
		t.Fatal(err)
	}
	unrelated := other.work.receipt.reservation.entry.intake.calendar
	unrelated.mu.Lock()
	calendarLocked := true
	defer func() {
		if calendarLocked {
			unrelated.mu.Unlock()
		}
	}()
	publication := f.domain.Publisher().BeginPublication()
	completed := false
	defer func() {
		if !completed {
			publication.Complete()
		}
	}()
	type result struct {
		permit OwnedWritePermit
		err    error
	}
	done := make(chan result, 1)
	go func() { _, permit, err := queued.Take(); done <- result{permit, err} }()
	// This proves q-housekeeping has finished while both original rows are
	// still valid. Change only the unrelated source under W BEFORE fresh R/f.
	ownedAcceptedWaitForPort(t)
	f.members[1].End()
	publication.Complete()
	completed = true
	var got result
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		unrelated.mu.Unlock()
		calendarLocked = false
		select {
		case got = <-done:
			if got.err == nil {
				_ = got.permit.Finish(false)
			}
		case <-time.After(2 * time.Second):
		}
		t.Fatal("current accepted start waited for unrelated post-T cleanup")
	}
	unrelated.mu.Unlock()
	calendarLocked = false
	if got.err != nil {
		t.Fatal("unrelated newly ended row revoked current accepted original", got.err)
	}
	if err := got.permit.Finish(false); err != nil {
		t.Fatal(err)
	}
	if err := other.Cancel(); err != nil {
		t.Fatal(err)
	}
}
