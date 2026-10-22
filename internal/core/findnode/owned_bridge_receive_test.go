package findnode

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func receivedFixture(t *testing.T) (DormantIntake, ObservationSink, findnodebridge.FixtureDomain, *atomic.Int64, Session, Session) {
	t.Helper()
	d, clock, _ := ownedOriginalDomain(t)
	intake, sink, err := NewDormantFixtureIntake(d.Consumer(), ownedBridgeProfile(), 8)
	if err != nil {
		t.Fatal(err)
	}
	return intake, sink, d, clock, ownedOriginalSession(t, 1, 0), ownedOriginalSession(t, 2, 0)
}
func receivedCut(t *testing.T, sink ObservationSink, d findnodebridge.FixtureDomain, source, target Session) OwnedCut {
	t.Helper()
	stamp, at := ownedBridgePoint(t, d)
	cut, err := sink.Seal(stamp, at, ownedBridgeBoundary(t, at, time.Second), []ObservedMember{{Session: source, Class: ClassOrdinary}, {Session: target, Class: ClassOrdinary}}, ObservedDegree{Incoming: 2, Window: 2})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}
func receivedRequest(t *testing.T, sink ObservationSink, d findnodebridge.FixtureDomain, source, target Session, id domain.FindNodeRequestID) OwnedReceivedRequest {
	t.Helper()
	stamp, at := ownedBridgePoint(t, d)
	raw, err := protocol.MarshalFindNodeRequest(request(id, target.Identity(), 9))
	if err != nil {
		t.Fatal(err)
	}
	input, err := sink.SealReceived(stamp, at, source, raw)
	if err != nil {
		t.Fatal(err)
	}
	return input
}
func TestOwnedReceivedOfferRetainsOriginalProvisionalAndNoFinalDebt(t *testing.T) {
	intake, sink, d, _, source, target := receivedFixture(t)
	input := receivedRequest(t, sink, d, source, target, 17)
	cut := receivedCut(t, sink, d, source, target)
	beforeID := target.state.nextID
	if err := intake.OfferReceived(input, target, cut); err != nil {
		t.Fatal(err)
	}
	o := intake.dormantOwner.coordinator
	o.mu.Lock()
	o.pending.mu.Lock()
	index := o.pending.sourceIndexLocked(pendingKey{source, 17})
	var entry *pendingEntry
	if index >= 0 {
		entry = o.pending.entries[index]
	}
	good := entry != nil && entry.source == source && entry.target == target && entry.intake.phase == candidateProvisional && entry.intake.ownedReceived == input.receivedRequest && entry.intake.ownedIdentity == input.receivedRequest.identity && ownedReceivedMatches(intake.dormantOwner, entry)
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if !good || source.state.sourceWindows.Load() != nil || target.state.nextID != beforeID {
		t.Fatal("owned offer replaced source/receipt or charged final debt")
	}
	if err := intake.OfferReceived(input, target, cut); !errors.Is(err, ErrStaleWork) {
		t.Fatal("opaque request replay", err)
	}
	second := receivedRequest(t, sink, d, source, target, 17)
	if err := intake.OfferReceived(second, target, cut); !errors.Is(err, ErrDuplicateID) {
		t.Fatal("accepted offer was discarded", err)
	}
}
func TestOwnedReceivedOfferUsesFreshPostWaitPointAtExactCollectionBoundary(t *testing.T) {
	intake, sink, d, clock, source, target := receivedFixture(t)
	input := receivedRequest(t, sink, d, source, target, 17)
	cut := receivedCut(t, sink, d, source, target)
	o := intake.dormantOwner.coordinator
	o.mu.Lock()
	done := make(chan error, 1)
	go func() { done <- intake.OfferReceived(input, target, cut) }()
	deadline := time.Now().Add(2 * time.Second)
	for !intake.dormantOwner.scratch.borrowed.Load() {
		if time.Now().After(deadline) {
			o.mu.Unlock()
			t.Fatal("offer did not enter original lane")
		}
		runtime.Gosched()
	}
	clock.Store(int64(ownedBridgeProfile().Round))
	o.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotReady) {
			t.Fatal("delayed offer extended original collection", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owned offer did not join")
	}
	if o.snapshot().pending != 0 || o.open {
		t.Fatal("late receive acquired provisional authority")
	}
}
func TestOwnedReceivedPlanRejectsPointerAndInPlaceIdentityReplacement(t *testing.T) {
	for _, kind := range []string{"pointer", "identity"} {
		t.Run(kind, func(t *testing.T) {
			intake, sink, d, clock, source, target := receivedFixture(t)
			input := receivedRequest(t, sink, d, source, target, 17)
			if err := intake.OfferReceived(input, target, receivedCut(t, sink, d, source, target)); err != nil {
				t.Fatal(err)
			}
			clock.Store(int64(ownedBridgeProfile().Round))
			attempt, err := intake.tryOwnedAttempt(receivedCut(t, sink, d, source, target))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cleanupErr := attempt.releaseOwnedAttempt(); cleanupErr != nil {
					t.Error("original attempt cleanup", cleanupErr)
				}
			}()
			if err := attempt.projectOwnedMembership([]domain.PeerIdentity{target.Identity()}, []Session{source}); err != nil {
				t.Fatal(err)
			}
			plan, err := attempt.prepareOwnedRoundPlan()
			if err != nil {
				t.Fatal(err)
			}
			o := intake.dormantOwner.coordinator
			o.mu.Lock()
			o.pending.mu.Lock()
			entry := o.pending.entries[o.pending.sourceIndexLocked(pendingKey{source, 17})]
			if kind == "pointer" {
				replacement := &ownedReceivedState{identity: input.receivedRequest.identity}
				replacement.consumed.Store(true)
				entry.intake.ownedReceived = replacement
			} else {
				input.receivedRequest.identity.request.ID++
			}
			o.pending.mu.Unlock()
			o.mu.Unlock()
			if _, err := attempt.consumeOwnedRoundPlan(plan); !errors.Is(err, ErrStaleWork) {
				t.Fatal("rewritten ingress snapshot admitted", kind, err)
			}
		})
	}
}
func TestOwnedReceivedRejectsForeignOwnerAndSourceOpenedAfterReceive(t *testing.T) {
	intake, sink, d, clock, source, target := receivedFixture(t)
	stamp, at := ownedBridgePoint(t, d)
	clock.Store(int64(time.Second))
	late := ownedOriginalSession(t, 3, Instant(time.Second))
	raw, err := protocol.MarshalFindNodeRequest(request(17, target.Identity(), 9))
	if err != nil {
		t.Fatal(err)
	}
	if input, err := sink.SealReceived(stamp, at, late, raw); !errors.Is(err, ErrUnauthenticated) || input != (OwnedReceivedRequest{}) {
		t.Fatal("late-open original backdated receive", input, err)
	}
	input := receivedRequest(t, sink, d, source, target, 17)
	other, _, _, _, _, _ := receivedFixture(t)
	if err := other.OfferReceived(input, target, receivedCut(t, sink, d, source, target)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("foreign dormant owner accepted ingress", err)
	}
	if err := intake.OfferReceived(input, target, receivedCut(t, sink, d, source, target)); err != nil {
		t.Fatal("foreign refusal consumed original authority", err)
	}
}

func TestOwnedReceivedOfferRefusesCleanupDueAfterHistoricalCut(t *testing.T) {
	intake, sink, d, clock, source, target := receivedFixture(t)
	if err := intake.OfferReceived(receivedRequest(t, sink, d, source, target, 17), target, receivedCut(t, sink, d, source, target)); err != nil {
		t.Fatal(err)
	}
	input := receivedRequest(t, sink, d, source, target, 18)
	cut := receivedCut(t, sink, d, source, target)
	o := intake.dormantOwner.coordinator
	o.mu.Lock()
	o.pending.mu.Lock()
	original := o.pending.entries[o.pending.sourceIndexLocked(pendingKey{source, 17})]
	original.expires = 1
	o.pending.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- intake.OfferReceived(input, target, cut) }()
	deadline := time.Now().Add(2 * time.Second)
	for !intake.dormantOwner.scratch.borrowed.Load() {
		if time.Now().After(deadline) {
			o.mu.Unlock()
			t.Fatal("offer did not enter original lane")
		}
		runtime.Gosched()
	}
	clock.Store(1)
	o.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotReady) {
			t.Fatal("stale cleanup admitted capacity decision", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup refusal did not join")
	}
	o.mu.Lock()
	o.pending.mu.Lock()
	same := o.pending.entries[o.pending.sourceIndexLocked(pendingKey{source, 17})] == original && o.pending.sourceIndexLocked(pendingKey{source, 18}) < 0
	o.pending.mu.Unlock()
	o.mu.Unlock()
	if !same || source.state.sourceWindows.Load() != nil || target.state.nextID != 0 {
		t.Fatal("post-q cleanup refusal changed original row or final debt")
	}
	if err := intake.OfferReceived(input, target, cut); !errors.Is(err, ErrStaleWork) {
		t.Fatal("cleanup-refused original receipt replay", err)
	}
}
