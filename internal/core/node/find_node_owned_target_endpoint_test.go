package node

import (
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Manually retain the ORIGINAL Node reserve handoff, then run the original
// worker/sign/Complete and original NetCore writer. This does not exercise the
// automatic local dispatch jobs queue or claim a complete automatic trace.
func TestOwnedReadyTargetOriginalReceiveCompleteReadout(t *testing.T) {
	cfg, id, _ := readyInboundInputs(t)
	s, err := newFindNodeReadyTimedLoadedInboundCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4}, findnode.OwnedTargetTimingProfile{ServiceBound: 2 * time.Second, RuntimeReserve: 500 * time.Millisecond})
	if err != nil || s == nil {
		t.Fatal("original normal timed Ready construction", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	if s.bridgeCapture == nil || !s.bridgeCapture.consumer.IsRuntime() || s.bridgeCapture.localLoop == nil || s.bridgeCapture.localLoop.signingGate != nil {
		t.Fatal("normal timed original Runtime/gate binding missing")
	}
	store := s.localContactPinStore()
	if store == nil || s.inboundPins == nil || s.inboundPins.store != store {
		t.Fatal("original loaded store missing")
	}
	store.mu.Lock()
	loaded := store.ninFormat3 && store.ninJournal != nil
	store.mu.Unlock()
	if !loaded {
		t.Fatal("actual format3 was not loaded")
	}
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	remoteCfg, remoteID := readyTestInputs(t)
	remote, err := newFindNodeReadyCandidateService(remoteCfg, remoteID, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil {
		t.Fatal(err)
	}
	remote.routeWithdrawalGracePeriodTest = -1
	t.Cleanup(func() { remote.runCancel(); remote.closeBackgroundAdmission(); remote.WaitBackground() })
	wire, original := ownedRawAccepted(t, s, remote)
	facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
	if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original {
		t.Fatal("original F1 member was replaced", err)
	}
	request := ownedLocalNodeRequest(t, s)
	receipt := ownedLocalNodeReadRequest(t, s, wire, request)
	if receipt.cell == nil {
		t.Fatal("actual strict receive receipt absent")
	}
	r := int64(receipt.cell.received.Elapsed())
	result := ownedLocalNodeReserve(t, s, receipt)
	if result.Kind != findnode.LocalReserved {
		t.Fatal("no original fresh signing work", result.Kind)
	}
	t.Cleanup(func() {
		if err := result.Work.Cancel(); err != nil && !errors.Is(err, findnode.ErrStaleWork) {
			t.Error("original local cleanup", err)
		}
	})
	before := result.Work.TargetEndpointObservation()
	if before.Clock != findnode.OwnedTargetEndpointClockRuntime || !before.ReceivedKnown || before.ReceivedNS != r || before.RequestID != uint64(request.ID) || before.TargetIdentity != request.Target || before.Challenge != request.Challenge || before.SourceConnID != uint64(original.ConnID()) || before.SourceIdentity != original.Identity() || before.SourceOpenedNS < 0 || before.SourceOpenedNS > r || before.CompleteKnown || before.PairKnown {
		t.Fatal("target r/attempt does not match original read and F1", before)
	}
	// The existing worker owns mandatory Complete(nil) and all original signing
	// errors. Its timer uses the existing retry input, never a new runtime clock.
	loop := s.bridgeCapture.localLoop
	timer := time.NewTimer(loop.retry)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	s.runOwnedFindNodeLocalWork(loop, timer, result.Work)
	answer := ownedLocalNodeReadAnswer(t, wire)
	if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
		t.Fatal("actual original signer/writer changed retained request", err)
	}
	after := result.Work.TargetEndpointObservation()
	if !after.PairKnown || !after.CompleteKnown || after.Clock != findnode.OwnedTargetEndpointClockRuntime || after.ReceivedNS != r || after.CompleteNS < r || after.SpanNS != after.CompleteNS-r || after.RequestID != before.RequestID || after.SourceIdentity != before.SourceIdentity || after.SourceConnID != before.SourceConnID || after.TargetIdentity != before.TargetIdentity || after.Challenge != before.Challenge {
		t.Fatal("original successful Complete lost target r/c binding", before, after)
	}
	copy := after
	copy.ReceivedNS = -1
	copy.Challenge[0]++
	if result.Work.TargetEndpointObservation() != after {
		t.Fatal("Node scalar readout aliased mutable original", copy)
	}
	resources := s.bridgeCapture.intake.ResourceSnapshot()
	if resources.Local.Local.Consumed != 1 || resources.Local.Local.Completed != 1 || resources.Local.CalendarPending != 0 {
		t.Fatal("original worker/signature completion missing", resources)
	}
	// Runtime is provenance only. No latency threshold, physical clock accuracy,
	// return-time/byte-completion claim or cross-process sample subtraction.
}
