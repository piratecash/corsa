package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// A refused input still owns its enter/leave interval. If it becomes the last
// such owner after a proof was cancelled, it must discharge that proof's purge
// rather than leave a reservation no remaining writer can ever release.
func TestRefusedRoutingInputCompletesCancelledProofPurgeWhenItIsLast(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(peer.Identity, idTargetX))
	if !routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Fatal("legacy baseline did not establish the route to purge")
	}
	release, admitted := svc.admitRoutingInput(sessionRoutingSender(impostor), "announce_routes")
	if !admitted {
		t.Fatal("legacy input refused before the pin")
	}
	firstReleased := false
	storeLocked := false
	var rejectedDone chan struct{}
	t.Cleanup(func() {
		if storeLocked {
			svc.secureSessions.store.mu.Unlock()
		}
		if !firstReleased {
			release()
		}
		if rejectedDone != nil {
			select {
			case <-rejectedDone:
			case <-time.After(5 * time.Second):
				t.Error("refused routing input did not finish cleanup")
			}
		}
	})
	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatal(err)
	}
	proof := sessionv2test.Proof(t, peer)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.onIdentityProvenV2(cancelled, proof); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled proof returned %v, want context.Canceled", err)
	}

	// The late input enters before checking the now-pinned identity. Hold
	// that check while the previously admitted writer finishes, so this
	// refused input is the last owner of the outstanding purge obligation.
	svc.secureSessions.store.mu.Lock()
	storeLocked = true
	rejectedDone = make(chan struct{})
	var lateAdmitted bool
	go func() {
		defer close(rejectedDone)
		lateRelease, ok := svc.admitRoutingInput(sessionRoutingSender(impostor), "announce_routes")
		lateAdmitted = ok
		if ok {
			lateRelease()
		}
	}()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		svc.unprovenRouting.mu.Lock()
		inflight := svc.unprovenRouting.inflight[peer.Identity]
		svc.unprovenRouting.mu.Unlock()
		if inflight == 2 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("late routing input did not reach the pin check")
		case <-tick.C:
		}
	}
	release()
	firstReleased = true
	svc.secureSessions.store.mu.Unlock()
	storeLocked = false
	select {
	case <-rejectedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("late routing input did not finish its refusal")
	}
	if lateAdmitted {
		t.Fatal("a pinned identity was admitted through a legacy claim")
	}
	svc.unprovenRouting.mu.Lock()
	inflight := svc.unprovenRouting.inflight[peer.Identity]
	_, owed := svc.unprovenRouting.owed[peer.Identity]
	svc.unprovenRouting.mu.Unlock()
	if inflight != 0 || owed {
		t.Errorf("refusal stranded purge ownership: inflight=%d owed=%v", inflight, owed)
	}
	if routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Error("refusal abandoned the cancelled proof's routing purge")
	}
	retry, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := svc.onIdentityProvenV2(retry, proof); err != nil {
		t.Errorf("next proof still waits for a nonexistent legacy writer: %v", err)
	}
}

func TestCancellationDuringAnOwedRoutingPurgeDoesNotStrandItsOwner(t *testing.T) {
	svc, peer, impostor := unpinnedRoutingNode(t)
	svc.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(peer.Identity, idTargetX))
	if !routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Fatal("legacy baseline did not establish the route to purge")
	}
	release, admitted := svc.admitRoutingInput(sessionRoutingSender(impostor), "announce_routes")
	if !admitted {
		t.Fatal("legacy input refused before the pin")
	}
	peerLocked, releaseStarted := false, false
	var releaseDone, secondDone chan struct{}
	var cancelSecond context.CancelFunc
	t.Cleanup(func() {
		if peerLocked {
			svc.peerMu.Unlock()
		}
		if cancelSecond != nil {
			cancelSecond()
		}
		if !releaseStarted {
			release()
		}
		for _, done := range []chan struct{}{releaseDone, secondDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("routing purge job did not finish cleanup")
			}
		}
	})
	if err := svc.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		t.Fatal(err)
	}
	proof := sessionv2test.Proof(t, peer)
	first, cancelFirst := context.WithCancel(context.Background())
	cancelFirst()
	if err := svc.onIdentityProvenV2(first, proof); !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancelled proof returned %v", err)
	}
	waitFor := func(label string, ready func(*unprovenRoutingWriters) bool) {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			svc.unprovenRouting.mu.Lock()
			ok := ready(&svc.unprovenRouting)
			svc.unprovenRouting.mu.Unlock()
			if ok {
				return
			}
			select {
			case <-deadline.C:
				t.Fatal(label)
			case <-tick.C:
			}
		}
	}
	second, stop := context.WithCancel(context.Background())
	cancelSecond = stop
	secondDone = make(chan struct{})
	var secondErr error
	go func() {
		defer close(secondDone)
		secondErr = svc.onIdentityProvenV2(second, proof)
	}()
	waitFor("second proof did not reach the drain wait", func(m *unprovenRoutingWriters) bool {
		return len(m.drained[peer.Identity]) == 1
	})

	// Pause the original purge after it takes the mark, before it can
	// inspect registered v2 peers and remove the routing residue.
	svc.peerMu.Lock()
	peerLocked = true
	releaseDone = make(chan struct{})
	releaseStarted = true
	go func() {
		defer close(releaseDone)
		release()
	}()
	waitFor("purge did not reach the protected peer snapshot", func(m *unprovenRoutingWriters) bool {
		_, marked := m.at[peer.Identity]
		_, owed := m.owed[peer.Identity]
		return !marked && !owed && m.inflight[peer.Identity] == 1
	})
	cancelSecond()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second proof did not cancel while the purge was paused")
	}
	if !errors.Is(secondErr, context.Canceled) {
		t.Fatalf("second proof returned %v, want context.Canceled", secondErr)
	}
	svc.peerMu.Unlock()
	peerLocked = false
	select {
	case <-releaseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("original purge did not finish")
	}
	svc.unprovenRouting.mu.Lock()
	inflight := svc.unprovenRouting.inflight[peer.Identity]
	_, owed := svc.unprovenRouting.owed[peer.Identity]
	svc.unprovenRouting.mu.Unlock()
	if inflight != 0 || owed {
		t.Errorf("repeated cancellation stranded purge ownership: inflight=%d owed=%v", inflight, owed)
	}
	if routeLearnedVia(svc, idTargetX, peer.Identity) {
		t.Error("the completed purge retained the legacy route")
	}
	retry, cancelRetry := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRetry()
	if err := svc.onIdentityProvenV2(retry, proof); err != nil {
		t.Errorf("proof after repeated cancellation still waits for an absent writer: %v", err)
	}
}
