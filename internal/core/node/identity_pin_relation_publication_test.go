package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// No sessionv2test proof is created for this subject: the only positive
// protection evidence is its real signed record and exact target proof.
func TestRelationPinPublicationDrainsLegacyWriterAndRejectsSupersededCap(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	svc.secureSessions = &secureSessions{mode: sessionv2.ModeTransition, store: loadSecureSessionStore("", newStoreClock().trusted()), marks: newSessionAddressMarks(time.Now)}
	f := signedRelationFixture(t)
	id := f.question.target
	store := svc.secureSessions.store
	impostor := legacyOutboundSession(legacyAttackerAddress, id)
	impostor.capabilities = routingTwinsCaps
	svc.peerMu.Lock()
	svc.sessions[impostor.address] = impostor
	svc.peerMu.Unlock()
	svc.unprovenRouting.enter(id)
	svc.unprovenRouting.note(id, time.Now())
	stage, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, id), f.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := svc.onRecordPinProtection(ctx, stage.publication); !errors.Is(err, context.Canceled) {
		t.Fatalf("record publication ignored legacy writer cancellation: %v", err)
	}
	// Existing release owns the owed purge and keeps the input in flight
	// through it; publication cannot silently abandon the same legacy writer.
	svc.unprovenRouting.mu.Lock()
	_, owed := svc.unprovenRouting.owed[id]
	svc.unprovenRouting.mu.Unlock()
	if !owed {
		t.Fatal("record publication dropped legacy purge obligation")
	}
	svc.releaseRoutingInput(id)
	if err := svc.onRecordPinProtection(t.Context(), stage.publication); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.deletePinEntryLocked(id)
	store.mu.Unlock()
	if err := svc.onRecordPinProtection(t.Context(), stage.publication); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("obsolete record cap republished after removal: %v", err)
	}
	if store.identityPinned(id) {
		t.Fatal("stale publication recreated protection")
	}
}

func TestRelationPinPublicationWaitsForLegacyInput(t *testing.T) {
	svc := newLegacyPenaltyService(t)
	svc.secureSessions = &secureSessions{mode: sessionv2.ModeTransition, store: loadSecureSessionStore("", newStoreClock().trusted()), marks: newSessionAddressMarks(time.Now)}
	f := signedRelationFixture(t)
	store := svc.secureSessions.store
	stage, err := store.stageRelationRecord(t.Context(), captureRelationOwner(t, store, f.question.target), f.verified(t))
	if err != nil {
		t.Fatal(err)
	}
	svc.unprovenRouting.enter(f.question.target)
	done := make(chan error, 1)
	go func() { done <- svc.onRecordPinProtection(t.Context(), stage.publication) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		svc.unprovenRouting.mu.Lock()
		waiting := len(svc.unprovenRouting.drained[f.question.target]) > 0
		svc.unprovenRouting.mu.Unlock()
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("publication completed before legacy input release: %v", err)
		case <-deadline.C:
			t.Fatal("publication never reached legacy barrier")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("publication did not drain admitted input: %v", err)
	default:
	}
	svc.releaseRoutingInput(f.question.target)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("publication remained blocked after last writer")
	}
}
