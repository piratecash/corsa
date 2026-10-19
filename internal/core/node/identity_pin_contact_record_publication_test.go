package node

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func waitContactRecordCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal(description)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestContactRecordStageGateSpansCurrentRecordReadToPinMutation(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	_, verified := verifiedContactRecord(t, s, peer)
	store := s.secureSessions.store
	store.mu.Lock()
	held := true
	defer func() {
		if held {
			store.mu.Unlock()
		}
	}()
	stageDone := make(chan error, 1)
	go func() { _, err := s.stageContactRecord(t.Context(), verified); stageDone <- err }()
	waitContactRecordCondition(t, "stage did not retain evidence read gate through blocked pin mutation", func() bool {
		if s.trust.pinEvidenceMu.TryLock() {
			s.trust.pinEvidenceMu.Unlock()
			return false
		}
		return true
	})
	record := buildContactRecord(t, peer, 2, domain.DeclaredMinSession(domain.SessionVersionLegacy))
	body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatal(err)
	}
	mergeDone := make(chan error, 1)
	go func() {
		_, err := s.trust.rememberOwnRecord(domain.NetworkID(networkName), record, body)
		mergeDone <- err
	}()
	// Waiting writer must be behind the K2 read gate, not only trust.mu.
	waitContactRecordCondition(t, "newer own-record merge did not serialize on actual evidence gate", func() bool {
		if s.trust.pinEvidenceMu.TryRLock() {
			s.trust.pinEvidenceMu.RUnlock()
			return false
		}
		return true
	})
	select {
	case err := <-mergeDone:
		t.Fatal("newer min1 replaced current record during stage", err)
	default:
	}
	store.mu.Unlock()
	held = false
	if err := <-stageDone; err != nil {
		t.Fatal(err)
	}
	if err := <-mergeDone; err != nil {
		t.Fatal(err)
	}
	if store.contactKStatus(domain.PeerIdentityFromWire(peer.Address)) != domain.LocalContactKStagedUnconfirmed {
		t.Fatal("serialized original min2 did not earn historical K")
	}
}

func TestContactRecordAlreadyLockedStageDoesNotReenterRemovalGate(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	_, verified := verifiedContactRecord(t, s, peer)
	store := s.secureSessions.store
	store.mu.Lock()
	held := true
	defer func() {
		if held {
			store.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := s.stageContactRecord(t.Context(), verified); done <- err }()
	waitContactRecordCondition(t, "stage never entered the gated store mutation", func() bool {
		if s.trust.pinEvidenceMu.TryLock() {
			s.trust.pinEvidenceMu.Unlock()
			return false
		}
		return true
	})
	writerDone := make(chan struct{})
	writerEntered := make(chan struct{})
	go func() {
		store.pinMutationGate.Lock()
		close(writerEntered)
		store.pinMutationGate.Unlock()
		close(writerDone)
	}()
	waitContactRecordCondition(t, "C3 writer did not become an actual removal-gate waiter", func() bool {
		if store.pinMutationGate.TryRLock() {
			store.pinMutationGate.RUnlock()
			return false
		}
		return true
	})
	store.mu.Unlock()
	held = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nested removal RLock deadlocked behind C3 writer")
	}
	select {
	case <-writerDone:
		select {
		case <-writerEntered:
		default:
			t.Fatal("exclusive waiter never entered its boundary")
		}
	case <-time.After(time.Second):
		t.Fatal("C3 waiter did not resume after stage")
	}
}

func TestContactRecordOwedDrainsActualPrePinLegacyWriterAfterContactDelete(t *testing.T) {
	for _, cancelDrain := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete_before_cover", true: "delete_during_cancelled_drain"}[cancelDrain], func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			_, verified := verifiedContactRecord(t, s, peer)
			id := domain.PeerIdentityFromWire(peer.Address)
			impostor := legacyOutboundSession(legacyAttackerAddress, id)
			impostor.capabilities = routingTwinsCaps
			s.peerMu.Lock()
			s.sessions[impostor.address] = impostor
			s.peerMu.Unlock()
			admitted, release := make(chan struct{}), make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			s.routingInputAdmittedHook = func(sender routingSender, frameType string) {
				if frameType == "announce_routes" {
					close(admitted)
					<-release
				}
			}
			handlerDone := make(chan struct{})
			go func() {
				defer close(handlerDone)
				s.dispatchPeerSessionFrame(impostor.address, impostor, legacyBaselineFrame(id, idTargetX))
			}()
			select {
			case <-admitted:
			case <-time.After(2 * time.Second):
				t.Fatal("real pre-pin legacy writer was not admitted")
			}
			stage, err := s.stageContactRecord(t.Context(), verified)
			if err != nil {
				t.Fatal(err)
			}
			files, err := s.secureSessions.store.openIdentityPinFiles()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = files.Close() }()
			if !cancelDrain {
				s.deleteTrustedContactFrame(id)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type completed struct {
				result contactRecordCompletion
				err    error
			}
			done := make(chan completed, 1)
			go func() { result, err := s.completeOwedContactRecord(ctx, stage, files); done <- completed{result, err} }()
			var early *completed
			waitContactRecordCondition(t, "owed completion skipped real legacy drain after contact change", func() bool {
				select {
				case result := <-done:
					early = &result
					return true
				default:
				}
				s.unprovenRouting.mu.Lock()
				waiting := len(s.unprovenRouting.drained[id]) > 0
				s.unprovenRouting.mu.Unlock()
				return waiting
			})
			if early != nil {
				close(release)
				released = true
				select {
				case <-handlerDone:
				case <-time.After(2 * time.Second):
					t.Fatal("early completion also stranded admitted writer")
				}
				if routeLearnedVia(s, idTargetX, id) {
					t.Fatal("completion skipped barrier; actual pre-pin legacy late write survived")
				}
				t.Fatalf("completion passed before admitted legacy input drained: %+v", early)
			}
			if !impostor.closedByLocalDecision() {
				t.Fatal("actual common barrier did not close legacy claim")
			}
			select {
			case result := <-done:
				t.Fatalf("completion passed before late legacy write: %+v", result)
			default:
			}
			if cancelDrain {
				s.deleteTrustedContactFrame(id)
				cancel()
				result := <-done
				if !errors.Is(result.err, context.Canceled) || result.result.state != contactRecordCoveredOwed {
					t.Fatal("cancelled drain dropped obligation", result.err)
				}
				s.unprovenRouting.mu.Lock()
				_, owed := s.unprovenRouting.owed[id]
				s.unprovenRouting.mu.Unlock()
				if !owed {
					t.Fatal("last-writer owePurge handoff lost")
				}
			}
			close(release)
			released = true
			select {
			case <-handlerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("real legacy writer remained blocked")
			}
			if cancelDrain {
				result, err := s.completeOwedContactRecord(t.Context(), stage, files)
				if err != nil || result.state != contactRecordHistoricalProtectionCompleted {
					t.Fatal("original-ticket retry failed after late writer purge", err)
				}
			} else {
				result := <-done
				if result.err != nil || result.result.state != contactRecordHistoricalProtectionCompleted {
					t.Fatal(result.err)
				}
			}
			if routeLearnedVia(s, idTargetX, id) {
				t.Fatal("pre-pin legacy late write survived earned-K purge")
			}
			if s.trust.isTrustedContact(id) || !s.secureSessions.store.identityPinned(id) {
				t.Fatal("cleanup needed current contact or removed historical protection")
			}
		})
	}
}

func TestContactRecordProtectiveC3RollbackRetainsSameOwedIncarnation(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	preview := pinRemovalPreview(t, store, id)
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentityFromWire(s.identity.Address), Store: "contact-store"}, RuntimeEpoch: "epoch", Generation: 1, Identity: id, ProofRevision: entry.evidenceRevision, PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	permit := domain.IdentityPinRemovalPermit{OperationID: "k2-rollback", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.MatchedBy(func(receipt domain.IdentityPinReceipt) bool {
		return receipt.Kind == domain.IdentityPinReceiptProtectionRestored
	})).Return(nil).Once()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().ReadGuard(mock.Anything).Return(nil, fs.ErrNotExist).Once()
	failure := errors.New("guard not committed")
	files.EXPECT().WriteGuard(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, failure).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(real.WriteMain).Once()
	files.EXPECT().RemoveGuard(mock.Anything).Return(secretfile.DurableCommitted, nil).Once()
	result, err := store.removeIdentityPin(t.Context(), binding, preview.Token, fence, files)
	if !errors.Is(err, failure) || result.Outcome != domain.IdentityPinRemovalNotCommitted {
		t.Fatalf("actual C3 protective rollback %+v %v", result, err)
	}
	store.mu.Lock()
	same := store.identities[id].incarnation == entry.incarnation
	epoch := store.removalEpoch
	store.mu.Unlock()
	if !same || epoch == 0 {
		t.Fatal("C3 rollback fabricated replacement owner or undid removal epoch")
	}
	completion, err := s.completeOwedContactRecord(t.Context(), stage, real)
	if err != nil || completion.state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("same-incarnation protective rollback discarded earned cleanup", err)
	}
}

func TestContactRecordCancellationWhileWaitingForStoreMutexPreventsStage(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	candidate, verified := verifiedContactRecord(t, s, peer)
	store := s.secureSessions.store
	store.mu.Lock()
	held := true
	defer func() {
		if held {
			store.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.stageContactRecord(ctx, verified); done <- err }()
	waitContactRecordCondition(t, "stage did not reach actual gated mutex wait", func() bool {
		if s.trust.pinEvidenceMu.TryLock() {
			s.trust.pinEvidenceMu.Unlock()
			return false
		}
		return true
	})
	cancel()
	store.mu.Unlock()
	held = false
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled store waiter staged protection", err)
	}
	if _, ok := candidate.stagedOutcome(); ok || store.identityPinned(domain.PeerIdentityFromWire(peer.Address)) {
		t.Fatal("cancelled waiter left owed/protection effects")
	}
}

func TestContactRecordEvidenceGateCoversCommonMemoryMutationPaths(t *testing.T) {
	for _, operation := range []string{"common-merge", "own-merge", "contact-refresh", "contact-conflict", "forget-contact", "forget-session", "stage-fact"} {
		t.Run(operation, func(t *testing.T) {
			s, peer := localContactFixture(t)
			id := domain.PeerIdentityFromWire(peer.Address)
			intent := contactIntent(peer)
			_, applied, err := s.applyTrustedContactOrigin(t.Context(), peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "local", contactApplyExplicitLocal)
			if err != nil {
				t.Fatal(err)
			}
			record := buildContactRecord(t, peer, 1, domain.DeclaredMinSession(domain.SessionVersionSecure))
			body, err := protocol.VerifyIdentityRecord(record, domain.NetworkID(networkName), id)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "forget-session" {
				if _, err := s.trust.forget(id); err != nil {
					t.Fatal(err)
				}
				if _, err := s.trust.rememberRecord(domain.NetworkID(networkName), record, body); err != nil {
					t.Fatal(err)
				}
			}
			s.trust.pinEvidenceMu.RLock()
			held := true
			defer func() {
				if held {
					s.trust.pinEvidenceMu.RUnlock()
				}
			}()
			done := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "common-merge":
					_, err = s.trust.rememberRecord(domain.NetworkID(networkName), record, body)
				case "own-merge":
					_, err = s.trust.rememberOwnRecord(domain.NetworkID(networkName), record, body)
				case "contact-refresh":
					_, err = s.trust.remember(trustedContact{Address: peer.Address, PubKey: string(intent.PubKey), BoxKey: string(intent.BoxKey), BoxSignature: string(intent.BoxSignature)})
				case "contact-conflict":
					_, err = s.trust.remember(trustedContact{Address: peer.Address, PubKey: "conflicting-public-key"})
					if errors.Is(err, errTrustConflict) {
						err = nil
					}
				case "forget-contact", "forget-session":
					_, err = s.trust.forget(id)
				case "stage-fact":
					_, err = s.trust.stageBoundLocalContact(t.Context(), applied)
				}
				done <- err
			}()
			waitContactRecordCondition(t, "common memory path bypassed K2 evidence writer gate: "+operation, func() bool {
				if s.trust.pinEvidenceMu.TryRLock() {
					s.trust.pinEvidenceMu.RUnlock()
					return false
				}
				return true
			})
			select {
			case err := <-done:
				t.Fatal("mutation completed while original evidence read gate held", err)
			default:
			}
			s.trust.pinEvidenceMu.RUnlock()
			held = false
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContactRecordC3DuringUnlockedBarrierReportsStalePartialEffects(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, files := stagedContactRecord(t, s, peer)
	id := domain.PeerIdentityFromWire(peer.Address)
	impostor := legacyOutboundSession(legacyAttackerAddress, id)
	s.peerMu.Lock()
	s.sessions[impostor.address] = impostor
	s.peerMu.Unlock()
	// Unit owner models one previously admitted writer; the separate dispatcher
	// regression above proves actual late-frame storage and purge behavior.
	s.unprovenRouting.enter(id)
	s.unprovenRouting.note(id, time.Now())
	type completed struct {
		result contactRecordCompletion
		err    error
	}
	done := make(chan completed, 1)
	go func() {
		result, err := s.completeOwedContactRecord(t.Context(), stage, files)
		done <- completed{result, err}
	}()
	waitContactRecordCondition(t, "completion did not reach unlocked legacy barrier", func() bool {
		s.unprovenRouting.mu.Lock()
		waiting := len(s.unprovenRouting.drained[id]) > 0
		s.unprovenRouting.mu.Unlock()
		return waiting
	})
	removeContactPinForTest(t, s, id)
	publishContactProof(t, s, peer)
	before := metadataSnapshot(t, s.secureSessions.store, id)
	s.releaseRoutingInput(id)
	result := <-done
	if !errors.Is(result.err, errPinMetadataSnapshotStale) || result.result.state != contactRecordStaleOwner || !result.result.barrierAttempted || !result.result.attemptCovered {
		t.Fatalf("C3 race hid partial barrier effects: %+v %v", result.result, result.err)
	}
	if after := metadataSnapshot(t, s.secureSessions.store, id); after != before {
		t.Fatal("stale post-barrier ticket changed replacement entry")
	}
	first := result.result
	replayed, replayErr := s.completeOwedContactRecord(t.Context(), stage, NewMockidentityPinDurableFiles(t))
	if !errors.Is(replayErr, errPinMetadataSnapshotStale) || replayed.attemptCovered {
		t.Fatal("cached stale result falsely reports a new strict covering write", replayErr)
	}
	expected := first
	expected.attemptCovered = false
	if replayed != expected {
		t.Fatal("stale replay changed historical receipt/barrier/state")
	}
	stage.owed.mu.Lock()
	stored := stage.owed.completion
	stage.owed.mu.Unlock()
	if stored != first {
		t.Fatal("stale replay mutated stored first-attempt history")
	}
}
