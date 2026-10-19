package node

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func TestContactRecordOwedSurvivesPoststageChangesAndUnknown(t *testing.T) {
	for _, change := range []string{"contact-delete", "contact-readd", "newer-min1", "newer-no-min", "reproof", "unrelated-metadata"} {
		t.Run(change, func(t *testing.T) {
			s, peer := localContactFixture(t)
			candidate, stage, real := stagedContactRecord(t, s, peer)
			id := domain.PeerIdentityFromWire(peer.Address)
			store := s.secureSessions.store
			switch change {
			case "contact-delete":
				s.deleteTrustedContactFrame(id)
			case "contact-readd":
				s.deleteTrustedContactFrame(id)
				addLocalContact(t, s, peer)
			case "newer-min1":
				contactRecordFixture(t, s, peer, 2, domain.DeclaredMinSession(domain.SessionVersionLegacy))
			case "newer-no-min":
				contactRecordFixture(t, s, peer, 2, domain.AbsentMinSession())
			case "reproof":
				publishContactProof(t, s, peer)
			case "unrelated-metadata":
				next := metadataSnapshot(t, store, id).metadata
				next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: time.Now(), SourceGroup: domain.SourceGroupLoopback}
				stageMetadata(t, store, id, next)
			}
			before := metadataSnapshot(t, store, id)
			unknown := NewMockidentityPinDurableFiles(t)
			unknown.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			injected := errors.New("strict commit ACK unknown")
			unknown.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableIndeterminate, injected).Once()
			result, err := s.completeOwedContactRecord(t.Context(), stage, unknown)
			if !errors.Is(err, injected) || result.state != contactRecordStagedOwed || result.attemptCovered || result.receipt.snapshotDigest != "" || result.barrierAttempted {
				t.Fatalf("unknown coverage falsely complete: %+v %v", result, err)
			}
			if got, ok := candidate.stagedOutcome(); !ok || got != stage {
				t.Fatal("Unknown dropped owed stage")
			}
			result, err = s.completeOwedContactRecord(t.Context(), stage, real)
			if err != nil || result.state != contactRecordHistoricalProtectionCompleted || !result.attemptCovered || !result.barrierAttempted {
				t.Fatalf("earned cleanup suppressed by poststage change: %+v %v", result, err)
			}
			after := metadataSnapshot(t, store, id)
			if before != after {
				t.Fatal("completion re-staged or revised protection")
			}
		})
	}
}

func TestContactRecordLateCancelReceiptIsHistoricalNotNewAttemptACK(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		out, err := real.WriteMain(ctx, raw)
		cancel()
		return out, err
	}).Once()
	first, err := s.completeOwedContactRecord(ctx, stage, files)
	if !errors.Is(err, context.Canceled) || first.state != contactRecordCoveredOwed || !first.attemptCovered || first.receipt.snapshotDigest == "" || first.barrierAttempted {
		t.Fatalf("late commit/cancel lost owed receipt: %+v %v", first, err)
	}
	next := metadataSnapshot(t, store, id).metadata
	next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: time.Now(), SourceGroup: domain.SourceGroupLoopback}
	stageMetadata(t, store, id, next)
	failed := NewMockidentityPinDurableFiles(t)
	failed.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	failed.EXPECT().WriteMain(mock.Anything, mock.Anything).Return(secretfile.DurableNotCommitted, nil).Once()
	second, err := s.completeOwedContactRecord(t.Context(), stage, failed)
	if err == nil || second.attemptCovered || second.receipt != first.receipt || second.barrierAttempted {
		t.Fatal("old receipt was promoted to newer attempt/current metadata ACK", err)
	}
	store.mu.Lock()
	current := store.identities[id]
	store.mu.Unlock()
	if current.metadata.confirmedRevision == current.protectionRevision {
		t.Fatal("failed newer snapshot ACKed current metadata")
	}
	third, err := s.completeOwedContactRecord(t.Context(), stage, real)
	if err != nil || third.state != contactRecordHistoricalProtectionCompleted || !third.attemptCovered || third.receipt.coveredProtectionRevision <= first.receipt.coveredProtectionRevision {
		t.Fatal("same-ticket fresh covering retry failed", err)
	}
}

func TestContactRecordCoverReceiptSurvivesConcurrentMetadataWithoutACKingIt(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	captured := metadataSnapshot(t, store, id)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		// A concurrent memory owner may advance unrelated class metadata while
		// strict I/O owns writeMu. No public recursive RLock on the same path.
		done := make(chan struct{})
		go func() {
			defer close(done)
			next := metadataSnapshot(t, store, id).metadata
			next.Selected = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: time.Now(), SourceGroup: domain.SourceGroupLoopback}
			stageMetadata(t, store, id, next)
		}()
		<-done
		return real.WriteMain(ctx, raw)
	}).Once()
	result, err := s.completeOwedContactRecord(t.Context(), stage, files)
	if err != nil || result.state != contactRecordHistoricalProtectionCompleted || result.receipt.coveredProtectionRevision != captured.protectionRevision {
		t.Fatalf("historical coverage lost: %+v %v", result, err)
	}
	store.mu.Lock()
	entry := store.identities[id]
	dirty := store.changes > store.written
	store.mu.Unlock()
	if entry.protectionRevision <= captured.protectionRevision || entry.metadata.confirmedRevision == entry.protectionRevision || !dirty {
		t.Fatal("old snapshot falsely ACKed newer metadata")
	}
}

func TestContactRecordStrictOutcomeMatrixAndCopiedCompletion(t *testing.T) {
	for _, outcome := range []secretfile.DurableCommit{secretfile.DurableNotCommitted, secretfile.DurableIndeterminate, secretfile.DurableCommitted} {
		s, peer := localContactFixture(t)
		_, stage, real := stagedContactRecord(t, s, peer)
		files := NewMockidentityPinDurableFiles(t)
		files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
		files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
			if outcome == secretfile.DurableCommitted {
				return real.WriteMain(ctx, raw)
			}
			return outcome, nil
		}).Once()
		result, err := s.completeOwedContactRecord(t.Context(), stage, files)
		if (err == nil) != (outcome == secretfile.DurableCommitted) || result.attemptCovered != (outcome == secretfile.DurableCommitted) {
			t.Fatalf("strict outcome %v: %+v %v", outcome, result, err)
		}
	}
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(real.WriteMain).Once()
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			result, err := s.completeOwedContactRecord(t.Context(), stage, files)
			if err != nil || result.state != contactRecordHistoricalProtectionCompleted {
				t.Error("copied completion handle lost one shared obligation", err)
			}
		})
	}
	group.Wait()
}

func TestContactRecordCancelledBeforeEffectsAndLostEarnedKRemainOwed(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	files := NewMockidentityPinDurableFiles(t) // cancelled attempt performs no I/O
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := s.completeOwedContactRecord(ctx, stage, files)
	if !errors.Is(err, context.Canceled) || result.state != contactRecordStagedOwed || result.attemptCovered {
		t.Fatal("cancelled attempt discarded/confirmed stage", err)
	}
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	before := metadataSnapshot(t, store, id)
	next := before.metadata
	next.Contact = domain.IdentityPinAllocation{}
	if _, err := store.stagePinMetadata(before, next); !errors.Is(err, errContactRecordInvariant) {
		t.Fatal("ordinary mutation removed an owned Contact allocation", err)
	}
	if after := metadataSnapshot(t, store, id); after != before {
		t.Fatal("refused metadata mutation had effects")
	}
	// Deliberate out-of-contract corruption exercises the retained invariant
	// diagnostic; production owner mutations now refuse this loss.
	store.mu.Lock()
	entry := store.identities[id]
	broken := *entry.metadata
	broken.value = next
	entry.metadata = &broken
	store.putPinEntryLocked(id, entry)
	store.mu.Unlock()
	result, err = s.completeOwedContactRecord(t.Context(), stage, files)
	if !errors.Is(err, errContactRecordInvariant) || result.state != contactRecordDeferredInvariant || store.contactKStatus(id) != domain.LocalContactKNotEvaluated {
		t.Fatal("lost earned K was silently re-staged/dropped", err)
	}
	store.mu.Lock()
	entry = store.identities[id]
	restored := *entry.metadata
	restored.value = before.metadata
	entry.metadata = &restored
	store.putPinEntryLocked(id, entry)
	store.mu.Unlock()
	result, err = s.completeOwedContactRecord(t.Context(), stage, real)
	if err != nil || result.state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("retained invariant obligation could not finish", err)
	}
}

func TestContactRecordC3ReplacementOrRestartStalesOwedTicket(t *testing.T) {
	for _, variant := range []string{"remove-recreate", "restart"} {
		t.Run(variant, func(t *testing.T) {
			s, peer := localContactFixture(t)
			_, stage, _ := stagedContactRecord(t, s, peer)
			id := domain.PeerIdentityFromWire(peer.Address)
			store := s.secureSessions.store
			if variant == "remove-recreate" {
				removeContactPinForTest(t, s, id)
				if err := store.noteProvenInbound(id); err != nil {
					t.Fatal(err)
				}
			} else {
				s.secureSessions.store = loadSecureSessionStore("", newStoreClock().trusted())
			}
			files := NewMockidentityPinDurableFiles(t) // stale ticket must NOT write
			result, err := s.completeOwedContactRecord(t.Context(), stage, files)
			if !errors.Is(err, errPinMetadataSnapshotStale) || result.state != contactRecordStaleOwner || result.barrierAttempted {
				t.Fatal("old ticket attached to replacement owner", err)
			}
		})
	}
}

func TestContactRecordStagedOutcomeRetrievalAfterRuntimeStoreReplacement(t *testing.T) {
	s, peer := localContactFixture(t)
	candidate, stage, _ := stagedContactRecord(t, s, peer)
	s.secureSessions.store = loadSecureSessionStore("", newStoreClock().trusted())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := s.stageContactRecord(ctx, verifiedContactMinSessionRecord{candidate: candidate})
	if err != nil || got != stage {
		t.Fatal("historical already-staged outcome hidden by later owner replacement", err)
	}
	result, err := s.completeOwedContactRecord(t.Context(), got, NewMockidentityPinDurableFiles(t))
	if !errors.Is(err, errPinMetadataSnapshotStale) || result.state != contactRecordStaleOwner {
		t.Fatal("historical retrieval granted replacement authority", err)
	}
}

func TestContactRecordOrdinarySaveStillRequiresFreshStrictCoverage(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	if store.contactKStatus(domain.PeerIdentityFromWire(peer.Address)) != domain.LocalContactKStagedUnconfirmed {
		t.Fatal("ordinary save manufactured strict ACK")
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(real.WriteMain).Once()
	result, err := s.completeOwedContactRecord(t.Context(), stage, files)
	if err != nil || result.state != contactRecordHistoricalProtectionCompleted || !result.attemptCovered {
		t.Fatal("ordinary save substituted for strict cover", err)
	}
}

func TestContactRecordDeleteDuringStrictWriteAndCommittedError(t *testing.T) {
	for _, commitError := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete_during_write", true: "committed_with_error"}[commitError], func(t *testing.T) {
			s, peer := localContactFixture(t)
			_, stage, real := stagedContactRecord(t, s, peer)
			id := domain.PeerIdentityFromWire(peer.Address)
			files := NewMockidentityPinDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			injected := errors.New("committed namespace ACK error")
			files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
				s.deleteTrustedContactFrame(id)
				out, err := real.WriteMain(ctx, raw)
				if commitError && err == nil {
					return out, injected
				}
				return out, err
			}).Once()
			result, err := s.completeOwedContactRecord(t.Context(), stage, files)
			if commitError {
				if !errors.Is(err, injected) || result.state != contactRecordStagedOwed || result.attemptCovered || result.receipt.snapshotDigest != "" {
					t.Fatal("ambiguous ACK created strict receipt", err)
				}
				if s.secureSessions.store.contactKStatus(id) != domain.LocalContactKStagedUnconfirmed {
					t.Fatal("committed-with-error ACKed current metadata")
				}
			} else if err != nil || result.state != contactRecordHistoricalProtectionCompleted || !result.attemptCovered {
				t.Fatal("contact deletion during strict I/O revoked cleanup", err)
			}
		})
	}
}

type contactStageBoundaryContext struct {
	context.Context
	store  *secureSessionStore
	cancel context.CancelFunc
	once   sync.Once
}

func (ctx *contactStageBoundaryContext) Err() error {
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	if ctx.store.mu.TryLock() {
		ctx.store.mu.Unlock()
		return nil
	}
	// In this uncontended fixture, only the final stage check owns store.mu.
	// Cancellation AFTER that accepted check must not hide its real effects.
	ctx.once.Do(ctx.cancel)
	return nil
}
func TestContactRecordLateStageCancellationRetainsActualOutcome(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	candidate, verified := verifiedContactRecord(t, s, peer)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &contactStageBoundaryContext{Context: base, store: s.secureSessions.store, cancel: cancel}
	stage, err := s.stageContactRecord(ctx, verified)
	if err != nil || base.Err() == nil || stage.owed == nil {
		t.Fatal("late boundary cancel hid actual staged protection", err)
	}
	if actual, ok := candidate.stagedOutcome(); !ok || actual != stage {
		t.Fatal("staged outcome lost after immediate cancellation")
	}
}

func TestContactRecordCachedCompletionReportsNoNewCover(t *testing.T) {
	s, peer := localContactFixture(t)
	_, stage, real := stagedContactRecord(t, s, peer)
	first, err := s.completeOwedContactRecord(t.Context(), stage, real)
	if err != nil || !first.attemptCovered || first.state != contactRecordHistoricalProtectionCompleted {
		t.Fatal("first covering completion failed", err)
	}
	noIO := NewMockidentityPinDurableFiles(t)
	second, err := s.completeOwedContactRecord(t.Context(), stage, noIO)
	if err != nil || second.attemptCovered {
		t.Fatal("cached completion falsely reports a new strict covering write", err)
	}
	expected := first
	expected.attemptCovered = false
	if second != expected {
		t.Fatal("cached historical state/receipt changed")
	}
	stage.owed.mu.Lock()
	stored := stage.owed.completion
	stage.owed.mu.Unlock()
	if stored != first {
		t.Fatal("per-invocation flag rewrite mutated shared completed history")
	}
}
