package node

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func pinTransactionFixture(t *testing.T) (*secureSessionStore, domain.IdentityPinFenceBinding, domain.IdentityPinConfirmationToken, identityPinDurableFiles) {
	t.Helper()
	store := loadSecureSessionStore(filepath.Join(t.TempDir(), "pins.json"), newStoreClock().trusted())
	id := domain.PeerIdentity{21}
	if _, err := store.noteProvenOutbound(dialledAt("192.0.2.21:64646"), id, endpointProtectionNone()); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, id)
	store.mu.Lock()
	entry := store.identities[id]
	store.mu.Unlock()
	binding := pinBindingBackup(t, domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentity{22}, Store: "enrolled-store"}, RuntimeEpoch: "process-epoch", Generation: 1, Identity: id, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt})
	files, err := store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	if err := files.Preflight(context.Background()); errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Skip("strict durable backend unavailable on this platform")
	} else if err != nil {
		t.Fatal(err)
	}
	return store, binding, preview.Token, files
}

func TestIdentityPinTransactionCannotMutateBeforeExternalFence(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	fence := NewMockIdentityPinRemovalFence(t)
	injected := errors.New("broker outcome unknown")
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(domain.IdentityPinRemovalPermit{}, injected).Once()
	before := store.snapshotLocked()
	result, err := store.removeIdentityPin(context.Background(), binding, token, fence, files)
	if !errors.Is(err, injected) || result.Outcome != domain.IdentityPinRemovalNotCommitted {
		t.Fatalf("begin failure %v %v", result, err)
	}
	if !store.identityPinned(binding.Identity) || len(store.snapshotLocked().Identities) != len(before.Identities) {
		t.Fatal("begin failure changed pins")
	}
	if raw, err := files.ReadGuard(context.Background()); err == nil {
		t.Fatalf("begin failure wrote guard: %q", raw)
	}
}

func TestIdentityPinTransactionDeletesMemoryOnlyAfterReceipt(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	fence := NewMockIdentityPinRemovalFence(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "operation-1", Binding: binding, Generation: 2}
	fence.EXPECT().BeginRemoval(mock.Anything, binding).Return(permit, nil).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, receipt domain.IdentityPinReceipt) error {
		if !store.identityPinned(binding.Identity) {
			t.Error("pin deleted before external receipt acknowledgement")
		}
		if receipt.Kind != domain.IdentityPinReceiptCleanCommitted || receipt.Permit.OperationID != permit.OperationID {
			t.Errorf("wrong receipt: %+v", receipt)
		}
		return nil
	}).Once()
	result, err := store.removeIdentityPin(context.Background(), binding, token, fence, files)
	if err != nil || result.Outcome != domain.IdentityPinRemovalCommitted {
		t.Fatalf("remove %v %v", result, err)
	}
	if store.identityPinned(binding.Identity) {
		t.Fatal("committed pin remained")
	}
	if !store.endpointRequiresV2("192.0.2.21:64646") {
		t.Fatal("identity removal cleared endpoint requirement")
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if restarted.identityPinned(binding.Identity) {
		t.Fatal("successful removal was lost across restart")
	}
	if !restarted.endpointRequiresV2("192.0.2.21:64646") {
		t.Fatal("restart lost endpoint binding")
	}
}

func TestIdentityPinGuardRequiresOfflineRecoveryBeforeStartup(t *testing.T) {
	store, binding, _, files := pinTransactionFixture(t)
	permit := domain.IdentityPinRemovalPermit{OperationID: "guarded-operation", Binding: binding, Generation: 2}
	raw, err := json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit})
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := files.WriteGuard(context.Background(), raw); err != nil || outcome != secretfile.DurableCommitted {
		t.Fatalf("guard %v %v", outcome, err)
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if err := restarted.startupRefusal(); !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("a guarded store could start networking: %v", err)
	}
}

func TestIdentityPinFatalStorageDominatesCancelledRunExit(t *testing.T) {
	svc := newDialChoiceService(t)
	svc.secureSessions.store.mu.Lock()
	svc.secureSessions.store.pinRecovery = &identityPinRecoveryState{fatal: true, cause: domain.ErrIdentityPinRecoveryRequired}
	svc.secureSessions.store.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.Run(ctx); !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) {
		t.Fatalf("fatal storage exit was erased by cancellation: %v", err)
	}
}

func TestIdentityPinUnsupportedPreflightCannotFenceOrMutate(t *testing.T) {
	store, binding, token, _ := pinTransactionFixture(t)
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
	fence := NewMockIdentityPinRemovalFence(t)
	if _, err := store.removeIdentityPin(context.Background(), binding, token, fence, files); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatalf("unsupported preflight %v", err)
	}
	if !store.identityPinned(binding.Identity) {
		t.Fatal("unsupported backend changed pin")
	}
}

func TestIdentityPinRecoveryServiceCannotStartNetworking(t *testing.T) {
	prior, binding, _, _ := pinTransactionFixture(t)
	store := loadSecureSessionStore(filepath.Join(prior.dir, prior.name), newStoreClock().trusted())
	svc := newDialChoiceService(t)
	svc.secureSessions.store = store
	binding.Scope.Node = peerIdentityOf(t, svc)
	permit := domain.IdentityPinRemovalPermit{OperationID: "offline-service", Binding: binding, Generation: 2}
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	receipt, err := svc.RecoverIdentityPinProtection(context.Background(), permit, fence)
	if err != nil || receipt.Kind != domain.IdentityPinReceiptProtectionRestored {
		t.Fatalf("offline service %v %v", receipt, err)
	}
	if err := svc.Run(context.Background()); !errors.Is(err, domain.ErrIdentityPinRecoveryOfflineOnly) {
		t.Fatalf("recovery service started Run: %v", err)
	}
}
