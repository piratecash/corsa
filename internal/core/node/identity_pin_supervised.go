package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

// This composition boundary accepts only the concrete private-channel session.
// Business transactions retain domain ports; a fake port cannot admit startup.
func (s *Service) supervisedIdentityPinScope(session *startupfence.SupervisedSession) (domain.IdentityPinStoreIdentity, error) {
	if !session.Verified() || s.identity == nil || s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.IdentityPinStoreIdentity{}, domain.ErrIdentityPinSupervisionRequired
	}
	node := domain.PeerIdentityFromWire(s.identity.Address)
	supervisedNode, supervisedStore, err := session.NodeStore()
	if err != nil {
		return domain.IdentityPinStoreIdentity{}, err
	}
	scope := domain.IdentityPinStoreIdentity{Node: supervisedNode, Store: domain.IdentityPinStoreID(supervisedStore)}
	if scope.Node != node || len(scope.Store) == 0 || len(scope.Store) > 128 {
		return domain.IdentityPinStoreIdentity{}, domain.ErrIdentityPinFenceMismatch
	}
	return scope, nil
}

// AdmitSupervisedIdentityPinStartup admits ordinary startup only. It does not
// enable removal or turn an offline recovery grant into network permission.
func (s *Service) AdmitSupervisedIdentityPinStartup(session *startupfence.SupervisedSession) error {
	scope, err := s.supervisedIdentityPinScope(session)
	if err != nil {
		return err
	}
	if _, recovery := session.RecoveryPermit(); recovery {
		return domain.ErrIdentityPinRecoveryOfflineOnly
	}
	store := s.secureSessions.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.pinNetworkStarted || store.pinOfflineRecovery {
		return domain.ErrIdentityPinRecoveryOfflineOnly
	}
	if store.dir == "" || store.unreadable {
		return domain.ErrIdentityPinStoreUnavailable
	}
	if store.pinRemovalScope != nil && *store.pinRemovalScope != scope {
		return domain.ErrIdentityPinFenceMismatch
	}
	store.pinSupervisedStartup = true
	return nil
}

// RecoverSupervisedIdentityPinProtection runs before App.Run and every listener.
// The caller closes the private channel only after all teardown, then exits.
func (s *Service) RecoverSupervisedIdentityPinProtection(ctx context.Context, session *startupfence.SupervisedSession) error {
	scope, err := s.supervisedIdentityPinScope(session)
	if err != nil {
		return err
	}
	permit, recovery := session.RecoveryPermit()
	if !recovery {
		return domain.ErrIdentityPinRecoveryOfflineOnly
	}
	trusted := identityPinPermitFromSupervisor(permit)
	if trusted.Binding.Scope != scope {
		return domain.ErrIdentityPinFenceMismatch
	}
	_, err = s.RecoverIdentityPinProtection(ctx, trusted, supervisedIdentityPinFence{session: session})
	return err
}

// Candidate tests exercise the real adapter through this private entry point.
// Public ConfirmIdentityPinRemoval stays unavailable until workflow admission.
func (s *Service) removeSupervisedIdentityPin(ctx context.Context, session *startupfence.SupervisedSession, id domain.PeerIdentity, token domain.IdentityPinConfirmationToken) (domain.IdentityPinRemovalResult, error) {
	scope, err := s.supervisedIdentityPinScope(session)
	if err != nil {
		return domain.IdentityPinRemovalResult{}, err
	}
	if _, recovery := session.RecoveryPermit(); recovery {
		return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinRecoveryOfflineOnly
	}
	store := s.secureSessions.store
	store.mu.Lock()
	entry, found := store.identities[id]
	admitted := store.pinSupervisedStartup && !store.pinOfflineRecovery
	store.mu.Unlock()
	if !admitted {
		return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinSupervisionRequired
	}
	if !found {
		return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinNotFound
	}
	supervisedBinding, err := session.Binding(id, startupfence.PinRevision(entry.evidenceRevision), entry.pinnedAt, entry.classMetadata())
	if err != nil {
		return domain.IdentityPinRemovalResult{}, err
	}
	binding := identityPinBindingFromSupervisor(supervisedBinding)
	if binding.Scope != scope {
		return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinFenceMismatch
	}
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return domain.IdentityPinRemovalResult{}, err
	}
	defer func() { _ = files.Close() }()
	return s.removeIdentityPinUsingFence(ctx, binding, token, supervisedIdentityPinFence{session: session}, files)
}

func identityPinBindingFromSupervisor(binding startupfence.Binding) domain.IdentityPinFenceBinding {
	return domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: binding.Node, Store: domain.IdentityPinStoreID(binding.Store)}, RuntimeEpoch: domain.IdentityPinRuntimeEpoch(binding.RuntimeEpoch), Generation: domain.IdentityPinFenceGeneration(binding.Generation), Identity: binding.Identity, ProofRevision: domain.IdentityPinProofRevision(binding.PinRevision), PreviousPinnedAt: binding.PreviousPinnedAt, Backup: binding.Backup}
}
func identityPinBindingToSupervisor(binding domain.IdentityPinFenceBinding) startupfence.Binding {
	return startupfence.Binding{Node: binding.Scope.Node, Store: startupfence.StoreID(binding.Scope.Store), RuntimeEpoch: startupfence.RuntimeEpoch(binding.RuntimeEpoch), Generation: startupfence.Generation(binding.Generation), Identity: binding.Identity, PinRevision: startupfence.PinRevision(binding.ProofRevision), PreviousPinnedAt: binding.PreviousPinnedAt, Backup: binding.Backup}
}
func identityPinPermitFromSupervisor(permit startupfence.RemovalPermit) domain.IdentityPinRemovalPermit {
	return domain.IdentityPinRemovalPermit{OperationID: permit.Operation, Binding: identityPinBindingFromSupervisor(permit.Binding), Generation: domain.IdentityPinFenceGeneration(permit.Generation)}
}
func identityPinPermitToSupervisor(permit domain.IdentityPinRemovalPermit) startupfence.RemovalPermit {
	return startupfence.RemovalPermit{Operation: permit.OperationID, Binding: identityPinBindingToSupervisor(permit.Binding), Generation: startupfence.Generation(permit.Generation)}
}

type supervisedIdentityPinFence struct {
	session *startupfence.SupervisedSession
}

func (f supervisedIdentityPinFence) BeginRemoval(ctx context.Context, binding domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error) {
	if !f.session.Verified() {
		return domain.IdentityPinRemovalPermit{}, domain.ErrIdentityPinSupervisionRequired
	}
	permit, err := f.session.BeginRemoval(ctx, identityPinBindingToSupervisor(binding))
	return identityPinPermitFromSupervisor(permit), err
}
func (f supervisedIdentityPinFence) RecordReceipt(ctx context.Context, receipt domain.IdentityPinReceipt) error {
	if !f.session.Verified() {
		return domain.ErrIdentityPinSupervisionRequired
	}
	var kind startupfence.ReceiptKind
	switch receipt.Kind {
	case domain.IdentityPinReceiptCleanCommitted:
		kind = startupfence.ReceiptCleanCommitted
	case domain.IdentityPinReceiptProtectionRestored:
		kind = startupfence.ReceiptProtectionRestored
	default:
		return domain.ErrIdentityPinFenceMismatch
	}
	return f.session.RecordReceipt(ctx, startupfence.Receipt{Permit: identityPinPermitToSupervisor(receipt.Permit), Kind: kind, SnapshotDigest: string(receipt.SnapshotDigest)})
}
