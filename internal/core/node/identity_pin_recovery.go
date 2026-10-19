package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
)

func validateIdentityPinMetadata(file secureSessionFile) error {
	scope := file.PinStoreIdentity
	if scope != nil && (scope.Node.IsZero() || len(scope.Store) == 0 || len(scope.Store) > 128) {
		return domain.ErrIdentityPinGuardInvalid
	}
	if len(file.PinRemovalAudit) > maxIdentityPinAuditRecords {
		return domain.ErrIdentityPinGuardInvalid
	}
	for _, record := range file.PinRemovalAudit {
		if scope == nil || record.Identity.IsZero() || len(record.OperationID) == 0 || len(record.OperationID) > 128 || record.At.IsZero() {
			return domain.ErrIdentityPinGuardInvalid
		}
		switch record.Phase {
		case identityPinAuditRemovalWritten, identityPinAuditProtectionRestored:
		default:
			return domain.ErrIdentityPinGuardInvalid
		}
	}
	return nil
}

func decodeIdentityPinGuard(raw []byte) (domain.IdentityPinRemovalPermit, error) {
	var record identityPinGuardRecord
	if len(raw) == 0 || len(raw) > maxIdentityPinGuardBytes {
		return domain.IdentityPinRemovalPermit{}, domain.ErrIdentityPinGuardInvalid
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return domain.IdentityPinRemovalPermit{}, fmt.Errorf("%w: %w", domain.ErrIdentityPinGuardInvalid, err)
	}
	if record.Format != identityPinGuardFormat || !validIdentityPinPermit(record.Permit) {
		return domain.IdentityPinRemovalPermit{}, domain.ErrIdentityPinGuardInvalid
	}
	return record.Permit, nil
}

// inspectIdentityPinGuard runs before the store is shared. A guard is never
// silently removed or treated as permission to listen; offline recovery owns it.
func (s *secureSessionStore) inspectIdentityPinGuard() {
	if s.dir == "" || s.refusal != nil {
		return
	}
	files, err := s.openIdentityPinFiles()
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		s.pinRecovery = &identityPinRecoveryState{cause: err}
		return
	}
	defer func() { _ = files.Close() }()
	// Construction has no caller context, like the existing main-store load.
	raw, err := files.ReadGuard(context.Background())
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		s.pinRecovery = &identityPinRecoveryState{cause: err}
		return
	}
	permit, err := decodeIdentityPinGuard(raw)
	if err != nil {
		s.pinRecovery = &identityPinRecoveryState{cause: err}
		return
	}
	if s.pinRemovalScope != nil && *s.pinRemovalScope != permit.Binding.Scope {
		s.pinRecovery = &identityPinRecoveryState{cause: domain.ErrIdentityPinFenceMismatch}
		return
	}
	s.pinRecovery = &identityPinRecoveryState{permit: &permit, cause: domain.ErrIdentityPinRecoveryRequired}
}

func (s *secureSessionStore) recoverIdentityPinProtection(ctx context.Context, permit domain.IdentityPinRemovalPermit, fence IdentityPinRemovalFence, files identityPinDurableFiles) (domain.IdentityPinReceipt, error) {
	if err := ctx.Err(); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	if !validIdentityPinPermit(permit) {
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinFenceMismatch
	}
	if s.dir == "" || fence == nil || files == nil {
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinRemovalUnavailable
	}
	if err := files.Preflight(ctx); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	s.pinMutationGate.Lock()
	defer s.pinMutationGate.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	s.mu.Lock()
	if s.pinRecovery != nil && s.pinRecovery.fatal {
		s.mu.Unlock()
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinRecoveryRequired
	}
	if s.unreadable || !s.pinMainLoaded {
		s.mu.Unlock()
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinStoreUnavailable
	}
	if s.pinRemovalScope != nil && *s.pinRemovalScope != permit.Binding.Scope {
		s.mu.Unlock()
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinFenceMismatch
	}
	s.mu.Unlock()
	raw, err := files.ReadGuard(ctx)
	if err == nil {
		guarded, decodeErr := decodeIdentityPinGuard(raw)
		if decodeErr != nil {
			return domain.IdentityPinReceipt{}, decodeErr
		}
		if !sameIdentityPinPermit(guarded, permit) {
			return domain.IdentityPinReceipt{}, domain.ErrIdentityPinFenceMismatch
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return domain.IdentityPinReceipt{}, err
	}
	// A trusted external permit also restores protection when cleanup removed
	// the guard before its namespace sync or a later rollback failed.
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), identityPinTransactionTimeout)
	defer cancel()
	record, err := permit.Binding.Backup.Restore(permit.Binding.Identity, permit.Binding.PreviousPinnedAt)
	if err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	s.mu.Lock()
	if _, found := s.identities[permit.Binding.Identity]; !found {
		if len(s.identities) >= maxPinnedIdentities {
			s.mu.Unlock()
			return domain.IdentityPinReceipt{}, errPinStoreFull
		}
		if err := s.restoreHistoricalPinLocked(historicalPinEvidence{identity: permit.Binding.Identity, at: permit.Binding.PreviousPinnedAt}); err != nil {
			s.mu.Unlock()
			return domain.IdentityPinReceipt{}, err
		}
		entry := s.identities[permit.Binding.Identity]
		if err := entry.setUnpublishedMetadata(record.Metadata); err != nil {
			s.mu.Unlock()
			return domain.IdentityPinReceipt{}, err
		}
		s.putPinEntryLocked(permit.Binding.Identity, entry)
		s.changes++
	} else if err := s.identities[permit.Binding.Identity].classMetadata().Validate(); err != nil {
		s.mu.Unlock()
		return domain.IdentityPinReceipt{}, err
	}
	// A later normal proof may already have restored this identity with newer
	// facts. Protective recovery keeps that complete entry instead of replacing
	// it with the earlier removal permit's backup.
	s.pinRecovery = &identityPinRecoveryState{permit: &permit, cause: domain.ErrIdentityPinRecoveryRequired}
	s.mu.Unlock()
	file, covers := s.identityPinSnapshot(permit, identityPinAuditProtectionRestored)
	encoded, digest, err := encodeIdentityPinSnapshot(file)
	if err == nil {
		err = confirmedIdentityPinWrite(files.WriteMain(work, encoded))
	}
	if err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	s.mu.Lock()
	s.publishIdentityPinSnapshotLocked(file, covers)
	s.mu.Unlock()
	if err := confirmedIdentityPinWrite(files.RemoveGuard(work)); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	receipt := domain.IdentityPinReceipt{Permit: permit, Kind: domain.IdentityPinReceiptProtectionRestored, SnapshotDigest: digest}
	if err := fence.RecordReceipt(work, receipt); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	s.logIdentityPinOutcome(permit, domain.IdentityPinRemovalNotCommitted, nil)
	return receipt, nil
}

// identityPinStartupRefusalLocked holds mu; only process-startup errors expose
// artifact paths. RPC and operation results receive pathless domain errors.
func (s *secureSessionStore) identityPinStartupRefusalLocked(reason domain.PinStoreRefusalReason, cause error) *PinStoreRefusalError {
	return &PinStoreRefusalError{Reason: reason, File: filepath.Join(s.dir, s.name), problem: "identity pin recovery or supervised startup is required", fix: "use the verified external recovery launcher before starting networking; retain the guard and the main store", cause: cause}
}

func (s *Service) identityPinFatalError() error {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return nil
	}
	store := s.secureSessions.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.pinRecovery != nil && store.pinRecovery.fatal {
		return errors.Join(domain.ErrIdentityPinRecoveryRequired, store.pinRecovery.cause)
	}
	return nil
}

func (s *Service) beginIdentityPinNetworkRun() error {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return nil
	}
	store := s.secureSessions.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.pinOfflineRecovery {
		return domain.ErrIdentityPinRecoveryOfflineOnly
	}
	store.pinNetworkStarted = true
	return nil
}

// RecoverIdentityPinProtection is a restoration-only offline port. The verified
// launcher adapter supplies its exact recovery permit and exits after this call;
// this Service is irreversibly barred from Run, RPC and peer listeners.
func (s *Service) RecoverIdentityPinProtection(ctx context.Context, permit domain.IdentityPinRemovalPermit, fence IdentityPinRemovalFence) (domain.IdentityPinReceipt, error) {
	if err := ctx.Err(); err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	if s.secureSessions == nil || s.secureSessions.store == nil || s.identity == nil {
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinStoreUnavailable
	}
	if permit.Binding.Scope.Node != domain.PeerIdentityFromWire(s.identity.Address) {
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinFenceMismatch
	}
	store := s.secureSessions.store
	store.mu.Lock()
	if store.pinNetworkStarted || store.pinOfflineRecovery {
		store.mu.Unlock()
		return domain.IdentityPinReceipt{}, domain.ErrIdentityPinRecoveryOfflineOnly
	}
	store.pinOfflineRecovery = true
	store.mu.Unlock()
	files, err := store.openIdentityPinFiles()
	if err != nil {
		return domain.IdentityPinReceipt{}, err
	}
	defer func() { _ = files.Close() }()
	return store.recoverIdentityPinProtection(ctx, permit, fence, files)
}

// removeIdentityPinUsingFence is intentionally private. A test port or generic
// interface cannot enable public confirmation; admission needs the real adapter.
func (s *Service) removeIdentityPinUsingFence(ctx context.Context, binding domain.IdentityPinFenceBinding, token domain.IdentityPinConfirmationToken, fence IdentityPinRemovalFence, files identityPinDurableFiles) (domain.IdentityPinRemovalResult, error) {
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinStoreUnavailable
	}
	result, err := s.secureSessions.store.removeIdentityPin(ctx, binding, token, fence, files)
	if result.Outcome == domain.IdentityPinRemovalRecoveryRequired {
		s.runCancel()
	}
	return result, err
}
