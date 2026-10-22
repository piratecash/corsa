package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

const identityPinTransactionTimeout = 30 * time.Second
const identityPinGuardFormat = 2

type identityPinAuditPhase string

const (
	identityPinAuditRemovalWritten     identityPinAuditPhase = "removal_written"
	identityPinAuditProtectionRestored identityPinAuditPhase = "protection_restored"
)
const maxIdentityPinAuditRecords = 32

type identityPinAuditRecord struct {
	OperationID domain.IdentityPinOperationID `json:"operation_id"`
	Identity    domain.PeerIdentity           `json:"identity"`
	At          time.Time                     `json:"at"`
	Phase       identityPinAuditPhase         `json:"phase"`
}

type identityPinRecoveryState struct {
	permit *domain.IdentityPinRemovalPermit
	cause  error
	fatal  bool
}

type identityPinGuardRecord struct {
	Format uint8                           `json:"format"`
	Permit domain.IdentityPinRemovalPermit `json:"permit"`
}

func validIdentityPinBinding(binding domain.IdentityPinFenceBinding) bool {
	if _, err := binding.Backup.Restore(binding.Identity, binding.PreviousPinnedAt); err != nil {
		return false
	}
	// Validate the largest possible guard before BeginRemoval can change the
	// broker. Its operation id is bounded independently of the backup payload.
	probe := identityPinGuardRecord{Format: identityPinGuardFormat, Permit: domain.IdentityPinRemovalPermit{OperationID: domain.IdentityPinOperationID(strings.Repeat("x", 128)), Binding: binding, Generation: ^domain.IdentityPinFenceGeneration(0)}}
	raw, err := json.Marshal(probe)
	if err != nil || len(raw) > maxIdentityPinGuardBytes {
		return false
	}
	return !binding.Scope.Node.IsZero() && len(binding.Scope.Store) > 0 && len(binding.Scope.Store) <= 128 && len(binding.RuntimeEpoch) > 0 && len(binding.RuntimeEpoch) <= 128 && binding.Generation > 0 && !binding.Identity.IsZero() && binding.ProofRevision > 0 && !binding.PreviousPinnedAt.IsZero()
}
func sameIdentityPinBinding(a, b domain.IdentityPinFenceBinding) bool {
	at, bt := a.PreviousPinnedAt, b.PreviousPinnedAt
	a.PreviousPinnedAt, b.PreviousPinnedAt = time.Time{}, time.Time{}
	return a == b && at.Equal(bt)
}
func validIdentityPinPermit(permit domain.IdentityPinRemovalPermit) bool {
	return validIdentityPinBinding(permit.Binding) && len(permit.OperationID) > 0 && len(permit.OperationID) <= 128 && permit.Generation > permit.Binding.Generation
}
func sameIdentityPinPermit(a, b domain.IdentityPinRemovalPermit) bool {
	return a.OperationID == b.OperationID && a.Generation == b.Generation && sameIdentityPinBinding(a.Binding, b.Binding)
}

// checkIdentityPinRemovalLocked holds mu and the exclusive pinMutationGate.
func (s *secureSessionStore) checkIdentityPinRemovalLocked(binding domain.IdentityPinFenceBinding, token domain.IdentityPinConfirmationToken) error {
	if s.unreadable {
		return domain.ErrIdentityPinStoreUnavailable
	}
	if s.pinRecovery != nil {
		return domain.ErrIdentityPinRecoveryRequired
	}
	if !validIdentityPinBinding(binding) {
		return domain.ErrIdentityPinFenceMismatch
	}
	if s.pinRemovalScope != nil && *s.pinRemovalScope != binding.Scope {
		return domain.ErrIdentityPinFenceMismatch
	}
	if err := s.validateIdentityPinTokenLocked(binding.Identity, token); err != nil {
		return err
	}
	entry := s.identities[binding.Identity]
	backup, err := domain.NewIdentityPinBackup(binding.Identity, entry.pinnedAt, entry.classMetadata())
	if err != nil || backup != binding.Backup {
		return domain.ErrIdentityPinConfirmationStale
	}
	if domain.IdentityPinProofRevision(entry.evidenceRevision) != binding.ProofRevision || !entry.pinnedAt.Equal(binding.PreviousPinnedAt) {
		return domain.ErrIdentityPinConfirmationStale
	}
	return nil
}

// removeIdentityPin is an internal port, never an admission capability. Gate
// order: pinMutationGate (exclusive) → writeMu → mu (short, no I/O).
func (s *secureSessionStore) removeIdentityPin(ctx context.Context, binding domain.IdentityPinFenceBinding, token domain.IdentityPinConfirmationToken, fence IdentityPinRemovalFence, files identityPinDurableFiles) (domain.IdentityPinRemovalResult, error) {
	result := domain.IdentityPinRemovalResult{Identity: binding.Identity, Outcome: domain.IdentityPinRemovalNotCommitted}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if s.dir == "" || fence == nil || files == nil {
		return result, domain.ErrIdentityPinRemovalUnavailable
	}
	if err := files.Preflight(ctx); err != nil {
		return result, err
	}
	s.pinMutationGate.Lock()
	defer s.pinMutationGate.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	s.mu.Lock()
	err := s.checkIdentityPinRemovalLocked(binding, token)
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	if _, err := files.ReadGuard(ctx); !errors.Is(err, fs.ErrNotExist) {
		if err == nil {
			err = domain.ErrIdentityPinRecoveryRequired
		}
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Invalidate absent observations before any external destructive effect.
	// Every later error/Unknown/protective restore retains this epoch.
	s.mu.Lock()
	// Waiting for mu may have outlived the last cancellation check.
	if err = ctx.Err(); err == nil {
		err = s.advancePinRemovalEpochLocked()
	}
	s.mu.Unlock()
	if err != nil {
		return result, err
	}
	permit, err := fence.BeginRemoval(ctx, binding)
	if err != nil {
		return result, err
	} // Unknown broker outcome grants no local write.
	if !validIdentityPinPermit(permit) || !sameIdentityPinBinding(permit.Binding, binding) {
		return result, domain.ErrIdentityPinFenceMismatch
	}
	// From this boundary caller cancellation cannot abandon protective cleanup.
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), identityPinTransactionTimeout)
	defer cancel()
	s.mu.Lock()
	s.pinRecovery = &identityPinRecoveryState{permit: &permit, cause: domain.ErrIdentityPinRecoveryRequired}
	s.mu.Unlock()
	return s.commitIdentityPinRemoval(work, permit, fence, files)
}

func (s *secureSessionStore) identityPinSnapshot(permit domain.IdentityPinRemovalPermit, phase identityPinAuditPhase) (secureSessionFile, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file := s.snapshotLocked()
	scope := permit.Binding.Scope
	file.PinStoreIdentity = &scope
	audit := identityPinAuditRecord{OperationID: permit.OperationID, Identity: permit.Binding.Identity, At: s.now(), Phase: phase}
	file.PinRemovalAudit = appendIdentityPinAudit(file.PinRemovalAudit, audit)
	if phase == identityPinAuditRemovalWritten {
		delete(file.Identities, permit.Binding.Identity)
		delete(file.IdentityMetadata, permit.Binding.Identity)
	}
	return file, s.changes
}

func cloneIdentityPinStoreIdentity(scope *domain.IdentityPinStoreIdentity) *domain.IdentityPinStoreIdentity {
	if scope == nil {
		return nil
	}
	copy := *scope
	return &copy
}

func appendIdentityPinAudit(previous []identityPinAuditRecord, record identityPinAuditRecord) []identityPinAuditRecord {
	audit := append([]identityPinAuditRecord(nil), previous...)
	if len(audit) >= maxIdentityPinAuditRecords {
		audit = audit[len(audit)-maxIdentityPinAuditRecords+1:]
	}
	return append(audit, record)
}

func encodeIdentityPinSnapshot(file secureSessionFile) ([]byte, domain.IdentityPinSnapshotDigest, error) {
	if err := validateSecureSessionPinClasses(file); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(file)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(raw)
	return raw, domain.IdentityPinSnapshotDigest(hex.EncodeToString(digest[:])), nil
}
func confirmedIdentityPinWrite(outcome secretfile.DurableCommit, err error) error {
	if err != nil {
		return err
	}
	if outcome != secretfile.DurableCommitted {
		return fmt.Errorf("identity pin save not durably confirmed: %d", outcome)
	}
	return nil
}

func (s *secureSessionStore) commitIdentityPinRemoval(ctx context.Context, permit domain.IdentityPinRemovalPermit, fence IdentityPinRemovalFence, files identityPinDurableFiles) (domain.IdentityPinRemovalResult, error) {
	guard, err := json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit})
	if err == nil {
		err = confirmedIdentityPinWrite(files.WriteGuard(ctx, guard))
	}
	if err != nil {
		return s.rollbackIdentityPinRemoval(ctx, permit, fence, files, err)
	}
	file, covers := s.identityPinSnapshot(permit, identityPinAuditRemovalWritten)
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err == nil {
		err = confirmedIdentityPinWrite(files.WriteMain(ctx, raw))
	}
	if err != nil {
		return s.rollbackIdentityPinRemoval(ctx, permit, fence, files, err)
	}
	if err = confirmedIdentityPinWrite(files.RemoveGuard(ctx)); err != nil {
		return s.rollbackIdentityPinRemoval(ctx, permit, fence, files, err)
	}
	receipt := domain.IdentityPinReceipt{Permit: permit, Kind: domain.IdentityPinReceiptCleanCommitted, SnapshotDigest: digest}
	if err = fence.RecordReceipt(ctx, receipt); err != nil {
		return s.rollbackIdentityPinRemoval(ctx, permit, fence, files, err)
	}
	s.mu.Lock()
	s.deletePinEntryLocked(permit.Binding.Identity)
	s.publishIdentityPinSnapshotLocked(file, covers)
	s.mu.Unlock()
	s.logIdentityPinOutcome(permit, domain.IdentityPinRemovalCommitted, nil)
	return domain.IdentityPinRemovalResult{OperationID: permit.OperationID, Identity: permit.Binding.Identity, Outcome: domain.IdentityPinRemovalCommitted}, nil
}

// publishIdentityPinSnapshotLocked holds mu. Endpoint mutations made during
// I/O remain dirty; no counter may jump over changes absent from this snapshot.
func (s *secureSessionStore) publishIdentityPinSnapshotLocked(file secureSessionFile, covers uint64) {
	s.recordNinSnapshotWrittenLocked(file.InboundQuota.stamp(), true)
	unchanged := s.changes == covers
	s.pinRemovalScope = cloneIdentityPinStoreIdentity(file.PinStoreIdentity)
	s.pinRemovalAudit = append([]identityPinAuditRecord(nil), file.PinRemovalAudit...)
	s.changes++
	if unchanged {
		s.markWrittenLocked(s.changes)
	} else {
		s.markWrittenLocked(covers)
	}
	// Never clear renewalsUnwritten here: an expiry sweep can set it during I/O.
}

func (s *secureSessionStore) rollbackIdentityPinRemoval(ctx context.Context, permit domain.IdentityPinRemovalPermit, fence IdentityPinRemovalFence, files identityPinDurableFiles, cause error) (domain.IdentityPinRemovalResult, error) {
	result := domain.IdentityPinRemovalResult{OperationID: permit.OperationID, Identity: permit.Binding.Identity, Outcome: domain.IdentityPinRemovalRecoveryRequired}
	file, covers := s.identityPinSnapshot(permit, identityPinAuditProtectionRestored)
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err == nil {
		err = confirmedIdentityPinWrite(files.WriteMain(ctx, raw))
	}
	if err != nil {
		failure := errors.Join(domain.ErrIdentityPinRecoveryRequired, cause, err)
		nextRecovery := &identityPinRecoveryState{permit: &permit, cause: failure, fatal: true}
		s.mu.Lock()
		publication := s.beginPinPublication()
		s.pinRecovery = nextRecovery
		if publication != nil {
			publication.Complete()
		}
		s.mu.Unlock()
		s.logIdentityPinOutcome(permit, result.Outcome, failure)
		return result, failure
	}
	s.mu.Lock()
	s.publishIdentityPinSnapshotLocked(file, covers)
	s.mu.Unlock()
	cleanup := confirmedIdentityPinWrite(files.RemoveGuard(ctx))
	var receiptErr error
	if cleanup == nil {
		receiptErr = fence.RecordReceipt(ctx, domain.IdentityPinReceipt{Permit: permit, Kind: domain.IdentityPinReceiptProtectionRestored, SnapshotDigest: digest})
	}
	result.Outcome = domain.IdentityPinRemovalNotCommitted
	if cleanup != nil || receiptErr != nil {
		result.Outcome = domain.IdentityPinRemovalIndeterminate
	}
	failure := errors.Join(cause, cleanup, receiptErr)
	s.logIdentityPinOutcome(permit, result.Outcome, failure)
	return result, failure
}

func (s *secureSessionStore) logIdentityPinOutcome(permit domain.IdentityPinRemovalPermit, outcome domain.IdentityPinRemovalOutcome, cause error) {
	s.logger.Info().Str("operation_id", string(permit.OperationID)).Str("identity", permit.Binding.Identity.String()).Str("outcome", string(outcome)).Err(cause).Msg("identity_pin_removal")
}
