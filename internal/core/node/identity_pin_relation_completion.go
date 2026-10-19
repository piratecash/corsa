package node

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

type owedRelationProtection struct {
	service  *Service
	binding  identityPinEntryBinding
	relation domain.IdentityPinAllocation
}
type relationRecordCoverReceipt struct {
	binding                   identityPinEntryBinding
	earnedRelation            domain.IdentityPinAllocation
	coveredProtectionRevision identityPinProtectionRevision
	snapshotDigest            domain.IdentityPinSnapshotDigest
}
type relationRecordCompletionState uint8

const (
	relationRecordStagedOwed relationRecordCompletionState = iota + 1
	relationRecordCoveredOwed
	relationRecordHistoricalCompleted
	relationRecordStaleOwner
	relationRecordDeferredInvariant
)

type relationRecordCompletion struct {
	state            relationRecordCompletionState
	receipt          relationRecordCoverReceipt
	attemptCovered   bool
	barrierAttempted bool
}

var errRelationRecordInvariant = errors.New("earned relation record protection changed")

// checkLocked requires store.mu, never the queue owner or a current proof token.
func (owed owedRelationProtection) checkLocked(store *secureSessionStore) error {
	if err := store.pinMetadataMutationRefusalLocked(); err != nil {
		return err
	}
	if !owed.binding.matchesLocked(store) {
		return errPinMetadataSnapshotStale
	}
	if store.identities[owed.binding.identity].classMetadata().Relation != owed.relation {
		return errRelationRecordInvariant
	}
	return nil
}
func (s *Service) checkOwedRelationRecord(owed owedRelationProtection) error {
	store := s.localContactPinStore()
	if store == nil || owed.service != s || owed.binding.owner != store {
		return errPinMetadataSnapshotStale
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return owed.checkLocked(store)
}

// A historical covering receipt cannot acknowledge a newer concurrent image.
// Capture the N-in fixed stamp BEFORE I/O; never retain a full DTO for new ACK.
func (s *secureSessionStore) confirmRelationRecordCoverage(ctx context.Context, owed owedRelationProtection, files identityPinDurableFiles) (relationRecordCoverReceipt, error) {
	if err := ctx.Err(); err != nil {
		return relationRecordCoverReceipt{}, err
	}
	if err := files.Preflight(ctx); err != nil {
		return relationRecordCoverReceipt{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	if err := ctx.Err(); err != nil {
		return relationRecordCoverReceipt{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return relationRecordCoverReceipt{}, err
	}
	if err := owed.checkLocked(s); err != nil {
		s.mu.Unlock()
		return relationRecordCoverReceipt{}, err
	}
	file, covers := s.snapshotLocked(), s.changes
	ninStamp := file.InboundQuota.stamp()
	revisions := s.pinMetadataRevisionsLocked()
	receipt := relationRecordCoverReceipt{binding: owed.binding, earnedRelation: owed.relation, coveredProtectionRevision: s.identities[owed.binding.identity].protectionRevision}
	s.mu.Unlock()
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err != nil {
		return relationRecordCoverReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return relationRecordCoverReceipt{}, err
	}
	commit, err := files.WriteMain(ctx, raw)
	if err != nil || commit != secretfile.DurableCommitted {
		return relationRecordCoverReceipt{}, errors.Join(errPinMetadataCommitUnconfirmed, err)
	}
	s.mu.Lock()
	s.ackPinMetadataLocked(revisions, covers, digest)
	s.recordNinSnapshotWrittenLocked(ninStamp, true)
	s.mu.Unlock()
	receipt.snapshotDigest = digest
	return receipt, nil
}

// The sole Processing row calls this synchronously and retains its actual I/O
// owner until return. Cancellation never destroys an already-earned obligation.
func (s *Service) completeOwedRelationRecord(ctx context.Context, owed owedRelationProtection, previous relationRecordCompletion, files identityPinDurableFiles) (relationRecordCompletion, error) {
	result := previous
	result.attemptCovered = false
	classify := func(err error) (relationRecordCompletion, error) {
		if errors.Is(err, errPinMetadataSnapshotStale) {
			result.state = relationRecordStaleOwner
		}
		if errors.Is(err, errRelationRecordInvariant) {
			result.state = relationRecordDeferredInvariant
		}
		return result, err
	}
	if err := s.checkOwedRelationRecord(owed); err != nil {
		return classify(err)
	}
	receipt, err := owed.binding.owner.confirmRelationRecordCoverage(ctx, owed, files)
	if err != nil {
		return classify(err)
	}
	result.receipt, result.state, result.attemptCovered = receipt, relationRecordCoveredOwed, true
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := s.checkOwedRelationRecord(owed); err != nil {
		return classify(err)
	}
	result.barrierAttempted = true
	if err := s.publishIdentityProtection(ctx, owed.binding.identity); err != nil {
		return result, err
	}
	if err := s.checkOwedRelationRecord(owed); err != nil {
		return classify(err)
	}
	result.state = relationRecordHistoricalCompleted
	return result, nil
}
