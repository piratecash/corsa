package node

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// This receipt covers an immutable historical snapshot, not all newer metadata.
type contactRecordCoverReceipt struct {
	binding                   identityPinEntryBinding
	coveredProtectionRevision identityPinProtectionRevision
	earnedContact             domain.IdentityPinAllocation
	snapshotDigest            domain.IdentityPinSnapshotDigest
}

func (s *secureSessionStore) confirmContactRecordCoverage(ctx context.Context, owed owedContactProtection, files identityPinDurableFiles) (contactRecordCoverReceipt, error) {
	if err := ctx.Err(); err != nil {
		return contactRecordCoverReceipt{}, err
	}
	if err := files.Preflight(ctx); err != nil {
		return contactRecordCoverReceipt{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	if err := ctx.Err(); err != nil {
		return contactRecordCoverReceipt{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return contactRecordCoverReceipt{}, err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return contactRecordCoverReceipt{}, err
	}
	if err := owed.checkLocked(s); err != nil {
		s.mu.Unlock()
		return contactRecordCoverReceipt{}, err
	}
	file, covers := s.snapshotLocked(), s.changes
	quota := file.InboundQuota.stamp()
	revisions := s.pinMetadataRevisionsLocked()
	receipt := contactRecordCoverReceipt{binding: owed.binding, earnedContact: owed.contact, coveredProtectionRevision: s.identities[owed.binding.identity].protectionRevision}
	s.mu.Unlock()
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err != nil {
		return contactRecordCoverReceipt{}, err
	}
	if err := ctx.Err(); err != nil {
		return contactRecordCoverReceipt{}, err
	}
	commit, err := files.WriteMain(ctx, raw)
	if err != nil || commit != secretfile.DurableCommitted {
		return contactRecordCoverReceipt{}, errors.Join(errPinMetadataCommitUnconfirmed, err)
	}
	// Confirmed commit outranks late cancellation; the read removal gate still
	// excludes C3. Ordinary proof/metadata can advance and remain unACKed.
	s.mu.Lock()
	s.ackPinMetadataLocked(revisions, covers, digest)
	s.recordNinSnapshotWrittenLocked(quota, true)
	s.mu.Unlock()
	receipt.snapshotDigest = digest
	return receipt, nil
}
