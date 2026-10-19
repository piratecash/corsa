package node

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

type identityPinEntryBinding struct {
	owner       *secureSessionStore
	identity    domain.PeerIdentity
	incarnation identityPinProtectionRevision
}

// pinEntryBindingLocked requires the store mu.
func (s *secureSessionStore) pinEntryBindingLocked(id domain.PeerIdentity) identityPinEntryBinding {
	entry, ok := s.identities[id]
	if !ok || entry.incarnation == 0 || entry.evidenceRevision == 0 || entry.protectionRevision == 0 {
		return identityPinEntryBinding{}
	}
	return identityPinEntryBinding{owner: s, identity: id, incarnation: entry.incarnation}
}
func (b identityPinEntryBinding) matchesLocked(s *secureSessionStore) bool {
	entry, found := s.identities[b.identity]
	return found && b.owner == s && !b.identity.IsZero() && b.incarnation != 0 && entry.incarnation == b.incarnation && entry.evidenceRevision != 0 && entry.protectionRevision != 0
}

// Metadata state has the same immutable/COW owner as its public value. An
// acknowledgment is runtime evidence only, never serialized or inferred from
// the ordinary writer's success.
type identityPinMetadataState struct {
	value             domain.IdentityPinMetadata
	confirmedRevision identityPinProtectionRevision
	recordOwner       *contactRecordOwedState
	recordMirror      contactRecordOwnerMirror
}
type identityPinMetadataCommit struct {
	digest    domain.IdentityPinSnapshotDigest
	confirmed int
}

var errPinMetadataCommitUnconfirmed = errors.New("identity pin metadata strict commit unconfirmed")

// confirmPinMetadata forces a strict full snapshot even when the ordinary
// writer already covered its generation. The gate excludes destructive C3;
// proof and endpoint updates can continue and remain dirty if not captured.
func (s *secureSessionStore) confirmPinMetadata(ctx context.Context, files identityPinDurableFiles) (identityPinMetadataCommit, error) {
	if err := ctx.Err(); err != nil {
		return identityPinMetadataCommit{}, err
	}
	if err := files.Preflight(ctx); err != nil {
		return identityPinMetadataCommit{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return identityPinMetadataCommit{}, err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return identityPinMetadataCommit{}, err
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		s.mu.Unlock()
		return identityPinMetadataCommit{}, err
	}
	file, covers := s.snapshotLocked(), s.changes
	quota := file.InboundQuota.stamp()
	revisions := s.pinMetadataRevisionsLocked()
	s.mu.Unlock()
	raw, digest, err := encodeIdentityPinSnapshot(file)
	if err != nil {
		return identityPinMetadataCommit{}, err
	}
	commit, err := files.WriteMain(ctx, raw)
	if err != nil || commit != secretfile.DurableCommitted {
		return identityPinMetadataCommit{}, errors.Join(errPinMetadataCommitUnconfirmed, err)
	}
	// A completed namespace commit outranks a cancellation arriving afterwards.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordNinSnapshotWrittenLocked(quota, true)
	return s.ackPinMetadataLocked(revisions, covers, digest), nil
}

// pinMetadataRevisionsLocked requires store.mu; it adds no new snapshot pass
// beyond the same metadata ACK inventory used by the existing strict writer.
func (s *secureSessionStore) pinMetadataRevisionsLocked() map[domain.PeerIdentity]identityPinProtectionRevision {
	revisions := make(map[domain.PeerIdentity]identityPinProtectionRevision)
	for id, entry := range s.identities {
		if entry.metadata != nil {
			revisions[id] = entry.protectionRevision
		}
	}
	return revisions
}

// ackPinMetadataLocked requires store.mu. Only exact captured revisions are
// ACKed; a newer unrelated metadata write is never confirmed by an old image.
func (s *secureSessionStore) ackPinMetadataLocked(revisions map[domain.PeerIdentity]identityPinProtectionRevision, covers uint64, digest domain.IdentityPinSnapshotDigest) identityPinMetadataCommit {
	ack := identityPinMetadataCommit{digest: digest}
	for id, revision := range revisions {
		entry, ok := s.identities[id]
		if !ok || revision == 0 || entry.protectionRevision != revision || entry.metadata == nil {
			continue
		}
		state := *entry.metadata
		state.confirmedRevision = revision
		entry.metadata = &state
		s.putPinEntryLocked(id, entry)
		ack.confirmed++
	}
	s.markWrittenLocked(covers)
	return ack
}
