package node

import (
	"errors"
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
)

type identityPinProtectionRevision uint64

var errPinMetadataSnapshotStale = errors.New("identity pin metadata snapshot is stale or belongs to another store")

// A snapshot is a runtime CAS stamp, never persisted authority or a promise
// that the corresponding metadata has reached disk. The owner prevents a
// restarted/other store with coincident revision values accepting it.
type identityPinMetadataSnapshot struct {
	owner              *secureSessionStore
	identity           domain.PeerIdentity
	evidenceRevision   identityPinEvidenceRevision
	protectionRevision identityPinProtectionRevision
	metadata           domain.IdentityPinMetadata
}

type pinMetadataMutationState uint8

const (
	pinMetadataUnchangedInMemory pinMetadataMutationState = iota + 1
	pinMetadataStagedUnflushed
)

// Neither outcome certifies persistence. A future live producer needs its
// own reviewed durable admission contract; the Selected producer uses its separate strict writer.
type identityPinMetadataMutation struct {
	state              pinMetadataMutationState
	protectionRevision identityPinProtectionRevision
}

func (s *secureSessionStore) pinMetadataSnapshot(id domain.PeerIdentity) (identityPinMetadataSnapshot, error) {
	if id.IsZero() {
		return identityPinMetadataSnapshot{}, domain.ErrIdentityPinIdentityRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return identityPinMetadataSnapshot{}, err
	}
	entry, found := s.identities[id]
	if !found {
		return identityPinMetadataSnapshot{}, domain.ErrIdentityPinNotFound
	}
	if entry.evidenceRevision == 0 || entry.protectionRevision == 0 {
		return identityPinMetadataSnapshot{}, domain.ErrIdentityPinStoreUnavailable
	}
	if err := entry.classMetadata().Validate(); err != nil {
		return identityPinMetadataSnapshot{}, err
	}
	return identityPinMetadataSnapshot{owner: s, identity: id, evidenceRevision: entry.evidenceRevision, protectionRevision: entry.protectionRevision, metadata: entry.classMetadata()}, nil
}

// stagePinMetadata performs no I/O. It stages only
// an already present entry, under the proof side of the removal gate. The
// existing writer may later persist its dirty generation; this operation
// neither waits for that writer nor makes any disk-commit assertion.
func (s *secureSessionStore) stagePinMetadata(expected identityPinMetadataSnapshot, next domain.IdentityPinMetadata) (identityPinMetadataMutation, error) {
	if expected.owner != s || expected.identity.IsZero() || expected.evidenceRevision == 0 || expected.protectionRevision == 0 {
		return identityPinMetadataMutation{}, errPinMetadataSnapshotStale
	}
	if err := next.Validate(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stagePinMetadataLocked(expected, next)
}

// stagePinMetadataLocked is shared by owner CAS and atomic class admission.
// Caller holds mu and the read side of pinMutationGate.
func (s *secureSessionStore) stagePinMetadataLocked(expected identityPinMetadataSnapshot, next domain.IdentityPinMetadata) (identityPinMetadataMutation, error) {
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	entry, found := s.identities[expected.identity]
	if !found || entry.evidenceRevision != expected.evidenceRevision || entry.protectionRevision != expected.protectionRevision || entry.classMetadata() != expected.metadata {
		return identityPinMetadataMutation{}, errPinMetadataSnapshotStale
	}
	if next.Inbound != expected.metadata.Inbound {
		return identityPinMetadataMutation{}, errNinMetadataRequiresJournal
	}
	if next == expected.metadata {
		return identityPinMetadataMutation{state: pinMetadataUnchangedInMemory, protectionRevision: entry.protectionRevision}, nil
	}
	if s.protectionRevision == math.MaxUint64 {
		return identityPinMetadataMutation{}, domain.ErrIdentityPinRevisionExhausted
	}
	if err := entry.setUnpublishedMetadata(next); err != nil {
		return identityPinMetadataMutation{}, err
	}
	s.protectionRevision++
	entry.protectionRevision = s.protectionRevision
	s.putPinEntryLocked(expected.identity, entry)
	s.changes++
	return identityPinMetadataMutation{state: pinMetadataStagedUnflushed, protectionRevision: entry.protectionRevision}, nil
}

func (s *secureSessionStore) pinMetadataMutationRefusalLocked() error {
	if s.unreadable {
		return domain.ErrIdentityPinStoreUnavailable
	}
	if s.pinRecovery != nil && s.pinRecovery.fatal {
		return domain.ErrIdentityPinRecoveryRequired
	}
	return nil
}
