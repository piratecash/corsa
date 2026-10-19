package node

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// The external type keeps its legacy name for fence protocol compatibility.
type identityPinEvidenceRevision = domain.IdentityPinProofRevision

type identityPinPreviewKey [32]byte

type identityPinEntry struct {
	pinnedAt           time.Time
	metadata           *identityPinMetadataState
	evidenceRevision   identityPinEvidenceRevision
	protectionRevision identityPinProtectionRevision
	incarnation        identityPinProtectionRevision
}

// Metadata is immutable once published. Nil explicitly represents an
// unclassified entry; serialization still writes the versioned unclassified
// value. Only classed entries allocate metadata, keeping current proofs small.
func (e identityPinEntry) classMetadata() domain.IdentityPinMetadata {
	if e.metadata == nil {
		return domain.IdentityPinMetadata{}
	}
	return e.metadata.value
}

// setUnpublishedMetadata validates a COW entry before publication. Live
// callers check its error before counters; only the owner CAS changes revisions.
func (e *identityPinEntry) setUnpublishedMetadata(metadata domain.IdentityPinMetadata) error {
	if err := metadata.Validate(); err != nil {
		return err
	}
	if e.metadata != nil && e.metadata.recordOwner != nil && metadata.Contact != e.metadata.value.Contact {
		return errContactRecordInvariant
	}
	if metadata == e.classMetadata() {
		return nil
	}
	if metadata == (domain.IdentityPinMetadata{}) {
		e.metadata = nil
		return nil
	}
	state := identityPinMetadataState{value: metadata, recordMirror: contactRecordOwnerNone}
	if e.metadata != nil {
		state.recordOwner, state.recordMirror = e.metadata.recordOwner, e.metadata.recordMirror
	}
	e.metadata = &state
	return nil
}

// recordPinEvidenceLocked advances protection-evidence ordering of an existing entry.
// A new entry also receives a fresh protection revision; both counters are
// checked before either advances, so refused evidence has no partial effect.
// Caller holds mu; the store is unpublished or pinMutationGate is held for read.
func (s *secureSessionStore) recordPinEvidenceLocked(id domain.PeerIdentity, at time.Time) error {
	entry := s.identities[id]
	newProtection := entry.protectionRevision == 0
	if s.evidenceRevision == math.MaxUint64 || (newProtection && s.protectionRevision == math.MaxUint64) {
		return domain.ErrIdentityPinRevisionExhausted
	}
	if newProtection {
		s.protectionRevision++
		entry.protectionRevision = s.protectionRevision
		entry.incarnation = s.protectionRevision
	}
	s.evidenceRevision++
	entry.pinnedAt, entry.evidenceRevision = at, s.evidenceRevision
	s.putPinEntryLocked(id, entry)
	return nil
}

// These values distinguish trusted session publication from reconstruction.
// Neither is a sessionv2 proof: the existing session caller must already have
// performed its genuine handshake. Record evidence has a separate verifier.
type sessionPinEvidence struct {
	identity domain.PeerIdentity
	at       time.Time
}
type historicalPinEvidence struct {
	identity domain.PeerIdentity
	at       time.Time
}

// recordSessionPinEvidenceLocked requires mu and pinMutationGate for read.
func (s *secureSessionStore) recordSessionPinEvidenceLocked(e sessionPinEvidence) error {
	return s.recordPinEvidenceLocked(e.identity, e.at)
}

// restoreHistoricalPinLocked requires mu and an unpublished store or the removal gate.
func (s *secureSessionStore) restoreHistoricalPinLocked(e historicalPinEvidence) error {
	return s.recordPinEvidenceLocked(e.identity, e.at)
}

// identityPinTokenLocked binds the process epoch, identity, current proof and
// warning version. Caller holds mu and the preview key has been initialised.
func (s *secureSessionStore) identityPinTokenLocked(id domain.PeerIdentity) domain.IdentityPinConfirmationToken {
	digest := hmac.New(sha256.New, s.previewKey[:])
	// hash.Hash.Write cannot fail.
	_, _ = digest.Write(id[:])
	var revision [8]byte
	binary.BigEndian.PutUint64(revision[:], uint64(s.identities[id].evidenceRevision))
	_, _ = digest.Write(revision[:])
	binary.BigEndian.PutUint64(revision[:], uint64(s.identities[id].protectionRevision))
	_, _ = digest.Write(revision[:])
	_, _ = digest.Write([]byte("pin-preview-metadata-v3\x00"))
	metadata, err := json.Marshal(s.identities[id].classMetadata())
	if err != nil {
		return ""
	}
	_, _ = digest.Write(metadata)
	var warning [4]byte
	binary.BigEndian.PutUint32(warning[:], uint32(domain.CurrentIdentityPinConsequenceVersion))
	_, _ = digest.Write(warning[:])
	_, _ = digest.Write([]byte(domain.IdentityPinRemovalConsequence))
	return domain.IdentityPinConfirmationToken(hex.EncodeToString(digest.Sum(nil)))
}

// validateIdentityPinTokenLocked accepts only the current proof in this process.
// Caller holds mu; removal must additionally hold pinMutationGate exclusively.
func (s *secureSessionStore) validateIdentityPinTokenLocked(id domain.PeerIdentity, token domain.IdentityPinConfirmationToken) error {
	if s.previewKey == nil || len(token) != sha256.Size*2 {
		return domain.ErrIdentityPinConfirmationStale
	}
	entry, found := s.identities[id]
	if !found || entry.evidenceRevision == 0 || entry.protectionRevision == 0 {
		return domain.ErrIdentityPinConfirmationStale
	}
	expected := s.identityPinTokenLocked(id)
	if !hmac.Equal([]byte(token), []byte(expected)) {
		return domain.ErrIdentityPinConfirmationStale
	}
	return nil
}

func (s *secureSessionStore) previewIdentityPinRemoval(id domain.PeerIdentity) (domain.IdentityPinRemovalPreview, error) {
	if id.IsZero() {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinIdentityRequired
	}
	// Entropy acquisition precedes mu. Concurrent first previews publish only one
	// key; unused candidates never leave this stack. No per-preview map grows.
	var candidate identityPinPreviewKey
	if _, err := rand.Read(candidate[:]); err != nil {
		return domain.IdentityPinRemovalPreview{}, fmt.Errorf("identity pin preview entropy: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinStoreUnavailable
	}
	entry, found := s.identities[id]
	if !found {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinNotFound
	}
	if entry.evidenceRevision == 0 || entry.protectionRevision == 0 {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinStoreUnavailable
	}
	if err := entry.classMetadata().Validate(); err != nil {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinStoreUnavailable
	}
	if s.previewKey == nil {
		s.previewKey = &candidate
	}
	return domain.IdentityPinRemovalPreview{Identity: id, PinnedAt: entry.pinnedAt, Consequence: domain.IdentityPinRemovalConsequence, ConsequenceVersion: domain.CurrentIdentityPinConsequenceVersion, Token: s.identityPinTokenLocked(id), RemovalAvailable: false}, nil
}

// PreviewIdentityPinRemoval shows one pin and the consequence of removing it.
func (s *Service) PreviewIdentityPinRemoval(ctx context.Context, id domain.PeerIdentity) (domain.IdentityPinRemovalPreview, error) {
	if err := ctx.Err(); err != nil {
		return domain.IdentityPinRemovalPreview{}, err
	}
	if s.secureSessions == nil || s.secureSessions.store == nil {
		return domain.IdentityPinRemovalPreview{}, domain.ErrIdentityPinStoreUnavailable
	}
	return s.secureSessions.store.previewIdentityPinRemoval(id)
}

// ConfirmIdentityPinRemoval refuses removal until verified storage and an
// external restart fence can enforce recovery across process restarts.
func (s *Service) ConfirmIdentityPinRemoval(ctx context.Context, id domain.PeerIdentity, token domain.IdentityPinConfirmationToken) (domain.IdentityPinRemovalResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.IdentityPinRemovalResult{}, err
	}
	return domain.IdentityPinRemovalResult{}, domain.ErrIdentityPinRemovalUnavailable
}
