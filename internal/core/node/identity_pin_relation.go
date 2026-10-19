package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

const maxRelationPins = 8192

var errRelationPinQuotaFull = errors.New("identity pin relation allocation quota full")
var errRelationPinAccounting = errors.New("identity pin relation allocation accounting unavailable")
var errRelationPinRecord = errors.New("identity pin relation record evidence invalid")

type identityPinRemovalEpoch uint64
type pinOwnerObservationKind uint8

const (
	pinOwnerUnobserved pinOwnerObservationKind = iota
	pinOwnerExisting
	pinOwnerAbsent
)

// This runtime value is not authority inferred from a current target lookup.
// Existing entries bind their incarnation; absence binds the destructive epoch.
type pinOwnerObservation struct {
	owner        *secureSessionStore
	identity     domain.PeerIdentity
	kind         pinOwnerObservationKind
	incarnation  identityPinProtectionRevision
	removalEpoch identityPinRemovalEpoch
}

// capturePinOwnerObservation is unused by production. A future attempt producer
// must hold the gate through exact attempt registration, using the Locked helper
// while holding mu briefly, rather than treating a post-attempt capture as proof.
func (s *secureSessionStore) capturePinOwnerObservation(ctx context.Context, id domain.PeerIdentity) (pinOwnerObservation, error) {
	if err := ctx.Err(); err != nil {
		return pinOwnerObservation{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return pinOwnerObservation{}, err
	}
	return s.pinOwnerObservationLocked(id)
}

// pinOwnerObservationLocked requires mu and the read side of pinMutationGate.
func (s *secureSessionStore) pinOwnerObservationLocked(id domain.PeerIdentity) (pinOwnerObservation, error) {
	if id.IsZero() {
		return pinOwnerObservation{}, domain.ErrIdentityPinIdentityRequired
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return pinOwnerObservation{}, err
	}
	observation := pinOwnerObservation{owner: s, identity: id, kind: pinOwnerAbsent, removalEpoch: s.removalEpoch}
	if _, present := s.identities[id]; present {
		binding := s.pinEntryBindingLocked(id)
		if !binding.matchesLocked(s) {
			return pinOwnerObservation{}, domain.ErrIdentityPinStoreUnavailable
		}
		observation.kind, observation.incarnation = pinOwnerExisting, binding.incarnation
	}
	return observation, nil
}

// matchesLocked requires the observed store mu.
func (o pinOwnerObservation) matchesLocked(s *secureSessionStore) bool {
	if o.owner != s || o.identity.IsZero() {
		return false
	}
	switch o.kind {
	case pinOwnerExisting:
		return (identityPinEntryBinding{owner: o.owner, identity: o.identity, incarnation: o.incarnation}).matchesLocked(s)
	case pinOwnerAbsent:
		_, present := s.identities[o.identity]
		return !present && o.removalEpoch == s.removalEpoch
	default:
		return false
	}
}

// advancePinRemovalEpochLocked requires mu and the exclusive removal gate.
// The caller never rolls it back after BeginRemoval, even on unknown outcome.
func (s *secureSessionStore) advancePinRemovalEpochLocked() error {
	if s.removalEpoch == math.MaxUint64 {
		return domain.ErrIdentityPinRevisionExhausted
	}
	s.removalEpoch++
	return nil
}

type relationPinQuestion struct {
	network domain.NetworkID
	target  domain.PeerIdentity
	label   domain.PeerIdentity
	qHash   [sha256.Size]byte
	minSeq  domain.IdentityRecordSeq
}

// Only verifyRelationPinRecord creates this immutable capsule. It is neither a
// session proof nor trusted Window/action admission. No production caller exists
// until the separate pre-attempt, queue and rate owner is reviewed and wired.
type verifiedRelationPinRecord struct {
	evidence *relationPinRecordEvidence
}
type relationPinRecordEvidence struct {
	question relationPinQuestion
	sequence domain.IdentityRecordSeq
}

func verifyRelationPinRecord(question relationPinQuestion, record protocol.SignedIdentityRecord, targetProof []byte) (verifiedRelationPinRecord, error) {
	if question.target.IsZero() || question.label.IsZero() {
		return verifiedRelationPinRecord{}, errRelationPinRecord
	}
	body, err := protocol.VerifyIdentityRecord(record, question.network, question.target)
	if err != nil {
		return verifiedRelationPinRecord{}, errors.Join(errRelationPinRecord, err)
	}
	version, declared := body.MinSession.Version()
	if !declared || version < domain.SessionVersionSecure || body.Seq < question.minSeq {
		return verifiedRelationPinRecord{}, errRelationPinRecord
	}
	if err := protocol.VerifyTargetProof(targetProof, body, question.network, question.label, question.qHash, record); err != nil {
		return verifiedRelationPinRecord{}, errors.Join(errRelationPinRecord, err)
	}
	return verifiedRelationPinRecord{evidence: &relationPinRecordEvidence{question: question, sequence: body.Seq}}, nil
}

type relationPinStage struct {
	binding     identityPinEntryBinding
	mutation    identityPinMetadataMutation
	publication recordPinPublication
}

// stageRelationRecord is an inactive store foundation, not a public admission
// port. There are no production callers, resolver consumers or retries. A live
// producer additionally needs approved action/rate/queue/cancel ownership.
func (s *secureSessionStore) stageRelationRecord(ctx context.Context, observed pinOwnerObservation, record verifiedRelationPinRecord) (relationPinStage, error) {
	if err := ctx.Err(); err != nil {
		return relationPinStage{}, err
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return relationPinStage{}, err
	}
	return s.stageRelationRecordLocked(observed, record)
}

// stageRelationRecordLocked requires mu and pinMutationGate for read. No I/O or
// callback occurs under either owner lock. Every check precedes all mutation.
func (s *secureSessionStore) stageRelationRecordLocked(observed pinOwnerObservation, record verifiedRelationPinRecord) (relationPinStage, error) {
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return relationPinStage{}, err
	}
	if record.evidence == nil || record.evidence.question.target != observed.identity {
		return relationPinStage{}, errRelationPinRecord
	}
	if !observed.matchesLocked(s) {
		return relationPinStage{}, errPinMetadataSnapshotStale
	}
	id := observed.identity
	entry, present := s.identities[id]
	metadata := entry.classMetadata()
	if !present {
		if s.identityPinAdmissionFullLocked(id) {
			return relationPinStage{}, errPinStoreFull
		}
		entry.pinnedAt = s.now()
		// A missing C3 owner reserves this same identity's old complete facts.
		// Reuse them instead of replacing the recovery backup with R-only data.
		if s.pinRecovery != nil && s.pinRecovery.permit != nil && s.pinRecovery.permit.Binding.Identity == id {
			binding := s.pinRecovery.permit.Binding
			backup, err := binding.Backup.Restore(id, binding.PreviousPinnedAt)
			if err != nil {
				return relationPinStage{}, err
			}
			metadata, entry.pinnedAt = backup.Metadata, backup.PinnedAt
		}
	}
	n, err := s.relationAllocationsLocked()
	if err != nil {
		return relationPinStage{}, err
	}
	if metadata.Relation.State == domain.IdentityPinAllocationAbsent && n >= maxRelationPins {
		return relationPinStage{}, errRelationPinQuotaFull
	}
	next := metadata
	if next.Relation.State != domain.IdentityPinAllocationEligible {
		next.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: s.now(), Reference: domain.IdentityPinEvidenceReference(record.evidence.question.label.String())}
	}
	if err := next.Validate(); err != nil {
		return relationPinStage{}, err
	}
	metadataChanged := !present || next != entry.classMetadata()
	if s.evidenceRevision == math.MaxUint64 || (metadataChanged && s.protectionRevision == math.MaxUint64) {
		return relationPinStage{}, domain.ErrIdentityPinRevisionExhausted
	}
	if err := entry.setUnpublishedMetadata(next); err != nil {
		return relationPinStage{}, err
	}
	s.evidenceRevision++
	entry.evidenceRevision = s.evidenceRevision
	mutation := identityPinMetadataMutation{state: pinMetadataUnchangedInMemory, protectionRevision: entry.protectionRevision}
	if metadataChanged {
		s.protectionRevision++
		entry.protectionRevision = s.protectionRevision
		if !present {
			entry.incarnation = entry.protectionRevision
		}
		s.changes++
		mutation = identityPinMetadataMutation{state: pinMetadataStagedUnflushed, protectionRevision: entry.protectionRevision}
	}
	s.putPinEntryLocked(id, entry)
	binding := s.pinEntryBindingLocked(id)
	return relationPinStage{binding: binding, mutation: mutation, publication: recordPinPublication{binding: binding, evidenceRevision: entry.evidenceRevision, protectionRevision: entry.protectionRevision}}, nil
}

// relationAllocationsLocked requires mu. Counts raw allocation, including K,
// unconfirmed state and an absent C3 backup exactly once; no extra identity map.
func (s *secureSessionStore) relationAllocationsLocked() (int, error) {
	n := s.relationAllocations
	if n < 0 || s.relationConfirmed < 0 || s.relationConfirmed > n {
		return n, errRelationPinAccounting
	}
	if s.pinRecovery != nil && s.pinRecovery.permit != nil {
		binding := s.pinRecovery.permit.Binding
		if _, present := s.identities[binding.Identity]; !present {
			backup, err := binding.Backup.Restore(binding.Identity, binding.PreviousPinnedAt)
			if err != nil {
				return n, err
			}
			if backup.Metadata.Relation.State != domain.IdentityPinAllocationAbsent {
				n++
			}
		}
	}
	return n, nil
}

// replaceRelationTalliesLocked requires mu, from the existing entry put/delete owner.
func (s *secureSessionStore) replaceRelationTalliesLocked(old, next identityPinEntry) {
	if old.classMetadata().Relation.State != domain.IdentityPinAllocationAbsent {
		s.relationAllocations--
	}
	if next.classMetadata().Relation.State != domain.IdentityPinAllocationAbsent {
		s.relationAllocations++
	}
	if old.relationConfirmed() {
		s.relationConfirmed--
	}
	if next.relationConfirmed() {
		s.relationConfirmed++
	}
}

func (e identityPinEntry) relationConfirmed() bool {
	return e.protectionRevision != 0 && e.metadata != nil && e.classMetadata().Relation.State == domain.IdentityPinAllocationEligible && e.metadata.confirmedRevision == e.protectionRevision
}
