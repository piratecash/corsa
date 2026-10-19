package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

var errContactRecordEvidence = errors.New("contact pin signed record evidence invalid")
var errContactRecordInvariant = errors.New("contact pin earned allocation unexpectedly changed")

// Verification has no production ingress. After stage, the capped pin owner
// retains completion independently of this handle. Raw payload <=2048+64 bytes;
// external handles and existing full snapshots have additional cost.
type contactRecordCandidate struct {
	mu       sync.Mutex
	service  *Service
	trust    *trustStore
	store    *secureSessionStore
	fact     localContactStamp
	factAt   time.Time
	observed pinOwnerObservation
	network  domain.NetworkID
	record   protocol.SignedIdentityRecord
	sequence domain.IdentityRecordSeq
	digest   [sha256.Size]byte
	verified bool
	rejected bool
	staged   contactRecordStage
}

type verifiedContactMinSessionRecord struct{ candidate *contactRecordCandidate }
type contactRecordStageDisposition uint8

const (
	contactRecordStageInvalid contactRecordStageDisposition = iota
	contactRecordNewOwnedStage
	contactRecordJoinExistingOwed
	contactRecordAlreadyHistoricalCompleted
	contactRecordAlreadyEligibleNoRecordObligation
)

type contactRecordStage struct {
	disposition contactRecordStageDisposition
	owed        *contactRecordOwedState
}

type contactRecordOwnerMirror uint8

const (
	contactRecordOwnerNone contactRecordOwnerMirror = iota
	contactRecordOwnerPending
	contactRecordOwnerCoveredOwed
	contactRecordOwnerDeferredInvariant
	contactRecordOwnerCompleted
)

// Earned facts are immutable history after stage, not current-contact gates.
type owedContactProtection struct {
	service            *Service
	binding            identityPinEntryBinding
	contact            domain.IdentityPinAllocation
	fact               localContactStamp
	recordDigest       [sha256.Size]byte
	recordSequence     domain.IdentityRecordSeq
	evidenceRevision   identityPinEvidenceRevision
	protectionRevision identityPinProtectionRevision
}

type contactRecordCompletionState uint8

const (
	contactRecordStagedOwed contactRecordCompletionState = iota + 1
	contactRecordCoveredOwed
	contactRecordHistoricalProtectionCompleted
	contactRecordStaleOwner
	contactRecordDeferredInvariant
)

type contactRecordCompletion struct {
	state            contactRecordCompletionState
	receipt          contactRecordCoverReceipt
	attemptCovered   bool // only THIS invocation obtained a durable covering image
	barrierAttempted bool // effects may be partial if a later check fails
}

// Terminal replay returns historical receipt/barrier state, but THIS invocation
// made no covering write. Change only the returned value, never shared history.
func (completion contactRecordCompletion) replay() contactRecordCompletion {
	completion.attemptCovered = false
	return completion
}

// Copies of a stage refer to this SAME completion owner. It has no reverse
// acquisition path and may serialize one explicit completion invocation/I/O.
type contactRecordOwedState struct {
	mu         sync.Mutex
	protection owedContactProtection
	completion contactRecordCompletion
}

// observeContactRecord has no production caller. Capture precedes asynchronous
// verification/import, with original durable local fact and pin owner/absence.
func (s *Service) observeContactRecord(ctx context.Context, id domain.PeerIdentity, record protocol.SignedIdentityRecord) (*contactRecordCandidate, error) {
	store := s.localContactPinStore()
	if store == nil || s.trust == nil || id.IsZero() || id.String() == s.identity.Address || len(record.Body) == 0 || len(record.Body) > domain.MaxIdentityRecordBodyBytes || len(record.Sig) != 64 {
		return nil, errContactRecordEvidence
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	candidate := &contactRecordCandidate{service: s, trust: s.trust, store: store, network: domain.NetworkID(networkName), record: cloneSignedIdentityRecord(record)}
	s.trustMutationMu.Lock()
	defer s.trustMutationMu.Unlock()
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.trust.pinEvidenceMu.RLock()
	defer s.trust.pinEvidenceMu.RUnlock()
	s.trust.mu.RLock()
	row := s.trust.contacts[id.String()]
	valid := s.trust.authorityReadErr == nil && row.LocalContact != nil && row.LocalContact.revision != 0 && row.LocalContact.confirmed == row.LocalContact.revision && row.LocalContact.fact.Network == candidate.network
	if valid {
		candidate.fact, candidate.factAt = row.LocalContact.stamp(s.trust), row.LocalContact.fact.At
	}
	s.trust.mu.RUnlock()
	if !valid {
		return nil, domain.ErrLocalContactFactUnconfirmed
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Already holding the removal read gate: NEVER call the public RLock facade
	// here. A waiting destructive writer would deadlock recursive RLock.
	observed, err := store.pinOwnerObservationLocked(id)
	if err != nil {
		return nil, err
	}
	candidate.observed = observed
	return candidate, nil
}

func verifyContactMinSessionRecord(candidate *contactRecordCandidate) (verifiedContactMinSessionRecord, error) {
	if candidate == nil {
		return verifiedContactMinSessionRecord{}, errContactRecordEvidence
	}
	candidate.mu.Lock()
	defer candidate.mu.Unlock()
	if candidate.rejected {
		return verifiedContactMinSessionRecord{}, errContactRecordEvidence
	}
	if candidate.verified {
		return verifiedContactMinSessionRecord{candidate: candidate}, nil
	}
	body, err := protocol.VerifyIdentityRecord(candidate.record, candidate.network, candidate.fact.identity)
	version, declared := body.MinSession.Version()
	if err != nil || !declared || version < domain.SessionVersionSecure {
		candidate.rejected = true
		return verifiedContactMinSessionRecord{}, errors.Join(errContactRecordEvidence, err)
	}
	candidate.sequence = body.Seq
	digest := sha256.New()
	_, _ = digest.Write(candidate.record.Body)
	_, _ = digest.Write(candidate.record.Sig)
	copy(candidate.digest[:], digest.Sum(nil))
	candidate.verified = true
	return verifiedContactMinSessionRecord{candidate: candidate}, nil
}

func (candidate *contactRecordCandidate) stagedOutcome() (contactRecordStage, bool) {
	if candidate == nil {
		return contactRecordStage{}, false
	}
	candidate.mu.Lock()
	defer candidate.mu.Unlock()
	return candidate.staged, candidate.staged.disposition != contactRecordStageInvalid
}

// stageContactRecord has no production caller. Only this original verified
// capsule can stage; repeated calls return its retained outcome without effects.
func (s *Service) stageContactRecord(ctx context.Context, verified verifiedContactMinSessionRecord) (contactRecordStage, error) {
	candidate := verified.candidate
	if candidate == nil {
		return contactRecordStage{}, errContactRecordEvidence
	}
	candidate.mu.Lock()
	defer candidate.mu.Unlock()
	if candidate.service != s || !candidate.verified || candidate.rejected {
		return contactRecordStage{}, errContactRecordEvidence
	}
	if candidate.staged.disposition != contactRecordStageInvalid {
		return candidate.staged, nil
	}
	if candidate.trust != s.trust || candidate.store != s.localContactPinStore() {
		return contactRecordStage{}, errContactRecordEvidence
	}
	if err := ctx.Err(); err != nil {
		return contactRecordStage{}, err
	}
	files, err := candidate.store.openIdentityPinFiles()
	if err != nil {
		return contactRecordStage{}, err
	}
	defer func() { _ = files.Close() }()
	if err := files.Preflight(ctx); err != nil {
		return contactRecordStage{}, err
	}
	s.trustMutationMu.Lock()
	defer s.trustMutationMu.Unlock()
	store := candidate.store
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	s.trust.pinEvidenceMu.RLock()
	defer s.trust.pinEvidenceMu.RUnlock()
	s.trust.mu.RLock()
	row := s.trust.contacts[candidate.fact.identity.String()]
	current, present := s.trust.records[trustRecordKey{network: candidate.network.String(), address: candidate.fact.identity.String()}]
	valid := s.trust.authorityReadErr == nil && candidate.fact.matches(row.LocalContact, s.trust) && row.LocalContact.confirmed == row.LocalContact.revision && present && current.record.Version == candidate.record.Version && bytes.Equal(current.record.Body, candidate.record.Body) && current.body.Seq == candidate.sequence
	s.trust.mu.RUnlock()
	if !valid {
		candidate.rejected = true
		return contactRecordStage{}, domain.ErrLocalContactFactStale
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return contactRecordStage{}, err
	}
	stage, err := store.stageContactRecordLocked(candidate)
	if err != nil {
		return contactRecordStage{}, err
	}
	// Publish owed ownership in this same serialized operation. No later context
	// error may hide a real staged protection or turn its ticket into zero.
	candidate.staged = stage
	return stage, nil
}

// stageContactRecordLocked requires store.mu, removal read gate and the trust
// evidence read gate. No I/O, callback or recursive public lock facade occurs.
func (s *secureSessionStore) stageContactRecordLocked(candidate *contactRecordCandidate) (contactRecordStage, error) {
	if candidate == nil || !candidate.verified || candidate.store != s {
		return contactRecordStage{}, errContactRecordEvidence
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return contactRecordStage{}, err
	}
	if !candidate.observed.matchesLocked(s) {
		return contactRecordStage{}, errPinMetadataSnapshotStale
	}
	id := candidate.fact.identity
	entry, present := s.identities[id]
	metadata := entry.classMetadata()
	if !present {
		if s.identityPinAdmissionFullLocked(id) {
			return contactRecordStage{}, errPinStoreFull
		}
		entry.pinnedAt = s.now()
		if s.pinRecovery != nil && s.pinRecovery.permit != nil && s.pinRecovery.permit.Binding.Identity == id {
			binding := s.pinRecovery.permit.Binding
			backup, err := binding.Backup.Restore(id, binding.PreviousPinnedAt)
			if err != nil {
				return contactRecordStage{}, err
			}
			metadata, entry.pinnedAt = backup.Metadata, backup.PinnedAt
		}
	}

	if present && metadata.Contact.State == domain.IdentityPinAllocationEligible {
		return s.existingContactRecordStageLocked(id, entry)
	}
	next := metadata
	if next.Contact.State != domain.IdentityPinAllocationEligible {
		next.Contact = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: candidate.factAt, Reference: domain.IdentityPinEvidenceReference(candidate.fact.id.String())}
	}
	if err := next.Validate(); err != nil {
		return contactRecordStage{}, err
	}
	if s.evidenceRevision == math.MaxUint64 || s.protectionRevision == math.MaxUint64 {
		return contactRecordStage{}, domain.ErrIdentityPinRevisionExhausted
	}
	if err := entry.setUnpublishedMetadata(next); err != nil {
		return contactRecordStage{}, err
	}
	entry.evidenceRevision = s.evidenceRevision + 1
	entry.protectionRevision = s.protectionRevision + 1
	if !present {
		entry.incarnation = entry.protectionRevision
	}
	binding := identityPinEntryBinding{owner: s, identity: id, incarnation: entry.incarnation}
	owed := owedContactProtection{service: candidate.service, binding: binding, contact: next.Contact, fact: candidate.fact, recordDigest: candidate.digest, recordSequence: candidate.sequence, evidenceRevision: entry.evidenceRevision, protectionRevision: entry.protectionRevision}
	state := &contactRecordOwedState{protection: owed, completion: contactRecordCompletion{state: contactRecordStagedOwed}}
	metadataState := *entry.metadata
	metadataState.recordOwner, metadataState.recordMirror = state, contactRecordOwnerPending
	entry.metadata = &metadataState
	s.evidenceRevision, s.protectionRevision = entry.evidenceRevision, entry.protectionRevision
	s.changes++
	s.putPinEntryLocked(id, entry)
	return contactRecordStage{disposition: contactRecordNewOwnedStage, owed: state}, nil
}

// existingContactRecordStageLocked requires store.mu; it never takes a ticket mutex.
func (s *secureSessionStore) existingContactRecordStageLocked(id domain.PeerIdentity, entry identityPinEntry) (contactRecordStage, error) {
	state := entry.metadata
	if state == nil || state.recordOwner == nil {
		return contactRecordStage{disposition: contactRecordAlreadyEligibleNoRecordObligation}, nil
	}
	if state.recordOwner.protection.binding != s.pinEntryBindingLocked(id) {
		return contactRecordStage{}, errPinMetadataSnapshotStale
	}
	if state.recordOwner.protection.contact != state.value.Contact {
		return contactRecordStage{}, errContactRecordInvariant
	}
	switch state.recordMirror {
	case contactRecordOwnerPending, contactRecordOwnerCoveredOwed, contactRecordOwnerDeferredInvariant:
		return contactRecordStage{disposition: contactRecordJoinExistingOwed, owed: state.recordOwner}, nil
	case contactRecordOwnerCompleted:
		return contactRecordStage{disposition: contactRecordAlreadyHistoricalCompleted, owed: state.recordOwner}, nil
	default:
		return contactRecordStage{}, errContactRecordInvariant
	}
}

// checkLocked requires the pin-store mu. Incarnation and earned K, rather than
// old evidence/protection revision or current contact, bind the owed obligation.
func (owed owedContactProtection) checkLocked(store *secureSessionStore) error {
	if err := store.pinMetadataMutationRefusalLocked(); err != nil {
		return err
	}
	if !owed.binding.matchesLocked(store) {
		return errPinMetadataSnapshotStale
	}
	if store.identities[owed.binding.identity].classMetadata().Contact != owed.contact {
		return errContactRecordInvariant
	}
	return nil
}

func (s *Service) checkOwedContactRecord(owed owedContactProtection) error {
	store := s.localContactPinStore()
	if store == nil || owed.service != s || owed.binding.owner != store {
		return errPinMetadataSnapshotStale
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return owed.checkLocked(store)
}

// completeOwedContactRecord is the explicit blocking facade. The joined worker
// uses its TryLock facade; neither recaptures authority or re-stages earned K.
func (s *Service) completeOwedContactRecord(ctx context.Context, stage contactRecordStage, files identityPinDurableFiles) (contactRecordCompletion, error) {
	if stage.owed == nil {
		return contactRecordCompletion{}, errContactRecordEvidence
	}
	state := stage.owed
	state.mu.Lock()
	defer state.mu.Unlock()
	return s.completeOwedContactRecordLocked(ctx, state, files)
}

type contactRecordAttemptState uint8

const (
	contactRecordAttemptBusy contactRecordAttemptState = iota + 1
	contactRecordAttemptFinished
)

type contactRecordAttempt struct {
	state      contactRecordAttemptState
	completion contactRecordCompletion
}

func (s *Service) tryCompleteOwedContactRecord(ctx context.Context, state *contactRecordOwedState, files identityPinDurableFiles) (contactRecordAttempt, error) {
	if err := ctx.Err(); err != nil {
		return contactRecordAttempt{}, err
	}
	if state == nil {
		return contactRecordAttempt{}, errContactRecordEvidence
	}
	if !state.mu.TryLock() {
		return contactRecordAttempt{state: contactRecordAttemptBusy}, nil
	}
	defer state.mu.Unlock()
	completion, err := s.completeOwedContactRecordLocked(ctx, state, files)
	return contactRecordAttempt{state: contactRecordAttemptFinished, completion: completion}, err
}

// completeOwedContactRecordLocked requires this ticket's mu. No store mutex
// may acquire it in reverse; only the invoking owner waits for IO or drain.
func (s *Service) completeOwedContactRecordLocked(ctx context.Context, state *contactRecordOwedState, files identityPinDurableFiles) (contactRecordCompletion, error) {
	owed := state.protection
	if owed.service != s {
		return contactRecordCompletion{}, errPinMetadataSnapshotStale
	}
	if state.completion.state == contactRecordHistoricalProtectionCompleted {
		return state.completion.replay(), nil // historical completion, not current trust
	}
	if state.completion.state == contactRecordStaleOwner {
		return state.completion.replay(), errPinMetadataSnapshotStale
	}
	// A previous receipt remains historical diagnostic evidence after a later
	// Unknown, never a confirmation of this incomplete attempt/current metadata.
	state.completion.attemptCovered = false
	classify := func(err error) (contactRecordCompletion, error) {
		if errors.Is(err, errPinMetadataSnapshotStale) {
			state.completion.state = contactRecordStaleOwner
		} else if errors.Is(err, errContactRecordInvariant) {
			state.completion.state = contactRecordDeferredInvariant
		}
		mirror := contactRecordOwnerPending
		if state.completion.state == contactRecordDeferredInvariant {
			mirror = contactRecordOwnerDeferredInvariant
		}
		mirrorErr := s.mirrorContactRecordOwner(state, mirror)
		return state.completion, errors.Join(err, mirrorErr)
	}
	if err := s.checkOwedContactRecord(owed); err != nil {
		return classify(err)
	}
	receipt, err := owed.binding.owner.confirmContactRecordCoverage(ctx, owed, files)
	if err != nil {
		return classify(err)
	}
	state.completion.receipt = receipt
	state.completion.state = contactRecordCoveredOwed
	state.completion.attemptCovered = true
	if err := s.mirrorContactRecordOwner(state, contactRecordOwnerCoveredOwed); err != nil {
		return classify(err)
	}
	if err := ctx.Err(); err != nil {
		return state.completion, err
	}
	if err := s.checkOwedContactRecord(owed); err != nil {
		return classify(err)
	}
	state.completion.barrierAttempted = true
	if err := s.publishIdentityProtection(ctx, owed.binding.identity); err != nil {
		return state.completion, err
	}
	if err := s.checkOwedContactRecord(owed); err != nil {
		return classify(err)
	}
	if err := s.mirrorContactRecordOwner(state, contactRecordOwnerCompleted); err != nil {
		return classify(err)
	}
	state.completion.state = contactRecordHistoricalProtectionCompleted
	return state.completion, nil
}

// mirrorContactRecordOwner may run with state.mu held. Store readers never
// acquire that mutex; the mirror belongs exclusively to store.mu.
func (s *Service) mirrorContactRecordOwner(state *contactRecordOwedState, mirror contactRecordOwnerMirror) error {
	owed := state.protection
	store := s.localContactPinStore()
	if store == nil || owed.service != s || owed.binding.owner != store {
		return errPinMetadataSnapshotStale
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if !owed.binding.matchesLocked(store) {
		return errPinMetadataSnapshotStale
	}
	entry := store.identities[owed.binding.identity]
	if entry.metadata == nil || entry.metadata.recordOwner != state {
		return errPinMetadataSnapshotStale
	}
	if mirror != contactRecordOwnerDeferredInvariant {
		if err := owed.checkLocked(store); err != nil {
			return err
		}
	}
	metadata := *entry.metadata
	metadata.recordMirror = mirror
	entry.metadata = &metadata
	store.putPinEntryLocked(owed.binding.identity, entry)
	return nil
}
