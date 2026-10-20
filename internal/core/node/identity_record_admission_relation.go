package node

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// reserveRelationAttemptLocked requires resolver.mu and the removal read gate.
// Store contention opts out ONLY this R candidate; ordinary Send remains usable.
func (r *identityResolver) reserveRelationAttemptLocked(label domain.PeerIdentity, entry identityAttemptEntry, store *secureSessionStore) *relationAdmissionRef {
	a := r.recordAdmission
	if !a.admitsR() || a.started == nil || a.closing || entry.action.origin != identityLookupOriginWindowUser || entry.owner.recordLifetime == nil {
		return nil
	}
	now, ok := a.sampleLocked()
	if !ok || !entry.owner.recordLifetime.live(now) {
		return nil
	}
	r.expireRelationSlotsLocked(now)
	index := -1
	for i := range a.slots {
		if a.slots[i].phase == relationAdmissionFree && a.slots[i].generation < math.MaxUint64 {
			index = i
			break
		}
	}
	if index < 0 || !store.mu.TryLock() {
		return nil
	}
	observed, err := store.pinOwnerObservationLocked(entry.dst)
	store.mu.Unlock()
	if err != nil {
		return nil
	}
	slot := &a.slots[index]
	generation := slot.generation + 1
	*slot = relationAdmissionSlot{generation: generation, phase: relationAdmissionProspective, label: label, attemptGeneration: entry.gen, resolution: entry.owner, action: entry.action, observed: observed, lifetime: *entry.owner.recordLifetime, registered: now.elapsed}
	return &relationAdmissionRef{owner: a, index: uint16(index), generation: generation}
}

// captureRelationAnswerLocked requires resolver.mu. Crypto already verified the
// decoder-owned bytes outside this lock; the slot owns its only retained copy.
func (r *identityResolver) captureRelationAnswerLocked(entry identityAttemptEntry, record protocol.SignedIdentityRecord, proof []byte, verified verifiedRelationPinRecord) {
	slot := r.relationSlotLocked(entry.recordAdmission)
	if slot == nil {
		return
	}
	now, ok := r.recordAdmission.sampleLocked()
	if !ok || slot.cancelled || verified.evidence == nil || slot.phase != relationAdmissionProspective || slot.resolution != entry.owner || slot.attemptGeneration != entry.gen || slot.action != entry.action || now.elapsed < slot.registered || now.elapsed-slot.registered > identityLookupAttemptWindow || !slot.lifetime.live(now) || len(record.Body) > len(slot.body) || len(record.Sig) != len(slot.signature) || len(proof) != len(slot.proof) {
		r.clearRelationSlotLocked(slot)
		return
	}
	slot.bodyLen = copy(slot.body[:], record.Body)
	copy(slot.signature[:], record.Sig)
	copy(slot.proof[:], proof)
	slot.verified, slot.verifiedAt, slot.phase = verified, now.elapsed, relationAdmissionVerifiedAwaitSend
	slot.protectionHeld = true
	r.svc.pinRecordProtection(entry.dst)
}

// noteRelationQueueLocked requires resolver.mu. Queue observation is original,
// not deferred to the answer or a later replay of a soft diagnostic completion.
func (r *identityResolver) noteRelationQueueLocked(entry identityAttemptEntry) {
	slot := r.relationSlotLocked(entry.recordAdmission)
	if slot == nil {
		return
	}
	now, ok := r.recordAdmission.sampleLocked()
	if !ok || entry.revoked || slot.cancelled || entry.queue != identityAttemptQueueAccepted || now.elapsed < slot.registered || now.elapsed-slot.registered > identityLookupAttemptWindow {
		r.clearRelationSlotLocked(slot)
		return
	}
	slot.queueAccepted, slot.queuedAt = true, now.elapsed
}
func (r *identityResolver) settleRelationClaimLocked(entry identityAttemptEntry) {
	slot := r.relationSlotLocked(entry.recordAdmission)
	if slot == nil {
		return
	}
	a := r.recordAdmission
	now, ok := a.sampleLocked()
	if !ok || a.closing || r.unavailableLocked() || entry.revoked || slot.cancelled || slot.phase != relationAdmissionVerifiedAwaitSend || !slot.queueAccepted || entry.queue != identityAttemptQueueAccepted || slot.resolution != entry.owner || slot.attemptGeneration != entry.gen || slot.action != entry.action || !slot.lifetime.live(now) || now.elapsed < slot.registered || now.elapsed-slot.registered > identityLookupAttemptWindow || a.fifo == math.MaxUint64 {
		r.clearRelationSlotLocked(slot)
		return
	}
	a.fifo++
	slot.fifo = a.fifo
	slot.phase = relationAdmissionQueued
	r.wakeRecordAdmissionLocked()
}
func (r *identityResolver) cancelRelationActionLocked(action UserIdentityLookup) {
	a := r.recordAdmission
	if !a.admitsR() {
		return
	}
	for i := range a.slots {
		slot := &a.slots[i]
		if slot.phase == relationAdmissionFree || slot.observed.identity != action.target || slot.action.generation != action.generation || slot.resolution == nil || slot.resolution.id != action.resolutionID {
			continue
		}
		slot.cancelled = true
		if slot.phase < relationAdmissionEarnedOwed {
			r.clearRelationSlotLocked(slot)
		}
	}
}

type relationAdmissionDisposition uint8

const (
	relationAdmissionWait relationAdmissionDisposition = iota
	relationAdmissionStaged
	relationAdmissionContactPath
)

var errRelationAdmissionStale = errors.New("identity record admission original claim stale")

// stageRelationAdmission serializes all final trials and the one infallible
// debit with pinGate.R -> evidence.R -> resolver.mu -> trust.R -> store.mu.
// No crypto, I/O or legacy barrier executes under these locks.
func (r *identityResolver) stageRelationAdmission(ctx context.Context, slot *relationAdmissionSlot) (relationAdmissionDisposition, error) {
	if slot.r2Claim != nil {
		return r.stageOwnDMRelationAdmission(ctx, slot)
	}
	if !r.recordAdmission.admitsR() {
		return relationAdmissionWait, errRelationAdmissionStale
	}
	store, trust := r.svc.localContactPinStore(), r.svc.trust
	if store == nil || trust == nil {
		return relationAdmissionWait, domain.ErrIdentityPinStoreUnavailable
	}
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	trust.pinEvidenceMu.RLock()
	defer trust.pinEvidenceMu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	trust.mu.RLock()
	defer trust.mu.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return relationAdmissionWait, err
	}
	a := r.recordAdmission
	now, ok := a.sampleLocked()
	if !ok || a.closing || r.unavailableLocked() || !slot.processing || slot.cancelled || slot.phase != relationAdmissionQueued || slot.verified.evidence == nil || !slot.lifetime.live(now) {
		return relationAdmissionWait, errRelationAdmissionStale
	}
	record, present := trust.recordLocked(trustRecordKey{network: r.network.String(), address: slot.observed.identity.String()})
	if trust.authorityReadErr != nil || !present || record.body.Seq != slot.verified.evidence.sequence || !recordRequiresSessionV2(record.body.MinSession) || !bytes.Equal(record.record.Body, slot.body[:slot.bodyLen]) || !bytes.Equal(record.record.Sig, slot.signature[:]) {
		return relationAdmissionWait, errRelationAdmissionStale
	}
	if !slot.observed.matchesLocked(store) {
		return relationAdmissionWait, errPinMetadataSnapshotStale
	}
	metadata := store.identities[slot.observed.identity].classMetadata()
	if _, present := store.identities[slot.observed.identity]; !present && store.pinRecovery != nil && store.pinRecovery.permit != nil && store.pinRecovery.permit.Binding.Identity == slot.observed.identity {
		binding := store.pinRecovery.permit.Binding
		backup, err := binding.Backup.Restore(binding.Identity, binding.PreviousPinnedAt)
		if err != nil {
			return relationAdmissionWait, err
		}
		metadata = backup.Metadata
	}
	contact := trust.contacts[slot.observed.identity.String()].LocalContact
	if metadata.Contact.State == domain.IdentityPinAllocationEligible || (contact != nil && contact.revision != 0 && contact.confirmed == contact.revision && contact.fact.Network == r.network && contact.fact.Identity == slot.observed.identity) {
		return relationAdmissionContactPath, nil
	}
	trial, ready := a.rateTrial(now)
	if !ready {
		return relationAdmissionWait, nil
	}
	stage, err := store.stageRelationRecordLocked(slot.observed, slot.verified)
	if err != nil {
		return relationAdmissionWait, err
	}
	// NO fallible branch between real stage, rate debit and earned publication.
	slot.owed = owedRelationProtection{service: r.svc, binding: stage.binding, relation: store.identities[slot.observed.identity].classMetadata().Relation}
	slot.completion = relationRecordCompletion{state: relationRecordStagedOwed}
	slot.phase = relationAdmissionEarnedOwed
	a.rate = trial
	// Raw evidence is no longer authority for re-staging/re-debiting this claim.
	clear(slot.body[:])
	clear(slot.signature[:])
	clear(slot.proof[:])
	slot.bodyLen = 0
	slot.verified = verifiedRelationPinRecord{}
	return relationAdmissionStaged, nil
}
