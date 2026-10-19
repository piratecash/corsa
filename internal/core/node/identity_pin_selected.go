package node

import (
	"context"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
)

const maxSelectedPins = 4096

var errSelectedPinQuotaFull = errors.New("identity pin selected allocation quota full")

type selectedPinStatus struct {
	current, allocated, confirmed, overCapacity bool
	allocations                                 int
}

// selectedAllocationsLocked requires the store mu; its cached tally has no independent owner.
func (s *secureSessionStore) selectedAllocationsLocked() (int, error) {
	n := s.selectedAllocations
	if s.pinRecovery != nil && s.pinRecovery.permit != nil {
		binding := s.pinRecovery.permit.Binding
		if _, present := s.identities[binding.Identity]; !present {
			record, err := binding.Backup.Restore(binding.Identity, binding.PreviousPinnedAt)
			if err != nil {
				return n, err
			}
			if record.Metadata.Selected.State != domain.IdentityPinAllocationAbsent {
				n++
			}
		}
	}
	return n, nil
}
func (s *secureSessionStore) selectedPinStatus(binding identityPinEntryBinding) selectedPinStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.selectedAllocationsLocked()
	status := selectedPinStatus{allocations: n, overCapacity: n > maxSelectedPins || err != nil}
	if !binding.matchesLocked(s) {
		return status
	}
	entry := s.identities[binding.identity]
	status.current = true
	status.allocated = entry.classMetadata().Selected.State != domain.IdentityPinAllocationAbsent
	status.confirmed = entry.selectedConfirmed()
	return status
}
func (s *secureSessionStore) stageSelectedPin(ctx context.Context, binding identityPinEntryBinding, allocation domain.IdentityPinAllocation) (identityPinMetadataMutation, error) {
	if err := ctx.Err(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	if allocation.State != domain.IdentityPinAllocationEligible || allocation.Origin != domain.IdentityPinOriginSelectedSession {
		return identityPinMetadataMutation{}, domain.ErrIdentityPinMetadata
	}
	s.pinMutationGate.RLock()
	defer s.pinMutationGate.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	if err := s.pinMetadataMutationRefusalLocked(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	if !binding.matchesLocked(s) {
		return identityPinMetadataMutation{}, errPinMetadataSnapshotStale
	}
	entry := s.identities[binding.identity]
	metadata := entry.classMetadata()
	if metadata.Selected.State == domain.IdentityPinAllocationEligible {
		return identityPinMetadataMutation{state: pinMetadataUnchangedInMemory, protectionRevision: entry.protectionRevision}, nil
	}
	n, err := s.selectedAllocationsLocked()
	if err != nil {
		return identityPinMetadataMutation{}, err
	}
	if metadata.Selected.State == domain.IdentityPinAllocationAbsent && n >= maxSelectedPins {
		return identityPinMetadataMutation{}, errSelectedPinQuotaFull
	}
	metadata.Selected = allocation
	if err := metadata.Validate(); err != nil {
		return identityPinMetadataMutation{}, err
	}
	expected := identityPinMetadataSnapshot{owner: s, identity: binding.identity, evidenceRevision: entry.evidenceRevision, protectionRevision: entry.protectionRevision, metadata: entry.classMetadata()}
	return s.stagePinMetadataLocked(expected, metadata)
}
func (s *secureSessionStore) selectedConfirmationNeeded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.identities {
		if entry.metadata != nil && entry.classMetadata().Selected.State == domain.IdentityPinAllocationEligible && entry.metadata.confirmedRevision != entry.protectionRevision {
			return true
		}
	}
	return false
}

// putPinEntryLocked requires the store mu and updates its derived counters atomically.
func (s *secureSessionStore) putPinEntryLocked(id domain.PeerIdentity, entry identityPinEntry) {
	old := s.identities[id]
	if old.inboundMetadataConfirmed() {
		s.inboundConfirmed--
	}
	if entry.inboundMetadataConfirmed() {
		s.inboundConfirmed++
	}
	if old.classMetadata().Inbound.State != domain.IdentityPinAllocationAbsent {
		s.inboundAllocations--
	}
	if entry.classMetadata().Inbound.State != domain.IdentityPinAllocationAbsent {
		s.inboundAllocations++
	}
	if old.classMetadata().Selected.State != domain.IdentityPinAllocationAbsent {
		s.selectedAllocations--
	}
	if old.selectedConfirmed() {
		s.selectedConfirmed--
	}
	if entry.classMetadata().Selected.State != domain.IdentityPinAllocationAbsent {
		s.selectedAllocations++
	}
	if entry.selectedConfirmed() {
		s.selectedConfirmed++
	}
	s.replaceRelationTalliesLocked(old, entry)
	s.identities[id] = entry
}

// deletePinEntryLocked requires the store mu.
func (s *secureSessionStore) deletePinEntryLocked(id domain.PeerIdentity) {
	entry := s.identities[id]
	if entry.inboundMetadataConfirmed() {
		s.inboundConfirmed--
	}
	if entry.classMetadata().Inbound.State != domain.IdentityPinAllocationAbsent {
		s.inboundAllocations--
	}
	if entry.classMetadata().Selected.State != domain.IdentityPinAllocationAbsent {
		s.selectedAllocations--
	}
	if entry.selectedConfirmed() {
		s.selectedConfirmed--
	}
	s.replaceRelationTalliesLocked(entry, identityPinEntry{})
	delete(s.identities, id)
}
func (e identityPinEntry) selectedConfirmed() bool {
	return e.protectionRevision != 0 && e.metadata != nil && e.classMetadata().Selected.State == domain.IdentityPinAllocationEligible && e.metadata.confirmedRevision == e.protectionRevision
}
