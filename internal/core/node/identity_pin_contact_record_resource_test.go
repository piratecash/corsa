package node

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

func fillContactRecordPins(t *testing.T, store *secureSessionStore, count int) {
	t.Helper()
	store.pinMutationGate.RLock()
	defer store.pinMutationGate.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
	for n := uint64(0); len(store.identities) < count; n++ {
		id := domain.PeerIdentity{0xe3}
		binary.BigEndian.PutUint64(id[1:9], n)
		// Logical capacity fixture uses the typed historical record constructor,
		// not a fabricated TLS proof or measured resource-admission profile.
		if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContactRecordHardGlobalBoundIncludingNewKAndExistingIdentity(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_K_refuses", true: "existing_classifies"}[existing], func(t *testing.T) {
			s, peer := localContactFixture(t)
			addLocalContact(t, s, peer)
			store := s.secureSessions.store
			id := domain.PeerIdentityFromWire(peer.Address)
			if existing {
				store.mu.Lock()
				err := store.recordPinEvidenceLocked(id, store.now())
				store.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			_, verified := verifiedContactRecord(t, s, peer)
			fillContactRecordPins(t, store, maxPinnedIdentities)
			store.mu.Lock()
			beforeEvidence, beforeProtection, beforeChanges := store.evidenceRevision, store.protectionRevision, store.changes
			store.mu.Unlock()
			_, err := s.stageContactRecord(t.Context(), verified)
			if existing {
				if err != nil || store.contactKStatus(id) != domain.LocalContactKStagedUnconfirmed {
					t.Fatal("existing K required another global place", err)
				}
			} else {
				if !errors.Is(err, errPinStoreFull) || store.identityPinned(id) {
					t.Fatal("new K bypassed hard20k", err)
				}
				store.mu.Lock()
				defer store.mu.Unlock()
				if store.evidenceRevision != beforeEvidence || store.protectionRevision != beforeProtection || store.changes != beforeChanges {
					t.Fatal("full refusal partially revised counters")
				}
			}
		})
	}
}

func TestContactRecordSameIdentityC3ReservationRestoresCompleteBackup(t *testing.T) {
	s, peer := localContactFixture(t)
	_, oldStage, _ := stagedContactRecord(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	metadata := metadataSnapshot(t, store, id).metadata
	metadata.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: store.now(), Reference: "earlier-independent-R"}
	stageMetadata(t, store, id, metadata)
	store.mu.Lock()
	previous := store.identities[id]
	store.mu.Unlock()
	removeContactPinForTest(t, s, id)
	fillContactRecordPins(t, store, maxPinnedIdentities-1)
	_, verified := verifiedContactRecord(t, s, peer)
	stage, err := s.stageContactRecord(t.Context(), verified)
	if err != nil || stage.owed == oldStage.owed {
		t.Fatal("fresh original observation could not consume own C3 reservation", err)
	}
	store.mu.Lock()
	current := store.identities[id]
	rawR, err := store.relationAllocationsLocked()
	count := len(store.identities)
	store.mu.Unlock()
	if err != nil || count != maxPinnedIdentities || rawR != 1 || current.incarnation == previous.incarnation || current.classMetadata() != previous.classMetadata() || current.pinnedAt != previous.pinnedAt {
		t.Fatal("same-identity reserved restore laundered historical metadata/timestamp")
	}
	if _, err := s.completeOwedContactRecord(t.Context(), oldStage, NewMockidentityPinDurableFiles(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatal("restored backup rebound old owed ticket", err)
	}
}

func TestContactRecordKDoesNotDiscountRawRelationCapacity(t *testing.T) {
	s, peer := localContactFixture(t)
	addLocalContact(t, s, peer)
	store := s.secureSessions.store
	id := domain.PeerIdentityFromWire(peer.Address)
	store.pinMutationGate.RLock()
	store.mu.Lock()
	if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
		t.Fatal(err)
	}
	metadata := store.identities[id].classMetadata()
	metadata.Relation = domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginUserLookup, At: store.now()}
	entry := store.identities[id]
	if err := entry.setUnpublishedMetadata(metadata); err != nil {
		store.mu.Unlock()
		store.pinMutationGate.RUnlock()
		t.Fatal("unowned raw-R fixture metadata refused", err)
	}
	store.putPinEntryLocked(id, entry)
	for n := 1; n < maxRelationPins; n++ {
		other := domain.PeerIdentity{0xc4}
		binary.BigEndian.PutUint64(other[1:9], uint64(n))
		if err := store.recordPinEvidenceLocked(other, store.now()); err != nil {
			t.Fatal(err)
		}
		entry := store.identities[other]
		if err := entry.setUnpublishedMetadata(metadata); err != nil {
			store.mu.Unlock()
			store.pinMutationGate.RUnlock()
			t.Fatal("unowned raw-R fixture metadata refused", err)
		}
		store.putPinEntryLocked(other, entry)
	}
	store.mu.Unlock()
	store.pinMutationGate.RUnlock()
	_, verified := verifiedContactRecord(t, s, peer)
	if _, err := s.stageContactRecord(t.Context(), verified); err != nil {
		t.Fatal("K incorrectly required an R slot", err)
	}
	stats := store.stats()
	if stats.RelationAllocations != maxRelationPins || stats.RelationCapacity != maxRelationPins {
		t.Fatal("K discounted raw R8192 allocations")
	}
}

func TestContactReconciliationSamePhysicalInventory(t *testing.T) {
	var owner relationRecordAdmission
	if len(owner.slots) != 256 {
		t.Fatal("reconciliation introduced second capacity")
	}
	// Each shared slot has mutually exclusive typed provenance; one worker cursor
	// and one transient bounded raw capsule add cost. Existing full trust/pin
	// snapshots and O(total contacts) scan/strict ACK work remain proportional.
	for i := range owner.slots {
		if owner.slots[i].kAuthority != nil || owner.slots[i].kReconciliation != nil {
			t.Fatal("zero slot retains authority")
		}
	}
	if len(owner.slots[0].body)*len(owner.slots) != 524288 {
		t.Fatal("shared raw body table changed")
	}
}
