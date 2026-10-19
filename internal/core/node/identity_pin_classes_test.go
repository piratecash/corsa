package node

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

func pinClassMetadataFixture(t *testing.T) domain.IdentityPinMetadata {
	t.Helper()
	return domain.IdentityPinMetadata{Contact: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: newStoreClock().now.UTC(), Reference: "local-contact"}, Selected: domain.IdentityPinAllocation{State: domain.IdentityPinAllocationPending, Origin: domain.IdentityPinOriginSelectedSession, At: newStoreClock().now.UTC(), SourceGroup: domain.SourceGroupLoopback}}
}
func TestPinMetadataSurvivesReproofAndPersistence(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	metadata := pinClassMetadataFixture(t)
	stageMetadata(t, store, binding.Identity, metadata)
	store.mu.Lock()
	entry := store.identities[binding.Identity]
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, store, binding.Identity)
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	current := store.identities[binding.Identity]
	snapshot := store.snapshotLocked()
	store.mu.Unlock()
	if !reflect.DeepEqual(current.classMetadata(), metadata) {
		t.Fatal("reproof erased class metadata")
	}
	if !current.pinnedAt.Equal(entry.pinnedAt) {
		t.Fatal("reproof changed protection timestamp")
	}
	restarted := loadSecureSessionStore("", newStoreClock().trusted())
	if err := restarted.loadFile(snapshot); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restarted.identities[binding.Identity].classMetadata(), metadata) {
		t.Fatal("persistence erased metadata")
	}
	store.mu.Lock()
	err := store.validateIdentityPinTokenLocked(binding.Identity, preview.Token)
	store.mu.Unlock()
	if !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatal("reproof did not invalidate preview")
	}
}
func TestLegacyPinFileMigratesWithoutInventingProvenance(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	original := store.snapshotLocked()
	original.Format = 1
	original.IdentityMetadata = nil
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSecureSessionFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	restarted := loadSecureSessionStore("", newStoreClock().trusted())
	if err := restarted.loadFile(decoded); err != nil {
		t.Fatal(err)
	}
	snapshot := restarted.snapshotLocked()
	if snapshot.Format != 2 || len(snapshot.Identities) != len(original.Identities) || !reflect.DeepEqual(snapshot.Endpoints, original.Endpoints) {
		t.Fatal("migration lost pins/endpoints")
	}
	for id, at := range original.Identities {
		if !snapshot.Identities[id].Equal(at) || snapshot.IdentityMetadata[id] != (domain.IdentityPinMetadata{}) {
			t.Fatal("migration invented provenance or changed timestamp")
		}
	}
	if !restarted.identityPinned(binding.Identity) {
		t.Fatal("migration lost protection")
	}
}
func TestPinMetadataChangeInvalidatesPreviewWithoutReproof(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	preview := pinRemovalPreview(t, store, binding.Identity)
	stageMetadata(t, store, binding.Identity, pinClassMetadataFixture(t))
	store.mu.Lock()
	err := store.validateIdentityPinTokenLocked(binding.Identity, preview.Token)
	store.mu.Unlock()
	if !errors.Is(err, domain.ErrIdentityPinConfirmationStale) {
		t.Fatal("metadata change preserved stale removal token")
	}
}

func classedPinTransactionFixture(t *testing.T) (*secureSessionStore, domain.IdentityPinFenceBinding, domain.IdentityPinConfirmationToken, identityPinDurableFiles) {
	t.Helper()
	store, binding, _, files := pinTransactionFixture(t)
	stageMetadata(t, store, binding.Identity, pinClassMetadataFixture(t))
	store.mu.Lock()
	entry := store.identities[binding.Identity]
	store.mu.Unlock()
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	backup, err := domain.NewIdentityPinBackup(binding.Identity, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding.Backup = backup
	return store, binding, pinRemovalPreview(t, store, binding.Identity).Token, files
}

func pinBindingBackup(t testing.TB, b domain.IdentityPinFenceBinding) domain.IdentityPinFenceBinding {
	t.Helper()
	backup, err := domain.NewIdentityPinBackup(b.Identity, b.PreviousPinnedAt, domain.IdentityPinMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	b.Backup = backup
	return b
}
func supervisorBindingBackup(t testing.TB, b startupfence.Binding) startupfence.Binding {
	t.Helper()
	backup, err := domain.NewIdentityPinBackup(b.Identity, b.PreviousPinnedAt, domain.IdentityPinMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	b.Backup = backup
	return b
}

func TestLegacyMigrationAtCapacityPreservesAllUniquePins(t *testing.T) {
	source, binding, _, _ := pinTransactionFixture(t)
	legacy := source.snapshotLocked()
	legacy.Format = 1
	legacy.IdentityMetadata = nil
	for index := 1; index < maxPinnedIdentities; index++ {
		id := domain.PeerIdentity{0x81, byte(index), byte(index >> 8), byte(index >> 16)}
		legacy.Identities[id] = binding.PreviousPinnedAt.Add(time.Duration(index))
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSecureSessionFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	store := loadSecureSessionStore("", newStoreClock().trusted())
	if err := store.loadFile(decoded); err != nil {
		t.Fatal(err)
	}
	snapshot := store.snapshotLocked()
	if len(snapshot.Identities) != maxPinnedIdentities {
		t.Fatal("migration changed unique capacity")
	}
	for id, at := range legacy.Identities {
		if !snapshot.Identities[id].Equal(at) || snapshot.IdentityMetadata[id] != (domain.IdentityPinMetadata{}) {
			t.Fatal("migration lost or classified a legacy pin")
		}
	}
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatalf("existing reproof at capacity: %v", err)
	}
	if err := store.noteProvenInbound(domain.PeerIdentity{0xff, 0xff, 0xff}); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("new identity at capacity: %v", err)
	}
	if store.identityPinCapacityUsedLocked() != maxPinnedIdentities {
		t.Fatal("reproof duplicated global place")
	}
}
func TestCurrentPinFormatRejectsMissingUnknownOrForeignMetadata(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	file := store.snapshotLocked()
	for _, mutation := range []string{"missing", "foreign", "invalid", "unknown_format"} {
		t.Run(mutation, func(t *testing.T) {
			sample := file
			sample.IdentityMetadata = make(map[domain.PeerIdentity]domain.IdentityPinMetadata)
			for id, m := range file.IdentityMetadata {
				sample.IdentityMetadata[id] = m
			}
			switch mutation {
			case "missing":
				delete(sample.IdentityMetadata, binding.Identity)
			case "foreign":
				delete(sample.IdentityMetadata, binding.Identity)
				sample.IdentityMetadata[domain.PeerIdentity{44}] = domain.IdentityPinMetadata{}
			case "invalid":
				sample.IdentityMetadata[binding.Identity] = domain.IdentityPinMetadata{Contact: domain.IdentityPinAllocation{State: 99}}
			case "unknown_format":
				sample.Format = 99
			}
			raw, err := json.Marshal(sample)
			if err == nil {
				if _, err = decodeSecureSessionFile(raw); err == nil {
					t.Fatal("unknown/missing metadata admitted")
				}
			}
		})
	}
}

func TestPinMetadataOwnershipAndSnapshotIsolation(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	original := pinClassMetadataFixture(t)
	candidate := original
	stageMetadata(t, store, binding.Identity, candidate)
	store.mu.Lock()
	entry := store.identities[binding.Identity]
	snapshot := store.snapshotLocked()
	store.mu.Unlock()
	candidate.Contact.Reference = "mutated caller input"
	if entry.classMetadata() != original || snapshot.IdentityMetadata[binding.Identity] != original {
		t.Fatal("entry or snapshot aliases caller metadata")
	}
	next := original
	next.Contact.Reference = "new independently admitted fact"
	updated := entry
	if err := updated.setUnpublishedMetadata(next); err != nil {
		t.Fatal("independent unowned metadata copy refused", err)
	}
	if updated.classMetadata() != next || entry.classMetadata() != original || snapshot.IdentityMetadata[binding.Identity] != original {
		t.Fatal("replacement mutated previous entry or in-flight snapshot")
	}
	snapshot.IdentityMetadata[binding.Identity] = domain.IdentityPinMetadata{}
	if entry.classMetadata() != original {
		t.Fatal("snapshot map mutation reached live entry")
	}
	backup, err := domain.NewIdentityPinBackup(binding.Identity, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	record, err := backup.Restore(binding.Identity, entry.pinnedAt)
	if err != nil {
		t.Fatal(err)
	}
	record.Metadata.Contact.Reference = "mutated restored value"
	restoredAgain, err := backup.Restore(binding.Identity, entry.pinnedAt)
	if err != nil || restoredAgain.Metadata != original || entry.classMetadata() != original {
		t.Fatal("backup/restored values alias entry metadata")
	}
	copy := backup
	copy.Payload = "changed"
	copy.Digest = "changed"
	restoredAgain, err = backup.Restore(binding.Identity, entry.pinnedAt)
	if err != nil || restoredAgain.Metadata != original {
		t.Fatal("copied backup mutation reached retained canonical record")
	}
}
func TestInvalidFullBackupRefusesBeforeBrokerBeginAndMainWrite(t *testing.T) {
	for _, kind := range []string{"missing", "version", "digest", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			store, binding, token, files := classedPinTransactionFixture(t)
			before := store.snapshotLocked()
			switch kind {
			case "missing":
				binding.Backup = domain.IdentityPinBackup{}
			case "version":
				binding.Backup.Version++
			case "digest":
				binding.Backup.Digest = "wrong"
			case "oversized":
				binding.Backup.Payload = strings.Repeat("x", domain.MaxIdentityPinBackupBytes+1)
			}
			fence := NewMockIdentityPinRemovalFence(t) // No Begin or receipt is authorized by an invalid binding.
			result, err := store.removeIdentityPin(t.Context(), binding, token, fence, files)
			if !errors.Is(err, domain.ErrIdentityPinFenceMismatch) || result.Outcome != domain.IdentityPinRemovalNotCommitted {
				t.Fatalf("invalid backup: %v %v", result, err)
			}
			if !reflect.DeepEqual(before, store.snapshotLocked()) {
				t.Fatal("invalid backup changed main snapshot")
			}
		})
	}
}
