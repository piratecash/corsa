package node

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/stretchr/testify/mock"
)

func metadataSnapshot(t *testing.T, store *secureSessionStore, id domain.PeerIdentity) identityPinMetadataSnapshot {
	t.Helper()
	snapshot, err := store.pinMetadataSnapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func stageMetadata(t *testing.T, store *secureSessionStore, id domain.PeerIdentity, next domain.IdentityPinMetadata) identityPinMetadataMutation {
	t.Helper()
	result, err := store.stagePinMetadata(metadataSnapshot(t, store, id), next)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPinMetadataOwnerCASPreventsABAAndPreservesProofSemantics(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	other := domain.PeerIdentity{92}
	if err := store.noteProvenInbound(other); err != nil {
		t.Fatal(err)
	}
	original := metadataSnapshot(t, store, binding.Identity)
	oldPreview, otherPreview := pinRemovalPreview(t, store, binding.Identity), pinRemovalPreview(t, store, other)
	changed := stageMetadata(t, store, binding.Identity, pinClassMetadataFixture(t))
	if changed.state != pinMetadataStagedUnflushed {
		t.Fatal("memory mutation claimed another boundary")
	}
	stageMetadata(t, store, binding.Identity, original.metadata)
	current := metadataSnapshot(t, store, binding.Identity)
	store.mu.Lock()
	oldErr := store.validateIdentityPinTokenLocked(binding.Identity, oldPreview.Token)
	otherErr := store.validateIdentityPinTokenLocked(other, otherPreview.Token)
	store.mu.Unlock()
	if !errors.Is(oldErr, domain.ErrIdentityPinConfirmationStale) || otherErr != nil {
		t.Fatalf("ABA revived a token or invalidated another identity: %v / %v", oldErr, otherErr)
	}
	if current.metadata != original.metadata || current.evidenceRevision != original.evidenceRevision || current.protectionRevision <= original.protectionRevision {
		t.Fatal("metadata ABA changed proof or reset protection revision")
	}
	if _, err := store.stagePinMetadata(original, pinClassMetadataFixture(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("stale ABA snapshot mutated entry: %v", err)
	}
}

func TestPinMetadataCASRejectsForeignZeroReproofAndRecreatedSnapshots(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	original := metadataSnapshot(t, store, binding.Identity)
	foreign, _, _, _ := pinTransactionFixture(t)
	zeroProof, zeroProtection := original, original
	zeroProof.evidenceRevision, zeroProtection.protectionRevision = 0, 0
	for _, sample := range []identityPinMetadataSnapshot{{}, zeroProof, zeroProtection, metadataSnapshot(t, foreign, binding.Identity)} {
		if _, err := store.stagePinMetadata(sample, pinClassMetadataFixture(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
			t.Fatalf("unowned snapshot accepted: %v", err)
		}
	}
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatal(err)
	}
	reproved := metadataSnapshot(t, store, binding.Identity)
	if reproved.protectionRevision != original.protectionRevision || reproved.evidenceRevision == original.evidenceRevision {
		t.Fatal("normal reproof changed metadata revision or failed to advance proof")
	}
	if _, err := store.stagePinMetadata(original, pinClassMetadataFixture(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("stale proof snapshot accepted: %v", err)
	}
	store.mu.Lock()
	delete(store.identities, binding.Identity)
	store.changes++
	store.mu.Unlock()
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatal(err)
	}
	recreated := metadataSnapshot(t, store, binding.Identity)
	if recreated.protectionRevision <= reproved.protectionRevision {
		t.Fatal("recreated entry reused a protection revision")
	}
	if _, err := store.stagePinMetadata(reproved, pinClassMetadataFixture(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("removed entry snapshot mutated a recreated pin: %v", err)
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if _, err := restarted.stagePinMetadata(recreated, pinClassMetadataFixture(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("previous store epoch snapshot accepted after restart: %v", err)
	}
}

func TestPinMetadataCASNoopValidationAndOwnership(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	before := metadataSnapshot(t, store, binding.Identity)
	store.mu.Lock()
	changes := store.changes
	store.mu.Unlock()
	result, err := store.stagePinMetadata(before, before.metadata)
	if err != nil || result.state != pinMetadataUnchangedInMemory || result.protectionRevision != before.protectionRevision {
		t.Fatalf("unchanged memory CAS: %v %v", result, err)
	}
	candidate := pinClassMetadataFixture(t)
	stageMetadata(t, store, binding.Identity, candidate)
	candidate.Inbound.Reference = "outside caller mutation"
	after := metadataSnapshot(t, store, binding.Identity)
	if after.metadata == candidate || before.metadata != (domain.IdentityPinMetadata{}) {
		t.Fatal("caller mutation changed published or previous metadata")
	}
	after.metadata.Contact.Reference = "outside snapshot mutation"
	if metadataSnapshot(t, store, binding.Identity).metadata == after.metadata {
		t.Fatal("snapshot mutation reached live entry")
	}
	if _, err := store.stagePinMetadata(before, before.metadata); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("equal desired value bypassed stale CAS: %v", err)
	}
	current := metadataSnapshot(t, store, binding.Identity)
	if _, err := store.stagePinMetadata(current, domain.IdentityPinMetadata{Inbound: domain.IdentityPinAllocation{State: 99}}); !errors.Is(err, domain.ErrIdentityPinMetadata) {
		t.Fatalf("invalid metadata accepted: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.changes != changes+1 {
		t.Fatal("noop/rejected mutation dirtied the store")
	}
}

func TestPinMetadataRevisionsExhaustBeforeAnyEffect(t *testing.T) {
	for _, exhausted := range []string{"proof", "protection"} {
		t.Run(exhausted, func(t *testing.T) {
			store, _, _, _ := pinTransactionFixture(t)
			store.mu.Lock()
			if exhausted == "proof" {
				store.evidenceRevision = math.MaxUint64
			} else {
				store.protectionRevision = math.MaxUint64
			}
			proof, protection, changes := store.evidenceRevision, store.protectionRevision, store.changes
			before := store.snapshotLocked()
			store.mu.Unlock()
			if err := store.noteProvenInbound(domain.PeerIdentity{93}); !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
				t.Fatalf("new proof exhausted %s: %v", exhausted, err)
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.evidenceRevision != proof || store.protectionRevision != protection || store.changes != changes || !reflect.DeepEqual(store.snapshotLocked(), before) {
				t.Fatal("overflow changed counters or protection")
			}
		})
	}
	store, binding, _, _ := pinTransactionFixture(t)
	original := metadataSnapshot(t, store, binding.Identity)
	store.mu.Lock()
	store.protectionRevision = math.MaxUint64
	store.mu.Unlock()
	if _, err := store.stagePinMetadata(original, pinClassMetadataFixture(t)); !errors.Is(err, domain.ErrIdentityPinRevisionExhausted) {
		t.Fatalf("metadata overflow accepted: %v", err)
	}
	if got := metadataSnapshot(t, store, binding.Identity); got != original {
		t.Fatal("metadata overflow changed entry")
	}
	if err := store.noteProvenInbound(binding.Identity); err != nil {
		t.Fatalf("metadata counter exhaustion blocked an existing pin reproof: %v", err)
	}
}

func TestPinMetadataStagingNeverPromisesDiskCommit(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	next := pinClassMetadataFixture(t)
	stageMetadata(t, store, binding.Identity, next)
	path := filepath.Join(store.dir, store.name)
	readDisk := func() domain.IdentityPinMetadata {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := decodeSecureSessionFile(raw)
		if err != nil {
			t.Fatal(err)
		}
		return file.IdentityMetadata[binding.Identity]
	}
	if readDisk() == next {
		t.Fatal("memory-only primitive wrote the store")
	}
	injected := errors.New("write acknowledged locally but caller saw an error")
	ordinary := store.writeFile
	store.writeFile = func(raw []byte) error {
		if err := ordinary(raw); err != nil {
			return err
		}
		return injected
	}
	if err := store.persist(); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if readDisk() != next || metadataSnapshot(t, store, binding.Identity).metadata != next {
		t.Fatal("write error falsely rolled metadata back or promised old disk")
	}
	store.mu.Lock()
	dirty := store.changes != store.written
	store.mu.Unlock()
	if !dirty {
		t.Fatal("uncertain write marked staged metadata durable")
	}
	store.writeFile = ordinary
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
}

func TestPinMetadataStagingDuringEndpointFlushRemainsDirty(t *testing.T) {
	store, binding, _, _ := pinTransactionFixture(t)
	store.mu.Lock()
	key, err := canonicalEndpoint("192.0.2.21:64646")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := store.endpoints[key]
	endpoint.lastV2 = endpoint.lastV2.Add(-time.Second)
	store.endpoints[key] = endpoint
	store.changes++
	store.mu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	ordinary := store.writeFile
	store.writeFile = func(raw []byte) error {
		close(entered)
		<-release
		return ordinary(raw)
	}
	done := make(chan error, 1)
	go func() { done <- store.persist() }()
	<-entered
	next := pinClassMetadataFixture(t)
	stageMetadata(t, store, binding.Identity, next)
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	dirty := store.changes != store.written
	store.mu.Unlock()
	if !dirty {
		t.Fatal("older endpoint snapshot consumed a later staged generation")
	}
	store.writeFile = ordinary
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	loaded := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	if loaded.identities[binding.Identity].classMetadata() != next || !loaded.endpoints[key].lastV2.Equal(endpoint.lastV2) {
		t.Fatal("later flush lost metadata or concurrent endpoint state")
	}
}

func TestPinMetadataCASWaitsForRemovalAndRejectsItsOldEntry(t *testing.T) {
	store, binding, token, files := pinTransactionFixture(t)
	snapshot := metadataSnapshot(t, store, binding.Identity)
	permit := domain.IdentityPinRemovalPermit{OperationID: "metadata-removal-serialization", Binding: binding, Generation: 2}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	fence := NewMockIdentityPinRemovalFence(t)
	fence.EXPECT().BeginRemoval(mock.Anything, binding).RunAndReturn(func(_ context.Context, _ domain.IdentityPinFenceBinding) (domain.IdentityPinRemovalPermit, error) {
		close(entered)
		<-release
		return permit, nil
	}).Once()
	fence.EXPECT().RecordReceipt(mock.Anything, mock.Anything).Return(nil).Once()
	removed := make(chan error, 1)
	go func() { _, err := store.removeIdentityPin(t.Context(), binding, token, fence, files); removed <- err }()
	<-entered
	staged := make(chan error, 1)
	go func() { _, err := store.stagePinMetadata(snapshot, pinClassMetadataFixture(t)); staged <- err }()
	select {
	case err := <-staged:
		t.Fatalf("metadata mutation escaped destructive gate: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if err := <-staged; !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("old pre-removal snapshot accepted: %v", err)
	}
}

func TestPinMetadataCASRefusesUnreadableFatalAndZeroRevisionEntries(t *testing.T) {
	for _, refusal := range []string{"unreadable", "fatal", "zero_proof", "zero_protection"} {
		t.Run(refusal, func(t *testing.T) {
			store, binding, _, _ := pinTransactionFixture(t)
			snapshot := metadataSnapshot(t, store, binding.Identity)
			store.mu.Lock()
			entry := store.identities[binding.Identity]
			switch refusal {
			case "unreadable":
				store.unreadable = true
			case "fatal":
				store.pinRecovery = &identityPinRecoveryState{fatal: true}
			case "zero_proof":
				entry.evidenceRevision = 0
			case "zero_protection":
				entry.protectionRevision = 0
			}
			store.identities[binding.Identity] = entry
			changes, revision := store.changes, store.protectionRevision
			store.mu.Unlock()
			if _, err := store.pinMetadataSnapshot(binding.Identity); err == nil {
				t.Fatal("unavailable metadata produced an authoritative snapshot")
			}
			if _, err := store.stagePinMetadata(snapshot, pinClassMetadataFixture(t)); err == nil {
				t.Fatal("unavailable metadata accepted a mutation")
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.identities[binding.Identity] != entry || store.changes != changes || store.protectionRevision != revision {
				t.Fatal("refusal changed metadata or counters")
			}
		})
	}
}
