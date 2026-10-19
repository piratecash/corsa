package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

type selectedTestClock struct {
	base    time.Time
	elapsed atomic.Int64
}

func (c *selectedTestClock) Now() time.Time          { return c.base.Add(time.Duration(c.elapsed.Load())) }
func (c *selectedTestClock) advance(d time.Duration) { c.elapsed.Add(int64(d)) }
func selectedFixture(t *testing.T) (*secureSessionStore, identityPinEntryBinding, identityPinDurableFiles) {
	t.Helper()
	store, binding, _, files := pinTransactionFixture(t)
	store.mu.Lock()
	lease := store.pinEntryBindingLocked(binding.Identity)
	store.mu.Unlock()
	return store, lease, files
}
func selectedAllocation(t *testing.T) domain.IdentityPinAllocation {
	t.Helper()
	return domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginSelectedSession, At: newStoreClock().now.UTC(), SourceGroup: domain.SourceGroupLoopback, Reference: "cm:1:1"}
}
func TestSelectedPinStrictAcknowledgmentAndRestart(t *testing.T) {
	store, lease, files := selectedFixture(t)
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	if store.selectedPinStatus(lease).confirmed {
		t.Fatal("memory staging claimed durable class")
	}
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	if store.selectedPinStatus(lease).confirmed {
		t.Fatal("ordinary writer claimed strict confirmation")
	}
	if _, err := store.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	if !store.selectedPinStatus(lease).confirmed {
		t.Fatal("strict committed snapshot was not acknowledged")
	}
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	restarted.mu.Lock()
	fresh := restarted.pinEntryBindingLocked(lease.identity)
	restarted.mu.Unlock()
	if !restarted.selectedPinStatus(fresh).allocated || restarted.selectedPinStatus(fresh).confirmed {
		t.Fatal("restart dropped eligibility or invented runtime acknowledgment")
	}
	if _, err := restarted.confirmPinMetadata(t.Context(), files); err != nil {
		t.Fatal(err)
	}
	if !restarted.selectedPinStatus(fresh).confirmed {
		t.Fatal("persisted eligibility required another session/ten minutes")
	}
}
func TestSelectedPinIncarnationSurvivesMetadataAndReproofButNotRecreate(t *testing.T) {
	store, lease, _ := selectedFixture(t)
	stageMetadata(t, store, lease.identity, pinClassMetadataFixture(t))
	if err := store.noteProvenInbound(lease.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
		t.Fatalf("valid session lease lost after metadata/reproof: %v", err)
	}
	store.mu.Lock()
	store.deletePinEntryLocked(lease.identity)
	store.changes++
	store.mu.Unlock()
	if err := store.noteProvenInbound(lease.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); !errors.Is(err, errPinMetadataSnapshotStale) {
		t.Fatalf("old lease rebound to a recreated entry: %v", err)
	}
}
func TestSelectedPinQuotaIncludesUnconfirmedAndRecoveryReservation(t *testing.T) {
	store, lease, _ := selectedFixture(t)
	allocation := selectedAllocation(t)
	store.mu.Lock()
	for i := 1; i <= maxSelectedPins; i++ {
		id := domain.PeerIdentity{88, byte(i), byte(i >> 8)}
		if err := store.recordPinEvidenceLocked(id, allocation.At); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		entry := store.identities[id]
		if err := entry.setUnpublishedMetadata(domain.IdentityPinMetadata{Selected: allocation}); err != nil {
			store.mu.Unlock()
			t.Fatal("unowned selected quota fixture refused metadata", err)
		}
		store.putPinEntryLocked(id, entry)
	}
	store.mu.Unlock()
	if _, err := store.stageSelectedPin(t.Context(), lease, allocation); !errors.Is(err, errSelectedPinQuotaFull) {
		t.Fatalf("class full admitted extra: %v", err)
	}
	if !store.identityPinned(lease.identity) || store.selectedPinStatus(lease).allocated {
		t.Fatal("class refusal removed pin or fabricated class")
	}
	store.mu.Lock()
	id := domain.PeerIdentity{88, 1, 0}
	entry := store.identities[id]
	backup, err := domain.NewIdentityPinBackup(id, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		store.mu.Unlock()
		t.Fatal(err)
	}
	store.deletePinEntryLocked(id)
	store.pinRecovery = &identityPinRecoveryState{permit: &domain.IdentityPinRemovalPermit{Binding: domain.IdentityPinFenceBinding{Identity: id, PreviousPinnedAt: entry.pinnedAt, Backup: backup}}}
	store.mu.Unlock()
	if _, err := store.stageSelectedPin(t.Context(), lease, allocation); !errors.Is(err, errSelectedPinQuotaFull) {
		t.Fatalf("absent C3 reservation freed class capacity: %v", err)
	}
}
func TestSelectedPinUnknownCommitNeverAcknowledgesOrRollsBack(t *testing.T) {
	store, lease, real := selectedFixture(t)
	if _, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t)); err != nil {
		t.Fatal(err)
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil)
	injected := errors.New("directory sync failed after rename")
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		if _, err := real.WriteMain(ctx, raw); err != nil {
			return secretfile.DurableNotCommitted, err
		}
		return secretfile.DurableIndeterminate, injected
	}).Once()
	if _, err := store.confirmPinMetadata(t.Context(), files); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if store.selectedPinStatus(lease).confirmed || !store.selectedPinStatus(lease).allocated {
		t.Fatal("unknown write confirmed or discarded allocation")
	}
	raw, err := os.ReadFile(filepath.Join(store.dir, store.name))
	if err != nil {
		t.Fatal(err)
	}
	file, err := decodeSecureSessionFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	if file.IdentityMetadata[lease.identity].Selected.State != domain.IdentityPinAllocationEligible {
		t.Fatal("test did not exercise a visible candidate despite write error")
	}
	if _, err := store.confirmPinMetadata(t.Context(), real); err != nil {
		t.Fatal(err)
	}
	if !store.selectedPinStatus(lease).confirmed {
		t.Fatal("strict retry did not confirm")
	}
}

func seedSelectedAllocations(t *testing.T, store *secureSessionStore, count int) {
	t.Helper()
	allocation := selectedAllocation(t)
	store.mu.Lock()
	for i := 1; i <= count; i++ {
		id := domain.PeerIdentity{89, byte(i), byte(i >> 8)}
		if err := store.recordPinEvidenceLocked(id, allocation.At); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		entry := store.identities[id]
		if err := entry.setUnpublishedMetadata(domain.IdentityPinMetadata{Selected: allocation}); err != nil {
			store.mu.Unlock()
			t.Fatal("unowned selected quota fixture refused metadata", err)
		}
		store.putPinEntryLocked(id, entry)
	}
	store.changes++
	store.mu.Unlock()
}

func TestSelectedEarlierOverCapacityFilePreservesMetadataAndRefusesNewAllocation(t *testing.T) {
	store, lease, _ := selectedFixture(t)
	seedSelectedAllocations(t, store, maxSelectedPins+1)
	if err := store.persist(); err != nil {
		t.Fatal(err)
	}
	reloaded := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	stats := reloaded.stats()
	if stats.PinnedIdentities != maxSelectedPins+2 || stats.SelectedAllocations != maxSelectedPins+1 || !stats.SelectedOverCapacity || !stats.SelectedAccountingAvailable || stats.SelectedConfirmed != 0 {
		t.Fatalf("incorrect preparation-file diagnostics: %+v", stats)
	}
	reloaded.mu.Lock()
	binding := reloaded.pinEntryBindingLocked(lease.identity)
	old := reloaded.identities[domain.PeerIdentity{89, 1}].classMetadata()
	reloaded.mu.Unlock()
	if old.Selected.State != domain.IdentityPinAllocationEligible {
		t.Fatal("preparation file metadata was demoted")
	}
	if _, err := reloaded.stageSelectedPin(t.Context(), binding, selectedAllocation(t)); !errors.Is(err, errSelectedPinQuotaFull) {
		t.Fatalf("overcapacity admitted a new allocation: %v", err)
	}
	if !reloaded.identityPinned(lease.identity) {
		t.Fatal("allocation refusal dropped an ordinary pin")
	}
}

func TestSelectedAdmissionAtomicallyAccountsForLastClassPlace(t *testing.T) {
	store, _, _ := selectedFixture(t)
	seedSelectedAllocations(t, store, maxSelectedPins-1)
	const contenders = 16
	leases := make([]identityPinEntryBinding, contenders)
	store.mu.Lock()
	for i := range contenders {
		id := domain.PeerIdentity{90, byte(i)}
		if err := store.recordPinEvidenceLocked(id, store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
		leases[i] = store.pinEntryBindingLocked(id)
	}
	store.mu.Unlock()
	var accepted atomic.Int32
	var group sync.WaitGroup
	for _, lease := range leases {
		group.Go(func() {
			_, err := store.stageSelectedPin(t.Context(), lease, selectedAllocation(t))
			if err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, errSelectedPinQuotaFull) {
				t.Errorf("unexpected admission: %v", err)
			}
		})
	}
	group.Wait()
	if accepted.Load() != 1 || store.stats().SelectedAllocations != maxSelectedPins {
		t.Fatalf("last slot admitted %d; stats=%+v", accepted.Load(), store.stats())
	}
}
