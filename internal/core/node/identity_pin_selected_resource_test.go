package node

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// This measures the extra representation cost; it grants no D1 resource
// acceptance. The original unclassified 36-MiB regression remains unchanged.
func TestSelectedClassifiedStorageCostIsMeasured(t *testing.T) {
	store := loadSecureSessionStore("", newStoreClock().trusted())
	store.mu.Lock()
	for i := range maxPinnedIdentities {
		if err := store.recordPinEvidenceLocked(boundIdentity(i), store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	unclassified := store.snapshotLocked()
	store.mu.Unlock()
	plain, _, err := encodeIdentityPinSnapshot(unclassified)
	if err != nil {
		t.Fatal(err)
	}
	// Two collections discard both primary/victim sync.Pool scratch from the
	// preceding JSON encoding before measuring live metadata ownership.
	runtime.GC()
	base := heapInUse()
	allocation := selectedAllocation(t)
	store.mu.Lock()
	for i := range maxSelectedPins {
		id := boundIdentity(i)
		entry := store.identities[id]
		allocation.Reference = domain.IdentityPinEvidenceReference(fmt.Sprintf("cm:%d:%d", i+1, i+1))
		allocation.SourceGroup = domain.SourceGroup(strings.Clone(string(domain.SourceGroupLoopback)))
		if err := entry.setUnpublishedMetadata(domain.IdentityPinMetadata{Selected: allocation}); err != nil {
			store.mu.Unlock()
			t.Fatal("unowned classified-cost fixture refused metadata", err)
		}
		store.putPinEntryLocked(id, entry)
	}
	store.mu.Unlock()
	runtime.GC()
	live := heapInUse()
	store.mu.Lock()
	classed := store.snapshotLocked()
	store.mu.Unlock()
	encoded, _, err := encodeIdentityPinSnapshot(classed)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= len(plain) || store.stats().SelectedAllocations != maxSelectedPins {
		t.Fatal("measurement did not include classified entries")
	}
	var added uint64
	if live > base {
		added = live - base
	}
	t.Logf("4096 Selected / 20000 pins: additional live heap %.3f MiB; additional encoded bytes %d (not resource acceptance)", float64(added)/mib, len(encoded)-len(plain))
	runtime.KeepAlive(store)
	runtime.KeepAlive(unclassified)
	runtime.KeepAlive(plain)
	runtime.KeepAlive(classed)
	runtime.KeepAlive(encoded)
}
