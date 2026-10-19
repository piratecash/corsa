package node

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// This measures the new public authority/metadata costs; it is not a resource
// admission budget. The existing unclassified 36-MiB regression is unchanged.
func TestK1LocalFactAndClassifiedStorageCost(t *testing.T) {
	const count = 1000
	trust, err := loadTrustStore(filepath.Join(t.TempDir(), "trust.json"), trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]domain.PeerIdentity, 0, count)
	for range count {
		peer, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		intent := contactIntent(peer)
		ids = append(ids, intent.Identity)
		trust.contacts[peer.Address] = trustedContact{Address: peer.Address, PubKey: string(intent.PubKey), BoxKey: string(intent.BoxKey), BoxSignature: string(intent.BoxSignature), Source: "resource_fixture"}
	}
	trust.mu.Lock()
	legacy := trust.snapshotLocked()
	trust.mu.Unlock()
	legacyRaw, err := encodeTrustSnapshot(legacy)
	if err != nil {
		t.Fatal(err)
	}
	before := heapInUse()
	for _, id := range ids {
		if _, err := trust.stageLocalContact(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}
	factHeap := heapInUse()
	runtime.KeepAlive(trust)
	trust.mu.Lock()
	facts := trust.snapshotLocked()
	trust.mu.Unlock()
	factRaw, err := encodeTrustSnapshot(facts)
	if err != nil {
		t.Fatal(err)
	}
	store := loadSecureSessionStore(filepath.Join(t.TempDir(), "pins.json"), newStoreClock().trusted())
	store.mu.Lock()
	for _, id := range ids {
		if _, err := store.pinLocked(id, store.now()); err != nil {
			store.mu.Unlock()
			t.Fatal(err)
		}
	}
	plain := store.snapshotLocked()
	store.mu.Unlock()
	plainRaw, _, err := encodeIdentityPinSnapshot(plain)
	if err != nil {
		t.Fatal(err)
	}
	plainHeap := heapInUse()
	for _, id := range ids {
		store.mu.Lock()
		binding := store.pinEntryBindingLocked(id)
		store.mu.Unlock()
		trust.mu.RLock()
		fact := trust.contacts[id.String()].LocalContact.fact
		trust.mu.RUnlock()
		allocation := domain.IdentityPinAllocation{State: domain.IdentityPinAllocationEligible, Origin: domain.IdentityPinOriginLocalContact, At: fact.At, Reference: domain.IdentityPinEvidenceReference(fact.ID.String())}
		if _, err := store.stageContactPin(t.Context(), binding, allocation); err != nil {
			t.Fatal(err)
		}
	}
	classHeap := heapInUse()
	runtime.KeepAlive(store)
	runtime.KeepAlive(trust)
	store.mu.Lock()
	classed := store.snapshotLocked()
	store.mu.Unlock()
	classRaw, _, err := encodeIdentityPinSnapshot(classed)
	if err != nil {
		t.Fatal(err)
	}
	if len(factRaw) <= len(legacyRaw) || len(classRaw) <= len(plainRaw) {
		t.Fatal("cost fixture omitted new authority/metadata")
	}
	t.Logf("structural allocation floors: optional_row_marker=%d optional_fact_state=%d optional_pending_proof=%d; exclude map/string backing and allocator overhead", domain.SizeOfAll(localContactRowIncarnation{}), domain.SizeOfAll(localContactState{}), domain.SizeOfAll(localContactProof{}))
	t.Logf("rows=%d contact_file_plain=%d contact_file_facts=%d fact_file_delta=%d fact_heap_before=%d fact_heap_after=%d pin_file_plain=%d pin_file_K=%d K_file_delta=%d pin_heap_before=%d pin_heap_after=%d; measurements only, no D1 admission", count, len(legacyRaw), len(factRaw), len(factRaw)-len(legacyRaw), before, factHeap, len(plainRaw), len(classRaw), len(classRaw)-len(plainRaw), plainHeap, classHeap)
}
