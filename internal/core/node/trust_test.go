package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestRecordLastOnlineAtPersistsAndDoesNotRegress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trust.json")
	self := domaintest.ID("last-online-self")
	peer := domaintest.ID("last-online-peer")

	store, err := loadTrustStore(path, trustedContact{Address: self.String(), PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}
	if stored, err := store.remember(trustedContact{Address: peer.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember peer: stored=%v err=%v", stored, err)
	}

	want := time.Date(2026, time.August, 21, 9, 6, 16, 123456789, time.FixedZone("test", 2*60*60))
	if updated, err := store.recordLastOnlineAt([]domain.PeerIdentity{peer, domaintest.ID("unknown")}, want); err != nil || updated != 1 {
		t.Fatalf("recordLastOnlineAt: updated=%d err=%v", updated, err)
	}
	want = want.UTC()

	// A delayed writer must not move the durable observation backwards.
	if updated, err := store.recordLastOnlineAt([]domain.PeerIdentity{peer}, want.Add(-time.Hour)); err != nil || updated != 0 {
		t.Fatalf("older recordLastOnlineAt: updated=%d err=%v", updated, err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trust file: %v", err)
	}
	var file trustFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("decode trust file: %v", err)
	}
	if file.Version != trustFileVersion {
		t.Fatalf("trust version = %d, want %d", file.Version, trustFileVersion)
	}
	if got := file.Contacts[peer.String()].LastOnlineAt; !got.Equal(want) {
		t.Fatalf("persisted last_online_at = %v, want %v", got, want)
	}
	if _, ok := file.Contacts[domaintest.ID("unknown").String()]; ok {
		t.Fatal("last-online observation created an untrusted contact")
	}

	reloaded, err := loadTrustStore(path, trustedContact{Address: self.String(), PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("reload trust store: %v", err)
	}
	if got := reloaded.trustedContacts()[peer.String()].LastOnlineAt; !got.Equal(want) {
		t.Fatalf("reloaded last_online_at = %v, want %v", got, want)
	}
}

// TestForgetRemovesContact verifies that forget deletes the contact from
// the in-memory map and persists the change to disk.
func TestForgetRemovesContact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	// forget(id) deletes by id.String(); contacts are keyed by the
	// trustedContact.Address string. Derive both from the same
	// PeerIdentity so the round-trip key matches.
	aaa := domaintest.ID("aaa")
	bbb := domaintest.ID("bbb")

	store, err := loadTrustStore(path, trustedContact{
		Address: domaintest.ID("self").String(),
		PubKey:  "pk-self",
	})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	// Add two contacts.
	if stored, err := store.remember(trustedContact{Address: aaa.String(), PubKey: "pk-a", BoxKey: "bk-a"}); err != nil || !stored {
		t.Fatalf("remember aaa: stored=%v err=%v", stored, err)
	}
	if stored, err := store.remember(trustedContact{Address: bbb.String(), PubKey: "pk-b", BoxKey: "bk-b"}); err != nil || !stored {
		t.Fatalf("remember bbb: stored=%v err=%v", stored, err)
	}

	// Forget aaa.
	removed, err := store.forget(aaa)
	if err != nil {
		t.Fatalf("forget aaa: %v", err)
	}
	if !removed {
		t.Fatal("forget should return true for existing contact")
	}

	// In-memory check.
	contacts := store.trustedContacts()
	if _, ok := contacts[aaa.String()]; ok {
		t.Fatal("aaa should not be in trustedContacts after forget")
	}
	if _, ok := contacts[bbb.String()]; !ok {
		t.Fatal("bbb should still exist after forgetting aaa")
	}

	// Persistence check — reload from disk.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trust file: %v", err)
	}
	var file trustFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("unmarshal trust file: %v", err)
	}
	if _, ok := file.Contacts[aaa.String()]; ok {
		t.Fatal("aaa should not be in persisted trust file")
	}
	if _, ok := file.Contacts[bbb.String()]; !ok {
		t.Fatal("bbb should be in persisted trust file")
	}
}

// TestForgetNonExistentContact verifies that forgetting an unknown address
// returns false without error.
func TestForgetNonExistentContact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	store, err := loadTrustStore(path, trustedContact{Address: domaintest.ID("self").String(), PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	removed, err := store.forget(domaintest.ID("unknown"))
	if err != nil {
		t.Fatalf("forget unknown: %v", err)
	}
	if removed {
		t.Fatal("forget should return false for non-existent contact")
	}
}

// TestForgetClearsConflict verifies that forget also removes any recorded
// conflict for the address.
func TestForgetClearsConflict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	store, err := loadTrustStore(path, trustedContact{Address: domaintest.ID("self").String(), PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	ccc := domaintest.ID("ccc")

	// Add a contact then trigger a conflict by remembering with different keys.
	_, _ = store.remember(trustedContact{Address: ccc.String(), PubKey: "pk-c1", BoxKey: "bk-c1"})
	_, _ = store.remember(trustedContact{Address: ccc.String(), PubKey: "pk-c2", BoxKey: "bk-c2"}) // conflict

	store.mu.RLock()
	_, hasConflict := store.conflicts[ccc.String()]
	store.mu.RUnlock()
	if !hasConflict {
		t.Fatal("expected conflict for ccc after key mismatch")
	}

	removed, err := store.forget(ccc)
	if err != nil {
		t.Fatalf("forget ccc: %v", err)
	}
	if !removed {
		t.Fatal("forget should return true")
	}

	store.mu.RLock()
	_, stillConflict := store.conflicts["ccc"]
	store.mu.RUnlock()
	if stillConflict {
		t.Fatal("conflict should be cleared after forget")
	}
}
