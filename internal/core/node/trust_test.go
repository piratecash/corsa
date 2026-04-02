package node

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"corsa/internal/core/domain"
)

// TestForgetRemovesContact verifies that forget deletes the contact from
// the in-memory map and persists the change to disk.
func TestForgetRemovesContact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	store, err := loadTrustStore(path, trustedContact{
		Address: "self",
		PubKey:  "pk-self",
	})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	// Add two contacts.
	if err := store.remember(trustedContact{Address: "aaa", PubKey: "pk-a", BoxKey: "bk-a"}); err != nil {
		t.Fatalf("remember aaa: %v", err)
	}
	if err := store.remember(trustedContact{Address: "bbb", PubKey: "pk-b", BoxKey: "bk-b"}); err != nil {
		t.Fatalf("remember bbb: %v", err)
	}

	// Forget aaa.
	removed, err := store.forget(domain.PeerIdentity("aaa"))
	if err != nil {
		t.Fatalf("forget aaa: %v", err)
	}
	if !removed {
		t.Fatal("forget should return true for existing contact")
	}

	// In-memory check.
	contacts := store.trustedContacts()
	if _, ok := contacts["aaa"]; ok {
		t.Fatal("aaa should not be in trustedContacts after forget")
	}
	if _, ok := contacts["bbb"]; !ok {
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
	if _, ok := file.Contacts["aaa"]; ok {
		t.Fatal("aaa should not be in persisted trust file")
	}
	if _, ok := file.Contacts["bbb"]; !ok {
		t.Fatal("bbb should be in persisted trust file")
	}
}

// TestForgetNonExistentContact verifies that forgetting an unknown address
// returns false without error.
func TestForgetNonExistentContact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trust.json")

	store, err := loadTrustStore(path, trustedContact{Address: "self", PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	removed, err := store.forget(domain.PeerIdentity("unknown"))
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

	store, err := loadTrustStore(path, trustedContact{Address: "self", PubKey: "pk-self"})
	if err != nil {
		t.Fatalf("loadTrustStore: %v", err)
	}

	// Add a contact then trigger a conflict by remembering with different keys.
	_ = store.remember(trustedContact{Address: "ccc", PubKey: "pk-c1", BoxKey: "bk-c1"})
	_ = store.remember(trustedContact{Address: "ccc", PubKey: "pk-c2", BoxKey: "bk-c2"}) // conflict

	store.mu.RLock()
	_, hasConflict := store.conflicts["ccc"]
	store.mu.RUnlock()
	if !hasConflict {
		t.Fatal("expected conflict for ccc after key mismatch")
	}

	removed, err := store.forget(domain.PeerIdentity("ccc"))
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
