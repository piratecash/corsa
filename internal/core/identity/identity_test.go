package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreatePersistsAddress(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "identity.json")

	first, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("LoadOrCreate first call failed: %v", err)
	}

	second, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("LoadOrCreate second call failed: %v", err)
	}

	if first.Address == "" {
		t.Fatal("expected non-empty address")
	}
	if first.Address != second.Address {
		t.Fatalf("expected persisted address, got %q and %q", first.Address, second.Address)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected identity file to exist: %v", err)
	}
}

func TestVerifyBoxKeyBinding(t *testing.T) {
	t.Parallel()

	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	signature := SignBoxKeyBinding(id)
	if err := VerifyBoxKeyBinding(id.Address, PublicKeyBase64(id.PublicKey), BoxPublicKeyBase64(id.BoxPublicKey), signature); err != nil {
		t.Fatalf("VerifyBoxKeyBinding failed: %v", err)
	}
}

func TestVerifyBoxKeyBindingRejectsTampering(t *testing.T) {
	t.Parallel()

	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	signature := SignBoxKeyBinding(id)
	if err := VerifyBoxKeyBinding(id.Address, PublicKeyBase64(id.PublicKey), BoxPublicKeyBase64(id.BoxPublicKey)+"tampered", signature); err == nil {
		t.Fatal("expected VerifyBoxKeyBinding to reject tampering")
	}
}
