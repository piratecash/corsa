package identity

import (
	"encoding/base64"
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

func TestValidateAddress(t *testing.T) {
	t.Parallel()

	// Generated address must pass validation.
	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if err := ValidateAddress(id.Address); err != nil {
		t.Errorf("generated address %q should be valid: %v", id.Address, err)
	}
	if len(id.Address) != AddressLength {
		t.Errorf("expected address length %d, got %d", AddressLength, len(id.Address))
	}

	// Valid 40-char lowercase hex.
	if err := ValidateAddress("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"); err != nil {
		t.Errorf("valid hex address rejected: %v", err)
	}

	// IsValidAddress convenience.
	if !IsValidAddress("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2") {
		t.Error("IsValidAddress should return true for valid address")
	}
}

func TestValidateAddressRejectsInvalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		addr string
	}{
		{"empty", ""},
		{"too_short", "a1b2c3d4e5f6"},
		{"too_long", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2ff"},
		{"uppercase", "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2"},
		{"mixed_case", "A1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"},
		{"non_hex", "g1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"},
		{"spaces", "a1b2c3d4 5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"},
		{"path_traversal", "../../etc/passwd/../../../a1b2c3d4e5f6"},
		{"sql_injection", "'; DROP TABLE routes;--a1b2c3d4e5f6a1b"},
		{"unicode", "а1б2в3г4д5е6ж1з2и3к4л5м6н7о8п9р0с1т2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateAddress(tc.addr); err == nil {
				t.Errorf("expected error for %q, got nil", tc.addr)
			}
			if IsValidAddress(tc.addr) {
				t.Errorf("IsValidAddress should return false for %q", tc.addr)
			}
		})
	}
}

func TestLoadReturnErrorWhenFileMissing(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nonexistent.json")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when identity file does not exist")
	}
}

func TestLoadReadsExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "identity.json")

	original, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.Address != original.Address {
		t.Fatalf("Load address = %q, want %q", loaded.Address, original.Address)
	}
}

func TestFromPrivateKeyBase64ProducesSameAddress(t *testing.T) {
	t.Parallel()

	original, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	privKeyB64 := base64.StdEncoding.EncodeToString(original.PrivateKey)
	restored, err := FromPrivateKeyBase64(privKeyB64)
	if err != nil {
		t.Fatalf("FromPrivateKeyBase64: %v", err)
	}

	if restored.Address != original.Address {
		t.Fatalf("address mismatch: got %q, want %q", restored.Address, original.Address)
	}
}

func TestFromPrivateKeyBase64DeterministicBoxKey(t *testing.T) {
	t.Parallel()

	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	privKeyB64 := base64.StdEncoding.EncodeToString(id.PrivateKey)

	first, err := FromPrivateKeyBase64(privKeyB64)
	if err != nil {
		t.Fatalf("first FromPrivateKeyBase64: %v", err)
	}
	second, err := FromPrivateKeyBase64(privKeyB64)
	if err != nil {
		t.Fatalf("second FromPrivateKeyBase64: %v", err)
	}

	if BoxPublicKeyBase64(first.BoxPublicKey) != BoxPublicKeyBase64(second.BoxPublicKey) {
		t.Fatal("box public key is not deterministic across calls")
	}
}

func TestFromPrivateKeyBase64RejectsInvalidInput(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		key  string
	}{
		{"not_base64", "this-is-not-base64!!!"},
		{"wrong_size", base64.StdEncoding.EncodeToString([]byte("tooshort"))},
		{"empty", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FromPrivateKeyBase64(tc.key)
			if err == nil {
				t.Fatal("expected error for invalid private key input")
			}
		})
	}
}

func TestSaveAndLoad(t *testing.T) {
	t.Parallel()

	id, err := Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	path := filepath.Join(t.TempDir(), "identity.json")
	if err := Save(path, id); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Address != id.Address {
		t.Fatalf("address mismatch after Save/Load: got %q, want %q", loaded.Address, id.Address)
	}
}

func TestFingerprintProducesValidAddress(t *testing.T) {
	t.Parallel()

	// Verify that Fingerprint always produces valid addresses.
	for i := 0; i < 10; i++ {
		id, err := Generate()
		if err != nil {
			t.Fatalf("Generate failed: %v", err)
		}
		addr := Fingerprint(id.PublicKey)
		if err := ValidateAddress(addr); err != nil {
			t.Errorf("Fingerprint produced invalid address %q: %v", addr, err)
		}
	}
}
