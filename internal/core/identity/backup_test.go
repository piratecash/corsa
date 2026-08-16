package identity

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// TestBackupRoundtrip: export → import restores both keys, the address and
// the record seq exactly.
func TestBackupRoundtrip(t *testing.T) {
	original, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	payload, err := ExportBackup(original, 42)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	restored, err := ImportBackup(payload)
	if err != nil {
		t.Fatalf("import: %v", err)
	}

	if restored.RecordSeq != 42 {
		t.Errorf("record seq = %d, want 42", restored.RecordSeq)
	}
	if restored.BoxKeyDerived {
		t.Error("full backup must not flag a derived box key")
	}
	if restored.Identity.Address != original.Address {
		t.Errorf("address = %s, want %s", restored.Identity.Address, original.Address)
	}
	if !bytes.Equal(restored.Identity.PrivateKey, original.PrivateKey) {
		t.Error("ed25519 private key lost in roundtrip")
	}
	if !bytes.Equal(restored.Identity.BoxPrivateKey.Bytes(), original.BoxPrivateKey.Bytes()) {
		t.Error("box private key lost in roundtrip — the whole point of the FULL backup")
	}
}

// TestBackupUnsupportedVersionRejected: a future format is a typed reject,
// not a best-effort parse.
func TestBackupUnsupportedVersionRejected(t *testing.T) {
	original, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	payload, err := ExportBackup(original, 1)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	bumped := strings.Replace(string(payload), `"version": 1`, `"version": 2`, 1)
	if _, err := ImportBackup([]byte(bumped)); !errors.Is(err, ErrBackupVersionUnsupported) {
		t.Errorf("err = %v, want ErrBackupVersionUnsupported", err)
	}
}

// TestBackupAddressMismatchRejected: a backup whose stored address does not
// match the key fingerprint is corrupted and must not restore silently.
func TestBackupAddressMismatchRejected(t *testing.T) {
	original, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	other, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	payload, err := ExportBackup(original, 1)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	swapped := strings.Replace(string(payload), original.Address, other.Address, 1)
	if _, err := ImportBackup([]byte(swapped)); !errors.Is(err, ErrBackupMalformed) {
		t.Errorf("err = %v, want ErrBackupMalformed", err)
	}
}

// TestLegacyImportFlagsDerivedBoxKey: the legacy single-key branch keeps
// the address but must announce that the box key was derived, because a
// randomly generated original box key cannot come back from an Ed25519 seed.
func TestLegacyImportFlagsDerivedBoxKey(t *testing.T) {
	original, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	restored, err := ImportLegacyEd25519(base64.StdEncoding.EncodeToString(original.PrivateKey))
	if err != nil {
		t.Fatalf("legacy import: %v", err)
	}

	if !restored.BoxKeyDerived {
		t.Error("legacy import must flag the derived box key")
	}
	if restored.RecordSeq != 0 {
		t.Errorf("legacy record seq = %d, want 0", restored.RecordSeq)
	}
	if restored.Identity.Address != original.Address {
		t.Errorf("address = %s, want %s — legacy import must preserve the address", restored.Identity.Address, original.Address)
	}
	if bytes.Equal(restored.Identity.BoxPrivateKey.Bytes(), original.BoxPrivateKey.Bytes()) {
		t.Error("test setup: Generate produced the derived box key; the flag scenario is vacuous")
	}
}
