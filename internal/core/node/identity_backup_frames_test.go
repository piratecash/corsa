package node

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func newBackupTestService(t *testing.T) (*Service, *identity.Identity, string) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	dir := t.TempDir()
	svc := NewService(config.Node{
		ListenAddress:     "127.0.0.1:64646",
		TrustStorePath:    filepath.Join(dir, "trust.json"),
		IdentityPath:      filepath.Join(dir, "identity.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)
	return svc, id, dir
}

// TestIdentityBackupRestoreRoundtrip: the local RPC pair exports both keys
// with the record seq into a file and restores them into the identity
// file exactly — no key material in either frame, restart flagged.
func TestIdentityBackupRestoreRoundtrip(t *testing.T) {
	t.Parallel()
	svc, id, dir := newBackupTestService(t)
	backupPath := filepath.Join(dir, "backup.json")

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupPath: backupPath})
	if reply.Type != "identity_backup" || reply.IdentityBackup == nil {
		t.Fatalf("backup reply: %+v", reply)
	}
	if reply.IdentityBackup.Address != id.Address || reply.IdentityBackup.Path != backupPath {
		t.Fatalf("backup frame = %+v", reply.IdentityBackup)
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file missing: %v", err)
	}

	restore := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupPath: backupPath})
	if restore.Type != "identity_restore" || restore.IdentityBackup == nil {
		t.Fatalf("restore reply: %+v", restore)
	}
	frame := restore.IdentityBackup
	if frame.Address != id.Address || !frame.RestartRequired || frame.BoxKeyDerived || frame.Warning != "" {
		t.Fatalf("restore frame = %+v, want same address, restart, no derived-key warning", frame)
	}

	restored, err := identity.Load(svc.cfg.IdentityPath)
	if err != nil {
		t.Fatalf("load restored identity: %v", err)
	}
	if restored.Address != id.Address {
		t.Fatalf("restored address = %s, want %s", restored.Address, id.Address)
	}
	if identity.BoxPublicKeyBase64(restored.BoxPublicKey) != identity.BoxPublicKeyBase64(id.BoxPublicKey) {
		t.Fatal("the box key did not survive the versioned backup roundtrip")
	}
}

// TestIdentityRestoreLegacyKeyWarns: the legacy bare-Ed25519 branch keeps
// the address, derives the box key and OBLIGES the caller to warn — the
// frame must carry both the flag and the human warning text.
func TestIdentityRestoreLegacyKeyWarns(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)
	legacy, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	keyPath := filepath.Join(dir, "legacy.key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(legacy.PrivateKey)), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	restore := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupPath: keyPath})
	if restore.Type != "identity_restore" || restore.IdentityBackup == nil {
		t.Fatalf("restore reply: %+v", restore)
	}
	frame := restore.IdentityBackup
	if frame.Address != legacy.Address {
		t.Fatalf("address = %s, want %s preserved", frame.Address, legacy.Address)
	}
	if !frame.BoxKeyDerived || frame.Warning == "" || !frame.RestartRequired {
		t.Fatalf("legacy caveats missing from the frame: %+v", frame)
	}
}

// TestIdentityRestoreRejectsGarbage: a malformed JSON backup is a typed
// reject, never silently retried as a legacy key.
func TestIdentityRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)
	badPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badPath, []byte(`{"version": 99}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_restore", BackupPath: badPath})
	if reply.Type != "error" {
		t.Fatalf("a future-version backup was accepted: %+v", reply)
	}
	if _, err := os.Stat(svc.cfg.IdentityPath); !os.IsNotExist(err) {
		t.Fatal("a rejected restore touched the identity file")
	}
}

// TestIdentityBackupTightensExistingFilePermissions: WriteFile's mode
// applies only on creation — overwriting a pre-existing 0644 target must
// still end 0600, or both private keys stay world-readable.
func TestIdentityBackupTightensExistingFilePermissions(t *testing.T) {
	t.Parallel()
	svc, _, dir := newBackupTestService(t)
	backupPath := filepath.Join(dir, "existing.json")
	if err := os.WriteFile(backupPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("pre-create: %v", err)
	}

	reply := svc.HandleLocalFrame(protocol.Frame{Type: "identity_backup", BackupPath: backupPath})
	if reply.Type != "identity_backup" {
		t.Fatalf("backup reply: %+v", reply)
	}
	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("backup permissions = %o, want 0600", perm)
	}
}
