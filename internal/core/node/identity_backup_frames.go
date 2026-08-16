package node

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// identity_backup_frames.go serves the LOCAL-ONLY identity backup RPC
// (docs/protocol/identity-lookup.md §5): identity_backup writes the
// versioned full backup — both private keys and the current record seq —
// to a file on the node's own disk, identity_restore replaces the identity
// FILE from a backup and reports that a restart is required. Key material
// never crosses the RPC boundary in either direction: both frames carry a
// file path, the response carries only the address, the seq and the
// warnings. Neither frame exists in the network dispatch — HandleLocalFrame
// is reachable from the authenticated local RPC and the desktop console
// only.

// backupWarningBoxKeyDerived is the §5 legacy-branch warning the UI is
// obliged to surface: the address survives, the encryption key does not.
const backupWarningBoxKeyDerived = "box key was DERIVED from the Ed25519 seed: the address is preserved, but the encryption key differs from the original — peers must obtain the new key before direct messages recover"

// identityBackupFrame serves identity_backup: export the running identity
// with its current record seq into a 0600 file at frame.BackupPath.
func (s *Service) identityBackupFrame(frame protocol.Frame) protocol.Frame {
	path := strings.TrimSpace(frame.BackupPath)
	if path == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: backup_path is required"}
	}
	// The exported seq is the FLOOR a restore starts above: the issued
	// self-record seq when one exists, the identity's own floor otherwise
	// (records may be disabled while the floor still must survive).
	seq := s.identity.RecordSeqFloor
	_, selfBody := s.SelfIdentityRecord()
	if uint64(selfBody.Seq) > seq {
		seq = uint64(selfBody.Seq)
	}
	payload, err := identity.ExportBackup(s.identity, seq)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	// Same serialisation as restore: writeSecretFile funnels through one
	// predictable <path>.tmp, and two concurrent backups to one path could
	// remove or rename each other's temp — one call would then acknowledge
	// a file another call wrote.
	s.identityFileMu.Lock()
	defer s.identityFileMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	if err := writeSecretFile(path, payload); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	log.Info().Str("path", path).Str("address", s.identity.Address).Msg("identity_backup_written")
	return protocol.Frame{Type: "identity_backup", IdentityBackup: &protocol.IdentityBackupFrame{
		Path:      path,
		Address:   s.identity.Address,
		RecordSeq: seq,
	}}
}

// identityRestoreFrame serves identity_restore: replace the identity FILE
// from the backup at frame.BackupPath. The running node keeps its current
// in-memory identity — swapping keys under live sessions is not a thing —
// so the response says restart_required and the caller relays that to the
// user.
func (s *Service) identityRestoreFrame(frame protocol.Frame) protocol.Frame {
	path := strings.TrimSpace(frame.BackupPath)
	if path == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: backup_path is required"}
	}
	if s.cfg.IdentityPath == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: node has no identity file (in-memory identity)"}
	}
	// identity.Save writes through one predictable <path>.tmp: two
	// concurrent restores could interleave the write-then-rename steps and
	// acknowledge an identity other than the one that actually survived on
	// disk. Serialized (together with identity_backup — same mutex, same
	// reason), each reply describes a fully completed save.
	s.identityFileMu.Lock()
	defer s.identityFileMu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: " + err.Error()}
	}
	restored, err := restoreIdentityFromBackupBytes(data)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: " + err.Error()}
	}
	if err := identity.Save(s.cfg.IdentityPath, restored.Identity); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: persist identity: " + err.Error()}
	}
	warning := ""
	if restored.BoxKeyDerived {
		warning = backupWarningBoxKeyDerived
	}
	log.Info().
		Str("address", restored.Identity.Address).
		Bool("box_key_derived", restored.BoxKeyDerived).
		Msg("identity_restore_written")
	return protocol.Frame{Type: "identity_restore", IdentityBackup: &protocol.IdentityBackupFrame{
		Path:            s.cfg.IdentityPath,
		Address:         restored.Identity.Address,
		RecordSeq:       restored.RecordSeq,
		BoxKeyDerived:   restored.BoxKeyDerived,
		RestartRequired: true,
		Warning:         warning,
	}}
}

// writeSecretFile lands secret material at path without a single moment
// of wider exposure: the bytes go into a FRESH temp file whose owner-only
// access exists BEFORE the first secret byte is written (a pre-existing
// world-readable target must never see the content, and a failed
// permission step must fail the write, not follow it), then an atomic
// rename replaces the target — which inherits the temp file's restricted
// inode, whatever the old file's mode was. On Windows the 0600 mode bits
// protect nothing, so restrictSecretFileAccess additionally applies an
// owner-only DACL to the temp file before the write.
func writeSecretFile(path string, payload []byte) error {
	tmp := path + ".tmp"
	// A leftover temp from a crashed run may carry stale permissions;
	// O_EXCL below then guarantees the file we open is OUR fresh inode.
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clear stale temp file: %w", err)
	}
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create secret temp file: %w", err)
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tmp)
	}
	if err := restrictSecretFileAccess(tmp); err != nil {
		cleanup()
		return fmt.Errorf("restrict secret file access: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		cleanup()
		return fmt.Errorf("write secret file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close secret file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace secret file: %w", err)
	}
	return nil
}

// restoreIdentityFromBackupBytes picks the import branch by shape: a JSON
// object is the versioned full backup (both keys, exact restore), anything
// else is treated as the legacy bare base64 Ed25519 private key with its
// derived-box-key caveat. The branches are explicit — a malformed JSON
// backup is a malformed backup, never silently retried as a legacy key.
func restoreIdentityFromBackupBytes(data []byte) (identity.RestoredIdentity, error) {
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") {
		restored, err := identity.ImportBackup(data)
		if err != nil {
			return identity.RestoredIdentity{}, err
		}
		return restored, nil
	}
	restored, err := identity.ImportLegacyEd25519(trimmed)
	if err != nil {
		return identity.RestoredIdentity{}, err
	}
	return restored, nil
}
