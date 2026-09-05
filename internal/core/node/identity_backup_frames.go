package node

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// identity_backup_frames.go serves the LOCAL-ONLY identity backup RPC
// (docs/protocol/identity-lookup.md §5): identity_backup writes the
// versioned full backup — both private keys and the current record seq —
// into the node's own backup directory, identity_restore replaces the
// identity FILE from a backup there and reports that a restart is required.
//
// Two separate promises hold here, and each is enforced by code rather than
// by this comment:
//
//   - Key material never crosses the RPC boundary. Both frames carry a
//     backup NAME; the reply carries the name, the address, the seq and the
//     warnings.
//   - The caller never picks a path. identity_backup_store.go maps the name
//     into config.Node.EffectiveIdentityBackupDir and refuses anything that
//     would leave it, symlinks included. A path in the request would be a
//     write-anywhere (and, on restore, read-anywhere) primitive.
//
// "Local-only" is likewise enforced rather than asserted: both commands are
// registered with rpc.TransportLoopbackOnly, so an HTTP caller must be on a
// loopback socket AND the listener must require credentials.

// backupWarningBoxKeyDerived is the §5 legacy-branch warning the UI is
// obliged to surface: the address survives, the encryption key does not.
const backupWarningBoxKeyDerived = "box key was DERIVED from the Ed25519 seed: the address is preserved, but the encryption key differs from the original — peers must obtain the new key before direct messages recover"

// identityBackupFrame serves identity_backup: export the running identity
// with its current record seq into a 0600 file named frame.BackupName inside
// the node's backup directory.
func (s *Service) identityBackupFrame(frame protocol.Frame) protocol.Frame {
	name := strings.TrimSpace(frame.BackupName)
	if name == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: backup_name is required"}
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
	// Serialised with restore: both end in a rename onto the same name in the
	// same directory, so two calls could otherwise acknowledge content the
	// other wrote. The mutex serialises this PROCESS; it says nothing about
	// another node sharing the data directory, which is why the directory is
	// pinned as a handle and the temp files are unique rather than derived.
	s.identityFileMu.Lock()
	defer s.identityFileMu.Unlock()
	backupDir, err := s.openIdentityBackupDir()
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	defer func() { _ = backupDir.Close() }()
	if err := checkIdentityBackupEntry(backupDir, name); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	if err := backupDir.Write(name, payload); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_backup: " + err.Error()}
	}
	log.Info().Str("backup", name).Str("address", s.identity.Address).Msg("identity_backup_written")
	return protocol.Frame{Type: "identity_backup", IdentityBackup: &protocol.IdentityBackupFrame{
		Name:      name,
		Address:   s.identity.Address,
		RecordSeq: seq,
	}}
}

// identityRestoreFrame serves identity_restore: replace the identity FILE
// from the backup named frame.BackupName in the node's backup directory. The
// running node keeps its current in-memory identity — swapping keys under
// live sessions is not a thing — so the response says restart_required and
// the caller relays that to the user.
//
// Restore reads from the SAME sandbox backup writes to. Accepting a path here
// would be the mirror image of the write primitive: an RPC client could point
// the node at any file it can read and learn, from the reply's address field
// and the error text, something about a file it was never shown.
func (s *Service) identityRestoreFrame(frame protocol.Frame) protocol.Frame {
	name := strings.TrimSpace(frame.BackupName)
	if name == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: backup_name is required"}
	}
	if s.cfg.IdentityPath == "" {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: node has no identity file (in-memory identity)"}
	}
	// Two concurrent restores could interleave the write-then-rename steps in
	// identity.Save and acknowledge an identity other than the one that
	// actually survived on disk. Serialized (together with identity_backup —
	// same mutex, same reason), each reply describes a completed save.
	s.identityFileMu.Lock()
	defer s.identityFileMu.Unlock()
	backupDir, err := s.openIdentityBackupDir()
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: " + err.Error()}
	}
	defer func() { _ = backupDir.Close() }()
	if err := checkIdentityBackupEntry(backupDir, name); err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: " + err.Error()}
	}
	// The read goes through the pinned directory handle, not through a path
	// rebuilt from a string — so the entry checked above is the entry read.
	// The two filesystem errors below are also the ones that used to answer a
	// request for a missing backup with the node's absolute data directory:
	// the reply names the backup and the failure, and where that backup lives
	// is the node's business.
	data, err := backupDir.ReadFile(name)
	if err != nil {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeProtocol,
			Error: fmt.Sprintf("identity_restore: read backup %q: %v", name, err),
		}
	}
	restored, err := restoreIdentityFromBackupBytes(data)
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeProtocol, Error: "identity_restore: " + err.Error()}
	}
	if err := identity.Save(s.cfg.IdentityPath, restored.Identity); err != nil {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeProtocol,
			Error: "identity_restore: persist identity: " + secretfile.StripPath(err).Error(),
		}
	}
	warning := ""
	if restored.BoxKeyDerived {
		warning = backupWarningBoxKeyDerived
	}
	log.Info().
		Str("backup", name).
		Str("address", restored.Identity.Address).
		Bool("box_key_derived", restored.BoxKeyDerived).
		Msg("identity_restore_written")
	return protocol.Frame{Type: "identity_restore", IdentityBackup: &protocol.IdentityBackupFrame{
		Name:            name,
		Address:         restored.Identity.Address,
		RecordSeq:       restored.RecordSeq,
		BoxKeyDerived:   restored.BoxKeyDerived,
		RestartRequired: true,
		Warning:         warning,
	}}
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
