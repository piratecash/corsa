package node

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// identity_backup_store.go owns the one directory identity backups may live
// in, and the translation from the NAME an RPC caller supplies to a file
// inside it.
//
// Three rules, and each closes something the others do not:
//
//   - The RPC surface never sees a path. It names a backup; the node decides
//     where that lives. A caller-supplied path is a write-anywhere primitive
//     on backup and a read-anywhere primitive on restore, and no amount of
//     authentication makes that safe to expose.
//   - Nothing is addressed by path AFTER the directory is chosen. The
//     directory is pinned once as an os.Root — a directory handle — and every
//     later operation resolves through that handle. Checking a path and then
//     using the path is not a check: between the two, another process with
//     write access to the data dir can replace a component with a symlink.
//   - Paths do not travel back out. Every error names the BACKUP, never the
//     file: an error string is a reply, and a reply that spells out the data
//     directory has published the node's filesystem layout to whoever asked
//     about a backup that does not exist.

// Backup failures are distinguishable sentinels so a caller can tell "you
// named it wrong" from "the sandbox is not where it should be" from "the disk
// failed", without reading text.
var (
	// ErrBackupName marks a rejected backup name.
	ErrBackupName = errors.New("identity backup name")

	// ErrBackupSandbox marks a backup directory that could not be pinned as a
	// directory inside the node's own data dir — today, one replaced by a
	// symlink pointing out of it.
	ErrBackupSandbox = errors.New("identity backup directory")
)

// identityBackupNameRe is the whitelist: an ASCII letter or digit, then up to
// 63 more of those plus dot, dash and underscore.
//
// A whitelist rather than a blacklist of dangerous sequences, because the
// dangerous set is open-ended and platform-specific: "/" and "\" escape the
// directory, ".." climbs out of it, a leading "-" is read as a flag by
// whatever tool the user pipes the name into, a leading dot hides the file,
// and Windows adds reserved device names and alternate data streams. Nothing
// outside this pattern is worth the argument.
//
// The leading-dot rejection is also load-bearing elsewhere: it is what keeps
// backup names disjoint from secretfile.TempPrefix, so no cleanup of temp
// files can ever delete a backup.
var identityBackupNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// staleSecretTempAge is how long a temp file must have gone untouched before
// the sweep treats it as debris from a crash.
//
// A threshold rather than "delete every temp", because the data directory is
// SHARED: nodes on different ports keep their state side by side in one
// directory by default, and each has its own in-process mutex and no
// knowledge of the others. An unconditional sweep deletes a temp file another
// node is writing right now, and that node's rename then fails.
//
// An hour is not a proof of exclusivity — nothing short of cross-process
// locking would be, and a lock file brings its own stale-lock problem — but a
// secret write is milliseconds of work, so an hour separates "in flight" from
// "orphaned" with several orders of magnitude to spare.
const staleSecretTempAge = time.Hour

// windowsReservedNames are DOS device names. On Windows they resolve to the
// device no matter which directory the path names and no matter what
// extension is appended — so "con" would send an identity backup, both
// private keys included, to the console instead of to a file. They pass the
// character whitelist, so they are rejected by name.
//
// Checked on every platform, not just Windows: a backup directory synced
// between machines must not contain a name that becomes a device on the other
// one, and a rule that only applies on some builds is a rule nobody tests.
var windowsReservedNames = map[string]bool{
	"con": true, "prn": true, "aux": true, "nul": true,
	"com1": true, "com2": true, "com3": true, "com4": true, "com5": true,
	"com6": true, "com7": true, "com8": true, "com9": true,
	"lpt1": true, "lpt2": true, "lpt3": true, "lpt4": true, "lpt5": true,
	"lpt6": true, "lpt7": true, "lpt8": true, "lpt9": true,
}

// validateIdentityBackupName rejects everything the pattern does not accept,
// and says why in terms of what the caller typed.
func validateIdentityBackupName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: a name is required", ErrBackupName)
	}
	if filepath.IsAbs(name) {
		return fmt.Errorf("%w: %q is a path, not a name — backups live in the node's own backup directory", ErrBackupName, name)
	}
	if !identityBackupNameRe.MatchString(name) {
		return fmt.Errorf("%w: %q is not a valid name — use up to 64 characters of letters, digits, dot, dash or underscore, starting with a letter or digit", ErrBackupName, name)
	}
	stem := strings.ToLower(name)
	if dot := strings.IndexByte(stem, '.'); dot >= 0 {
		stem = stem[:dot]
	}
	if windowsReservedNames[stem] {
		return fmt.Errorf("%w: %q is a reserved device name", ErrBackupName, name)
	}
	return nil
}

// openIdentityBackupDir pins the backup directory and returns a handle every
// later operation goes through. The caller closes it.
//
// The containment check is the open itself, not a string comparison. Two
// separate things have to be true, and neither implies the other:
//
//   - The entry must not lead OUT of the data dir. os.Root refuses that.
//   - The entry must not lead somewhere ELSE INSIDE it. A symlink
//     "identity-backups" → "." stays comfortably within the root and
//     redirects every write into the data directory itself, where a backup
//     named "trust-64646.json" lands on the node's trust store. secretfile.Sub
//     rejects that by proving the directory it opened IS the entry it named.
//
// Once open, the handle refers to that inode for its whole life: a directory
// swapped afterwards does not affect operations already addressed through it,
// which is the difference between this and comparing resolved paths, where
// every use after the comparison starts from the string again.
func (s *Service) openIdentityBackupDir() (*secretfile.Dir, error) {
	dataDir := s.cfg.EffectiveDataDir()
	// Creating the directory is unavoidably path-based; whether the result is
	// really ours is decided below, before anything is written or modified.
	if err := os.MkdirAll(filepath.Join(dataDir, config.IdentityBackupDirName), 0o700); err != nil {
		return nil, fmt.Errorf("create the backup directory: %w", secretfile.StripPath(err))
	}
	dataRoot, err := secretfile.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open the data directory: %w", err)
	}
	defer func() { _ = dataRoot.Close() }()

	backupDir, err := dataRoot.Sub(config.IdentityBackupDirName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not a directory of its own inside the node's data directory — refusing to write key material through it",
			ErrBackupSandbox, config.IdentityBackupDirName)
	}
	if err := backupDir.Restrict(); err != nil {
		_ = backupDir.Close()
		return nil, err
	}
	sweepStaleSecretTemps(backupDir)
	return backupDir, nil
}

// sweepStaleSecretTemps removes temp files left behind by a crash between
// "write the secret" and "rename it into place". They are 0600, but they are
// two private keys under a name nobody recognises, and they would otherwise
// accumulate forever.
//
// Two things make deleting safe here. The prefix: a valid backup name can
// never begin with a dot, so nothing removed can be a backup. And the age:
// see staleSecretTempAge — the directory is shared between nodes, and a
// young temp may belong to one of them right now.
//
// Best effort throughout — a sweep that fails must not fail the backup the
// caller actually asked for.
func sweepStaleSecretTemps(dir *secretfile.Dir) {
	entries, err := dir.Entries()
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-staleSecretTempAge)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), secretfile.TempPrefix) {
			continue
		}
		// A directory or a symlink under that name is not ours; leave it
		// rather than follow or recurse into it.
		if !entry.Type().IsRegular() {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		_ = dir.Remove(entry.Name())
	}
}

// checkIdentityBackupEntry validates the name and, if something already
// exists under it, that the existing entry is an ordinary file.
//
// os.Root already refuses a symlink that leaves the directory, so this is
// about the ones that stay inside: a link from "recovery" to "old" would make
// two names silently the same backup, and a restore would report a file the
// user did not choose. Lstat, not Stat, because Stat reports on the target.
func checkIdentityBackupEntry(dir *secretfile.Dir, name string) error {
	if err := validateIdentityBackupName(name); err != nil {
		return err
	}
	info, err := dir.Lstat(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Nothing there yet — a backup being created for the first time.
		return nil
	case err != nil:
		return fmt.Errorf("inspect backup %q: %w", name, err)
	case info.Mode()&fs.ModeSymlink != 0:
		return fmt.Errorf("%w: %q is a symlink — refusing to follow it", ErrBackupName, name)
	case !info.Mode().IsRegular():
		return fmt.Errorf("%w: %q is not a regular file", ErrBackupName, name)
	}
	return nil
}
