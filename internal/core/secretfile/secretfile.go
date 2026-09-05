// Package secretfile lands secret bytes on disk without ever exposing them,
// and is the ONE place in the tree that knows how.
//
// It exists because the same four mistakes are easy to make independently in
// every caller that writes a key:
//
//   - Writing through a PREDICTABLE temp name. "<target>.tmp" puts a service
//     name in the same namespace as the caller's own names, so writing one
//     artifact can delete another, and a temp planted in advance decides where
//     the secret lands.
//   - Trusting a path. Between "check the directory" and "write the file",
//     another process can replace a component with a symlink; every operation
//     here is therefore relative to a PINNED directory handle (os.Root), which
//     resolves names through that handle and refuses anything that leaves it.
//   - Trusting that "inside the root" means "where I meant". A symlink that
//     points at the root's own parent stays inside it, so Sub additionally
//     proves the directory it opened IS the entry it named.
//   - Assuming the mode argument applies. os.WriteFile passes the mode only
//     when it CREATES the file, so an existing 0644 target — or a symlink —
//     silently keeps its own terms.
//
// The guarantee: the bytes exist only in a fresh, owner-only file that no
// other name refers to, and appear at the target through an atomic rename.
package secretfile

import (
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// TempPrefix is the prefix of every temp file this package creates.
//
// It begins with a dot on purpose: callers whitelist their artifact names, and
// every such whitelist in this tree rejects a leading dot. That is what makes
// the two namespaces disjoint — no artifact can ever be named like a temp, so
// nothing that cleans up temps can delete an artifact.
const TempPrefix = ".corsa-secret-"

// tempNameAttempts bounds the retry when a random temp name already exists.
// Collisions are astronomically unlikely; a bound is here so a broken
// filesystem produces an error rather than a spin.
const tempNameAttempts = 8

// ErrNotADirectory marks a directory entry that is not the directory it
// claims to be — today, one replaced by a symlink.
var ErrNotADirectory = errors.New("secretfile: not a directory")

// Dir is a directory a caller may write secrets into: nothing but a pinned
// handle.
//
// It deliberately does NOT keep the path it was opened from. Every operation
// resolves through the handle, on both platforms, so a stored path could only
// be used for something that ought not to use one — and an earlier version of
// this package proved that by using it for the Windows ACL call, which split
// the protection from the write.
type Dir struct {
	root *os.Root
}

// Open pins an existing directory. The caller closes the result.
//
// This is the one and only place a path is resolved. After it returns, the
// handle refers to that directory for its whole life, whatever happens to the
// name afterwards.
func Open(path string) (*Dir, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("secretfile: open directory: %w", stripPath(err))
	}
	return &Dir{root: root}, nil
}

// Sub pins a subdirectory by a single-element name, and proves it is really
// that entry rather than a link wearing its name.
//
// os.Root already refuses a name resolving OUTSIDE the root — but "outside" is
// not the whole question. A symlink named "identity-backups" pointing at "."
// stays comfortably inside and silently redirects every write into the parent,
// where a backup called "trust-64646.json" lands on the node's trust store.
//
// The proof is handle-based rather than a pre-flight Lstat, because a
// pre-flight check answers about the name and the write happens through the
// handle: Lstat the NAME without following it, Stat the DIRECTORY THAT WAS
// OPENED, and require them to be the same file. A symlink's own inode is never
// its target's, so the link case fails; a swap racing between the two calls
// also fails, which is the right direction to fail in.
func (d *Dir) Sub(name string) (*Dir, error) {
	if err := checkSingleName(name); err != nil {
		return nil, err
	}
	root, err := d.root.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %q is not a directory inside this one", ErrNotADirectory, name)
	}
	linkInfo, err := d.root.Lstat(name)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("secretfile: inspect %q: %w", name, stripPath(err))
	}
	openedInfo, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("secretfile: inspect opened %q: %w", name, stripPath(err))
	}
	if !os.SameFile(linkInfo, openedInfo) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: %q is a link to another directory, not a directory of its own", ErrNotADirectory, name)
	}
	return &Dir{root: root}, nil
}

// Close releases the pinned handle.
func (d *Dir) Close() error { return d.root.Close() }

// Write lands payload at name: a fresh owner-only temp file, then an atomic
// rename onto the target.
//
// name must be a single path element. The rename is what makes the write
// atomic for readers, and it also means the target inherits the temp's
// restricted inode whatever the old file's mode or type was — including when
// the target is a symlink, which is replaced rather than written through.
func (d *Dir) Write(name string, payload []byte) error {
	if err := checkSingleName(name); err != nil {
		return err
	}

	file, tmp, err := d.createTemp()
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = file.Close()
		_ = d.root.Remove(tmp)
	}
	// No permission step here, and that absence is the design: createExclusive
	// returns a file that is ALREADY owner-only. Tightening afterwards would
	// leave a window in which the file exists under the directory's terms, and
	// a handle another process obtained in that window keeps its access.
	if _, err := file.Write(payload); err != nil {
		cleanup()
		return fmt.Errorf("secretfile: write: %w", stripPath(err))
	}
	// Durability before visibility: a crash after the rename must not leave a
	// present-but-empty key file, which reads as "identity lost" rather than
	// as "retry me".
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("secretfile: sync: %w", stripPath(err))
	}
	if err := file.Close(); err != nil {
		_ = d.root.Remove(tmp)
		return fmt.Errorf("secretfile: close: %w", stripPath(err))
	}
	if err := d.root.Rename(tmp, name); err != nil {
		_ = d.root.Remove(tmp)
		return fmt.Errorf("secretfile: replace %q: %w", name, stripPath(err))
	}
	return nil
}

// ReadFile reads one entry through the pinned handle.
func (d *Dir) ReadFile(name string) ([]byte, error) {
	if err := checkSingleName(name); err != nil {
		return nil, err
	}
	data, err := d.root.ReadFile(name)
	if err != nil {
		return nil, stripPath(err)
	}
	return data, nil
}

// Lstat reports on one entry WITHOUT following it, through the pinned handle.
func (d *Dir) Lstat(name string) (fs.FileInfo, error) {
	if err := checkSingleName(name); err != nil {
		return nil, err
	}
	info, err := d.root.Lstat(name)
	if err != nil {
		return nil, stripPath(err)
	}
	return info, nil
}

// Remove deletes one entry through the pinned handle.
func (d *Dir) Remove(name string) error {
	if err := checkSingleName(name); err != nil {
		return err
	}
	if err := d.root.Remove(name); err != nil {
		return stripPath(err)
	}
	return nil
}

// Entries lists the directory through the pinned handle.
func (d *Dir) Entries() ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(d.root.FS(), ".")
	if err != nil {
		return nil, stripPath(err)
	}
	return entries, nil
}

// Restrict makes the directory owner-only.
//
// A directory matters as much as the files in it: a readable directory hands
// out the names, and the names are the only thing standing between a reader
// and a key file whose own mode they still cannot change.
//
// MkdirAll leaves an EXISTING directory's mode untouched, so a directory
// created by an earlier build, restored from an archive or unpacked by a sync
// tool can still be group- or world-readable. Tighten every time.
//
// It acts through the pinned handle on both platforms — see restrictDirectory
// in create_posix.go / create_windows.go. This is for a directory the caller
// OWNS, such as the node's identity-backup sandbox; it is deliberately not
// called by WriteFile, which is handed an arbitrary parent whose permissions
// are not this package's to decide.
func (d *Dir) Restrict() error {
	if err := restrictDirectory(d); err != nil {
		return fmt.Errorf("secretfile: restrict directory: %w", stripPath(err))
	}
	return nil
}

// WriteFile is the convenience form for callers holding a path: it pins the
// parent directory and delegates to Dir.Write. The parent must already exist —
// creating it is the caller's decision, because the mode it should have is.
func WriteFile(path string, payload []byte) error {
	dir, err := Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Write(filepath.Base(path), payload)
}

// checkSingleName rejects anything that is not one path element. These
// operations are the primitive callers reach for once they have decided WHERE;
// letting a separator through would quietly undo that decision.
func checkSingleName(name string) error {
	if name == "" || name != filepath.Base(name) || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("secretfile: %q is not a single file name", name)
	}
	return nil
}

// createTemp opens a fresh file under a random name inside the directory. It
// is os.CreateTemp's contract, expressed against a pinned handle, because
// os.CreateTemp only takes a path.
func (d *Dir) createTemp() (*os.File, string, error) {
	for range tempNameAttempts {
		suffix, err := randomSuffix()
		if err != nil {
			return nil, "", fmt.Errorf("secretfile: random temp name: %w", err)
		}
		name := TempPrefix + suffix
		file, err := createExclusive(d, name)
		if err == nil {
			return file, name, nil
		}
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return nil, "", fmt.Errorf("secretfile: create temp: %w", stripPath(err))
	}
	return nil, "", errors.New("secretfile: could not create a unique temp file")
}

// randomSuffix returns 16 lowercase base32 characters of entropy. Base32
// rather than hex so the name stays short, lowercase so it is identical on
// case-insensitive filesystems.
//
// Unpredictability is load-bearing beyond collision avoidance: it is what
// keeps the Windows ACL step safe despite naming the file by path — nothing
// can be planted at a name nobody can guess.
func randomSuffix() (string, error) {
	raw := make([]byte, 10)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)), nil
}

// stripPath removes the file name from an os error.
//
// os reports *fs.PathError and *os.LinkError, whose Error() spells out the
// absolute path. These errors travel back to RPC callers, and the node's
// filesystem layout — on a desktop build, the operator's home directory name —
// is not something a caller learns by asking about an artifact.
func stripPath(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", pathErr.Op, pathErr.Err)
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return fmt.Errorf("%s: %w", linkErr.Op, linkErr.Err)
	}
	return err
}

// StripPath exposes stripPath to callers that produce their own filesystem
// errors around this package — the rule "an error names the artifact, not the
// path" has to hold for the whole subsystem or it holds for none of it.
func StripPath(err error) error { return stripPath(err) }
