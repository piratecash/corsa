package runjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// keptSourcesDir is the journal subdirectory that holds copies of the sources.
const keptSourcesDir = "sources"

// manifestName is the listing inside one kept snapshot.
const manifestName = "MANIFEST.sha256"

// pendingPrefix starts every temporary name the journal creates. It begins
// with a dot, which neither a record name nor a kept snapshot name can, so a
// half-written entry is never mistaken for a finished one or for a stranger.
const pendingPrefix = ".pending-"

// KeptDir is where the copy of one snapshot lives beside a journal. The name
// carries the stamp, and the stamp is the content, so two batches from the
// same sources share one copy. It builds the path only; every reader in this
// package validates the stamp before using it.
func KeptDir(journalDir string, stamp SourceStamp) string {
	return filepath.Join(journalDir, keptSourcesDir, fmt.Sprintf("%s-%s", stamp.Name, stamp.Stamp))
}

// KeepSources copies every snapshot of the journal beside it, or verifies the
// copy already there. A copy is what makes the binding of a number to its code
// checkable after the fact: the stamp alone says only that the code differed.
//
// It first proves the filesystem keeps the no-overwrite promise (see
// probeHardLinks); Sweep calls it before the first Step for that reason.
func (j *Journal) KeepSources(ctx context.Context) error {
	if err := probeHardLinks(ctx, j.dir, j.link); err != nil {
		return err
	}
	for _, snapshot := range j.sources {
		if err := keepSnapshot(ctx, j.dir, snapshot); err != nil {
			return err
		}
	}
	return nil
}

// keepSnapshot publishes a snapshot directory atomically: it is written and
// verified under a pending name, then renamed into place. While it is pending
// nobody can take it for a snapshot, so a crash mid-copy leaves nothing a
// later batch would accept. The copied files are not fsynced: a copy damaged
// by a power loss fails verification and is refused, never accepted.
func keepSnapshot(ctx context.Context, journalDir string, snapshot Snapshot) error {
	target := KeptDir(journalDir, snapshot.Stamp())
	if _, err := os.Lstat(target); err == nil {
		return verifyKeptSnapshot(ctx, target, snapshot)
	}

	parent := filepath.Join(journalDir, keptSourcesDir)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return fmt.Errorf("creating the kept sources directory: %w", err)
	}
	pending, err := os.MkdirTemp(parent, pendingPrefix+"*")
	if err != nil {
		return fmt.Errorf("creating a pending sources directory: %w", err)
	}
	// After a successful rename the pending name no longer exists and this is a
	// no-op; after a failure it removes our own half-written copy. Its error is
	// dropped because the outcome that matters is already being returned.
	defer func() { _ = os.RemoveAll(pending) }()

	if err := writeSnapshotFiles(ctx, pending, snapshot); err != nil {
		return err
	}
	if err := verifyKeptSnapshot(ctx, pending, snapshot); err != nil {
		return fmt.Errorf("the copy did not land as it was read: %w", err)
	}
	if err := os.Rename(pending, target); err != nil {
		// Renaming onto a non-empty directory fails: a concurrent batch of the
		// same sources published first. Its copy is accepted only if it verifies.
		if _, statErr := os.Lstat(target); statErr == nil {
			return verifyKeptSnapshot(ctx, target, snapshot)
		}
		return fmt.Errorf("publishing kept sources %s: %w", snapshot.Stamp().Stamp, err)
	}
	return nil
}

func writeSnapshotFiles(ctx context.Context, dir string, snapshot Snapshot) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("opening the pending sources directory: %w", err)
	}
	// Every file written through the root is closed by WriteFile itself, so
	// closing the directory handle cannot lose data and has nothing to report.
	defer func() { _ = root.Close() }()

	for _, file := range snapshot.files {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("copying sources: %w", err)
		}
		local := filepath.FromSlash(file.Path)
		if err := root.MkdirAll(filepath.Dir(local), 0o750); err != nil {
			return fmt.Errorf("creating the directory of %s: %w", file.Path, err)
		}
		if err := root.WriteFile(local, file.Content, 0o600); err != nil {
			return fmt.Errorf("copying %s: %w", file.Path, err)
		}
	}
	if err := root.WriteFile(manifestName, snapshot.Manifest(), 0o600); err != nil {
		return fmt.Errorf("writing the manifest: %w", err)
	}
	return nil
}

// verifyKeptSnapshot checks a kept copy against the snapshot in memory: the
// manifest must be the same bytes, and the files must match the manifest.
func verifyKeptSnapshot(ctx context.Context, dir string, snapshot Snapshot) error {
	manifest, err := verifyKeptFiles(ctx, dir, snapshot.Stamp())
	if err != nil {
		return err
	}
	if !bytes.Equal(manifest, snapshot.Manifest()) {
		return fmt.Errorf("%w: %s holds another listing under stamp %s", ErrSnapshotMismatch, filepath.Base(dir), snapshot.stamp)
	}
	return nil
}

// VerifyKept checks a kept snapshot from disk alone: the manifest must hash to
// the stamp it is named for, every file it lists must hash to its digest, and
// nothing unlisted may be present. A reader of the journal uses it to confirm
// that the code beside a number is the code that produced it.
func VerifyKept(ctx context.Context, journalDir string, stamp SourceStamp) error {
	if err := stamp.validate(); err != nil {
		return err
	}
	_, err := verifyKeptFiles(ctx, KeptDir(journalDir, stamp), stamp)
	return err
}

// verifyKeptFiles verifies a kept directory and returns its manifest.
//
// Both halves of the check matter: a missing or edited file breaks the digest
// comparison, and a file nobody listed breaks the second — a copy that holds
// more than its listing is not the code that was measured either.
func verifyKeptFiles(ctx context.Context, dir string, stamp SourceStamp) ([]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: opening %s: %w", ErrSnapshotMismatch, filepath.Base(dir), err)
	}
	// Only read through, so closing it cannot lose anything worth reporting.
	defer func() { _ = root.Close() }()

	manifest, err := root.ReadFile(manifestName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s has no readable manifest: %w", ErrSnapshotMismatch, filepath.Base(dir), err)
	}
	if got := stampOf(manifest); got != stamp.Stamp {
		return nil, fmt.Errorf("%w: the manifest hashes to %s, the copy is named %s", ErrSnapshotMismatch, got, stamp.Stamp)
	}
	listed, err := parseManifest(manifest, stamp.Name)
	if err != nil {
		return nil, err
	}
	for _, file := range listed {
		if err := verifyKeptFile(ctx, root, file); err != nil {
			return nil, err
		}
	}
	if err := refuseUnlisted(root, listed); err != nil {
		return nil, err
	}
	return manifest, nil
}

func verifyKeptFile(ctx context.Context, root *os.Root, file SourceFile) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("verifying kept sources: %w", err)
	}
	content, err := root.ReadFile(filepath.FromSlash(file.Path))
	if err != nil {
		return fmt.Errorf("%w: %s is listed and unreadable: %w", ErrSnapshotMismatch, file.Path, err)
	}
	sum := sha256.Sum256(content)
	if got := hex.EncodeToString(sum[:]); got != file.Digest {
		return fmt.Errorf("%w: %s hashes to %s and is listed as %s", ErrSnapshotMismatch, file.Path, got, file.Digest)
	}
	return nil
}

func refuseUnlisted(root *os.Root, listed []SourceFile) error {
	known := make(map[string]struct{}, len(listed)+1)
	known[manifestName] = struct{}{}
	for _, file := range listed {
		known[file.Path] = struct{}{}
	}
	return fs.WalkDir(root.FS(), ".", func(entryPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("%w: walking the copy: %w", ErrSnapshotMismatch, walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		if _, ok := known[entryPath]; !ok || !entry.Type().IsRegular() {
			return fmt.Errorf("%w: %s is in the copy and not a listed file", ErrSnapshotMismatch, entryPath)
		}
		return nil
	})
}

// parseManifest reads a manifest back, refusing anything the writer would not
// have produced: an unknown format, another name, a path outside the copy,
// paths out of byte order or repeated.
func parseManifest(manifest []byte, name FingerprintName) ([]SourceFile, error) {
	lines := strings.Split(strings.TrimSuffix(string(manifest), "\n"), "\n")
	if len(lines) < 2 || lines[0] != sourcesFormat || lines[1] != "name "+string(name) {
		return nil, fmt.Errorf("%w: the manifest is not a %s listing of %s", ErrSnapshotMismatch, sourcesFormat, name)
	}
	files := make([]SourceFile, 0, len(lines)-2)
	previous := ""
	for _, line := range lines[2:] {
		file, err := parseManifestLine(line)
		if err != nil {
			return nil, err
		}
		if file.Path <= previous {
			return nil, fmt.Errorf("%w: %s is out of byte order or repeated", ErrSnapshotMismatch, file.Path)
		}
		previous = file.Path
		files = append(files, file)
	}
	return files, nil
}

func parseManifestLine(line string) (SourceFile, error) {
	digest, filePath, ok := strings.Cut(line, manifestSeparator)
	decoded, err := hex.DecodeString(digest)
	switch {
	case !ok:
		return SourceFile{}, fmt.Errorf("%w: manifest line %q has no separator", ErrSnapshotMismatch, line)
	case err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest):
		return SourceFile{}, fmt.Errorf("%w: manifest line %q has no sha256 digest", ErrSnapshotMismatch, line)
	case !filepath.IsLocal(filepath.FromSlash(filePath)) || path.Clean(filePath) != filePath:
		return SourceFile{}, fmt.Errorf("%w: manifest path %q leaves the copy", ErrSnapshotMismatch, filePath)
	}
	return SourceFile{Path: filePath, Digest: digest}, nil
}
