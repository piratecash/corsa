package runjournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// sourcesFormat tags a manifest. It changes when the manifest layout or the
// stamp rule changes, and a reader that does not know the tag refuses it.
const sourcesFormat = "runjournal-sources/v1"

// stampHexLen matches the identifier length: short enough to read in a file
// name, long enough that two versions of one tree do not collide.
const stampHexLen = 16

// manifestSeparator sits between a digest and a path. A digest is fixed-width
// hex, so the first separator is unambiguous even when a path holds spaces.
const manifestSeparator = "  "

var (
	fingerprintNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	stampPattern           = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// validateFingerprintName refuses a name that cannot become the kept copy's
// directory name: outside [a-z0-9-], or too long to fit beside its stamp.
func validateFingerprintName(name FingerprintName) error {
	if !fingerprintNamePattern.MatchString(string(name)) || len(name) > maxFingerprintNameBytes {
		return fmt.Errorf("%w: fingerprint name %q is not 1..%d of [a-z0-9-]",
			ErrInvalidSourceSpec, name, maxFingerprintNameBytes)
	}
	return nil
}

// validate refuses a stamp that could not have come from a snapshot. Both
// halves become a directory name beside the journal, so a stamp read from a
// record or passed by a reader is checked before it is turned into a path.
func (s SourceStamp) validate() error {
	if err := validateFingerprintName(s.Name); err != nil {
		return err
	}
	if !stampPattern.MatchString(string(s.Stamp)) {
		return fmt.Errorf("%w: %q is not a source stamp", ErrInvalidSourceSpec, s.Stamp)
	}
	return nil
}

// NonTestGo selects the Go files a build compiles into a package: *.go but not
// *_test.go. It is the filter for "the code being measured".
func NonTestGo(relPath string) bool {
	return strings.HasSuffix(relPath, ".go") && !strings.HasSuffix(relPath, "_test.go")
}

// TestGo selects only *_test.go files: the code of a stand that lives in test
// files of the package it drives.
func TestGo(relPath string) bool { return strings.HasSuffix(relPath, "_test.go") }

// AnyGo selects every Go file, tests included.
func AnyGo(relPath string) bool { return strings.HasSuffix(relPath, ".go") }

// ExactNames selects files whose path relative to the base is exactly one of
// names. Whole paths rather than base names, so a go.mod of a nested module
// cannot slip into a fingerprint meant for the root one.
func ExactNames(names ...string) FileFilter {
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	return func(relPath string) bool {
		_, ok := wanted[relPath]
		return ok
	}
}

// BaseGlob selects files whose base name matches a path.Match pattern. The
// pattern is checked here, because a malformed one would otherwise match
// nothing and look like an empty directory.
func BaseGlob(pattern string) (FileFilter, error) {
	if _, err := path.Match(pattern, ""); err != nil {
		return nil, fmt.Errorf("%w: pattern %q: %w", ErrInvalidSourceSpec, pattern, err)
	}
	return func(relPath string) bool {
		matched, err := path.Match(pattern, path.Base(relPath))
		// The pattern was validated above, so an error here cannot happen; a
		// non-match is the only safe reading if it ever did.
		return err == nil && matched
	}, nil
}

// TakeSnapshot reads every file the spec selects, once, and derives the stamp
// from their names and digests.
//
// Every selected file is read in full and its bytes are kept: the manifest,
// the stamp and the copy kept beside the journal all come from this one read,
// so they cannot disagree about which code was measured.
func TakeSnapshot(ctx context.Context, spec SourceSpec) (Snapshot, error) {
	if err := validateSpec(spec); err != nil {
		return Snapshot{}, err
	}
	collector := snapshotCollector{base: spec.Base, files: map[string]SourceFile{}}
	for _, root := range spec.Roots {
		if err := collector.collectRoot(ctx, root); err != nil {
			return Snapshot{}, err
		}
	}
	files := make([]SourceFile, 0, len(collector.files))
	for _, file := range collector.files {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })

	snapshot := Snapshot{spec: spec, files: files}
	snapshot.stamp = stampOf(snapshot.Manifest())
	return snapshot, nil
}

func validateSpec(spec SourceSpec) error {
	if err := validateFingerprintName(spec.Name); err != nil {
		return err
	}
	if spec.Base == "" || len(spec.Roots) == 0 {
		return fmt.Errorf("%w: %s needs a base and at least one root", ErrInvalidSourceSpec, spec.Name)
	}
	for _, root := range spec.Roots {
		if root.Filter == nil {
			return fmt.Errorf("%w: %s root %q has no filter", ErrInvalidSourceSpec, spec.Name, root.Dir)
		}
		// IsLocal refuses absolute paths, "..", and the empty path: a root
		// outside the base would put files the base does not describe into a
		// fingerprint named for it.
		if !filepath.IsLocal(filepath.FromSlash(root.Dir)) {
			return fmt.Errorf("%w: %s root %q is not inside the base", ErrInvalidSourceSpec, spec.Name, root.Dir)
		}
	}
	return nil
}

// snapshotCollector accumulates the files of one spec, keyed by path relative
// to the base, so a file reached through two roots is noticed.
type snapshotCollector struct {
	base  string
	files map[string]SourceFile
}

// collectRoot adds one root's files and refuses a root that adds none.
//
// Per root, not per spec: a fingerprint over two directories where one of them
// moved away would otherwise stamp the remaining one and still claim to cover
// both — a stamp that silently stopped describing half of what it names.
func (c *snapshotCollector) collectRoot(ctx context.Context, root SourceRoot) error {
	rootPath, err := c.resolveRoot(root.Dir)
	if err != nil {
		return err
	}
	before := len(c.files)
	collect := c.collectFlat
	if root.Recursive {
		collect = c.collectTree
	}
	if err := collect(ctx, rootPath, root.Filter); err != nil {
		return err
	}
	if len(c.files) == before {
		return fmt.Errorf("%w: root %q matched no file", ErrNoSources, root.Dir)
	}
	return nil
}

// resolveRoot checks the base and every component of a root below it without
// following links. A root reached through a symlink reads files the base does
// not contain, and the manifest would name them as if it did.
//
// The base itself is checked too, not only what lies under it: a flat walk
// reads through a linked directory (ReadDir follows it) while a tree walk
// refuses it (WalkDir does not), so a linked base would make the same spec
// stamp a tree or fail depending on Recursive. The path LEADING to the base is
// the operator's choice and is taken as given — on macOS the temporary
// directory itself lives under a symlinked /var.
func (c *snapshotCollector) resolveRoot(dir string) (string, error) {
	if err := requireRealDirectory(c.base, dir, "the base"); err != nil {
		return "", err
	}
	current := c.base
	for _, component := range strings.Split(path.Clean(dir), "/") {
		if component == "." {
			continue
		}
		current = filepath.Join(current, component)
		if err := requireRealDirectory(current, dir, component); err != nil {
			return "", err
		}
	}
	return current, nil
}

// requireRealDirectory refuses anything at dirPath but a directory, without
// following a link: Lstat reports a symlink as a symlink, never as a directory.
func requireRealDirectory(dirPath, root, component string) error {
	info, err := os.Lstat(dirPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: root %q does not exist (%s is missing)", ErrNoSources, root, component)
	case err != nil:
		return fmt.Errorf("inspecting source root %q: %w", root, err)
	case !info.IsDir():
		return fmt.Errorf("%w: root %q passes through %s (%s), not a directory",
			ErrIrregularSource, root, component, info.Mode().Type())
	}
	return nil
}

// collectFlat reads the root's own entries. Real subdirectories are not
// descended into; everything else goes through consider, exactly as in a tree.
func (c *snapshotCollector) collectFlat(ctx context.Context, rootPath string, filter FileFilter) error {
	entries, err := os.ReadDir(rootPath)
	if err != nil {
		return fmt.Errorf("reading source root: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := c.consider(ctx, filepath.Join(rootPath, entry.Name()), entry, filter); err != nil {
			return err
		}
	}
	return nil
}

// collectTree walks the root. WalkDir never follows a symlinked directory: it
// reports the link as an entry, and consider refuses it.
func (c *snapshotCollector) collectTree(ctx context.Context, rootPath string, filter FileFilter) error {
	return filepath.WalkDir(rootPath, func(entryPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walking source root: %w", walkErr)
		}
		if entry.IsDir() {
			return nil
		}
		return c.consider(ctx, entryPath, entry, filter)
	})
}

// consider refuses any entry that is not a regular file BEFORE asking the
// filter. A link is refused even when the filter would skip it: whether it is
// skipped then depends on the name the link has, not on what it points at, and
// a directory link skipped today becomes a tree of borrowed files tomorrow.
func (c *snapshotCollector) consider(ctx context.Context, entryPath string, entry fs.DirEntry, filter FileFilter) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("taking a source snapshot: %w", err)
	}
	rel, err := filepath.Rel(c.base, entryPath)
	if err != nil {
		return fmt.Errorf("locating %s under the base: %w", entry.Name(), err)
	}
	rel = filepath.ToSlash(rel)
	if !entry.Type().IsRegular() {
		return fmt.Errorf("%w: %s (%s)", ErrIrregularSource, rel, entry.Type())
	}
	if !filter(rel) {
		return nil
	}
	if strings.ContainsAny(rel, "\r\n") {
		return fmt.Errorf("%w: %q cannot be held by a line-oriented manifest", ErrInvalidSourceSpec, rel)
	}
	if rel == manifestName {
		// The kept copy stores its listing under this name at the top level, so
		// a source file of that name would be overwritten by its own manifest.
		return fmt.Errorf("%w: a source may not be named %s", ErrInvalidSourceSpec, manifestName)
	}
	if _, twice := c.files[rel]; twice {
		return fmt.Errorf("%w: %s is reached through two roots", ErrInvalidSourceSpec, rel)
	}
	content, err := os.ReadFile(entryPath) //nolint:gosec // a source file the spec selected
	if err != nil {
		return fmt.Errorf("reading source %s: %w", rel, err)
	}
	sum := sha256.Sum256(content)
	c.files[rel] = SourceFile{Path: rel, Digest: hex.EncodeToString(sum[:]), Content: content}
	return nil
}

// Manifest is the canonical listing the stamp is taken over: the format tag,
// the fingerprint name, then one "<sha256>  <path>" line per file in byte
// order of paths.
func (s Snapshot) Manifest() []byte {
	var out strings.Builder
	fmt.Fprintf(&out, "%s\nname %s\n", sourcesFormat, s.spec.Name)
	for _, file := range s.files {
		out.WriteString(file.Digest)
		out.WriteString(manifestSeparator)
		out.WriteString(file.Path)
		out.WriteByte('\n')
	}
	return []byte(out.String())
}

func stampOf(manifest []byte) Stamp {
	sum := sha256.Sum256(manifest)
	return Stamp(hex.EncodeToString(sum[:])[:stampHexLen])
}

// Stamp names the snapshot: the fingerprint's name and the stamp derived from
// its manifest.
func (s Snapshot) Stamp() SourceStamp {
	return SourceStamp{Name: s.spec.Name, Stamp: s.stamp}
}

// Files returns the snapshot's files in manifest order. The slice is a copy;
// the contents are shared and must not be modified.
func (s Snapshot) Files() []SourceFile {
	return append([]SourceFile(nil), s.files...)
}

// VerifyTree takes the snapshot again and refuses if the tree no longer
// matches. A driver calls it after a batch: results recorded under a stamp
// while the sources moved underneath are labelled with code they were not
// measured with.
//
// A tree that can no longer be stamped at all — a root gone or emptied, a link
// or other irregular entry that appeared — took a snapshot once and does not
// now, so it is a change, reported as ErrSourcesChanged with the cause kept.
func (s Snapshot) VerifyTree(ctx context.Context) error {
	again, err := TakeSnapshot(ctx, s.spec)
	if errors.Is(err, ErrNoSources) || errors.Is(err, ErrIrregularSource) {
		return fmt.Errorf("%w: %s (was %s): %w", ErrSourcesChanged, s.spec.Name, s.stamp, err)
	}
	if err != nil {
		return fmt.Errorf("re-reading sources %s: %w", s.spec.Name, err)
	}
	if again.stamp != s.stamp {
		return fmt.Errorf("%w: %s stamped %s at the start and %s now", ErrSourcesChanged, s.spec.Name, s.stamp, again.stamp)
	}
	return nil
}

// maxFingerprintNameBytes keeps "<name>-<stamp>", the kept copy's directory
// name, within what a filesystem accepts.
const maxFingerprintNameBytes = maxFileNameBytes - len("-") - stampHexLen
