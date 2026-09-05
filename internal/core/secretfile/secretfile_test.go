package secretfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// secretfile_test.go holds the behavioural contract, and it is deliberately
// free of platform branches.
//
// "Is this owner-only?" is the one question whose ANSWER differs by platform —
// mode bits on POSIX, a DACL on NTFS — so it lives behind assertOwnerOnly,
// which has a build-tagged implementation on each side. Everything else here
// is about behaviour that must hold everywhere: a symlink is not followed, a
// name cannot leave the directory, a temp is unique. Those used to be skipped
// wholesale on Windows because the mode assertion was mixed into them, which
// threw away the behavioural half along with the part that did not apply.
//
// Symlink-dependent tests do not guess whether the platform allows links —
// they ask this machine (fsprobe.RequireSymlinks). On Windows the answer
// depends on privilege, not on the OS, so an elevated shell or Developer Mode
// runs them for real.

func openDir(t *testing.T, path string) *Dir {
	t.Helper()
	dir, err := Open(path)
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	t.Cleanup(func() { _ = dir.Close() })
	return dir
}

// TestWriteReplacesWiderModeTarget: os.WriteFile applies its mode only when
// it CREATES the file, so writing over an existing wide-open target leaves the
// secret readable. The rename here hands the target the temp's own restricted
// inode instead.
func TestWriteReplacesWiderModeTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "secret")
	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatalf("pre-create: %v", err)
	}

	if err := openDir(t, dir).Write("secret", []byte("new secret")); err != nil {
		t.Fatalf("write: %v", err)
	}

	assertOwnerOnly(t, "the replaced target", target)
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(contents) != "new secret" {
		t.Fatalf("contents = %q", contents)
	}
}

// TestTempIsOwnerOnlyBeforeAnyContent pins the ordering the whole platform
// split exists for: the file is protected from the instant it exists, not
// after the bytes are in it.
//
// A create-then-restrict sequence leaves a window in which the file exists
// under the parent directory's terms, and a handle another process obtained
// during that window keeps its access — tightening afterwards does not revoke
// what was already granted. The assertion is therefore made on an EMPTY file.
func TestTempIsOwnerOnlyBeforeAnyContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	pinned := openDir(t, dir)

	file, name, err := pinned.createTemp()
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer func() { _ = file.Close() }()

	path := filepath.Join(dir, name)
	assertOwnerOnly(t, "a freshly created temp file", path)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("the temp file already holds %d bytes — the check above proves nothing", info.Size())
	}
}

// TestWriteReplacesSymlinkTargetInsteadOfFollowingIt: a symlink planted at
// the target name must not receive the secret.
//
// The write never opens the target — it fills a fresh temp and renames onto
// the name, and rename replaces the LINK rather than writing through it. That
// is the whole reason the write is shaped this way: os.WriteFile on the target
// would have followed the link and put both private keys wherever it pointed.
// The result is a real, owner-only file at the name the caller asked for and
// an untouched victim.
func TestWriteReplacesSymlinkTargetInsteadOfFollowingIt(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)

	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("original"), 0o600); err != nil {
		t.Fatalf("create victim: %v", err)
	}
	target := filepath.Join(dir, "secret")
	if err := os.Symlink(victim, target); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := openDir(t, dir).Write("secret", []byte("both private keys")); err != nil {
		t.Fatalf("write: %v", err)
	}

	contents, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read victim: %v", err)
	}
	if string(contents) != "original" {
		t.Fatal("the secret was written through the symlink into its target")
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("lstat target: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("the target is still a symlink — the secret did not land where the caller asked")
	}
	assertOwnerOnly(t, "the replaced symlink target", target)
}

// TestSubRefusesLinkThatStaysInsideTheRoot is the escape "must not leave the
// root" does not cover.
//
// A symlink pointing at "." — or at any other directory within the root —
// satisfies os.Root completely, and then every write through the returned
// handle lands somewhere the caller never named. In the node's backup sandbox
// that turns a backup called "trust-64646.json" into an overwrite of the trust
// store. Sub answers it by proving the directory it OPENED is the entry it
// NAMED, which no pre-flight check on the name can do race-free.
func TestSubRefusesLinkThatStaysInsideTheRoot(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sibling"), 0o700); err != nil {
		t.Fatalf("mkdir sibling: %v", err)
	}
	if err := os.Symlink(".", filepath.Join(dir, "self")); err != nil {
		t.Fatalf("symlink to parent: %v", err)
	}
	if err := os.Symlink("sibling", filepath.Join(dir, "aside")); err != nil {
		t.Fatalf("symlink to sibling: %v", err)
	}

	pinned := openDir(t, dir)
	for _, name := range []string{"self", "aside"} {
		sub, err := pinned.Sub(name)
		if err == nil {
			_ = sub.Close()
			t.Fatalf("Sub accepted %q, a link to another directory inside the root", name)
		}
		if !errors.Is(err, ErrNotADirectory) {
			t.Fatalf("Sub(%q) error = %v, want ErrNotADirectory", name, err)
		}
	}

	// A real subdirectory must still open, or the check is just a ban.
	sub, err := pinned.Sub("sibling")
	if err != nil {
		t.Fatalf("Sub refused a real subdirectory: %v", err)
	}
	defer func() { _ = sub.Close() }()
	if err := sub.Write("secret", []byte("payload")); err != nil {
		t.Fatalf("write into a real subdirectory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sibling", "secret")); err != nil {
		t.Fatalf("the write did not land in the subdirectory: %v", err)
	}
}

// TestSubRefusesEscapingLink: names that resolve outside the pinned directory
// are refused by the handle-relative resolution itself, not by string
// inspection.
func TestSubRefusesEscapingLink(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)

	dir := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	pinned := openDir(t, dir)
	if sub, err := pinned.Sub("link"); err == nil {
		_ = sub.Close()
		t.Fatal("the root opened a subdirectory outside itself")
	}
	if _, err := os.Stat(filepath.Join(outside, "secret")); !os.IsNotExist(err) {
		t.Fatal("something was written outside the root")
	}
}

// TestRestrictMakesTheDirectoryOwnerOnly covers the directory half. On
// Windows this needs a handle opened with WRITE_DAC — the right whose absence
// broke identity.Save, and with it every application start, in an earlier
// attempt — so it is worth asserting on both platforms rather than trusting
// that a Chmod-shaped POSIX pass means the NTFS side works.
func TestRestrictMakesTheDirectoryOwnerOnly(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	sub := filepath.Join(parent, "sandbox")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(sub, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	child, err := openDir(t, parent).Sub("sandbox")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	defer func() { _ = child.Close() }()

	if err := child.Restrict(); err != nil {
		t.Fatalf("Restrict: %v", err)
	}
	assertOwnerOnly(t, "the restricted directory", sub)

	// And the directory must still be usable afterwards — a DACL that locks
	// the node out of its own sandbox would be a very quiet outage.
	if err := child.Write("secret", []byte("payload")); err != nil {
		t.Fatalf("write into the restricted directory: %v", err)
	}
	if _, err := child.ReadFile("secret"); err != nil {
		t.Fatalf("read back from the restricted directory: %v", err)
	}
}

// TestWriteRejectsNonSingleNames: a path element, never a path. Write is the
// primitive callers reach for when they already decided WHERE; letting a
// separator through would quietly undo that decision.
func TestWriteRejectsNonSingleNames(t *testing.T) {
	t.Parallel()
	dir := openDir(t, t.TempDir())
	for _, name := range []string{"", ".", "..", "sub/secret", "/abs"} {
		if err := dir.Write(name, []byte("x")); err == nil {
			t.Fatalf("write accepted %q as a file name", name)
		}
	}
}

// TestWriteLeavesNoTemp: on success the directory holds the target and
// nothing else — the temp is renamed, not left behind.
func TestWriteLeavesNoTemp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := openDir(t, dir).Write("secret", []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "secret" {
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("directory holds %v, want only the target", names)
	}
}

// TestTempNamesAreUniqueAndOutsideTheCallerNamespace: two writes must not
// collide on a temp, and the temp must be unnameable by a caller whose own
// names reject a leading dot. That disjointness is what lets a cleanup sweep
// delete temps without ever deleting an artifact.
func TestTempNamesAreUniqueAndOutsideTheCallerNamespace(t *testing.T) {
	t.Parallel()
	if !strings.HasPrefix(TempPrefix, ".") {
		t.Fatalf("TempPrefix %q does not start with a dot — it is no longer disjoint from artifact names", TempPrefix)
	}

	pinned := openDir(t, t.TempDir())
	seen := map[string]bool{}
	files := []*os.File{}
	for range 32 {
		file, name, err := pinned.createTemp()
		if err != nil {
			t.Fatalf("create temp: %v", err)
		}
		if seen[name] {
			t.Fatalf("temp name %q was handed out twice", name)
		}
		seen[name] = true
		files = append(files, file)
	}
	for _, file := range files {
		_ = file.Close()
	}
}

// TestCreateExclusiveRefusesExistingName: the create must be exclusive, so a
// planted file is a refusal rather than a target — and the error has to
// classify as "already exists", because createTemp's retry loop continues only
// on that. On Windows the underlying call reports an NTSTATUS, which satisfies
// no errors.Is check until it is translated.
func TestCreateExclusiveRefusesExistingName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	planted := TempPrefix + "planted"
	if err := os.WriteFile(filepath.Join(dir, planted), []byte("planted"), 0o600); err != nil {
		t.Fatalf("plant: %v", err)
	}

	file, err := createExclusive(openDir(t, dir), planted)
	if err == nil {
		_ = file.Close()
		t.Fatal("createExclusive opened an existing file")
	}
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("error = %v, want it to satisfy errors.Is(err, os.ErrExist)", err)
	}
}

// TestStripPathRemovesFileNames: these errors travel back to RPC callers, and
// the node's filesystem layout is not something a caller learns by asking
// about a file that does not exist.
func TestStripPathRemovesFileNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	missing := filepath.Join(dir, "definitely-absent")

	_, err := os.ReadFile(missing)
	if err == nil {
		t.Fatal("expected a read error")
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("the raw error does not contain the path, so this test proves nothing: %v", err)
	}

	stripped := StripPath(err)
	if strings.Contains(stripped.Error(), dir) {
		t.Fatalf("StripPath left the path in: %v", stripped)
	}
	if !errors.Is(stripped, os.ErrNotExist) {
		t.Fatalf("StripPath lost the cause: %v", stripped)
	}
}

// TestWriteErrorsCarryNoPath: the same rule for the errors this package
// produces itself.
func TestWriteErrorsCarryNoPath(t *testing.T) {
	t.Parallel()
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	readonly := filepath.Join(dir, "readonly")
	if err := os.Mkdir(readonly, 0o500); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(readonly, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if writable(t, readonly) {
		t.Skip("this filesystem does not enforce directory write permission, so the error path cannot be reached")
	}

	err := openDir(t, readonly).Write("secret", []byte("payload"))
	if err == nil {
		t.Fatal("write into a read-only directory succeeded")
	}
	if strings.Contains(err.Error(), dir) {
		t.Fatalf("the error leaked the path: %v", err)
	}
}

// writable reports whether a file can still be created in dir — measured, not
// assumed, so this test skips on the filesystems (and the platforms) where
// directory permissions do not deny writes rather than pretending to cover
// them.
func writable(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, "fsprobe-write")
	file, err := os.OpenFile(probe, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return false
	}
	_ = file.Close()
	_ = os.Remove(probe)
	return true
}
