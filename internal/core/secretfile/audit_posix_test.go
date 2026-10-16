//go:build !windows

package secretfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// audit_posix_test.go covers the part of AuditLocation that exists only on
// POSIX: the ancestor chain judged by owner and mode bits. The first group
// works on real directories under t.TempDir; the second feeds the judgement
// synthetic owners, because a foreign or root owner cannot be produced
// without privileges the test does not have.

// makeChain creates base/<names...> as 0700 directories and returns the paths
// in order, so a test can widen exactly one of them.
func makeChain(t *testing.T, names ...string) []string {
	t.Helper()
	paths := []string{t.TempDir()}
	for _, name := range names {
		next := filepath.Join(paths[len(paths)-1], name)
		if err := os.Mkdir(next, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		paths = append(paths, next)
	}
	return paths
}

func chmodForTest(t *testing.T, path string, mode fs.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
}

// The Docker case: a volume mounted 0777 as the data directory.
func TestAuditReportsAWorldWritableDirectoryHoldingTheFile(t *testing.T) {
	t.Parallel()
	chain := makeChain(t, "data")
	chmodForTest(t, chain[1], 0o777)
	path := filepath.Join(chain[1], "secure-sessions-64646.json")

	err := AuditLocation(path)
	unsafe := unsafeLocationOf(t, err)
	want := LocationComponent{Kind: ComponentDirectory, LevelsAbove: 1}
	if unsafe.Violation != ViolationWritableByOthers || unsafe.Component != want {
		t.Fatalf("violation = %s at %s, want %s at %s", unsafe.Violation, unsafe.Component, ViolationWritableByOthers, want)
	}
	if unsafe.Mode.Perm() != 0o777 {
		t.Fatalf("reported mode = %s, want the directory's own", unsafe.Mode)
	}
	assertNoPath(t, err, chain[1])
}

// Not only the directory holding the file: every ancestor up to the root.
func TestAuditReportsAGroupWritableAncestor(t *testing.T) {
	t.Parallel()
	chain := makeChain(t, "home", "data")
	chmodForTest(t, chain[1], 0o770)
	path := filepath.Join(chain[2], "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}

	unsafe := unsafeLocationOf(t, AuditLocation(path))
	want := LocationComponent{Kind: ComponentDirectory, LevelsAbove: 2}
	if unsafe.Violation != ViolationWritableByOthers || unsafe.Component != want {
		t.Fatalf("violation = %s at %s, want %s at %s", unsafe.Violation, unsafe.Component, ViolationWritableByOthers, want)
	}
}

// The data directory under /tmp: a sticky world-writable directory is an
// acceptable step when the next component belongs to this process's user —
// others can create names there but cannot rename or remove ours.
func TestAuditAcceptsAStickyDirectoryWhoseNextComponentIsOurs(t *testing.T) {
	t.Parallel()
	chain := makeChain(t, "shared", "data")
	chmodForTest(t, chain[1], 0o777|fs.ModeSticky)
	path := filepath.Join(chain[2], "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := AuditLocation(path); err != nil {
		t.Fatalf("AuditLocation = %v, want nil under a sticky directory", err)
	}
}

// The writer creates 0600; an owner-only 0400 is still not what it creates,
// and the contract names the mode exactly.
func TestAuditReportsAFileModeOtherThanTheWritersOwn(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	unsafe := unsafeLocationOf(t, AuditLocation(path))
	if unsafe.Violation != ViolationFileNotOwnerOnly {
		t.Fatalf("violation = %s, want %s", unsafe.Violation, ViolationFileNotOwnerOnly)
	}
}

// A link on the way is followed — macOS itself reaches every temp directory
// through /var -> private/var — and judged by the directory that holds it:
// whoever can write there can point the link anywhere.
func TestAuditJudgesALinkByTheDirectoryHoldingIt(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	chain := makeChain(t, "open")
	real := filepath.Join(chain[0], "private", "data")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	link := filepath.Join(chain[1], "data")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	path := filepath.Join(link, "secure-sessions-64646.json")
	if err := AuditLocation(path); err != nil {
		t.Fatalf("AuditLocation = %v, want nil through a link in a private directory", err)
	}

	chmodForTest(t, chain[1], 0o777)
	unsafe := unsafeLocationOf(t, AuditLocation(path))
	if unsafe.Violation != ViolationWritableByOthers || unsafe.Component.Kind != ComponentLinkRoute {
		t.Fatalf("violation = %s at %s, want %s on the link's route", unsafe.Violation, unsafe.Component, ViolationWritableByOthers)
	}
}

const (
	testEUID    uint32 = 501
	testForeign uint32 = 1234
)

func directoryEntry(levels int, owner uint32, mode fs.FileMode) chainEntry {
	return chainEntry{
		component: LocationComponent{Kind: ComponentDirectory, LevelsAbove: levels},
		owner:     owner,
		mode:      fs.ModeDir | mode,
	}
}

func TestJudgeChainSyntheticOwners(t *testing.T) {
	t.Parallel()
	file := func(owner uint32) chainEntry {
		return chainEntry{component: fileComponent(), owner: owner, mode: 0o600}
	}
	sticky := directoryEntry(2, rootUID, 0o777|fs.ModeSticky)
	childOf := func(entry chainEntry) chainEntry {
		entry.parentShared = true
		return entry
	}

	cases := []struct {
		name    string
		entries []chainEntry
		want    *LocationViolation
	}{
		{
			name:    "root-owned 0755 chain and our file",
			entries: []chainEntry{directoryEntry(2, rootUID, 0o755), directoryEntry(1, testEUID, 0o700), file(testEUID)},
		},
		{
			name:    "a root-owned directory only root writes is accepted",
			entries: []chainEntry{directoryEntry(1, rootUID, 0o755), file(testEUID)},
		},
		{
			name:    "a directory owned by another user",
			entries: []chainEntry{directoryEntry(2, testForeign, 0o755), directoryEntry(1, testEUID, 0o700), file(testEUID)},
			want:    violationOf(ViolationForeignOwner),
		},
		{
			name:    "the file owned by root while the process is not root",
			entries: []chainEntry{directoryEntry(1, testEUID, 0o700), file(rootUID)},
			want:    violationOf(ViolationForeignOwner),
		},
		{
			name:    "a root-owned group-writable directory",
			entries: []chainEntry{directoryEntry(1, rootUID, 0o775), file(testEUID)},
			want:    violationOf(ViolationWritableByOthers),
		},
		{
			name:    "a sticky root directory followed by our directory",
			entries: []chainEntry{sticky, childOf(directoryEntry(1, testEUID, 0o700)), file(testEUID)},
		},
		{
			name:    "a sticky root directory followed by a root-owned one",
			entries: []chainEntry{sticky, childOf(directoryEntry(1, rootUID, 0o755)), file(testEUID)},
			want:    violationOf(ViolationStickyChildNotOwned),
		},
	}
	for _, tc := range cases {
		got := judgeChain(tc.entries, testEUID)
		switch {
		case tc.want == nil && got != nil:
			t.Errorf("%s: judged %s at %s, want accepted", tc.name, got.Violation, got.Component)
		case tc.want != nil && got == nil:
			t.Errorf("%s: accepted, want %s", tc.name, *tc.want)
		case tc.want != nil && got.Violation != *tc.want:
			t.Errorf("%s: judged %s, want %s", tc.name, got.Violation, *tc.want)
		}
	}
}

func violationOf(v LocationViolation) *LocationViolation { return &v }

// Two links naming each other never resolve. The audit must stop with its
// own error, as the kernel stops with ELOOP, rather than walk forever or
// report the location as safe.
func TestAuditStopsAtALinkLoop(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	dir := t.TempDir()
	if err := os.Symlink("b", filepath.Join(dir, "a")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Symlink("a", filepath.Join(dir, "b")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	err := AuditLocation(filepath.Join(dir, "a", "secure-sessions-64646.json"))
	if !errors.Is(err, errLinkLoop) {
		t.Fatalf("AuditLocation through a link loop = %v, want errLinkLoop", err)
	}
	assertNoPath(t, err, dir)
}
