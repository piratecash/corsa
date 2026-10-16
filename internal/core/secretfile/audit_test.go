package secretfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// audit_test.go holds the behaviour of AuditLocation and of the "directory
// not writable" error that holds on every platform. The one platform-dependent
// step — widening a file or taking write access away — goes through helpers
// with one signature and two build tags (owner_posix_test.go,
// owner_windows_test.go), so none of these tests branches on the OS.

func unsafeLocationOf(t *testing.T, err error) *UnsafeLocationError {
	t.Helper()
	if !errors.Is(err, ErrUnsafeLocation) {
		t.Fatalf("AuditLocation = %v, want ErrUnsafeLocation", err)
	}
	var unsafe *UnsafeLocationError
	if !errors.As(err, &unsafe) {
		t.Fatalf("AuditLocation = %v, want *UnsafeLocationError", err)
	}
	return unsafe
}

// assertNoPath enforces "errors without a path": the location of the node's
// data is the operator's business, and these errors reach logs and callers.
func assertNoPath(t *testing.T, err error, dir string) {
	t.Helper()
	// The test's own directory name is unique; the last element of t.TempDir
	// ("001") is not, and could match a uid.
	if strings.Contains(err.Error(), dir) || strings.Contains(err.Error(), filepath.Base(filepath.Dir(dir))) {
		t.Fatalf("error names the path: %v", err)
	}
}

func TestAuditAcceptsASecretThisPackageWrote(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := AuditLocation(path); err != nil {
		t.Fatalf("AuditLocation = %v, want nil for a file this package wrote into a private directory", err)
	}
}

// Before the first write there is no file; the directory above it is still
// audited, and a directory not created yet has nothing in it to audit.
func TestAuditAcceptsALocationNotWrittenYet(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	for _, path := range []string{
		filepath.Join(base, "secure-sessions-64646.json"),
		filepath.Join(base, "not", "created", "secure-sessions-64646.json"),
	} {
		if err := AuditLocation(path); err != nil {
			t.Fatalf("AuditLocation(%s) = %v, want nil", path, err)
		}
	}
}

// A symlink in place of the file is a violation even when it points at a
// perfectly private file: the link decides where the protection is read from
// and where the next write lands.
func TestAuditReportsASymlinkInPlaceOfTheFile(t *testing.T) {
	t.Parallel()
	fsprobe.RequireSymlinks(t)
	dir := t.TempDir()
	if err := WriteFile(filepath.Join(dir, "real.json"), []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	path := filepath.Join(dir, "secure-sessions-64646.json")
	if err := os.Symlink("real.json", path); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	err := AuditLocation(path)
	unsafe := unsafeLocationOf(t, err)
	if unsafe.Violation != ViolationSymlink || unsafe.Component != fileComponent() {
		t.Fatalf("violation = %s at %s, want %s at the file", unsafe.Violation, unsafe.Component, ViolationSymlink)
	}
	assertNoPath(t, err, dir)
}

func TestAuditReportsAFileOthersCanRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	grantOthersRead(t, path)

	err := AuditLocation(path)
	unsafe := unsafeLocationOf(t, err)
	if unsafe.Violation != ViolationFileNotOwnerOnly || unsafe.Component != fileComponent() {
		t.Fatalf("violation = %s at %s, want %s at the file", unsafe.Violation, unsafe.Component, ViolationFileNotOwnerOnly)
	}
	assertNoPath(t, err, dir)
}

// A directory the process cannot write into is a configuration problem the
// operator must be able to read off the error: which directory (by role),
// who owns it and with what mode — not a bare "permission denied".
func TestWriteNamesADirectoryThisProcessCannotWriteInto(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	denyOwnerWrites(t, dir)
	fsprobe.RequireWriteRefused(t, dir)

	err := WriteFile(filepath.Join(dir, "secure-sessions-64646.json"), []byte("{}"))
	if !errors.Is(err, ErrDirectoryNotWritable) {
		t.Fatalf("WriteFile = %v, want ErrDirectoryNotWritable", err)
	}
	if !strings.Contains(err.Error(), "owner") || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("error does not name the directory's owner and mode: %v", err)
	}
	assertNoPath(t, err, dir)
}
