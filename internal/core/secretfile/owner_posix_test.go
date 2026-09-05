//go:build !windows

package secretfile

import (
	"os"
	"testing"
)

// assertOwnerOnly is the POSIX half of "is this owner-only?".
//
// It is a separate file from the tests that call it, and it has a Windows
// counterpart with the same signature in secretfile_windows_test.go, so a test
// about BEHAVIOUR — a symlink not being followed, a rename replacing a wider
// target — can assert the protection without knowing which platform it is on.
// Mixing the two used to force such a test to skip on Windows entirely, which
// threw away the behavioural half along with the mode check.
func assertOwnerOnly(t *testing.T, what, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("%s: lstat: %v", what, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("%s: is a symlink, not a file of its own", what)
	}
	want := os.FileMode(0o600)
	if info.IsDir() {
		want = 0o700
	}
	if perm := info.Mode().Perm(); perm != want {
		t.Fatalf("%s: permissions = %o, want %o", what, perm, want)
	}
}
