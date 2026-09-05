// Package fsprobe answers "can this filesystem do X here?" by trying it,
// for tests that need a capability the platform may or may not grant.
//
// It exists because the obvious alternative is wrong in a way that hides
// itself: `if runtime.GOOS == "windows" { t.Skip() }` reads like "this cannot
// work on Windows", but what it actually does is guarantee the test NEVER runs
// there — including in the elevated shell where the capability is present and
// where somebody was told the test would cover them. A skip that is a
// prediction about the platform silently outlives the prediction; a skip that
// is a measurement of the machine does not.
package fsprobe

import (
	"os"
	"path/filepath"
	"testing"
)

// RequireSymlinks skips the test only when this machine actually refuses to
// create a symlink.
//
// On Windows that depends on privilege (SeCreateSymbolicLinkPrivilege) or
// Developer Mode rather than on the OS, so the answer is per-run and can only
// be had by asking. On POSIX it is normally yes, but not on every filesystem a
// CI runner might hand out. Either way the decision is made by attempting the
// operation, and the reason a run skipped is reported rather than assumed.
func RequireSymlinks(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	target := filepath.Join(dir, "fsprobe-target")
	if err := os.WriteFile(target, []byte("probe"), 0o600); err != nil {
		t.Fatalf("fsprobe: create probe target: %v", err)
	}
	link := filepath.Join(dir, "fsprobe-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("fsprobe: this machine cannot create symlinks, so the test cannot run: %v", err)
	}
	// Creating it is not the same as it BEING one: a filesystem that quietly
	// copies instead of linking would let a symlink test pass while proving
	// nothing about symlinks.
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("fsprobe: inspect probe link: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Skipf("fsprobe: this filesystem does not keep symlinks as links, so the test cannot run")
	}
}
