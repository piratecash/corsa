//go:build windows

package secretfile

import (
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// owner_windows_test.go is the half of this package's contract that a POSIX
// machine cannot check.
//
// On NTFS the mode bits mean nothing, so every guarantee about who can read a
// key file is a statement about its DACL — and that DACL is set through the NT
// API, in code that only compiles on this platform. Compiling it proves the
// types line up and nothing else.
//
// There are no Windows-only TESTS here, on purpose. The behavioural tests in
// secretfile_test.go run on both platforms; this file supplies the one thing
// whose answer differs, so those tests assert the protection without a
// platform branch. Mixing the two used to force a whole test to skip on
// Windows, throwing away the behavioural half along with the part that did not
// apply. The symlink-dependent tests ask this machine whether it can create a
// link rather than assuming the OS cannot, so an elevated shell or Developer
// Mode exercises them for real.
//
// Run the suite on Windows before shipping any change to this package:
//
//	go test ./internal/core/secretfile/...

// currentUserSID is the SID the owner-only descriptor grants.
func currentUserSID(t *testing.T) string {
	t.Helper()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatalf("resolve current user: %v", err)
	}
	return user.User.Sid.String()
}

// assertOwnerOnly is the Windows half of "is this owner-only?" — the
// counterpart of the mode check in owner_posix_test.go, with the same
// signature.
//
// It reads the object's real DACL back and checks the two things that matter:
// the DACL is PROTECTED (so the parent's inheritable ACEs are not merged back
// in) and it grants the current user and nobody else.
func assertOwnerOnly(t *testing.T, what, path string) {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("%s: read security info: %v", what, err)
	}
	control, _, err := descriptor.Control()
	if err != nil {
		t.Fatalf("%s: read control flags: %v", what, err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("%s: DACL is not protected, so the parent's inheritable ACEs still apply: %s",
			what, descriptor.String())
	}

	sddl := descriptor.String()
	dacl := sddl
	if index := strings.Index(sddl, "D:"); index >= 0 {
		dacl = sddl[index:]
	}
	sid := currentUserSID(t)
	if !strings.Contains(dacl, sid) {
		t.Fatalf("%s: DACL does not grant the current user (%s): %s", what, sid, dacl)
	}
	// Exactly one ACE. Any second entry is somebody else with access.
	if aces := strings.Count(dacl, "(A;"); aces != 1 {
		t.Fatalf("%s: DACL grants %d ACEs, want exactly one (the current user): %s", what, aces, dacl)
	}
	if denied := strings.Count(dacl, "(D;"); denied != 0 {
		t.Fatalf("%s: DACL carries deny ACEs, which is not the shape this package builds: %s", what, dacl)
	}
}
