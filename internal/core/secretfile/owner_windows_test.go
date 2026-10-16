//go:build windows

package secretfile

import (
	"fmt"
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

// grantOthersRead is the Windows half of "make this file readable by somebody
// other than its owner": a DACL that keeps the current user and adds read for
// Everyone, the way an inherited ACL from a shared folder would.
func grantOthersRead(t *testing.T, path string) {
	t.Helper()
	setDACL(t, path, fmt.Sprintf("D:P(A;;FA;;;%s)(A;;FR;;;WD)", currentUserSID(t)))
}

// denyOwnerWrites is the Windows half of "this process may no longer create
// files in dir": a DACL that leaves the current user read and traverse only.
// Full control is given back on cleanup so t.TempDir can remove it.
func denyOwnerWrites(t *testing.T, dir string) {
	t.Helper()
	sid := currentUserSID(t)
	setDACL(t, dir, fmt.Sprintf("D:P(A;;FRFX;;;%s)", sid))
	t.Cleanup(func() {
		descriptor, err := windows.SecurityDescriptorFromString(fmt.Sprintf("D:P(A;OICI;FA;;;%s)", sid))
		if err != nil {
			return
		}
		dacl, _, err := descriptor.DACL()
		if err != nil {
			return
		}
		_ = windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil)
	})
}

func setDACL(t *testing.T, path, sddl string) {
	t.Helper()
	descriptor, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatalf("read dacl: %v", err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatalf("apply dacl: %v", err)
	}
}
