//go:build windows

package secretfile

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

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
// The behavioural tests in secretfile_test.go run on both platforms; this
// file supplies the one thing whose answer differs, so those tests assert
// the protection without a platform branch. The descriptor table below
// checks this Windows-only oracle against SID aliases and unsafe ACL shapes.
// A platform branch in each behavioural test used to force a whole test to
// skip on Windows, throwing away the behavioural half as well. The symlink-dependent tests ask this machine whether it can create a
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
	sid, err := windows.StringToSid(currentUserSID(t))
	if err != nil {
		t.Fatalf("%s: parse current user SID: %v", what, err)
	}
	if err := ownerOnlyDescriptorError(descriptor, sid); err != nil {
		t.Fatalf("%s: %v: %s", what, err, descriptor.String())
	}
}

// ownerOnlyDescriptorError checks binary trustees, not their SDDL spelling:
// Windows may print a numeric account SID using an alias such as LA.
// This is intentionally stricter than the general location audit: it checks
// the exact single full-access grant that this package creates.
func ownerOnlyDescriptorError(descriptor *windows.SECURITY_DESCRIPTOR, owner *windows.SID) error {
	if descriptor == nil || owner == nil || !owner.IsValid() {
		return fmt.Errorf("missing descriptor or valid owner SID")
	}
	// The ACE and its variable-length SID point into the descriptor.
	defer runtime.KeepAlive(descriptor)
	control, _, err := descriptor.Control()
	if err != nil {
		return fmt.Errorf("read control flags: %w", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		return fmt.Errorf("DACL is not protected")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read DACL: %w", err)
	}
	if dacl == nil {
		return fmt.Errorf("DACL is NULL and grants unrestricted access")
	}
	if dacl.AceCount != 1 {
		return fmt.Errorf("DACL has %d ACEs, want exactly one", dacl.AceCount)
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(dacl, 0, &ace); err != nil {
		return fmt.Errorf("read ACE: %w", err)
	}
	if ace == nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
		return fmt.Errorf("DACL does not contain one ordinary allow ACE")
	}
	if ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
		return fmt.Errorf("owner grant is inherit-only and does not apply to the object")
	}
	if ace.Mask != windows.ACCESS_MASK(fileAllAccess) {
		return fmt.Errorf("owner grant mask = %#x, want FILE_ALL_ACCESS", ace.Mask)
	}
	const sidHeaderBytes = 8
	sidOffset := unsafe.Offsetof(ace.SidStart)
	if uintptr(ace.Header.AceSize) < sidOffset+sidHeaderBytes {
		return fmt.Errorf("owner grant has a truncated SID")
	}
	trustee := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if sidBytes := sidHeaderBytes + 4*uintptr(trustee.SubAuthorityCount()); sidBytes > uintptr(ace.Header.AceSize)-sidOffset {
		return fmt.Errorf("owner grant has a truncated SID")
	}
	if !trustee.IsValid() || !trustee.Equals(owner) {
		return fmt.Errorf("DACL does not grant the expected owner SID")
	}
	return nil
}

func TestOwnerOnlyDescriptorOracleUsesBinarySIDs(t *testing.T) {
	// Local System has a fixed numeric SID and the SDDL alias SY. This
	// exercises alias equivalence independently of which account runs CI;
	// assertOwnerOnly above still checks the real process token's SID.
	owner, err := windows.StringToSid("S-1-5-18")
	if err != nil {
		t.Fatalf("parse expected SID: %v", err)
	}
	for _, tc := range []struct {
		name          string
		sddl          string
		wantOwnerOnly bool
	}{
		{"numeric SID", "D:P(A;;FA;;;S-1-5-18)", true},
		{"SID alias", "D:P(A;;FA;;;SY)", true},
		{"wrong SID", "D:P(A;;FA;;;WD)", false},
		{"extra allow ACE", "D:P(A;;FA;;;SY)(A;;FR;;;WD)", false},
		{"duplicate owner ACE", "D:P(A;;FA;;;SY)(A;;FA;;;SY)", false},
		{"deny ACE", "D:P(D;;FA;;;SY)", false},
		{"allow plus deny ACE", "D:P(D;;FW;;;WD)(A;;FA;;;SY)", false},
		{"insufficient mask", "D:P(A;;FR;;;SY)", false},
		{"NULL DACL", "D:PNO_ACCESS_CONTROL", false},
		{"empty DACL", "D:P", false},
		{"unprotected DACL", "D:(A;;FA;;;SY)", false},
		{"inherit-only grant", "D:P(A;OICIIO;FA;;;SY)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descriptor, err := windows.SecurityDescriptorFromString(tc.sddl)
			if err != nil {
				t.Fatalf("build fixture descriptor: %v", err)
			}
			err = ownerOnlyDescriptorError(descriptor, owner)
			if got := err == nil; got != tc.wantOwnerOnly {
				t.Fatalf("owner-only = %t, want %t (error: %v)", got, tc.wantOwnerOnly, err)
			}
		})
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
