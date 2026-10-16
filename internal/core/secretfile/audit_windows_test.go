//go:build windows

package secretfile

import (
	"fmt"
	"path/filepath"
	"testing"
)

// audit_windows_test.go checks the Windows half of AuditLocation against the
// real DACL — the one thing a POSIX run cannot establish. The behavioural
// tests in audit_test.go run here too; these cover the shapes of DACL the
// shared helpers do not produce.
//
// Run before shipping any change to audit_windows.go:
//
//	go test ./internal/core/secretfile/...

// A DACL with a second allowed ACE is not owner-only, whoever that ACE is
// for — here a narrower right for Authenticated Users rather than Everyone.
func TestAuditReportsADACLWithASecondAllowedACE(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	setDACL(t, path, fmt.Sprintf("D:P(A;;FA;;;%s)(A;;FW;;;AU)", currentUserSID(t)))

	unsafe := unsafeLocationOf(t, AuditLocation(path))
	if unsafe.Violation != ViolationFileNotOwnerOnly {
		t.Fatalf("violation = %s, want %s", unsafe.Violation, ViolationFileNotOwnerOnly)
	}
}

// A deny ACE only removes access, so a DACL of the user's grant plus a deny
// for Everyone is still owner-only.
func TestAuditAcceptsADenyACEBesideTheOwnersGrant(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	setDACL(t, path, fmt.Sprintf("D:P(D;;FW;;;WD)(A;;FA;;;%s)", currentUserSID(t)))

	if err := AuditLocation(path); err != nil {
		t.Fatalf("AuditLocation = %v, want nil", err)
	}
}
