//go:build windows

package secretfile

import (
	"fmt"
	"os"
	"os/exec"
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

// The tests below set real DACLs on directories under t.TempDir and read the
// verdict back: the directory rule of the Windows audit (audit_acl.go). The
// user's grant is inheritable (OICI) so everything below keeps working for
// the test and its cleanup; the stranger's grant is not, so it applies to
// that one directory only.

// grantEveryoneOn gives Everyone mask on dir, beside the user's full control.
func grantEveryoneOn(t *testing.T, dir string, mask uint32) {
	t.Helper()
	setDACL(t, dir, fmt.Sprintf("D:P(A;OICI;FA;;;%s)(A;;0x%x;;;WD)", currentUserSID(t), mask))
}

func directoryChain(t *testing.T) (ancestor, holding string) {
	t.Helper()
	ancestor = filepath.Join(t.TempDir(), "a")
	holding = filepath.Join(ancestor, "b")
	if err := os.MkdirAll(holding, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return ancestor, holding
}

// Before the first write: the store does not exist yet, and a holding
// directory others may add files to is already a refusal — a file planted
// there under the store's name would be read as the store.
func TestAuditReportsAHoldingDirectoryOthersMayWriteBeforeTheFirstWrite(t *testing.T) {
	t.Parallel()
	_, holding := directoryChain(t)
	grantEveryoneOn(t, holding, rightAddFile)

	unsafe := unsafeLocationOf(t, AuditLocation(filepath.Join(holding, "secure-sessions-64646.json")))
	want := LocationComponent{Kind: ComponentDirectory, LevelsAbove: 1}
	if unsafe.Violation != ViolationWritableByOthers || unsafe.Component != want {
		t.Fatalf("violation = %s at %s, want %s at %s", unsafe.Violation, unsafe.Component, ViolationWritableByOthers, want)
	}
}

func TestAuditReportsAnAncestorOthersMayChange(t *testing.T) {
	t.Parallel()
	for name, mask := range map[string]uint32{
		"delete or rename it":    rightDelete,
		"delete its children":    rightDeleteChild,
		"rewrite its DACL":       rightWriteDAC,
		"take ownership of it":   rightWriteOwner,
		"hold GENERIC_ALL on it": rightGenericAll,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ancestor, holding := directoryChain(t)
			path := filepath.Join(holding, "secure-sessions-64646.json")
			if err := WriteFile(path, []byte("{}")); err != nil {
				t.Fatalf("write: %v", err)
			}
			grantEveryoneOn(t, ancestor, mask)

			unsafe := unsafeLocationOf(t, AuditLocation(path))
			want := LocationComponent{Kind: ComponentDirectory, LevelsAbove: 2}
			if unsafe.Violation != ViolationWritableByOthers || unsafe.Component != want {
				t.Fatalf("violation = %s at %s, want %s at %s", unsafe.Violation, unsafe.Component, ViolationWritableByOthers, want)
			}
		})
	}
}

// Creating siblings is not replacing the directory on the path: the right
// the volume root gives Authenticated Users is accepted on an ancestor.
func TestAuditAcceptsAnAncestorOthersMayOnlyAddDirectoriesTo(t *testing.T) {
	t.Parallel()
	ancestor, holding := directoryChain(t)
	path := filepath.Join(holding, "secure-sessions-64646.json")
	if err := WriteFile(path, []byte("{}")); err != nil {
		t.Fatalf("write: %v", err)
	}
	grantEveryoneOn(t, ancestor, rightAddSubdirectory)

	if err := AuditLocation(path); err != nil {
		t.Fatalf("AuditLocation = %v, want nil", err)
	}
}

// A junction on the path decides where the store is read from and written
// to, whatever the DACLs on either side say.
func TestAuditReportsAJunctionOnThePath(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	junction := filepath.Join(base, "junction")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", junction, target).CombinedOutput(); err != nil {
		t.Fatalf("mklink /J: %v: %s", err, out)
	}

	unsafe := unsafeLocationOf(t, AuditLocation(filepath.Join(junction, "secure-sessions-64646.json")))
	if unsafe.Violation != ViolationSymlink || unsafe.Component.Kind != ComponentLinkRoute {
		t.Fatalf("violation = %s at %s, want %s on the way", unsafe.Violation, unsafe.Component, ViolationSymlink)
	}
}
