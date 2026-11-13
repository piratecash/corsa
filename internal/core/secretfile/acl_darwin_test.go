package secretfile

import (
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"testing"
)

func setFixtureACE(t *testing.T, path, ace string) {
	t.Helper()
	if raw, err := exec.Command("/bin/chmod", "+a", ace, path).CombinedOutput(); err != nil {
		t.Fatalf("set fixture ACL: %v (%s)", err, raw)
	}
}

func TestMacOSACLAllowsProtectiveDeny(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	setFixtureACE(t, dir, "everyone deny delete")
	t.Cleanup(func() {
		if raw, err := exec.Command("/bin/chmod", "-N", dir).CombinedOutput(); err != nil {
			t.Errorf("clear fixture ACL: %v (%s)", err, raw)
		}
	})
	path := filepath.Join(dir, "identity.json")
	if err := WriteFile(path, []byte("fixture")); err != nil {
		t.Fatal(err)
	}
	owner, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	setFixtureACE(t, path, owner.Username+" allow read")
	pinned, file, err := OpenAuditedRead(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	_ = pinned.Close()
}

func TestMacOSACLRefusesForeignFileGrant(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	setFixtureACE(t, path, "everyone allow read")
	dir, file, err := OpenAuditedRead(path)
	if dir != nil || file != nil || !errors.Is(err, ErrUnsafeLocation) {
		t.Fatalf("expected refusal before reading the empty file: %v", err)
	}
	var unsafe *UnsafeLocationError
	if !errors.As(err, &unsafe) || unsafe.Violation != ViolationExtendedACL || unsafe.Component != fileComponent() {
		t.Fatalf("unexpected location violation: %v", err)
	}
}

func TestMacOSACLRefusesInheritedGrantBeforeWriting(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	setFixtureACE(t, path, "everyone allow read,file_inherit")
	dir, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()

	for _, attempt := range []func() error{
		func() error {
			file, err := dir.CreateExclusive("capture")
			if file != nil {
				_ = file.Close()
			}
			return err
		},
		func() error { return dir.Write("identity.json", []byte("fixture")) },
	} {
		var unsafe *UnsafeLocationError
		err := attempt()
		if !errors.As(err, &unsafe) || unsafe.Violation != ViolationExtendedACL || unsafe.Component != fileComponent() {
			t.Fatalf("expected refusal for the new file's inherited ACL: %v", err)
		}
		entries, err := dir.Entries()
		if err != nil || len(entries) != 0 {
			t.Fatalf("refused creation left an entry: %v (%v)", entries, err)
		}
	}
}

func TestMacOSACLRefusesMutableDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	setFixtureACE(t, dir, "everyone allow delete_child")
	err := AuditLocation(filepath.Join(dir, "absent"))
	var unsafe *UnsafeLocationError
	want := LocationComponent{Kind: ComponentDirectory, LevelsAbove: 1}
	if !errors.As(err, &unsafe) || unsafe.Violation != ViolationExtendedACL || unsafe.Component != want {
		t.Fatalf("expected refusal at the directory: %v", err)
	}
}

func TestMacOSACLQueryDistinguishesFailureFromAbsence(t *testing.T) {
	t.Parallel()
	api, err := loadDarwinACL()
	if err != nil {
		t.Fatal(err)
	}
	if acl, err := api.read(-1); acl != nil || err == nil {
		t.Fatal("invalid descriptor was treated as a file without an ACL")
	}
	path := filepath.Join(t.TempDir(), "empty")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if acl, err := api.read(int32(file.Fd())); acl != nil || err != nil {
		t.Fatalf("expected explicit absence of ACL on empty fixture: %v", err)
	}
}
