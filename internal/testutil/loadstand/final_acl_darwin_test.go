//go:build darwin && cgo

package loadstand

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestFinalDarwinACLActualFD(t *testing.T) {
	for _, kind := range []string{"empty", "allow", "deny", "closed"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "slot")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			if kind == "allow" || kind == "deny" {
				entry := "everyone allow read"
				if kind == "deny" {
					entry = "everyone deny delete"
				}
				if out, err := exec.Command("/bin/chmod", "+a", entry, path).CombinedOutput(); err != nil {
					t.Fatalf("set own ACL: %v %s", err, out)
				}
				t.Cleanup(func() {
					if out, err := exec.Command("/bin/chmod", "-N", path).CombinedOutput(); err != nil {
						t.Errorf("clear own ACL: %v %s", err, out)
					}
				})
			}
			if kind == "closed" {
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			status := nativeFinalACL(file)
			wantOK := kind == "empty" || kind == "deny"
			if (status == 0) != wantOK {
				t.Fatalf("actual native FD ACL %s status=%d", kind, status)
			}
		})
	}
}

func TestFinalDarwinDirectoryGrantRefusesActualFD(t *testing.T) {
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := auditPrivateFinalFD(f, true); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("/bin/chmod", "+a", "everyone allow read", path).CombinedOutput(); err != nil {
		t.Fatalf("set own directory ACL: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := exec.Command("/bin/chmod", "-N", path).CombinedOutput(); err != nil {
			t.Errorf("clear own directory ACL: %v %s", err, out)
		}
	})
	if err := auditPrivateFinalFD(f, true); err == nil {
		t.Fatal("directory grant accepted through actual retained FD")
	}
}
