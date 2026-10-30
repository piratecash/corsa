package filetransfer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureWithinDirMissingBaseResolvesExistingAncestor(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(aliasParent, "partial")
	realResolved, err := filepath.EvalSymlinks(realParent)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(realResolved, "partial", "by-ref", "future.part")
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatalf("base must remain absent before containment: %v", err)
	}
	if err := ensureWithinDir(base, target); err != nil {
		t.Fatalf("missing aliased base rejected its canonical child: %v", err)
	}
	for name, outside := range map[string]string{
		"sibling-prefix":   filepath.Join(realResolved, "partial-evil", "future.part"),
		"parent-traversal": filepath.Join(base, "..", "outside", "future.part"),
		"absolute-outside": filepath.Join(realResolved, "..", "outside", "future.part"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ensureWithinDir(base, outside); err == nil {
				t.Fatalf("missing-base containment accepted real escape: %s", outside)
			}
		})
	}
	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Fatalf("containment created the absent base: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("containment created the absent child: %v", err)
	}
}

func TestEnsureWithinDirStillRejectsSymlinkEscapeToMissingChild(t *testing.T) {
	root := t.TempDir()
	base, outside := filepath.Join(root, "contained"), filepath.Join(root, "outside")
	for _, dir := range []string{base, outside} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(base, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(link, "future.part")
	if err := ensureWithinDir(base, target); err == nil {
		t.Fatal("symlink escape was accepted for an absent child")
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("containment created an outside child")
	}
}
