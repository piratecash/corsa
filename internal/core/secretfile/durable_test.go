//go:build !windows

package secretfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableWriteReportsTheVisibilityBoundary(t *testing.T) {
	for _, step := range []durableStep{durablePreflight, durableCreate, durableWrite, durableFileSync, durableFileClose, durableRename, durableDirectorySync} {
		t.Run(string(step), func(t *testing.T) {
			path := t.TempDir()
			dir, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dir.Close() }()
			if err := dir.Write("main", []byte("old")); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected durable failure")
			dir.durableFault = func(at durableStep) error {
				if at == step {
					return injected
				}
				return nil
			}
			outcome, err := dir.DurableWrite("main", []byte("new"))
			if !errors.Is(err, injected) {
				t.Fatalf("failure point not reached: %v", err)
			}
			want := DurableNotCommitted
			visible := "old"
			if step == durableDirectorySync {
				want, visible = DurableIndeterminate, "new"
			}
			if outcome != want {
				t.Fatalf("outcome=%v want=%v", outcome, want)
			}
			raw, err := os.ReadFile(filepath.Join(path, "main"))
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != visible {
				t.Fatalf("visible=%q want=%q", raw, visible)
			}
			if step == durablePreflight {
				entries, err := os.ReadDir(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 1 {
					t.Fatal("unsupported preflight mutated directory")
				}
			}
		})
	}
}

func TestDurableRemoveReportsPostUnlinkUnknown(t *testing.T) {
	path := t.TempDir()
	dir, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	if outcome, err := dir.DurableWrite("guard", []byte("protected")); err != nil || outcome != DurableCommitted {
		t.Fatalf("write %v %v", outcome, err)
	}
	injected := errors.New("directory sync failure")
	dir.durableFault = func(step durableStep) error {
		if step == durableDirectorySync {
			return injected
		}
		return nil
	}
	outcome, err := dir.DurableRemove("guard")
	if outcome != DurableIndeterminate || !errors.Is(err, injected) {
		t.Fatalf("remove %v %v", outcome, err)
	}
	if _, err := os.Stat(filepath.Join(path, "guard")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unlink did not become visible: %v", err)
	}
}

func TestDurableOperationsStayRelativeToThePinnedDirectory(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "original")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	moved := filepath.Join(parent, "moved")
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if outcome, err := dir.DurableWrite("guard", []byte("protected")); err != nil || outcome != DurableCommitted {
		t.Fatalf("write %v %v", outcome, err)
	}
	if _, err := os.Stat(filepath.Join(path, "guard")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("writer followed replacement pathname")
	}
	if outcome, err := dir.DurableRemove("guard"); err != nil || outcome != DurableCommitted {
		t.Fatalf("remove %v %v", outcome, err)
	}
	if _, err := os.Stat(filepath.Join(moved, "guard")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("remove did not use pinned directory")
	}
}
