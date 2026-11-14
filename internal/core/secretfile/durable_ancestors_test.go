//go:build !windows

package secretfile

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSyncAncestorsConfirmsOwnedRouteAndPreservesFaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new-parent", "leaf")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := dir.Write("main", []byte("protected")); err != nil {
		t.Fatal(err)
	}
	route, err := resolveRoute(path)
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for _, step := range route.steps {
		if syncsTestAncestor(t, step, route.deepestInfo) {
			want++
		}
	}
	if want < 3 {
		t.Fatal("fixture did not include leaf and newly created ancestors")
	}
	calls := 0
	dir.durableFault = func(step durableStep) error {
		if step == durableAncestorSync {
			calls++
		}
		return nil
	}
	if err := dir.SyncAncestors(path); err != nil {
		t.Fatal(err)
	}
	if calls != want {
		t.Fatalf("ancestor sync checks=%d want=%d", calls, want)
	}
	// Explicitly exercise a symlink owned under our private directory,
	// rather than relying on the host's spelling of its temporary root.
	alias := filepath.Join(filepath.Dir(path), "owned-alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	dir.durableFault = nil
	if err := dir.SyncAncestors(alias); err != nil {
		t.Fatalf("owned alias to the held directory was refused: %v", err)
	}
	for _, at := range []int{1, want} {
		t.Run(map[bool]string{true: "first", false: "last"}[at == 1], func(t *testing.T) {
			injected := errors.New("ancestor synchronization refused")
			calls = 0
			dir.durableFault = func(step durableStep) error {
				if step == durableAncestorSync {
					calls++
					if calls == at {
						return injected
					}
				}
				return nil
			}
			if err := dir.SyncAncestors(path); !errors.Is(err, injected) {
				t.Fatalf("ancestor fault lost: %v", err)
			}
			if calls != at {
				t.Fatalf("continued past failed ancestor: calls=%d want=%d", calls, at)
			}
			raw, err := os.ReadFile(filepath.Join(path, "main"))
			if err != nil || string(raw) != "protected" {
				t.Fatalf("sync-only failure changed target: %q %v", raw, err)
			}
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != 1 || entries[0].Name() != "main" {
				t.Fatalf("sync-only failure mutated namespace: %v %v", entries, err)
			}
		})
	}
}

func TestSyncAncestorsRejectsChangedPinnedNamespace(t *testing.T) {
	for _, change := range []string{"different-leaf", "missing-component", "ancestor-replaced-during-sync", "ancestor-replaced-at-final-bookend"} {
		t.Run(change, func(t *testing.T) {
			base := t.TempDir()
			parent := filepath.Join(base, "parent")
			path := filepath.Join(parent, "leaf")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			dir, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := dir.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := dir.Write("main", []byte("held-original")); err != nil {
				t.Fatal(err)
			}
			requested := path
			moved := filepath.Join(base, "moved")
			if change == "different-leaf" {
				requested = filepath.Join(base, "other")
				if err := os.Mkdir(requested, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if change == "missing-component" {
				requested = filepath.Join(path, "absent")
			}
			changed := false
			replacing := change == "ancestor-replaced-during-sync" || change == "ancestor-replaced-at-final-bookend"
			calls, at := 0, 1
			if change == "ancestor-replaced-at-final-bookend" {
				route, err := resolveRoute(path)
				if err != nil {
					t.Fatal(err)
				}
				at = 0
				for _, step := range route.steps {
					if syncsTestAncestor(t, step, route.deepestInfo) {
						at++
					}
				}
			}
			if replacing {
				dir.durableFault = func(step durableStep) error {
					if step != durableAncestorSync {
						return nil
					}
					calls++
					if calls != at || changed {
						return nil
					}
					changed = true
					if err := os.Rename(parent, moved); err != nil {
						return err
					}
					return os.MkdirAll(path, 0700)
				}
			}
			if err := dir.SyncAncestors(requested); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("changed route not refused: %v", err)
			}
			original := filepath.Join(path, "main")
			if replacing {
				if !changed {
					t.Fatal("namespace mutation fixture did not execute")
				}
				if change == "ancestor-replaced-at-final-bookend" && calls != at {
					t.Fatal("final-bookend fixture did not pass every per-ancestor check first")
				}
				original = filepath.Join(moved, "leaf", "main")
				if _, err := os.Stat(filepath.Join(path, "main")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("replacement namespace acquired original secret")
				}
			}
			raw, err := os.ReadFile(original)
			if err != nil || string(raw) != "held-original" {
				t.Fatalf("original changed: %q %v", raw, err)
			}
		})
	}
}

func syncsTestAncestor(t *testing.T, step routeStep, leaf fs.FileInfo) bool {
	t.Helper()
	if !step.info.IsDir() {
		return false
	}
	if runtime.GOOS != "android" || os.SameFile(step.info, leaf) {
		return true
	}
	owner, group, err := ownerAndGroup(step.info)
	if err != nil {
		t.Fatal(err)
	}
	return !androidManagedAncestor(owner, group, step.info.Mode())
}

func TestAndroidManagedAncestor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		owner, group uint32
		mode         fs.FileMode
		managed      bool
	}{
		{"root", 0, 0, 0o755, true},
		{"system", 1000, 1000, 0o771, true},
		{"app", 10197, 10197, 0o771, false},
		{"foreign writable group", 1000, 2000, 0o771, false},
		{"world writable", 1000, 1000, 0o773, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := androidManagedAncestor(tc.owner, tc.group, tc.mode); got != tc.managed {
				t.Fatalf("managed = %t, want %t", got, tc.managed)
			}
		})
	}
}
