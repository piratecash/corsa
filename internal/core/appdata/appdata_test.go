package appdata

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGoTestDetectionCoversWindowsBinaries(t *testing.T) {
	// Windows builds the test binary as "<pkg>.test.exe". A plain ".test"
	// suffix check reports false there, so DefaultDir resolves to the real
	// %AppData%\CorsaCore — and any test that cleans up after itself would be
	// cleaning up the user's identity, state database and message history.
	original := os.Args[0]
	t.Cleanup(func() { os.Args[0] = original })

	for name, want := range map[string]bool{
		"desktop.test":      true,
		"desktop.test.exe":  true,
		"corsa-desktop":     false,
		"corsa-desktop.exe": false,
	} {
		os.Args[0] = filepath.Join("some", "dir", name)
		if got := RunningUnderGoTest(); got != want {
			t.Fatalf("RunningUnderGoTest() = %t for %q, want %t", got, name, want)
		}
	}
}

// anchorTestEnv isolates one Anchor call: the override is process-global, so
// every test that may set it restores the unset state, and CORSA_DATA_DIR is
// pinned to the value the test wants (empty = not configured).
func anchorTestEnv(t *testing.T, dataDir string) {
	t.Helper()
	t.Setenv(DataDirEnv, dataDir)
	t.Cleanup(func() { baseDirOverride = "" })
}

func TestAnchorUsesConfiguredDataDir(t *testing.T) {
	dir := t.TempDir()
	anchorTestEnv(t, dir)

	if err := Anchor(); err != nil {
		t.Fatalf("Anchor() = %v, want nil for an existing directory", err)
	}
	if got := DefaultDir(); got != dir {
		t.Fatalf("DefaultDir() = %q after Anchor, want %q", got, dir)
	}
}

func TestAnchorResolvesRelativeDataDir(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	if err := os.Mkdir("vault", 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	anchorTestEnv(t, "vault")

	if err := Anchor(); err != nil {
		t.Fatalf("Anchor() = %v, want nil", err)
	}
	// A relative path would follow every later chdir; the anchor must pin
	// the directory the user meant at startup.
	want, err := filepath.Abs("vault")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	if got := DefaultDir(); got != want {
		t.Fatalf("DefaultDir() = %q, want absolute %q", got, want)
	}
}

// TestAnchorRefusesMissingDataDir is the unmounted-container case: the
// configured directory lives inside an encrypted volume that is not mounted
// right now. Creating it would put a fresh identity and history on the plain
// disk — exactly what the user moved them away from.
func TestAnchorRefusesMissingDataDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "unmounted-volume", "corsa")
	anchorTestEnv(t, missing)

	err := Anchor()
	if !errors.Is(err, ErrDataDirMissing) {
		t.Fatalf("Anchor() = %v, want ErrDataDirMissing", err)
	}
	if _, statErr := os.Stat(missing); !os.IsNotExist(statErr) {
		t.Fatalf("Anchor created the missing data dir (stat err = %v); it must never create a configured dir", statErr)
	}
	if got := DefaultDir(); got == missing {
		t.Fatal("DefaultDir() points at the refused dir; a failed Anchor must not override anything")
	}
}

func TestAnchorRefusesDataDirThatIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	anchorTestEnv(t, file)

	if err := Anchor(); !errors.Is(err, ErrDataDirNotDirectory) {
		t.Fatalf("Anchor() = %v, want ErrDataDirNotDirectory", err)
	}
}

// TestAnchorRefusesConfiguredDanglingLink: the configured path is itself a
// link into the unmounted volume — still "missing", never created.
func TestAnchorRefusesConfiguredDanglingLink(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, "corsa")
	symlinkOrSkip(t, filepath.Join(root, "unmounted", "corsa"), link)
	anchorTestEnv(t, link)

	if err := Anchor(); !errors.Is(err, ErrDataDirMissing) {
		t.Fatalf("Anchor() = %v, want ErrDataDirMissing", err)
	}
}

// TestAnchorRefusesBrokenDefaultLink is the symlink recipe: the platform
// default dir was replaced by a link into an encrypted volume, and the volume
// is not mounted. Starting anyway would fail later with an opaque mkdir
// error at best — the anchor names the cause up front.
func TestAnchorRefusesBrokenDefaultLink(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	anchorTestEnv(t, "")
	symlinkOrSkip(t, filepath.Join(root, "unmounted", "corsa"), DefaultDir())

	if err := Anchor(); !errors.Is(err, ErrDataDirLinkBroken) {
		t.Fatalf("Anchor() = %v, want ErrDataDirLinkBroken", err)
	}
}

func TestAnchorAcceptsWorkingDefaultLink(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	anchorTestEnv(t, "")
	target := filepath.Join(root, "mounted", "corsa")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	symlinkOrSkip(t, target, DefaultDir())

	if err := Anchor(); err != nil {
		t.Fatalf("Anchor() = %v, want nil for a link to a mounted volume", err)
	}
}

// TestAnchorAcceptsAbsentDefaultDir: a first start with nothing configured
// keeps creating the platform default on demand, as before.
func TestAnchorAcceptsAbsentDefaultDir(t *testing.T) {
	t.Chdir(t.TempDir())
	anchorTestEnv(t, "")

	if err := Anchor(); err != nil {
		t.Fatalf("Anchor() = %v, want nil on a first start", err)
	}
}

func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		// Windows without Developer Mode or elevation cannot create
		// symlinks; the logic under test is platform-neutral.
		t.Skipf("cannot create symlink here: %v", err)
	}
}
