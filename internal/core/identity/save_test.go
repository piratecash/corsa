package identity

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/testutil/fsprobe"
)

// save_test.go covers the way the identity file reaches disk. The file holds
// both private keys, so how it is written is as much a security property as
// what it contains.

// TestSaveIgnoresPredictableTempFile: the identity file must come out 0600
// even when a file already sits at the old predictable temp name.
//
// The write used to go through "<path>.tmp" with os.WriteFile, whose mode
// argument applies ONLY on creation — an existing 0644 file there kept its own
// terms and handed them to the identity file through the rename. The shared
// secret writer creates a unique temp instead, so the name below is now
// irrelevant, and the assertion is that it stays irrelevant.
func TestSaveIgnoresPredictableTempFile(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("this assertion is about POSIX mode bits; the NTFS statement of the same property is a DACL, asserted in internal/core/secretfile")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	if err := os.WriteFile(path+".tmp", []byte("planted"), 0o644); err != nil {
		t.Fatalf("plant temp: %v", err)
	}

	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if err := Save(path, id); err != nil {
		t.Fatalf("save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat identity file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("identity file permissions = %o, want 0600 — a planted temp file dictated them", perm)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("the saved identity does not load back: %v", err)
	}
}

// TestSaveDoesNotFollowPredictableTempSymlink: a symlink at the old temp name
// used to receive both private keys, because os.WriteFile follows links.
// Nothing may be written through it now.
func TestSaveDoesNotFollowPredictableTempSymlink(t *testing.T) {
	t.Parallel()
	// Not "skip on Windows": whether links can be created there depends on
	// privilege, not on the OS, so an elevated shell runs this for real.
	fsprobe.RequireSymlinks(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("original"), 0o600); err != nil {
		t.Fatalf("create victim: %v", err)
	}
	if err := os.Symlink(victim, path+".tmp"); err != nil {
		t.Fatalf("symlink temp: %v", err)
	}

	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if err := Save(path, id); err != nil {
		t.Fatalf("save: %v", err)
	}

	contents, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read victim: %v", err)
	}
	if string(contents) != "original" {
		t.Fatal("the identity write went through a symlink at the predictable temp name")
	}
	for encoding, value := range map[string]string{
		"raw":    string(id.PrivateKey),
		"base64": base64.StdEncoding.EncodeToString(id.PrivateKey),
		"hex":    hex.EncodeToString(id.PrivateKey),
	} {
		if strings.Contains(string(contents), value) {
			t.Fatalf("the private key reached the symlink target (%s)", encoding)
		}
	}
	// And the identity itself must still have been written correctly.
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load saved identity: %v", err)
	}
	if loaded.Address != id.Address {
		t.Fatalf("saved address = %s, want %s", loaded.Address, id.Address)
	}
}

// TestSaveLeavesNoPredictableTemp: the temp is unique, so a caller cannot
// guess it, and it must not survive a successful write either.
func TestSaveLeavesNoPredictableTemp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	id, err := Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if err := Save(path, id); err != nil {
		t.Fatalf("save: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "identity.json" {
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("directory holds %v, want only the identity file", names)
	}
}
