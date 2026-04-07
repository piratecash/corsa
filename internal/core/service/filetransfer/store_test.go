package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCopyFileWritesDestinationAndCleansTemp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.txt")
	content := []byte("windows-safe-temp-copy")
	if err := os.WriteFile(srcPath, content, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	dstPath := filepath.Join(dir, "transmit", expectedHash+".txt")
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o700); err != nil {
		t.Fatalf("mkdir transmit dir: %v", err)
	}

	if err := copyFile(srcPath, dstPath, expectedHash); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("destination content = %q, want %q", got, content)
	}

	entries, err := os.ReadDir(filepath.Dir(dstPath))
	if err != nil {
		t.Fatalf("read transmit dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("unexpected temp file left behind: %s", entry.Name())
		}
	}
}

func TestCopyFileRemovesTempOnHashMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.bin")
	if err := os.WriteFile(srcPath, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	dstPath := filepath.Join(dir, "transmit", "dest.bin")
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o700); err != nil {
		t.Fatalf("mkdir transmit dir: %v", err)
	}

	err := copyFile(srcPath, dstPath, strings.Repeat("0", 64))
	if err == nil {
		t.Fatal("copyFile succeeded with mismatched hash, want error")
	}

	if _, statErr := os.Stat(dstPath); !os.IsNotExist(statErr) {
		t.Fatalf("destination should not exist after hash mismatch, stat err = %v", statErr)
	}

	entries, readErr := os.ReadDir(filepath.Dir(dstPath))
	if readErr != nil {
		t.Fatalf("read transmit dir: %v", readErr)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("unexpected temp file left behind after failure: %s", entry.Name())
		}
	}
}

func TestCopyFileCreatesTransmitDirWhenMissing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.png")
	content := []byte("recreate-missing-transmit-dir")
	if err := os.WriteFile(srcPath, content, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	dstPath := filepath.Join(dir, "transmit", expectedHash+".png")

	if err := copyFile(srcPath, dstPath, expectedHash); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("destination content = %q, want %q", got, content)
	}
}
