package desktop

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// fakeStoreFile returns a storeFile function that records calls and returns
// a deterministic hash based on file content (real SHA-256), plus a matching
// storedFileSize function that tracks the size of the last stored content.
func fakeStoreFile(t *testing.T) (
	storeFile func(string) (string, error),
	storedFileSize func(string) (uint64, error),
	calls *[]string,
) {
	t.Helper()
	var recorded []string
	stored := make(map[string]uint64) // hash → size
	store := func(srcPath string) (string, error) {
		recorded = append(recorded, srcPath)
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return "", err
		}
		h := sha256.Sum256(data)
		hash := hex.EncodeToString(h[:])
		stored[hash] = uint64(len(data))
		return hash, nil
	}
	sizeFunc := func(hash string) (uint64, error) {
		size, ok := stored[hash]
		if !ok {
			return 0, fmt.Errorf("hash %s not found in fake store", hash)
		}
		return size, nil
	}
	return store, sizeFunc, &recorded
}

// noopRemoveOrphan is a removeOrphan callback for tests that don't
// exercise the orphan-cleanup path.
func noopRemoveOrphan(string) {}

func TestPrepareFileForTransmitRoundTrip(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	content := []byte("round-trip-content-for-transmit")
	srcPath := filepath.Join(srcDir, "document.pdf")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	store, sizeFunc, calls := fakeStoreFile(t)
	result, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, srcPath)
	if err != nil {
		t.Fatalf("prepareFileForTransmit: %v", err)
	}

	if result.FileName != "document.pdf" {
		t.Errorf("FileName = %q, want document.pdf", result.FileName)
	}
	if result.FileSize != uint64(len(content)) {
		t.Errorf("FileSize = %d, want %d", result.FileSize, len(content))
	}

	h := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(h[:])
	if result.FileHash != expectedHash {
		t.Errorf("FileHash = %q, want %q", result.FileHash, expectedHash)
	}
	if result.ContentType != "application/pdf" {
		t.Errorf("ContentType = %q, want application/pdf", result.ContentType)
	}

	// storeFile must have been called exactly once with the source path.
	if len(*calls) != 1 || (*calls)[0] != srcPath {
		t.Fatalf("storeFile calls = %v, want [%s]", *calls, srcPath)
	}
}

func TestPrepareFileForTransmitDedupSameHash(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	content := []byte("same-content-different-names")

	src1 := filepath.Join(srcDir, "a.txt")
	src2 := filepath.Join(srcDir, "b.bin")
	if err := os.WriteFile(src1, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src2, content, 0o644); err != nil {
		t.Fatal(err)
	}

	store, sizeFunc, _ := fakeStoreFile(t)

	r1, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, src1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, src2)
	if err != nil {
		t.Fatal(err)
	}

	if r1.FileHash != r2.FileHash {
		t.Fatal("same content should produce same hash")
	}
	// FileName and ContentType differ — that's correct, they come from the source.
	if r1.FileName == r2.FileName {
		t.Fatal("FileName should differ for different source files")
	}
}

func TestPrepareFileForTransmitRejectsDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	store, sizeFunc, _ := fakeStoreFile(t)
	_, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, dir)
	if err == nil {
		t.Fatal("expected error for directory")
	}
}

func TestPrepareFileForTransmitRejectsEmptyFile(t *testing.T) {
	t.Parallel()
	srcDir := t.TempDir()
	empty := filepath.Join(srcDir, "empty.txt")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	store, sizeFunc, _ := fakeStoreFile(t)
	_, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, empty)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestPrepareFileForTransmitStoreError(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "file.txt")
	if err := os.WriteFile(srcPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	failStore := func(string) (string, error) {
		return "", fmt.Errorf("disk full")
	}
	noopSize := func(string) (uint64, error) { return 0, nil }

	_, err := prepareFileForTransmit(failStore, noopSize, noopRemoveOrphan, srcPath)
	if err == nil {
		t.Fatal("expected error when storeFile fails")
	}
}

// TestPrepareFileForTransmitSizeFromStore verifies that FileSize in the result
// comes from the stored blob, not from the original os.Stat. This matters when
// the source file changes between Stat and storeFile — the announce must
// describe the exact bytes the receiver will get.
func TestPrepareFileForTransmitSizeFromStore(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "mutable.bin")

	// Write initial content — os.Stat will see this size.
	if err := os.WriteFile(srcPath, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}

	// storeFile replaces the content mid-flight (simulating a concurrent write)
	// and hashes/stores the new content. storedFileSize returns the new size.
	storedSize := uint64(100)
	fakeStore := func(path string) (string, error) {
		return "abc123", nil
	}
	fakeSize := func(hash string) (uint64, error) {
		return storedSize, nil
	}

	result, err := prepareFileForTransmit(fakeStore, fakeSize, noopRemoveOrphan, srcPath)
	if err != nil {
		t.Fatal(err)
	}

	// FileSize must come from storedFileSize (100), not from os.Stat (5).
	if result.FileSize != storedSize {
		t.Errorf("FileSize = %d, want %d (from stored blob, not os.Stat)", result.FileSize, storedSize)
	}
}

// TestPrepareFileForTransmitOrphanCleanupOnSizeError verifies that when
// storeFile succeeds but storedFileSize fails, the removeOrphan callback
// is invoked with the stored hash so the orphaned blob does not linger
// in the transmit store with refs=0 and pending=0.
func TestPrepareFileForTransmitOrphanCleanupOnSizeError(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "payload.bin")
	if err := os.WriteFile(srcPath, []byte("orphan-test"), 0o644); err != nil {
		t.Fatal(err)
	}

	const storedHash = "deadbeef1234"
	storeOK := func(string) (string, error) { return storedHash, nil }
	sizeFail := func(string) (uint64, error) { return 0, fmt.Errorf("stat blob: i/o error") }

	var removedHash string
	trackRemove := func(hash string) { removedHash = hash }

	_, err := prepareFileForTransmit(storeOK, sizeFail, trackRemove, srcPath)
	if err == nil {
		t.Fatal("expected error when storedFileSize fails")
	}
	if removedHash != storedHash {
		t.Fatalf("removeOrphan called with %q, want %q", removedHash, storedHash)
	}
}

// TestPrepareFileForTransmitNoOrphanOnStoreError verifies that removeOrphan
// is NOT called when storeFile itself fails — there is no blob to clean up.
func TestPrepareFileForTransmitNoOrphanOnStoreError(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "file.txt")
	if err := os.WriteFile(srcPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	storeFail := func(string) (string, error) { return "", fmt.Errorf("disk full") }
	noopSize := func(string) (uint64, error) { return 0, nil }

	removed := false
	trackRemove := func(string) { removed = true }

	_, err := prepareFileForTransmit(storeFail, noopSize, trackRemove, srcPath)
	if err == nil {
		t.Fatal("expected error when storeFile fails")
	}
	if removed {
		t.Fatal("removeOrphan must not be called when storeFile fails — no blob exists")
	}
}

func TestPrepareFileForTransmitUnknownExtension(t *testing.T) {
	t.Parallel()

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "data.xyz123")
	if err := os.WriteFile(srcPath, []byte("binary"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, sizeFunc, _ := fakeStoreFile(t)
	result, err := prepareFileForTransmit(store, sizeFunc, noopRemoveOrphan, srcPath)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContentType != "application/octet-stream" {
		t.Errorf("ContentType = %q, want application/octet-stream", result.ContentType)
	}
}
