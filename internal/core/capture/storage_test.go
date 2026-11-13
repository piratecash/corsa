package capture

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureStorageAccountsForOldFilesAndConcurrentWriters(t *testing.T) {
	base := t.TempDir()
	oldDir := filepath.Join(base, "old-session")
	if err := os.Mkdir(oldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(oldDir, "old.jsonl")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	storage := &captureStorage{baseDir: base, free: func(string) (uint64, error) { return 1 << 40, nil }}
	open := func(group string) Writer {
		t.Helper()
		w, err := storage.create(filepath.Join(base, group, "capture.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	first, second := open("first"), open("second")
	if storage.stored.bytes != 3 || storage.stored.files != 3 || len(storage.stored.dirs) != 3 {
		t.Fatalf("old file or new files not counted: %+v", storage.stored)
	}
	// Leave two bytes for two active writers to compete for without filling
	// the real filesystem to the production quota.
	storage.stored.bytes = maxCaptureStoredBytes - 2
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, w := range []Writer{first, second} {
		go func() {
			<-start
			_, err := w.Write([]byte("ab"))
			results <- err
		}()
	}
	close(start)
	var stored, refused int
	for range 2 {
		switch err := <-results; {
		case err == nil:
			stored++
		case errors.Is(err, errCaptureStorageLimit):
			refused++
		default:
			t.Fatalf("unexpected write outcome: %v", err)
		}
	}
	if stored != 1 || refused != 1 || storage.stored.bytes != maxCaptureStoredBytes {
		t.Fatalf("shared budget: stored %d, refused %d, used %d", stored, refused, storage.stored.bytes)
	}
	refusedDir := filepath.Join(base, "refused")
	if w, err := storage.create(filepath.Join(refusedDir, "capture.jsonl")); !errors.Is(err, errCaptureStorageLimit) {
		if w != nil {
			_ = w.Close()
		}
		t.Fatalf("full directory unexpectedly admitted a writer: %v", err)
	}
	if _, err := os.Lstat(refusedDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refusal created a session directory: %v", err)
	}
	if err := errors.Join(first.Close(), second.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(oldPath); err != nil {
		t.Fatal(err)
	}
	open("after-archive")
	if storage.stored.bytes != 2 || storage.stored.files != 3 {
		t.Fatalf("idle refresh did not account for remaining files: %+v", storage.stored)
	}
}

func TestCaptureStorageFreeSpaceReserve(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "captures")
	dir := filepath.Join(base, "session")
	path := filepath.Join(dir, "capture.jsonl")
	available := uint64(minCaptureFreeBytes + captureCreateHeadroom - 1)
	var probed string
	var probeErr error
	storage := &captureStorage{
		baseDir: base,
		free: func(dir string) (uint64, error) {
			probed = dir
			return available, probeErr
		},
	}
	if w, err := storage.create(path); !errors.Is(err, errCaptureFreeSpace) {
		if w != nil {
			_ = w.Close()
		}
		t.Fatalf("low disk admitted a capture: %v", err)
	}
	if probed != parent {
		t.Fatalf("probe did not use nearest existing parent: %s", probed)
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("free space refusal created a directory: %v", err)
	}
	available++
	w, err := storage.create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	available = minCaptureFreeBytes + 5
	if err := storage.reserve(dir, 3); err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write([]byte("abc")); n != 0 || !errors.Is(err, errCaptureFreeSpace) {
		t.Fatalf("write ignored an in-flight reservation: %d, %v", n, err)
	}
	storage.finish(3, 0)
	if n, err := w.Write([]byte("12345")); n != 5 || err != nil || probed != dir {
		t.Fatalf("write at exact reserve boundary: %d, %v, probe %s", n, err, probed)
	}
	probeErr = os.ErrPermission
	if n, err := w.Write([]byte("x")); n != 0 || !errors.Is(err, probeErr) {
		t.Fatalf("unavailable measurement admitted a write: %d, %v", n, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "12345" {
		t.Fatalf("refused writes changed the file: %q, %v", data, err)
	}
	if storage.inFlight != 0 || storage.stored.bytes != 5 {
		t.Fatalf("incorrect final reservations: in-flight %d, stored %d", storage.inFlight, storage.stored.bytes)
	}
}
