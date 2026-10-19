//go:build !windows

package secretfile

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestBoundedReadHandleRejectsFIFOWithoutOpeningWriter(t *testing.T) {
	directory := t.TempDir()
	dir, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	path := filepath.Join(directory, "guard")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		file, err := dir.OpenFileForRead("guard")
		if file != nil {
			_ = file.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO was accepted as a regular bounded reader")
		}
	case <-time.After(100 * time.Millisecond):
		// Release the old blocking implementation and join it before failing.
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_ = writer.Close()
		<-done
		t.Fatal("bounded guard open blocked on a FIFO")
	}
}

func TestBoundedReadHandleRejectsSymlinksAndDirectories(t *testing.T) {
	directory := t.TempDir()
	dir, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	if err := os.WriteFile(filepath.Join(directory, "regular"), []byte("guard"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("regular", filepath.Join(directory, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alias", "nested"} {
		file, err := dir.OpenFileForRead(name)
		if file != nil {
			_ = file.Close()
		}
		if err == nil {
			t.Errorf("accepted %s as a regular guard", name)
		}
	}
	file, err := dir.OpenFileForRead("regular")
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
}
