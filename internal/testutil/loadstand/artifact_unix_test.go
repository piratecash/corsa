//go:build unix

package loadstand

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestPublicArtifactScannerRejectsFIFOWithoutWaitingForWriter(t *testing.T) {
	for _, holdWriter := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_writer", true: "held_writer"}[holdWriter], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "public.fifo")
			if err := syscall.Mkfifo(path, 0600); err != nil {
				t.Fatal(err)
			}
			// RDWR|NONBLOCK retains a writer without itself blocking on a reader.
			held, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if holdWriter {
				defer func() {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}()
			} else if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- ScanPublicArtifacts(ctx, [][]byte{[]byte("private-material")}, []string{path}) }()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("FIFO treated as clean artifact")
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("scanner waited on unsupported FIFO instead of refusing immediately")
				cancel()
				select {
				case <-result:
				case <-time.After(100 * time.Millisecond):
					t.Fatal("scanner waited on FIFO after context cancellation")
				}
			}
		})
	}
}
func TestArtifactOpenRefusesRegularReplacedByFIFO(t *testing.T) {
	for _, link := range []bool{false, true} {
		t.Run(map[bool]string{false: "fifo", true: "link_to_fifo"}[link], func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "public")
			if err := os.WriteFile(path, []byte("public"), 0600); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			}()
			before, err := root.Lstat("public")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			fifo := path
			if link {
				fifo = filepath.Join(directory, "fifo")
			}
			if err := syscall.Mkfifo(fifo, 0600); err != nil {
				t.Fatal(err)
			}
			if link {
				if err := os.Symlink("fifo", path); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan error, 1)
			go func() {
				file, err := openVerifiedArtifact(t.Context(), root, "public", before)
				if file != nil {
					err = errors.Join(err, file.Close())
				}
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, ErrUnsupportedArtifact) {
					t.Fatalf("replacement not rejected: %v", err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("replacement FIFO blocked open")
			}

		})
	}
}
