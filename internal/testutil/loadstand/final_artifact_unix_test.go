//go:build (darwin && cgo) || linux

package loadstand

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func finalArtifactsFixture(t *testing.T) *launchArtifacts {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	a, err := newLaunchArtifacts(t.Context(), dir, launchBinding{nonce: LaunchNonce{1}, fingerprint: LaunchFingerprint{2}})
	if err != nil {
		t.Fatal("private retained-root capability unavailable", err)
	}
	t.Cleanup(func() { _ = a.close() })
	return a
}
func TestFinalArtifactReadsOnlyOriginalRootAndInode(t *testing.T) {
	a := finalArtifactsFixture(t)
	raw := []byte(`{"bounded":"original"}`)
	if _, err := a.writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := a.writer.Close(); err != nil {
		t.Fatal(err)
	}
	a.writer = nil
	got, err := a.read(t.Context())
	if err != nil || string(got) != string(raw) {
		t.Fatal("could not read original slot", string(got), err)
	}
	original := a.root.Name()
	moved := original + "-moved"
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	if err := os.Mkdir(original, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(original, a.name), []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err = a.read(t.Context())
	if err == nil && string(got) != string(raw) {
		t.Fatal("reopened replacement directory")
	}
	if err := a.root.Remove(a.name); err != nil {
		t.Fatal(err)
	}
	if err := a.root.WriteFile(a.name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.read(t.Context()); err == nil {
		t.Fatal("replacement inode accepted")
	}
}
func TestFinalArtifactRefusesModeGrowthAndCancellation(t *testing.T) {
	for _, kind := range []string{"mode", "oversize", "symlink", "directory", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			a := finalArtifactsFixture(t)
			if err := a.writer.Close(); err != nil {
				t.Fatal(err)
			}
			a.writer = nil
			switch kind {
			case "mode":
				if err := a.root.Chmod(a.name, 0644); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := a.root.WriteFile(a.name, make([]byte, int(FinalReportLimit)+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := a.root.Remove(a.name); err != nil {
					t.Fatal(err)
				}
				if err := a.root.Symlink("other", a.name); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := a.root.Remove(a.name); err != nil {
					t.Fatal(err)
				}
				if err := a.root.Mkdir(a.name, 0700); err != nil {
					t.Fatal(err)
				}
			}
			ctx := t.Context()
			if kind == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := a.read(ctx); err == nil {
				t.Fatal("unsafe slot accepted", kind)
			}
		})
	}
	if _, err := newLaunchArtifacts(t.Context(), t.TempDir(), launchBinding{}); !errors.Is(err, ErrUnsupportedArtifact) && !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}

func TestFinalArtifactFIFOReplacementNeverOpensOrBlocks(t *testing.T) {
	a := finalArtifactsFixture(t)
	if err := a.writer.Close(); err != nil {
		t.Fatal(err)
	}
	a.writer = nil
	if err := a.root.Remove(a.name); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(a.root.Name(), a.name), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.read(t.Context()); err == nil {
		t.Fatal("FIFO replacement accepted")
	}
}
func TestFinalSameFDScannersRejectPrivateMaterial(t *testing.T) {
	a := finalArtifactsFixture(t)
	secret := []byte("fixture-private-not-a-user-key")
	if _, err := a.writer.Write(secret); err != nil {
		t.Fatal(err)
	}
	if err := ScanPublicArtifactFD(t.Context(), [][]byte{secret}, a.writer, FinalReportLimit); !errors.Is(err, ErrSecretLeak) {
		t.Fatal("identity FD scan missed private bytes", err)
	}
	if err := a.writer.Truncate(int64(FinalReportLimit) + 1); err != nil {
		t.Fatal(err)
	}
	if err := ScanPublicArtifactFD(t.Context(), nil, a.writer, FinalReportLimit); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatal("FD scan escaped bound", err)
	}
}

func TestFinalArtifactOriginalInodePinLivesUntilConclusiveClose(t *testing.T) {
	a := finalArtifactsFixture(t)
	writer, pin, root := a.writer, a.pin, a.root
	if pin == nil {
		t.Fatal("original inode has no parent capability")
	}
	flags, err := unix.FcntlInt(pin.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("parent inode pin could accidentally cross exec", flags, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	a.writer = nil
	info, err := pin.Stat()
	if err != nil || !os.SameFile(info, a.original) {
		t.Fatal("writer close released original inode pin", err)
	}
	if err := a.root.Remove(a.name); err != nil {
		t.Fatal(err)
	}
	if err := a.root.WriteFile(a.name, []byte("different inode"), 0600); err != nil {
		t.Fatal(err)
	}
	replacement, err := a.root.Lstat(a.name)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(replacement, info) {
		t.Fatal("live original inode capability allowed identifier reuse")
	}
	if _, err := a.read(t.Context()); err == nil {
		t.Fatal("replacement path accepted while original pinned")
	}
	if err := a.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pin.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("parent pin leaked after conclusive close", err)
	}
	if _, err := root.Open("."); !errors.Is(err, os.ErrClosed) {
		t.Fatal("retained root leaked after close", err)
	}
	if err := a.close(); err != nil {
		t.Fatal("capability cleanup not idempotent", err)
	}
}
