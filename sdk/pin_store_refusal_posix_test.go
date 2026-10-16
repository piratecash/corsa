//go:build !windows

package sdk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// An SDK node whose pin store sits in a directory others may write fails New
// with the pin store refusal — before its RPC server or its node exists — and
// the directory is left as the operator made it.
func TestNewRefusesANodeWhosePinStoreIsInAWorldWritableDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	shared := filepath.Join(dir, "state")
	if err := os.Mkdir(shared, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(shared, 0o777); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(shared, 0o700) })

	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = dir
	cfg.Node.IdentityPath = filepath.Join(dir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(dir, "trust.json")
	// The pin store defaults to the peers file's directory.
	cfg.Node.PeersStatePath = filepath.Join(shared, "peers.json")
	if err := EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		t.Fatalf("EnsureIdentityFile() error = %v", err)
	}

	runtime, err := New(cfg)
	if !errors.Is(err, ErrPinStoreRefused) {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatalf("New() = %v, want ErrPinStoreRefused", err)
	}
	if runtime != nil {
		t.Fatal("New() returned a runtime alongside the refusal")
	}
	if info, err := os.Stat(shared); err != nil || info.Mode().Perm() != 0o777 {
		t.Fatal("the SDK changed the directory's permissions itself")
	}
}
