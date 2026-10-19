//go:build !windows

package node

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestK1InitialTrustReadRejectsFIFO(t *testing.T) {
	const key = "CORSA_TEST_K1_TRUST_FIFO"
	if path := os.Getenv(key); path != "" {
		if _, err := loadTrustStore(path, trustedContact{}); err == nil {
			t.Fatal("FIFO accepted as trust JSON")
		}
		return
	}
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestK1InitialTrustReadRejectsFIFO$")
	command.Env = append(os.Environ(), key+"="+path)
	output, err := command.CombinedOutput()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("initial trust read blocked opening FIFO before authority audit")
	}
	if err != nil {
		t.Fatalf("FIFO child failed: %v: %s", err, output)
	}
}

func TestK1InitialTrustReadKeepsLegacySymlinkAndRejectsDirectory(t *testing.T) {
	directory := t.TempDir()
	regular := filepath.Join(directory, "regular")
	raw := []byte(`{"version":3,"contacts":{}}`)
	if err := os.WriteFile(regular, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(directory, "alias")
	if err := os.Symlink(regular, alias); err != nil {
		t.Fatal(err)
	}
	read, err := readInitialTrustFile(alias)
	if err != nil || string(read) != string(raw) {
		t.Fatalf("legacy symlink read: %s, %v", read, err)
	}
	if _, err := readInitialTrustFile(directory); err == nil {
		t.Fatal("directory accepted")
	}
}
