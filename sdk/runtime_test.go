package sdk

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
)

func TestRuntimeExecuteHelp(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Node.ListenAddress = "127.0.0.1:0"
	cfg.Node.AdvertiseAddress = "127.0.0.1:0"
	cfg.Node.BootstrapPeers = []string{}
	cfg.Node.ChatLogDir = t.TempDir()
	cfg.Node.IdentityPath = filepath.Join(cfg.Node.ChatLogDir, "identity.json")
	cfg.Node.TrustStorePath = filepath.Join(cfg.Node.ChatLogDir, "trust.json")
	cfg.Node.QueueStatePath = filepath.Join(cfg.Node.ChatLogDir, "queue.json")
	cfg.Node.PeersStatePath = filepath.Join(cfg.Node.ChatLogDir, "peers.json")

	// SDK no longer auto-generates identity — create one for the test.
	if err := EnsureIdentityFile(cfg.Node.IdentityPath); err != nil {
		t.Fatalf("EnsureIdentityFile() error = %v", err)
	}

	runtime, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	raw, err := runtime.Execute("help", nil)
	if err != nil {
		t.Fatalf("Execute(help) error = %v", err)
	}

	var result struct {
		Commands []CommandInfo `json:"commands"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("unmarshal help result: %v", err)
	}

	if len(result.Commands) == 0 {
		t.Fatal("expected help to return commands")
	}

	hasSendDM := false
	for _, command := range result.Commands {
		if command.Name == "sendDm" {
			hasSendDM = true
			break
		}
	}
	if !hasSendDM {
		t.Fatal("expected SDK runtime to expose sendDm")
	}
}

func TestResolveIdentityFromPrivateKey(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = base64.StdEncoding.EncodeToString(id.PrivateKey)
	cfg.Node.IdentityPath = "" // no file — PrivateKey takes priority

	resolved, err := resolveIdentity(cfg)
	if err != nil {
		t.Fatalf("resolveIdentity: %v", err)
	}
	if resolved.Address != id.Address {
		t.Fatalf("address = %q, want %q", resolved.Address, id.Address)
	}
}

func TestResolveIdentityFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "identity.json")

	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("EnsureIdentityFile: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = ""
	cfg.Node.IdentityPath = path

	_, err := resolveIdentity(cfg)
	if err != nil {
		t.Fatalf("resolveIdentity: %v", err)
	}
}

func TestResolveIdentityFailsWithoutKeyOrFile(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Node.PrivateKey = ""
	cfg.Node.IdentityPath = filepath.Join(t.TempDir(), "nonexistent.json")

	_, err := resolveIdentity(cfg)
	if err == nil {
		t.Fatal("expected error when neither PrivateKey nor identity file provided")
	}
}

func TestEnsureIdentityFileCreatesAndReuses(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "identity.json")

	// First call creates the file.
	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("first EnsureIdentityFile: %v", err)
	}

	first, err := identity.Load(path)
	if err != nil {
		t.Fatalf("Load after create: %v", err)
	}

	// Second call reuses existing file.
	if err := EnsureIdentityFile(path); err != nil {
		t.Fatalf("second EnsureIdentityFile: %v", err)
	}

	second, err := identity.Load(path)
	if err != nil {
		t.Fatalf("Load after reuse: %v", err)
	}

	if first.Address != second.Address {
		t.Fatalf("EnsureIdentityFile regenerated identity: %q vs %q", first.Address, second.Address)
	}
}
