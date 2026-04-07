package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/appdata"
)

func expectedDefaultDataDir(t *testing.T) string {
	t.Helper()
	return appdata.DefaultDir()
}

func TestDefaultPeersStatePath(t *testing.T) {
	t.Parallel()

	path := defaultPeersStatePath(":64646")
	if path != filepath.Join(expectedDefaultDataDir(t), "peers-64646.json") {
		t.Fatalf("unexpected default peers path: %s", path)
	}

	path = defaultPeersStatePath(":9999")
	if path != filepath.Join(expectedDefaultDataDir(t), "peers-9999.json") {
		t.Fatalf("unexpected default peers path: %s", path)
	}
}

func TestEffectivePeersStatePathUsesDefault(t *testing.T) {
	t.Parallel()

	n := Node{ListenAddress: ":64646"}
	path := n.EffectivePeersStatePath()
	if path != filepath.Join(expectedDefaultDataDir(t), "peers-64646.json") {
		t.Fatalf("unexpected effective peers path: %s", path)
	}
}

func TestEffectivePeersStatePathUsesOverride(t *testing.T) {
	t.Parallel()

	n := Node{
		ListenAddress:  ":64646",
		PeersStatePath: "/custom/peers.json",
	}
	path := n.EffectivePeersStatePath()
	if path != "/custom/peers.json" {
		t.Fatalf("unexpected effective peers path: %s", path)
	}
}

// Environment variable tests use t.Setenv which automatically restores
// the original value after the test.  These tests intentionally do NOT
// call t.Parallel (t.Setenv panics in parallel tests) to keep the
// process-wide env mutations hermetic.

func TestPeersPathFromEnv(t *testing.T) {
	t.Setenv("CORSA_PEERS_PATH", "/tmp/test-peers.json")

	got := envOrDefault("CORSA_PEERS_PATH", "fallback")
	if got != "/tmp/test-peers.json" {
		t.Fatalf("expected env value, got %s", got)
	}
}

func TestProxyAddressFromEnv(t *testing.T) {
	t.Setenv("CORSA_PROXY", "127.0.0.1:9050")

	got := envOrDefault("CORSA_PROXY", "")
	if got != "127.0.0.1:9050" {
		t.Fatalf("expected proxy from env, got %s", got)
	}
}

func TestProxyAddressDefaultEmpty(t *testing.T) {
	t.Setenv("CORSA_PROXY", "")

	got := envOrDefault("CORSA_PROXY", "")
	if got != "" {
		t.Fatalf("expected empty proxy, got %s", got)
	}
}

func TestDefaultChatLogDir(t *testing.T) {
	t.Parallel()

	dir := defaultChatLogDir()
	if dir != expectedDefaultDataDir(t) {
		t.Fatalf("unexpected default chatlog dir: %s", dir)
	}
}

func TestEffectiveChatLogDirUsesDefault(t *testing.T) {
	t.Parallel()

	n := Node{ListenAddress: ":64646"}
	dir := n.EffectiveChatLogDir()
	if dir != expectedDefaultDataDir(t) {
		t.Fatalf("unexpected effective chatlog dir: %s", dir)
	}
}

func TestEffectiveChatLogDirUsesOverride(t *testing.T) {
	t.Parallel()

	n := Node{
		ListenAddress: ":64646",
		ChatLogDir:    "/custom/chatlogs",
	}
	dir := n.EffectiveChatLogDir()
	if dir != "/custom/chatlogs" {
		t.Fatalf("unexpected effective chatlog dir: %s", dir)
	}
}

func TestChatLogDirFromEnv(t *testing.T) {
	t.Setenv("CORSA_CHATLOG_DIR", "/tmp/test-chatlogs")

	got := envOrDefault("CORSA_CHATLOG_DIR", "fallback")
	if got != "/tmp/test-chatlogs" {
		t.Fatalf("expected env value, got %s", got)
	}
}

func TestDefaultConfigIncludesPeersAndProxy(t *testing.T) {
	// t.Setenv restores the original value after the test.
	t.Setenv("CORSA_PEERS_PATH", "")
	t.Setenv("CORSA_PROXY", "")
	t.Setenv("CORSA_LISTEN_ADDRESS", "")
	t.Setenv("CORSA_CHATLOG_DIR", "")

	cfg := Default()
	wantPeers, err := filepath.Abs(filepath.Join(expectedDefaultDataDir(t), "peers-64646.json"))
	if err != nil {
		t.Fatalf("filepath.Abs peers: %v", err)
	}
	if cfg.Node.PeersStatePath != wantPeers {
		t.Fatalf("unexpected default PeersStatePath: %s", cfg.Node.PeersStatePath)
	}
	if cfg.Node.ProxyAddress != "" {
		t.Fatalf("expected empty ProxyAddress by default, got %s", cfg.Node.ProxyAddress)
	}
	wantChatlog, err := filepath.Abs(expectedDefaultDataDir(t))
	if err != nil {
		t.Fatalf("filepath.Abs chatlog: %v", err)
	}
	if cfg.Node.ChatLogDir != wantChatlog {
		t.Fatalf("unexpected default ChatLogDir: %s", cfg.Node.ChatLogDir)
	}
}

func TestDefaultAnchorsPathsToStartupDirectory(t *testing.T) {
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}

	baseDir := t.TempDir()
	if err := os.Chdir(baseDir); err != nil {
		t.Fatalf("chdir baseDir: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWD)
	}()

	t.Setenv("CORSA_PEERS_PATH", "")
	t.Setenv("CORSA_QUEUE_STATE_PATH", "")
	t.Setenv("CORSA_TRUST_STORE_PATH", "")
	t.Setenv("CORSA_IDENTITY_PATH", "")
	t.Setenv("CORSA_CHATLOG_DIR", "")
	t.Setenv("CORSA_DOWNLOAD_DIR", "")
	t.Setenv("CORSA_LISTEN_ADDRESS", "")

	cfg := Default()
	dataDir, err := filepath.Abs(expectedDefaultDataDir(t))
	if err != nil {
		t.Fatalf("filepath.Abs dataDir: %v", err)
	}

	otherDir := t.TempDir()
	if err := os.Chdir(otherDir); err != nil {
		t.Fatalf("chdir otherDir: %v", err)
	}

	if got, want := cfg.Node.ChatLogDir, dataDir; got != want {
		t.Fatalf("ChatLogDir = %q, want %q", got, want)
	}
	if got, want := cfg.Node.PeersStatePath, filepath.Join(dataDir, "peers-64646.json"); got != want {
		t.Fatalf("PeersStatePath = %q, want %q", got, want)
	}
	if got, want := cfg.Node.QueueStatePath, filepath.Join(dataDir, "queue-64646.json"); got != want {
		t.Fatalf("QueueStatePath = %q, want %q", got, want)
	}
	if got, want := cfg.Node.IdentityPath, filepath.Join(dataDir, "identity-64646.json"); got != want {
		t.Fatalf("IdentityPath = %q, want %q", got, want)
	}
	if got, want := cfg.Node.TrustStorePath, filepath.Join(dataDir, "trust-64646.json"); got != want {
		t.Fatalf("TrustStorePath = %q, want %q", got, want)
	}
}
