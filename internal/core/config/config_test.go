package config

import (
	"testing"
)

func TestDefaultPeersStatePath(t *testing.T) {
	t.Parallel()

	path := defaultPeersStatePath(":64646")
	if path != ".corsa/peers-64646.json" {
		t.Fatalf("unexpected default peers path: %s", path)
	}

	path = defaultPeersStatePath(":9999")
	if path != ".corsa/peers-9999.json" {
		t.Fatalf("unexpected default peers path: %s", path)
	}
}

func TestEffectivePeersStatePathUsesDefault(t *testing.T) {
	t.Parallel()

	n := Node{ListenAddress: ":64646"}
	path := n.EffectivePeersStatePath()
	if path != ".corsa/peers-64646.json" {
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

func TestDefaultConfigIncludesPeersAndProxy(t *testing.T) {
	// t.Setenv restores the original value after the test.
	t.Setenv("CORSA_PEERS_PATH", "")
	t.Setenv("CORSA_PROXY", "")
	t.Setenv("CORSA_LISTEN_ADDRESS", "")

	cfg := Default()
	if cfg.Node.PeersStatePath != ".corsa/peers-64646.json" {
		t.Fatalf("unexpected default PeersStatePath: %s", cfg.Node.PeersStatePath)
	}
	if cfg.Node.ProxyAddress != "" {
		t.Fatalf("expected empty ProxyAddress by default, got %s", cfg.Node.ProxyAddress)
	}
}
