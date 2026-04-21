package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/domain"
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

// TestAdvertisePortFromEnv covers the full CORSA_ADVERTISE_PORT parse
// matrix required by the advertise-address phase 1 deprecation contract.
// Only a string integer in the inclusive range 1..65535 produces a
// non-nil *domain.PeerPort — every other input (empty, non-numeric, out
// of range, negative, zero, whitespace-only) returns nil so the caller
// falls back to config.DefaultPeerPort through EffectiveAdvertisePort.
// The return type is a pointer so "operator did not configure this" is
// modelled explicitly at the type level and a stray zero never leaks
// through as a silent "valid port" signal.
func TestAdvertisePortFromEnv(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantNil bool
		want    domain.PeerPort
	}{
		{name: "empty_returns_nil", raw: "", wantNil: true},
		{name: "whitespace_only_returns_nil", raw: "   ", wantNil: true},
		{name: "non_numeric_returns_nil", raw: "abc", wantNil: true},
		{name: "numeric_zero_returns_nil", raw: "0", wantNil: true},
		{name: "negative_returns_nil", raw: "-1", wantNil: true},
		{name: "above_upper_bound_returns_nil", raw: "65536", wantNil: true},
		{name: "large_returns_nil", raw: "1000000", wantNil: true},
		{name: "fractional_returns_nil", raw: "64646.5", wantNil: true},
		{name: "low_bound_accepted", raw: "1", want: 1},
		{name: "default_accepted", raw: "64646", want: 64646},
		{name: "upper_bound_accepted", raw: "65535", want: 65535},
		{name: "leading_whitespace_trimmed", raw: "  64646  ", want: 64646},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_ADVERTISE_PORT", tc.raw)
			got := advertisePortFromEnv()
			if tc.wantNil {
				if got != nil {
					t.Fatalf("advertisePortFromEnv(%q) = &%d, want nil", tc.raw, *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("advertisePortFromEnv(%q) = nil, want &%d", tc.raw, tc.want)
			}
			if *got != tc.want {
				t.Fatalf("advertisePortFromEnv(%q) = &%d, want &%d", tc.raw, *got, tc.want)
			}
			if !got.IsValid() {
				t.Fatalf("advertisePortFromEnv(%q) returned non-IsValid PeerPort %d — parser must only surface values in 1..65535", tc.raw, *got)
			}
		})
	}
}

// TestNodeEffectiveAdvertisePort asserts the fallback cascade: a nil or
// invalid AdvertisePort collapses to the PeerPort form of DefaultPeerPort,
// a valid pointer is returned verbatim. The EffectiveAdvertisePort result
// flows into both hello.advertise_port on the wire and JoinHostPort on
// the self-advertise endpoint, so the single-source fallback rule is
// enforced here rather than fanning out to every call site.
func TestNodeEffectiveAdvertisePort(t *testing.T) {
	t.Parallel()

	defaultValue, err := parseDefaultPeerPort()
	if err != nil {
		t.Fatalf("parse DefaultPeerPort %q: %v", DefaultPeerPort, err)
	}

	t.Run("nil_falls_back_to_default", func(t *testing.T) {
		t.Parallel()
		n := Node{}
		if got := n.EffectiveAdvertisePort(); got != defaultValue {
			t.Fatalf("EffectiveAdvertisePort with nil AdvertisePort: got %d want %d", got, defaultValue)
		}
	})

	t.Run("valid_pointer_wins", func(t *testing.T) {
		t.Parallel()
		value := domain.PeerPort(55555)
		n := Node{AdvertisePort: &value}
		if got := n.EffectiveAdvertisePort(); got != 55555 {
			t.Fatalf("EffectiveAdvertisePort with explicit value: got %d want 55555", got)
		}
	})

	t.Run("invalid_pointer_falls_back_to_default", func(t *testing.T) {
		t.Parallel()
		// An invalid value at the pointer is still treated as "not
		// configured" so the fallback is consistent with env-parse
		// behaviour (which would never surface such a pointer in the
		// first place, but a synthetic Node in a test or a manually
		// constructed Config could).
		value := domain.PeerPort(0)
		n := Node{AdvertisePort: &value}
		if got := n.EffectiveAdvertisePort(); got != defaultValue {
			t.Fatalf("EffectiveAdvertisePort with invalid pointer: got %d want %d", got, defaultValue)
		}
	})

	t.Run("out_of_range_pointer_falls_back_to_default", func(t *testing.T) {
		t.Parallel()
		value := domain.PeerPort(70000)
		n := Node{AdvertisePort: &value}
		if got := n.EffectiveAdvertisePort(); got != defaultValue {
			t.Fatalf("EffectiveAdvertisePort with out-of-range pointer: got %d want %d", got, defaultValue)
		}
	})
}

// parseDefaultPeerPort re-parses the string DefaultPeerPort constant
// into its PeerPort form via the same strconv.Atoi path used by the
// runtime fallback in EffectiveAdvertisePort. Keeping the parse in the
// test helper rather than hardcoding the numeric value prevents the
// test from drifting if DefaultPeerPort is retuned.
func parseDefaultPeerPort() (domain.PeerPort, error) {
	value, err := strconv.Atoi(DefaultPeerPort)
	if err != nil {
		return 0, err
	}
	return domain.PeerPort(value), nil
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
