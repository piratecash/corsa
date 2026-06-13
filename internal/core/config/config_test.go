package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/appdata"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
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

// TestEnableMeshRoutingV3FromEnv pins the post-soak default: v3 is ON unless
// the operator explicitly opts out. Unset/empty/truthy/unrecognised → true;
// only the falsey set ("0","false","no","off", any case) → false.
func TestEnableMeshRoutingV3FromEnv(t *testing.T) {
	// Default-on: unset and empty both enable.
	for _, v := range []string{"", "  "} {
		t.Setenv("CORSA_ENABLE_MESH_ROUTING_V3", v)
		if !enableMeshRoutingV3FromEnv() {
			t.Fatalf("CORSA_ENABLE_MESH_ROUTING_V3=%q: want default true", v)
		}
	}

	// Explicit opt-out set.
	for _, v := range []string{"0", "false", "no", "off", "OFF", " False "} {
		t.Setenv("CORSA_ENABLE_MESH_ROUTING_V3", v)
		if enableMeshRoutingV3FromEnv() {
			t.Fatalf("CORSA_ENABLE_MESH_ROUTING_V3=%q: want false (opt-out)", v)
		}
	}

	// Truthy and unrecognised both stay enabled.
	for _, v := range []string{"1", "true", "yes", "on", "ON", "wat"} {
		t.Setenv("CORSA_ENABLE_MESH_ROUTING_V3", v)
		if !enableMeshRoutingV3FromEnv() {
			t.Fatalf("CORSA_ENABLE_MESH_ROUTING_V3=%q: want true", v)
		}
	}
}

// TestHoldDMUntilReachableFromEnv pins the default-ON / kill-switch semantics
// for the reachability gate: unset/empty/truthy/unrecognised → true (the storm
// cure is on by default); only the explicit falsey set restores the legacy
// blind-gossip baseline. Guards against an accidental flip back to opt-in.
func TestHoldDMUntilReachableFromEnv(t *testing.T) {
	// Default-on: unset and empty both enable.
	for _, v := range []string{"", "  "} {
		t.Setenv("CORSA_HOLD_DM_UNTIL_REACHABLE", v)
		if !holdDMUntilReachableFromEnv() {
			t.Fatalf("CORSA_HOLD_DM_UNTIL_REACHABLE=%q: want default true", v)
		}
	}

	// Explicit kill-switch.
	for _, v := range []string{"0", "false", "no", "off", "OFF", " False "} {
		t.Setenv("CORSA_HOLD_DM_UNTIL_REACHABLE", v)
		if holdDMUntilReachableFromEnv() {
			t.Fatalf("CORSA_HOLD_DM_UNTIL_REACHABLE=%q: want false (kill-switch)", v)
		}
	}

	// Truthy and unrecognised both stay enabled.
	for _, v := range []string{"1", "true", "yes", "on", "ON", "wat"} {
		t.Setenv("CORSA_HOLD_DM_UNTIL_REACHABLE", v)
		if !holdDMUntilReachableFromEnv() {
			t.Fatalf("CORSA_HOLD_DM_UNTIL_REACHABLE=%q: want true", v)
		}
	}

	// And the wired Config default surfaces ON via Default().
	t.Setenv("CORSA_HOLD_DM_UNTIL_REACHABLE", "")
	if !Default().Node.HoldDMUntilReachable {
		t.Fatal("Default().Node.HoldDMUntilReachable: want true (default ON)")
	}
}

// TestAcceptDirectMessagesFromEnv pins the headless DM-acceptance knob:
// default OFF (unset/empty/unrecognised) — a console node has no user
// reading messages — only explicit truthy values enable acceptance.
func TestAcceptDirectMessagesFromEnv(t *testing.T) {
	// Default-off: unset, empty, and unrecognised all stay disabled.
	for _, v := range []string{"", "  ", "0", "false", "no", "off", "wat"} {
		t.Setenv("CORSA_ACCEPT_DM", v)
		if AcceptDirectMessagesFromEnv() {
			t.Fatalf("CORSA_ACCEPT_DM=%q: want default false", v)
		}
	}

	// Explicit opt-in.
	for _, v := range []string{"1", "true", "yes", "on", "ON", " True "} {
		t.Setenv("CORSA_ACCEPT_DM", v)
		if !AcceptDirectMessagesFromEnv() {
			t.Fatalf("CORSA_ACCEPT_DM=%q: want true (opt-in)", v)
		}
	}
}

// TestDefaultAcceptsDirectMessages pins that the SHARED default keeps
// today's behaviour: the DisableDirectMessages zero value means DMs are
// accepted, so the desktop client and existing Service constructions
// (including struct-literal fixtures) are untouched. The headless binary
// opts out explicitly via AcceptDirectMessagesFromEnv — that override is
// pinned in internal/app/node, not here.
func TestDefaultAcceptsDirectMessages(t *testing.T) {
	cfg := Default()
	if cfg.Node.DisableDirectMessages {
		t.Fatal("Default(): want DisableDirectMessages=false (desktop/back-compat default)")
	}
}

// TestServiceListMessagesGate pins that "messages" is advertised only by
// nodes that actually accept DMs; peers display the list in console UIs,
// so a drop-everything relay must not claim a live inbox.
func TestServiceListMessagesGate(t *testing.T) {
	accepting := Node{Type: NodeTypeFull}
	want := []string{"identity", "contacts", "messages", "gazeta", "relay"}
	if got := accepting.ServiceList(); !slicesEqual(got, want) {
		t.Fatalf("accepting full node: got %v, want %v", got, want)
	}

	relayOnly := Node{Type: NodeTypeFull, DisableDirectMessages: true}
	want = []string{"identity", "contacts", "gazeta", "relay"}
	if got := relayOnly.ServiceList(); !slicesEqual(got, want) {
		t.Fatalf("relay-only full node: got %v, want %v", got, want)
	}

	client := Node{Type: NodeTypeClient}
	want = []string{"identity", "contacts", "messages", "gazeta"}
	if got := client.ServiceList(); !slicesEqual(got, want) {
		t.Fatalf("accepting client node: got %v, want %v", got, want)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRecordAllTrafficFromEnv pins the startup traffic recording knob:
// default OFF (unset/empty/unrecognised), only explicit truthy values
// enable it. Mirrors the opt-in contract documented on Node.RecordAllTraffic.
func TestRecordAllTrafficFromEnv(t *testing.T) {
	// Default-off: unset, empty, and unrecognised all stay disabled.
	for _, v := range []string{"", "  ", "0", "false", "no", "off", "wat"} {
		t.Setenv("CORSA_RECORD_ALL_TRAFFIC", v)
		if recordAllTrafficFromEnv() {
			t.Fatalf("CORSA_RECORD_ALL_TRAFFIC=%q: want default false", v)
		}
	}

	// Explicit opt-in.
	for _, v := range []string{"1", "true", "yes", "on", "ON", " True "} {
		t.Setenv("CORSA_RECORD_ALL_TRAFFIC", v)
		if !recordAllTrafficFromEnv() {
			t.Fatalf("CORSA_RECORD_ALL_TRAFFIC=%q: want true (opt-in)", v)
		}
	}
}

// TestRecordTrafficFormatFromEnv pins that the format knob flows through
// Default() verbatim (validation happens later in Service.Run via
// domain.ParseCaptureFormat, same as the RPC command).
func TestRecordTrafficFormatFromEnv(t *testing.T) {
	t.Setenv("CORSA_RECORD_TRAFFIC_FORMAT", "pretty")
	cfg := Default()
	if cfg.Node.RecordTrafficFormat != "pretty" {
		t.Fatalf("RecordTrafficFormat = %q, want %q", cfg.Node.RecordTrafficFormat, "pretty")
	}

	t.Setenv("CORSA_RECORD_TRAFFIC_FORMAT", "")
	cfg = Default()
	if cfg.Node.RecordTrafficFormat != "" {
		t.Fatalf("RecordTrafficFormat = %q, want empty default", cfg.Node.RecordTrafficFormat)
	}
}

// TestPendingRingSizeFromEnv pins the per-peer pending ring size knob:
// unset/empty/non-numeric/<=0 → 0 ("use built-in default"); a positive
// integer is taken as-is.
func TestPendingRingSizeFromEnv(t *testing.T) {
	for _, v := range []string{"", "  ", "abc", "0", "-5"} {
		t.Setenv("CORSA_PENDING_RING_SIZE", v)
		if got := pendingRingSizeFromEnv(); got != 0 {
			t.Fatalf("CORSA_PENDING_RING_SIZE=%q: want 0 (default sentinel), got %d", v, got)
		}
	}
	for _, tc := range []struct {
		raw  string
		want int
	}{{"1", 1}, {"256", 256}, {" 1000 ", 1000}} {
		t.Setenv("CORSA_PENDING_RING_SIZE", tc.raw)
		if got := pendingRingSizeFromEnv(); got != tc.want {
			t.Fatalf("CORSA_PENDING_RING_SIZE=%q: got %d, want %d", tc.raw, got, tc.want)
		}
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
	if got, want := cfg.Node.IdentityPath, filepath.Join(dataDir, "identity-64646.json"); got != want {
		t.Fatalf("IdentityPath = %q, want %q", got, want)
	}
	if got, want := cfg.Node.TrustStorePath, filepath.Join(dataDir, "trust-64646.json"); got != want {
		t.Fatalf("TrustStorePath = %q, want %q", got, want)
	}
}

// TestMaxNextHopsPerOriginRolloutDefaults pins the second-release
// rollout shape for the routing-table cap: with no env override, the
// loader returns the recommended ceiling
// (routing.DefaultMaxNextHopsPerOrigin = 4); explicit "0" remains a
// first-class operator opt-out so a future field regression can be
// neutralised without a rebuild; explicit positive values pass
// through; unparsable / negative values fall back to the default in
// the same shape as maxClockDriftFromEnv.
//
// The first rollout release shipped with the loader default at 0 so
// existing deployments observed pre-cap behaviour exactly during the
// soak period; this test guards the flip to 4 introduced by the
// second release. Reverting either side without a deliberate plan
// (e.g. silently restoring `return 0` for the unset case, or
// promoting "0" into "use default" semantics) breaks the rollout
// contract and trips this test immediately.
func TestMaxNextHopsPerOriginRolloutDefaults(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{
			name: "unset_uses_recommended_default",
			raw:  "",
			want: routing.DefaultMaxNextHopsPerOrigin,
		},
		{
			name: "explicit_zero_disables_cap",
			raw:  "0",
			want: 0,
		},
		{
			name: "explicit_positive_passes_through",
			raw:  "2",
			want: 2,
		},
		{
			name: "explicit_recommended_passes_through",
			raw:  "4",
			want: routing.DefaultMaxNextHopsPerOrigin,
		},
		{
			name: "negative_falls_back_to_default",
			raw:  "-1",
			want: routing.DefaultMaxNextHopsPerOrigin,
		},
		{
			name: "unparsable_falls_back_to_default",
			raw:  "abc",
			want: routing.DefaultMaxNextHopsPerOrigin,
		},
		{
			name: "whitespace_only_treated_as_unset",
			raw:  "   ",
			want: routing.DefaultMaxNextHopsPerOrigin,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_MAX_NEXT_HOPS_PER_ORIGIN", tc.raw)
			got := maxNextHopsPerOriginFromEnv()
			if got != tc.want {
				t.Fatalf("maxNextHopsPerOriginFromEnv() = %d, want %d (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}

// TestAnnounceIntervalFromEnv covers the env-loader contract for the
// Phase 0 density-aware tuning knob: empty/invalid/non-positive values
// fall back to zero (which the Service init translates into "use the
// routing-package default"), valid positive integers pass through as
// seconds. The forced-full-sync cap (DefaultTTL/2) is enforced inside
// the AnnounceLoop, NOT here — this loader is just text→duration.
func TestAnnounceIntervalFromEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset_returns_zero_for_package_default", "", 0},
		{"explicit_30_seconds_passes_through", "30", 30 * time.Second},
		{"dense_mesh_60_seconds_passes_through", "60", 60 * time.Second},
		{"explicit_zero_treated_as_unset", "0", 0},
		{"negative_falls_back_to_zero", "-30", 0},
		{"unparsable_falls_back_to_zero", "abc", 0},
		{"whitespace_only_treated_as_unset", "   ", 0},
		{"large_value_passes_through", "300", 300 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_ANNOUNCE_INTERVAL_SECONDS", tc.raw)
			got := announceIntervalFromEnv()
			if got != tc.want {
				t.Fatalf("announceIntervalFromEnv() = %v, want %v (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}

// TestOverloadGoroutineThresholdFromEnv covers the env-loader contract
// for the Phase 0 overload-mode trip point: empty/invalid/non-positive
// values fall back to zero (which the Service init translates into
// "disable the gate"); valid positive integers pass through. There is
// no domain default — operators must set the value deliberately.
func TestOverloadGoroutineThresholdFromEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"unset_disables_gate", "", 0},
		{"explicit_zero_disables_gate", "0", 0},
		{"explicit_negative_disables_gate", "-1000", 0},
		{"unparsable_disables_gate", "abc", 0},
		{"whitespace_only_disables_gate", "   ", 0},
		{"hub_node_threshold_5000", "5000", 5000},
		{"high_threshold_passes_through", "20000", 20000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_OVERLOAD_GOROUTINE_THRESHOLD", tc.raw)
			got := overloadGoroutineThresholdFromEnv()
			if got != tc.want {
				t.Fatalf("overloadGoroutineThresholdFromEnv() = %d, want %d (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}

// TestDefaultConfigActivatesCapAtRecommendedCeiling pins the
// integration shape: a Default() build with no env overrides surfaces
// the recommended cap on Node.MaxNextHopsPerOrigin, which is what the
// Service-init path threads into routing.WithMaxNextHopsPerOrigin. A
// regression here means production deployments stop activating the
// cap on upgrade — the operator-visible symptom would be the
// `cap_admission` counters stuck at zero on busy nodes that should be
// evicting routes.
func TestDefaultConfigActivatesCapAtRecommendedCeiling(t *testing.T) {
	t.Setenv("CORSA_MAX_NEXT_HOPS_PER_ORIGIN", "")

	cfg := Default()
	if got, want := cfg.Node.MaxNextHopsPerOrigin, routing.DefaultMaxNextHopsPerOrigin; got != want {
		t.Fatalf("Default().Node.MaxNextHopsPerOrigin = %d, want %d", got, want)
	}
}

// TestMaxSeqAdvancePerWindowFromEnv pins the kill-switch contract on
// Node.MaxSeqAdvancePerWindow doc-comment: "Zero (or negative)
// disables the cap entirely". Unset / malformed / whitespace-only
// inputs fall back to the production default
// (routing.DefaultMaxSeqAdvancePerWindow = 10). Negative integers
// MUST pass through unchanged — they reach
// FlapDetector.recordSeqAdvanceLocked's `maxSeqAdvancePerWindow <= 0`
// short-circuit and disable the cap, matching the documented
// operator contract. Without the pass-through,
// CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW=-1 would silently activate the
// production default (10), contradicting the documented kill-switch.
func TestMaxSeqAdvancePerWindowFromEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"unset_uses_production_default", "", routing.DefaultMaxSeqAdvancePerWindow},
		{"explicit_zero_disables_cap", "0", 0},
		{"negative_disables_cap_per_doc", "-1", -1},
		{"explicit_positive_passes_through", "25", 25},
		{"unparsable_falls_back_to_default", "abc", routing.DefaultMaxSeqAdvancePerWindow},
		{"whitespace_only_treated_as_unset", "   ", routing.DefaultMaxSeqAdvancePerWindow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW", tc.raw)
			got := maxSeqAdvancePerWindowFromEnv()
			if got != tc.want {
				t.Fatalf("maxSeqAdvancePerWindowFromEnv() = %d, want %d (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}

// TestSeqAdvanceWindowFromEnv mirrors TestMaxSeqAdvancePerWindowFromEnv
// for the sliding-window length: negative seconds pass through as a
// negative time.Duration that disables the cap via
// `FlapDetector.seqAdvanceWindow <= 0`. Production default for unset
// / malformed inputs is routing.DefaultSeqAdvanceWindow (5 min).
func TestSeqAdvanceWindowFromEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"unset_uses_production_default", "", routing.DefaultSeqAdvanceWindow},
		{"explicit_zero_disables_cap", "0", 0},
		{"negative_disables_cap_per_doc", "-1", -1 * time.Second},
		{"explicit_positive_passes_through", "120", 120 * time.Second},
		{"unparsable_falls_back_to_default", "abc", routing.DefaultSeqAdvanceWindow},
		{"whitespace_only_treated_as_unset", "   ", routing.DefaultSeqAdvanceWindow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_SEQNO_ADVANCE_WINDOW_SECONDS", tc.raw)
			got := seqAdvanceWindowFromEnv()
			if got != tc.want {
				t.Fatalf("seqAdvanceWindowFromEnv() = %v, want %v (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}

// TestMaxSaneHopsFromEnv pins the kill-switch contract on
// Node.MaxSaneHops doc-comment: "Zero (or negative) disables the
// path". Negative integers MUST pass through unchanged — they reach
// the `s.maxSaneHops > 0` gate in ApplyUpdate's fast-invalidation
// branch and disable the path, matching the documented operator
// contract. Without the pass-through, CORSA_MAX_SANE_HOPS=-1 would
// silently activate the production default (routing.MaxSaneHops=8),
// contradicting the documented kill-switch.
func TestMaxSaneHopsFromEnv(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"unset_uses_production_default", "", routing.MaxSaneHops},
		{"explicit_zero_disables_cap", "0", 0},
		{"negative_disables_cap_per_doc", "-1", -1},
		{"explicit_positive_passes_through", "12", 12},
		{"unparsable_falls_back_to_default", "abc", routing.MaxSaneHops},
		{"whitespace_only_treated_as_unset", "   ", routing.MaxSaneHops},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CORSA_MAX_SANE_HOPS", tc.raw)
			got := maxSaneHopsFromEnv()
			if got != tc.want {
				t.Fatalf("maxSaneHopsFromEnv() = %d, want %d (raw=%q)", got, tc.want, tc.raw)
			}
		})
	}
}
