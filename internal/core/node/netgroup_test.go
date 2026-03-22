package node

import (
	"sort"
	"testing"

	"corsa/internal/core/config"
)

func TestClassifyAddressIPv4(t *testing.T) {
	cases := []struct {
		addr string
		want NetGroup
	}{
		{"1.2.3.4:64646", NetGroupIPv4},
		{"203.0.113.1:8080", NetGroupIPv4},
		{"8.8.8.8:53", NetGroupIPv4},
	}
	for _, tc := range cases {
		if got := classifyAddress(tc.addr); got != tc.want {
			t.Errorf("classifyAddress(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestClassifyAddressIPv6(t *testing.T) {
	cases := []struct {
		addr string
		want NetGroup
	}{
		{"[2001:db8::1]:64646", NetGroupIPv6},
		{"[2607:f8b0:4004:800::200e]:443", NetGroupIPv6},
	}
	for _, tc := range cases {
		if got := classifyAddress(tc.addr); got != tc.want {
			t.Errorf("classifyAddress(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestClassifyAddressTorV3(t *testing.T) {
	// 56 base32 characters + ".onion"
	onion := "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion:64646"
	if got := classifyAddress(onion); got != NetGroupTorV3 {
		t.Errorf("classifyAddress(tor-v3) = %v, want torv3", got)
	}
}

func TestClassifyAddressTorV2(t *testing.T) {
	// 16 base32 characters + ".onion"
	onion := "expyuzz4wqqyqhjn.onion:64646"
	if got := classifyAddress(onion); got != NetGroupTorV2 {
		t.Errorf("classifyAddress(tor-v2) = %v, want torv2", got)
	}
}

func TestClassifyAddressI2P(t *testing.T) {
	addr := "ukeu3k5oycgaauneqgtnvselmt4yemvoilkln7jpvamvfx7dnkdq.b32.i2p:64646"
	if got := classifyAddress(addr); got != NetGroupI2P {
		t.Errorf("classifyAddress(i2p) = %v, want i2p", got)
	}
}

func TestClassifyAddressCJDNS(t *testing.T) {
	addr := "[fc00::1]:64646"
	if got := classifyAddress(addr); got != NetGroupCJDNS {
		t.Errorf("classifyAddress(cjdns) = %v, want cjdns", got)
	}
}

func TestClassifyAddressLocal(t *testing.T) {
	cases := []string{
		"127.0.0.1:64646",
		"[::1]:64646",
		"192.168.1.1:64646",
		"10.0.0.1:64646",
	}
	for _, addr := range cases {
		if got := classifyAddress(addr); got != NetGroupLocal {
			t.Errorf("classifyAddress(%q) = %v, want local", addr, got)
		}
	}
}

func TestClassifyAddressUnknown(t *testing.T) {
	cases := []string{
		"",
		"not-a-valid-address",
		"hostname.example.com:64646", // non-IP, non-overlay hostname
	}
	for _, addr := range cases {
		if got := classifyAddress(addr); got != NetGroupUnknown {
			t.Errorf("classifyAddress(%q) = %v, want unknown", addr, got)
		}
	}
}

func TestClassifyAddressBadOnionLength(t *testing.T) {
	// .onion with wrong name length should be unknown
	addr := "tooshort.onion:64646"
	if got := classifyAddress(addr); got != NetGroupUnknown {
		t.Errorf("classifyAddress(%q) = %v, want unknown", addr, got)
	}
}

func TestNetGroupIsOverlay(t *testing.T) {
	overlays := []NetGroup{NetGroupTorV2, NetGroupTorV3, NetGroupI2P, NetGroupCJDNS}
	for _, g := range overlays {
		if !g.IsOverlay() {
			t.Errorf("%v.IsOverlay() = false, want true", g)
		}
	}
	nonOverlays := []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupLocal, NetGroupUnknown}
	for _, g := range nonOverlays {
		if g.IsOverlay() {
			t.Errorf("%v.IsOverlay() = true, want false", g)
		}
	}
}

func TestNetGroupIsRoutable(t *testing.T) {
	routable := []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupTorV2, NetGroupTorV3, NetGroupI2P, NetGroupCJDNS}
	for _, g := range routable {
		if !g.IsRoutable() {
			t.Errorf("%v.IsRoutable() = false, want true", g)
		}
	}
	nonRoutable := []NetGroup{NetGroupLocal, NetGroupUnknown}
	for _, g := range nonRoutable {
		if g.IsRoutable() {
			t.Errorf("%v.IsRoutable() = true, want false", g)
		}
	}
}

func TestComputeReachableGroupsWithoutProxy(t *testing.T) {
	cfg := config.Node{}
	groups := computeReachableGroups(cfg)

	for _, g := range []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupLocal} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v to be reachable without proxy", g)
		}
	}
	for _, g := range []NetGroup{NetGroupTorV2, NetGroupTorV3, NetGroupI2P, NetGroupCJDNS} {
		if _, ok := groups[g]; ok {
			t.Errorf("expected %v to be unreachable without proxy", g)
		}
	}
}

func TestComputeReachableGroupsWithProxy(t *testing.T) {
	cfg := config.Node{ProxyAddress: "127.0.0.1:9050"}
	groups := computeReachableGroups(cfg)

	for _, g := range []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupLocal, NetGroupTorV2, NetGroupTorV3, NetGroupI2P} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v to be reachable with proxy", g)
		}
	}
	// CJDNS uses its own tun interface, not SOCKS5 — should not be reachable via proxy.
	if _, ok := groups[NetGroupCJDNS]; ok {
		t.Errorf("expected cjdns to be unreachable even with proxy (requires tun interface)")
	}
}

func TestNetGroupString(t *testing.T) {
	cases := map[NetGroup]string{
		NetGroupIPv4:    "ipv4",
		NetGroupIPv6:    "ipv6",
		NetGroupTorV3:   "torv3",
		NetGroupTorV2:   "torv2",
		NetGroupI2P:     "i2p",
		NetGroupCJDNS:   "cjdns",
		NetGroupLocal:   "local",
		NetGroupUnknown: "unknown",
	}
	for g, want := range cases {
		if got := g.String(); got != want {
			t.Errorf("NetGroup(%d).String() = %q, want %q", g, got, want)
		}
	}
}

func TestReachableGroupNames(t *testing.T) {
	groups := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupIPv6:  {},
		NetGroupTorV3: {},
		NetGroupLocal: {}, // should be excluded — not routable
	}
	got := reachableGroupNames(groups)
	want := []string{"ipv4", "ipv6", "torv3"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("reachableGroupNames() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reachableGroupNames()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseNetGroups(t *testing.T) {
	names := []string{"ipv4", "ipv6", "torv3", "bogus", "i2p"}
	groups := parseNetGroups(names)
	for _, g := range []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupTorV3, NetGroupI2P} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v in parsed groups", g)
		}
	}
	// "bogus" should not produce a valid group — only known names parse.
	if len(groups) != 4 {
		t.Errorf("expected 4 groups, got %d: %v", len(groups), groups)
	}
}

func TestPeerReachableGroupsClearnet(t *testing.T) {
	// A clearnet peer (IPv4 address) should get ipv4+ipv6 only.
	groups := peerReachableGroups("203.0.113.1:64646")
	if _, ok := groups[NetGroupIPv4]; !ok {
		t.Error("expected ipv4 for clearnet peer")
	}
	if _, ok := groups[NetGroupIPv6]; !ok {
		t.Error("expected ipv6 for clearnet peer")
	}
	if _, ok := groups[NetGroupTorV3]; ok {
		t.Error("clearnet peer should not get torv3")
	}
}

func TestPeerReachableGroupsTorV3(t *testing.T) {
	// A Tor v3 peer should get ipv4+ipv6+torv2+torv3.
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	groups := peerReachableGroups(onion)
	for _, g := range []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupTorV2, NetGroupTorV3} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v for Tor v3 peer", g)
		}
	}
	if _, ok := groups[NetGroupI2P]; ok {
		t.Error("Tor peer should not get i2p")
	}
}

func TestPeerReachableGroupsFingerprint(t *testing.T) {
	// A non-listener peer identified only by fingerprint gets clearnet groups.
	// This is the conservative fallback — the "networks" field is the proper fix.
	groups := peerReachableGroups("a1b2c3d4e5f6")
	if _, ok := groups[NetGroupIPv4]; !ok {
		t.Error("expected ipv4 for fingerprint peer")
	}
	if _, ok := groups[NetGroupTorV3]; ok {
		t.Error("fingerprint peer should not get torv3 (use networks field instead)")
	}
}

func TestPeerReachableGroupsI2P(t *testing.T) {
	i2p := "ukeu3k5oycgaauneqgtnvselmt4yemvoilkln7jpvamvfx7dnkdq.b32.i2p:64646"
	groups := peerReachableGroups(i2p)
	if _, ok := groups[NetGroupI2P]; !ok {
		t.Error("expected i2p for I2P peer")
	}
	if _, ok := groups[NetGroupTorV3]; ok {
		t.Error("I2P peer should not get torv3")
	}
}

func TestValidateDeclaredNetworksClearnetCannotClaimOverlay(t *testing.T) {
	// A clearnet peer declares torv3 — should be stripped.
	declared := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupIPv6:  {},
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, "203.0.113.1:64646")
	if _, ok := result[NetGroupTorV3]; ok {
		t.Error("clearnet peer should not be allowed to claim torv3")
	}
	if _, ok := result[NetGroupIPv4]; !ok {
		t.Error("clearnet peer should keep ipv4")
	}
	if _, ok := result[NetGroupIPv6]; !ok {
		t.Error("clearnet peer should keep ipv6")
	}
}

func TestValidateDeclaredNetworksTorPeerKeepsOverlay(t *testing.T) {
	// A Tor peer declares torv3 — should be kept.
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupIPv6:  {},
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, onion)
	if _, ok := result[NetGroupTorV3]; !ok {
		t.Error("Tor peer should keep torv3")
	}
	if _, ok := result[NetGroupIPv4]; !ok {
		t.Error("Tor peer should keep ipv4")
	}
}

func TestValidateDeclaredNetworksTorPeerCanClaimBothTorVersions(t *testing.T) {
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupTorV2: {},
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, onion)
	if _, ok := result[NetGroupTorV2]; !ok {
		t.Error("Tor v3 peer should be allowed torv2")
	}
	if _, ok := result[NetGroupTorV3]; !ok {
		t.Error("Tor v3 peer should keep torv3")
	}
}

func TestValidateDeclaredNetworksTorPeerCannotClaimI2P(t *testing.T) {
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupI2P:   {},
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, onion)
	if _, ok := result[NetGroupI2P]; ok {
		t.Error("Tor peer should not be allowed to claim i2p")
	}
}

func TestValidateDeclaredNetworksEmptyReturnsNil(t *testing.T) {
	result := validateDeclaredNetworks(nil, "203.0.113.1:64646")
	if result != nil {
		t.Error("empty declared should return nil")
	}
}

func TestValidateDeclaredNetworksFingerprintAddr(t *testing.T) {
	// Fingerprint-only peer declares torv3 — address doesn't classify as overlay.
	declared := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, "a1b2c3d4e5f6")
	// peerReachableGroups("a1b2c3d4e5f6") → ipv4+ipv6 (fingerprint = unknown)
	if _, ok := result[NetGroupTorV3]; ok {
		t.Error("fingerprint peer should not be allowed torv3")
	}
	if _, ok := result[NetGroupIPv4]; !ok {
		t.Error("fingerprint peer should keep ipv4")
	}
}

func TestValidateDeclaredNetworksAllStrippedFallsBackToInferred(t *testing.T) {
	// Clearnet peer declares ONLY torv3 — everything gets stripped.
	// Should fall back to inferred set (ipv4+ipv6), NOT nil (which would
	// disable filtering entirely).
	declared := map[NetGroup]struct{}{
		NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, "203.0.113.1:64646")
	if result == nil {
		t.Fatal("all-stripped result must not be nil (nil = no filtering)")
	}
	if _, ok := result[NetGroupTorV3]; ok {
		t.Error("torv3 should have been stripped")
	}
	if _, ok := result[NetGroupIPv4]; !ok {
		t.Error("should fall back to inferred ipv4")
	}
	if _, ok := result[NetGroupIPv6]; !ok {
		t.Error("should fall back to inferred ipv6")
	}
}
