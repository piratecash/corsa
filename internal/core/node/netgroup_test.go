package node

import (
	"sort"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

func TestClassifyAddressIPv4(t *testing.T) {
	cases := []struct {
		addr string
		want domain.NetGroup
	}{
		{"1.2.3.4:64646", domain.NetGroupIPv4},
		{"203.0.113.1:8080", domain.NetGroupIPv4},
		{"8.8.8.8:53", domain.NetGroupIPv4},
	}
	for _, tc := range cases {
		if got := classifyAddress(domain.PeerAddress(tc.addr)); got != tc.want {
			t.Errorf("classifyAddress(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestClassifyAddressIPv6(t *testing.T) {
	cases := []struct {
		addr string
		want domain.NetGroup
	}{
		{"[2001:db8::1]:64646", domain.NetGroupIPv6},
		{"[2607:f8b0:4004:800::200e]:443", domain.NetGroupIPv6},
	}
	for _, tc := range cases {
		if got := classifyAddress(domain.PeerAddress(tc.addr)); got != tc.want {
			t.Errorf("classifyAddress(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestClassifyAddressTorV3(t *testing.T) {
	// 56 base32 characters + ".onion"
	onion := "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion:64646"
	if got := classifyAddress(domain.PeerAddress(onion)); got != domain.NetGroupTorV3 {
		t.Errorf("classifyAddress(tor-v3) = %v, want torv3", got)
	}
}

func TestClassifyAddressTorV2(t *testing.T) {
	// 16 base32 characters + ".onion"
	onion := "expyuzz4wqqyqhjn.onion:64646"
	if got := classifyAddress(domain.PeerAddress(onion)); got != domain.NetGroupTorV2 {
		t.Errorf("classifyAddress(tor-v2) = %v, want torv2", got)
	}
}

func TestClassifyAddressI2P(t *testing.T) {
	addr := "ukeu3k5oycgaauneqgtnvselmt4yemvoilkln7jpvamvfx7dnkdq.b32.i2p:64646"
	if got := classifyAddress(domain.PeerAddress(addr)); got != domain.NetGroupI2P {
		t.Errorf("classifyAddress(i2p) = %v, want i2p", got)
	}
}

func TestClassifyAddressCJDNS(t *testing.T) {
	addr := "[fc00::1]:64646"
	if got := classifyAddress(domain.PeerAddress(addr)); got != domain.NetGroupCJDNS {
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
		if got := classifyAddress(domain.PeerAddress(addr)); got != domain.NetGroupLocal {
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
		if got := classifyAddress(domain.PeerAddress(addr)); got != domain.NetGroupUnknown {
			t.Errorf("classifyAddress(%q) = %v, want unknown", addr, got)
		}
	}
}

func TestClassifyAddressBadOnionLength(t *testing.T) {
	// .onion with wrong name length should be unknown
	addr := "tooshort.onion:64646"
	if got := classifyAddress(domain.PeerAddress(addr)); got != domain.NetGroupUnknown {
		t.Errorf("classifyAddress(%q) = %v, want unknown", addr, got)
	}
}

func TestComputeReachableGroupsWithoutProxy(t *testing.T) {
	cfg := config.Node{}
	groups := computeReachableGroups(cfg)

	for _, g := range []domain.NetGroup{domain.NetGroupIPv4, domain.NetGroupIPv6, domain.NetGroupLocal} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v to be reachable without proxy", g)
		}
	}
	for _, g := range []domain.NetGroup{domain.NetGroupTorV2, domain.NetGroupTorV3, domain.NetGroupI2P, domain.NetGroupCJDNS} {
		if _, ok := groups[g]; ok {
			t.Errorf("expected %v to be unreachable without proxy", g)
		}
	}
}

func TestComputeReachableGroupsWithProxy(t *testing.T) {
	cfg := config.Node{ProxyAddress: "127.0.0.1:9050"}
	groups := computeReachableGroups(cfg)

	for _, g := range []domain.NetGroup{domain.NetGroupIPv4, domain.NetGroupIPv6, domain.NetGroupLocal, domain.NetGroupTorV2, domain.NetGroupTorV3, domain.NetGroupI2P} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v to be reachable with proxy", g)
		}
	}
	// CJDNS uses its own tun interface, not SOCKS5 — should not be reachable via proxy.
	if _, ok := groups[domain.NetGroupCJDNS]; ok {
		t.Errorf("expected cjdns to be unreachable even with proxy (requires tun interface)")
	}
}

func TestReachableGroupNames(t *testing.T) {
	groups := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupIPv6:  {},
		domain.NetGroupTorV3: {},
		domain.NetGroupLocal: {}, // should be excluded — not routable
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

func TestPeerReachableGroupsClearnet(t *testing.T) {
	// A clearnet peer (IPv4 address) should get ipv4+ipv6 only.
	groups := peerReachableGroups(domain.PeerAddress("203.0.113.1:64646"))
	if _, ok := groups[domain.NetGroupIPv4]; !ok {
		t.Error("expected ipv4 for clearnet peer")
	}
	if _, ok := groups[domain.NetGroupIPv6]; !ok {
		t.Error("expected ipv6 for clearnet peer")
	}
	if _, ok := groups[domain.NetGroupTorV3]; ok {
		t.Error("clearnet peer should not get torv3")
	}
}

func TestPeerReachableGroupsTorV3(t *testing.T) {
	// A Tor v3 peer should get ipv4+ipv6+torv2+torv3.
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	groups := peerReachableGroups(domain.PeerAddress(onion))
	for _, g := range []domain.NetGroup{domain.NetGroupIPv4, domain.NetGroupIPv6, domain.NetGroupTorV2, domain.NetGroupTorV3} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v for Tor v3 peer", g)
		}
	}
	if _, ok := groups[domain.NetGroupI2P]; ok {
		t.Error("Tor peer should not get i2p")
	}
}

func TestPeerReachableGroupsFingerprint(t *testing.T) {
	// A non-listener peer identified only by fingerprint gets clearnet groups.
	// This is the conservative fallback — the "networks" field is the proper fix.
	groups := peerReachableGroups(domain.PeerAddress("a1b2c3d4e5f6"))
	if _, ok := groups[domain.NetGroupIPv4]; !ok {
		t.Error("expected ipv4 for fingerprint peer")
	}
	if _, ok := groups[domain.NetGroupTorV3]; ok {
		t.Error("fingerprint peer should not get torv3 (use networks field instead)")
	}
}

func TestPeerReachableGroupsI2P(t *testing.T) {
	i2p := "ukeu3k5oycgaauneqgtnvselmt4yemvoilkln7jpvamvfx7dnkdq.b32.i2p:64646"
	groups := peerReachableGroups(domain.PeerAddress(i2p))
	if _, ok := groups[domain.NetGroupI2P]; !ok {
		t.Error("expected i2p for I2P peer")
	}
	if _, ok := groups[domain.NetGroupTorV3]; ok {
		t.Error("I2P peer should not get torv3")
	}
}

func TestValidateDeclaredNetworksClearnetCannotClaimOverlay(t *testing.T) {
	// A clearnet peer declares torv3 — should be stripped.
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupIPv6:  {},
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress("203.0.113.1:64646"))
	if _, ok := result[domain.NetGroupTorV3]; ok {
		t.Error("clearnet peer should not be allowed to claim torv3")
	}
	if _, ok := result[domain.NetGroupIPv4]; !ok {
		t.Error("clearnet peer should keep ipv4")
	}
	if _, ok := result[domain.NetGroupIPv6]; !ok {
		t.Error("clearnet peer should keep ipv6")
	}
}

func TestValidateDeclaredNetworksTorPeerKeepsOverlay(t *testing.T) {
	// A Tor peer declares torv3 — should be kept.
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupIPv6:  {},
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress(onion))
	if _, ok := result[domain.NetGroupTorV3]; !ok {
		t.Error("Tor peer should keep torv3")
	}
	if _, ok := result[domain.NetGroupIPv4]; !ok {
		t.Error("Tor peer should keep ipv4")
	}
}

func TestValidateDeclaredNetworksTorPeerCanClaimBothTorVersions(t *testing.T) {
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupTorV2: {},
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress(onion))
	if _, ok := result[domain.NetGroupTorV2]; !ok {
		t.Error("Tor v3 peer should be allowed torv2")
	}
	if _, ok := result[domain.NetGroupTorV3]; !ok {
		t.Error("Tor v3 peer should keep torv3")
	}
}

func TestValidateDeclaredNetworksTorPeerCannotClaimI2P(t *testing.T) {
	onion := "p53lf57qovyuvwsc6xnrppyply3vtqm7l6pcobkmyqsiofyeznfu5uqd.onion:64646"
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupI2P:   {},
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress(onion))
	if _, ok := result[domain.NetGroupI2P]; ok {
		t.Error("Tor peer should not be allowed to claim i2p")
	}
}

func TestValidateDeclaredNetworksEmptyReturnsNil(t *testing.T) {
	result := validateDeclaredNetworks(nil, domain.PeerAddress("203.0.113.1:64646"))
	if result != nil {
		t.Error("empty declared should return nil")
	}
}

func TestValidateDeclaredNetworksFingerprintAddr(t *testing.T) {
	// Fingerprint-only peer declares torv3 — address doesn't classify as overlay.
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress("a1b2c3d4e5f6"))
	// peerReachableGroups("a1b2c3d4e5f6") → ipv4+ipv6 (fingerprint = unknown)
	if _, ok := result[domain.NetGroupTorV3]; ok {
		t.Error("fingerprint peer should not be allowed torv3")
	}
	if _, ok := result[domain.NetGroupIPv4]; !ok {
		t.Error("fingerprint peer should keep ipv4")
	}
}

func TestValidateDeclaredNetworksAllStrippedFallsBackToInferred(t *testing.T) {
	// Clearnet peer declares ONLY torv3 — everything gets stripped.
	// Should fall back to inferred set (ipv4+ipv6), NOT nil (which would
	// disable filtering entirely).
	declared := map[domain.NetGroup]struct{}{
		domain.NetGroupTorV3: {},
	}
	result := validateDeclaredNetworks(declared, domain.PeerAddress("203.0.113.1:64646"))
	if result == nil {
		t.Fatal("all-stripped result must not be nil (nil = no filtering)")
	}
	if _, ok := result[domain.NetGroupTorV3]; ok {
		t.Error("torv3 should have been stripped")
	}
	if _, ok := result[domain.NetGroupIPv4]; !ok {
		t.Error("should fall back to inferred ipv4")
	}
	if _, ok := result[domain.NetGroupIPv6]; !ok {
		t.Error("should fall back to inferred ipv6")
	}
}
