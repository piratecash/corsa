package domain

import (
	"sort"
	"testing"
)

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
			t.Errorf("NetGroup(%q).String() = %q, want %q", string(g), got, want)
		}
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

func TestParseNetGroup(t *testing.T) {
	valid := []struct {
		input string
		want  NetGroup
	}{
		{"ipv4", NetGroupIPv4},
		{"ipv6", NetGroupIPv6},
		{"torv3", NetGroupTorV3},
		{"torv2", NetGroupTorV2},
		{"i2p", NetGroupI2P},
		{"cjdns", NetGroupCJDNS},
		{"local", NetGroupLocal},
	}
	for _, tc := range valid {
		g, ok := ParseNetGroup(tc.input)
		if !ok {
			t.Errorf("ParseNetGroup(%q) returned false, want true", tc.input)
		}
		if g != tc.want {
			t.Errorf("ParseNetGroup(%q) = %v, want %v", tc.input, g, tc.want)
		}
	}

	// Case-insensitive parsing for wire-format compatibility.
	caseInsensitive := []struct {
		input string
		want  NetGroup
	}{
		{"IPV4", NetGroupIPv4},
		{"TORV3", NetGroupTorV3},
		{"Ipv6", NetGroupIPv6},
	}
	for _, tc := range caseInsensitive {
		g, ok := ParseNetGroup(tc.input)
		if !ok {
			t.Errorf("ParseNetGroup(%q) returned false, want true", tc.input)
		}
		if g != tc.want {
			t.Errorf("ParseNetGroup(%q) = %v, want %v", tc.input, g, tc.want)
		}
	}

	invalid := []string{"", "bogus"}
	for _, s := range invalid {
		g, ok := ParseNetGroup(s)
		if ok {
			t.Errorf("ParseNetGroup(%q) returned true, want false", s)
		}
		if g != NetGroupUnknown {
			t.Errorf("ParseNetGroup(%q) = %v, want NetGroupUnknown", s, g)
		}
	}
}

func TestParseNetGroups(t *testing.T) {
	names := []string{"ipv4", "ipv6", "torv3", "bogus", "i2p"}
	groups := ParseNetGroups(names)
	for _, g := range []NetGroup{NetGroupIPv4, NetGroupIPv6, NetGroupTorV3, NetGroupI2P} {
		if _, ok := groups[g]; !ok {
			t.Errorf("expected %v in parsed groups", g)
		}
	}
	if len(groups) != 4 {
		t.Errorf("expected 4 groups, got %d", len(groups))
	}
}

func TestNetGroupStringValuesAreStable(t *testing.T) {
	// Verify that string values remain consistent for wire-format stability.
	all := []NetGroup{
		NetGroupIPv4, NetGroupIPv6, NetGroupTorV3, NetGroupTorV2,
		NetGroupI2P, NetGroupCJDNS, NetGroupLocal, NetGroupUnknown,
	}
	strs := make([]string, len(all))
	for i, g := range all {
		strs[i] = g.String()
	}
	sorted := make([]string, len(strs))
	copy(sorted, strs)
	sort.Strings(sorted)

	// All labels must be unique.
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			t.Errorf("duplicate NetGroup string label: %q", sorted[i])
		}
	}
}
