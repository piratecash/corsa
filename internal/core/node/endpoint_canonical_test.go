package node

import (
	"errors"
	"net/netip"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_canonical_test.go pins the one form an endpoint binding is keyed
// by and the source group its quota is counted in (docs/protocol/session_v2.md,
// "Endpoint keys and source groups").

const (
	testOnionV3 = "pg6mmjiyjmcrsslvykfwnntlaru7p5svn6y2ymmju6nubxndf4pscryd"
	testI2PB32  = "ukeu3k5oycgaauneqgtnvselmt4yemvoilkln7jpvamvfx7dnkdq"
)

// mustCanonical is the canonical endpoint of address, failing the test when
// it has none.
func mustCanonical(t *testing.T, address domain.PeerAddress) domain.CanonicalEndpoint {
	t.Helper()
	endpoint, err := canonicalEndpoint(address)
	if err != nil {
		t.Fatalf("canonicalEndpoint(%q): %v", address, err)
	}
	return endpoint
}

// Spellings of one endpoint are one key.
func TestSpellingsOfOneEndpointShareOneBindingKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		spellings []domain.PeerAddress
		key       domain.PeerAddress
		transport domain.EndpointTransport
	}{
		{"ipv4 and its mapped ipv6 form", []domain.PeerAddress{"1.2.3.4:64646", "[::ffff:1.2.3.4]:64646", "[::FFFF:1.2.3.4]:64646", " 1.2.3.4:064646"}, "1.2.3.4:64646", domain.EndpointTransportTCP},
		{"ipv6 letter case and zero compression", []domain.PeerAddress{"[2001:DB8::1]:64646", "[2001:db8:0:0:0:0:0:1]:64646", "[2001:0db8::0001]:64646"}, "[2001:db8::1]:64646", domain.EndpointTransportTCP},
		{"link-local with its zone", []domain.PeerAddress{"[fe80::1%eth0]:64646", "[FE80:0::1%eth0]:64646"}, "[fe80::1%eth0]:64646", domain.EndpointTransportTCP},
		{"dns name letter case and trailing dot", []domain.PeerAddress{"Node.Example.COM.:64646", "node.example.com:64646"}, "node.example.com:64646", domain.EndpointTransportTCP},
		{"punycode name", []domain.PeerAddress{"XN--80AK6AA92E.com:64646", "xn--80ak6aa92e.com:64646"}, "xn--80ak6aa92e.com:64646", domain.EndpointTransportTCP},
		{"onion in upper case", []domain.PeerAddress{domain.PeerAddress(strings.ToUpper(testOnionV3) + ".ONION:64646"), domain.PeerAddress(testOnionV3 + ".onion:64646")}, domain.PeerAddress(testOnionV3 + ".onion:64646"), domain.EndpointTransportTor},
		{"i2p in upper case", []domain.PeerAddress{domain.PeerAddress(strings.ToUpper(testI2PB32) + ".B32.I2P:64646"), domain.PeerAddress(testI2PB32 + ".b32.i2p:64646")}, domain.PeerAddress(testI2PB32 + ".b32.i2p:64646"), domain.EndpointTransportI2P},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := mustCanonical(t, tc.key)
			if want.Address() != tc.key || want.Transport() != tc.transport {
				t.Fatalf("canonical form of %q is %q over %s, want %q over %s", tc.key, want.Address(), want.Transport(), tc.key, tc.transport)
			}
			for _, spelling := range tc.spellings {
				if got := mustCanonical(t, spelling); got != want {
					t.Errorf("%q gives the key %q over %s, want %q over %s", spelling, got.Address(), got.Transport(), want.Address(), want.Transport())
				}
			}
		})
	}
}

// A zone names an interface: the same link-local address on another
// interface, or without one, is another endpoint.
func TestALinkLocalZoneIsPartOfTheKey(t *testing.T) {
	t.Parallel()
	eth0 := mustCanonical(t, "[fe80::1%eth0]:64646")
	for _, other := range []domain.PeerAddress{"[fe80::1%eth1]:64646", "[fe80::1]:64646"} {
		if mustCanonical(t, other) == eth0 {
			t.Errorf("%q shares the key of [fe80::1%%eth0]:64646", other)
		}
	}
	// A zone only scopes a link-local or interface-local address; on any
	// other it names nothing, and keeping it would give one host endless keys.
	for _, spelling := range []domain.PeerAddress{"[2001:db8::1%eth0]:64646", "[2001:db8::1%anything]:64646"} {
		if got := mustCanonical(t, spelling); got.Address() != "[2001:db8::1]:64646" {
			t.Errorf("%q keeps its zone: key %q", spelling, got.Address())
		}
	}
	for _, scoped := range []domain.PeerAddress{"[ff02::1%eth0]:64646", "[ff01::1%eth0]:64646"} {
		if got := mustCanonical(t, scoped); !strings.Contains(string(got.Address()), "%eth0") {
			t.Errorf("%q lost the zone of a scoped address: key %q", scoped, got.Address())
		}
	}
	if mustCanonical(t, "1.2.3.4:1") == mustCanonical(t, "1.2.3.4:2") {
		t.Error("one IP on two ports is one endpoint")
	}
}

// A host that a resolver could read as another host, or that is
// not plain ASCII, has no canonical form — and so no binding.
func TestAnAmbiguousOrNonASCIIHostHasNoCanonicalForm(t *testing.T) {
	t.Parallel()
	for _, address := range []domain.PeerAddress{
		"пример.рф:64646",
		"bücher.de:64646",
		"127.1:64646",
		"0x7f000001:64646",
		"2130706433:64646",
		"1.2.3.4.5:64646",
		"foo_bar.example:64646",
		"a..b:64646",
		"abcdefghijklmnop.onion:64646",
		"1.2.3.4:0",
		"1.2.3.4:65536",
		"1.2.3.4:-1",
		"1.2.3.4:http",
		":64646",
		"1.2.3.4",
		domain.PeerAddress("[fe80::1%" + strings.Repeat("z", 300) + "]:64646"),
	} {
		if endpoint, err := canonicalEndpoint(address); !errors.Is(err, errEndpointNotCanonical) {
			t.Errorf("canonicalEndpoint(%q) = %q, %v; want errEndpointNotCanonical", address, endpoint.Address(), err)
		}
	}
}

// §7.1: an IPv4 /16, an IPv6 /32, and all of loopback as one group.
func TestTheSourceGroupOfAnAddress(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.SourceGroup{
		"203.0.113.7":        "203.0.0.0/16",
		"203.0.200.1":        "203.0.0.0/16",
		"::ffff:203.0.113.7": "203.0.0.0/16",
		"203.1.0.1":          "203.1.0.0/16",
		"10.1.2.3":           "10.1.0.0/16",
		"2001:db8:1::1":      "2001:db8::/32",
		"2001:db8:ffff::2":   "2001:db8::/32",
		"2001:db9::1":        "2001:db9::/32",
		"fe80::1%eth0":       "fe80::/32",
		"127.0.0.1":          domain.SourceGroupLoopback,
		"127.200.3.4":        domain.SourceGroupLoopback,
		"::1":                domain.SourceGroupLoopback,
		"::ffff:127.0.0.1":   domain.SourceGroupLoopback,
	}
	for literal, want := range cases {
		if got := sourceGroup(netip.MustParseAddr(literal)); got != want {
			t.Errorf("sourceGroup(%s) = %q, want %q", literal, got, want)
		}
	}
}

// §7.7: an IP endpoint is in its address's group, a name in the group of the
// address the dial reached, an onion in tor and an i2p name in i2p.
func TestTheSourceGroupOfAnEndpoint(t *testing.T) {
	t.Parallel()
	reached := netip.MustParseAddr("198.51.100.20")
	cases := []struct {
		address   domain.PeerAddress
		connected *netip.Addr
		want      domain.SourceGroup
	}{
		{"198.51.100.7:64646", nil, "198.51.0.0/16"},
		{"198.51.100.7:64646", &reached, "198.51.0.0/16"},
		{"node.example.com:64646", &reached, "198.51.0.0/16"},
		{"node.example.com:64646", nil, domain.SourceGroupUnresolvedName},
		{domain.PeerAddress(testOnionV3 + ".onion:64646"), &reached, domain.SourceGroupTor},
		{domain.PeerAddress(testI2PB32 + ".b32.i2p:64646"), nil, domain.SourceGroupI2P},
		{"[::1]:64646", nil, domain.SourceGroupLoopback},
	}
	for _, tc := range cases {
		if got := endpointSourceGroup(mustCanonical(t, tc.address), tc.connected); got != tc.want {
			t.Errorf("endpointSourceGroup(%q, %v) = %q, want %q", tc.address, tc.connected, got, tc.want)
		}
	}
}

// A numeric alias is a name whose LAST label a resolver in the inet_aton or
// WHATWG style reads as an IPv4 number — decimal, octal with a leading 0, or
// 0x followed by hex digits only, alone or in the a.b, a.b.c forms. A label
// that merely starts with 0x elsewhere in the name is an ordinary name.
func TestOnlyANumericLastLabelMakesANameAnIPv4Alias(t *testing.T) {
	t.Parallel()
	for _, name := range []domain.PeerAddress{"node.0x.org:64646", "0xnode.example.com:64646", "node.0X1f.example:64646", "example.0xg:64646", "0x.example:64646"} {
		if _, err := canonicalEndpoint(name); err != nil {
			t.Errorf("%q is an ordinary name, refused: %v", name, err)
		}
	}
	for _, alias := range []domain.PeerAddress{
		"127.1:64646", "127.0.1:64646", "10.1:64646", "0x7f000001:64646", "0X7F000001:64646",
		"0x7f.0x1:64646", "017700000001:64646", "2130706433:64646", "example.0x1f:64646", "example.0x:64646",
	} {
		if _, err := canonicalEndpoint(alias); !errors.Is(err, errEndpointHostAlias) {
			t.Errorf("%q reads as an IPv4 address, got %v; want errEndpointHostAlias", alias, err)
		}
	}
}
