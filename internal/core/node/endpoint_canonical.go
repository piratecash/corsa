package node

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
)

// endpoint_canonical.go is the one place an endpoint gets the form its
// binding is keyed by, and an address the network group its quotas are
// counted in (docs/protocol/session_v2.md, "Endpoint keys and source
// groups"). Two spellings of one endpoint must never get two bindings, or a
// man in the middle could pick the spelling the binding does not cover.

// errEndpointNotCanonical is an address with no canonical form. Such an
// address gets no endpoint binding; the session's identity is still pinned.
var errEndpointNotCanonical = errors.New("endpoint: no canonical form")

// errEndpointHostAlias is an address without a canonical form that names a
// host which does have one — a numeric spelling of an IPv4 address, a
// subdomain of an onion service, a v2 onion name. A binding of that host is
// not found under the alias, so a lookup of an alias fails closed
// (secureSessionStore.endpointRequiresV2).
var errEndpointHostAlias = fmt.Errorf("%w: an alias of another host", errEndpointNotCanonical)

const (
	// onionV3HostLength is a v3 onion name with its ".onion": 56 base32
	// characters and the suffix. v2 onion services no longer exist in Tor,
	// and a 16-character name would only be another spelling to bind.
	onionV3HostLength = 56 + len(".onion")
	maxDNSNameLength  = 253
	// maxHostLength bounds every host, an IP literal with its zone included,
	// by the longest DNS name.
	maxHostLength     = maxDNSNameLength
	maxDNSLabelLength = 63
	// sourceGroupBitsIPv4 and sourceGroupBitsIPv6 size the source groups.
	// They are wider than the subnet-diversity groups (subnetGroupBits*):
	// those spread this node's own outbound picks, these bound what a whole
	// provider-sized block may make this node store.
	sourceGroupBitsIPv4 = 16
	sourceGroupBitsIPv6 = 32
)

// canonicalEndpoint brings address to the form its binding is keyed by. The
// host is parsed as an IP first and only otherwise read as a name:
//
//   - an IP becomes its canonical text — an IPv4-mapped IPv6 address its
//     IPv4 form, an IPv6 address RFC 5952 (lower case, compressed); the zone
//     is kept only on a link-local or interface-local address, where it
//     names the interface (canonicalIP);
//   - a name becomes lower case without its trailing dot, and must be plain
//     ASCII — letters, digits, '-' and dots; an internationalised name is
//     accepted only in its punycode form, because mapping Unicode to it
//     needs tables that differ between library versions;
//   - a name a resolver may read as an IPv4 address (its last label an IPv4
//     number: 127.1, 127.0.1, 0x7f000001, 2130706433 — isIPv4NumberLabel) is
//     refused as an alias (errEndpointHostAlias): it would give one host a
//     second key;
//   - .onion must be a bare v3 name (transport tor) — a subdomain or a v2
//     name is refused as an alias; .b32.i2p is transport i2p;
//   - the port is decimal, 1–65535, and no host is longer than 253 bytes.
func canonicalEndpoint(address domain.PeerAddress) (domain.CanonicalEndpoint, error) {
	host, portText, ok := splitHostPort(string(address))
	if !ok {
		return domain.CanonicalEndpoint{}, fmt.Errorf("%w: not host:port", errEndpointNotCanonical)
	}
	if len(host) > maxHostLength {
		// An IPv6 zone is free text: without the bound one endpoint could
		// carry an arbitrarily long key into the store and its file.
		return domain.CanonicalEndpoint{}, fmt.Errorf("%w: host longer than %d bytes", errEndpointNotCanonical, maxHostLength)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return domain.CanonicalEndpoint{}, fmt.Errorf("%w: port is not 1-65535", errEndpointNotCanonical)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return newCanonicalEndpoint(domain.EndpointTransportTCP, canonicalIP(ip).String(), uint16(port))
	}
	name, err := canonicalHostName(host)
	if err != nil {
		return domain.CanonicalEndpoint{}, err
	}
	transport, err := nameTransport(name)
	if err != nil {
		return domain.CanonicalEndpoint{}, err
	}
	return newCanonicalEndpoint(transport, name, uint16(port))
}

// canonicalIP is ip as a key: an IPv4-mapped address as IPv4, and a zone kept
// only where it scopes the address — link-local and interface-local. On any
// other address the zone names nothing, and keeping it would give one host
// as many keys as there are strings to append.
func canonicalIP(ip netip.Addr) netip.Addr {
	ip = ip.Unmap()
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
		return ip
	}
	return ip.WithZone("")
}

func newCanonicalEndpoint(transport domain.EndpointTransport, host string, port uint16) (domain.CanonicalEndpoint, error) {
	endpoint, err := domain.NewCanonicalEndpoint(transport, host, port)
	if err != nil {
		return domain.CanonicalEndpoint{}, fmt.Errorf("%w: %w", errEndpointNotCanonical, err)
	}
	return endpoint, nil
}

// canonicalHostName is host as a canonical name: lower case, no trailing dot,
// ASCII labels of letters, digits and '-', and not a numeric spelling of an
// IPv4 address.
func canonicalHostName(host string) (string, error) {
	name := strings.TrimSuffix(strings.ToLower(host), ".")
	if name == "" || len(name) > maxDNSNameLength {
		return "", fmt.Errorf("%w: empty or over-long name", errEndpointNotCanonical)
	}
	labels := strings.Split(name, ".")
	for _, label := range labels {
		if err := checkNameLabel(label); err != nil {
			return "", err
		}
	}
	if isIPv4NumberLabel(labels[len(labels)-1]) {
		return "", fmt.Errorf("%w: a name ending in a number reads as an IPv4 address", errEndpointHostAlias)
	}
	return name, nil
}

// checkNameLabel refuses an empty or over-long label and a character outside
// lower-case ASCII letters, digits and '-'.
func checkNameLabel(label string) error {
	if label == "" || len(label) > maxDNSLabelLength {
		return fmt.Errorf("%w: empty or over-long label", errEndpointNotCanonical)
	}
	for _, c := range label {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return fmt.Errorf("%w: a name must be ASCII letters, digits and '-' (punycode for others)", errEndpointNotCanonical)
		}
	}
	return nil
}

// isIPv4NumberLabel reports whether a resolver in the inet_aton or WHATWG
// style reads label, the last of a name, as an IPv4 number — and so the whole
// name as an IPv4 address (127.1, 127.0.1, 2130706433, 0x7f000001,
// 017700000001): decimal or octal digits, or 0x followed by hex digits only
// (none included). Only the last label decides; a label that merely starts
// with 0x elsewhere (node.0x.org, 0xnode.example.com) is an ordinary name.
func isIPv4NumberLabel(label string) bool {
	if hex, prefixed := strings.CutPrefix(label, "0x"); prefixed {
		return strings.Trim(hex, "0123456789abcdef") == ""
	}
	return isDigits(label)
}

func isDigits(label string) bool {
	for _, c := range label {
		if c < '0' || c > '9' {
			return false
		}
	}
	return label != ""
}

// nameTransport is the transport a canonical name is dialled over: the
// overlays go through the SOCKS5 proxy (dialPeer), every other name direct.
func nameTransport(name string) (domain.EndpointTransport, error) {
	switch {
	case strings.HasSuffix(name, ".onion"):
		if len(name) != onionV3HostLength || !isOnionAddress(name) {
			return "", fmt.Errorf("%w: not a bare v3 onion name", errEndpointHostAlias)
		}
		return domain.EndpointTransportTor, nil
	case isI2PAddress(name):
		return domain.EndpointTransportI2P, nil
	default:
		return domain.EndpointTransportTCP, nil
	}
}

// sourceGroup is the network group of addr (docs/protocol/session_v2.md,
// "Endpoint keys and source groups"): an IPv4 /16, an IPv6 /32, and all of
// loopback as one group — every inbound onion connection arrives from
// loopback, so it must not pass for many sources. Private ranges get their
// /16 like any other: the quota counted in a group is a bound on what a block
// may make this node store, not a judgement of the block.
func sourceGroup(addr netip.Addr) domain.SourceGroup {
	if addr.Unmap().IsLoopback() {
		return domain.SourceGroupLoopback
	}
	return domain.SourceGroup(addressGroupPrefix(addr, sourceGroupBitsIPv4, sourceGroupBitsIPv6).String())
}

// addressGroupPrefix is the network of addr masked to v4Bits or v6Bits, an
// IPv4-mapped address counted as IPv4 and a zone dropped. Shared by the
// source groups and the subnet-diversity groups (subnetGroupKey).
func addressGroupPrefix(addr netip.Addr, v4Bits, v6Bits int) netip.Prefix {
	addr = addr.Unmap().WithZone("")
	bits := v6Bits
	if addr.Is4() {
		bits = v4Bits
	}
	prefix, err := addr.Prefix(bits)
	if err != nil {
		// Unreachable for a valid address and the constant widths above; an
		// invalid address has no network and is its own group.
		return netip.PrefixFrom(addr, addr.BitLen())
	}
	return prefix
}

// endpointSourceGroup is the source group of a dialled endpoint, fixed when
// its binding is created: an IP endpoint's own group; for a name, the group
// of the address the direct dial reached (connected), or the shared
// unresolved-name group when that is not known; tor and i2p for the overlays,
// whose endpoints cost nothing to make and are kept apart from clearnet.
func endpointSourceGroup(endpoint domain.CanonicalEndpoint, connected *netip.Addr) domain.SourceGroup {
	switch endpoint.Transport() {
	case domain.EndpointTransportTor:
		return domain.SourceGroupTor
	case domain.EndpointTransportI2P:
		return domain.SourceGroupI2P
	}
	if ip, err := netip.ParseAddr(endpoint.Host()); err == nil {
		return sourceGroup(ip)
	}
	if connected != nil {
		return sourceGroup(*connected)
	}
	return domain.SourceGroupUnresolvedName
}
