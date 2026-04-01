package domain

import "strings"

// NetGroup classifies a peer address into a network reachability group.
// Peers within the same group can always reach each other.  Peers in
// different groups may or may not be reachable depending on the local
// node's network configuration (e.g. a clearnet node cannot dial a Tor
// address without a SOCKS5 proxy).
//
// The type is string-backed so that serialized values (JSON, RPC,
// protocol frames) remain stable human-readable labels regardless of
// the order in which constants are defined.
type NetGroup string

const (
	// NetGroupIPv4 — public IPv4 addresses.
	NetGroupIPv4 NetGroup = "ipv4"

	// NetGroupIPv6 — public IPv6 addresses (non-Teredo, non-6to4).
	NetGroupIPv6 NetGroup = "ipv6"

	// NetGroupTorV3 — Tor v3 hidden services (56-char base32 + ".onion").
	NetGroupTorV3 NetGroup = "torv3"

	// NetGroupTorV2 — Tor v2 hidden services (16-char base32 + ".onion").
	// Deprecated by the Tor project, but we still classify them.
	NetGroupTorV2 NetGroup = "torv2"

	// NetGroupI2P — I2P addresses (*.b32.i2p).
	NetGroupI2P NetGroup = "i2p"

	// NetGroupCJDNS — CJDNS addresses (fc00::/8).
	NetGroupCJDNS NetGroup = "cjdns"

	// NetGroupLocal — loopback and private addresses.
	// Used for development/testing.  Not relayed to remote peers.
	NetGroupLocal NetGroup = "local"

	// NetGroupUnknown — unclassifiable address (hostnames, malformed, etc.).
	NetGroupUnknown NetGroup = "unknown"
)

// String returns the stable string label for the group.
func (g NetGroup) String() string { return string(g) }

// IsOverlay returns true for network groups that require a proxy or
// special transport to reach (Tor, I2P, CJDNS).
func (g NetGroup) IsOverlay() bool {
	switch g {
	case NetGroupTorV2, NetGroupTorV3, NetGroupI2P, NetGroupCJDNS:
		return true
	default:
		return false
	}
}

// IsRoutable returns true for groups that carry routable (non-local,
// non-unknown) addresses suitable for peer exchange.
func (g NetGroup) IsRoutable() bool {
	switch g {
	case NetGroupLocal, NetGroupUnknown:
		return false
	default:
		return true
	}
}

// ParseNetGroup converts a string name to a NetGroup.
// Returns the group and true on success, or NetGroupUnknown and false
// for unrecognised names.
func ParseNetGroup(s string) (NetGroup, bool) {
	g := NetGroup(strings.ToLower(s))
	switch g {
	case NetGroupIPv4, NetGroupIPv6, NetGroupTorV3, NetGroupTorV2,
		NetGroupI2P, NetGroupCJDNS, NetGroupLocal:
		return g, true
	default:
		return NetGroupUnknown, false
	}
}

// ParseNetGroups converts a list of network group name strings into a set.
// Unknown names are silently ignored.
func ParseNetGroups(names []string) map[NetGroup]struct{} {
	groups := make(map[NetGroup]struct{}, len(names))
	for _, name := range names {
		if g, ok := ParseNetGroup(name); ok {
			groups[g] = struct{}{}
		}
	}
	return groups
}
