package node

import (
	"net"
	"sort"
	"strings"

	"corsa/internal/core/config"
)

// NetGroup classifies a peer address into a network reachability group.
// Peers within the same group can always reach each other.  Peers in
// different groups may or may not be reachable depending on the local
// node's network configuration (e.g. a clearnet node cannot dial a Tor
// address without a SOCKS5 proxy).
//
// The design follows Bitcoin Core's approach (CNetAddr::GetNetwork):
// each address belongs to exactly one group, and the node tracks which
// groups it can reach.
type NetGroup int

const (
	// NetGroupIPv4 — public IPv4 addresses.
	NetGroupIPv4 NetGroup = iota

	// NetGroupIPv6 — public IPv6 addresses (non-Teredo, non-6to4).
	NetGroupIPv6

	// NetGroupTorV3 — Tor v3 hidden services (56-char base32 + ".onion").
	NetGroupTorV3

	// NetGroupTorV2 — Tor v2 hidden services (16-char base32 + ".onion").
	// Deprecated by the Tor project, but we still classify them.
	NetGroupTorV2

	// NetGroupI2P — I2P addresses (*.b32.i2p).
	NetGroupI2P

	// NetGroupCJDNS — CJDNS addresses (fc00::/8).
	NetGroupCJDNS

	// NetGroupLocal — loopback and private addresses.
	// Used for development/testing.  Not relayed to remote peers.
	NetGroupLocal

	// NetGroupUnknown — unclassifiable address (hostnames, malformed, etc.).
	NetGroupUnknown
)

// String returns a short human-readable label for the group.
func (g NetGroup) String() string {
	switch g {
	case NetGroupIPv4:
		return "ipv4"
	case NetGroupIPv6:
		return "ipv6"
	case NetGroupTorV3:
		return "torv3"
	case NetGroupTorV2:
		return "torv2"
	case NetGroupI2P:
		return "i2p"
	case NetGroupCJDNS:
		return "cjdns"
	case NetGroupLocal:
		return "local"
	default:
		return "unknown"
	}
}

// ClassifyAddress returns the NetGroup for a host:port address string.
// It never fails — unknown formats map to NetGroupUnknown.
func ClassifyAddress(address string) NetGroup {
	return classifyAddress(address)
}

func classifyAddress(address string) NetGroup {
	host, _, ok := splitHostPort(address)
	if !ok {
		return NetGroupUnknown
	}
	return classifyHost(host)
}

// classifyHost returns the NetGroup for a bare hostname or IP (no port).
func classifyHost(host string) NetGroup {
	lower := strings.ToLower(host)

	// --- Overlay networks (by suffix) ---

	if strings.HasSuffix(lower, ".onion") {
		name := lower[:len(lower)-6]
		switch len(name) {
		case 56:
			return NetGroupTorV3
		case 16:
			return NetGroupTorV2
		default:
			return NetGroupUnknown
		}
	}

	if strings.HasSuffix(lower, ".b32.i2p") {
		return NetGroupI2P
	}

	// --- IP-based networks ---

	ip := net.ParseIP(host)
	if ip == nil {
		// Non-IP hostname that isn't .onion or .i2p.
		return NetGroupUnknown
	}

	// CJDNS: fc00::/8 — must be checked before IsPrivate() because
	// Go's net.IP.IsPrivate() covers fc00::/7 which overlaps CJDNS.
	if ip.To4() == nil {
		if ip[0] == 0xfc {
			return NetGroupCJDNS
		}
	}

	// Local / private addresses.
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return NetGroupLocal
	}

	// Distinguish IPv4 vs IPv6.
	if ip.To4() != nil {
		return NetGroupIPv4
	}
	return NetGroupIPv6
}

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

// computeReachableGroups builds the set of NetGroups a node can reach
// based on its configuration:
//   - IPv4 and IPv6 are always reachable (direct TCP).
//   - Local is always reachable (for dev/testing).
//   - Tor (v2/v3) is reachable only when a SOCKS5 proxy is configured.
//   - I2P is reachable via SOCKS5 proxy (I2P SAM bridge exposes SOCKS5).
//   - CJDNS uses its own virtual network interface (tun), not SOCKS5,
//     so it's not reachable through a generic proxy — omitted here.
func computeReachableGroups(cfg config.Node) map[NetGroup]struct{} {
	groups := map[NetGroup]struct{}{
		NetGroupIPv4:  {},
		NetGroupIPv6:  {},
		NetGroupLocal: {},
	}
	if cfg.ProxyAddress != "" {
		groups[NetGroupTorV2] = struct{}{}
		groups[NetGroupTorV3] = struct{}{}
		groups[NetGroupI2P] = struct{}{}
	}
	return groups
}

// canReach returns true if the given address belongs to a reachable group.
func (s *Service) canReach(address string) bool {
	g := classifyAddress(address)
	_, ok := s.reachableGroups[g]
	return ok
}

// peerReachableGroups returns the set of groups a remote peer can likely
// reach based on its advertised address from the hello frame.
// Every peer is assumed to reach IPv4/IPv6 (clearnet).  If the peer's
// own address is on an overlay network, that overlay is added too.
// We conservatively don't assume a Tor peer can also reach I2P (or vice
// versa) — we only know about the overlay the peer actually advertised.
func peerReachableGroups(peerAddr string) map[NetGroup]struct{} {
	groups := map[NetGroup]struct{}{
		NetGroupIPv4: {},
		NetGroupIPv6: {},
	}
	pg := classifyAddress(peerAddr)
	if pg.IsOverlay() {
		groups[pg] = struct{}{}
		// A Tor peer can reach both v2 and v3 onion addresses.
		if pg == NetGroupTorV2 || pg == NetGroupTorV3 {
			groups[NetGroupTorV2] = struct{}{}
			groups[NetGroupTorV3] = struct{}{}
		}
	}
	return groups
}

// validateDeclaredNetworks intersects a peer's self-declared network groups
// with the set of groups we can verify from the peer's advertised address.
//
// Rules:
//   - Clearnet groups (ipv4, ipv6) are always allowed — any peer can reach them.
//   - Overlay groups (torv3, torv2, i2p, cjdns) are allowed ONLY if the peer's
//     advertised address belongs to an overlay network.  A Tor peer (v2 or v3)
//     may claim both torv2 and torv3 since they share the same network.
//   - If no declared groups survive validation, the result falls back to the
//     inferred set from the advertised address (never nil — the peer sent a
//     hello, so we always have a baseline).
//
// Returns nil only when declared is empty (peer didn't send "networks").
//
// This prevents a clearnet peer from lying about overlay reachability to
// harvest .onion / .i2p addresses from peer exchange.
func validateDeclaredNetworks(declared map[NetGroup]struct{}, peerAddr string) map[NetGroup]struct{} {
	if len(declared) == 0 {
		return nil
	}

	// Build the ceiling: what groups the advertised address entitles.
	allowed := peerReachableGroups(peerAddr)

	// Intersect: keep only groups that are both declared and allowed.
	result := make(map[NetGroup]struct{}, len(declared))
	for g := range declared {
		if _, ok := allowed[g]; ok {
			result[g] = struct{}{}
		}
	}

	// If nothing survived, fall back to the inferred set.  Never return
	// an empty map — that would block all peer exchange.  Never return
	// nil — the peer DID send a hello so we have enough info to filter.
	if len(result) == 0 {
		return allowed
	}
	return result
}

// reachableGroupNames returns a sorted list of group name strings suitable
// for the "networks" field in the hello frame.  Only routable groups are
// included — local and unknown are omitted.
func reachableGroupNames(groups map[NetGroup]struct{}) []string {
	names := make([]string, 0, len(groups))
	for g := range groups {
		if g.IsRoutable() {
			names = append(names, g.String())
		}
	}
	sort.Strings(names)
	return names
}

// parseNetGroup converts a string name back to a NetGroup.
func parseNetGroup(name string) (NetGroup, bool) {
	switch strings.ToLower(name) {
	case "ipv4":
		return NetGroupIPv4, true
	case "ipv6":
		return NetGroupIPv6, true
	case "torv3":
		return NetGroupTorV3, true
	case "torv2":
		return NetGroupTorV2, true
	case "i2p":
		return NetGroupI2P, true
	case "cjdns":
		return NetGroupCJDNS, true
	case "local":
		return NetGroupLocal, true
	default:
		return NetGroupUnknown, false
	}
}

// parseNetGroups converts a list of network group name strings into a set.
// Unknown names are silently ignored.
func parseNetGroups(names []string) map[NetGroup]struct{} {
	groups := make(map[NetGroup]struct{}, len(names))
	for _, name := range names {
		if g, ok := parseNetGroup(name); ok {
			groups[g] = struct{}{}
		}
	}
	return groups
}
