package node

import (
	"net"
	"sort"
	"strings"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
)

// ClassifyAddress returns the NetGroup for a host:port address string.
// It never fails — unknown formats map to NetGroupUnknown.
func ClassifyAddress(address string) domain.NetGroup {
	return classifyAddress(domain.PeerAddress(address))
}

func classifyAddress(address domain.PeerAddress) domain.NetGroup {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return domain.NetGroupUnknown
	}
	return classifyHost(host)
}

// classifyHost returns the NetGroup for a bare hostname or IP (no port).
func classifyHost(host string) domain.NetGroup {
	lower := strings.ToLower(host)

	// --- Overlay networks (by suffix) ---

	if strings.HasSuffix(lower, ".onion") {
		name := lower[:len(lower)-6]
		switch len(name) {
		case 56:
			return domain.NetGroupTorV3
		case 16:
			return domain.NetGroupTorV2
		default:
			return domain.NetGroupUnknown
		}
	}

	if strings.HasSuffix(lower, ".b32.i2p") {
		return domain.NetGroupI2P
	}

	// --- IP-based networks ---

	ip := net.ParseIP(host)
	if ip == nil {
		// Non-IP hostname that isn't .onion or .i2p.
		return domain.NetGroupUnknown
	}

	// CJDNS: fc00::/8 — must be checked before IsPrivate() because
	// Go's net.IP.IsPrivate() covers fc00::/7 which overlaps CJDNS.
	if ip.To4() == nil {
		if ip[0] == 0xfc {
			return domain.NetGroupCJDNS
		}
	}

	// Local / private addresses.
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return domain.NetGroupLocal
	}

	// Distinguish IPv4 vs IPv6.
	if ip.To4() != nil {
		return domain.NetGroupIPv4
	}
	return domain.NetGroupIPv6
}

// computeReachableGroups builds the set of NetGroups a node can reach
// based on its configuration:
//   - IPv4 and IPv6 are always reachable (direct TCP).
//   - Local is always reachable (for dev/testing).
//   - Tor (v2/v3) is reachable only when a SOCKS5 proxy is configured.
//   - I2P is reachable via SOCKS5 proxy (I2P SAM bridge exposes SOCKS5).
//   - CJDNS uses its own virtual network interface (tun), not SOCKS5,
//     so it's not reachable through a generic proxy — omitted here.
func computeReachableGroups(cfg config.Node) map[domain.NetGroup]struct{} {
	groups := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4:  {},
		domain.NetGroupIPv6:  {},
		domain.NetGroupLocal: {},
	}
	if cfg.ProxyAddress != "" {
		groups[domain.NetGroupTorV2] = struct{}{}
		groups[domain.NetGroupTorV3] = struct{}{}
		groups[domain.NetGroupI2P] = struct{}{}
	}
	return groups
}

// canReach returns true if the given address belongs to a reachable group.
func (s *Service) canReach(address domain.PeerAddress) bool {
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
func peerReachableGroups(peerAddr domain.PeerAddress) map[domain.NetGroup]struct{} {
	groups := map[domain.NetGroup]struct{}{
		domain.NetGroupIPv4: {},
		domain.NetGroupIPv6: {},
	}
	pg := classifyAddress(peerAddr)
	if pg.IsOverlay() {
		groups[pg] = struct{}{}
		// A Tor peer can reach both v2 and v3 onion addresses.
		if pg == domain.NetGroupTorV2 || pg == domain.NetGroupTorV3 {
			groups[domain.NetGroupTorV2] = struct{}{}
			groups[domain.NetGroupTorV3] = struct{}{}
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
func validateDeclaredNetworks(declared map[domain.NetGroup]struct{}, peerAddr domain.PeerAddress) map[domain.NetGroup]struct{} {
	if len(declared) == 0 {
		return nil
	}

	// Build the ceiling: what groups the advertised address entitles.
	allowed := peerReachableGroups(peerAddr)

	// Intersect: keep only groups that are both declared and allowed.
	result := make(map[domain.NetGroup]struct{}, len(declared))
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
func reachableGroupNames(groups map[domain.NetGroup]struct{}) []string {
	names := make([]string, 0, len(groups))
	for g := range groups {
		if g.IsRoutable() {
			names = append(names, g.String())
		}
	}
	sort.Strings(names)
	return names
}
