package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// RoutingSnapshot returns the cached routing snapshot maintained by the
// hot-reads refresher (see routing_snapshot.go and hot_reads_refresh.go).
// Reads are lock-free: a single atomic.Pointer.Load with no acquisition
// of routing.Table.t.mu.RLock on the RPC path.
//
// Two staleness bounds apply (see docs/routing.md "Snapshot freshness"):
//
//   - Structural changes (route accepted/withdrawn/replaced, direct peer
//     added/removed, flap burst arming hold-down, flap-state cleanup
//     after a writer touched the table) are reflected within
//     networkStatsSnapshotInterval (~500 ms) plus the publisher's
//     t.mu.RLock acquisition time.
//   - Time-derived state (`IsExpired`, `ttl_seconds` evaluated against
//     `snap.TakenAt`, `FlapEntry.InHoldDown` flipping from true to
//     false on hold-down expiry) lags up to TickTTL_interval (≈10 s)
//     plus one refresh tick: the dirty-flag publisher only republishes
//     when a writer touches the table, but TTL elapse and hold-down
//     expiry are wall-clock events without a writer until TickTTL
//     rewrites the entry. Hold-down arming is a writer event and is
//     reflected within the structural bound.
//
// Returned routing.Snapshot must NOT be mutated by callers — the underlying
// routes map is shared between every concurrent reader of the same
// publish generation. Mutating it would corrupt observers downstream;
// the next refresh produces a brand-new map under a new pointer rather
// than mutating in place.
//
// Callers that require a strictly fresh view — most commonly because
// they make decisions that depend on TTL freshness, such as
// file_integration.isPeerReachable — should call
// s.routingTable.Snapshot() or s.routingTable.Lookup() directly. Those
// paths take t.mu.RLock and pay the full deep-copy cost (Snapshot) or
// an O(K) per-destination scan (Lookup); they are not exposed over RPC.
//
// Implements rpc.RoutingProvider.
func (s *Service) RoutingSnapshot() routing.Snapshot {
	return s.loadRoutingSnapshot()
}

// PeerTransport resolves a peer identity to its current transport address
// and network group. Returns zero values if the peer is not currently
// connected. When a peer has multiple sessions, the lexicographically
// smallest connected address is returned for deterministic output.
//
// The health map (Connected flag) is the source of truth for live
// connectivity. peerIDs entries alone are not sufficient because normal
// disconnect paths do not clear them immediately.
//
// Locking: takes s.peerMu.RLock for the iteration of peerIDs / health.
// Unlike Service.RoutingSnapshot — which is fully lock-free via the
// cached atomic.Pointer — this method is still coupled to peer-domain
// writers. routeTableHandler calls it once per unique next-hop identity
// (cached inside the handler), so under a peer-domain writer storm
// fetchRouteTable can still observe stalls even though the routing
// snapshot itself is now lock-free. Migrating PeerTransport onto a
// cached peer-transport view is a follow-up task; see docs/routing.md
// "Snapshot freshness" for the explicit contract.
// Implements rpc.RoutingProvider.
func (s *Service) PeerTransport(peerIdentity domain.PeerIdentity) (address domain.PeerAddress, network domain.NetGroup) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	var bestAddr domain.PeerAddress
	for addr, id := range s.peerIDs {
		if id != peerIdentity {
			continue
		}
		resolved := s.resolveHealthAddress(addr)
		h := s.health[resolved]
		if h == nil || !h.Connected {
			continue
		}
		if bestAddr == "" || addr < bestAddr {
			bestAddr = addr
		}
	}
	if bestAddr == "" {
		return "", ""
	}
	return bestAddr, classifyAddress(bestAddr)
}

// reachableIDsFrame returns identities that have at least one live route in
// the routing table. Used by desktop clients in remote TCP mode where direct
// RoutingSnapshot() is not available.
//
// Reads from the cached routing snapshot — the same lock-free path that
// fetchRouteTable / fetchRouteSummary use. Two staleness bounds apply
// (see RoutingSnapshot doc): structural reachability changes (peer
// connect / withdraw) are visible within ~500 ms; TTL-derived expiry
// inside BestRoute is evaluated against snap.TakenAt and may lag up
// to TickTTL_interval + refresh (~10.5 s) for routes that age out
// without any other table mutation. Both bounds are acceptable for a
// UI reachability indicator: this frame feeds desktop polling, not
// message routing — operators see reachable=true for an aged-out route
// for at most one TickTTL cycle, never an action that depends on
// strict TTL freshness.
//
// The synthetic local self-route (RouteSourceLocal) is excluded because
// reachability is about remote peers, not the node itself.
func (s *Service) reachableIDsFrame() protocol.Frame {
	snap := s.loadRoutingSnapshot()
	var ids []string
	for id := range snap.Routes {
		best := snap.BestRoute(id)
		if best != nil && best.Source != routing.RouteSourceLocal {
			ids = append(ids, string(id))
		}
	}
	return protocol.Frame{
		Type:       "reachable_ids",
		Identities: ids,
	}
}
