package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// RoutingSnapshot returns an immutable point-in-time copy of the routing table.
// Implements rpc.RoutingProvider.
func (s *Service) RoutingSnapshot() routing.Snapshot {
	return s.routingTable.Snapshot()
}

// PeerTransport resolves a peer identity to its current transport address
// and network group. Returns zero values if the peer is not currently
// connected. When a peer has multiple sessions, the lexicographically
// smallest connected address is returned for deterministic output.
//
// The health map (Connected flag) is the source of truth for live
// connectivity. peerIDs entries alone are not sufficient because normal
// disconnect paths do not clear them immediately.
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
// The synthetic local self-route (RouteSourceLocal) is excluded because
// reachability is about remote peers, not the node itself.
func (s *Service) reachableIDsFrame() protocol.Frame {
	snap := s.routingTable.Snapshot()
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
