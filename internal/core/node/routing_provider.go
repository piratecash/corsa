package node

import "corsa/internal/core/routing"

// RoutingSnapshot returns an immutable point-in-time copy of the routing table.
// Implements rpc.RoutingProvider.
func (s *Service) RoutingSnapshot() routing.Snapshot {
	return s.routingTable.Snapshot()
}

// PeerTransport resolves a peer identity to its current transport address
// and network type. Returns empty strings if the peer is not currently
// connected. When a peer has multiple sessions, the lexicographically
// smallest connected address is returned for deterministic output.
//
// The health map (Connected flag) is the source of truth for live
// connectivity. peerIDs entries alone are not sufficient because normal
// disconnect paths do not clear them immediately.
// Implements rpc.RoutingProvider.
func (s *Service) PeerTransport(peerIdentity string) (address string, network string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var bestAddr string
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
	return bestAddr, classifyAddress(bestAddr).String()
}
