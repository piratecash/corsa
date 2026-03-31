package node

import "net"

// localCapabilities returns the set of capability tokens this node advertises
// during the handshake. Peers whose negotiated set includes a given token
// will receive frames gated by that capability.
//
//   - mesh_relay_v1: hop-by-hop relay (relay_message frames)
//   - mesh_routing_v1: distance-vector routing via announce_routes frames (Phase 1.2)
func localCapabilities() []string {
	return []string{"mesh_relay_v1", "mesh_routing_v1"}
}

// intersectCapabilities returns the intersection of two capability slices.
// The result preserves the order of the local slice. Only tokens present in
// both sets are included.
func intersectCapabilities(local, remote []string) []string {
	if len(local) == 0 || len(remote) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(remote))
	for _, capability := range remote {
		set[capability] = struct{}{}
	}
	var result []string
	for _, capability := range local {
		if _, ok := set[capability]; ok {
			result = append(result, capability)
		}
	}
	return result
}

// sessionHasCapability returns true when the outbound peer session for the
// given address has the specified capability in its negotiated set.
func (s *Service) sessionHasCapability(address, capability string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session := s.sessions[address]
	if session == nil {
		return false
	}
	for _, c := range session.capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// connHasCapability returns true when the inbound connection has the specified
// capability in its negotiated set (stored during the hello handshake).
func (s *Service) connHasCapability(conn net.Conn, capability string) bool {
	s.mu.RLock()
	info := s.connPeerInfo[conn]
	s.mu.RUnlock()
	if info == nil {
		return false
	}
	for _, c := range info.capabilities {
		if c == capability {
			return true
		}
	}
	return false
}
