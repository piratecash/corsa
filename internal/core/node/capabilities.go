package node

import "net"

// localCapabilities returns the set of capability tokens this node advertises
// during the handshake. Iteration 1 enables hop-by-hop relay via
// "mesh_relay_v1". Only peers whose negotiated set includes this token
// will receive relay_message frames.
func localCapabilities() []string {
	return []string{"mesh_relay_v1"}
}

// intersectCapabilities returns the intersection of two capability slices.
// The result preserves the order of the local slice. Only tokens present in
// both sets are included.
func intersectCapabilities(local, remote []string) []string {
	if len(local) == 0 || len(remote) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(remote))
	for _, cap := range remote {
		set[cap] = struct{}{}
	}
	var result []string
	for _, cap := range local {
		if _, ok := set[cap]; ok {
			result = append(result, cap)
		}
	}
	return result
}

// sessionHasCapability returns true when the outbound peer session for the
// given address has the specified capability in its negotiated set.
func (s *Service) sessionHasCapability(address, cap string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session := s.sessions[address]
	if session == nil {
		return false
	}
	for _, c := range session.capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// connHasCapability returns true when the inbound connection has the specified
// capability in its negotiated set (stored during the hello handshake).
func (s *Service) connHasCapability(conn net.Conn, cap string) bool {
	s.mu.RLock()
	info := s.connPeerInfo[conn]
	s.mu.RUnlock()
	if info == nil {
		return false
	}
	for _, c := range info.capabilities {
		if c == cap {
			return true
		}
	}
	return false
}
