package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// localCapabilities returns the set of capability tokens this node advertises
// during the handshake. Peers whose negotiated set includes a given token
// will receive frames gated by that capability.
//
//   - mesh_relay_v1: hop-by-hop relay (relay_message frames)
//   - mesh_routing_v1: distance-vector routing via announce_routes frames (Phase 1.2)
//   - file_transfer_v1: file transfer commands (Iteration 21)
func localCapabilities() []domain.Capability {
	return []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1, domain.CapFileTransferV1}
}

// localCapabilityStrings returns the wire-format string list for the hello/
// welcome frame. Used at the protocol boundary where Frame.Capabilities
// is []string.
func localCapabilityStrings() []string {
	return domain.CapabilityStrings(localCapabilities())
}

// intersectCapabilities returns the intersection of two capability slices.
// The result preserves the order of the local slice. Only tokens present in
// both sets are included.
func intersectCapabilities(local []domain.Capability, remote []string) []domain.Capability {
	if len(local) == 0 || len(remote) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(remote))
	for _, capability := range remote {
		set[capability] = struct{}{}
	}
	var result []domain.Capability
	for _, capability := range local {
		if _, ok := set[string(capability)]; ok {
			result = append(result, capability)
		}
	}
	return result
}

// sessionHasCapability returns true when the outbound peer session for the
// given address has the specified capability in its negotiated set.
func (s *Service) sessionHasCapability(address domain.PeerAddress, capability domain.Capability) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	session := s.resolveSessionLocked(address)
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
func (s *Service) connHasCapability(id domain.ConnID, capability domain.Capability) bool {
	pc := s.netCoreForID(id)
	if pc == nil {
		return false
	}
	return pc.HasCapability(capability)
}

// connCapabilitiesForID returns the peer's negotiated capability set for
// the inbound connection id as a defensive copy. Returns nil when the
// connection is not registered. Used by session-lifecycle hooks that need
// the full capability list (not just a single relay-cap boolean) so
// routing-announce state can record what the peer actually supports.
func (s *Service) connCapabilitiesForID(id domain.ConnID) []domain.Capability {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	info, ok := s.connInfoByIDLocked(id)
	if !ok {
		return nil
	}
	return info.capabilities
}
