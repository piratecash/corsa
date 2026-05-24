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
//   - mesh_routing_v2: delta announces via routes_update frames; opt-in
//     refinement over v1. Advertised only when v1 is also advertised so a
//     mixed-version network never sees v2 without v1.
//   - file_transfer_v1: file transfer commands (Iteration 21)
//   - mesh_route_probe_v1: active route reachability probes
//     (route_probe_v1 / route_probe_ack_v1) introduced in Phase 2
//     (docs/protocol/route_health.md). Probes are sent only to
//     peers advertising this capability. Mixed-version interop:
//     mesh_routing_v1-only peers still produce health entries on
//     our side (every accepted claim is seeded Questionable by
//     UpdateRoute regardless of caps); what they skip is the
//     active probe send path — their pairs stay Questionable until
//     organic relay hop_ack traffic confirms them, ranked with the
//     standard scoreHealthQPenalty CompositeScore penalty (sized
//     for strict-tier ordering — every Questionable below every
//     Good). See docs/protocol/route_health.md "Capability gating"
//     for the full contract.
//   - mesh_route_query_v1: targeted single-hop route queries
//     (route_query_v1 / route_query_response_v1) introduced in Phase 2.
//     Queries are sent on-demand when all known uplinks for a target
//     identity are Bad/Dead; rate-limited 3 per target per 30s; never
//     forwarded. Fan-out targets must advertise the FULL triplet
//     mesh_route_query_v1 + mesh_relay_v1 + mesh_routing_v1 because
//     the ingested response lands as a transit next-hop — see
//     CapMeshRouteQueryV1 doc-comment in internal/core/domain/capability.go.
func localCapabilities() []domain.Capability {
	return []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapFileTransferV1,
		domain.CapMeshRouteProbeV1,
		domain.CapMeshRouteQueryV1,
	}
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
