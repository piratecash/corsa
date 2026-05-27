package domain

import "strings"

// Capability is a typed token representing a negotiated protocol feature.
// Capabilities are exchanged during the hello/welcome handshake; only
// features present in both peer's sets are active for the session.
//
// The type is string-backed so that wire-format values (JSON, protocol
// frames) remain stable human-readable labels.
type Capability string

const (
	// CapMeshRelayV1 gates hop-by-hop relay (relay_message frames).
	CapMeshRelayV1 Capability = "mesh_relay_v1"

	// CapMeshRoutingV1 gates distance-vector routing via announce_routes
	// frames (Phase 1.2).
	CapMeshRoutingV1 Capability = "mesh_routing_v1"

	// CapMeshRoutingV2 gates the v2 routing announce path. Peers that
	// negotiate this capability receive incremental delta updates as
	// routes_update frames, while the first sync and any forced full
	// resync still travel as legacy announce_routes. CapMeshRoutingV2
	// is meaningful only when CapMeshRoutingV1 is also negotiated —
	// v2 is an opt-in refinement of the v1 control plane, not a
	// replacement; peers that advertise v2 without v1 are treated as
	// legacy (v1-only) because the receive path for the first sync
	// (announce_routes) is gated on v1. The request_resync frame is
	// also gated on v2, since only v2 peers can receive routes_update
	// and therefore need the escape hatch.
	CapMeshRoutingV2 Capability = "mesh_routing_v2"

	// CapFileTransferV1 gates file transfer commands (Iteration 21).
	// Only peers advertising this capability receive or relay
	// FileCommandFrame traffic. The file_announce DM is not gated
	// because it travels through the standard DM pipeline.
	CapFileTransferV1 Capability = "file_transfer_v1"

	// CapMeshRouteProbeV1 gates active reachability probes
	// (route_probe_v1 / route_probe_ack_v1 wire frames) introduced
	// in Phase 2 (docs/protocol/route_health.md). Probes verify that
	// a (target identity, uplink peer) pair is currently usable
	// without waiting for organic traffic to generate a hop_ack.
	// Only peers advertising this capability receive probes from us.
	//
	// Mixed-version interop. A peer with mesh_routing_v1 but without
	// this capability is NOT excluded from the Phase 2 ranking model:
	// every accepted (Identity, Uplink) claim is still seeded as
	// Questionable in our local RouteHealthState and aged by the
	// passive idle timeline, regardless of the announcing peer's
	// capabilities (see Table.UpdateRoute and docs/routing.md
	// "Capability gating"). What this capability gates is strictly
	// the active probe send path — pairs we observe but cannot
	// probe stay Questionable until either organic relay traffic
	// emits a hop_ack or the passive timeline escalates them
	// further. CompositeScore ranks them with the standard
	// Questionable penalty, so they sit below confirmed
	// alternatives but remain selectable.
	CapMeshRouteProbeV1 Capability = "mesh_route_probe_v1"

	// CapMeshRouteQueryV1 gates targeted single-hop route queries
	// (route_query_v1 / route_query_response_v1 wire frames)
	// introduced in Phase 2. Queries trigger an on-demand lookup of
	// the best route to a target identity from a directly connected
	// peer, used for fast recovery after all known uplinks for that
	// identity transition to Bad/Dead health. Queries are
	// rate-limited to 3 per target per 30s and never forwarded —
	// they are strictly single-hop.
	//
	// Fan-out triplet. A peer is eligible as a route_query_v1
	// fan-out target only if it advertises the FULL triplet:
	// mesh_route_query_v1 + mesh_relay_v1 + mesh_routing_v1.
	// route_query_v1 alone would let the peer answer queries, but
	// the ingested response always lands as a transit next-hop
	// (Hops = BestHops + 1) and requires both mesh_relay_v1 (to
	// forward relay_message frames addressed to us through that
	// peer) and mesh_routing_v1 (to act as transit at all). The
	// sender filters candidates accordingly in peersWithRouteQueryCap
	// (internal/core/node/routing_query_sender.go).
	CapMeshRouteQueryV1 Capability = "mesh_route_query_v1"

	// CapMeshRouteSyncV1 gates incremental table sync via the
	// route_sync_digest_v1 / route_sync_summary_v1 wire frames
	// introduced in Phase 3 PR 12.5
	// (docs/cluster-mesh/phase-3-multipath-reputation.md §4.5). On
	// reconnect to a known peer the sender emits its last-observed
	// digest of the (Identity, MaxSeqNo) pairs reachable through
	// that peer; the receiver compares against its current table
	// and replies with match=true/false. A match short-circuits
	// the next forced full-sync to that peer, saving the full
	// announce payload on the common "reconnect within 5 min and
	// nothing changed" case. A mismatch is a no-op; the normal
	// announce cycle still emits a full snapshot.
	//
	// The capability is ORTHOGONAL to the announce-plane caps
	// (mesh_routing_v1 / mesh_routing_v2 / future mesh_routing_v3).
	// Peers without it negotiate the digest exchange away and
	// continue receiving the full announce stream unchanged; the
	// suppression only applies between pairs that both negotiated
	// mesh_route_sync_v1. The digest itself is local-only metadata
	// (no trust transferred) — a mismatched or absent reply is
	// harmless because the announce path remains the authoritative
	// source of routing state. See the Phase 3 plan §4.5 for the
	// full protocol contract and §2.4 for the wire-vs-RPC
	// separation invariant.
	CapMeshRouteSyncV1 Capability = "mesh_route_sync_v1"
)

// String returns the stable string label for the capability.
func (c Capability) String() string { return string(c) }

// ParseCapability converts a string to a Capability.
// Returns the capability and true on success, or empty string and false
// for unrecognised names.
func ParseCapability(s string) (Capability, bool) {
	c := Capability(strings.ToLower(s))
	switch c {
	case CapMeshRelayV1, CapMeshRoutingV1, CapMeshRoutingV2, CapFileTransferV1, CapMeshRouteProbeV1, CapMeshRouteQueryV1, CapMeshRouteSyncV1:
		return c, true
	default:
		return "", false
	}
}

// ParseCapabilities converts a list of capability name strings into a
// typed slice. Unknown names are silently ignored.
func ParseCapabilities(names []string) []Capability {
	caps := make([]Capability, 0, len(names))
	for _, name := range names {
		if c, ok := ParseCapability(name); ok {
			caps = append(caps, c)
		}
	}
	return caps
}

// CapabilityStrings converts a typed capability slice back to raw strings.
// Used at protocol/JSON/RPC boundaries.
func CapabilityStrings(caps []Capability) []string {
	if len(caps) == 0 {
		return nil
	}
	out := make([]string, len(caps))
	for i, c := range caps {
		out[i] = string(c)
	}
	return out
}
