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
	// (mesh_routing_v1 / mesh_routing_v2 / mesh_routing_v3 — all
	// shipped).
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

	// CapMeshRoutingV3 gates the Phase 4 compact announce wire frame
	// (route_announce_v3) introduced in
	// docs/cluster-mesh/phase-4-compact-wire-signed.md §3.1 (overview
	// §7.1). The v3 frame drops the redundant Origin field that legacy
	// announce_routes / routes_update carry — the receiver derives the
	// uplink from the sending session, so Origin conveys no
	// routing-relevant information after the Phase 1 per-uplink storage
	// refactor (overview §4.1). A single route_announce_v3 frame carries
	// both full sync and delta via its kind discriminator, and a local
	// epoch counter lets a receiver detect a peer table reset (process
	// restart) and request a fresh baseline instead of diffing against a
	// stale one.
	//
	// Mixed-version interop. v3 is preferred only when BOTH peers
	// advertise it; otherwise the sender falls back to routes_update
	// (mesh_routing_v2) or announce_routes (mesh_routing_v1) exactly as
	// before — see classifyDeltaMode / the wire-format selection in
	// internal/core/routing/announce.go and overview §5.2 Phase B. Like
	// v2, v3 is meaningful only when v1 is also negotiated: v1 gates the
	// receive path the legacy fallback depends on, and a peer advertising
	// v3 without v1 is treated as legacy. v3 is additive — it never
	// raises MinimumProtocolVersion; legacy ingest stays alive until the
	// Phase 6 cleanup window (overview §5.2 Phase C).
	CapMeshRoutingV3 Capability = "mesh_routing_v3"

	// CapMeshAttestedLinksV1 gates Ed25519-signed route announcements
	// (Phase 4) introduced in
	// docs/cluster-mesh/phase-4-compact-wire-signed.md §3.2 (overview
	// §7.1 "sig" field). When negotiated alongside CapMeshRoutingV3,
	// each route_announce_v3 entry MAY carry the origin identity's
	// signature over the entry's canonical bytes
	// (RouteAnnounceV3Entry.CanonicalSigningBytes). The signature
	// proves that the destination identity itself sanctioned the
	// (Identity, Extra) shape of the announcement — hops, epoch, and
	// seq_no are all per-emitter wire fields that transit restamp on
	// re-emit and are therefore EXCLUDED from the canonical signing
	// payload (see CanonicalSigningBytes doc for the multi-hop
	// verification rationale). Transit nodes forward the sig bytes
	// verbatim so the signature remains verifiable after the entry
	// has crossed nodes that do not understand the payload.
	//
	// Verification is locally enforced (zero-trust budget, overview
	// §4.2): a present-but-invalid signature is treated as evidence
	// the entry must NOT be accepted; an absent signature is allowed
	// at Tier 2 but ranked lower by the trust score, and is
	// MANDATORY for entries that participate in Phase 5 anchor
	// publication (identity_record_v1). This capability is the wire
	// prerequisite that lets Phase 5 require attested links without
	// breaking pre-existing peers.
	//
	// Mixed-version interop. Like every Phase 4 capability,
	// CapMeshAttestedLinksV1 is additive. The wire-level treatment
	// of the sig field is:
	//
	//   - The sig field on route_announce_v3 entries is forwarded
	//     verbatim regardless of negotiation — transit nodes do not
	//     strip sigs (they cannot know which downstream peers will
	//     verify), and the origin emits sig whenever its store has
	//     one. A peer without the capability simply observes the
	//     extra field and ignores it.
	//   - What the capability gates is RECEIVER-SIDE behaviour:
	//     when negotiated, the receiver runs the ed25519 verifier
	//     against the destination identity's public key and applies
	//     the trust-score bonus (scoreSignedBonus) on success;
	//     present-but-invalid signatures drop the entry. When NOT
	//     negotiated, the receiver treats sig bytes as informational
	//     only — no verification, no trust-score impact, the entry
	//     ranks identically to an unsigned one.
	//
	// We never reject an unsigned announcement solely because the
	// cap is absent (Tier 2 leniency). The capability is meaningful
	// only when CapMeshRoutingV3 is also negotiated:
	// route_announce_v3 is the only frame that carries the sig
	// field. See docs/protocol/attested_links.md "Capability
	// negotiation" for the wire contract.
	CapMeshAttestedLinksV1 Capability = "mesh_attested_links_v1"

	// CapMeshPoisonReverseV1 gates Phase 4 explicit poison-reverse
	// signals (route_poison_v1 wire frame) introduced in
	// docs/cluster-mesh/phase-4-compact-wire-signed.md §3.3 (overview
	// §7.7). A transit node that loses its upstream for an identity
	// — or whose health for that identity transitions to dead, or
	// who detects a loop — emits route_poison_v1 to its direct
	// neighbours, naming the lost identity and the reason. The
	// neighbour receiver invalidates ONLY its claims[identity][sender]
	// slot (not other uplinks for the same identity, and never the
	// origin's own claim — overview §4.2 zero-trust budget).
	//
	// This replaces the implicit hops=N+1 oscillation a count-to-
	// infinity scenario produces: instead of waiting TTL for stale
	// claims to expire while routes bounce up the hop ladder, an
	// explicit poison-reverse collapses the bad path in one
	// announce cycle, sharply accelerating convergence.
	//
	// Mixed-version interop. Like every Phase 4 capability,
	// CapMeshPoisonReverseV1 is additive — peers without it never
	// see or emit route_poison_v1 frames, and the legacy convergence
	// path (TTL expiry + announce cycles) remains unchanged for
	// them. Single-hop only: route_poison_v1 is NEVER forwarded by
	// the receiver; a transit node that wants to propagate the
	// poison further must emit its own frame after invalidating
	// its own claim (which then signals via the normal route_announce
	// flow at hops=HopsInfinity).
	CapMeshPoisonReverseV1 Capability = "mesh_poison_reverse_v1"
)

// String returns the stable string label for the capability.
func (c Capability) String() string { return string(c) }

// ParseCapability converts a string to a Capability.
// Returns the capability and true on success, or empty string and false
// for unrecognised names.
func ParseCapability(s string) (Capability, bool) {
	c := Capability(strings.ToLower(s))
	switch c {
	case CapMeshRelayV1, CapMeshRoutingV1, CapMeshRoutingV2, CapFileTransferV1, CapMeshRouteProbeV1, CapMeshRouteQueryV1, CapMeshRouteSyncV1, CapMeshRoutingV3, CapMeshAttestedLinksV1, CapMeshPoisonReverseV1:
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
