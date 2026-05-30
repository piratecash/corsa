package node

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"

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
//   - mesh_route_sync_v1: incremental table sync via the
//     route_sync_digest_v1 / route_sync_summary_v1 exchange
//     introduced in Phase 3 PR 12.5. On reconnect to a known peer
//     the sender emits a digest of its last-known (Identity,
//     MaxSeqNo) view through that peer; on match the receiver
//     short-circuits the next forced full-sync. Orthogonal to the
//     announce-plane caps — peers without it keep receiving the
//     full announce stream as before. See
//     docs/cluster-mesh/phase-3-multipath-reputation.md §4.5.
//   - mesh_routing_v3: Phase 4 compact announce wire frame
//     (route_announce_v3). Conditionally added when enableV3 is true
//     (operator opt-in via CORSA_ENABLE_MESH_ROUTING_V3, default
//     false). The flag is threaded as a parameter rather than read
//     from a package-level global so a Service constructed with v3
//     disabled never accidentally advertises v3 because some other
//     Service in the same process flipped a global. See
//     docs/cluster-mesh/phase-4-compact-wire-signed.md §7 PR 13.1
//     part 2c.
func localCapabilities(enableV3 bool) []domain.Capability {
	caps := []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapFileTransferV1,
		domain.CapMeshRouteProbeV1,
		domain.CapMeshRouteQueryV1,
		domain.CapMeshRouteSyncV1,
	}
	if enableV3 {
		caps = append(caps, domain.CapMeshRoutingV3)
		// Phase 4 13.2 attested-links advertise is INTENTIONALLY NOT
		// piggybacked here. The earlier plan coupled the advertise to
		// the v3 opt-in for a single-knob rollout, but a Round-7
		// review surfaced that the emitter path produces no real
		// signed entries: signOwnOriginV3Entries only signs entries
		// whose Identity == localIdentity, and the production wire
		// projection (route_store.AnnounceProjectionFor) iterates
		// stored buckets and never emits the local identity (the
		// synthetic self-route lives in Lookup/Snapshot, not in
		// AnnounceProjectionFor). The advertise therefore promised a
		// signed-announcement contract that no v3 frame on the wire
		// actually delivered — peers' trust-score logic would never
		// see a signed bonus and the Phase 5 anchor-publication
		// prereq depended on a path that did not exist.
		//
		// The infrastructure stays in place (signOwnOriginV3Entries +
		// verifyRouteAnnounceV3Sigs + AttestedSig storage round-trip
		// + scoreSignedBonus + the capability constant itself) so
		// Phase 5 can re-enable the advertise the same day it wires
		// the self-attestation entry stream that puts Identity ==
		// localIdentity entries (with a per-emitter SeqNo and the
		// anchor metadata in Extra) into the v3 emit path. Until
		// then, advertising the cap would be dishonest — see
		// docs/protocol/attested_links.md "Production advertisement
		// status" for the full contract.
		// caps = append(caps, domain.CapMeshAttestedLinksV1)

		// Phase 4 13.3 piggybacks poison-reverse on the same opt-in
		// flag for the same single-knob rollout rationale.
		// Advertising the capability means "I can receive and
		// invalidate per route_poison_v1"; the emit side
		// (SendRoutePoison call sites tied to session-close /
		// health-dead / loop-detected hooks) lands in 13.3-B as a
		// separate behavioural opt-in. The receive path is fully
		// wired here (handleRoutePoison + Table.InvalidateUplinkClaim),
		// so advertising the cap is honest about what this node will
		// do with incoming poison frames.
		caps = append(caps, domain.CapMeshPoisonReverseV1)
	}
	return caps
}

// localCapabilityStrings returns the wire-format string list for the hello/
// welcome frame. Used at the protocol boundary where Frame.Capabilities
// is []string. The enableV3 flag is threaded through to localCapabilities
// — see that function's doc for the opt-in contract.
func localCapabilityStrings(enableV3 bool) []string {
	return domain.CapabilityStrings(localCapabilities(enableV3))
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

// peerSupportsRoutingV3 reports whether the peer reachable at address
// has the FULL v3 wire triplet negotiated: CapMeshRoutingV1 +
// CapMeshRoutingV3 + CapMeshRelayV1. Used by the connect-time /
// forced-full full-sync paths to pick between the legacy
// announce_routes frame and the Phase 4 compact route_announce_v3
// kind="full" frame (overview §7.1).
//
// Round-19 fix: relay was added to the predicate. The send-side
// `SendRouteAnnounceV3` dispatch in dispatchAnnouncePlaneFrameWithCaps
// requires v1+v3+relay (same triplet the inbound /
// outbound dispatchers gate the receive path on), so a relay-less
// peer that passed the old v1+v3 helper check would silently fail
// the send-side cap gate with no legacy fallback — the connect-time
// sync would simply drop the full snapshot. Aligning the helper with
// the actual send-side gate makes the fallback explicit: if the
// triplet is missing, peerSupportsRoutingV3 returns false and
// sendConnectTimeFullSync routes through legacy SendAnnounceRoutes
// instead.
//
// Handles both address shapes the announce-plane uses:
//   - Outbound session address → consult s.sessions and read
//     session.capabilities under s.peerMu.RLock (same pattern as
//     sessionHasCapability).
//   - "inbound:remoteAddr" prefix → walk tracked inbound conns under
//     s.peerMu.RLock and match on remoteAddr.
//
// Returns false when the address resolves to no live transport, which
// keeps the caller on the legacy frame rather than silently dropping
// the full-sync attempt.
func (s *Service) peerSupportsRoutingV3(address domain.PeerAddress) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if strings.HasPrefix(string(address), "inbound:") {
		remoteAddr := strings.TrimPrefix(string(address), "inbound:")
		var supports bool
		s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
			if info.remoteAddr != remoteAddr {
				return true
			}
			supports = capsContain(info.capabilities, domain.CapMeshRoutingV1) &&
				capsContain(info.capabilities, domain.CapMeshRoutingV3) &&
				capsContain(info.capabilities, domain.CapMeshRelayV1)
			return false // stop iteration
		})
		return supports
	}
	session := s.resolveSessionLocked(address)
	if session == nil {
		return false
	}
	return capsContain(session.capabilities, domain.CapMeshRoutingV1) &&
		capsContain(session.capabilities, domain.CapMeshRoutingV3) &&
		capsContain(session.capabilities, domain.CapMeshRelayV1)
}

// capsContain reports whether the cap slice contains target. Small
// helper used by the v3 admission check; not exposed because the rest
// of the package already uses sessionHasCap-shaped helpers that need
// peerMu separately.
func capsContain(caps []domain.Capability, target domain.Capability) bool {
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

// peerSupportsAttestedLinks reports whether the peer reachable at
// address has CapMeshAttestedLinksV1 negotiated. Mirrors
// peerSupportsRoutingV3 for both address shapes (outbound session and
// "inbound:remoteAddr" prefix). Used by handleRouteAnnounceV3 to gate
// the Phase 4 13.2-B / 13.2-C verifier path: when the cap is NOT
// negotiated, the receiver treats incoming sig bytes as informational
// only (no ed25519.Verify, no entry drop on invalid, no trust-score
// bonus on success). This matches the Tier-2 contract documented in
// docs/protocol/attested_links.md "Capability negotiation".
func (s *Service) peerSupportsAttestedLinks(address domain.PeerAddress) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if strings.HasPrefix(string(address), "inbound:") {
		remoteAddr := strings.TrimPrefix(string(address), "inbound:")
		var supports bool
		s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
			if info.remoteAddr != remoteAddr {
				return true
			}
			supports = capsContain(info.capabilities, domain.CapMeshAttestedLinksV1)
			return false // stop iteration
		})
		return supports
	}
	session := s.resolveSessionLocked(address)
	if session == nil {
		return false
	}
	return capsContain(session.capabilities, domain.CapMeshAttestedLinksV1)
}

// publicKeyForIdentity looks up the Ed25519 public key the knowledge
// store holds for the given identity fingerprint and decodes it from the
// stored base64 form. Returns (key, true) on a hit with a structurally
// valid key; (nil, false) on miss, malformed base64, or wrong key
// length. Used by the Phase 4 13.2-B route_announce_v3 verifier to
// resolve the destination identity's pubkey for ed25519.Verify; on miss
// the verifier treats the signature as unverified (Tier-2 lenient — see
// docs/protocol/attested_links.md "Receive contract").
//
// Threading: takes knowledgeMu.RLock for the map read; the base64 decode
// runs outside the lock since the stored string is immutable.
func (s *Service) publicKeyForIdentity(identity domain.PeerIdentity) (ed25519.PublicKey, bool) {
	if identity == "" {
		return nil, false
	}
	s.knowledgeMu.RLock()
	encoded := s.pubKeys[string(identity)]
	s.knowledgeMu.RUnlock()
	if encoded == "" {
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, false
	}
	return ed25519.PublicKey(raw), true
}
