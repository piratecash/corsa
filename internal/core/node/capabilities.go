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
//     (CORSA_ENABLE_MESH_ROUTING_V3, default TRUE; operators opt out
//     with =0/false/no/off). The flag is threaded as a parameter
//     rather than read from a package-level global so a Service
//     constructed with v3 disabled never accidentally advertises v3
//     because some other Service in the same process flipped a global.
//     See docs/cluster-mesh/phase-4-compact-wire-signed.md §7.
//   - mesh_datagram_v1 / mesh_datagram_transit_v1: the datagram transport
//     layer, gated by datagrams (see datagramAdvertise). Threaded as a
//     parameter for the same reason enableV3 is: the advertise must be a
//     property of THIS Service, not of a package global some other Service
//     in the process flipped. See
//     docs/refactoring/datagram-transport.md §6.
func localCapabilities(enableV3 bool, datagrams datagramAdvertise) []domain.Capability {
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
		// Batched poison-reverse (route_poison_v2): advertising it means
		// "I can receive and apply a list-of-identities poison frame". The
		// emit side only uses it toward peers that also advertise it, and
		// falls back to per-identity v1 otherwise (poisonReverseToOtherPeers).
		caps = append(caps, domain.CapMeshPoisonReverseV2)
	}
	// The two datagram capabilities are advertised INDEPENDENTLY of each
	// other and of every routing capability (§6). Endpoint support says
	// "datagrams addressed to me are welcome"; transit support says "I
	// will carry other people's". A client node advertises the first and
	// never the second, so an honest neighbour never picks it as a relay.
	if datagrams.Endpoint {
		caps = append(caps, domain.CapMeshDatagramV1)
	}
	if datagrams.Transit {
		caps = append(caps, domain.CapMeshDatagramTransitV1)
	}
	return caps
}

// localCapabilityStrings returns the wire-format string list for the hello/
// welcome frame. Used at the protocol boundary where Frame.Capabilities
// is []string. The enableV3 flag and the datagram advertise are threaded
// through to localCapabilities — see that function's doc for both opt-in
// contracts.
func localCapabilityStrings(enableV3 bool, datagrams datagramAdvertise) []string {
	return domain.CapabilityStrings(localCapabilities(enableV3, datagrams))
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

// sessionHasCapability returns true when THIS outbound peer session — the one
// that actually carried the frame — negotiated the capability during ITS
// handshake.
//
// It takes the session and not its address because on a receive path those are
// two different questions and only one of them is the right one. A reconnect
// registers a replacement session under the SAME dial address while the
// previous session's goroutines are still unwinding (the ownedCleanup block in
// onCMSessionEstablished exists precisely for that overlap), so an
// address-keyed lookup taken from a frame that arrived on the old session
// answers about the new one — and both directions of that answer are wrong: a
// frame behind a capability the peer never declared on THIS connection gets
// accepted, or a legitimate frame gets dropped because an unrelated socket
// handshook differently.
//
// The address-keyed form is sendTargetHasCapability, which asks the other
// question — see its doc. Splitting them by PARAMETER TYPE rather than by
// discipline is deliberate: a receive path can no longer reach the wrong one by
// accident, because a domain.PeerAddress does not compile here.
//
// peerMu is what makes reading the fields ordered rather than merely plausible:
// applyWelcomeMetadata writes session.capabilities and
// markSessionHandshakeComplete writes session.authOK, both under peerMu
// (peer_sessions.go), and readPeerSession — one of this helper's callers — is
// started BEFORE either of them, so a peer that pipelines a frame behind its own
// welcome has the reader here while the handshake goroutine is still assigning.
//
// A capability is NEGOTIATED only once the handshake has COMPLETED, and that is
// a different question from whether the set has been assigned. Conflating them
// left a real window on the wire: applyWelcomeMetadata publishes the
// intersection the moment the welcome validates, while auth_ok — the last step
// of this direction's handshake — arrives one round trip later, and the reader
// is dispatching throughout. A `datagram` or a `file_command` pipelined into
// that window therefore reached the datagram pipeline and the file router on a
// connection whose handshake had not finished, and the announce-plane arms of
// dispatchPeerSessionFrame were reachable the same way. Requiring authOK here
// closes all of them at the one point they already ask the question, and states
// the same rule the INBOUND direction enforces a layer higher, where
// dispatchNetworkFrame answers auth_required to every p2pWireCommand arriving on
// an unauthenticated connection.
//
// The refusal drops the frame rather than deferring it, and that costs nothing
// legitimate. No conforming peer may put plane traffic in this window — its own
// inbound reader refuses ours identically — so a frame landing here is one the
// sender was never entitled to send; and holding the window's frames back would
// mean a per-session queue whose size the neighbour alone decides. §2 already
// makes every refusal of the datagram plane a silent drop, and the announce
// plane self-heals through the forced periodic full sync and route TTL.
//
// The address-keyed siblings (sendTargetHasCapability, peerSupportsRoutingV3,
// peerSupportsAttestedLinks, sessionDeclarations) need no such gate: they resolve
// through s.sessions, and both outbound paths register a session there only after
// authenticatePeerSession has returned — openPeerSession inline, the CM path in
// the onCMSessionEstablished goroutine.
func (s *Service) sessionHasCapability(session *peerSession, capability domain.Capability) bool {
	if session == nil {
		return false
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	if !session.authOK {
		return false
	}
	return capsContain(session.capabilities, capability)
}

// sendTargetHasCapability returns true when a frame handed to address RIGHT NOW
// would leave over an outbound session that negotiated the capability.
//
// This is the SEND-side question and the only one an address can answer: the
// caller holds a routing decision, not a connection, and what it needs to know
// is whether the socket the send will pick is capable. Resolving the address is
// therefore not a defect here — it IS the question, and the gate and the send
// agree because both key on the same address (see tryForwardToDirectPeer, which
// gates and then enqueues on one address).
//
// Never use it to judge a frame that has ALREADY arrived: there the delivering
// session is in hand and sessionHasCapability is the helper.
func (s *Service) sendTargetHasCapability(address domain.PeerAddress, capability domain.Capability) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	session := s.resolveSessionLocked(address)
	if session == nil {
		return false
	}
	return capsContain(session.capabilities, capability)
}

// sessionPeerIdentity returns the authenticated neighbour behind THIS session —
// the identity every budget, ban and metric of a received frame is charged to.
//
// It exists for the same ordering reason sessionHasCapability takes peerMu:
// peerIdentity is written by applyWelcomeMetadata (under peerMu) AFTER
// readPeerSession is already running, so the receive path must not read the
// field straight off the struct. A session that has not reached that write
// answers the zero identity, which every caller already treats as
// "unauthenticated, refuse".
//
// It deliberately does NOT carry the completed-handshake requirement
// sessionHasCapability grew. The two answer different questions: the capability
// is a NEGOTIATION, and a negotiation is not in force until the handshake ends,
// while the identity is a CLAIM the welcome carried and is no truer after
// auth_ok than before it (nothing on the dialled direction proves it — see the
// datagramNeighbour doc). Gating it too would only relabel a refusal the
// capability gate has already made, and it would relabel it wrongly: a frame
// refused for arriving off the plane would be counted as one from a peer that
// named no identity.
func (s *Service) sessionPeerIdentity(session *peerSession) domain.PeerIdentity {
	if session == nil {
		return domain.PeerIdentity{}
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return session.peerIdentity
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
//
// info.capabilities is now a READ-ONLY alias of NetCore storage (see
// snapshotEntryLocked / the connInfo type doc). This accessor still hands its
// callers an OWNED copy: it feeds onPeerSessionEstablished/Closed, which may
// retain the slice in routing-announce state, and it runs only on session
// establish / close (not a hot path), so the copy is cheap and keeps callers
// free of the "must not mutate the alias" constraint.
func (s *Service) connCapabilitiesForID(id domain.ConnID) []domain.Capability {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	info, ok := s.connInfoByIDLocked(id)
	if !ok || len(info.capabilities) == 0 {
		return nil
	}
	return append([]domain.Capability(nil), info.capabilities...)
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
	if identity.IsZero() {
		return nil, false
	}
	s.knowledgeMu.RLock()
	encoded := s.pubKeys[identity.String()]
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
