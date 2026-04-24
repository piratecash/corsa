// routing_resolver.go hosts the address-resolution layer used by the
// announce and relay paths: resolveRoutableAddress / resolveRelayAddress /
// resolvePeerIdentity, plus the capability-matching predicates and the
// inbound-connection-key helpers. These are the "given an identity, find
// a transport address (and vice versa)" primitives that both wire paths
// depend on.
//
// The resolver is intentionally dependency-free: it only imports domain
// and walks the Service peer maps under s.peerMu.RLock. Callers that
// already hold s.peerMu must use inboundConnKeyFromInfo (not
// inboundConnKeyForID) to avoid the recursive-RLock deadlock described
// on those helpers.
//
// Companion files: announce-plane wire path in routing_announce.go,
// relay-plane forwarding in routing_relay.go, session-lifecycle routing
// hooks in routing_session.go, hop_ack route confirmation in
// routing_hop_ack.go, pending-queue drain in routing_drain.go. The
// per-file scope table is the durable record — see the
// "internal/core/node/" section of the file map in docs/routing.md.
package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
)

// inboundConnKeyForID derives the routing-key shape used for inbound
// AnnounceTarget addresses (and any other table entry that needs to
// distinguish an inbound conn from an outbound session) from a ConnID via
// the netcore.Network registry, without materialising a *netcore.NetCore
// handle. The "inbound:" prefix keeps these keys disjoint from outbound
// session addresses. An empty RemoteAddr (registry miss) yields the literal
// "inbound:" prefix; callers that care about a live connection must guard
// separately.
//
// WARNING: this helper goes through Network().RemoteAddr, which internally
// acquires s.peerMu.RLock. Callers MUST NOT hold s.peerMu (read or write)
// when calling this helper — the recursive RLock deadlocks under Go's
// writer-preferring sync.RWMutex semantics as soon as any writer queues
// between the outer and inner acquisition. For walker callbacks where
// s.peerMu.RLock is already held (forEachTrackedInboundConnLocked et al.),
// use inboundConnKeyFromInfo instead: it reads the snapshot's pre-captured
// remoteAddr and touches no mutex.
func (s *Service) inboundConnKeyForID(id domain.ConnID) domain.PeerAddress {
	return domain.PeerAddress("inbound:" + s.Network().RemoteAddr(id))
}

// inboundConnKeyFromInfo is the lock-free counterpart of inboundConnKeyForID.
// It builds the "inbound:<remoteAddr>" routing key from a connInfo snapshot
// that was already populated under s.peerMu. Because the remoteAddr string
// is copied into the snapshot at capture time, this helper acquires no lock
// and is safe to call from walker callbacks that already hold s.peerMu.RLock.
//
// Prefer this helper in every call site that iterates s.conns under
// s.peerMu — calling inboundConnKeyForID (which re-enters s.peerMu.RLock
// via Network().RemoteAddr) from such a site is a recursive-RLock deadlock
// waiting for the first concurrent writer.
func inboundConnKeyFromInfo(info connInfo) domain.PeerAddress {
	return domain.PeerAddress("inbound:" + info.remoteAddr)
}

// resolveRoutableAddress finds a transport address for a peer identity that
// can serve as a table-directed relay TRANSIT next-hop. The peer must have
// BOTH capabilities:
//   - mesh_routing_v1: understands announce_routes / routing table
//   - mesh_relay_v1:   can accept relay_message frames
//
// Use this for transit next-hops (hops > 1) where the peer must forward
// the message further using its own routing table. For destination peers
// (hops == 1), use resolveRelayAddress instead — they only need relay cap.
//
// Checks both outbound sessions and inbound connections. For inbound-only
// peers, the returned address is an "inbound:" prefixed key that must be
// handled by sendFrameToAddress (not enqueuePeerFrame directly).
func (s *Service) resolveRoutableAddress(peerIdentity domain.PeerIdentity) domain.PeerAddress {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	// Outbound sessions first — preferred because they have async send queues.
	for address, session := range s.sessions {
		if session.peerIdentity != peerIdentity {
			continue
		}
		if sessionHasBothCaps(session.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			return domain.PeerAddress(address)
		}
	}

	// Inbound connections — synchronous write path.
	var result domain.PeerAddress
	s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
		if info.identity != peerIdentity {
			return true
		}
		if sessionHasBothCaps(info.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			// Snapshot-based derivation: must not re-enter s.peerMu.RLock
			// via inboundConnKeyForID while the outer RLock is held.
			result = inboundConnKeyFromInfo(info)
			return false // Stop iteration
		}
		return true
	})

	return result
}

// resolveRelayAddress finds a transport address for a peer identity that
// can accept relay_message frames. Only requires mesh_relay_v1.
//
// Used for destination peers (direct routes, hops == 1) where the peer
// IS the final recipient and does not need to forward further.
//
// Checks both outbound sessions and inbound connections. For inbound-only
// peers, the returned address is an "inbound:" prefixed key.
func (s *Service) resolveRelayAddress(peerIdentity domain.PeerIdentity) domain.PeerAddress {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	// Outbound sessions first.
	for address, session := range s.sessions {
		if session.peerIdentity != peerIdentity {
			continue
		}
		if sessionHasCap(session.capabilities, domain.CapMeshRelayV1) {
			return domain.PeerAddress(address)
		}
	}

	// Inbound connections.
	var result domain.PeerAddress
	s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
		if info.identity != peerIdentity {
			return true
		}
		if sessionHasCap(info.capabilities, domain.CapMeshRelayV1) {
			// Snapshot-based derivation: must not re-enter s.peerMu.RLock
			// via inboundConnKeyForID while the outer RLock is held.
			result = inboundConnKeyFromInfo(info)
			return false // Stop iteration
		}
		return true
	})

	return result
}

// sessionHasCap returns true if the capability slice contains the given capability.
func sessionHasCap(caps []domain.Capability, cap domain.Capability) bool {
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

// sessionHasBothCaps returns true if the capability slice contains both a and b.
func sessionHasBothCaps(caps []domain.Capability, a, b domain.Capability) bool {
	var hasA, hasB bool
	for _, c := range caps {
		if c == a {
			hasA = true
		}
		if c == b {
			hasB = true
		}
		if hasA && hasB {
			return true
		}
	}
	return false
}

// resolvePeerIdentity returns the peer identity for a transport address.
// Checks both outbound sessions and inbound connections.
//
// For outbound sessions, the address is the session map key (e.g., "host:port").
// For inbound connections, the address is the connection's remote address
// (conn.RemoteAddr().String()). The returned value is the peer's Ed25519
// identity fingerprint.
func (s *Service) resolvePeerIdentity(address domain.PeerAddress) domain.PeerIdentity {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	// Outbound session: address is the session map key.
	if session := s.sessions[address]; session != nil {
		return session.peerIdentity
	}

	// Inbound connection: match on the connection's transport address.
	// Outbound NetCores are resolved via s.sessions above, so skip them
	// here — a pre-activation outbound entry must not answer identity
	// lookups before the session is installed.
	var result domain.PeerIdentity
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.remoteAddr == string(address) {
			result = info.identity
			return false // Stop iteration
		}
		return true
	})

	return result
}
