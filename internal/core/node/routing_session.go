// routing_session.go hosts routing-related session lifecycle hooks:
// onPeerSessionEstablished / onPeerSessionClosed that register or withdraw
// direct-peer routes, publish RouteTableChanged events, and coordinate the
// per-peer announce state registry across connect/disconnect transitions.
// inboundPeerIdentity lives here because it is the routing-side bridge
// from ConnID to peer identity used by session-tied code paths.
//
// The disconnect path in onPeerSessionClosed is the site that invokes the
// parallel withdrawal fan-out documented in the "Disconnect withdrawal
// fan-out (parallel contract)" section of docs/routing.md; the fan-out
// helper itself (fanoutAnnounceRoutes) lives in routing_announce.go so
// that the announce-plane write path is in one place.
//
// Companion files: announce-plane wire path in routing_announce.go,
// relay-plane forwarding in routing_relay.go, address↔identity resolution
// in routing_resolver.go, hop_ack route confirmation in routing_hop_ack.go,
// pending-queue drain in routing_drain.go. The per-file scope table is
// the durable record — see the "internal/core/node/" section of the
// file map in docs/routing.md.
package node

import (
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// inboundPeerIdentity returns the peer identity (Ed25519 fingerprint)
// for an inbound connection, derived from the hello frame's Address field.
// This is distinct from NetCore.Address() which stores the listen
// address (transport) for health tracking. NATed peers advertise a
// non-routable listen address (e.g. 127.0.0.1:64646) that must never
// be used as a routing identity.
func (s *Service) inboundPeerIdentity(id domain.ConnID) domain.PeerIdentity {
	pc := s.netCoreForID(id)
	if pc == nil {
		return ""
	}
	return pc.Identity()
}

// onPeerSessionEstablished is called when a session is fully established,
// for both outbound sessions (after handshake, auth, and sync) and inbound
// connections (after hello exchange). It registers the peer as a direct
// route in the routing table and triggers an immediate announcement cycle.
//
// caps is the peer's full negotiated capability set captured at session
// establishment. The hook flattens it to a relay-cap boolean for its own
// direct-route decision, but also forwards the full list to the announce
// state registry so routing-announce v2 can make mode decisions without a
// second s.peerMu round-trip. Only relay-capable peers become direct routes
// — a direct route advertises that this node can deliver relay_message to
// the destination. Without relay capability the route is non-deliverable;
// announcing it would cause other routing-capable nodes to learn a path
// that fails at the last hop. When relay cap is absent, session counting
// still occurs (so that onPeerSessionClosed can safely decrement) but no
// routing table entry is created and no announcement is triggered.
//
// Multi-session awareness: AddDirectPeer is called only on the 0→1
// transition (first session for this identity). Subsequent sessions for
// the same identity increment the session count but do not touch the
// routing table — AddDirectPeer is idempotent at the model level, but
// the session-count gate here is the primary defense against churn.
func (s *Service) onPeerSessionEstablished(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	if peerIdentity == "" {
		return
	}

	hasRelayCap := sessionHasCap(caps, domain.CapMeshRelayV1)

	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_wait").Str("peer_identity", string(peerIdentity)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_held").Str("peer_identity", string(peerIdentity)).Msg("peer_mu_writer")
	s.identitySessions[peerIdentity]++

	if hasRelayCap {
		s.identityRelaySessions[peerIdentity]++
	}
	firstRelay := s.identityRelaySessions[peerIdentity] == 1
	s.peerMu.Unlock()
	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_released").Str("peer_identity", string(peerIdentity)).Msg("peer_mu_writer")

	if !hasRelayCap {
		log.Debug().
			Str("peer", string(peerIdentity)).
			Msg("routing_peer_no_relay_cap_skip_direct_route")
		return
	}

	if !firstRelay {
		// Routing table is unchanged (the direct route already exists for
		// this identity), no announce trigger fires, and the persistent
		// capability snapshot in AnnouncePeerState is intentionally NOT
		// touched here. Caps are reconciled at cycle start in
		// AnnounceLoop.announceToAllPeers, where the registry snapshot is
		// synced to the per-cycle AnnounceTarget caps (i.e. the same
		// session routingCapablePeers actually picked). Updating caps in
		// this hook from caps unrelated to that target — e.g. a relay-only
		// session that routingCapablePeers cannot pick, or an overlapping
		// session with different v2 support than the older one that ends
		// up being chosen — only adds a window of inconsistency between
		// state.capabilities and the cycle-time target. The cycle-time
		// sync is the single point of truth for persistent caps; no
		// lifecycle-hook variant can match it without re-implementing the
		// same selection rule routingCapablePeers uses.
		log.Debug().
			Str("peer", string(peerIdentity)).
			Msg("routing_additional_session_no_table_update")
		return
	}

	result, err := s.routingTable.AddDirectPeer(peerIdentity)
	if err != nil {
		log.Error().Err(err).Str("peer", string(peerIdentity)).Msg("routing_add_direct_peer_failed")
		return
	}

	log.Info().
		Str("peer", string(peerIdentity)).
		Uint64("seq", result.Entry.SeqNo).
		Bool("penalized", result.Penalized).
		Msg("routing_direct_peer_added")

	s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason:   domain.RouteChangeDirectPeerAdded,
		PeerID:   peerIdentity,
		Accepted: 1,
	})

	// Register or reset per-peer announce state for this routing-capable peer.
	// The registry takes a defensive copy of caps so subsequent mutation of
	// the session's capability slice cannot leak into AnnouncePeerState.
	s.announceLoop.StateRegistry().MarkReconnected(peerIdentity, caps)

	// When flap detection penalizes a peer, skip the triggered announce.
	// The route will be picked up by the next periodic announce cycle,
	// giving the link time to stabilize.
	if result.Penalized {
		log.Warn().
			Str("peer", string(peerIdentity)).
			Msg("routing_peer_in_holddown_skipping_announce")
		return
	}

	s.announceLoop.TriggerUpdate()
}

// onPeerSessionClosed is called when a session for a peer identity closes.
// caps must describe the same capability set that was passed to
// onPeerSessionEstablished for this session so that the relay-capable
// session counter stays balanced.
//
// Multi-session awareness: RemoveDirectPeer is called on the relay-session
// 1→0 transition (last relay-capable session for this identity). Total
// session count cleanup happens on the total-session 1→0 transition.
//
// On disconnect, own-origin direct routes are withdrawn on the wire
// (returned as wire-ready AnnounceEntry items) and transit routes
// learned through this peer are silently invalidated locally.
func (s *Service) onPeerSessionClosed(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	if peerIdentity == "" {
		return
	}

	hadRelayCap := sessionHasCap(caps, domain.CapMeshRelayV1)

	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_wait").Str("peer_identity", string(peerIdentity)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_held").Str("peer_identity", string(peerIdentity)).Msg("peer_mu_writer")
	count := s.identitySessions[peerIdentity]
	if count <= 0 {
		s.peerMu.Unlock()
		log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_released").Str("peer_identity", string(peerIdentity)).Str("reason", "no_session").Msg("peer_mu_writer")
		return
	}
	s.identitySessions[peerIdentity]--
	isLastTotal := s.identitySessions[peerIdentity] == 0
	if isLastTotal {
		delete(s.identitySessions, peerIdentity)
	}

	lastRelay := false
	if hadRelayCap {
		relayCount := s.identityRelaySessions[peerIdentity]
		if relayCount > 0 {
			s.identityRelaySessions[peerIdentity]--
			lastRelay = s.identityRelaySessions[peerIdentity] == 0
			if lastRelay {
				delete(s.identityRelaySessions, peerIdentity)
			}
		}
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_released").Str("peer_identity", string(peerIdentity)).Bool("last_total", isLastTotal).Bool("last_relay", lastRelay).Msg("peer_mu_writer")

	if !lastRelay {
		// No relay sessions left (or never had one). If this is also the
		// last total session, invalidate any transit routes learned through
		// this peer. This covers the routing-only peer case (mesh_routing_v1
		// without mesh_relay_v1): the receive-path gate blocks new
		// announcements, but defense-in-depth ensures stale lineages are
		// cleaned up on disconnect.
		if isLastTotal {
			invalidated, exposed := s.routingTable.InvalidateTransitRoutes(peerIdentity)
			if invalidated > 0 {
				log.Info().
					Str("peer", string(peerIdentity)).
					Int("transit_invalidated", invalidated).
					Msg("routing_transit_routes_invalidated_on_disconnect")

				s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
					Reason:    domain.RouteChangeTransitInvalidated,
					PeerID:    peerIdentity,
					Withdrawn: invalidated,
				})

				// Trigger an immediate announce cycle so neighbors learn the
				// invalidated routes are withdrawn. Without this, stale routes
				// persist until the next periodic cycle (up to 30s).
				s.announceLoop.TriggerUpdate()
			}
			s.triggerDrainForExposed(exposed)
		} else {
			log.Debug().
				Str("peer", string(peerIdentity)).
				Int("remaining", count-1).
				Msg("routing_session_closed_others_remain")
		}
		return
	}

	result, err := s.routingTable.RemoveDirectPeer(peerIdentity)
	if err != nil {
		log.Error().Err(err).Str("peer", string(peerIdentity)).Msg("routing_remove_direct_peer_failed")
		return
	}

	// Mark per-peer announce state as disconnected. The next announce
	// cycle for this peer (if it reconnects) will require a forced full sync.
	s.announceLoop.StateRegistry().MarkDisconnected(peerIdentity)

	// Send wire withdrawals to all routing-capable peers.
	// This is the immediate own-origin withdrawal path — it bypasses the
	// announce loop and per-peer cache intentionally (see section 8.2.1
	// of the routing-announce-traffic-optimization plan).
	//
	// The fanout is parallel (one goroutine per peer) because
	// SendAnnounceRoutes is bounded by syncFlushTimeout per inbound peer;
	// any stuck peer would otherwise serialise the whole disconnect path
	// and starve unrelated s.peerMu readers (the 2s bootstrapLoop ticker
	// and the 1s metrics collector) for N * syncFlushTimeout seconds.
	var sent, dropped int
	if len(result.Withdrawals) > 0 {
		sent, dropped = s.fanoutAnnounceRoutes(s.runCtx, s.routingCapablePeers(), result.Withdrawals)
	}

	log.Info().
		Str("peer", string(peerIdentity)).
		Int("withdrawals", len(result.Withdrawals)).
		Int("transit_invalidated", result.TransitInvalidated).
		Int("fanout_sent", sent).
		Int("fanout_dropped", dropped).
		Msg("routing_direct_peer_removed")

	s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason:    domain.RouteChangeDirectPeerRemoved,
		PeerID:    peerIdentity,
		Withdrawn: len(result.Withdrawals),
	})

	s.announceLoop.TriggerUpdate()
	s.triggerDrainForExposed(result.ExposedBackups)
}
