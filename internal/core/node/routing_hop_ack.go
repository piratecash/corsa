// routing_hop_ack.go hosts route confirmation via relay_hop_ack. When an
// intermediate or destination peer acknowledges a relay_message forwarded
// by this node, the route through that peer is promoted to source=hop_ack
// — the strongest route-confirmation signal short of a reverse-direction
// announcement.
//
// Companion files: announce-plane wire path in routing_announce.go,
// relay-plane forwarding in routing_relay.go, address↔identity resolution
// in routing_resolver.go, session-lifecycle routing hooks in
// routing_session.go, pending-queue drain in routing_drain.go. The
// per-file scope table is the durable record — see the
// "internal/core/node/" section of the file map in docs/routing.md.
package node

import (
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// confirmRouteViaHopAck promotes the route through the peer that sent us
// the relay_hop_ack to source=hop_ack. The ackSenderAddress parameter is
// the transport address of the peer that produced the ack — this is the
// provably correct next-hop, even under gossip fan-out where multiple
// peers received the relay_message but only one actually delivered it.
//
// routeOrigin scopes the confirmation to the exact (Identity, Origin,
// NextHop) triple that was used for the original routing decision. When
// non-empty, only the route matching all three fields is promoted. When
// empty (gossip path where no specific triple was chosen), the first
// matching NextHop in Lookup order is promoted — this is a weaker
// guarantee but acceptable for gossip-originated relays.
//
// If ackSenderAddress is empty, no route can be confirmed (message was
// stored locally without forwarding).
func (s *Service) confirmRouteViaHopAck(recipientIdentity domain.PeerIdentity, ackSenderAddress domain.PeerAddress, routeOrigin domain.PeerIdentity) {
	if ackSenderAddress == "" {
		return
	}

	// ackSenderAddress is a transport address. Resolve to peer identity
	// so we can match against routing table entries (keyed by identity).
	nextHopIdentity := s.resolvePeerIdentity(ackSenderAddress)
	if nextHopIdentity == "" {
		// Session may have closed. Try using ackSenderAddress as identity directly.
		nextHopIdentity = domain.PeerIdentity(ackSenderAddress)
	}

	routes := s.routingTable.Lookup(recipientIdentity)
	if len(routes) == 0 {
		return
	}

	// Find the route that matches the actual triple used for the send.
	for _, route := range routes {
		if route.NextHop != nextHopIdentity {
			continue
		}
		// When routeOrigin is known, enforce exact triple match.
		if routeOrigin != "" && route.Origin != routeOrigin {
			continue
		}
		if route.Source >= routing.RouteSourceHopAck {
			return // already confirmed or better
		}

		confirmed := routing.RouteEntry{
			Identity:  route.Identity,
			Origin:    route.Origin,
			NextHop:   route.NextHop,
			Hops:      route.Hops,
			SeqNo:     route.SeqNo,
			Source:    routing.RouteSourceHopAck,
			ExpiresAt: route.ExpiresAt,
			Extra:     route.Extra,
		}

		status, err := s.routingTable.UpdateRoute(confirmed)
		if err != nil {
			log.Warn().Err(err).
				Str("identity", string(recipientIdentity)).
				Str("next_hop", string(nextHopIdentity)).
				Str("origin", string(route.Origin)).
				Msg("route_hop_ack_confirm_failed")
			return
		}
		if status == routing.RouteAccepted {
			log.Debug().
				Str("identity", string(recipientIdentity)).
				Str("origin", string(route.Origin)).
				Str("next_hop", string(route.NextHop)).
				Msg("route_confirmed_via_hop_ack")
		}
		return
	}

	log.Debug().
		Str("identity", string(recipientIdentity)).
		Str("ack_sender", string(ackSenderAddress)).
		Str("route_origin", string(routeOrigin)).
		Msg("route_hop_ack_no_matching_route")
}
