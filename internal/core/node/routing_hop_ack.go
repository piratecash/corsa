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
// routeOrigin is retained on the signature for caller-stability (the
// relay-side plumbing still threads it through tableForwardResult /
// sendTableDirectedRelay) but is IGNORED post-Phase-A: the routing
// table no longer stores Origin (per-(Identity, Uplink) storage),
// and the synthesised Origin on Lookup output is always localOrigin
// (or Identity fallback), which carries no per-route disambiguation
// signal. The (Identity, NextHop) pair is the post-A1 unique key —
// the same uplink cannot have two distinct "lineages" to the same
// destination, so the per-Origin scoping became degenerate.
//
// If ackSenderAddress is empty, no route can be confirmed (message
// was stored locally without forwarding).
//
// Cap-eviction edge case. When MaxNextHopsPerOrigin is active and the
// (Identity, Uplink) claim was evicted from the routing table
// between the relay send and the hop_ack arrival, InspectTriple
// returns nil and the function logs "route_hop_ack_no_matching_route"
// without re-creating the entry. This is by design: the hop_ack
// frame only carries the (recipient, next-hop) pair — neither
// Origin nor SeqNo are on the wire — and a synthetic RouteEntry
// without authentic SeqNo would forge ranking input that other
// paths (split-horizon, withdrawal authority) rely on. Subsequent
// announcement traffic for the same (Identity, Uplink) re-populates
// the bucket through the normal admission rules.
func (s *Service) confirmRouteViaHopAck(recipientIdentity domain.PeerIdentity, ackSenderAddress domain.PeerAddress, routeOrigin domain.PeerIdentity) {
	_ = routeOrigin // post-A1: per-Origin scoping is degenerate; see doc-comment above.

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

	// PR 11.12 P2#1 — atomic confirmation. Table.ConfirmHopAck
	// folds three previously-separate steps (InspectTriple,
	// MarkHopAck, UpdateRoute) into one t.mu.Lock critical
	// section so a concurrent withdrawal / TickTTL-expiry /
	// cap-eviction cannot interleave between the storage check
	// and the health/source mutations and leave orphan health
	// behind. The method also folds in the Dead-filter fix
	// (raw InspectTriple inside, no Lookup-style Dead filter)
	// and the inactive-route guard (withdrawn/expired short-
	// circuits before any health touch).
	//
	// `applied=false` means the (recipient, next-hop) pair has
	// no live storage claim — no health was written and no
	// promotion ran. This is the cap-eviction edge case
	// documented on the function header.
	//
	// `applied=true` means health was refreshed to Good (with
	// rtt=0 we preserve the prior EWMA value untouched). status
	// distinguishes whether source promotion actually happened
	// (RouteAccepted) vs. the claim already being at HopAck or
	// better (RouteUnchanged) vs. an internal refusal that left
	// health refreshed but did not promote (RouteRejected).
	status, applied := s.routingTable.ConfirmHopAck(recipientIdentity, nextHopIdentity, 0)
	if !applied {
		log.Debug().
			Str("identity", string(recipientIdentity)).
			Str("next_hop", string(nextHopIdentity)).
			Str("ack_sender", string(ackSenderAddress)).
			Msg("route_hop_ack_no_matching_route")
		return
	}
	if status == routing.RouteAccepted {
		log.Debug().
			Str("identity", string(recipientIdentity)).
			Str("next_hop", string(nextHopIdentity)).
			Msg("route_confirmed_via_hop_ack")
	}
}
