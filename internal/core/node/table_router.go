package node

import (
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"

	"github.com/rs/zerolog/log"
)

// capMeshRoutingV1 is the capability token that gates announce_routes frames.
const capMeshRoutingV1 = "mesh_routing_v1"

// TableRouter extends GossipRouter with distance-vector table lookups.
// If the routing table has a valid next-hop with an active, capable session,
// RelayNextHop is populated. Otherwise, delivery degrades to gossip
// transparently — this is the gossip fallback contract (Phase 1.2,
// release-blocking).
//
// Gossip fallback triggers:
//   - Lookup() returns empty (no route known)
//   - RelayNextHop identity has no active session
//   - RelayNextHop peer lacks required capabilities
//
// Capability requirements vary by next-hop role:
//   - Destination (hops=1): only mesh_relay_v1 (accepts relay_message)
//   - Transit (hops>1): mesh_relay_v1 + mesh_routing_v1 (forwards via table)
//
// Enqueue/send failure between lookup and send is handled by the caller
// (node.Service relay path) — if the chosen session cannot accept the
// frame, gossip targets are still available in the same RoutingDecision.
type TableRouter struct {
	svc   *Service
	table *routing.Table

	// sessionChecker verifies that a peer identity has an active, capable
	// session for the given route. The hops parameter determines capability
	// requirements: hops=1 (destination) needs only relay cap; hops>1
	// (transit) needs both relay and routing caps.
	// Injected for testability; defaults to Service.resolveRouteNextHopAddress.
	sessionChecker func(peerIdentity string, hops int) string
}

// NewTableRouter creates a TableRouter that consults the routing table
// before falling back to gossip.
func NewTableRouter(svc *Service, table *routing.Table) *TableRouter {
	tr := &TableRouter{
		svc:   svc,
		table: table,
	}
	tr.sessionChecker = svc.resolveRouteNextHopAddress
	return tr
}

// Route implements the Router interface. It first checks the routing table
// for a directed next-hop. If a usable route exists, RelayNextHop is set.
// Regardless, PushSubscribers and GossipTargets are always populated so
// gossip delivery remains available as fallback.
func (r *TableRouter) Route(msg protocol.Envelope) RoutingDecision {
	// Always compute gossip targets — they serve as fallback.
	var pushSubs []*subscriber
	if msg.Topic == "dm" && msg.Recipient != "*" {
		pushSubs = r.svc.subscribersForRecipient(msg.Recipient)
	}
	gossipTargets := r.svc.routingTargetsForMessage(msg)

	decision := RoutingDecision{
		PushSubscribers: pushSubs,
		GossipTargets:   gossipTargets,
	}

	// Table lookup for directed relay.
	routes := r.table.Lookup(msg.Recipient)
	if len(routes) == 0 {
		log.Debug().
			Str("recipient", msg.Recipient).
			Msg("route_via_table_no_route")
		return decision
	}

	// Try routes in preference order until we find one with a usable session.
	// Capability requirements depend on hop count: direct destinations (hops=1)
	// only need relay capability; transit nodes (hops>1) need both.
	for _, route := range routes {
		address := r.sessionChecker(route.NextHop, route.Hops)
		if address == "" {
			continue
		}
		nextHop := route.NextHop
		decision.RelayNextHop = &nextHop
		decision.RelayNextHopAddress = address
		decision.RelayRouteOrigin = route.Origin
		decision.RelayNextHopHops = route.Hops
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", route.NextHop).
			Str("address", address).
			Int("hops", route.Hops).
			Str("source", route.Source.String()).
			Msg("route_via_table")
		return decision
	}

	// All routes found but no usable session — gossip fallback.
	log.Debug().
		Str("recipient", msg.Recipient).
		Int("routes_found", len(routes)).
		Msg("route_via_table_no_session_fallback_gossip")
	return decision
}

