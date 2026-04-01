package node

import (
	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
)

// RoutingDecision is a multi-strategy routing result. Each field represents an
// independent delivery path; the caller executes all applicable paths. This
// avoids flattening fundamentally different delivery mechanisms into a single
// list of addresses.
type RoutingDecision struct {
	// PushSubscribers are locally connected subscribers for the recipient.
	// The slice is a snapshot taken under s.mu — safe to iterate without
	// holding the lock. Maps to the existing subscribersForRecipient() path.
	PushSubscribers []*subscriber

	// DirectPeers are peers that the recipient is directly connected to.
	// Empty in Iteration 0 — populated by future routing table lookups.
	DirectPeers []string

	// RelayNextHop is the next-hop peer identity (Ed25519 fingerprint)
	// from the routing table. nil means no route is known.
	// Populated starting from Phase 1.2.
	RelayNextHop *domain.PeerIdentity

	// RelayNextHopAddress is the transport address (or "inbound:" prefixed
	// key) of the session that was validated by the router at lookup time.
	// Using this address directly avoids re-resolution, which could pick a
	// different session for the same identity — one that lacks the required
	// capabilities (e.g., a relay-only session instead of a routing-capable
	// session for a transit next-hop). Empty when RelayNextHop is nil.
	RelayNextHopAddress domain.PeerAddress

	// RelayRouteOrigin is the Origin field from the RouteEntry selected by
	// the router. Stored in relayForwardState so that hop_ack confirmation
	// can match the exact (Identity, Origin, NextHop) triple that carried
	// the message, rather than promoting any route with a matching NextHop.
	// Empty when RelayNextHop is nil.
	RelayRouteOrigin domain.PeerIdentity

	// RelayNextHopHops is the hop count from the selected RouteEntry.
	// Used by the retry path in sendTableDirectedRelay to re-resolve the
	// next-hop address with the correct capability requirements: hops=1
	// (destination) needs only relay cap; hops>1 (transit) needs both
	// relay and routing caps. Zero when RelayNextHop is nil.
	RelayNextHopHops int

	// GossipTargets are the top-N peers selected by score for blind gossip
	// delivery. Maps to the existing routingTargetsForMessage() path.
	GossipTargets []domain.PeerAddress
}

// Router determines how a message should be delivered across the mesh network.
// Implementations return a RoutingDecision describing all applicable delivery
// paths; the caller is responsible for executing each path.
type Router interface {
	Route(msg protocol.Envelope) RoutingDecision
}

// GossipRouter wraps the existing gossip-based delivery logic without changing
// network behavior. PushSubscribers come from subscribersForRecipient() and
// GossipTargets come from routingTargetsForMessage(). DirectPeers and
// RelayNextHop are always empty in this implementation.
type GossipRouter struct {
	svc *Service
}

// NewGossipRouter creates a GossipRouter that delegates to the Service's
// existing subscriber and target-selection methods.
func NewGossipRouter(svc *Service) *GossipRouter {
	return &GossipRouter{svc: svc}
}

func (r *GossipRouter) Route(msg protocol.Envelope) RoutingDecision {
	var pushSubs []*subscriber
	if msg.Topic == "dm" && msg.Recipient != "*" {
		pushSubs = r.svc.subscribersForRecipient(msg.Recipient)
	}

	gossipTargets := r.svc.routingTargetsForMessage(msg)

	return RoutingDecision{
		PushSubscribers: pushSubs,
		GossipTargets:   gossipTargets,
	}
}
