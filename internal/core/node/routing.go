package node

import (
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

	// RelayNextHop is the next-hop peer address from the routing table.
	// nil means no route is known. Populated starting from Iteration 1.
	RelayNextHop *string

	// GossipTargets are the top-N peers selected by score for blind gossip
	// delivery. Maps to the existing routingTargetsForMessage() path.
	GossipTargets []string
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
