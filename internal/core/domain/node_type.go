package domain

import "strings"

// NodeType classifies a mesh node's role in the network.
// Full nodes accept inbound connections and relay traffic; client nodes
// only initiate outbound connections and never relay.
//
// The type is string-backed so that JSON/wire serialization produces stable
// human-readable labels ("full", "client") without custom marshalers.
type NodeType string

const (
	NodeTypeFull    NodeType = "full"
	NodeTypeClient  NodeType = "client"
	NodeTypeUnknown NodeType = "unknown"
)

// String returns the wire-format label.
func (t NodeType) String() string { return string(t) }

// IsFull returns true for full nodes that accept inbound connections
// and participate in relay/routing.
func (t NodeType) IsFull() bool { return t == NodeTypeFull }

// IsClient returns true for client-only nodes that do not relay.
func (t NodeType) IsClient() bool { return t == NodeTypeClient }

// ParseNodeType converts a raw wire/config string into a typed NodeType.
// Parsing is case-insensitive for backward compatibility with wire data.
// Returns the typed value and true if recognized, or NodeTypeUnknown and
// false for unrecognized values.
func ParseNodeType(s string) (NodeType, bool) {
	t := NodeType(strings.ToLower(strings.TrimSpace(s)))
	switch t {
	case NodeTypeFull, NodeTypeClient:
		return t, true
	default:
		return NodeTypeUnknown, false
	}
}
