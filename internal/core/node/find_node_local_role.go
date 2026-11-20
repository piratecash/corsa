package node

import "github.com/piratecash/corsa/internal/core/domain"

// Private construction-only role, never a config/environment/RPC switch.
// F1 is symmetric session participation; Responder is THIS node's direction.
// Ordinary factories select neither nonzero value; tagged diagnostics are closed.
type findNodeLocalRole uint8

const (
	findNodeRoleOff findNodeLocalRole = iota
	findNodeRoleTarget
	findNodeRoleResponder
)

func (role findNodeLocalRole) validFor(kind domain.NodeType) bool {
	switch role {
	case findNodeRoleOff:
		return true
	case findNodeRoleTarget:
		return kind == domain.NodeTypeFull || kind == domain.NodeTypeClient
	case findNodeRoleResponder:
		return kind == domain.NodeTypeFull
	default:
		return false
	}
}
