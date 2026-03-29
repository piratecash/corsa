package node

import (
	"corsa/internal/core/protocol"
)

// RelayService handles hop-by-hop message forwarding through intermediate
// nodes. This interface is a placeholder introduced in Iteration 0; the
// actual relay logic is implemented in Iteration 1 when the "mesh_relay_v1"
// capability is activated.
type RelayService interface {
	// Relay forwards a message toward its final recipient through the mesh.
	// Returns true if the message was accepted for relay, false otherwise.
	Relay(msg protocol.Envelope) bool
}
