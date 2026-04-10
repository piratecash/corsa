package connauth

import "net"

// AuthStore provides read/write access to per-connection auth state.
// The node.Service implements this interface using its inboundNetCores map.
type AuthStore interface {
	// ConnAuthState returns the auth state for the connection, or nil
	// if the connection is not registered or has no auth state.
	ConnAuthState(conn net.Conn) *State

	// SetConnAuthState stores auth state for the connection.
	// The connection must already be registered.
	SetConnAuthState(conn net.Conn, state *State)
}
