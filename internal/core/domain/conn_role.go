package domain

import "fmt"

// ConnectionRole defines the trust level of a TCP connection on the data port.
// Only two levels exist — the data port serves exclusively P2P traffic.
// Operator access is available only through the RPC port and is not modeled
// here.
//
// The role is determined by server-side auth state (pc.Auth()), never by
// frame.Client which the attacker controls. This eliminates the
// requiresSessionAuth bypass vector (GAP-0).
type ConnectionRole int

const (
	// RoleUnauthPeer — TCP connection established, auth_session not completed.
	// Handshake commands (hello, ping, pong, auth_session) and a limited
	// set of read-only queries (get_peers, fetch_contacts, fetch_messages,
	// etc.) are available. State-changing, relay, and routing commands are
	// blocked. See connauth.Commands in internal/core/connauth/scope.go
	// for the full role×command matrix.
	RoleUnauthPeer ConnectionRole = iota

	// RoleAuthPeer — auth_session verified via Ed25519 signature.
	// Full P2P command set is available.
	RoleAuthPeer
)

// String returns a human-readable label for logging and diagnostics.
func (r ConnectionRole) String() string {
	switch r {
	case RoleUnauthPeer:
		return "unauth_peer"
	case RoleAuthPeer:
		return "auth_peer"
	default:
		return fmt.Sprintf("unknown_role(%d)", int(r))
	}
}
