package connauth

import (
	"net"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Commands defines which commands are permitted on the data port and the
// ConnectionRole(s) that may execute each. Commands absent from this map
// return ScopeUnknown via Scope.CheckCommand.
//
// The role is determined by server-side auth state (AuthStore), never by
// frame.Client which the attacker controls. This eliminates the
// requiresSessionAuth bypass vector (GAP-0).
//
// Security-critical commands (relay, routing, file transfer, inbox
// subscription) require RoleAuthPeer. Lower-risk commands remain available
// to both roles for backward compatibility; a follow-up PR will migrate
// data-only commands to the RPC port exclusively.
var Commands = map[string][]domain.ConnectionRole{
	// --- Handshake: always available ---
	// Note: hello is allowed by Commands for both roles, but the
	// handler rejects re-hello once auth has been initiated (challenge
	// issued) or completed (ErrCodeHelloAfterAuth). This prevents both
	// post-auth identity spoofing and mid-handshake address overwrites.
	"hello":        {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"ping":         {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"pong":         {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"auth_session": {domain.RoleUnauthPeer, domain.RoleAuthPeer},

	// --- Security-critical: RoleAuthPeer only ---
	// These commands have high-trust side effects (relay injection, route
	// manipulation, file transfer, live inbox subscription). They MUST
	// require completed auth_session.
	"subscribe_inbox": {domain.RoleAuthPeer},
	"relay_message":   {domain.RoleAuthPeer},
	"relay_hop_ack":   {domain.RoleAuthPeer},
	"announce_routes": {domain.RoleAuthPeer},
	"file_command":    {domain.RoleAuthPeer},

	// --- P2P wire commands: RoleAuthPeer only ---
	// Inbound push, subscription ack, peer announcement, and deletion ack
	// are part of the authenticated P2P wire protocol.
	"push_message":          {domain.RoleAuthPeer},
	"push_delivery_receipt": {domain.RoleAuthPeer},
	"subscribed":            {domain.RoleAuthPeer},
	"announce_peer":         {domain.RoleAuthPeer},
	"ack_delete":            {domain.RoleAuthPeer},

	// --- State-changing commands: RoleAuthPeer only ---
	// Unauthenticated peers must not modify local stores (messages,
	// contacts, trust entries, receipts) or publish content.
	"delete_trusted_contact": {domain.RoleAuthPeer},
	"import_contacts":        {domain.RoleAuthPeer},
	"send_message":           {domain.RoleAuthPeer},
	"import_message":         {domain.RoleAuthPeer},
	"send_delivery_receipt":  {domain.RoleAuthPeer},
	"publish_notice":         {domain.RoleAuthPeer},

	// --- Sensitive reads: RoleAuthPeer only ---
	// fetch_inbox identity binding is enforced only for authenticated
	// peers; leaving it open to RoleUnauthPeer allows exfiltration of
	// stored DM metadata for arbitrary identities.
	// fetch_delivery_receipts has no identity binding at all.
	"fetch_inbox":             {domain.RoleAuthPeer},
	"fetch_delivery_receipts": {domain.RoleAuthPeer},

	// --- Read-only queries: both roles (backward compatibility) ---
	// These return public or low-sensitivity data. A follow-up PR
	// should migrate them to the RPC port exclusively.
	"get_peers":              {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_identities":       {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_contacts":         {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_trusted_contacts": {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_peer_health":      {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_network_stats":    {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_pending_messages": {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_messages":         {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_message_ids":      {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_message":          {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_notices":          {domain.RoleUnauthPeer, domain.RoleAuthPeer},
	"fetch_reachable_ids":    {domain.RoleUnauthPeer, domain.RoleAuthPeer},
}

// ScopeResult distinguishes why a command was rejected so the caller
// can return the appropriate error code.
type ScopeResult int

const (
	Allowed   ScopeResult = iota
	Unknown               // not in Commands — ErrCodeUnknownCommand
	Forbidden             // in scope but role insufficient — ErrCodeAuthRequired
)

// Scope provides role-based command gating for inbound TCP connections.
// It delegates auth state lookups to the AuthStore interface, keeping
// the RBAC logic independent from connection lifecycle management.
type Scope struct {
	store AuthStore
}

// NewScope creates a Scope that reads auth state from the given store.
func NewScope(store AuthStore) *Scope {
	return &Scope{store: store}
}

// ConnectionRole determines the trust level of an inbound TCP connection
// by inspecting the server-side auth state, never by frame.Client which
// the attacker controls. This eliminates GAP-0.
func (sc *Scope) ConnectionRole(conn net.Conn) domain.ConnectionRole {
	state := sc.store.ConnAuthState(conn)
	if state != nil && state.Verified {
		return domain.RoleAuthPeer
	}
	return domain.RoleUnauthPeer
}

// AuthInitiated returns true if the connection has started (challenge
// issued, Verified=false) or completed (Verified=true) the auth handshake.
// Used by the re-hello guard to block identity/address overwrites once
// PrepareAuth has recorded the initial hello and challenge.
func (sc *Scope) AuthInitiated(conn net.Conn) bool {
	return sc.store.ConnAuthState(conn) != nil
}

// CheckCommand verifies whether the given command is allowed for the
// connection's current role. Returns Allowed when permitted, Unknown
// when the command is not registered in Commands, or Forbidden when the
// role is insufficient.
func (sc *Scope) CheckCommand(conn net.Conn, command string) ScopeResult {
	roles, exists := Commands[command]
	if !exists {
		return Unknown
	}
	role := sc.ConnectionRole(conn)
	for _, r := range roles {
		if r == role {
			return Allowed
		}
	}
	return Forbidden
}

// IsAllowed is a convenience wrapper that returns true only when the
// command is permitted for the connection's role.
func (sc *Scope) IsAllowed(conn net.Conn, command string) bool {
	return sc.CheckCommand(conn, command) == Allowed
}
