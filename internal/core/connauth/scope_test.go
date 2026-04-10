package connauth

import (
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// mockAuthStore is a minimal AuthStore for scope unit tests.
// It maps connections to their auth state without any locking or
// connection lifecycle management.
type mockAuthStore struct {
	states map[net.Conn]*State
}

func newMockAuthStore() *mockAuthStore {
	return &mockAuthStore{states: make(map[net.Conn]*State)}
}

func (m *mockAuthStore) ConnAuthState(conn net.Conn) *State {
	return m.states[conn]
}

func (m *mockAuthStore) SetConnAuthState(conn net.Conn, state *State) {
	m.states[conn] = state
}

// mockConn is a minimal net.Conn that satisfies the interface for map key usage.
type mockConn struct {
	id     int
	local  net.Addr
	remote net.Addr
}

func newMockConn(id int) *mockConn {
	return &mockConn{
		id:     id,
		local:  &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 12345},
		remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 54321},
	}
}

func (m *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (m *mockConn) Write(b []byte) (int, error)        { return len(b), nil }
func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return m.local }
func (m *mockConn) RemoteAddr() net.Addr               { return m.remote }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// --- ConnectionRole tests ---

func TestConnectionRole_NoState(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	role := scope.ConnectionRole(conn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

func TestConnectionRole_NilState(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = nil

	role := scope.ConnectionRole(conn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

func TestConnectionRole_Unverified(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = &State{Verified: false}

	role := scope.ConnectionRole(conn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

func TestConnectionRole_Verified(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = &State{Verified: true}

	role := scope.ConnectionRole(conn)
	if role != domain.RoleAuthPeer {
		t.Errorf("ConnectionRole = %s, want %s", role, domain.RoleAuthPeer)
	}
}

// --- AuthInitiated tests ---

func TestAuthInitiated_NoState(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	if scope.AuthInitiated(conn) {
		t.Error("AuthInitiated should be false for unregistered connection")
	}
}

func TestAuthInitiated_PendingAuth(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = &State{
		Hello:     protocol.Frame{Address: "alice"},
		Challenge: "test",
		Verified:  false,
	}

	if !scope.AuthInitiated(conn) {
		t.Error("AuthInitiated should be true when challenge has been issued")
	}
}

func TestAuthInitiated_CompletedAuth(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = &State{Verified: true}

	if !scope.AuthInitiated(conn) {
		t.Error("AuthInitiated should be true for completed auth")
	}
}

// --- CheckCommand tests ---

func TestCheckCommand_Unknown(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	for _, cmd := range []string{"nonexistent", "admin_reset", "drop_database", ""} {
		result := scope.CheckCommand(conn, cmd)
		if result != Unknown {
			t.Errorf("CheckCommand(%q) = %d, want Unknown(%d)", cmd, result, Unknown)
		}
	}
}

func TestCheckCommand_HandshakeAlwaysAllowed(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	// No auth state → RoleUnauthPeer.

	for _, cmd := range []string{"hello", "ping", "pong", "auth_session"} {
		result := scope.CheckCommand(conn, cmd)
		if result != Allowed {
			t.Errorf("CheckCommand(%q) for unauth = %d, want Allowed(%d)", cmd, result, Allowed)
		}
	}
}

func TestCheckCommand_AuthOnlyForbiddenForUnauth(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	// No auth state → RoleUnauthPeer.

	authOnly := []string{
		"relay_message", "subscribe_inbox", "file_command",
		"announce_routes", "push_message", "announce_peer",
		"send_message", "import_message", "delete_trusted_contact",
		"fetch_inbox", "fetch_delivery_receipts", "publish_notice",
	}
	for _, cmd := range authOnly {
		result := scope.CheckCommand(conn, cmd)
		if result != Forbidden {
			t.Errorf("CheckCommand(%q) for unauth = %d, want Forbidden(%d)", cmd, result, Forbidden)
		}
	}
}

func TestCheckCommand_AuthOnlyAllowedForAuth(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)
	store.states[conn] = &State{Verified: true}

	authOnly := []string{
		"relay_message", "subscribe_inbox", "file_command",
		"announce_routes", "push_message", "announce_peer",
		"send_message", "import_message", "delete_trusted_contact",
		"fetch_inbox", "fetch_delivery_receipts", "publish_notice",
	}
	for _, cmd := range authOnly {
		result := scope.CheckCommand(conn, cmd)
		if result != Allowed {
			t.Errorf("CheckCommand(%q) for auth = %d, want Allowed(%d)", cmd, result, Allowed)
		}
	}
}

func TestCheckCommand_ReadOnlyBothRoles(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)

	readOnly := []string{
		"get_peers", "fetch_identities", "fetch_contacts",
		"fetch_trusted_contacts", "fetch_peer_health",
		"fetch_network_stats", "fetch_pending_messages",
		"fetch_messages", "fetch_message_ids", "fetch_message",
		"fetch_notices", "fetch_reachable_ids",
	}

	// Unauth peer.
	unauthConn := newMockConn(1)
	for _, cmd := range readOnly {
		result := scope.CheckCommand(unauthConn, cmd)
		if result != Allowed {
			t.Errorf("CheckCommand(%q) for unauth = %d, want Allowed", cmd, result)
		}
	}

	// Auth peer.
	authConn := newMockConn(2)
	store.states[authConn] = &State{Verified: true}
	for _, cmd := range readOnly {
		result := scope.CheckCommand(authConn, cmd)
		if result != Allowed {
			t.Errorf("CheckCommand(%q) for auth = %d, want Allowed", cmd, result)
		}
	}
}

// --- IsAllowed tests ---

func TestIsAllowed_Convenience(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	if scope.IsAllowed(conn, "relay_message") {
		t.Error("IsAllowed(relay_message) should be false for unauth")
	}
	if !scope.IsAllowed(conn, "ping") {
		t.Error("IsAllowed(ping) should be true for unauth")
	}
	if scope.IsAllowed(conn, "nonexistent") {
		t.Error("IsAllowed(nonexistent) should be false for unknown command")
	}
}

// --- Commands map integrity ---

func TestCommandsMapNotEmpty(t *testing.T) {
	t.Parallel()
	if len(Commands) == 0 {
		t.Fatal("Commands map is empty")
	}
}

func TestCommandsMapRolesNotEmpty(t *testing.T) {
	t.Parallel()
	for cmd, roles := range Commands {
		if len(roles) == 0 {
			t.Errorf("Commands[%q] has empty role list", cmd)
		}
	}
}

func TestCommandsMapRolesValid(t *testing.T) {
	t.Parallel()
	validRoles := map[domain.ConnectionRole]bool{
		domain.RoleUnauthPeer: true,
		domain.RoleAuthPeer:   true,
	}
	for cmd, roles := range Commands {
		for _, r := range roles {
			if !validRoles[r] {
				t.Errorf("Commands[%q] contains unknown role %d", cmd, r)
			}
		}
	}
}

// --- GAP-0 security: role does NOT depend on frame.Client ---

func TestGAP0_RoleDependsOnAuthState(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	// No auth state — connection is unauthenticated regardless of what
	// Client field might say in any frame.
	role := scope.ConnectionRole(conn)
	if role != domain.RoleUnauthPeer {
		t.Fatalf("SECURITY: ConnectionRole = %s, want RoleUnauthPeer — "+
			"role must not depend on frame.Client", role)
	}

	for _, cmd := range []string{"relay_message", "push_message", "subscribe_inbox"} {
		if scope.IsAllowed(conn, cmd) {
			t.Errorf("SECURITY: IsAllowed(%q) = true for unauth — GAP-0 not fixed", cmd)
		}
	}
}

func TestGAP0_UnauthPeerCannotAccessAnyAuthCommand(t *testing.T) {
	t.Parallel()
	store := newMockAuthStore()
	scope := NewScope(store)
	conn := newMockConn(1)

	for cmd, roles := range Commands {
		onlyAuth := true
		for _, r := range roles {
			if r == domain.RoleUnauthPeer {
				onlyAuth = false
				break
			}
		}
		if !onlyAuth {
			continue
		}
		if scope.IsAllowed(conn, cmd) {
			t.Errorf("SECURITY: unauthenticated peer allowed auth-only command %q", cmd)
		}
	}
}
