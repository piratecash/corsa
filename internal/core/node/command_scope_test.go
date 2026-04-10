package node

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
)

// newTestServiceWithScope creates a minimal Service with inboundNetCores map
// and scope initialized. This is the standard way to construct a Service for
// unit tests that exercise RBAC logic.
func newTestServiceWithScope() *Service {
	svc := &Service{
		inboundNetCores: make(map[net.Conn]*NetCore),
	}
	svc.scope = connauth.NewScope(svc)
	return svc
}

// extractSwitchCasesFromDispatch parses service.go via go/ast and returns
// every string literal used as a case label inside the dispatchNetworkFrame
// method's switch statement. This is the single source of truth — no manual
// list to keep in sync.
func extractSwitchCasesFromDispatch(t *testing.T) map[string]bool {
	t.Helper()

	// Locate service.go relative to this test file.
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	serviceFile := filepath.Join(filepath.Dir(thisFile), "service.go")

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, serviceFile, nil, 0)
	if err != nil {
		t.Fatalf("parse service.go: %v", err)
	}

	cases := make(map[string]bool)
	var insideDispatch bool

	ast.Inspect(node, func(n ast.Node) bool {
		// Find the dispatchNetworkFrame method.
		if fn, ok := n.(*ast.FuncDecl); ok {
			insideDispatch = fn.Name.Name == "dispatchNetworkFrame"
			return true
		}
		if !insideDispatch {
			return true
		}
		// Inside dispatchNetworkFrame — look for case clauses.
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, expr := range cc.List {
			if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				// Strip quotes from the string literal.
				val := strings.Trim(lit.Value, `"`)
				cases[val] = true
			}
		}
		return true
	})

	if len(cases) == 0 {
		t.Fatal("no case labels found in dispatchNetworkFrame — AST parsing failed")
	}
	return cases
}

// TestCommandScopeCoversAllSwitchCases ensures that every command handled by
// dispatchNetworkFrame is registered in connauth.Commands. Uses AST parsing of
// service.go as the source of truth — no manual list to keep in sync.
func TestCommandScopeCoversAllSwitchCases(t *testing.T) {
	t.Parallel()

	switchCases := extractSwitchCasesFromDispatch(t)
	for cmd := range switchCases {
		if _, exists := connauth.Commands[cmd]; !exists {
			t.Errorf("command %q is handled in dispatchNetworkFrame switch but missing from connauth.Commands", cmd)
		}
	}
}

// TestCommandScopeNoExtraEntries verifies that connauth.Commands does not
// contain entries that are NOT in the switch. Phantom entries would silently
// pass the gate but hit the default case with ErrCodeUnknownCommand.
func TestCommandScopeNoExtraEntries(t *testing.T) {
	t.Parallel()

	switchCases := extractSwitchCasesFromDispatch(t)
	for cmd := range connauth.Commands {
		if !switchCases[cmd] {
			t.Errorf("connauth.Commands contains %q which is not in dispatchNetworkFrame switch", cmd)
		}
	}
}

// TestCommandScopeRoleMatrix exhaustively checks every command×role
// combination in connauth.Commands. The expected categorisation is defined
// here (the security contract); a coverage check ensures the expected map
// stays in sync with connauth.Commands — adding or removing a command in
// either place without updating the other will fail the test.
func TestCommandScopeRoleMatrix(t *testing.T) {
	t.Parallel()

	// expected defines the contract: which roles each command must allow.
	// "both" = {RoleUnauthPeer, RoleAuthPeer}, "authOnly" = {RoleAuthPeer}.
	type category string
	const (
		both     category = "both"
		authOnly category = "authOnly"
	)

	expected := map[string]category{
		// Handshake: both roles.
		"hello":        both,
		"ping":         both,
		"pong":         both,
		"auth_session": both,

		// Security-critical P2P wire: auth only.
		"subscribe_inbox":       authOnly,
		"relay_message":         authOnly,
		"relay_hop_ack":         authOnly,
		"announce_routes":       authOnly,
		"file_command":          authOnly,
		"push_message":          authOnly,
		"push_delivery_receipt": authOnly,
		"subscribed":            authOnly,
		"announce_peer":         authOnly,
		"ack_delete":            authOnly,

		// State-changing writes: auth only.
		"delete_trusted_contact": authOnly,
		"import_contacts":        authOnly,
		"send_message":           authOnly,
		"import_message":         authOnly,
		"send_delivery_receipt":  authOnly,
		"publish_notice":         authOnly,

		// Sensitive reads: auth only.
		"fetch_inbox":             authOnly,
		"fetch_delivery_receipts": authOnly,

		// Read-only queries: both roles (backward compat).
		"get_peers":              both,
		"fetch_identities":       both,
		"fetch_contacts":         both,
		"fetch_trusted_contacts": both,
		"fetch_peer_health":      both,
		"fetch_network_stats":    both,
		"fetch_pending_messages": both,
		"fetch_messages":         both,
		"fetch_message_ids":      both,
		"fetch_message":          both,
		"fetch_notices":          both,
		"fetch_reachable_ids":    both,
	}

	// Coverage gate: every key in connauth.Commands must be in expected,
	// and vice versa. This prevents silent drift.
	for cmd := range connauth.Commands {
		if _, ok := expected[cmd]; !ok {
			t.Errorf("connauth.Commands contains %q but TestCommandScopeRoleMatrix has no expectation for it — "+
				"add it to the expected map", cmd)
		}
	}
	for cmd := range expected {
		if _, ok := connauth.Commands[cmd]; !ok {
			t.Errorf("expected map contains %q but connauth.Commands does not — "+
				"remove it from the expected map or add it to connauth.Commands", cmd)
		}
	}

	// Test every command×role combination.
	for cmd, cat := range expected {
		unauthAllowed := cat == both
		for _, tc := range []struct {
			role    domain.ConnectionRole
			allowed bool
		}{
			{domain.RoleUnauthPeer, unauthAllowed},
			{domain.RoleAuthPeer, true}, // auth peer is always allowed
		} {
			t.Run(cmd+"_"+tc.role.String(), func(t *testing.T) {
				roles := connauth.Commands[cmd]
				got := roleInSlice(tc.role, roles)
				if got != tc.allowed {
					t.Errorf("connauth.Commands[%q] role=%s: got allowed=%v, want %v",
						cmd, tc.role, got, tc.allowed)
				}
			})
		}
	}
}

func roleInSlice(role domain.ConnectionRole, roles []domain.ConnectionRole) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}

// TestCheckCommandScopeUnknown verifies that commands not in connauth.Commands
// return connauth.Unknown.
func TestCheckCommandScopeUnknown(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()

	unknowns := []string{"nonexistent", "admin_reset", "drop_database"}
	for _, cmd := range unknowns {
		result := svc.scope.CheckCommand(mockConn, cmd)
		if result != connauth.Unknown {
			t.Errorf("CheckCommand(%q) = %d, want Unknown(%d)", cmd, result, connauth.Unknown)
		}
	}
}

// TestCheckCommandScopeForbidden verifies that auth-only commands return
// connauth.Forbidden for unauthenticated connections.
func TestCheckCommandScopeForbidden(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	// Register a NetCore without auth state -> RoleUnauthPeer.
	nc := &NetCore{}
	svc.inboundNetCores[mockConn] = nc

	forbidden := []string{
		"relay_message", "subscribe_inbox", "file_command",
		"announce_routes", "push_message", "announce_peer",
		"send_message", "import_message", "delete_trusted_contact",
		"fetch_inbox", "fetch_delivery_receipts",
	}
	for _, cmd := range forbidden {
		result := svc.scope.CheckCommand(mockConn, cmd)
		if result != connauth.Forbidden {
			t.Errorf("CheckCommand(%q) for unauth peer = %d, want Forbidden(%d)", cmd, result, connauth.Forbidden)
		}
	}
}

// TestCheckCommandScopeAllowed verifies that auth-only commands return
// connauth.Allowed for authenticated connections.
func TestCheckCommandScopeAllowed(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	nc := &NetCore{}
	nc.auth = &connauth.State{Verified: true}
	svc.inboundNetCores[mockConn] = nc

	allowed := []string{
		"relay_message", "subscribe_inbox", "file_command",
		"hello", "ping", "send_message", "get_peers",
	}
	for _, cmd := range allowed {
		result := svc.scope.CheckCommand(mockConn, cmd)
		if result != connauth.Allowed {
			t.Errorf("CheckCommand(%q) for auth peer = %d, want Allowed(%d)", cmd, result, connauth.Allowed)
		}
	}
}

// TestConnectionRole_NilNetCore ensures that a connection with no registered
// NetCore defaults to RoleUnauthPeer (fail-safe).
func TestConnectionRole_NilNetCore(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole for nil NetCore = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

// TestConnectionRole_NilAuth ensures that a connection with NetCore but nil
// auth state defaults to RoleUnauthPeer.
func TestConnectionRole_NilAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	svc.inboundNetCores[mockConn] = &NetCore{}

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole for nil auth = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

// TestConnectionRole_UnverifiedAuth ensures that a connection with auth state
// where Verified=false defaults to RoleUnauthPeer (auth in progress).
func TestConnectionRole_UnverifiedAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	nc := &NetCore{}
	nc.auth = &connauth.State{Verified: false}
	svc.inboundNetCores[mockConn] = nc

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleUnauthPeer {
		t.Errorf("ConnectionRole for unverified auth = %s, want %s", role, domain.RoleUnauthPeer)
	}
}

// TestConnectionRole_VerifiedAuth ensures that a connection with verified auth
// state is RoleAuthPeer.
func TestConnectionRole_VerifiedAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	nc := &NetCore{}
	nc.auth = &connauth.State{Verified: true}
	svc.inboundNetCores[mockConn] = nc

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleAuthPeer {
		t.Errorf("ConnectionRole for verified auth = %s, want %s", role, domain.RoleAuthPeer)
	}
}

// TestGAP0_ClientFieldDoesNotAffectRole verifies the core GAP-0 fix:
// frame.Client ("mobile", "attacker", etc.) does NOT influence
// ConnectionRole. The role depends solely on pc.Auth().
func TestGAP0_ClientFieldDoesNotAffectRole(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	nc := &NetCore{}
	// No auth state — regardless of what Client field might say in the hello
	// frame, the connection is unauthenticated.
	svc.inboundNetCores[mockConn] = nc

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleUnauthPeer {
		t.Fatalf("SECURITY: ConnectionRole = %s, want RoleUnauthPeer — "+
			"role must not depend on frame.Client", role)
	}

	// Verify security-critical commands are blocked.
	for _, cmd := range []string{"relay_message", "push_message", "subscribe_inbox"} {
		if svc.scope.IsAllowed(mockConn, cmd) {
			t.Errorf("SECURITY: IsAllowed(%q) = true for unauth peer — GAP-0 not fixed", cmd)
		}
	}
}

// TestGAP0_UnauthPeerCannotAccessAuthCommands verifies that a peer which
// connects without completing auth_session cannot access any auth-only
// command in connauth.Commands.
func TestGAP0_UnauthPeerCannotAccessAuthCommands(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := newMockNetConn()
	svc.inboundNetCores[mockConn] = &NetCore{} // no auth

	for cmd, roles := range connauth.Commands {
		onlyAuth := true
		for _, r := range roles {
			if r == domain.RoleUnauthPeer {
				onlyAuth = false
				break
			}
		}
		if !onlyAuth {
			continue // available to unauth peers — skip
		}

		if svc.scope.IsAllowed(mockConn, cmd) {
			t.Errorf("SECURITY: unauthenticated peer allowed to execute auth-only command %q", cmd)
		}
	}
}

// TestLoopback_DoesNotElevateRole verifies that a loopback (127.0.0.1)
// connection does NOT receive elevated privileges. The role is determined
// by auth state, never by source IP.
func TestLoopback_DoesNotElevateRole(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithScope()
	mockConn := &mockNetConnWithAddr{
		local:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345},
		remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321},
	}
	svc.inboundNetCores[mockConn] = &NetCore{} // no auth

	role := svc.scope.ConnectionRole(mockConn)
	if role != domain.RoleUnauthPeer {
		t.Fatalf("SECURITY: loopback connection has role %s, want RoleUnauthPeer — "+
			"source IP must not elevate role", role)
	}

	if svc.scope.IsAllowed(mockConn, "relay_message") {
		t.Fatal("SECURITY: loopback connection allowed relay_message without auth")
	}
}

// TestConnectionRoleString verifies the String() method for logging.
func TestConnectionRoleString(t *testing.T) {
	t.Parallel()

	if s := domain.RoleUnauthPeer.String(); s != "unauth_peer" {
		t.Errorf("RoleUnauthPeer.String() = %q, want %q", s, "unauth_peer")
	}
	if s := domain.RoleAuthPeer.String(); s != "auth_peer" {
		t.Errorf("RoleAuthPeer.String() = %q, want %q", s, "auth_peer")
	}
	// Unknown value should not panic.
	unknown := domain.ConnectionRole(99)
	if s := unknown.String(); s == "" {
		t.Error("unknown ConnectionRole.String() should not be empty")
	}
}

// --- mock helpers ---

// mockNetConnSimple is a minimal net.Conn for unit tests that don't need real I/O.
type mockNetConnSimple struct {
	local  net.Addr
	remote net.Addr
}

func newMockNetConn() net.Conn {
	return &mockNetConnSimple{
		local:  &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 12345},
		remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 54321},
	}
}

func (m *mockNetConnSimple) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockNetConnSimple) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockNetConnSimple) Close() error                       { return nil }
func (m *mockNetConnSimple) LocalAddr() net.Addr                { return m.local }
func (m *mockNetConnSimple) RemoteAddr() net.Addr               { return m.remote }
func (m *mockNetConnSimple) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConnSimple) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConnSimple) SetWriteDeadline(t time.Time) error { return nil }

// mockNetConnWithAddr allows tests to specify arbitrary local/remote addresses.
type mockNetConnWithAddr struct {
	local  net.Addr
	remote net.Addr
}

func (m *mockNetConnWithAddr) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockNetConnWithAddr) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockNetConnWithAddr) Close() error                       { return nil }
func (m *mockNetConnWithAddr) LocalAddr() net.Addr                { return m.local }
func (m *mockNetConnWithAddr) RemoteAddr() net.Addr               { return m.remote }
func (m *mockNetConnWithAddr) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConnWithAddr) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConnWithAddr) SetWriteDeadline(t time.Time) error { return nil }
