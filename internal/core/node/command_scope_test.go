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

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// newTestServiceForAuth creates a minimal Service with conns map
// initialized. This is the standard way to construct a Service for unit tests
// that exercise auth gating logic.
func newTestServiceForAuth() *Service {
	return &Service{
		conns:           make(map[netcore.ConnID]*connEntry),
		connIDByNetConn: make(map[net.Conn]netcore.ConnID),
	}
}

// --- Protocol oracle: independent hardcoded command sets ---
//
// These lists are the wire-contract oracle. They are NOT derived from the
// implementation — they encode what the protocol specification requires.
// If someone removes a command from dispatchNetworkFrame, the AST-derived
// set will silently reclassify it as data-only. These oracle lists catch
// that regression by asserting independently of the AST.
//
// Rule: adding or removing a command here is a conscious protocol change
// that must update the documentation and pass review.

// handshakeCommands is the set of commands that do NOT require auth_session.
// These four are the only commands handled before the auth gate in
// dispatchNetworkFrame. This list is intentionally hardcoded because
// handshake commands are a protocol constant — adding a new one is a
// conscious protocol change that must update this list and pass review.
var handshakeCommands = map[string]bool{
	"hello":        true,
	"ping":         true,
	"pong":         true,
	"auth_session": true,
}

// requiredP2PWireCommands is the independent protocol oracle for P2P wire
// commands. Every command listed here MUST have a case in
// dispatchNetworkFrame and an entry in p2pWireCommands. This list exists
// because the AST-derived set (derivedP2PCommands) validates the
// implementation against itself — if a command is accidentally removed
// from dispatchNetworkFrame, AST derivation silently reclassifies it as
// data-only, and all tests pass. This oracle prevents that failure mode.
//
// When a new P2P wire command is added to dispatchNetworkFrame, it must
// also be added here. When a command is intentionally moved to data-only,
// it must be explicitly removed from this list with a comment explaining why.
var requiredP2PWireCommands = map[string]bool{
	// Peer sync (used by syncPeer and syncPeerSession)
	"get_peers":      true,
	"fetch_contacts": true,

	// Inbox subscription lifecycle
	"subscribe_inbox": true,
	"subscribed":      true,

	// Realtime message delivery
	"push_message":           true,
	"push_delivery_receipt":  true,
	"relay_delivery_receipt": true,
	"push_notice":            true,

	// Acknowledgment
	"ack_delete": true,

	// Mesh protocol
	"announce_peer":   true,
	"relay_message":   true,
	"relay_hop_ack":   true,
	"announce_routes": true,
	// v2 routing announce plane (mesh_routing_v2 capability gate). Both
	// frames piggyback on the same dispatchNetworkFrame switch as the
	// legacy announce_routes path: routes_update carries an incremental
	// delta against the legacy baseline; request_resync is the no-payload
	// control frame the receive side emits when a delta arrives before
	// the baseline. See docs/routing.md "Wire format" and "Baseline gate
	// on v2 receive" for the protocol contract.
	"routes_update":  true,
	"request_resync": true,

	// File transfer
	"file_command": true,
}

// extractSwitchCasesFromFunc parses service.go via go/ast and returns every
// string literal used as a case label inside the named method's switch
// statements. This is the single source of truth — no manual list to keep
// in sync.
func extractSwitchCasesFromFunc(t *testing.T, funcName string) map[string]bool {
	t.Helper()

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
	var insideFunc bool

	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			insideFunc = fn.Name.Name == funcName
			return true
		}
		if !insideFunc {
			return true
		}
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		for _, expr := range cc.List {
			if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				val := strings.Trim(lit.Value, `"`)
				cases[val] = true
			}
		}
		return true
	})

	if len(cases) == 0 {
		t.Fatalf("no case labels found in %s — AST parsing failed", funcName)
	}
	return cases
}

// derivedP2PCommands returns the P2P command set derived from AST:
// all cases in dispatchNetworkFrame minus handshake commands.
//
// IMPORTANT: this function validates the implementation against itself.
// If a P2P command is removed from dispatchNetworkFrame, it silently
// disappears from this set. Use requiredP2PWireCommands (the protocol
// oracle) as the independent anchor — see TestRequiredP2PCommandsPresent.
func derivedP2PCommands(t *testing.T) map[string]bool {
	t.Helper()
	allNetworkCases := extractSwitchCasesFromFunc(t, "dispatchNetworkFrame")
	p2p := make(map[string]bool)
	for cmd := range allNetworkCases {
		if !handshakeCommands[cmd] {
			p2p[cmd] = true
		}
	}
	if len(p2p) == 0 {
		t.Fatal("no P2P commands derived from AST — something is wrong")
	}
	return p2p
}

// derivedDataOnlyCommands returns the data-only command set derived from AST:
// all cases in handleLocalFrameDispatch minus all cases in
// dispatchNetworkFrame. This is the set of commands that exist locally
// but must NOT be reachable from the network.
//
// IMPORTANT: this function validates the implementation against itself.
// If a P2P command is removed from dispatchNetworkFrame but still exists
// in handleLocalFrameDispatch, it silently migrates to this data-only set.
// See TestRequiredP2PCommandsPresent for the independent oracle check.
func derivedDataOnlyCommands(t *testing.T) map[string]bool {
	t.Helper()
	localCases := extractSwitchCasesFromFunc(t, "handleLocalFrameDispatch")
	networkCases := extractSwitchCasesFromFunc(t, "dispatchNetworkFrame")
	dataOnly := make(map[string]bool)
	for cmd := range localCases {
		if !networkCases[cmd] {
			dataOnly[cmd] = true
		}
	}
	if len(dataOnly) == 0 {
		t.Fatal("no data-only commands derived — handleLocalFrameDispatch " +
			"and dispatchNetworkFrame have identical command sets")
	}
	return dataOnly
}

// --- Invariant: p2pWireCommands matches the real dispatch switch ---

// TestP2PWireCommandsMatchesDispatch verifies that the runtime
// p2pWireCommands map (used by the auth gate) is exactly the set of
// non-handshake commands in the dispatchNetworkFrame switch.
// A mismatch means:
//   - missing entry → unauth peer gets unknown_command instead of
//     auth_required (weakened protocol contract)
//   - extra entry → unauth peer gets auth_required for a command
//     that doesn't exist (confusing, but not a security issue)
func TestP2PWireCommandsMatchesDispatch(t *testing.T) {
	t.Parallel()

	derived := derivedP2PCommands(t)

	for cmd := range derived {
		if !p2pWireCommands[cmd] {
			t.Errorf("command %q is in dispatchNetworkFrame P2P switch but "+
				"missing from p2pWireCommands — unauth peer will get "+
				"unknown_command instead of auth_required", cmd)
		}
	}
	for cmd := range p2pWireCommands {
		if !derived[cmd] {
			t.Errorf("p2pWireCommands contains %q which has no case in "+
				"dispatchNetworkFrame — phantom entry", cmd)
		}
	}
}

// --- Invariant: protocol oracle matches implementation ---

// TestRequiredP2PCommandsPresent is the independent oracle test.
// It catches the exact regression that AST-derived tests cannot:
// if a required P2P wire command is removed from dispatchNetworkFrame,
// derivedP2PCommands silently reclassifies it as data-only and all
// AST tests pass. This test fails because the oracle is hardcoded
// independently of the implementation.
//
// Failure here means one of:
//   - A required wire command was accidentally removed from
//     dispatchNetworkFrame → restore it
//   - A wire command was intentionally moved to data-only →
//     remove it from requiredP2PWireCommands with a comment
func TestRequiredP2PCommandsPresent(t *testing.T) {
	t.Parallel()

	networkCases := extractSwitchCasesFromFunc(t, "dispatchNetworkFrame")

	for cmd := range requiredP2PWireCommands {
		if !networkCases[cmd] {
			t.Errorf("WIRE-CONTRACT: required P2P command %q is missing from "+
				"dispatchNetworkFrame — protocol oracle requires it on the "+
				"TCP data port. If this removal is intentional, update "+
				"requiredP2PWireCommands with a comment explaining why", cmd)
		}
		if !p2pWireCommands[cmd] {
			t.Errorf("WIRE-CONTRACT: required P2P command %q is missing from "+
				"p2pWireCommands — auth gate will return unknown_command "+
				"instead of auth_required for unauthenticated peers", cmd)
		}
	}

	// Reverse: every command in the AST-derived P2P set should be in the oracle.
	// If a new command appears in dispatchNetworkFrame but not in the oracle,
	// the developer forgot to register it — flag it so the oracle stays complete.
	derived := derivedP2PCommands(t)
	for cmd := range derived {
		if !requiredP2PWireCommands[cmd] {
			t.Errorf("P2P command %q exists in dispatchNetworkFrame but is not "+
				"in requiredP2PWireCommands — add it to the protocol oracle "+
				"so future removals are caught", cmd)
		}
	}
}

// --- Invariant: no data commands on the network ---

// TestNoDataCommandsInNetworkDispatch verifies that commands exclusive to
// handleLocalFrameDispatch do not appear in dispatchNetworkFrame. The
// data-only set is computed from AST, not a manual list, so newly added
// local-only commands (add_peer, fetch_dm_headers, etc.) are automatically
// covered.
func TestNoDataCommandsInNetworkDispatch(t *testing.T) {
	t.Parallel()

	dataOnly := derivedDataOnlyCommands(t)
	networkCases := extractSwitchCasesFromFunc(t, "dispatchNetworkFrame")

	for cmd := range dataOnly {
		if networkCases[cmd] {
			t.Errorf("SECURITY: data-only command %q found in "+
				"dispatchNetworkFrame — must live exclusively in "+
				"handleLocalFrameDispatch/RPC", cmd)
		}
	}
}

// TestDispatchNetworkFrameOnlyP2PAndHandshake verifies that every case
// in dispatchNetworkFrame is either a handshake or a P2P command — no
// unexpected entries.
func TestDispatchNetworkFrameOnlyP2PAndHandshake(t *testing.T) {
	t.Parallel()

	switchCases := extractSwitchCasesFromFunc(t, "dispatchNetworkFrame")
	derived := derivedP2PCommands(t)

	for cmd := range switchCases {
		if !handshakeCommands[cmd] && !derived[cmd] {
			t.Errorf("dispatchNetworkFrame contains unexpected command %q — "+
				"not handshake, not P2P", cmd)
		}
	}

	// Every expected handshake command must be present.
	for cmd := range handshakeCommands {
		if !switchCases[cmd] {
			t.Errorf("handshake command %q missing from dispatchNetworkFrame", cmd)
		}
	}
}

// TestHandleLocalFrameDispatchCoversAllDataCommands verifies that every
// data-only command has a handler in handleLocalFrameDispatch (it's the
// identity check — derivedDataOnlyCommands already comes from
// handleLocalFrameDispatch, so this test just validates the derivation
// isn't empty and the count is sane).
func TestHandleLocalFrameDispatchCoversAllDataCommands(t *testing.T) {
	t.Parallel()

	dataOnly := derivedDataOnlyCommands(t)
	// Sanity: we expect at least 15 data-only commands (the 20 that were
	// removed + the 3 that were always local-only: add_peer,
	// fetch_dm_headers, fetch_relay_status). Some may share with
	// dispatchNetworkFrame (hello, ping are in both local and network),
	// so the diff gives us the net data-only set.
	if len(dataOnly) < 15 {
		t.Errorf("expected at least 15 data-only commands, got %d: %v",
			len(dataOnly), dataOnly)
	}
}

// --- Auth gating tests ---

// TestIsConnAuthenticated_NilNetCore ensures that a connection with no
// registered NetCore defaults to unauthenticated (fail-safe).
func TestIsConnAuthenticated_NilNetCore(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)

	id, _ := svc.connIDFor(mc)
	if svc.isConnAuthenticated(id) {
		t.Error("isConnAuthenticated should be false for nil NetCore")
	}
}

// TestIsConnAuthenticated_NilAuth ensures that a connection with NetCore
// but nil auth state defaults to unauthenticated.
func TestIsConnAuthenticated_NilAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	svc.setTestConnEntryLocked(mc, &connEntry{core: &netcore.NetCore{}})

	id, _ := svc.connIDFor(mc)
	if svc.isConnAuthenticated(id) {
		t.Error("isConnAuthenticated should be false for nil auth")
	}
}

// TestIsConnAuthenticated_UnverifiedAuth ensures that a connection with
// auth state where Verified=false is unauthenticated (auth in progress).
func TestIsConnAuthenticated_UnverifiedAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	nc := &netcore.NetCore{}
	nc.SetAuth(&connauth.State{Verified: false})
	svc.setTestConnEntryLocked(mc, &connEntry{core: nc})

	id, _ := svc.connIDFor(mc)
	if svc.isConnAuthenticated(id) {
		t.Error("isConnAuthenticated should be false for unverified auth")
	}
}

// TestIsConnAuthenticated_VerifiedAuth ensures that a connection with
// verified auth state is authenticated.
func TestIsConnAuthenticated_VerifiedAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	nc := &netcore.NetCore{}
	nc.SetAuth(&connauth.State{Verified: true})
	svc.setTestConnEntryLocked(mc, &connEntry{core: nc})

	id, _ := svc.connIDFor(mc)
	if !svc.isConnAuthenticated(id) {
		t.Error("isConnAuthenticated should be true for verified auth")
	}
}

// --- isAuthInitiated tests ---

// TestIsAuthInitiated_NilState ensures that a connection with no auth state
// reports auth not initiated.
func TestIsAuthInitiated_NilState(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	svc.setTestConnEntryLocked(mc, &connEntry{core: &netcore.NetCore{}})

	id, _ := svc.connIDFor(mc)
	if svc.isAuthInitiated(id) {
		t.Error("isAuthInitiated should be false for nil auth state")
	}
}

// TestIsAuthInitiated_PendingAuth ensures that a connection with a pending
// auth state (challenge issued) reports auth initiated.
func TestIsAuthInitiated_PendingAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	nc := &netcore.NetCore{}
	nc.SetAuth(&connauth.State{Verified: false, Challenge: "test-challenge"})
	svc.setTestConnEntryLocked(mc, &connEntry{core: nc})

	id, _ := svc.connIDFor(mc)
	if !svc.isAuthInitiated(id) {
		t.Error("isAuthInitiated should be true when challenge has been issued")
	}
}

// TestIsAuthInitiated_CompletedAuth ensures that a connection with completed
// auth reports auth initiated.
func TestIsAuthInitiated_CompletedAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	nc := &netcore.NetCore{}
	nc.SetAuth(&connauth.State{Verified: true})
	svc.setTestConnEntryLocked(mc, &connEntry{core: nc})

	id, _ := svc.connIDFor(mc)
	if !svc.isAuthInitiated(id) {
		t.Error("isAuthInitiated should be true for completed auth")
	}
}

// --- GAP-0 security: role does NOT depend on frame.Client ---

// TestGAP0_ClientFieldDoesNotAffectAuth verifies the core GAP-0 fix:
// frame.Client ("mobile", "attacker", etc.) does NOT influence auth status.
// The auth state depends solely on server-side ConnAuthState.
func TestGAP0_ClientFieldDoesNotAffectAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newSimpleMockConn(t)
	// No auth state — regardless of what Client field might say in the hello
	// frame, the connection is unauthenticated.
	svc.setTestConnEntryLocked(mc, &connEntry{core: &netcore.NetCore{}})

	id, _ := svc.connIDFor(mc)
	if svc.isConnAuthenticated(id) {
		t.Fatal("SECURITY: isConnAuthenticated = true for unauth peer — " +
			"auth must not depend on frame.Client")
	}
}

// TestLoopback_DoesNotElevateAuth verifies that a loopback (127.0.0.1)
// connection does NOT receive elevated privileges. The auth status is
// determined by auth state, never by source IP.
func TestLoopback_DoesNotElevateAuth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceForAuth()
	mc := newMockConnWithAddr(t,
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345},
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321},
	)
	svc.setTestConnEntryLocked(mc, &connEntry{core: &netcore.NetCore{}}) // no auth

	id, _ := svc.connIDFor(mc)
	if svc.isConnAuthenticated(id) {
		t.Fatal("SECURITY: loopback connection authenticated without auth_session — " +
			"source IP must not elevate auth")
	}
}
