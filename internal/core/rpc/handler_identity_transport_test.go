package rpc_test

import (
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// TestClientFetchCommandsDecodesTransportPolicy walks the whole loop the
// transport field newly participates in: the real help handler encodes every
// registered CommandInfo, the HTTP server ships it, and rpc.Client decodes it
// back into []CommandInfo. Encoding a policy as a string without a matching
// decoder broke exactly this — and broke it for the ENTIRE list, not just the
// two restricted commands, because one undecodable field fails the response.
func TestClientFetchCommandsDecodesTransportPolicy(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

	cfg := config.RPC{Host: "127.0.0.1", Port: freeLoopbackPort(t), Username: "u", Password: "p"}
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := server.StartAsync(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.ShutdownWithTimeout(5 * time.Second) })

	commands, err := rpc.NewClient(cfg).FetchCommands()
	if err != nil {
		t.Fatalf("fetch commands: %v", err)
	}
	if len(commands) == 0 {
		t.Fatal("help returned no commands")
	}

	found := map[string]rpc.TransportPolicy{}
	for _, cmd := range commands {
		found[cmd.Name] = cmd.Transport
	}
	for _, name := range []string{"identityBackup", "identityRestore"} {
		policy, ok := found[name]
		if !ok {
			t.Fatalf("%s missing from the decoded command list", name)
		}
		if policy != rpc.TransportLoopbackOnly {
			t.Fatalf("%s decoded as %s, want loopback_only", name, policy)
		}
	}
	if policy, ok := found["ping"]; !ok || policy != rpc.TransportAnyAuthenticated {
		t.Fatalf("ping decoded as %s (present=%v), want any_authenticated", policy, ok)
	}
}

// freeLoopbackPort reserves and releases a loopback port so the server can
// bind a known one: the RPC config takes a port string and StartAsync does not
// report the bound port back, so "port 0" would leave nothing to dial.
func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split port: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

// TestIdentityBackupCommandsAreLoopbackOnly is the registration-level guard.
// The gate in server.go is only as good as the policy each command declares,
// and a policy is a single field that a future edit — a reformat, a copied
// registration block — can drop without breaking anything visible.
//
// These two commands are the ones that matter: both move BOTH private keys
// across a file boundary on the node's machine.
func TestIdentityBackupCommandsAreLoopbackOnly(t *testing.T) {
	t.Parallel()
	table := rpc.NewCommandTable()
	// The full registration, because the two backup commands live in the
	// network group (a pre-existing placement) and because the snake_case
	// aliases — a spelling the gate must also cover — are only wired up here.
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

	for _, name := range []string{"identityBackup", "identityRestore", "identity_backup", "identity_restore"} {
		policy, known := table.TransportPolicyFor(name)
		if !known {
			t.Fatalf("%s is not registered", name)
		}
		if policy != rpc.TransportLoopbackOnly {
			t.Fatalf("%s declares %s, want loopback_only", name, policy)
		}
	}
}

// TestOrdinaryIdentityCommandsStayReachable: the restriction is targeted, not
// a blanket lockdown of the identity category. A read-only command that a
// remote operator legitimately polls must keep its previous reach.
func TestOrdinaryIdentityCommandsStayReachable(t *testing.T) {
	t.Parallel()
	table := rpc.NewCommandTable()
	// The full registration, because the two backup commands live in the
	// network group (a pre-existing placement) and because the snake_case
	// aliases — a spelling the gate must also cover — are only wired up here.
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

	for _, name := range []string{"fetchIdentities", "fetchContacts", "fetchPresence", "resolveIdentity"} {
		policy, known := table.TransportPolicyFor(name)
		if !known {
			t.Fatalf("%s is not registered", name)
		}
		if policy != rpc.TransportAnyAuthenticated {
			t.Fatalf("%s declares %s, want any_authenticated", name, policy)
		}
	}
}
