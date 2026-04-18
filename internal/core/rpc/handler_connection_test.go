package rpc_test

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

func newMockConnDiagProvider(t *testing.T) *rpcmocks.MockConnectionDiagnosticProvider {
	t.Helper()
	m := rpcmocks.NewMockConnectionDiagnosticProvider(t)
	emptySlots, _ := json.Marshal(struct {
		Slots    []interface{} `json:"slots"`
		Count    int           `json:"count"`
		MaxSlots int           `json:"max_slots"`
	}{[]interface{}{}, 0, 8})
	m.On("ActivePeersJSON").Return(json.RawMessage(emptySlots), nil).Maybe()
	emptyPeers, _ := json.Marshal(struct {
		Peers []interface{} `json:"peers"`
		Count int           `json:"count"`
	}{[]interface{}{}, 0})
	m.On("ListPeersJSON").Return(json.RawMessage(emptyPeers), nil).Maybe()
	emptyBanned, _ := json.Marshal(struct {
		BannedIPs []interface{} `json:"banned_ips"`
		Count     int           `json:"count"`
	}{[]interface{}{}, 0})
	m.On("ListBannedJSON").Return(json.RawMessage(emptyBanned), nil).Maybe()
	emptyConns, _ := json.Marshal(struct {
		Version     int           `json:"version"`
		Connections []interface{} `json:"connections"`
		Count       int           `json:"count"`
	}{1, []interface{}{}, 0})
	m.On("ActiveConnectionsJSON").Return(json.RawMessage(emptyConns), nil).Maybe()
	return m
}

// nodeWithConnDiag combines NodeProvider + ConnectionDiagnosticProvider for
// RegisterAllCommands tests. Type assertion inside RegisterAllCommands discovers
// the ConnectionDiagnosticProvider capability.
type nodeWithConnDiag struct {
	*rpcmocks.MockNodeProvider
	*rpcmocks.MockConnectionDiagnosticProvider
}

// ---------------------------------------------------------------------------
// Tests: command registration
// ---------------------------------------------------------------------------

func TestConnectionCommandsRegistered(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{newDefaultNodeProvider(t), newMockConnDiagProvider(t)}, nil, nil, nil)

	expected := []string{"getActivePeers", "getActiveConnections", "listPeers", "listBanned"}
	commands := table.Commands()
	cmdSet := make(map[string]struct{}, len(commands))
	for _, c := range commands {
		cmdSet[c.Name] = struct{}{}
	}

	for _, name := range expected {
		if _, ok := cmdSet[name]; !ok {
			t.Errorf("expected command %q to be registered, not found in %v", name, commands)
		}
	}
}

func TestConnectionCommandsUnavailableWhenNil(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

	commands := table.Commands()
	cmdSet := make(map[string]struct{}, len(commands))
	for _, c := range commands {
		cmdSet[c.Name] = struct{}{}
	}

	for _, name := range []string{"getActivePeers", "getActiveConnections", "listPeers", "listBanned"} {
		if _, ok := cmdSet[name]; ok {
			t.Errorf("command %q should be unavailable (hidden) when provider is nil", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: snake_case aliases
// ---------------------------------------------------------------------------

func TestConnectionCommandsSnakeCaseAliases(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{newDefaultNodeProvider(t), newMockConnDiagProvider(t)}, nil, nil, nil)

	for _, alias := range []string{"get_active_peers", "get_active_connections", "list_peers", "list_banned"} {
		resp := table.Execute(rpc.CommandRequest{Name: alias})
		if resp.Error != nil {
			t.Errorf("snake_case alias %q returned error: %v", alias, resp.Error)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests: empty state returns valid JSON, not errors
// ---------------------------------------------------------------------------

func TestConnectionCommandsEmptyState(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{newDefaultNodeProvider(t), newMockConnDiagProvider(t)}, nil, nil, nil)

	for _, cmd := range []string{"getActivePeers", "getActiveConnections", "listPeers", "listBanned"} {
		resp := table.Execute(rpc.CommandRequest{Name: cmd})
		if resp.Error != nil {
			t.Errorf("%s returned error: %v", cmd, resp.Error)
			continue
		}
		if !json.Valid(resp.Data) {
			t.Errorf("%s returned invalid JSON: %s", cmd, string(resp.Data))
		}
	}
}
