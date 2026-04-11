package rpc_test

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

type connTestNodeProvider struct{}

func (m *connTestNodeProvider) HandleLocalFrame(protocol.Frame) protocol.Frame {
	return protocol.Frame{Type: "ok"}
}
func (m *connTestNodeProvider) Address() string                              { return "test:64646" }
func (m *connTestNodeProvider) ClientVersion() string                        { return "test/1.0" }
func (m *connTestNodeProvider) FetchFileTransfers() (json.RawMessage, error) { return nil, nil }
func (m *connTestNodeProvider) FetchFileMappings() (json.RawMessage, error)  { return nil, nil }
func (m *connTestNodeProvider) RetryFileChunk(domain.FileID) error           { return nil }
func (m *connTestNodeProvider) StartFileDownload(domain.FileID) error        { return nil }
func (m *connTestNodeProvider) CancelFileDownload(domain.FileID) error       { return nil }
func (m *connTestNodeProvider) RestartFileDownload(domain.FileID) error      { return nil }

type mockConnDiagProvider struct{}

func (m *mockConnDiagProvider) ActivePeersJSON() (json.RawMessage, error) {
	return json.Marshal(struct {
		Slots    []interface{} `json:"slots"`
		Count    int           `json:"count"`
		MaxSlots int           `json:"max_slots"`
	}{Slots: []interface{}{}, Count: 0, MaxSlots: 8})
}

func (m *mockConnDiagProvider) ListPeersJSON() (json.RawMessage, error) {
	return json.Marshal(struct {
		Peers []interface{} `json:"peers"`
		Count int           `json:"count"`
	}{Peers: []interface{}{}, Count: 0})
}

func (m *mockConnDiagProvider) ListBannedJSON() (json.RawMessage, error) {
	return json.Marshal(struct {
		BannedIPs []interface{} `json:"banned_ips"`
		Count     int           `json:"count"`
	}{BannedIPs: []interface{}{}, Count: 0})
}

// nodeWithConnDiag combines NodeProvider + ConnectionDiagnosticProvider for
// RegisterAllCommands tests. Type assertion inside RegisterAllCommands discovers
// the ConnectionDiagnosticProvider capability.
type nodeWithConnDiag struct {
	connTestNodeProvider
	mockConnDiagProvider
}

// ---------------------------------------------------------------------------
// Tests: command registration
// ---------------------------------------------------------------------------

func TestConnectionCommandsRegistered(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{}, nil, nil, nil)

	expected := []string{"getActivePeers", "listPeers", "listBanned"}
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
	rpc.RegisterAllCommands(table, &connTestNodeProvider{}, nil, nil, nil)

	commands := table.Commands()
	cmdSet := make(map[string]struct{}, len(commands))
	for _, c := range commands {
		cmdSet[c.Name] = struct{}{}
	}

	for _, name := range []string{"getActivePeers", "listPeers", "listBanned"} {
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
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{}, nil, nil, nil)

	for _, alias := range []string{"get_active_peers", "list_peers", "list_banned"} {
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
	rpc.RegisterAllCommands(table, &nodeWithConnDiag{}, nil, nil, nil)

	for _, cmd := range []string{"getActivePeers", "listPeers", "listBanned"} {
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
