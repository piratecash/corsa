package rpc_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// newMockDiagnosticProvider creates a MockDiagnosticProvider with given version.
func newMockDiagnosticProvider(t *testing.T, version string) *rpcmocks.MockDiagnosticProvider {
	t.Helper()
	m := rpcmocks.NewMockDiagnosticProvider(t)
	m.On("DesktopVersion").Return(version).Maybe()
	return m
}

func TestSystemVersion(t *testing.T) {
	node := newNodeProviderWithMeta(t, "peer-addr-12345", "0.16-alpha")
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "peer-addr-12345")
	expectField(t, result, "protocol_version", float64(config.ProtocolVersion))
}

func TestSystemVersionDefaultValues(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "test-address-abc123")
}

func TestSystemPing(t *testing.T) {
	expectedPeers := []interface{}{"peer1", "peer2", "peer3"}
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "ping" {
			return protocol.Frame{
				Type:  "ping_response",
				Peers: []string{"peer1", "peer2", "peer3"},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/ping", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ping_response")

	peers, ok := result["peers"].([]interface{})
	if !ok {
		t.Errorf("expected peers to be array, got %T", result["peers"])
		return
	}
	if len(peers) != len(expectedPeers) {
		t.Errorf("expected %d peers, got %d", len(expectedPeers), len(peers))
	}
}

func TestSystemPingEmptyPeers(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		return protocol.Frame{
			Type:  "ping_response",
			Peers: []string{},
		}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/ping", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ping_response")
}

func TestSystemHello(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "hello" {
			return protocol.Frame{
				Type:    "hello_response",
				Address: "my-address",
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/hello", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "hello_response")
	expectField(t, result, "address", "my-address")
}

func TestSystemHelloSendsProtocolMetadata(t *testing.T) {
	var receivedFrame protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		receivedFrame = frame
		return protocol.Frame{Type: "welcome"}
	})
	// Override ClientVersion for this specific test.
	node.ExpectedCalls = filterCalls(node.ExpectedCalls, "ClientVersion")
	node.On("ClientVersion").Return("test-1.0").Maybe()

	server := setupTestServer(t, node, nil)

	code, _ := postJSON(t, server, "/rpc/v1/system/hello", map[string]interface{}{})
	expectStatusCode(t, code, 200)

	if receivedFrame.Version < config.MinimumProtocolVersion {
		t.Errorf("hello frame Version=%d, want >= %d (MinimumProtocolVersion)",
			receivedFrame.Version, config.MinimumProtocolVersion)
	}
	if receivedFrame.ClientVersion == "" {
		t.Error("hello frame ClientVersion is empty, want non-empty")
	}
	if receivedFrame.ClientBuild != config.ClientBuild {
		t.Errorf("hello frame ClientBuild=%d, want %d", receivedFrame.ClientBuild, config.ClientBuild)
	}
}

func TestDesktopOverridePingNotReplaced(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "ping" {
			return protocol.Frame{
				Type:  "pong",
				Peers: []string{"peer1"},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	diag := newMockDiagnosticProvider(t, "1.0.0-test")
	rpc.RegisterDesktopOverrides(table, diag, node)

	resp := table.Execute(rpc.CommandRequest{Name: "ping"})
	if resp.Error != nil {
		t.Fatalf("raw ping error: %v", resp.Error)
	}
	if strings.Contains(string(resp.Data), `"count"`) {
		t.Errorf("raw ping should not contain enriched fields, got %s", string(resp.Data))
	}
	if !strings.Contains(string(resp.Data), `"pong"`) {
		t.Errorf("raw ping should return pong frame, got %s", string(resp.Data))
	}
}

func TestDesktopOverrideGetPeersNotReplaced(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "get_peers" {
			return protocol.Frame{
				Type:  "peers",
				Peers: []string{"peer-a", "peer-b"},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	diag := newMockDiagnosticProvider(t, "1.0.0-test")
	rpc.RegisterDesktopOverrides(table, diag, node)

	resp := table.Execute(rpc.CommandRequest{Name: "getPeers"})
	if resp.Error != nil {
		t.Fatalf("raw getPeers error: %v", resp.Error)
	}
	if strings.Contains(string(resp.Data), `"total"`) {
		t.Errorf("raw getPeers should not contain enriched fields, got %s", string(resp.Data))
	}
	if !strings.Contains(string(resp.Data), `"peer-a"`) {
		t.Errorf("raw getPeers should contain peers from node, got %s", string(resp.Data))
	}
	if !strings.Contains(string(resp.Data), `"peers"`) {
		t.Errorf("raw getPeers should return peers frame type, got %s", string(resp.Data))
	}
}

func TestDesktopOverrideNilSkips(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)
	rpc.RegisterDesktopOverrides(table, nil, node)

	resp := table.Execute(rpc.CommandRequest{Name: "ping"})
	if resp.Error != nil {
		t.Fatalf("ping should still work after nil override: %v", resp.Error)
	}
}

func TestDesktopOverrideNilNodePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil NodeProvider with non-nil DiagnosticProvider")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "non-nil NodeProvider") {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	diag := newMockDiagnosticProvider(t, "1.0.0-test")
	table := rpc.NewCommandTable()
	rpc.RegisterDesktopOverrides(table, diag, nil) // should panic
}

func TestDesktopOverrideHelloIdentifiesAsDesktop(t *testing.T) {
	var capturedFrame protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		capturedFrame = frame
		return protocol.Frame{Type: "welcome"}
	})
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	// Before override: base handler identifies as "rpc".
	resp := table.Execute(rpc.CommandRequest{Name: "hello"})
	if resp.Error != nil {
		t.Fatalf("base hello error: %v", resp.Error)
	}
	if capturedFrame.Client != "rpc" {
		t.Errorf("base hello: expected Client='rpc', got %q", capturedFrame.Client)
	}

	diag := newMockDiagnosticProvider(t, "2.5.0")
	rpc.RegisterDesktopOverrides(table, diag, node)

	// After override: desktop handler identifies as "desktop".
	resp = table.Execute(rpc.CommandRequest{Name: "hello"})
	if resp.Error != nil {
		t.Fatalf("desktop hello error: %v", resp.Error)
	}
	if capturedFrame.Client != "desktop" {
		t.Errorf("desktop hello: expected Client='desktop', got %q", capturedFrame.Client)
	}
	if capturedFrame.ClientVersion != "2.5.0" {
		t.Errorf("desktop hello: expected ClientVersion='2.5.0', got %q", capturedFrame.ClientVersion)
	}
	if capturedFrame.ClientBuild != config.ClientBuild {
		t.Errorf("desktop hello: expected ClientBuild=%d, got %d", config.ClientBuild, capturedFrame.ClientBuild)
	}
}

func TestSystemHelp(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectFieldExists(t, result, "commands")

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Errorf("expected commands to be array, got %T", result["commands"])
		return
	}

	if len(commands) < 5 {
		t.Errorf("expected at least 5 commands, got %d", len(commands))
	}

	found := make(map[string]bool)
	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		if name, ok := cmdMap["name"].(string); ok {
			found[name] = true
		}
	}

	expectedCommands := []string{"help", "ping", "hello", "version"}
	for _, expected := range expectedCommands {
		if !found[expected] {
			t.Errorf("expected command %q not found", expected)
		}
	}
}

func TestSystemHelpIncludesAllCategories(t *testing.T) {
	node := newDefaultNodeProvider(t)
	chatlog := newDefaultChatlogProvider(t)
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array")
	}

	categories := make(map[string]int)
	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		if category, ok := cmdMap["category"].(string); ok {
			categories[category]++
		}
	}

	expectedCategories := []string{"system", "network", "identity", "message", "file", "chatlog", "notice"}
	for _, cat := range expectedCategories {
		if _, exists := categories[cat]; !exists {
			t.Errorf("expected category %q not found", cat)
		}
	}
}

func TestSystemCommandMetadata(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array")
	}

	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}
		if _, hasName := cmdMap["name"]; !hasName {
			t.Error("command missing 'name' field")
		}
		if _, hasDesc := cmdMap["description"]; !hasDesc {
			t.Error("command missing 'description' field")
		}
		if _, hasCat := cmdMap["category"]; !hasCat {
			t.Error("command missing 'category' field")
		}
	}
}

func TestSystemHelpUsagePresent(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	commands := table.Commands()

	requireUsage := map[string]bool{
		"addPeer":               true,
		"fetchMessages":         true,
		"fetchMessageIds":       true,
		"fetchMessage":          true,
		"fetchInbox":            true,
		"fetchPendingMessages":  true,
		"fetchDeliveryReceipts": true,
	}

	found := 0
	for _, cmd := range commands {
		if requireUsage[cmd.Name] {
			found++
			if cmd.Usage == "" {
				t.Errorf("command %q takes arguments but has empty Usage — desktop console will lose suggestion template", cmd.Name)
			}
		}
	}

	if found != len(requireUsage) {
		t.Errorf("expected to find %d argument-taking commands in help, found %d — some may be missing from registration", len(requireUsage), found)
	}

	noArgCommands := map[string]bool{
		"help": true, "ping": true, "hello": true, "version": true,
		"getPeers": true, "fetchPeerHealth": true,
		"fetchIdentities": true, "fetchContacts": true, "fetchTrustedContacts": true,
		"fetchNotices": true, "fetchDmHeaders": true,
	}

	for _, cmd := range commands {
		if noArgCommands[cmd.Name] && cmd.Usage != "" {
			t.Errorf("no-arg command %q has non-empty Usage %q — unexpected", cmd.Name, cmd.Usage)
		}
	}
}

func TestSystemHelpUsageModeGatedCommands(t *testing.T) {
	node := newDefaultNodeProvider(t)
	chatlog := newDefaultChatlogProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)

	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, chatlog, dmRouter, nil)

	commands := table.Commands()

	modeGatedWithUsage := map[string]bool{
		"sendDm":       true,
		"fetchChatlog": true,
	}

	found := 0
	for _, cmd := range commands {
		if modeGatedWithUsage[cmd.Name] {
			found++
			if cmd.Usage == "" {
				t.Errorf("mode-gated command %q takes arguments but has empty Usage", cmd.Name)
			}
		}
	}

	if found != len(modeGatedWithUsage) {
		t.Errorf("expected %d mode-gated commands visible with providers, found %d", len(modeGatedWithUsage), found)
	}
}

// filterCalls removes expectations for the given method name from a call list.
// Used when a default helper registered a Maybe() expectation that needs to be
// overridden with a specific return value.
func filterCalls(calls []*mock.Call, method string) []*mock.Call {
	filtered := make([]*mock.Call, 0, len(calls))
	for _, c := range calls {
		if c.Method != method {
			filtered = append(filtered, c)
		}
	}
	return filtered
}
