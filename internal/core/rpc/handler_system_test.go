package rpc_test

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
)

// mockDiagnosticProvider implements rpc.DiagnosticProvider for testing.
type mockDiagnosticProvider struct {
	desktopVersion string
}

func (m *mockDiagnosticProvider) DesktopVersion() string {
	if m.desktopVersion == "" {
		return "1.0.0-test"
	}
	return m.desktopVersion
}

func TestSystemVersion(t *testing.T) {
	node := &mockNodeProvider{
		address: "peer-addr-12345",
		version: "0.16-alpha",
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "peer-addr-12345")
	expectField(t, result, "protocol_version", float64(config.ProtocolVersion))
}

func TestSystemVersionDefaultValues(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "test-address-abc123")
}

func TestSystemPing(t *testing.T) {
	expectedPeers := []interface{}{"peer1", "peer2", "peer3"}
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "ping" {
				return protocol.Frame{
					Type:  "ping_response",
					Peers: []string{"peer1", "peer2", "peer3"},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/ping", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ping_response")

	// Check peers array
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
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			return protocol.Frame{
				Type:  "ping_response",
				Peers: []string{},
			}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/ping", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "ping_response")
}

func TestSystemHello(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "hello" {
				return protocol.Frame{
					Type:    "hello_response",
					Address: "my-address",
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/hello", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "hello_response")
	expectField(t, result, "address", "my-address")
}

func TestSystemHelloSendsProtocolMetadata(t *testing.T) {
	// The hello handler must populate protocol version and client info
	// in the frame it sends to HandleLocalFrame. Without these fields,
	// a real node.Service rejects the handshake with "protocol version 0 is too old".
	var receivedFrame protocol.Frame
	node := &mockNodeProvider{
		version: "test-1.0",
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			receivedFrame = frame
			return protocol.Frame{Type: "welcome"}
		},
	}
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
	// After RegisterDesktopOverrides, raw ping must retain its base
	// semantics (pong frame), not return the enriched view payload.
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "ping" {
				return protocol.Frame{
					Type:  "pong",
					Peers: []string{"peer1"},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	diag := &mockDiagnosticProvider{}
	rpc.RegisterDesktopOverrides(table, diag, node)

	resp := table.Execute(rpc.CommandRequest{Name: "ping"})
	if resp.Error != nil {
		t.Fatalf("raw ping error: %v", resp.Error)
	}
	// Raw ping returns a pong frame, not enriched diagnostic JSON.
	if strings.Contains(string(resp.Data), `"count"`) {
		t.Errorf("raw ping should not contain enriched fields, got %s", string(resp.Data))
	}
	if !strings.Contains(string(resp.Data), `"pong"`) {
		t.Errorf("raw ping should return pong frame, got %s", string(resp.Data))
	}
}

func TestDesktopOverrideGetPeersNotReplaced(t *testing.T) {
	// After RegisterDesktopOverrides, raw getPeers must retain its base
	// semantics (peer-exchange frame), not return the enriched view payload.
	// Production node returns Frame{Type: "peers", Peers: [...]} for get_peers.
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			if frame.Type == "get_peers" {
				return protocol.Frame{
					Type:  "peers",
					Peers: []string{"peer-a", "peer-b"},
				}
			}
			return protocol.Frame{Type: "ok"}
		},
	}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	diag := &mockDiagnosticProvider{}
	rpc.RegisterDesktopOverrides(table, diag, node)

	resp := table.Execute(rpc.CommandRequest{Name: "getPeers"})
	if resp.Error != nil {
		t.Fatalf("raw getPeers error: %v", resp.Error)
	}
	// Raw getPeers returns the node's wire frame, not enriched view JSON.
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
	// RegisterDesktopOverrides(nil) should not panic or change anything.
	node := &mockNodeProvider{}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)
	rpc.RegisterDesktopOverrides(table, nil, node)

	resp := table.Execute(rpc.CommandRequest{Name: "ping"})
	if resp.Error != nil {
		t.Fatalf("ping should still work after nil override: %v", resp.Error)
	}
}

// TestDesktopOverrideNilNodePanics verifies that passing a non-nil DiagnosticProvider
// with a nil NodeProvider panics early instead of crashing later in a handler.
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

	diag := &mockDiagnosticProvider{}
	table := rpc.NewCommandTable()
	rpc.RegisterDesktopOverrides(table, diag, nil) // should panic
}

func TestDesktopOverrideHelloIdentifiesAsDesktop(t *testing.T) {
	var capturedFrame protocol.Frame
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
			capturedFrame = frame
			return protocol.Frame{Type: "welcome"}
		},
	}
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

	diag := &mockDiagnosticProvider{desktopVersion: "2.5.0"}
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
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectFieldExists(t, result, "commands")

	// Verify commands is an array
	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Errorf("expected commands to be array, got %T", result["commands"])
		return
	}

	// Verify we have multiple commands from all services
	if len(commands) < 5 {
		t.Errorf("expected at least 5 commands, got %d", len(commands))
	}

	// Verify system commands are present
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
	node := &mockNodeProvider{}
	chatlog := &mockChatlogProvider{}
	server := setupTestServer(t, node, chatlog)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array")
	}

	// Extract all command names and categories
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

	// Verify we have commands from different categories
	expectedCategories := []string{"system", "network", "identity", "message", "file", "chatlog", "notice"}
	for _, cat := range expectedCategories {
		if _, exists := categories[cat]; !exists {
			t.Errorf("expected category %q not found", cat)
		}
	}
}

func TestSystemCommandMetadata(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array")
	}

	// Verify that commands have required fields
	for _, cmd := range commands {
		cmdMap, ok := cmd.(map[string]interface{})
		if !ok {
			continue
		}

		// Check required fields
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
	// Commands that accept arguments must expose a non-empty "usage" field
	// in help output. The desktop console renders suggestion templates from
	// this field — if it regresses to empty, the UI loses argument hints.
	node := &mockNodeProvider{}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil)

	commands := table.Commands()

	// Commands that take arguments and MUST have Usage set.
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

	// No-arg commands must NOT have Usage (keeps help clean).
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
	// When providers are present, mode-gated commands (send_dm, chatlog)
	// must also have Usage set for desktop console suggestion templates.
	node := &mockNodeProvider{}
	chatlog := &mockChatlogProvider{}
	dmRouter := &mockDMRouterProvider{}

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
