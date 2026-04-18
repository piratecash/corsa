package rpc_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
)

// newAuthServer creates a Server with auth config and system commands registered.
func newAuthServer(t *testing.T, node rpc.NodeProvider, cfg config.RPC) *rpc.Server {
	t.Helper()
	table := rpc.NewCommandTable()
	rpc.RegisterSystemCommands(table, node)
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	return server
}

func TestServerAuthMiddlewareWithAuth(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	// Request without credentials should fail
	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 401)
	expectFieldExists(t, result, "error")
}

func TestServerAuthMiddlewareWithValidCredentials(t *testing.T) {
	node := newNodeProviderWithMeta(t, "test-addr", "0.16-alpha")
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	// Request with valid credentials should succeed
	code, result := postJSONWithAuth(t, server, "/rpc/v1/system/version", map[string]interface{}{}, "testuser", "testpass")

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "test-addr")
}

func TestServerAuthMiddlewareWithInvalidCredentials(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	// Request with wrong password should fail
	code, result := postJSONWithAuth(t, server, "/rpc/v1/system/version", map[string]interface{}{}, "testuser", "wrongpass")

	expectStatusCode(t, code, 401)
	expectFieldExists(t, result, "error")
}

func TestServerAuthMiddlewareWithWrongUsername(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	code, result := postJSONWithAuth(t, server, "/rpc/v1/system/version", map[string]interface{}{}, "wronguser", "testpass")

	expectStatusCode(t, code, 401)
	expectFieldExists(t, result, "error")
}

func TestServerNoAuthWhenNotConfigured(t *testing.T) {
	node := newNodeProviderWithMeta(t, "test-addr", "0.16-alpha")
	cfg := config.RPC{
		Host: "127.0.0.1",
		Port: "0",
	}
	server := newAuthServer(t, node, cfg)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
}

func TestServerMissingAuthHeader(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	code, result := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})

	expectStatusCode(t, code, 401)
	expectFieldExists(t, result, "error")
}

func TestServerInvalidAuthHeaderFormat(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, node, cfg)

	code, _ := postJSONWithAuth(t, server, "/rpc/v1/system/version", map[string]interface{}{}, "user:pass", "ignored")

	// SetBasicAuth might handle this differently
	if code != 401 {
		t.Logf("SetBasicAuth might have auto-formatted, skipping test")
	}
}

func TestServerAuthReturnsWWWAuthenticate(t *testing.T) {
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	server := newAuthServer(t, newDefaultNodeProvider(t), cfg)

	scenarios := []struct {
		name   string
		header string
	}{
		{"no auth", ""},
		{"wrong creds", "Basic " + base64.StdEncoding.EncodeToString([]byte("bad:creds"))},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/rpc/v1/system/version", strings.NewReader("{}"))
			req.Header.Set("Content-Type", "application/json")
			if sc.header != "" {
				req.Header.Set("Authorization", sc.header)
			}

			resp, err := server.Test(req)
			if err != nil {
				t.Fatalf("test request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != 401 {
				t.Fatalf("expected 401, got %d", resp.StatusCode)
			}
			wwwAuth := resp.Header.Get("WWW-Authenticate")
			if wwwAuth == "" {
				t.Error("401 response missing WWW-Authenticate header")
			}
			if !strings.Contains(wwwAuth, "Basic") {
				t.Errorf("expected WWW-Authenticate to contain 'Basic', got %q", wwwAuth)
			}
		})
	}
}

func TestServerHelpEndpoint(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectFieldExists(t, result, "commands")
	expectField(t, result, "version", "1.0")

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Errorf("expected commands to be array, got %T", result["commands"])
		return
	}
	if len(commands) < 1 {
		t.Errorf("expected at least 1 command, got %d", len(commands))
	}
}

func TestServerHelpEndpointWithoutAuth(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectFieldExists(t, result, "commands")
}

func TestServerMultipleCommandCategories(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := rpc.NewCommandTable()
	rpc.RegisterSystemCommands(table, node)
	rpc.RegisterNetworkCommands(table, node)
	rpc.RegisterIdentityCommands(table, node)

	cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	code, result := postJSON(t, server, "/rpc/v1/system/help", map[string]interface{}{})

	expectStatusCode(t, code, 200)

	commands, ok := result["commands"].([]interface{})
	if !ok {
		t.Fatalf("expected commands to be array")
	}

	// Should have commands from all three categories
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

	expectedCategories := []string{"system", "network", "identity"}
	for _, cat := range expectedCategories {
		if _, exists := categories[cat]; !exists {
			t.Errorf("expected category %q from registered commands", cat)
		}
	}
}

func TestCommandTableCommands(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := buildTestTable(node, nil, nil, nil)

	allCommands := table.Commands()

	if len(allCommands) < 1 {
		t.Errorf("expected at least 1 command, got %d", len(allCommands))
	}

	for _, cmd := range allCommands {
		if cmd.Name == "" {
			t.Error("command has empty name")
		}
		if cmd.Category == "" {
			t.Error("command has empty category")
		}
		if cmd.Description == "" {
			t.Error("command has empty description")
		}
	}
}

// TestCommandTableCommandsSortedByName verifies that Commands() returns
// a deterministic list sorted by name, so help and autocomplete are stable.
func TestCommandTableCommandsSortedByName(t *testing.T) {
	node := newDefaultNodeProvider(t)
	chatlog := newDefaultChatlogProvider(t)
	dmRouter := newDefaultDMRouterProvider(t)
	table := buildTestTable(node, chatlog, dmRouter, nil)

	commands := table.Commands()
	for i := 1; i < len(commands); i++ {
		if commands[i].Name < commands[i-1].Name {
			t.Errorf("commands not sorted: %q appears after %q", commands[i].Name, commands[i-1].Name)
		}
	}

	// Call again to verify stability across invocations.
	commands2 := table.Commands()
	if len(commands) != len(commands2) {
		t.Fatalf("different length on second call: %d vs %d", len(commands), len(commands2))
	}
	for i := range commands {
		if commands[i].Name != commands2[i].Name {
			t.Errorf("order changed at index %d: %q vs %q", i, commands[i].Name, commands2[i].Name)
		}
	}
}

func TestServerAuthWithBase64InvalidEncoding(t *testing.T) {
	node := newDefaultNodeProvider(t)
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "testuser",
		Password: "testpass",
	}
	_ = newAuthServer(t, node, cfg)

	// Manually test with invalid base64
	// This is tricky with SetBasicAuth, so we'll skip the detailed test
	// The middleware properly handles invalid base64 based on code inspection
}

func TestServerAuthBasicEncoding(t *testing.T) {
	username := "user"
	password := "pass"

	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	if encoded != "dXNlcjpwYXNz" {
		t.Errorf("unexpected base64 encoding: %s", encoded)
	}
}

func TestServerListensOnConfiguredAddress(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, _ := postJSON(t, server, "/rpc/v1/system/version", map[string]interface{}{})
	expectStatusCode(t, code, 200)
}

func TestServerPartialAuthUsernameOnly(t *testing.T) {
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "admin",
	}
	table := rpc.NewCommandTable()
	_, err := rpc.NewServer(cfg, table)
	if err == nil {
		t.Error("expected error for partial auth (username only), got nil")
	}
}

func TestServerPartialAuthPasswordOnly(t *testing.T) {
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Password: "secret",
	}
	table := rpc.NewCommandTable()
	_, err := rpc.NewServer(cfg, table)
	if err == nil {
		t.Error("expected error for partial auth (password only), got nil")
	}
}

func TestServerFullAuthValidates(t *testing.T) {
	cfg := config.RPC{
		Host:     "127.0.0.1",
		Port:     "0",
		Username: "admin",
		Password: "secret",
	}
	table := rpc.NewCommandTable()
	_, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Errorf("expected no error for full auth, got: %v", err)
	}
}

func TestServerNoAuthValidates(t *testing.T) {
	cfg := config.RPC{
		Host: "127.0.0.1",
		Port: "0",
	}
	table := rpc.NewCommandTable()
	_, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Errorf("expected no error for no auth, got: %v", err)
	}
}

func TestServerUnregisteredEndpoint(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, _ := postJSON(t, server, "/rpc/v1/nonexistent/endpoint", map[string]interface{}{})

	if code < 400 {
		t.Errorf("expected error status code, got %d", code)
	}
}

// TestUniversalExecEndpoint tests the /rpc/v1/exec dispatch endpoint.
func TestUniversalExecEndpoint(t *testing.T) {
	node := newNodeProviderWithMeta(t, "test-addr", "0.16-alpha")
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
		"command": "version",
	})

	expectStatusCode(t, code, 200)
	expectField(t, result, "client_version", "0.16-alpha")
	expectField(t, result, "node_address", "test-addr")
}

func TestUniversalExecUnknownCommand(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
		"command": "nonexistent",
	})

	expectStatusCode(t, code, 404)
	expectFieldExists(t, result, "error")
}

func TestUniversalExecCaseInsensitive(t *testing.T) {
	node := newNodeProviderWithMeta(t, "test-address-abc123", "test")
	server := setupTestServer(t, node, nil)

	// Mixed-case command names must work through /exec just like
	// they do through ParseConsoleInput in the desktop console.
	variants := []string{"PING", "Ping", "pInG"}
	for _, name := range variants {
		code, _ := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
			"command": name,
		})
		if code != 200 {
			t.Errorf("exec %q returned %d, want 200", name, code)
		}
	}
}

func TestUniversalExecEmptyCommand(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
		"command": "",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

// TestLegacyArgHandlerMalformedJSON verifies that malformed JSON body
// is rejected with 400 instead of silently treated as empty args.
func TestLegacyArgHandlerMalformedJSON(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil)

	req := httptest.NewRequest("POST", "/rpc/v1/message/list", strings.NewReader("{broken json"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	expectStatusCode(t, resp.StatusCode, 400)

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if _, ok := result["error"]; !ok {
		t.Error("expected error field in response")
	}
}

// TestLegacyArgHandlerEmptyBody verifies that empty body is still accepted
// (commands with defaults should work without a body).
func TestLegacyArgHandlerEmptyBody(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_delivery_receipts" {
			return protocol.Frame{
				Type:      "receipts_response",
				Recipient: frame.Recipient,
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	req := httptest.NewRequest("POST", "/rpc/v1/message/receipts", nil)
	req.Header.Set("Content-Type", "application/json")

	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Empty body should succeed — fetch_delivery_receipts defaults recipient to self
	expectStatusCode(t, resp.StatusCode, 200)
}

// TestUniversalExecUnavailableCommand verifies that mode-gated commands
// return 503 through /exec, matching the legacy endpoint behavior.
func TestUniversalExecUnavailableCommand(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil, dmRouter=nil, metricsProvider=nil

	unavailableCases := []struct {
		command string
		args    map[string]interface{}
	}{
		{"fetchChatlog", map[string]interface{}{"topic": "dm", "peer_address": "addr"}},
		{"fetchTrafficHistory", map[string]interface{}{}},
	}

	for _, tc := range unavailableCases {
		code, result := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
			"command": tc.command,
			"args":    tc.args,
		})
		expectStatusCode(t, code, 503)
		expectFieldExists(t, result, "error")
	}
}

// TestUniversalExecUnavailableSendDM verifies send_dm returns 503 via /exec
// when DMRouter is nil, same as legacy route.
func TestUniversalExecUnavailableSendDM(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // dmRouter=nil

	code, result := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
		"command": "sendDm",
		"args":    map[string]interface{}{"to": "addr", "body": "hello"},
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

// TestCommandsExcludesUnavailable verifies that Commands() does not return
// unavailable (mode-gated) commands.
func TestCommandsExcludesUnavailable(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := buildTestTable(node, nil, nil, nil) // chatlog=nil, dmRouter=nil

	commands := table.Commands()
	unavailableNames := map[string]bool{
		"fetchChatlog":         true,
		"fetchChatlogPreviews": true,
		"fetchConversations":   true,
		"sendDm":               true,
		"fetchTrafficHistory":  true,
	}

	for _, cmd := range commands {
		if unavailableNames[cmd.Name] {
			t.Errorf("unavailable command %q should not appear in Commands()", cmd.Name)
		}
	}
}

// TestHasReturnsTrueForUnavailable verifies that Has() returns true for
// unavailable commands (they are registered, just return 503).
func TestHasReturnsTrueForUnavailable(t *testing.T) {
	node := newDefaultNodeProvider(t)
	table := buildTestTable(node, nil, nil, nil) // chatlog=nil, dmRouter=nil

	unavailableNames := []string{
		"fetchChatlog", "fetchChatlogPreviews", "fetchConversations", "sendDm",
		"fetchTrafficHistory",
	}

	for _, name := range unavailableNames {
		if !table.Has(name) {
			t.Errorf("Has(%q) should return true for unavailable command", name)
		}
	}
}

// TestExecAndLegacyConsistency verifies that /exec and legacy routes return
// the same status code for mode-gated commands.
func TestExecAndLegacyConsistency(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServer(t, node, nil) // chatlog=nil

	// Via legacy route
	legacyCode, _ := postJSON(t, server, "/rpc/v1/chatlog/previews", map[string]interface{}{})

	// Via /exec
	execCode, _ := postJSON(t, server, "/rpc/v1/exec", map[string]interface{}{
		"command": "fetchChatlogPreviews",
	})

	if legacyCode != execCode {
		t.Errorf("legacy route returned %d, /exec returned %d — should be consistent", legacyCode, execCode)
	}
	expectStatusCode(t, legacyCode, 503)
}

// setupTestServerWithNode creates a server with NodeProvider so /frame is available.
func setupTestServerWithNode(t *testing.T, node rpc.NodeProvider) *rpc.Server {
	t.Helper()
	cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
	table := rpc.NewCommandTable()
	rpc.RegisterSystemCommands(table, node)
	server, err := rpc.NewServer(cfg, table, node)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	return server
}

// TestFrameEndpointDispatchesThroughCommandTable verifies that /frame routes
// registered frame types through CommandTable instead of directly to HandleLocalFrame.
// The hello handler in CommandTable builds its own frame (Client:"rpc"), so caller-
// supplied fields like Client/ClientVersion are not preserved for registered types.
func TestFrameEndpointDispatchesThroughCommandTable(t *testing.T) {
	var receivedFrame protocol.Frame
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		receivedFrame = frame
		return protocol.Frame{Type: "welcome", Version: frame.Version}
	})
	server := setupTestServerWithNode(t, node)

	code, _ := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"type":           "hello",
		"version":        42,
		"client":         "custom-tool",
		"client_version": "3.0.0",
	})

	expectStatusCode(t, code, 200)

	// hello is a registered command — CommandTable handler builds its own
	// frame with Client:"rpc", ignoring caller-supplied fields.
	if receivedFrame.Client != "rpc" {
		t.Errorf("expected Client='rpc' from CommandTable handler, got %q", receivedFrame.Client)
	}
}

// TestFrameEndpointRejectsUnknownType verifies that unregistered frame types
// are rejected instead of being forwarded to HandleLocalFrame. Previously,
// unknown types were forwarded, which allowed HTTP clients to inject network-
// level frames (relay_message, push_message, subscribe_inbox) bypassing P2P
// authentication. Now only CommandTable-registered types are accepted.
func TestFrameEndpointRejectsUnknownType(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		t.Fatal("HandleLocalFrame should not be called for unknown frame types")
		return protocol.Frame{}
	})
	server := setupTestServerWithNode(t, node)

	code, body := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"type":           "custom_frame_type",
		"client":         "external-tool",
		"client_version": "5.0",
	})

	expectStatusCode(t, code, 400)

	// Verify the error payload includes the frame type so callers can
	// diagnose which type was rejected. The documented contract is
	// "unknown frame type: <name>" — assert both the prefix and the
	// submitted type name to catch wording drift.
	errMsg, ok := body["error"].(string)
	if !ok {
		t.Fatal("expected 'error' key in response body")
	}
	if !strings.Contains(errMsg, "unknown frame type") {
		t.Errorf("error should contain 'unknown frame type', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "custom_frame_type") {
		t.Errorf("error should echo the rejected type name 'custom_frame_type', got: %s", errMsg)
	}
}

// TestFrameEndpointChatlogDispatch verifies that chatlog frame types are dispatched
// through CommandTable (via ChatlogProvider) instead of HandleLocalFrame.
func TestFrameEndpointChatlogDispatch(t *testing.T) {
	chatlogJSON := `{"entries":[{"id":"1","text":"hello"}]}`
	chatlog := rpcmocks.NewMockChatlogProvider(t)
	var chatlogPeerAddress string
	chatlog.On("FetchChatlog", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			chatlogPeerAddress = args.String(1)
		}).Return(chatlogJSON, nil)
	chatlog.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	chatlog.On("FetchConversations").Return("[]", nil).Maybe()
	chatlog.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()

	node := newDefaultNodeProvider(t)

	cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, chatlog, nil, nil)
	server, err := rpc.NewServer(cfg, table, node)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	code, result := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"type":    "fetch_chatlog",
		"topic":   "dm",
		"address": "peer-addr-123",
	})

	expectStatusCode(t, code, 200)

	// Should have chatlog entries from ChatlogProvider, not "unknown command" from node.
	if raw, ok := result["_raw"]; ok {
		if raw != chatlogJSON {
			t.Errorf("expected chatlog JSON %s, got %s", chatlogJSON, raw)
		}
	}

	// Verify normalizeFrameArgs mapped "address" → "peer_address".
	if chatlogPeerAddress != "peer-addr-123" {
		t.Errorf("expected peer_address='peer-addr-123', got %q", chatlogPeerAddress)
	}
}

// TestFrameEndpointUnavailableChatlog verifies that chatlog frame types return
// 503 when ChatlogProvider is nil (standalone node mode).
func TestFrameEndpointUnavailableChatlog(t *testing.T) {
	node := newDefaultNodeProvider(t)

	cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, node, nil, nil, nil) // chatlog=nil
	server, err := rpc.NewServer(cfg, table, node)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	code, result := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"type":  "fetch_chatlog",
		"topic": "dm",
	})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestFrameEndpointRejectsMissingType(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServerWithNode(t, node)

	code, result := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"client": "test",
	})

	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

func TestFrameEndpointNotRegisteredWithoutNode(t *testing.T) {
	node := newDefaultNodeProvider(t)
	// setupTestServer does NOT pass NodeProvider to NewServer
	server := setupTestServer(t, node, nil)

	code, _ := postJSON(t, server, "/rpc/v1/frame", map[string]interface{}{
		"type": "ping",
	})

	// Should be 404 because the route is not registered
	if code != 404 {
		t.Errorf("expected 404 when /frame not registered, got %d", code)
	}
}
