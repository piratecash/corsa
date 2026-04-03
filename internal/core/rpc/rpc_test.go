package rpc_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"corsa/internal/core/config"
	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
	"corsa/internal/core/rpc"
	"corsa/internal/core/service"
)

// mockNodeProvider is a test double for NodeProvider.
// It allows configurable responses for testing different scenarios.
type mockNodeProvider struct {
	handleFunc func(frame protocol.Frame) protocol.Frame
	address    string
	version    string
}

// HandleLocalFrame returns either the custom handleFunc result or a default response.
func (m *mockNodeProvider) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	if m.handleFunc != nil {
		return m.handleFunc(frame)
	}
	return protocol.Frame{Type: "ok"}
}

// Address returns the configured address or a default test address.
func (m *mockNodeProvider) Address() string {
	if m.address != "" {
		return m.address
	}
	return "test-address-abc123"
}

// ClientVersion returns the configured version or a default version.
func (m *mockNodeProvider) ClientVersion() string {
	if m.version != "" {
		return m.version
	}
	return "0.16-alpha"
}

// mockChatlogProvider is a test double for ChatlogProvider.
// It allows configurable responses for testing different scenarios.
type mockChatlogProvider struct {
	chatlogResult       string
	chatlogErr          error
	chatlogTopic        string
	chatlogPeerAddress  string
	previewsResult      string
	previewsErr         error
	conversationsResult string
	conversationsErr    error
}

// FetchChatlog returns the configured chatlog result or error.
// Captures topic and peerAddress for test assertions.
func (m *mockChatlogProvider) FetchChatlog(topic, peerAddress string) (string, error) {
	m.chatlogTopic = topic
	m.chatlogPeerAddress = peerAddress
	return m.chatlogResult, m.chatlogErr
}

// FetchChatlogPreviews returns the configured previews result or error.
func (m *mockChatlogProvider) FetchChatlogPreviews() (string, error) {
	return m.previewsResult, m.previewsErr
}

// FetchConversations returns the configured conversations result or error.
func (m *mockChatlogProvider) FetchConversations() (string, error) {
	return m.conversationsResult, m.conversationsErr
}

// mockDMRouterProvider is a test double for DMRouterProvider.
type mockDMRouterProvider struct {
	lastTo   string
	lastBody string
}

func (m *mockDMRouterProvider) Snapshot() service.RouterSnapshot {
	return service.RouterSnapshot{}
}

func (m *mockDMRouterProvider) SendMessage(to domain.PeerIdentity, body string) {
	m.lastTo = string(to)
	m.lastBody = body
}

// buildTestTable creates a CommandTable with all commands registered using given providers.
// Pass nil for chatlogProvider, dmRouter, or metricsProvider to simulate modes where
// those features are unavailable — their commands are registered via RegisterUnavailable
// (503, hidden from help).
func buildTestTable(nodeProvider rpc.NodeProvider, chatlogProvider rpc.ChatlogProvider, dmRouter rpc.DMRouterProvider, metricsProvider rpc.MetricsProvider) *rpc.CommandTable {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, nodeProvider, chatlogProvider, dmRouter, metricsProvider, nil)
	return table
}

// setupTestServer creates a test RPC server with chatlog=nil, dmRouter=nil,
// metricsProvider=nil — simulating a minimal node where only core commands are available.
func setupTestServer(t *testing.T, nodeProvider rpc.NodeProvider, chatlogProvider rpc.ChatlogProvider) *rpc.Server {
	t.Helper()
	return setupTestServerWithDMRouter(t, nodeProvider, chatlogProvider, nil)
}

// setupTestServerWithDMRouter creates a test RPC server with an explicit DMRouterProvider.
// MetricsProvider defaults to nil; use setupTestServerWithMetrics for metrics tests.
func setupTestServerWithDMRouter(t *testing.T, nodeProvider rpc.NodeProvider, chatlogProvider rpc.ChatlogProvider, dmRouter rpc.DMRouterProvider) *rpc.Server {
	t.Helper()
	cfg := config.RPC{
		Host: "127.0.0.1",
		Port: "0",
	}
	table := buildTestTable(nodeProvider, chatlogProvider, dmRouter, nil)
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create test server: %v", err)
	}
	return server
}

// postJSON sends a POST request with JSON body and returns status code and parsed response.
// Returns the response body as a map[string]interface{} or raw string in "_raw" key if unmarshal fails.
func postJSON(t *testing.T, server *rpc.Server, path string, body interface{}) (int, map[string]interface{}) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req := httptest.NewRequest("POST", path, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		// might be an array or raw value
		result = map[string]interface{}{"_raw": string(respBody)}
	}
	return resp.StatusCode, result
}

// postJSONWithAuth sends a POST request with authentication header.
func postJSONWithAuth(t *testing.T, server *rpc.Server, path string, body interface{}, username, password string) (int, map[string]interface{}) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req := httptest.NewRequest("POST", path, strings.NewReader(string(data)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := server.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		result = map[string]interface{}{"_raw": string(respBody)}
	}
	return resp.StatusCode, result
}

// expectStatusCode asserts that the response status code matches the expected value.
func expectStatusCode(t *testing.T, actual, expected int) {
	t.Helper()
	if actual != expected {
		t.Errorf("expected status %d, got %d", expected, actual)
	}
}

// expectField asserts that a field exists and has the expected value.
func expectField(t *testing.T, result map[string]interface{}, field string, expected interface{}) {
	t.Helper()
	actual, exists := result[field]
	if !exists {
		t.Errorf("expected field %q not found in response", field)
		return
	}
	if actual != expected {
		t.Errorf("field %q: expected %v, got %v", field, expected, actual)
	}
}

// errorWithMessage creates a simple error for testing.
func errorWithMessage(msg string) error {
	return fmt.Errorf("%s", msg)
}

// expectFieldExists asserts that a field exists in the response.
func expectFieldExists(t *testing.T, result map[string]interface{}, field string) {
	t.Helper()
	if _, exists := result[field]; !exists {
		t.Errorf("expected field %q not found in response", field)
	}
}
