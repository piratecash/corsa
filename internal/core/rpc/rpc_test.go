package rpc_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
	rpcmocks "github.com/piratecash/corsa/internal/core/rpc/mocks"
	"github.com/piratecash/corsa/internal/core/service"
)

// defaultTestNodeStatus is the NodeStatus value returned by the shared
// MockNodeProvider helpers when no test overrides it. Centralised so a
// new field on domain.NodeStatus only needs to be defaulted once.
func defaultTestNodeStatus() domain.NodeStatus {
	started := time.Date(2026, time.April, 30, 0, 0, 0, 0, time.UTC)
	return domain.NodeStatus{
		Identity:               "test-address-abc123",
		Address:                "test-address-abc123",
		PublicKey:              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		BoxPublicKey:           "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBA=",
		ProtocolVersion:        config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		ClientVersion:          "0.16-alpha",
		ClientBuild:            config.ClientBuild,
		ConnectedPeers:         3,
		StartedAt:              started,
		UptimeSeconds:          42,
		CurrentTime:            started.Add(42 * time.Second),
	}
}

// ---------------------------------------------------------------------------
// MockNodeProvider helpers
// ---------------------------------------------------------------------------

// newDefaultNodeProvider creates a MockNodeProvider with all methods returning
// sensible defaults. All expectations use Maybe() so uncalled methods don't
// cause test failures. Equivalent to the old `&mockNodeProvider{}`.
func newDefaultNodeProvider(t *testing.T) *rpcmocks.MockNodeProvider {
	t.Helper()
	m := rpcmocks.NewMockNodeProvider(t)
	m.On("HandleLocalFrame", mock.Anything).Return(protocol.Frame{Type: "ok"}).Maybe()
	m.On("Address").Return("test-address-abc123").Maybe()
	m.On("ClientVersion").Return("0.16-alpha").Maybe()
	m.On("FetchFileTransfers").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("FetchFileMappings").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("RetryFileChunk", mock.Anything).Return(nil).Maybe()
	m.On("StartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("CancelFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("RestartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("ExplainFileRoute", mock.Anything).Return(json.RawMessage("[]"), nil).Maybe()
	m.On("NodeStatus").Return(defaultTestNodeStatus()).Maybe()
	return m
}

// newNodeProviderWithHandler creates a MockNodeProvider that delegates
// HandleLocalFrame to fn. Other methods return sensible defaults.
// Equivalent to the old `&mockNodeProvider{handleFunc: fn}`.
func newNodeProviderWithHandler(t *testing.T, fn func(protocol.Frame) protocol.Frame) *rpcmocks.MockNodeProvider {
	t.Helper()
	m := rpcmocks.NewMockNodeProvider(t)
	m.EXPECT().HandleLocalFrame(mock.Anything).RunAndReturn(fn).Maybe()
	m.On("Address").Return("test-address-abc123").Maybe()
	m.On("ClientVersion").Return("0.16-alpha").Maybe()
	m.On("FetchFileTransfers").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("FetchFileMappings").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("RetryFileChunk", mock.Anything).Return(nil).Maybe()
	m.On("StartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("CancelFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("RestartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("ExplainFileRoute", mock.Anything).Return(json.RawMessage("[]"), nil).Maybe()
	m.On("NodeStatus").Return(defaultTestNodeStatus()).Maybe()
	return m
}

// newNodeProviderWithMeta creates a MockNodeProvider with custom Address
// and ClientVersion. HandleLocalFrame returns {Type: "ok"} by default.
// Equivalent to the old `&mockNodeProvider{address: addr, version: ver}`.
func newNodeProviderWithMeta(t *testing.T, address, version string) *rpcmocks.MockNodeProvider {
	t.Helper()
	m := rpcmocks.NewMockNodeProvider(t)
	m.On("HandleLocalFrame", mock.Anything).Return(protocol.Frame{Type: "ok"}).Maybe()
	m.On("Address").Return(address).Maybe()
	m.On("ClientVersion").Return(version).Maybe()
	m.On("FetchFileTransfers").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("FetchFileMappings").Return(json.RawMessage("[]"), nil).Maybe()
	m.On("RetryFileChunk", mock.Anything).Return(nil).Maybe()
	m.On("StartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("CancelFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("RestartFileDownload", mock.Anything).Return(nil).Maybe()
	m.On("ExplainFileRoute", mock.Anything).Return(json.RawMessage("[]"), nil).Maybe()
	m.On("NodeStatus").Return(defaultTestNodeStatus()).Maybe()
	return m
}

// ---------------------------------------------------------------------------
// MockDMRouterProvider helpers
// ---------------------------------------------------------------------------

// newDefaultDMRouterProvider creates a MockDMRouterProvider with all methods
// set to reasonable defaults. Equivalent to the old `&mockDMRouterProvider{}`.
func newDefaultDMRouterProvider(t *testing.T) *rpcmocks.MockDMRouterProvider {
	t.Helper()
	m := rpcmocks.NewMockDMRouterProvider(t)
	m.On("Snapshot").Return(service.RouterSnapshot{}).Maybe()
	m.On("SendMessage", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.On("SendFileAnnounce", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	return m
}

// ---------------------------------------------------------------------------
// MockChatlogProvider helpers
// ---------------------------------------------------------------------------

// newDefaultChatlogProvider creates a MockChatlogProvider with all methods
// set to reasonable defaults. Equivalent to the old `&mockChatlogProvider{}`.
func newDefaultChatlogProvider(t *testing.T) *rpcmocks.MockChatlogProvider {
	t.Helper()
	m := rpcmocks.NewMockChatlogProvider(t)
	m.On("FetchChatlog", mock.Anything, mock.Anything).Return("[]", nil).Maybe()
	m.On("FetchChatlogPreviews").Return("[]", nil).Maybe()
	m.On("FetchConversations").Return("[]", nil).Maybe()
	m.On("HasEntryInConversation", mock.Anything, mock.Anything).Return(false).Maybe()
	return m
}

// ---------------------------------------------------------------------------
// MockMetricsProvider helpers
// ---------------------------------------------------------------------------

// newMockMetricsProvider creates a MockMetricsProvider that returns the given snapshot.
func newMockMetricsProvider(t *testing.T, snapshot protocol.Frame) *rpcmocks.MockMetricsProvider {
	t.Helper()
	m := rpcmocks.NewMockMetricsProvider(t)
	m.On("TrafficSnapshot").Return(snapshot).Maybe()
	return m
}

// ---------------------------------------------------------------------------
// Test table and server setup
// ---------------------------------------------------------------------------

// buildTestTable creates a CommandTable with all commands registered using given providers.
// Pass nil for chatlogProvider, dmRouter, or metricsProvider to simulate modes where
// those features are unavailable — their commands are registered via RegisterUnavailable
// (503, hidden from help).
func buildTestTable(nodeProvider rpc.NodeProvider, chatlogProvider rpc.ChatlogProvider, dmRouter rpc.DMRouterProvider, metricsProvider rpc.MetricsProvider) *rpc.CommandTable {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, nodeProvider, chatlogProvider, dmRouter, metricsProvider)
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

// setupTestServerWithDMRouterAndChatlog creates a test RPC server with both
// DMRouterProvider and ChatlogProvider for tests that need reply_to validation.
func setupTestServerWithDMRouterAndChatlog(t *testing.T, nodeProvider rpc.NodeProvider, chatlogProvider rpc.ChatlogProvider, dmRouter rpc.DMRouterProvider) *rpc.Server {
	t.Helper()
	return setupTestServerWithDMRouter(t, nodeProvider, chatlogProvider, dmRouter)
}

// ---------------------------------------------------------------------------
// Request / assertion helpers
// ---------------------------------------------------------------------------

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
