package rpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestClientExecuteCommandUsesExecEndpoint verifies that ExecuteCommand sends
// all requests to /rpc/v1/exec with {command, args} JSON body.
func TestClientExecuteCommandUsesExecEndpoint(t *testing.T) {
	var receivedPath string
	var receivedBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"ok"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, err := client.ExecuteCommand("ping")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedPath != "/rpc/v1/exec" {
		t.Errorf("expected path /rpc/v1/exec, got %q", receivedPath)
	}

	if receivedBody["command"] != "ping" {
		t.Errorf("expected command=ping, got %v", receivedBody["command"])
	}
}

// TestClientExecuteCommandWithArgs verifies that positional args are parsed
// via ParseConsoleInput and sent as named args in the exec body.
func TestClientExecuteCommandWithArgs(t *testing.T) {
	var receivedBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"queued","to":"peer-addr"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, err := client.ExecuteCommand("send_dm peer-addr hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedBody["command"] != "send_dm" {
		t.Errorf("expected command=send_dm, got %v", receivedBody["command"])
	}

	args, ok := receivedBody["args"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected args to be a map, got %T", receivedBody["args"])
	}
	if args["to"] != "peer-addr" {
		t.Errorf("expected args.to=peer-addr, got %v", args["to"])
	}
	if args["body"] != "hello world" {
		t.Errorf("expected args.body='hello world', got %v", args["body"])
	}
}

// TestClientExecuteCommandEmptyInput verifies that empty input returns an error
// without making an HTTP request.
func TestClientExecuteCommandEmptyInput(t *testing.T) {
	client := &Client{
		baseURL:    "http://should-not-be-called",
		httpClient: http.DefaultClient,
	}

	_, err := client.ExecuteCommand("")
	if err == nil {
		t.Error("expected error for empty command, got nil")
	}
}

// TestClientExecuteCommandServerError verifies that HTTP 4xx/5xx responses
// are returned as errors with the server's error message.
func TestClientExecuteCommandServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unknown command: nosuch"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, err := client.ExecuteCommand("nosuch")
	if err == nil {
		t.Fatal("expected error for unknown command, got nil")
	}
	if err.Error() != "rpc error: unknown command: nosuch" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestClientExecuteCommandUnavailable verifies that 503 responses are
// properly returned as errors.
func TestClientExecuteCommandUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"fetch_chatlog not available in this mode"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, err := client.ExecuteCommand("fetch_chatlog dm peer-123")
	if err == nil {
		t.Fatal("expected error for unavailable command, got nil")
	}
}

// TestClientFetchCommandsUsesExec verifies that FetchCommands goes through
// /rpc/v1/exec with command "help".
func TestClientFetchCommandsUsesExec(t *testing.T) {
	var receivedPath string
	var receivedBody map[string]interface{}

	helpResponse := map[string]interface{}{
		"commands": []map[string]interface{}{
			{"name": "ping", "description": "Send ping", "category": "system"},
		},
		"version": "1.0",
	}
	respData, _ := json.Marshal(helpResponse)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(respData)
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	commands, err := client.FetchCommands()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedPath != "/rpc/v1/exec" {
		t.Errorf("expected path /rpc/v1/exec, got %q", receivedPath)
	}
	if receivedBody["command"] != "help" {
		t.Errorf("expected command=help, got %v", receivedBody["command"])
	}
	if len(commands) != 1 {
		t.Errorf("expected 1 command, got %d", len(commands))
	}
	if commands[0].Name != "ping" {
		t.Errorf("expected command name=ping, got %q", commands[0].Name)
	}
}

// TestClientAuthHeader verifies that basic auth credentials are sent when configured.
func TestClientAuthHeader(t *testing.T) {
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"pong"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
		username:   "admin",
		password:   "secret",
	}

	_, _ = client.ExecuteCommand("ping")

	if receivedAuth == "" {
		t.Error("expected Authorization header, got empty")
	}
}

// TestClientNoAuthWhenNotConfigured verifies that no auth header is sent
// when credentials are empty.
func TestClientNoAuthWhenNotConfigured(t *testing.T) {
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"pong"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, _ = client.ExecuteCommand("ping")

	if receivedAuth != "" {
		t.Errorf("expected no Authorization header, got %q", receivedAuth)
	}
}

// TestClientRawJSONUsesFrameEndpoint verifies that raw JSON input
// is sent to /rpc/v1/frame verbatim instead of being parsed into {command, args}.
func TestClientRawJSONUsesFrameEndpoint(t *testing.T) {
	var receivedPath string
	var receivedBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"welcome"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, err := client.ExecuteCommand(`{"type":"hello","client":"my-tool","client_version":"1.0"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedPath != "/rpc/v1/frame" {
		t.Errorf("expected path /rpc/v1/frame for raw JSON, got %q", receivedPath)
	}

	// The frame should be sent verbatim — all fields preserved.
	if receivedBody["type"] != "hello" {
		t.Errorf("expected type=hello, got %v", receivedBody["type"])
	}
	if receivedBody["client"] != "my-tool" {
		t.Errorf("expected client=my-tool, got %v", receivedBody["client"])
	}
	if receivedBody["client_version"] != "1.0" {
		t.Errorf("expected client_version=1.0, got %v", receivedBody["client_version"])
	}
}

// TestClientNamedCommandStillUsesExec verifies that named commands (not JSON)
// still go through /rpc/v1/exec after the raw JSON routing was added.
func TestClientNamedCommandStillUsesExec(t *testing.T) {
	var receivedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"pong"}`))
	}))
	defer server.Close()

	client := &Client{
		baseURL:    server.URL,
		httpClient: server.Client(),
	}

	_, _ = client.ExecuteCommand("ping")

	if receivedPath != "/rpc/v1/exec" {
		t.Errorf("expected /rpc/v1/exec for named command, got %q", receivedPath)
	}
}

// TestClientNoLegacyRoutes verifies that no requests go to legacy
// per-command endpoints — everything must use /rpc/v1/exec.
func TestClientNoLegacyRoutes(t *testing.T) {
	commands := []string{
		"ping", "help", "version", "hello",
		"get_peers", "fetch_peer_health",
		"add_peer 1.2.3.4:8080",
		"fetch_messages dm",
		"fetch_dm_headers",
		"fetch_notices",
	}

	for _, cmd := range commands {
		var receivedPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"ok"}`))
		}))

		client := &Client{
			baseURL:    server.URL,
			httpClient: server.Client(),
		}

		_, _ = client.ExecuteCommand(cmd)
		server.Close()

		if receivedPath != "/rpc/v1/exec" {
			t.Errorf("command %q: expected /rpc/v1/exec, got %q", cmd, receivedPath)
		}
	}
}
