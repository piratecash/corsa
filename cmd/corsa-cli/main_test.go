package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- parseArgs tests ---

func TestParseArgsEmpty(t *testing.T) {
	result, err := parseArgs("test", nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestParseArgsJSON(t *testing.T) {
	result, err := parseArgs("test", []string{`{"address": "1.2.3.4:8080"}`}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["address"] != "1.2.3.4:8080" {
		t.Errorf("expected address=1.2.3.4:8080, got %v", result["address"])
	}
}

func TestParseArgsInvalidJSON(t *testing.T) {
	_, err := parseArgs("test", []string{`{broken`}, false)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestParseArgsNamedKeyValue(t *testing.T) {
	result, err := parseArgs("test", []string{"to=peer-addr", "body=hello world"}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["to"] != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", result["to"])
	}
	if result["body"] != "hello world" {
		t.Errorf("expected body='hello world', got %v", result["body"])
	}
}

func TestParseArgsKeyValueWithoutFlag(t *testing.T) {
	// Multiple bare args without -named flag — still parsed as key=value
	result, err := parseArgs("test", []string{"topic=dm", "peer_address=abc"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["topic"] != "dm" {
		t.Errorf("expected topic=dm, got %v", result["topic"])
	}
}

func TestParseArgsBareValueWithNamedFlag(t *testing.T) {
	// Bare value without '=' is an error when -named flag is set
	_, err := parseArgs("test", []string{"just-a-value"}, true)
	if err == nil {
		t.Error("expected error for bare value with -named flag, got nil")
	}
}

func TestParseArgsPositional(t *testing.T) {
	// Bare positional args delegate to ParseConsoleInput.
	// "add_peer 1.2.3.4:8080" → {address: "1.2.3.4:8080"}
	result, err := parseArgs("add_peer", []string{"1.2.3.4:8080"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["address"] != "1.2.3.4:8080" {
		t.Errorf("expected address=1.2.3.4:8080, got %v", result)
	}
}

func TestParseArgsPositionalSendDM(t *testing.T) {
	result, err := parseArgs("send_dm", []string{"peer-abc", "hello", "world"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["to"] != "peer-abc" {
		t.Errorf("expected to=peer-abc, got %v", result["to"])
	}
	if result["body"] != "hello world" {
		t.Errorf("expected body='hello world', got %v", result["body"])
	}
}

func TestParseArgsPositionalBodyWithEquals(t *testing.T) {
	// Body containing '=' must not trigger key=value mode.
	// "send_dm peer-addr a=b" should parse positionally.
	result, err := parseArgs("send_dm", []string{"peer-addr", "a=b"}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["to"] != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", result["to"])
	}
	if result["body"] != "a=b" {
		t.Errorf("expected body='a=b', got %v", result["body"])
	}
}

func TestParseArgsNumericValueIsString(t *testing.T) {
	// Key=value always stores strings; handlers use numericArg() for conversion.
	result, err := parseArgs("test", []string{"limit=10"}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["limit"] != "10" {
		t.Errorf("expected limit='10' (string), got %v (%T)", result["limit"], result["limit"])
	}
}

func TestParseArgsBoolLikeValueIsString(t *testing.T) {
	// "true" stays as string, not parsed as bool.
	result, err := parseArgs("test", []string{"verbose=true"}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["verbose"] != "true" {
		t.Errorf("expected verbose='true' (string), got %v (%T)", result["verbose"], result["verbose"])
	}
}

func TestParseArgsNullLikeValueIsString(t *testing.T) {
	// "null" stays as string, not parsed as nil.
	result, err := parseArgs("test", []string{"filter=null"}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["filter"] != "null" {
		t.Errorf("expected filter='null' (string), got %v (%T)", result["filter"], result["filter"])
	}
}

// --- execRPC tests ---

func TestExecRPCAlwaysUsesExecEndpoint(t *testing.T) {
	commands := []string{"ping", "help", "get_peers", "send_dm", "fetch_chatlog"}

	for _, cmd := range commands {
		var receivedPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"type":"ok"}`))
		}))

		_, _, err := execRPC(server.URL, cmd, nil, "", "")
		server.Close()
		if err != nil {
			t.Fatalf("command %q: unexpected error: %v", cmd, err)
		}
		if receivedPath != "/rpc/v1/exec" {
			t.Errorf("command %q: expected /rpc/v1/exec, got %q", cmd, receivedPath)
		}
	}
}

func TestExecRPCSendsCommandAndArgs(t *testing.T) {
	var receivedBody map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"pending"}`))
	}))
	defer server.Close()

	args := map[string]interface{}{"to": "peer", "body": "hello"}
	_, _, err := execRPC(server.URL, "send_dm", args, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedBody["command"] != "send_dm" {
		t.Errorf("expected command=send_dm, got %v", receivedBody["command"])
	}
	receivedArgs, ok := receivedBody["args"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected args map, got %T", receivedBody["args"])
	}
	if receivedArgs["to"] != "peer" {
		t.Errorf("expected to=peer, got %v", receivedArgs["to"])
	}
}

func TestExecRPCNoArgsOmitsArgsField(t *testing.T) {
	var receivedBody map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&receivedBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"pong"}`))
	}))
	defer server.Close()

	_, _, err := execRPC(server.URL, "ping", nil, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, exists := receivedBody["args"]; exists {
		t.Error("expected no 'args' field when args is nil")
	}
}

func TestExecRPCSendsAuth(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	_, _, err := execRPC(server.URL, "ping", nil, "admin", "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedAuth == "" {
		t.Error("expected Authorization header, got empty")
	}
}

func TestExecRPCNoAuthWhenEmpty(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	_, _, err := execRPC(server.URL, "ping", nil, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedAuth != "" {
		t.Errorf("expected no auth header, got %q", receivedAuth)
	}
}

func TestExecRPCReturnsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unknown command: nosuch"}`))
	}))
	defer server.Close()

	body, statusCode, err := execRPC(server.URL, "nosuch", nil, "", "")
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if statusCode != 404 {
		t.Errorf("expected status 404, got %d", statusCode)
	}
	if len(body) == 0 {
		t.Error("expected non-empty response body")
	}
}
