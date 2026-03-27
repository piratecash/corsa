package rpc

import (
	"testing"
)

func TestParseConsoleInputEmptyInput(t *testing.T) {
	_, err := ParseConsoleInput("")
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestParseConsoleInputSimpleCommand(t *testing.T) {
	tests := []string{
		"ping", "help", "hello", "version",
		"get_peers", "fetch_peer_health",
		"fetch_identities", "fetch_contacts", "fetch_trusted_contacts",
		"fetch_notices", "fetch_chatlog_previews", "fetch_conversations",
		"fetch_dm_headers",
	}

	for _, cmd := range tests {
		req, err := ParseConsoleInput(cmd)
		if err != nil {
			t.Errorf("ParseConsoleInput(%q): unexpected error: %v", cmd, err)
			continue
		}
		if req.Name != cmd {
			t.Errorf("ParseConsoleInput(%q): expected name %q, got %q", cmd, cmd, req.Name)
		}
		if req.Args != nil {
			t.Errorf("ParseConsoleInput(%q): expected nil args, got %v", cmd, req.Args)
		}
	}
}

func TestParseConsoleInputAddPeer(t *testing.T) {
	req, err := ParseConsoleInput("add_peer 192.168.1.1:9000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "add_peer" {
		t.Errorf("expected name add_peer, got %s", req.Name)
	}
	if req.Args["address"] != "192.168.1.1:9000" {
		t.Errorf("expected address 192.168.1.1:9000, got %v", req.Args["address"])
	}
}

func TestParseConsoleInputAddPeerMissingArg(t *testing.T) {
	_, err := ParseConsoleInput("add_peer")
	if err == nil {
		t.Error("expected error for add_peer without address")
	}
}

func TestParseConsoleInputFetchMessages(t *testing.T) {
	req, err := ParseConsoleInput("fetch_messages global")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "global" {
		t.Errorf("expected topic global, got %v", req.Args["topic"])
	}
}

func TestParseConsoleInputFetchMessagesDefault(t *testing.T) {
	req, err := ParseConsoleInput("fetch_messages")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "global" {
		t.Errorf("expected default topic global, got %v", req.Args["topic"])
	}
}

func TestParseConsoleInputFetchMessage(t *testing.T) {
	req, err := ParseConsoleInput("fetch_message dm abc-def-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected topic dm, got %v", req.Args["topic"])
	}
	if req.Args["id"] != "abc-def-123" {
		t.Errorf("expected id abc-def-123, got %v", req.Args["id"])
	}
}

func TestParseConsoleInputFetchMessageMissingArgs(t *testing.T) {
	_, err := ParseConsoleInput("fetch_message dm")
	if err == nil {
		t.Error("expected error for fetch_message with only topic")
	}
}

func TestParseConsoleInputFetchInbox(t *testing.T) {
	req, err := ParseConsoleInput("fetch_inbox dm peer-addr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected topic dm, got %v", req.Args["topic"])
	}
	if req.Args["recipient"] != "peer-addr" {
		t.Errorf("expected recipient peer-addr, got %v", req.Args["recipient"])
	}
}

func TestParseConsoleInputFetchInboxDefault(t *testing.T) {
	req, err := ParseConsoleInput("fetch_inbox")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected default topic dm, got %v", req.Args["topic"])
	}
}

func TestParseConsoleInputSendDM(t *testing.T) {
	req, err := ParseConsoleInput("send_dm peer-addr hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["to"] != "peer-addr" {
		t.Errorf("expected to peer-addr, got %v", req.Args["to"])
	}
	if req.Args["body"] != "hello world" {
		t.Errorf("expected body 'hello world', got %v", req.Args["body"])
	}
}

func TestParseConsoleInputSendDMMissingArgs(t *testing.T) {
	_, err := ParseConsoleInput("send_dm peer-addr")
	if err == nil {
		t.Error("expected error for send_dm with only 'to'")
	}
}

func TestParseConsoleInputFetchChatlog(t *testing.T) {
	req, err := ParseConsoleInput("fetch_chatlog dm peer-addr-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected topic dm, got %v", req.Args["topic"])
	}
	if req.Args["peer_address"] != "peer-addr-123" {
		t.Errorf("expected peer_address peer-addr-123, got %v", req.Args["peer_address"])
	}
}

func TestParseConsoleInputFetchDeliveryReceipts(t *testing.T) {
	req, err := ParseConsoleInput("fetch_delivery_receipts peer-addr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["recipient"] != "peer-addr" {
		t.Errorf("expected recipient peer-addr, got %v", req.Args["recipient"])
	}
}

func TestParseConsoleInputFetchDeliveryReceiptsNoArgs(t *testing.T) {
	req, err := ParseConsoleInput("fetch_delivery_receipts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req.Args) != 0 {
		t.Errorf("expected empty args, got %v", req.Args)
	}
}

func TestParseConsoleInputUnknownCommand(t *testing.T) {
	req, err := ParseConsoleInput("unknown_cmd")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "unknown_cmd" {
		t.Errorf("expected name unknown_cmd, got %s", req.Name)
	}
}

// --- Case-insensitivity tests ---

func TestParseConsoleInputCaseInsensitive(t *testing.T) {
	tests := []struct {
		input    string
		wantName string
	}{
		{"HELP", "help"},
		{"Ping", "ping"},
		{"GET_PEERS", "get_peers"},
		{"Fetch_Peer_Health", "fetch_peer_health"},
		{"Send_DM peer hello", "send_dm"},
		{"ADD_PEER 1.2.3.4:8080", "add_peer"},
	}

	for _, tt := range tests {
		req, err := ParseConsoleInput(tt.input)
		if err != nil {
			t.Errorf("ParseConsoleInput(%q): unexpected error: %v", tt.input, err)
			continue
		}
		if req.Name != tt.wantName {
			t.Errorf("ParseConsoleInput(%q): expected name %q, got %q", tt.input, tt.wantName, req.Name)
		}
	}
}

// --- Raw JSON frame tests ---

func TestParseConsoleInputJSONFrameSimple(t *testing.T) {
	req, err := ParseConsoleInput(`{"type":"ping"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "ping" {
		t.Errorf("expected name ping, got %s", req.Name)
	}
}

func TestParseConsoleInputJSONFrameWithArgs(t *testing.T) {
	req, err := ParseConsoleInput(`{"type":"fetch_messages","topic":"dm"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetch_messages" {
		t.Errorf("expected name fetch_messages, got %s", req.Name)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected topic=dm, got %v", req.Args["topic"])
	}
}

func TestParseConsoleInputJSONFrameMissingType(t *testing.T) {
	_, err := ParseConsoleInput(`{"topic":"dm"}`)
	if err == nil {
		t.Error("expected error for JSON frame without type, got nil")
	}
}

func TestParseConsoleInputJSONFrameEmptyType(t *testing.T) {
	_, err := ParseConsoleInput(`{"type":""}`)
	if err == nil {
		t.Error("expected error for JSON frame with empty type, got nil")
	}
}

func TestParseConsoleInputJSONFrameInvalid(t *testing.T) {
	_, err := ParseConsoleInput(`{broken json}`)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestParseConsoleInputJSONFrameCaseInsensitive(t *testing.T) {
	req, err := ParseConsoleInput(`{"type":"GET_PEERS"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "get_peers" {
		t.Errorf("expected name get_peers, got %s", req.Name)
	}
}

// --- Raw JSON frame normalization tests ---
// These verify that protocol wire frame field names are translated
// to RPC arg names so that pasting real frames into the console works.

func TestParseConsoleInputJSONFrameAddPeerFromWire(t *testing.T) {
	// Wire format uses "peers" array; RPC handler expects "address" string.
	req, err := ParseConsoleInput(`{"type":"add_peer","peers":["192.168.1.1:9000"]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "add_peer" {
		t.Errorf("expected name add_peer, got %s", req.Name)
	}
	addr, ok := req.Args["address"].(string)
	if !ok || addr != "192.168.1.1:9000" {
		t.Errorf("expected address=192.168.1.1:9000, got %v", req.Args["address"])
	}
}

func TestParseConsoleInputJSONFrameAddPeerRPCFormat(t *testing.T) {
	// RPC format already uses "address" — should pass through unchanged.
	req, err := ParseConsoleInput(`{"type":"add_peer","address":"10.0.0.1:8080"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	addr, ok := req.Args["address"].(string)
	if !ok || addr != "10.0.0.1:8080" {
		t.Errorf("expected address=10.0.0.1:8080, got %v", req.Args["address"])
	}
}

func TestParseConsoleInputJSONFrameAddPeerRPCWins(t *testing.T) {
	// Both "address" and "peers" present — RPC field wins.
	req, err := ParseConsoleInput(`{"type":"add_peer","address":"rpc-addr","peers":["wire-addr"]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	addr, _ := req.Args["address"].(string)
	if addr != "rpc-addr" {
		t.Errorf("expected RPC address to win, got %v", addr)
	}
}

func TestParseConsoleInputJSONFrameSendDMFromWire(t *testing.T) {
	// Wire format uses "recipient"; RPC handler expects "to".
	req, err := ParseConsoleInput(`{"type":"send_dm","recipient":"peer-addr","body":"hello"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "send_dm" {
		t.Errorf("expected name send_dm, got %s", req.Name)
	}
	to, ok := req.Args["to"].(string)
	if !ok || to != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", req.Args["to"])
	}
	body, _ := req.Args["body"].(string)
	if body != "hello" {
		t.Errorf("expected body=hello, got %v", body)
	}
}

func TestParseConsoleInputJSONFrameSendDMRPCFormat(t *testing.T) {
	// RPC format already uses "to" — should pass through unchanged.
	req, err := ParseConsoleInput(`{"type":"send_dm","to":"peer","body":"msg"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	to, _ := req.Args["to"].(string)
	if to != "peer" {
		t.Errorf("expected to=peer, got %v", to)
	}
}

func TestParseConsoleInputJSONFrameFetchChatlogFromWire(t *testing.T) {
	// Wire format uses "address"; RPC handler expects "peer_address".
	req, err := ParseConsoleInput(`{"type":"fetch_chatlog","topic":"dm","address":"abc123"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "fetch_chatlog" {
		t.Errorf("expected name fetch_chatlog, got %s", req.Name)
	}
	pa, ok := req.Args["peer_address"].(string)
	if !ok || pa != "abc123" {
		t.Errorf("expected peer_address=abc123, got %v", req.Args["peer_address"])
	}
}

func TestParseConsoleInputJSONFrameFetchChatlogRPCFormat(t *testing.T) {
	// RPC format already uses "peer_address" — should pass through unchanged.
	req, err := ParseConsoleInput(`{"type":"fetch_chatlog","topic":"dm","peer_address":"xyz"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pa, _ := req.Args["peer_address"].(string)
	if pa != "xyz" {
		t.Errorf("expected peer_address=xyz, got %v", pa)
	}
}

func TestParseConsoleInputJSONFrameCountToOffset(t *testing.T) {
	// Wire format uses "count" for pagination; RPC handlers read "offset".
	req, err := ParseConsoleInput(`{"type":"fetch_messages","topic":"dm","count":10}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	offset, ok := req.Args["offset"].(float64)
	if !ok || offset != 10 {
		t.Errorf("expected offset=10, got %v", req.Args["offset"])
	}
}

func TestParseConsoleInputJSONFrameOffsetNotOverwritten(t *testing.T) {
	// If both "offset" and "count" present, "offset" wins.
	req, err := ParseConsoleInput(`{"type":"fetch_messages","topic":"dm","offset":5,"count":99}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	offset, _ := req.Args["offset"].(float64)
	if offset != 5 {
		t.Errorf("expected offset=5 (not overwritten by count), got %v", offset)
	}
}

func TestStringArgOrDefault(t *testing.T) {
	args := []string{"a", "b", "c"}

	if v := stringArgOrDefault(args, 0, "x"); v != "a" {
		t.Errorf("expected a, got %s", v)
	}
	if v := stringArgOrDefault(args, 3, "x"); v != "x" {
		t.Errorf("expected x (default), got %s", v)
	}
	if v := stringArgOrDefault(nil, 0, "x"); v != "x" {
		t.Errorf("expected x (default for nil), got %s", v)
	}
}
