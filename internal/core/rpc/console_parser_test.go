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

func TestParseConsoleInputQuoteOnlyInput(t *testing.T) {
	// Quoted-only inputs produce zero tokens after stripping quotes.
	// Must return an error, not panic.
	inputs := []string{`""`, `''`, `"  "`, `' '`}
	for _, input := range inputs {
		_, err := ParseConsoleInput(input)
		if err == nil {
			t.Errorf("ParseConsoleInput(%q): expected error, got nil", input)
		}
	}
}

func TestParseConsoleInputUnterminatedQuote(t *testing.T) {
	// Unterminated quotes must return an error instead of silently
	// folding subsequent key=value tokens into the quoted value.
	inputs := []struct {
		input string
		desc  string
	}{
		{`send_dm to=peer body="hello world`, "unterminated double quote in body"},
		{`send_dm to=peer body='hello world`, "unterminated single quote in body"},
		{`send_dm to=peer body="hello world reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5`, "unterminated quote swallows reply_to"},
	}
	for _, tt := range inputs {
		_, err := ParseConsoleInput(tt.input)
		if err == nil {
			t.Errorf("%s: ParseConsoleInput(%q): expected error, got nil", tt.desc, tt.input)
		}
	}
}

func TestParseConsoleInputSimpleCommand(t *testing.T) {
	tests := []struct {
		input    string
		wantName string
	}{
		{"ping", "ping"}, {"help", "help"}, {"hello", "hello"}, {"version", "version"},
		{"get_peers", "getPeers"}, {"fetch_peer_health", "fetchPeerHealth"},
		{"fetch_identities", "fetchIdentities"}, {"fetch_contacts", "fetchContacts"},
		{"fetch_trusted_contacts", "fetchTrustedContacts"},
		{"fetch_notices", "fetchNotices"}, {"fetch_chatlog_previews", "fetchChatlogPreviews"},
		{"fetch_conversations", "fetchConversations"}, {"fetch_dm_headers", "fetchDmHeaders"},
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
		if req.Args != nil {
			t.Errorf("ParseConsoleInput(%q): expected nil args, got %v", tt.input, req.Args)
		}
	}
}

func TestParseConsoleInputAddPeer(t *testing.T) {
	req, err := ParseConsoleInput("add_peer 192.168.1.1:9000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "addPeer" {
		t.Errorf("expected name addPeer, got %s", req.Name)
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
		{"GET_PEERS", "getPeers"},
		{"Fetch_Peer_Health", "fetchPeerHealth"},
		{"Send_DM peer hello", "sendDm"},
		{"ADD_PEER 1.2.3.4:8080", "addPeer"},
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
	if req.Name != "fetchMessages" {
		t.Errorf("expected name fetchMessages, got %s", req.Name)
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
	if req.Name != "getPeers" {
		t.Errorf("expected name getPeers, got %s", req.Name)
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
	if req.Name != "addPeer" {
		t.Errorf("expected name addPeer, got %s", req.Name)
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
	if req.Name != "sendDm" {
		t.Errorf("expected name sendDm, got %s", req.Name)
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
	if req.Name != "fetchChatlog" {
		t.Errorf("expected name fetchChatlog, got %s", req.Name)
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

func TestParseConsoleInputKeyValueSendDMWithReplyTo(t *testing.T) {
	req, err := ParseConsoleInput(`send_dm to=peer-addr body=hello reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "sendDm" {
		t.Errorf("expected command sendDm, got %s", req.Name)
	}
	if req.Args["to"] != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", req.Args["to"])
	}
	if req.Args["body"] != "hello" {
		t.Errorf("expected body=hello, got %v", req.Args["body"])
	}
	if req.Args["reply_to"] != "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" {
		t.Errorf("expected reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5, got %v", req.Args["reply_to"])
	}
}

func TestParseConsoleInputKeyValueQuotedMultiWordBody(t *testing.T) {
	req, err := ParseConsoleInput(`send_dm to=peer-addr body="hello world" reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "sendDm" {
		t.Errorf("expected command sendDm, got %s", req.Name)
	}
	if req.Args["to"] != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", req.Args["to"])
	}
	if req.Args["body"] != "hello world" {
		t.Errorf("expected body='hello world', got %v", req.Args["body"])
	}
	if req.Args["reply_to"] != "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5" {
		t.Errorf("expected reply_to=a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5, got %v", req.Args["reply_to"])
	}
}

func TestParseConsoleInputKeyValueSingleQuotes(t *testing.T) {
	req, err := ParseConsoleInput(`send_dm to=peer-addr body='multi word message'`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["body"] != "multi word message" {
		t.Errorf("expected body='multi word message', got %v", req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueEscapedDoubleQuotes(t *testing.T) {
	// body="He said \"hi\"" should produce: He said "hi"
	req, err := ParseConsoleInput(`send_dm to=peer-addr body="He said \"hi\""`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `He said "hi"`
	if req.Args["body"] != want {
		t.Errorf("expected body=%q, got %v", want, req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueEscapedSingleQuotes(t *testing.T) {
	// body='It\'s fine' should produce: It's fine
	req, err := ParseConsoleInput(`send_dm to=peer-addr body='It\'s fine'`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "It's fine"
	if req.Args["body"] != want {
		t.Errorf("expected body=%q, got %v", want, req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueEscapedBackslash(t *testing.T) {
	// body="path\\to\\file" should produce: path\to\file
	req, err := ParseConsoleInput(`send_dm to=peer-addr body="path\\to\\file"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `path\to\file`
	if req.Args["body"] != want {
		t.Errorf("expected body=%q, got %v", want, req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueBackslashNonSpecial(t *testing.T) {
	// Backslash before non-special char inside quotes is kept literal.
	req, err := ParseConsoleInput(`send_dm to=peer-addr body="hello\nworld"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `hello\nworld`
	if req.Args["body"] != want {
		t.Errorf("expected body=%q, got %v", want, req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueFallsBackToPositional(t *testing.T) {
	// Mixed bare and key=value tokens — should fall through to positional parsing.
	req, err := ParseConsoleInput("send_dm peer-addr hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["to"] != "peer-addr" {
		t.Errorf("expected to=peer-addr, got %v", req.Args["to"])
	}
	if req.Args["body"] != "hello world" {
		t.Errorf("expected body='hello world', got %v", req.Args["body"])
	}
}

func TestParseConsoleInputKeyValueScalarBodyTrue(t *testing.T) {
	// body=true must be kept as the string "true", not parsed as bool.
	req, err := ParseConsoleInput(`send_dm to=peer-addr body=true`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, ok := req.Args["body"].(string)
	if !ok {
		t.Fatalf("expected body to be string, got %T", req.Args["body"])
	}
	if body != "true" {
		t.Errorf("expected body='true', got %q", body)
	}
}

func TestParseConsoleInputKeyValueScalarBodyNumber(t *testing.T) {
	// body=123 must be kept as the string "123", not parsed as float64.
	req, err := ParseConsoleInput(`send_dm to=peer-addr body=123`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, ok := req.Args["body"].(string)
	if !ok {
		t.Fatalf("expected body to be string, got %T", req.Args["body"])
	}
	if body != "123" {
		t.Errorf("expected body='123', got %q", body)
	}
}

func TestParseConsoleInputKeyValueScalarBodyNull(t *testing.T) {
	// body=null must be kept as the string "null", not parsed as nil.
	req, err := ParseConsoleInput(`send_dm to=peer-addr body=null`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body, ok := req.Args["body"].(string)
	if !ok {
		t.Fatalf("expected body to be string, got %T (%v)", req.Args["body"], req.Args["body"])
	}
	if body != "null" {
		t.Errorf("expected body='null', got %q", body)
	}
}

func TestParseConsoleInputKeyValueNoArgs(t *testing.T) {
	// No args after command — key=value should not activate, positional handles it.
	req, err := ParseConsoleInput("ping")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Name != "ping" {
		t.Errorf("expected command ping, got %s", req.Name)
	}
}

func TestNumericArg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    map[string]interface{}
		key     string
		wantVal int
		wantOK  bool
	}{
		{"float64 from JSON", map[string]interface{}{"limit": float64(10)}, "limit", 10, true},
		{"string from key=value", map[string]interface{}{"limit": "10"}, "limit", 10, true},
		{"zero float64", map[string]interface{}{"limit": float64(0)}, "limit", 0, false},
		{"zero string", map[string]interface{}{"limit": "0"}, "limit", 0, false},
		{"negative float64", map[string]interface{}{"offset": float64(-5)}, "offset", -5, false},
		{"negative string", map[string]interface{}{"offset": "-5"}, "offset", -5, false},
		{"non-numeric string", map[string]interface{}{"limit": "abc"}, "limit", 0, false},
		{"missing key", map[string]interface{}{}, "limit", 0, false},
		{"nil args", nil, "limit", 0, false},
		{"bool value", map[string]interface{}{"limit": true}, "limit", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := numericArg(tt.args, tt.key)
			if ok != tt.wantOK {
				t.Errorf("numericArg(%v, %q) ok = %v, want %v", tt.args, tt.key, ok, tt.wantOK)
			}
			if val != tt.wantVal {
				t.Errorf("numericArg(%v, %q) val = %d, want %d", tt.args, tt.key, val, tt.wantVal)
			}
		})
	}
}

func TestParseConsoleInputKeyValuePagination(t *testing.T) {
	// key=value with numeric params should produce strings that numericArg can parse.
	req, err := ParseConsoleInput(`fetch_messages topic=dm limit=10 offset=5`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Args["topic"] != "dm" {
		t.Errorf("expected topic=dm, got %v", req.Args["topic"])
	}
	// Values arrive as strings from key=value parser.
	if req.Args["limit"] != "10" {
		t.Errorf("expected limit='10', got %v (%T)", req.Args["limit"], req.Args["limit"])
	}
	if req.Args["offset"] != "5" {
		t.Errorf("expected offset='5', got %v (%T)", req.Args["offset"], req.Args["offset"])
	}
	// numericArg should extract them correctly.
	if limit, ok := numericArg(req.Args, "limit"); !ok || limit != 10 {
		t.Errorf("numericArg(limit) = %d, %v; want 10, true", limit, ok)
	}
	if offset, ok := numericArg(req.Args, "offset"); !ok || offset != 5 {
		t.Errorf("numericArg(offset) = %d, %v; want 5, true", offset, ok)
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
