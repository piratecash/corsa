package desktop

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/rpc"
)

// newTestConsoleWindow creates a ConsoleWindow backed by a CommandTable
// with no GUI dependencies. Only the parent.cmdTable field is used
// by executeCommand, so all other Window fields are left at zero values.
func newTestConsoleWindow(table *rpc.CommandTable) *ConsoleWindow {
	return &ConsoleWindow{
		parent: &Window{
			cmdTable: table,
		},
	}
}

// testTable returns a CommandTable with a few deterministic commands
// for exercising the console dispatch path.
func testTable() *rpc.CommandTable {
	t := rpc.NewCommandTable()

	t.Register(
		rpc.CommandInfo{Name: "ping", Description: "Pong", Category: "system"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "pong"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "send_dm", Description: "Send DM", Category: "message", Usage: "<to> <body>"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(req.Args)
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetch_traffic_history", Description: "Traffic history", Category: "metrics"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetch_route_table", Description: "Full routing table snapshot", Category: "routing"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetch_route_summary", Description: "Routing table summary", Category: "routing"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetch_route_lookup", Description: "Lookup routes for identity", Category: "routing", Usage: "<identity>"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(req.Args)
			return rpc.CommandResponse{Data: data}
		},
	)

	t.RegisterUnavailable(
		rpc.CommandInfo{Name: "fetch_chatlog", Description: "Unavailable in test mode", Category: "chatlog"},
	)

	return t
}

func TestExecuteCommandDispatchesViaCommandTable(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand("ping")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "pong") {
		t.Errorf("expected pong in result, got: %s", result)
	}
}

func TestExecuteCommandWithArgs(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand("send_dm peer-abc hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "peer-abc") {
		t.Errorf("expected 'peer-abc' in result, got: %s", result)
	}
	if !strings.Contains(result, "hello world") {
		t.Errorf("expected 'hello world' in result, got: %s", result)
	}
}

func TestExecuteCommandJSONFrame(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand(`{"type":"ping"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "pong") {
		t.Errorf("expected pong in JSON frame result, got: %s", result)
	}
}

func TestExecuteCommandUnavailableReturnsError(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	_, err := cw.executeCommand("fetch_chatlog")
	if err == nil {
		t.Fatal("expected error for unavailable command")
	}

	if !strings.Contains(err.Error(), "not available") {
		t.Errorf("expected 'not available' error, got: %v", err)
	}
}

func TestExecuteCommandPrettyPrintsJSON(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand("ping")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pretty-printed JSON should contain newlines and indentation.
	if !strings.Contains(result, "\n") {
		t.Errorf("expected pretty-printed JSON with newlines, got: %s", result)
	}
	if !strings.Contains(result, "  ") {
		t.Errorf("expected pretty-printed JSON with indentation, got: %s", result)
	}
}

func TestLoadCommandsPopulatesSuggestions(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	cw.loadCommands()
	suggestions := cw.getCommands()

	if len(suggestions) == 0 {
		t.Fatal("expected non-empty suggestions after loadCommands")
	}

	// Unavailable commands should be excluded.
	for _, s := range suggestions {
		if s.Insert == "fetch_chatlog" {
			t.Error("unavailable command fetch_chatlog should not appear in suggestions")
		}
	}

	// Available commands should be present.
	found := make(map[string]bool)
	for _, s := range suggestions {
		found[s.Insert] = true
	}
	if !found["ping"] {
		t.Error("expected ping in suggestions")
	}
	if !found["send_dm"] {
		t.Error("expected send_dm in suggestions")
	}
}

func TestExecuteCommandHelpReturnsHumanReadable(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand("help")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be human-readable text, not machine JSON.
	if strings.HasPrefix(strings.TrimSpace(result), "{") {
		t.Error("expected human-readable help, got JSON")
	}

	// Should contain category headers.
	if !strings.Contains(result, "==") {
		t.Error("expected category headers with '==' in help text")
	}

	// Should contain registered commands.
	if !strings.Contains(result, "ping") {
		t.Error("expected 'ping' in help text")
	}
	if !strings.Contains(result, "send_dm") {
		t.Error("expected 'send_dm' in help text")
	}

	// Should contain usage hints for argument-taking commands.
	if !strings.Contains(result, "<to> <body>") {
		t.Error("expected usage hint '<to> <body>' in help text")
	}

	// Should contain the raw JSON hint.
	if !strings.Contains(result, "raw JSON") {
		t.Error("expected 'raw JSON' hint in help text")
	}
}

func TestExecuteCommandUnknownFallsBackWithNilClient(t *testing.T) {
	// When client is nil and command is unknown, executeCommand should
	// return the CommandTable error (not panic).
	cw := newTestConsoleWindow(testTable())

	_, err := cw.executeCommand("nonexistent_command")
	if err == nil {
		t.Fatal("expected error for unknown command with nil client")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected 'unknown command' error, got: %v", err)
	}
}

func TestConsoleHelpTextGroupsByCategory(t *testing.T) {
	table := testTable()
	text := consoleHelpText(table, "self-addr-123")

	// Should contain self-address in defaults.
	if !strings.Contains(text, "self-addr-123") {
		t.Error("expected self-address in help defaults")
	}

	// Unavailable commands should not appear.
	if strings.Contains(text, "fetch_chatlog") {
		t.Error("unavailable command fetch_chatlog should not appear in console help")
	}

	// Metrics category must be present with its command.
	if !strings.Contains(text, "== Metrics ==") {
		t.Error("expected '== Metrics ==' section in console help")
	}
	if !strings.Contains(text, "fetch_traffic_history") {
		t.Error("expected fetch_traffic_history in console help")
	}

	// Routing category must be present with its commands.
	if !strings.Contains(text, "== Routing ==") {
		t.Error("expected '== Routing ==' section in console help")
	}
	if !strings.Contains(text, "fetch_route_table") {
		t.Error("expected fetch_route_table in console help")
	}
	if !strings.Contains(text, "fetch_route_summary") {
		t.Error("expected fetch_route_summary in console help")
	}
	if !strings.Contains(text, "fetch_route_lookup") {
		t.Error("expected fetch_route_lookup in console help")
	}
}

func TestCommandInfoToSuggestionsUsage(t *testing.T) {
	commands := []rpc.CommandInfo{
		{Name: "send_dm", Description: "Send DM", Category: "message", Usage: "<to> <body>"},
		{Name: "ping", Description: "Pong", Category: "system"},
	}

	suggestions := commandInfoToSuggestions(commands)

	if len(suggestions) != 2 {
		t.Fatalf("expected 2 suggestions, got %d", len(suggestions))
	}

	// With Usage: Label includes usage, Insert is just the name.
	for _, s := range suggestions {
		if s.Label == "send_dm <to> <body>" {
			if s.Insert != "send_dm" {
				t.Errorf("expected insert 'send_dm', got %q", s.Insert)
			}
		}
		if s.Label == "ping" {
			if s.Insert != "ping" {
				t.Errorf("expected insert 'ping', got %q", s.Insert)
			}
		}
	}
}

func TestCommandInfoToSuggestionsPrefill(t *testing.T) {
	// fetch_chatlog should prefill the default topic "dm" on autocomplete,
	// preserving the old desktop console UX shortcut.
	commands := []rpc.CommandInfo{
		{Name: "fetch_chatlog", Description: "Fetch chatlog", Category: "chatlog", Usage: "[topic] [peer_address]"},
		{Name: "ping", Description: "Pong", Category: "system"},
	}

	suggestions := commandInfoToSuggestions(commands)

	for _, s := range suggestions {
		if s.Label == "fetch_chatlog [topic] [peer_address]" {
			if s.Insert != "fetch_chatlog dm" {
				t.Errorf("expected insert 'fetch_chatlog dm', got %q", s.Insert)
			}
			return
		}
	}
	t.Error("fetch_chatlog suggestion not found")
}

func TestFormatUptime(t *testing.T) {
	tests := []struct {
		duration time.Duration
		want     string
	}{
		{0, "0s"},
		{5 * time.Second, "5s"},
		{59 * time.Second, "59s"},
		{60 * time.Second, "1m0s"},
		{90 * time.Second, "1m30s"},
		{3599 * time.Second, "59m59s"},
		{3600 * time.Second, "1h0m"},
		{3661 * time.Second, "1h1m"},
		{86400 * time.Second, "1d0h"},
		{90061 * time.Second, "1d1h"},  // 25h1m1s → 1d1h
		{172800 * time.Second, "2d0h"}, // 48h
		{-5 * time.Second, "0s"},       // negative clamped
	}

	for _, tc := range tests {
		got := formatUptime(tc.duration)
		if got != tc.want {
			t.Errorf("formatUptime(%v) = %q, want %q", tc.duration, got, tc.want)
		}
	}
}

func TestNewConsoleDonateEntries(t *testing.T) {
	entries := newConsoleDonateEntries()

	if len(entries) != 7 {
		t.Fatalf("expected 7 donate entries, got %d", len(entries))
	}

	want := map[string]string{
		"PirateCash":                "PB2vfGqfagNb12DyYTZBYWGnreyt7E4Pug",
		"Cosanta":                   "Cbbp3meofT1ESU5p4d9ucXpXw9pxKCMEyi",
		"PIRATE / COSANTA (BEP-20)": "0x52be29951B0D10d5eFa48D58363a25fE5Cc097e9",
		"Bitcoin":                   "bc1q2ph64sryt6skegze6726fp98u44kjsc5exktap",
		"Dash":                      "Xv7U37XKp5d4fjvbeuganwhqXN7Sm4JJkt",
		"Zcash":                     "zs1hwyqs4mfrynq0ysjmhv8wuau5zam0gwpx8ujfv8epgyufkmmsp6t7cfk9y0th7qyx7fsc5azm08",
		"Monero":                    "4AzdEoZxeGMFkdtAxaNLAZakqEVsWpVb2at4u6966WGDiXkS7ZPyi7haeThTGUAWXVKDTmQ9DYTWRHMjGVSBW82xRQqPxkg",
	}

	for _, entry := range entries {
		addr, ok := want[entry.Label]
		if !ok {
			t.Fatalf("unexpected donate entry label %q", entry.Label)
		}
		if entry.Address != addr {
			t.Fatalf("entry %q address = %q, want %q", entry.Label, entry.Address, addr)
		}
		delete(want, entry.Label)
	}

	if len(want) != 0 {
		t.Fatalf("missing donate entries: %v", want)
	}

	if consoleDonateURL != "https://pirate.cash/donate/" {
		t.Fatalf("consoleDonateURL = %q, want https://pirate.cash/donate/", consoleDonateURL)
	}
}
