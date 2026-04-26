package desktop

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/rpc"
	"github.com/piratecash/corsa/internal/core/service"
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
		rpc.CommandInfo{Name: "sendDm", Description: "Send DM", Category: "message", Usage: "<to> <body>"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(req.Args)
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetchTrafficHistory", Description: "Traffic history", Category: "metrics"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetchRouteTable", Description: "Full routing table snapshot", Category: "routing"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetchRouteSummary", Description: "Routing table summary", Category: "routing"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]string{"status": "ok"})
			return rpc.CommandResponse{Data: data}
		},
	)

	t.Register(
		rpc.CommandInfo{Name: "fetchRouteLookup", Description: "Lookup routes for identity", Category: "routing", Usage: "<identity>"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(req.Args)
			return rpc.CommandResponse{Data: data}
		},
	)

	t.RegisterUnavailable(
		rpc.CommandInfo{Name: "fetchChatlog", Description: "Unavailable in test mode", Category: "chatlog"},
	)

	return t
}

func TestExecuteCommandDispatchesViaCommandTable(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand(context.Background(), "ping")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "pong") {
		t.Errorf("expected pong in result, got: %s", result)
	}
}

func TestExecuteCommandWithArgs(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand(context.Background(), "sendDm peer-abc hello world")
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

	result, err := cw.executeCommand(context.Background(), `{"type":"ping"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result, "pong") {
		t.Errorf("expected pong in JSON frame result, got: %s", result)
	}
}

func TestExecuteCommandUnavailableReturnsError(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	_, err := cw.executeCommand(context.Background(), "fetchChatlog")
	if err == nil {
		t.Fatal("expected error for unavailable command")
	}

	if !strings.Contains(err.Error(), "not available") {
		t.Errorf("expected 'not available' error, got: %v", err)
	}
}

func TestExecuteCommandPrettyPrintsJSON(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand(context.Background(), "ping")
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
		if s.Insert == "fetchChatlog" {
			t.Error("unavailable command fetchChatlog should not appear in suggestions")
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
	if !found["sendDm"] {
		t.Error("expected sendDm in suggestions")
	}
}

func TestExecuteCommandHelpReturnsHumanReadable(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	result, err := cw.executeCommand(context.Background(), "help")
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
	if !strings.Contains(result, "sendDm") {
		t.Error("expected 'sendDm' in help text")
	}

	// Should contain usage hints for argument-taking commands.
	if !strings.Contains(result, "<to> <body>") {
		t.Error("expected usage hint '<to> <body>' in help text")
	}

	// Should contain the registered-only raw JSON hint (not the old
	// unrestricted "raw JSON protocol frame" wording).
	if !strings.Contains(result, "raw JSON frame for any registered command") {
		t.Error("expected 'raw JSON frame for any registered command' hint in help text")
	}
}

func TestExecuteCommandUnknownFallsBackWithNilClient(t *testing.T) {
	// When client is nil and command is unknown, executeCommand should
	// return the CommandTable error (not panic).
	cw := newTestConsoleWindow(testTable())

	_, err := cw.executeCommand(context.Background(), "nonexistent_command")
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
	if strings.Contains(text, "fetchChatlog") {
		t.Error("unavailable command fetchChatlog should not appear in console help")
	}

	// Metrics category must be present with its command.
	if !strings.Contains(text, "== Metrics ==") {
		t.Error("expected '== Metrics ==' section in console help")
	}
	if !strings.Contains(text, "fetchTrafficHistory") {
		t.Error("expected fetchTrafficHistory in console help")
	}

	// Routing category must be present with its commands.
	if !strings.Contains(text, "== Routing ==") {
		t.Error("expected '== Routing ==' section in console help")
	}
	if !strings.Contains(text, "fetchRouteTable") {
		t.Error("expected fetchRouteTable in console help")
	}
	if !strings.Contains(text, "fetchRouteSummary") {
		t.Error("expected fetchRouteSummary in console help")
	}
	if !strings.Contains(text, "fetchRouteLookup") {
		t.Error("expected fetchRouteLookup in console help")
	}
}

func TestCommandInfoToSuggestionsUsage(t *testing.T) {
	commands := []rpc.CommandInfo{
		{Name: "sendDm", Description: "Send DM", Category: "message", Usage: "<to> <body>"},
		{Name: "ping", Description: "Pong", Category: "system"},
	}

	suggestions := commandInfoToSuggestions(commands)

	if len(suggestions) != 2 {
		t.Fatalf("expected 2 suggestions, got %d", len(suggestions))
	}

	// With Usage: Label includes usage, Insert is just the name.
	for _, s := range suggestions {
		if s.Label == "sendDm <to> <body>" {
			if s.Insert != "sendDm" {
				t.Errorf("expected insert 'sendDm', got %q", s.Insert)
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
	// fetchChatlog should prefill the default topic "dm" on autocomplete,
	// preserving the old desktop console UX shortcut.
	commands := []rpc.CommandInfo{
		{Name: "fetchChatlog", Description: "Fetch chatlog", Category: "chatlog", Usage: "[topic] [peer_address]"},
		{Name: "ping", Description: "Pong", Category: "system"},
	}

	suggestions := commandInfoToSuggestions(commands)

	for _, s := range suggestions {
		if s.Label == "fetchChatlog [topic] [peer_address]" {
			if s.Insert != "fetchChatlog dm" {
				t.Errorf("expected insert 'fetchChatlog dm', got %q", s.Insert)
			}
			return
		}
	}
	t.Error("fetchChatlog suggestion not found")
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

func TestCountUniquePeersExcludesPendingOnlyPlaceholders(t *testing.T) {
	peers := []service.PeerHealth{
		// Real observed peer with PeerID.
		{Address: "1.2.3.4:9000", PeerID: "peer-a", State: "healthy", Connected: true},
		// Pre-handshake peer — no PeerID but has State from health snapshot.
		{Address: "1.2.3.5:9000", State: "reconnecting"},
		// Inbound peer — no PeerID but Connected.
		{Address: "1.2.3.6:9000", Connected: true, Direction: "inbound"},
		// Pending-only placeholder (created by applyPeerPendingDelta) — should NOT count.
		{Address: "1.2.3.7:9000", PendingCount: 5},
		// Another pending-only placeholder.
		{Address: "1.2.3.8:9000", PendingCount: 1},
		// Peer with only Direction set (outbound slot allocated).
		{Address: "1.2.3.9:9000", Direction: "outbound"},
	}

	got := countUniquePeers(service.NodeStatus{PeerHealth: peers})
	// 4 observed peers: peer-a, 1.2.3.5, 1.2.3.6, 1.2.3.9.
	// 2 pending-only (1.2.3.7, 1.2.3.8) excluded.
	if got != 4 {
		t.Fatalf("countUniquePeers = %d, want 4", got)
	}
}

func TestCountUniquePeersIncludesSlotOnlyPeers(t *testing.T) {
	peers := []service.PeerHealth{
		// CM slot-only peer (queued/dialing before any health delta) — should count.
		{Address: "1.2.3.4:9000", SlotState: "queued"},
		// CM slot-only peer (dialing) — should count.
		{Address: "1.2.3.5:9000", SlotState: "dialing"},
		// CM slot-only peer (retry_wait) — should count.
		{Address: "1.2.3.6:9000", SlotState: "retry_wait"},
		// Pending-only placeholder — should NOT count.
		{Address: "1.2.3.7:9000", PendingCount: 2},
	}

	got := countUniquePeers(service.NodeStatus{PeerHealth: peers})
	// 3 slot-managed peers, 1 pending-only excluded.
	if got != 3 {
		t.Fatalf("countUniquePeers = %d, want 3 (slot-only peers must count)", got)
	}
}

func TestCountUniquePeersDeduplicatesByPeerID(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:9000", PeerID: "peer-a", Connected: true},
		// Same PeerID, different address (reconnect).
		{Address: "1.2.3.5:9000", PeerID: "peer-a", Connected: false, State: "reconnecting"},
	}

	got := countUniquePeers(service.NodeStatus{PeerHealth: peers})
	if got != 1 {
		t.Fatalf("countUniquePeers = %d, want 1 (same PeerID)", got)
	}
}

func TestCountUniquePeersEmptySlice(t *testing.T) {
	if got := countUniquePeers(service.NodeStatus{}); got != 0 {
		t.Fatalf("countUniquePeers(empty) = %d, want 0", got)
	}
}

func TestIsPeerObserved(t *testing.T) {
	tests := []struct {
		name string
		peer service.PeerHealth
		want bool
	}{
		{"with PeerID", service.PeerHealth{PeerID: "abc"}, true},
		{"connected", service.PeerHealth{Connected: true}, true},
		{"with State", service.PeerHealth{State: "healthy"}, true},
		{"with Direction", service.PeerHealth{Direction: "outbound"}, true},
		{"with SlotState", service.PeerHealth{Address: "1.2.3.4:9000", SlotState: "dialing"}, true},
		{"slot queued", service.PeerHealth{Address: "1.2.3.4:9000", SlotState: "queued"}, true},
		{"slot retry_wait", service.PeerHealth{Address: "1.2.3.4:9000", SlotState: "retry_wait"}, true},
		{"pending-only placeholder", service.PeerHealth{Address: "1.2.3.4:9000", PendingCount: 3}, false},
		{"empty entry", service.PeerHealth{}, false},
	}
	for _, tc := range tests {
		if got := isPeerObserved(tc.peer); got != tc.want {
			t.Errorf("isPeerObserved(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestActivePeerHealth_FiltersCorrectly(t *testing.T) {
	peers := []service.PeerHealth{
		{Address: "1.2.3.4:9000", Connected: true, SlotState: "active"},         // CM slot + connected → include
		{Address: "1.2.3.5:9000", Connected: false, SlotState: "queued"},        // CM slot queued → include
		{Address: "1.2.3.6:9000", Connected: false, SlotState: "dialing"},       // CM slot dialing → include
		{Address: "1.2.3.7:9000", Connected: false, SlotState: "retry_wait"},    // CM slot retry_wait → include
		{Address: "1.2.3.8:9000", Connected: true, SlotState: ""},               // inbound connected, no slot → include
		{Address: "1.2.3.9:9000", Connected: false, SlotState: "active"},        // CM slot active, not connected yet → include
		{Address: "1.2.3.10:9000", Connected: false, SlotState: "reconnecting"}, // CM slot reconnecting → include
		{Address: "1.2.3.11:9000", Connected: false, SlotState: ""},             // no slot, not connected → exclude (known-only)
	}

	active := activePeerHealth(peers)

	// All peers with SlotState or Connected should be included;
	// only 1.2.3.11 (no slot, not connected) should be excluded.
	if len(active) != 7 {
		t.Fatalf("expected 7 active peers, got %d", len(active))
	}

	addresses := make(map[string]bool)
	for _, p := range active {
		addresses[p.Address] = true
	}

	want := []string{
		"1.2.3.4:9000", "1.2.3.5:9000", "1.2.3.6:9000", "1.2.3.7:9000",
		"1.2.3.8:9000", "1.2.3.9:9000", "1.2.3.10:9000",
	}
	for _, addr := range want {
		if !addresses[addr] {
			t.Errorf("expected %s in active peers", addr)
		}
	}

	if addresses["1.2.3.11:9000"] {
		t.Error("did not expect known-only peer 1.2.3.11:9000 in active peers")
	}
}

func TestActivePeerHealth_EmptyInput(t *testing.T) {
	active := activePeerHealth(nil)
	if len(active) != 0 {
		t.Fatalf("expected 0 active peers for nil input, got %d", len(active))
	}
}

// trafficStatsTable returns a CommandTable wired with mocks suitable for
// exercising loadTrafficHistory + sampleTraffic flows. sentTotal and recvTotal
// control the cumulative byte counters reported by fetchNetworkStats; emptyHistory
// controls whether fetchTrafficHistory returns an empty-but-well-formed frame
// (mimicking a collector restart) or a single-sample frame.
func trafficStatsTable(sentTotal, recvTotal int64, emptyHistory bool) *rpc.CommandTable {
	tt := rpc.NewCommandTable()

	tt.Register(
		rpc.CommandInfo{Name: "fetchNetworkStats", Description: "Network stats", Category: "metrics"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			data, _ := json.Marshal(map[string]any{
				"network_stats": map[string]any{
					"total_bytes_sent":     sentTotal,
					"total_bytes_received": recvTotal,
				},
			})
			return rpc.CommandResponse{Data: data}
		},
	)

	tt.Register(
		rpc.CommandInfo{Name: "fetchTrafficHistory", Description: "Traffic history", Category: "metrics"},
		func(req rpc.CommandRequest) rpc.CommandResponse {
			if emptyHistory {
				data, _ := json.Marshal(map[string]any{
					"traffic_history": map[string]any{"samples": []any{}},
				})
				return rpc.CommandResponse{Data: data}
			}
			data, _ := json.Marshal(map[string]any{
				"traffic_history": map[string]any{
					"samples": []map[string]any{
						{
							"bytes_sent_ps":  int64(10),
							"bytes_recv_ps":  int64(20),
							"total_sent":     sentTotal,
							"total_received": recvTotal,
						},
					},
				},
			})
			return rpc.CommandResponse{Data: data}
		},
	)

	return tt
}

func TestLoadTrafficHistoryEmptyLeavesUnloaded(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(5000, 3000, true))

	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false for valid empty frame")
	}

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if cw.trafficLoaded {
		t.Error("trafficLoaded should stay false after empty history — baseline not captured yet")
	}
	if cw.trafficTotalSent != 0 || cw.trafficTotalRecv != 0 {
		t.Errorf("totals should be zero for empty history, got sent=%d recv=%d",
			cw.trafficTotalSent, cw.trafficTotalRecv)
	}
	if len(cw.trafficSamplesIn) != 0 || len(cw.trafficSamplesOut) != 0 {
		t.Errorf("no samples expected for empty history, got in=%d out=%d",
			len(cw.trafficSamplesIn), len(cw.trafficSamplesOut))
	}
}

func TestLoadTrafficHistoryNonEmptyPopulatesAndMarksLoaded(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(5000, 3000, false))

	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if !cw.trafficLoaded {
		t.Error("trafficLoaded should be true after non-empty history")
	}
	if cw.trafficTotalSent != 5000 || cw.trafficTotalRecv != 3000 {
		t.Errorf("totals mismatch: sent=%d recv=%d", cw.trafficTotalSent, cw.trafficTotalRecv)
	}
	if len(cw.trafficSamplesIn) != 1 || len(cw.trafficSamplesOut) != 1 {
		t.Fatalf("expected 1 sample each, got in=%d out=%d",
			len(cw.trafficSamplesIn), len(cw.trafficSamplesOut))
	}
	if cw.trafficSamplesIn[0] != 20 || cw.trafficSamplesOut[0] != 10 {
		t.Errorf("sample mismatch: in=%v out=%v", cw.trafficSamplesIn[0], cw.trafficSamplesOut[0])
	}
}

// TestSampleTrafficSeedsBaselineWhenNotLoaded verifies that the first
// sampleTraffic call after an empty history load records the current counters
// as baseline and flips trafficLoaded to true, without appending any sample.
// This is the precondition for the baseline-seed fix in startTrafficTicker:
// the seeded baseline lets the first ticker-driven tick produce a real delta
// rather than being wasted on baseline capture.
func TestSampleTrafficSeedsBaselineWhenNotLoaded(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(7777, 4444, true))

	// Simulate the state after a successful empty-history load.
	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}

	cw.sampleTraffic()

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if !cw.trafficLoaded {
		t.Error("trafficLoaded should flip to true after baseline seed")
	}
	if cw.trafficTotalSent != 7777 || cw.trafficTotalRecv != 4444 {
		t.Errorf("baseline totals mismatch: sent=%d recv=%d",
			cw.trafficTotalSent, cw.trafficTotalRecv)
	}
	if len(cw.trafficSamplesIn) != 0 || len(cw.trafficSamplesOut) != 0 {
		t.Errorf("baseline seed must NOT append a sample, got in=%d out=%d",
			len(cw.trafficSamplesIn), len(cw.trafficSamplesOut))
	}
}

// TestSampleTrafficAppendsDeltaAfterBaseline verifies that once the baseline
// is captured, the next sampleTraffic call appends a real delta sample. This
// exercises the path the first ticker tick takes after the baseline seed.
func TestSampleTrafficAppendsDeltaAfterBaseline(t *testing.T) {
	// First call seeds baseline at 1000/500.
	tableBaseline := trafficStatsTable(1000, 500, true)
	cw := newTestConsoleWindow(tableBaseline)
	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}
	cw.sampleTraffic()

	// Swap in a table with advanced counters to simulate a later tick.
	cw.parent.cmdTable = trafficStatsTable(1100, 520, true)
	cw.sampleTraffic()

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if cw.trafficTotalSent != 1100 || cw.trafficTotalRecv != 520 {
		t.Errorf("cumulative totals not updated: sent=%d recv=%d",
			cw.trafficTotalSent, cw.trafficTotalRecv)
	}
	if len(cw.trafficSamplesOut) != 1 || cw.trafficSamplesOut[0] != 100 {
		t.Errorf("expected single sent delta=100, got %v", cw.trafficSamplesOut)
	}
	if len(cw.trafficSamplesIn) != 1 || cw.trafficSamplesIn[0] != 20 {
		t.Errorf("expected single recv delta=20, got %v", cw.trafficSamplesIn)
	}
}

// TestSeedTrafficBaselineIfNeededSeedsAfterEmptyHistory mirrors the
// startTrafficTicker orchestration: after loadTrafficHistory returns with an
// empty frame (trafficLoaded==false), the helper must populate the baseline
// totals from fetchNetworkStats and flip trafficLoaded so the next tick
// produces a real delta sample. This is the regression path for the
// traffic-chart "bars appear one tick late" bug.
func TestSeedTrafficBaselineIfNeededSeedsAfterEmptyHistory(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(9999, 8888, true))
	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}

	cw.mu.RLock()
	if cw.trafficLoaded {
		cw.mu.RUnlock()
		t.Fatal("precondition failed: empty history must leave trafficLoaded=false")
	}
	cw.mu.RUnlock()

	cw.seedTrafficBaselineIfNeeded()

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if !cw.trafficLoaded {
		t.Error("trafficLoaded should be true after baseline seed")
	}
	if cw.trafficTotalSent != 9999 || cw.trafficTotalRecv != 8888 {
		t.Errorf("baseline totals mismatch: sent=%d recv=%d",
			cw.trafficTotalSent, cw.trafficTotalRecv)
	}
	if len(cw.trafficSamplesIn) != 0 || len(cw.trafficSamplesOut) != 0 {
		t.Errorf("baseline seed must NOT append a sample, got in=%d out=%d",
			len(cw.trafficSamplesIn), len(cw.trafficSamplesOut))
	}
}

// TestSeedTrafficBaselineIfNeededNoOpWhenLoaded verifies the helper is a
// no-op when history already populated the baseline, so calling it after a
// non-empty loadTrafficHistory does not double-sample or disturb the cached
// samples.
func TestSeedTrafficBaselineIfNeededNoOpWhenLoaded(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(5000, 3000, false))
	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}

	beforeIn := len(cw.trafficSamplesIn)
	beforeOut := len(cw.trafficSamplesOut)
	beforeSent := cw.trafficTotalSent
	beforeRecv := cw.trafficTotalRecv

	cw.seedTrafficBaselineIfNeeded()

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if len(cw.trafficSamplesIn) != beforeIn || len(cw.trafficSamplesOut) != beforeOut {
		t.Errorf("sample count changed on no-op path: in %d→%d, out %d→%d",
			beforeIn, len(cw.trafficSamplesIn), beforeOut, len(cw.trafficSamplesOut))
	}
	if cw.trafficTotalSent != beforeSent || cw.trafficTotalRecv != beforeRecv {
		t.Errorf("totals changed on no-op path: sent %d→%d, recv %d→%d",
			beforeSent, cw.trafficTotalSent, beforeRecv, cw.trafficTotalRecv)
	}
}

// TestSampleTrafficClampsNegativeDeltaToZero verifies that if the collector
// reports a smaller cumulative value than the cached baseline (e.g. after a
// restart without a tab reload), the delta is clamped to zero rather than
// producing a negative sample that would render as a downward bar.
func TestSampleTrafficClampsNegativeDeltaToZero(t *testing.T) {
	cw := newTestConsoleWindow(trafficStatsTable(1000, 500, true))
	if ok := cw.loadTrafficHistory(context.Background()); !ok {
		t.Fatalf("loadTrafficHistory returned false")
	}
	cw.sampleTraffic() // baseline at 1000/500

	// Counters regressed (lower than baseline).
	cw.parent.cmdTable = trafficStatsTable(200, 100, true)
	cw.sampleTraffic()

	cw.mu.RLock()
	defer cw.mu.RUnlock()

	if len(cw.trafficSamplesOut) != 1 || cw.trafficSamplesOut[0] != 0 {
		t.Errorf("expected clamped sent delta=0, got %v", cw.trafficSamplesOut)
	}
	if len(cw.trafficSamplesIn) != 1 || cw.trafficSamplesIn[0] != 0 {
		t.Errorf("expected clamped recv delta=0, got %v", cw.trafficSamplesIn)
	}
}

func TestActivePeerSummary_Fallback(t *testing.T) {
	peers := []service.PeerHealth{
		{State: "healthy", BytesReceived: 1024, BytesSent: 512},
		{State: "healthy", BytesReceived: 2048, BytesSent: 1024},
		{State: "degraded", BytesReceived: 100, BytesSent: 50},
	}

	// activePeerSummary with nil parent.t will hit the fallback path
	// because parent.t("node.active_peer.summary", ...) returns the key itself.
	summary := activePeerSummary(&Window{}, peers)

	if !strings.Contains(summary, "Healthy: 2") {
		t.Errorf("expected 'Healthy: 2' in summary, got: %s", summary)
	}
	if !strings.Contains(summary, "Degraded: 1") {
		t.Errorf("expected 'Degraded: 1' in summary, got: %s", summary)
	}
	if !strings.Contains(summary, "Stalled: 0") {
		t.Errorf("expected 'Stalled: 0' in summary, got: %s", summary)
	}
}

// TestAppendCommandHistoryDeduplicatesConsecutive ensures the same command
// submitted twice in a row collapses into a single history entry — otherwise
// pressing Up after a re-run would replay the duplicate before reaching the
// previous distinct command.
func TestAppendCommandHistoryDeduplicatesConsecutive(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	cw.appendCommandHistory("ping")
	cw.appendCommandHistory("ping")
	cw.appendCommandHistory("help")
	cw.appendCommandHistory("help")
	cw.appendCommandHistory("ping")

	want := []string{"ping", "help", "ping"}
	if len(cw.commandHistory) != len(want) {
		t.Fatalf("expected history length %d, got %d (%v)", len(want), len(cw.commandHistory), cw.commandHistory)
	}
	for i, v := range want {
		if cw.commandHistory[i] != v {
			t.Errorf("history[%d] = %q, want %q", i, cw.commandHistory[i], v)
		}
	}
}

// TestAppendCommandHistoryIgnoresEmpty makes sure whitespace-only or empty
// submits never enter the ring. submitConsoleCommand already trims and
// short-circuits empty input, but the helper enforces the same contract on
// its own so future callers cannot accidentally pollute history.
func TestAppendCommandHistoryIgnoresEmpty(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	cw.appendCommandHistory("")
	if len(cw.commandHistory) != 0 {
		t.Fatalf("empty command must not be recorded, got %v", cw.commandHistory)
	}
}

// TestAppendCommandHistoryCapsAtMax verifies the ring drops the oldest
// entries once it grows past maxConsoleCommandHistory, so long-running
// console sessions cannot accumulate unbounded memory.
func TestAppendCommandHistoryCapsAtMax(t *testing.T) {
	cw := newTestConsoleWindow(testTable())

	// Use a synthetic alphabet that produces non-consecutive duplicates.
	for i := 0; i < maxConsoleCommandHistory+5; i++ {
		cw.appendCommandHistory(string(rune('a'+(i%26))) + "_" + string(rune('0'+(i%10))) + "_" + string(rune('A'+(i/26)%26)))
	}

	if len(cw.commandHistory) != maxConsoleCommandHistory {
		t.Fatalf("expected history capped at %d, got %d", maxConsoleCommandHistory, len(cw.commandHistory))
	}
}

// TestNavigateHistoryUpReplaysLatest covers the most common interaction —
// pressing Up on an empty input restores the most recently submitted command.
func TestNavigateHistoryUpReplaysLatest(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.appendCommandHistory("ping")
	cw.appendCommandHistory("help")
	cw.resetHistoryNavigation()

	cw.navigateHistory(-1)

	if got := cw.consoleEditor.Text(); got != "help" {
		t.Errorf("expected editor text 'help', got %q", got)
	}
	if cw.historyCursor != 1 {
		t.Errorf("expected cursor at index 1, got %d", cw.historyCursor)
	}
}

// TestNavigateHistoryWalksOlderToNewer exercises the full ring traversal:
// repeated Up reaches the oldest entry and clamps there; Down walks back
// toward the most recent entry and finally restores the original draft.
func TestNavigateHistoryWalksOlderToNewer(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.appendCommandHistory("a")
	cw.appendCommandHistory("b")
	cw.appendCommandHistory("c")
	cw.resetHistoryNavigation()

	cw.consoleEditor.SetText("draft")

	// Up walks toward older entries.
	cw.navigateHistory(-1)
	if got := cw.consoleEditor.Text(); got != "c" {
		t.Errorf("after 1st Up: got %q, want %q", got, "c")
	}
	cw.navigateHistory(-1)
	if got := cw.consoleEditor.Text(); got != "b" {
		t.Errorf("after 2nd Up: got %q, want %q", got, "b")
	}
	cw.navigateHistory(-1)
	if got := cw.consoleEditor.Text(); got != "a" {
		t.Errorf("after 3rd Up: got %q, want %q", got, "a")
	}
	// Past the oldest — clamp.
	cw.navigateHistory(-1)
	if got := cw.consoleEditor.Text(); got != "a" {
		t.Errorf("after Up at oldest: got %q, want clamped %q", got, "a")
	}

	// Down walks back toward the newest, then restores the draft.
	cw.navigateHistory(1)
	if got := cw.consoleEditor.Text(); got != "b" {
		t.Errorf("after 1st Down: got %q, want %q", got, "b")
	}
	cw.navigateHistory(1)
	if got := cw.consoleEditor.Text(); got != "c" {
		t.Errorf("after 2nd Down: got %q, want %q", got, "c")
	}
	cw.navigateHistory(1)
	if got := cw.consoleEditor.Text(); got != "draft" {
		t.Errorf("after Down past newest: got %q, want draft %q", got, "draft")
	}
	// Past the draft — clamp at draft.
	cw.navigateHistory(1)
	if got := cw.consoleEditor.Text(); got != "draft" {
		t.Errorf("after Down at draft: got %q, want clamped %q", got, "draft")
	}
}

// TestNavigateHistoryEmptyIsNoop guards against panics or stray writes when
// the user presses Up before submitting any commands.
func TestNavigateHistoryEmptyIsNoop(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.consoleEditor.SetText("typing")

	cw.navigateHistory(-1)

	if got := cw.consoleEditor.Text(); got != "typing" {
		t.Errorf("editor text must not change on empty history, got %q", got)
	}
	if cw.historyCursor != 0 {
		t.Errorf("cursor must stay at 0 on empty history, got %d", cw.historyCursor)
	}
}

// TestSyncHistoryNavigationResetsOnUserEdit verifies that mid-browse manual
// editing of the editor text drops navigation state, so the next Up press
// snapshots the new draft instead of the stale one.
func TestSyncHistoryNavigationResetsOnUserEdit(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.appendCommandHistory("alpha")
	cw.appendCommandHistory("beta")
	cw.resetHistoryNavigation()

	cw.consoleEditor.SetText("draft")
	cw.navigateHistory(-1) // editor = "beta", cursor = 1

	if cw.historyCursor != 1 {
		t.Fatalf("precondition: cursor must be at 1, got %d", cw.historyCursor)
	}

	// Simulate the user typing — text now diverges from historyText.
	cw.consoleEditor.SetText("beta-extra")

	cw.syncHistoryNavigation()

	if cw.historyCursor != len(cw.commandHistory) {
		t.Errorf("cursor must reset to len(history)=%d after manual edit, got %d", len(cw.commandHistory), cw.historyCursor)
	}
	if cw.historyDraft != "" {
		t.Errorf("draft must clear after manual edit, got %q", cw.historyDraft)
	}

	// Next Up snapshots the new text as the draft, then jumps to the latest
	// history entry.
	cw.navigateHistory(-1)
	if cw.historyDraft != "beta-extra" {
		t.Errorf("expected new draft 'beta-extra' after edit + Up, got %q", cw.historyDraft)
	}
	if got := cw.consoleEditor.Text(); got != "beta" {
		t.Errorf("expected editor 'beta' after Up, got %q", got)
	}
}

// TestNavigateHistorySuppressesSuggestions ensures that walking history does
// not leave the suggestion popup primed to hijack the next arrow press —
// every navigation step must mark suggestions as hidden and snap the
// completion cursor back to a clean state.
func TestNavigateHistorySuppressesSuggestions(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.appendCommandHistory("ping")
	cw.resetHistoryNavigation()

	cw.hideSuggestions = false
	cw.selectedSuggest = 2
	cw.suggestSnapshot = []consoleSuggestion{{Label: "ping", Insert: "ping"}}

	cw.navigateHistory(-1)

	if !cw.hideSuggestions {
		t.Error("hideSuggestions must be true while browsing history")
	}
	if cw.selectedSuggest != -1 {
		t.Errorf("selectedSuggest must reset to -1, got %d", cw.selectedSuggest)
	}
	if cw.suggestSnapshot != nil {
		t.Errorf("suggestSnapshot must clear, got %v", cw.suggestSnapshot)
	}
}

// TestSubmitConsoleCommandRecordsHistory drives the full submit path on a
// quick command and asserts the command lands in history with the cursor
// parked one past the end. Uses the registered "ping" command so executeCommand
// completes synchronously without touching network or RPC client.
func TestSubmitConsoleCommandRecordsHistory(t *testing.T) {
	cw := newTestConsoleWindow(testTable())
	cw.consoleEditor.SetText("ping")

	cw.submitConsoleCommand()

	// submitConsoleCommand spawns a goroutine for the actual command — wait
	// for the busy flag to clear before asserting on the late-mutated state.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && cw.isConsoleBusy() {
		time.Sleep(5 * time.Millisecond)
	}

	if len(cw.commandHistory) != 1 || cw.commandHistory[0] != "ping" {
		t.Fatalf("expected history=[ping], got %v", cw.commandHistory)
	}
	if cw.historyCursor != len(cw.commandHistory) {
		t.Errorf("cursor must reset to len(history)=%d after submit, got %d", len(cw.commandHistory), cw.historyCursor)
	}
}
