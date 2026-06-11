package rpc_test

import (
	"encoding/json"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/rpc"
)

func setupTestServerWithMetrics(t *testing.T, node rpc.NodeProvider, metrics rpc.MetricsProvider) *rpc.Server {
	t.Helper()
	cfg := config.RPC{Host: "127.0.0.1", Port: "0"}
	table := buildTestTable(node, nil, nil, metrics)
	server, err := rpc.NewServer(cfg, table)
	if err != nil {
		t.Fatalf("create test server: %v", err)
	}
	return server
}

func TestMetricsTrafficHistory(t *testing.T) {
	samples := []protocol.TrafficSampleFrame{
		{Timestamp: "2026-03-27T10:00:00Z", BytesSentPS: 100, BytesRecvPS: 200, TotalSent: 100, TotalReceived: 200},
		{Timestamp: "2026-03-27T10:00:01Z", BytesSentPS: 150, BytesRecvPS: 250, TotalSent: 250, TotalReceived: 450},
	}
	metrics := newMockMetricsProvider(t, protocol.Frame{
		Type: "traffic_history",
		TrafficHistory: &protocol.TrafficHistoryFrame{
			IntervalSeconds: 1,
			Capacity:        3600,
			Count:           2,
			Samples:         samples,
		},
	})
	node := newDefaultNodeProvider(t)
	server := setupTestServerWithMetrics(t, node, metrics)

	code, result := postJSON(t, server, "/rpc/v1/metrics/traffic_history", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "traffic_history")

	history, ok := result["traffic_history"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected traffic_history to be object, got %T", result["traffic_history"])
	}
	samplesResult, ok := history["samples"].([]interface{})
	if !ok {
		t.Fatalf("expected samples to be array, got %T", history["samples"])
	}
	if len(samplesResult) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samplesResult))
	}
}

// TestMetricsTrafficHistoryLegacyRouteSince verifies the legacy HTTP route
// forwards the JSON body as command args: {"since": ...} must filter samples
// exactly like the CommandTable path, and a malformed since must surface the
// validation error instead of being silently ignored.
func TestMetricsTrafficHistoryLegacyRouteSince(t *testing.T) {
	samples := []protocol.TrafficSampleFrame{
		{Timestamp: "2026-03-27T10:00:00Z", BytesSentPS: 100, BytesRecvPS: 200, TotalSent: 100, TotalReceived: 200},
		{Timestamp: "2026-03-27T10:00:01Z", BytesSentPS: 150, BytesRecvPS: 250, TotalSent: 250, TotalReceived: 450},
	}
	metrics := newMockMetricsProvider(t, protocol.Frame{
		Type: "traffic_history",
		TrafficHistory: &protocol.TrafficHistoryFrame{
			IntervalSeconds: 1,
			Capacity:        3600,
			Count:           2,
			Samples:         samples,
		},
	})
	server := setupTestServerWithMetrics(t, newDefaultNodeProvider(t), metrics)

	code, result := postJSON(t, server, "/rpc/v1/metrics/traffic_history",
		map[string]interface{}{"since": "2026-03-27T10:00:00Z"})

	expectStatusCode(t, code, 200)
	history, ok := result["traffic_history"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected traffic_history to be object, got %T", result["traffic_history"])
	}
	samplesResult, ok := history["samples"].([]interface{})
	if !ok {
		t.Fatalf("expected samples to be array, got %T", history["samples"])
	}
	if len(samplesResult) != 1 {
		t.Fatalf("expected 1 sample newer than cursor, got %d", len(samplesResult))
	}
	first, ok := samplesResult[0].(map[string]interface{})
	if !ok || first["timestamp"] != "2026-03-27T10:00:01Z" {
		t.Fatalf("expected the 10:00:01Z sample, got %+v", samplesResult[0])
	}

	// Malformed since → validation error from the command, not a silent
	// full-history reply.
	code, result = postJSON(t, server, "/rpc/v1/metrics/traffic_history",
		map[string]interface{}{"since": "yesterday"})
	expectStatusCode(t, code, 400)
	expectFieldExists(t, result, "error")
}

// TestMetricsTrafficHistorySinceFiltersSamples verifies the incremental
// cursor: with args.since set, only samples strictly newer than the given
// RFC3339 timestamp are returned, and the provider's shared frame is not
// mutated (a follow-up call without since still sees the full history).
func TestMetricsTrafficHistorySinceFiltersSamples(t *testing.T) {
	samples := []protocol.TrafficSampleFrame{
		{Timestamp: "2026-03-27T10:00:00Z", BytesSentPS: 100, BytesRecvPS: 200, TotalSent: 100, TotalReceived: 200},
		{Timestamp: "2026-03-27T10:00:01Z", BytesSentPS: 150, BytesRecvPS: 250, TotalSent: 250, TotalReceived: 450},
		{Timestamp: "2026-03-27T10:00:02Z", BytesSentPS: 10, BytesRecvPS: 20, TotalSent: 260, TotalReceived: 470},
	}
	metrics := newMockMetricsProvider(t, protocol.Frame{
		Type: "traffic_history",
		TrafficHistory: &protocol.TrafficHistoryFrame{
			IntervalSeconds: 1,
			Capacity:        3600,
			Count:           3,
			Samples:         samples,
		},
	})
	table := buildTestTable(newDefaultNodeProvider(t), nil, nil, metrics)

	decode := func(t *testing.T, data []byte) []protocol.TrafficSampleFrame {
		t.Helper()
		var frame protocol.Frame
		if err := json.Unmarshal(data, &frame); err != nil || frame.TrafficHistory == nil {
			t.Fatalf("decode traffic_history: err=%v frame=%+v", err, frame)
		}
		if frame.TrafficHistory.Count != len(frame.TrafficHistory.Samples) {
			t.Errorf("count=%d does not match samples=%d",
				frame.TrafficHistory.Count, len(frame.TrafficHistory.Samples))
		}
		return frame.TrafficHistory.Samples
	}

	resp := table.Execute(rpc.CommandRequest{
		Name: "fetchTrafficHistory",
		Args: map[string]interface{}{"since": "2026-03-27T10:00:00Z"},
	})
	if resp.Error != nil {
		t.Fatalf("execute with since: %v", resp.Error)
	}
	got := decode(t, resp.Data)
	if len(got) != 2 || got[0].Timestamp != "2026-03-27T10:00:01Z" {
		t.Fatalf("expected 2 samples newer than cursor, got %+v", got)
	}

	// since == newest timestamp → empty tail.
	resp = table.Execute(rpc.CommandRequest{
		Name: "fetchTrafficHistory",
		Args: map[string]interface{}{"since": "2026-03-27T10:00:02Z"},
	})
	if resp.Error != nil {
		t.Fatalf("execute with newest since: %v", resp.Error)
	}
	if got := decode(t, resp.Data); len(got) != 0 {
		t.Fatalf("expected empty tail, got %+v", got)
	}

	// Offset form denoting the same instant as the first sample's "...Z"
	// timestamp must filter identically — the comparison is by parsed
	// time.Time, not by string.
	resp = table.Execute(rpc.CommandRequest{
		Name: "fetchTrafficHistory",
		Args: map[string]interface{}{"since": "2026-03-27T11:00:00+01:00"},
	})
	if resp.Error != nil {
		t.Fatalf("execute with offset since: %v", resp.Error)
	}
	got = decode(t, resp.Data)
	if len(got) != 2 || got[0].Timestamp != "2026-03-27T10:00:01Z" {
		t.Fatalf("offset since must exclude the equal instant, got %+v", got)
	}

	// Unparseable since → validation error.
	resp = table.Execute(rpc.CommandRequest{
		Name: "fetchTrafficHistory",
		Args: map[string]interface{}{"since": "yesterday"},
	})
	if resp.ErrorKind != rpc.ErrValidation {
		t.Fatalf("expected ErrValidation for bad since, got kind=%v err=%v", resp.ErrorKind, resp.Error)
	}

	// No since → full history; also proves the filtered calls above did not
	// mutate the provider's shared frame.
	resp = table.Execute(rpc.CommandRequest{Name: "fetchTrafficHistory"})
	if resp.Error != nil {
		t.Fatalf("execute without since: %v", resp.Error)
	}
	if got := decode(t, resp.Data); len(got) != 3 {
		t.Fatalf("expected full history of 3 samples, got %+v", got)
	}
}

func TestMetricsTrafficHistoryNilProvider(t *testing.T) {
	node := newDefaultNodeProvider(t)
	server := setupTestServerWithMetrics(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/metrics/traffic_history", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestMetricsCommandHiddenWhenProviderNil(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, nil)

	for _, cmd := range table.Commands() {
		if cmd.Name == "fetchTrafficHistory" {
			t.Fatal("fetch_traffic_history should be hidden from Commands() when MetricsProvider is nil")
		}
	}

	resp := table.Execute(rpc.CommandRequest{Name: "fetchTrafficHistory"})
	if resp.ErrorKind != rpc.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %v", resp.ErrorKind)
	}
}

func TestMetricsCommandVisibleWhenProviderSet(t *testing.T) {
	table := rpc.NewCommandTable()
	metrics := newMockMetricsProvider(t, protocol.Frame{
		Type:           "traffic_history",
		TrafficHistory: &protocol.TrafficHistoryFrame{},
	})
	rpc.RegisterAllCommands(table, newDefaultNodeProvider(t), nil, nil, metrics)

	found := false
	for _, cmd := range table.Commands() {
		if cmd.Name == "fetchTrafficHistory" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("fetch_traffic_history should be visible in Commands() when MetricsProvider is set")
	}
}

func TestNetworkStatsCommand(t *testing.T) {
	node := newNodeProviderWithHandler(t, func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_network_stats" {
			return protocol.Frame{
				Type: "network_stats",
				NetworkStats: &protocol.NetworkStatsFrame{
					TotalBytesSent:     1024,
					TotalBytesReceived: 2048,
					TotalTraffic:       3072,
					ConnectedPeers:     2,
					KnownPeers:         3,
					PeerTraffic: []protocol.PeerTrafficFrame{
						{Address: "peer1:8000", BytesSent: 512, BytesReceived: 1024, TotalTraffic: 1536, Connected: true},
						{Address: "peer2:8000", BytesSent: 512, BytesReceived: 1024, TotalTraffic: 1536, Connected: true},
					},
				},
			}
		}
		return protocol.Frame{Type: "ok"}
	})
	server := setupTestServer(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/network/stats", map[string]interface{}{})

	expectStatusCode(t, code, 200)
	expectField(t, result, "type", "network_stats")

	stats, ok := result["network_stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected network_stats to be object, got %T", result["network_stats"])
	}
	if int64(stats["total_bytes_sent"].(float64)) != 1024 {
		t.Errorf("expected total_bytes_sent=1024, got %v", stats["total_bytes_sent"])
	}
	if int64(stats["total_bytes_received"].(float64)) != 2048 {
		t.Errorf("expected total_bytes_received=2048, got %v", stats["total_bytes_received"])
	}
	peers, ok := stats["peer_traffic"].([]interface{})
	if !ok {
		t.Fatalf("expected peer_traffic to be array, got %T", stats["peer_traffic"])
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 peer_traffic entries, got %d", len(peers))
	}
}
