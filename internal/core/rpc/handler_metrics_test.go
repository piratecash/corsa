package rpc_test

import (
	"testing"

	"corsa/internal/core/protocol"
	"corsa/internal/core/rpc"

	"corsa/internal/core/config"
)

// mockMetricsProvider implements rpc.MetricsProvider for testing.
type mockMetricsProvider struct {
	snapshot protocol.Frame
}

func (m *mockMetricsProvider) TrafficSnapshot() protocol.Frame {
	return m.snapshot
}

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
	metrics := &mockMetricsProvider{
		snapshot: protocol.Frame{
			Type: "traffic_history",
			TrafficHistory: &protocol.TrafficHistoryFrame{
				IntervalSeconds: 1,
				Capacity:        3600,
				Count:           2,
				Samples:         samples,
			},
		},
	}
	node := &mockNodeProvider{}
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

func TestMetricsTrafficHistoryNilProvider(t *testing.T) {
	node := &mockNodeProvider{}
	server := setupTestServerWithMetrics(t, node, nil)

	code, result := postJSON(t, server, "/rpc/v1/metrics/traffic_history", map[string]interface{}{})

	expectStatusCode(t, code, 503)
	expectFieldExists(t, result, "error")
}

func TestMetricsCommandHiddenWhenProviderNil(t *testing.T) {
	table := rpc.NewCommandTable()
	rpc.RegisterAllCommands(table, &mockNodeProvider{}, nil, nil, nil, nil)

	for _, cmd := range table.Commands() {
		if cmd.Name == "fetch_traffic_history" {
			t.Fatal("fetch_traffic_history should be hidden from Commands() when MetricsProvider is nil")
		}
	}

	// Verify it still returns 503 on execution (registered as unavailable, not missing).
	resp := table.Execute(rpc.CommandRequest{Name: "fetch_traffic_history"})
	if resp.ErrorKind != rpc.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %v", resp.ErrorKind)
	}
}

func TestMetricsCommandVisibleWhenProviderSet(t *testing.T) {
	table := rpc.NewCommandTable()
	metrics := &mockMetricsProvider{
		snapshot: protocol.Frame{Type: "traffic_history", TrafficHistory: &protocol.TrafficHistoryFrame{}},
	}
	rpc.RegisterAllCommands(table, &mockNodeProvider{}, nil, nil, metrics, nil)

	found := false
	for _, cmd := range table.Commands() {
		if cmd.Name == "fetch_traffic_history" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("fetch_traffic_history should be visible in Commands() when MetricsProvider is set")
	}
}

func TestNetworkStatsCommand(t *testing.T) {
	node := &mockNodeProvider{
		handleFunc: func(frame protocol.Frame) protocol.Frame {
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
		},
	}
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
