package metrics

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// mockTrafficSource simulates node.Service for testing.
type mockTrafficSource struct {
	sent int64
	recv int64
}

func (m *mockTrafficSource) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	if frame.Type == "fetch_network_stats" {
		return protocol.Frame{
			Type: "network_stats",
			NetworkStats: &protocol.NetworkStatsFrame{
				TotalBytesSent:     m.sent,
				TotalBytesReceived: m.recv,
				TotalTraffic:       m.sent + m.recv,
			},
		}
	}
	return protocol.Frame{Type: "error", Error: "unknown"}
}

func TestCollectorCollectsSamples(t *testing.T) {
	source := &mockTrafficSource{sent: 100, recv: 200}
	collector := NewCollector(source)

	// Drive collection directly instead of sleeping for real ticks.
	collector.collectTrafficSample()
	collector.collectTrafficSample()
	collector.collectTrafficSample()

	// Change traffic mid-collection.
	source.sent = 500
	source.recv = 800

	collector.collectTrafficSample()

	frame := collector.TrafficSnapshot()
	if frame.Type != "traffic_history" {
		t.Fatalf("expected type 'traffic_history', got %q", frame.Type)
	}
	if frame.TrafficHistory == nil {
		t.Fatal("TrafficHistory is nil")
	}
	if frame.TrafficHistory.Count < 3 {
		t.Errorf("expected at least 3 samples, got %d", frame.TrafficHistory.Count)
	}
	if frame.TrafficHistory.IntervalSeconds != 1 {
		t.Errorf("expected IntervalSeconds=1, got %d", frame.TrafficHistory.IntervalSeconds)
	}

	samples := frame.TrafficHistory.Samples
	// First sample should have delta=0 (no previous).
	if samples[0].BytesSentPS != 0 {
		t.Errorf("expected first delta=0, got %d", samples[0].BytesSentPS)
	}

	// Last sample should reflect the updated source values.
	lastSample := samples[len(samples)-1]
	if lastSample.TotalSent != 500 {
		t.Errorf("expected last TotalSent=500, got %d", lastSample.TotalSent)
	}
}

func TestCollectorHandlesNilNetworkStats(t *testing.T) {
	nilSource := &nilStatsSource{}
	collector := NewCollector(nilSource)

	// Drive collection directly — nil NetworkStats should be skipped.
	collector.collectTrafficSample()
	collector.collectTrafficSample()
	collector.collectTrafficSample()

	frame := collector.TrafficSnapshot()
	if frame.TrafficHistory.Count != 0 {
		t.Errorf("expected 0 samples when NetworkStats is nil, got %d", frame.TrafficHistory.Count)
	}
}

type nilStatsSource struct{}

func (n *nilStatsSource) HandleLocalFrame(frame protocol.Frame) protocol.Frame {
	return protocol.Frame{Type: "network_stats"} // NetworkStats is nil
}

func TestCollectorTrafficSnapshotFrame(t *testing.T) {
	source := &mockTrafficSource{sent: 1000, recv: 2000}
	collector := NewCollector(source)

	// Manually record data via the traffic history buffer.
	collector.traffic.Record(1000, 2000)
	collector.traffic.Record(1500, 2800)

	frame := collector.TrafficSnapshot()
	if frame.TrafficHistory.Count != 2 {
		t.Fatalf("expected 2 samples, got %d", frame.TrafficHistory.Count)
	}
	if frame.TrafficHistory.Capacity != trafficHistoryCapacity {
		t.Errorf("expected capacity=%d, got %d", trafficHistoryCapacity, frame.TrafficHistory.Capacity)
	}

	// Verify second sample has correct deltas.
	s := frame.TrafficHistory.Samples[1]
	if s.BytesSentPS != 500 {
		t.Errorf("expected BytesSentPS=500, got %d", s.BytesSentPS)
	}
	if s.BytesRecvPS != 800 {
		t.Errorf("expected BytesRecvPS=800, got %d", s.BytesRecvPS)
	}
}
