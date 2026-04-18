package metrics_test

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/metrics"
	metricsmocks "github.com/piratecash/corsa/internal/core/metrics/mocks"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/stretchr/testify/mock"
)

// newMutableMockTrafficSource creates a mock that reads sent/recv through
// pointers, allowing the caller to change values between calls.
func newMutableMockTrafficSource(t *testing.T, sent, recv *int64) *metricsmocks.MockTrafficSource {
	t.Helper()
	m := metricsmocks.NewMockTrafficSource(t)
	m.EXPECT().HandleLocalFrame(mock.Anything).RunAndReturn(func(frame protocol.Frame) protocol.Frame {
		if frame.Type == "fetch_network_stats" {
			return protocol.Frame{
				Type: "network_stats",
				NetworkStats: &protocol.NetworkStatsFrame{
					TotalBytesSent:     *sent,
					TotalBytesReceived: *recv,
					TotalTraffic:       *sent + *recv,
				},
			}
		}
		return protocol.Frame{Type: "error", Error: "unknown"}
	}).Maybe()
	return m
}

// newNilStatsSource creates a mock that returns a frame with nil NetworkStats.
func newNilStatsSource(t *testing.T) *metricsmocks.MockTrafficSource {
	t.Helper()
	m := metricsmocks.NewMockTrafficSource(t)
	m.EXPECT().HandleLocalFrame(mock.Anything).RunAndReturn(func(frame protocol.Frame) protocol.Frame {
		return protocol.Frame{Type: "network_stats"}
	}).Maybe()
	return m
}

func TestCollectorCollectsSamples(t *testing.T) {
	sent := int64(100)
	recv := int64(200)
	source := newMutableMockTrafficSource(t, &sent, &recv)
	collector := metrics.NewCollector(source)

	// Drive collection directly instead of sleeping for real ticks.
	collector.CollectTrafficSample()
	collector.CollectTrafficSample()
	collector.CollectTrafficSample()

	// Change traffic mid-collection.
	sent = 500
	recv = 800

	collector.CollectTrafficSample()

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
	source := newNilStatsSource(t)
	collector := metrics.NewCollector(source)

	// Drive collection directly — nil NetworkStats should be skipped.
	collector.CollectTrafficSample()
	collector.CollectTrafficSample()
	collector.CollectTrafficSample()

	frame := collector.TrafficSnapshot()
	if frame.TrafficHistory.Count != 0 {
		t.Errorf("expected 0 samples when NetworkStats is nil, got %d", frame.TrafficHistory.Count)
	}
}

func TestCollectorTrafficSnapshotFrame(t *testing.T) {
	sent := int64(1000)
	recv := int64(2000)
	source := newMutableMockTrafficSource(t, &sent, &recv)
	collector := metrics.NewCollector(source)

	// Manually record data via the traffic history buffer.
	collector.CollectorTraffic().Record(1000, 2000)
	collector.CollectorTraffic().Record(1500, 2800)

	frame := collector.TrafficSnapshot()
	if frame.TrafficHistory.Count != 2 {
		t.Fatalf("expected 2 samples, got %d", frame.TrafficHistory.Count)
	}
	if frame.TrafficHistory.Capacity != metrics.TrafficHistoryCapacityForTest {
		t.Errorf("expected capacity=%d, got %d", metrics.TrafficHistoryCapacityForTest, frame.TrafficHistory.Capacity)
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
