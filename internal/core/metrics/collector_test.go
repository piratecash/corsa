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
	// Without Seed, the first sample reports delta == totals because the
	// baseline is zero and the collector is observing from first byte. This
	// is intentional — the previous skip-on-first behavior silently dropped
	// real bootstrap traffic. Callers attaching to an already-running system
	// must Seed before CollectTrafficSample to suppress this initial spike.
	if samples[0].BytesSentPS != 100 {
		t.Errorf("expected first delta=100 (totals without Seed), got %d", samples[0].BytesSentPS)
	}
	if samples[0].BytesRecvPS != 200 {
		t.Errorf("expected first BytesRecvPS=200, got %d", samples[0].BytesRecvPS)
	}

	// Last sample should reflect the updated source values.
	lastSample := samples[len(samples)-1]
	if lastSample.TotalSent != 500 {
		t.Errorf("expected last TotalSent=500, got %d", lastSample.TotalSent)
	}
}

func TestCollectorSeedSuppressesFirstSpike(t *testing.T) {
	// Collector attaches to a running source whose counters are already
	// non-zero. Seed captures the current totals as baseline; subsequent
	// Record produces a real delta rather than dumping the pre-attach
	// cumulative as a single-second spike.
	sent := int64(50_000)
	recv := int64(30_000)
	source := newMutableMockTrafficSource(t, &sent, &recv)
	collector := metrics.NewCollector(source)

	collector.Seed()

	// No traffic flowed between Seed and the first Record.
	collector.CollectTrafficSample()

	// New traffic flows between first and second Record.
	sent = 50_400
	recv = 30_150
	collector.CollectTrafficSample()

	frame := collector.TrafficSnapshot()
	samples := frame.TrafficHistory.Samples
	if len(samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samples))
	}

	// First sample after Seed: no new traffic → delta 0/0.
	if samples[0].BytesSentPS != 0 || samples[0].BytesRecvPS != 0 {
		t.Errorf("expected first delta=0/0 after Seed, got sent=%d recv=%d",
			samples[0].BytesSentPS, samples[0].BytesRecvPS)
	}

	// Second sample reflects the 400/150 that flowed between ticks.
	if samples[1].BytesSentPS != 400 {
		t.Errorf("expected second BytesSentPS=400, got %d", samples[1].BytesSentPS)
	}
	if samples[1].BytesRecvPS != 150 {
		t.Errorf("expected second BytesRecvPS=150, got %d", samples[1].BytesRecvPS)
	}
}

func TestCollectorSeedWithNilStatsLeavesBaselineAtZero(t *testing.T) {
	// Seed tolerates a source that returns nil NetworkStats (e.g. collector
	// wired up before the source is ready) — baseline stays at zero. The
	// first Record then reports delta==totals, which is safe because zero
	// baseline matches the "no pre-existing traffic" assumption.
	source := newNilStatsSource(t)
	collector := metrics.NewCollector(source)

	collector.Seed() // must not panic and must not alter the empty buffer

	frame := collector.TrafficSnapshot()
	if frame.TrafficHistory.Count != 0 {
		t.Errorf("Seed must not append a sample, got %d", frame.TrafficHistory.Count)
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
