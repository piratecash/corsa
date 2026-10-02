package protocol

import (
	"strings"
	"testing"
	"time"
)

// TestTransportTrafficFrameRoundTrip pins that the transport block survives
// the frame codec with nanosecond timestamps intact — a rate is computed from
// two read_at values, and a codec that rounded them to seconds would turn two
// reads inside one second into a zero denominator.
func TestTransportTrafficFrameRoundTrip(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 10, 2, 9, 0, 0, 123456789, time.UTC)
	read := started.Add(90*time.Second + 987*time.Nanosecond)
	sent := Frame{
		Type: "network_stats",
		NetworkStats: &NetworkStatsFrame{
			TotalBytesSent:     5,
			TotalBytesReceived: 6,
			TotalTraffic:       11,
			Transport: &TransportTrafficFrame{
				StartedAt:     started,
				ReadAt:        read,
				BytesSent:     1 << 40,
				BytesReceived: 42,
			},
		},
	}

	line, err := MarshalFrameLine(sent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"transport"`, `"started_at"`, `"read_at"`, `"bytes_sent"`, `"bytes_received"`} {
		if !strings.Contains(line, key) {
			t.Fatalf("encoded frame lacks %s: %s", key, line)
		}
	}
	got, err := ParseFrameLine(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.NetworkStats == nil || got.NetworkStats.Transport == nil {
		t.Fatalf("decoded frame lost the transport block: %+v", got.NetworkStats)
	}
	transport := *got.NetworkStats.Transport
	want := *sent.NetworkStats.Transport
	if !transport.StartedAt.Equal(want.StartedAt) || !transport.ReadAt.Equal(want.ReadAt) ||
		transport.BytesSent != want.BytesSent || transport.BytesReceived != want.BytesReceived {
		t.Fatalf("round trip = %+v, want %+v", transport, want)
	}
	if got.NetworkStats.TotalBytesSent != 5 || got.NetworkStats.TotalBytesReceived != 6 {
		t.Fatalf("existing totals changed by the round trip: %+v", got.NetworkStats)
	}
}

// TestNetworkStatsFrameOmitsAbsentTransport pins backward compatibility: a
// network_stats answer that does not carry the block encodes exactly as
// before, so existing decoders see no new key.
func TestNetworkStatsFrameOmitsAbsentTransport(t *testing.T) {
	t.Parallel()

	line, err := MarshalFrameLine(Frame{Type: "network_stats", NetworkStats: &NetworkStatsFrame{}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(line, `"transport"`) {
		t.Fatalf("absent transport block was encoded: %s", line)
	}
}
