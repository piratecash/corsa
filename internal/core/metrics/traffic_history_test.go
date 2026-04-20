package metrics

import (
	"testing"
)

func TestTrafficHistoryRecordFromZeroBaseline(t *testing.T) {
	// Collector started at the same time as the producer (totals==0 when
	// buffer was created). First Record reports delta = totals because the
	// baseline is zero and the caller is observing traffic from the very
	// first byte.
	th := NewTrafficHistory()

	th.Record(100, 200)

	if th.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", th.Len())
	}

	samples := th.Snapshot()
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if samples[0].TotalSent != 100 {
		t.Errorf("expected TotalSent=100, got %d", samples[0].TotalSent)
	}
	if samples[0].TotalReceived != 200 {
		t.Errorf("expected TotalReceived=200, got %d", samples[0].TotalReceived)
	}
	if samples[0].BytesSentPS != 100 {
		t.Errorf("expected BytesSentPS=100 (no Seed → delta is totals), got %d", samples[0].BytesSentPS)
	}
	if samples[0].BytesRecvPS != 200 {
		t.Errorf("expected BytesRecvPS=200, got %d", samples[0].BytesRecvPS)
	}
}

func TestTrafficHistorySeedSuppressesFirstSpike(t *testing.T) {
	// Collector attaches to an already-running system whose counters are
	// non-zero. Seed captures the current totals as baseline so the next
	// Record reports only genuine new traffic, not the entire pre-attach
	// accumulation as a single-second spike.
	th := NewTrafficHistory()
	th.Seed(10_000, 20_000)

	th.Record(10_100, 20_200)

	samples := th.Snapshot()
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if samples[0].BytesSentPS != 100 {
		t.Errorf("expected BytesSentPS=100 after Seed, got %d", samples[0].BytesSentPS)
	}
	if samples[0].BytesRecvPS != 200 {
		t.Errorf("expected BytesRecvPS=200 after Seed, got %d", samples[0].BytesRecvPS)
	}
	if samples[0].TotalSent != 10_100 {
		t.Errorf("expected TotalSent=10_100, got %d", samples[0].TotalSent)
	}
}

func TestTrafficHistorySeedOverwritesPreviousBaseline(t *testing.T) {
	// Calling Seed after Record overwrites the internal baseline (delta
	// reference) — the previously recorded sample stays in the buffer but
	// the next Record computes its delta against the new Seed totals.
	// This documents the API: Seed is intended for startup, not mid-stream
	// re-baselining, but if a caller uses it mid-stream the behavior is
	// predictable rather than silently corrupt.
	th := NewTrafficHistory()
	th.Record(100, 200)
	th.Seed(500, 1000)
	th.Record(550, 1100)

	samples := th.Snapshot()
	if len(samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samples))
	}
	if samples[1].BytesSentPS != 50 {
		t.Errorf("expected BytesSentPS=50 after Seed re-baseline, got %d", samples[1].BytesSentPS)
	}
	if samples[1].BytesRecvPS != 100 {
		t.Errorf("expected BytesRecvPS=100 after Seed re-baseline, got %d", samples[1].BytesRecvPS)
	}
}

func TestTrafficHistoryDeltas(t *testing.T) {
	th := NewTrafficHistory()

	th.Record(100, 200)
	th.Record(350, 500)

	samples := th.Snapshot()
	if len(samples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(samples))
	}
	if samples[1].BytesSentPS != 250 {
		t.Errorf("expected BytesSentPS=250, got %d", samples[1].BytesSentPS)
	}
	if samples[1].BytesRecvPS != 300 {
		t.Errorf("expected BytesRecvPS=300, got %d", samples[1].BytesRecvPS)
	}
}

func TestTrafficHistoryNegativeDelta(t *testing.T) {
	th := NewTrafficHistory()

	th.Record(100, 200)
	// simulate counter reset (should clamp delta to 0)
	th.Record(50, 100)

	samples := th.Snapshot()
	if samples[1].BytesSentPS != 0 {
		t.Errorf("expected BytesSentPS=0 on counter reset, got %d", samples[1].BytesSentPS)
	}
	if samples[1].BytesRecvPS != 0 {
		t.Errorf("expected BytesRecvPS=0 on counter reset, got %d", samples[1].BytesRecvPS)
	}
}

func TestTrafficHistoryRingWrap(t *testing.T) {
	th := NewTrafficHistory()

	// fill beyond capacity
	for i := 0; i < trafficHistoryCapacity+10; i++ {
		th.Record(int64(i*10), int64(i*20))
	}

	if th.Len() != trafficHistoryCapacity {
		t.Fatalf("expected Len=%d, got %d", trafficHistoryCapacity, th.Len())
	}

	samples := th.Snapshot()
	if len(samples) != trafficHistoryCapacity {
		t.Fatalf("expected %d samples, got %d", trafficHistoryCapacity, len(samples))
	}

	// oldest sample should be index 10 (first 10 were evicted)
	if samples[0].TotalSent != 100 {
		t.Errorf("expected oldest TotalSent=100, got %d", samples[0].TotalSent)
	}

	// newest should be the last written value
	last := samples[len(samples)-1]
	expectedSent := int64((trafficHistoryCapacity + 9) * 10)
	if last.TotalSent != expectedSent {
		t.Errorf("expected newest TotalSent=%d, got %d", expectedSent, last.TotalSent)
	}

	// verify chronological order
	for i := 1; i < len(samples); i++ {
		if samples[i].TotalSent < samples[i-1].TotalSent {
			t.Fatalf("samples not in chronological order at index %d: %d < %d",
				i, samples[i].TotalSent, samples[i-1].TotalSent)
		}
	}
}

func TestTrafficHistorySnapshotIsCopy(t *testing.T) {
	th := NewTrafficHistory()
	th.Record(10, 20)

	snap1 := th.Snapshot()
	th.Record(30, 40)
	snap2 := th.Snapshot()

	if len(snap1) != 1 {
		t.Errorf("snap1 should have 1 sample, got %d", len(snap1))
	}
	if len(snap2) != 2 {
		t.Errorf("snap2 should have 2 samples, got %d", len(snap2))
	}
	// mutating snap1 should not affect snap2
	snap1[0].TotalSent = 999
	if snap2[0].TotalSent == 999 {
		t.Error("Snapshot returned aliased slice, expected independent copy")
	}
}

func TestTrafficHistoryCapacity(t *testing.T) {
	th := NewTrafficHistory()
	if th.Capacity() != trafficHistoryCapacity {
		t.Errorf("expected Capacity=%d, got %d", trafficHistoryCapacity, th.Capacity())
	}
}

func TestTrafficHistoryEmptySnapshot(t *testing.T) {
	th := NewTrafficHistory()

	samples := th.Snapshot()
	if len(samples) != 0 {
		t.Errorf("expected empty snapshot, got %d samples", len(samples))
	}
	if th.Len() != 0 {
		t.Errorf("expected Len=0, got %d", th.Len())
	}
}
