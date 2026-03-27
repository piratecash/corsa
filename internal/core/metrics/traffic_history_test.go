package metrics

import (
	"testing"
)

func TestTrafficHistoryRecord(t *testing.T) {
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
	// first sample has no previous → delta = 0
	if samples[0].BytesSentPS != 0 {
		t.Errorf("expected BytesSentPS=0 for first sample, got %d", samples[0].BytesSentPS)
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
