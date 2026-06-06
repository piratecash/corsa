package node

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{5*1024*1024 + 512*1024, "5.50 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{6227702087, "5.80 GB"}, // ~ the 5.8 GB RSS from the field incident
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
	}
	for _, c := range cases {
		if got := formatBytes(c.in); got != c.want {
			t.Errorf("formatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatUptime(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   time.Duration
		want string
	}{
		{-time.Second, "0 s"}, // defensive clamp on backwards clock
		{0, "0 s"},
		{45 * time.Second, "45 s"},
		{59 * time.Minute, "3540 s"}, // still under 1 h → seconds tier
		{90 * time.Minute, "1.50 h"}, // hours tier
		{23 * time.Hour, "23.00 h"},  // last hour before days tier
		{36 * time.Hour, "1.50 d"},   // days tier
		{10 * 24 * time.Hour, "10.00 d"},
	}
	for _, c := range cases {
		if got := formatUptime(c.in); got != c.want {
			t.Errorf("formatUptime(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestResourceUsage_PopulatesBothRepresentations verifies the snapshot
// carries consistent machine + human fields and a non-negative uptime.
func TestResourceUsage_PopulatesBothRepresentations(t *testing.T) {
	t.Parallel()

	svc := &Service{startedAt: time.Now().Add(-90 * time.Minute)}
	ru := svc.ResourceUsage()

	if ru.MemSysBytes == 0 {
		t.Fatal("MemSysBytes should be non-zero for a live process")
	}
	if ru.MemSysHuman != formatBytes(ru.MemSysBytes) {
		t.Fatalf("MemSysHuman %q inconsistent with bytes %d", ru.MemSysHuman, ru.MemSysBytes)
	}
	if ru.MemHeapAllocHuman != formatBytes(ru.MemHeapAllocBytes) {
		t.Fatalf("MemHeapAllocHuman %q inconsistent with bytes %d", ru.MemHeapAllocHuman, ru.MemHeapAllocBytes)
	}
	if ru.UptimeSeconds < 5300 || ru.UptimeSeconds > 5500 {
		t.Fatalf("UptimeSeconds = %d, want ~5400 (90 min)", ru.UptimeSeconds)
	}
	if ru.UptimeHuman != "1.50 h" {
		t.Fatalf("UptimeHuman = %q, want %q", ru.UptimeHuman, "1.50 h")
	}
	if ru.SampledAt.IsZero() {
		t.Fatal("SampledAt must be set")
	}
}
