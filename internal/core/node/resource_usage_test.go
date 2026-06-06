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

	// Extended memory fields: human strings must stay consistent with
	// their byte counters. HeapInuse is always non-zero for a live
	// process; the rest may legitimately be zero, so we only assert the
	// human/byte consistency for them.
	if ru.HeapInuseBytes == 0 {
		t.Fatal("HeapInuseBytes should be non-zero for a live process")
	}
	for _, c := range []struct {
		name  string
		bytes uint64
		human string
	}{
		{"HeapInuse", ru.HeapInuseBytes, ru.HeapInuseHuman},
		{"HeapIdle", ru.HeapIdleBytes, ru.HeapIdleHuman},
		{"HeapReleased", ru.HeapReleasedBytes, ru.HeapReleasedHuman},
		{"GCSys", ru.GCSysBytes, ru.GCSysHuman},
		{"CgroupMemUsage", ru.CgroupMemUsageBytes, ru.CgroupMemUsageHuman},
	} {
		if c.human != formatBytes(c.bytes) {
			t.Errorf("%s human %q inconsistent with bytes %d", c.name, c.human, c.bytes)
		}
	}

	// Cgroup limit: "unlimited" when zero (the common case off-cgroup),
	// otherwise a formatted size matching the byte count.
	if ru.CgroupMemLimitBytes == 0 {
		if ru.CgroupMemLimitHuman != "unlimited" {
			t.Errorf("CgroupMemLimitHuman = %q, want \"unlimited\" when limit is 0", ru.CgroupMemLimitHuman)
		}
	} else if ru.CgroupMemLimitHuman != formatBytes(ru.CgroupMemLimitBytes) {
		t.Errorf("CgroupMemLimitHuman %q inconsistent with bytes %d", ru.CgroupMemLimitHuman, ru.CgroupMemLimitBytes)
	}

	// Connection count: zero on this bare fixture (no conns map), must
	// not be negative.
	if ru.ConnectionCount != 0 {
		t.Errorf("ConnectionCount = %d, want 0 on a bare fixture", ru.ConnectionCount)
	}
}

// TestResourceUsageFrame_CarriesExtendedFields guards the embedded
// desktop path: the fetch_resource_usage wire frame must carry the FULL
// field set, not just the legacy mem/uptime subset (the bug being that
// the frame DTO silently truncated heap_*/gc_sys/cgroup_*/connection_count
// the public RPC exposes). The human strings are checked against their
// byte counters — a dropped field would leave a zero counter with a "0 B"
// mismatch (or an empty human).
func TestResourceUsageFrame_CarriesExtendedFields(t *testing.T) {
	t.Parallel()

	svc := &Service{startedAt: time.Now().Add(-90 * time.Minute)}
	f := svc.resourceUsageFrame().ResourceUsage
	if f == nil {
		t.Fatal("resourceUsageFrame must populate the ResourceUsage payload")
	}

	for _, c := range []struct {
		name  string
		bytes uint64
		human string
	}{
		{"MemSys", f.MemSysBytes, f.MemSysHuman},
		{"MemHeapAlloc", f.MemHeapAllocBytes, f.MemHeapAllocHuman},
		{"HeapInuse", f.HeapInuseBytes, f.HeapInuseHuman},
		{"HeapIdle", f.HeapIdleBytes, f.HeapIdleHuman},
		{"HeapReleased", f.HeapReleasedBytes, f.HeapReleasedHuman},
		{"GCSys", f.GCSysBytes, f.GCSysHuman},
		{"CgroupMemUsage", f.CgroupMemUsageBytes, f.CgroupMemUsageHuman},
	} {
		if c.human != formatBytes(c.bytes) {
			t.Errorf("frame %s human %q inconsistent with bytes %d (field dropped?)", c.name, c.human, c.bytes)
		}
	}
	if f.CgroupMemLimitBytes == 0 && f.CgroupMemLimitHuman != "unlimited" {
		t.Errorf("frame CgroupMemLimitHuman = %q, want \"unlimited\" at zero limit", f.CgroupMemLimitHuman)
	}
	if f.UptimeHuman != "1.50 h" {
		t.Errorf("frame UptimeHuman = %q, want %q", f.UptimeHuman, "1.50 h")
	}
	if f.SampledAt == "" {
		t.Error("frame SampledAt must be set")
	}
}
