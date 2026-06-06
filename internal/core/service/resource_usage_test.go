package service

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestResourceUsageFromFrame_Populated verifies the wire→service
// mapping copies every machine and human field.
func TestResourceUsageFromFrame_Populated(t *testing.T) {
	t.Parallel()

	// Every field seeded with a distinct value so a dropped/mismatched
	// mapping (the bug this guards: the embedded desktop path silently
	// truncating fields the public RPC carries) is caught.
	got := resourceUsageFromFrame(protocol.Frame{
		Type: "resource_usage",
		ResourceUsage: &protocol.ResourceUsageFrame{
			MemSysBytes:         62390272,
			MemSysHuman:         "59.50 MB",
			MemHeapAllocBytes:   41943040,
			MemHeapAllocHuman:   "40.00 MB",
			HeapInuseBytes:      45088768,
			HeapInuseHuman:      "43.00 MB",
			HeapIdleBytes:       12582912,
			HeapIdleHuman:       "12.00 MB",
			HeapReleasedBytes:   8388608,
			HeapReleasedHuman:   "8.00 MB",
			GCSysBytes:          4194304,
			GCSysHuman:          "4.00 MB",
			CgroupMemLimitBytes: 536870912,
			CgroupMemLimitHuman: "512.00 MB",
			CgroupMemUsageBytes: 157286400,
			CgroupMemUsageHuman: "150.00 MB",
			ConnectionCount:     12,
			UptimeSeconds:       192600,
			UptimeHuman:         "2.23 d",
			SampledAt:           "2026-06-06T05:15:47Z",
		},
	})
	if got == nil {
		t.Fatal("expected non-nil ResourceUsage for a populated frame")
	}
	want := &ResourceUsage{
		MemSysBytes:         62390272,
		MemSysHuman:         "59.50 MB",
		MemHeapAllocBytes:   41943040,
		MemHeapAllocHuman:   "40.00 MB",
		HeapInuseBytes:      45088768,
		HeapInuseHuman:      "43.00 MB",
		HeapIdleBytes:       12582912,
		HeapIdleHuman:       "12.00 MB",
		HeapReleasedBytes:   8388608,
		HeapReleasedHuman:   "8.00 MB",
		GCSysBytes:          4194304,
		GCSysHuman:          "4.00 MB",
		CgroupMemLimitBytes: 536870912,
		CgroupMemLimitHuman: "512.00 MB",
		CgroupMemUsageBytes: 157286400,
		CgroupMemUsageHuman: "150.00 MB",
		ConnectionCount:     12,
		UptimeSeconds:       192600,
		UptimeHuman:         "2.23 d",
		SampledAt:           "2026-06-06T05:15:47Z",
	}
	if *got != *want {
		t.Fatalf("resourceUsageFromFrame dropped/mismapped a field:\n got  %+v\n want %+v", *got, *want)
	}
}

// TestResourceUsageFromFrame_NilPayload verifies a frame without the
// payload (older node) maps to nil so the Info tab omits the rows.
func TestResourceUsageFromFrame_NilPayload(t *testing.T) {
	t.Parallel()

	if got := resourceUsageFromFrame(protocol.Frame{Type: "error"}); got != nil {
		t.Fatalf("expected nil for a frame without ResourceUsage payload, got %+v", got)
	}
}
