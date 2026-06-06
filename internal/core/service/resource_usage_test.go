package service

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestResourceUsageFromFrame_Populated verifies the wire→service
// mapping copies every machine and human field.
func TestResourceUsageFromFrame_Populated(t *testing.T) {
	t.Parallel()

	got := resourceUsageFromFrame(protocol.Frame{
		Type: "resource_usage",
		ResourceUsage: &protocol.ResourceUsageFrame{
			MemSysBytes:       62390272,
			MemSysHuman:       "59.50 MB",
			MemHeapAllocBytes: 41943040,
			MemHeapAllocHuman: "40.00 MB",
			UptimeSeconds:     192600,
			UptimeHuman:       "2.23 d",
			SampledAt:         "2026-06-06T05:15:47Z",
		},
	})
	if got == nil {
		t.Fatal("expected non-nil ResourceUsage for a populated frame")
	}
	if got.MemSysBytes != 62390272 || got.MemSysHuman != "59.50 MB" {
		t.Fatalf("mem sys mismatch: %+v", got)
	}
	if got.MemHeapAllocBytes != 41943040 || got.MemHeapAllocHuman != "40.00 MB" {
		t.Fatalf("mem heap mismatch: %+v", got)
	}
	if got.UptimeSeconds != 192600 || got.UptimeHuman != "2.23 d" {
		t.Fatalf("uptime mismatch: %+v", got)
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
