package node

import (
	"fmt"
	"runtime"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ResourceUsage returns a point-in-time snapshot of the process's
// memory footprint (via the standard-library runtime.MemStats) and
// uptime since this node's startedAt. Both machine-readable integers
// and human-formatted strings are populated. Surfaced by the
// getResourceUsage RPC command and the desktop console Info tab.
//
// runtime.ReadMemStats triggers a brief stop-the-world; at a per-RPC
// / once-per-render call rate that cost is negligible. Takes no
// domain mutex — startedAt is immutable after construction and
// MemStats is global runtime state.
func (s *Service) ResourceUsage() domain.ResourceUsage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	now := time.Now().UTC()
	uptime := now.Sub(s.startedAt)
	if uptime < 0 {
		// Defensive: clock moved backwards between construction and
		// call — clamp so the reported uptime stays non-negative.
		uptime = 0
	}

	return domain.ResourceUsage{
		MemSysBytes:       m.Sys,
		MemSysHuman:       formatBytes(m.Sys),
		MemHeapAllocBytes: m.HeapAlloc,
		MemHeapAllocHuman: formatBytes(m.HeapAlloc),
		UptimeSeconds:     int64(uptime / time.Second),
		UptimeHuman:       formatUptime(uptime),
		SampledAt:         now,
	}
}

// resourceUsageFrame wraps ResourceUsage() into the wire frame
// returned by the fetch_resource_usage local RPC command. Desktop's
// prober consumes this through HandleLocalFrame (the same local-frame
// path as fetch_aggregate_status), keeping the Info-tab data flow
// uniform rather than reaching into the Service directly.
func (s *Service) resourceUsageFrame() protocol.Frame {
	ru := s.ResourceUsage()
	return protocol.Frame{
		Type: "resource_usage",
		ResourceUsage: &protocol.ResourceUsageFrame{
			MemSysBytes:       ru.MemSysBytes,
			MemSysHuman:       ru.MemSysHuman,
			MemHeapAllocBytes: ru.MemHeapAllocBytes,
			MemHeapAllocHuman: ru.MemHeapAllocHuman,
			UptimeSeconds:     ru.UptimeSeconds,
			UptimeHuman:       ru.UptimeHuman,
			SampledAt:         ru.SampledAt.Format(time.RFC3339Nano),
		},
	}
}

// formatBytes renders a byte count with the largest unit that keeps
// the integer part below 1024, picking B / KB / MB / GB / TB / PB.
// Two decimals for KB and above, none for raw bytes.
func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	value := float64(b)
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	idx := -1
	for value >= unit && idx < len(units)-1 {
		value /= unit
		idx++
	}
	return fmt.Sprintf("%.2f %s", value, units[idx])
}

// formatUptime renders an elapsed duration in the largest of three
// tiers — seconds (< 1 h), hours (< 1 day), or days. Two decimals for
// hours and days, whole numbers for seconds.
func formatUptime(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%d s", int64(d/time.Second))
	case d < 24*time.Hour:
		return fmt.Sprintf("%.2f h", d.Hours())
	default:
		return fmt.Sprintf("%.2f d", d.Hours()/24)
	}
}
