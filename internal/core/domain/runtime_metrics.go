package domain

import "time"

// RuntimeMetricName is a Go runtime/metrics name, including its unit.
type RuntimeMetricName string

// RuntimeMetrics describes one process, not a node's share of a shared process.
// Nil means unsupported, never a measured zero. CPU figures are runtime
// estimates updated by GC; OS rusage remains the CPU accounting source.
type RuntimeMetrics struct {
	Version          int                 `json:"version"`
	StartedAt        time.Time           `json:"started_at"`
	ReadAt           time.Time           `json:"read_at"`
	HeapLiveBytes    *uint64             `json:"heap_live_bytes,omitempty"`
	HeapObjectsBytes *uint64             `json:"heap_objects_bytes,omitempty"`
	HeapAllocsBytes  *uint64             `json:"heap_allocs_bytes,omitempty"`
	GoMemoryBytes    *uint64             `json:"go_memory_bytes,omitempty"`
	GCCycles         *uint64             `json:"gc_cycles,omitempty"`
	Goroutines       *uint64             `json:"goroutines,omitempty"`
	GOMAXPROCS       *uint64             `json:"gomaxprocs,omitempty"`
	CPUTotalSeconds  *float64            `json:"cpu_total_seconds,omitempty"`
	CPUUserSeconds   *float64            `json:"cpu_user_seconds,omitempty"`
	CPUGCSeconds     *float64            `json:"cpu_gc_seconds,omitempty"`
	CPUIdleSeconds   *float64            `json:"cpu_idle_seconds,omitempty"`
	Unsupported      []RuntimeMetricName `json:"unsupported"`
}
