package node

import (
	"runtime/metrics"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type runtimeMetricSpec struct {
	name   domain.RuntimeMetricName
	kind   metrics.ValueKind
	assign func(*domain.RuntimeMetrics, metrics.Value)
}

func runtimeCountMetric(name domain.RuntimeMetricName, field func(*domain.RuntimeMetrics) **uint64) runtimeMetricSpec {
	return runtimeMetricSpec{name: name, kind: metrics.KindUint64, assign: func(sample *domain.RuntimeMetrics, value metrics.Value) {
		count := value.Uint64()
		*field(sample) = &count
	}}
}

func runtimeCPUMetric(name domain.RuntimeMetricName, field func(*domain.RuntimeMetrics) **float64) runtimeMetricSpec {
	return runtimeMetricSpec{name: name, kind: metrics.KindFloat64, assign: func(sample *domain.RuntimeMetrics, value metrics.Value) {
		seconds := value.Float64()
		*field(sample) = &seconds
	}}
}

var runtimeMetricSpecs = []runtimeMetricSpec{
	runtimeCountMetric("/gc/heap/live:bytes", func(s *domain.RuntimeMetrics) **uint64 { return &s.HeapLiveBytes }),
	runtimeCountMetric("/memory/classes/heap/objects:bytes", func(s *domain.RuntimeMetrics) **uint64 { return &s.HeapObjectsBytes }),
	runtimeCountMetric("/gc/heap/allocs:bytes", func(s *domain.RuntimeMetrics) **uint64 { return &s.HeapAllocsBytes }),
	runtimeCountMetric("/memory/classes/total:bytes", func(s *domain.RuntimeMetrics) **uint64 { return &s.GoMemoryBytes }),
	runtimeCountMetric("/gc/cycles/total:gc-cycles", func(s *domain.RuntimeMetrics) **uint64 { return &s.GCCycles }),
	runtimeCountMetric("/sched/goroutines:goroutines", func(s *domain.RuntimeMetrics) **uint64 { return &s.Goroutines }),
	runtimeCountMetric("/sched/gomaxprocs:threads", func(s *domain.RuntimeMetrics) **uint64 { return &s.GOMAXPROCS }),
	runtimeCPUMetric("/cpu/classes/total:cpu-seconds", func(s *domain.RuntimeMetrics) **float64 { return &s.CPUTotalSeconds }),
	runtimeCPUMetric("/cpu/classes/user:cpu-seconds", func(s *domain.RuntimeMetrics) **float64 { return &s.CPUUserSeconds }),
	runtimeCPUMetric("/cpu/classes/gc/total:cpu-seconds", func(s *domain.RuntimeMetrics) **float64 { return &s.CPUGCSeconds }),
	runtimeCPUMetric("/cpu/classes/idle:cpu-seconds", func(s *domain.RuntimeMetrics) **float64 { return &s.CPUIdleSeconds }),
}

// RuntimeMetrics samples the Go process without domain locks or ReadMemStats.
// It does not force GC: a missing metric is explicit, while a supported
// counter which has not yet advanced is legitimately zero.
func (s *Service) RuntimeMetrics() domain.RuntimeMetrics {
	sample := readRuntimeMetrics(s.startedAt, runtimeMetricSpecs, metrics.Read)
	sample.ReadAt = time.Now().UTC()
	return sample
}

func readRuntimeMetrics(startedAt time.Time, specs []runtimeMetricSpec, read func([]metrics.Sample)) domain.RuntimeMetrics {
	sample := domain.RuntimeMetrics{Version: 1, StartedAt: startedAt, Unsupported: []domain.RuntimeMetricName{}}
	values := make([]metrics.Sample, len(specs))
	for i, spec := range specs {
		values[i].Name = string(spec.name)
	}
	read(values)
	for i, spec := range specs {
		if values[i].Value.Kind() != spec.kind {
			sample.Unsupported = append(sample.Unsupported, spec.name)
			continue
		}
		spec.assign(&sample, values[i].Value)
	}
	return sample
}
