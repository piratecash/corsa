package node

import (
	"reflect"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestRuntimeMetricsNamesKindsAndAvailability(t *testing.T) {
	descriptions := map[string]metrics.Description{}
	for _, description := range metrics.All() {
		descriptions[description.Name] = description
	}
	if len(runtimeMetricSpecs) != 11 {
		t.Fatalf("metric families=%d,want11", len(runtimeMetricSpecs))
	}
	for _, spec := range runtimeMetricSpecs {
		description, ok := descriptions[string(spec.name)]
		if !ok || description.Kind != spec.kind {
			t.Fatalf("unsupported specification %s kind=%v", spec.name, spec.kind)
		}
	}
	started := time.Now().UTC().Add(-time.Minute)
	svc := &Service{startedAt: started}
	sample := svc.RuntimeMetrics()
	if sample.Version != 1 || !sample.StartedAt.Equal(started) || sample.ReadAt.Before(started) || len(sample.Unsupported) != 0 {
		t.Fatalf("runtime availability: %+v", sample)
	}
	if sample.HeapObjectsBytes == nil || *sample.HeapObjectsBytes == 0 || sample.Goroutines == nil || sample.GOMAXPROCS == nil || sample.CPUIdleSeconds == nil {
		t.Fatalf("supported metric missing: %+v", sample)
	}
}

func TestRuntimeMetricsUnsupportedAndWrongKindAreNotZero(t *testing.T) {
	unknown := runtimeCountMetric("/not-a-runtime-metric:bytes", func(s *domain.RuntimeMetrics) **uint64 { return &s.HeapLiveBytes })
	wrong := runtimeMetricSpecs[0]
	wrong.kind = metrics.KindFloat64
	sample := readRuntimeMetrics(time.Time{}, []runtimeMetricSpec{unknown, wrong}, metrics.Read)
	if sample.HeapLiveBytes != nil || !reflect.DeepEqual(sample.Unsupported, []domain.RuntimeMetricName{unknown.name, wrong.name}) {
		t.Fatalf("unsupported value became measured zero: %+v", sample)
	}
}

func TestRuntimeMetricsCountersNeverDecreaseAndGoroutinesAreObserved(t *testing.T) {
	svc := &Service{}
	before := svc.RuntimeMetrics()
	quit := make(chan struct{})
	ready := make(chan struct{}, 100)
	done := make(chan struct{}, 100)
	defer func() {
		close(quit)
		for range 100 {
			<-done
		}
	}()
	for range 100 {
		go func() { ready <- struct{}{}; <-quit; done <- struct{}{} }()
	}
	for range 100 {
		<-ready
	}
	after := svc.RuntimeMetrics()
	if *after.Goroutines < *before.Goroutines+100 {
		t.Fatalf("parked goroutines omitted: %d -> %d", *before.Goroutines, *after.Goroutines)
	}
	if *after.HeapAllocsBytes < *before.HeapAllocsBytes || *after.GCCycles < *before.GCCycles || *after.CPUTotalSeconds < *before.CPUTotalSeconds {
		t.Fatal("runtime counters decreased")
	}
}
