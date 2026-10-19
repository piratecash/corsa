package loadstand

import (
	"math"
	"testing"
	"time"
)

func TestOwnedKernelUsageUnitsAndOverflowRefusal(t *testing.T) {
	for _, unit := range []uint64{1, 1024} {
		got := kernelProcessUsage(3*time.Millisecond, 4*time.Millisecond, 2, unit)
		if got.State != ProcessMetricMeasured || got.Value == nil || got.Value.UserNanoseconds != 3_000_000 || got.Value.SystemNanoseconds != 4_000_000 || got.Value.HighWaterRSS != ProcessBytes(2*unit) {
			t.Fatal("kernel unit conversion changed", got)
		}
	}
	for _, v := range []struct {
		user, system time.Duration
		rss          int64
		unit         uint64
	}{{-1, 0, 0, 1}, {0, -1, 0, 1}, {0, 0, -1, 1}, {0, 0, math.MaxInt64, 1024}, {0, 0, 1, 0}} {
		got := kernelProcessUsage(v.user, v.system, v.rss, v.unit)
		if got.State != ProcessMetricUnavailable || got.Value != nil || got.Reason != ProcessMalformed {
			t.Fatal("invalid kernel usage became zero or saturation", got)
		}
	}
	got := kernelProcessUsage(0, 0, 0, 1)
	if got.State != ProcessMetricMeasured || got.Value == nil || got.Value.HighWaterRSS != 0 {
		t.Fatal("genuine kernel zero became unavailable", got)
	}
}
