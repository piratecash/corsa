package loadstand

import (
	"math"
	"time"
)

// Kernel RSS units are platform-specific. Conversion refuses overflow rather
// than saturating a measured result; zero remains a genuine kernel reading.
func kernelProcessUsage(user, system time.Duration, maxRSS int64, rssUnit uint64) ProcessMetric[ProcessFinalUsage] {
	if rssUnit == 0 || maxRSS < 0 || uint64(maxRSS) > math.MaxUint64/rssUnit || user < 0 || system < 0 {
		return unavailableProcessMetric[ProcessFinalUsage](ProcessMalformed)
	}
	return measuredProcessMetric(ProcessFinalUsage{UserNanoseconds: ProcessNanoseconds(user), SystemNanoseconds: ProcessNanoseconds(system), HighWaterRSS: ProcessBytes(uint64(maxRSS) * rssUnit)})
}
