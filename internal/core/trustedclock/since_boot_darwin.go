//go:build darwin

package trustedclock

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// SinceBoot is CLOCK_MONOTONIC on darwin, which keeps running while the
// machine sleeps — unlike CLOCK_UPTIME_RAW and mach_absolute_time, which Go's
// own monotonic clock reads.
func SinceBoot() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return 0, fmt.Errorf("%w: CLOCK_MONOTONIC: %w", ErrSinceBootUnsupported, err)
	}
	return time.Duration(ts.Nano()), nil
}
