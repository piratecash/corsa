//go:build linux

package trustedclock

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// SinceBoot is CLOCK_BOOTTIME on Linux and Android: the monotonic clock with
// suspend included, which CLOCK_MONOTONIC (Go's own) leaves out. A kernel
// without it (before 2.6.39) answers ErrSinceBootUnsupported.
func SinceBoot() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, fmt.Errorf("%w: CLOCK_BOOTTIME: %w", ErrSinceBootUnsupported, err)
	}
	return time.Duration(ts.Nano()), nil
}
