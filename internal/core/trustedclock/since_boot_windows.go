//go:build windows

package trustedclock

import (
	"time"

	"golang.org/x/sys/windows"
)

// SinceBoot is GetTickCount64 on Windows, which counts sleep and hibernation
// — unlike the unbiased interrupt time Go's own monotonic clock reads.
func SinceBoot() (time.Duration, error) {
	return windows.DurationSinceBoot(), nil
}
