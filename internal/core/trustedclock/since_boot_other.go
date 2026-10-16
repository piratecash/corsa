//go:build !linux && !darwin && !windows

package trustedclock

import "time"

// SinceBoot has no source here: the clock is unguarded and the wall clock is
// taken as it is.
func SinceBoot() (time.Duration, error) {
	return 0, ErrSinceBootUnsupported
}
