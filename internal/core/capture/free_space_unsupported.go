//go:build !linux && !darwin && !windows

package capture

import "errors"

func captureFreeBytes(string) (uint64, error) {
	return 0, errors.New("filesystem free space check unavailable on this platform")
}
