//go:build linux || darwin

package capture

import (
	"errors"
	"math/bits"

	"golang.org/x/sys/unix"
)

func captureFreeBytes(dir string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	if st.Bsize <= 0 {
		return 0, errors.New("invalid filesystem block size")
	}
	hi, free := bits.Mul64(uint64(st.Bavail), uint64(st.Bsize))
	if hi != 0 {
		return 0, errors.New("filesystem free bytes overflow")
	}
	return free, nil
}
