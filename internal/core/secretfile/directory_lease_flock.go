//go:build darwin || linux

package secretfile

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func tryDirectoryFlock(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return errors.Join(ErrDirectoryLeaseBusy, err)
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		return errors.Join(ErrDirectoryLeaseUnsupported, err)
	}
	return err
}
