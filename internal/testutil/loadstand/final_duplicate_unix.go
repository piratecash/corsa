//go:build darwin || linux

package loadstand

import (
	"golang.org/x/sys/unix"
	"os"
)

func duplicateFinalFD(file *os.File) (*os.File, error) {
	// Atomic CLOEXEC avoids a concurrent owned exec inheriting the parent pin
	// between Dup and a later flag update. No extra child descriptor is passed.
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "loadstand-final-capability"), nil
}
