//go:build !windows

package secretfile

import (
	"golang.org/x/sys/unix"
	"io/fs"
	"os"
)

// Directory sync itself is the preflight: a filesystem unable to confirm its
// namespace is rejected before any guard or main-file replacement.
func (d *Dir) durablePlatformPreflight() error { return d.syncDurableDirectory() }

func (d *Dir) syncDurableDirectory() error {
	directory, err := d.root.Open(".")
	if err != nil {
		return stripPath(err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return stripPath(err)
	}
	return stripPath(directory.Close())
}

func (d *Dir) openRegularForRead(name string) (*os.File, error) {
	directory, err := d.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	// os.Root.OpenFile follows in-root symlinks even with O_NOFOLLOW. A single
	// validated component and pinned directory fd permit a strict openat here.
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "bounded-secret-reader")
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fs.ErrInvalid
	}
	return file, nil
}
