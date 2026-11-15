//go:build linux

package loadstand

import (
	"io"
	"os"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
)

// ReadLinuxRouteWireClockDomain reads proc's boot identifier and checks
// both the process and the current OS thread's time namespace descriptors.
func ReadLinuxRouteWireClockDomain() (RouteWireClockDomain, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	process, err := openRouteWireTimeNamespace("/proc/self/ns/time")
	if err != nil {
		return RouteWireClockDomain{}, err
	}
	defer func() { _ = process.Close() }()
	thread, err := openRouteWireTimeNamespace("/proc/thread-self/ns/time")
	if err != nil {
		return RouteWireClockDomain{}, err
	}
	defer func() { _ = thread.Close() }()
	var processStat, threadStat unix.Stat_t
	if unix.Fstat(int(process.Fd()), &processStat) != nil || unix.Fstat(int(thread.Fd()), &threadStat) != nil ||
		processStat.Dev != threadStat.Dev || processStat.Ino != threadStat.Ino {
		return RouteWireClockDomain{}, ErrFinalAccountingUnavailable
	}
	boot, err := os.Open("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return RouteWireClockDomain{}, err
	}
	defer func() { _ = boot.Close() }()
	var fs unix.Statfs_t
	if unix.Fstatfs(int(boot.Fd()), &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return RouteWireClockDomain{}, ErrFinalAccountingUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(boot, 38))
	if err != nil {
		return RouteWireClockDomain{}, err
	}
	domain := RouteWireClockDomain{BootID: strings.TrimSuffix(string(raw), "\n"), Device: uint64(threadStat.Dev), Inode: threadStat.Ino}
	if !domain.valid() {
		return RouteWireClockDomain{}, ErrFinalAccountingUnavailable
	}
	return domain, nil
}

func openRouteWireTimeNamespace(path string) (*os.File, error) {
	// Follow the kernel's namespace magic link and inspect the opened NSFS FD.
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	var fs unix.Statfs_t
	kind, typeErr := unix.IoctlRetInt(fd, unix.NS_GET_NSTYPE)
	if typeErr != nil || kind != unix.CLONE_NEWTIME || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.NSFS_MAGIC {
		_ = unix.Close(fd)
		return nil, ErrFinalAccountingUnavailable
	}
	return os.NewFile(uintptr(fd), "route-wire-time-namespace"), nil
}
