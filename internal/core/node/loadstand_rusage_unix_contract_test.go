//go:build unix

package node

import (
	"syscall"
	"testing"
	"time"
)

// TestLoadStandRusageUnits pins how the kernel's record is read: user and
// system time each from its own field, and ru_maxrss in the unit the OS
// defines for it — bytes on Apple systems, kilobytes on every other unix.
func TestLoadStandRusageUnits(t *testing.T) {
	t.Parallel()

	usage := syscall.Rusage{
		Utime:  syscall.NsecToTimeval(int64(1500 * time.Millisecond)),
		Stime:  syscall.NsecToTimeval(int64(250 * time.Millisecond)),
		Maxrss: 3,
	}
	wantRSS := map[string]loadStandByteCount{"darwin": 3, "ios": 3, "linux": 3 * 1024, "freebsd": 3 * 1024}
	for goos, rss := range wantRSS {
		got := loadStandRusageFrom(usage, goos)
		want := loadStandRusage{UserCPU: 1500 * time.Millisecond, SystemCPU: 250 * time.Millisecond, MaxRSS: rss}
		if got != want {
			t.Errorf("%s: %+v, want %+v", goos, got, want)
		}
	}
}
