//go:build windows

package trustedclock

import (
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// systemTimeOfDayInformation is SYSTEM_TIMEOFDAY_INFORMATION: the boot time
// and the current time as FILETIME counts, the boot time moved by the kernel
// along with every step of the wall clock.
type systemTimeOfDayInformation struct {
	BootTime      int64
	CurrentTime   int64
	TimeZoneBias  int64
	TimeZoneID    uint32
	Reserved      uint32
	BootTimeBias  uint64
	SleepTimeBias uint64
}

// On Windows the clock since boot agrees with the kernel's own figure: the
// current time minus the boot time, sleep included — the property the
// trusted time relies on. The tolerance covers GetTickCount64's resolution
// and the kernel's slewing of the current time.
func TestSinceBootAgreesWithTheKernelBootTime(t *testing.T) {
	t.Parallel()
	const tolerance = 5 * time.Second
	reading := sinceBootOrSkip(t)
	var info systemTimeOfDayInformation
	if err := windows.NtQuerySystemInformation(windows.SystemTimeOfDayInformation, unsafe.Pointer(&info), uint32(unsafe.Sizeof(info)), nil); err != nil {
		t.Fatalf("NtQuerySystemInformation(SystemTimeOfDayInformation): %v", err)
	}
	// FILETIME counts 100 ns intervals.
	sinceBoot := time.Duration(info.CurrentTime-info.BootTime) * 100
	if diff := (reading - sinceBoot).Abs(); diff > tolerance {
		t.Fatalf("SinceBoot = %v, the kernel's current time minus boot time = %v: off by %v", reading, sinceBoot, diff)
	}
}
