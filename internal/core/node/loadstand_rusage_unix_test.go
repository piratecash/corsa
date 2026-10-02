//go:build unix

package node

import (
	"fmt"
	"runtime"
	"syscall"
	"time"
)

// loadStandMaxRSSUnit is what one unit of ru_maxrss is: getrusage reports it
// in bytes on Apple systems and in kilobytes on every other unix. This is the
// meaning of the field on each OS, not a guess about what the OS can do.
var loadStandMaxRSSUnit = map[string]loadStandByteCount{
	"darwin": 1,
	"ios":    1,
}

const loadStandMaxRSSDefaultUnit loadStandByteCount = 1024

func readLoadStandRusage() (loadStandRusage, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return loadStandRusage{}, fmt.Errorf("getrusage: %w", err)
	}
	return loadStandRusageFrom(usage, runtime.GOOS), nil
}

// loadStandRusageFrom converts the kernel's record as goos defines its units.
func loadStandRusageFrom(usage syscall.Rusage, goos string) loadStandRusage {
	unit, known := loadStandMaxRSSUnit[goos]
	if !known {
		unit = loadStandMaxRSSDefaultUnit
	}
	return loadStandRusage{
		UserCPU:   time.Duration(usage.Utime.Nano()),
		SystemCPU: time.Duration(usage.Stime.Nano()),
		MaxRSS:    loadStandByteCount(usage.Maxrss) * unit,
	}
}
