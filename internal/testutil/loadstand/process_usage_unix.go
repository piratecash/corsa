//go:build linux || darwin

package loadstand

import (
	"os"
	"runtime"
	"syscall"
)

// Only the existing sole Cmd.Wait owner may supply its final ProcessState.
// A signal exit does not erase genuine owned-command kernel lifetime usage.
func ownedProcessUsage(state *os.ProcessState) ProcessMetric[ProcessFinalUsage] {
	if state == nil {
		return unavailableProcessMetric[ProcessFinalUsage](ProcessMissing)
	}
	usage, ok := state.SysUsage().(*syscall.Rusage)
	unit := uint64(1)
	if runtime.GOOS == "linux" {
		unit = 1024
	}
	if !ok {
		return unavailableProcessMetric[ProcessFinalUsage](ProcessMalformed)
	}
	return kernelProcessUsage(state.UserTime(), state.SystemTime(), int64(usage.Maxrss), unit)
}
