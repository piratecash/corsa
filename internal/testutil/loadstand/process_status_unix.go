//go:build unix

package loadstand

import (
	"os"
	"syscall"
)

func ownedWaitSignal(state *os.ProcessState) ProcessSignal {
	status, ok := state.Sys().(syscall.WaitStatus)
	if ok && status.Signaled() {
		return ProcessSignal(status.Signal())
	}
	return 0
}
