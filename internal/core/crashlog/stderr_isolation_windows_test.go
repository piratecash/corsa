//go:build windows

package crashlog

import (
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func preserveCrashlogStderr(t *testing.T) {
	t.Helper()
	originalStderr := os.Stderr
	originalHandle, err := windows.GetStdHandle(windows.STD_ERROR_HANDLE)
	if err != nil {
		t.Fatalf("save original stderr handle: %v", err)
	}
	if originalHandle == 0 || originalHandle == windows.InvalidHandle {
		t.Fatal("original stderr handle is invalid")
	}
	t.Cleanup(func() {
		// Production redirects the Windows standard handle and os.Stderr,
		// but does not replace stdout or syscall.Stderr. Restore both fields
		// it changes; the original handle was never owned by Setup.
		err := windows.SetStdHandle(windows.STD_ERROR_HANDLE, originalHandle)
		os.Stderr = originalStderr
		if err != nil {
			t.Errorf("restore original stderr handle: %v", err)
		}
	})
}
