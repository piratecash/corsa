//go:build !windows

package crashlog

import (
	"os"
	"syscall"
	"testing"
)

func preserveCrashlogStderr(t *testing.T) {
	t.Helper()
	originalStderr := os.Stderr
	backup, err := syscall.Dup(2)
	if err != nil {
		t.Fatalf("save original stderr: %v", err)
	}
	syscall.CloseOnExec(backup)

	// In -json mode testing aliases os.Stderr to os.Stdout. Redirecting that
	// descriptor would steal the test event stream. Exercise the real FD 2
	// instead, without changing the testing package's stdout writer.
	stderr := os.NewFile(2, "crashlog-test-stderr")
	os.Stderr = stderr
	t.Cleanup(func() {
		// Close this wrapper before restoring FD 2 so its eventual finalizer
		// cannot close the restored descriptor. Setup's deferred cleanup has
		// already closed its separate log handles by this point.
		closeErr := stderr.Close()
		restoreErr := dupFD2(backup, 2)
		backupErr := syscall.Close(backup)
		os.Stderr = originalStderr
		if closeErr != nil {
			t.Errorf("close isolated stderr: %v", closeErr)
		}
		if restoreErr != nil {
			t.Errorf("restore original stderr: %v", restoreErr)
		}
		if backupErr != nil {
			t.Errorf("close saved stderr: %v", backupErr)
		}
	})
}
