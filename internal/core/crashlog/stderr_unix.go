//go:build !windows

package crashlog

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// redirectStderr duplicates file descriptor 2 (stderr) to a log file so that
// Go runtime fatal errors (like "concurrent map writes" or SIGSEGV) which
// bypass recover() are captured on disk. Without this, the process dies and
// the error message is lost if the terminal is not visible.
func redirectStderr(dir string) *os.File {
	path := filepath.Join(dir, stderrFileName)

	// Rotate the stderr log if it gets too large (reuse the same threshold).
	rotateIfNeeded(path)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "crashlog: cannot open stderr log: %v\n", err)
		return nil
	}

	// Write a separator so consecutive runs are distinguishable.
	_, _ = fmt.Fprintf(f, "\n--- stderr redirected (pid %d) ---\n", os.Getpid())

	if err := syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd())); err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "crashlog: dup2 stderr failed: %v\n", err)
		_ = f.Close()
		return nil
	}

	return f
}
