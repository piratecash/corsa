//go:build windows

package crashlog

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// redirectStderr redirects file descriptor 2 (stderr) to a log file so that
// Go runtime fatal errors (like "concurrent map writes") which bypass
// recover() are captured on disk. On Windows syscall.Dup2 is not available,
// so we use SetStdHandle + os.NewFile to achieve the same effect.
func redirectStderr(dir string) *os.File {
	path := filepath.Join(dir, stderrFileName)

	rotateIfNeeded(path)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "crashlog: cannot open stderr log: %v\n", err)
		return nil
	}

	_, _ = fmt.Fprintf(f, "\n--- stderr redirected (pid %d) ---\n", os.Getpid())

	if err := windows.SetStdHandle(windows.STD_ERROR_HANDLE, windows.Handle(f.Fd())); err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "crashlog: SetStdHandle failed: %v\n", err)
		_ = f.Close()
		return nil
	}

	// Update the Go-level os.Stderr so that fmt.Fprint(os.Stderr, ...) and
	// the runtime's write(2, ...) both go to the file.
	os.Stderr = f

	return f
}
