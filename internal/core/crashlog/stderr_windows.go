//go:build windows

package crashlog

import "os"

// redirectStderr is a no-op on Windows where syscall.Dup2 is not available.
// TODO: implement using SetStdHandle for Windows support.
func redirectStderr(_ string) *os.File { return nil }
