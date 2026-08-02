//go:build !windows && !linux

package crashlog

import "syscall"

// dupFD2 duplicates oldfd onto newfd via dup2. Split per-platform because
// linux/arm64 (and therefore Android) has no dup2 syscall, so
// syscall.Dup2 is undefined there — see stderr_dup3.go for the
// dup3-based Linux variant.
func dupFD2(oldfd, newfd int) error {
	return syscall.Dup2(oldfd, newfd)
}
