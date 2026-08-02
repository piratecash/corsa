//go:build linux

package crashlog

import "syscall"

// dupFD2 duplicates oldfd onto newfd. Linux uses dup3: the dup2 syscall
// does not exist on arm64, and GOOS=android satisfies the linux build
// constraint, so this variant covers Android too.
//
// dup3 differs from dup2 in exactly one case: equal descriptors fail
// with EINVAL instead of succeeding as a no-op. Equal descriptors are
// possible here — if fd 2 was closed before startup, the crashlog
// OpenFile receives fd 2 itself, and failing would make the caller close
// the freshly opened file and leave the process with no stderr at all.
// Short-circuit to dup2's no-op semantics instead.
func dupFD2(oldfd, newfd int) error {
	if oldfd == newfd {
		return nil
	}
	return syscall.Dup3(oldfd, newfd, 0)
}
