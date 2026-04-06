//go:build windows

package filetransfer

import "os"

// openNoFollow falls back to a normal open on Windows because syscall.O_NOFOLLOW
// is not available there. Callers must still run verifyNotSymlink immediately
// after open, which provides the shared user-space protection path for symlink
// rejection and post-open swap detection.
func openNoFollow(path string, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flags, perm)
}
