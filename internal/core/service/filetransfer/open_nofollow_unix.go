//go:build !windows

package filetransfer

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// openNoFollow opens a file with O_NOFOLLOW on platforms that support it,
// causing the kernel to reject the open with ELOOP if the final path component
// is a symlink. This prevents O_CREATE from following the symlink and creating
// a file at the target path before user-space checks run.
func openNoFollow(path string, flags int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, flags|syscall.O_NOFOLLOW, perm)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("path is a symlink (O_NOFOLLOW rejected): %s", path)
		}
		return nil, err
	}
	return f, nil
}
