//go:build !windows

package storage

import (
	"fmt"
	"os"
)

// restrictFileAccess narrows the file to its owner. On POSIX the mode bits
// ARE the guarantee; the Windows variant applies a DACL instead, because
// there they are not.
func restrictFileAccess(path string) error {
	if err := os.Chmod(path, databaseFileMode); err != nil {
		return fmt.Errorf("storage: restrict %s: %w", path, err)
	}
	return nil
}
