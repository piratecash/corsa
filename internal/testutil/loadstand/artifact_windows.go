//go:build windows

package loadstand

import (
	"context"
	"os"

	"golang.org/x/sys/windows"
)

func openArtifactFile(ctx context.Context, root *os.Root, name string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Root confines NT path resolution to the pinned filesystem directory.
	// Request overlapped rather than synchronous I/O; the caller verifies disk
	// file type and entry/handle identity before any read, rejecting reparse links.
	return root.OpenFile(name, os.O_RDONLY|int(windows.FILE_FLAG_OVERLAPPED), 0)
}
