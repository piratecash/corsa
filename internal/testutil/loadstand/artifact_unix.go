//go:build unix

package loadstand

import (
	"context"
	"os"
	"syscall"
)

func openArtifactFile(ctx context.Context, root *os.Root, name string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Root pins the parent; NONBLOCK prevents a replacement FIFO from waiting at
	// open. Root may resolve a link within itself, so fstat/Lstat/SameFile checks
	// in openVerifiedArtifact still refuse links and substituted files before read.
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
}
