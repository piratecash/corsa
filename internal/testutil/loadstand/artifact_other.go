//go:build !unix && !windows

package loadstand

import (
	"context"
	"os"
)

func openArtifactFile(ctx context.Context, _ *os.Root, _ string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrUnsupportedArtifact
}
