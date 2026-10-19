package loadstand

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// ArtifactReadLimit is the required maximum number of public artifact bytes.
type ArtifactReadLimit int64

func readPublicArtifact(ctx context.Context, path string) ([]byte, error) {
	return ReadPublicArtifact(ctx, path, 8<<20)
}

// ReadPublicArtifact reuses the pinned regular-file boundary for bounded public
// metadata. It refuses links, FIFO/device substitutions and unsupported platforms.
// Context checks cannot interrupt a regular filesystem syscall stuck in the kernel.
func ReadPublicArtifact(ctx context.Context, path string, limit ArtifactReadLimit) (contents []byte, err error) {
	if limit <= 0 || limit == 1<<63-1 {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("open public scan directory: %w", err)
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	name := filepath.Base(path)
	before, err := root.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect public scan artifact: %w", err)
	}
	file, err := openVerifiedArtifact(ctx, root, name, before)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if before.Size() > int64(limit) {
		return nil, ErrArtifactTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contents, err = io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("read public scan artifact: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(contents)) > int64(limit) {
		return nil, ErrArtifactTooLarge
	}
	return contents, nil
}

func openVerifiedArtifact(ctx context.Context, root *os.Root, name string, before fs.FileInfo) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, ErrUnsupportedArtifact
	}
	// The platform open must not wait for a FIFO writer even if a regular entry
	// is swapped after Lstat. Never read until both descriptor and name agree.
	file, err := openArtifactFile(ctx, root, name)
	if err != nil {
		return nil, fmt.Errorf("open public scan artifact: %w", err)
	}
	opened, statErr := file.Stat()
	if statErr != nil {
		return nil, errors.Join(fmt.Errorf("inspect opened public artifact: %w", statErr), file.Close())
	}
	named, nameErr := root.Lstat(name)
	if nameErr != nil {
		return nil, errors.Join(fmt.Errorf("reinspect public artifact: %w", nameErr), file.Close())
	}
	if !opened.Mode().IsRegular() || !named.Mode().IsRegular() || !os.SameFile(before, opened) || !os.SameFile(named, opened) {
		return nil, errors.Join(ErrUnsupportedArtifact, file.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
