//go:build !windows

package node

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// Preserve legacy symlink reads without treating them as authority. A local
// fact separately audits the original path and re-reads those exact bytes.
func readInitialTrustFile(path string) ([]byte, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	dir, err := secretfile.Open(filepath.Dir(resolved))
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	name := filepath.Base(resolved)
	named, err := dir.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !named.Mode().IsRegular() {
		return nil, fmt.Errorf("trust store is not regular: %w", domain.ErrLocalContactIntent)
	}
	file, err := dir.OpenFileForRead(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || !os.SameFile(named, before) || before.Size() < 0 || before.Size() == math.MaxInt64 {
		return nil, fmt.Errorf("trust store changed before read: %w", domain.ErrLocalContactIntent)
	}
	raw, err := io.ReadAll(io.LimitReader(file, before.Size()+1))
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	current, err := dir.Lstat(name)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != before.Size() || after.Size() != before.Size() || !before.ModTime().Equal(after.ModTime()) || !os.SameFile(before, current) {
		return nil, fmt.Errorf("trust store changed during read: %w", domain.ErrLocalContactIntent)
	}
	return raw, nil
}
