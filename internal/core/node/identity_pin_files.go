package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

const maxIdentityPinGuardBytes = 8192

type pinnedIdentityPinFiles struct {
	directory *secretfile.Dir
	mainName  string
	guardName string
}

func identityPinGuardName(mainName string) string {
	digest := sha256.Sum256([]byte(mainName))
	return ".corsa-pin-removal-" + hex.EncodeToString(digest[:16]) + ".json"
}

func (s *secureSessionStore) openIdentityPinFiles() (identityPinDurableFiles, error) {
	if s.dir == "" {
		return nil, fmt.Errorf("%w: memory-only store", secretfile.ErrDurabilityUnsupported)
	}
	if err := secretfile.AuditLocation(filepath.Join(s.dir, identityPinGuardName(s.name))); err != nil {
		return nil, err
	}
	directory, err := secretfile.Open(s.dir)
	if err != nil {
		return nil, err
	}
	return &pinnedIdentityPinFiles{directory: directory, mainName: s.name, guardName: identityPinGuardName(s.name)}, nil
}
func (f *pinnedIdentityPinFiles) Preflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.directory.DurablePreflight()
}
func (f *pinnedIdentityPinFiles) ReadGuard(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The bounded reader is handle-relative, like replacement and cleanup.
	named, err := f.directory.Lstat(f.guardName)
	if err != nil {
		return nil, err
	}
	if !named.Mode().IsRegular() {
		return nil, domain.ErrIdentityPinGuardInvalid
	}
	file, err := f.directory.OpenFileForRead(f.guardName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return nil, secretfile.StripPath(err)
	}
	if !os.SameFile(named, opened) {
		return nil, domain.ErrIdentityPinGuardInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxIdentityPinGuardBytes+1))
	if err != nil {
		return nil, secretfile.StripPath(err)
	}
	if len(raw) > maxIdentityPinGuardBytes {
		return nil, fmt.Errorf("identity pin guard exceeds %d bytes", maxIdentityPinGuardBytes)
	}
	return raw, nil
}
func (f *pinnedIdentityPinFiles) WriteGuard(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
	if err := ctx.Err(); err != nil {
		return secretfile.DurableNotCommitted, err
	}
	if len(raw) > maxIdentityPinGuardBytes {
		return secretfile.DurableNotCommitted, fmt.Errorf("identity pin guard too large")
	}
	return f.directory.DurableWrite(f.guardName, raw)
}
func (f *pinnedIdentityPinFiles) WriteMain(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
	if err := ctx.Err(); err != nil {
		return secretfile.DurableNotCommitted, err
	}
	return f.directory.DurableWrite(f.mainName, raw)
}
func (f *pinnedIdentityPinFiles) RemoveGuard(ctx context.Context) (secretfile.DurableCommit, error) {
	if err := ctx.Err(); err != nil {
		return secretfile.DurableNotCommitted, err
	}
	return f.directory.DurableRemove(f.guardName)
}
func (f *pinnedIdentityPinFiles) Close() error { return f.directory.Close() }
