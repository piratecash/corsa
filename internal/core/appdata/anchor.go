package appdata

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DataDirEnv names the variable that moves every node-local file — identity
// keys, contacts, the chat log, downloads, crash logs, staging copies — to
// one directory of the user's choosing, typically inside an encrypted volume.
const DataDirEnv = "CORSA_DATA_DIR"

var (
	// ErrDataDirMissing: CORSA_DATA_DIR names a directory that cannot be
	// reached — most often an encrypted volume that is not mounted.
	ErrDataDirMissing = errors.New("configured data directory is not available")

	// ErrDataDirNotDirectory: CORSA_DATA_DIR names something that exists
	// but is not a directory.
	ErrDataDirNotDirectory = errors.New("configured data directory is not a directory")

	// ErrDataDirLinkBroken: the platform default data directory is a link
	// (symlink or junction) whose target cannot be reached — the same
	// unmounted volume, reached through the link recipe instead of the
	// variable.
	ErrDataDirLinkBroken = errors.New("data directory link points to an unreachable target")
)

// Anchor fixes the data directory for this process and checks that it can be
// used. Call it first in main, before crash log setup or anything else that
// derives a path from DefaultDir.
//
// It is fail-closed on purpose. A data directory the user placed inside an
// encrypted volume is absent whenever the volume is not mounted, and a node
// that created it anew would mint a fresh identity and write history onto
// the plain disk — the very thing the user moved it away from. So a
// configured directory is never created here, and a link whose target is
// gone stops the start instead of being worked around.
func Anchor() error {
	configured := strings.TrimSpace(os.Getenv(DataDirEnv))
	if configured == "" {
		return requireReachableLinkTarget(DefaultDir())
	}

	dir, err := filepath.Abs(configured)
	if err != nil {
		return fmt.Errorf("%w: resolve %s=%q: %w", ErrDataDirMissing, DataDirEnv, configured, err)
	}
	if err := requireExistingDir(dir); err != nil {
		return err
	}
	SetDir(dir)
	return nil
}

// requireExistingDir accepts dir only when it already exists and is a
// directory, following links: the user creates the directory once, inside
// the volume, and its absence means the volume is not there.
func requireExistingDir(dir string) error {
	info, err := os.Stat(dir)
	switch {
	case err != nil:
		return fmt.Errorf("%w: %s: %w", ErrDataDirMissing, dir, err)
	case !info.IsDir():
		return fmt.Errorf("%w: %s", ErrDataDirNotDirectory, dir)
	default:
		return nil
	}
}

// requireReachableLinkTarget rejects a default data dir that is present as a
// link but cannot be followed. A path that does not exist at all is fine: a
// first start creates it, as it always has.
func requireReachableLinkTarget(dir string) error {
	if _, err := os.Lstat(dir); err != nil {
		return nil
	}
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrDataDirLinkBroken, dir, err)
	}
	return nil
}
