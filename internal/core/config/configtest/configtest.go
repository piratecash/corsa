// Package configtest builds node configurations for tests.
package configtest

import (
	"path/filepath"
	"testing"

	"github.com/piratecash/corsa/internal/core/config"
)

// WithTempDataDirs returns cfg with the paths that would otherwise resolve to
// the default data directory placed in one t.TempDir(). Under `go test` that
// default is the relative `.corsa` in the package's working directory, so a
// node built without these writes there: its peer state on shutdown, the
// file-transfer store and mappings on every Run.
//
// Set when empty:
//   - PeersStatePath — the peer state; the secure-session store lives next
//     to it;
//   - ChatLogDir — the data directory: transmit/, downloads/, transfer
//     mappings, traffic captures, identity backups and the default state
//     database all derive from it.
//
// A path the caller set is kept. TrustStorePath and IdentityIntentsPath are
// left alone: empty, the node keeps them in memory and writes nothing.
func WithTempDataDirs(t testing.TB, cfg config.Node) config.Node {
	t.Helper()
	dir := t.TempDir()
	if cfg.PeersStatePath == "" {
		cfg.PeersStatePath = filepath.Join(dir, "peers.json")
	}
	if cfg.ChatLogDir == "" {
		cfg.ChatLogDir = dir
	}
	return cfg
}
