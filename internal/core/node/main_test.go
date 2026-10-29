package node

import (
	"io"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/appdata/appdatatest"
)

// TestMain fails the package when any of its tests wrote under the default
// data directory (.corsa in this package's directory) instead of its own
// t.TempDir(); see appdatatest.RunGuardingDefaultDir.
func TestMain(m *testing.M) {
	// Keep incidental diagnostics bounded; logging tests explicitly select and
	// restore their required Trace/Debug levels. Production defaults are unchanged.
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	// Log assertions install their own writers. Discard only the incidental
	// default diagnostics; testing output and explicit test captures stay intact.
	log.Logger = log.Logger.Output(io.Discard)
	os.Exit(appdatatest.RunGuardingDefaultDir(m))
}
