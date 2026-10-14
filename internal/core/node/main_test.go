package node

import (
	"os"
	"testing"

	"github.com/piratecash/corsa/internal/core/appdata/appdatatest"
)

// TestMain fails the package when any of its tests wrote under the default
// data directory (.corsa in this package's directory) instead of its own
// t.TempDir(); see appdatatest.RunGuardingDefaultDir.
func TestMain(m *testing.M) {
	os.Exit(appdatatest.RunGuardingDefaultDir(m))
}
