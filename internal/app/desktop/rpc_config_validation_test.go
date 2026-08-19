package desktop

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/appdata"
)

// TestPartialRPCCredentialsAreRefusedBeforeAnythingOpens is the regression for
// a startup mistake that used to exit the process outright.
//
// The check sat next to the RPC server — after the shared database was open
// and the node was running — and failed through log.Fatal, which is os.Exit:
// no cancelNode, no drains, no Database.Close, none of the deferred cleanup
// Run is built around. It runs before any of that now, so the failure is an
// ordinary returned error and nothing has been opened to leak.
func TestPartialRPCCredentialsAreRefusedBeforeAnythingOpens(t *testing.T) {
	// The application data directory is redirected FIRST and unconditionally.
	// appdata's own go-test detection matches a binary named "*.test", which
	// Windows does not produce — there the test binary is "desktop.test.exe"
	// and DefaultDir() resolves to the real %AppData%\CorsaCore. Anything this
	// test then removes would be the user's identity, state database and
	// message history.
	appdata.SetDir(t.TempDir())
	t.Cleanup(func() { appdata.SetDir("") })

	// Run() returns before it touches ANYTHING — including cleanupAttachTmp,
	// which deletes the attachment staging directory. That ordering is what
	// this test is about, and it is asserted twice below.
	directory := t.TempDir()
	t.Setenv("CORSA_CHATLOG_DIR", directory)
	t.Setenv("CORSA_IDENTITY_PATH", filepath.Join(directory, "identity.json"))
	t.Setenv("CORSA_TRUST_STORE_PATH", filepath.Join(directory, "trust.json"))
	t.Setenv("CORSA_PEERS_STATE_PATH", filepath.Join(directory, "peers.json"))
	t.Setenv("CORSA_STATE_DB_PATH", filepath.Join(directory, "state.db"))

	// Only the username: the pair is what makes RPC auth valid.
	t.Setenv("CORSA_RPC_USERNAME", "operator")
	t.Setenv("CORSA_RPC_PASSWORD", "")

	err := Run()
	if err == nil {
		t.Fatal("Run() accepted a half-configured RPC credential pair")
	}
	if !strings.Contains(err.Error(), "rpc config invalid") {
		t.Fatalf("error = %v, want the RPC configuration to be what failed", err)
	}

	// Nothing was opened: the state database is what the old ordering had
	// already created — and left behind — by the time it called os.Exit.
	if _, statErr := os.Stat(filepath.Join(directory, "state.db")); statErr == nil {
		t.Fatal("the state database was opened before the configuration was checked")
	}
	// And nothing was swept: the staging directory is deleted a few lines
	// into Run, which this failure must not reach.
	staging := filepath.Join(appdata.DefaultDir(), attachTmpDirName)
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatalf("place a staging directory: %v", err)
	}
	if err := Run(); err == nil {
		t.Fatal("Run() accepted a half-configured RPC credential pair")
	}
	if _, statErr := os.Stat(staging); statErr != nil {
		t.Fatalf("the staging directory was swept before the configuration was checked: %v", statErr)
	}
}
