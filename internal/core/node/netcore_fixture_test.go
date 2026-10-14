package node

import (
	"net"
	"testing"

	"github.com/piratecash/corsa/internal/core/netcore"
)

// newTestNetCore is the one place test code in this package builds a NetCore,
// and it closes the core when the test ends.
//
// Closing the conn underneath is not enough: an idle writerLoop returns only
// on Close, so every core a test forgot to close kept its writer goroutine and
// its buffers alive for the rest of the package run, and a stress test that
// builds one per iteration multiplies that by its -count. Tying Close to
// construction makes forgetting it impossible, and
// TestNetCoresInTestsAreBuiltOnlyByNewTestNetCore keeps direct
// netcore.New calls out of every other test file.
//
// The order against the caller's own conn cleanups does not matter: Close
// closes the conn itself before it waits for the writer, so it never blocks on
// a far end that is still open. Close is idempotent, so a test that closes the
// core itself — mid-test, through its session, or through a teardown path under
// test — is fine. conn must be closable; a fixture that needs a chosen remote
// address uses remoteAddrConn, whose Close is always safe.
//
// Cores that production code builds while a test drives it (registerInboundConn,
// handleConn, the dial paths) are not test-built and stay owned by that code.
func newTestNetCore(t *testing.T, id netcore.ConnID, conn net.Conn, dir netcore.Direction, opts netcore.Options) *netcore.NetCore {
	t.Helper()
	core := netcore.New(id, conn, dir, opts)
	t.Cleanup(core.Close)
	return core
}
