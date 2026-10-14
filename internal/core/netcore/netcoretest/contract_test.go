package netcoretest_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
)

// backendContractReadBound bounds how long the Backend harness waits for a
// frame. Every frame the contract waits for is already sent or being sent, so
// the bound only decides how a broken Backend fails, never whether a working
// one passes.
const backendContractReadBound = 5 * time.Second

// TestBackendSatisfiesTheNetworkContract runs the contract the production
// bridge is held to (TestNetworkBridgeSatisfiesTheNetworkContract in package
// node) against the Backend, so a Backend that answers differently fails here
// instead of letting a node test pass on a path that fails in production.
func TestBackendSatisfiesTheNetworkContract(t *testing.T) {
	t.Parallel()
	netcoretest.RunNetworkContract(t, newBackendContractHarness)
}

func newBackendContractHarness(t *testing.T) netcoretest.Harness {
	b := netcoretest.New()
	t.Cleanup(b.Shutdown)

	// The remote end of a conn is the outbound channel captured at
	// registration, not a fresh Outbound(id) lookup: what the remote sees must
	// not depend on whether the registration is still there.
	var mu sync.Mutex
	var lastID domain.ConnID
	remotes := make(map[domain.ConnID]<-chan []byte)
	return netcoretest.Harness{
		Network: b.Network(),
		Register: func(t *testing.T, dir netcore.Direction) (domain.ConnID, string) {
			mu.Lock()
			defer mu.Unlock()
			lastID++
			id := lastID
			addr := "198.51.100.1:" + strconv.FormatUint(uint64(id), 10)
			b.Register(id, dir, addr)
			remotes[id] = b.Outbound(id)
			return id, addr
		},
		ReadFrame: func(t *testing.T, id domain.ConnID) ([]byte, bool) {
			t.Helper()
			mu.Lock()
			remote := remotes[id]
			mu.Unlock()
			if remote == nil {
				t.Fatalf("harness: conn %d was not registered by this harness", id)
			}
			select {
			case frame, ok := <-remote:
				return frame, !ok
			case <-time.After(backendContractReadBound):
				t.Fatalf("no frame and no end of stream on conn %d within %v", id, backendContractReadBound)
				return nil, false
			}
		},
		Closed: b.Closed,
	}
}
