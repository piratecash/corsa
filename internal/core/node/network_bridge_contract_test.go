package node

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
)

// bridgeContractReadBound bounds how long the bridge harness waits for the
// remote end of a pipe to receive something. It only decides how a broken
// bridge fails, never whether a working one passes.
const bridgeContractReadBound = 5 * time.Second

// TestNetworkBridgeSatisfiesTheNetworkContract runs the shared
// netcore.Network contract against the production bridge over a real Service
// registry. The Backend runs the same suite (netcoretest), which is what makes
// "the Backend behaves like the bridge" a checked fact rather than a promise in
// its package header.
func TestNetworkBridgeSatisfiesTheNetworkContract(t *testing.T) {
	t.Parallel()
	netcoretest.RunNetworkContract(t, newBridgeContractHarness)
}

// registerBridgeContractConn registers near on svc the way production does for
// dir — registerInboundConn for an accepted socket, attachOutboundNetCore for
// a dialled session — and returns its id and the core the registry built.
func registerBridgeContractConn(t *testing.T, svc *Service, near net.Conn, dir netcore.Direction) (domain.ConnID, *netcore.NetCore) {
	t.Helper()
	switch dir {
	case netcore.Inbound:
		if err := svc.registerInboundConn(near, nil); err != nil {
			t.Fatalf("harness: registerInboundConn: %v", err)
		}
		id, ok := svc.connIDFor(near)
		if !ok {
			t.Fatal("harness: the registered inbound conn has no ConnID")
		}
		return id, registeredInboundCore(t, svc, near)
	case netcore.Outbound:
		svc.peerMu.Lock()
		connID := svc.nextConnIDLocked()
		svc.peerMu.Unlock()
		session := &peerSession{
			address: "127.0.0.1:64646",
			connID:  connID,
			conn:    near,
		}
		if err := svc.attachOutboundNetCore(session); err != nil {
			t.Fatalf("harness: attachOutboundNetCore: %v", err)
		}
		// The session's own Close is what unregisters an outbound conn
		// (onClose), exactly as the connection manager does.
		t.Cleanup(func() { _ = session.Close() })
		return connID, session.netCore
	default:
		t.Fatalf("harness: unknown direction %v", dir)
		return 0, nil
	}
}

// bridgeContractRemote is the far end of one registered pipe and the core the
// registry built on its near end.
type bridgeContractRemote struct {
	conn   net.Conn
	reader *bufio.Reader
	core   *netcore.NetCore
}

func newBridgeContractHarness(t *testing.T) netcoretest.Harness {
	var mu sync.Mutex
	remotes := make(map[domain.ConnID]*bridgeContractRemote)

	// The harness pins its own teardown. Registered before anything else, so
	// it runs after every other cleanup — the fixture's inbound drain and
	// each outbound session's Close — and fails the case for any core they
	// left unreleased, instead of leaking its writer into the package run.
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for id, remote := range remotes {
			if !writerReturned(remote.core) {
				t.Errorf("harness: conn %d was never released; its writer outlives the test", id)
			}
		}
	})
	svc, _ := newInboundTeardownFixture(t)
	remoteOf := func(t *testing.T, id domain.ConnID) *bridgeContractRemote {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		remote := remotes[id]
		if remote == nil {
			t.Fatalf("harness: conn %d was not registered by this harness", id)
		}
		return remote
	}

	return netcoretest.Harness{
		Network: svc.Network(),
		Register: func(t *testing.T, dir netcore.Direction) (domain.ConnID, string) {
			t.Helper()
			near, far := net.Pipe()
			t.Cleanup(func() { _ = far.Close() })
			id, core := registerBridgeContractConn(t, svc, near, dir)
			mu.Lock()
			remotes[id] = &bridgeContractRemote{conn: far, reader: bufio.NewReader(far), core: core}
			mu.Unlock()
			return id, near.RemoteAddr().String()
		},
		ReadFrame: func(t *testing.T, id domain.ConnID) ([]byte, bool) {
			t.Helper()
			remote := remoteOf(t, id)
			// net.Pipe refuses a deadline once the near end is closed; the read
			// below then reports the end of the stream, which is the answer.
			err := remote.conn.SetReadDeadline(time.Now().Add(bridgeContractReadBound))
			if err != nil && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("harness: set read deadline: %v", err)
			}
			line, err := remote.reader.ReadBytes('\n')
			switch {
			case err == nil:
				return line, false
			case errors.Is(err, io.EOF) && len(line) == 0:
				return nil, true
			default:
				t.Fatalf("harness: remote read on conn %d: %q, %v", id, line, err)
				return nil, false
			}
		},
		// Exact at any moment: the writer returns only after Close signals
		// it, and Close waits for that return (writerReturned reads
		// WriterExited, not WriterDone, which a write failure also closes).
		Closed: func(id domain.ConnID) bool {
			return writerReturned(remoteOf(t, id).core)
		},
	}
}
