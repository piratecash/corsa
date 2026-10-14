package node

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// inbound_release_close_test.go pins the invariant that releasing a
// connection is not cancellable work: Network.Close closes the core it names
// whatever the state of the ctx it is given, so an inbound connection leaves
// the registry only after its NetCore was closed.
//
// The NetCore's writer goroutine returns only from Close. A registry entry
// removed while its core is still open takes the last handle on that core with
// it — the shutdown drain enumerates the registry and cannot see it any more —
// so the writer and its queue stay alive for the rest of the process. That is
// what happened to every inbound connection whose handler ended on its own
// after Run's lifecycle context was cancelled and before the drain ran: the
// teardown passed the cancelled context to Close, and Close returned ctx.Err()
// without closing anything.
//
// The witness is WriterDone. On a core that never failed a write it closes only
// when writerLoop returns, and NetCore.Close waits for that return, so once a
// Close has run the channel is already closed and a non-blocking read is exact.

// writerReturned reports whether core's writer goroutine has returned. Only
// meaningful on a core that never failed a write: a failed write closes
// WriterDone while the writer is still alive, draining.
func writerReturned(core *netcore.NetCore) bool {
	select {
	case <-core.WriterDone():
		return true
	default:
		return false
	}
}

// registeredInboundCore returns the NetCore the registry holds for conn.
func registeredInboundCore(t *testing.T, svc *Service, conn net.Conn) *netcore.NetCore {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	entry := svc.testConnEntry(conn)
	if entry == nil || entry.core == nil {
		t.Fatal("fixture: the inbound conn has no registry entry with a core")
	}
	return entry.core
}

// TestUnregisterInboundConnClosesTheCoreEvenAfterRunCtxEnded drives the
// production teardown helper, with the lifecycle context live and cancelled.
// The cancelled case is the state every handler that ends during shutdown
// tears down in.
func TestUnregisterInboundConnClosesTheCoreEvenAfterRunCtxEnded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		cancelRunCtx bool
	}{
		{name: "run_ctx_live", cancelRunCtx: false},
		{name: "run_ctx_cancelled", cancelRunCtx: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc, _ := newInboundTeardownFixture(t)
			server, client := net.Pipe()
			t.Cleanup(func() { _ = client.Close() })
			if !svc.registerInboundConn(server, nil) {
				t.Fatal("fixture: registerInboundConn refused the conn")
			}
			core := registeredInboundCore(t, svc, server)
			if writerReturned(core) {
				t.Fatal("fixture: the writer returned before any teardown ran")
			}

			if tc.cancelRunCtx {
				// What Run's AfterFunc does the moment its caller's ctx ends.
				svc.runCancel()
			}
			// handleConn's teardown order: the wire first, then the registry.
			_ = server.Close()
			svc.unregisterInboundConn(server)

			if _, still := svc.connIDFor(server); still {
				t.Fatal("the conn is still registered after unregisterInboundConn")
			}
			if !writerReturned(core) {
				t.Fatal("the conn left the registry while its NetCore was still open: " +
					"the writer goroutine is alive and nothing can reach it any more")
			}
		})
	}
}

// TestNetworkBridgeCloseClosesTheCoreUnderACancelledContext pins the bridge's
// side of the Network.Close contract: a cancelled ctx neither skips the close
// nor turns it into an error.
func TestNetworkBridgeCloseClosesTheCoreUnderACancelledContext(t *testing.T) {
	t.Parallel()

	svc, _ := newInboundTeardownFixture(t)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	if !svc.registerInboundConn(server, nil) {
		t.Fatal("fixture: registerInboundConn refused the conn")
	}
	core := registeredInboundCore(t, svc, server)
	id, ok := svc.connIDFor(server)
	if !ok {
		t.Fatal("fixture: the registered conn has no ConnID")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.Network().Close(ctx, id); err != nil {
		t.Fatalf("Close under a cancelled ctx = %v, want nil: the conn exists and was closed", err)
	}
	if !writerReturned(core) {
		t.Fatal("Close under a cancelled ctx left the NetCore open")
	}
	// Registry eviction stays with the lifecycle helpers; Close only closes.
	if _, still := svc.connIDFor(server); !still {
		t.Fatal("Close removed the registry entry; eviction belongs to unregisterInboundConn")
	}
	svc.unregisterInboundConn(server)
}

// TestAnInboundConnThatEndsDuringShutdownDoesNotLeakItsWriter drives the
// window through Service.Run itself: the caller's ctx is cancelled, Run's
// lifecycle ctx follows, and an inbound handler ends on its own — the peer
// hangs up — before Run reaches its inbound drain. When Run returns, that
// connection's writer must have returned too.
//
// The window is held open by parking Run's own goroutine in the accept loop:
// the test holds svc.connLimiter.mu and makes a second connection, so Run
// blocks in allowConnect until the test lets go. Run reaches its drain only
// after the accept loop returns, so while it is parked there the handler of
// the first connection runs its teardown with nothing else closing its core.
// The goroutine dump is the witness that Run is parked rather than about to
// lock. allowConnect has no other production caller, and the match names this
// node's limiter by its receiver pointer, followed by the separator before the
// ip argument so one pointer cannot match as a hex prefix of another; a
// parallel test's node parked the same way cannot stand in for it.
func TestAnInboundConnThatEndsDuringShutdownDoesNotLeakItsWriter(t *testing.T) {
	t.Parallel()

	cfg := configtest.WithTempDataDirs(t, config.Node{
		ListenAddress:  freeAddress(t),
		BootstrapPeers: []string{},
	})
	cfg.AllowPrivatePeers = true
	cfg = deriveTestAdvertisePort(cfg)
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	svc := NewService(cfg, id, nil)
	// Rate limiting stays on: it is what makes the accept loop take
	// connLimiter.mu, the lock the test parks Run on.
	svc.markPeerStateIntervalTest = -1
	_, stopNode := startTestService(t, ctx, cancel, svc)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(stopNode) }
	defer stop()

	first, err := net.Dial("tcp", cfg.ListenAddress)
	if err != nil {
		t.Fatalf("dial the first conn: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	// One byte selects the v1 session and lets the handler register; with no
	// newline the handler then waits in its read, writing nothing.
	if _, err := first.Write([]byte("{")); err != nil {
		t.Fatalf("write the first byte: %v", err)
	}
	var core *netcore.NetCore
	waitFor(t, 10*time.Second, "the first conn registered", func() bool {
		core = soleInboundCore(svc)
		return core != nil
	})
	if writerReturned(core) {
		t.Fatal("fixture: the writer returned before the scenario began")
	}

	svc.connLimiter.mu.Lock()
	var releaseOnce sync.Once
	releaseAcceptLoop := func() { releaseOnce.Do(svc.connLimiter.mu.Unlock) }
	defer releaseAcceptLoop()
	second, err := net.Dial("tcp", cfg.ListenAddress)
	if err != nil {
		t.Fatalf("dial the second conn: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	waitFor(t, 10*time.Second, "Run parked in allowConnect", func() bool {
		return goroutineParkedOn(waitReasonMutexLock, fmt.Sprintf("(*connRateLimiter).allowConnect(%p,", svc.connLimiter))
	})

	cancel()
	<-svc.runCtx.Done()
	if soleInboundCore(svc) != core {
		t.Fatal("fixture: the first conn left the registry before its peer hung up")
	}

	_ = first.Close()
	waitFor(t, 10*time.Second, "the first conn's handler unregistered it", func() bool {
		return soleInboundCore(svc) == nil
	})
	if !writerReturned(core) {
		t.Error("the conn left the registry while its NetCore was still open")
	}

	// The second conn says nothing; hanging it up lets its handler end at its
	// first read instead of holding Run's connWg.Wait.
	_ = second.Close()
	releaseAcceptLoop()
	stop()
	if !writerReturned(core) {
		t.Fatal("Run returned with the writer of an inbound conn that ended during shutdown still alive: " +
			"no one holds its NetCore, so it lives until the process exits")
	}
}

// soleInboundCore returns the core of the node's only inbound registry entry,
// or nil when it has none.
func soleInboundCore(svc *Service) *netcore.NetCore {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	var found *netcore.NetCore
	svc.forEachConnLocked(func(info connInfo) bool {
		if info.dir == netcore.Inbound {
			found = svc.coreForIDLocked(info.id)
		}
		return true
	})
	return found
}
