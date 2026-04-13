package node

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestPeerSessionCloseDelegatesToNetCore verifies that peerSession.Close()
// invokes onClose exactly once, delegates to NetCore.Close(), and is safe
// to call multiple times — the invariants relied on by openPeerSession's
// defer, openPeerSessionForCM's closeOnError and the CM teardown goroutine
// after PR 2 migrated outbound writes onto the managed path.
func TestPeerSessionCloseDelegatesToNetCore(t *testing.T) {
	conn := &mockConn{}
	nc := newNetCore(1, conn, Outbound, NetCoreOpts{})

	onCloseCalls := 0
	session := &peerSession{
		conn:    conn,
		netCore: nc,
		onClose: func() { onCloseCalls++ },
	}

	if err := session.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if onCloseCalls != 1 {
		t.Fatalf("onClose should fire exactly once, got %d", onCloseCalls)
	}
	if !conn.IsClosed() {
		t.Fatal("underlying connection must be closed via NetCore.Close")
	}

	// Second Close must be a no-op (idempotent teardown contract).
	if err := session.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if onCloseCalls != 1 {
		t.Fatalf("onClose must not fire twice, got %d", onCloseCalls)
	}
}

// TestPeerSessionCloseWithoutNetCoreClosesConn verifies the fallback path
// used by unit-test fixtures that build a peerSession directly without the
// service-wired NetCore.
func TestPeerSessionCloseWithoutNetCoreClosesConn(t *testing.T) {
	conn := &mockConn{}
	session := &peerSession{conn: conn}

	if err := session.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !conn.IsClosed() {
		t.Fatal("conn must be closed when netCore is nil")
	}
}

// TestPeerSessionCloseOrderingProtectsSingleWriter locks in the P1 fix:
// onClose (which unregisters the NetCore from s.inboundNetCores) must run
// strictly AFTER netCore.Close() has returned, so that while writerLoop is
// still alive the map lookup continues to resolve to the managed path.
// Unregistering first would let a concurrent writeJSONFrame fall through
// to the enqueueUnregistered branch and call conn.Write directly, racing
// with the still-live writer goroutine.
func TestPeerSessionCloseOrderingProtectsSingleWriter(t *testing.T) {
	conn := &mockConn{}
	nc := newNetCore(1, conn, Outbound, NetCoreOpts{})

	var writerDoneAtOnClose bool
	session := &peerSession{
		conn:    conn,
		netCore: nc,
		onClose: func() {
			// When onClose fires, the writer goroutine must already have
			// exited — writerDone is closed by writerLoop on return.
			select {
			case <-nc.writerDone:
				writerDoneAtOnClose = true
			default:
				writerDoneAtOnClose = false
			}
		},
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !writerDoneAtOnClose {
		t.Fatal("onClose must run after NetCore writer goroutine has exited; P1 ordering violated")
	}
}

// TestPeerSessionCloseIsConcurrencySafe locks in the P2 fix: Close() is
// built on sync.Once, so racing callers (defer + ctx-watcher + CM failure
// path) cannot double-fire onClose or double-close the underlying NetCore.
func TestPeerSessionCloseIsConcurrencySafe(t *testing.T) {
	conn := &mockConn{}
	nc := newNetCore(1, conn, Outbound, NetCoreOpts{})

	var onCloseCalls int32
	session := &peerSession{
		conn:    conn,
		netCore: nc,
		onClose: func() { atomic.AddInt32(&onCloseCalls, 1) },
	}

	const racers = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			<-start
			_ = session.Close()
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&onCloseCalls); got != 1 {
		t.Fatalf("onClose must fire exactly once under %d concurrent Close() callers, got %d", racers, got)
	}
}
