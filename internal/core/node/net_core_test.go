package node

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// mockConn is a minimal net.Conn for testing NetCore writer behavior.
type mockConn struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	closed  bool
	writeFn func([]byte) (int, error) // optional override
}

func (m *mockConn) Read(b []byte) (int, error) { return 0, nil }
func (m *mockConn) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeFn != nil {
		return m.writeFn(b)
	}
	return m.buf.Write(b)
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 64646} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func (m *mockConn) Written() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Bytes()
}

func (m *mockConn) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// TestNetCoreSendWritesToSocket verifies that Send() routes a frame through
// the writer goroutine to the underlying socket.
func TestNetCoreSendWritesToSocket(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(1, conn, Inbound, NetCoreOpts{})
	defer pc.Close()

	frame := protocol.Frame{Type: "ping"}
	if pc.Send(frame) != sendOK {
		t.Fatal("Send did not return sendOK")
	}

	// Give the writer goroutine time to drain.
	time.Sleep(50 * time.Millisecond)

	written := conn.Written()
	if len(written) == 0 {
		t.Fatal("expected data written to socket, got nothing")
	}
	if !bytes.Contains(written, []byte(`"type":"ping"`)) {
		t.Fatalf("unexpected written data: %s", written)
	}
}

// TestNetCoreSendSyncBlocksUntilWrite verifies that SendSync() blocks until
// the writer goroutine has flushed the frame to the socket.
func TestNetCoreSendSyncBlocksUntilWrite(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(2, conn, Inbound, NetCoreOpts{})
	defer pc.Close()

	frame := protocol.Frame{Type: "pong"}
	if pc.SendSync(frame) != sendOK {
		t.Fatal("SendSync did not return sendOK")
	}

	// After SendSync returns true, data must already be in the socket.
	written := conn.Written()
	if !bytes.Contains(written, []byte(`"type":"pong"`)) {
		t.Fatalf("expected pong frame written, got: %s", written)
	}
}

// TestNetCoreWriteDeadlinePerDirection verifies that newNetCore selects
// the per-write socket deadline based on Direction: outbound connections
// must use the shorter sessionWriteTimeout so that slow-peer eviction for
// dialled sessions keeps the same back-pressure window it had before
// outbound writes were routed through the managed send path.
func TestNetCoreWriteDeadlinePerDirection(t *testing.T) {
	inbound := newNetCore(100, &mockConn{}, Inbound, NetCoreOpts{})
	defer inbound.Close()
	if inbound.writeDeadline != connWriteTimeout {
		t.Fatalf("inbound writeDeadline = %v, want %v", inbound.writeDeadline, connWriteTimeout)
	}

	outbound := newNetCore(101, &mockConn{}, Outbound, NetCoreOpts{})
	defer outbound.Close()
	if outbound.writeDeadline != sessionWriteTimeout {
		t.Fatalf("outbound writeDeadline = %v, want %v", outbound.writeDeadline, sessionWriteTimeout)
	}

	if got := writeDeadlineFor(Inbound); got != connWriteTimeout {
		t.Fatalf("writeDeadlineFor(Inbound) = %v, want %v", got, connWriteTimeout)
	}
	if got := writeDeadlineFor(Outbound); got != sessionWriteTimeout {
		t.Fatalf("writeDeadlineFor(Outbound) = %v, want %v", got, sessionWriteTimeout)
	}
}

// TestNetCoreSendReturnsFalseWhenQueueFull verifies that Send() returns false
// instead of blocking when the write channel is full.
func TestNetCoreSendReturnsFalseWhenQueueFull(t *testing.T) {
	// writerStarted signals that the writer goroutine has pulled the first
	// item off the channel and is blocked in writeFn. Without this
	// synchronization the test may fill the channel before the writer
	// goroutine starts, causing a false failure at sendChBuffer.
	writerStarted := make(chan struct{})
	blocker := make(chan struct{})
	var once sync.Once
	conn := &mockConn{
		writeFn: func(b []byte) (int, error) {
			once.Do(func() { close(writerStarted) })
			<-blocker
			return len(b), nil
		},
	}
	pc := newNetCore(3, conn, Inbound, NetCoreOpts{})
	defer func() {
		close(blocker)
		pc.Close()
	}()

	// Send the first item and wait for the writer to pull it.
	frame := protocol.Frame{Type: "ping"}
	if pc.Send(frame) != sendOK {
		t.Fatal("first Send should succeed")
	}
	<-writerStarted

	// Now fill the remaining sendChBuffer buffered slots.
	for i := 0; i < sendChBuffer; i++ {
		if pc.Send(frame) != sendOK {
			t.Fatalf("Send failed at item %d (channel not full yet)", i)
		}
	}

	// Channel is now full (128 buffered + writer blocked on 1st). Next must fail.
	if pc.Send(frame) == sendOK {
		t.Fatal("Send returned sendOK when channel should be full")
	}
}

// TestNetCoreCloseIdemponent verifies that Close() can be called multiple
// times without panicking.
func TestNetCoreCloseIdempotent(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(4, conn, Inbound, NetCoreOpts{})

	pc.Close()
	pc.Close() // must not panic

	if !conn.IsClosed() {
		t.Fatal("underlying connection should be closed")
	}
}

// TestNetCoreSendAfterCloseReturnsFalse verifies that Send() on a closed
// NetCore returns false without panicking.
func TestNetCoreSendAfterCloseReturnsFalse(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(5, conn, Inbound, NetCoreOpts{})
	pc.Close()

	if pc.Send(protocol.Frame{Type: "ping"}) == sendOK {
		t.Fatal("Send on closed NetCore should not return sendOK")
	}
}

// TestNetCoreHasCapability verifies capability lookup.
func TestNetCoreHasCapability(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(6, conn, Inbound, NetCoreOpts{})
	defer pc.Close()

	pc.SetCapabilities([]domain.Capability{domain.CapFileTransferV1, domain.CapMeshRelayV1})

	if !pc.HasCapability(domain.CapFileTransferV1) {
		t.Fatal("should have file_transfer_v1")
	}
	if !pc.HasCapability(domain.CapMeshRelayV1) {
		t.Fatal("should have mesh_relay_v1")
	}
	if pc.HasCapability(domain.CapMeshRoutingV1) {
		t.Fatal("should not have mesh_routing_v1")
	}
}

// TestNetCoreIdentityLifecycle verifies the identity set/get flow.
func TestNetCoreIdentityLifecycle(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(7, conn, Inbound, NetCoreOpts{})
	defer pc.Close()

	if pc.Identity() != "" {
		t.Fatal("identity should be empty before handshake")
	}

	pc.SetIdentity("abc123fingerprint")
	if pc.Identity() != "abc123fingerprint" {
		t.Fatalf("expected abc123fingerprint, got %s", pc.Identity())
	}
}

// TestNetCoreConcurrentSendNoRace runs multiple goroutines calling Send()
// concurrently to verify there are no data races.
func TestNetCoreConcurrentSendNoRace(t *testing.T) {
	conn := &mockConn{}
	pc := newNetCore(8, conn, Inbound, NetCoreOpts{})
	defer pc.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc.Send(protocol.Frame{Type: "ping"})
		}()
	}
	wg.Wait()
}

// TestSendStatusStringCoversAllValues verifies that every sendStatus constant
// has a human-readable label and that the zero value is clearly marked invalid.
func TestSendStatusStringCoversAllValues(t *testing.T) {
	cases := []struct {
		s    sendStatus
		want string
	}{
		{sendStatusInvalid, "INVALID(zero)"},
		{sendOK, "ok"},
		{sendBufferFull, "buffer_full"},
		{sendWriterDone, "writer_done"},
		{sendTimeout, "timeout"},
		{sendChanClosed, "chan_closed"},
		{sendMarshalError, "marshal_error"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("sendStatus(%d).String() = %q, want %q", int(tc.s), got, tc.want)
		}
	}

	// Uninitialised variable must resolve to the invalid sentinel.
	var zero sendStatus
	if zero != sendStatusInvalid {
		t.Fatalf("zero-value sendStatus = %d, want sendStatusInvalid (%d)", int(zero), int(sendStatusInvalid))
	}
	if zero.String() != "INVALID(zero)" {
		t.Fatalf("zero-value String() = %q, want %q", zero.String(), "INVALID(zero)")
	}
}

// saturateSendCh drives NetCore into a fully-saturated backpressure state:
// one item is pulled by the writer (blocked in the caller-owned mockConn
// gate), and sendChBuffer more items fill every buffered slot. The caller
// passes writerStarted so the helper can wait until the writer has
// definitely pulled the first frame before filling the buffer — otherwise
// the first Send might still sit in the channel and the loop would hit
// sendBufferFull one slot early.
func saturateSendCh(t *testing.T, pc *NetCore, writerStarted <-chan struct{}) {
	t.Helper()
	frame := protocol.Frame{Type: "ping"}
	if st := pc.Send(frame); st != sendOK {
		t.Fatalf("saturateSendCh: first Send: got %s, want sendOK", st.String())
	}
	<-writerStarted
	for i := 0; i < sendChBuffer; i++ {
		if st := pc.Send(frame); st != sendOK {
			t.Fatalf("saturateSendCh: Send #%d: got %s, want sendOK (buffer not full yet)", i, st.String())
		}
	}
	if st := pc.Send(frame); st != sendBufferFull {
		t.Fatalf("saturateSendCh: Send on full queue: got %s, want sendBufferFull", st.String())
	}
}

// TestNetCoreSendRawSyncFastFailsOnFullQueue pins the inbound error-path
// contract: when the send channel is saturated, sendRawSync must return
// sendBufferFull immediately so enqueueFrameSync can evict the slow peer
// rather than block for seconds on a malformed/oversized frame response.
func TestNetCoreSendRawSyncFastFailsOnFullQueue(t *testing.T) {
	writerStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	conn := &mockConn{
		writeFn: func(b []byte) (int, error) {
			once.Do(func() { close(writerStarted) })
			<-release
			return len(b), nil
		},
	}
	pc := newNetCore(41, conn, Inbound, NetCoreOpts{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		pc.Close()
	}()

	saturateSendCh(t, pc, writerStarted)

	line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "ping"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	start := time.Now()
	st := pc.sendRawSync([]byte(line))
	elapsed := time.Since(start)

	if st != sendBufferFull {
		t.Fatalf("sendRawSync on saturated queue: got %s, want sendBufferFull (fast-fail)", st.String())
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("sendRawSync took %v — should be fast-fail, not blocking", elapsed)
	}
}

// TestNetCoreSendRawSyncBlockingDoesNotStarveOnFullQueue verifies that
// outbound control-plane writes are not starved by fire-and-forget traffic
// already queued on sendCh. When the buffer is fully saturated,
// sendRawSyncBlocking must wait for a slot rather than fail fast with
// sendBufferFull (that signal remains reserved for the best-effort path
// used by slow-peer eviction). Once the writer drains one item,
// sendRawSyncBlocking proceeds and returns sendOK.
func TestNetCoreSendRawSyncBlockingDoesNotStarveOnFullQueue(t *testing.T) {
	writerStarted := make(chan struct{})
	release := make(chan struct{})
	writes := make(chan struct{}, sendChBuffer+2)
	var once sync.Once
	conn := &mockConn{
		writeFn: func(b []byte) (int, error) {
			once.Do(func() { close(writerStarted) })
			<-release
			writes <- struct{}{}
			return len(b), nil
		},
	}
	pc := newNetCore(42, conn, Outbound, NetCoreOpts{})
	defer func() {
		// Release the writer (if still waiting) before closing so Close()
		// can drain sendCh and the writer goroutine can exit cleanly.
		select {
		case <-release:
		default:
			close(release)
		}
		pc.Close()
	}()

	saturateSendCh(t, pc, writerStarted)

	// Blocking sync path must NOT fail fast on a saturated queue.
	syncResult := make(chan sendStatus, 1)
	go func() {
		line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "subscribe_inbox"})
		if err != nil {
			syncResult <- sendMarshalError
			return
		}
		syncResult <- pc.sendRawSyncBlocking([]byte(line))
	}()

	select {
	case st := <-syncResult:
		t.Fatalf("sendRawSyncBlocking returned early (%s) while queue was saturated — "+
			"control-plane must block, not fail fast", st.String())
	case <-time.After(50 * time.Millisecond):
		// Expected: still waiting for a slot.
	}

	// Let the writer drain items one by one. Each drained item frees a
	// sendCh slot; at least one is enough for sendRawSyncBlocking to enqueue.
	close(release)

	select {
	case st := <-syncResult:
		if st != sendOK {
			t.Fatalf("sendRawSyncBlocking: got %s, want sendOK", st.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendRawSyncBlocking did not complete after writer drained the queue")
	}

	// Drain writes counter to avoid blocking the writer — we've already
	// verified the control-plane contract.
	for {
		select {
		case <-writes:
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

// TestNetCoreIsLocal verifies IsLocal/SetLocal behavior.
func TestNetCoreIsLocal(t *testing.T) {
	conn := &mockConn{}
	nc := newNetCore(1, conn, Inbound, NetCoreOpts{})
	defer nc.Close()

	if nc.IsLocal() {
		t.Fatal("new NetCore should not be local by default")
	}

	nc.SetLocal(true)
	if !nc.IsLocal() {
		t.Fatal("IsLocal should return true after SetLocal(true)")
	}
}
