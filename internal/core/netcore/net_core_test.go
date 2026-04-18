package netcore

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/netmocks"
)

// connBuffer is a thread-safe write buffer used alongside netmocks.MockConn.
type connBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (cb *connBuffer) Written() []byte {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.buf.Bytes()
}

// newBufferedMockConn creates a MockConn that captures all Write calls into a
// shared connBuffer. The caller inspects cb.Written() instead of the old
// mockConn.Written().
func newBufferedMockConn(t *testing.T) (*netmocks.MockConn, *connBuffer) {
	t.Helper()
	cb := &connBuffer{}
	m := netmocks.NewMockConn(t)
	m.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		return cb.buf.Write(b)
	}).Maybe()
	m.On("Read", mock.Anything).Return(0, nil).Maybe()
	m.On("Close").Return(nil).Maybe()
	m.On("LocalAddr").Return(&net.TCPAddr{}).Maybe()
	m.On("RemoteAddr").Return(&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 64646}).Maybe()
	m.On("SetDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetReadDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetWriteDeadline", mock.Anything).Return(nil).Maybe()
	return m, cb
}

// newMockConnWithWriter creates a MockConn with a custom write function.
func newMockConnWithWriter(t *testing.T, writeFn func([]byte) (int, error)) *netmocks.MockConn {
	t.Helper()
	m := netmocks.NewMockConn(t)
	m.EXPECT().Write(mock.Anything).RunAndReturn(writeFn).Maybe()
	m.On("Read", mock.Anything).Return(0, nil).Maybe()
	m.On("Close").Return(nil).Maybe()
	m.On("LocalAddr").Return(&net.TCPAddr{}).Maybe()
	m.On("RemoteAddr").Return(&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 64646}).Maybe()
	m.On("SetDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetReadDeadline", mock.Anything).Return(nil).Maybe()
	m.On("SetWriteDeadline", mock.Anything).Return(nil).Maybe()
	return m
}

// TestNetCoreSendWritesToSocket verifies that Send() routes a frame through
// the writer goroutine to the underlying socket.
func TestNetCoreSendWritesToSocket(t *testing.T) {
	conn, cb := newBufferedMockConn(t)
	pc := New(1, conn, Inbound, Options{})
	defer pc.Close()

	frame := protocol.Frame{Type: "ping"}
	if pc.Send(frame) != SendOK {
		t.Fatal("Send did not return SendOK")
	}

	// Give the writer goroutine time to drain.
	time.Sleep(50 * time.Millisecond)

	written := cb.Written()
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
	conn, cb := newBufferedMockConn(t)
	pc := New(2, conn, Inbound, Options{})
	defer pc.Close()

	frame := protocol.Frame{Type: "pong"}
	if pc.SendSync(frame) != SendOK {
		t.Fatal("SendSync did not return SendOK")
	}

	// After SendSync returns true, data must already be in the socket.
	written := cb.Written()
	if !bytes.Contains(written, []byte(`"type":"pong"`)) {
		t.Fatalf("expected pong frame written, got: %s", written)
	}
}

// TestNetCoreWriteDeadlinePerDirection verifies that New selects
// the per-write socket deadline based on Direction: outbound connections
// must use the shorter sessionWriteTimeout so that slow-peer eviction for
// dialled sessions keeps the same back-pressure window it had before
// outbound writes were routed through the managed send path.
func TestNetCoreWriteDeadlinePerDirection(t *testing.T) {
	inConn, _ := newBufferedMockConn(t)
	inbound := New(100, inConn, Inbound, Options{})
	defer inbound.Close()
	if inbound.writeDeadline != connWriteTimeout {
		t.Fatalf("inbound writeDeadline = %v, want %v", inbound.writeDeadline, connWriteTimeout)
	}

	outConn, _ := newBufferedMockConn(t)
	outbound := New(101, outConn, Outbound, Options{})
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
	conn := newMockConnWithWriter(t, func(b []byte) (int, error) {
		once.Do(func() { close(writerStarted) })
		<-blocker
		return len(b), nil
	})
	pc := New(3, conn, Inbound, Options{})
	defer func() {
		close(blocker)
		pc.Close()
	}()

	// Send the first item and wait for the writer to pull it.
	frame := protocol.Frame{Type: "ping"}
	if pc.Send(frame) != SendOK {
		t.Fatal("first Send should succeed")
	}
	<-writerStarted

	// Now fill the remaining sendChBuffer buffered slots.
	for i := 0; i < sendChBuffer; i++ {
		if pc.Send(frame) != SendOK {
			t.Fatalf("Send failed at item %d (channel not full yet)", i)
		}
	}

	// Channel is now full (128 buffered + writer blocked on 1st). Next must fail.
	if pc.Send(frame) == SendOK {
		t.Fatal("Send returned SendOK when channel should be full")
	}
}

// TestNetCoreCloseIdempotent verifies that Close() can be called multiple
// times without panicking.
func TestNetCoreCloseIdempotent(t *testing.T) {
	conn, _ := newBufferedMockConn(t)
	pc := New(4, conn, Inbound, Options{})

	pc.Close()
	pc.Close() // must not panic

	conn.AssertCalled(t, "Close")
}

// TestNetCoreSendAfterCloseReturnsFalse verifies that Send() on a closed
// NetCore returns false without panicking.
func TestNetCoreSendAfterCloseReturnsFalse(t *testing.T) {
	conn, _ := newBufferedMockConn(t)
	pc := New(5, conn, Inbound, Options{})
	pc.Close()

	if pc.Send(protocol.Frame{Type: "ping"}) == SendOK {
		t.Fatal("Send on closed NetCore should not return SendOK")
	}
}

// TestNetCoreHasCapability verifies capability lookup.
func TestNetCoreHasCapability(t *testing.T) {
	conn, _ := newBufferedMockConn(t)
	pc := New(6, conn, Inbound, Options{})
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
	conn, _ := newBufferedMockConn(t)
	pc := New(7, conn, Inbound, Options{})
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
	conn, _ := newBufferedMockConn(t)
	pc := New(8, conn, Inbound, Options{})
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

// TestSendStatusStringCoversAllValues verifies that every SendStatus constant
// has a human-readable label and that the zero value is clearly marked invalid.
func TestSendStatusStringCoversAllValues(t *testing.T) {
	cases := []struct {
		s    SendStatus
		want string
	}{
		{SendStatusInvalid, "INVALID(zero)"},
		{SendOK, "ok"},
		{SendBufferFull, "buffer_full"},
		{SendWriterDone, "writer_done"},
		{SendTimeout, "timeout"},
		{SendChanClosed, "chan_closed"},
		{SendMarshalError, "marshal_error"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("SendStatus(%d).String() = %q, want %q", int(tc.s), got, tc.want)
		}
	}

	// Uninitialised variable must resolve to the invalid sentinel.
	var zero SendStatus
	if zero != SendStatusInvalid {
		t.Fatalf("zero-value SendStatus = %d, want SendStatusInvalid (%d)", int(zero), int(SendStatusInvalid))
	}
	if zero.String() != "INVALID(zero)" {
		t.Fatalf("zero-value String() = %q, want %q", zero.String(), "INVALID(zero)")
	}
}

// saturateSendCh drives NetCore into a fully-saturated backpressure state:
// one item is pulled by the writer (blocked in the caller-owned MockConn
// gate), and sendChBuffer more items fill every buffered slot. The caller
// passes writerStarted so the helper can wait until the writer has
// definitely pulled the first frame before filling the buffer — otherwise
// the first Send might still sit in the channel and the loop would hit
// SendBufferFull one slot early.
func saturateSendCh(t *testing.T, pc *NetCore, writerStarted <-chan struct{}) {
	t.Helper()
	frame := protocol.Frame{Type: "ping"}
	if st := pc.Send(frame); st != SendOK {
		t.Fatalf("saturateSendCh: first Send: got %s, want SendOK", st.String())
	}
	<-writerStarted
	for i := 0; i < sendChBuffer; i++ {
		if st := pc.Send(frame); st != SendOK {
			t.Fatalf("saturateSendCh: Send #%d: got %s, want SendOK (buffer not full yet)", i, st.String())
		}
	}
	if st := pc.Send(frame); st != SendBufferFull {
		t.Fatalf("saturateSendCh: Send on full queue: got %s, want SendBufferFull", st.String())
	}
}

// TestNetCoreSendRawSyncFastFailsOnFullQueue pins the inbound error-path
// contract: when the send channel is saturated, SendRawSync must return
// SendBufferFull immediately so the caller can evict the slow peer
// rather than block for seconds on a malformed/oversized frame response.
func TestNetCoreSendRawSyncFastFailsOnFullQueue(t *testing.T) {
	writerStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	conn := newMockConnWithWriter(t, func(b []byte) (int, error) {
		once.Do(func() { close(writerStarted) })
		<-release
		return len(b), nil
	})
	pc := New(41, conn, Inbound, Options{})
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
	st := pc.SendRawSync([]byte(line))
	elapsed := time.Since(start)

	if st != SendBufferFull {
		t.Fatalf("SendRawSync on saturated queue: got %s, want SendBufferFull (fast-fail)", st.String())
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("SendRawSync took %v — should be fast-fail, not blocking", elapsed)
	}
}

// TestNetCoreSendRawSyncBlockingDoesNotStarveOnFullQueue verifies that
// outbound control-plane writes are not starved by fire-and-forget traffic
// already queued on sendCh. When the buffer is fully saturated,
// SendRawSyncBlocking must wait for a slot rather than fail fast with
// SendBufferFull (that signal remains reserved for the best-effort path
// used by slow-peer eviction). Once the writer drains one item,
// SendRawSyncBlocking proceeds and returns SendOK.
func TestNetCoreSendRawSyncBlockingDoesNotStarveOnFullQueue(t *testing.T) {
	writerStarted := make(chan struct{})
	release := make(chan struct{})
	writes := make(chan struct{}, sendChBuffer+2)
	var once sync.Once
	conn := newMockConnWithWriter(t, func(b []byte) (int, error) {
		once.Do(func() { close(writerStarted) })
		<-release
		writes <- struct{}{}
		return len(b), nil
	})
	pc := New(42, conn, Outbound, Options{})
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
	syncResult := make(chan SendStatus, 1)
	go func() {
		line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "subscribe_inbox"})
		if err != nil {
			syncResult <- SendMarshalError
			return
		}
		syncResult <- pc.SendRawSyncBlocking([]byte(line))
	}()

	select {
	case st := <-syncResult:
		t.Fatalf("SendRawSyncBlocking returned early (%s) while queue was saturated — "+
			"control-plane must block, not fail fast", st.String())
	case <-time.After(50 * time.Millisecond):
		// Expected: still waiting for a slot.
	}

	// Let the writer drain items one by one. Each drained item frees a
	// sendCh slot; at least one is enough for SendRawSyncBlocking to enqueue.
	close(release)

	select {
	case st := <-syncResult:
		if st != SendOK {
			t.Fatalf("SendRawSyncBlocking: got %s, want SendOK", st.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendRawSyncBlocking did not complete after writer drained the queue")
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
	conn, _ := newBufferedMockConn(t)
	nc := New(1, conn, Inbound, Options{})
	defer nc.Close()

	if nc.IsLocal() {
		t.Fatal("new NetCore should not be local by default")
	}

	nc.SetLocal(true)
	if !nc.IsLocal() {
		t.Fatal("IsLocal should return true after SetLocal(true)")
	}
}
