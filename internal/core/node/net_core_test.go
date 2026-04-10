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
