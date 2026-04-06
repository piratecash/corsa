package node

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
)

// mockConn is a minimal net.Conn for testing PeerConn writer behavior.
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

// TestPeerConnSendWritesToSocket verifies that Send() routes a frame through
// the writer goroutine to the underlying socket.
func TestPeerConnSendWritesToSocket(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(1, conn, Inbound, PeerConnOpts{})
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

// TestPeerConnSendSyncBlocksUntilWrite verifies that SendSync() blocks until
// the writer goroutine has flushed the frame to the socket.
func TestPeerConnSendSyncBlocksUntilWrite(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(2, conn, Inbound, PeerConnOpts{})
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

// TestPeerConnSendReturnsFalseWhenQueueFull verifies that Send() returns false
// instead of blocking when the write channel is full.
func TestPeerConnSendReturnsFalseWhenQueueFull(t *testing.T) {
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
	pc := newPeerConn(3, conn, Inbound, PeerConnOpts{})
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

// TestPeerConnCloseIdemponent verifies that Close() can be called multiple
// times without panicking.
func TestPeerConnCloseIdempotent(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(4, conn, Inbound, PeerConnOpts{})

	pc.Close()
	pc.Close() // must not panic

	if !conn.IsClosed() {
		t.Fatal("underlying connection should be closed")
	}
}

// TestPeerConnSendAfterCloseReturnsFalse verifies that Send() on a closed
// PeerConn returns false without panicking.
func TestPeerConnSendAfterCloseReturnsFalse(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(5, conn, Inbound, PeerConnOpts{})
	pc.Close()

	if pc.Send(protocol.Frame{Type: "ping"}) == sendOK {
		t.Fatal("Send on closed PeerConn should not return sendOK")
	}
}

// TestPeerConnHasCapability verifies capability lookup.
func TestPeerConnHasCapability(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(6, conn, Inbound, PeerConnOpts{})
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

// TestPeerConnIdentityLifecycle verifies the identity set/get flow.
func TestPeerConnIdentityLifecycle(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(7, conn, Inbound, PeerConnOpts{})
	defer pc.Close()

	if pc.Identity() != "" {
		t.Fatal("identity should be empty before handshake")
	}

	pc.SetIdentity("abc123fingerprint")
	if pc.Identity() != "abc123fingerprint" {
		t.Fatalf("expected abc123fingerprint, got %s", pc.Identity())
	}
}

// TestPeerConnConcurrentSendNoRace runs multiple goroutines calling Send()
// concurrently to verify there are no data races.
func TestPeerConnConcurrentSendNoRace(t *testing.T) {
	conn := &mockConn{}
	pc := newPeerConn(8, conn, Inbound, PeerConnOpts{})
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
