package node

import (
	"bytes"
	"net"
	"sync"
	"time"
)

// mockConn is a minimal net.Conn used by node-package tests that need a
// stub transport without touching a real socket. The netcore package keeps
// a parallel copy for its own unit tests — see §2.6.11 on why test fixtures
// are duplicated across the package boundary instead of shared through an
// exported testing helper (exposing mockConn publicly would leak test-only
// plumbing into production API surface).
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
