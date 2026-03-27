package node

import (
	"net"
	"sync/atomic"
)

// MeteredConn wraps a net.Conn and transparently counts bytes
// read from and written to the underlying connection.
// All counters are safe for concurrent access via atomic operations.
type MeteredConn struct {
	net.Conn
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
}

// NewMeteredConn wraps an existing connection with byte counters.
func NewMeteredConn(conn net.Conn) *MeteredConn {
	return &MeteredConn{Conn: conn}
}

func (m *MeteredConn) Read(p []byte) (int, error) {
	n, err := m.Conn.Read(p)
	if n > 0 {
		m.bytesRead.Add(int64(n))
	}
	return n, err
}

func (m *MeteredConn) Write(p []byte) (int, error) {
	n, err := m.Conn.Write(p)
	if n > 0 {
		m.bytesWritten.Add(int64(n))
	}
	return n, err
}

// BytesRead returns the total number of bytes read from this connection.
func (m *MeteredConn) BytesRead() int64 {
	return m.bytesRead.Load()
}

// BytesWritten returns the total number of bytes written to this connection.
func (m *MeteredConn) BytesWritten() int64 {
	return m.bytesWritten.Load()
}
