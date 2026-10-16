package netcore

import (
	"errors"
	"net"
	"sync/atomic"
)

// ErrNilTransportTotals is returned by NewMeteredConn when no accumulator is
// supplied. A wrapper without one cannot count its bytes into the node-wide
// totals, so the socket is refused instead of carried uncounted.
var ErrNilTransportTotals = errors.New("netcore: metered connection without transport totals")

// ErrNoHalfClose is returned by MeteredConn.CloseWrite when the socket
// underneath cannot close only its write side.
var ErrNoHalfClose = errors.New("netcore: the connection cannot half-close")

// MeteredConn wraps a net.Conn and transparently counts bytes
// read from and written to the underlying connection.
// All counters are safe for concurrent access via atomic operations.
//
// It is the single point where transport bytes are measured: every byte is
// added to the per-connection counters (per-peer attribution) and to the
// process-wide TransportTotals in the same call, so the two can never
// disagree about what crossed the socket.
type MeteredConn struct {
	net.Conn
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
	totals       *TransportTotals
}

// NewMeteredConn wraps an existing connection with byte counters. totals is
// the process-wide accumulator the same bytes are added to. A nil totals is
// refused with ErrNilTransportTotals: the check runs once here, so the
// Read/Write hot path never has to decide what an absent accumulator means.
// Wrap a socket once: a second wrapper around the same socket would count
// every byte twice.
func NewMeteredConn(conn net.Conn, totals *TransportTotals) (*MeteredConn, error) {
	if totals == nil {
		return nil, ErrNilTransportTotals
	}
	return &MeteredConn{Conn: conn, totals: totals}, nil
}

func (m *MeteredConn) Read(p []byte) (int, error) {
	n, err := m.Conn.Read(p)
	if n > 0 {
		m.bytesRead.Add(int64(n))
		m.totals.addReceived(n)
	}
	return n, err
}

func (m *MeteredConn) Write(p []byte) (int, error) {
	n, err := m.Conn.Write(p)
	if n > 0 {
		m.bytesWritten.Add(int64(n))
		m.totals.addSent(n)
	}
	return n, err
}

// CloseWrite half-closes the socket underneath, so a listener can send its
// last bytes and a FIN while it still reads what the peer had already sent.
// A socket that cannot half-close is reported, not closed.
func (m *MeteredConn) CloseWrite() error {
	halfCloser, ok := m.Conn.(interface{ CloseWrite() error })
	if !ok {
		return ErrNoHalfClose
	}
	return halfCloser.CloseWrite()
}

// BytesRead returns the total number of bytes read from this connection.
func (m *MeteredConn) BytesRead() int64 {
	return m.bytesRead.Load()
}

// BytesWritten returns the total number of bytes written to this connection.
func (m *MeteredConn) BytesWritten() int64 {
	return m.bytesWritten.Load()
}
