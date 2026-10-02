package netcore

import (
	"errors"
	"io"
	"net"
	"testing"
)

// mustMeteredConn is NewMeteredConn for fixtures whose accumulator is known to
// be present.
func mustMeteredConn(t *testing.T, conn net.Conn, totals *TransportTotals) *MeteredConn {
	t.Helper()
	metered, err := NewMeteredConn(conn, totals)
	if err != nil {
		t.Fatalf("NewMeteredConn: %v", err)
	}
	return metered
}

// TestMeteredConnFeedsTransportTotals pins the single point of measurement:
// every byte that crosses a MeteredConn lands in the shared totals exactly
// once, in the direction it travelled, and two connections sharing one
// accumulator add up instead of overwriting each other.
func TestMeteredConnFeedsTransportTotals(t *testing.T) {
	t.Parallel()

	var totals TransportTotals

	firstLocal, firstRemote := net.Pipe()
	secondLocal, secondRemote := net.Pipe()
	t.Cleanup(func() {
		for _, c := range []net.Conn{firstLocal, firstRemote, secondLocal, secondRemote} {
			_ = c.Close()
		}
	})
	first := mustMeteredConn(t, firstLocal, &totals)
	second := mustMeteredConn(t, secondLocal, &totals)

	go func() { _, _ = io.Copy(io.Discard, firstRemote) }()
	go func() { _, _ = io.Copy(io.Discard, secondRemote) }()
	go func() { _, _ = secondRemote.Write(make([]byte, 40)) }()

	if _, err := first.Write(make([]byte, 100)); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := second.Write(make([]byte, 7)); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if _, err := io.ReadFull(second, make([]byte, 40)); err != nil {
		t.Fatalf("read second: %v", err)
	}

	if got, want := totals.BytesSent(), uint64(107); got != want {
		t.Fatalf("BytesSent = %d, want %d", got, want)
	}
	if got, want := totals.BytesReceived(), uint64(40); got != want {
		t.Fatalf("BytesReceived = %d, want %d", got, want)
	}
	// The per-connection counters see the same bytes; the totals are their
	// sum, not an additional count layered on top.
	perConnSent := first.BytesWritten() + second.BytesWritten()
	perConnReceived := first.BytesRead() + second.BytesRead()
	if uint64(perConnSent) != totals.BytesSent() || uint64(perConnReceived) != totals.BytesReceived() {
		t.Fatalf("per-connection sum (%d sent, %d received) differs from totals (%d, %d)",
			perConnSent, perConnReceived, totals.BytesSent(), totals.BytesReceived())
	}
}

// TestTransportTotalsSurviveClose pins that closing a connection — the moment
// the per-peer totals hand bytes from the live session to the health row —
// takes nothing away from the transport totals.
func TestTransportTotalsSurviveClose(t *testing.T) {
	t.Parallel()

	var totals TransportTotals
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = remote.Close() })
	metered := mustMeteredConn(t, local, &totals)

	go func() { _, _ = io.Copy(io.Discard, remote) }()
	if _, err := metered.Write(make([]byte, 64)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := metered.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got, want := totals.BytesSent(), uint64(64); got != want {
		t.Fatalf("BytesSent after close = %d, want %d", got, want)
	}
}

// TestNewMeteredConnRefusesMissingTotals pins the fail-closed constructor: a
// wrapper without an accumulator would crash the I/O goroutine on its first
// byte, so it is refused before it can carry traffic, with an error a caller
// can tell apart by type.
func TestNewMeteredConnRefusesMissingTotals(t *testing.T) {
	t.Parallel()

	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})

	metered, err := NewMeteredConn(local, nil)
	if !errors.Is(err, ErrNilTransportTotals) {
		t.Fatalf("NewMeteredConn(conn, nil) error = %v, want ErrNilTransportTotals", err)
	}
	if metered != nil {
		t.Fatalf("NewMeteredConn(conn, nil) returned a wrapper: %#v", metered)
	}
}

// shortConn accepts at most `limit` bytes per Write and returns at most
// `limit` bytes per Read — a socket under back-pressure, or a deadline that
// fires in the middle of a buffer.
type shortConn struct {
	net.Conn
	limit int
}

func (c shortConn) Write(p []byte) (int, error) {
	if len(p) <= c.limit {
		return c.Conn.Write(p)
	}
	n, err := c.Conn.Write(p[:c.limit])
	if err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (c shortConn) Read(p []byte) (int, error) {
	if len(p) > c.limit {
		p = p[:c.limit]
	}
	return c.Conn.Read(p)
}

// TestMeteredConnCountsWhatTheSocketTook pins the transport semantics of a
// partial operation: the bytes counted are the ones the socket actually
// accepted or delivered (n), not the size of the caller's buffer. Counting
// len(p) would report bytes that never crossed the wire.
func TestMeteredConnCountsWhatTheSocketTook(t *testing.T) {
	t.Parallel()

	var totals TransportTotals
	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	metered := mustMeteredConn(t, shortConn{Conn: local, limit: 10}, &totals)

	go func() { _, _ = io.Copy(io.Discard, remote) }()
	n, err := metered.Write(make([]byte, 64))
	if !errors.Is(err, io.ErrShortWrite) || n != 10 {
		t.Fatalf("Write = (%d, %v), want (10, io.ErrShortWrite)", n, err)
	}
	go func() { _, _ = remote.Write(make([]byte, 64)) }()
	n, err = metered.Read(make([]byte, 64))
	if err != nil || n != 10 {
		t.Fatalf("Read = (%d, %v), want (10, nil)", n, err)
	}

	if got := totals.BytesSent(); got != 10 {
		t.Fatalf("BytesSent = %d after a 10-byte partial write of a 64-byte buffer, want 10", got)
	}
	if got := totals.BytesReceived(); got != 10 {
		t.Fatalf("BytesReceived = %d after a 10-byte read into a 64-byte buffer, want 10", got)
	}
	if metered.BytesWritten() != 10 || metered.BytesRead() != 10 {
		t.Fatalf("per-connection counters = %d written / %d read, want 10 / 10",
			metered.BytesWritten(), metered.BytesRead())
	}
}
