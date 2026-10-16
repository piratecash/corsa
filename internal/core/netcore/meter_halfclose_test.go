package netcore

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// A listener refusing a connection sends its last bytes, half-closes and
// still reads what the peer had sent: through the meter the FIN reaches the
// peer while the read side stays open, and a socket that cannot half-close
// says so instead of closing.
func TestMeteredConnHalfClosesTheSocketUnderneath(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	server, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	var totals TransportTotals
	metered, err := NewMeteredConn(server, &totals)
	if err != nil {
		t.Fatalf("meter: %v", err)
	}
	defer func() { _ = metered.Close() }()

	if _, err := metered.Write([]byte("bye")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := metered.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := io.ReadAll(client)
	if err != nil || string(got) != "bye" {
		t.Fatalf("the peer read %q then %v, want the bytes and a FIN", got, err)
	}
	if _, err := client.Write([]byte("late")); err != nil {
		t.Fatalf("the peer's write after the half-close: %v", err)
	}
	buf := make([]byte, 4)
	_ = metered.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(metered, buf); err != nil || string(buf) != "late" {
		t.Fatalf("read after the half-close: %q, %v", buf, err)
	}

	pipeEnd, other := net.Pipe()
	defer func() { _ = other.Close() }()
	piped, err := NewMeteredConn(pipeEnd, &totals)
	if err != nil {
		t.Fatalf("meter the pipe: %v", err)
	}
	defer func() { _ = piped.Close() }()
	if err := piped.CloseWrite(); !errors.Is(err, ErrNoHalfClose) {
		t.Fatalf("CloseWrite on a pipe = %v, want ErrNoHalfClose", err)
	}
}
