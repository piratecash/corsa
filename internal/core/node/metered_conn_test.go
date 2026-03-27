package node

import (
	"io"
	"net"
	"testing"
	"time"
)

// pipeConn wraps net.Conn returned by net.Pipe so it satisfies the interface.
// net.Pipe already returns net.Conn, but we use this alias for clarity.

func TestMeteredConnRead(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	metered := NewMeteredConn(client)

	payload := []byte("hello world")
	go func() {
		_, _ = server.Write(payload)
	}()

	buf := make([]byte, 64)
	n, err := metered.Read(buf)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes read, got %d", len(payload), n)
	}
	if metered.BytesRead() != int64(len(payload)) {
		t.Fatalf("expected BytesRead=%d, got %d", len(payload), metered.BytesRead())
	}
	if metered.BytesWritten() != 0 {
		t.Fatalf("expected BytesWritten=0, got %d", metered.BytesWritten())
	}
}

func TestMeteredConnWrite(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	metered := NewMeteredConn(client)

	payload := []byte("test message")
	go func() {
		buf := make([]byte, 64)
		_, _ = server.Read(buf)
	}()

	n, err := metered.Write(payload)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}
	if metered.BytesWritten() != int64(len(payload)) {
		t.Fatalf("expected BytesWritten=%d, got %d", len(payload), metered.BytesWritten())
	}
	if metered.BytesRead() != 0 {
		t.Fatalf("expected BytesRead=0, got %d", metered.BytesRead())
	}
}

func TestMeteredConnBidirectional(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	metered := NewMeteredConn(client)

	outgoing := []byte("request data")
	incoming := []byte("response data from server")

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		n, _ := server.Read(buf)
		if string(buf[:n]) != string(outgoing) {
			t.Errorf("server received unexpected data: %s", buf[:n])
		}
		_, _ = server.Write(incoming)
	}()

	if _, err := metered.Write(outgoing); err != nil {
		t.Fatalf("write error: %v", err)
	}

	buf := make([]byte, 64)
	n, err := metered.Read(buf)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(buf[:n]) != string(incoming) {
		t.Fatalf("unexpected response: %s", buf[:n])
	}

	<-done

	if metered.BytesWritten() != int64(len(outgoing)) {
		t.Fatalf("expected BytesWritten=%d, got %d", len(outgoing), metered.BytesWritten())
	}
	if metered.BytesRead() != int64(len(incoming)) {
		t.Fatalf("expected BytesRead=%d, got %d", len(incoming), metered.BytesRead())
	}
}

func TestMeteredConnMultipleOperations(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	metered := NewMeteredConn(client)

	messages := []string{"first", "second", "third"}
	var totalWritten int

	go func() {
		buf := make([]byte, 256)
		for {
			_, err := server.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	for _, msg := range messages {
		data := []byte(msg)
		n, err := metered.Write(data)
		if err != nil {
			t.Fatalf("write error on %q: %v", msg, err)
		}
		totalWritten += n
	}

	if metered.BytesWritten() != int64(totalWritten) {
		t.Fatalf("expected cumulative BytesWritten=%d, got %d", totalWritten, metered.BytesWritten())
	}
}

func TestMeteredConnEOFCountsCorrectly(t *testing.T) {
	server, client := net.Pipe()

	metered := NewMeteredConn(client)

	payload := []byte("data before close")
	go func() {
		_, _ = server.Write(payload)
		_ = server.Close()
	}()

	buf := make([]byte, 64)
	n, _ := metered.Read(buf)
	if n != len(payload) {
		t.Fatalf("expected %d bytes, got %d", len(payload), n)
	}

	// second read should get EOF but BytesRead should remain accurate
	_, err := metered.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}

	if metered.BytesRead() != int64(len(payload)) {
		t.Fatalf("expected BytesRead=%d after EOF, got %d", len(payload), metered.BytesRead())
	}

	_ = client.Close()
}

func TestMeteredConnConcurrentAccess(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = client.Close() }()

	metered := NewMeteredConn(client)

	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()

	go func() {
		for {
			_, err := server.Write([]byte("x"))
			if err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	done := make(chan struct{})
	// concurrent writes
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_, _ = metered.Write([]byte("w"))
		}
	}()

	// concurrent reads
	buf := make([]byte, 1)
	for i := 0; i < 50; i++ {
		_, _ = metered.Read(buf)
	}

	<-done

	if metered.BytesWritten() != 100 {
		t.Fatalf("expected 100 bytes written, got %d", metered.BytesWritten())
	}
	if metered.BytesRead() < 50 {
		t.Fatalf("expected at least 50 bytes read, got %d", metered.BytesRead())
	}
}
