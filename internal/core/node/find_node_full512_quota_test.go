package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/protocol"
)

var errFull512AppQuota = errors.New("full512 application traffic quota exceeded")

type full512AppQuota struct {
	mu                       sync.Mutex
	frameLimit, payloadLimit int
	frames, payload          int
	exceeded                 bool
}

func (q *full512AppQuota) admit(data []byte) error {
	frames := bytes.Count(data, []byte{'\n'})
	payload := len(data) - frames
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.exceeded || frames > q.frameLimit-q.frames || payload > q.payloadLimit-q.payload {
		q.exceeded = true
		return errFull512AppQuota
	}
	q.frames += frames
	q.payload += payload
	return nil
}

func (q *full512AppQuota) snapshot() (frames, payload int, exceeded bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.frames, q.payload, q.exceeded
}

type full512AppConn struct {
	net.Conn
	quota *full512AppQuota
}

func (c full512AppConn) Read(data []byte) (int, error) {
	if _, _, exceeded := c.quota.snapshot(); exceeded {
		return 0, errFull512AppQuota
	}
	n, err := c.Conn.Read(data)
	if n > 0 {
		if quotaErr := c.quota.admit(data[:n]); quotaErr != nil {
			return 0, quotaErr
		}
	}
	return n, err
}

type full512AppDrain struct {
	net.Conn
	cancel context.CancelFunc
}

func (c full512AppDrain) Close() error {
	c.cancel()
	return c.Conn.Close()
}

func drainFull512Application(t *testing.T, conn net.Conn, reader *bufio.Reader) net.Conn {
	t.Helper()
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	client := full512AppDrain{Conn: conn, cancel: cancel}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("application reader did not join")
		}
	})
	go func() {
		defer close(done)
		report := func(err error) {
			if ctx.Err() == nil {
				t.Error("application reader stopped", err)
			}
		}
		for {
			line, err := reader.ReadSlice('\n')
			if err != nil {
				report(err)
				return
			}
			frame, err := protocol.ParseFrameLine(string(line[:len(line)-1]))
			if err != nil {
				report(err)
				return
			}
			if frame.Type != "ping" {
				continue
			}
			if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
				report(err)
				return
			}
			pong, err := protocol.MarshalFrameLine(protocol.Frame{Type: "pong"})
			if err != nil {
				report(err)
				return
			}
			if _, err := io.WriteString(conn, pong); err != nil {
				report(err)
				return
			}
		}
	}()
	return client
}

func (c full512AppConn) Write(data []byte) (int, error) {
	// Charge before writing. An underlying write error invalidates the run.
	if err := c.quota.admit(data); err != nil {
		return 0, err
	}
	return c.Conn.Write(data)
}

func TestFull512ApplicationQuotaFragmentedLines(t *testing.T) {
	q := &full512AppQuota{frameLimit: 2, payloadLimit: 5}
	for _, chunk := range []string{"a", "b\nc", "de", "\n"} {
		if err := q.admit([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if frames, payload, exceeded := q.snapshot(); frames != 2 || payload != 5 || exceeded {
		t.Fatalf("quota state = %d, %d, %v", frames, payload, exceeded)
	}
	if err := q.admit([]byte{'\n'}); !errors.Is(err, errFull512AppQuota) {
		t.Fatal("accepted an excess frame", err)
	}
	if err := q.admit(nil); !errors.Is(err, errFull512AppQuota) {
		t.Fatal("quota failure was lost", err)
	}
}

func TestFull512ApplicationQuotaConcurrentAdmission(t *testing.T) {
	q := &full512AppQuota{frameLimit: 16, payloadLimit: 8}
	var work sync.WaitGroup
	for range 16 {
		work.Go(func() { _ = q.admit([]byte("x\n")) })
	}
	work.Wait()
	if frames, payload, exceeded := q.snapshot(); frames != 8 || payload != 8 || !exceeded {
		t.Fatalf("concurrent quota state = %d, %d, %v", frames, payload, exceeded)
	}
}

func TestFull512ApplicationQuotaReadAndWrite(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	q := &full512AppQuota{frameLimit: 2, payloadLimit: 3}
	conn := full512AppConn{Conn: left, quota: q}
	sent := make(chan error, 1)
	go func() {
		_, err := right.Write([]byte("ab\n"))
		sent <- err
	}()
	if n, err := conn.Read(make([]byte, 8)); n != 3 || err != nil {
		t.Fatalf("inbound application frame = %d, %v", n, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	read := make(chan int, 1)
	go func() {
		n, _ := right.Read(make([]byte, 8))
		read <- n
	}()
	if n, err := conn.Write([]byte("c\n")); n != 2 || err != nil {
		t.Fatalf("outbound application frame = %d, %v", n, err)
	}
	if n := <-read; n != 2 {
		t.Fatal("outbound frame did not cross the original connection", n)
	}
	if n, err := conn.Write([]byte("d")); n != 0 || !errors.Is(err, errFull512AppQuota) {
		t.Fatalf("excess payload reached the underlying writer: %d, %v", n, err)
	}
}

func TestFull512ApplicationQuotaDrain(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	if err := right.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	q := &full512AppQuota{frameLimit: 2, payloadLimit: 100}
	conn := full512AppConn{Conn: left, quota: q}
	client := drainFull512Application(t, conn, bufio.NewReader(conn))
	ping, err := protocol.MarshalFrameLine(protocol.Frame{Type: "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(right, ping); err != nil {
		t.Fatal(err)
	}
	pong, err := bufio.NewReader(right).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ParseFrameLine(pong[:len(pong)-1])
	if err != nil || frame.Type != "pong" {
		t.Fatal("application drain did not return pong", frame.Type, err)
	}
	if frames, payload, exceeded := q.snapshot(); frames != 2 || payload != len(ping)+len(pong)-2 || exceeded {
		t.Fatalf("application drain quota state = %d, %d, %v", frames, payload, exceeded)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
