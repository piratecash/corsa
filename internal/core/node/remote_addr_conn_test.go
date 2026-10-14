package node

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// remoteAddrConn is the one fixture conn in this package's tests that reports
// a remote address of the test's choosing. Code under test keys inbound peers,
// ban scores and subnet groups on RemoteAddr, and a bare net.Pipe answers
// "pipe", which no host:port parser accepts — so a fixture that needs a
// routable peer wraps its transport in this.
//
// Everything other than RemoteAddr goes to the transport underneath, and
// there are exactly two kinds of transport, chosen by the constructor:
//
//   - withRemoteAddr wraps a real conn whose far end the test reads: bytes on
//     the wire are part of what the test asserts.
//   - newNoIOConn has no wire at all: any Read or Write fails the test that
//     built it, and Close succeeds. A fixture that only has to be resolvable
//     (a registry key, a NetCore whose remote address a lookup reads) uses
//     this, so a code path that starts writing to it turns the test red at
//     once instead of blocking on a pipe nobody reads until a deadline or
//     the cleanup quietly unblocks it.
//
// Detection on a no-I/O conn is certain only for synchronous NetCore sends
// (SendSync/SendRawSync, e.g. via enqueueFrameSyncByID), which wait for the
// writer to take the frame. An asynchronous send is caught or missed
// depending on whether the writer dequeues the frame before the cleanup closes
// the core — a frame still queued then is discarded, as in production, and
// never reaches the conn — so in practice it is missed far more often than
// caught. "Any unexpected async
// send turns the test red" is NOT a guarantee this fixture gives; a test that
// must prove nothing was sent asynchronously has to observe that itself.
//
// A no-I/O conn reports through the t it was built with, from the NetCore
// writer goroutine. That is safe only because the core over it is built by
// newTestNetCore with the same t (or a subtest's t): the cleanup Close waits
// for the writer to exit, so every report lands before the test is marked
// done. Never hand a no-I/O conn to production code that builds its own core
// around it (registerInboundConn, handleConn): that core's writer outlives the
// test, and a late write would call Errorf after the test completed and panic.
//
// Close is always safe — newTestNetCore closes every core on cleanup, and the
// core closes its conn.
type remoteAddrConn struct {
	net.Conn
	remote net.Addr
}

func (c *remoteAddrConn) RemoteAddr() net.Addr { return c.remote }

// withRemoteAddr reports remote over conn, a transport the test owns and reads.
func withRemoteAddr(conn net.Conn, remote net.Addr) *remoteAddrConn {
	return &remoteAddrConn{Conn: conn, remote: remote}
}

// newNoIOConn is a conn from remote that carries no bytes; see remoteAddrConn.
func newNoIOConn(t ioFailureReporter, remote net.Addr) *remoteAddrConn {
	return &remoteAddrConn{Conn: ioForbiddenConn{t: t, remote: remote}, remote: remote}
}

// ioFailureReporter is the part of *testing.T an ioForbiddenConn reports
// through. It is an interface so the fixture's own test can observe the
// report instead of failing; a recorder with no testing.TB underneath cannot
// satisfy testing.TB (TB has an unexported method).
type ioFailureReporter interface {
	Helper()
	Errorf(format string, args ...any)
}

// errFixtureCarriesNoIO is what a Read or Write on a no-I/O fixture returns,
// after it has already failed the test: the error makes the writer treat the
// socket as dead, so one unexpected frame is reported once rather than per
// queued frame.
var errFixtureCarriesNoIO = errors.New("fixture conn carries no I/O")

// noIOFrameExcerpt bounds how much of an unexpected write the failure quotes:
// enough to name the frame type, not a whole payload.
const noIOFrameExcerpt = 120

// ioForbiddenConn is the transport of newNoIOConn. Reads and writes fail the
// test that built it. The calls a NetCore and the registry make on every conn
// regardless of traffic — deadlines, Close, the addresses — succeed, because
// they say nothing about bytes on the wire.
type ioForbiddenConn struct {
	t      ioFailureReporter
	remote net.Addr
}

func (c ioForbiddenConn) Read([]byte) (int, error) {
	c.t.Helper()
	c.t.Errorf("unexpected Read on the no-I/O fixture conn from %s: the code under test reads a connection this test says carries nothing", c.remote)
	return 0, errFixtureCarriesNoIO
}

func (c ioForbiddenConn) Write(b []byte) (int, error) {
	c.t.Helper()
	excerpt := b
	if len(excerpt) > noIOFrameExcerpt {
		excerpt = excerpt[:noIOFrameExcerpt]
	}
	c.t.Errorf("unexpected Write of %d bytes to the no-I/O fixture conn from %s: the code under test sends on a connection this test says carries nothing: %q",
		len(b), c.remote, excerpt)
	return 0, errFixtureCarriesNoIO
}

func (c ioForbiddenConn) Close() error        { return nil }
func (c ioForbiddenConn) LocalAddr() net.Addr { return hostPortAddr("127.0.0.1:0") }

// RemoteAddr is shadowed by remoteAddrConn and never reached through it; it
// exists because net.Conn requires it, and answers the same address.
func (c ioForbiddenConn) RemoteAddr() net.Addr { return c.remote }

func (c ioForbiddenConn) SetDeadline(time.Time) error      { return nil }
func (c ioForbiddenConn) SetReadDeadline(time.Time) error  { return nil }
func (c ioForbiddenConn) SetWriteDeadline(time.Time) error { return nil }

// hostPortAddr is a net.Addr that is a host:port string and nothing else.
type hostPortAddr string

func (a hostPortAddr) Network() string { return "tcp" }
func (a hostPortAddr) String() string  { return string(a) }

// recordedFailures is an ioFailureReporter that records instead of failing, so
// the no-I/O fixture's own report can be asserted. The NetCore writer reports
// from its own goroutine, hence the mutex.
type recordedFailures struct {
	mu       sync.Mutex
	messages []string
}

func (r *recordedFailures) Helper() {}

func (r *recordedFailures) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, fmt.Sprintf(format, args...))
}

func (r *recordedFailures) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.messages...)
}

// A frame sent on a NetCore over a no-I/O fixture fails the test that built
// the fixture, once, and kills the writer — instead of blocking on a pipe
// nobody reads, which a test asserting "nothing was sent" could not tell from
// silence.
func TestANoIOConnFailsTheTestOnAnUnexpectedWrite(t *testing.T) {
	t.Parallel()

	reported := &recordedFailures{}
	remote := &net.TCPAddr{IP: net.ParseIP("10.0.0.61"), Port: 40061}
	core := newTestNetCore(t, netcore.ConnID(9610), newNoIOConn(reported, remote), netcore.Inbound, netcore.Options{})

	if status := core.Send(protocol.Frame{Type: "ping"}); status != netcore.SendOK {
		t.Fatalf("Send = %v, want SendOK: the frame has to reach the writer for the fixture to see it", status)
	}
	select {
	case <-core.WriterDone():
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never gave up on the no-I/O conn")
	}

	messages := reported.snapshot()
	if len(messages) != 1 {
		t.Fatalf("the fixture reported %d failures, want exactly 1: %q", len(messages), messages)
	}
	if !strings.Contains(messages[0], "unexpected Write") || !strings.Contains(messages[0], `\"type\":\"ping\"`) {
		t.Fatalf("the report does not name the write and the frame: %q", messages[0])
	}
}

func TestANoIOConnFailsTheTestOnAnUnexpectedRead(t *testing.T) {
	t.Parallel()

	reported := &recordedFailures{}
	conn := newNoIOConn(reported, &net.TCPAddr{IP: net.ParseIP("10.0.0.62"), Port: 40062})

	if _, err := conn.Read(make([]byte, 16)); !errors.Is(err, errFixtureCarriesNoIO) {
		t.Fatalf("Read error = %v, want errFixtureCarriesNoIO", err)
	}
	if messages := reported.snapshot(); len(messages) != 1 || !strings.Contains(messages[0], "unexpected Read") {
		t.Fatalf("the fixture reported %q, want one unexpected Read", messages)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close = %v, want nil: closing a no-I/O conn is how every test ends", err)
	}
}
