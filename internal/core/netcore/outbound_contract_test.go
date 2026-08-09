package netcore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// outbound_contract_test.go covers the OUTBOUND CONTRACT of a queued frame:
// the send deadline the writer re-checks immediately before the socket write,
// the per-frame write grace that bounds the write itself, and the queue's own
// refusals.
//
// Every assertion here is made on what the SOCKET saw, on the deadline the
// writer set and on the status the producer was answered — because that is the
// whole observable surface. The contract used to have a second half, a terminal
// outcome delivered back to an observer, and its tests asserted "exactly one
// terminal" instead of any of the above. Nothing in production ever attached an
// observer, so those tests only ever proved that the notification machinery
// notified itself.

// gatedConn is a net.Conn double with a fully controlled Write: the test
// decides when (and with what result) each write completes. It also honours
// the write deadline the writer goroutine sets, so the write-grace contract
// can be exercised without depending on a real socket's timing.
type gatedConn struct {
	mu        sync.Mutex
	attempts  [][]byte    // bytes handed to Write, recorded on ENTRY
	completed [][]byte    // bytes of writes that returned success
	deadlines []time.Time // every SetWriteDeadline argument, in order
	closes    int

	entered chan struct{} // one token per Write entry
	release chan error    // result the test hands back to a blocked Write
	auto    bool          // when true Write returns immediately

	closeOnce sync.Once
	closed    chan struct{}
}

func newGatedConn() *gatedConn {
	return &gatedConn{
		entered: make(chan struct{}, 64),
		release: make(chan error),
		closed:  make(chan struct{}),
	}
}

// newAutoConn returns a conn that accepts every write immediately. Used by
// tests that care about what reached the socket, not about when.
func newAutoConn() *gatedConn {
	c := newGatedConn()
	c.auto = true
	return c
}

func (c *gatedConn) Write(b []byte) (int, error) {
	buf := append([]byte(nil), b...)
	c.mu.Lock()
	c.attempts = append(c.attempts, buf)
	deadline := time.Time{}
	if n := len(c.deadlines); n > 0 {
		deadline = c.deadlines[n-1]
	}
	auto := c.auto
	c.mu.Unlock()

	select {
	case c.entered <- struct{}{}:
	default:
	}

	if auto {
		c.mu.Lock()
		c.completed = append(c.completed, buf)
		c.mu.Unlock()
		return len(b), nil
	}

	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		expired = timer.C
	}

	select {
	case err := <-c.release:
		if err != nil {
			return 0, err
		}
		c.mu.Lock()
		c.completed = append(c.completed, buf)
		c.mu.Unlock()
		return len(b), nil
	case <-expired:
		return 0, os.ErrDeadlineExceeded
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *gatedConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *gatedConn) Read(_ []byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *gatedConn) LocalAddr() net.Addr { return &net.TCPAddr{} }
func (c *gatedConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 64646}
}
func (c *gatedConn) SetDeadline(time.Time) error     { return nil }
func (c *gatedConn) SetReadDeadline(time.Time) error { return nil }
func (c *gatedConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return nil
}

// waitWriteEntered blocks until the writer goroutine is inside Write.
func (c *gatedConn) waitWriteEntered(t *testing.T) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer goroutine never entered Write")
	}
}

func (c *gatedConn) attemptCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.attempts)
}

func (c *gatedConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func (c *gatedConn) lastWriteDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.deadlines) - 1; i >= 0; i-- {
		if !c.deadlines[i].IsZero() {
			return c.deadlines[i], true
		}
	}
	return time.Time{}, false
}

// awaitCompletedWrites blocks until the socket has completed `want` writes and
// returns their bytes, joined in the order they left.
func (c *gatedConn) awaitCompletedWrites(t *testing.T, want int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		got := len(c.completed)
		joined := bytes.Join(c.completed, nil)
		c.mu.Unlock()
		switch {
		case got == want:
			return joined
		case got > want:
			t.Fatalf("socket completed %d writes, want %d: %s", got, want, joined)
		case time.Now().After(deadline):
			t.Fatalf("socket completed %d of %d writes within %v: %s", got, want, timeout, joined)
		}
		time.Sleep(time.Millisecond)
	}
}

// tracked builds a ticket whose contract cannot expire while a test runs. The
// cases that use it are about the QUEUE, so the frame has to be tracked —
// otherwise the ticket is nil and the tracked path is never exercised — without
// the deadline itself deciding anything.
func tracked() *WriteTicket {
	return NewWriteTicket(OutboundWrite{SendUntil: domain.TimeOf(time.Now().Add(time.Hour))})
}

// TestNewWriteTicketEmptyContractIsNil pins the allocation-free legacy
// path: a contract with neither a deadline nor a grace must not produce a
// ticket at all, and the resulting nil ticket must be inert.
func TestNewWriteTicketEmptyContractIsNil(t *testing.T) {
	t.Parallel()
	if ticket := NewWriteTicket(OutboundWrite{}); ticket != nil {
		t.Fatalf("NewWriteTicket(empty) = %v, want nil", ticket)
	}
	// A nil ticket must tolerate every operation the writer performs on it.
	var nilTicket *WriteTicket
	if nilTicket.expiredAt(time.Now()) {
		t.Fatal("nil ticket must never report an expired send deadline")
	}
	now := time.Now()
	if got := nilTicket.writeDeadlineAt(now, connWriteTimeout); !got.Equal(now.Add(connWriteTimeout)) {
		t.Fatalf("nil ticket write deadline = %v, want connection default", got)
	}
}

// TestSendTrackedFullWriteReachesTheSocket covers the happy path: a frame
// inside its contract is written to the socket in full.
func TestSendTrackedFullWriteReachesTheSocket(t *testing.T) {
	t.Parallel()
	conn := newAutoConn()
	pc := New(1, conn, Inbound, Options{})
	defer pc.Close()

	st := pc.SendTracked(protocol.Frame{Type: "ping"}, NewWriteTicket(OutboundWrite{
		SendUntil: domain.TimeOf(time.Now().Add(time.Minute)),
	}))
	if st != SendOK {
		t.Fatalf("SendTracked = %v, want SendOK", st)
	}
	if written := conn.awaitCompletedWrites(t, 1, 2*time.Second); !bytes.Contains(written, []byte(`"type":"ping"`)) {
		t.Fatalf("socket received %s, want the tracked frame", written)
	}
}

// TestOneTicketServesEveryConnectionItIsAttachedTo pins the SHAREABILITY of a
// ticket, which the datagram candidate walk relies on: it builds one ticket per
// send and offers the same pointer to each of the peer's sockets in turn, so a
// ticket that could only be spent once would leave every fallback connection
// writing under the connection default instead of the frame's own grace.
//
// Two connections, one ticket, and the assertion is on the deadline each writer
// really set: a ticket answers the second writer exactly as it answered the
// first.
func TestOneTicketServesEveryConnectionItIsAttachedTo(t *testing.T) {
	t.Parallel()

	const grace = 700 * time.Millisecond
	// Inbound default is connWriteTimeout (30s), so a deadline near the grace
	// can only have come from the shared ticket.
	ticket := NewWriteTicket(OutboundWrite{WriteGrace: grace})

	for index, id := range []ConnID{20, 21} {
		conn := newGatedConn()
		pc := New(id, conn, Inbound, Options{})

		start := time.Now()
		if st := pc.SendRawTracked([]byte("shared\n"), ticket); st != SendOK {
			t.Fatalf("connection %d: SendRawTracked = %v, want SendOK", index, st)
		}
		conn.waitWriteEntered(t)

		deadline, ok := conn.lastWriteDeadline()
		if !ok {
			t.Fatalf("connection %d: writer never set a write deadline", index)
		}
		if applied := deadline.Sub(start); applied < grace/2 || applied > grace+2*time.Second {
			t.Fatalf("connection %d: applied write deadline = %v, want ~%v — the shared ticket "+
				"stopped answering after the first connection", index, applied, grace)
		}

		conn.release <- nil
		if written := conn.awaitCompletedWrites(t, 1, 2*time.Second); !bytes.Equal(written, []byte("shared\n")) {
			t.Fatalf("connection %d: socket received %q, want the shared frame", index, written)
		}
		pc.Close()
	}
}

// TestSendTrackedExpiredSendUntilNeverReachesSocket pins the §4.2 send
// deadline, which is the ONE guarantee that survives on this path: the writer
// re-checks SendUntil immediately BEFORE the socket write, so a frame that
// expired while it waited is never handed to the socket at all — and, because
// an expired frame is not a link failure, the connection keeps serving the
// frames behind it.
//
// It is asserted on the socket and not on a notification: the writes the
// connection actually performed are the only place "was it written" is a fact
// rather than a report about a fact.
func TestSendTrackedExpiredSendUntilNeverReachesSocket(t *testing.T) {
	t.Parallel()
	conn := newAutoConn()
	pc := New(2, conn, Inbound, Options{})
	defer pc.Close()

	st := pc.SendRawTracked([]byte("expired\n"), NewWriteTicket(OutboundWrite{
		SendUntil: domain.TimeOf(time.Now().Add(-time.Second)),
	}))
	if st != SendOK {
		t.Fatalf("SendRawTracked = %v, want SendOK", st)
	}

	// The live frame behind it is the synchronisation point AND the positive
	// control: once it has been written the writer has provably walked past the
	// expired one, so "zero writes so far" cannot be a race with a slow writer.
	if st := pc.SendRawTracked([]byte("live\n"), NewWriteTicket(OutboundWrite{
		SendUntil: domain.TimeOf(time.Now().Add(time.Minute)),
	})); st != SendOK {
		t.Fatalf("second SendRawTracked = %v, want SendOK", st)
	}

	written := conn.awaitCompletedWrites(t, 1, 2*time.Second)
	if !bytes.Equal(written, []byte("live\n")) {
		t.Fatalf("socket received %q, want only the live frame", written)
	}
	if conn.attemptCount() != 1 {
		t.Fatalf("write attempts = %d, want 1 — the expired frame was handed to the socket", conn.attemptCount())
	}
}

// TestWriteFailureShutsTheWholeConnectionQueue is the §9 contract for the
// LOWER queue: a frame cut mid-write tears the connection down, nothing queued
// behind it is ever handed to the socket, and a producer arriving afterwards is
// refused instead of being told its frame was accepted.
func TestWriteFailureShutsTheWholeConnectionQueue(t *testing.T) {
	t.Parallel()
	conn := newGatedConn()
	pc := New(3, conn, Inbound, Options{})

	if st := pc.SendRawTracked([]byte("first\n"), tracked()); st != SendOK {
		t.Fatalf("first SendRawTracked = %v, want SendOK", st)
	}
	conn.waitWriteEntered(t)

	if st := pc.SendRawTracked([]byte("second\n"), tracked()); st != SendOK {
		t.Fatalf("second SendRawTracked = %v, want SendOK", st)
	}

	// Break the in-flight write.
	conn.release <- errors.New("broken pipe")

	select {
	case <-pc.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reported the socket failure")
	}
	if conn.closeCount() == 0 {
		t.Fatal("a frame cut mid-write must tear the connection down")
	}

	// A frame that arrives AFTER the failure is refused at the door: it is
	// never written and never accounted as accepted (see
	// TestQueueIsShutAfterSocketFailure).
	if st := pc.SendRawTracked([]byte("late\n"), tracked()); st != SendWriterDone {
		t.Fatalf("late SendRawTracked = %v, want SendWriterDone", st)
	}
	if conn.attemptCount() != 1 {
		t.Fatalf("socket write attempts = %d, want 1 (only the broken frame)", conn.attemptCount())
	}

	pc.Close()
}

// TestWriteGraceBoundsSocketWriteAndTearsDownConnection pins the §4.2
// write-grace contract: the grace — not the connection default — bounds the
// socket write of a tracked frame, and a write that does not finish inside
// it aborts ambiguous and kills the connection.
func TestWriteGraceBoundsSocketWriteAndTearsDownConnection(t *testing.T) {
	t.Parallel()
	conn := newGatedConn()
	// Inbound default is connWriteTimeout (30s); the grace must win.
	pc := New(4, conn, Inbound, Options{})

	const grace = 120 * time.Millisecond
	start := time.Now()
	if st := pc.SendRawTracked([]byte("slow\n"), NewWriteTicket(OutboundWrite{
		WriteGrace: grace,
	})); st != SendOK {
		t.Fatalf("SendRawTracked = %v, want SendOK", st)
	}

	// Nobody releases the write: it can only end on the grace deadline, and
	// the writer treats that as a dead link.
	select {
	case <-pc.WriterDone():
	case <-time.After(5 * time.Second):
		t.Fatal("the write outlived the grace without killing the connection")
	}
	elapsed := time.Since(start)
	if elapsed >= connWriteTimeout {
		t.Fatalf("write ran for %v — the connection default, not the grace, bounded it", elapsed)
	}
	deadline, ok := conn.lastWriteDeadline()
	if !ok {
		t.Fatal("writer never set a write deadline")
	}
	if applied := deadline.Sub(start); applied < grace/2 || applied > grace+2*time.Second {
		t.Fatalf("applied write deadline = %v, want ~%v", applied, grace)
	}
	if conn.closeCount() == 0 {
		t.Fatal("an over-grace write must tear the connection down")
	}

	pc.Close()
}

// TestUntrackedFrameKeepsConnectionWriteDeadline is the regression twin of
// the grace test: a frame WITHOUT a contract must still get the
// direction's default deadline, unchanged from before the ticket path.
func TestUntrackedFrameKeepsConnectionWriteDeadline(t *testing.T) {
	t.Parallel()
	conn := newGatedConn()
	pc := New(5, conn, Outbound, Options{})
	defer pc.Close()

	start := time.Now()
	if st := pc.SendRaw([]byte("legacy\n")); st != SendOK {
		t.Fatalf("SendRaw = %v, want SendOK", st)
	}
	conn.waitWriteEntered(t)
	deadline, ok := conn.lastWriteDeadline()
	if !ok {
		t.Fatal("writer never set a write deadline")
	}
	applied := deadline.Sub(start)
	if applied < sessionWriteTimeout/2 || applied > sessionWriteTimeout+2*time.Second {
		t.Fatalf("applied write deadline = %v, want ~%v (outbound default)", applied, sessionWriteTimeout)
	}
	conn.release <- nil
}

// TestSendTrackedRefusalsNeverReachTheSocket covers every non-OK return of
// the tracked send entry points: a frame the queue refuses provably never
// started a write, and the STATUS is the caller's whole answer.
func TestSendTrackedRefusalsNeverReachTheSocket(t *testing.T) {
	t.Parallel()

	t.Run("saturated queue", func(t *testing.T) {
		t.Parallel()
		writerStarted := make(chan struct{})
		blocker := make(chan struct{})
		var once sync.Once
		conn := newMockConnWithWriter(t, func(b []byte) (int, error) {
			once.Do(func() { close(writerStarted) })
			<-blocker
			return len(b), nil
		})
		pc := New(8, conn, Inbound, Options{})
		defer func() {
			close(blocker)
			pc.Close()
		}()

		if st := pc.SendRaw([]byte("head\n")); st != SendOK {
			t.Fatalf("first SendRaw = %v, want SendOK", st)
		}
		<-writerStarted
		for i := 0; i < sendChBuffer; i++ {
			if st := pc.SendRaw([]byte("filler\n")); st != SendOK {
				t.Fatalf("filler %d = %v, want SendOK", i, st)
			}
		}

		if st := pc.SendRawTracked([]byte("refused\n"), tracked()); st != SendBufferFull {
			t.Fatalf("SendRawTracked on full queue = %v, want SendBufferFull", st)
		}
	})

	t.Run("closed connection", func(t *testing.T) {
		t.Parallel()
		conn := newAutoConn()
		pc := New(9, conn, Inbound, Options{})
		pc.Close()

		st := pc.SendRawTracked([]byte("after-close\n"), tracked())
		if st != SendChanClosed {
			t.Fatalf("SendRawTracked after Close = %v, want SendChanClosed", st)
		}
		if conn.attemptCount() != 0 {
			t.Fatalf("socket writes = %d, want 0", conn.attemptCount())
		}
	})

	t.Run("marshal error", func(t *testing.T) {
		t.Parallel()
		conn := newAutoConn()
		pc := New(10, conn, Inbound, Options{})
		defer pc.Close()

		bad := protocol.Frame{Type: "error", Details: json.RawMessage("{not-json")}
		if st := pc.SendTracked(bad, tracked()); st != SendMarshalError {
			t.Fatalf("SendTracked(bad frame) = %v, want SendMarshalError", st)
		}
		if conn.attemptCount() != 0 {
			t.Fatalf("socket writes = %d, want 0", conn.attemptCount())
		}
	})
}

// TestQueueIsShutAfterSocketFailure pins the door check: once the socket has
// failed, the writer keeps consuming sendCh to discard the residue, which
// frees slots. A producer must NOT read a free slot as "accepted" — every
// send entry point reports SendWriterDone instead, so the session layer does
// not account a discarded frame as a useful write.
//
// The wait for the failure is WriterDone, which closes strictly after
// the gate is raised and the queue is drained. The previous version of
// this test polled SendRaw in a millisecond sleep loop and accepted every
// SendOK it collected on the way — it asserted the very behaviour it was
// written to forbid and could not fail for it.
func TestQueueIsShutAfterSocketFailure(t *testing.T) {
	t.Parallel()
	conn := newGatedConn()
	pc := New(13, conn, Inbound, Options{})
	defer pc.Close()

	if st := pc.SendRaw([]byte("in-flight\n")); st != SendOK {
		t.Fatalf("SendRaw = %v, want SendOK", st)
	}
	conn.waitWriteEntered(t)
	conn.release <- errors.New("broken pipe")

	select {
	case <-pc.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reported the socket failure")
	}

	if st := pc.SendRaw([]byte("after\n")); st != SendWriterDone {
		t.Fatalf("SendRaw after socket failure = %v, want SendWriterDone", st)
	}
	if st := pc.SendRawSync([]byte("after\n")); st != SendWriterDone {
		t.Fatalf("SendRawSync after socket failure = %v, want SendWriterDone", st)
	}
	if st := pc.SendRawSyncBlocking([]byte("after\n")); st != SendWriterDone {
		t.Fatalf("SendRawSyncBlocking after socket failure = %v, want SendWriterDone", st)
	}
	if st := pc.SendRawTracked([]byte("after\n"), tracked()); st != SendWriterDone {
		t.Fatalf("SendRawTracked after socket failure = %v, want SendWriterDone", st)
	}
	if conn.attemptCount() != 1 {
		t.Fatalf("socket write attempts = %d, want 1", conn.attemptCount())
	}
}

// producerBarrier reproduces the one interleaving the two-sided gate
// check exists for: a producer passes the pre-offer check, the writer then
// fails its socket write, raises the gate and drains the queue, and only
// after all of that does the producer's frame reach sendCh.
//
// The check and the offer live inside a single call, so nothing outside the
// package can interleave them and no arrangement of public calls can produce
// this ordering. NetCore.enqueueBarrier is the explicit in-package
// synchronisation point that makes the window reproducible without sleeps and
// without hoping for a particular scheduling.
type producerBarrier struct {
	pc       *NetCore
	conn     *gatedConn
	entered  chan struct{}
	proceed  chan struct{}
	armed    sync.Once
	released sync.Once
}

// newProducerBarrier returns a connection whose writer is already blocked
// inside the socket write of one in-flight frame, with the barrier armed for
// the NEXT producer only — the in-flight frame goes through untouched.
func newProducerBarrier(t *testing.T, id ConnID) *producerBarrier {
	t.Helper()
	b := &producerBarrier{
		conn:    newGatedConn(),
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	b.pc = New(id, b.conn, Inbound, Options{})
	if st := b.pc.SendRaw([]byte("in-flight\n")); st != SendOK {
		t.Fatalf("in-flight SendRaw = %v, want SendOK", st)
	}
	b.conn.waitWriteEntered(t)

	// Installed after the in-flight frame and before any producer goroutine
	// exists. The writer goroutine never reads the field, and the `go`
	// statement in park orders this write against the only reader there is.
	b.pc.enqueueBarrier = func() {
		b.armed.Do(func() {
			close(b.entered)
			<-b.proceed
		})
	}
	// A failed assertion in park or breakSocket returns without ever calling
	// resume, and the parked producer would then sit on b.proceed for the rest
	// of the package run. The cleanup releases it exactly once, so a red test
	// costs one failure and not a goroutine.
	t.Cleanup(b.release)
	return b
}

// release lets the parked producer go, whatever the test's fate. Idempotent:
// resume and the cleanup both call it.
func (b *producerBarrier) release() {
	b.released.Do(func() { close(b.proceed) })
}

// park runs producer in its own goroutine and returns once it is stopped
// between its gate check and its channel offer.
func (b *producerBarrier) park(t *testing.T, producer func()) {
	t.Helper()
	go producer()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("producer never reached the enqueue barrier")
	}
}

// breakSocket completes the in-flight write with an error and waits until the
// writer has raised the gate AND finished draining the queue: writerDone
// closes strictly after both, which turns "the window is open now" into an
// edge to wait on instead of a duration to sleep through.
func (b *producerBarrier) breakSocket(t *testing.T) {
	t.Helper()
	select {
	case b.conn.release <- errors.New("broken pipe"):
	case <-time.After(2 * time.Second):
		t.Fatal("writer never took the socket write result")
	}
	select {
	case <-b.pc.WriterDone():
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reported the socket failure")
	}
}

// resume lets the parked producer perform its channel offer.
func (b *producerBarrier) resume() { b.release() }

// TestSendRefusesFrameQueuedAfterSocketFailure is the regression test for the
// frame that enters the queue AFTER the writer gave up on it. Producers used
// to read socketFailed once, before the offer, so a producer that read false a
// moment before the failure was answered SendOK for a frame the writer could
// only discard — and the caller then accounted a discarded frame as a useful
// write (markPeerWrite), which is what keeps a dead peer looking healthy.
func TestSendRefusesFrameQueuedAfterSocketFailure(t *testing.T) {
	t.Parallel()

	// SendRawTracked is the row that pins the change under test; it used to be
	// a case of its own, asserting a terminal that no longer exists, and the
	// status plus "nothing reached the socket" is all it ever observed besides
	// that. The other four are GUARDS that the entry points stayed consistent
	// with each other, and the honest caveat is that the three sync ones
	// answered SendWriterDone before the change as well — they also watch
	// `writerDone`, which is closed by then. `SendRawSyncBlocking` in particular
	// offers into a select where both the free slot and `writerDone` are ready,
	// so which of the two answers it gives is up to the runtime; the row asserts
	// the answer, not the path.
	entries := []struct {
		name string
		id   ConnID
		send func(pc *NetCore) SendStatus
	}{
		{"SendRawTracked", 20, func(pc *NetCore) SendStatus {
			return pc.SendRawTracked([]byte("after-drain\n"), tracked())
		}},
		{"SendRaw", 21, func(pc *NetCore) SendStatus { return pc.SendRaw([]byte("after-drain\n")) }},
		{"SendRawSync", 22, func(pc *NetCore) SendStatus { return pc.SendRawSync([]byte("after-drain\n")) }},
		{"SendRawSyncCtx", 23, func(pc *NetCore) SendStatus {
			return pc.SendRawSyncCtx(context.Background(), []byte("after-drain\n"))
		}},
		{"SendRawSyncBlocking", 24, func(pc *NetCore) SendStatus { return pc.SendRawSyncBlocking([]byte("after-drain\n")) }},
	}
	for _, entry := range entries {
		t.Run(entry.name, func(t *testing.T) {
			t.Parallel()
			b := newProducerBarrier(t, entry.id)
			defer b.pc.Close()

			got := make(chan SendStatus, 1)
			b.park(t, func() { got <- entry.send(b.pc) })
			b.breakSocket(t)
			b.resume()

			select {
			case st := <-got:
				if st != SendWriterDone {
					t.Fatalf("%s into a queue shut mid-call = %v, want SendWriterDone", entry.name, st)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("%s never returned after the barrier was released", entry.name)
			}
			if b.conn.attemptCount() != 1 {
				t.Fatalf("socket write attempts = %d, want 1", b.conn.attemptCount())
			}
		})
	}

	// Positive control. Without it every assertion above would also hold for
	// an implementation that answered SendWriterDone unconditionally: the
	// same parked producer, on a link that stays alive, must still be
	// answered SendOK and its frame must still reach the socket.
	t.Run("live socket accepts through the same barrier", func(t *testing.T) {
		t.Parallel()
		b := newProducerBarrier(t, 25)
		defer b.pc.Close()

		got := make(chan SendStatus, 1)
		b.park(t, func() {
			got <- b.pc.SendRawTracked([]byte("after-live\n"), tracked())
		})

		select {
		case b.conn.release <- nil:
		case <-time.After(2 * time.Second):
			t.Fatal("writer never took the socket write result")
		}
		b.resume()

		select {
		case st := <-got:
			if st != SendOK {
				t.Fatalf("SendRawTracked on a live socket = %v, want SendOK", st)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("producer never returned after the barrier was released")
		}

		b.conn.waitWriteEntered(t)
		select {
		case b.conn.release <- nil:
		case <-time.After(2 * time.Second):
			t.Fatal("the frame accepted with SendOK never reached the socket")
		}
		// Both writes: the in-flight one and the frame this producer was told
		// SendOK for. Waiting for the count is what makes the assertion below a
		// statement about the socket rather than a race with the writer.
		written := b.conn.awaitCompletedWrites(t, 2, 2*time.Second)
		if !bytes.Contains(written, []byte("after-live\n")) {
			t.Fatalf("socket received %q, want the frame accepted with SendOK", written)
		}
	})
}

// TestTeardownReasonFollowsTheGate pins WHICH refusal a producer already past
// the door is answered when the connection dies under it.
//
// Both death paths close ONE channel, writerDone, so a producer that read its
// wait as "whichever channel fired" had exactly one answer for two different
// facts: it reported SendWriterDone — "the socket died on its own" — for a
// connection whose owner had called Close() and was holding the teardown. The
// distinction is public contract, not diagnostics: SendChanClosed tells the
// caller the teardown is already owned, SendWriterDone invites it to run one.
// The gate is the state that knows the difference, and it is raised BEFORE
// writerDone is signalled on both paths.
//
// The two waits are pinned differently because only one of them has an
// observable edge:
//
//   - the ENQUEUE wait (queueFrameBlocking) is driven end to end. Its select is
//     forced onto the writerDone arm by leaving the queue full with no consumer
//     left, so the outcome is the arm under test rather than a race with a
//     freed slot.
//   - the FLUSH wait is driven through awaitFlush, the helper all three sync
//     entry points share. Reaching it from outside requires settleEnqueuedFrame
//     to read an OPEN gate and the teardown to land afterwards, and the only
//     proof a producer got that far is the very status under test — a test that
//     tried would pass whenever the enqueue half answered first. The two
//     terminal states it is driven in are the real ones, produced by Close()
//     and by a failed socket write.
//
// The socket-failure half of the ENQUEUE wait is already pinned by
// TestSendRefusesFrameQueuedAfterSocketFailure (row SendRawSyncBlocking) and is
// deliberately not repeated here.
func TestTeardownReasonFollowsTheGate(t *testing.T) {
	t.Parallel()

	t.Run("enqueue wait parked across an orderly Close", func(t *testing.T) {
		t.Parallel()
		conn := newAutoConn()
		pc := New(30, conn, Inbound, Options{})

		entered := make(chan struct{})
		proceed := make(chan struct{})
		var armed sync.Once
		pc.enqueueBarrier = func() {
			armed.Do(func() {
				close(entered)
				<-proceed
			})
		}

		got := make(chan SendStatus, 1)
		go func() { got <- pc.SendRawSyncBlocking([]byte("parked\n")) }()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("producer never reached the enqueue barrier")
		}

		// Close() completes while the producer sits between its door check
		// (which read an open gate) and its offer.
		pc.Close()

		// The writer has returned, so the queue has no consumer left and a
		// full one stays full. Without this the drain would free every slot
		// and the producer would answer from settleEnqueuedFrame instead —
		// the arm that was already correct.
		for range sendChBuffer {
			pc.sendCh <- sendItem{data: []byte("filler\n")}
		}
		close(proceed)

		select {
		case st := <-got:
			if st != SendChanClosed {
				t.Fatalf("blocking enqueue waiting for a slot across Close() = %v, "+
					"want SendChanClosed — the queue was shut by its owner, not by a dead socket", st)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("blocking enqueue never returned after Close")
		}
	})

	t.Run("flush wait across an orderly Close", func(t *testing.T) {
		t.Parallel()
		pc := New(31, newAutoConn(), Inbound, Options{})
		pc.Close()

		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		// An ack that never closes is a frame the writer released instead of
		// writing — exactly what Close() does to the queue residue.
		if st := pc.awaitFlush(make(chan struct{}), nil, deadline.C); st != SendChanClosed {
			t.Fatalf("flush wait across Close() = %v, want SendChanClosed", st)
		}
	})

	t.Run("flush wait across a socket failure", func(t *testing.T) {
		t.Parallel()
		conn := newGatedConn()
		pc := New(32, conn, Inbound, Options{})
		defer pc.Close()

		if st := pc.SendRaw([]byte("in-flight\n")); st != SendOK {
			t.Fatalf("SendRaw = %v, want SendOK", st)
		}
		conn.waitWriteEntered(t)
		conn.release <- errors.New("broken pipe")
		select {
		case <-pc.WriterDone():
		case <-time.After(2 * time.Second):
			t.Fatal("writer never reported the socket failure")
		}

		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		if st := pc.awaitFlush(make(chan struct{}), nil, deadline.C); st != SendWriterDone {
			t.Fatalf("flush wait across a socket failure = %v, want SendWriterDone", st)
		}
	})
}

// TestUntrackedTrafficUnchangedAlongsideTrackedFrames is the regression
// guard for existing traffic: frames without a contract keep flowing
// through the same writer, in order, while tracked frames are accounted.
func TestUntrackedTrafficUnchangedAlongsideTrackedFrames(t *testing.T) {
	t.Parallel()
	conn := newAutoConn()
	pc := New(12, conn, Inbound, Options{})
	defer pc.Close()

	if st := pc.Send(protocol.Frame{Type: "ping"}); st != SendOK {
		t.Fatalf("Send = %v, want SendOK", st)
	}
	if st := pc.SendTracked(protocol.Frame{Type: "pong"}, tracked()); st != SendOK {
		t.Fatalf("SendTracked = %v, want SendOK", st)
	}
	if st := pc.SendSync(protocol.Frame{Type: "get_messages"}); st != SendOK {
		t.Fatalf("SendSync = %v, want SendOK", st)
	}

	written := conn.awaitCompletedWrites(t, 3, 2*time.Second)
	for _, want := range []string{`"type":"ping"`, `"type":"pong"`, `"type":"get_messages"`} {
		if !bytes.Contains(written, []byte(want)) {
			t.Fatalf("frame %s missing from the wire: %s", want, written)
		}
	}
	if i, j := bytes.Index(written, []byte(`"ping"`)), bytes.Index(written, []byte(`"pong"`)); i > j {
		t.Fatalf("frame order broken: ping at %d, pong at %d", i, j)
	}
}
