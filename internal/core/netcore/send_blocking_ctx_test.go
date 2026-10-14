package netcore

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"testing/synctest"
)

// send_blocking_ctx_test.go pins SendRawBlockingCtx at the NetCore level: the
// wait for a slot ends on room (the frame is queued in order), on the caller's
// ctx (SendCtxCancelled, and the frame never enters the queue) or on a ctx
// that was already over (nothing is offered at all).
//
// Every test runs in a synctest bubble over net.Pipe: an unread pipe parks the
// writer inside its Write, which is how a peer that stopped draining leaves
// the real writer, and synctest.Wait returns only once every goroutine —
// writer and sender alike — is parked, so "the sender is waiting for a slot"
// is a fact the test establishes rather than a sleep it hopes is long enough.

// blockingCtxConn is one inbound NetCore over a pipe whose remote reads only
// when the test asks it to.
type blockingCtxConn struct {
	pc     *NetCore
	remote *bufio.Reader
}

// newBlockingCtxConn must be called inside a bubble; it registers the
// teardown that lets the bubble empty — closing the core releases the writer.
func newBlockingCtxConn(t *testing.T, id ConnID) *blockingCtxConn {
	t.Helper()
	near, far := net.Pipe()
	pc := New(id, near, Inbound, Options{})
	t.Cleanup(func() {
		pc.Close()
		_ = far.Close()
	})
	return &blockingCtxConn{pc: pc, remote: bufio.NewReader(far)}
}

// saturate fills the queue until it refuses a frame and stays full, and
// returns the frames in the order the remote will read them. The refill after
// synctest.Wait covers a writer that took the first frame out only after the
// first fill found the queue full.
func (c *blockingCtxConn) saturate(t *testing.T) [][]byte {
	t.Helper()
	var queued [][]byte
	fill := func() {
		for len(queued) <= 2*sendChBuffer {
			frame := []byte(fmt.Sprintf("filler-%d\n", len(queued)))
			switch st := c.pc.SendRaw(frame); st {
			case SendOK:
				queued = append(queued, frame)
			case SendBufferFull:
				return
			default:
				t.Fatalf("filler #%d = %v, want SendOK until SendBufferFull", len(queued), st)
			}
		}
		t.Fatalf("%d fillers and the queue never refused one", len(queued))
	}
	fill()
	synctest.Wait()
	fill()
	return queued
}

// readFrame returns the next line the remote receives.
func (c *blockingCtxConn) readFrame(t *testing.T) []byte {
	t.Helper()
	line, err := c.remote.ReadBytes('\n')
	if err != nil {
		t.Fatalf("remote read: %q, %v", line, err)
	}
	return line
}

func (c *blockingCtxConn) expectFrames(t *testing.T, want ...[]byte) {
	t.Helper()
	for i, frame := range want {
		if got := c.readFrame(t); !bytes.Equal(got, frame) {
			t.Fatalf("remote frame #%d = %q, want %q", i, got, frame)
		}
	}
}

// parkBlockingCtxSend starts SendRawBlockingCtx and returns once the sender is
// parked, failing the test if it returned instead of waiting.
func parkBlockingCtxSend(t *testing.T, pc *NetCore, ctx context.Context, frame []byte) <-chan SendStatus {
	t.Helper()
	got := make(chan SendStatus, 1)
	go func() { got <- pc.SendRawBlockingCtx(ctx, frame) }()
	synctest.Wait()
	select {
	case st := <-got:
		t.Fatalf("SendRawBlockingCtx into a full queue returned %v; want it to wait for a slot", st)
	default:
	}
	return got
}

func TestSendRawBlockingCtxQueuesOnceRoomAppears(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockingCtxConn(t, 60)
		queued := c.saturate(t)

		parked := []byte("parked\n")
		got := parkBlockingCtxSend(t, c.pc, context.Background(), parked)

		c.expectFrames(t, queued...)
		if st := <-got; st != SendOK {
			t.Fatalf("SendRawBlockingCtx after room appeared = %v, want SendOK", st)
		}
		c.expectFrames(t, parked)
	})
}

// TestSendRawBlockingCtxCancelWhileParked: cancelling the caller's ctx ends
// the wait for a slot with SendCtxCancelled, and the frame never enters the
// queue — the remote receives the fillers and then the next frame sent, not
// the cancelled one.
func TestSendRawBlockingCtxCancelWhileParked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockingCtxConn(t, 61)
		queued := c.saturate(t)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		got := parkBlockingCtxSend(t, c.pc, ctx, []byte("cancelled\n"))

		cancel()
		if st := <-got; st != SendCtxCancelled {
			t.Fatalf("SendRawBlockingCtx parked on a full queue and cancelled = %v, want SendCtxCancelled", st)
		}

		c.expectFrames(t, queued...)
		marker := []byte("marker\n")
		if st := c.pc.SendRaw(marker); st != SendOK {
			t.Fatalf("SendRaw(marker) = %v, want SendOK", st)
		}
		c.expectFrames(t, marker)
	})
}

// TestSendRawBlockingCtxPreCancelledOffersNothing: a ctx that is already over
// is answered before the queue is touched, even with room in it.
func TestSendRawBlockingCtxPreCancelledOffersNothing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newBlockingCtxConn(t, 62)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if st := c.pc.SendRawBlockingCtx(ctx, []byte("cancelled\n")); st != SendCtxCancelled {
			t.Fatalf("SendRawBlockingCtx(cancelled ctx) = %v, want SendCtxCancelled", st)
		}

		marker := []byte("marker\n")
		if st := c.pc.SendRaw(marker); st != SendOK {
			t.Fatalf("SendRaw(marker) = %v, want SendOK", st)
		}
		c.expectFrames(t, marker)
	})
}
