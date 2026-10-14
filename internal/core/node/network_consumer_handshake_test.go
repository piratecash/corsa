package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// sendHandshakeReplyViaNetwork — control-plane reply path that must NOT
// trigger slow-peer eviction on writer-queue pressure.
//
// Background: the regular sendFrameViaNetwork helper closes the connection
// on netcore.ErrSendBufferFull as the eviction signal for slow peers. That
// behaviour is correct for data-plane fan-out (push_message, announce_*)
// but wrong for handshake replies (peers, contacts, auth_ok): during session
// setup our queue toward the peer can already be full of fire-and-forget
// frames the asynchronous senders (sendFrameViaNetwork,
// sendFrameBytesViaNetwork, enqueueFrameByID) queued without waiting for
// room. Evicting on the reply that meets that full queue guarantees the
// handshake fails — which is exactly the cascade observed in
// cm_session_setup_failed storms against bootstrap nodes.
//
// sendHandshakeReplyViaNetwork:
//   - on a full queue waits for room with SendFrameBlocking under a bounded
//     internal timeout (handshakeReplyTimeout), so a transient burst of
//     fire-and-forget frames is absorbed but a wedged peer cannot park the
//     goroutine forever.
//   - NEVER calls Network.Close. Even on timeout, the connection stays
//     registered. If the peer is genuinely dead, the heartbeat path
//     (inboundHeartbeat) is the right place to evict.
// ---------------------------------------------------------------------------

func TestSendHandshakeReply_HappyPath(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7001)
	backend.Register(connID, netcore.Inbound, "10.0.0.42:64646")

	reply := protocol.Frame{Type: "auth_ok", Topic: "dm", Recipient: "x", Subscriber: "x"}
	err := svc.sendHandshakeReplyViaNetwork(context.Background(), connID, reply)
	if err != nil {
		t.Fatalf("sendHandshakeReplyViaNetwork(empty buffer): unexpected error %v", err)
	}

	select {
	case data := <-backend.Outbound(connID):
		got, err := parseFrameLineForTest(data)
		if err != nil {
			t.Fatalf("parse outbound frame: %v", err)
		}
		if got.Type != "auth_ok" {
			t.Fatalf("got type %q, want auth_ok", got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("auth_ok reply did not arrive on backend.Outbound")
	}
}

// handshakeReplyConn is one registered connection a handshake-reply test
// saturates, seen from the Service that replies on it and from the remote
// that reads it. The same scenarios run against the production bridge over a
// real NetCore and against netcoretest.Backend, because a wait that exists on
// only one of them is exactly the drift these tests used to hide: the
// Backend waited for room, the bridge answered ErrSendBufferFull at once, and
// the old test ran on the Backend alone.
type handshakeReplyConn struct {
	svc *Service
	id  domain.ConnID
	// readFrame blocks until the remote receives its next frame.
	readFrame func(t *testing.T) []byte
	// released reports whether the connection has been closed. The helper
	// under test must never close it.
	released func() bool
}

// handshakeReplyImplementations builds a handshakeReplyConn on each
// netcore.Network implementation. Every builder runs inside a synctest
// bubble, so its channels, timers and goroutines belong to the bubble and the
// tests can both see a parked wait (synctest.Wait) and run the
// handshakeReplyTimeout bound in virtual time.
var handshakeReplyImplementations = []struct {
	name  string
	build func(t *testing.T) handshakeReplyConn
}{
	{"bridge", newBridgeHandshakeReplyConn},
	{"backend", newBackendHandshakeReplyConn},
}

// newBridgeHandshakeReplyConn registers an accepted socket the way production
// does (registerInboundConn), over a pipe whose remote reads only when the
// test asks it to. An unread pipe parks the NetCore writer inside Write, which
// is the state a peer that stopped draining puts the real writer in. The
// inbound per-write deadline is far longer than handshakeReplyTimeout, so in
// virtual time the reply's bound expires while that write is still parked and
// the link is still alive.
func newBridgeHandshakeReplyConn(t *testing.T) handshakeReplyConn {
	t.Helper()
	svc, _ := newInboundTeardownFixture(t)
	near, far := net.Pipe()
	// Runs before the fixture's inbound drain (cleanups are LIFO): the
	// writer's Write fails at once instead of waiting for its deadline.
	t.Cleanup(func() { _ = far.Close() })
	if err := svc.registerInboundConn(near, nil); err != nil {
		t.Fatalf("registerInboundConn: %v", err)
	}
	id, ok := svc.connIDFor(near)
	if !ok {
		t.Fatal("the registered inbound conn has no ConnID")
	}
	core := registeredInboundCore(t, svc, near)
	reader := bufio.NewReader(far)
	return handshakeReplyConn{
		svc: svc,
		id:  id,
		readFrame: func(t *testing.T) []byte {
			t.Helper()
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatalf("remote read: %q, %v", line, err)
			}
			return line
		},
		released: func() bool { return writerReturned(core) },
	}
}

// newBackendHandshakeReplyConn registers the same connection on the
// in-memory Backend. A shallow queue keeps the saturation short; the depth is
// the implementation's business, and the scenarios never assert it.
func newBackendHandshakeReplyConn(t *testing.T) handshakeReplyConn {
	t.Helper()
	backend := netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: 4})
	t.Cleanup(backend.Shutdown)
	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	id := netcore.ConnID(7002)
	backend.Register(id, netcore.Inbound, "10.0.0.43:64646")
	remote := backend.Outbound(id)
	return handshakeReplyConn{
		svc: svc,
		id:  id,
		readFrame: func(t *testing.T) []byte {
			t.Helper()
			frame, ok := <-remote
			if !ok {
				t.Fatal("the remote stream ended")
			}
			return frame
		},
		released: func() bool { return backend.Closed(id) },
	}
}

// handshakeReplySaturationLimit bounds the saturation loop. Every queue
// involved is far shallower, so reaching it means the queue never refused.
const handshakeReplySaturationLimit = 1 << 12

// saturateHandshakeReplyQueue sends filler frames until the connection's
// queue refuses one and stays full, and returns them in the order the remote
// will receive them. Must run inside a synctest bubble.
func saturateHandshakeReplyQueue(t *testing.T, c handshakeReplyConn) [][]byte {
	t.Helper()
	var queued [][]byte
	fill := func() {
		for len(queued) < handshakeReplySaturationLimit {
			frame := []byte(fmt.Sprintf("{\"type\":\"filler\",\"count\":%d}\n", len(queued)))
			err := c.svc.Network().SendFrame(context.Background(), c.id, frame)
			if errors.Is(err, netcore.ErrSendBufferFull) {
				return
			}
			if err != nil {
				t.Fatalf("filler #%d: %v", len(queued), err)
			}
			queued = append(queued, frame)
		}
		t.Fatalf("%d fillers and the queue never refused one", handshakeReplySaturationLimit)
	}
	fill()
	// The bridge's writer may take the first filler out of the queue only
	// after the loop above found it full. Once every goroutine is parked the
	// writer is inside its Write on the unread pipe, and refilling the slot it
	// freed leaves a queue that stays full until the remote reads.
	synctest.Wait()
	fill()
	return queued
}

// TestSendHandshakeReply_BufferFullWaitsAndDelivers pins the
// absorb-the-backpressure contract: when the reply finds the queue full, the
// helper waits for room instead of failing, and the reply lands behind
// everything that was already queued. Room appears only after the helper is
// parked (synctest.Wait), so a helper that does not wait has already
// returned by then and fails here instead of passing on a lucky schedule.
func TestSendHandshakeReply_BufferFullWaitsAndDelivers(t *testing.T) {
	t.Parallel()
	for _, impl := range handshakeReplyImplementations {
		t.Run(impl.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := impl.build(t)
				queued := saturateHandshakeReplyQueue(t, c)

				result := make(chan error, 1)
				go func() {
					result <- c.svc.sendHandshakeReplyViaNetwork(context.Background(), c.id, protocol.Frame{Type: "peers", Count: 0})
				}()
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("the reply returned %v while the queue was still full; want it to wait for room", err)
				default:
				}

				for i, want := range queued {
					if got := c.readFrame(t); !bytes.Equal(got, want) {
						t.Fatalf("remote frame #%d = %q, want filler %q", i, got, want)
					}
				}
				got, err := parseFrameLineForTest(c.readFrame(t))
				if err != nil {
					t.Fatalf("parse the frame after the fillers: %v", err)
				}
				if got.Type != "peers" {
					t.Fatalf("frame after the fillers has type %q, want the peers reply", got.Type)
				}
				if err := <-result; err != nil {
					t.Fatalf("sendHandshakeReplyViaNetwork after room appeared = %v, want nil", err)
				}
				if c.released() {
					t.Fatal("the connection was closed; the handshake reply path must never evict")
				}
			})
		})
	}
}

// TestSendHandshakeReply_BufferFullTimesOutNoClose pins the bound: a queue
// that never drains ends the wait after exactly handshakeReplyTimeout (the
// bubble's clock is virtual and exact), the answer names the full queue, and
// the connection stays open — eviction belongs to the heartbeat path.
func TestSendHandshakeReply_BufferFullTimesOutNoClose(t *testing.T) {
	t.Parallel()
	for _, impl := range handshakeReplyImplementations {
		t.Run(impl.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := impl.build(t)
				saturateHandshakeReplyQueue(t, c)

				start := time.Now()
				err := c.svc.sendHandshakeReplyViaNetwork(context.Background(), c.id, protocol.Frame{Type: "auth_ok"})
				waited := time.Since(start)

				if !errors.Is(err, netcore.ErrSendBufferFull) {
					t.Fatalf("reply into a queue that never drains = %v, want ErrSendBufferFull", err)
				}
				if waited != handshakeReplyTimeout {
					t.Fatalf("the reply gave up after %v, want exactly handshakeReplyTimeout (%v)", waited, handshakeReplyTimeout)
				}
				if c.released() {
					t.Fatal("the connection was closed on timeout; the handshake reply path must never evict")
				}
			})
		})
	}
}

// TestSendHandshakeReply_CallerCtxEndsTheWait: the internal bound is stacked
// on the caller's ctx, so a caller whose own deadline is shorter ends the wait
// at that deadline and learns its own ctx error, not the queue's.
func TestSendHandshakeReply_CallerCtxEndsTheWait(t *testing.T) {
	t.Parallel()
	for _, impl := range handshakeReplyImplementations {
		t.Run(impl.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := impl.build(t)
				saturateHandshakeReplyQueue(t, c)

				callerBound := handshakeReplyTimeout / 4
				ctx, cancel := context.WithTimeout(context.Background(), callerBound)
				defer cancel()
				start := time.Now()
				err := c.svc.sendHandshakeReplyViaNetwork(ctx, c.id, protocol.Frame{Type: "auth_ok"})
				waited := time.Since(start)

				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("reply under a caller deadline = %v, want context.DeadlineExceeded", err)
				}
				if waited != callerBound {
					t.Fatalf("the reply gave up after %v, want the caller's deadline (%v)", waited, callerBound)
				}
				if c.released() {
					t.Fatal("the connection was closed; the handshake reply path must never evict")
				}
			})
		})
	}
}

// TestSendHandshakeReply_UnknownConnReportsButDoesNotPanic covers the
// degenerate case where the conn vanished between handler entry and the
// reply send — we surface a sentinel but do not panic and do not attempt
// to close (there is nothing to close).
func TestSendHandshakeReply_UnknownConnReportsButDoesNotPanic(t *testing.T) {
	t.Parallel()

	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7099) // never registered

	reply := protocol.Frame{Type: "auth_ok"}
	err := svc.sendHandshakeReplyViaNetwork(context.Background(), connID, reply)
	if err == nil {
		t.Fatal("expected error on unknown ConnID, got nil")
	}
	// The exact sentinel is an implementation detail; the contract is
	// "non-nil, and the helper survives".
}
