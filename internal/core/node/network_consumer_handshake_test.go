package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
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
// but wrong for handshake replies (subscribed, peers, contacts, auth_ok):
// during session setup the peer is the one flooding US with announce_routes,
// so OUR sendCh saturates from THEIR broadcast. Evicting on the next reply
// in line guarantees the handshake fails — which is exactly the cascade
// observed in cm_session_setup_failed storms against bootstrap nodes with
// large routing tables.
//
// sendHandshakeReplyViaNetwork:
//   - uses SendFrameSync with a bounded internal timeout
//     (handshakeReplyTimeout) so a wedged peer cannot park the goroutine
//     forever, but a transient backpressure burst from a large
//     announce_routes flush is absorbed.
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
		QueueStatePath: t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7001)
	backend.Register(connID, netcore.Inbound, "10.0.0.42:64646")

	reply := protocol.Frame{Type: "subscribed", Topic: "dm", Recipient: "x", Subscriber: "x"}
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
		if got.Type != "subscribed" {
			t.Fatalf("got type %q, want subscribed", got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("subscribed reply did not arrive on backend.Outbound")
	}
}

// TestSendHandshakeReply_BufferFullWaitsAndDelivers verifies the
// absorb-the-backpressure contract: when the outbound channel is briefly
// saturated by a peer-side announce_routes burst, the handshake reply
// must wait for the writer to drain rather than evict the connection.
// We simulate this by filling the buffer, then draining a slot mid-call.
func TestSendHandshakeReply_BufferFullWaitsAndDelivers(t *testing.T) {
	t.Parallel()

	backend := netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: 1})
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
		QueueStatePath: t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7002)
	backend.Register(connID, netcore.Inbound, "10.0.0.43:64646")

	// Fill the single outbound slot directly via SendFrame (so subsequent
	// SendFrame calls would return ErrSendBufferFull). The handshake reply
	// path uses SendFrameSync so it must block rather than fail.
	if err := backend.Network().SendFrame(context.Background(), connID, []byte("blocker\n")); err != nil {
		t.Fatalf("seed outbound: %v", err)
	}

	// Drain the seeded frame after a brief delay so the handshake reply
	// can be enqueued. If the helper aborted on buffer pressure, the
	// reply would never appear.
	go func() {
		time.Sleep(50 * time.Millisecond)
		<-backend.Outbound(connID) // drop the blocker
	}()

	reply := protocol.Frame{Type: "peers", Count: 0}
	if err := svc.sendHandshakeReplyViaNetwork(context.Background(), connID, reply); err != nil {
		t.Fatalf("sendHandshakeReplyViaNetwork (briefly full buffer): %v", err)
	}

	select {
	case data := <-backend.Outbound(connID):
		got, err := parseFrameLineForTest(data)
		if err != nil {
			t.Fatalf("parse outbound: %v", err)
		}
		if got.Type != "peers" {
			t.Fatalf("got type %q, want peers", got.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peers reply did not arrive after the blocker was drained")
	}

	// Connection must still be registered — no eviction on buffer pressure.
	if backend.RemoteAddr(connID) == "" {
		t.Fatal("connection was evicted despite handshake-reply path being eviction-free")
	}
}

// TestSendHandshakeReply_BufferFullTimesOutNoClose pins the no-eviction
// contract: even when the buffer never drains and the internal timeout
// elapses, the connection must remain registered. The caller learns
// about the failure via the returned error.
func TestSendHandshakeReply_BufferFullTimesOutNoClose(t *testing.T) {
	t.Parallel()

	backend := netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: 1})
	t.Cleanup(backend.Shutdown)

	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
		QueueStatePath: t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7003)
	backend.Register(connID, netcore.Inbound, "10.0.0.44:64646")

	// Fill the buffer and never drain it.
	if err := backend.Network().SendFrame(context.Background(), connID, []byte("blocker\n")); err != nil {
		t.Fatalf("seed outbound: %v", err)
	}

	// Use a tight ctx so the test does not have to wait the full
	// handshakeReplyTimeout. The helper must respect the caller's ctx
	// in addition to its internal cap.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	reply := protocol.Frame{Type: "subscribed"}
	err := svc.sendHandshakeReplyViaNetwork(ctx, connID, reply)
	if err == nil {
		t.Fatal("expected error from wedged outbound buffer, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// Critical assertion: no eviction.
	if backend.RemoteAddr(connID) == "" {
		t.Fatal("connection was evicted on timeout — handshake reply path must NOT close on buffer pressure")
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
		QueueStatePath: t.TempDir() + "/queue.json",
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)

	connID := netcore.ConnID(7099) // never registered

	reply := protocol.Frame{Type: "subscribed"}
	err := svc.sendHandshakeReplyViaNetwork(context.Background(), connID, reply)
	if err == nil {
		t.Fatal("expected error on unknown ConnID, got nil")
	}
	// The exact sentinel is an implementation detail; the contract is
	// "non-nil, and the helper survives".
}
