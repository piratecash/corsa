package node

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
)

// ---------------------------------------------------------------------------
// P1 + P2 — full subscribe_inbox handshake path under buffer pressure.
//
// Two regressions guarded here:
//
//   P1 — the REVERSE subscribe_inbox emitted right after the subscribed
//        ack must also use the no-eviction helper. Before the fix the
//        ack was protected but the reverse frame went through plain
//        sendFrameViaNetwork, which calls network.Close() on
//        ErrSendBufferFull — undoing the protection on the very next
//        line of the handler.
//
//   P2 — when the subscribed ack itself fails to reach the peer (timeout
//        or ErrUnknownConn) the handler must roll back the subscriber it
//        just registered in subscribeInboxFrame. Otherwise the gossip
//        map grows a phantom subscriber that will never be cleaned up
//        because the connection no longer dies on buffer pressure (the
//        whole point of the helper).
// ---------------------------------------------------------------------------

// fullPeerIdentity is a 40-char hex string that satisfies the identity
// match in case "subscribe_inbox" (recipient == authenticated identity).
const fullPeerIdentity = "1111111111111111111111111111111111111111"

// newSubscribeInboxFixture wires up a Service with a netcoretest.Backend
// whose outbound queue has the supplied capacity. The returned conn is
// auth_verified and has its Identity set so the subscribe_inbox handler
// passes the recipient-match gate.
//
// runCtxTimeout overrides svc.runCtx so the helper's internal
// handshakeReplyTimeout does not need to fully elapse on the
// not-going-to-drain path — speeds the tests from ~4s to ~runCtxTimeout.
func newSubscribeInboxFixture(t *testing.T, outboundBuffer int, runCtxTimeout time.Duration) (svc *Service, connID netcore.ConnID, backend *netcoretest.Backend, cancel context.CancelFunc) {
	t.Helper()

	backend = netcoretest.NewWithOptions(netcoretest.Options{OutboundBuffer: outboundBuffer})
	t.Cleanup(backend.Shutdown)

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	svc = NewServiceWithNetwork(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: t.TempDir() + "/trust.json",
		QueueStatePath: t.TempDir() + "/queue.json",
	}, id, backend)
	t.Cleanup(svc.WaitBackground)

	// Inject a bounded runCtx so handshakeReplyTimeout does not have to
	// fully elapse on the wedged-buffer paths below.
	ctx, cancel := context.WithTimeout(context.Background(), runCtxTimeout)
	svc.runCtx = ctx
	t.Cleanup(cancel)

	connID = netcore.ConnID(9201)
	backend.Register(connID, netcore.Inbound, "10.0.0.55:50001")

	// Mint a NetCore for auth + identity wiring. The conn used is a
	// throwaway since the actual wire-bytes path goes via the backend.
	cliPipe, srvPipe := net.Pipe()
	t.Cleanup(func() { _ = cliPipe.Close() })
	t.Cleanup(func() { _ = srvPipe.Close() })
	pc := netcore.New(connID, srvPipe, netcore.Inbound, netcore.Options{})
	t.Cleanup(pc.Close)
	pc.SetAuth(&connauth.State{Verified: true})
	pc.SetIdentity(domain.PeerIdentity(fullPeerIdentity))

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(cliPipe, &connEntry{core: pc})
	svc.peerMu.Unlock()

	return svc, connID, backend, cancel
}

// TestSubscribeInbox_BufferFull_DoesNotCloseConn covers the combined
// P1 + P2 contract: with the outbound buffer wedged the handler must
// (a) keep the connection registered (P1: reverse subscribe_inbox does
// not evict), (b) leave no phantom subscriber in s.subs (P2: the
// failed ack rolled back its own registration).
func TestSubscribeInbox_BufferFull_DoesNotCloseConn(t *testing.T) {
	t.Parallel()

	svc, connID, backend, _ := newSubscribeInboxFixture(t, 1, 250*time.Millisecond)

	// Wedge the buffer with one frame that nobody will drain.
	if err := backend.Network().SendFrame(context.Background(), connID, []byte("blocker\n")); err != nil {
		t.Fatalf("seed outbound: %v", err)
	}

	// subscribe_inbox with recipient == authenticated identity.
	frameLine := `{"type":"subscribe_inbox","topic":"dm","recipient":"` + fullPeerIdentity + `","subscriber":"sub-1"}`
	if !svc.dispatchNetworkFrame(connID, frameLine) {
		t.Fatal("dispatchNetworkFrame returned false on subscribe_inbox")
	}

	// P1: connection must still be registered. Pre-fix the reverse
	// subscribe_inbox would call network.Close() on ErrSendBufferFull.
	if backend.RemoteAddr(connID) == "" {
		t.Fatal("P1 regression: connection was evicted on buffer-full reverse subscribe_inbox")
	}

	// P2: the subscriber registered by subscribeInboxFrame must have
	// been rolled back when the ack failed to land. SubscriberCount
	// reports zero only if the rollback happened.
	if got := svc.SubscriberCount(fullPeerIdentity); got != 0 {
		t.Fatalf("P2 regression: phantom subscriber survived failed ack; SubscriberCount=%d, want 0", got)
	}
}

// TestSubscribeInbox_HappyPath_KeepsSubscriber covers the positive
// case: when the ack lands successfully, the subscriber is NOT rolled
// back. This is the regression sister test for P2 — without it a
// rollback path that fires unconditionally would silently break live
// delivery on every healthy subscribe.
func TestSubscribeInbox_HappyPath_KeepsSubscriber(t *testing.T) {
	t.Parallel()

	svc, connID, backend, _ := newSubscribeInboxFixture(t, 16, 2*time.Second)

	// Drain backend.Outbound in the background so the ack and the
	// reverse subscribe_inbox land without blocking.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-backend.Outbound(connID):
			case <-time.After(500 * time.Millisecond):
				return
			}
		}
	}()

	frameLine := `{"type":"subscribe_inbox","topic":"dm","recipient":"` + fullPeerIdentity + `","subscriber":"sub-happy"}`
	if !svc.dispatchNetworkFrame(connID, frameLine) {
		t.Fatal("dispatchNetworkFrame returned false on subscribe_inbox")
	}

	// Wait until the drain goroutine sees both frames.
	<-drained

	if got := svc.SubscriberCount(fullPeerIdentity); got != 1 {
		t.Fatalf("happy-path regression: SubscriberCount=%d, want 1 (subscriber was incorrectly rolled back)", got)
	}
	if backend.RemoteAddr(connID) == "" {
		t.Fatal("happy-path regression: connection was evicted on a healthy subscribe_inbox")
	}
}

// TestSubscribeInbox_BufferFull_ReverseFrameNeverArrives is the
// observable witness to P1: count the frames the backend received. With
// a buffer of capacity 1 already wedged, the failed ack must roll back
// the subscriber so the reverse subscribe_inbox is SKIPPED entirely.
// Pre-P2 fix the reverse frame would be issued anyway and either evict
// (pre-P1 fix) or stack up another timeout. After the fix the handler
// returns immediately on ack failure.
func TestSubscribeInbox_BufferFull_ReverseFrameNeverArrives(t *testing.T) {
	t.Parallel()

	svc, connID, backend, _ := newSubscribeInboxFixture(t, 1, 250*time.Millisecond)

	// Wedge.
	if err := backend.Network().SendFrame(context.Background(), connID, []byte("blocker\n")); err != nil {
		t.Fatalf("seed outbound: %v", err)
	}

	frameLine := `{"type":"subscribe_inbox","topic":"dm","recipient":"` + fullPeerIdentity + `","subscriber":"sub-1"}`
	if !svc.dispatchNetworkFrame(connID, frameLine) {
		t.Fatal("dispatchNetworkFrame returned false on subscribe_inbox")
	}

	// Drain the wedged blocker and see what's behind it. The handler
	// MUST have returned after the failed ack; the reverse frame must
	// NOT be queued behind the blocker.
	select {
	case data := <-backend.Outbound(connID):
		if string(data) != "blocker\n" {
			t.Fatalf("expected only the wedged blocker, got %q — handler queued extra frames", string(data))
		}
	case <-time.After(time.Second):
		t.Fatal("blocker did not surface — backend wiring broken?")
	}

	// At this point the buffer has one free slot. Nothing else should
	// arrive — the handler bailed out after the rollback.
	select {
	case data := <-backend.Outbound(connID):
		f, _ := parseFrameLineForTest(data)
		t.Fatalf("unexpected frame after rollback: type=%q raw=%q — handler must skip reverse subscribe on ack failure", f.Type, data)
	case <-time.After(200 * time.Millisecond):
		// Expected: nothing follows.
	}
}
