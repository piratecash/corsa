package node

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// peer_send_queue_test.go covers the UPPER outbound queue: what it accepts,
// what it refuses, and that neither exit leaves a frame pinned in it.
//
// The queue used to owe every tracked frame a terminal notification, and these
// tests were written around that: they asserted "exactly one terminal per
// operation" and read the queue's real behaviour off it. Nothing in production
// ever listened, so what is left to assert is the queue itself — the accept/
// refuse answer, the length after a teardown, and the bytes on the wire.

// trackedItem builds an upper-queue element carrying an outbound contract. The
// deadline is far enough away that it never decides anything here: what makes
// the item TRACKED is the ticket's existence, which is what routes it onto the
// managed writer path in servePeerSession.
func trackedItem(frameType, id string) peerSendItem {
	return peerSendItem{
		Frame: protocol.Frame{Type: frameType, ID: id},
		ticket: netcore.NewWriteTicket(netcore.OutboundWrite{
			SendUntil: domain.TimeOf(time.Now().Add(time.Hour)),
		}),
	}
}

// trackedTicket is trackedItem's contract on its own, for the Service-level
// entry points that take a ticket rather than a queue element.
func trackedTicket() *netcore.WriteTicket {
	return netcore.NewWriteTicket(netcore.OutboundWrite{
		SendUntil: domain.TimeOf(time.Now().Add(time.Hour)),
	})
}

// newQueueTestSession builds a peerSession wired to a live NetCore over a
// net.Pipe. Nothing reads the far end unless the test starts a reader, so
// the writer goroutine blocks on its first socket write — which is what lets
// a test park frames in either of the two queues on purpose.
func newQueueTestSession(t *testing.T, addr domain.PeerAddress, sendBuffer int) (*peerSession, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close() })
	t.Cleanup(func() { _ = remote.Close() })

	session := &peerSession{
		address:      addr,
		conn:         remote,
		sendCh:       make(chan peerSendItem, sendBuffer),
		inboxCh:      make(chan protocol.Frame, 4),
		errCh:        make(chan error, 1),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}
	session.netCore = netcore.New(netcore.ConnID(1), remote, netcore.Outbound, netcore.Options{})
	return session, local
}

// TestUpperQueueResidueIsReleasedWhenSessionDies is the §9 upper-queue
// contract: a frame still sitting in peerSession.sendCh — never handed to
// NetCore — is released when the session dies rather than pinned by a buffered
// channel for as long as anything holds the session, and the producer fence
// that makes the drain complete is permanent.
func TestUpperQueueResidueIsReleasedWhenSessionDies(t *testing.T) {
	t.Parallel()
	addr := domain.PeerAddress("10.0.0.71:64646")
	session, _ := newQueueTestSession(t, addr, 8)

	// No serve loop runs, so these frames provably never reach NetCore.
	const residue = 3
	for i := 0; i < residue; i++ {
		if !session.enqueueSend(trackedItem("push_message", "residue")) {
			t.Fatalf("enqueueSend(%d) refused on an empty queue", i)
		}
	}
	if got := session.sendQueueLen(); got != residue {
		t.Fatalf("upper queue length = %d, want %d", got, residue)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("session.Close: %v", err)
	}
	if got := session.sendQueueLen(); got != 0 {
		t.Fatalf("upper queue still holds %d frames after teardown", got)
	}

	// The fence is permanent: a producer arriving after the teardown is
	// refused, so nothing can be added behind the drain that already ran.
	if session.enqueueSend(trackedItem("push_message", "late")) {
		t.Fatal("enqueueSend accepted a frame after the queue was fenced")
	}

	// Idempotent: a second teardown (Close racing the serve-loop exit) must
	// neither panic nor un-fence the queue.
	session.closeSendQueue()
	if session.enqueueSend(trackedItem("push_message", "later")) {
		t.Fatal("a second teardown re-opened the queue")
	}
}

// TestServePeerSessionExitDrainsTheUpperQueue drives the real serve loop:
// frames are parked in BOTH queues, the session dies, and the upper queue must
// come out empty. Which queue a given frame died in is deliberately not
// asserted — the contract is that the serve loop's exit accounts for the
// residue whatever the cause of the exit was.
func TestServePeerSessionExitDrainsTheUpperQueue(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("10.0.0.72:64646")
	session, _ := newQueueTestSession(t, addr, 32)

	svc.peerMu.Lock()
	svc.sessions[addr] = session
	svc.health[addr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Saturate the NetCore queue first: nobody reads the pipe, so the writer
	// goroutine is stuck inside its first socket write and everything the
	// serve loop forwards piles up behind it.
	saturateSessionWriteQueue(t, session)

	const tracked = 24
	for i := 0; i < tracked; i++ {
		if !session.enqueueSend(trackedItem("push_message", "tracked")) {
			t.Fatalf("enqueueSend(%d) refused", i)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.servePeerSession(ctx, session) }()

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("servePeerSession did not exit on context cancellation")
	}

	if got := session.sendQueueLen(); got != 0 {
		t.Fatalf("upper queue still holds %d frames after the serve loop exited", got)
	}
}

// TestUpperQueueRefusesOnSaturation pins the eviction-at-the-door case: a
// frame the upper queue has no room for is refused to its producer, which owns
// the fallback policy, instead of being swallowed.
func TestUpperQueueRefusesOnSaturation(t *testing.T) {
	t.Parallel()
	addr := domain.PeerAddress("10.0.0.73:64646")
	session, _ := newQueueTestSession(t, addr, 2)
	defer func() { _ = session.Close() }()

	for i := 0; i < 2; i++ {
		if !session.enqueueSend(legacyPeerSendItem(protocol.Frame{Type: "push_message", ID: "filler"})) {
			t.Fatalf("filler %d refused before the queue was full", i)
		}
	}

	if session.enqueueSend(trackedItem("push_message", "refused")) {
		t.Fatal("enqueueSend accepted a frame on a full queue")
	}
	if got := session.sendQueueLen(); got != 2 {
		t.Fatalf("upper queue length = %d, want 2 — the refused frame was queued anyway", got)
	}
}

// TestSendTrackedFrameToSessionRefusesEveryClosedDoor covers the Service-level
// admission gate: every reason the frame cannot be accepted (no session at all,
// stalled peer, fenced queue) is reported to the caller as a refusal, and the
// frame reaches no queue.
func TestSendTrackedFrameToSessionRefusesEveryClosedDoor(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	t.Run("no session", func(t *testing.T) {
		accepted := svc.sendTrackedFrameToSession(
			nil,
			protocol.Frame{Type: "push_message", ID: "no-session"},
			trackedTicket(),
		)
		if accepted {
			t.Fatal("sendTrackedFrameToSession accepted a frame without a session")
		}
	})

	t.Run("stalled peer", func(t *testing.T) {
		addr := domain.PeerAddress("10.0.0.75:64646")
		session, _ := newQueueTestSession(t, addr, 4)
		defer func() { _ = session.Close() }()

		svc.peerMu.Lock()
		svc.sessions[addr] = session
		// Connected but silent for far longer than two heartbeat cycles —
		// computePeerStateAtLocked reports stalled.
		svc.health[addr] = &peerHealth{
			Connected:           true,
			LastUsefulReceiveAt: time.Now().UTC().Add(-10 * time.Minute),
		}
		svc.peerMu.Unlock()

		accepted := svc.sendTrackedFrameToSession(
			session,
			protocol.Frame{Type: "push_message", ID: "stalled"},
			trackedTicket(),
		)
		if accepted {
			t.Fatal("sendTrackedFrameToSession accepted a frame for a stalled peer")
		}
		if got := session.sendQueueLen(); got != 0 {
			t.Fatalf("refused frame reached the queue: %d elements", got)
		}
	})

	t.Run("fenced queue", func(t *testing.T) {
		addr := domain.PeerAddress("10.0.0.78:64646")
		session, _ := newQueueTestSession(t, addr, 4)

		svc.peerMu.Lock()
		svc.sessions[addr] = session
		svc.health[addr] = &peerHealth{Connected: true, LastUsefulReceiveAt: time.Now().UTC()}
		svc.peerMu.Unlock()

		// A session the caller still holds after its connection died is the
		// reconnect case: the refusal must come from THIS session's fence and
		// must not be answered by whatever now sits under its address.
		if err := session.Close(); err != nil {
			t.Fatalf("session.Close: %v", err)
		}
		if svc.sendTrackedFrameToSession(
			session,
			protocol.Frame{Type: "push_message", ID: "fenced"},
			trackedTicket(),
		) {
			t.Fatal("sendTrackedFrameToSession accepted a frame on a fenced queue")
		}
	})
}

// TestTrackedFrameTakesManagedWriterPath verifies that a tracked frame whose
// type is NOT fire-and-forget still goes to the managed writer instead of the
// request/reply path. A tracked frame expects no reply, so the request path
// would park this serve loop on a reply that is never coming — which is what
// the SECOND frame here detects: it can only reach the wire if the loop went
// back for it.
func TestTrackedFrameTakesManagedWriterPath(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	addr := domain.PeerAddress("10.0.0.76:64646")
	session, peer := newQueueTestSession(t, addr, 4)

	svc.peerMu.Lock()
	svc.sessions[addr] = session
	svc.health[addr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Drain the far end so the writer goroutine can complete the write, but
	// NEVER answer: a request/reply dispatch would block here forever.
	lines := make(chan string, 8)
	go func() {
		reader := bufio.NewReader(peer)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- svc.servePeerSession(ctx, session) }()

	// "get_messages" is a request-type frame: isFireAndForgetFrame is false.
	if isFireAndForgetFrame("get_messages") {
		t.Fatal("test premise broken: get_messages is fire-and-forget")
	}
	for i, id := range []string{"tracked-request", "tracked-second"} {
		if !session.enqueueSend(trackedItem("get_messages", id)) {
			t.Fatalf("enqueueSend(%d) refused", i)
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case line := <-lines:
			if line == "" {
				t.Fatal("empty line on the wire")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d tracked frames reached the socket: the serve loop is "+
				"waiting for a reply the request/reply path would expect", i)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("servePeerSession did not exit on context cancellation")
	}
}

// TestSendTrackedFrameToConnUsesTheSingleWriterQueue covers the inbound
// direction, which has no peerSession and therefore only ONE queue: the same
// contract must work where the upper queue does not exist at all.
//
// The peer-state gate is part of that contract on this tier too, so the fixture
// carries the health entry a post-handshake inbound connection has. A
// connection whose peer has gone is the last case: the candidate walk that
// reaches this helper has to be sent on to the peer's next connection rather
// than have the frame accepted by a queue whose socket is finished.
func TestSendTrackedFrameToConnUsesTheSingleWriterQueue(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	address := domain.PeerAddress("10.0.0.91:64646")

	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })
	core := netcore.New(netcore.ConnID(9911), serverPipe, netcore.Inbound, netcore.Options{Address: address})
	t.Cleanup(core.Close)

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: core, tracked: true})
	svc.health[address] = &peerHealth{Connected: true, LastUsefulReceiveAt: time.Now().UTC()}
	svc.peerMu.Unlock()

	lines := make(chan string, 4)
	go func() {
		reader := bufio.NewReader(clientPipe)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()

	if !svc.sendTrackedFrameToConn(core.ConnID(), protocol.Frame{Type: "ping"}, trackedTicket()) {
		t.Fatal("sendTrackedFrameToConn refused a live inbound conn")
	}
	select {
	case <-lines:
	case <-time.After(5 * time.Second):
		t.Fatal("frame never reached the inbound socket")
	}

	if svc.sendTrackedFrameToConn(
		domain.ConnID(4242),
		protocol.Frame{Type: "ping"},
		trackedTicket(),
	) {
		t.Fatal("sendTrackedFrameToConn accepted an unknown connection")
	}

	// The socket is still writable — only the peer state moved. The refusal
	// must come from the admission, exactly as it does on the session tier.
	svc.markPeerDisconnected(address, errors.New("peer closed the socket"))
	if svc.sendTrackedFrameToConn(core.ConnID(), protocol.Frame{Type: "ping"}, trackedTicket()) {
		t.Fatal("sendTrackedFrameToConn accepted a frame for a disconnected peer, so the " +
			"candidate walk stops on a connection nothing will read")
	}
}

// TestLegacyFrameKeepsPreTicketQueueBehaviour is the regression guard for
// existing traffic: an item without a contract is accepted, dequeued and
// delivered exactly as before, and the teardown drain does not choke on it.
func TestLegacyFrameKeepsPreTicketQueueBehaviour(t *testing.T) {
	t.Parallel()
	addr := domain.PeerAddress("10.0.0.77:64646")
	session, _ := newQueueTestSession(t, addr, 4)

	frame := protocol.Frame{Type: "announce_routes", ID: "legacy"}
	if !session.enqueueSend(legacyPeerSendItem(frame)) {
		t.Fatal("enqueueSend refused a legacy frame")
	}
	item := <-session.sendCh
	if item.tracked() {
		t.Fatal("legacy item must carry no outbound contract")
	}
	if item.ID != frame.ID || item.Type != frame.Type {
		t.Fatalf("dequeued %+v, want %+v", item.Frame, frame)
	}

	// Residue of untracked frames must drain without panicking on the nil
	// ticket — the fire-and-forget majority of the traffic looks like this.
	if !session.enqueueSend(legacyPeerSendItem(frame)) {
		t.Fatal("enqueueSend refused the second legacy frame")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("session.Close: %v", err)
	}
	if got := session.sendQueueLen(); got != 0 {
		t.Fatalf("upper queue still holds %d legacy frames", got)
	}
}

// TestNilSessionEnqueueRefusesInsteadOfPanicking covers the last refusal
// branch: a lookup that returned no session at all must answer "refused"
// rather than dereference nil.
func TestNilSessionEnqueueRefusesInsteadOfPanicking(t *testing.T) {
	t.Parallel()
	var session *peerSession
	if session.enqueueSend(trackedItem("push_message", "nil-session")) {
		t.Fatal("enqueueSend on a nil session returned accepted")
	}
	session.closeSendQueue() // must not panic
	if got := session.sendQueueLen(); got != 0 {
		t.Fatalf("nil session reported %d queued frames", got)
	}
}
