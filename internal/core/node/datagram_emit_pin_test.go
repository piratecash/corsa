package node

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// datagram_emit_pin_test.go pins the last step of the hand-over: the frame is
// queued on the CONNECTION the selection chose, and on no other.
//
// The selection reads a peer's connections under peerMu, applies the frame's
// gates per connection (§4.3, §6) and, for a frame the layer pinned to a
// channel, narrows the walk to that one socket (§4.2). All of that describes
// concrete sockets. The hand-over used to describe an ADDRESS — the dial
// address of an outbound session, the "inbound:<remoteAddr>" key of an accepted
// one — and both were re-resolved at the queue. A peer that reconnected in
// between owns those addresses with a NEW socket: a different handshake, a
// different declared dtype set and a different ConnID. The frame then left over
// a connection nobody had gated, and on the request plane over a channel the
// reverse record does not name — which is exactly the substitution the pin
// exists to prevent.
//
// Every test below drives the window through the emitter's selectionBarrier,
// which stops the emission between the selection and the first hand-over. The
// window lives between two statements of EmitTo, so nothing outside the package
// can produce it and a sleep would pin the scheduler instead of the addressing.

// datagramPinFixture is one peer with ONE outbound session, the emitter that
// serves it, and the outbound frame under test.
type datagramPinFixture struct {
	service  *Service
	selected *peerSession
	out      datagram.OutboundFrame
}

// newDatagramPinFixture installs a single outbound session that passes every
// gate of the frame, so the only thing a test can change is WHICH session the
// hand-over reaches.
func newDatagramPinFixture(
	t *testing.T,
	peer domain.PeerIdentity,
	frame protocol.DatagramFrame,
) datagramPinFixture {
	t.Helper()

	svc := newDatagramLayerService(t, true)
	now := time.Now().UTC()
	addresses := installDatagramPeer(t, svc, peer, datagramPeerConn{
		version:     27,
		connectedAt: now.Add(-time.Hour),
	})
	requireDatagramPlane(t, svc)

	svc.peerMu.RLock()
	session := svc.sessions[addresses[false]]
	svc.peerMu.RUnlock()
	if session == nil {
		t.Fatal("the fixture did not install the outbound session")
	}

	return datagramPinFixture{
		service:  svc,
		selected: session,
		out: datagram.OutboundFrame{
			Peer:      peer,
			Frame:     frame,
			Line:      []byte(mustDatagramLine(t, frame)),
			Class:     frame.Class,
			SendUntil: now.Add(5 * time.Second),
		},
	}
}

// reconnectPeerSession is what a reconnect leaves behind: the peer's address is
// rebound to a BRAND NEW peerSession with its own ConnID and its own queue,
// while the session the selection captured keeps existing as an object.
//
// It deliberately does not touch the old session's queue. Whether the old
// channel is still writable is the difference between the two failures under
// test — a frame diverted onto a live new socket, and a pinned frame delivered
// after its own channel died — so each test decides that for itself.
func reconnectPeerSession(t *testing.T, svc *Service, old *peerSession) *peerSession {
	t.Helper()
	fresh := &peerSession{
		address:      old.address,
		connID:       old.connID + 1,
		peerIdentity: old.peerIdentity,
		capabilities: old.capabilities,
		declarations: old.declarations.Clone(),
		sendCh:       make(chan peerSendItem, 8),
		authOK:       true,
		version:      old.version,
	}
	svc.peerMu.Lock()
	svc.sessions[old.address] = fresh
	svc.peerMu.Unlock()
	return fresh
}

// TestEmitToQueuesOnTheSelectedSessionAfterAReconnect is the finding on the
// routed plane, where no channel is pinned and the peer's address still names a
// perfectly usable socket.
//
// The mutation it kills is the original hand-over: SendTrackedFrameToPeer keyed
// on target.address, which resolves s.sessions[address] AGAIN. Under that
// mutation the frame lands in the reconnected session — the one whose
// capabilities and declared dtypes the scheduler never saw.
func TestEmitToQueuesOnTheSelectedSessionAfterAReconnect(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	fixture := newDatagramPinFixture(t, peer, newNodeDatagram(t, nil))

	var reconnected *peerSession
	emitter := datagramFrameEmitter{
		service: fixture.service,
		selectionBarrier: func() {
			reconnected = reconnectPeerSession(t, fixture.service, fixture.selected)
		},
	}

	if !emitter.EmitTo(context.Background(), fixture.out) {
		t.Fatal("the selected session was writable, so the frame had a queue to land in")
	}
	if got := fixture.selected.sendQueueLen(); got != 1 {
		t.Fatalf("the selected session holds %d frames, want 1: the frame was addressed by "+
			"the peer's address and followed the reconnect instead of staying on the socket "+
			"the scheduler judged", got)
	}
	if got := len(reconnected.sendCh); got != 0 {
		t.Fatalf("the reconnected session holds %d frames: its handshake was never gated for "+
			"this frame", got)
	}
}

// TestEmitToRefusesAPinnedFrameWhenItsChannelIsGone is the reverse plane.
//
// A `response` travels to the upstream of a reverse-state record (§4.2), and
// the record stores the CHANNEL the question arrived on. EmitTo narrows the walk
// to that one connection — and then handed the frame over by address, so a
// reconnect turned "answer over the channel the record names" back into "answer
// over whatever session that node's name resolves to now". A refusal is the
// correct outcome here: the return path of the question is gone, and the caller
// treats it like any other unreachable target.
func TestEmitToRefusesAPinnedFrameWhenItsChannelIsGone(t *testing.T) {
	t.Parallel()

	upstream := domain.PeerIdentityFromWire("2222222222222222222222222222222222222222")
	fixture := newDatagramPinFixture(t, upstream, newDatagramResponseFrame(t))
	fixture.out.Channel = datagram.NetworkChannel(fixture.selected.connID)

	var reconnected *peerSession
	emitter := datagramFrameEmitter{
		service: fixture.service,
		selectionBarrier: func() {
			reconnected = reconnectPeerSession(t, fixture.service, fixture.selected)
			// The channel the record names is gone with its session: a
			// reconnect fences the old queue before the new one appears.
			fixture.selected.closeSendQueue()
		},
	}

	if emitter.EmitTo(context.Background(), fixture.out) {
		t.Fatal("a pinned answer reported success although the channel it was pinned to no longer exists")
	}
	if got := len(reconnected.sendCh); got != 0 {
		t.Fatalf("the answer left over the reconnected session: %d frames queued on a channel "+
			"the reverse record does not name", got)
	}
}

// installDatagramInboundConn registers ONE accepted connection of peer at the
// given ConnID and returns the far end of its socket, so a test can both retire
// the connection and watch what does or does not reach the wire.
//
// It mirrors the inbound branch of installDatagramPeer, with the ConnID and the
// net.Conn handed back rather than hidden: this file needs to retire a specific
// connection and register its successor under the SAME remote address, which is
// what a reconnect looks like to a walker that matches on the address.
func installDatagramInboundConn(
	t *testing.T,
	svc *Service,
	peer domain.PeerIdentity,
	id domain.ConnID,
	address domain.PeerAddress,
	connectedAt time.Time,
) (net.Conn, net.Conn) {
	t.Helper()

	declarations := datagramPeerDeclarations()
	clientPipe, serverPipe := net.Pipe()
	t.Cleanup(func() { _ = clientPipe.Close() })
	t.Cleanup(func() { _ = serverPipe.Close() })

	core := netcore.New(id, serverPipe, netcore.Inbound, netcore.Options{
		Address:         address,
		Identity:        peer,
		Caps:            []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1},
		ProtocolVersion: 27,
		Declarations:    &declarations,
	})
	t.Cleanup(core.Close)

	svc.peerMu.Lock()
	svc.setTestConnEntryLocked(clientPipe, &connEntry{core: core, tracked: true})
	svc.health[address] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     connectedAt,
		LastUsefulReceiveAt: time.Now().UTC(),
	}
	svc.peerMu.Unlock()

	return clientPipe, serverPipe
}

// TestEmitToDoesNotFollowAReconnectToANewInboundConnection is the same finding
// on the accepted tier, where the address is a "inbound:<remoteAddr>" key and
// the sender behind it walked the tracked connections looking for a match.
//
// A reconnect from the same host:port produces a second connection under that
// very key, so the walk found the successor of the socket the selection had
// chosen — and its declarations, like any freshly accepted connection's, had
// been through no gate for this frame.
func TestEmitToDoesNotFollowAReconnectToANewInboundConnection(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)

	address := domain.PeerAddress("10.9.0.1:64646")
	const selectedID, reconnectedID = domain.ConnID(9101), domain.ConnID(9102)
	retiring, _ := installDatagramInboundConn(t, svc, peer, selectedID, address, time.Now().UTC().Add(-time.Hour))

	frame := newNodeDatagram(t, nil)
	successorWire := make(chan string, 4)
	emitter := datagramFrameEmitter{
		service: svc,
		selectionBarrier: func() {
			svc.peerMu.Lock()
			svc.unregisterConnLocked(retiring)
			svc.peerMu.Unlock()
			far, _ := installDatagramInboundConn(t, svc, peer, reconnectedID, address, time.Now().UTC())
			go readWireLines(far, successorWire)
		},
	}

	if emitter.EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: time.Now().UTC().Add(5 * time.Second),
	}) {
		t.Fatal("the selected connection was retired, yet the emitter reported the frame queued")
	}
	select {
	case line := <-successorWire:
		t.Fatalf("the frame left over the connection that replaced the selected one: %q", line)
	case <-time.After(250 * time.Millisecond):
	}
}

// readWireLines drains conn into lines until the connection ends. Used to watch
// a socket that must stay silent.
func readWireLines(conn net.Conn, lines chan<- string) {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			lines <- line
		}
		if err != nil {
			return
		}
	}
}
