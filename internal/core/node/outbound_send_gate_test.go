package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// outbound_send_gate_test.go pins the ADMISSION into an outbound session's
// queue: which peer states may still be handed a frame.
//
// The window both tests below live in is the one a disconnect actually
// produces, and it is not the one the old gate defended against. A session
// dies, servePeerSession calls markPeerDisconnected — which leaves the peer in
// peerStateReconnecting, NOT peerStateStalled — and only then returns, running
// the deferred closeSendQueue. Between those two statements the health map
// already says "gone" while the queue still accepts, and a gate that knew only
// about stalled let the frame through: the queue took it, the producer read
// that as a delivery, and the drain discarded it moments later.
//
// Both tests therefore set exactly that window: health flipped, queue NOT
// fenced. A test that closed the queue would be testing the fence, which was
// never broken.

// disconnectPeerLeavingTheQueueOpen produces the window under test on a session
// the caller still holds: the peer is marked disconnected exactly as the serve
// loop marks it, and the queue is deliberately left open because in production
// closeSendQueue runs one deferred call later.
func disconnectPeerLeavingTheQueueOpen(t *testing.T, svc *Service, session *peerSession) {
	t.Helper()
	svc.markPeerDisconnected(session.address, errors.New("peer closed the socket"))
	if state := svc.peerStateForTest(session.address); state != peerStateReconnecting {
		t.Fatalf("peer state after a disconnect = %q, want %q: the test no longer covers "+
			"the window it names", state, peerStateReconnecting)
	}
}

// peerStateForTest reports the state the admission gate reads. It exists so the
// tests below can ASSERT their own premise instead of assuming it. An address
// with no health entry answers the empty string, which is neither of the states
// a test names.
func (s *Service) peerStateForTest(address domain.PeerAddress) string {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	health := s.health[s.resolveHealthAddress(address)]
	if health == nil {
		return ""
	}
	return s.computePeerStateAtLocked(health, time.Now().UTC())
}

// TestEnqueueSessionSendItemRefusesADisconnectedSession is the finding itself,
// asked of the admission point directly.
func TestEnqueueSessionSendItemRefusesADisconnectedSession(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	svc := newDatagramLayerService(t, true)
	addresses := installDatagramPeer(t, svc, peer, datagramPeerConn{
		version:     27,
		connectedAt: time.Now().UTC().Add(-time.Hour),
	})

	svc.peerMu.RLock()
	session := svc.sessions[addresses[false]]
	svc.peerMu.RUnlock()
	if session == nil {
		t.Fatal("the fixture did not install the outbound session")
	}

	disconnectPeerLeavingTheQueueOpen(t, svc, session)

	item := legacyPeerSendItem(protocol.Frame{Type: "announce_peer", ID: "after-disconnect"})
	if svc.enqueueSessionSendItem(session, item) {
		t.Fatal("the admission accepted a frame for a session whose peer is already " +
			"disconnected: the queue will be drained without sending it")
	}
	if got := session.sendQueueLen(); got != 0 {
		t.Fatalf("the refused frame reached the queue anyway: %d elements", got)
	}

	// The refusal has to come from the ADMISSION and not from the fence:
	// closeSendQueue has not run, which is the whole point of the window.
	if !session.enqueueSend(item) {
		t.Fatal("the fixture fenced the queue, so the refusal above proves nothing")
	}
}

// TestEmitToWalksOnWhenTheSelectedSessionDisconnects pins the consequence that
// makes the finding a lost frame rather than a late one: the emitter must not
// read the acceptance of a dead session's queue as a delivery, because that
// ENDS the candidate walk and the peer's remaining connection never sees the
// frame.
func TestEmitToWalksOnWhenTheSelectedSessionDisconnects(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	now := time.Now().UTC()
	installDatagramPeer(t, svc, peer,
		datagramPeerConn{version: 27, connectedAt: now.Add(-2 * time.Hour)},
		datagramPeerConn{version: 27, connectedAt: now.Add(-time.Hour)},
	)

	frame := newNodeDatagram(t, nil)
	// Read the attempt order from the selection itself: the test's premise is
	// "the FIRST candidate dies", and only the selection can say which that is.
	svc.peerMu.RLock()
	targets := svc.datagramFrameSendTargetsLocked(frame, peer, time.Now().UTC())
	svc.peerMu.RUnlock()
	if len(targets) != 2 || targets[0].session == nil || targets[1].session == nil {
		t.Fatalf("the fixture must offer two outbound candidates, got %d", len(targets))
	}
	selected, next := targets[0].session, targets[1].session

	emitter := datagramFrameEmitter{
		service: svc,
		selectionBarrier: func() {
			disconnectPeerLeavingTheQueueOpen(t, svc, selected)
		},
	}

	if !emitter.EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: now.Add(5 * time.Second),
	}) {
		t.Fatal("the peer still had a live connection, so the walk had somewhere to go")
	}
	if got := selected.sendQueueLen(); got != 0 {
		t.Fatalf("the disconnected session took %d frames: its queue is drained without "+
			"sending, so the frame is lost", got)
	}
	if got := next.sendQueueLen(); got != 1 {
		t.Fatalf("the next candidate holds %d frames, want 1: the emitter read the dead "+
			"session's acceptance as a delivery and stopped the walk", got)
	}
}
