package node

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestFireAndForgetBufferFullDropsFrameKeepsSession pins the slow-peer
// backpressure contract of servePeerSession: when the netcore write
// queue is saturated (Send returns SendBufferFull), a fire-and-forget
// frame is DROPPED and the session stays alive.
//
// The previous behaviour (markPeerDisconnected + teardown) converted
// transient backpressure into a reconnect storm: the teardown discarded
// the entire write queue, forced a redial, a connect-time full table
// sync and a baseline resync — far more wire traffic than the single
// dropped frame — and the self-inflicted disconnect fed the
// disconnect_storm quarantine of an innocent peer.
//
// The test saturates a real NetCore over a net.Pipe whose remote end is
// never read: the writer goroutine blocks on its first Write, the queue
// fills up, and the next fire-and-forget frame hits SendBufferFull
// deterministically.
func TestFireAndForgetBufferFullDropsFrameKeepsSession(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.52:64646")
	session := &peerSession{
		address:      peerAddr,
		conn:         remote,
		sendCh:       make(chan protocol.Frame, 4),
		inboxCh:      make(chan protocol.Frame, 4),
		errCh:        make(chan error, 1),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}
	attachTestNetCore(svc, session)

	svc.peerMu.Lock()
	svc.sessions[peerAddr] = session
	svc.health[peerAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Saturate the write queue. Nobody reads from `local`, so the writer
	// goroutine blocks inside its first rawConn.Write and every further
	// Send lands in the queue until it reports SendBufferFull. The loop
	// bound is a safety net only — saturation needs queue-depth+1 sends.
	filler := protocol.Frame{Type: "push_message", ID: "filler"}
	saturated := false
	for i := 0; i < 100000; i++ {
		if st := session.netCore.Send(filler); st == netcore.SendBufferFull {
			saturated = true
			break
		}
	}
	if !saturated {
		t.Fatal("could not saturate the netcore send queue")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.servePeerSession(ctx, session)
	}()

	// Deliver a fire-and-forget frame into the saturated session. The
	// serve loop must drop it and keep running.
	session.sendCh <- protocol.Frame{Type: "push_message", ID: "dropped-on-full-queue"}

	// The assertion window must stay well under the netcore per-write
	// deadline (sessionWriteTimeout, 3s): once the blocked Write times
	// out the writer goroutine exits and servePeerSession legitimately
	// returns via writerDoneCh — that exit path is NOT under test here.
	select {
	case err := <-done:
		t.Fatalf("servePeerSession exited on SendBufferFull, want frame drop: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	svc.peerMu.RLock()
	health := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if health == nil {
		t.Fatal("health entry should exist")
	}
	if !health.Connected {
		t.Fatal("peer must stay connected after a best-effort frame drop")
	}
	// A dropped frame never reached the writer queue, so it must NOT be
	// accounted as a useful send (markPeerWrite runs only on SendOK) —
	// otherwise backpressure would keep a stalled peer looking healthy
	// and protocol_trace would report accepted sends that never left.
	if !health.LastUsefulSendAt.IsZero() {
		t.Fatal("LastUsefulSendAt must not be updated by a dropped frame")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("servePeerSession did not exit on context cancellation")
	}
}

// TestFireAndForgetBufferFullDoesNotFeedDisconnectHistory verifies the
// quarantine-accounting side of the drop contract: a saturated write
// queue must leave no trace in the peer's disconnect_storm sliding
// window, because no disconnect happened at all.
func TestFireAndForgetBufferFullDoesNotFeedDisconnectHistory(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.53:64646")
	peerID := domaintest.ID("ff-drop-peer-identity")
	session := &peerSession{
		address:      peerAddr,
		peerIdentity: peerID,
		conn:         remote,
		sendCh:       make(chan protocol.Frame, 4),
		inboxCh:      make(chan protocol.Frame, 4),
		errCh:        make(chan error, 1),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}
	attachTestNetCore(svc, session)

	svc.peerMu.Lock()
	svc.sessions[peerAddr] = session
	svc.health[peerAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	filler := protocol.Frame{Type: "push_message", ID: "filler"}
	saturated := false
	for i := 0; i < 100000; i++ {
		if st := session.netCore.Send(filler); st == netcore.SendBufferFull {
			saturated = true
			break
		}
	}
	if !saturated {
		t.Fatal("could not saturate the netcore send queue")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.servePeerSession(ctx, session)
	}()

	session.sendCh <- protocol.Frame{Type: "push_message", ID: "dropped-1"}
	session.sendCh <- protocol.Frame{Type: "push_message", ID: "dropped-2"}

	select {
	case err := <-done:
		t.Fatalf("servePeerSession exited on SendBufferFull, want frame drops: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	svc.peerMu.RLock()
	histLen := len(svc.peerDisconnectHistory[peerID])
	svc.peerMu.RUnlock()
	if histLen != 0 {
		t.Fatalf("disconnect history has %d entries after frame drops, want 0", histLen)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("servePeerSession did not exit on context cancellation")
	}
}
