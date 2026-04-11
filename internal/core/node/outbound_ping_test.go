package node

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestDispatchPeerSessionFrame_PingRespondsPong verifies that an outbound peer
// session correctly responds with a pong frame when the remote side sends a
// ping. Without this, the remote inboundHeartbeat monitor closes the
// connection after pongStallTimeout because it never receives a pong.
func TestDispatchPeerSessionFrame_PingRespondsPong(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	// net.Pipe is synchronous — writes block until reads happen on the
	// other end. Start the reader goroutine before calling the handler.
	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.99:64646")

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		inboxCh: make(chan protocol.Frame, 8),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 8),
	}
	svc.mu.Lock()
	svc.sessions[peerAddr] = session
	svc.mu.Unlock()

	type readResult struct {
		frame protocol.Frame
		err   error
	}
	resultCh := make(chan readResult, 1)

	// Start reading from the remote end before writing so the pipe
	// does not block the handler goroutine.
	go func() {
		_ = remote.SetReadDeadline(time.Now().Add(3 * time.Second))
		scanner := bufio.NewScanner(remote)
		if !scanner.Scan() {
			resultCh <- readResult{err: scanner.Err()}
			return
		}
		f, err := protocol.ParseFrameLine(scanner.Text())
		resultCh <- readResult{frame: f, err: err}
	}()

	svc.dispatchPeerSessionFrame(peerAddr, session, protocol.Frame{Type: "ping"})

	res := <-resultCh
	if res.err != nil {
		t.Fatalf("reading pong from pipe: %v", res.err)
	}
	if res.frame.Type != "pong" {
		t.Fatalf("expected pong frame, got %q", res.frame.Type)
	}
}

// TestPeerSessionRequest_PingDuringWaitRespondsPong verifies that a ping
// arriving while the outbound session is waiting for a response to another
// command is answered with a pong instead of being silently dropped.
func TestPeerSessionRequest_PingDuringWaitRespondsPong(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.100:64646")

	session := &peerSession{
		address: peerAddr,
		conn:    local,
		inboxCh: make(chan protocol.Frame, 8),
		errCh:   make(chan error, 1),
		sendCh:  make(chan protocol.Frame, 8),
	}
	svc.mu.Lock()
	svc.sessions[peerAddr] = session
	svc.health[peerAddr] = &peerHealth{Connected: true}
	svc.mu.Unlock()

	// Read outbound frames from the pipe in a goroutine. The pipe is
	// synchronous, so this must run concurrently with the writes that
	// peerSessionRequest performs (get_peers request + pong reply).
	type readResult struct {
		frames []protocol.Frame
		err    error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		scanner := bufio.NewScanner(remote)
		var frames []protocol.Frame
		_ = remote.SetReadDeadline(time.Now().Add(3 * time.Second))
		for scanner.Scan() {
			f, err := protocol.ParseFrameLine(scanner.Text())
			if err != nil {
				resultCh <- readResult{err: err}
				return
			}
			frames = append(frames, f)
			if f.Type == "pong" {
				break
			}
		}
		resultCh <- readResult{frames: frames, err: scanner.Err()}
	}()

	// Inject frames into the inbox after a brief delay so
	// peerSessionRequest has time to start the read loop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		session.inboxCh <- protocol.Frame{Type: "ping"}
		time.Sleep(50 * time.Millisecond)
		session.inboxCh <- protocol.Frame{Type: "peers"}
	}()

	resp, err := svc.peerSessionRequest(session, protocol.Frame{Type: "get_peers"}, "peers", false)
	if err != nil {
		t.Fatalf("peerSessionRequest failed: %v", err)
	}
	if resp.Type != "peers" {
		t.Fatalf("expected peers response, got %q", resp.Type)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("reading from pipe: %v", result.err)
	}

	foundPong := false
	for _, f := range result.frames {
		if f.Type == "pong" {
			foundPong = true
		}
	}
	if !foundPong {
		t.Fatal("expected pong frame to be written in response to ping during request wait")
	}
}
