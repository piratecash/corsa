package node

import (
	"bufio"
	"bytes"
	"context"
	"github.com/piratecash/corsa/internal/core/protocol"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/config/configtest"
	"github.com/piratecash/corsa/internal/core/identity"
)

// The SDK's normal-origin latch depends on Run joining the actual managed CM
// and bootstrap owners. This uses the private per-instance TCP seam below CM,
// not a replacement Network, fake final counters, or fabricated clean status.
func TestFinalTransportManagedRunWaitsForActualBootstrapCMDial(t *testing.T) {
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", BootstrapPeers: []string{"198.51.100.7:64646"}, AllowPrivatePeers: true})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(cfg, id, nil)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	svc.peerTCPDial = func(ctx context.Context, _ string, _ time.Duration) (net.Conn, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	running := runServiceForTest(t.Context(), svc)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = running.Stop(ctx)
		svc.WaitBackground()
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual CM bootstrap dial never reached network seam")
	}
	running.cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("managed worker did not receive Run cancellation")
	}
	// Cancellation reached the real worker, which is still inside its network
	// call. Run cannot have returned and SDK Close cannot yet claim quiescence.
	select {
	case <-running.exited:
		t.Fatal("Run returned before actual managed dial joined")
	default:
	}
	unblock()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := running.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := running.DrainBackground(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFinalTransportBackgroundJoinWaitsForActualOneShotDial(t *testing.T) {
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0"})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(cfg, id, nil)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	svc.peerTCPDial = func(ctx context.Context, _ string, _ time.Duration) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	running := runServiceForTest(t.Context(), svc)
	ready, cancelReady := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelReady()
	if err := running.AwaitReady(ready); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = running.Stop(ctx)
		svc.WaitBackground()
	})
	if !svc.goBackground(func() { svc.syncPeer(svc.runCtx, "198.51.100.8:64646", false) }) {
		t.Fatal("one-shot not admitted")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("one-shot bypassed actual dial seam")
	}
	running.cancel()
	<-canceled
	done := make(chan struct{})
	go func() { <-running.exited; svc.WaitBackground(); close(done) }()
	select {
	case <-done:
		t.Fatal("background drain returned before actual one-shot network call")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("actual one-shot failed to join")
	}
}

// These barriers retain actual NetCore I/O calls after the underlying pipe has
// closed/written. Closing a socket is not a join of its reader or writer.
type finalTransportHeldWire struct {
	net.Conn
	readHeld, writeHeld       chan struct{}
	releaseRead, releaseWrite <-chan struct{}
	readOnce, writeOnce       sync.Once
}

func (c *finalTransportHeldWire) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil || bytes.Contains(p[:n], []byte("final-read-tail")) {
		c.readOnce.Do(func() { close(c.readHeld) })
		<-c.releaseRead
	}
	return n, err
}
func (c *finalTransportHeldWire) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if bytes.Contains(p, []byte("final-tail-barrier")) {
		c.writeOnce.Do(func() { close(c.writeHeld) })
		<-c.releaseWrite
	}
	return n, err
}
func TestFinalTransportManagedRunJoinsActualNetCoreReaderAndWriter(t *testing.T) {
	cfg := configtest.WithTempDataDirs(t, config.Node{ListenAddress: "127.0.0.1:0", BootstrapPeers: []string{"198.51.100.9:64646"}})
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(cfg, id, nil)
	legacySelfRecordFixture(t)(svc)
	local, remote := net.Pipe()
	releaseRead, releaseWrite := make(chan struct{}), make(chan struct{})
	wire := &finalTransportHeldWire{Conn: local, readHeld: make(chan struct{}), writeHeld: make(chan struct{}), releaseRead: releaseRead, releaseWrite: releaseWrite}
	var readOnce, writeOnce sync.Once
	unblockRead := func() { readOnce.Do(func() { close(releaseRead) }) }
	unblockWrite := func() { writeOnce.Do(func() { close(releaseWrite) }) }
	svc.peerTCPDial = func(context.Context, string, time.Duration) (net.Conn, error) { return wire, nil }
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	_, welcome := oldNodeWelcomes(t, peer, config.ProtocolVersion)
	remoteDone := make(chan struct{})
	go func() {
		defer close(remoteDone)
		reader := bufio.NewReader(remote)
		if _, err := reader.ReadString('\n'); err != nil || !writeFrameLine(remote, welcome) {
			return
		}
		if _, err := reader.ReadString('\n'); err != nil || !writeFrameLine(remote, protocol.Frame{Type: "auth_ok"}) {
			return
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			frame, err := protocol.ParseFrameLine(line)
			if err != nil {
				return
			}
			if bytes.Contains([]byte(line), []byte("final-tail-barrier")) {
				_, _ = io.WriteString(remote, "{\"type\":\"pong\",\"message\":\"final-read-tail\"}\n")
				continue
			}
			reply := protocol.Frame{}
			switch frame.Type {
			case "get_peers":
				reply.Type = "peers"
			case "fetch_contacts":
				reply.Type = "contacts"
			default:
				continue
			}
			if !writeFrameLine(remote, reply) {
				return
			}
		}
	}()
	running := runServiceForTest(t.Context(), svc)
	t.Cleanup(func() {
		unblockRead()
		unblockWrite()
		_ = remote.Close()
		_ = local.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = running.Stop(ctx)
		svc.WaitBackground()
		<-remoteDone
	})
	var session *peerSession
	waitForConditionMsg(t, 3*time.Second, "actual managed session was never published", func() bool {
		svc.peerMu.RLock()
		defer svc.peerMu.RUnlock()
		for _, s := range svc.sessions {
			session = s
			return true
		}
		return false
	})
	if session.netCore == nil {
		t.Fatal("actual managed NetCore absent")
	}
	svc.enqueueSessionFrame(session, []byte("{\"type\":\"ping\",\"message\":\"final-tail-barrier\"}\n"))
	select {
	case <-wire.writeHeld:
	case <-time.After(time.Second):
		t.Fatal("actual NetCore writer never reached barrier")
	}
	select {
	case <-wire.readHeld:
	case <-time.After(time.Second):
		t.Fatal("actual read tail never reached barrier")
	}
	before := svc.TransportTrafficStats()
	running.cancel()
	select {
	case <-wire.readHeld:
	case <-time.After(time.Second):
		t.Fatal("reader wasn't closed by managed shutdown")
	}
	select {
	case <-running.exited:
		t.Fatal("Run returned with actual reader+writer held")
	default:
	}
	unblockWrite()
	select {
	case <-session.netCore.WriterExited():
	case <-time.After(time.Second):
		t.Fatal("NetCore writer did not join after release")
	}
	select {
	case <-running.exited:
		t.Fatal("Run returned with actual reader held")
	default:
	}
	unblockRead()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := running.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	after := svc.TransportTrafficStats()
	if after.BytesReceived <= before.BytesReceived {
		t.Fatal("post-join counters omitted actual shutdown read tail", before, after)
	}

}
