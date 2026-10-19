package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// The real writer has sent auth_session, but its flush completion is withheld.
// The reader can receive the expected auth_ok while the handshake owner cannot
// apply it yet: a deterministic version of the production scheduling window.
type pausedAuthFlushConn struct {
	net.Conn
	release       <-chan struct{}
	authReplyRead chan struct{}
	replyOnce     sync.Once
}

func (c *pausedAuthFlushConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil && bytes.Contains(p, []byte(`"type":"auth_session"`)) {
		<-c.release
	}
	return n, err
}

func (c *pausedAuthFlushConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if bytes.Contains(p[:n], []byte(`"type":"auth_ok"`)) {
		c.replyOnce.Do(func() { close(c.authReplyRead) })
	}
	return n, err
}

func TestLegacyAuthReplyPipelinedDatagramWaitsForValidatedReadiness(t *testing.T) {
	synctest.Test(t, legacyAuthReplyPipelinedDatagram)
}

func legacyAuthReplyPipelinedDatagram(t *testing.T) {
	svc := newDatagramLayerService(t, true)
	registerFixtureDatagramTypes(t, svc)
	legacySelfRecordFixture(t)(svc)
	ctx, cancel := context.WithCancel(context.Background())
	svc.runCtx = ctx
	local, remote := net.Pipe()
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpause := func() { releaseOnce.Do(func() { close(release) }) }
	wire := &pausedAuthFlushConn{Conn: local, release: release, authReplyRead: make(chan struct{})}
	svc.peerTCPDial = func(context.Context, string, time.Duration) (net.Conn, error) { return wire, nil }
	t.Cleanup(func() {
		unpause()
		cancel()
		_ = remote.Close()
		_ = local.Close()
		svc.runLoopsWg.Wait()
	})

	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	_, welcome := oldNodeWelcomes(t, peer, config.ProtocolVersion)
	welcome.Capabilities = []string{domain.CapMeshDatagramV1.String()}
	types := []string{fixtureDatagramTypes()[0].String()}
	welcome.DTypes = &types
	datagram := mustDatagramLine(t, newNodeDatagram(t, nil))
	authOK, err := protocol.MarshalFrameLine(protocol.Frame{Type: "auth_ok"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := authOK + datagram
	if len(pipeline) >= 4096 {
		t.Fatal("the deterministic pipeline must fit the reader's first buffer")
	}
	written := make(chan struct{})
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		reader := bufio.NewReader(remote)
		if _, err := reader.ReadString('\n'); err != nil || !writeFrameLine(remote, welcome) {
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		if _, err := io.WriteString(remote, pipeline); err != nil {
			return
		}
		close(written)
		_, _ = io.Copy(io.Discard, reader)
	}()
	t.Cleanup(func() { _ = remote.Close(); <-peerDone })

	type established struct {
		session *peerSession
		err     error
	}
	result := make(chan established, 1)
	go func() {
		session, err := svc.openPeerSessionForCM(ctx, "198.51.100.7:64646")
		result <- established{session: session, err: err}
	}()
	awaitClosed(t, wire.authReplyRead, "auth_ok to be read from the wire")
	awaitClosed(t, written, "the peer to pipeline its datagram behind auth_ok")
	synctest.Wait()
	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("unauthenticated waiter admitted %d datagrams before validation", got)
	}
	unpause()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("handshake failed: %v", got.err)
		}
		t.Cleanup(func() { _ = got.session.Close() })
	case <-time.After(2 * time.Second):
		t.Fatal("handshake owner did not finish after its flush was released")
	}
	waitForConditionMsg(t, time.Second, "the valid pipelined datagram was lost while the auth waiter was pending", func() bool {
		return datagramObservedCount(svc) == 1
	})
}

func TestSessionReadinessCommittedReadyWinsLaterDialCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			ctx, cancel := context.WithCancel(context.Background())
			svc := &Service{}
			session := &peerSession{readiness: newPeerSessionReadiness(ctx)}
			finished := make(chan error, 1)
			go func() { finished <- session.readiness.wait() }()
			synctest.Wait()
			if err := svc.markSessionHandshakeComplete(session); err != nil {
				t.Fatal(err)
			}
			cancel()
			if err := <-finished; err != nil {
				t.Fatalf("ready lost to subsequent dial cancellation: %v", err)
			}
			if err := session.readiness.wait(); err != nil {
				t.Fatalf("both ready/cancel set: committed ready must win: %v", err)
			}
			if err := session.readiness.abort(context.Canceled); err != nil {
				t.Fatalf("dial watcher closed a committed session: %v", err)
			}
			_ = session.Close()
			if err := session.readiness.wait(); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("real session close must still stop readiness: %v", err)
			}
		}
	})
}

func TestSessionReadinessAbortBeforeReadyIsTerminal(t *testing.T) {
	for _, kind := range []string{"cancel", "close", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				svc := &Service{}
				session := &peerSession{readiness: newPeerSessionReadiness(ctx)}
				finished := make(chan error, 1)
				go func() { finished <- session.readiness.wait() }()
				synctest.Wait()
				want := error(context.DeadlineExceeded)
				switch kind {
				case "cancel":
					cancel()
					want = context.Canceled
				case "close":
					_ = session.Close()
					want = net.ErrClosed
				}
				if err := <-finished; !errors.Is(err, want) {
					t.Fatalf("readiness abort=%v, want %v", err, want)
				}
				if err := svc.markSessionHandshakeComplete(session); !errors.Is(err, errDialAbortedLocally) || !errors.Is(err, want) {
					t.Fatalf("late handshake resurrected an aborted session: %v", err)
				}
				if session.authOK {
					t.Fatal("aborted readiness published authentication")
				}
			})
		})
	}
}

func newReadinessReaderFixture(t *testing.T, ctx context.Context) (*Service, net.Conn, *peerSession, <-chan struct{}) {
	t.Helper()
	svc := newDatagramLayerService(t, true)
	svc.runCtx = ctx
	registerFixtureDatagramTypes(t, svc)
	local, remote := net.Pipe()
	session := &peerSession{
		readiness: newPeerSessionReadiness(ctx), address: "198.51.100.7:64646",
		peerIdentity: domain.PeerIdentityFromWire(datagramTestDstHex), connID: 9301,
		conn: local, capabilities: []domain.Capability{domain.CapMeshDatagramV1},
		sendCh: make(chan peerSendItem, 4), inboxCh: make(chan protocol.Frame, 8), errCh: make(chan error, 1),
	}
	if err := svc.attachOutboundNetCore(session); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() { defer close(finished); svc.readPeerSession(bufio.NewReader(local), session) }()
	t.Cleanup(func() { _ = session.Close(); _ = remote.Close(); <-finished })
	return svc, remote, session, finished
}

func TestExpectedAuthReadinessWaitReleasesReaderOnCancelCloseAndTimeout(t *testing.T) {
	for _, kind := range []string{"cancel", "close", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				_, remote, session, finished := newReadinessReaderFixture(t, ctx)
				release := session.admission.expectReply("auth_ok")
				defer release()
				writeJSONFrame(t, remote, protocol.Frame{Type: "auth_ok"})
				if reply := <-session.inboxCh; reply.Type != "auth_ok" {
					t.Fatalf("reply=%s", reply.Type)
				}
				synctest.Wait()
				switch kind {
				case "cancel":
					cancel()
				case "close":
					_ = session.Close()
				}
				<-finished
				select {
				case err := <-session.errCh:
					if !errors.Is(err, errDialAbortedLocally) {
						t.Fatalf("readiness reader exited without meaningful local outcome: %v", err)
					}
				default:
					t.Fatal("readiness reader exited without reporting an error")
				}
			})
		})
	}
}

func TestUnsolicitedAuthOKAndAuthenticationErrorDoNotHoldOrAdmitData(t *testing.T) {
	for _, replyType := range []string{"auth_ok", "error"} {
		t.Run(replyType, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				svc, remote, session, _ := newReadinessReaderFixture(t, context.Background())
				if replyType == "error" {
					release := session.admission.expectReply("auth_ok")
					defer release()
				}
				writeJSONFrame(t, remote, protocol.Frame{Type: replyType})
				if reply := <-session.inboxCh; reply.Type != replyType {
					t.Fatalf("reply=%s", reply.Type)
				}
				line := mustDatagramLine(t, newNodeDatagram(t, nil))
				writeWireLine(t, remote, line)
				writeJSONFrame(t, remote, protocol.Frame{Type: "pong"})
				select {
				case reply := <-session.inboxCh:
					if reply.Type != "pong" {
						t.Fatalf("after unrequested/failed auth got %s", reply.Type)
					}
				case <-time.After(time.Second):
					t.Fatal("unrequested/failed auth response incorrectly blocked the reader")
				}
				if got := datagramObservedCount(svc); got != 0 {
					t.Fatalf("unrequested/failed auth admitted %d datagrams", got)
				}
			})
		})
	}
}

func TestProvenSessionReaderAcceptsInitialRecordAfterDialCancellation(t *testing.T) {
	listener, stopListener := startEmptyRegistryDatagramNode(t, freeAddress(t))
	defer stopListener()
	dialer, stopDialer := startDatagramNodeWithRegistry(t, freeAddress(t), withoutOwnDials(nil))
	defer stopDialer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := dialer.openPeerSessionForCM(ctx, domain.PeerAddress(normalizeAddress(listener.ListenAddress())))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	if session.proven == nil {
		t.Fatal("fixture did not establish a proven v2 session")
	}
	cancel()
	listenerID := domain.PeerIdentityFromWire(listener.Address())
	waitForConditionMsg(t, 2*time.Second, "proven reader lost the initial identity record or dial cancellation killed its committed readiness", func() bool {
		_, _, exists := dialer.trust.recordFor(testRecordStoreNetwork, listenerID)
		return exists
	})
	if _, err := dialer.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong", false); err != nil {
		t.Fatalf("successful dial cancellation closed the handed-off v2 session: %v", err)
	}
}

func TestSessionReadinessConcurrentCommitAndCancelHaveOneOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			ctx, cancel := context.WithCancel(context.Background())
			svc := &Service{}
			session := &peerSession{readiness: newPeerSessionReadiness(ctx)}
			start := make(chan struct{})
			committed := make(chan error, 1)
			cancelled := make(chan struct{})
			go func() { <-start; committed <- svc.markSessionHandshakeComplete(session) }()
			go func() { <-start; cancel(); close(cancelled) }()
			close(start)
			err := <-committed
			<-cancelled
			waitErr := session.readiness.wait()
			if err == nil {
				if waitErr != nil || !session.authOK {
					t.Fatalf("committed auth was lost to cancellation: ready=%v wait=%v", session.authOK, waitErr)
				}
			} else if !errors.Is(err, context.Canceled) || !errors.Is(waitErr, context.Canceled) || session.authOK {
				t.Fatalf("cancelled auth was resurrected: ready=%v commit=%v wait=%v", session.authOK, err, waitErr)
			}
			_ = session.Close()
		}
	})
}

func TestReadinessBelongsToDeliveringSessionAcrossReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		svc, remote, delivering, _ := newReadinessReaderFixture(t, context.Background())
		release := delivering.admission.expectReply("auth_ok")
		defer release()
		writeJSONFrame(t, remote, protocol.Frame{Type: "auth_ok"})
		<-delivering.inboxCh
		replacement := registerReplacementSession(t, svc, delivering.address, delivering.peerIdentity, domain.CapMeshRelayV1)
		if err := svc.markSessionHandshakeComplete(replacement); err != nil {
			t.Fatal(err)
		}
		written := make(chan error, 1)
		line := mustDatagramLine(t, newNodeDatagram(t, nil))
		go func() { _, err := io.WriteString(remote, line); written <- err }()
		synctest.Wait()
		select {
		case err := <-written:
			t.Fatalf("replacement session released the delivering session's reader: %v", err)
		default:
		}
		if got := datagramObservedCount(svc); got != 0 {
			t.Fatalf("replacement readiness admitted %d early datagrams", got)
		}
		if err := svc.markSessionHandshakeComplete(delivering); err != nil {
			t.Fatal(err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if got := datagramObservedCount(svc); got != 1 {
			t.Fatalf("delivering session's negotiated datagram was lost to replacement capabilities: %d", got)
		}
	})
}
