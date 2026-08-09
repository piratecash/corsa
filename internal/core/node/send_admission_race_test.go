package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// send_admission_race_test.go pins the ORDER in which a dying connection
// publishes its two facts, on BOTH send tiers.
//
// A producer is admitted by the peer-state gate and only then hands its frame
// to a queue; the two statements are not one instruction, and a teardown fits
// between them. The teardown publishes "this peer is gone" (markPeerDisconnected)
// and, separately, stops the queue from accepting (closeSendQueue on the
// outbound tier, the send gate on the accepted one). Published FIRST, the
// disconnect left a window in which a producer that had just passed the gate
// still deposited its frame into a queue the same teardown was about to drain —
// and the emitter, reading that acceptance as a delivery, STOPPED walking the
// peer's remaining connections. The frame reached neither this socket nor the
// next candidate.
//
// The teardown therefore runs on a goroutine of its OWN and is parked BETWEEN
// its two publications, and the producer is resumed exactly at that point
// (teardownRendezvous below). That is what makes the order an observable rather
// than an implementation detail: a teardown that ran to completion inside the
// producer's barrier would show the producer the same world whichever way round
// the two statements were written, so the test would pass on the broken
// implementation it was written to catch.
//
// What they deliberately do NOT assert is that an accepted frame is written. A
// session may die right after a successful enqueue and that is the ordinary
// asynchronous case — the receiving side's anti-replay covers the retry. The
// property under test is narrower: the emitter must not count as delivered a
// frame the teardown has already condemned.

// rendezvousTimeout bounds every wait of the rendezvous. It is generous because
// it is never reached on a healthy run: it exists so a seam that stopped being
// called fails the test with a diagnosis instead of hanging the package.
const rendezvousTimeout = 10 * time.Second

// teardownRendezvous interleaves ONE producer and ONE teardown so that the
// producer's hand-over to a queue happens while the teardown sits between its
// own two publications.
//
// The two seams are halves of one interleaving and neither replaces the other.
// The PRODUCER half (Service.sendAdmissionBarrier) puts the producer inside the
// admission window — past the peer-state gate, before the queue offer — which is
// the only state from which a disconnect published too early can still do
// damage; without it the producer would simply be refused by the gate and the
// broken order would look correct. The TEARDOWN half
// (Service.peerTeardownBarrier) freezes the teardown between the fence and the
// publication, which is the only point at which the two orders differ:
//
//   - fence → publish (correct): at the park the queue already refuses, the
//     disconnect is not out yet, and the resumed producer is turned away at the
//     door and walks on to the peer's next connection;
//   - publish → fence (broken): at the park the disconnect is out but the queue
//     still accepts, so the resumed producer — already past the gate — is given
//     an "accepted" for a frame this same teardown is about to discard, and the
//     emitter ends its walk on it.
type teardownRendezvous struct {
	// admitted closes when the producer has entered the admission window.
	admitted chan struct{}
	// parked closes when the teardown sits between its two publications.
	parked chan struct{}
	// released closes when the test is done observing the window.
	released chan struct{}
	// finished closes when the teardown goroutine has returned.
	finished chan struct{}
	// abort unblocks every wait when the test ends early, so a failed
	// assertion is reported as a failure and not as a hung package.
	abort chan struct{}

	admitOnce sync.Once
	parkOnce  sync.Once
}

// startTeardownRendezvous installs both halves on svc and starts teardown on its
// own goroutine, where it waits for the producer to reach the admission window.
//
// teardown is the production function under test, called exactly as production
// calls it — the point of the rendezvous is to place it in time, not to
// reimplement it.
func startTeardownRendezvous(t *testing.T, svc *Service, teardown func()) *teardownRendezvous {
	t.Helper()
	r := &teardownRendezvous{
		admitted: make(chan struct{}),
		parked:   make(chan struct{}),
		released: make(chan struct{}),
		finished: make(chan struct{}),
		abort:    make(chan struct{}),
	}

	svc.sendAdmissionBarrier = func() {
		r.admitOnce.Do(func() {
			close(r.admitted)
			r.wait(r.parked)
		})
	}
	svc.peerTeardownBarrier = func() {
		r.parkOnce.Do(func() {
			close(r.parked)
			r.wait(r.released)
		})
	}

	go func() {
		defer close(r.finished)
		select {
		case <-r.admitted:
		case <-r.abort:
			return
		}
		teardown()
	}()

	// Unblock and JOIN the teardown before the fixture's own cleanups run: a
	// failed assertion leaves it parked, and a teardown still running while the
	// sockets and cores behind it are being closed would answer a red test with
	// noise from a second failure.
	t.Cleanup(func() {
		close(r.abort)
		select {
		case <-r.finished:
		case <-time.After(rendezvousTimeout):
		}
	})
	return r
}

// wait blocks until ch closes, the test aborts, or the timeout expires. A
// timeout is not reported here: the caller of the barrier is production code
// with nowhere to report to, so the diagnosis is left to requireInterleaved,
// which runs on the test's own goroutine.
func (r *teardownRendezvous) wait(ch <-chan struct{}) {
	select {
	case <-ch:
	case <-r.abort:
	case <-time.After(rendezvousTimeout):
	}
}

// requireInterleaved fails unless the interleaving the test claims to exercise
// actually happened. Without it a seam that silently stopped being called would
// leave both tests passing on a code path they never entered.
func (r *teardownRendezvous) requireInterleaved(t *testing.T) {
	t.Helper()
	select {
	case <-r.admitted:
	default:
		t.Fatal("the producer never entered the admission window: sendAdmissionBarrier was not " +
			"called between the peer-state gate and the queue offer, so nothing was interleaved")
	}
	select {
	case <-r.parked:
	default:
		t.Fatal("the teardown never parked between its two publications: peerTeardownBarrier was " +
			"not called there, so the order of the two was never observed")
	}
}

// release lets the parked teardown run its second publication and waits for it.
func (r *teardownRendezvous) release(t *testing.T) {
	t.Helper()
	close(r.released)
	select {
	case <-r.finished:
	case <-time.After(rendezvousTimeout):
		t.Fatalf("the teardown did not finish within %s after being released", rendezvousTimeout)
	}
}

// TestEmitToWalksOnWhenTheSelectedSessionIsRetiredInsideTheAdmissionWindow is
// the outbound tier: the queue is peerSession.sendCh and the fence is
// closeSendQueue.
func TestEmitToWalksOnWhenTheSelectedSessionIsRetiredInsideTheAdmissionWindow(t *testing.T) {
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
	// Read the attempt order from the selection itself: the premise is "the
	// FIRST candidate dies", and only the selection can say which that is.
	svc.peerMu.RLock()
	targets := svc.datagramFrameSendTargetsLocked(frame, peer, now)
	svc.peerMu.RUnlock()
	if len(targets) != 2 || targets[0].session == nil || targets[1].session == nil {
		t.Fatalf("the fixture must offer two outbound candidates, got %d", len(targets))
	}
	selected, next := targets[0].session, targets[1].session

	// servePeerSession's exit, reproduced where production runs it: on the
	// serve loop's own goroutine, concurrently with the producer, and frozen
	// between the fence and the disconnect.
	rendezvous := startTeardownRendezvous(t, svc, func() {
		svc.retirePeerSession(selected, errors.New("peer closed the socket"))
	})

	if !datagramEmitterOf(svc).EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(mustDatagramLine(t, frame)),
		Class:     frame.Class,
		SendUntil: now.Add(5 * time.Second),
	}) {
		t.Fatal("the peer still had a live connection, so the walk had somewhere to go")
	}
	rendezvous.requireInterleaved(t)

	// Asserted while the teardown is still parked, because that is where the
	// two orders differ: releasing first would let closeSendQueue drain the
	// evidence the broken order leaves behind.
	if got := next.sendQueueLen(); got != 1 {
		t.Fatalf("the next candidate holds %d frames, want 1: the retired session accepted the "+
			"frame while its disconnect was already published, and the emitter read that "+
			"acceptance as a delivery and stopped the walk", got)
	}
	if got := selected.sendQueueLen(); got != 0 {
		t.Fatalf("the retired session took %d frames: its queue is drained without sending, "+
			"so the frame is lost", got)
	}

	rendezvous.release(t)
}

// admissionInboundConn is one accepted connection of the fixture peer, kept
// together with BOTH ends of its socket. The accepted tier has no upper queue
// to count, so what a queue took is observable only on the wire — and net.Pipe
// hands a write straight to its reader.
type admissionInboundConn struct {
	id      domain.ConnID
	address domain.PeerAddress
	client  net.Conn
}

// installAdmissionInboundPeer wires count accepted connections of one peer
// exactly where the send path looks for them: a NetCore in the conn registry, a
// connected health row per address, and the inbound health reference
// trackInboundConnect would have taken — without it the teardown never reaches
// markPeerDisconnected and the test would prove nothing.
//
// The addresses differ per connection on purpose: health is keyed by address,
// so two connections sharing one would go down together and there would be no
// surviving candidate for the walk to reach.
func installAdmissionInboundPeer(
	t *testing.T,
	svc *Service,
	peer domain.PeerIdentity,
	count int,
) []admissionInboundConn {
	t.Helper()
	now := time.Now().UTC()
	conns := make([]admissionInboundConn, 0, count)
	for i := range count {
		declarations := datagramPeerDeclarations()
		id := domain.ConnID(9300 + i)
		address := domain.PeerAddress(fmt.Sprintf("10.9.7.%d:64646", i+1))
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })
		t.Cleanup(func() { _ = server.Close() })
		core := netcore.New(id, server, netcore.Inbound, netcore.Options{
			Address:         address,
			Identity:        peer,
			Caps:            []domain.Capability{domain.CapMeshDatagramV1, domain.CapMeshDatagramTransitV1},
			ProtocolVersion: 27,
			Declarations:    &declarations,
		})
		t.Cleanup(core.Close)

		svc.peerMu.Lock()
		svc.setTestConnEntryLocked(client, &connEntry{core: core, tracked: true})
		svc.health[address] = &peerHealth{
			Connected: true,
			// Oldest first is the attempt order, so index 0 is the head the
			// test retires.
			LastConnectedAt:     now.Add(-time.Duration(count-i) * time.Hour),
			LastUsefulReceiveAt: now,
		}
		svc.inboundHealthRefs[address] = 1
		svc.peerMu.Unlock()

		conns = append(conns, admissionInboundConn{id: id, address: address, client: client})
	}
	return conns
}

// readWireLine returns the next newline-terminated frame the connection wrote,
// or fails the test when nothing arrives before the deadline.
func readWireLine(t *testing.T, conn net.Conn, within time.Duration) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("no frame reached the socket within %s: %v", within, err)
	}
	return line
}

// TestEmitToWalksOnWhenTheSelectedConnIsRetiredInsideTheAdmissionWindow is the
// accepted tier: the queue is NetCore.sendCh and the fence is its send gate.
//
// The teardown driven here is the production one, trackInboundDisconnect, which
// is the function that publishes an accepted peer as gone.
func TestEmitToWalksOnWhenTheSelectedConnIsRetiredInsideTheAdmissionWindow(t *testing.T) {
	t.Parallel()

	peer := domain.PeerIdentityFromWire(datagramTestDstHex)
	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	conns := installAdmissionInboundPeer(t, svc, peer, 2)

	frame := newNodeDatagram(t, nil)
	now := time.Now().UTC()
	svc.peerMu.RLock()
	targets := svc.datagramFrameSendTargetsLocked(frame, peer, now)
	svc.peerMu.RUnlock()
	if len(targets) != 2 || targets[0].session != nil || targets[1].session != nil {
		t.Fatalf("the fixture must offer two accepted candidates, got %d", len(targets))
	}
	if targets[0].connID != conns[0].id || targets[1].connID != conns[1].id {
		t.Fatalf("attempt order is %d then %d, want %d then %d: the test no longer retires the head",
			targets[0].connID, targets[1].connID, conns[0].id, conns[1].id)
	}

	rendezvous := startTeardownRendezvous(t, svc, func() {
		svc.trackInboundDisconnect(conns[0].id, conns[0].address)
	})

	line := mustDatagramLine(t, frame)
	if !datagramEmitterOf(svc).EmitTo(context.Background(), datagram.OutboundFrame{
		Peer:      peer,
		Frame:     frame,
		Line:      []byte(line),
		Class:     frame.Class,
		SendUntil: now.Add(5 * time.Second),
	}) {
		t.Fatal("the peer still had a live connection, so the walk had somewhere to go")
	}
	rendezvous.requireInterleaved(t)

	// The accepted tier keeps no upper queue, so the surviving candidate is
	// read on the wire. Asserted while the teardown is still parked, for the
	// same reason as on the outbound tier.
	if got := readWireLine(t, conns[1].client, 2*time.Second); got != line {
		t.Fatalf("the next candidate wrote %q, want the emitted datagram: the retired connection "+
			"accepted the frame while its disconnect was already published, and the emitter read "+
			"that acceptance as a delivery and stopped the walk", got)
	}

	rendezvous.release(t)
}
