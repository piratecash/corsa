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

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_unproven_failure_test.go pins what a dial that ended before the
// peer was authenticated costs the address it dialled
// (docs/protocol/session_v2.md, "A failure before authentication is not a
// failure of the address either"): a probe answer
// that admits nothing — broken, forged, cut off, or none at all — and a TLS
// handshake that failed before the identity proof verified show no fault of
// the node there, since nothing proved who answered. Such a dial is not a
// failure of the address (no failure counted, no score lost, no ban) and it
// creates no pin, binding or v2_strong; it only paces the next dial to that
// address — one step of its v2 retry backoff per attempt — whatever the
// TLS-first switch says.

// v2RetrySteps is how many steps the v2 retry backoff of address has taken.
func v2RetrySteps(marks *sessionAddressMarks, address domain.PeerAddress) int {
	marks.mu.Lock()
	defer marks.mu.Unlock()
	if retry := marks.marks[address].v2Retry; retry != nil {
		return retry.unanswered
	}
	return 0
}

// requireUnprovenFailureCost checks what an unauthenticated failed dial left:
// handed to what the connection manager calls with it, it is no failure of the
// address, and the address's v2 retry backoff took exactly one step.
func requireUnprovenFailureCost(t *testing.T, svc *Service, address domain.PeerAddress, dialErr error) {
	t.Helper()
	if dialErr == nil {
		t.Fatal("the dial succeeded")
	}
	if !isUnchargedDialRefusal(dialErr) {
		t.Fatalf("an unauthenticated failure is charged to the address: %v", dialErr)
	}
	svc.onCMDialFailed(address, dialErr, false)
	svc.peerMu.RLock()
	health := svc.health[address]
	svc.peerMu.RUnlock()
	if health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || !health.BannedUntil.IsZero()) {
		t.Fatalf("the address was charged: failures %d, score %d, banned until %v", health.ConsecutiveFailures, health.Score, health.BannedUntil)
	}
	if steps := v2RetrySteps(svc.secureSessions.marks, address); steps != 1 {
		t.Fatalf("the v2 retry backoff took %d steps, want exactly 1", steps)
	}
	requireNothingProvenAt(t, svc, address)
}

// A failed TLS handshake only paces the next dial.
func TestATLSHandshakeBrokenBeforeTheProofOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	peer := newScriptedPeer(answerClientHello(tlsHandshakeGarbage))
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	sockets := peer.sockets(t)
	if len(sockets) != 1 || !isClientHello(sockets[0]) {
		t.Fatalf("sockets = %q, want the dial to end on its TLS attempt", sockets)
	}
	requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
}

// requireNoCost checks a failed dial that must leave nothing at the address:
// not charged, no step of its v2 retry backoff, nothing proven.
func requireNoCost(t *testing.T, svc *Service, address domain.PeerAddress, dialErr error) {
	t.Helper()
	if dialErr == nil {
		t.Fatal("the dial succeeded")
	}
	if !isUnchargedDialRefusal(dialErr) {
		t.Fatalf("the failure is charged to the address: %v", dialErr)
	}
	svc.onCMDialFailed(address, dialErr, false)
	svc.peerMu.RLock()
	health := svc.health[address]
	svc.peerMu.RUnlock()
	if health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || !health.BannedUntil.IsZero()) {
		t.Fatalf("the address was charged: failures %d, score %d", health.ConsecutiveFailures, health.Score)
	}
	if steps := v2RetrySteps(svc.secureSessions.marks, address); steps != 0 {
		t.Fatalf("the v2 retry backoff took %d steps, want none", steps)
	}
	requireNothingProvenAt(t, svc, address)
}

// tlsUnansweredEnds are the ends of a TLS attempt, over a TCP connection that
// was opened, that answered nothing a proof could come from: silence until
// the deadline, a reset, bytes that are neither TLS nor v1, and a TLS alert
// that is not the listener's refusal.
func tlsUnansweredEnds() map[string]socketScript {
	return map[string]socketScript{
		"silence":              silentPeer,
		"a reset":              resetAfterFirstBytes,
		"other bytes":          answerClientHello([]byte("\x00\x01garbage")),
		"an alert, no refusal": answerClientHello([]byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28}),
	}
}

// A TLS attempt that ends before proof adds one step of retry backoff
// without charging the address or opening another socket.
func TestATLSAttemptEndedBeforeAProofWhereV1IsRuledOutOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	for name, end := range tlsUnansweredEnds() {
		t.Run(name+"/a dial to X", func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(end)
			_, err := dialThrough(t, svc, dialChoiceAddress, dialToIdentity(t, domain.PeerIdentity{9}), peer)
			sockets := peer.sockets(t)
			if len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("sockets = %q, want the TLS attempt alone", sockets)
			}
			requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
		})
		t.Run(name+"/a dial to an address", func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(end)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			sockets := peer.sockets(t)
			if len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("sockets = %q, want one TLS attempt", sockets)
			}
			requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
		})
	}
}

// A TLS handshake that completed but whose identity proof does not verify —
// here a proof made for another network — ends the attempt: no probe, no v1,
// not charged, one step.
func TestACompletedTLSHandshakeWithAProofThatDoesNotVerifyOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	stranger := generateProbeIdentity(t)
	local, err := sessionv2.NewLocal(stranger, domain.NetworkID("another-network"))
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		t.Fatalf("certificates: %v", err)
	}
	welcome := oldReleaseWelcome(t, stranger, config.ProtocolVersionSecureSession)
	peer := newScriptedPeer(func(conn net.Conn, _ *bufio.Reader) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// The session is the dialler's to refuse; what the listener side
		// makes of it is not under test.
		_, _ = sessionv2.Accept(ctx, conn, local, welcome, certificates, sessionv2.DefaultTimeouts)
	})
	_, dialErr := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	sockets := peer.sockets(t)
	if len(sockets) != 1 || !isClientHello(sockets[0]) {
		t.Fatalf("sockets = %q, want the TLS attempt alone: no probe after a TLS answer", sockets)
	}
	if !errors.Is(dialErr, errV2FailedBeforeProof) {
		t.Fatalf("err = %v, want errV2FailedBeforeProof", dialErr)
	}
	requireUnprovenFailureCost(t, svc, dialChoiceAddress, dialErr)
}

// lockedClock is a clock a scripted peer may move while the node reads it.
type lockedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *lockedClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *lockedClock) advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(by)
}

// One dial takes one retry step even when the attempt takes longer than
// the full backoff window.
func TestOneDialStepsTheV2RetryBackoffOnceEvenWhenItOutlastsTheWait(t *testing.T) {
	t.Parallel()
	clock := &lockedClock{now: time.Unix(1780000000, 0)}
	svc := newDialChoiceService(t)
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	slowTLS := func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.Peek(1); err != nil {
			return
		}
		clock.advance(v2RetryMax + time.Minute)
		answerClientHello(tlsHandshakeGarbage)(conn, reader)
	}
	peer := newScriptedPeer(slowTLS)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
		t.Fatal("the dial succeeded")
	}
	if steps := v2RetrySteps(svc.secureSessions.marks, dialChoiceAddress); steps != 1 {
		t.Fatalf("one dial took %d steps of the v2 retry backoff, want 1", steps)
	}
}

// A dial this node cancels — its own stop, or the caller giving up — is no
// failure of the address and paces nothing.
func TestADialCancelledByThisNodeCostsNothing(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	slow := testDialTimeouts()
	slow.direct.TLS = 10 * time.Second
	svc.secureSessions.dialTimeouts = slow
	peer := newScriptedPeer(silentPeer)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err := svc.dialPeerTransport(ctx, dialChoiceAddress, domain.DialToAddress(), peer.dialer())
	cancel()
	peer.sockets(t)
	requireNoCost(t, svc, dialChoiceAddress, err)
}

// A TCP connection that cannot be opened is an ordinary failed dial of the
// address — a reachability failure, nothing more: no ban, no step of the v2
// retry backoff, no pin.
func TestAnUnopenedTCPConnectionIsAnOrdinaryFailedDial(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	refused := func(context.Context) (net.Conn, error) {
		return nil, fmt.Errorf("%w: connection refused", errPeerDialTransport)
	}
	_, err := svc.dialPeerTransport(context.Background(), dialChoiceAddress, domain.DialToAddress(), refused)
	if err == nil || isUnchargedDialRefusal(err) {
		t.Fatalf("err = %v, want a charged reachability failure", err)
	}
	svc.onCMDialFailed(dialChoiceAddress, err, false)
	svc.peerMu.RLock()
	health := svc.health[dialChoiceAddress]
	svc.peerMu.RUnlock()
	if health == nil || health.ConsecutiveFailures != 1 || !health.BannedUntil.IsZero() {
		t.Fatalf("health = %+v, want one failure and no ban", health)
	}
	if steps := v2RetrySteps(svc.secureSessions.marks, dialChoiceAddress); steps != 0 {
		t.Fatalf("the v2 retry backoff took %d steps for an unopened connection, want none", steps)
	}
	requireNothingProvenAt(t, svc, dialChoiceAddress)
}
