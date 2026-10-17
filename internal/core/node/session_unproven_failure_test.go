package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
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

// probeAnsweredWith plays an endpoint that answers the probe with answer and
// closes; an answer without its newline is one cut off on the way.
func probeAnsweredWith(answer []byte) socketScript {
	return func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		if len(answer) > 0 {
			if _, err := conn.Write(answer); err != nil {
				return
			}
		}
	}
}

// frameLine is frame as one wire line.
func frameLine(t *testing.T, frame protocol.Frame) []byte {
	t.Helper()
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return []byte(line)
}

// unprovenProbeAnswers are probe answers that admit nothing, by what was
// wrong with them.
func unprovenProbeAnswers(t *testing.T) map[string][]byte {
	t.Helper()
	answers := unprovenProbeAnswerLines(t)
	answers["a line over the frame limit"] = bytes.Repeat([]byte("a"), protocol.MaxFrameLine+1)
	return answers
}

// unprovenProbeEnds are the far ends of a probe that admits nothing: every
// answer of unprovenProbeAnswers, and a connection reset while the probe was
// being written.
func unprovenProbeEnds(t *testing.T) map[string]socketScript {
	t.Helper()
	ends := map[string]socketScript{
		// Reading nothing, the far end leaves the probe's write pending and
		// then closes: the write is reset under it.
		"the connection reset under the probe's write": func(net.Conn, *bufio.Reader) { time.Sleep(50 * time.Millisecond) },
	}
	for name, answer := range unprovenProbeAnswers(t) {
		ends[name] = probeAnsweredWith(answer)
	}
	return ends
}

func unprovenProbeAnswerLines(t *testing.T) map[string][]byte {
	t.Helper()
	old := generateProbeIdentity(t)
	other := generateProbeIdentity(t)
	welcome := oldReleaseWelcome(t, old, config.MinimumProtocolVersion)
	correct := frameLine(t, welcome)

	unbound := welcome
	unbound.Address = other.Address
	foreignNetwork := welcome
	foreignNetwork.Network = "another-network"
	challenged := welcome
	challenged.Challenge = "challenge-on-a-probe"

	return map[string][]byte{
		"identity fields that do not bind": frameLine(t, unbound),
		"an answer cut off half way":       correct[:len(correct)/2],
		"no answer, the connection closed": nil,
		"not a frame":                      []byte("not json at all\n"),
		"an error frame":                   v1ErrorLine,
		"another network":                  frameLine(t, foreignNetwork),
		"a welcome with a challenge":       frameLine(t, challenged),
	}
}

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

// A probe answer that admits nothing costs the address nothing but the pacing
// of its next dial, with TLS-first on (the TLS attempt answered by a v1 line,
// then the probe) and off (the probe first).
func TestAProbeAnswerThatAdmitsNothingOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	for name, end := range unprovenProbeEnds(t) {
		for _, tlsFirstDisabled := range []bool{false, true} {
			position := map[bool]string{false: "tls-first on", true: "tls-first off"}[tlsFirstDisabled]
			t.Run(name+"/"+position, func(t *testing.T) {
				t.Parallel()
				svc := newTLSFirstSwitchService(t, tlsFirstDisabled)
				scripts := []socketScript{end}
				if !tlsFirstDisabled {
					scripts = []socketScript{answerClientHello(v1ErrorLine), end}
				}
				peer := newScriptedPeer(scripts...)
				_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
				if sockets := peer.sockets(t); len(sockets) != len(scripts) {
					t.Fatalf("%d sockets, want %d", len(sockets), len(scripts))
				}
				requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
				if svc.secureSessions.marks.legacyHeld(dialChoiceAddress) {
					t.Fatal("an answer that admits nothing held the fall back to v1: nothing was refused")
				}
			})
		}
	}
}

// A TLS handshake that answered and then failed — before the identity proof
// verified — costs the address nothing but the pacing of its next dial, and
// leads to v1 by no path: no probe follows. With TLS-first off the TLS
// attempt is the one a v2 welcome to the probe called for.
func TestATLSHandshakeBrokenBeforeTheProofOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	v2Probe, _ := oldNodeWelcomes(t, generateProbeIdentity(t), config.ProtocolVersionSecureSession)
	for _, tlsFirstDisabled := range []bool{false, true} {
		position := map[bool]string{false: "tls-first on", true: "tls-first off"}[tlsFirstDisabled]
		t.Run(position, func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, tlsFirstDisabled)
			scripts := []socketScript{answerClientHello(tlsHandshakeGarbage)}
			if tlsFirstDisabled {
				scripts = []socketScript{probeAnsweredWith(frameLine(t, v2Probe)), answerClientHello(tlsHandshakeGarbage)}
			}
			peer := newScriptedPeer(scripts...)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			sockets := peer.sockets(t)
			if len(sockets) != len(scripts) || !isClientHello(sockets[len(sockets)-1]) {
				t.Fatalf("sockets = %q, want the dial to end on its TLS attempt", sockets)
			}
			requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
		})
	}
}

// Once the pacing ran out, the next dial is chosen as usual: with TLS-first
// off the probe opens it again.
func TestAfterTheUnprovenFailurePacingTheDialIsChosenAsUsual(t *testing.T) {
	t.Parallel()
	svc := newTLSFirstSwitchService(t, true)
	answer := unprovenProbeAnswerLines(t)["identity fields that do not bind"]
	first := newScriptedPeer(probeAnsweredWith(answer))
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), first); err == nil {
		t.Fatal("a probe answer that admits nothing opened a session")
	}
	held := newScriptedPeer(probeAnsweredWith(answer))
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), held); err == nil || !isUnchargedDialRefusal(err) {
		t.Fatalf("a dial inside the pacing: %v, want it held uncharged", err)
	}
	if sockets := held.sockets(t); len(sockets) != 0 {
		t.Fatalf("%d sockets inside the pacing, want none", len(sockets))
	}
	svc.secureSessions.marks.clearV2RetryWait(dialChoiceAddress)
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	after := newScriptedPeer(oldNodeSocket(probe, full))
	transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), after)
	if err != nil {
		t.Fatalf("the dial after the pacing: %v", err)
	}
	_ = transport.wire.Close()
	sockets := after.sockets(t)
	noClientHello(t, sockets)
	if len(sockets) != 1 || !isProbe(t, sockets[0]) {
		t.Fatalf("sockets = %q, want the probe first again", sockets)
	}
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

// Where v1 is ruled out — a dial "to X", or the TLS attempt a v2 welcome to
// the probe called for — a TLS attempt over an opened TCP connection that
// ends before any proof is no failure of the address: no probe, no v1, one
// step of the v2 retry backoff.
func TestATLSAttemptEndedBeforeAProofWhereV1IsRuledOutOnlyPacesTheAddress(t *testing.T) {
	t.Parallel()
	v2Probe, _ := oldNodeWelcomes(t, generateProbeIdentity(t), config.ProtocolVersionSecureSession)
	for name, end := range tlsUnansweredEnds() {
		t.Run(name+"/a dial to X", func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, false)
			peer := newScriptedPeer(end)
			_, err := dialThrough(t, svc, dialChoiceAddress, dialToIdentity(t, domain.PeerIdentity{9}), peer)
			sockets := peer.sockets(t)
			if len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("sockets = %q, want the TLS attempt alone", sockets)
			}
			requireUnprovenFailureCost(t, svc, dialChoiceAddress, err)
		})
		t.Run(name+"/after a v2 welcome to the probe", func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, true)
			peer := newScriptedPeer(probeAnsweredWith(frameLine(t, v2Probe)), end)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			sockets := peer.sockets(t)
			if len(sockets) != 2 || !isProbe(t, sockets[0]) || !isClientHello(sockets[1]) {
				t.Fatalf("sockets = %q, want the probe, then the TLS attempt alone", sockets)
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
	svc := newTLSFirstSwitchService(t, false)
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

// One dial is one step of the v2 retry backoff however long it takes: with
// TLS-first off, a v2 welcome to the probe steps it, and the TLS attempt that
// follows — here outlasting the whole wait, as overlay timeouts can — fails
// without a second step.
func TestOneDialStepsTheV2RetryBackoffOnceEvenWhenItOutlastsTheWait(t *testing.T) {
	t.Parallel()
	clock := &lockedClock{now: time.Unix(1780000000, 0)}
	svc := newTLSFirstSwitchService(t, true)
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	v2Probe, _ := oldNodeWelcomes(t, generateProbeIdentity(t), config.ProtocolVersionSecureSession)
	slowTLS := func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.Peek(1); err != nil {
			return
		}
		clock.advance(v2RetryMax + time.Minute)
		answerClientHello(tlsHandshakeGarbage)(conn, reader)
	}
	peer := newScriptedPeer(probeAnsweredWith(frameLine(t, v2Probe)), slowTLS)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
		t.Fatal("the dial succeeded")
	}
	if steps := v2RetrySteps(svc.secureSessions.marks, dialChoiceAddress); steps != 1 {
		t.Fatalf("one dial took %d steps of the v2 retry backoff, want 1", steps)
	}
}

// A probe that failed on this node before anything reached the far end says
// nothing about the address: not charged, no step.
func TestAProbeThatFailsOnThisNodeCostsNothing(t *testing.T) {
	t.Parallel()
	svc := newTLSFirstSwitchService(t, true)
	broken := testDialTimeouts()
	broken.direct.probe.timeout = 0
	svc.secureSessions.dialTimeouts = broken
	peer := newScriptedPeer(silentPeer)
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, errLegacyProbeOptionsInvalid) {
		t.Fatalf("err = %v, want the probe's own options refused", err)
	}
	requireNoCost(t, svc, dialChoiceAddress, err)
}

// A dial this node cancels — its own stop, or the caller giving up — is no
// failure of the address and paces nothing, in the TLS attempt and in the
// probe alike.
func TestADialCancelledByThisNodeCostsNothing(t *testing.T) {
	t.Parallel()
	for _, tlsFirstDisabled := range []bool{false, true} {
		position := map[bool]string{false: "during the TLS attempt", true: "during the probe"}[tlsFirstDisabled]
		t.Run(position, func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, tlsFirstDisabled)
			slow := testDialTimeouts()
			slow.direct.tls.TLS, slow.direct.probe.timeout = 10*time.Second, 10*time.Second
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
		})
	}
}

// A TCP connection that cannot be opened is an ordinary failed dial of the
// address — a reachability failure, nothing more: no ban, no step of the v2
// retry backoff, no pin.
func TestAnUnopenedTCPConnectionIsAnOrdinaryFailedDial(t *testing.T) {
	t.Parallel()
	svc := newTLSFirstSwitchService(t, false)
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
