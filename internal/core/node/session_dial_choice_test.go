package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_dial_choice_test.go pins how the dialler chooses the session kind
// for one address (docs/protocol/session_v2.md, "Choosing the session kind on
// dial"): TLS first; what answered a failed TLS attempt decides whether a
// separate identity-less probe may follow; only a correct old-protocol answer
// to that probe lets v1 in, and only after the identity it names passed the
// pin; the full hello then goes on the probe's own connection and its welcome
// must name what the probe's did. An address, identity or intent that
// requires v2 never probes. The hints the answers leave are unproven and live
// in memory; v2_strong is set by a proven outbound session alone.

// testDialTimeouts keep the TLS attempt and the probe short: a silent peer is
// one of the answers under test, and the defaults wait seconds for it.
func testDialTimeouts() sessionDialTimeouts {
	short := dialStepTimeouts{
		tls:   sessionv2.Timeouts{TLS: 400 * time.Millisecond, Proof: 400 * time.Millisecond},
		probe: legacyProbeOptions{timeout: 400 * time.Millisecond, systemNow: time.Now},
	}
	return sessionDialTimeouts{direct: short, overlay: short}
}

// newDialChoiceService is a v2-capable node that dials nothing on its own,
// with short dial timeouts.
func newDialChoiceService(t *testing.T) *Service {
	t.Helper()
	svc := newLegacyRefusalService(t)
	svc.secureSessions.dialTimeouts = testDialTimeouts()
	return svc
}

// socketScript plays the far end of one socket the node under test opened.
// reader reads what the node wrote; every byte it reads is recorded.
type socketScript func(conn net.Conn, reader *bufio.Reader)

// scriptedPeer is a raw dialer whose sockets are served by scripts, one per
// socket in order (the last one repeats). It records every byte the node
// wrote on each socket.
type scriptedPeer struct {
	scripts []socketScript

	mu      sync.Mutex
	written []*bytes.Buffer
	done    sync.WaitGroup
}

func newScriptedPeer(scripts ...socketScript) *scriptedPeer {
	return &scriptedPeer{scripts: scripts}
}

func (p *scriptedPeer) dialer() rawDialer {
	return func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		p.mu.Lock()
		index := len(p.written)
		record := &bytes.Buffer{}
		p.written = append(p.written, record)
		script := p.scripts[min(index, len(p.scripts)-1)]
		p.done.Add(1)
		p.mu.Unlock()
		recorded := &recordingConn{Conn: remote, peer: p, record: record}
		go func() {
			defer p.done.Done()
			defer func() { _ = remote.Close() }()
			script(recorded, bufio.NewReaderSize(recorded, 64<<10))
		}()
		return local, nil
	}
}

// sockets returns, per socket opened so far, the bytes the node wrote on it.
// It waits for every script to end first, so nothing is still being read.
func (p *scriptedPeer) sockets(t *testing.T) [][]byte {
	t.Helper()
	finished := make(chan struct{})
	go func() { p.done.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("a scripted socket did not end within 10s")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]byte, len(p.written))
	for i, record := range p.written {
		out[i] = append([]byte(nil), record.Bytes()...)
	}
	return out
}

type recordingConn struct {
	net.Conn
	peer   *scriptedPeer
	record *bytes.Buffer
}

func (c *recordingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.peer.mu.Lock()
		c.record.Write(b[:n])
		c.peer.mu.Unlock()
	}
	return n, err
}

// drainUntilClosed reads until the node closes its end.
func drainUntilClosed(reader *bufio.Reader) { _, _ = io.Copy(io.Discard, reader) }

// answerClientHello waits for the node's first bytes and answers them.
func answerClientHello(answer []byte) socketScript {
	return func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.Peek(1); err != nil {
			return
		}
		_, _ = conn.Write(answer)
		drainUntilClosed(reader)
	}
}

var (
	v1ErrorLine = []byte(`{"type":"error","code":"invalid-json"}` + "\n")
	// tlsHandshakeGarbage is a handshake record the client cannot parse: the
	// first byte says TLS, what follows fails the handshake.
	tlsHandshakeGarbage = []byte{sessionv2.TLSRecordHandshake, 0x03, 0x03, 0x00, 0x01, 0x00}
)

// resetAfterFirstBytes closes the socket as soon as the node wrote.
func resetAfterFirstBytes(_ net.Conn, reader *bufio.Reader) {
	_, _ = reader.Peek(1)
}

// silentPeer reads and never answers, until the node gives up.
func silentPeer(_ net.Conn, reader *bufio.Reader) { drainUntilClosed(reader) }

// oldNodeSocket plays an old release on one socket: a ClientHello gets the v1
// error line; a probe gets probeWelcome (no challenge); the full hello that
// may follow gets fullWelcome (with a challenge). It stops after that.
func oldNodeSocket(probeWelcome, fullWelcome protocol.Frame) socketScript {
	return func(conn net.Conn, reader *bufio.Reader) {
		first, err := reader.Peek(1)
		if err != nil {
			return
		}
		if first[0] == sessionv2.TLSRecordHandshake {
			_, _ = conn.Write(v1ErrorLine)
			drainUntilClosed(reader)
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		if !writeFrameLine(conn, probeWelcome) {
			return
		}
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		if !writeFrameLine(conn, fullWelcome) {
			return
		}
		drainUntilClosed(reader)
	}
}

func writeFrameLine(conn net.Conn, frame protocol.Frame) bool {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return false
	}
	_, err = conn.Write([]byte(line))
	return err == nil
}

// oldNodeWelcomes are the two welcomes a release of version answers with:
// to the probe without a challenge, to the full hello with one.
func oldNodeWelcomes(t *testing.T, id *identity.Identity, version int) (probe, full protocol.Frame) {
	t.Helper()
	probe = oldReleaseWelcome(t, id, version)
	full = probe
	full.Challenge = "challenge-of-the-old-node"
	return probe, full
}

func dialThrough(t *testing.T, svc *Service, address domain.PeerAddress, intent domain.DialIntent, peer *scriptedPeer) (*dialledTransport, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport, err := svc.dialPeerTransport(ctx, address, intent, peer.dialer())
	if transport != nil {
		t.Cleanup(func() { _ = transport.wire.Close() })
	}
	return transport, err
}

// firstLine is the first line the node wrote on a socket.
func firstLine(written []byte) []byte {
	line, _, _ := bytes.Cut(written, []byte("\n"))
	return line
}

// isProbe reports whether the node opened the socket with the probe.
func isProbe(t *testing.T, written []byte) bool {
	t.Helper()
	probe, err := legacyProbeLine()
	if err != nil {
		t.Fatalf("legacyProbeLine: %v", err)
	}
	return bytes.HasPrefix(written, probe)
}

func isClientHello(written []byte) bool {
	return len(written) > 0 && written[0] == sessionv2.TLSRecordHandshake
}

const dialChoiceAddress = domain.PeerAddress("198.51.100.20:64646")

// --- the marks ---

// The three hints: v2_hint lasts an hour, legacy_observed a day, v2_strong a
// week. None of them outlives its term, and each is kept per address.
func TestTheSessionHintsExpireOnTheirOwnTerms(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		note  func(*sessionAddressMarks, domain.PeerAddress)
		holds func(*sessionAddressMarks, domain.PeerAddress) bool
		term  time.Duration
	}{
		"v2_hint":         {(*sessionAddressMarks).noteV2Hint, (*sessionAddressMarks).v2Hinted, time.Hour},
		"legacy_observed": {(*sessionAddressMarks).noteLegacyObserved, (*sessionAddressMarks).legacyObserved, 24 * time.Hour},
		"v2_strong":       {(*sessionAddressMarks).noteOutboundV2, (*sessionAddressMarks).v2Strong, 7 * 24 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			clock := newStoreClock()
			marks := newSessionAddressMarks(clock.read)
			tc.note(marks, dialChoiceAddress)
			if tc.holds(marks, "198.51.100.21:64646") {
				t.Fatal("another address holds this address's mark")
			}
			clock.now = clock.now.Add(tc.term - time.Second)
			if !tc.holds(marks, dialChoiceAddress) {
				t.Fatal("the mark ended a second early")
			}
			clock.now = clock.now.Add(time.Second)
			if tc.holds(marks, dialChoiceAddress) {
				t.Fatalf("the mark outlived its %v", tc.term)
			}
		})
	}
}

// The hints contradict each other, and the later one wins: a v2 hint or a
// proven v2 session forgets that the address answered like an old node, and
// an old node's answer forgets an earlier v2 hint.
func TestALaterHintReplacesTheContradictingOne(t *testing.T) {
	t.Parallel()
	marks := newSessionAddressMarks(newStoreClock().read)
	marks.noteLegacyObserved(dialChoiceAddress)
	marks.noteV2Hint(dialChoiceAddress)
	if marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("a v2 hint left legacy_observed in place")
	}
	marks.noteLegacyObserved(dialChoiceAddress)
	if marks.v2Hinted(dialChoiceAddress) {
		t.Fatal("an old node's answer left the v2 hint in place")
	}
	marks.noteOutboundV2(dialChoiceAddress)
	if marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("a proven v2 session left legacy_observed in place")
	}
}

// A hint is not a requirement: v2_hint and legacy_observed never decide
// whether v1 may follow; v2_strong does — the address requires v2 while it
// lasts.
func TestOnlyV2StrongMakesAnAddressRequireV2(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	mode := svc.sessionMode()
	marks := svc.secureSessions.marks

	marks.noteV2Hint(dialChoiceAddress)
	if plan := svc.planSessionDial(mode, dialChoiceAddress, domain.DialToAddress()); plan.required || !plan.mayProbe() {
		t.Fatalf("v2_hint: plan %+v, want v1 not ruled out", plan)
	}
	marks.noteLegacyObserved(dialChoiceAddress)
	if plan := svc.planSessionDial(mode, dialChoiceAddress, domain.DialToAddress()); plan.required || !plan.mayProbe() || !plan.legacyObserved {
		t.Fatalf("legacy_observed: plan %+v, want v1 not ruled out", plan)
	}
	marks.noteOutboundV2(dialChoiceAddress)
	if plan := svc.planSessionDial(mode, dialChoiceAddress, domain.DialToAddress()); !plan.required || plan.mayProbe() {
		t.Fatalf("v2_strong: plan %+v, want v2 required", plan)
	}
}

// --- what a failed TLS attempt allows ---

// No evidence — a v1 line, a reset, silence — leaves no mark, and the dial
// goes on with the probe on a separate socket. The probe's answer alone
// decides; here it is no welcome at all, so nothing is allowed.
func TestAFailedTLSAttemptWithoutEvidenceIsFollowedByTheProbe(t *testing.T) {
	t.Parallel()
	for name, attempt := range map[string]socketScript{
		"a v1 line": answerClientHello(v1ErrorLine),
		"a reset":   resetAfterFirstBytes,
		"silence":   silentPeer,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(attempt, resetAfterFirstBytes)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err == nil {
				t.Fatal("the dial succeeded against a peer that never answered the probe")
			}
			sockets := peer.sockets(t)
			if len(sockets) != 2 || !isClientHello(sockets[0]) || !isProbe(t, sockets[1]) {
				t.Fatalf("sockets = %q, want the TLS attempt and then the probe on a second socket", sockets)
			}
			marks := svc.secureSessions.marks
			if marks.v2Hinted(dialChoiceAddress) || marks.legacyObserved(dialChoiceAddress) {
				t.Fatal("an answer that proves nothing left a hint")
			}
		})
	}
}

// A TLS handshake record that then fails is a listener that speaks TLS: no
// probe, a v2 hint, and the next v2 attempt waits for the retry backoff.
func TestATLSFailureAfterTheHandshakeRecordIsAV2HintWithoutProbe(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	peer := newScriptedPeer(answerClientHello(tlsHandshakeGarbage))
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, sessionv2.ErrPeerSpeaksTLS) {
		t.Fatalf("err = %v, want the TLS failure", err)
	}
	if sockets := peer.sockets(t); len(sockets) != 1 {
		t.Fatalf("%d sockets, want the TLS attempt alone", len(sockets))
	}
	marks := svc.secureSessions.marks
	if !marks.v2Hinted(dialChoiceAddress) {
		t.Fatal("no v2 hint after a TLS handshake record")
	}
	if !marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatal("the next v2 attempt is not paced after a TLS failure")
	}
}

// A TLS alert is a v2 hint and, the first time, nothing more; an alert that
// meets an alert's hint lets the probe run, so forged alerts cannot keep an
// old node off v1 for good.
func TestATLSAlertIsAV2HintAndARepeatedOneAllowsTheProbe(t *testing.T) {
	t.Parallel()
	for name, alert := range map[string][]byte{
		"the listener's refusal": sessionv2.RefusalAlert(),
		"a handshake_failure":    {0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(answerClientHello(alert), resetAfterFirstBytes, answerClientHello(alert), resetAfterFirstBytes)
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
				t.Fatal("the first dial succeeded")
			}
			if !svc.secureSessions.marks.v2Hinted(dialChoiceAddress) {
				t.Fatal("no v2 hint after an alert")
			}
			svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
				t.Fatal("the second dial succeeded")
			}
			sockets := peer.sockets(t)
			if len(sockets) != 3 || !isClientHello(sockets[0]) || !isClientHello(sockets[1]) || !isProbe(t, sockets[2]) {
				t.Fatalf("sockets = %q, want two TLS attempts and the probe after the second alert", sockets)
			}
		})
	}
}

// --- what requires v2 ---

// v2RequiringDials are the four ways a dial to dialChoiceAddress requires
// v2; each prepares svc and returns the dial's intent.
func v2RequiringDials() map[string]func(t *testing.T, svc *Service) domain.DialIntent {
	return map[string]func(t *testing.T, svc *Service) domain.DialIntent{
		"endpoint bound": func(t *testing.T, svc *Service) domain.DialIntent {
			if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(dialChoiceAddress), domain.PeerIdentity{1}, endpointProtectionNone()); err != nil {
				t.Fatalf("bind: %v", err)
			}
			return domain.DialToAddress()
		},
		"v2_strong": func(_ *testing.T, svc *Service) domain.DialIntent {
			svc.secureSessions.marks.noteOutboundV2(dialChoiceAddress)
			return domain.DialToAddress()
		},
		"dial to X": func(t *testing.T, _ *Service) domain.DialIntent {
			intent, err := domain.DialToIdentity(domain.PeerIdentity{9})
			if err != nil {
				t.Fatalf("intent: %v", err)
			}
			return intent
		},
		"v2-only node": func(_ *testing.T, svc *Service) domain.DialIntent {
			svc.secureSessions.mode = sessionv2.ModeV2Only
			return domain.DialToAddress()
		},
	}
}

// For an address that requires v2 — bound in the store, proven v2 (v2_strong),
// dialled "to X", or a v2-only node — nothing that answers the TLS attempt
// starts a probe: one socket, whatever the answer.
func TestAnAddressThatRequiresV2IsNeverProbed(t *testing.T) {
	t.Parallel()
	requiring := v2RequiringDials()
	answers := map[string]socketScript{
		"a v1 line":     answerClientHello(v1ErrorLine),
		"silence":       silentPeer,
		"a reset":       resetAfterFirstBytes,
		"a TLS error":   answerClientHello(tlsHandshakeGarbage),
		"an old node":   oldNodeSocket(protocol.Frame{}, protocol.Frame{}),
		"a TLS alert":   answerClientHello(sessionv2.RefusalAlert()),
		"another alert": answerClientHello([]byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28}),
	}
	for reason, require := range requiring {
		for answer, script := range answers {
			t.Run(reason+"/"+answer, func(t *testing.T) {
				t.Parallel()
				svc := newDialChoiceService(t)
				intent := require(t, svc)
				peer := newScriptedPeer(script)
				// Twice: the second attempt runs after the first left its
				// marks, and must not have been opened to v1 by them.
				for range 2 {
					svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
					if transport, err := dialThrough(t, svc, dialChoiceAddress, intent, peer); err == nil {
						t.Fatalf("the dial succeeded: proven=%v", transport.proven != nil)
					}
				}
				for i, written := range peer.sockets(t) {
					if !isClientHello(written) {
						t.Fatalf("socket %d opened with %q, want only TLS attempts", i, firstLine(written))
					}
				}
				if svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
					t.Fatal("an address that requires v2 was marked as an old node")
				}
			})
		}
	}
}

// --- what the probe allows ---

// An old release's correct answer to the probe opens v1 on the probe's own
// connection: legacy_observed is set, the transport carries what the probe's
// welcome named, and the reader keeps the bytes behind the welcome.
func TestACorrectOldAnswerToTheProbeOpensV1OnItsConnection(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			old := generateProbeIdentity(t)
			probe, full := oldNodeWelcomes(t, old, version)
			peer := newScriptedPeer(oldNodeSocket(probe, full))
			transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			if transport.proven != nil || transport.probed == nil {
				t.Fatalf("transport proven=%v probed=%v, want v1 opened by the probe", transport.proven != nil, transport.probed)
			}
			if transport.probed.identity != domain.PeerIdentityFromWire(old.Address) || transport.probed.version != domain.ProtocolVersion(version) {
				t.Fatalf("probed = %+v, want %s at v%d", *transport.probed, old.Address, version)
			}
			if !svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
				t.Fatal("legacy_observed was not set by a correct old answer")
			}
			// The full hello goes on the same connection, and the full
			// welcome comes back through the transport's reader.
			if _, err := io.WriteString(transport.wire, svc.nodeHelloJSONLine()); err != nil {
				t.Fatalf("write the full hello: %v", err)
			}
			line, err := readFrameLine(transport.reader, maxResponseLineBytes)
			if err != nil {
				t.Fatalf("read the full welcome: %v", err)
			}
			welcome, err := protocol.ParseFrameLine(strings.TrimSpace(line))
			if err != nil || welcome.Challenge != full.Challenge {
				t.Fatalf("full welcome = %+v (%v), want the one with the challenge", welcome, err)
			}
			_ = transport.wire.Close()
			sockets := peer.sockets(t)
			if len(sockets) != 2 || !isClientHello(sockets[0]) || !isProbe(t, sockets[1]) {
				t.Fatalf("sockets = %q, want the TLS attempt, then the probe and the full hello on one socket", sockets)
			}
		})
	}
}

// The probe says nothing about this node: the bytes that reach the far end
// before its answer carry none of this node's identity in any encoding.
func TestTheProbeRevealsNoIdentity(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	peer := newScriptedPeer(oldNodeSocket(probe, full))
	transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = transport.wire.Close()
	sockets := peer.sockets(t)
	if len(sockets) != 2 || !isProbe(t, sockets[1]) {
		t.Fatalf("sockets = %q, want the TLS attempt and the probe", sockets)
	}
	beforeAnswer := firstLine(sockets[1])
	for _, secret := range identityEncodings(svc.identity) {
		if bytes.Contains(beforeAnswer, secret) {
			t.Fatalf("the probe carries this node's identity (%q)", secret)
		}
	}
}

// identityEncodings are the forms in which a node's public identity can
// appear on the wire.
func identityEncodings(id *identity.Identity) [][]byte {
	raw := [][]byte{id.PublicKey, id.BoxPublicKey[:], []byte(id.Address)}
	var out [][]byte
	for _, value := range raw {
		out = append(out, value,
			[]byte(base64.StdEncoding.EncodeToString(value)),
			[]byte(base64.RawURLEncoding.EncodeToString(value)),
			[]byte(hex.EncodeToString(value)))
	}
	return append(out, []byte(identity.SignBoxKeyBinding(id)))
}

// A probe answered by a node that speaks v2 is a v2 hint: the connection is
// closed, nothing opens v1, and the next v2 attempt is paced, not charged.
func TestAProbeAnsweredByANewNodeIsAV2Hint(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.ProtocolVersionSecureSession)
	peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(probe, full))
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, errLegacyProbeAnsweredV2) {
		t.Fatalf("err = %v, want errLegacyProbeAnsweredV2", err)
	}
	if !isUnchargedDialRefusal(err) {
		t.Fatal("a v2 answer to the probe is charged to the address")
	}
	marks := svc.secureSessions.marks
	if !marks.v2Hinted(dialChoiceAddress) || marks.legacyObserved(dialChoiceAddress) || !marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatalf("marks after a v2 answer: hint %v, legacy %v, retry %v; want hint and retry only",
			marks.v2Hinted(dialChoiceAddress), marks.legacyObserved(dialChoiceAddress), marks.v2RetryHeld(dialChoiceAddress))
	}
}

// A probe welcome naming an identity that requires v2 is refused before the
// full hello: the far end reads the probe and nothing else, and the fall back
// to v1 at the address is held.
func TestAPinnedIdentityInTheProbeWelcomeStopsTheDialBeforeTheFullHello(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	old := generateProbeIdentity(t)
	if err := svc.secureSessions.store.noteProvenInbound(domain.PeerIdentityFromWire(old.Address)); err != nil {
		t.Fatalf("pin: %v", err)
	}
	probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
	peer := newScriptedPeer(oldNodeSocket(probe, full))
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, errLegacyRefusedPinned) {
		t.Fatalf("err = %v, want errLegacyRefusedPinned", err)
	}
	sockets := peer.sockets(t)
	if len(sockets) != 2 {
		t.Fatalf("%d sockets, want the TLS attempt and the probe", len(sockets))
	}
	probeLine, err := legacyProbeLine()
	if err != nil {
		t.Fatalf("legacyProbeLine: %v", err)
	}
	if !bytes.Equal(sockets[1], probeLine) {
		t.Fatalf("the probe socket carried %q, want the probe alone", sockets[1])
	}
	if !svc.secureSessions.marks.legacyHeld(dialChoiceAddress) || svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("the refusal did not hold the fall back to v1, or marked the address as an old node")
	}
}

// TLS goes first even to an address seen as an old node: skipping the TLS
// attempt is the operator's separate mode, never a dial's own choice. legacy_observed only lets the
// probe follow at once a first alert, which otherwise needs a repeated one;
// the probe still decides afresh.
func TestAnObservedOldNodeIsStillDialledTLSFirst(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	svc.secureSessions.marks.noteLegacyObserved(dialChoiceAddress)
	peer := newScriptedPeer(oldNodeSocket(probe, full))
	transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = transport.wire.Close()
	if sockets := peer.sockets(t); len(sockets) != 2 || !isClientHello(sockets[0]) || !isProbe(t, sockets[1]) {
		t.Fatalf("sockets = %q, want the TLS attempt, then the probe", sockets)
	}

	alerted := newScriptedPeer(answerClientHello([]byte{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28}), oldNodeSocket(probe, full))
	transport, err = dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), alerted)
	if err != nil {
		t.Fatalf("a first alert at an observed old node: %v, want the probe to follow at once", err)
	}
	_ = transport.wire.Close()
	if sockets := alerted.sockets(t); len(sockets) != 2 || !isProbe(t, sockets[1]) {
		t.Fatalf("sockets = %q, want the TLS attempt and the probe", sockets)
	}

	svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
	again := newScriptedPeer(answerClientHello(v1ErrorLine))
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), again); err == nil {
		t.Fatal("a probe answered by a v1 error line opened v1")
	}
	if svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("a probe that allowed nothing left legacy_observed in place")
	}
}

// --- the full welcome on the probe's connection ---

// A full welcome that names another identity or another version than the
// probe's answer on the same connection is a substitution: the dial ends
// before anything is signed, and legacy_observed is forgotten.
func TestAFullWelcomeUnlikeTheProbeAnswerEndsTheDial(t *testing.T) {
	t.Parallel()
	old := generateProbeIdentity(t)
	other := generateProbeIdentity(t)
	for name, swap := range map[string]func(full *protocol.Frame){
		"another identity": func(full *protocol.Frame) {
			full.Address = other.Address
			full.PubKey = identity.PublicKeyBase64(other.PublicKey)
			full.BoxKey = identity.BoxPublicKeyBase64(other.BoxPublicKey)
			full.BoxSig = identity.SignBoxKeyBinding(other)
		},
		"another version": func(full *protocol.Frame) { full.Version = config.MinimumProtocolVersion + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
			swap(&full)
			peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(probe, full))
			transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			if _, err := io.WriteString(transport.wire, svc.nodeHelloJSONLine()); err != nil {
				t.Fatalf("write the full hello: %v", err)
			}
			line, err := readFrameLine(transport.reader, maxResponseLineBytes)
			if err != nil {
				t.Fatalf("read the full welcome: %v", err)
			}
			welcome, err := protocol.ParseFrameLine(strings.TrimSpace(line))
			if err != nil {
				t.Fatalf("parse the full welcome: %v", err)
			}
			if err := svc.refuseFullLegacyWelcome(dialChoiceAddress, transport, welcome); !errors.Is(err, errLegacyWelcomeUnlikeProbe) {
				t.Fatalf("err = %v, want errLegacyWelcomeUnlikeProbe", err)
			}
			if svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
				t.Fatal("a substituted welcome left legacy_observed in place")
			}
			same := probe
			same.Challenge = "x"
			if err := svc.refuseFullLegacyWelcome(dialChoiceAddress, transport, same); err != nil {
				t.Fatalf("the probe's own welcome with a challenge was refused: %v", err)
			}
		})
	}
}

// The same substitution through a real dial path: syncPeer reads the full
// welcome on the probe's connection and never signs auth_session for it.
func TestSyncPeerRefusesAFullWelcomeUnlikeTheProbeAnswer(t *testing.T) {
	t.Parallel()
	old := generateProbeIdentity(t)
	other := generateProbeIdentity(t)
	probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
	full.Address = other.Address
	full.PubKey = identity.PublicKeyBase64(other.PublicKey)
	full.BoxKey = identity.BoxPublicKeyBase64(other.BoxPublicKey)
	full.BoxSig = identity.SignBoxKeyBinding(other)

	svc := newTestService(t, config.NodeTypeFull)
	svc.secureSessions.dialTimeouts = testDialTimeouts()
	frames := &recordedLines{}
	listener := startLineRecordingOldNode(t, probe, full, frames)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if imported := svc.syncPeer(ctx, listener, false); imported != 0 {
		t.Fatalf("syncPeer imported %d contacts through a substituted welcome", imported)
	}
	for _, sent := range frames.all() {
		if sent == "auth_session" {
			t.Fatal("syncPeer signed auth_session for a welcome unlike the probe's answer")
		}
	}
	if got := frames.all(); len(got) != 2 || got[0] != "hello" || got[1] != "hello" {
		t.Fatalf("the old node heard %v, want the probe and the full hello", got)
	}
}

// recordedLines are the frame types one fake old node read, in order.
type recordedLines struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordedLines) add(frameType string) {
	r.mu.Lock()
	r.lines = append(r.lines, frameType)
	r.mu.Unlock()
}

func (r *recordedLines) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.lines...)
}

// startLineRecordingOldNode serves TCP connections as an old node: a TLS
// attempt gets the v1 error line, a v1 connection — probe, then full hello —
// gets probe and full and has every frame type it read recorded, until the
// node under test closes. It is waited for in the test's cleanup.
func startLineRecordingOldNode(t *testing.T, probe, full protocol.Frame, frames *recordedLines) domain.PeerAddress {
	t.Helper()
	return startAnsweringOldNode(t, frames, probe, full)
}

// startAnsweringOldNode is startLineRecordingOldNode with its own answers:
// the n-th frame read on a v1 connection gets answers[n], and frames past
// the last answer are only recorded.
func startAnsweringOldNode(t *testing.T, frames *recordedLines, answers ...protocol.Frame) domain.PeerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var served sync.WaitGroup
	served.Add(1)
	go func() {
		defer served.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			served.Add(1)
			go func() {
				defer served.Done()
				serveRecordedOldNodeConn(conn, answers, frames)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		finished := make(chan struct{})
		go func() { served.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("the fake old node did not end within 10s")
		}
	})
	return domain.PeerAddress(listener.Addr().String())
}

func serveRecordedOldNodeConn(conn net.Conn, answers []protocol.Frame, frames *recordedLines) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReaderSize(conn, 64<<10)
	first, err := reader.Peek(1)
	if err != nil {
		return
	}
	if first[0] == sessionv2.TLSRecordHandshake {
		_, _ = conn.Write(v1ErrorLine)
		drainUntilClosed(reader)
		return
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		if err != nil {
			return
		}
		frames.add(frame.Type)
		if len(answers) == 0 {
			continue
		}
		if !writeFrameLine(conn, answers[0]) {
			return
		}
		answers = answers[1:]
	}
}

// --- the v2 retry backoff ---

// The backoff grows per endpoint — 30 s doubling to 30 min — and one
// endpoint's backoff holds no other endpoint.
func TestTheV2RetryBackoffGrowsPerEndpoint(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	a, b := domain.PeerAddress("198.51.100.30:64646"), domain.PeerAddress("198.51.100.31:64646")
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	for i, wait := range want {
		if got := marks.noteV2Unanswered(a); got != wait {
			t.Fatalf("end %d: wait %v, want %v", i+1, got, wait)
		}
		if marks.v2RetryHeld(b) {
			t.Fatal("endpoint B is held by A's backoff")
		}
		clock.now = clock.now.Add(wait)
	}
	if got := marks.noteV2Unanswered(b); got != 30*time.Second {
		t.Fatalf("B's first wait = %v, want 30s: A's count leaked into it", got)
	}
}

// A dial "to X" on which another identity proved itself paces the next v2
// attempt to that endpoint and charges nothing.
func TestAnOwnerMismatchPacesTheEndpointWithoutChargingIt(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))
	intent, err := domain.DialToIdentity(domain.PeerIdentity{0x42})
	if err != nil {
		t.Fatalf("intent: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = dialer.dialPeerTransport(ctx, target, intent, func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	})
	if !errors.Is(err, errEndpointOwnerMismatch) {
		t.Fatalf("err = %v, want errEndpointOwnerMismatch", err)
	}
	if !dialer.secureSessions.marks.v2RetryHeld(target) {
		t.Fatal("an owner mismatch left the endpoint unpaced")
	}
	if !isUnchargedDialRefusal(err) {
		t.Fatal("an owner mismatch is charged to the address")
	}
}

// The backoff of an endpoint ends with a proven outbound v2 session to that
// endpoint alone: an inbound v2 session of the same identity resets neither
// of its endpoints, and the outbound one to A leaves B's backoff running.
func TestOnlyAnOutboundV2SessionToTheEndpointResetsItsBackoff(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, dialerAddress, stopDialer := listenerNode(t, withoutOwnDials(nil))
	defer stopDialer()
	a := domain.PeerAddress(normalizeAddress(address))
	b := startTCPForwarder(t, address)
	marks := dialer.secureSessions.marks
	for _, endpoint := range []domain.PeerAddress{a, b} {
		marks.noteV2Unanswered(endpoint)
		marks.noteClaimedOldVersion(endpoint, config.MinimumProtocolVersion-1)
	}

	if established, err := dialPastTransport(t, listener, domain.PeerAddress(normalizeAddress(dialerAddress))); !established {
		t.Fatalf("the inbound v2 session from X was not established: %v", err)
	}
	for _, endpoint := range []domain.PeerAddress{a, b} {
		if !marks.v2RetryHeld(endpoint) || !marks.oldVersionHeld(endpoint) {
			t.Fatalf("an inbound v2 session of X reset a delay of its endpoint %s", endpoint)
		}
	}

	marks.clearV2RetryWait(a)
	marks.clearOldVersionWait(a)
	if established, err := dialPastTransport(t, dialer, a); !established {
		t.Fatalf("the outbound v2 session to A was not established: %v", err)
	}
	if got := marks.noteV2Unanswered(a); got != v2RetryBase {
		t.Fatalf("A's next wait = %v, want the first step: the outbound v2 session did not reset it", got)
	}
	if got := marks.noteClaimedOldVersion(a, config.MinimumProtocolVersion-1); got != oldVersionRetryBase {
		t.Fatalf("A's next old-version wait = %v, want the first step: the outbound v2 session did not reset it", got)
	}
	if !marks.v2RetryHeld(b) || !marks.oldVersionHeld(b) {
		t.Fatal("the outbound v2 session to A reset B's delays")
	}
}

// startTCPForwarder is a second endpoint of target: a plain TCP forwarder.
func startTCPForwarder(t *testing.T, target string) domain.PeerAddress {
	t.Helper()
	address, _ := startCountingForwarder(t, target)
	return address
}

// startCountingForwarder is startTCPForwarder that also reports how many
// connections it has accepted.
func startCountingForwarder(t *testing.T, target string) (domain.PeerAddress, func() int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = io.Copy(server, client); _ = server.Close() }()
			go func() { defer wg.Done(); _, _ = io.Copy(client, server); _ = client.Close() }()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); wg.Wait() })
	return domain.PeerAddress(listener.Addr().String()), func() int { return int(accepted.Load()) }
}

// clearV2RetryWait ends the current wait of address's v2 retry backoff and
// keeps its count, as the passing of time would.
func (m *sessionAddressMarks) clearV2RetryWait(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark := m.marks[address]
	if mark.v2Retry != nil {
		mark.v2Retry.until = time.Time{}
		m.marks[address] = mark
	}
}

// A full pin store is this node's own limit, not the peer's failure: the
// refusal charges nothing, the endpoint is not retried while the store has
// no room — at most 30 minutes, never doubled — and once room frees the next
// dial goes ahead at once.
func TestAFullPinStoreIsALocalRefusalRetriedWhenRoomFrees(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startQuietNode(t, func(svc *Service) { fillPinStore(t, svc.secureSessions.store) })
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))

	_, err := dialPastTransport(t, dialer, target)
	if !errors.Is(err, errPinStoreFull) {
		t.Fatalf("err = %v, want errPinStoreFull", err)
	}
	if !isUnchargedDialRefusal(err) {
		t.Fatal("pin_store_full is charged to the address")
	}
	if dialer.secureSessions.marks.v2Strong(target) {
		t.Fatal("a session refused for its pin set v2_strong")
	}
	_, err = dialPastTransport(t, dialer, target)
	if !errors.Is(err, errPinRoomAwaited) {
		t.Fatalf("the next dial with the store still full: err = %v, want errPinRoomAwaited", err)
	}
	if wait := dialer.secureSessions.marks.notePinStoreFull(target); wait != pinRoomWaitMax {
		t.Fatalf("a further refusal waits %v, want %v without doubling", wait, pinRoomWaitMax)
	}

	freeOnePin(dialer.secureSessions.store)
	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("the dial after room freed was not established: %v", err)
	}
	if !dialer.secureSessions.marks.v2Strong(target) {
		t.Fatal("the proven outbound session did not set v2_strong")
	}
}

// freeOnePin removes one pin, the way only a future operator action could.
func freeOnePin(store *secureSessionStore) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for id := range store.identities {
		delete(store.identities, id)
		return
	}
}

// The wait for room ends after 30 minutes even when the store stays full.
func TestTheWaitForPinRoomEndsAfterThirtyMinutes(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	marks.notePinStoreFull(dialChoiceAddress)
	clock.now = clock.now.Add(pinRoomWaitMax - time.Second)
	if !marks.awaitsPinRoom(dialChoiceAddress) {
		t.Fatal("the wait ended early")
	}
	clock.now = clock.now.Add(time.Second)
	if marks.awaitsPinRoom(dialChoiceAddress) {
		t.Fatalf("the wait outlived %v", pinRoomWaitMax)
	}
}

// The onCMDialFailed hook does not charge pin_store_full or a dial held for
// pin room: a hundred of them leave the address as it was.
func TestAFullPinStoreIsNotAFailureOfTheAddress(t *testing.T) {
	t.Parallel()
	for name, refusal := range map[string]error{
		"pin_store_full":     fmt.Errorf("%w: test", errPinStoreFull),
		"held for pin room":  fmt.Errorf("%w: test", errPinRoomAwaited),
		"v2 answer to probe": fmt.Errorf("%w: test", errLegacyProbeAnsweredV2),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newLegacyRefusalService(t)
			address := domain.PeerAddress("198.51.100.7:64646")
			for range 100 {
				svc.onCMDialFailed(address, refusal, false)
			}
			svc.peerMu.RLock()
			defer svc.peerMu.RUnlock()
			if health := svc.health[address]; health != nil && (health.ConsecutiveFailures != 0 || health.Score != 0 || !health.LastDisconnectedAt.IsZero()) {
				t.Fatalf("the refusals charged the address: %+v", *health)
			}
		})
	}
}

// v2_strong comes only from a proven outbound session: an inbound one of the
// same peer does not set it for the peer's address.
func TestV2StrongComesOnlyFromAProvenOutboundSession(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, dialerAddress, stopDialer := listenerNode(t, withoutOwnDials(nil))
	defer stopDialer()
	target := domain.PeerAddress(normalizeAddress(address))

	if established, err := dialPastTransport(t, listener, domain.PeerAddress(normalizeAddress(dialerAddress))); !established {
		t.Fatalf("inbound session: %v", err)
	}
	if dialer.secureSessions.marks.v2Strong(target) {
		t.Fatal("an inbound v2 session set v2_strong")
	}
	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("outbound session: %v", err)
	}
	if !dialer.secureSessions.marks.v2Strong(target) {
		t.Fatal("a proven outbound v2 session did not set v2_strong")
	}
}

// --- against an emulated old release and a node of this build ---

// A node of this build reaches an old release (v28, v29, v30) the way the
// contract says: a TLS attempt the old node answers with a v1 line, then the
// probe on a second connection — identity-less — and the full hello on that
// same connection. The next dial does the same: TLS first again, then the
// probe — two more connections.
func TestDiallingAnOldReleaseGoesTLSThenProbeThenLegacy(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			oldAddress := freeAddress(t)
			proxy := startLegacyVersionProxy(t, oldAddress, oldPeerListens, version)
			_, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  oldAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(asOldNode))
			defer stopOld()
			dialer, stopDialer := startQuietNode(t, nil)
			defer stopDialer()
			target := proxy.peerAddress()

			if established, err := dialPastTransport(t, dialer, target); !established {
				t.Fatalf("the dial to the old release was not established: %v", err)
			}
			if got := proxy.connections(); got != 2 {
				t.Fatalf("%d connections, want the TLS attempt and the probe's", got)
			}
			if hellos := proxy.nodeHellos(); len(hellos) < 2 || hellos[0] || !hellos[1] {
				t.Fatalf("hellos (identified?) = %v, want the identity-less probe, then the full hello", hellos)
			}
			if !dialer.secureSessions.marks.legacyObserved(target) || dialer.secureSessions.marks.v2Strong(target) {
				t.Fatal("the old release was not recorded as legacy_observed, or was taken for v2")
			}

			if established, err := dialPastTransport(t, dialer, target); !established {
				t.Fatalf("the second dial to the old release was not established: %v", err)
			}
			if got := proxy.connections(); got != 4 {
				t.Fatalf("%d connections after the second dial, want two more: TLS first again, then the probe", got)
			}
		})
	}
}

// startOldNodeBehind starts an emulated old release of version and a proxy
// in front of it whose behaviour configure sets before it starts.
func startOldNodeBehind(t *testing.T, version int, configure func(*legacyVersionProxy)) (*legacyVersionProxy, func()) {
	t.Helper()
	oldAddress := freeAddress(t)
	proxy := &legacyVersionProxy{target: oldAddress, side: oldPeerListens, version: version}
	configure(proxy)
	launchLegacyProxy(t, proxy)
	_, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(asOldNode))
	return proxy, stopOld
}

// A TLS attempt dropped right after its ClientHello is no evidence either
// way: the probe follows on a new connection, and v1 comes from the old
// node's correct answer to it — never from the TLS error itself.
func TestADroppedTLSAttemptIsFollowedByTheProbe(t *testing.T) {
	t.Parallel()
	proxy, stopOld := startOldNodeBehind(t, config.MinimumProtocolVersion, func(p *legacyVersionProxy) { p.interceptTLS = tlsDropped })
	defer stopOld()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()

	if established, err := dialPastTransport(t, dialer, proxy.peerAddress()); !established {
		t.Fatalf("the old node was not reached after the dropped TLS attempt: %v", err)
	}
	if got := proxy.intercepted(); got != 1 {
		t.Fatalf("%d TLS attempts dropped, want 1", got)
	}
	if hellos := proxy.nodeHellos(); len(hellos) < 2 || hellos[0] || !hellos[1] {
		t.Fatalf("hellos (identified?) = %v, want the identity-less probe, then the full hello", hellos)
	}
}

// An answer to the probe cut off before its line ended admits nothing: the
// dial ends with no complete answer, and no hello with this node's identity
// is sent.
func TestACutOffProbeAnswerAdmitsNothing(t *testing.T) {
	t.Parallel()
	proxy, stopOld := startOldNodeBehind(t, config.MinimumProtocolVersion, func(p *legacyVersionProxy) { p.cutWelcome = true })
	defer stopOld()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()

	established, err := dialPastTransport(t, dialer, proxy.peerAddress())
	if established || !errors.Is(err, errLegacyProbeNoAnswer) {
		t.Fatalf("established=%v err=%v, want errLegacyProbeNoAnswer", established, err)
	}
	proxy.drain(t)
	if proxy.cut() == 0 {
		t.Fatal("the proxy cut no welcome: the dial ended for another reason")
	}
	for i, identified := range proxy.nodeHellos() {
		if identified {
			t.Fatalf("hello %d carried this node's identity after a cut-off answer", i)
		}
	}
}

// A node of this build is reached over TLS at once: one connection, a proven
// session, v2_strong, and no probe.
func TestDiallingANodeOfThisBuildGoesStraightToV2(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startQuietNode(t, nil)
	defer stopDialer()
	target, connections := startCountingForwarder(t, address)

	if established, err := dialPastTransport(t, dialer, target); !established {
		t.Fatalf("the dial was not established: %v", err)
	}
	if got := connections(); got != 1 {
		t.Fatalf("%d connections, want the TLS one alone", got)
	}
	marks := dialer.secureSessions.marks
	if !marks.v2Strong(target) || marks.legacyObserved(target) || marks.v2Hinted(target) {
		t.Fatalf("marks: strong %v, legacy %v, hint %v; want v2_strong alone",
			marks.v2Strong(target), marks.legacyObserved(target), marks.v2Hinted(target))
	}
}

// An old release whose identity this node has pinned is refused on the
// probe's answer: the full hello — the first frame carrying this node's
// identity — is never sent.
func TestAPinnedOldReleaseNeverHearsTheFullHello(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			oldAddress := freeAddress(t)
			proxy := startLegacyVersionProxy(t, oldAddress, oldPeerListens, version)
			old, stopOld := startTestNodeWithSetup(t, config.Node{
				ListenAddress:  oldAddress,
				BootstrapPeers: []string{},
				Type:           config.NodeTypeFull,
			}, withoutOwnDials(asOldNode))
			defer stopOld()
			oldID := peerIdentityOf(t, old)
			dialer, stopDialer := startQuietNode(t, func(svc *Service) {
				if err := svc.secureSessions.store.noteProvenInbound(oldID); err != nil {
					t.Fatalf("pin: %v", err)
				}
			})
			defer stopDialer()

			established, err := dialPastTransport(t, dialer, proxy.peerAddress())
			if established || !errors.Is(err, errLegacyRefusedPinned) {
				t.Fatalf("established=%v err=%v, want errLegacyRefusedPinned", established, err)
			}
			proxy.drain(t)
			for i, identified := range proxy.nodeHellos() {
				if identified {
					t.Fatalf("hello %d carried this node's identity to a pinned old release", i)
				}
			}
		})
	}
}

// A probe answered by a release below the version floor is an unproven
// claim: the dial ends with the incompatible version, and the endpoint gets
// its old-version delay — no score, no ban of the
// address or its IP.
func TestAProbeAnswerBelowTheFloorIsAnIncompatibleVersion(t *testing.T) {
	t.Parallel()
	belowFloor := config.MinimumProtocolVersion - 1
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), belowFloor)

	t.Run("the transport", func(t *testing.T) {
		t.Parallel()
		svc := newDialChoiceService(t)
		peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(probe, full))
		_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
		var incompatible *incompatibleProtocolError
		if !errors.As(err, &incompatible) || incompatible.PeerVersion != domain.ProtocolVersion(belowFloor) || incompatible.Evidence != versionClaimed {
			t.Fatalf("err = %v, want a claimed incompatible version %d", err, belowFloor)
		}
	})

	t.Run("a one-shot dial", func(t *testing.T) {
		t.Parallel()
		svc := newTestService(t, config.NodeTypeFull)
		svc.secureSessions.dialTimeouts = testDialTimeouts()
		frames := &recordedLines{}
		address := startLineRecordingOldNode(t, probe, full, frames)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc.syncPeer(ctx, address, false)
		assertNoVersionHarm(t, svc, address)
		if !svc.secureSessions.marks.oldVersionHeld(address) {
			t.Fatal("a correct answer below the floor left its endpoint undelayed")
		}
	})
}

// --- review round: races, timeouts, local failures, one step per attempt ---

// A protection that appears while the TLS attempt runs — a parallel dial
// proved v2 at the address — is seen before the probe: the plan is read
// again, and no probe follows.
func TestAProofDuringTheTLSAttemptStopsTheProbe(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	provedInParallel := func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.Peek(1); err != nil {
			return
		}
		svc.secureSessions.marks.noteOutboundV2(dialChoiceAddress)
		_, _ = conn.Write(v1ErrorLine)
		drainUntilClosed(reader)
	}
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	peer := newScriptedPeer(provedInParallel, oldNodeSocket(probe, full))
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedV2Bound) {
		t.Fatalf("err = %v, want the downgrade refused", err)
	}
	if sockets := peer.sockets(t); len(sockets) != 1 {
		t.Fatalf("%d sockets, want the TLS attempt alone", len(sockets))
	}
}

// The same protection appearing while the probe runs is seen before v1 is
// admitted: the connection is closed on the probe, no full hello follows.
func TestAProofDuringTheProbeStopsTheLegacyAdmission(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	probe, _ := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	provedInParallel := func(conn net.Conn, reader *bufio.Reader) {
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		svc.secureSessions.marks.noteOutboundV2(dialChoiceAddress)
		if !writeFrameLine(conn, probe) {
			return
		}
		drainUntilClosed(reader)
	}
	peer := newScriptedPeer(answerClientHello(v1ErrorLine), provedInParallel)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedV2Bound) {
		t.Fatalf("err = %v, want the downgrade refused", err)
	}
	sockets := peer.sockets(t)
	probeLine, err := legacyProbeLine()
	if err != nil {
		t.Fatalf("legacyProbeLine: %v", err)
	}
	if len(sockets) != 2 || !bytes.Equal(sockets[1], probeLine) {
		t.Fatalf("sockets = %q, want the probe alone on the second", sockets)
	}
	if svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("a refused admission marked the address as an old node")
	}
}

// A probe nobody answers says nothing against the address: not charged, the
// next attempt only paced.
func TestAProbeTimeoutIsPacedNotCharged(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	peer := newScriptedPeer(answerClientHello(v1ErrorLine), silentPeer)
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, errLegacyProbeTimeout) {
		t.Fatalf("err = %v, want the probe timeout", err)
	}
	if !isUnchargedDialRefusal(err) {
		t.Fatal("a probe timeout is charged to the address")
	}
	if !svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatal("a probe timeout left the address unpaced")
	}
}

// Overlay addresses — onion, i2p — get their own, longer dial timeouts: a
// cold circuit answers in seconds, not in the direct timeout.
func TestAnOverlayAddressGetsItsOwnDialTimeouts(t *testing.T) {
	t.Parallel()
	timeouts := defaultSessionDialTimeouts()
	onion := domain.PeerAddress("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:64646")
	i2p := domain.PeerAddress("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz.b32.i2p:64646")
	for _, overlay := range []domain.PeerAddress{onion, i2p} {
		if got := timeouts.forAddress(overlay); !sameDialSteps(got, timeouts.overlay) {
			t.Fatalf("%s got %+v, want the overlay timeouts", overlay, got)
		}
	}
	if got := timeouts.forAddress(dialChoiceAddress); !sameDialSteps(got, timeouts.direct) {
		t.Fatalf("a direct address got %+v, want the direct timeouts", got)
	}
	if timeouts.overlay.tls.TLS <= timeouts.direct.tls.TLS || timeouts.overlay.probe.timeout <= timeouts.direct.probe.timeout {
		t.Fatalf("overlay %+v is not longer than direct %+v", timeouts.overlay, timeouts.direct)
	}
}

func sameDialSteps(a, b dialStepTimeouts) bool {
	return a.tls == b.tls && a.probe.timeout == b.probe.timeout
}

// A v2 attempt this node could not even start — its own identity unusable —
// is neither the peer's answer nor a reason to probe: a local failure, not
// charged, nothing marked.
func TestALocalFailureBeforeTheFirstByteIsNotAProbe(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	svc.secureSessions.local = sessionv2.Local{}
	peer := newScriptedPeer(answerClientHello(v1ErrorLine))
	_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if !errors.Is(err, errV2DialLocalFailure) {
		t.Fatalf("err = %v, want the local failure", err)
	}
	if !isUnchargedDialRefusal(err) {
		t.Fatal("a local failure is charged to the address")
	}
	for i, written := range peer.sockets(t) {
		if len(written) != 0 {
			t.Fatalf("socket %d carried %q, want nothing", i, firstLine(written))
		}
	}
	marks := svc.secureSessions.marks
	if marks.v2RetryHeld(dialChoiceAddress) || marks.v2Hinted(dialChoiceAddress) {
		t.Fatal("a local failure left marks on the address")
	}
}

// One attempt steps the v2 retry backoff once: the listener's refusal and a
// v2 answer to the probe that follows it are one attempt.
func TestOneAttemptStepsTheV2RetryBackoffOnce(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	svc.secureSessions.marks.jitter = func(time.Duration) time.Duration { return 0 }
	refusal := answerClientHello(sessionv2.RefusalAlert())
	peer := newScriptedPeer(refusal, refusal, answerClientHello(sessionv2.RefusalAlert()))
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
		t.Fatal("the first dial succeeded")
	}
	svc.secureSessions.marks.clearV2RetryWait(dialChoiceAddress)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyProbeAnsweredV2) {
		t.Fatalf("the second dial: %v, want the probe answered by v2", err)
	}
	svc.secureSessions.marks.mu.Lock()
	unanswered := svc.secureSessions.marks.marks[dialChoiceAddress].v2Retry.unanswered
	svc.secureSessions.marks.mu.Unlock()
	if unanswered != 2 {
		t.Fatalf("%d steps after two attempts, want 2", unanswered)
	}
}

// A backoff still running is not stepped again by a dial that started before
// it: parallel dials that fail together count as one.
func TestARunningV2RetryBackoffIsNotSteppedAgain(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	first := marks.noteV2Unanswered(dialChoiceAddress)
	if again := marks.noteV2Unanswered(dialChoiceAddress); again != first {
		t.Fatalf("a parallel end stepped the running wait to %v, want %v", again, first)
	}
	clock.now = clock.now.Add(first)
	if next := marks.noteV2Unanswered(dialChoiceAddress); next != 2*first {
		t.Fatalf("the next end waits %v, want %v", next, 2*first)
	}
}

// A probe this node aborted itself says nothing about the address: its
// legacy_observed stays, and nothing is paced.
func TestAProbeAbortedByOurContextKeepsLegacyObserved(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	svc.secureSessions.marks.noteLegacyObserved(dialChoiceAddress)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	abortOnProbe := func(_ net.Conn, reader *bufio.Reader) {
		if _, err := reader.ReadString('\n'); err != nil {
			return
		}
		cancel()
		drainUntilClosed(reader)
	}
	peer := newScriptedPeer(answerClientHello(v1ErrorLine), abortOnProbe)
	if _, err := svc.dialPeerTransport(ctx, dialChoiceAddress, domain.DialToAddress(), peer.dialer()); !errors.Is(err, errLegacyProbeAborted) {
		t.Fatalf("err = %v, want the aborted probe", err)
	}
	peer.sockets(t)
	if !svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
		t.Fatal("our own abort forgot legacy_observed")
	}
	if svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatal("our own abort paced the address")
	}
}

// The notice dial in a node that speaks v2: a full welcome unlike the probe's
// answer ends it before auth_session, and a forged low version on the probe
// only paces the address.
func TestSendNoticeToPeerChecksTheProbeAnswer(t *testing.T) {
	t.Parallel()
	t.Run("a substituted welcome", func(t *testing.T) {
		t.Parallel()
		old, other := generateProbeIdentity(t), generateProbeIdentity(t)
		probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
		swapped, _ := oldNodeWelcomes(t, other, config.MinimumProtocolVersion)
		full.Address, full.PubKey, full.BoxKey, full.BoxSig = swapped.Address, swapped.PubKey, swapped.BoxKey, swapped.BoxSig
		svc := newTestService(t, config.NodeTypeFull)
		svc.secureSessions.dialTimeouts = testDialTimeouts()
		frames := &recordedLines{}
		address := startLineRecordingOldNode(t, probe, full, frames)
		svc.sendNoticeToPeer(address, time.Minute, "probe-checked-ciphertext")
		if got := frames.all(); len(got) != 2 || got[0] != "hello" || got[1] != "hello" {
			t.Fatalf("the old node heard %v, want the probe and the full hello only", got)
		}
	})
	t.Run("a forged low version", func(t *testing.T) {
		t.Parallel()
		svc := newTestService(t, config.NodeTypeFull)
		svc.secureSessions.dialTimeouts = testDialTimeouts()
		forged := forgedLowVersionAnswer()
		address := startLineRecordingOldNode(t, forged, forged, &recordedLines{})
		svc.sendNoticeToPeer(address, time.Minute, "probe-checked-ciphertext")
		assertOnlyPaced(t, svc, address)
	})
}

// The full welcome is checked against the plan once more: a protection that
// appeared after the probe admitted v1 — a parallel dial proved v2 — refuses
// it before anything is signed.
func TestAProofAfterTheProbeStopsTheFullWelcome(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(probe, full))
	transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	svc.secureSessions.marks.noteOutboundV2(dialChoiceAddress)
	if err := svc.refuseFullLegacyWelcome(dialChoiceAddress, transport, full); !errors.Is(err, errLegacyRefusedV2Bound) {
		t.Fatalf("err = %v, want the downgrade refused", err)
	}
}

// clearOldVersionWait ends the current wait of address's old-version delay
// and keeps its count, as the passing of time would.
func (m *sessionAddressMarks) clearOldVersionWait(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark := m.marks[address]
	if mark.oldVersion != nil {
		mark.oldVersion.until = time.Time{}
		m.marks[address] = mark
	}
}
