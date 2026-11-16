package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
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
	short := sessionv2.Timeouts{TLS: 400 * time.Millisecond, Proof: 400 * time.Millisecond}
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

func isClientHello(written []byte) bool {
	return len(written) > 0 && written[0] == sessionv2.TLSRecordHandshake
}

const dialChoiceAddress = domain.PeerAddress("198.51.100.20:64646")

// --- the marks ---

func TestTheOutboundProofMarkExpires(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	marks.noteOutboundV2(dialChoiceAddress)
	if marks.v2Strong("198.51.100.21:64646") {
		t.Fatal("another address holds this address's outbound proof mark")
	}
	clock.now = clock.now.Add(7*24*time.Hour - time.Second)
	if !marks.v2Strong(dialChoiceAddress) {
		t.Fatal("the outbound proof mark ended a second early")
	}
	clock.now = clock.now.Add(time.Second)
	if marks.v2Strong(dialChoiceAddress) {
		t.Fatal("the outbound proof mark outlived one week")
	}
}

// --- what a failed TLS attempt allows ---

// No unauthenticated answer to TLS starts a second connection.
func TestFloor31UnprovenTLSFailureNeverOpensAnotherSocket(t *testing.T) {
	t.Parallel()
	for name, attempt := range map[string]socketScript{
		"a v1 line": answerClientHello(v1ErrorLine),
		"a reset":   resetAfterFirstBytes,
		"silence":   silentPeer,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(attempt)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err == nil {
				t.Fatal("the TLS dial succeeded without a peer proof")
			}
			sockets := peer.sockets(t)
			if len(sockets) != 1 || !isClientHello(sockets[0]) {
				t.Fatalf("sockets = %q, want exactly one TLS attempt", sockets)
			}
		})
	}
}

// A failed handshake paces the next TLS attempt.
func TestATLSFailureAfterTheHandshakeRecordPacesRetry(t *testing.T) {
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
	if !marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatal("the next v2 attempt is not paced after a TLS failure")
	}
}

// Repeated TLS alerts cannot open plaintext.
func TestATLSAlertNeverAllowsPlaintext(t *testing.T) {
	t.Parallel()
	for name, alert := range map[string][]byte{
		"the listener's refusal": sessionv2.RefusalAlert(),
		"a handshake_failure":    {0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(answerClientHello(alert))
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
				t.Fatal("the first dial succeeded")
			}
			if !svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
				t.Fatal("no TLS retry backoff after an alert")
			}
			svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
				t.Fatal("the second dial succeeded")
			}
			sockets := peer.sockets(t)
			if len(sockets) != 2 || !isClientHello(sockets[0]) || !isClientHello(sockets[1]) {
				t.Fatalf("sockets = %q, want exactly two TLS attempts", sockets)
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
			})
		}
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
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	defer stopListener()
	dialer, stopDialer := startTestNodeWithSetup(t, config.Node{
		ListenAddress:    freeAddress(t),
		BootstrapPeers:   []string{},
		ReservedPinPeers: []string{listener.Address()},
		Type:             domain.NodeTypeFull,
	}, withoutOwnDials(func(svc *Service) { fillPinStore(t, svc.secureSessions.store) }))
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
		"pin_store_full":    fmt.Errorf("%w: test", errPinStoreFull),
		"held for pin room": fmt.Errorf("%w: test", errPinRoomAwaited),
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
	if !marks.v2Strong(target) {
		t.Fatal("the outbound proof did not leave a strong mark")
	}
}

// --- review round: races, timeouts, local failures, one step per attempt ---

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
	if timeouts.overlay.TLS <= timeouts.direct.TLS || timeouts.overlay.Proof <= timeouts.direct.Proof {
		t.Fatalf("overlay %+v is not longer than direct %+v", timeouts.overlay, timeouts.direct)
	}
}

func sameDialSteps(a, b sessionv2.Timeouts) bool {
	return a == b
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
	if marks.v2RetryHeld(dialChoiceAddress) {
		t.Fatal("a local failure left marks on the address")
	}
}

// Each listener refusal steps the v2 retry backoff once.
func TestOneAttemptStepsTheV2RetryBackoffOnce(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	svc.secureSessions.marks.jitter = func(time.Duration) time.Duration { return 0 }
	refusal := answerClientHello(sessionv2.RefusalAlert())
	peer := newScriptedPeer(refusal)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
		t.Fatal("the first dial succeeded")
	}
	svc.secureSessions.marks.clearV2RetryWait(dialChoiceAddress)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errV2RefusedByListener) {
		t.Fatalf("the second dial: %v, want the TLS listener refusal", err)
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
