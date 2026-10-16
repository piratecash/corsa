package node

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore/netcoretest"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// session_tls_first_switch_test.go pins the operator's TLS-first switch
// (CORSA_V2_TLS_FIRST, config.Node.DisableV2TLSFirst): it only skips the
// preliminary ClientHello and never relaxes what requires v2. Switched off, a
// dial where v1 is not ruled out opens with the identity-less probe and no
// ClientHello, and a correct v2 welcome to that probe leads to TLS in the same
// dial whose failure allows no v1, since the endpoint has shown it speaks v2. A
// pinned identity at an endpoint nothing binds is probed too — it is known only
// from the probe's answer — and refused on that answer before this node's
// identity is on the wire; the hold the refusal starts makes the next dial open
// with TLS. Where v2 is required before any answer — a bound endpoint,
// v2_strong, a dial "to X", a v2-only node — nothing changes: TLS, no probe, no
// v1. Switched on, which is the default, every dial opens with TLS as before.

// newTLSFirstSwitchService is newDialChoiceService with the switch set as
// tlsFirstDisabled says, through the node's config.
func newTLSFirstSwitchService(t *testing.T, tlsFirstDisabled bool) *Service {
	t.Helper()
	backend := netcoretest.New()
	t.Cleanup(backend.Shutdown)
	svc := NewServiceWithNetwork(config.Node{
		ListenAddress:     "127.0.0.1:0",
		Type:              config.NodeTypeFull,
		TrustStorePath:    t.TempDir() + "/trust.json",
		DisableV2TLSFirst: tlsFirstDisabled,
	}, testIdentityForNetworkConsumerTest(t), backend)
	t.Cleanup(svc.WaitBackground)
	svc.secureSessions.dialTimeouts = testDialTimeouts()
	return svc
}

// noClientHello fails the test if any socket opened with a ClientHello.
func noClientHello(t *testing.T, sockets [][]byte) {
	t.Helper()
	for i, written := range sockets {
		if isClientHello(written) {
			t.Fatalf("socket %d opened with a ClientHello while TLS-first is off", i)
		}
	}
}

// Switched off, an old release at an address nothing binds is reached on
// one connection: the probe first, the full hello after it on the same
// socket, and no ClientHello anywhere. Switched on, the same dial opens with
// TLS as before.
func TestTheTLSFirstSwitchDecidesWhatAnUnpinnedDialOpensWith(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		t.Run(versionName(version)+"/off", func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, true)
			probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), version)
			peer := newScriptedPeer(oldNodeSocket(probe, full))
			transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			if transport.proven != nil || transport.probed == nil {
				t.Fatalf("transport proven=%v probed=%v, want v1 opened by the probe", transport.proven != nil, transport.probed)
			}
			_ = transport.wire.Close()
			sockets := peer.sockets(t)
			noClientHello(t, sockets)
			if len(sockets) != 1 || !isProbe(t, sockets[0]) {
				t.Fatalf("sockets = %q, want the probe alone on one socket", sockets)
			}
			if !svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
				t.Fatal("a correct old answer did not set legacy_observed")
			}
			if stats := svc.SecureSessionStoreStats(); !stats.TLSFirstDisabled || stats.DialsOpenedByProbe != 1 {
				t.Fatalf("stats: disabled %v, dials opened by the probe %d; want true and 1", stats.TLSFirstDisabled, stats.DialsOpenedByProbe)
			}
		})
		t.Run(versionName(version)+"/on", func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, false)
			probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), version)
			peer := newScriptedPeer(oldNodeSocket(probe, full))
			transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			_ = transport.wire.Close()
			sockets := peer.sockets(t)
			if len(sockets) != 2 || !isClientHello(sockets[0]) || !isProbe(t, sockets[1]) {
				t.Fatalf("sockets = %q, want the TLS attempt, then the probe", sockets)
			}
			if stats := svc.SecureSessionStoreStats(); stats.TLSFirstDisabled || stats.DialsOpenedByProbe != 0 {
				t.Fatalf("stats: disabled %v, dials opened by the probe %d; want false and 0", stats.TLSFirstDisabled, stats.DialsOpenedByProbe)
			}
		})
	}
}

// Switched off, nothing changes where v2 is required — the switch spares old
// nodes a ClientHello and must not open a downgrade:
// every socket opens with TLS, no probe runs, and a v1 answer is refused.
func TestWithTLSFirstOffWhatRequiresV2IsStillDialledOverTLSOnly(t *testing.T) {
	t.Parallel()
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.MinimumProtocolVersion)
	answers := map[string]socketScript{
		"a v1 line":   answerClientHello(v1ErrorLine),
		"an old node": oldNodeSocket(probe, full),
		"silence":     silentPeer,
		"a TLS alert": answerClientHello(sessionv2.RefusalAlert()),
	}
	for reason, require := range v2RequiringDials() {
		for answer, script := range answers {
			t.Run(reason+"/"+answer, func(t *testing.T) {
				t.Parallel()
				svc := newTLSFirstSwitchService(t, true)
				intent := require(t, svc)
				peer := newScriptedPeer(script)
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
				if got := svc.SecureSessionStoreStats().DialsOpenedByProbe; got != 0 {
					t.Fatalf("%d dials opened by the probe where v2 is required", got)
				}
			})
		}
	}
}

// Switched off, a pinned identity at an address nothing binds is refused on
// the probe's answer — the far end reads the probe and nothing else — and the
// refusal's hold makes the next dial — once its pacing ran out — open with
// TLS, whose v1 answer is refused in turn: the probe never leads to v1 for an identity that requires
// v2, the switch notwithstanding.
func TestWithTLSFirstOffAPinnedIdentityIsRefusedOnTheProbeThenDialledOverTLS(t *testing.T) {
	t.Parallel()
	svc := newTLSFirstSwitchService(t, true)
	old := generateProbeIdentity(t)
	if err := svc.secureSessions.store.noteProvenInbound(domain.PeerIdentityFromWire(old.Address)); err != nil {
		t.Fatalf("pin: %v", err)
	}
	probe, full := oldNodeWelcomes(t, old, config.MinimumProtocolVersion)
	peer := newScriptedPeer(oldNodeSocket(probe, full))

	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedPinned) {
		t.Fatalf("first dial: %v, want errLegacyRefusedPinned", err)
	}
	// The refusal paces the next attempt; the test does not wait it out.
	svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
	if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedBackoff) {
		t.Fatalf("second dial: %v, want the v1 answer to its TLS attempt held", err)
	}
	sockets := peer.sockets(t)
	probeLine, err := legacyProbeLine()
	if err != nil {
		t.Fatalf("legacyProbeLine: %v", err)
	}
	if len(sockets) != 2 || !bytes.Equal(sockets[0], probeLine) || !isClientHello(sockets[1]) {
		t.Fatalf("sockets = %q, want the probe alone, then a TLS attempt", sockets)
	}
}

// Switched off, a correct v2 welcome to the probe requires v2 from then on,
// since a v1 answer after it can only be a downgrade: the same dial goes on with a TLS attempt on a new
// connection, and when that attempt fails nothing in it leads to v1 — no
// second probe, no full hello. A TLS alert to the probe ends the dial, and
// the next dial opens with the probe again.
func TestWithTLSFirstOffAV2WelcomeToTheProbeRequiresV2(t *testing.T) {
	t.Parallel()
	probe, full := oldNodeWelcomes(t, generateProbeIdentity(t), config.ProtocolVersionSecureSession)
	failures := map[string]struct {
		script socketScript
		want   error
	}{
		"a v1 line":   {answerClientHello(v1ErrorLine), errLegacyRefusedAfterV2Welcome},
		"silence":     {silentPeer, nil},
		"a TLS error": {answerClientHello(tlsHandshakeGarbage), nil},
		"an alert":    {answerClientHello(sessionv2.RefusalAlert()), errV2RefusedByListener},
	}
	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, true)
			peer := newScriptedPeer(oldNodeSocket(probe, full), failure.script, oldNodeSocket(probe, full))
			transport, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			if err == nil {
				t.Fatalf("the dial succeeded: proven=%v probed=%v; want no v1 after a v2 welcome", transport.proven != nil, transport.probed)
			}
			if failure.want != nil && !errors.Is(err, failure.want) {
				t.Fatalf("err = %v, want %v", err, failure.want)
			}
			sockets := peer.sockets(t)
			probeLine, lineErr := legacyProbeLine()
			if lineErr != nil {
				t.Fatalf("legacyProbeLine: %v", lineErr)
			}
			if len(sockets) != 2 || !bytes.Equal(sockets[0], probeLine) || !isClientHello(sockets[1]) {
				t.Fatalf("sockets = %q, want the probe alone, then one TLS attempt", sockets)
			}
			if svc.secureSessions.marks.legacyObserved(dialChoiceAddress) {
				t.Fatal("a v2 welcome left legacy_observed")
			}
			if !svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
				t.Fatal("the next dial does not wait for the v2 retry backoff")
			}
			if failure.want != nil && !isUnchargedDialRefusal(err) {
				t.Fatalf("the refusal is charged to the address: %v", err)
			}
		})
	}
	// A v1 line on the TLS attempt after a v2 welcome is counted on its own,
	// not as a refusal of a bound endpoint.
	t.Run("a v1 line is counted apart", func(t *testing.T) {
		t.Parallel()
		svc := newTLSFirstSwitchService(t, true)
		peer := newScriptedPeer(oldNodeSocket(probe, full), answerClientHello(v1ErrorLine))
		if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedAfterV2Welcome) || errors.Is(err, errLegacyRefusedV2Bound) {
			t.Fatalf("err = %v, want errLegacyRefusedAfterV2Welcome alone", err)
		}
		stats := svc.SecureSessionStoreStats()
		if stats.V1RefusalsAfterV2Welcome != 1 || stats.V1RefusalsEndpointBound != 0 {
			t.Fatalf("counters: after v2 welcome %d, endpoint bound %d; want 1 and 0", stats.V1RefusalsAfterV2Welcome, stats.V1RefusalsEndpointBound)
		}
	})
	t.Run("alert to the probe", func(t *testing.T) {
		t.Parallel()
		svc := newTLSFirstSwitchService(t, true)
		peer := newScriptedPeer(answerClientHello(sessionv2.RefusalAlert()))
		for range 2 {
			svc.secureSessions.marks.clearV2Retry(dialChoiceAddress)
			_, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer)
			// The listener's refusal, as on a TLS attempt: paced with a
			// jitter, not charged.
			if !errors.Is(err, errLegacyProbeAnsweredV2) || !errors.Is(err, errV2RefusedByListener) || !isUnchargedDialRefusal(err) {
				t.Fatalf("dial: %v, want an uncharged listener refusal of the probe", err)
			}
			if !svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
				t.Fatal("the next dial does not wait after an alert to the probe")
			}
		}
		sockets := peer.sockets(t)
		noClientHello(t, sockets)
		for i, written := range sockets {
			if !isProbe(t, written) {
				t.Fatalf("socket %d opened with %q, want the probe", i, firstLine(written))
			}
		}
	})
}

// Switched off, a probe the far end closes without an answer — what a node
// that serves TLS alone does to a non-TLS connection — paces the next dial
// like a probe nobody answered in time, so the connection manager does not
// redial the address at its own rate; the next dial is held before any
// socket.
func TestWithTLSFirstOffAProbeClosedWithoutAnAnswerPacesTheNextDial(t *testing.T) {
	t.Parallel()
	closers := map[string]socketScript{
		"EOF":   resetAfterFirstBytes,
		"reset": func(conn net.Conn, reader *bufio.Reader) { _, _ = reader.Peek(1); resetConnOf(conn) },
	}
	for name, closer := range closers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newTLSFirstSwitchService(t, true)
			peer := newScriptedPeer(closer)
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); err == nil {
				t.Fatal("a probe closed without an answer opened a session")
			}
			if !svc.secureSessions.marks.v2RetryHeld(dialChoiceAddress) {
				t.Fatal("a probe closed without an answer left the next dial unpaced")
			}
			if _, err := dialThrough(t, svc, dialChoiceAddress, domain.DialToAddress(), peer); !errors.Is(err, errV2RetryHeld) {
				t.Fatalf("second dial: %v, want it held by the v2 retry backoff", err)
			}
			if sockets := peer.sockets(t); len(sockets) != 1 {
				t.Fatalf("%d sockets, want the first probe's alone", len(sockets))
			}
		})
	}
}

// resetConnOf closes conn with a reset when it is a TCP connection, and
// plainly otherwise.
func resetConnOf(conn net.Conn) {
	if recorded, ok := conn.(*recordingConn); ok {
		conn = recorded.Conn
	}
	resetConn(conn)
}

// The node reports the switch: fetch_aggregate_status carries v2_tls_first,
// true by default and false once the operator switched it off.
func TestTheAggregateStatusReportsTheTLSFirstSwitch(t *testing.T) {
	t.Parallel()
	for disabled, want := range map[bool]bool{false: true, true: false} {
		svc := newTLSFirstSwitchService(t, disabled)
		frame := svc.aggregateStatusFrame()
		if frame.AggregateStatus == nil || frame.AggregateStatus.V2TLSFirst != want {
			t.Fatalf("disabled=%v: aggregate status %+v, want v2_tls_first=%v", disabled, frame.AggregateStatus, want)
		}
		encoded, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !strings.Contains(encoded, `"v2_tls_first":`) {
			t.Fatalf("disabled=%v: the frame omits v2_tls_first: %s", disabled, encoded)
		}
	}
}

// Switching TLS-first off is said loudly at start, with the variable that
// turns it back on; left on, nothing is said.
func TestSwitchingTLSFirstOffIsWarnedAtStart(t *testing.T) {
	t.Parallel()
	for _, mode := range []sessionv2.Mode{sessionv2.ModeTransition, sessionv2.ModeLegacyOnly} {
		var journal bytes.Buffer
		warnTLSFirstDisabled(zerolog.New(&journal), true, mode)
		line := journal.String()
		for _, want := range []string{`"level":"warn"`, "secure_session_v2_tls_first_disabled", "CORSA_V2_TLS_FIRST", mode.String()} {
			if !strings.Contains(line, want) {
				t.Fatalf("%s: the warning %q lacks %q", mode, line, want)
			}
		}
		journal.Reset()
		warnTLSFirstDisabled(zerolog.New(&journal), false, mode)
		if journal.Len() != 0 {
			t.Fatalf("%s: TLS-first on still logged %q", mode, journal.String())
		}
	}
}

// --- against emulated releases and a node of this build ---

// startTLSFirstOffQuietNode is startQuietNode with TLS-first switched off
// through its config.
func startTLSFirstOffQuietNode(t *testing.T) (*Service, func()) {
	t.Helper()
	return startTestNodeWithSetup(t, config.Node{
		ListenAddress:     freeAddress(t),
		BootstrapPeers:    []string{},
		Type:              domain.NodeTypeFull,
		DisableV2TLSFirst: true,
	}, withoutOwnDials(nil))
}

// Switched off, an emulated old release (v28, v29, v30) is reached on one
// connection: the identity-less probe, then the full hello — and no
// ClientHello reaches the wire.
func TestWithTLSFirstOffAnOldReleaseNeverSeesAClientHello(t *testing.T) {
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
			dialer, stopDialer := startTLSFirstOffQuietNode(t)
			defer stopDialer()

			if established, err := dialPastTransport(t, dialer, proxy.peerAddress()); !established {
				t.Fatalf("the dial to the old release was not established: %v", err)
			}
			proxy.drain(t)
			if got := proxy.connections(); got != 1 {
				t.Fatalf("%d connections, want the probe's alone", got)
			}
			if got := proxy.clientHellos(); got != 0 {
				t.Fatalf("%d ClientHellos on the wire with TLS-first off", got)
			}
			if hellos := proxy.nodeHellos(); len(hellos) != 2 || hellos[0] || !hellos[1] {
				t.Fatalf("hellos (identified?) = %v, want the identity-less probe, then the full hello", hellos)
			}
		})
	}
}

// Switched off, an emulated old release whose identity this node pinned is
// refused on the probe's answer and never hears this node's identity; the next
// dial goes TLS first and its v1 answer is refused too.
func TestWithTLSFirstOffAPinnedOldReleaseIsRefusedThenDialledOverTLS(t *testing.T) {
	t.Parallel()
	oldAddress := freeAddress(t)
	proxy := startLegacyVersionProxy(t, oldAddress, oldPeerListens, config.MinimumProtocolVersion)
	old, stopOld := startTestNodeWithSetup(t, config.Node{
		ListenAddress:  oldAddress,
		BootstrapPeers: []string{},
		Type:           config.NodeTypeFull,
	}, withoutOwnDials(asOldNode))
	defer stopOld()
	dialer, stopDialer := startTLSFirstOffQuietNode(t)
	defer stopDialer()
	if err := dialer.secureSessions.store.noteProvenInbound(peerIdentityOf(t, old)); err != nil {
		t.Fatalf("pin: %v", err)
	}
	target := proxy.peerAddress()

	if established, err := dialPastTransport(t, dialer, target); established || !errors.Is(err, errLegacyRefusedPinned) {
		t.Fatalf("first dial: established=%v err=%v, want errLegacyRefusedPinned", established, err)
	}
	dialer.secureSessions.marks.clearV2Retry(target)
	if established, err := dialPastTransport(t, dialer, target); established || !errors.Is(err, errLegacyRefusedBackoff) {
		t.Fatalf("second dial: established=%v err=%v, want the v1 answer to TLS held", established, err)
	}
	proxy.drain(t)
	if got := proxy.clientHellos(); got != 1 {
		t.Fatalf("%d ClientHellos, want the second dial's alone", got)
	}
	for i, identified := range proxy.nodeHellos() {
		if identified {
			t.Fatalf("hello %d carried this node's identity to a pinned old release", i)
		}
	}
}

// Switched off, a node of this build at an endpoint nothing binds is probed
// first and, on its v2 welcome, reached over TLS in the same dial — two
// connections, a v2 session; at an endpoint bound to it, over TLS at once.
func TestWithTLSFirstOffANodeOfThisBuildIsReachedOverV2(t *testing.T) {
	t.Parallel()
	t.Run("unbound", func(t *testing.T) {
		t.Parallel()
		_, address, stopListener := listenerNode(t, withoutOwnDials(nil))
		defer stopListener()
		dialer, stopDialer := startTLSFirstOffQuietNode(t)
		defer stopDialer()
		target, connections := startCountingForwarder(t, address)

		session, err := dialEstablishedPastTransport(t, dialer, target)
		if err != nil {
			t.Fatalf("the dial was not established: %v", err)
		}
		if _, proven := session.provenIdentity(); !proven {
			t.Fatal("the session is v1, want v2: a v2 welcome to the probe must lead to TLS")
		}
		if got := connections(); got != 2 {
			t.Fatalf("%d connections, want the probe's, then the TLS one", got)
		}
	})
	t.Run("bound", func(t *testing.T) {
		t.Parallel()
		listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
		defer stopListener()
		dialer, stopDialer := startTLSFirstOffQuietNode(t)
		defer stopDialer()
		target, connections := startCountingForwarder(t, address)
		if _, err := dialer.secureSessions.store.noteProvenOutbound(dialledAt(target), peerIdentityOf(t, listener), endpointProtectionNone()); err != nil {
			t.Fatalf("bind: %v", err)
		}

		session, err := dialEstablishedPastTransport(t, dialer, target)
		if err != nil {
			t.Fatalf("the dial was not established: %v", err)
		}
		if _, proven := session.provenIdentity(); !proven {
			t.Fatal("the session to a bound endpoint is v1, want v2")
		}
		if got := connections(); got != 1 {
			t.Fatalf("%d connections, want the TLS one alone", got)
		}
		if got := dialer.SecureSessionStoreStats().DialsOpenedByProbe; got != 0 {
			t.Fatalf("%d dials opened by the probe to a bound endpoint", got)
		}
	})
}
