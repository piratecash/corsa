package sessionv2

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// handshake_test.go exercises the v2 handshake end to end over in-memory
// connections, including the attack it exists to stop (A1): a man in the
// middle carrying an identity proof from one connection into another.

const testNetwork = domain.NetworkID("gazeta-devnet")

var testTimeouts = Timeouts{TLS: 5 * time.Second, Proof: 5 * time.Second}

type node struct {
	id    *identity.Identity
	peer  domain.PeerIdentity
	local Local
	certs CertificateSource
}

// testIntro is the metadata a node puts into its hello or welcome; the
// identity fields are stamped by the package.
func testIntro() protocol.Frame { return protocol.Frame{Version: 31} }

func newNode(t *testing.T) node {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return nodeFor(t, id)
}

func nodeFor(t *testing.T, id *identity.Identity) node {
	t.Helper()
	peer, err := domain.ParsePeerIdentity(id.Address)
	if err != nil {
		t.Fatalf("peer identity: %v", err)
	}
	local, err := NewLocal(id, testNetwork)
	if err != nil {
		t.Fatalf("local: %v", err)
	}
	certs, err := NewCertificateSource(time.Now)
	if err != nil {
		t.Fatalf("certificates: %v", err)
	}
	return node{id: id, peer: peer, local: local, certs: certs}
}

// connPair is a connected TCP pair on loopback. Unlike net.Pipe it has kernel
// buffers, so an end that refuses and closes (TLS sends close_notify) is not
// blocked behind a peer that is itself blocked writing.
func connPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn
	}()
	dialed, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	other := <-accepted
	if other == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = dialed.Close(); _ = other.Close() })
	return dialed, other
}

type outcome struct {
	session ProvenSession
	err     error
}

func dialAsync(ctx context.Context, conn net.Conn, n node, expect ExpectedPeer) <-chan outcome {
	done := make(chan outcome, 1)
	go func() {
		session, err := Dial(ctx, conn, n.local, testIntro(), expect, testTimeouts)
		done <- outcome{session, err}
	}()
	return done
}

func acceptAsync(ctx context.Context, conn net.Conn, n node) <-chan outcome {
	done := make(chan outcome, 1)
	go func() {
		session, err := Accept(ctx, conn, n.local, testIntro(), n.certs, testTimeouts)
		done <- outcome{session, err}
	}()
	return done
}

func TestBothEndsProveThemselvesAndTheSessionCarriesFrames(t *testing.T) {
	ctx := context.Background()
	dialer, listener := newNode(t), newNode(t)
	dc, lc := connPair(t)
	accepted := acceptAsync(ctx, lc, listener)
	dialed := <-dialAsync(ctx, dc, dialer, ExpectPeer(listener.peer))
	got := <-accepted
	if dialed.err != nil || got.err != nil {
		t.Fatalf("dial %v, accept %v", dialed.err, got.err)
	}
	if peer, _ := dialed.session.Peer(); peer.Identity != listener.peer || peer.Intro.Type != "welcome" {
		t.Fatalf("dialer sees %v (%s), want the listener", peer.Identity, peer.Intro.Type)
	}
	if peer, _ := got.session.Peer(); peer.Identity != dialer.peer || peer.Intro.Type != "hello" {
		t.Fatalf("listener sees %v (%s), want the dialer", peer.Identity, peer.Intro.Type)
	}

	dconn, _ := dialed.session.Conn()
	lconn, _ := got.session.Conn()
	go func() { _, _ = dconn.Write([]byte("after the proof\n")) }()
	line, err := readLine(lconn, 64)
	if err != nil || string(line) != "after the proof" {
		t.Fatalf("frame after the handshake: %q, %v", line, err)
	}
}

// relay is a man in the middle holding a TLS session with each victim. It
// can read and write every frame of both legs; what it cannot do is make a
// proof over one leg's exporter verify on the other.
type relay struct {
	toDialer   *tls.Conn // M as the listener of the victim that dials
	toListener *tls.Conn // M as the dialer of the victim that listens
}

func newRelay(t *testing.T, m node, fromDialer, toListener net.Conn) relay {
	t.Helper()
	r := relay{
		toDialer:   tls.Server(fromDialer, listenerConfig(m.certs)),
		toListener: tls.Client(toListener, dialerConfig()),
	}
	errs := make(chan error, 2)
	go func() { errs <- r.toDialer.Handshake() }()
	go func() { errs <- r.toListener.Handshake() }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("relay handshake: %v", err)
		}
	}
	return r
}

func (r relay) forward(t *testing.T, from, to *tls.Conn, lines int) {
	t.Helper()
	for range lines {
		line, err := readLine(from, maxIntroBytes)
		if err != nil {
			t.Fatalf("relay read: %v", err)
		}
		if _, err := to.Write(append(line, '\n')); err != nil {
			t.Fatalf("relay write: %v", err)
		}
	}
}

// A1, first leg: M carries the listener B's welcome and proof to the
// dialer X, which meant to reach B. The proof was made over E(M↔B); X checks
// it over E(X↔M) and refuses before it signs anything.
func TestARelayedListenerProofDoesNotVerifyOnAnotherConnection(t *testing.T) {
	ctx := context.Background()
	x, b, m := newNode(t), newNode(t), newNode(t)
	xSide, mFromX := connPair(t)
	mToB, bSide := connPair(t)

	bResult := acceptAsync(ctx, bSide, b)
	xResult := dialAsync(ctx, xSide, x, ExpectPeer(b.peer))
	r := newRelay(t, m, mFromX, mToB)
	r.forward(t, r.toListener, r.toDialer, 2) // B's welcome and proof, to X

	if got := <-xResult; !errors.Is(got.err, ErrProofInvalid) {
		t.Fatalf("X dialling B through M = %v, want ErrProofInvalid", got.err)
	}
	// X refused before proving itself: M holds no hello and no signature
	// of X that it could carry anywhere else.
	_ = r.toDialer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if line, err := readLine(r.toDialer, maxIntroBytes); err == nil {
		t.Fatalf("X sent %q to a listener that had not proved itself", line)
	}
	_ = r.toListener.Close()
	if got := <-bResult; got.err == nil {
		t.Fatal("B established a session although X never proved itself")
	}
}

// A1, the attack itself: M proves its own identity to X (an address dial, so
// X accepts whoever proves), receives X's proof, and hands it to B to be
// taken for X. Under v1 this worked — the second half of the test shows it —
// because "challenge|address" names neither the verifier nor the connection.
// Under v2 X's proof is over E(X↔M), and B checks it over E(M↔B).
func TestARelayedDialerProofIsRefusedWhereV1AcceptedIt(t *testing.T) {
	ctx := context.Background()
	x, b, m := newNode(t), newNode(t), newNode(t)
	xSide, mFromX := connPair(t)
	mToB, bSide := connPair(t)

	bResult := acceptAsync(ctx, bSide, b)
	xResult := dialAsync(ctx, xSide, x, AnyPeer())
	r := newRelay(t, m, mFromX, mToB)

	if _, err := readLine(r.toListener, maxIntroBytes); err != nil { // B's welcome
		t.Fatalf("B's welcome: %v", err)
	}
	if _, err := readLine(r.toListener, maxProofBytes); err != nil { // B's proof
		t.Fatalf("B's proof: %v", err)
	}
	mWelcome, err := m.local.stamp(testIntro(), RoleListener)
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	if err := sendIntroAndProof(newSessionConn(r.toDialer), m.local, mWelcome, RoleListener, exporterOf(t, r.toDialer)); err != nil {
		t.Fatalf("M proving itself to X: %v", err)
	}
	r.forward(t, r.toDialer, r.toListener, 2) // X's hello and proof, to B as if M were X

	if got := <-bResult; !errors.Is(got.err, ErrProofInvalid) {
		t.Fatalf("B accepting X's relayed proof = %v, want ErrProofInvalid", got.err)
	}
	if got := <-xResult; got.err != nil {
		t.Fatalf("X's address dial to M (M proved itself honestly): %v", got.err)
	}

	// The same relay under v1: B's challenge, signed by X for M, verifies at B.
	challengeFromB := "challenge-issued-by-B"
	signedByX := identity.SignPayload(x.id, protocol.SessionAuthPayload(challengeFromB, x.id.Address))
	if err := identity.VerifyPayload(x.id.Address, identity.PublicKeyBase64(x.id.PublicKey),
		protocol.SessionAuthPayload(challengeFromB, x.id.Address), signedByX); err != nil {
		t.Fatalf("the v1 contrast no longer holds (%v): the test no longer shows what v2 fixes", err)
	}
}

func exporterOf(t *testing.T, conn *tls.Conn) []byte {
	t.Helper()
	state := conn.ConnectionState()
	exporter, err := state.ExportKeyingMaterial(ExporterLabel, nil, ExporterLength)
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}
	return exporter
}

// fakeListener completes a real v2 TLS handshake and then writes whatever
// lines the test scripts, so a dialer can be fed a malformed or forged intro.
func fakeListener(t *testing.T, conn net.Conn, certs CertificateSource, script func(*tls.Conn, []byte)) {
	t.Helper()
	go func() {
		server := tls.Server(conn, listenerConfig(certs))
		if err := server.Handshake(); err != nil {
			return
		}
		state := server.ConnectionState()
		exporter, _ := state.ExportKeyingMaterial(ExporterLabel, nil, ExporterLength)
		script(server, exporter)
		_, _ = io.Copy(io.Discard, server)
	}()
}

func writeLines(conn *tls.Conn, lines ...[]byte) {
	for _, line := range lines {
		_, _ = conn.Write(append(line, '\n'))
	}
}

func welcomeOf(t *testing.T, n node) protocol.Frame {
	t.Helper()
	intro, err := n.local.stamp(testIntro(), RoleListener)
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	return intro
}

func frameLine(t *testing.T, frame protocol.Frame) []byte {
	t.Helper()
	line, err := protocol.MarshalFrameLineBytes(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return []byte(string(line[:len(line)-1]))
}

func proofLine(t *testing.T, private ed25519.PrivateKey, role Role, exporter []byte) []byte {
	t.Helper()
	line, err := marshalProofFrame(ed25519.Sign(private, proofPayload(testNetwork, role, exporter)))
	if err != nil {
		t.Fatalf("proof: %v", err)
	}
	return line
}

func dialScripted(t *testing.T, x node, expect ExpectedPeer, script func(*tls.Conn, []byte)) error {
	t.Helper()
	xSide, lSide := connPair(t)
	fakeListener(t, lSide, newNode(t).certs, script)
	return (<-dialAsync(context.Background(), xSide, x, expect)).err
}

func TestTheDialerRefusesWhatTheContractRefuses(t *testing.T) {
	x, b := newNode(t), newNode(t)
	// The neutral point with R = neutral, S = 0 verifies for EVERY message
	// under the permissive stdlib verifier (pinned in identity by
	// TestStdlibAcceptsTheUniversalSignature): such a "key" forges the box
	// binding and the proof alike. Only identity.ParsePublicKey stops it.
	neutralKey := edforgery.NeutralPublicKey()
	forgedSignature := edforgery.UniversalSignature()
	smallOrder := welcomeOf(t, b)
	smallOrder.PubKey = base64.StdEncoding.EncodeToString(neutralKey)
	smallOrder.Address = identity.Fingerprint(neutralKey)
	smallOrder.BoxSig = base64.RawURLEncoding.EncodeToString(forgedSignature)
	forgedProof, err := marshalProofFrame(forgedSignature)
	if err != nil {
		t.Fatalf("forged proof: %v", err)
	}

	withoutBoxSig := welcomeOf(t, b)
	withoutBoxSig.BoxSig = ""
	withChallenge := welcomeOf(t, b)
	withChallenge.Challenge = "v1"
	otherNetwork := welcomeOf(t, b)
	otherNetwork.Network = "gazeta-mainnet"
	asHello := welcomeOf(t, b)
	asHello.Type = "hello"

	cases := map[string]struct {
		expect ExpectedPeer
		script func(*tls.Conn, []byte)
		want   error
	}{
		"a small-order key forging every signature (#17)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, smallOrder), forgedProof)
		}, identity.ErrPublicKeySmallOrder},
		"a v2 field removed (#7)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, withoutBoxSig), proofLine(t, b.id.PrivateKey, RoleListener, e))
		}, ErrIntro},
		"v1 challenge in a v2 intro": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, withChallenge), proofLine(t, b.id.PrivateKey, RoleListener, e))
		}, ErrIntro},
		"another network": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, otherNetwork), proofLine(t, b.id.PrivateKey, RoleListener, e))
		}, ErrIntro},
		"the dialer's frame type": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, asHello), proofLine(t, b.id.PrivateKey, RoleListener, e))
		}, ErrIntro},
		"another frame before the proof (#4.4)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, b)), []byte(`{"type":"ping"}`))
		}, ErrProofFrame},
		"the proof in the dialer's role — reflection (#9)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, b)), proofLine(t, b.id.PrivateKey, RoleDialer, e))
		}, ErrProofInvalid},
		"a proof over another exporter (#8)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, b)), proofLine(t, b.id.PrivateKey, RoleListener, byteRun(0x55, ExporterLength)))
		}, ErrProofInvalid},
		// KCI (#18): M holds X's own key and claims to be B. Knowing the
		// dialer's key lets M sign only as X — never as B.
		"key compromise impersonation (#18)": {ExpectPeer(b.peer), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, b)), proofLine(t, x.id.PrivateKey, RoleListener, e))
		}, ErrProofInvalid},
		"self-connection (#17)": {AnyPeer(), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, x)), proofLine(t, x.id.PrivateKey, RoleListener, e))
		}, ErrSelfConnection},
		"someone other than the dialled identity": {ExpectPeer(newNode(t).peer), func(c *tls.Conn, e []byte) {
			writeLines(c, frameLine(t, welcomeOf(t, b)), proofLine(t, b.id.PrivateKey, RoleListener, e))
		}, ErrPeerMismatch},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := dialScripted(t, x, tc.expect, tc.script); !errors.Is(err, tc.want) {
				t.Fatalf("Dial = %v, want %v", err, tc.want)
			}
		})
	}
}

// #6: no ALPN on either side is not v2, whatever else the handshake does.
func TestNoALPNIsNotV2(t *testing.T) {
	ctx := context.Background()
	x, b := newNode(t), newNode(t)

	t.Run("listener without ALPN", func(t *testing.T) {
		xSide, lSide := connPair(t)
		go func() {
			config := listenerConfig(b.certs)
			config.NextProtos = nil
			_ = tls.Server(lSide, config).Handshake()
		}()
		if got := <-dialAsync(ctx, xSide, x, AnyPeer()); !errors.Is(got.err, ErrNotV2) {
			t.Fatalf("Dial = %v, want ErrNotV2", got.err)
		}
	})
	t.Run("dialer without ALPN", func(t *testing.T) {
		dSide, bSide := connPair(t)
		accepted := acceptAsync(ctx, bSide, b)
		go func() {
			config := dialerConfig()
			config.NextProtos = nil
			client := tls.Client(dSide, config)
			_ = client.Handshake()
			_, _ = io.Copy(io.Discard, client)
		}()
		if got := <-accepted; !errors.Is(got.err, ErrNotV2) {
			t.Fatalf("Accept = %v, want ErrNotV2", got.err)
		}
	})
}

// tamperConn alters this end's records once armed: a flipped bit, or a
// record sent twice.
type tamperConn struct {
	net.Conn
	mode chan string
}

func (c *tamperConn) Write(b []byte) (int, error) {
	select {
	case mode := <-c.mode:
		switch mode {
		case "flip":
			altered := append([]byte(nil), b...)
			altered[len(altered)-1] ^= 0x01
			return c.Conn.Write(altered)
		case "duplicate":
			if _, err := c.Conn.Write(b); err != nil {
				return 0, err
			}
			return c.Conn.Write(b)
		}
	default:
	}
	return c.Conn.Write(b)
}

// #10: an altered or replayed record after the handshake breaks the session.
func TestAnAlteredOrReplayedRecordBreaksTheSession(t *testing.T) {
	for _, mode := range []string{"flip", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			x, b := newNode(t), newNode(t)
			xRaw, bSide := connPair(t)
			xSide := &tamperConn{Conn: xRaw, mode: make(chan string, 1)}
			accepted := acceptAsync(ctx, bSide, b)
			dialed := <-dialAsync(ctx, xSide, x, ExpectPeer(b.peer))
			got := <-accepted
			if dialed.err != nil || got.err != nil {
				t.Fatalf("dial %v, accept %v", dialed.err, got.err)
			}
			xConn, _ := dialed.session.Conn()
			bConn, _ := got.session.Conn()
			xSide.mode <- mode
			go func() { _, _ = xConn.Write([]byte("one\n")); _, _ = xConn.Write([]byte("two\n")) }()

			_ = bConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			var lines []string
			var readErr error
			for readErr == nil {
				var line []byte
				if line, readErr = readLine(bConn, 64); readErr == nil {
					lines = append(lines, string(line))
				}
			}
			var timeout net.Error
			if errors.As(readErr, &timeout) && timeout.Timeout() {
				t.Fatalf("reading ended by a timeout, not by the record check: the tampered record never arrived (%v)", readErr)
			}
			want := map[string][]string{"flip": nil, "duplicate": {"one"}}[mode]
			if !slices.Equal(lines, want) {
				t.Fatalf("read %q, want %q: a tampered record was accepted", lines, want)
			}
		})
	}
}

// #30: past the record budget a write is refused instead of sent, and the
// caller re-establishes the session.
func TestWritesPastTheRecordBudgetAreRefused(t *testing.T) {
	ctx := context.Background()
	x, b := newNode(t), newNode(t)
	xSide, bSide := connPair(t)
	accepted := acceptAsync(ctx, bSide, b)
	dialed := <-dialAsync(ctx, xSide, x, AnyPeer())
	got := <-accepted
	if dialed.err != nil || got.err != nil {
		t.Fatalf("dial %v, accept %v", dialed.err, got.err)
	}
	conn := dialed.session.state.conn
	conn.limit = conn.records.Load() + 1
	bConn, _ := got.session.Conn()
	go func() { _, _ = io.Copy(io.Discard, bConn) }()
	if _, err := conn.Write([]byte("last\n")); err != nil {
		t.Fatalf("the last write inside the budget: %v", err)
	}
	if _, err := conn.Write([]byte("over\n")); !errors.Is(err, ErrRotationDue) {
		t.Fatalf("write past the budget = %v, want ErrRotationDue", err)
	}
}

func TestAZeroProvenSessionRefusesEveryQuestion(t *testing.T) {
	var zero ProvenSession
	if _, err := zero.Peer(); !errors.Is(err, ErrZeroSession) {
		t.Errorf("Peer = %v", err)
	}
	if _, err := zero.Role(); !errors.Is(err, ErrZeroSession) {
		t.Errorf("Role = %v", err)
	}
	if _, err := zero.Conn(); !errors.Is(err, ErrZeroSession) {
		t.Errorf("Conn = %v", err)
	}
}

func TestACancelledContextStopsTheHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	x := newNode(t)
	xSide, other := connPair(t)
	defer func() { _ = other.Close() }()
	result := dialAsync(ctx, xSide, x, AnyPeer())
	cancel()
	select {
	case got := <-result:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("Dial = %v, want context.Canceled", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled handshake kept waiting")
	}
}

// Concurrent writers share one record budget: the charge and the check are
// one atomic step, so no write slips past the limit and -race stays quiet.
func TestConcurrentWritersShareOneRecordBudget(t *testing.T) {
	ctx := context.Background()
	x, b := newNode(t), newNode(t)
	xSide, bSide := connPair(t)
	accepted := acceptAsync(ctx, bSide, b)
	dialed := <-dialAsync(ctx, xSide, x, AnyPeer())
	got := <-accepted
	if dialed.err != nil || got.err != nil {
		t.Fatalf("dial %v, accept %v", dialed.err, got.err)
	}
	bConn, _ := got.session.Conn()
	go func() { _, _ = io.Copy(io.Discard, bConn) }()
	conn := dialed.session.state.conn
	const budget = 50
	conn.limit = conn.records.Load() + budget

	var wg sync.WaitGroup
	var accepted64 atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if _, err := conn.Write([]byte("x\n")); err == nil {
					accepted64.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := accepted64.Load(); got != budget {
		t.Fatalf("%d writes accepted, want exactly the budget of %d", got, budget)
	}
}

// A v1 node answers a ClientHello with a JSON error line. The dial reports
// that as ErrPeerSpeaksV1 — the one refusal a caller may follow with a v1
// dial — and a silent peer as something else.
func TestADialAnsweredByV1IsRecognisedAndSilenceIsNot(t *testing.T) {
	x := newNode(t)
	t.Run("v1 answer", func(t *testing.T) {
		xSide, oldNode := connPair(t)
		go func() {
			buf := make([]byte, 1)
			_, _ = oldNode.Read(buf)
			_, _ = oldNode.Write([]byte(`{"type":"error","code":"invalid-json"}` + "\n"))
			_ = oldNode.Close()
		}()
		if got := <-dialAsync(context.Background(), xSide, x, AnyPeer()); !errors.Is(got.err, ErrPeerSpeaksV1) {
			t.Fatalf("Dial to a v1 node = %v, want ErrPeerSpeaksV1", got.err)
		}
	})
	// Only a v1 line may open the way to v1. A TLS alert before ServerHello
	// is a TLS refusal (a v2 failure), and arbitrary bytes are not a v1 node:
	// neither may become ErrPeerSpeaksV1.
	for name, answer := range map[string][]byte{
		"TLS alert":       {0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x50},
		"arbitrary bytes": []byte("HTTP/1.1 400 Bad Request\r\n\r\n"),
	} {
		t.Run(name, func(t *testing.T) {
			xSide, other := connPair(t)
			go func() {
				buf := make([]byte, 1)
				_, _ = other.Read(buf)
				_, _ = other.Write(answer)
				_ = other.Close()
			}()
			got := <-dialAsync(context.Background(), xSide, x, AnyPeer())
			if got.err == nil || errors.Is(got.err, ErrPeerSpeaksV1) {
				t.Fatalf("Dial answered with %s = %v, want a v2 failure that is not ErrPeerSpeaksV1", name, got.err)
			}
		})
	}
	t.Run("silence", func(t *testing.T) {
		xSide, silent := connPair(t)
		go func() { _, _ = io.Copy(io.Discard, silent) }()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if got := <-dialAsync(ctx, xSide, x, AnyPeer()); got.err == nil || errors.Is(got.err, ErrPeerSpeaksV1) {
			t.Fatalf("Dial to a silent peer = %v, want an error that is not ErrPeerSpeaksV1", got.err)
		}
	})
}

// countingConn counts the bytes read through it.
type countingConn struct {
	net.Conn
	read atomic.Int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.read.Add(int64(n))
	return n, err
}

// Closing a session puts nothing more on the wire. A TLS close_notify would
// be a record the peer may never read — it often has stopped reading by
// then — and those bytes would be counted as sent by one end and received
// by none, breaking the exact byte accounting of the node.
func TestClosingASessionSendsNoCloseNotify(t *testing.T) {
	ctx := context.Background()
	x, b := newNode(t), newNode(t)
	xSide, bRaw := connPair(t)
	bSide := &countingConn{Conn: bRaw}
	accepted := acceptAsync(ctx, bSide, b)
	dialed := <-dialAsync(ctx, xSide, x, AnyPeer())
	got := <-accepted
	if dialed.err != nil || got.err != nil {
		t.Fatalf("dial %v, accept %v", dialed.err, got.err)
	}
	xConn, _ := dialed.session.Conn()
	bConn, _ := got.session.Conn()
	before := bSide.read.Load()
	if err := xConn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_ = bConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := bConn.Read(make([]byte, 64)); err == nil {
		t.Fatal("read after the peer closed returned data")
	}
	if after := bSide.read.Load(); after != before {
		t.Fatalf("closing sent %d bytes after the last frame, want none", after-before)
	}
}
