package node

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// The one-shot service dials — the syncPeer recovery dial and the push_notice
// fallback in sendNoticeToPeer — never become sessions, but they send
// commands (get_peers, fetch_contacts, push_notice) that a responder serves
// only to an authenticated connection. Every responder this node still talks
// to (MinimumProtocolVersion and up) answers a hello carrying identity fields
// with a challenge, and refuses everything but handshake commands until
// auth_session verifies. So a welcome without a challenge, a welcome below the
// floor and an error in place of the welcome all end the dial at the welcome:
// the responder hears nothing after our hello, and we import nothing from it.

// recordFramesAfterWelcome accepts one dial on ln, answers its hello with
// welcome and returns the type of every frame the dialler sent, hello
// included, until the dialler closes. auth_session gets authAnswer; every
// other frame gets the terminal answer a current responder gives an
// unauthenticated connection (auth-required), so a dialler that goes on past
// the welcome fails fast instead of waiting out its deadline — and is
// recorded doing so.
func recordFramesAfterWelcome(ln net.Listener, welcome, authAnswer protocol.Frame) ([]string, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	hello, err := readTestFrameLine(reader)
	if err != nil {
		return nil, err
	}
	received := []string{hello.Type}
	welcomeLine, err := protocol.MarshalFrameLine(welcome)
	if err != nil {
		return received, err
	}
	if _, err := io.WriteString(conn, welcomeLine); err != nil {
		return received, err
	}
	for {
		frame, err := readTestFrameLine(reader)
		if err != nil {
			return received, nil
		}
		received = append(received, frame.Type)
		reply := protocol.Frame{Type: "error", Code: protocol.ErrCodeAuthRequired}
		if frame.Type == "auth_session" {
			reply = authAnswer
		}
		replyLine, err := protocol.MarshalFrameLine(reply)
		if err != nil {
			return received, err
		}
		if _, err := io.WriteString(conn, replyLine); err != nil {
			return received, nil
		}
	}
}

// welcomeWithoutChallenge is the welcome of a responder at the current
// version that does not authenticate the dialler.
func welcomeWithoutChallenge(responder *identity.Identity) protocol.Frame {
	return protocol.Frame{
		Type:                   "welcome",
		Version:                config.ProtocolVersion,
		MinimumProtocolVersion: config.MinimumProtocolVersion,
		Address:                responder.Address,
	}
}

// welcomeBelowFloor is a complete, challenging welcome from a responder whose
// protocol version is below this node's MinimumProtocolVersion.
func welcomeBelowFloor(responder *identity.Identity) protocol.Frame {
	return protocol.Frame{
		Type:                   "welcome",
		Version:                config.MinimumProtocolVersion - 1,
		MinimumProtocolVersion: config.MinimumProtocolVersion - 1,
		ClientVersion:          "0.0.1-below-floor",
		Address:                responder.Address,
		Challenge:              "below-floor-challenge",
	}
}

func newDialWelcomeResponder(t *testing.T) *identity.Identity {
	t.Helper()
	responder, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	return responder
}

func listenForDialWelcome(t *testing.T) (net.Listener, domain.PeerAddress) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln, domain.PeerAddress(ln.Addr().String())
}

type recordedDial struct {
	received []string
	err      error
}

func serveRecordedDial(ln net.Listener, welcome protocol.Frame) <-chan recordedDial {
	return serveRecordedDialAnsweringAuth(ln, welcome, protocol.Frame{Type: "auth_ok"})
}

func serveRecordedDialAnsweringAuth(ln net.Listener, welcome, authAnswer protocol.Frame) <-chan recordedDial {
	done := make(chan recordedDial, 1)
	go func() {
		received, err := recordFramesAfterWelcome(ln, welcome, authAnswer)
		done <- recordedDial{received: received, err: err}
	}()
	return done
}

func awaitOnlyHello(t *testing.T, done <-chan recordedDial) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("scripted responder: %v", got.err)
		}
		if len(got.received) != 1 || got.received[0] != "hello" {
			t.Fatalf("the responder heard %v after its welcome, want only the hello", got.received)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the dial did not end")
	}
}

func assertIncompatibleWelcomePenalised(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	health := svc.health[svc.resolveHealthAddress(address)]
	if health == nil {
		t.Fatal("no health record: a welcome below the floor was not penalised")
	}
	if health.LastErrorCode != protocol.ErrCodeIncompatibleProtocol {
		t.Fatalf("LastErrorCode = %q, want %q", health.LastErrorCode, protocol.ErrCodeIncompatibleProtocol)
	}
	if health.IncompatibleVersionAttempts != 1 {
		t.Fatalf("IncompatibleVersionAttempts = %d, want 1", health.IncompatibleVersionAttempts)
	}
	if want := domain.ProtocolVersion(config.MinimumProtocolVersion - 1); health.ObservedPeerVersion != want {
		t.Fatalf("ObservedPeerVersion = %d, want %d", health.ObservedPeerVersion, want)
	}
	if got := svc.peerVersions[svc.resolveHealthAddress(address)]; got != "0.0.1-below-floor" {
		t.Fatalf("peerVersions = %q, want the refused welcome's client version", got)
	}
}

func syncPeerAgainstWelcome(t *testing.T, welcome func(*identity.Identity) protocol.Frame) (*Service, domain.PeerAddress) {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, welcome(newDialWelcomeResponder(t)))

	markOldNode(svc, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// requestPeers=true: neither get_peers nor fetch_contacts may follow.
	if imported := svc.syncPeer(ctx, address, true); imported != 0 {
		t.Fatalf("syncPeer imported %d contacts from a refused welcome", imported)
	}
	awaitOnlyHello(t, done)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
	return svc, address
}

func TestSyncPeer_WelcomeWithoutChallenge_SendsOnlyHello(t *testing.T) {
	t.Parallel()
	syncPeerAgainstWelcome(t, welcomeWithoutChallenge)
}

func TestSyncPeer_WelcomeBelowFloor_SendsOnlyHelloAndPenalises(t *testing.T) {
	t.Parallel()
	svc, address := syncPeerAgainstWelcome(t, welcomeBelowFloor)
	assertIncompatibleWelcomePenalised(t, svc, address)
}

func TestSyncPeer_ErrorInPlaceOfWelcome_SendsOnlyHello(t *testing.T) {
	t.Parallel()
	syncPeerAgainstWelcome(t, func(*identity.Identity) protocol.Frame {
		return protocol.Frame{
			Type:                   "error",
			Code:                   protocol.ErrCodeIncompatibleProtocol,
			Version:                config.ProtocolVersion,
			MinimumProtocolVersion: config.MinimumProtocolVersion,
		}
	})
}

func noticeDialAgainstWelcome(t *testing.T, welcome func(*identity.Identity) protocol.Frame) (*Service, domain.PeerAddress) {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, welcome(newDialWelcomeResponder(t)))

	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "gated-notice-ciphertext")
	awaitOnlyHello(t, done)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
	return svc, address
}

func TestSendNoticeToPeer_WelcomeWithoutChallenge_SendsOnlyHello(t *testing.T) {
	t.Parallel()
	noticeDialAgainstWelcome(t, welcomeWithoutChallenge)
}

func TestSendNoticeToPeer_WelcomeBelowFloor_SendsOnlyHelloAndPenalises(t *testing.T) {
	t.Parallel()
	svc, address := noticeDialAgainstWelcome(t, welcomeBelowFloor)
	assertIncompatibleWelcomePenalised(t, svc, address)
}

// TestAdmitServiceDialWelcome_RefusesWithTypedErrors pins what the shared
// gate of the one-shot dials answers for each welcome, by type: the callers
// end the dial on any error, and errors.Is / errors.As is how a caller or a
// test tells the outcomes apart.
func TestAdmitServiceDialWelcome_RefusesWithTypedErrors(t *testing.T) {
	t.Parallel()
	responder := newDialWelcomeResponder(t)
	withChallenge := func(frame protocol.Frame) protocol.Frame {
		frame.Challenge = "gate-challenge"
		return frame
	}
	cases := []struct {
		name        string
		welcome     func(svc *Service) protocol.Frame
		proven      bool
		pinned      bool
		want        func(error) bool
		wantPenalty bool
	}{
		{
			name:    "v1 welcome with a challenge is admitted",
			welcome: func(*Service) protocol.Frame { return withChallenge(welcomeWithoutChallenge(responder)) },
			want:    func(err error) bool { return err == nil },
		},
		{
			name:    "v2 intro needs no challenge",
			welcome: func(*Service) protocol.Frame { return welcomeWithoutChallenge(responder) },
			proven:  true,
			want:    func(err error) bool { return err == nil },
		},
		{
			name:        "v1 welcome below the floor",
			welcome:     func(*Service) protocol.Frame { return welcomeBelowFloor(responder) },
			want:        func(err error) bool { return errors.Is(err, errIncompatibleProtocol) },
			wantPenalty: true,
		},
		{
			// The floor outranks the challenge: the responder is charged
			// whether or not it challenged.
			name: "v1 welcome below the floor without a challenge",
			welcome: func(*Service) protocol.Frame {
				welcome := welcomeBelowFloor(responder)
				welcome.Challenge = ""
				return welcome
			},
			want:        func(err error) bool { return errors.Is(err, errIncompatibleProtocol) },
			wantPenalty: true,
		},
		{
			// Security property: a v1 welcome naming an identity pinned to
			// v2 may come from a man in the middle, so it is refused BEFORE
			// the floor — a stranger cannot charge the real peer's record.
			name:    "v1 welcome below the floor naming an identity pinned to v2",
			welcome: func(*Service) protocol.Frame { return welcomeBelowFloor(responder) },
			pinned:  true,
			want:    func(err error) bool { return errors.Is(err, errLegacyRefusedPinned) },
		},
		{
			name: "v2 intro below the floor",
			welcome: func(*Service) protocol.Frame {
				intro := welcomeBelowFloor(responder)
				intro.Challenge = ""
				return intro
			},
			proven:      true,
			want:        func(err error) bool { return errors.Is(err, errIncompatibleProtocol) },
			wantPenalty: true,
		},
		{
			name:    "an answer that is not a welcome",
			welcome: func(*Service) protocol.Frame { return notAWelcome(responder) },
			want:    func(err error) bool { return errors.Is(err, errNotAWelcome) },
		},
		{
			name: "an error in place of the welcome",
			welcome: func(*Service) protocol.Frame {
				return protocol.Frame{Type: "error", Code: protocol.ErrCodeIncompatibleProtocol}
			},
			want: func(err error) bool { return errors.Is(err, protocol.ErrIncompatibleProtocol) },
		},
		{
			name: "a connection_notice in place of the welcome",
			welcome: func(*Service) protocol.Frame {
				return protocol.Frame{Type: protocol.FrameTypeConnectionNotice, Code: protocol.ErrCodePeerBanned}
			},
			want: func(err error) bool { return errors.Is(err, errServiceDialNotice) },
		},
		{
			name: "our own identity",
			welcome: func(svc *Service) protocol.Frame {
				own := withChallenge(welcomeWithoutChallenge(responder))
				own.Address = svc.identity.Address
				return own
			},
			want: func(err error) bool {
				var selfErr *selfIdentityError
				return errors.As(err, &selfErr)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := newTestService(t, config.NodeTypeFull)
			if tc.pinned {
				if err := svc.secureSessions.store.noteProvenInbound(domain.PeerIdentityFromWire(responder.Address)); err != nil {
					t.Fatalf("pin: %v", err)
				}
			}
			address := domain.PeerAddress("203.0.113.20:64646")
			err := svc.admitServiceDialWelcome(serviceDialNotice, address, tc.welcome(svc), tc.proven)
			if !tc.want(err) {
				t.Fatalf("admitServiceDialWelcome = %v", err)
			}
			svc.peerMu.RLock()
			health := svc.health[address]
			svc.peerMu.RUnlock()
			penalised := health != nil && health.IncompatibleVersionAttempts > 0
			if penalised != tc.wantPenalty {
				t.Fatalf("penalised = %v, want %v", penalised, tc.wantPenalty)
			}
		})
	}
}

// serveV2Intro accepts one dial on ln as a v2 listener proving responder and
// announcing intro, then records the type of every frame the dialler sends
// inside the proven session until it closes. A v2 dial writes nothing outside
// the handshake before its first command, so an empty record means the dialler
// stopped at the intro. push_notice gets the answer a node gives it, so a
// dialler that does go on is not kept waiting for its deadline.
func serveV2Intro(ln net.Listener, responder *identity.Identity, intro protocol.Frame) <-chan recordedDial {
	done := make(chan recordedDial, 1)
	go func() {
		received, err := recordFramesAfterV2Intro(ln, responder, intro)
		done <- recordedDial{received: received, err: err}
	}()
	return done
}

func recordFramesAfterV2Intro(ln net.Listener, responder *identity.Identity, intro protocol.Frame) ([]string, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	local, err := sessionv2.NewLocal(responder, domain.NetworkID(networkName))
	if err != nil {
		return nil, err
	}
	certificates, err := sessionv2.NewCertificateSource(time.Now)
	if err != nil {
		return nil, err
	}
	session, err := sessionv2.Accept(context.Background(), conn, local, intro, certificates, sessionv2.DefaultTimeouts)
	if err != nil {
		return nil, err
	}
	wire, err := session.Conn()
	if err != nil {
		return nil, err
	}
	defer func() { _ = wire.Close() }()
	reader := bufio.NewReader(wire)
	received := []string{}
	for {
		frame, err := readTestFrameLine(reader)
		if err != nil {
			return received, nil
		}
		received = append(received, frame.Type)
		if err := writeTestFrameLine(wire, protocol.Frame{Type: "ok"}); err != nil {
			return received, nil
		}
	}
}

// v2IntroBelowFloor is the v2 listener intro of a node below this node's
// MinimumProtocolVersion; the identity fields are stamped by the handshake.
func v2IntroBelowFloor() protocol.Frame {
	return protocol.Frame{
		Version:                config.MinimumProtocolVersion - 1,
		MinimumProtocolVersion: config.MinimumProtocolVersion - 1,
		ClientVersion:          "0.0.1-below-floor",
	}
}

func awaitNothingAfterV2Intro(t *testing.T, done <-chan recordedDial) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("v2 listener: %v", got.err)
		}
		if len(got.received) != 0 {
			t.Fatalf("the listener received %v inside the proven session, want nothing", got.received)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dial did not end")
	}
}

func assertNotRecordedAsOutboundSuccess(t *testing.T, svc *Service, address domain.PeerAddress) {
	t.Helper()
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if pm := svc.persistedMeta[address]; pm != nil && pm.TrustedAdvertiseSource == trustedAdvertiseSourceOutbound {
		t.Fatal("a refused dial was recorded as an outbound auth success")
	}
}

func TestSyncPeer_V2IntroBelowFloor_SendsNothingAndPenalises(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), v2IntroBelowFloor())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if imported := svc.syncPeer(ctx, address, true); imported != 0 {
		t.Fatalf("syncPeer imported %d contacts from a listener below the floor", imported)
	}
	awaitNothingAfterV2Intro(t, done)
	assertIncompatibleWelcomePenalised(t, svc, address)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

func TestSendNoticeToPeer_V2IntroBelowFloor_WritesNothingAndPenalises(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveV2Intro(ln, newDialWelcomeResponder(t), v2IntroBelowFloor())

	svc.sendNoticeToPeer(address, time.Minute, "gated-notice-ciphertext")
	awaitNothingAfterV2Intro(t, done)
	assertIncompatibleWelcomePenalised(t, svc, address)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

// notAWelcome is a v1 answer to hello that is neither a welcome, an error
// nor a connection_notice — and otherwise carries everything a welcome the
// dialler accepts would (current version, identity, challenge), so only the
// frame type can stop the dial.
func notAWelcome(responder *identity.Identity) protocol.Frame {
	answer := welcomeWithoutChallenge(responder)
	answer.Type = "peers"
	answer.Challenge = "not-a-welcome-challenge"
	answer.Peers = []string{"198.51.100.9:64646"}
	return answer
}

func TestSyncPeer_AnswerThatIsNotAWelcome_SendsOnlyHello(t *testing.T) {
	t.Parallel()
	syncPeerAgainstWelcome(t, notAWelcome)
}

func TestSendNoticeToPeer_AnswerThatIsNotAWelcome_SendsOnlyHello(t *testing.T) {
	t.Parallel()
	noticeDialAgainstWelcome(t, notAWelcome)
}

// currentWelcome is the welcome of a responder at the current version that
// challenges the dialler, so the gate admits it and auth_session follows.
func currentWelcome(responder *identity.Identity) protocol.Frame {
	welcome := welcomeWithoutChallenge(responder)
	welcome.Challenge = "admitted-challenge"
	return welcome
}

// refusedAuth is a responder's verdict on an auth_session it did not accept.
var refusedAuth = protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidAuthSignature}

// A dial the gate admitted is still not a success until auth_ok: a responder
// that refuses auth_session must leave no outbound-confirmed trace, and must
// hear nothing after it.
func TestSyncPeer_AuthRefused_NotRecordedAsOutboundSuccess(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDialAnsweringAuth(ln, currentWelcome(newDialWelcomeResponder(t)), refusedAuth)

	markOldNode(svc, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if imported := svc.syncPeer(ctx, address, true); imported != 0 {
		t.Fatalf("syncPeer imported %d contacts after a refused auth", imported)
	}
	awaitHelloAndAuthOnly(t, done)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

func TestSendNoticeToPeer_AuthRefused_NotRecordedAsOutboundSuccess(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDialAnsweringAuth(ln, currentWelcome(newDialWelcomeResponder(t)), refusedAuth)

	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "gated-notice-ciphertext")
	awaitHelloAndAuthOnly(t, done)
	assertNotRecordedAsOutboundSuccess(t, svc, address)
}

func awaitHelloAndAuthOnly(t *testing.T, done <-chan recordedDial) {
	t.Helper()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("scripted responder: %v", got.err)
		}
		if len(got.received) != 2 || got.received[0] != "hello" || got.received[1] != "auth_session" {
			t.Fatalf("the responder heard %v, want hello and auth_session only", got.received)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the dial did not end")
	}
}

// serveNoticeWithAuthBurst answers a notice dial as a loaded current responder
// does: auth-time side effects (pending-ring flush, route sync, announce
// baseline) reach the dialler BEFORE auth_ok. Returns the frames the dialler
// sent after its auth_session.
func serveNoticeWithAuthBurst(ln net.Listener, burst int) ([]string, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	responder, err := newV1HandshakeResponder()
	if err != nil {
		return nil, err
	}
	if _, err := responder.answerHello(conn, reader); err != nil {
		return nil, err
	}
	authSession, err := readTestFrameLine(reader)
	if err != nil {
		return nil, err
	}
	interleaved := []protocol.Frame{{Type: "push_delivery_receipt"}, {Type: "announce_routes"}}
	for i := 0; i < burst; i++ {
		if err := writeTestFrameLine(conn, interleaved[i%len(interleaved)]); err != nil {
			return nil, err
		}
	}
	if err := writeTestFrameLine(conn, responder.authReply(authSession)); err != nil {
		return nil, err
	}
	received := []string{}
	for {
		frame, err := readTestFrameLine(reader)
		if err != nil {
			return received, nil
		}
		received = append(received, frame.Type)
		if err := writeTestFrameLine(conn, protocol.Frame{Type: "ok"}); err != nil {
			return received, nil
		}
	}
}

// The notice dial waits for auth_ok the way the recovery dial does: frames the
// responder flushes ahead of it are skipped, not taken for a refusal.
func TestSendNoticeToPeer_FramesAheadOfAuthOK_StillDeliversTheNotice(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	type result struct {
		received []string
		err      error
	}
	done := make(chan result, 1)
	go func() {
		received, err := serveNoticeWithAuthBurst(ln, 64)
		done <- result{received, err}
	}()

	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "burst-notice-ciphertext")
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("scripted responder: %v", got.err)
		}
		if len(got.received) != 1 || got.received[0] != "push_notice" {
			t.Fatalf("after auth_ok the responder heard %v, want the push_notice", got.received)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the dial did not end")
	}
}

// A notice to an address under a running ban is not worth a socket: the
// connection manager would not dial it either, and the dial could only end
// at the welcome again.
func TestSendNoticeToPeer_AddressUnderBan_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, currentWelcome(newDialWelcomeResponder(t)))
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	svc.peerMu.Lock()
	svc.ensurePeerHealthLocked(address).BannedUntil = time.Now().Add(time.Hour)
	svc.peerMu.Unlock()
	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "banned-notice-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a banned address was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}

// banSiblingPortForIncompatibleVersion arms the incompatible-version ban on
// another port of address's host, the way repeated old-version welcomes do:
// the ban lands on that port and is widened to the whole IP.
func banSiblingPortForIncompatibleVersion(t *testing.T, svc *Service, address domain.PeerAddress) domain.PeerAddress {
	t.Helper()
	host, _, ok := splitHostPort(string(address))
	if !ok {
		t.Fatalf("splitHostPort(%q) failed", address)
	}
	sibling := domain.PeerAddress(net.JoinHostPort(host, "1"))
	oldVersion := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	for range peerBanThresholdIncompatible / peerBanIncrementIncompatible {
		svc.penalizeOldProtocolPeer(sibling, oldVersion, oldVersion, "")
	}
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if health := svc.health[sibling]; health == nil || !time.Now().Before(health.BannedUntil) {
		t.Fatalf("fixture: the sibling port %s is not under an incompatible-version ban", sibling)
	}
	return sibling
}

// A notice to another port of a host banned for an incompatible version is
// not dialled either: the connection manager's candidate filter drops every
// port of an IP-wide ban, and the old client answers on all of them.
func TestSendNoticeToPeer_HostBannedForIncompatibleVersion_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, currentWelcome(newDialWelcomeResponder(t)))
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	banSiblingPortForIncompatibleVersion(t, svc, address)
	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "ip-banned-notice-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a port of an IP-wide banned host was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}

// The self-identity cooldown is address-scoped and never widens to the IP
// (buildBannedIPsSet carve-out): a sibling port of a self-alias is a
// different node behind the same host, and its notice is still dialled.
func TestSendNoticeToPeer_SiblingOfSelfIdentityAlias_IsStillDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, currentWelcome(newDialWelcomeResponder(t)))
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	host, _, _ := splitHostPort(string(address))
	alias := domain.PeerAddress(net.JoinHostPort(host, "1"))
	svc.peerProvider.Add(alias, domain.PeerSourceAnnounce)
	svc.applySelfIdentityCooldown(alias, svc.newSelfIdentityError(alias))
	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "self-alias-sibling-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent == 0 {
		t.Fatal("the sibling port of a self-identity alias was not dialled: the address-scoped cooldown widened to the IP")
	}
}

// The v1 challenge has one check, at the start of authentication, shared
// with the peer sessions (authSessionFor): a welcome without one is refused
// before anything is signed or written, so the dial ends with nothing past
// its hello.
func TestAuthenticateServiceDial_WelcomeWithoutChallenge_WritesNothing(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	var written []string
	writeLine := func(line string) error {
		written = append(written, line)
		return nil
	}

	authenticated := svc.authenticateServiceDial(context.Background(), serviceDialNotice,
		domain.PeerAddress("203.0.113.21:64646"), welcomeWithoutChallenge(newDialWelcomeResponder(t)),
		writeLine, bufio.NewReader(strings.NewReader("")), func() {})

	if authenticated {
		t.Fatal("a welcome without a challenge was authenticated")
	}
	if len(written) != 0 {
		t.Fatalf("the dial wrote %d line(s) after a welcome without a challenge: %q", len(written), written)
	}
}

// Source (b) of the IP-wide ban: a running address ban of another KNOWN port
// of the host covers the host even when no bannedIPSet entry was written.
func TestSendNoticeToPeer_HostWithBannedKnownSiblingPort_IsNotDialled(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	ln, address := listenForDialWelcome(t)
	done := serveRecordedDial(ln, currentWelcome(newDialWelcomeResponder(t)))
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})

	host, _, _ := splitHostPort(string(address))
	sibling := domain.PeerAddress(net.JoinHostPort(host, "1"))
	svc.peerProvider.Add(sibling, domain.PeerSourceAnnounce)
	svc.peerMu.Lock()
	health := svc.ensurePeerHealthLocked(sibling)
	health.BannedUntil = time.Now().Add(time.Hour)
	health.LastErrorCode = protocol.ErrCodeIncompatibleProtocol
	svc.peerMu.Unlock()
	svc.ipStateMu.RLock()
	_, ipEntry := svc.bannedIPSet[host]
	svc.ipStateMu.RUnlock()
	if ipEntry {
		t.Fatal("fixture: the host must have no bannedIPSet entry")
	}

	markOldNode(svc, address)
	svc.sendNoticeToPeer(address, time.Minute, "sibling-banned-ciphertext")

	if traffic := svc.TransportTrafficStats(); traffic.BytesSent != 0 || traffic.BytesReceived != 0 {
		t.Fatalf("a host with a banned known sibling port was dialled: %d bytes sent, %d received", traffic.BytesSent, traffic.BytesReceived)
	}
}
