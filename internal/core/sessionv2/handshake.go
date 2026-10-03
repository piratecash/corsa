package sessionv2

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// handshake.go establishes a v2 session (docs/protocol/session_v2.md,
// "Handshake"):
//
//	D → L  ClientHello (ALPN corsa/2) … TLS 1.3 …
//	L → D  welcome v2, session_proof (role listener, over E)
//	D → L  hello v2,   session_proof (role dialer,   over E)
//
// Nothing about the peer is applied anywhere before its proof verifies: the
// intro frame is parsed into a local value and handed out only inside a
// ProvenSession.

// Refusals. Every one of them closes the connection.
var (
	ErrNotV2           = errors.New("sessionv2: the peer did not negotiate TLS 1.3 with ALPN corsa/2")
	ErrIntro           = errors.New("sessionv2: malformed v2 hello/welcome")
	ErrProofInvalid    = errors.New("sessionv2: session_proof does not verify for this session")
	ErrSelfConnection  = errors.New("sessionv2: the peer is this node")
	ErrPeerMismatch    = errors.New("sessionv2: the peer proved another identity than the one dialled")
	ErrInvalidArgument = errors.New("sessionv2: invalid handshake argument")
	// ErrPeerSpeaksV1 is a dial answered the way an old node answers a
	// ClientHello: with a v1 JSON line, first byte '{'. It is the only
	// refusal a caller may follow with a v1 dial — and only when nothing
	// pins the peer to v2. A TLS alert (0x15) is a TLS refusal, and any other
	// byte is not a v1 node: both are plain v2 failures.
	ErrPeerSpeaksV1 = errors.New("sessionv2: the peer answered without TLS")
)

// TLSRecordHandshake is the first byte of every TLS connection: the record
// type of the ClientHello. A listener tells v2 from v1 by it.
const TLSRecordHandshake = 0x16

// V1FrameStart is the first byte of every v1 frame: a JSON object.
const V1FrameStart = '{'

// Timeouts bound the handshake phases after the connection is accepted or
// dialled: TLS done, then the peer's proof.
type Timeouts struct {
	TLS   time.Duration
	Proof time.Duration
}

// DefaultTimeouts are the contract's phases 2 and 3.
var DefaultTimeouts = Timeouts{TLS: 5 * time.Second, Proof: 3 * time.Second}

// maxIntroBytes bounds the hello/welcome line read before the peer is proven.
const maxIntroBytes = 64 * 1024

// maxProofBytes bounds the session_proof line: the frame is ~120 bytes.
const maxProofBytes = 256

// Local is this node's side of every handshake: the identity that proves and
// the network it proves on. The intro frame — hello when dialling, welcome
// when listening — is given per handshake, because a welcome carries the
// address this listener observed for the very peer it answers.
type Local struct {
	identity *identity.Identity
	network  domain.NetworkID
	pubKey   string
	boxKey   string
	boxSig   string
}

// NewLocal binds this node's identity to its network.
func NewLocal(id *identity.Identity, network domain.NetworkID) (Local, error) {
	if id == nil || len(id.PrivateKey) != ed25519.PrivateKeySize {
		return Local{}, fmt.Errorf("%w: no identity", ErrInvalidArgument)
	}
	if _, err := domain.ParseNetworkID(network.String()); err != nil {
		return Local{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if len(network.String()) > 0xffff {
		return Local{}, fmt.Errorf("%w: network name too long", ErrInvalidArgument)
	}
	return Local{
		identity: id,
		network:  network,
		pubKey:   identity.PublicKeyBase64(id.PublicKey),
		boxKey:   identity.BoxPublicKeyBase64(id.BoxPublicKey),
		boxSig:   identity.SignBoxKeyBinding(id),
	}, nil
}

// stamp makes intro this node's frame for role: the type, the identity
// fields and the network come from the Local, never from the caller, so a
// caller cannot announce one identity and prove another, and the v1
// challenge fields are cleared. A RawLine is refused: it is sent verbatim
// when the frame is marshalled, past every field set here.
func (l Local) stamp(intro protocol.Frame, role Role) (protocol.Frame, error) {
	if l.identity == nil {
		return protocol.Frame{}, fmt.Errorf("%w: no identity", ErrInvalidArgument)
	}
	if intro.RawLine != "" {
		return protocol.Frame{}, fmt.Errorf("%w: an intro with a raw line", ErrInvalidArgument)
	}
	intro.Type = introType[role]
	intro.Address = l.identity.Address
	intro.PubKey = l.pubKey
	intro.BoxKey = l.boxKey
	intro.BoxSig = l.boxSig
	intro.Network = l.network.String()
	intro.Challenge = ""
	intro.Signature = ""
	return intro, nil
}

// ExpectedPeer is whom the caller meant to reach. The expectation exists
// only when the caller names a peer (sending to X, opening contact X); an
// address dial expects nobody and accepts whoever proves itself.
type ExpectedPeer struct {
	identity domain.PeerIdentity
	named    bool
}

// AnyPeer is an address dial: no expectation.
func AnyPeer() ExpectedPeer { return ExpectedPeer{} }

// ExpectPeer is a dial "to X".
func ExpectPeer(id domain.PeerIdentity) ExpectedPeer {
	return ExpectedPeer{identity: id, named: true}
}

// handshakePhase names a point of the handshake a test can stop at.
type handshakePhase int

const (
	phaseTLSDone handshakePhase = iota + 1
	phaseProven
	phaseSettled
)

// handshakeHooks are test-only observation points; production passes none.
type handshakeHooks struct {
	phase     func(handshakePhase)
	cancelRan func()
}

func (h handshakeHooks) at(p handshakePhase) {
	if h.phase != nil {
		h.phase(p)
	}
}

// handshakeOption is unexported on purpose: only this package's tests can
// pass one.
type handshakeOption func(*handshakeHooks)

func withHooks(hooks handshakeHooks) handshakeOption {
	return func(h *handshakeHooks) { *h = hooks }
}

func collectHooks(options []handshakeOption) handshakeHooks {
	var hooks handshakeHooks
	for _, option := range options {
		option(&hooks)
	}
	return hooks
}

// Dial runs the dialer side over raw, which the caller has connected.
func Dial(ctx context.Context, raw net.Conn, local Local, hello protocol.Frame, expect ExpectedPeer, timeouts Timeouts, options ...handshakeOption) (ProvenSession, error) {
	if expect.named && expect.identity.IsZero() {
		_ = raw.Close()
		return ProvenSession{}, fmt.Errorf("%w: an expectation without an identity", ErrInvalidArgument)
	}
	intro, err := local.stamp(hello, RoleDialer)
	if err != nil {
		_ = raw.Close()
		return ProvenSession{}, err
	}
	first := &firstByteConn{Conn: raw}
	conn := tls.Client(first, dialerConfig())
	session, err := establish(ctx, conn, local, intro, RoleDialer, expect, timeouts, collectHooks(options))
	if err != nil && first.answeredWithV1() {
		return ProvenSession{}, errors.Join(ErrPeerSpeaksV1, err)
	}
	return session, err
}

// firstByteConn remembers the first byte the peer sent, so a failed dial can
// tell "a v1 node answered" (a JSON line) from everything else — a TLS
// alert, other bytes, a timeout, a reset. Only the first is a reason to
// speak v1.
type firstByteConn struct {
	net.Conn
	mu    sync.Mutex
	first []byte
}

func (c *firstByteConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.mu.Lock()
		if c.first == nil {
			c.first = []byte{b[0]}
		}
		c.mu.Unlock()
	}
	return n, err
}

// NetConn is the connection under this one, the way tls.Conn exposes its
// own: it lets the node find the metered socket under a v2 session.
func (c *firstByteConn) NetConn() net.Conn { return c.Conn }

func (c *firstByteConn) answeredWithV1() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.first != nil && c.first[0] == V1FrameStart
}

// Accept runs the listener side over raw, which the caller has accepted.
func Accept(ctx context.Context, raw net.Conn, local Local, welcome protocol.Frame, certificates CertificateSource, timeouts Timeouts, options ...handshakeOption) (ProvenSession, error) {
	if certificates.state == nil {
		_ = raw.Close()
		return ProvenSession{}, fmt.Errorf("%w: no certificate source", ErrInvalidArgument)
	}
	intro, err := local.stamp(welcome, RoleListener)
	if err != nil {
		_ = raw.Close()
		return ProvenSession{}, err
	}
	conn := tls.Server(raw, listenerConfig(certificates))
	return establish(ctx, conn, local, intro, RoleListener, AnyPeer(), timeouts, collectHooks(options))
}

// establish is both sides: the listener proves first, the dialer verifies
// it before proving itself, so a dialer never signs for an unproven
// listener.
func establish(ctx context.Context, conn *tls.Conn, local Local, intro protocol.Frame, role Role, expect ExpectedPeer, timeouts Timeouts, hooks handshakeHooks) (ProvenSession, error) {
	deadlines := &handshakeDeadlines{conn: conn}
	stop := context.AfterFunc(ctx, func() {
		deadlines.cancel()
		if hooks.cancelRan != nil {
			hooks.cancelRan()
		}
	})
	defer stop()
	session, err := runHandshake(ctx, conn, deadlines, local, intro, role, expect, timeouts, hooks)
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ProvenSession{}, errors.Join(err, ctxErr)
		}
		return ProvenSession{}, err
	}
	return session, nil
}

func runHandshake(ctx context.Context, conn *tls.Conn, deadlines *handshakeDeadlines, local Local, intro protocol.Frame, role Role, expect ExpectedPeer, timeouts Timeouts, hooks handshakeHooks) (ProvenSession, error) {
	if err := deadlines.set(time.Now().Add(timeouts.TLS)); err != nil {
		return ProvenSession{}, err
	}
	if err := conn.HandshakeContext(ctx); err != nil {
		return ProvenSession{}, fmt.Errorf("%w: %v", ErrNotV2, err)
	}
	state := conn.ConnectionState()
	if state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != ALPN {
		return ProvenSession{}, ErrNotV2
	}
	exporter, err := state.ExportKeyingMaterial(ExporterLabel, nil, ExporterLength)
	if err != nil {
		return ProvenSession{}, fmt.Errorf("%w: exporter: %v", ErrNotV2, err)
	}
	hooks.at(phaseTLSDone)
	if err := deadlines.set(time.Now().Add(timeouts.Proof)); err != nil {
		return ProvenSession{}, err
	}
	sconn := newSessionConn(conn)

	var peer Peer
	if role == RoleListener {
		if err := sendIntroAndProof(sconn, local, intro, role, exporter); err != nil {
			return ProvenSession{}, err
		}
		if peer, err = receiveProvenPeer(conn, local, role, expect, exporter); err != nil {
			return ProvenSession{}, err
		}
	} else {
		if peer, err = receiveProvenPeer(conn, local, role, expect, exporter); err != nil {
			return ProvenSession{}, err
		}
		if err := sendIntroAndProof(sconn, local, intro, role, exporter); err != nil {
			return ProvenSession{}, err
		}
	}
	hooks.at(phaseProven)
	if err := deadlines.settle(); err != nil {
		return ProvenSession{}, err
	}
	hooks.at(phaseSettled)
	return ProvenSession{state: &provenState{conn: sconn, role: role, peer: peer}}, nil
}

// handshakeDeadlines serialises every deadline change of the handshake with
// the cancel callback. context.AfterFunc runs that callback on its own
// goroutine and stop() does not wait for one already running, so without a
// shared lock a late cancel could expire the deadline of a session already
// handed out, and a phase's new deadline could overwrite a cancel and revive
// the handshake. Under the lock exactly one of two orders exists: the cancel
// lands first and every later step refuses, or the handshake settles first
// and the cancel finds nothing left to stop.
type handshakeDeadlines struct {
	conn net.Conn

	mu        sync.Mutex
	settled   bool
	cancelled bool
}

// errHandshakeCancelled is joined with the context's error by establish.
var errHandshakeCancelled = errors.New("sessionv2: handshake cancelled")

func (d *handshakeDeadlines) cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return
	}
	d.cancelled = true
	_ = d.conn.SetDeadline(time.Unix(1, 0))
}

func (d *handshakeDeadlines) set(deadline time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancelled {
		return errHandshakeCancelled
	}
	return d.conn.SetDeadline(deadline)
}

// settle clears the handshake deadline and ends the cancel's reach in one
// step.
func (d *handshakeDeadlines) settle() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancelled {
		return errHandshakeCancelled
	}
	if err := d.conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	d.settled = true
	return nil
}

// introType is the frame each role opens with.
var introType = map[Role]string{RoleDialer: "hello", RoleListener: "welcome"}

func sendIntroAndProof(conn *sessionConn, local Local, intro protocol.Frame, role Role, exporter []byte) error {
	line, err := protocol.MarshalFrameLineBytes(intro)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	signature := ed25519.Sign(local.identity.PrivateKey, proofPayload(local.network, role, exporter))
	proof, err := marshalProofFrame(signature)
	if err != nil {
		return err
	}
	if !bytes.HasSuffix(line, []byte{'\n'}) {
		line = append(line, '\n')
	}
	_, err = conn.Write(append(line, append(proof, '\n')...))
	return err
}

// receiveProvenPeer reads the peer's intro and proof and verifies, in order:
// the intro's identity fields (key acceptable and certifying the address,
// box binding), the proof over THIS session's exporter in the PEER's role,
// that the peer is not this node, and that it is whom the caller expected.
func receiveProvenPeer(conn *tls.Conn, local Local, role Role, expect ExpectedPeer, exporter []byte) (Peer, error) {
	introLine, err := readLine(conn, maxIntroBytes)
	if err != nil {
		return Peer{}, err
	}
	intro, err := protocol.ParseFrameLine(string(introLine))
	if err != nil {
		return Peer{}, fmt.Errorf("%w: %v", ErrIntro, err)
	}
	peerRole := role.peer()
	key, peerID, err := checkIntro(intro, introType[peerRole], local.network)
	if err != nil {
		return Peer{}, err
	}
	proofLine, err := readLine(conn, maxProofBytes)
	if err != nil {
		return Peer{}, err
	}
	signature, err := parseProofFrame(proofLine)
	if err != nil {
		return Peer{}, err
	}
	if !verifyProof(key, local.network, peerRole, exporter, signature) {
		return Peer{}, ErrProofInvalid
	}
	if intro.Address == local.identity.Address {
		return Peer{}, ErrSelfConnection
	}
	if expect.named && peerID != expect.identity {
		return Peer{}, fmt.Errorf("%w: proved %s", ErrPeerMismatch, peerID)
	}
	return Peer{Identity: peerID, PublicKey: key, Intro: intro}, nil
}

// checkIntro verifies the v2 intro on its own: the right frame type, no v1
// challenge machinery, every identity field present, the key one this node
// accepts (identity.ParsePublicKey) and certifying the address, the box key
// bound to it, and the network this node is on.
func checkIntro(intro protocol.Frame, wantType string, network domain.NetworkID) (identity.PublicKey, domain.PeerIdentity, error) {
	switch {
	case intro.Type != wantType:
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: %q where %q was due", ErrIntro, intro.Type, wantType)
	case intro.Challenge != "" || intro.Signature != "":
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: v1 challenge fields in a v2 intro", ErrIntro)
	case intro.Address == "" || intro.PubKey == "" || intro.BoxKey == "" || intro.BoxSig == "":
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: identity fields missing", ErrIntro)
	case intro.Network != network.String():
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: network %q", ErrIntro, intro.Network)
	}
	key, err := identity.ParsePublicKeyBase64(intro.PubKey)
	if err != nil {
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: %w", ErrIntro, err)
	}
	if key.Fingerprint() != intro.Address {
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: the key does not certify the address", ErrIntro)
	}
	if err := identity.VerifyBoxKeyBinding(intro.Address, intro.PubKey, intro.BoxKey, intro.BoxSig); err != nil {
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: %w", ErrIntro, err)
	}
	peerID, err := domain.ParsePeerIdentity(intro.Address)
	if err != nil {
		return identity.PublicKey{}, domain.PeerIdentity{}, fmt.Errorf("%w: %w", ErrIntro, err)
	}
	return key, peerID, nil
}

// readLine reads one newline-terminated line of at most limit bytes. It reads
// byte by byte so that nothing past the line is consumed: whatever the peer
// sends after its proof belongs to the caller of the proven session.
func readLine(conn io.Reader, limit int) ([]byte, error) {
	line := make([]byte, 0, 256)
	var one [1]byte
	for len(line) <= limit {
		if _, err := io.ReadFull(conn, one[:]); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrIntro, err)
		}
		if one[0] == '\n' {
			return line, nil
		}
		line = append(line, one[0])
	}
	return nil, fmt.Errorf("%w: a line longer than %d bytes", ErrIntro, limit)
}
