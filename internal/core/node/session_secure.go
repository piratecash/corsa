package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/trustedclock"
)

// session_secure.go is where a node chooses between the two session kinds
// and runs the secure one (docs/protocol/session_v2.md).
//
//   - Listener: the first byte of an accepted connection says which kind it
//     is. 0x16 opens TLS — the v2 handshake runs before the connection is
//     registered anywhere, and the peer's hello is applied only once its
//     session_proof verified. Anything else is a v1 connection, handled as
//     before.
//   - Dialler — every path that opens a session (the connection manager,
//     syncPeer, sendNoticeToPeer): a TLS attempt first; v1 only on a separate
//     connection whose identity-less probe got a correct old-protocol answer
//     naming an identity that does not require v2, the full hello following
//     on that connection (session_dial_choice.go). Where v2 is required — an
//     endpoint bound to v2 (secure_session_store.go) or proven by an outbound
//     session (v2_strong), a dial "to X", a v2-only node — nothing leads to
//     v1. A refusal of a v1 answer by pin or binding is not charged to the
//     address; it holds the fall back to v1 at that address for a while
//     (legacyRefusalHoldBase), and the next dials there start with v2 — a v2
//     attempt is never held by it. Every path dials an address today; a dial
//     "to X" (domain.DialIntent) expects X in the handshake and alone may
//     protect the endpoint, and no path makes one yet. What the dialler
//     remembers per address lives in session_address_marks.go.
//   - Listener, v1: a hello naming a pinned identity is refused.
//
// Which of the two kinds a build may use follows from its version constants
// (session_mode.go); in ModeLegacyOnly none of this runs.

// secureSessions is the per-node state of the v2 session. The pointer is set
// once in NewService and never replaced; Local and the certificate source
// are immutable or synchronise themselves, the dial timeouts and the
// operator's TLS-first switch are set once with it, the address marks own
// their mutex, and the probe-first counter is atomic. No domain mutex is
// taken for any of it.
type secureSessions struct {
	mode         sessionv2.Mode
	local        sessionv2.Local
	certificates sessionv2.CertificateSource
	store        *secureSessionStore
	marks        *sessionAddressMarks
	dialTimeouts sessionDialTimeouts
	// tlsFirstDisabled is the operator's TLS-first switch
	// (config.Node.DisableV2TLSFirst): a dial where v1 is not ruled out
	// opens with the probe instead of a ClientHello (session_dial_choice.go).
	// The zero value is TLS-first on.
	tlsFirstDisabled bool
	// dialsOpenedByProbe counts the dials that opened with the probe because
	// of tlsFirstDisabled.
	dialsOpenedByProbe atomic.Uint64
	// v1RefusalsAfterV2Welcome counts errLegacyRefusedAfterV2Welcome.
	v1RefusalsAfterV2Welcome atomic.Uint64
	// refusal is why the node must not start although its store, if any,
	// would let it: the v2 state of a build with the secure session could
	// not be built, or a v1-only build finds a pin store on disk. nil when
	// nothing here stands in the way. Set once with the state, never
	// changed.
	refusal *PinStoreRefusalError
}

// secureSessionsOptions is what a node's v2 state is built from besides its
// identity.
type secureSessionsOptions struct {
	pinMembership    *pinMembershipRequest         // copied before startup callbacks; no observer policy
	bridgePublisher  findnodebridge.Publisher      // immutable before store constructor/load
	guardObservation *guardObservationConstruction // immutable before the actual store clock/load
	// ninElapsed is immutable before the store loads/anchors a journal.
	ninElapsed func() (time.Duration, error)
	// storePath is the downgrade-protection store's file.
	storePath string
	// markerPath is the store's marker next to the identity
	// (pinStoreMarkerPath).
	markerPath string
	// clock is the wall clock the certificates, the store's trusted time
	// and the address marks read.
	clock func() time.Time
	// tlsFirstDisabled is config.Node.DisableV2TLSFirst.
	tlsFirstDisabled bool
}

// newSecureSessions builds the v2 state for a node. A build with the secure
// session whose v2 state cannot be built — an identity that cannot prove
// (nil in some test Services), an unusable certificate source — is a
// startup refusal, never a quiet fall back to v1 (unavailableSessions). The store's terms run on the trusted time over clock
// and this machine's clock since boot, started here, so a wall clock set
// forward while the node runs does not end ordinary bindings early. A
// TLS-first switched off is said at Warn here, once, with the mode it lands
// in.
func newSecureSessions(id *identity.Identity, opts secureSessionsOptions) *secureSessions {
	sessions := buildSecureSessions(id, opts, productionSecureSessionBuilders())
	warnTLSFirstDisabled(log.Logger, sessions.tlsFirstDisabled, sessions.mode)
	return sessions
}

// secureSessionBuilders are the two parts of the v2 state that can fail for
// reasons outside the options: the mode this build's version constants give,
// and the certificate source. Tests inject the failures.
type secureSessionBuilders struct {
	mode         func() (sessionv2.Mode, error)
	certificates func(sessionv2.Clock) (sessionv2.CertificateSource, error)
}

func productionSecureSessionBuilders() secureSessionBuilders {
	return secureSessionBuilders{mode: configuredSessionMode, certificates: sessionv2.NewCertificateSource}
}

func buildSecureSessions(id *identity.Identity, opts secureSessionsOptions, builders secureSessionBuilders) *secureSessions {
	mode, err := builders.mode()
	if err != nil {
		return unavailableSessions(opts, fmt.Errorf("session mode: %w", err))
	}
	if mode == sessionv2.ModeLegacyOnly {
		return legacyOnlyBuildSessions(opts)
	}
	local, err := sessionv2.NewLocal(id, domain.NetworkID(networkName))
	if err != nil {
		return unavailableSessions(opts, fmt.Errorf("identity: %w", err))
	}
	certificates, err := builders.certificates(opts.clock)
	if err != nil {
		return unavailableSessions(opts, fmt.Errorf("certificate source: %w", err))
	}
	store := openSecureSessionStore(secureSessionStoreOptions{
		bridgePublisher: opts.bridgePublisher,
		pinMembership:   opts.pinMembership,
		ninElapsed:      opts.ninElapsed,
		path:            opts.storePath,
		markerPath:      opts.markerPath,
		clock:           opts.guardObservation.newClock(trustedclock.Sources{Wall: opts.clock, SinceBoot: trustedclock.SinceBoot}),
		logger:          log.Logger,
		audit:           secretfile.AuditLocation,
	})
	return &secureSessions{
		mode:             mode,
		local:            local,
		certificates:     certificates,
		store:            store,
		marks:            newSessionAddressMarks(opts.clock),
		dialTimeouts:     defaultSessionDialTimeouts(),
		tlsFirstDisabled: opts.tlsFirstDisabled,
	}
}

// legacyOnlySessions is the state of a node that speaks v1 only. It keeps
// the address marks all the same: the delays that pace redials after a
// claimed version hold whatever the node speaks. The TLS-first switch is kept
// too, so the node reports what the operator set; with nothing dialled over
// TLS it changes no dial.
func legacyOnlySessions(opts secureSessionsOptions) *secureSessions {
	return &secureSessions{mode: sessionv2.ModeLegacyOnly, marks: newSessionAddressMarks(opts.clock), tlsFirstDisabled: opts.tlsFirstDisabled}
}

// legacyOnlyBuildSessions is the state of a build whose version constants
// speak v1 only — a rollback to a build without the secure session. It
// enforces no pin, so a pin store or a marker on disk is a refusal: the
// pins would be ignored, not kept.
func legacyOnlyBuildSessions(opts secureSessionsOptions) *secureSessions {
	sessions := legacyOnlySessions(opts)
	if found, present := pinStoreFileOnDisk(opts.storePath, opts.markerPath); present {
		sessions.refusal = refuseLegacyOnlyBuild(found)
		logSessionsRefusal(sessions.refusal)
	}
	return sessions
}

// unavailableSessions is the state of a build with the secure session whose
// v2 state could not be built. It is a refusal, never a quiet fall back to
// v1: on v1 alone not a single pin would be enforced. The state itself is the
// v1-only one, which the refused node never runs.
func unavailableSessions(opts secureSessionsOptions, cause error) *secureSessions {
	sessions := legacyOnlySessions(opts)
	sessions.refusal = refuseSecureSessionUnavailable(cause)
	logSessionsRefusal(sessions.refusal)
	return sessions
}

// logSessionsRefusal says once why the node will not start. The line carries
// the reason and the cause, never a path.
func logSessionsRefusal(refusal *PinStoreRefusalError) {
	event := log.Error().Str("reason", string(refusal.Reason))
	if refusal.Reason == domain.PinStoreRefusalSecureSessionUnavailable {
		event = event.Err(refusal.cause)
	}
	event.Msg("pin_store_refused")
}

// startupRefusal is why the node must not start over its v2 state, or nil:
// the state's own refusal first, then its store's.
func (s *secureSessions) startupRefusal() error {
	if s.refusal != nil {
		return s.refusal
	}
	if s.store == nil {
		return nil
	}
	return s.store.startupRefusal()
}

// warnTLSFirstDisabled says loudly that the operator switched TLS-first off:
// the node then dials unpinned peers without first offering v2, which an
// operator must be able to find in the log long after setting it.
func warnTLSFirstDisabled(logger zerolog.Logger, disabled bool, mode sessionv2.Mode) {
	if !disabled {
		return
	}
	logger.Warn().
		Str("session_mode", mode.String()).
		Str("scope", "dials where v1 is not ruled out open with the identity-less probe instead of a ClientHello; a v2 welcome still leads to TLS, pinned identities and bound endpoints keep v2").
		Str("restore", "unset CORSA_V2_TLS_FIRST (headless) or leave sdk.NodeConfig.V2TLSFirst nil").
		Msg("secure_session_v2_tls_first_disabled")
}

// tlsFirstDisabled reports the operator's TLS-first switch; a struct-literal
// test Service without v2 state has it on.
func (s *Service) tlsFirstDisabled() bool {
	return s.secureSessions != nil && s.secureSessions.tlsFirstDisabled
}

// refuseLegacyIdentity reports errLegacyRefusedPinned for a v1 session whose
// hello or welcome names an identity that requires v2 — pinned, or with a
// live v2 connection (identityRequiresV2) — on accept and on dial alike. A
// v1-only build pins nothing and refuses nothing. Every refusal is counted
// (SecureSessionStoreStats.V1RefusalsIdentityPinned). The caller must hold no
// domain mutex.
func (s *Service) refuseLegacyIdentity(id domain.PeerIdentity) error {
	if s.identityRequiresV2(id) {
		s.secureSessions.store.noteIdentityRefusal()
		return fmt.Errorf("%w: %s", errLegacyRefusedPinned, id)
	}
	return nil
}

// sessionMode is the mode this node runs; a struct-literal test Service
// without the state is a v1 node.
func (s *Service) sessionMode() sessionv2.Mode {
	if s.secureSessions == nil {
		return sessionv2.ModeLegacyOnly
	}
	return s.secureSessions.mode
}

// --- listener ---

// errV1RefusedV2Only is a v1 connection on a v2-only node. On dial it is
// charged to the address as an ordinary failure (onCMDialFailed), unlike the
// refusals of a peer that requires v2 (isUnchargedDialRefusal), although a
// man in the middle can provoke both with the same forged v1 answer. The
// mode is not wired yet; whether the two should be treated alike is open.
var errV1RefusedV2Only = errors.New("secure session: a v1 connection on a v2-only node")

// openInboundTransport decides the kind of an accepted connection and, for
// v2, completes the handshake. It returns the connection the session's
// frames travel on, a reader positioned at the first frame after the
// handshake, and the proven peer for v2 (nil for v1). Nothing is registered
// or applied here: a failed handshake leaves no trace.
//
// source is the read side of metered every byte is read through — the
// handshake's and, for v1, the frames' directly, for v2 under TLS — so the
// read loop's frame bound sees the socket's bytes and not TLS's records.
func (s *Service) openInboundTransport(metered *netcore.MeteredConn, source io.Reader) (net.Conn, *bufio.Reader, *sessionv2.Peer, error) {
	reader := bufio.NewReader(source)
	mode := s.sessionMode()
	if mode == sessionv2.ModeLegacyOnly {
		return metered, reader, nil, nil
	}
	if err := metered.SetReadDeadline(time.Now().Add(s.sessionLiveness().inboundIdle(false))); err != nil {
		return nil, nil, nil, err
	}
	// The socket is not registered yet, so the registry's close-all at
	// shutdown cannot reach it: the wait for the first byte follows the
	// node's lifecycle itself, or a silent peer would hold Run's connWg for
	// the whole read timeout.
	stopWaiting := context.AfterFunc(s.runCtx, func() { _ = metered.SetReadDeadline(time.Unix(1, 0)) })
	first, err := reader.Peek(1)
	stopWaiting()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("secure session: first byte: %w", err)
	}
	if err := metered.SetReadDeadline(time.Time{}); err != nil {
		return nil, nil, nil, err
	}
	if first[0] != sessionv2.TLSRecordHandshake {
		if mode == sessionv2.ModeV2Only {
			return nil, nil, nil, errV1RefusedV2Only
		}
		return metered, reader, nil, nil
	}

	conn, peer, err := s.acceptSecureInbound(metered, reader)
	if err != nil {
		return nil, nil, nil, err
	}
	if proof, proven := peer.Proof(); proven {
		if err := s.refuseBannedLoopbackIdentity(metered.RemoteAddr(), proof); err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
		if err := s.onIdentityProvenV2(s.runCtx, proof); err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
	}
	s.publishDegreeInbound(conn)
	return conn, bufio.NewReader(conn), &peer, nil
}

// acceptSecureInbound runs the listener side of a v2 handshake on a
// connection whose first byte is 0x16, within the inbound handshake bounds
// (inbound_handshakes.go): the source's slot from the first byte, the whole
// ClientHello record within clientHelloRecordTimeout, then a node slot until
// the handshake and the pin it earns are done, whatever the outcome. A
// source or node over its bound gets a *listenerRefusalError and no
// handshake.
func (s *Service) acceptSecureInbound(metered *netcore.MeteredConn, reader *bufio.Reader) (net.Conn, sessionv2.Peer, error) {
	source := handshakeSourceOf(metered.RemoteAddr(), !s.disableRateLimiting)
	leaveSource, err := s.inboundHandshakes.admitSource(source)
	if err != nil {
		return nil, sessionv2.Peer{}, err
	}
	defer leaveSource()
	record, err := s.readClientHelloRecord(metered, reader)
	if err != nil {
		return nil, sessionv2.Peer{}, err
	}
	leave, err := s.inboundHandshakes.enter(source)
	if err != nil {
		return nil, sessionv2.Peer{}, err
	}
	defer leave()

	welcome := s.welcomeFrame("", remoteIPFromString(metered.RemoteAddr().String()))
	replayed := &peekedConn{Conn: metered, reader: io.MultiReader(bytes.NewReader(record), reader)}
	session, err := sessionv2.Accept(s.runCtx, replayed,
		s.secureSessions.local, welcome, s.secureSessions.certificates, sessionv2.DefaultTimeouts)
	if err != nil {
		return nil, sessionv2.Peer{}, fmt.Errorf("secure session: %w", err)
	}
	conn, err := session.Conn()
	if err != nil {
		return nil, sessionv2.Peer{}, err
	}
	peer, err := session.Peer()
	if err != nil {
		return nil, sessionv2.Peer{}, err
	}
	// Publish the original mandatory binding before this session counts, using
	// its existing persistence contract (memory-only stores remain supported).
	// This publication is not an N-in strict metadata/journal acknowledgment.
	binding, pinErr := s.secureSessions.store.noteProvenInboundBinding(peer.Identity)
	if pinErr != nil {
		_ = conn.Close()
		return nil, sessionv2.Peer{}, pinErr
	}
	if proof, ok := peer.Proof(); ok {
		s.noteLocalContactProof(proof, binding)
	}
	if s.inboundPins != nil || s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {
		l := &proofTransportLifetime{token: s.proofToken, bridgeTerminal: s.bridgeTerminal, role: sessionv2.RoleListener}
		wire := &peekedConn{Conn: conn, reader: conn}
		l.evidence = &proofTransportEvidence{session: session, protected: conn, raw: replayed, wire: wire, group: sourceGroup(resolveRemoteIPFromAddr(metered.RemoteAddr())), identity: peer.Identity}
		if s.inboundPins != nil {
			l.nin = &matureInboundWitness{transport: l, binding: binding, session: session, group: l.evidence.group, producer: s.inboundPins}
		}
		s.captureFindNodeSeed(l, session, welcome)
		if !wire.bindProofTransport(l) || !replayed.bindProofTransport(l) {
			l.end()
			_ = conn.Close()
			return nil, sessionv2.Peer{}, errProofTransportBinding
		}
		return wire, peer, nil
	}

	return conn, peer, nil
}

// maxClientHelloRecord bounds the length of the first TLS record: a TLS
// plaintext record carries at most 2^14 bytes.
const maxClientHelloRecord = 1 << 14

// errClientHelloRecord is a first TLS record that is not a ClientHello record
// this listener reads: a wrong type or an impossible length.
var errClientHelloRecord = errors.New("secure session: malformed ClientHello record")

// readClientHelloRecord reads the first TLS record — the ClientHello — whole,
// within clientHelloRecordTimeout, and returns its bytes for the handshake to
// read again. A dialler that sends it slower holds its source's slot for no
// longer than that.
func (s *Service) readClientHelloRecord(metered *netcore.MeteredConn, reader *bufio.Reader) ([]byte, error) {
	if err := metered.SetReadDeadline(time.Now().Add(clientHelloRecordTimeout)); err != nil {
		return nil, err
	}
	// Not registered yet, so the shutdown drain cannot reach this socket:
	// the wait follows the node's lifecycle itself, as the first byte's does.
	stopWaiting := context.AfterFunc(s.runCtx, func() { _ = metered.SetReadDeadline(time.Unix(1, 0)) })
	record, err := readTLSRecord(reader)
	stopWaiting()
	if err != nil {
		return nil, err
	}
	if err := metered.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return record, nil
}

// readTLSRecord reads one whole handshake record of at most
// maxClientHelloRecord bytes of payload.
func readTLSRecord(reader *bufio.Reader) ([]byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("secure session: ClientHello record header: %w", err)
	}
	length := int(binary.BigEndian.Uint16(header[3:5]))
	if header[0] != sessionv2.TLSRecordHandshake || length == 0 || length > maxClientHelloRecord {
		return nil, errClientHelloRecord
	}
	record := make([]byte, len(header)+length)
	copy(record, header)
	if _, err := io.ReadFull(reader, record[len(header):]); err != nil {
		return nil, fmt.Errorf("secure session: ClientHello record: %w", err)
	}
	return record, nil
}

// Capture-only evidence: only genuine Accept/Dial writes this tuple. No getter
// exports it. Publication consumes it; End releases every proof/socket alias.
var errProofTransportBinding = errors.New("secure session: original proof transport already bound")

type proofTransportEvidence struct {
	session   sessionv2.ProvenSession
	protected net.Conn
	raw, wire *peekedConn
	group     domain.SourceGroup
	identity  domain.PeerIdentity
}

// peekedConn reads through the buffered reader that holds the peeked first
// byte, so the TLS handshake sees the connection from its first byte.
type peekedConn struct {
	net.Conn
	reader io.Reader
	// Same immutable leaf on raw R and candidate-only decoded W.
	common   atomic.Pointer[proofTransportLifetime]
	terminal atomic.Bool
}

// Bind once and replay an already-observed terminal event. The SC ordering
// covers terminal-before-CAS and CAS-before-terminal without a second lock.
func (c *peekedConn) bindProofTransport(l *proofTransportLifetime) bool {
	if l == nil {
		return false
	}
	if !c.common.CompareAndSwap(nil, l) && c.common.Load() != l {
		return false
	}
	if c.terminal.Load() {
		l.end()
	}
	return true
}
func (c *peekedConn) endProofTransportIO() {
	c.terminal.Store(true)
	c.common.Load().end()
}

func (c *peekedConn) Read(b []byte) (int, error) {
	n, err := c.reader.Read(b)
	if err != nil {
		c.endProofTransportIO()
	}
	return n, err
}
func (c *peekedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if err != nil {
		c.endProofTransportIO()
	}
	return n, err
}
func (c *peekedConn) Close() error {
	c.endProofTransportIO()
	return c.Conn.Close()
}

// NetConn exposes the socket under the peeked bytes (netcore.MeteredOf).
func (c *peekedConn) NetConn() net.Conn { return c.Conn }

// acceptProvenInbound applies a v2 peer's hello exactly as a v1 hello is
// applied once auth_session verified: the same version gate, address and
// advertise bookkeeping, the same auth completion and the same delivery
// start. The only difference is what proved the identity — session_proof
// over this connection's exporter instead of a challenge signature.
func (s *Service) acceptProvenInboundOwned(connID domain.ConnID, peer sessionv2.Peer, remoteAddr string, owner *proofTransportOwner) bool {
	hello := peer.Intro
	if err := validateProtocolHandshake(hello); err != nil {
		owner.end()
		log.Warn().Err(err).Uint64("conn_id", uint64(connID)).Str("addr", remoteAddr).Msg("secure_session_inbound_version_refused")
		return false
	}
	proof, proven := peer.Proof()
	if !proven {
		owner.end()
		// Only the handshake mints a proof; a peer without one did not come
		// from it, and admitting it here would make it count as proven.
		log.Error().Uint64("conn_id", uint64(connID)).Str("addr", remoteAddr).Msg("secure_session_inbound_without_proof")
		return false
	}
	advertiseResult := validateAdvertisedAddress(remoteAddr, hello)
	verified := connauth.ProvenBySessionV2(hello, proof)
	// The auth state goes in before the address bookkeeping, as on the v1
	// path, so a second hello on this connection meets the re-hello guard.
	s.setConnAuthStateByID(connID, verified)
	s.rememberConnPeerAddr(connID, hello, remoteAddr)
	s.applyAdvertiseOnInboundAccept(remoteAddr, advertiseResult)
	log.Info().Uint64("conn_id", uint64(connID)).Str("peer", hello.Address).Str("addr", remoteAddr).Msg("secure_session_inbound_established")
	backlogSub, fullSync := s.completeInboundAuth(connID, verified)
	s.startInboundSessionDeliveryOwned(connID, backlogSub, fullSync, owner)
	return true
}

// --- dialler ---

// errV2RefusedByListener is a v2 dial the listener answered with its refusal
// alert (sessionv2.ErrRefusedByListener): busy, not broken.
var errV2RefusedByListener = errors.New("secure session: the listener refused the v2 dial")

// errLegacyRefusedV2Bound is a v1 answer where v2 is required — an endpoint
// bound to v2 in the store or by a proven outbound session (v2_strong), or a
// dial "to X": a downgrade, refused rather than followed.
var errLegacyRefusedV2Bound = errors.New("secure session: v1 answer from an endpoint bound to v2")

// errEndpointOwnerMismatch is a dial "to X" on which another identity proved
// itself. The connection with that identity is not a connection with X: it
// is closed before this node proves itself, nothing is pinned or bound for
// it, the address is not charged (isUnchargedDialRefusal), and the next v2
// attempt to the endpoint waits for its v2 retry backoff.
var errEndpointOwnerMismatch = errors.New("secure session: the endpoint proved another identity than the one the dial was meant for")

// dialledTransport is a dialled connection ready for a session: raw is the
// TCP socket (keepalive), metered counts its bytes, wire carries the frames
// (TLS for v2, metered itself for v1), and proven is the v2 peer or nil.
type dialledTransport struct {
	common   *proofTransportLifetime
	proofRaw *peekedConn

	pinBinding identityPinEntryBinding
	pinGroup   domain.SourceGroup
	// connID names this socket from the moment it is dialled, so what the
	// handshake records — a change of an endpoint's owner — names the same
	// connection the session later runs on.
	connID  domain.ConnID
	raw     net.Conn
	metered *netcore.MeteredConn
	wire    net.Conn
	proven  *sessionv2.Peer
	// reader reads the session's frames from wire. On a connection the probe
	// opened it already holds whatever the listener sent behind the probe's
	// answer, so the caller must read through it, never through a new one.
	reader *bufio.Reader
	// probed is what the probe's answer on this connection named; nil for v2
	// and for a node that runs no probe (ModeLegacyOnly). The full welcome
	// must name the same (refuseFullLegacyWelcome).
	probed *legacyProbeWelcome
}

// rawDialer opens one socket to the address being dialled; each caller
// brings its own, because each pays for its socket differently (the CM slot
// reservation, the shared connection budget). dialPeerTransport meters it.
type rawDialer func(ctx context.Context) (net.Conn, error)

// transportDial opens one metered socket to the address being dialled.
type transportDial func(ctx context.Context) (*dialledTransport, error)

// transportDialer meters each socket dialRaw opens to address.
func (s *Service) transportDialer(address domain.PeerAddress, dialRaw rawDialer) transportDial {
	return func(ctx context.Context) (*dialledTransport, error) {
		raw, err := dialRaw(ctx)
		if err != nil {
			return nil, err
		}
		metered, err := netcore.NewMeteredConn(raw, &s.transportTotals)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("meter peer session socket %s: %w", address, err)
		}
		return &dialledTransport{
			connID:  s.allocateConnID("dialPeerTransport", address),
			raw:     raw,
			metered: metered,
			wire:    metered,
			reader:  bufio.NewReader(metered),
		}, nil
	}
}

// dialPeerTransport opens the session kind the mode, the store and the
// marks call for (session_dial_choice.go): a TLS attempt first; v1 only on a
// separate connection whose identity-less probe got a correct old-protocol
// answer naming an identity that does not require v2; never where v2 is
// required, and never while the fall back to v1 at the address is held. With
// TLS-first switched off by the operator, a dial where v1 is not ruled out
// opens with that probe instead of the TLS attempt, and a v2 welcome to it
// leads to a TLS attempt that no failure turns into v1 (dialOpenedByProbe).
//
// intent is whom the dial means to reach; a path that states none (the zero
// DialIntent) is refused with domain.ErrDialIntentUnset before any socket. A
// dial "to X" expects X in the v2 handshake: another identity proven there
// closes it with errEndpointOwnerMismatch and records nothing; X proven there
// may protect the endpoint (dialProtection). A dial "to X" requires v2: it
// never probes.
func (s *Service) dialPeerTransport(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, dialRaw rawDialer) (*dialledTransport, error) {
	if !intent.Valid() {
		return nil, fmt.Errorf("dial %s: %w", address, domain.ErrDialIntentUnset)
	}
	if err := s.refuseHeldDial(address); err != nil {
		return nil, err
	}
	defer s.openDial(address)()
	transport, err := s.dialChosenSessionKind(ctx, address, intent, dialRaw)
	return transport, endedLocally(ctx, err)
}

// openDial records one dial to address for as long as it runs, so the dial
// moves the address's v2 retry backoff on once at most (openDial of the
// marks). A struct-literal test Service without marks records nothing.
func (s *Service) openDial(address domain.PeerAddress) (closeDial func()) {
	marks := s.dialMarks()
	if marks == nil {
		return func() {}
	}
	return marks.openDial(address)
}

// endedLocally marks err as a dial this node ended itself when ctx was
// cancelled or ran out: the stage that failed then failed because of it, and
// that is no failure of the address (errDialAbortedLocally).
func endedLocally(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || errors.Is(err, errDialAbortedLocally) {
		return err
	}
	return fmt.Errorf("%w: %w", errDialAbortedLocally, err)
}

// dialChosenSessionKind is the dial itself: the session kind the mode, the
// plan and the switch call for.
func (s *Service) dialChosenSessionKind(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, dialRaw rawDialer) (*dialledTransport, error) {
	dial := s.transportDialer(address, dialRaw)
	mode := s.sessionMode()
	if mode == sessionv2.ModeLegacyOnly {
		return dial(ctx)
	}
	plan := s.planSessionDial(mode, address, intent)
	if plan.opensWithProbe() {
		return s.dialOpenedByProbe(ctx, address, intent, plan, dial)
	}
	return s.dialV2(ctx, address, intent, plan, dial)
}

// dialV2 is one TLS attempt on a new connection, and what plan lets follow
// its failure (afterFailedV2Attempt).
func (s *Service) dialV2(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, plan sessionDialPlan, dial transportDial) (*dialledTransport, error) {
	transport, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	intro := s.nodeHelloFrame()
	var proofWire net.Conn = transport.metered
	if s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability() {
		transport.proofRaw = &peekedConn{Conn: transport.metered, reader: transport.metered}
		proofWire = transport.proofRaw
	}
	session, err := sessionv2.Dial(ctx, proofWire, s.secureSessions.local, intro, expectedPeerOf(intent), s.secureSessions.dialTimeouts.forAddress(address).tls)
	if err != nil {
		attempt := failedV2Attempt{address: address, intent: intent, connID: transport.connID, plan: plan, err: err}
		return s.afterFailedV2Attempt(ctx, attempt, dial)
	}
	return s.provenTransport(ctx, transport, session, address, intent, intro)
}

// dialHeld reports whether the next dial to address waits — for its
// old-version delay, its v2 retry backoff, or room in a full pin store that
// refused its last v2 session. The candidate filter skips such an address.
// It holds in every mode: a node that fell back to v1 only still paces the
// redials a claimed version calls for. Takes only leaf mutexes.
func (s *Service) dialHeld(address domain.PeerAddress) bool {
	return s.heldDialReason(address) != nil
}

// dialMarks is the address marks, or nil for a struct-literal test Service
// without them.
func (s *Service) dialMarks() *sessionAddressMarks {
	if s.secureSessions == nil {
		return nil
	}
	return s.secureSessions.marks
}

// heldDialReason is why the next dial to address must wait, or nil.
func (s *Service) heldDialReason(address domain.PeerAddress) error {
	marks := s.dialMarks()
	if marks == nil {
		return nil
	}
	store := s.secureSessions.store
	switch {
	case marks.oldVersionHeld(address):
		return errOldVersionAwaited
	case marks.v2RetryHeld(address):
		return errV2RetryHeld
	case marks.awaitsPinRoom(address) && store != nil && store.pinStoreFull():
		return errPinRoomAwaited
	default:
		return nil
	}
}

// refuseHeldDial refuses, before any socket, a dial that must wait. The v2
// retry backoff and the wait for pin room are counted together
// (SecureSessionStoreStats.DialsHeldByV2Retry); the old-version delay is
// shown per endpoint by listEndpointDialDelays.
func (s *Service) refuseHeldDial(address domain.PeerAddress) error {
	reason := s.heldDialReason(address)
	if reason == nil {
		return nil
	}
	if store := s.secureSessions.store; store != nil && !errors.Is(reason, errOldVersionAwaited) {
		store.noteDialHeldByV2Retry()
	}
	return fmt.Errorf("%w: %s", reason, address)
}

// holdLegacyFallback refuses the fall back to v1 after attempt, a v2 attempt
// the address answered with a v1 line, while the address is held.
func (s *Service) holdLegacyFallback(address domain.PeerAddress, attempt error) error {
	s.secureSessions.store.noteLegacyDialHeld()
	retry := s.secureSessions.marks.noteV2Unanswered(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_v1_fallback_held_after_refusal")
	return fmt.Errorf("%w: %w", errLegacyRefusedBackoff, attempt)
}

// noteV2RefusedByListener answers a v2 attempt at address that the listener
// refused with its alert: the node there speaks v2 and is busy, not broken.
// The address is not charged; the next attempt waits for the v2 retry
// backoff, spread by a jitter so that the diallers one busy listener refused
// together do not come back together.
func (s *Service) noteV2RefusedByListener(address domain.PeerAddress, attempt error) error {
	retry := s.secureSessions.marks.noteListenerRefused(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_v2_refused_by_listener")
	return fmt.Errorf("%w: %w", errV2RefusedByListener, attempt)
}

// forgetLegacyObservedWithoutWelcome forgets legacy_observed of address
// after a v1 dial there got no welcome. An address that answered the probe
// once and now hangs up on the full hello — a v2 node that pinned this node
// does exactly that — would otherwise be probed first again and again for
// the rest of the mark; the next dial tries v2 first. transport is the
// dial's; a v2 one carries no mark.
func (s *Service) forgetLegacyObservedWithoutWelcome(address domain.PeerAddress, transport *dialledTransport) {
	if transport.proven != nil || s.sessionMode() == sessionv2.ModeLegacyOnly {
		return
	}
	s.secureSessions.marks.forgetLegacyObserved(address)
}

// refuseLegacyWelcome is refuseLegacyIdentity for a v1 welcome naming claimed
// that this node received on a dial to address — the probe's answer or the
// full welcome: a refusal also starts the hold of the fall back to v1 at that
// address. The caller must hold no domain mutex.
func (s *Service) refuseLegacyWelcome(address domain.PeerAddress, claimed domain.PeerIdentity) error {
	err := s.refuseLegacyIdentity(claimed)
	if err == nil {
		return nil
	}
	hold := s.secureSessions.marks.noteLegacyRefused(address)
	log.Debug().Str("peer", string(address)).Dur("legacy_hold", hold).Msg("secure_session_v1_fallback_held_after_refusal_started")
	return err
}

// unchargedDialOutcomes are the dial outcomes that say nothing against the
// address. This node refused a v1 answer from a peer that requires v2, held
// the fall back to v1 after such a refusal, or held the dial itself on the
// v2 retry backoff those refusals start — the man in the middle that forged
// the answer can repeat it at will; the listener refused the v2 attempt with
// its alert — busy; a dial "to X" met another identity on the address, which
// may simply have changed hands; the probe was answered by a node that speaks
// v2, whose TLS attempt failed — a hint, forgeable like the rest — or with
// an answer that admitted nothing: none in time, none at all, one cut off,
// broken or forged, which a man in the middle can arrange as easily; a TLS
// attempt over an opened TCP connection ended before the identity proof
// verified — nothing was authenticated, so nothing shows the node at fault;
// the probe failed on this node, or this node ended the dial itself; a v1 line
// answered the TLS attempt that followed a v2 welcome to the probe — one of
// the two forged; this
// node's own pin store had no room for the proven peer's pin or exhausted its
// runtime proof revision, or the dial
// waited for that room — this node's limit, not the peer's fault; or this node
// could not even start its v2 attempt; or the dial waited for the endpoint's
// old-version delay. None of them is charged to the address.
var unchargedDialOutcomes = []error{
	errAddressRecheckHeld,
	errV2RefusedByListener,
	errLegacyRefusedPinned,
	errLegacyRefusedV2Bound,
	errLegacyRefusedBackoff,
	errV2RetryHeld,
	errEndpointOwnerMismatch,
	errLegacyProbeAnsweredV2,
	errLegacyRefusedAfterV2Welcome,
	errLegacyProbeAdmittedNothing,
	errLegacyProbeLocalFailure,
	errV2FailedBeforeProof,
	errDialAbortedLocally,
	errPinStoreFull,
	domain.ErrIdentityPinRevisionExhausted,
	errPinRoomAwaited,
	errV2DialLocalFailure,
	errOldVersionAwaited,
}

// isUnchargedDialRefusal reports whether err is one of unchargedDialOutcomes.
func isUnchargedDialRefusal(err error) bool {
	for _, refusal := range unchargedDialOutcomes {
		if errors.Is(err, refusal) {
			return true
		}
	}
	return false
}

// dialPeerTransportForCM is the connection manager's dial: its socket is paid
// for by the CM slot reservation taken before the dial starts.
func (s *Service) dialPeerTransportForCM(ctx context.Context, address domain.PeerAddress) (*dialledTransport, error) {
	return s.dialPeerTransport(ctx, address, domain.DialToAddress(), func(ctx context.Context) (net.Conn, error) {
		return s.dialPeerRawForCM(ctx, address)
	})
}

// budgetedSessionDialer dials address for a session on the shared connection
// budget; a socket that could not be opened is errPeerDialTransport.
func (s *Service) budgetedSessionDialer(address domain.PeerAddress) rawDialer {
	return func(ctx context.Context) (net.Conn, error) {
		raw, err := s.dialPeerWithBudget(ctx, address, dialTimeout)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errPeerDialTransport, err)
		}
		return raw, nil
	}
}

func (s *Service) dialPeerRawForCM(ctx context.Context, address domain.PeerAddress) (net.Conn, error) {
	raw, err := s.dialPeer(ctx, address, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPeerDialTransport, err)
	}
	return raw, nil
}

// provenTransport hands out the proven session and records the protection it
// earned: the peer's identity is pinned and the dialled endpoint bound with the
// protection the dial's intent asks for (dialProtection) — for a name dialled
// direct, the address the socket reached too, as an ordinary derived key. A
// protection that could not be written fails the dial: the session is not
// established on it; a pin refused because the store is full makes the next
// dial to the address wait for room. Once it is written, the legacy
// connections naming the same identity are closed and their routing residue
// forgotten (onIdentityProvenV2), before this session is registered. Only
// then is the address marked v2_strong and its backoffs reset: a proof whose
// protection did not land proves nothing this node keeps.
func (s *Service) provenTransport(ctx context.Context, transport *dialledTransport, session sessionv2.ProvenSession, address domain.PeerAddress, intent domain.DialIntent, intro protocol.Frame) (*dialledTransport, error) {
	wire, err := session.Conn()
	if err != nil {
		return nil, err
	}
	peer, err := session.Peer()
	if err != nil {
		return nil, err
	}
	result, err := s.secureSessions.store.noteProvenOutbound(outboundDialOf(address, transport.raw), peer.Identity, s.dialProtection(intent, peer.Identity))
	// A change of owner is made in memory even when its write failed, so it
	// is journalled either way.
	s.secureSessions.store.logOwnerChange(peer.Identity, result, transport.connID)
	if err != nil {
		_ = wire.Close()
		return nil, s.notePinNotStored(address, err)
	}
	if proof, proven := peer.Proof(); proven {
		if err := s.onIdentityProvenV2(ctx, proof); err != nil {
			_ = wire.Close()
			return nil, err
		}
	}
	s.secureSessions.marks.noteOutboundV2(address)
	if proof, ok := peer.Proof(); ok {
		s.noteLocalContactProof(proof, result.pinBinding)
	}
	transport.pinBinding, transport.pinGroup = result.pinBinding, result.pinGroup
	if (s.findNode != nil || s.degree != nil || s.hasContactRecordPushTransportCapability()) && transport.proofRaw != nil {
		l := &proofTransportLifetime{token: s.proofToken, bridgeTerminal: s.bridgeTerminal, role: sessionv2.RoleDialer, outbound: true, id: transport.connID}
		wrapped := &peekedConn{Conn: wire, reader: wire}
		l.evidence = &proofTransportEvidence{session: session, protected: wire, raw: transport.proofRaw, wire: wrapped, group: sourceGroup(resolveRemoteIPFromAddr(transport.raw.RemoteAddr())), identity: peer.Identity}
		s.captureFindNodeSeed(l, session, intro)
		if !wrapped.bindProofTransport(l) || !transport.proofRaw.bindProofTransport(l) {
			l.end()
			_ = wire.Close()
			return nil, errProofTransportBinding
		}
		transport.common = l
		wire = wrapped
	}
	transport.wire, transport.proven, transport.reader = wire, &peer, bufio.NewReader(wire)
	s.publishDegreeTransport(transport.common)
	return transport, nil
}

// notePinNotStored passes on why the protection of a proven outbound session
// was not stored. A full pin store is this node's own limit: the next dial to
// the address waits until the store has room, pinRoomWaitMax at most.
func (s *Service) notePinNotStored(address domain.PeerAddress, err error) error {
	if !errors.Is(err, errPinStoreFull) {
		return err
	}
	wait := s.secureSessions.marks.notePinStoreFull(address)
	log.Debug().Str("peer", string(address)).Dur("pin_room_wait_max", wait).Msg("secure_session_dial_waits_for_pin_room")
	return err
}

// expectedPeerOf is what the v2 handshake of a dial with intent expects: X for
// a dial "to X", nobody for an address dial.
func expectedPeerOf(intent domain.DialIntent) sessionv2.ExpectedPeer {
	if x, named := intent.Target(); named {
		return sessionv2.ExpectPeer(x)
	}
	return sessionv2.AnyPeer()
}

// dialProtection is the protection a dial with intent asks of the endpoint on
// which proven proved itself. Only a dial "to X" that proved X, X being one of
// this node's contacts, asks — for pool K. An address dial may reach whoever
// stands on the address, so it creates and renews only ordinary bindings,
// however pinned the proven identity is; otherwise a peer that once stood on
// the path of an address dial (a Tor exit, say) would earn a protection that
// never expires. A relation (pool R) is not derived by any path yet, so a dial
// "to X" for an X that is not a contact asks nothing either. Whether the
// protection is given — a free place, X already owning an ordinary binding —
// is the store's decision (noteProvenOutbound); protected bindings already in
// the store stay as they are, never lowered automatically.
func (s *Service) dialProtection(intent domain.DialIntent, proven domain.PeerIdentity) endpointProtection {
	x, named := intent.Target()
	if !named || x != proven || s.trust == nil || !s.trust.isTrustedContact(x) {
		return endpointProtectionNone()
	}
	return endpointProtectionWanted(domain.ProtectedBindingPoolContact)
}

// noteEndpointOwnerMismatch answers a dial "to X" on which another identity
// proved itself: the dial fails with errEndpointOwnerMismatch, the endpoint's
// binding stays as it is, and the mismatch is journalled with the endpoint, X,
// the identity proven instead and the dial's connection. The address is not
// charged — it may have changed hands, which says nothing against it — but
// the next v2 attempt to it waits for its v2 retry backoff: redialling at
// once would meet the same identity.
func (s *Service) noteEndpointOwnerMismatch(address domain.PeerAddress, intent domain.DialIntent, connID domain.ConnID, attempt error) error {
	expected, _ := intent.Target()
	mismatch := endpointOwnerMismatch{address: address, expected: expected, connID: connID}
	var proven *sessionv2.PeerMismatchError
	if errors.As(attempt, &proven) {
		mismatch.proven = &proven.Proven
	}
	s.secureSessions.store.logOwnerMismatch(mismatch)
	retry := s.secureSessions.marks.noteV2Unanswered(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_owner_mismatch_paced")
	return fmt.Errorf("%w: %w", errEndpointOwnerMismatch, attempt)
}
