package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
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
//     syncPeer, sendNoticeToPeer): v2 first. A peer that answers with a v1
//     line is an old node: the address is marked as speaking v1 and dialled
//     again over v1 — unless the endpoint is bound to v2
//     (secure_session_store.go), in which case the answer is a downgrade and
//     the dial fails. A v1 welcome naming a pinned identity is refused too.
//     Such a refusal is not charged to the address; it holds the fall back to
//     v1 at that address for a while (legacyRefusalHoldBase), and the next
//     dials there start with v2 — a v2 attempt is never held. Every one of
//     these paths dials an address; a dial "to X" (domain.DialIntent)
//     expects X in the handshake and alone may protect the endpoint, and no
//     path makes one yet.
//   - Listener, v1: a hello naming a pinned identity is refused.
//
// Which of the two kinds a build may use follows from its version constants
// (session_mode.go); in ModeLegacyOnly none of this runs.

// secureSessions is the per-node state of the v2 session. The pointer is set
// once in NewService and never replaced; Local and the certificate source
// are immutable or synchronise themselves, and the address marks own their
// mutex. No domain mutex is taken for any of it.
type secureSessions struct {
	mode         sessionv2.Mode
	local        sessionv2.Local
	certificates sessionv2.CertificateSource
	store        *secureSessionStore
	marks        *sessionAddressMarks
}

// newSecureSessions builds the v2 state for a node. An identity that cannot
// prove (nil in some test Services) leaves the node on v1: there is nothing
// v2 could prove with. The store's terms run on the trusted time over clock
// and this machine's clock since boot, started here, so a wall clock set
// forward while the node runs does not end ordinary bindings early.
func newSecureSessions(id *identity.Identity, storePath string, clock func() time.Time) *secureSessions {
	mode, err := configuredSessionMode()
	if err != nil {
		log.Error().Err(err).Msg("secure_session_mode_invalid")
		return &secureSessions{mode: sessionv2.ModeLegacyOnly}
	}
	if mode == sessionv2.ModeLegacyOnly {
		return &secureSessions{mode: mode}
	}
	local, err := sessionv2.NewLocal(id, domain.NetworkID(networkName))
	if err != nil {
		log.Error().Err(err).Msg("secure_session_identity_unusable")
		return &secureSessions{mode: sessionv2.ModeLegacyOnly}
	}
	certificates, err := sessionv2.NewCertificateSource(clock)
	if err != nil {
		log.Error().Err(err).Msg("secure_session_certificates_unusable")
		return &secureSessions{mode: sessionv2.ModeLegacyOnly}
	}
	return &secureSessions{
		mode:         mode,
		local:        local,
		certificates: certificates,
		store:        loadSecureSessionStore(storePath, trustedclock.New(trustedclock.Sources{Wall: clock, SinceBoot: trustedclock.SinceBoot})),
		marks:        newSessionAddressMarks(clock),
	}
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
func (s *Service) openInboundTransport(metered *netcore.MeteredConn) (net.Conn, *bufio.Reader, *sessionv2.Peer, error) {
	reader := bufio.NewReader(metered)
	mode := s.sessionMode()
	if mode == sessionv2.ModeLegacyOnly {
		return metered, reader, nil, nil
	}
	if err := metered.SetReadDeadline(time.Now().Add(inboundReadTimeout)); err != nil {
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
		if err := s.onIdentityProvenV2(s.runCtx, proof); err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
	}
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
	// The protection this session earns must be on disk before the session
	// counts: otherwise a restart would forget the pin and let v1 back in.
	if err := s.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		_ = conn.Close()
		return nil, sessionv2.Peer{}, err
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

// peekedConn reads through the buffered reader that holds the peeked first
// byte, so the TLS handshake sees the connection from its first byte.
type peekedConn struct {
	net.Conn
	reader io.Reader
}

func (c *peekedConn) Read(b []byte) (int, error) { return c.reader.Read(b) }

// NetConn exposes the socket under the peeked bytes (netcore.MeteredOf).
func (c *peekedConn) NetConn() net.Conn { return c.Conn }

// acceptProvenInbound applies a v2 peer's hello exactly as a v1 hello is
// applied once auth_session verified: the same version gate, address and
// advertise bookkeeping, the same auth completion and the same delivery
// start. The only difference is what proved the identity — session_proof
// over this connection's exporter instead of a challenge signature.
func (s *Service) acceptProvenInbound(connID domain.ConnID, peer sessionv2.Peer, remoteAddr string) bool {
	hello := peer.Intro
	if err := validateProtocolHandshake(hello); err != nil {
		log.Warn().Err(err).Uint64("conn_id", uint64(connID)).Str("addr", remoteAddr).Msg("secure_session_inbound_version_refused")
		return false
	}
	proof, proven := peer.Proof()
	if !proven {
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
	s.startInboundSessionDelivery(connID, backlogSub, fullSync)
	return true
}

// --- dialler ---

// errV2RefusedByListener is a v2 dial the listener answered with its refusal
// alert (sessionv2.ErrRefusedByListener): busy, not broken.
var errV2RefusedByListener = errors.New("secure session: the listener refused the v2 dial")

// errLegacyRefusedV2Bound is a v1 answer from an endpoint bound to v2: a
// downgrade, refused rather than followed.
var errLegacyRefusedV2Bound = errors.New("secure session: v1 answer from an endpoint bound to v2")

// errEndpointOwnerMismatch is a dial "to X" on which another identity proved
// itself. The connection with that identity is not a connection with X: it
// is closed before this node proves itself, nothing is pinned or bound for
// it, and the address is not charged (isUnchargedDialRefusal).
var errEndpointOwnerMismatch = errors.New("secure session: the endpoint proved another identity than the one the dial was meant for")

// dialledTransport is a dialled connection ready for a session: raw is the
// TCP socket (keepalive), metered counts its bytes, wire carries the frames
// (TLS for v2, metered itself for v1), and proven is the v2 peer or nil.
type dialledTransport struct {
	// connID names this socket from the moment it is dialled, so what the
	// handshake records — a change of an endpoint's owner — names the same
	// connection the session later runs on.
	connID  domain.ConnID
	raw     net.Conn
	metered *netcore.MeteredConn
	wire    net.Conn
	proven  *sessionv2.Peer
}

// rawDialer opens one socket to the address being dialled; each caller
// brings its own, because each pays for its socket differently (the CM slot
// reservation, the shared connection budget). dialPeerTransport meters it.
type rawDialer func(ctx context.Context) (net.Conn, error)

// dialPeerTransport opens the session kind the mode, the store and the
// marks call for: v2 first; v1 for an address known to speak v1 and not
// bound to v2; v1 after a v2 attempt answered with a v1 line — never after
// one that failed otherwise, never for an endpoint bound to v2, and never
// while the v1 dial to the address is held after a refused v1 answer.
//
// intent is whom the dial means to reach; a path that states none (the zero
// DialIntent) is refused with domain.ErrDialIntentUnset before any socket. A
// dial "to X" expects X in the v2
// handshake: another identity proven there closes it with
// errEndpointOwnerMismatch and records nothing; X proven there may protect
// the endpoint (dialProtection). A v1 answer carries no proof, so on the v1
// path the intent decides nothing here: whoever reads the v1 welcome checks
// the identity it names.
func (s *Service) dialPeerTransport(ctx context.Context, address domain.PeerAddress, intent domain.DialIntent, dialRaw rawDialer) (*dialledTransport, error) {
	if !intent.Valid() {
		return nil, fmt.Errorf("dial %s: %w", address, domain.ErrDialIntentUnset)
	}
	dial := func(ctx context.Context) (*dialledTransport, error) {
		raw, err := dialRaw(ctx)
		if err != nil {
			return nil, err
		}
		metered, err := netcore.NewMeteredConn(raw, &s.transportTotals)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("meter peer session socket %s: %w", address, err)
		}
		return &dialledTransport{connID: s.allocateConnID("dialPeerTransport", address), raw: raw, metered: metered, wire: metered}, nil
	}
	mode := s.sessionMode()
	if mode == sessionv2.ModeLegacyOnly {
		return dial(ctx)
	}
	if s.v2RetryHeld(address) {
		s.secureSessions.store.noteDialHeldByV2Retry()
		return nil, fmt.Errorf("%w: %s", errV2RetryHeld, address)
	}
	plan := s.planSessionDial(mode, address)
	if plan.v1Direct() {
		return dial(ctx)
	}
	transport, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	session, err := sessionv2.Dial(ctx, transport.metered, s.secureSessions.local, s.nodeHelloFrame(), expectedPeerOf(intent), sessionv2.DefaultTimeouts)
	switch {
	case err == nil:
		return s.provenTransport(ctx, transport, session, address, intent)
	case errors.Is(err, sessionv2.ErrPeerMismatch):
		return nil, s.noteEndpointOwnerMismatch(address, intent, transport.connID, err)
	case errors.Is(err, sessionv2.ErrRefusedByListener):
		return nil, s.noteV2RefusedByListener(address, err)
	case !errors.Is(err, sessionv2.ErrPeerSpeaksV1):
		return nil, err
	case mode == sessionv2.ModeV2Only:
		return nil, fmt.Errorf("%w: %w", errV1RefusedV2Only, err)
	case plan.required:
		s.secureSessions.store.noteEndpointRefusal()
		hold := s.secureSessions.marks.noteLegacyRefused(address)
		log.Warn().Str("peer", string(address)).Dur("legacy_hold", hold).Msg("secure_session_downgrade_refused")
		return nil, fmt.Errorf("%w: %w", errLegacyRefusedV2Bound, err)
	}
	if plan.held {
		// No v1 mark: the refusal that started the hold forgot it because
		// the v1 answers here may be forged, and a mark set now would send
		// the next dial past v2 once the hold is over.
		return nil, s.holdLegacyFallback(address, err)
	}
	log.Info().Str("peer", string(address)).Msg("secure_session_peer_speaks_v1")
	s.secureSessions.marks.noteV1(address)
	return dial(ctx)
}

// sessionDialPlan is what the store and the marks say about the next dial to
// one address, for a node that speaks v2.
type sessionDialPlan struct {
	// required: only v2 may be used — a v2-only node, or an endpoint bound
	// to v2.
	required bool
	// v1Known: the address answered as an old node recently.
	v1Known bool
	// held: a v1 answer from the address was refused and its hold runs.
	held bool
}

func (s *Service) planSessionDial(mode sessionv2.Mode, address domain.PeerAddress) sessionDialPlan {
	return sessionDialPlan{
		required: mode == sessionv2.ModeV2Only || s.secureSessions.store.endpointRequiresV2(address),
		v1Known:  s.secureSessions.marks.v1Seen(address),
		held:     s.secureSessions.marks.legacyHeld(address),
	}
}

// v1Direct: the dial goes straight to v1, without a v2 attempt first. A hold
// cancels it: the v1 answer that earned the address its v1 mark may have been
// forged by the same man in the middle whose v1 welcome was refused, and
// skipping v2 on its word would keep v2 away from the address — so a held
// address is dialled v2 first and only the fall back to v1 is held.
func (p sessionDialPlan) v1Direct() bool { return !p.required && p.v1Known && !p.held }

// v2RetryHeld reports whether the next dial to address waits for its v2
// retry backoff. Takes only leaf mutexes.
func (s *Service) v2RetryHeld(address domain.PeerAddress) bool {
	if s.sessionMode() == sessionv2.ModeLegacyOnly {
		return false
	}
	return s.secureSessions.marks.v2RetryHeld(address)
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
// The address is not charged and gets no v1 mark; the next attempt waits for
// the v2 retry backoff, spread by a jitter so that the diallers one busy
// listener refused together do not come back together.
func (s *Service) noteV2RefusedByListener(address domain.PeerAddress, attempt error) error {
	retry := s.secureSessions.marks.noteListenerRefused(address)
	log.Debug().Str("peer", string(address)).Dur("v2_retry", retry).Msg("secure_session_v2_refused_by_listener")
	return fmt.Errorf("%w: %w", errV2RefusedByListener, attempt)
}

// forgetV1MarkWithoutWelcome forgets the v1 mark of address after a v1 dial
// there got no welcome. An address that answered v1 once and now hangs up on
// a v1 hello — a v2 node that pinned this node does exactly that — would
// otherwise be dialled over v1 again and again for the rest of the mark; the
// next dial tries v2 first. transport is the dial's; a v2 one carries no mark.
func (s *Service) forgetV1MarkWithoutWelcome(address domain.PeerAddress, transport *dialledTransport) {
	if transport.proven != nil || s.sessionMode() == sessionv2.ModeLegacyOnly {
		return
	}
	s.secureSessions.marks.forgetV1(address)
}

// refuseLegacyWelcome is refuseLegacyIdentity for a v1 welcome naming claimed
// that this node received on a dial to address: a refusal also starts the
// hold of the fall back to v1 at that address. The caller must hold no domain
// mutex.
func (s *Service) refuseLegacyWelcome(address domain.PeerAddress, claimed domain.PeerIdentity) error {
	err := s.refuseLegacyIdentity(claimed)
	if err == nil {
		return nil
	}
	hold := s.secureSessions.marks.noteLegacyRefused(address)
	log.Debug().Str("peer", string(address)).Dur("legacy_hold", hold).Msg("secure_session_v1_fallback_held_after_refusal_started")
	return err
}

// isUnchargedDialRefusal reports whether a dial ended on a decision that says
// nothing against the address: this node refused a v1 answer from a peer that
// requires v2, held the fall back to v1 after such a refusal, or held the dial
// itself on the v2 retry backoff those refusals start — the man in the middle
// that forged the answer can repeat it at will; the listener refused the v2
// attempt with its alert — busy; or a dial "to X" met another identity on the
// address, which may simply have changed hands. None of them is charged to
// the address.
func isUnchargedDialRefusal(err error) bool {
	return errors.Is(err, errV2RefusedByListener) ||
		errors.Is(err, errLegacyRefusedPinned) ||
		errors.Is(err, errLegacyRefusedV2Bound) ||
		errors.Is(err, errLegacyRefusedBackoff) ||
		errors.Is(err, errV2RetryHeld) ||
		errors.Is(err, errEndpointOwnerMismatch)
}

// dialPeerTransportForCM is the connection manager's dial: its socket is paid
// for by the CM slot reservation taken before the dial starts.
func (s *Service) dialPeerTransportForCM(ctx context.Context, address domain.PeerAddress) (*dialledTransport, error) {
	return s.dialPeerTransport(ctx, address, domain.DialToAddress(), func(ctx context.Context) (net.Conn, error) {
		return s.dialPeerRawForCM(ctx, address)
	})
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
// established on it. Once it is written, the legacy connections naming the
// same identity are closed and their routing residue forgotten
// (onIdentityProvenV2), before this session is registered.
func (s *Service) provenTransport(ctx context.Context, transport *dialledTransport, session sessionv2.ProvenSession, address domain.PeerAddress, intent domain.DialIntent) (*dialledTransport, error) {
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
		return nil, err
	}
	if proof, proven := peer.Proof(); proven {
		if err := s.onIdentityProvenV2(ctx, proof); err != nil {
			_ = wire.Close()
			return nil, err
		}
	}
	s.secureSessions.marks.clearLegacyRefusal(address)
	transport.wire, transport.proven = wire, &peer
	return transport, nil
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
// the identity proven instead and the dial's connection. Nothing else is
// recorded: the address may have changed hands, which says nothing against
// it.
func (s *Service) noteEndpointOwnerMismatch(address domain.PeerAddress, intent domain.DialIntent, connID domain.ConnID, attempt error) error {
	expected, _ := intent.Target()
	mismatch := endpointOwnerMismatch{address: address, expected: expected, connID: connID}
	var proven *sessionv2.PeerMismatchError
	if errors.As(attempt, &proven) {
		mismatch.proven = &proven.Proven
	}
	s.secureSessions.store.logOwnerMismatch(mismatch)
	return fmt.Errorf("%w: %w", errEndpointOwnerMismatch, attempt)
}

// --- address marks ---

const (
	// v1SeenMark lets the next dials to an old node start with v1 instead of
	// paying for a refused TLS attempt each time. It never overrides an
	// endpoint bound to v2, nor a hold.
	v1SeenMark = 24 * time.Hour
	// legacyRefusalHoldBase and legacyRefusalHoldMax bound the hold of the
	// fall back to v1 at an address whose v1 answer was refused because the
	// peer requires v2: 5 minutes after the first refusal, doubled after each
	// further one, at most 6 hours. A man in the middle can provoke such a
	// refusal at will, so it costs the address nothing but this hold — no
	// failure, no score, no cooldown — and the hold never delays a v2
	// attempt.
	legacyRefusalHoldBase = 5 * time.Minute
	legacyRefusalHoldMax  = 6 * time.Hour
	// v2RetryBase and v2RetryMax bound the v2 retry backoff: after a v2
	// attempt that ended without v2 — the address answered v1 and the v1
	// answer was refused or held — the next dial to the address waits 30 s,
	// doubled per further such end, at most 30 min. The connection manager's
	// own retry backoff still applies; the later of the two wins, because a
	// dial inside this backoff is refused before any socket. It is the
	// pacing of the next v2 attempt, not a penalty: score, failures and
	// setup failures are untouched, and a v2 session resets it.
	v2RetryBase = 30 * time.Second
	v2RetryMax  = 30 * time.Minute
	// maxSessionAddressMarks bounds the marks: addresses come from peer
	// exchange, so the map must not grow with what others tell us.
	maxSessionAddressMarks = 16384
)

// errLegacyRefusedBackoff is a fall back to v1 withheld because an earlier v1
// answer from the same address was refused (errLegacyRefusedPinned,
// errLegacyRefusedV2Bound) and the hold has not run out yet.
var errLegacyRefusedBackoff = errors.New("secure session: v1 fall back held after a refused v1 answer from this address")

// errV2RetryHeld is a dial refused before any socket because the address's v2
// retry backoff runs.
var errV2RetryHeld = errors.New("secure session: dial held by the v2 retry backoff of this address")

// sessionAddressMark is what the dialler remembers about one address.
type sessionAddressMark struct {
	// v1 is the address's recent v1 answer; nil when none is on record.
	v1 *v1Mark
	// legacyHold is the backoff after a refused v1 answer; nil when no such
	// refusal is on record since the last v2 session on the address.
	legacyHold *legacyRefusalHold
	// v2Retry is the backoff of the next v2 attempt; nil when no attempt
	// ended without v2 since the last v2 session on the address.
	v2Retry *v2RetryBackoff
	// touched is the last time the mark was written: when the map is full,
	// the least recently written mark goes first.
	touched time.Time
}

// v1Mark: the address answered as an old node; until then the next dial
// starts with v1.
type v1Mark struct {
	until time.Time
}

// legacyRefusalHold is the backoff of the fall back to v1 at one address.
type legacyRefusalHold struct {
	until time.Time
	// refusals since the last v2 session on the address; the next hold is
	// legacyRefusalHoldBase doubled refusals-1 times.
	refusals int
}

// v2RetryBackoff is the backoff of the next v2 attempt to one address.
type v2RetryBackoff struct {
	until time.Time
	// unanswered v2 attempts since the last v2 session on the address; the
	// next wait is v2RetryBase doubled unanswered-1 times.
	unanswered int
}

// counted reports whether the mark carries a count that only a v2 session
// may forget: its next backoff doubles on it.
func (m sessionAddressMark) counted() bool {
	return m.legacyHold != nil || m.v2Retry != nil
}

// forgettableAt reports whether the mark says nothing any more at now. A
// hold whose time ran out still carries its count — the next one doubles on
// it — so only a v2 session forgets it.
func (m sessionAddressMark) forgettableAt(now time.Time) bool {
	return (m.v1 == nil || !now.Before(m.v1.until)) && !m.counted()
}

// doubledBackoff is base doubled count-1 times, at most ceiling.
func doubledBackoff(base, ceiling time.Duration, count int) time.Duration {
	wait := base
	for i := 1; i < count && wait < ceiling; i++ {
		wait *= 2
	}
	return min(wait, ceiling)
}

// legacyRefusalHoldFor is the hold after the given number of consecutive
// refusals.
func legacyRefusalHoldFor(refusals int) time.Duration {
	return doubledBackoff(legacyRefusalHoldBase, legacyRefusalHoldMax, refusals)
}

// sessionAddressMarks remember, per address, what the dialler learnt about
// the session kind it may use there. Its own mutex is a leaf: nothing is
// called under it.
type sessionAddressMarks struct {
	clock func() time.Time
	// jitter returns a random wait in [0, upTo); set once at construction.
	jitter func(upTo time.Duration) time.Duration

	mu    sync.Mutex
	marks map[domain.PeerAddress]sessionAddressMark
}

func newSessionAddressMarks(clock func() time.Time) *sessionAddressMarks {
	return &sessionAddressMarks{clock: clock, jitter: randomWait, marks: make(map[domain.PeerAddress]sessionAddressMark)}
}

func (m *sessionAddressMarks) noteV1(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	mark := m.markForUpdateLocked(address, now)
	mark.v1 = &v1Mark{until: now.Add(v1SeenMark)}
	m.storeLocked(address, mark, now)
}

func (m *sessionAddressMarks) v1Seen(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark := m.marks[address].v1
	return mark != nil && m.clock().Before(mark.until)
}

// forgetV1 drops the v1 mark of address, so its next dial starts with v2.
func (m *sessionAddressMarks) forgetV1(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.v1 == nil {
		return
	}
	mark.v1 = nil
	m.storeLocked(address, mark, m.clock())
}

// noteLegacyRefused starts the next, longer hold of the fall back to v1 at
// address and returns its length. It also forgets the address's v1 mark: the
// v1 answer behind it may have been forged by whoever forged the refused
// one, and the next dial must try v2 first. It also moves the v2 retry
// backoff on (see below for why).
func (m *sessionAddressMarks) noteLegacyRefused(address domain.PeerAddress) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	mark := m.markForUpdateLocked(address, now)
	refusals := 1
	if mark.legacyHold != nil {
		refusals = mark.legacyHold.refusals + 1
	}
	hold := legacyRefusalHoldFor(refusals)
	mark.v1 = nil
	mark.legacyHold = &legacyRefusalHold{until: now.Add(hold), refusals: refusals}
	// The hold above only stops the fall back to v1; it does not delay v2.
	// The refusal is not charged to the address (isUnchargedDialRefusal:
	// whoever forged the v1 answer can repeat it at will), so nothing else
	// slows the connection manager down, and without pacing the refused
	// address would be redialled every few seconds for good. The v2 retry
	// backoff paces it instead; a dial it holds is not charged either, so
	// it touches neither the address's score nor its failure counts.
	mark.v2Retry = nextV2Retry(mark.v2Retry, now)
	m.storeLocked(address, mark, now)
	return hold
}

// randomWait is a uniformly random wait in [0, upTo), or zero for a bound
// that leaves no room.
func randomWait(upTo time.Duration) time.Duration {
	if upTo <= 0 {
		return 0
	}
	return rand.N(upTo)
}

// noteListenerRefused moves the v2 retry backoff of address on after the
// listener there refused a v2 attempt with its alert, with a jitter of up to
// half the wait on top, and returns the wait it now imposes. The v1 mark is
// left as it is: the refusal says nothing about v1.
func (m *sessionAddressMarks) noteListenerRefused(address domain.PeerAddress) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	mark := m.markForUpdateLocked(address, now)
	mark.v2Retry = nextV2Retry(mark.v2Retry, now)
	mark.v2Retry.until = mark.v2Retry.until.Add(m.jitter(mark.v2Retry.until.Sub(now) / 2))
	m.storeLocked(address, mark, now)
	return mark.v2Retry.until.Sub(now)
}

// noteV2Unanswered moves the v2 retry backoff of address on after a v2
// attempt there ended without v2, and returns the wait it now imposes.
func (m *sessionAddressMarks) noteV2Unanswered(address domain.PeerAddress) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	mark := m.markForUpdateLocked(address, now)
	mark.v2Retry = nextV2Retry(mark.v2Retry, now)
	m.storeLocked(address, mark, now)
	return mark.v2Retry.until.Sub(now)
}

func nextV2Retry(previous *v2RetryBackoff, now time.Time) *v2RetryBackoff {
	unanswered := 1
	if previous != nil {
		unanswered = previous.unanswered + 1
	}
	return &v2RetryBackoff{until: now.Add(doubledBackoff(v2RetryBase, v2RetryMax, unanswered)), unanswered: unanswered}
}

// legacyHeld reports whether the fall back to v1 at address is held now.
func (m *sessionAddressMarks) legacyHeld(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	hold := m.marks[address].legacyHold
	return hold != nil && m.clock().Before(hold.until)
}

// v2RetryHeld reports whether the next dial to address must wait for its v2
// retry backoff.
func (m *sessionAddressMarks) v2RetryHeld(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	retry := m.marks[address].v2Retry
	return retry != nil && m.clock().Before(retry.until)
}

// clearLegacyRefusal forgets the refusals and the unanswered v2 attempts of
// address once a v2 session on it succeeded: the next ones start the
// backoffs over.
func (m *sessionAddressMarks) clearLegacyRefusal(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || !mark.counted() {
		return
	}
	mark.legacyHold, mark.v2Retry = nil, nil
	m.storeLocked(address, mark, m.clock())
}

// storeLocked writes mark back, or drops it when it says nothing any more.
// Caller holds m.mu.
func (m *sessionAddressMarks) storeLocked(address domain.PeerAddress, mark sessionAddressMark, now time.Time) {
	if mark.forgettableAt(now) {
		delete(m.marks, address)
		return
	}
	mark.touched = now
	m.marks[address] = mark
}

// markForUpdateLocked returns the mark of address, making room for it first
// when it is new and the map is at its bound. What goes, in this order: a
// mark that says nothing any more; the least recently written mark without a
// count; the least recently written mark of all. The last choice loses a
// count, so that address's backoffs start over at their first step —
// accepted, it takes 16 384 fresher marks. One pass, made only when the map
// is full. Caller holds m.mu.
func (m *sessionAddressMarks) markForUpdateLocked(address domain.PeerAddress, now time.Time) sessionAddressMark {
	mark, present := m.marks[address]
	if present || len(m.marks) < maxSessionAddressMarks {
		return mark
	}
	var oldest, oldestUncounted markCandidate
	for candidate, other := range m.marks {
		if other.forgettableAt(now) {
			delete(m.marks, candidate)
			return mark
		}
		oldest.consider(candidate, other.touched)
		if !other.counted() {
			oldestUncounted.consider(candidate, other.touched)
		}
	}
	if oldestUncounted.found {
		delete(m.marks, oldestUncounted.address)
		return mark
	}
	delete(m.marks, oldest.address)
	return mark
}

// markCandidate is the least recently written mark seen so far in a scan.
type markCandidate struct {
	found   bool
	address domain.PeerAddress
	touched time.Time
}

func (c *markCandidate) consider(address domain.PeerAddress, touched time.Time) {
	if c.found && !touched.Before(c.touched) {
		return
	}
	c.found, c.address, c.touched = true, address, touched
}
