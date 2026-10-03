package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
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
// v2 could prove with.
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
		store:        loadSecureSessionStore(storePath, clock),
		marks:        newSessionAddressMarks(clock),
	}
}

// refuseLegacyIdentity reports errLegacyRefusedPinned for a v1 session whose
// hello or welcome names an identity that proved v2 — on accept and on dial
// alike. A v1-only build pins nothing and refuses nothing.
func (s *Service) refuseLegacyIdentity(address string) error {
	if s.sessionMode() == sessionv2.ModeLegacyOnly {
		return nil
	}
	id := domain.PeerIdentityFromWire(address)
	if !id.IsZero() && s.secureSessions.store.identityPinned(id) {
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

	welcome := s.welcomeFrame("", remoteIPFromString(metered.RemoteAddr().String()))
	session, err := sessionv2.Accept(s.runCtx, &peekedConn{Conn: metered, reader: reader},
		s.secureSessions.local, welcome, s.secureSessions.certificates, sessionv2.DefaultTimeouts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("secure session: %w", err)
	}
	conn, err := session.Conn()
	if err != nil {
		return nil, nil, nil, err
	}
	peer, err := session.Peer()
	if err != nil {
		return nil, nil, nil, err
	}
	// The protection this session earns must be on disk before the session
	// counts: otherwise a restart would forget the pin and let v1 back in.
	if err := s.secureSessions.store.noteProvenInbound(peer.Identity); err != nil {
		_ = conn.Close()
		return nil, nil, nil, err
	}
	return conn, bufio.NewReader(conn), &peer, nil
}

// peekedConn reads through the buffered reader that holds the peeked first
// byte, so the TLS handshake sees the connection from its first byte.
type peekedConn struct {
	net.Conn
	reader *bufio.Reader
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
	advertiseResult := validateAdvertisedAddress(remoteAddr, hello)
	verified := &connauth.State{Hello: hello, Verified: true}
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

// errLegacyRefusedV2Bound is a v1 answer from an endpoint bound to v2: a
// downgrade, refused rather than followed.
var errLegacyRefusedV2Bound = errors.New("secure session: v1 answer from an endpoint bound to v2")

// dialledTransport is a dialled connection ready for a session: raw is the
// TCP socket (keepalive), metered counts its bytes, wire carries the frames
// (TLS for v2, metered itself for v1), and proven is the v2 peer or nil.
type dialledTransport struct {
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
// one that failed otherwise, and never for an endpoint bound to v2.
func (s *Service) dialPeerTransport(ctx context.Context, address domain.PeerAddress, dialRaw rawDialer) (*dialledTransport, error) {
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
		return &dialledTransport{raw: raw, metered: metered, wire: metered}, nil
	}
	mode := s.sessionMode()
	if mode == sessionv2.ModeLegacyOnly {
		return dial(ctx)
	}
	required := mode == sessionv2.ModeV2Only || s.secureSessions.store.endpointRequiresV2(address)
	if !required && s.secureSessions.marks.v1Seen(address) {
		return dial(ctx)
	}
	transport, err := dial(ctx)
	if err != nil {
		return nil, err
	}
	session, err := sessionv2.Dial(ctx, transport.metered, s.secureSessions.local, s.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	switch {
	case err == nil:
		return s.provenTransport(transport, session, address)
	case !errors.Is(err, sessionv2.ErrPeerSpeaksV1):
		return nil, err
	case mode == sessionv2.ModeV2Only:
		return nil, fmt.Errorf("%w: %w", errV1RefusedV2Only, err)
	case required:
		log.Warn().Str("peer", string(address)).Msg("secure_session_downgrade_refused")
		return nil, fmt.Errorf("%w: %w", errLegacyRefusedV2Bound, err)
	}
	log.Info().Str("peer", string(address)).Msg("secure_session_peer_speaks_v1")
	s.secureSessions.marks.noteV1(address)
	return dial(ctx)
}

// dialPeerTransportForCM is the connection manager's dial: its socket is paid
// for by the CM slot reservation taken before the dial starts.
func (s *Service) dialPeerTransportForCM(ctx context.Context, address domain.PeerAddress) (*dialledTransport, error) {
	return s.dialPeerTransport(ctx, address, func(ctx context.Context) (net.Conn, error) {
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
// earned: the peer's identity is pinned and the dialled endpoint bound —
// protected when the peer is one of this node's contacts. A protection that
// could not be written fails the dial: the session is not established on it.
func (s *Service) provenTransport(transport *dialledTransport, session sessionv2.ProvenSession, address domain.PeerAddress) (*dialledTransport, error) {
	wire, err := session.Conn()
	if err != nil {
		return nil, err
	}
	peer, err := session.Peer()
	if err != nil {
		return nil, err
	}
	protected := s.trust != nil && s.trust.isTrustedContact(peer.Identity)
	if err := s.secureSessions.store.noteProvenOutbound(address, peer.Identity, protected); err != nil {
		_ = wire.Close()
		return nil, err
	}
	transport.wire, transport.proven = wire, &peer
	return transport, nil
}

// --- address marks ---

const (
	// v1SeenMark lets the next dials to an old node start with v1 instead of
	// paying for a refused TLS attempt each time. It never overrides an
	// endpoint bound to v2.
	v1SeenMark = 24 * time.Hour
	// maxSessionAddressMarks bounds the marks: addresses come from peer
	// exchange, so the map must not grow with what others tell us. A v1
	// mark protects nothing, so dropping one costs one TLS attempt.
	maxSessionAddressMarks = 16384
)

// sessionAddressMarks remember the addresses that answered as old nodes. Its
// own mutex is a leaf: nothing is called under it.
type sessionAddressMarks struct {
	clock func() time.Time

	mu sync.Mutex
	v1 map[domain.PeerAddress]time.Time
}

func newSessionAddressMarks(clock func() time.Time) *sessionAddressMarks {
	return &sessionAddressMarks{clock: clock, v1: make(map[domain.PeerAddress]time.Time)}
}

func (m *sessionAddressMarks) noteV1(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	if _, present := m.v1[address]; !present && len(m.v1) >= maxSessionAddressMarks {
		for candidate, until := range m.v1 {
			if !now.Before(until) || len(m.v1) >= maxSessionAddressMarks {
				delete(m.v1, candidate)
			}
			if len(m.v1) < maxSessionAddressMarks {
				break
			}
		}
	}
	m.v1[address] = now.Add(v1SeenMark)
}

func (m *sessionAddressMarks) v1Seen(address domain.PeerAddress) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clock().Before(m.v1[address])
}
