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
//     Such a refusal is not charged to the address; it holds the fall back to
//     v1 at that address for a while (legacyRefusalHoldBase), and the next
//     dials there start with v2 — a v2 attempt is never held.
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
// refusals of a peer that requires v2 (isOutboundLegacyRefusal), although a
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
	if proof, proven := peer.Proof(); proven {
		if err := s.onIdentityProvenV2(s.runCtx, proof); err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
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
// one that failed otherwise, never for an endpoint bound to v2, and never
// while the v1 dial to the address is held after a refused v1 answer.
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
	session, err := sessionv2.Dial(ctx, transport.metered, s.secureSessions.local, s.nodeHelloFrame(), sessionv2.AnyPeer(), sessionv2.DefaultTimeouts)
	switch {
	case err == nil:
		return s.provenTransport(ctx, transport, session, address)
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

// isOutboundLegacyRefusal reports whether a dial ended because this node
// refused a v1 answer from a peer that requires v2, held the fall back to v1
// after such a refusal, or held the dial itself on the v2 retry backoff those
// refusals start. The man in the middle that forged the answer can
// repeat it at will, so the address is not charged for it.
func isOutboundLegacyRefusal(err error) bool {
	return errors.Is(err, errLegacyRefusedPinned) ||
		errors.Is(err, errLegacyRefusedV2Bound) ||
		errors.Is(err, errLegacyRefusedBackoff) ||
		errors.Is(err, errV2RetryHeld)
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
// earned: the peer's identity is pinned and the dialled endpoint bound with an
// ordinary binding (addressDialProtection). A protection that
// could not be written fails the dial: the session is not established on it.
// Once it is written, the legacy connections naming the same identity are
// closed and their routing residue forgotten (onIdentityProvenV2), before this
// session is registered.
func (s *Service) provenTransport(ctx context.Context, transport *dialledTransport, session sessionv2.ProvenSession, address domain.PeerAddress) (*dialledTransport, error) {
	wire, err := session.Conn()
	if err != nil {
		return nil, err
	}
	peer, err := session.Peer()
	if err != nil {
		return nil, err
	}
	if _, err := s.secureSessions.store.noteProvenOutbound(address, peer.Identity, addressDialProtection()); err != nil {
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

// addressDialProtection is the protection every dial path asks for today:
// none. Every path dials an address — the connection manager's candidate,
// the sender of the traffic for syncPeer, a gossip target for the notice —
// and none dials "to X". An address dial may reach whoever stands on the
// address, so it creates and renews only ordinary bindings, however pinned
// the proven identity is; otherwise a peer that once stood on the path of an
// address dial (a Tor exit, say) would earn a protection that never expires.
// Protection is for a dial with the intent "to X" that proved X, X being a
// contact (pool K) or a relation (pool R); the first such path brings it.
// Protected bindings already in the store stay as they are: they are never
// lowered automatically.
func addressDialProtection() endpointProtection {
	return endpointProtectionNone()
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

	mu    sync.Mutex
	marks map[domain.PeerAddress]sessionAddressMark
}

func newSessionAddressMarks(clock func() time.Time) *sessionAddressMarks {
	return &sessionAddressMarks{clock: clock, marks: make(map[domain.PeerAddress]sessionAddressMark)}
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
	// The refusal is not charged to the address (isOutboundLegacyRefusal:
	// whoever forged the v1 answer can repeat it at will), so nothing else
	// slows the connection manager down, and without pacing the refused
	// address would be redialled every few seconds for good. The v2 retry
	// backoff paces it instead; a dial it holds is not charged either, so
	// it touches neither the address's score nor its failure counts.
	mark.v2Retry = nextV2Retry(mark.v2Retry, now)
	m.storeLocked(address, mark, now)
	return hold
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

// clearV2Retry forgets the v2 retry backoff of address alone.
func (m *sessionAddressMarks) clearV2Retry(address domain.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mark, present := m.marks[address]
	if !present || mark.v2Retry == nil {
		return
	}
	mark.v2Retry = nil
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
