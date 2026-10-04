package sessionv2

import (
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// session.go is what a successful v2 handshake hands out, and the only place
// "proven" can come from: ProvenSession is returned by Dial and Accept and by
// nothing else, its fields are unexported, and its zero value refuses every
// question.

// ErrZeroSession is a ProvenSession that no handshake produced.
var ErrZeroSession = errors.New("sessionv2: not a proven session")

// ErrRotationDue is a write refused because this end has written about 2^24
// records under one key. Go never initiates TLS KeyUpdate, so the session is
// re-established instead (docs/protocol/session_v2.md, "Key rotation").
var ErrRotationDue = errors.New("sessionv2: record limit reached, reconnect")

// rotationRecords is the conservative per-direction record budget.
const rotationRecords uint64 = 1 << 24

// maxRecordPlaintext is the TLS record size; with dynamic record sizing off,
// a Write of n bytes takes at most 1 + n/maxRecordPlaintext records.
const maxRecordPlaintext = 16384

// Peer is the other end as the handshake proved it.
type Peer struct {
	Identity  domain.PeerIdentity
	PublicKey identity.PublicKey
	// Intro is the peer's hello (it dialled) or welcome (it listened),
	// verified field by field and NOT applied to anything yet: the caller
	// applies its metadata only now, after the proof.
	Intro protocol.Frame
	// proof is set by receiveProvenPeer once session_proof verified, and
	// nowhere else. The exported fields describe a peer; only this one says
	// a handshake proved it, so a Peer built by hand carries no proof.
	proof ProvenIdentity
}

// Proof is the proof this peer's identity was verified over its own
// connection; ok is false for a Peer no handshake produced.
func (p Peer) Proof() (ProvenIdentity, bool) {
	_, ok := p.proof.Identity()
	return p.proof, ok
}

// ProvenIdentity is an identity a v2 handshake proved over one connection
// (session_proof over that connection's TLS exporter). Its field is
// unexported and only receiveProvenPeer sets it, so holding one IS holding
// the result of that verification: anything that may be charged to, trusted
// as or keyed by a proven identity takes this type rather than a bare
// domain.PeerIdentity, and the compiler refuses a caller that has only a
// name. A v1 auth_session never yields one — its signature names neither the
// verifier nor the connection and can be relayed.
type ProvenIdentity struct {
	id domain.PeerIdentity
}

// Identity is the proven identity; ok is false for the zero value.
func (p ProvenIdentity) Identity() (domain.PeerIdentity, bool) {
	return p.id, !p.id.IsZero()
}

// ProvenSession is a v2 session whose peer proved its identity over this
// session's exporter.
type ProvenSession struct {
	state *provenState
}

type provenState struct {
	conn *sessionConn
	role Role
	peer Peer
}

// Peer is the proven peer.
func (s ProvenSession) Peer() (Peer, error) {
	if s.state == nil {
		return Peer{}, ErrZeroSession
	}
	return s.state.peer, nil
}

// Role is this end's role in the session.
func (s ProvenSession) Role() (Role, error) {
	if s.state == nil {
		return 0, ErrZeroSession
	}
	return s.state.role, nil
}

// Conn is the protected connection the session's frames travel on. Writes
// count records toward the rotation limit.
func (s ProvenSession) Conn() (net.Conn, error) {
	if s.state == nil {
		return nil, ErrZeroSession
	}
	return s.state.conn, nil
}

// sessionConn is the TLS connection with this end's record count. The count
// is an upper bound and is never reset: when the peer asks for KeyUpdate Go
// rotates our write key too without telling us, and counting on is the
// conservative answer — at worst a reconnect comes early.
type sessionConn struct {
	*tls.Conn
	// records is shared by concurrent writers: tls.Conn allows concurrent
	// Write calls, so the budget check and the charge are one CAS.
	records atomic.Uint64
	limit   uint64
}

func newSessionConn(conn *tls.Conn) *sessionConn {
	return &sessionConn{Conn: conn, limit: rotationRecords}
}

// Close closes the socket without a TLS close_notify. The alert carries
// nothing this protocol uses — frames are lines, every record is
// authenticated, and a truncation can only cut an unfinished line the reader
// discards — and it is the one record the peer has often stopped reading by
// then: bytes one end counts as sent and the other never receives, which
// breaks the node's exact byte accounting.
func (c *sessionConn) Close() error {
	return c.NetConn().Close()
}

// Write refuses once the record budget is spent rather than sending past it.
func (c *sessionConn) Write(b []byte) (int, error) {
	cost := 1 + uint64(len(b))/maxRecordPlaintext
	for {
		spent := c.records.Load()
		if spent+cost > c.limit {
			return 0, ErrRotationDue
		}
		if c.records.CompareAndSwap(spent, spent+cost) {
			return c.Conn.Write(b)
		}
	}
}
