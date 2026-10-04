package node

import (
	"net/netip"
	"strconv"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// penalty_subject.go names WHO per-peer punitive and budget state is charged
// to: the route quarantine and the disconnect and announce histories that arm
// it, the announce-plane token bucket and the request_resync debounce.
//
// An identity is a valid key only when the remote side PROVED it over the
// connection the event arrived on — a secure session v2 (session_proof over
// this connection's TLS exporter). A legacy (v1) session proves nothing this
// node can attribute:
//
//   - on a session this node DIALLED, the identity is whatever the welcome
//     frame says; the challenge travels the other way;
//   - on a connection this node ACCEPTED, auth_session signs
//     `corsa-session-auth-v1|<challenge>|<address>`, which names neither the
//     verifier nor the connection, so a node the victim dials can carry the
//     victim's signature to us and be taken for the victim.
//
// Charging such an identity let any legacy peer that named an unpinned
// identity quarantine, rate-limit and debounce the REAL owner of it. A legacy
// session is therefore charged by what this node itself observed about the
// connection, never by the identity it claims — so the protections still
// stop the connecting peer, and stop only it.
type penaltySubjectSpace uint8

const (
	// penaltySubjectUnset is the zero value and names nobody: state is never
	// charged to it, so a caller that could not say who sent the frame cannot
	// open one shared bucket that every such caller drains for the others.
	penaltySubjectUnset penaltySubjectSpace = iota
	// penaltySubjectProvenIdentity is an identity proven by a v2 session.
	// Nobody else can present it, so the same node reconnecting — from any
	// address, in either direction — meets the same record.
	penaltySubjectProvenIdentity
	// penaltySubjectDialledAddress is the host:port THIS node dialled for a
	// legacy session. It is ours, fixed for the session and identical across
	// reconnects to the same peer — what a quarantine needs to outlive a
	// flap.
	penaltySubjectDialledAddress
	// penaltySubjectInboundHost is the TCP source IP of a legacy connection
	// this node accepted from an external address. The port is dropped: it
	// changes on every reconnect. This is the key the IP ban already uses for
	// the same connections.
	penaltySubjectInboundHost
	// penaltySubjectConnection is a legacy connection accepted from loopback.
	// Every onion peer arrives from 127.0.0.1, so the source IP would let one
	// onion peer mute every other one; the connection is the only key that
	// belongs to this peer alone.
	penaltySubjectConnection
)

var penaltySubjectSpaceNames = map[penaltySubjectSpace]string{
	penaltySubjectUnset:          "unset",
	penaltySubjectProvenIdentity: "proven_identity",
	penaltySubjectDialledAddress: "dialled_address",
	penaltySubjectInboundHost:    "inbound_host",
	penaltySubjectConnection:     "connection",
}

func (s penaltySubjectSpace) String() string {
	if name, ok := penaltySubjectSpaceNames[s]; ok {
		return name
	}
	return "unknown"
}

// penaltySubject is the key of every per-peer punitive and budget record
// listed in the file comment. The fields are unexported and the only way in
// is a constructor, so a call site cannot produce a key without stating what
// it knows about the peer. It is comparable and is used as a map key.
type penaltySubject struct {
	space    penaltySubjectSpace
	identity domain.PeerIdentity
	address  domain.PeerAddress
	conn     domain.ConnID
}

// provenIdentitySubject is the proven-namespace key of an identity. It is
// what a READER asks with ("is the identity X quarantined?"); state is
// CHARGED to the proven namespace only through provenIdentitySubjectOf, from
// a v2 proof. A zero identity yields the unset subject.
func provenIdentitySubject(peer domain.PeerIdentity) penaltySubject {
	if peer.IsZero() {
		return penaltySubject{}
	}
	return penaltySubject{space: penaltySubjectProvenIdentity, identity: peer}
}

// provenIdentitySubjectOf is the subject of a v2 proof — the only way
// production code reaches the proven namespace.
func provenIdentitySubjectOf(proof sessionv2.ProvenIdentity) penaltySubject {
	id, _ := proof.Identity()
	return provenIdentitySubject(id)
}

// dialledAddressSubject keys state on the address this node dialled for a
// legacy session. A blank address yields the unset subject.
func dialledAddressSubject(address domain.PeerAddress) penaltySubject {
	if address == "" {
		return penaltySubject{}
	}
	return penaltySubject{space: penaltySubjectDialledAddress, address: address}
}

// acceptedLegacySubject keys state for a legacy connection this node
// accepted, from the socket's own remote address: the source IP for an
// external address, the connection itself for loopback or for an address
// that does not parse (an address we cannot read is not one we may share).
func acceptedLegacySubject(conn domain.ConnID, remoteAddr string) penaltySubject {
	host, external := acceptedExternalHost(remoteAddr)
	if !external {
		if conn == 0 {
			return penaltySubject{}
		}
		return penaltySubject{space: penaltySubjectConnection, conn: conn}
	}
	return penaltySubject{space: penaltySubjectInboundHost, address: domain.PeerAddress(host.String())}
}

// acceptedExternalHost is the source IP of an accepted connection when it is
// one a legacy subject or budget may be keyed on — parseable and not loopback.
// It is the ONE classification both the penalty subject and the datagram
// admission key use, so the two cannot disagree about which connections share
// a host.
func acceptedExternalHost(remoteAddr string) (netip.Addr, bool) {
	addrPort, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return netip.Addr{}, false
	}
	host := addrPort.Addr().Unmap()
	if !host.IsValid() || host.IsLoopback() {
		return netip.Addr{}, false
	}
	return host, true
}

// IsZero reports whether the subject names nobody.
func (p penaltySubject) IsZero() bool { return p.space == penaltySubjectUnset }

// provenIdentity returns the identity of a proven subject; ok is false for
// every legacy subject, whose state must never be read as the identity's.
func (p penaltySubject) provenIdentity() (domain.PeerIdentity, bool) {
	if p.space != penaltySubjectProvenIdentity {
		return domain.PeerIdentity{}, false
	}
	return p.identity, true
}

// String renders the subject for a log line, namespace first so two subjects
// carrying the same text in different namespaces never read as one peer.
func (p penaltySubject) String() string {
	switch p.space {
	case penaltySubjectProvenIdentity:
		return p.space.String() + ":" + p.identity.String()
	case penaltySubjectDialledAddress, penaltySubjectInboundHost:
		return p.space.String() + ":" + string(p.address)
	case penaltySubjectConnection:
		return p.space.String() + ":" + strconv.FormatUint(uint64(p.conn), 10)
	default:
		return p.space.String()
	}
}

// penaltySubject is the subject of a session this node dialled: the proven
// identity for v2, the dialled address for a legacy session.
func (session *peerSession) penaltySubject() penaltySubject {
	if proof, ok := session.provenIdentity(); ok {
		return provenIdentitySubjectOf(proof)
	}
	return dialledAddressSubject(session.address)
}

// provenIdentity is the v2 proof this dialled session carries; ok is false
// for a legacy session and for a peer description no handshake produced.
func (session *peerSession) provenIdentity() (sessionv2.ProvenIdentity, bool) {
	if session.proven == nil {
		return sessionv2.ProvenIdentity{}, false
	}
	return session.proven.Proof()
}

// connPenaltySubject is the subject of a connection this node accepted.
// Takes the peer-domain read lock through netCoreForID; a caller already
// holding peerMu resolves the core itself and uses penaltySubjectOfCore.
func (s *Service) connPenaltySubject(id domain.ConnID) penaltySubject {
	return penaltySubjectOfCore(id, s.netCoreForID(id))
}

// penaltySubjectOfCore is the proven identity when this connection's auth
// state carries a v2 proof, the legacy subject of its socket otherwise —
// including a relayable auth_session and a connection with no auth state
// yet. Reads only the core's own synchronised state, so it is safe under
// peerMu.
func penaltySubjectOfCore(id domain.ConnID, core *netcore.NetCore) penaltySubject {
	if core == nil {
		return acceptedLegacySubject(id, "")
	}
	if proof, ok := core.Auth().ProvenIdentity(); ok {
		return provenIdentitySubjectOf(proof)
	}
	return acceptedLegacySubject(id, core.RemoteAddr())
}

// routingSender is the neighbour a routing-plane frame arrived from, as the
// two facts the receive path needs and must not confuse:
//
//   - identity is the routing table's key — the name the session goes by.
//     A legacy session keeps its right to it: routes it announces are routes
//     via that identity;
//   - penalty is who the quarantine, the announce budget and the resync
//     debounce charge.
type routingSender struct {
	identity domain.PeerIdentity
	penalty  penaltySubject
}

// sessionRoutingSender is the sender behind a session this node dialled.
func sessionRoutingSender(session *peerSession) routingSender {
	return routingSender{identity: session.peerIdentity, penalty: session.penaltySubject()}
}

// inboundRoutingSender is the sender behind a connection this node accepted.
func (s *Service) inboundRoutingSender(id domain.ConnID) routingSender {
	return routingSender{identity: s.inboundPeerIdentity(id), penalty: s.connPenaltySubject(id)}
}
