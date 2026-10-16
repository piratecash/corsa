package node

import "github.com/piratecash/corsa/internal/core/domain"

// addressIdentity is the identity recorded for an address together with what
// stands behind it (domain.PeerIdentityOrigin).
type addressIdentity struct {
	identity domain.PeerIdentity
	origin   domain.PeerIdentityOrigin
}

// claimedAddressIdentity is an identity named for an address — by a v1 hello
// or welcome, or a hello/welcome frame learnt over a v1 session — and proven
// by nothing. Peer exchange carries no identity today: addPeerAddress is
// called with none, and its identity parameter records a claim only for a
// caller that passes one (tests).
func claimedAddressIdentity(id domain.PeerIdentity) addressIdentity {
	return addressIdentity{identity: id, origin: domain.PeerIdentityClaimed}
}

// identityOriginOf is the origin of the identity a session names: proven
// when the session proved it over v2, claimed otherwise.
func identityOriginOf(proven bool) domain.PeerIdentityOrigin {
	if proven {
		return domain.PeerIdentityProvenV2
	}
	return domain.PeerIdentityClaimed
}

// restoredIdentityOrigin is the origin of an identity read from peers.json:
// the one written beside it, or claimed when there is none or one this build
// does not know — an identity is never taken for proven on the word of a
// file that does not say so.
func restoredIdentityOrigin(written domain.PeerIdentityOrigin) domain.PeerIdentityOrigin {
	if written.Valid() {
		return written
	}
	return domain.PeerIdentityClaimed
}

// addressIdentities is Service.peerIDs: address → the identity last recorded
// for it, with its origin. It is guarded by peerMu like the field.
type addressIdentities map[domain.PeerAddress]addressIdentity

// identityOf is the identity recorded for address; the zero identity when
// none is.
func (m addressIdentities) identityOf(address domain.PeerAddress) domain.PeerIdentity {
	return m[address].identity
}

// record writes next for address and reports whether the entry changed. A
// claim never replaces what v2 recorded: a v1 peer could otherwise rename an
// address a v2 session proved.
func (m addressIdentities) record(address domain.PeerAddress, next addressIdentity) bool {
	current, present := m[address]
	if present && current.origin == domain.PeerIdentityProvenV2 && next.origin != domain.PeerIdentityProvenV2 {
		return false
	}
	if present && current == next {
		return false
	}
	m[address] = next
	return true
}
