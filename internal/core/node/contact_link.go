package node

import (
	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
)

// PeerContactLink exports a locally known public key binding without looking
// up the peer on the network or importing the contact into the trust store.
func (s *Service) PeerContactLink(peer domain.PeerIdentity) string {
	if peer.IsZero() {
		return ""
	}
	address := peer.String()
	s.knowledgeMu.RLock()
	pubKey, boxKey, boxSig := s.pubKeys[address], s.boxKeys[address], s.boxSigs[address]
	s.knowledgeMu.RUnlock()
	if s.trust != nil {
		s.trust.mu.RLock()
		row, trusted := s.trust.contacts[address]
		s.trust.mu.RUnlock()
		if trusted {
			// A cached proof can fill a gap only when it covers the current
			// trusted key pair. An older encryption key must stay retired.
			if row.BoxSignature != "" || row.PubKey != pubKey || row.BoxKey != boxKey {
				pubKey, boxKey, boxSig = row.PubKey, row.BoxKey, row.BoxSignature
			}
		}
	}
	if pubKey == "" || boxKey == "" || boxSig == "" {
		return ""
	}
	link, err := contactlink.BuildVerified(contactlink.Contact{
		Address: peer,
		Network: domain.NetworkID(networkName),
		PubKey:  domain.PeerPublicKey(pubKey),
		BoxKey:  domain.PeerBoxKey(boxKey),
		BoxSig:  domain.PeerBoxSignature(boxSig),
	})
	if err != nil {
		return ""
	}
	return link
}
