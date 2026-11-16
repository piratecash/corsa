package node

import (
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Bound local policy independently of the space reserved on disk.
const maxReservedPinPeerApprovals = maxPinnedIdentities - ordinarySessionPinLimit

func approvedReservedPinPeers(values []string) (map[domain.PeerIdentity]struct{}, error) {
	if len(values) > maxReservedPinPeerApprovals {
		return nil, fmt.Errorf("reserved pin peers exceed limit %d", maxReservedPinPeerApprovals)
	}
	if len(values) == 0 {
		return nil, nil
	}
	peers := make(map[domain.PeerIdentity]struct{}, len(values))
	for i, value := range values {
		id, err := domain.ParsePeerIdentity(value)
		if err != nil || id.IsZero() {
			return nil, fmt.Errorf("invalid reserved pin peer at position %d", i+1)
		}
		peers[id] = struct{}{}
	}
	return peers, nil
}

func (s *secureSessions) sessionPinAdmission(id domain.PeerIdentity, needsPin bool) sessionPinAdmission {
	_, approved := s.reservedPinPeers[id]
	return sessionPinAdmission{allowEphemeral: !needsPin, reserve: approved}
}

// Only a persisted pin authorizes a new retained address identity after the
// high-water mark. Connection-bound routing has its own TLS proof.
func (s *secureSessionStore) suppressesUnpinnedAddressIdentity(id domain.PeerIdentity) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unreadable || s.locationUnsafe {
		return true
	}
	return s.aboveOrdinarySessionPinLimitLocked(id)
}
