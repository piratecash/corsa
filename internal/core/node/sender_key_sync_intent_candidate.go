package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// The sole named policy is an UNUSED private candidate, never a Service/config
// option. It selects only extra fan-out; the previous hop stays an address.
type senderKeySyncPolicy uint8

const (
	senderKeySyncAddress senderKeySyncPolicy = iota
	senderKeySyncProvenFanout
)

type senderKeySyncCandidate struct {
	address    domain.PeerAddress
	named      bool
	owner      *Service
	session    *peerSession
	connID     domain.ConnID
	peer       *sessionv2.Peer
	proof      sessionv2.ProvenIdentity
	identity   domain.PeerIdentity
	generation uint64
}

// triggerSenderKeySyncIntentCandidate has no active/default production caller.
// It shares the ORIGINAL admission, single-flight, cooldown, global3, per-hop,
// release-once and lifecycle body; no direct pass entry bypasses that admission.
func (s *Service) triggerSenderKeySyncIntentCandidate(prevHop domain.PeerAddress, sender string, ownedSession *peerSession) {
	s.triggerSenderKeySyncWithPolicy(prevHop, sender, ownedSession, senderKeySyncProvenFanout)
}

// Called only inside the ORIGINAL one locked selection pass. This never reads
// identity maps/pins/endpoints or the missing sender's identity. No N-sel proof
// or nonzero selectedGeneration prerequisite is introduced.
func (s *Service) captureSenderKeySyncCandidateLocked(address domain.PeerAddress, session *peerSession, policy senderKeySyncPolicy) senderKeySyncCandidate {
	c := senderKeySyncCandidate{address: address}
	if policy != senderKeySyncProvenFanout || s.sessionMode() == sessionv2.ModeLegacyOnly {
		return c
	}
	proof, ok := session.provenIdentity()
	x, valid := proof.Identity()
	if !ok || !valid {
		return c // Original legacy/unproven fan-out remains address-purpose.
	}
	c.named, c.owner, c.session = true, s, session
	c.connID, c.peer, c.proof, c.identity = session.connID, session.proven, proof, x
	c.generation = session.selectedGeneration
	return c
}

func (s *Service) senderKeySyncCandidateCurrent(c senderKeySyncCandidate) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	p := c.session
	if !c.named || c.owner != s || p == nil || s.sessions[c.address] != p || p.address != c.address || c.connID == 0 || p.connID != c.connID || p.proven == nil || p.proven != c.peer || p.selectedGeneration != c.generation || p.eligibilityEnded.Load() || p.closedBy.Load() != nil || s.sessionMode() == sessionv2.ModeLegacyOnly {
		return false
	}
	proof, ok := p.provenIdentity()
	x, valid := proof.Identity()
	health := s.health[s.resolveHealthAddress(c.address)]
	return ok && valid && proof == c.proof && x == c.identity && p.peerIdentity == c.identity && c.peer.Identity == c.identity && health != nil && health.Connected
}

func (s *Service) syncSenderKeyCandidate(ctx context.Context, c senderKeySyncCandidate) int {
	if !c.named {
		return s.syncSenderKeys(ctx, c.address, nil)
	}
	if ctx.Err() != nil || !s.senderKeySyncCandidateCurrent(c) {
		return 0 // Stale NAMED selection never becomes an address dial/refill.
	}
	// peerMu was released before transport/store or any waits. This is an
	// admission-time snapshot, not a lease on the old wire through the new dial.
	intent, err := domain.DialToIdentity(c.identity)
	if err != nil {
		return 0
	}
	dialCtx, cancel := context.WithTimeout(ctx, syncRecoveryTimeout)
	defer cancel()
	s.logPeerExchangeSkipped(peerExchangePathSenderKeyFreshDial, c.address, peerExchangeSkipByNarrowRecovery)
	return s.syncPeerWithIntent(dialCtx, c.address, false, intent)
}
