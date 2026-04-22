package node

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
)

// peersExchangeSnapshot caches the s.mu-protected slices used by
// buildPeerExchangeResponse (get_peers RPC).  Primed synchronously by
// primeHotReadSnapshots() in Run() before the listener opens, then rebuilt
// by hotReadsRefreshLoop every networkStatsSnapshotInterval under a short
// s.mu.RLock.  The RPC handler performs a single atomic load and never
// acquires s.mu — there is no synchronous fallback rebuild on the RPC
// goroutine, so the hot path is statically decoupled from s.mu.
//
// The sets are small (len(persistedMeta) + len(health)) and stable between
// reconnect events, so even a brief staleness window does not change
// peer-exchange outcomes materially — but under a writer storm on s.mu the
// snapshot lets get_peers return in microseconds instead of waiting behind a
// queued writer.
//
// Peer state transitions (markPeerConnected / markPeerDisconnected) do NOT
// force an out-of-band rebuild of this snapshot.  The eager refresh helper
// on the peer-transition path only rebuilds peer_health because
// rebuildPeersExchangeSnapshot calls peerProvider.Candidates() which
// re-acquires s.mu.RLock via its callbacks — doing so from the caller's
// goroutine (session teardown, shutdown drain) couples those paths to
// s.mu contention from other writers.  The 500 ms staleness window on
// get_peers is acceptable because it feeds gossip propagation, not the
// click-to-render UI paths.
type peersExchangeSnapshot struct {
	// knownInPersistedMeta holds every address that has a persistedMeta row,
	// even if the announce decision is still pending.  Used by the handler
	// to distinguish "no decision yet → propagate by default" from
	// "decision is not-announceable → suppress".
	knownInPersistedMeta map[domain.PeerAddress]struct{}
	// announceable is the subset of knownInPersistedMeta whose
	// persistedMeta.AnnounceState == announceStateAnnounceable.
	announceable map[domain.PeerAddress]struct{}
	// inboundConnected is the pre-filtered list of health entries with
	// Direction == peerDirectionInbound && Connected == true.  Handler still
	// applies shouldHidePeerExchangeAddress and isAnnounceable (pure
	// functions and snapshot lookups) outside the lock.
	inboundConnected []domain.PeerAddress
	// candidateAddresses holds the addresses returned by
	// peerProvider.Candidates() captured at refresh time.  The candidates
	// helper itself acquires s.mu.RLock via its HealthFn / ConnectedFn /
	// NetworksFn / BannedIPsFn / VersionLockedOutFn / RemoteBannedFn
	// callbacks, so invoking it on the RPC path would re-couple get_peers
	// to s.mu and reintroduce the writer-storm starvation the snapshot is
	// meant to break.  Computing the list inside the refresher moves that
	// coupling onto the background goroutine (which is allowed to stall
	// under bounded staleness) and leaves the handler lock-free.
	candidateAddresses []domain.PeerAddress
	generatedAt        time.Time
}

// loadPeersExchangeSnapshot atomically retrieves the last-published peers
// exchange snapshot.  Guaranteed non-nil on the RPC path once Run() has
// executed primeHotReadSnapshots() before opening the listener.  Returns
// nil only for unit tests that bypass Run() without priming manually.
func (s *Service) loadPeersExchangeSnapshot() *peersExchangeSnapshot {
	return s.peersExchangeSnap.Load()
}

// rebuildPeersExchangeSnapshot reads persistedMeta and health under a short
// s.mu.RLock and stores the resulting snapshot atomically.  The RPC handler
// runs its filtering logic (shouldHidePeerExchangeAddress, isAnnounceable,
// splitHostPort) on the snapshot without touching s.mu.
func (s *Service) rebuildPeersExchangeSnapshot() {
	log.Trace().Msg("peers_exchange_snapshot_refresh_begin")

	s.mu.RLock()

	announceable := make(map[domain.PeerAddress]struct{}, len(s.persistedMeta))
	knownInPersistedMeta := make(map[domain.PeerAddress]struct{}, len(s.persistedMeta))
	for addr, pm := range s.persistedMeta {
		if pm == nil {
			continue
		}
		knownInPersistedMeta[addr] = struct{}{}
		if pm.AnnounceState == announceStateAnnounceable {
			announceable[addr] = struct{}{}
		}
	}

	inboundConnected := make([]domain.PeerAddress, 0, len(s.health))
	for addr, h := range s.health {
		if h.Direction != peerDirectionInbound || !h.Connected {
			continue
		}
		inboundConnected = append(inboundConnected, addr)
	}

	s.mu.RUnlock()
	log.Trace().Int("persisted", len(knownInPersistedMeta)).Int("inbound", len(inboundConnected)).Msg("peers_exchange_snapshot_rlock_released")

	// Capture the candidate list OUTSIDE the s.mu.RLock window.  Candidates()
	// acquires pp.mu.RLock internally and, via its callbacks, reacquires
	// s.mu.RLock per call — doing this from inside the snapshot's s.mu.RLock
	// window would be a recursive RLock and is forbidden by sync.RWMutex.
	// The refresher itself may stall here if a writer is holding s.mu; that
	// is the bounded-staleness price we pay so the RPC path stays lock-free.
	var candidateAddresses []domain.PeerAddress
	if s.peerProvider != nil {
		cands := s.peerProvider.Candidates()
		candidateAddresses = make([]domain.PeerAddress, 0, len(cands))
		for _, c := range cands {
			candidateAddresses = append(candidateAddresses, c.Address)
		}
	}

	snap := &peersExchangeSnapshot{
		knownInPersistedMeta: knownInPersistedMeta,
		announceable:         announceable,
		inboundConnected:     inboundConnected,
		candidateAddresses:   candidateAddresses,
		generatedAt:          time.Now().UTC(),
	}
	s.peersExchangeSnap.Store(snap)
	log.Trace().
		Int("persisted", len(knownInPersistedMeta)).
		Int("inbound", len(inboundConnected)).
		Int("candidates", len(candidateAddresses)).
		Msg("peers_exchange_snapshot_refresh_end")
}

// isAnnounceable applies the persisted announce-state gate to a single
// address using cached sets.  Pure function of the snapshot — safe to call
// from the RPC handler without any lock.  Addresses with no persistedMeta row
// fall back to "allow" so bootstrap/manual peers that have never been through
// a handshake still propagate.
func (snap *peersExchangeSnapshot) isAnnounceable(addr domain.PeerAddress) bool {
	if snap == nil {
		return true
	}
	if _, ok := snap.knownInPersistedMeta[addr]; !ok {
		return true
	}
	_, ok := snap.announceable[addr]
	return ok
}

// peersExchangeSnapPtr wraps atomic.Pointer[peersExchangeSnapshot] for the
// Service struct — mirror of networkStatsSnapPtr / peerHealthSnapPtr.
type peersExchangeSnapPtr = atomic.Pointer[peersExchangeSnapshot]
