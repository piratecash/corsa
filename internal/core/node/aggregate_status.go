package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/rs/zerolog/log"
)

// computeAggregateStatusLocked derives the aggregate network status from the
// current contents of s.health. The algorithm mirrors the logic previously
// duplicated in the Desktop layer (networkStatusSummary) and is now the
// single source of truth.
//
// Must be called with s.mu held (at least read lock).
func (s *Service) computeAggregateStatusLocked(now time.Time) domain.AggregateStatusSnapshot {
	var usable, stalled, reconnecting, pending int

	healthAddrs := make(map[domain.PeerAddress]struct{}, len(s.health))
	for _, health := range s.health {
		healthAddrs[health.Address] = struct{}{}
		state := s.computePeerStateAtLocked(health, now)
		switch state {
		case peerStateHealthy, peerStateDegraded:
			usable++
		case peerStateStalled:
			stalled++
		case peerStateReconnecting:
			reconnecting++
		}
		pending += len(s.pending[health.Address])
	}

	// Count pending frames for peers that have no health entry yet
	// (e.g. queued via queuePeerFrame before the first connection).
	for addr, frames := range s.pending {
		if _, ok := healthAddrs[addr]; !ok {
			pending += len(frames)
		}
	}

	// Count orphaned frames — persisted backlog from legacy key migration
	// that could not be matched to a current peer. These survive restarts
	// and represent unsent data, so they belong in the pending total.
	for _, frames := range s.orphaned {
		pending += len(frames)
	}

	connected := usable + stalled
	total := connected + reconnecting

	var status domain.NetworkStatus
	switch {
	case total == 0:
		status = domain.NetworkStatusOffline
	case connected == 0:
		status = domain.NetworkStatusReconnecting
	case usable == 0:
		// Peers exist but none can route — functionally limited.
		status = domain.NetworkStatusLimited
	case usable == 1:
		status = domain.NetworkStatusLimited
	case usable*2 < connected:
		// Less than half of currently live peers are usable.
		status = domain.NetworkStatusWarning
	default:
		status = domain.NetworkStatusHealthy
	}

	return domain.AggregateStatusSnapshot{
		Status:          status,
		UsablePeers:     usable,
		ConnectedPeers:  connected,
		TotalPeers:      total,
		PendingMessages: pending,
	}
}

// refreshAggregateStatusLocked recomputes the aggregate network status and
// stores it in s.aggregateStatus. If the status changed, it logs the
// transition. Must be called with s.mu held (write lock).
func (s *Service) refreshAggregateStatusLocked() {
	now := time.Now().UTC()
	next := s.computeAggregateStatusLocked(now)

	prev := s.aggregateStatus
	s.aggregateStatus = next

	if prev.Status != next.Status {
		log.Info().
			Str("from", string(prev.Status)).
			Str("to", string(next.Status)).
			Int("usable", next.UsablePeers).
			Int("connected", next.ConnectedPeers).
			Int("total", next.TotalPeers).
			Int("pending", next.PendingMessages).
			Msg("aggregate_status_change")
	}
}

// refreshAggregateStatus acquires the write lock and recomputes the
// materialized aggregate status. Called periodically from bootstrapLoop
// to catch time-based peer-state drift (e.g. peers silently aging from
// healthy → degraded → stalled without explicit disconnect events) and
// to keep PendingMessages up to date even between peer-state transitions.
func (s *Service) refreshAggregateStatus() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshAggregateStatusLocked()
}

// AggregateStatus returns a point-in-time snapshot of the node's aggregate
// network health. Safe to call from any goroutine.
func (s *Service) AggregateStatus() domain.AggregateStatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.aggregateStatus
}

// aggregateStatusFrame builds the protocol frame for the
// fetch_aggregate_status local RPC command.
func (s *Service) aggregateStatusFrame() protocol.Frame {
	snap := s.AggregateStatus()
	return protocol.Frame{
		Type: "aggregate_status",
		AggregateStatus: &protocol.AggregateStatusFrame{
			Status:          string(snap.Status),
			UsablePeers:     snap.UsablePeers,
			ConnectedPeers:  snap.ConnectedPeers,
			TotalPeers:      snap.TotalPeers,
			PendingMessages: snap.PendingMessages,
		},
	}
}
