package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/rs/zerolog/log"
)

// aggregateStatusHeartbeatInterval is the upper bound on how long a
// subscriber may remain stale if Publish dropped an async delivery
// because the inbox was full. ebus is intentionally lossy to protect
// publishers during storms, so dedup on the publisher side cannot rely
// on "subscribers saw at least one event for this state" — the first
// event during a storm is exactly the one most likely to be dropped.
// The heartbeat forces a re-publish of the current snapshot at most
// once per interval, even when content is byte-identical, so a dropped
// publish cannot leave the UI permanently stale. The 2 s bootstrap
// ticker in bootstrapLoop drives this (via refreshAggregateStatus),
// so the effective resync latency is bounded by max(tick, interval).
const aggregateStatusHeartbeatInterval = 5 * time.Second

// computeAggregateStatusLocked derives the aggregate network status from the
// current contents of s.health. The algorithm mirrors the logic previously
// duplicated in the Desktop layer (networkStatusSummary) and is now the
// single source of truth.
//
// Caller MUST hold s.peerMu at least for read (peer domain owns s.health)
// AND s.deliveryMu at least for read (delivery domain owns s.pending and
// s.orphaned, both consumed here).  Canonical order is peerMu → deliveryMu.
// Writer-preferring RWMutex semantics make a self-nested RLock unsafe if
// another writer queues between acquisitions, so this helper never calls
// RLock itself — the caller's lock scope must already cover both domains.
// This helper reads no status-domain fields, so s.statusMu is NOT required.
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

	target := s.cfg.EffectiveMaxOutgoingPeers()

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
		// More than half of currently live peers are stalled — network issues.
		status = domain.NetworkStatusWarning
	case usable < target/2:
		// Below 50% of outbound target — still building connectivity.
		status = domain.NetworkStatusLimited
	case usable < target:
		// Progressing toward target but not yet at full capacity.
		status = domain.NetworkStatusWarning
	default:
		// Usable peers meet or exceed target — fully healthy.
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
// transition.
//
// Caller MUST hold:
//   - s.peerMu at least for read (peer-domain inputs for
//     computeAggregateStatusLocked and maybeRecomputeVersionPolicyPeriodic)
//   - s.deliveryMu at least for read (delivery-domain inputs for
//     computeAggregateStatusLocked)
//   - s.statusMu.Lock (status domain — s.aggregateStatus is written here,
//     and maybeRecomputeVersionPolicyPeriodic may write s.versionPolicy)
//
// Canonical order: peerMu → deliveryMu → statusMu with statusMu INNERMOST.
func (s *Service) refreshAggregateStatusLocked() {
	now := time.Now().UTC()

	// Periodic repair path for version policy: expire stale reporter
	// observations within a bounded window (versionPolicyRepairInterval)
	// even when no new observation events arrive.
	s.maybeRecomputeVersionPolicyPeriodic(now)

	next := s.computeAggregateStatusLocked(now)
	next.ComputedAt = now

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

// refreshAggregatePendingLocked recounts only the PendingMessages field of
// the cached aggregate snapshot without recomputing peer states or version
// policy. Called from queue mutation paths (enqueue, flush, drop) where
// only the pending total changes; peer health and connectivity remain the
// same.
//
// Caller MUST hold:
//   - s.peerMu at least for read (s.health is iterated to discover which
//     addresses own pending frames)
//   - s.deliveryMu at least for read (s.pending / s.orphaned live in the
//     delivery domain)
//   - s.statusMu.Lock (status domain — s.aggregateStatus.PendingMessages is
//     written here)
//
// Canonical order: peerMu → deliveryMu → statusMu with statusMu INNERMOST.
func (s *Service) refreshAggregatePendingLocked() {
	var pending int
	healthAddrs := make(map[domain.PeerAddress]struct{}, len(s.health))
	for _, health := range s.health {
		healthAddrs[health.Address] = struct{}{}
		pending += len(s.pending[health.Address])
	}
	for addr, frames := range s.pending {
		if _, ok := healthAddrs[addr]; !ok {
			pending += len(frames)
		}
	}
	for _, frames := range s.orphaned {
		pending += len(frames)
	}
	s.aggregateStatus.PendingMessages = pending
}

// refreshAggregateStatus acquires the full canonical lock stack
// (peerMu → deliveryMu → statusMu) and recomputes the materialized
// aggregate status, funnelling through the heartbeat-aware publisher.
// Called periodically from bootstrapLoop to catch time-based peer-state
// drift (e.g. peers silently aging from healthy → degraded → stalled
// without explicit disconnect events) and to keep PendingMessages current
// between peer-state transitions. Routing the ticker through the
// publisher is deliberate: it is the heartbeat that protects subscribers
// against a dropped initial publish during a storm, so the UI eventually
// converges to the true snapshot even if the inbox was full earlier.
//
// peerMu is acquired as a writer because maybeRecomputeVersionPolicyPeriodic
// (invoked transitively via refreshAggregateStatusLocked) may mutate
// s.peerBuilds / s.peerIDs when an observation window expires.
//
// Canonical order: peerMu → deliveryMu → statusMu.
func (s *Service) refreshAggregateStatus() {
	log.Trace().Str("site", "refreshAggregateStatus").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "refreshAggregateStatus").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.deliveryMu.RLock()
	s.statusMu.Lock()
	defer func() {
		s.statusMu.Unlock()
		s.deliveryMu.RUnlock()
		s.peerMu.Unlock()
		log.Trace().Str("site", "refreshAggregateStatus").Str("phase", "lock_released").Msg("peer_mu_writer")
	}()
	s.publishAggregateStatusChangedLocked()
}

// publishAggregateStatusChangedLocked recomputes the aggregate status and
// publishes TopicAggregateStatusChanged under two conditions:
//
//  1. The semantic payload (ignoring the ComputedAt heartbeat) differs
//     from the last published snapshot. This collapses byte-identical
//     bursts triggered by cascades like cm_session_setup_failed, where
//     several peers transition through identical internal states in
//     quick succession and every redundant publish used to force the
//     Desktop layer to rebuild a full NodeStatus snapshot.
//
//  2. aggregateStatusHeartbeatInterval has elapsed since the last publish.
//     ebus delivery is intentionally lossy: if a subscriber inbox is full,
//     Publish drops the event rather than block the publisher. Without
//     the heartbeat, a dropped initial publish during a storm would leave
//     the UI permanently stale until content changes for some other
//     reason. The heartbeat also carries the fresh ComputedAt so that
//     user-visible "last checked" indicators keep advancing on a quiet
//     but healthy node.
//
// Dedup compares against lastPublishedAggregateStatus rather than the
// previous value of s.aggregateStatus because other paths (startup init,
// orphan eviction rechecks) may mutate s.aggregateStatus without notifying
// subscribers. The anchor must reflect what the bus actually saw.
//
// Caller MUST hold:
//   - s.peerMu at least for read (peer-domain inputs consumed by the
//     underlying refreshAggregateStatusLocked chain)
//   - s.deliveryMu at least for read (delivery-domain inputs consumed by
//     the underlying refreshAggregateStatusLocked chain)
//   - s.statusMu.Lock (status domain — s.aggregateStatus,
//     s.lastPublishedAggregateStatus and s.lastAggregateStatusPublishAt
//     are all written here)
//
// Canonical order: peerMu → deliveryMu → statusMu with statusMu INNERMOST.
func (s *Service) publishAggregateStatusChangedLocked() {
	s.refreshAggregateStatusLocked()
	next := s.aggregateStatus

	contentChanged := !s.lastPublishedAggregateStatus.EqualContent(next)
	heartbeatDue := !s.lastAggregateStatusPublishAt.IsZero() &&
		next.ComputedAt.Sub(s.lastAggregateStatusPublishAt) >= aggregateStatusHeartbeatInterval
	firstPublish := s.lastAggregateStatusPublishAt.IsZero()

	if !contentChanged && !heartbeatDue && !firstPublish {
		return
	}

	s.lastPublishedAggregateStatus = next
	s.lastAggregateStatusPublishAt = next.ComputedAt
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, next)
}

// AggregateStatus returns a point-in-time snapshot of the node's aggregate
// network health. Safe to call from any goroutine.
//
// Uses s.statusMu.RLock alone — the snapshot was already materialised by
// the most recent refreshAggregateStatusLocked under the full peerMu →
// deliveryMu → statusMu stack, so pure readers do not need to take any
// peer-domain or delivery-domain lock. This decouples Desktop UI readers
// from peer-management writers during a reconnect storm.
func (s *Service) AggregateStatus() domain.AggregateStatusSnapshot {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.aggregateStatus
}

// aggregateStatusFrame builds the protocol frame for the
// fetch_aggregate_status local RPC command. Both snapshots are read under
// a single RLock acquisition to avoid re-acquiring the lock twice — with
// Go's writer-preferring RWMutex, each separate RLock is a window where
// a queued writer can interleave and stall the caller.
//
// Uses s.statusMu.RLock alone — both s.aggregateStatus and s.versionPolicy
// live in the status domain, so no peer/delivery lock is needed. The frame
// is served entirely from materialised snapshots populated by earlier
// recomputes.
func (s *Service) aggregateStatusFrame() protocol.Frame {
	s.statusMu.RLock()
	snap := s.aggregateStatus
	var vpSnap domain.VersionPolicySnapshot
	if s.versionPolicy != nil {
		vpSnap = s.versionPolicy.snapshot
	}
	s.statusMu.RUnlock()

	return protocol.Frame{
		Type: "aggregate_status",
		AggregateStatus: &protocol.AggregateStatusFrame{
			Status:          string(snap.Status),
			UsablePeers:     snap.UsablePeers,
			ConnectedPeers:  snap.ConnectedPeers,
			TotalPeers:      snap.TotalPeers,
			PendingMessages: snap.PendingMessages,

			UpdateAvailable:              vpSnap.UpdateAvailable,
			UpdateReason:                 string(vpSnap.UpdateReason),
			IncompatibleVersionReporters: int(vpSnap.IncompatibleVersionReporters),
			MaxObservedPeerBuild:         vpSnap.MaxObservedPeerBuild,
			MaxObservedPeerVersion:       int(vpSnap.MaxObservedPeerVersion),
		},
	}
}
