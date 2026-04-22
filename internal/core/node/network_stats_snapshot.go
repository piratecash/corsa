package node

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// networkStatsSnapshotInterval is how often the background refresher rebuilds
// the cached network_stats snapshot.
//
// Trade-off: shorter interval → fresher RPC answers but more s.mu.RLock
// acquisitions per second from the refresher; longer interval → bounded
// staleness (users see numbers up to this old).  500 ms is responsive enough
// for the desktop UI polling cycle and short enough to feel real-time without
// inducing a measurable readers-per-second load on s.mu.
const networkStatsSnapshotInterval = 500 * time.Millisecond

// networkStatsSnapshot holds a fully materialised protocol.NetworkStatsFrame
// plus the timestamp at which it was built.  Stored in an atomic.Pointer
// so RPC readers (fetch_network_stats) can load it with zero locking —
// completely decoupling the hot read path from any writer holding s.mu.
//
// The snapshot is immutable once stored: every refresh produces a brand-new
// value and replaces the pointer atomically.  This removes any need for a
// dedicated mutex around the snapshot itself.
type networkStatsSnapshot struct {
	// totalSent / totalReceived aggregate all per-peer counters (persisted
	// + live).  connectedCount / knownPeers mirror the fields of the
	// protocol frame; see networkStatsFrame for the semantics.
	totalSent      int64
	totalReceived  int64
	connectedCount int
	knownPeers     int
	// peerTraffic is the per-peer breakdown, already sorted by total
	// traffic descending / address ascending so the RPC handler can
	// return it as-is.
	peerTraffic []protocol.PeerTrafficFrame
	// generatedAt is the wall-clock timestamp when rebuildNetworkStatsSnapshot
	// produced this snapshot.  Exposed for future observability but not
	// currently part of the wire frame.
	generatedAt time.Time
}

// toFrame renders the snapshot into a protocol.Frame ready to be sent to the
// RPC caller.  Separated from the rebuild step so the refresher goroutine
// can cache the expensive computation while the RPC handler retains full
// control over frame-level framing.
func (snap *networkStatsSnapshot) toFrame() protocol.Frame {
	if snap == nil {
		return protocol.Frame{
			Type:         "network_stats",
			NetworkStats: &protocol.NetworkStatsFrame{},
		}
	}
	return protocol.Frame{
		Type: "network_stats",
		NetworkStats: &protocol.NetworkStatsFrame{
			TotalBytesSent:     snap.totalSent,
			TotalBytesReceived: snap.totalReceived,
			TotalTraffic:       snap.totalSent + snap.totalReceived,
			ConnectedPeers:     snap.connectedCount,
			KnownPeers:         snap.knownPeers,
			PeerTraffic:        snap.peerTraffic,
		},
	}
}

// loadNetworkStatsSnapshot atomically retrieves the last-published snapshot.
// Guaranteed non-nil on the RPC path once Run() has executed
// primeHotReadSnapshots() before opening the listener.  Returns nil only
// for unit tests that bypass Run() without calling
// rebuildNetworkStatsSnapshot manually; toFrame() handles that case by
// emitting an empty-but-valid network_stats frame.
func (s *Service) loadNetworkStatsSnapshot() *networkStatsSnapshot {
	return s.networkStatsSnap.Load()
}

// rebuildNetworkStatsSnapshot constructs a fresh snapshot under a short
// s.mu.RLock, then swaps it into s.networkStatsSnap atomically.
//
// The RLock window covers only the in-memory reads (iteration of s.health,
// s.peers and s.sessions/s.conns via liveTrafficLocked).  All sort and
// frame-building work runs after RUnlock, so writers waiting on s.mu.Lock
// do not have to wait for an O(N log N) sort to complete.
//
// If writers saturate s.mu (causing this refresher to stall on RLock),
// RPC readers continue to serve the PREVIOUS successfully-built snapshot
// — the RPC never blocks on s.mu regardless of writer behaviour.
func (s *Service) rebuildNetworkStatsSnapshot() {
	type healthSnap struct {
		address   domain.PeerAddress
		sent      int64
		received  int64
		connected bool
	}

	log.Trace().Msg("network_stats_snapshot_refresh_begin")
	s.mu.RLock()
	live := s.liveTrafficLocked()

	healthSnaps := make([]healthSnap, 0, len(s.health))
	for _, h := range s.health {
		healthSnaps = append(healthSnaps, healthSnap{
			address:   h.Address,
			sent:      h.BytesSent,
			received:  h.BytesReceived,
			connected: h.Connected,
		})
	}

	peerAddrs := make([]domain.PeerAddress, len(s.peers))
	for i, p := range s.peers {
		peerAddrs[i] = p.Address
	}
	s.mu.RUnlock()
	log.Trace().Int("health_count", len(healthSnaps)).Int("peer_count", len(peerAddrs)).Msg("network_stats_snapshot_rlock_released")

	// All further work is pure computation on local copies — no locks held.
	var totalSent, totalReceived int64
	var connectedCount int
	peerTraffic := make([]protocol.PeerTrafficFrame, 0, len(healthSnaps)+len(live))
	seen := make(map[domain.PeerAddress]struct{}, len(healthSnaps))

	for _, h := range healthSnaps {
		seen[h.address] = struct{}{}
		sent := h.sent
		recv := h.received
		if lv, ok := live[h.address]; ok {
			sent += lv.sent
			recv += lv.received
		}
		totalSent += sent
		totalReceived += recv
		if h.connected {
			connectedCount++
		}
		peerTraffic = append(peerTraffic, protocol.PeerTrafficFrame{
			Address:       string(h.address),
			BytesSent:     sent,
			BytesReceived: recv,
			TotalTraffic:  sent + recv,
			Connected:     h.connected,
		})
	}

	// Include inbound-only peers whose address is not yet in s.health.
	for addr, lv := range live {
		if _, ok := seen[addr]; ok {
			continue
		}
		totalSent += lv.sent
		totalReceived += lv.received
		connectedCount++
		peerTraffic = append(peerTraffic, protocol.PeerTrafficFrame{
			Address:       string(addr),
			BytesSent:     lv.sent,
			BytesReceived: lv.received,
			TotalTraffic:  lv.sent + lv.received,
			Connected:     true,
		})
	}

	sort.Slice(peerTraffic, func(i, j int) bool {
		if peerTraffic[i].TotalTraffic != peerTraffic[j].TotalTraffic {
			return peerTraffic[i].TotalTraffic > peerTraffic[j].TotalTraffic
		}
		return peerTraffic[i].Address < peerTraffic[j].Address
	})

	// known_peers is the union of the configured peer list and any peers
	// we've seen via health or live traffic.
	knownSet := make(map[string]struct{}, len(peerAddrs)+len(seen))
	for _, addr := range peerAddrs {
		knownSet[string(addr)] = struct{}{}
	}
	for addr := range seen {
		knownSet[string(addr)] = struct{}{}
	}
	for addr := range live {
		knownSet[string(addr)] = struct{}{}
	}

	snap := &networkStatsSnapshot{
		totalSent:      totalSent,
		totalReceived:  totalReceived,
		connectedCount: connectedCount,
		knownPeers:     len(knownSet),
		peerTraffic:    peerTraffic,
		generatedAt:    time.Now().UTC(),
	}
	s.networkStatsSnap.Store(snap)
	log.Trace().Int("connected", connectedCount).Int("known", len(knownSet)).Msg("network_stats_snapshot_refresh_end")
}

// networkStatsSnapPtr is a thin wrapper type used only inside the Service
// struct declaration to make the zero value of the atomic pointer usable.
// Go does not allow declaring `atomic.Pointer[T]` directly as an embedded
// field without exposing the generic type on every call site; exposing it
// via this alias keeps Service's field list readable while preserving the
// zero-value semantics (.Load() on a zero atomic.Pointer returns nil).
type networkStatsSnapPtr = atomic.Pointer[networkStatsSnapshot]
