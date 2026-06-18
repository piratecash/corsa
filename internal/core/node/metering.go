package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// liveTraffic holds real-time byte counters snapshot from active MeteredConn
// instances. Used by networkStatsFrame to combine persisted totals with
// in-flight session counters.
type liveTraffic struct {
	sent     int64
	received int64
}

// accumulateSessionTraffic adds byte counters from an outbound peer session's
// MeteredConn to the peer's cumulative traffic totals in health.
func (s *Service) accumulateSessionTraffic(address domain.PeerAddress, mc *netcore.MeteredConn) {
	sent := mc.BytesWritten()
	received := mc.BytesRead()
	if sent == 0 && received == 0 {
		return
	}

	log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	}()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	health.BytesSent += sent
	health.BytesReceived += received
}

// isConnTrafficTrustedByIDLocked returns true when the connection's claimed
// peer address can be trusted for traffic attribution.  On the session-auth
// path NetCore.auth exists and we require Verified==true; on the legacy
// (no-session-auth) path auth is nil and attribution is allowed.
// Reads peer-domain registry state (s.conns via coreForIDLocked) —
// caller MUST hold s.peerMu (read or write).
func (s *Service) isConnTrafficTrustedByIDLocked(id domain.ConnID) bool {
	core := s.coreForIDLocked(id)
	if core == nil {
		return false
	}
	state := core.Auth()
	return state == nil || state.Verified
}

// accumulateInboundTraffic adds byte counters from an inbound MeteredConn
// to the peer's cumulative traffic totals. The peer address is resolved
// from the NetCore populated during the hello handshake.
// Traffic is only attributed when the connection's identity has been
// verified (or no session-auth was required), preventing unauthenticated
// clients from spoofing another peer's traffic counters.
func (s *Service) accumulateInboundTraffic(mc *netcore.MeteredConn) {
	sent := mc.BytesWritten()
	received := mc.BytesRead()
	if sent == 0 && received == 0 {
		return
	}

	log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_held").Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_released").Msg("peer_mu_writer")
	}()

	id, ok := s.connIDForLocked(mc)
	if !ok {
		return
	}
	if !s.isConnTrafficTrustedByIDLocked(id) {
		return
	}
	core := s.coreForIDLocked(id)
	if core == nil || core.Address() == "" {
		return
	}
	address := s.resolveHealthAddress(core.Address())
	health := s.ensurePeerHealthLocked(address)
	health.BytesSent += sent
	health.BytesReceived += received
}

// liveTrafficLocked collects current byte counters from all active
// MeteredConn instances (both outbound peer sessions and inbound
// connections). Reads peer-domain session / registry state — caller
// MUST hold s.peerMu at least for read.
func (s *Service) liveTrafficLocked() map[domain.PeerAddress]liveTraffic {
	result := make(map[domain.PeerAddress]liveTraffic)

	// outbound peer sessions
	for addr, session := range s.sessions {
		if session.metered == nil {
			continue
		}
		address := s.resolveHealthAddress(addr)
		lt := result[address]
		lt.sent += session.metered.BytesWritten()
		lt.received += session.metered.BytesRead()
		result[address] = lt
	}

	// inbound connections — only attribute traffic for verified (or
	// non-session-auth) connections to prevent address spoofing.
	//
	// Iterate the unified registry and filter to inbound direction because
	// outbound NetCores now share s.conns; outbound traffic
	// is already accumulated via s.sessions above.
	s.forEachInboundConnLocked(func(info connInfo) bool {
		metered := s.meteredForIDLocked(info.id)
		if metered == nil {
			return true
		}
		if !s.isConnTrafficTrustedByIDLocked(info.id) {
			return true
		}
		if info.address == "" {
			return true
		}
		address := s.resolveHealthAddress(info.address)
		lt := result[address]
		lt.sent += metered.BytesWritten()
		lt.received += metered.BytesRead()
		result[address] = lt
		return true
	})

	return result
}

// sumLiveTrafficLocked returns the grand total of live byte counters across all
// active MeteredConn instances, without materialising the per-address map that
// liveTrafficLocked allocates. Address resolution and per-address aggregation
// only affect how counters are GROUPED; they do not change the grand total, so
// summing the raw counters yields exactly the same totals
// rebuildNetworkStatsSnapshot derives from liveTrafficLocked. Used by the
// map/slice-free trafficTotalsFrame fast path. Caller MUST hold s.peerMu at
// least for read.
//
// The inbound walk uses forEachInboundConnIDLocked, NOT forEachInboundConnLocked:
// the latter builds a full connInfo per entry via snapshotEntryLocked, which is
// wasted work on this once-a-second collector path that only needs id / address.
// (snapshotEntryLocked's capability read is now the no-copy CapabilitiesRef, so
// it no longer clones, but the rest of the connInfo build is still pointless
// here.) The lightweight iterator exposes exactly the id / address this sum
// needs (the trust check and metered lookup are keyed by id), applying the
// identical inbound-direction filter, so the total is unchanged.
func (s *Service) sumLiveTrafficLocked() (sent, received int64) {
	for _, session := range s.sessions {
		if session.metered == nil {
			continue
		}
		sent += session.metered.BytesWritten()
		received += session.metered.BytesRead()
	}

	s.forEachInboundConnIDLocked(func(id domain.ConnID, address domain.PeerAddress, _ bool) bool {
		metered := s.meteredForIDLocked(id)
		if metered == nil {
			return true
		}
		if !s.isConnTrafficTrustedByIDLocked(id) {
			return true
		}
		if address == "" {
			return true
		}
		sent += metered.BytesWritten()
		received += metered.BytesRead()
		return true
	})

	return sent, received
}

// networkStatsFrame returns the cached network_stats snapshot.
//
// The frame is rebuilt in the background by hotReadsRefreshLoop while a recent
// fetch_network_stats reader is active, and primed synchronously by
// primeHotReadSnapshots() from Run() before the listener opens; the RPC path
// here performs a single atomic load and no locking at all.  This statically
// decouples fetch_network_stats from every writer holding s.peerMu —
// previously a burst of writers on s.peerMu.Lock (bootstrapLoop eviction,
// announce_routes fanout, inbound connect path) starved the RPC's RLock until
// the command timeout.
//
// If the atomic load returns nil (a unit test that bypasses Run() and
// does not prime), toFrame() emits an empty-but-valid network_stats
// frame rather than falling back to a synchronous rebuild — the
// fallback would reach s.peerMu.RLock on the RPC goroutine and break the
// lock-free contract the snapshot infrastructure enforces.
func (s *Service) networkStatsFrame() protocol.Frame {
	log.Trace().Msg("network_stats_frame_begin")
	s.networkStatsAccessNanos.Store(time.Now().UnixNano())
	snap := s.loadNetworkStatsSnapshot()
	frame := snap.toFrame()
	if snap != nil && frame.NetworkStats != nil {
		log.Trace().
			Int("connected", frame.NetworkStats.ConnectedPeers).
			Int("known", frame.NetworkStats.KnownPeers).
			Msg("network_stats_frame_end")
	}
	return frame
}

// trafficTotalsFrame answers the cumulative byte totals (sent / received /
// total) with NO per-peer breakdown, NO knownPeers/connectedPeers, and — most
// importantly — WITHOUT marking networkStatsAccessNanos.
//
// The metrics collector polls traffic totals once a second for the desktop
// traffic chart's running deltas. Previously it called fetch_network_stats,
// which (a) ran the full O(peers) rebuild with its knownSet + peerAddrs +
// peerTraffic allocations and sort, and (b) stamped the reader-access clock,
// pinning the background rebuild-gate permanently awake even on a headless node
// with no UI attached. That single poll was the reason rebuildNetworkStatsSnapshot
// never slept — the dominant alloc_space line item over long runs.
//
// This path computes only the two int64 totals under a short RLock with zero
// map/slice allocation (see sumLiveTrafficLocked), and deliberately does not
// touch networkStatsAccessNanos, so the full snapshot's rebuild-gate is free to
// idle out whenever no genuine fetch_network_stats reader (the UI) is active.
//
// The reply reuses the network_stats frame shape so the collector reads
// TotalBytesSent / TotalBytesReceived unchanged; the per-peer fields stay zero.
func (s *Service) trafficTotalsFrame() protocol.Frame {
	s.peerMu.RLock()
	sent, received := s.sumLiveTrafficLocked()
	for _, h := range s.health {
		sent += h.BytesSent
		received += h.BytesReceived
	}
	s.peerMu.RUnlock()

	return protocol.Frame{
		Type: "network_stats",
		NetworkStats: &protocol.NetworkStatsFrame{
			TotalBytesSent:     sent,
			TotalBytesReceived: received,
			TotalTraffic:       sent + received,
		},
	}
}

// emitTrafficDeltas publishes a single PeerTrafficBatch event containing
// snapshots for all peers whose cumulative byte count (persisted + live)
// changed since the last emission. Called from bootstrapLoop on every tick
// (~2s). Uses a short RLock to snapshot health + live counters, then
// compares and publishes outside the lock.
func (s *Service) emitTrafficDeltas() {
	s.peerMu.RLock()
	live := s.liveTrafficLocked()

	type snap struct {
		address domain.PeerAddress
		sent    int64
		recv    int64
	}
	snaps := make([]snap, 0, len(s.health))
	for _, h := range s.health {
		sent := h.BytesSent
		recv := h.BytesReceived
		if lv, ok := live[h.Address]; ok {
			sent += lv.sent
			recv += lv.received
		}
		snaps = append(snaps, snap{h.Address, sent, recv})
	}

	// Include live-only peers not yet in health (inbound before handshake).
	for addr, lv := range live {
		if _, ok := s.health[addr]; ok {
			continue
		}
		snaps = append(snaps, snap{addr, lv.sent, lv.received})
	}
	s.peerMu.RUnlock()

	// Compare with last emission and publish only changed peers.
	s.trafficMu.Lock()
	if s.lastTrafficSnap == nil {
		s.lastTrafficSnap = make(map[domain.PeerAddress][2]int64, len(snaps))
	}
	var changed []ebus.PeerTrafficSnapshot
	for _, sn := range snaps {
		prev := s.lastTrafficSnap[sn.address]
		if prev[0] == sn.sent && prev[1] == sn.recv {
			continue
		}
		s.lastTrafficSnap[sn.address] = [2]int64{sn.sent, sn.recv}
		changed = append(changed, ebus.PeerTrafficSnapshot{
			Address:       sn.address,
			BytesSent:     sn.sent,
			BytesReceived: sn.recv,
		})
	}
	s.trafficMu.Unlock()

	if len(changed) > 0 {
		s.eventBus.Publish(ebus.TopicPeerTrafficUpdated, ebus.PeerTrafficBatch{Peers: changed})
	}
}
