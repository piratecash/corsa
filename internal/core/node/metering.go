package node

import (
	"sort"

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

	log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_wait").Str("address", string(address)).Msg("s_mu_writer")
	s.mu.Lock()
	log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_held").Str("address", string(address)).Msg("s_mu_writer")
	defer func() {
		s.mu.Unlock()
		log.Trace().Str("site", "accumulateSessionTraffic").Str("phase", "lock_released").Str("address", string(address)).Msg("s_mu_writer")
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
// Caller must hold s.mu (read or write).
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

	log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_wait").Msg("s_mu_writer")
	s.mu.Lock()
	log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_held").Msg("s_mu_writer")
	defer func() {
		s.mu.Unlock()
		log.Trace().Str("site", "accumulateInboundTraffic").Str("phase", "lock_released").Msg("s_mu_writer")
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
// connections). Must be called while holding s.mu at least for read.
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

// networkStatsFrame builds a network_stats frame with cumulative and live
// traffic counters for all known peers.
func (s *Service) networkStatsFrame() protocol.Frame {
	// ---------------------------------------------------------------
	// Short critical section: snapshot health, live traffic and peer
	// list under lock, then release before sorting and frame building.
	// Prevents writer starvation on s.mu (see peerHealthFrames comment).
	// ---------------------------------------------------------------
	type healthSnap struct {
		Address   domain.PeerAddress
		Sent      int64
		Received  int64
		Connected bool
	}

	log.Trace().Msg("network_stats_frame_begin")
	log.Trace().Msg("network_stats_frame_before_rlock")
	s.mu.RLock()
	log.Trace().Msg("network_stats_frame_rlock_acquired")
	live := s.liveTrafficLocked()

	healthSnaps := make([]healthSnap, 0, len(s.health))
	for _, health := range s.health {
		healthSnaps = append(healthSnaps, healthSnap{
			Address:   health.Address,
			Sent:      health.BytesSent,
			Received:  health.BytesReceived,
			Connected: health.Connected,
		})
	}

	peerAddrs := make([]domain.PeerAddress, len(s.peers))
	for i, p := range s.peers {
		peerAddrs[i] = p.Address
	}
	s.mu.RUnlock()
	log.Trace().Int("health_count", len(healthSnaps)).Int("peer_count", len(peerAddrs)).Msg("network_stats_frame_rlock_released")

	// Build traffic frames from snapshots — no lock held.
	var totalSent, totalReceived int64
	var connectedCount int
	peerTraffic := make([]protocol.PeerTrafficFrame, 0, len(healthSnaps)+len(live))
	seen := make(map[domain.PeerAddress]struct{}, len(healthSnaps))

	for _, h := range healthSnaps {
		seen[h.Address] = struct{}{}
		sent := h.Sent
		recv := h.Received
		if lv, ok := live[h.Address]; ok {
			sent += lv.sent
			recv += lv.received
		}
		totalSent += sent
		totalReceived += recv
		if h.Connected {
			connectedCount++
		}
		peerTraffic = append(peerTraffic, protocol.PeerTrafficFrame{
			Address:       string(h.Address),
			BytesSent:     sent,
			BytesReceived: recv,
			TotalTraffic:  sent + recv,
			Connected:     h.Connected,
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
	// we've seen via health/live traffic (includes non-listener clients
	// that don't appear in s.peers but are actively connected).
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

	stats := &protocol.NetworkStatsFrame{
		TotalBytesSent:     totalSent,
		TotalBytesReceived: totalReceived,
		TotalTraffic:       totalSent + totalReceived,
		ConnectedPeers:     connectedCount,
		KnownPeers:         len(knownSet),
		PeerTraffic:        peerTraffic,
	}

	log.Trace().Int("connected", connectedCount).Int("known", len(knownSet)).Msg("network_stats_frame_end")

	return protocol.Frame{
		Type:         "network_stats",
		NetworkStats: stats,
	}
}

// emitTrafficDeltas publishes a single PeerTrafficBatch event containing
// snapshots for all peers whose cumulative byte count (persisted + live)
// changed since the last emission. Called from bootstrapLoop on every tick
// (~2s). Uses a short RLock to snapshot health + live counters, then
// compares and publishes outside the lock.
func (s *Service) emitTrafficDeltas() {
	s.mu.RLock()
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
	s.mu.RUnlock()

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
