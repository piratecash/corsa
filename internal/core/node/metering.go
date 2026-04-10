package node

import (
	"net"
	"sort"

	"github.com/piratecash/corsa/internal/core/domain"
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
func (s *Service) accumulateSessionTraffic(address domain.PeerAddress, mc *MeteredConn) {
	sent := mc.BytesWritten()
	received := mc.BytesRead()
	if sent == 0 && received == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	health.BytesSent += sent
	health.BytesReceived += received
}

// isConnTrafficTrustedLocked returns true when the connection's claimed
// peer address can be trusted for traffic attribution.  On the session-auth
// path NetCore.auth exists and we require Verified==true; on the legacy
// (no-session-auth) path auth is nil and attribution is allowed.
// Caller must hold s.mu (read or write).
func (s *Service) isConnTrafficTrustedLocked(conn net.Conn) bool {
	pc := s.inboundNetCores[conn]
	if pc == nil {
		return false
	}
	state := pc.Auth()
	return state == nil || state.Verified
}

// accumulateInboundTraffic adds byte counters from an inbound MeteredConn
// to the peer's cumulative traffic totals. The peer address is resolved
// from the NetCore populated during the hello handshake.
// Traffic is only attributed when the connection's identity has been
// verified (or no session-auth was required), preventing unauthenticated
// clients from spoofing another peer's traffic counters.
func (s *Service) accumulateInboundTraffic(mc *MeteredConn) {
	sent := mc.BytesWritten()
	received := mc.BytesRead()
	if sent == 0 && received == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isConnTrafficTrustedLocked(mc) {
		return
	}
	pc := s.inboundNetCores[mc]
	if pc == nil || pc.Address() == "" {
		return
	}
	address := s.resolveHealthAddress(pc.Address())
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
	for conn, mc := range s.inboundMetered {
		if !s.isConnTrafficTrustedLocked(conn) {
			continue
		}
		pc := s.inboundNetCores[conn]
		if pc == nil || pc.Address() == "" {
			continue
		}
		address := s.resolveHealthAddress(pc.Address())
		lt := result[address]
		lt.sent += mc.BytesWritten()
		lt.received += mc.BytesRead()
		result[address] = lt
	}

	return result
}

// networkStatsFrame builds a network_stats frame with cumulative and live
// traffic counters for all known peers.
func (s *Service) networkStatsFrame() protocol.Frame {
	s.mu.RLock()
	defer s.mu.RUnlock()

	live := s.liveTrafficLocked()

	var totalSent, totalReceived int64
	var connectedCount int
	peerTraffic := make([]protocol.PeerTrafficFrame, 0, len(s.health)+len(live))

	// Track which addresses we've already counted from s.health so we can
	// pick up inbound-only peers that have live traffic but no health entry yet.
	seen := make(map[domain.PeerAddress]struct{}, len(s.health))

	for _, health := range s.health {
		seen[health.Address] = struct{}{}
		sent := health.BytesSent
		recv := health.BytesReceived
		if lv, ok := live[health.Address]; ok {
			sent += lv.sent
			recv += lv.received
		}
		totalSent += sent
		totalReceived += recv
		if health.Connected {
			connectedCount++
		}
		peerTraffic = append(peerTraffic, protocol.PeerTrafficFrame{
			Address:       string(health.Address),
			BytesSent:     sent,
			BytesReceived: recv,
			TotalTraffic:  sent + recv,
			Connected:     health.Connected,
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
	knownSet := make(map[string]struct{}, len(s.peers)+len(seen))
	for _, p := range s.peers {
		knownSet[string(p.Address)] = struct{}{}
	}
	for addr := range seen {
		knownSet[string(addr)] = struct{}{}
	}
	// live-only peers were appended after the seen loop; add them too.
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

	return protocol.Frame{
		Type:         "network_stats",
		NetworkStats: stats,
	}
}
