package node

import (
	"bufio"
	"context"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"corsa/internal/core/crashlog"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
)

// announceWriteDeadline caps how long we block on a synchronous write
// to an inbound peer during announce_routes. Prevents a single slow
// connection from stalling the entire announce cycle.
const announceWriteDeadline = 5 * time.Second

// inboundPeerIdentity returns the peer identity (Ed25519 fingerprint)
// for an inbound connection, derived from the hello frame's Address field.
// This is distinct from connPeerHello.address which stores the listen
// address (transport) for health tracking. NATed peers advertise a
// non-routable listen address (e.g. 127.0.0.1:64646) that must never
// be used as a routing identity.
func (s *Service) inboundPeerIdentity(conn net.Conn) domain.PeerIdentity {
	s.mu.RLock()
	info := s.connPeerInfo[conn]
	s.mu.RUnlock()
	if info == nil {
		return ""
	}
	return info.identity
}

// routingTableTTLLoop runs periodic TTL cleanup on the routing table.
// Expired entries are removed every 10 seconds. When an expiry exposes
// a surviving backup route for an identity, the event-driven drain is
// triggered so pending send_message frames can be delivered immediately
// instead of waiting for the normal retry loop.
func (s *Service) routingTableTTLLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			exposed := s.routingTable.TickTTL()
			s.triggerDrainForExposed(exposed)
		}
	}
}

// tableForwardResult describes the outcome of tryForwardViaRoutingTable.
type tableForwardResult struct {
	// Address is the transport address the frame was sent to. Empty if no
	// table route could be used.
	Address domain.PeerAddress

	// RouteOrigin is the Origin field from the RouteEntry that was selected.
	// Stored in relayForwardState so that hop_ack confirmation at this
	// intermediate node can match the exact (Identity, Origin, NextHop) triple.
	RouteOrigin domain.PeerIdentity
}

// tryForwardViaRoutingTable consults the routing table for a next-hop to
// the given recipient and tries to forward the relay frame there. Returns
// a tableForwardResult with the transport address and the route origin from
// the selected entry. Called from intermediate relay hops (handleRelayMessage)
// to enable multi-hop table-directed relay chains.
//
// excludeIdentity is the identity of the peer that sent the relay to us —
// we must not send back to them (split horizon on relay path).
func (s *Service) tryForwardViaRoutingTable(recipient domain.PeerIdentity, frame protocol.Frame, excludeIdentity domain.PeerIdentity) tableForwardResult {
	routes := s.routingTable.Lookup(recipient)
	if len(routes) == 0 {
		return tableForwardResult{}
	}

	for _, route := range routes {
		// Don't send back to where it came from.
		if route.NextHop == excludeIdentity {
			continue
		}
		// Direct destination (hops=1) only needs relay cap; transit needs both.
		address := s.resolveRouteNextHopAddress(route.NextHop, route.Hops)
		if address == "" {
			continue
		}
		if s.sendFrameToAddress(address, frame) {
			log.Debug().
				Str("id", frame.ID).
				Str("recipient", string(recipient)).
				Str("next_hop", string(route.NextHop)).
				Str("address", string(address)).
				Str("origin", string(route.Origin)).
				Int("hops", route.Hops).
				Msg("relay_forward_via_routing_table")
			return tableForwardResult{Address: address, RouteOrigin: route.Origin}
		}
	}

	return tableForwardResult{}
}

// SendAnnounceRoutes implements routing.PeerSender. It builds an
// announce_routes frame and sends it to the peer. Supports both outbound
// sessions (by session address) and inbound connections (by "inbound:"
// prefixed address from inboundConnKey).
func (s *Service) SendAnnounceRoutes(peerAddress domain.PeerAddress, routes []routing.AnnounceEntry) bool {
	if len(routes) == 0 {
		return true
	}

	wireRoutes := make([]protocol.AnnounceRouteFrame, len(routes))
	for i, r := range routes {
		wireRoutes[i] = protocol.AnnounceRouteFrame{
			Identity: string(r.Identity),
			Origin:   string(r.Origin),
			Hops:     r.Hops,
			SeqNo:    r.SeqNo,
			Extra:    r.Extra,
		}
	}

	frame := protocol.Frame{
		Type:           "announce_routes",
		AnnounceRoutes: wireRoutes,
	}

	// Inbound connections use synchronous write; outbound use enqueue.
	if strings.HasPrefix(string(peerAddress), "inbound:") {
		return s.sendAnnounceRoutesToInbound(string(peerAddress), frame)
	}
	return s.enqueuePeerFrame(peerAddress, frame)
}

// sendAnnounceRoutesToInbound finds the inbound connection matching the
// "inbound:remoteAddr" key and writes the frame synchronously. Returns
// false if the connection is gone or the write fails, so the announce
// loop can log the failure.
func (s *Service) sendAnnounceRoutesToInbound(key string, frame protocol.Frame) bool {
	return s.writeFrameToInbound(domain.PeerAddress(key), frame)
}

// writeFrameToInbound writes a marshaled frame to an inbound connection
// identified by an "inbound:remoteAddr" key. Uses enqueueFrameSync if a
// send channel exists, otherwise falls back to direct write with a deadline
// to prevent head-of-line blocking.
//
// This is the inbound equivalent of enqueuePeerFrame for outbound sessions.
// Used by both announce_routes and relay_message paths.
func (s *Service) writeFrameToInbound(address domain.PeerAddress, frame protocol.Frame) bool {
	remoteAddr := strings.TrimPrefix(string(address), "inbound:")

	s.mu.RLock()
	var target net.Conn
	for conn := range s.inboundTracked {
		if conn.RemoteAddr().String() == remoteAddr {
			target = conn
			break
		}
	}
	s.mu.RUnlock()

	if target == nil {
		return false
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		log.Warn().Err(err).Str("peer", remoteAddr).Msg("frame_inbound_marshal_failed")
		return false
	}

	result := s.enqueueFrameSync(target, []byte(line))
	switch result {
	case enqueueSent:
		return true
	case enqueueUnregistered:
		// No send channel — fall back to direct write with a deadline
		// so one slow peer cannot stall the caller.
		_ = target.SetWriteDeadline(time.Now().Add(announceWriteDeadline))
		_, writeErr := io.WriteString(target, line)
		_ = target.SetWriteDeadline(time.Time{}) // clear deadline
		if writeErr != nil {
			log.Debug().Err(writeErr).Str("peer", remoteAddr).Msg("frame_inbound_write_failed")
			return false
		}
		return true
	default:
		// enqueueDropped — buffer full or conn closing.
		log.Debug().Str("peer", remoteAddr).Msg("frame_inbound_dropped")
		return false
	}
}

// sendFrameToAddress sends a protocol frame to the given address, handling
// both outbound sessions (plain address) and inbound connections ("inbound:"
// prefixed key). This is the unified send dispatch for all table-directed
// relay and forwarding paths.
func (s *Service) sendFrameToAddress(address domain.PeerAddress, frame protocol.Frame) bool {
	if strings.HasPrefix(string(address), "inbound:") {
		return s.writeFrameToInbound(address, frame)
	}
	return s.enqueuePeerFrame(address, frame)
}

// routingCapablePeers returns all peers (outbound sessions AND inbound
// connections) that have negotiated both mesh_routing_v1 and mesh_relay_v1
// capabilities. Used by AnnounceLoop to discover announcement targets.
// A peer that appears in both maps is deduplicated by identity.
//
// Both capabilities are required because a routing-only peer (mesh_routing_v1
// without mesh_relay_v1) cannot carry data-plane relay traffic. Advertising
// routes through such a peer would create non-deliverable paths.
func (s *Service) routingCapablePeers() []routing.AnnounceTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[domain.PeerIdentity]struct{})
	var targets []routing.AnnounceTarget

	// Outbound sessions — one announce per identity, even if multiple
	// sessions exist for the same peer.
	for address, session := range s.sessions {
		if session.peerIdentity == "" {
			continue
		}
		if _, dup := seen[session.peerIdentity]; dup {
			continue
		}
		if sessionHasBothCaps(session.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			seen[session.peerIdentity] = struct{}{}
			targets = append(targets, routing.AnnounceTarget{
				Address:  domain.PeerAddress(address),
				Identity: session.peerIdentity,
			})
		}
	}

	// Inbound connections — only if identity not already covered by an
	// outbound session above (dedup by identity).
	for conn := range s.inboundTracked {
		info := s.connPeerInfo[conn]
		if info == nil || info.identity == "" {
			continue
		}
		if _, dup := seen[info.identity]; dup {
			continue
		}
		if sessionHasBothCaps(info.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			seen[info.identity] = struct{}{}
			targets = append(targets, routing.AnnounceTarget{
				Address:  inboundConnKey(conn),
				Identity: info.identity,
			})
		}
	}

	return targets
}

// inboundConnKey returns a unique key for an inbound connection that can be
// used as an AnnounceTarget address. Prefixed with "inbound:" to distinguish
// from outbound session addresses.
func inboundConnKey(conn net.Conn) domain.PeerAddress {
	return domain.PeerAddress("inbound:" + conn.RemoteAddr().String())
}

// sendFullTableSyncToInbound sends the current routing table to a newly
// connected inbound peer. This is the inbound-path counterpart of the
// outbound full-table sync (Phase 1.2: always full sync on connect).
// Without this, inbound-only peers would wait until the next periodic
// or triggered announce cycle before learning the current table.
func (s *Service) sendFullTableSyncToInbound(conn net.Conn, peerIdentity domain.PeerIdentity) {
	if peerIdentity == "" {
		return
	}

	routes := s.routingTable.AnnounceTo(peerIdentity)
	if len(routes) == 0 {
		return
	}

	sendAddr := inboundConnKey(conn)
	if !s.SendAnnounceRoutes(sendAddr, routes) {
		log.Warn().
			Str("peer", string(peerIdentity)).
			Str("address", string(sendAddr)).
			Int("routes", len(routes)).
			Msg("routing_inbound_full_sync_failed")
	}
}

// handleAnnounceRoutes processes an incoming announce_routes frame from a
// peer. Each route entry is inserted into the local routing table with +1
// hop (the wire carries the sender's local hop count; the receiver adds 1).
//
// Withdrawals (hops=16) are applied directly via Table.WithdrawRoute.
// Normal routes are inserted via Table.UpdateRoute with source=announcement.
func (s *Service) handleAnnounceRoutes(senderIdentity domain.PeerIdentity, frame protocol.Frame) {
	if senderIdentity == "" {
		log.Warn().Msg("announce_routes_no_sender_identity")
		return
	}
	if !identity.IsValidAddress(string(senderIdentity)) {
		log.Warn().Str("sender", string(senderIdentity)).Msg("announce_routes_malformed_sender")
		return
	}

	accepted := 0
	unchanged := 0
	rejected := 0
	drainIdentities := make(map[domain.PeerIdentity]struct{})

	for _, wireRoute := range frame.AnnounceRoutes {
		if wireRoute.Identity == "" || wireRoute.Origin == "" {
			rejected++
			continue
		}

		// Reject malformed identity fingerprints early — before any table
		// operations. Valid addresses are exactly 40 lowercase hex chars
		// (SHA-256 of Ed25519 public key, truncated to 20 bytes).
		if !identity.IsValidAddress(wireRoute.Identity) || !identity.IsValidAddress(wireRoute.Origin) {
			rejected++
			log.Warn().
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("announce_rejected_malformed_address")
			continue
		}

		// Skip routes about ourselves — we know our own connectivity.
		if wireRoute.Identity == s.identity.Address {
			continue
		}

		// Reject own-origin forgery: a foreign sender must never advertise
		// routes with Origin == our identity. Only this node may originate
		// routes under its own identity. Accepting such an announcement
		// would let a neighbor poison our monotonic SeqNo counter via
		// syncSeqCounterLocked, breaking the per-origin SeqNo invariant.
		if wireRoute.Origin == s.identity.Address {
			rejected++
			log.Debug().
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("announce_rejected_forged_own_origin")
			continue
		}

		if wireRoute.Hops >= routing.HopsInfinity {
			// Withdrawal: only the origin may emit a wire withdrawal.
			// Transit nodes that lose upstream must invalidate locally and
			// stop advertising — they must not forward hops=16 on the wire.
			// Accepting a withdrawal from a non-origin sender would let an
			// intermediate neighbor unilaterally kill a route it does not own.
			if wireRoute.Origin != string(senderIdentity) {
				rejected++
				log.Warn().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Msg("announce_rejected_transit_withdrawal")
				continue
			}

			withdrawnID := domain.PeerIdentity(wireRoute.Identity)
			if s.routingTable.WithdrawRoute(
				withdrawnID,
				domain.PeerIdentity(wireRoute.Origin),
				senderIdentity,
				wireRoute.SeqNo,
			) {
				accepted++
				// After withdrawing the best triple, a less-preferred backup
				// route may now be the active path. If Lookup still returns
				// reachable entries for this identity, trigger a drain so
				// pending send_message frames can be delivered via the backup
				// route immediately instead of waiting for the retry loop.
				if remaining := s.routingTable.Lookup(withdrawnID); len(remaining) > 0 {
					drainIdentities[withdrawnID] = struct{}{}
				}
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Uint64("seq", wireRoute.SeqNo).
					Str("from", string(senderIdentity)).
					Msg("route_withdrawal_applied")
			} else {
				rejected++
			}
			continue
		}

		// Normal route: add +1 hop (receiver convention).
		receivedHops := wireRoute.Hops + 1
		if receivedHops > routing.HopsInfinity {
			receivedHops = routing.HopsInfinity
		}

		// ExpiresAt is left zero — Table.UpdateRoute applies the table's
		// own defaultTTL and clock, ensuring consistent TTL policy between
		// local (AddDirectPeer) and learned (announcement) routes.
		entry := routing.RouteEntry{
			Identity: domain.PeerIdentity(wireRoute.Identity),
			Origin:   domain.PeerIdentity(wireRoute.Origin),
			NextHop:  senderIdentity,
			Hops:     receivedHops,
			SeqNo:    wireRoute.SeqNo,
			Source:   routing.RouteSourceAnnouncement,
			Extra:    wireRoute.Extra,
		}

		status, err := s.routingTable.UpdateRoute(entry)
		if err != nil {
			log.Warn().
				Err(err).
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("route_update_rejected_invalid")
			rejected++
			continue
		}
		switch status {
		case routing.RouteAccepted:
			accepted++
			drainIdentities[domain.PeerIdentity(wireRoute.Identity)] = struct{}{}
		case routing.RouteUnchanged:
			unchanged++
			drainIdentities[domain.PeerIdentity(wireRoute.Identity)] = struct{}{}
		case routing.RouteRejected:
			rejected++
			existing := s.routingTable.InspectTriple(entry.DedupKey())
			if existing != nil {
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Int("incoming_hops", receivedHops).
					Uint64("incoming_seq", wireRoute.SeqNo).
					Int("existing_hops", existing.Hops).
					Uint64("existing_seq", existing.SeqNo).
					Str("existing_source", existing.Source.String()).
					Bool("existing_withdrawn", existing.IsWithdrawn()).
					Str("existing_expires", existing.ExpiresAt.Format(time.RFC3339)).
					Msg("route_rejected_by_existing")
			} else {
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Int("incoming_hops", receivedHops).
					Uint64("incoming_seq", wireRoute.SeqNo).
					Msg("route_rejected_no_existing_triple")
			}
		}
	}

	log.Debug().
		Str("from", string(senderIdentity)).
		Int("total", len(frame.AnnounceRoutes)).
		Int("accepted", accepted).
		Int("unchanged", unchanged).
		Int("rejected", rejected).
		Msg("announce_routes_processed")

	// Event-driven pending queue drain: new or reconfirmed transit routes
	// mean that own outbound frames waiting for these recipients can be
	// delivered via the learned next-hop. Both accepted (new/improved) and
	// unchanged (reconfirmed alive) routes qualify — a reconnected peer
	// re-announcing the same table still proves the path is alive.
	// Batched after the entire announce is processed to avoid per-entry
	// scans during high-volume table syncs.
	//
	// Fast-path: skip goroutine launch when the pending queue is empty.
	// This avoids unnecessary Lock contention on every announce_routes,
	// which was causing inter-test timing regressions in full package runs.
	if len(drainIdentities) > 0 {
		s.mu.RLock()
		hasPending := len(s.pending) > 0
		s.mu.RUnlock()
		if hasPending {
			go s.drainPendingForIdentities(drainIdentities)
		}
	}
}

// onPeerSessionEstablished is called when a session is fully established,
// for both outbound sessions (after handshake, auth, and sync) and inbound
// connections (after hello exchange). It registers the peer as a direct
// route in the routing table and triggers an immediate announcement cycle.
//
// The hasRelayCap flag indicates whether the peer negotiated mesh_relay_v1.
// Only relay-capable peers become direct routes — a direct route advertises
// that this node can deliver relay_message to the destination. Without
// relay capability the route is non-deliverable; announcing it would cause
// other routing-capable nodes to learn a path that fails at the last hop.
// When hasRelayCap is false, session counting still occurs (so that
// onPeerSessionClosed can safely decrement) but no routing table entry
// is created and no announcement is triggered.
//
// Multi-session awareness: AddDirectPeer is called only on the 0→1
// transition (first session for this identity). Subsequent sessions for
// the same identity increment the session count but do not touch the
// routing table — AddDirectPeer is idempotent at the model level, but
// the session-count gate here is the primary defense against churn.
func (s *Service) onPeerSessionEstablished(peerIdentity domain.PeerIdentity, hasRelayCap bool) {
	if peerIdentity == "" {
		return
	}

	s.mu.Lock()
	s.identitySessions[peerIdentity]++

	if hasRelayCap {
		s.identityRelaySessions[peerIdentity]++
	}
	firstRelay := s.identityRelaySessions[peerIdentity] == 1
	s.mu.Unlock()

	if !hasRelayCap {
		log.Debug().
			Str("peer", string(peerIdentity)).
			Msg("routing_peer_no_relay_cap_skip_direct_route")
		return
	}

	if !firstRelay {
		log.Debug().
			Str("peer", string(peerIdentity)).
			Msg("routing_additional_session_no_table_update")
		return
	}

	result, err := s.routingTable.AddDirectPeer(peerIdentity)
	if err != nil {
		log.Error().Err(err).Str("peer", string(peerIdentity)).Msg("routing_add_direct_peer_failed")
		return
	}

	log.Info().
		Str("peer", string(peerIdentity)).
		Uint64("seq", result.Entry.SeqNo).
		Bool("penalized", result.Penalized).
		Msg("routing_direct_peer_added")

	// When flap detection penalizes a peer, skip the triggered announce.
	// The route will be picked up by the next periodic announce cycle,
	// giving the link time to stabilize.
	if result.Penalized {
		log.Warn().
			Str("peer", string(peerIdentity)).
			Msg("routing_peer_in_holddown_skipping_announce")
		return
	}

	s.announceLoop.TriggerUpdate()
}

// onPeerSessionClosed is called when a session for a peer identity closes.
// The hasRelayCap flag must match the value passed to onPeerSessionEstablished
// for this session so that the relay-capable session counter stays balanced.
//
// Multi-session awareness: RemoveDirectPeer is called on the relay-session
// 1→0 transition (last relay-capable session for this identity). Total
// session count cleanup happens on the total-session 1→0 transition.
//
// On disconnect, own-origin direct routes are withdrawn on the wire
// (returned as wire-ready AnnounceEntry items) and transit routes
// learned through this peer are silently invalidated locally.
func (s *Service) onPeerSessionClosed(peerIdentity domain.PeerIdentity, hasRelayCap bool) {
	if peerIdentity == "" {
		return
	}

	s.mu.Lock()
	count := s.identitySessions[peerIdentity]
	if count <= 0 {
		s.mu.Unlock()
		return
	}
	s.identitySessions[peerIdentity]--
	isLastTotal := s.identitySessions[peerIdentity] == 0
	if isLastTotal {
		delete(s.identitySessions, peerIdentity)
	}

	lastRelay := false
	if hasRelayCap {
		relayCount := s.identityRelaySessions[peerIdentity]
		if relayCount > 0 {
			s.identityRelaySessions[peerIdentity]--
			lastRelay = s.identityRelaySessions[peerIdentity] == 0
			if lastRelay {
				delete(s.identityRelaySessions, peerIdentity)
			}
		}
	}
	s.mu.Unlock()

	if !lastRelay {
		// No relay sessions left (or never had one). If this is also the
		// last total session, invalidate any transit routes learned through
		// this peer. This covers the routing-only peer case (mesh_routing_v1
		// without mesh_relay_v1): the receive-path gate blocks new
		// announcements, but defense-in-depth ensures stale lineages are
		// cleaned up on disconnect.
		if isLastTotal {
			invalidated, exposed := s.routingTable.InvalidateTransitRoutes(peerIdentity)
			if invalidated > 0 {
				log.Info().
					Str("peer", string(peerIdentity)).
					Int("transit_invalidated", invalidated).
					Msg("routing_transit_routes_invalidated_on_disconnect")
				// Trigger an immediate announce cycle so neighbors learn the
				// invalidated routes are withdrawn. Without this, stale routes
				// persist until the next periodic cycle (up to 30s).
				s.announceLoop.TriggerUpdate()
			}
			s.triggerDrainForExposed(exposed)
		} else {
			log.Debug().
				Str("peer", string(peerIdentity)).
				Int("remaining", count-1).
				Msg("routing_session_closed_others_remain")
		}
		return
	}

	result, err := s.routingTable.RemoveDirectPeer(peerIdentity)
	if err != nil {
		log.Error().Err(err).Str("peer", string(peerIdentity)).Msg("routing_remove_direct_peer_failed")
		return
	}

	// Send wire withdrawals to all routing-capable peers.
	if len(result.Withdrawals) > 0 {
		peers := s.routingCapablePeers()
		for _, peer := range peers {
			s.SendAnnounceRoutes(peer.Address, result.Withdrawals)
		}
	}

	log.Info().
		Str("peer", string(peerIdentity)).
		Int("withdrawals", len(result.Withdrawals)).
		Int("transit_invalidated", result.TransitInvalidated).
		Msg("routing_direct_peer_removed")

	s.announceLoop.TriggerUpdate()
	s.triggerDrainForExposed(result.ExposedBackups)
}

// triggerDrainForExposed converts a slice of routing.PeerIdentity (identities
// with newly exposed backup routes) into the identities map format and launches
// drainPendingForIdentities if the pending queue is non-empty. Used by
// onPeerSessionClosed (disconnect-triggered drain), routingTableTTLLoop
// (TTL-expiry drain), and handleAnnounceRoutes (withdrawal drain) to share
// the drain-trigger logic.
func (s *Service) triggerDrainForExposed(exposed []routing.PeerIdentity) {
	if len(exposed) == 0 {
		return
	}
	identities := make(map[domain.PeerIdentity]struct{}, len(exposed))
	for _, id := range exposed {
		identities[domain.PeerIdentity(id)] = struct{}{}
	}
	s.mu.RLock()
	hasPending := len(s.pending) > 0
	s.mu.RUnlock()
	if hasPending {
		go s.drainPendingForIdentities(identities)
	}
}

// sendTableDirectedRelay sends a relay_message to the table-selected next-hop.
// The validatedAddress was obtained from TableRouter at route-decision time
// and already had the correct capability check (relay-only for destinations,
// both caps for transit). Using it directly avoids re-resolution, which
// could pick a different session for the same identity with weaker capabilities.
//
// routeOrigin is the Origin field from the selected RouteEntry. It is stored
// in relayForwardState so that hop_ack confirmation can match the exact
// (Identity, Origin, NextHop) triple.
//
// nextHopHops is the hop count from the selected RouteEntry. Used by the
// retry path to re-resolve with correct capability requirements: hops=1
// (destination) needs only relay cap; hops>1 (transit) needs both caps.
//
// If validatedAddress is empty (e.g., retry path without cached address),
// falls back to resolveRouteNextHopAddress with the original hop role.
//
// Handles both outbound sessions and inbound connections ("inbound:" prefix).
func (s *Service) sendTableDirectedRelay(msg protocol.Envelope, nextHopIdentity domain.PeerIdentity, validatedAddress domain.PeerAddress, routeOrigin domain.PeerIdentity, nextHopHops int) {
	address := validatedAddress
	if address == "" {
		// Retry path or caller without cached address — re-resolve with
		// the correct capability requirements for the hop role.
		address = s.resolveRouteNextHopAddress(nextHopIdentity, nextHopHops)
	}

	if address == "" {
		// Session gone — gossip fallback.
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", string(nextHopIdentity)).
			Msg("table_relay_no_session_fallback_gossip")
		targets := s.routingTargetsForMessage(msg)
		s.tryRelayToCapableFullNodes(msg, targets)
		return
	}

	if !s.sendRelayToAddress(address, msg, routeOrigin) {
		// Send failed — gossip fallback.
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", string(nextHopIdentity)).
			Str("address", string(address)).
			Msg("table_relay_send_failed_fallback_gossip")
		targets := s.routingTargetsForMessage(msg)
		s.tryRelayToCapableFullNodes(msg, targets)
		return
	}

	log.Info().
		Str("recipient", msg.Recipient).
		Str("next_hop", string(nextHopIdentity)).
		Str("address", string(address)).
		Msg("table_relay_sent")
}

// sendRelayToAddress sends a relay_message to the given address, handling
// both outbound sessions and inbound connections. For outbound sessions it
// delegates to sendRelayMessage (which persists relayForwardState internally).
// For inbound connections it builds the frame, writes via writeFrameToInbound,
// and persists relayForwardState explicitly so that hop_ack and receipt
// routing work identically regardless of connection direction.
//
// routeOrigin is the Origin field from the routing decision. Stored in
// relayForwardState for triple-scoped hop_ack confirmation.
func (s *Service) sendRelayToAddress(address domain.PeerAddress, msg protocol.Envelope, routeOrigin domain.PeerIdentity) bool {
	if strings.HasPrefix(string(address), "inbound:") {
		frame := protocol.Frame{
			Type:        "relay_message",
			ID:          string(msg.ID),
			Address:     msg.Sender,
			Recipient:   msg.Recipient,
			Topic:       msg.Topic,
			Flag:        string(msg.Flag),
			CreatedAt:   msg.CreatedAt.UTC().Format(time.RFC3339),
			TTLSeconds:  msg.TTLSeconds,
			Body:        string(msg.Payload),
			HopCount:    1,
			MaxHops:     defaultMaxHops,
			PreviousHop: s.identity.Address,
		}
		if !s.writeFrameToInbound(address, frame) {
			return false
		}
		// Persist relay state — mirrors what sendRelayMessage does for outbound.
		stored := s.relayStates.store(&relayForwardState{
			MessageID:        string(msg.ID),
			PreviousHop:      "",
			ReceiptForwardTo: "",
			ForwardedTo:      address,
			Recipient:        domain.PeerIdentity(msg.Recipient),
			RouteOrigin:      routeOrigin,
			HopCount:         1,
			RemainingTTL:     relayStateTTLSeconds,
		})
		if !stored {
			log.Warn().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("address", string(address)).
				Msg("relay_state_store_failed_inbound")
			return false
		}
		s.persistRelayState()
		return true
	}
	return s.sendRelayMessageWithOrigin(address, msg, routeOrigin)
}

// resolveRoutableAddress finds a transport address for a peer identity that
// can serve as a table-directed relay TRANSIT next-hop. The peer must have
// BOTH capabilities:
//   - mesh_routing_v1: understands announce_routes / routing table
//   - mesh_relay_v1:   can accept relay_message frames
//
// Use this for transit next-hops (hops > 1) where the peer must forward
// the message further using its own routing table. For destination peers
// (hops == 1), use resolveRelayAddress instead — they only need relay cap.
//
// Checks both outbound sessions and inbound connections. For inbound-only
// peers, the returned address is an "inbound:" prefixed key that must be
// handled by sendFrameToAddress (not enqueuePeerFrame directly).
func (s *Service) resolveRoutableAddress(peerIdentity domain.PeerIdentity) domain.PeerAddress {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Outbound sessions first — preferred because they have async send queues.
	for address, session := range s.sessions {
		if session.peerIdentity != peerIdentity {
			continue
		}
		if sessionHasBothCaps(session.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			return domain.PeerAddress(address)
		}
	}

	// Inbound connections — synchronous write path.
	for conn := range s.inboundTracked {
		info := s.connPeerInfo[conn]
		if info == nil || info.identity != peerIdentity {
			continue
		}
		if sessionHasBothCaps(info.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			return inboundConnKey(conn)
		}
	}

	return ""
}

// resolveRelayAddress finds a transport address for a peer identity that
// can accept relay_message frames. Only requires mesh_relay_v1.
//
// Used for destination peers (direct routes, hops == 1) where the peer
// IS the final recipient and does not need to forward further.
//
// Checks both outbound sessions and inbound connections. For inbound-only
// peers, the returned address is an "inbound:" prefixed key.
func (s *Service) resolveRelayAddress(peerIdentity domain.PeerIdentity) domain.PeerAddress {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Outbound sessions first.
	for address, session := range s.sessions {
		if session.peerIdentity != peerIdentity {
			continue
		}
		if sessionHasCap(session.capabilities, domain.CapMeshRelayV1) {
			return domain.PeerAddress(address)
		}
	}

	// Inbound connections.
	for conn := range s.inboundTracked {
		info := s.connPeerInfo[conn]
		if info == nil || info.identity != peerIdentity {
			continue
		}
		if sessionHasCap(info.capabilities, domain.CapMeshRelayV1) {
			return inboundConnKey(conn)
		}
	}

	return ""
}

// resolveRouteNextHopAddress finds a transport address for a route's
// next-hop, applying the right capability check based on hop count:
//   - hops == 1 (direct peer, destination): only mesh_relay_v1 required
//   - hops >  1 (transit node): both mesh_relay_v1 and mesh_routing_v1
//
// This is the unified resolver used by TableRouter and tryForwardViaRoutingTable.
// Returns outbound session address or "inbound:" prefixed key.
func (s *Service) resolveRouteNextHopAddress(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
	if hops <= 1 {
		return s.resolveRelayAddress(peerIdentity)
	}
	return s.resolveRoutableAddress(peerIdentity)
}

// sessionHasCap returns true if the capability slice contains the given capability.
func sessionHasCap(caps []domain.Capability, cap domain.Capability) bool {
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

// sessionHasBothCaps returns true if the capability slice contains both a and b.
func sessionHasBothCaps(caps []domain.Capability, a, b domain.Capability) bool {
	var hasA, hasB bool
	for _, c := range caps {
		if c == a {
			hasA = true
		}
		if c == b {
			hasB = true
		}
		if hasA && hasB {
			return true
		}
	}
	return false
}

// confirmRouteViaHopAck promotes the route through the peer that sent us
// the relay_hop_ack to source=hop_ack. The ackSenderAddress parameter is
// the transport address of the peer that produced the ack — this is the
// provably correct next-hop, even under gossip fan-out where multiple
// peers received the relay_message but only one actually delivered it.
//
// routeOrigin scopes the confirmation to the exact (Identity, Origin,
// NextHop) triple that was used for the original routing decision. When
// non-empty, only the route matching all three fields is promoted. When
// empty (gossip path where no specific triple was chosen), the first
// matching NextHop in Lookup order is promoted — this is a weaker
// guarantee but acceptable for gossip-originated relays.
//
// If ackSenderAddress is empty, no route can be confirmed (message was
// stored locally without forwarding).
func (s *Service) confirmRouteViaHopAck(recipientIdentity domain.PeerIdentity, ackSenderAddress domain.PeerAddress, routeOrigin domain.PeerIdentity) {
	if ackSenderAddress == "" {
		return
	}

	// ackSenderAddress is a transport address. Resolve to peer identity
	// so we can match against routing table entries (keyed by identity).
	nextHopIdentity := s.resolvePeerIdentity(ackSenderAddress)
	if nextHopIdentity == "" {
		// Session may have closed. Try using ackSenderAddress as identity directly.
		nextHopIdentity = domain.PeerIdentity(ackSenderAddress)
	}

	routes := s.routingTable.Lookup(recipientIdentity)
	if len(routes) == 0 {
		return
	}

	// Find the route that matches the actual triple used for the send.
	for _, route := range routes {
		if route.NextHop != nextHopIdentity {
			continue
		}
		// When routeOrigin is known, enforce exact triple match.
		if routeOrigin != "" && route.Origin != routeOrigin {
			continue
		}
		if route.Source >= routing.RouteSourceHopAck {
			return // already confirmed or better
		}

		confirmed := routing.RouteEntry{
			Identity:  route.Identity,
			Origin:    route.Origin,
			NextHop:   route.NextHop,
			Hops:      route.Hops,
			SeqNo:     route.SeqNo,
			Source:    routing.RouteSourceHopAck,
			ExpiresAt: route.ExpiresAt,
			Extra:     route.Extra,
		}

		status, err := s.routingTable.UpdateRoute(confirmed)
		if err != nil {
			log.Warn().Err(err).
				Str("identity", string(recipientIdentity)).
				Str("next_hop", string(nextHopIdentity)).
				Str("origin", string(route.Origin)).
				Msg("route_hop_ack_confirm_failed")
			return
		}
		if status == routing.RouteAccepted {
			log.Debug().
				Str("identity", string(recipientIdentity)).
				Str("origin", string(route.Origin)).
				Str("next_hop", string(route.NextHop)).
				Msg("route_confirmed_via_hop_ack")
		}
		return
	}

	log.Debug().
		Str("identity", string(recipientIdentity)).
		Str("ack_sender", string(ackSenderAddress)).
		Str("route_origin", string(routeOrigin)).
		Msg("route_hop_ack_no_matching_route")
}

// resolvePeerIdentity returns the peer identity for a transport address.
// Checks both outbound sessions and inbound connections.
//
// For outbound sessions, the address is the session map key (e.g., "host:port").
// For inbound connections, the address is the connection's remote address
// (conn.RemoteAddr().String()). The returned value is the peer's Ed25519
// identity fingerprint.
func (s *Service) resolvePeerIdentity(address domain.PeerAddress) domain.PeerIdentity {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Outbound session: address is the session map key.
	if session := s.sessions[address]; session != nil {
		return session.peerIdentity
	}

	// Inbound connection: match on the connection's transport address.
	for conn, info := range s.connPeerInfo {
		if info != nil && conn.RemoteAddr().String() == string(address) {
			return info.identity
		}
	}

	return ""
}

// executeGossipTargets sends a message to pre-computed gossip targets from a
// RoutingDecision. The target list is provided by the Router — targets are not
// recomputed here.
func (s *Service) executeGossipTargets(msg protocol.Envelope, targets []domain.PeerAddress) {
	defer crashlog.DeferRecover()
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendMessageToPeer(address, msg)
	}
}

func (s *Service) routingTargets() []domain.PeerAddress {
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, _ domain.PeerIdentity) bool {
		return !peerType.IsClient()
	})
}

func (s *Service) routingTargetsForMessage(msg protocol.Envelope) []domain.PeerAddress {
	if msg.Topic != "dm" || msg.Recipient == "*" {
		return s.routingTargets()
	}
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity) bool {
		return !peerType.IsClient() || string(peerID) == msg.Recipient
	})
}

func (s *Service) routingTargetsForRecipient(recipient string) []domain.PeerAddress {
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity) bool {
		return !peerType.IsClient() || string(peerID) == recipient
	})
}

func (s *Service) routingTargetsFiltered(allow func(address domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity) bool) []domain.PeerAddress {
	s.mu.RLock()
	if len(s.sessions) > 0 {
		type scoredTarget struct {
			address domain.PeerAddress
			score   int64
		}
		scored := make([]scoredTarget, 0, len(s.sessions))
		for address := range s.sessions {
			if address == "" || s.isSelfAddress(address) {
				continue
			}
			primaryAddr := s.resolveHealthAddress(address)
			peerType := s.peerTypeForAddressLocked(primaryAddr)
			peerID := s.peerIDs[primaryAddr]
			if !allow(address, peerType, peerID) {
				continue
			}
			health := s.health[primaryAddr]
			if health == nil || !health.Connected {
				continue
			}
			if s.computePeerStateLocked(health) == peerStateStalled {
				continue
			}
			scored = append(scored, scoredTarget{
				address: address,
				score:   scorePeerTargetLocked(health),
			})
		}
		if len(scored) > 0 {
			s.mu.RUnlock()
			sort.Slice(scored, func(i, j int) bool {
				if scored[i].score == scored[j].score {
					return string(scored[i].address) < string(scored[j].address)
				}
				return scored[i].score > scored[j].score
			})
			limit := min(3, len(scored))
			targets := make([]domain.PeerAddress, 0, limit)
			for _, item := range scored[:limit] {
				targets = append(targets, item.address)
			}
			return targets
		}
	}
	s.mu.RUnlock()

	targets := make([]domain.PeerAddress, 0, len(s.Peers()))
	for _, peer := range s.Peers() {
		if peer.Address == "" || s.isSelfAddress(peer.Address) {
			continue
		}
		if !allow(peer.Address, s.peerTypeForAddress(peer.Address), s.peerIdentityForAddress(peer.Address)) {
			continue
		}
		targets = append(targets, peer.Address)
	}
	sort.Slice(targets, func(i, j int) bool {
		return string(targets[i]) < string(targets[j])
	})
	return targets
}

func (s *Service) sendMessageToPeer(address domain.PeerAddress, msg protocol.Envelope) {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:       "send_message",
		Topic:      msg.Topic,
		ID:         string(msg.ID),
		Address:    msg.Sender,
		Recipient:  msg.Recipient,
		Flag:       string(msg.Flag),
		CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
		TTLSeconds: msg.TTLSeconds,
		Body:       string(msg.Payload),
	}
	if s.enqueuePeerFrame(address, frame) {
		s.clearOutboundQueued(frame.ID)
		log.Debug().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "session").Msg("route_message_attempt")
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "queued").Msg("route_message_attempt")
		return
	}
	s.markOutboundTerminal(frame, "failed", "unable to queue outbound frame")
	log.Debug().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "dropped").Msg("route_message_attempt")
}

func (s *Service) gossipNotice(ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	for _, address := range s.routingTargets() {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendNoticeToPeer(address, ttl, ciphertext)
	}
}

func (s *Service) sendNoticeToPeer(address domain.PeerAddress, ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:       "publish_notice",
		TTLSeconds: int(ttl.Seconds()),
		Ciphertext: ciphertext,
	}
	if s.enqueuePeerFrame(address, frame) {
		return
	}

	conn, err := net.DialTimeout("tcp", string(address), syncHandshakeTimeout)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		return
	}
	welcomeLine, err := readFrameLine(reader, maxResponseLineBytes)
	if err != nil {
		return
	}
	welcome, err := protocol.ParseFrameLine(strings.TrimSpace(welcomeLine))
	if err != nil {
		return
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			return
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			return
		}
		reply, err := readFrameLine(reader, maxResponseLineBytes)
		if err != nil {
			return
		}
		authReply, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
		if err != nil || authReply.Type != "auth_ok" {
			return
		}
	}
	if welcome.Type == "error" {
		return
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return
	}
	_, _ = io.WriteString(conn, line)
	_, _ = readFrameLine(reader, maxResponseLineBytes)
}

// drainPendingForIdentities scans the pending queue for own outbound
// send_message frames whose Recipient matches one of the given identities
// and attempts to deliver them via the routing table.
//
// Only send_message is drained — other frame types are not route-recoverable:
//   - relay_message, announce_peer: other peers' traffic, handled by relay
//     retry loop and peer session flush respectively.
//   - send_delivery_receipt: delivered via relayStates hop chain
//     (lookupReceiptForwardTo), not via the routing table. A new route to
//     the receipt's Recipient does not create a delivery path for the receipt.
//     Receipts are retried through flushPendingPeerFrames (peer reconnect)
//     and the relay retry loop, both of which already handle them correctly.
//
// This is an event-driven optimization: instead of waiting for the original
// target peer to reconnect (flushPendingPeerFrames) or for the 2-second
// relay retry loop to fire, we react immediately when a new route appears.
//
// Concurrency safety: matched frames are atomically removed from pending
// under the lock before delivery is attempted. This prevents concurrent
// drain goroutines (servePeerSession + handleAnnounceRoutes) from
// sending the same frame twice. Frames that fail delivery are returned to
// pending so the normal retry mechanisms can pick them up.
//
// Crash safety: all outbound state changes (markOutboundTerminalLocked,
// markOutboundRetryingLocked, clearOutboundQueuedLocked) are collected as
// deferred actions during the delivery loop and applied under the lock
// together with the pending queue return in a single persist. No
// intermediate writes to queue-*.json occur while extracted frames are
// absent from s.pending.
//
// Called from servePeerSession (direct peer connect, inboxCh is actively
// read so Lock contention cannot cause overflow) and handleAnnounceRoutes
// (transit route learned) to minimize delivery latency.
func (s *Service) drainPendingForIdentities(identities map[domain.PeerIdentity]struct{}) {
	defer crashlog.DeferRecover()

	if len(identities) == 0 {
		return
	}

	// Bail out if the service is shutting down. Drain goroutines are
	// fire-and-forget; without this check they could run against a
	// half-torn-down Service (closed connections, cancelled contexts),
	// causing write-deadline timeouts that serialize on s.mu and stall
	// other goroutines sharing the same lock.
	select {
	case <-s.done:
		return
	default:
	}

	type pendingMatch struct {
		peerAddr domain.PeerAddress
		item     pendingFrame
		origIdx  int // position in original s.pending[peerAddr] before extraction
	}

	// Per-address extraction metadata. Stored during the extract phase so
	// the return phase can merge failed frames back into their original
	// positions — preserving queue order across recipients sharing the
	// same peer address.
	type addrExtractMeta struct {
		origLen      int   // length of s.pending[addr] before extraction
		extractedPos []int // original indices of extracted frames (ascending)
	}

	// Atomically extract matching frames from the pending queue.
	// Removing them under the lock prevents concurrent drain goroutines
	// from processing the same frame (double-send prevention).
	//
	// No persist happens here — the on-disk state still contains the
	// extracted frames. If the process crashes before we persist below,
	// frames survive in queue-*.json and will be retried on restart.
	s.mu.Lock()
	var extracted []pendingMatch
	extractMeta := make(map[domain.PeerAddress]*addrExtractMeta)
	for addr, frames := range s.pending {
		var kept []pendingFrame
		var meta *addrExtractMeta
		for i, item := range frames {
			if item.Frame.Type != "send_message" {
				kept = append(kept, item)
				continue
			}
			if _, ok := identities[domain.PeerIdentity(item.Frame.Recipient)]; !ok {
				kept = append(kept, item)
				continue
			}
			if meta == nil {
				meta = &addrExtractMeta{origLen: len(frames)}
			}
			meta.extractedPos = append(meta.extractedPos, i)
			extracted = append(extracted, pendingMatch{peerAddr: addr, item: item, origIdx: i})
			delete(s.pendingKeys, pendingFrameKey(addr, item.Frame))
		}
		if meta != nil {
			extractMeta[addr] = meta
		}
		if len(kept) == 0 {
			delete(s.pending, addr)
		} else {
			s.pending[addr] = kept
		}
	}
	s.mu.Unlock()

	if len(extracted) == 0 {
		return
	}

	// Attempt delivery for each extracted frame. Track failures
	// so they can be returned to the pending queue.
	//
	// Outbound state changes (terminal, retrying, cleared) are collected
	// as deferred actions and applied under the lock in a single batch
	// at the end. This eliminates intermediate persists that would
	// snapshot s.pending without the extracted frames, creating a
	// crash-loss window.
	type deferredOutbound struct {
		action string // "terminal", "retrying", "clear"
		frame  protocol.Frame
		// Fields for "retrying":
		queuedAt time.Time
		retries  int
		errText  string
		// Fields for "terminal":
		status string
	}

	now := time.Now().UTC()
	var failed []pendingMatch
	var deferred []deferredOutbound

	for i, m := range extracted {
		// Re-check shutdown before each delivery attempt. If the service
		// is shutting down, return all remaining extracted frames to
		// pending without attempting delivery — the next startup will
		// retry them from the persisted queue state.
		select {
		case <-s.done:
			failed = append(failed, extracted[i:]...)
			goto returnFrames
		default:
		}

		if s.pendingFrameExpired(m.item.Frame, m.item.QueuedAt, now) {
			deferred = append(deferred, deferredOutbound{
				action:  "terminal",
				frame:   m.item.Frame,
				status:  "expired",
				errText: "message delivery expired",
			})
			continue
		}

		delivered, attempted := s.drainSendMessage(m.item.Frame, m.peerAddr)
		if !delivered {
			// Only increment retry counter when a real delivery attempt was
			// made (route existed, address resolved, send was tried). When
			// no usable route exists the frame goes back to pending without
			// burning retry budget — a future routing event will trigger
			// another drain cycle.
			if attempted {
				m.item.Retries++
			}
			if m.item.Retries >= maxPendingFrameRetries {
				deferred = append(deferred, deferredOutbound{
					action:  "terminal",
					frame:   m.item.Frame,
					status:  "failed",
					errText: "max retries exceeded",
				})
				continue
			}
			// Only emit a "retrying" outbound state change when a real
			// delivery attempt was made. When no usable route exists, the
			// frame goes back to pending silently — no Status flip to
			// "retrying", no LastAttemptAt update. This preserves the
			// semantic that "retrying" in outbound state means a real
			// network handoff was tried and failed, matching the behavior
			// of flushPendingPeerFrames where retrying only fires on
			// sendCh backpressure.
			if attempted {
				deferred = append(deferred, deferredOutbound{
					action:   "retrying",
					frame:    m.item.Frame,
					queuedAt: m.item.QueuedAt,
					retries:  m.item.Retries,
					errText:  "drain delivery failed",
				})
			}
			failed = append(failed, m)
		} else {
			deferred = append(deferred, deferredOutbound{
				action: "clear",
				frame:  m.item.Frame,
			})
		}
	}

returnFrames:
	// Apply all state changes in a single lock+persist. This is the
	// only persist in the entire drain cycle — no intermediate writes
	// that could snapshot s.pending without extracted frames.
	s.mu.Lock()

	// Merge failed frames back into their original positions in the
	// per-address pending queue. The extraction phase recorded each
	// frame's origIdx and per-addr metadata (origLen, extractedPos).
	// We walk through the original position range 0..origLen-1: at
	// extracted positions, emit the returning frame (if any); at kept
	// positions, consume the next frame from the current queue. Frames
	// added by other goroutines during the unlocked delivery window
	// (beyond the kept portion) are appended at the end. This preserves
	// exact original ordering across recipients sharing the same peer
	// address — flushPendingPeerFrames delivers in slice order, so any
	// reordering would change DM delivery sequence after route churn.
	type returningEntry struct {
		origIdx int
		item    pendingFrame
	}
	returningByAddr := make(map[domain.PeerAddress][]returningEntry, len(failed))
	for _, m := range failed {
		key := pendingFrameKey(m.peerAddr, m.item.Frame)
		if _, dup := s.pendingKeys[key]; dup {
			continue
		}
		returningByAddr[m.peerAddr] = append(returningByAddr[m.peerAddr], returningEntry{
			origIdx: m.origIdx,
			item:    m.item,
		})
		s.pendingKeys[key] = struct{}{}
	}
	for addr, rets := range returningByAddr {
		meta := extractMeta[addr]
		if meta == nil {
			// No metadata (defensive) — prepend as fallback.
			fallback := make([]pendingFrame, 0, len(rets)+len(s.pending[addr]))
			for _, r := range rets {
				fallback = append(fallback, r.item)
			}
			s.pending[addr] = append(fallback, s.pending[addr]...)
			continue
		}

		current := s.pending[addr]

		// current = [kept frames in order] + [new frames from other goroutines].
		// keptCount is the number of original non-extracted frames.
		keptCount := meta.origLen - len(meta.extractedPos)
		var keptPortion, newFrames []pendingFrame
		if keptCount <= len(current) {
			keptPortion = current[:keptCount]
			newFrames = current[keptCount:]
		} else {
			// Some kept frames were removed by a concurrent flush — use
			// whatever is left; merge degrades gracefully.
			keptPortion = current
		}

		merged := make([]pendingFrame, 0, len(rets)+len(current))
		ki := 0 // index into keptPortion
		ri := 0 // index into rets (sorted ascending by origIdx via extraction order)
		ei := 0 // index into meta.extractedPos

		for pos := 0; pos < meta.origLen; pos++ {
			if ei < len(meta.extractedPos) && meta.extractedPos[ei] == pos {
				// This position was extracted.
				if ri < len(rets) && rets[ri].origIdx == pos {
					merged = append(merged, rets[ri].item)
					ri++
				}
				// else: delivered, expired, or terminal — gap is correct.
				ei++
			} else {
				// This position was kept.
				if ki < len(keptPortion) {
					merged = append(merged, keptPortion[ki])
					ki++
				}
			}
		}
		// Defensive: drain any remaining kept or returning frames.
		for ; ki < len(keptPortion); ki++ {
			merged = append(merged, keptPortion[ki])
		}
		for ; ri < len(rets); ri++ {
			merged = append(merged, rets[ri].item)
		}
		// Append frames queued by other goroutines during the unlock window.
		merged = append(merged, newFrames...)

		if len(merged) == 0 {
			delete(s.pending, addr)
		} else {
			s.pending[addr] = merged
		}
	}
	for _, d := range deferred {
		switch d.action {
		case "terminal":
			s.markOutboundTerminalLocked(d.frame, d.status, d.errText)
		case "retrying":
			s.markOutboundRetryingLocked(d.frame, d.queuedAt, d.retries, d.errText)
		case "clear":
			s.clearOutboundQueuedLocked(d.frame.ID)
		}
	}
	snapshot := s.queueStateSnapshotLocked()
	drainDone := s.drainDone
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	if drainDone != nil {
		drainDone()
	}
}

// drainSendMessage attempts to deliver a single send_message frame via the
// routing table. Uses sendRelayToAddress directly (not sendTableDirectedRelay)
// to get a definitive success/failure signal.
//
// Returns two booleans:
//   - delivered: true when the frame was durably handed off to a relay session.
//   - attempted: true when a real send was attempted (route existed, address
//     resolved). When attempted is false the caller should NOT increment the
//     retry counter — no network resources were consumed, the route simply
//     does not exist yet and a future routing event may provide one.
//
// On success, the caller is responsible for clearing outbound state —
// this function does not call clearOutboundQueued to avoid intermediate
// persists during the drain cycle.
func (s *Service) drainSendMessage(frame protocol.Frame, originalPeer domain.PeerAddress) (delivered, attempted bool) {
	createdAt, err := time.Parse(time.RFC3339, strings.TrimSpace(frame.CreatedAt))
	if err != nil {
		// Parse error is a data problem, not a routing problem.
		// Treat as attempted — the frame is malformed and retries won't help.
		return false, true
	}
	envelope := protocol.Envelope{
		ID:         protocol.MessageID(frame.ID),
		Topic:      frame.Topic,
		Sender:     frame.Address,
		Recipient:  frame.Recipient,
		Flag:       protocol.MessageFlag(frame.Flag),
		TTLSeconds: frame.TTLSeconds,
		Payload:    []byte(frame.Body),
		CreatedAt:  createdAt,
	}

	decision := s.router.Route(envelope)
	if decision.RelayNextHop == nil {
		// No route — not a real delivery attempt, don't burn retry budget.
		return false, false
	}

	// Resolve the next-hop address. Use the validated address from the
	// routing decision when available, otherwise re-resolve.
	address := decision.RelayNextHopAddress
	if address == "" {
		address = s.resolveRouteNextHopAddress(*decision.RelayNextHop, decision.RelayNextHopHops)
	}
	if address == "" {
		// Route exists but address unresolvable — transient, don't burn retry budget.
		return false, false
	}

	// sendRelayToAddress returns true only when the frame is durably
	// enqueued in a session or persistent peer queue, with relayForwardState
	// stored. No silent gossip fallback — if this fails, the frame goes
	// back to pending for the normal retry loop.
	if !s.sendRelayToAddress(address, envelope, decision.RelayRouteOrigin) {
		// Real send attempt failed — this is a genuine delivery failure.
		return false, true
	}

	log.Info().
		Str("id", frame.ID).
		Str("recipient", frame.Recipient).
		Str("next_hop", string(*decision.RelayNextHop)).
		Str("original_peer", string(originalPeer)).
		Msg("pending_drained_via_new_route")
	return true, true
}
