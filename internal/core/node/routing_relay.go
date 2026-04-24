// routing_relay.go hosts the relay forwarding and gossip dispatch paths:
// tryForwardViaRoutingTable / sendTableDirectedRelay / sendRelayToAddress
// for table-directed relay forwarding; sendFrameToAddress as the unified
// inbound/outbound send dispatch; executeGossipTargets / routingTargets*
// for gossip fanout target selection; sendMessageToPeer / gossipNotice /
// sendNoticeToPeer for push_message and push_notice delivery;
// resolveRouteNextHopAddress as the hop-role-aware resolver wrapper.
//
// This file also owns the three §4.4 bootstrap handshake raw-write edges
// inside sendNoticeToPeer (node-hello, auth challenge, challenge-response)
// and is therefore in the §2.9 net-import whitelist enforced by
// scripts/enforce-netcore-boundary.sh.
//
// This is the relay-plane counterpart to the announce-plane logic in
// routing_announce.go. Address↔identity resolution lives in
// routing_resolver.go, session-lifecycle hooks in routing_session.go,
// hop_ack route confirmation in routing_hop_ack.go, and event-driven
// pending-queue drain in routing_drain.go. The per-file scope table is
// the durable record — see the "internal/core/node/" section of the
// file map in docs/routing.md (both the English and Russian
// component-map sections).
package node

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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
//
// ctx is the caller's request/cycle context. It is threaded into
// sendFrameToAddress so that a pre-cancelled or mid-flight cancelled ctx
// aborts the inbound sync-flush wait instead of letting the NetCore writer
// burn the full syncFlushTimeout. Outbound enqueue is non-blocking and
// does not observe ctx beyond the pre-entry check.
func (s *Service) tryForwardViaRoutingTable(ctx context.Context, recipient domain.PeerIdentity, frame protocol.Frame, excludeIdentity domain.PeerIdentity) tableForwardResult {
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
		if s.sendFrameToAddress(ctx, address, frame) {
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

// sendFrameToAddress sends a protocol frame to the given address, handling
// both outbound sessions (plain address) and inbound connections ("inbound:"
// prefixed key). This is the unified send dispatch for all table-directed
// relay and forwarding paths.
//
// ctx is propagated to the inbound sync-write path so cancellation from
// request-scoped contexts actually interrupts the send wait. Outbound
// enqueue is non-blocking and does not wait on a transport flush.
func (s *Service) sendFrameToAddress(ctx context.Context, address domain.PeerAddress, frame protocol.Frame) bool {
	if strings.HasPrefix(string(address), "inbound:") {
		return s.writeFrameToInbound(ctx, address, frame)
	}
	return s.enqueuePeerFrame(address, frame)
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
//
// ctx is the caller's request/cycle context and is propagated to
// sendRelayToAddress so the inbound sync-flush wait honours cancellation.
func (s *Service) sendTableDirectedRelay(ctx context.Context, msg protocol.Envelope, nextHopIdentity domain.PeerIdentity, validatedAddress domain.PeerAddress, routeOrigin domain.PeerIdentity, nextHopHops int) {
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

	if !s.sendRelayToAddress(ctx, address, msg, routeOrigin) {
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
//
// ctx is the caller's request/cycle context and is propagated into
// writeFrameToInbound so the inbound sync-flush wait honours cancellation.
// Outbound enqueue is non-blocking and does not observe ctx beyond the
// pre-entry check inside the network bridge.
func (s *Service) sendRelayToAddress(ctx context.Context, address domain.PeerAddress, msg protocol.Envelope, routeOrigin domain.PeerIdentity) bool {
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
		if !s.writeFrameToInbound(ctx, address, frame) {
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

// executeGossipTargets sends a message to pre-computed gossip targets from a
// RoutingDecision. The target list is provided by the Router — targets are not
// recomputed here.
func (s *Service) executeGossipTargets(msg protocol.Envelope, targets []domain.PeerAddress) {
	defer crashlog.DeferRecover()
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		s.goBackground(func() { s.sendMessageToPeer(address, msg) })
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
	s.peerMu.RLock()
	now := time.Now().UTC()

	// Phase 1: collect scored session targets (preferred — health-checked,
	// low-latency delivery via enqueuePeerFrame).
	type scoredTarget struct {
		address domain.PeerAddress
		score   int64
	}
	sessionSeen := make(map[domain.PeerAddress]struct{}, len(s.sessions)*2)
	scored := make([]scoredTarget, 0, len(s.sessions))
	for address := range s.sessions {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		sessionSeen[address] = struct{}{}
		primaryAddr := s.resolveHealthAddress(address)
		// Also mark the canonical (primary) address as seen. When the
		// session connected via a fallback port (e.g. host:64646 instead of
		// host:7777), the session key and the canonical peer address differ.
		// Without this, Phase 2 treats the canonical peer as "no session"
		// and adds it as a pending target — wasting a fanout slot on the
		// same physical host that already has an active session.
		if primaryAddr != address {
			sessionSeen[primaryAddr] = struct{}{}
		}
		peerType := s.peerTypeForAddressLocked(primaryAddr)
		peerID := s.peerIDs[primaryAddr]
		if !allow(address, peerType, peerID) {
			continue
		}
		health := s.health[primaryAddr]
		if health == nil || !health.Connected {
			continue
		}
		if s.computePeerStateAtLocked(health, now) == peerStateStalled {
			continue
		}
		scored = append(scored, scoredTarget{
			address: address,
			score:   scorePeerTargetLocked(health),
		})
	}

	// Phase 2: collect non-session peers that pass the filter. These peers
	// don't have an active outbound session yet, but sendMessageToPeer can
	// still reach them via queuePeerFrame → pending queue → drain on session
	// establishment. Without this, a message arriving before all sessions
	// are up gets gossipped only to existing sessions (possibly back to the
	// sender) and never reaches peers whose sessions haven't started yet.
	peers := s.peersSnapshotLocked()
	s.peerMu.RUnlock()

	var pendingPeers []domain.PeerAddress
	for _, peer := range peers {
		if peer.Address == "" || s.isSelfAddress(peer.Address) {
			continue
		}
		if _, ok := sessionSeen[peer.Address]; ok {
			continue
		}
		if !allow(peer.Address, s.peerTypeForAddress(peer.Address), s.peerIdentityForAddress(peer.Address)) {
			continue
		}
		pendingPeers = append(pendingPeers, peer.Address)
	}

	// Phase 3: merge — session targets first (scored), then pending peers
	// (alphabetical, lower priority).
	//
	// When session targets exist, cap total at 3 (gossip fanout limit).
	// When no sessions exist at all, return all matching peers uncapped
	// (bootstrap scenario — identical to the pre-merge behavior).
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return string(scored[i].address) < string(scored[j].address)
		}
		return scored[i].score > scored[j].score
	})
	sort.Slice(pendingPeers, func(i, j int) bool {
		return string(pendingPeers[i]) < string(pendingPeers[j])
	})

	if len(scored) > 0 {
		// Session targets exist — cap at gossip fanout limit, fill remaining
		// slots with pending peers so messages reach not-yet-connected nodes.
		const maxTargets = 3
		targets := make([]domain.PeerAddress, 0, maxTargets)
		for _, item := range scored {
			if len(targets) >= maxTargets {
				break
			}
			targets = append(targets, item.address)
		}
		for _, addr := range pendingPeers {
			if len(targets) >= maxTargets {
				break
			}
			targets = append(targets, addr)
		}
		return targets
	}

	// No active sessions — return all matching peers uncapped (bootstrap).
	targets := make([]domain.PeerAddress, 0, len(pendingPeers))
	targets = append(targets, pendingPeers...)
	return targets
}

func (s *Service) sendMessageToPeer(address domain.PeerAddress, msg protocol.Envelope) {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:  "push_message",
		Topic: msg.Topic,
		Item: &protocol.MessageFrame{
			ID:         string(msg.ID),
			Sender:     msg.Sender,
			Recipient:  msg.Recipient,
			Flag:       string(msg.Flag),
			CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
			TTLSeconds: msg.TTLSeconds,
			Body:       string(msg.Payload),
		},
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Info().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "session").Msg("gossip_message_attempt")
		return
	}

	// No outbound session — try writing directly to an authenticated inbound
	// connection from this peer. This covers the common case where the peer
	// has connected to us (inbound) but we have no outbound session (CM slot
	// full). push_message is fire-and-forget so it is safe to interleave on
	// the inbound conn without disrupting request/reply traffic.
	resolved := s.resolveHealthAddress(address)
	s.peerMu.RLock()
	inboundID, haveInbound := s.inboundConnIDForAddressLocked(resolved)
	s.peerMu.RUnlock()
	if haveInbound {
		// Fire-and-forget gossip inbound-direct fallback — Network-routed
		// so a test backend can intercept it; ctx is Service lifecycle.
		_ = s.sendFrameViaNetwork(s.runCtx, inboundID, frame)
		log.Info().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "inbound_direct").Msg("gossip_message_attempt")
		return
	}

	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "queued").Msg("gossip_message_attempt")
		return
	}
	// No outbound delivery tracking for gossip: push_message is fire-and-forget
	// relay, not a locally initiated send. The local send_message path has its
	// own outbound tracking via markOutboundTerminal/clearOutboundQueued.
	log.Debug().Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Str("peer", string(address)).Str("mode", "dropped").Msg("gossip_message_attempt")
}

func (s *Service) gossipNotice(ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	for _, address := range s.routingTargets() {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		s.goBackground(func() { s.sendNoticeToPeer(address, ttl, ciphertext) })
	}
}

func (s *Service) sendNoticeToPeer(address domain.PeerAddress, ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:       "push_notice",
		TTLSeconds: int(ttl.Seconds()),
		Ciphertext: ciphertext,
	}
	if s.enqueuePeerFrame(address, frame) {
		return
	}

	// Try authenticated inbound connection before expensive TCP dial.
	resolved := s.resolveHealthAddress(address)
	s.peerMu.RLock()
	inboundID, haveInbound := s.inboundConnIDForAddressLocked(resolved)
	s.peerMu.RUnlock()
	if haveInbound {
		// Fire-and-forget push_notice inbound-direct fallback — see
		// gossip-message counterpart above for the same ctx rationale.
		_ = s.sendFrameViaNetwork(s.runCtx, inboundID, frame)
		return
	}

	conn, err := net.DialTimeout("tcp", string(address), syncHandshakeTimeout)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	// netcore-migration: §4.4 bootstrap exception. This raw write pre-dates
	// the NetCore managed-write path and is pending architectural review of
	// the inbound-absent dial fallback itself. A repository-wide forbidden-
	// list grep gate whitelists this write by explicit line number using
	// the exact prefix `netcore-migration: §4.4 bootstrap exception` as the
	// whitelist token — removing the sentinel without removing the raw
	// write triggers the gate. Same applies to the two writes below in
	// this function body.
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
	// Advertise convergence: the inbound side may respond with a
	// connection_notice (observed-address-mismatch) instead of welcome,
	// meaning it is about to close. Record the observed IP hint so the
	// next outbound hello self-corrects, then abort this bootstrap write.
	if welcome.Type == protocol.FrameTypeConnectionNotice {
		s.handleConnectionNotice(address, welcome)
		// Remote-first self-loopback discovery on the raw push_notice
		// fan-out path. routingTargets() can route onto an alternate
		// alias (alternate listen port, onion/clearnet mirror) that
		// slips past the isSelfAddress() gate in gossipNotice, landing
		// on our own inbound hello handler which answers with
		// connection_notice{code=peer-banned, reason=self-identity}.
		// Without this branch handleConnectionNotice would only record
		// the (possibly empty) remote ban-until in persistedMeta,
		// leaving health.BannedUntil and LastErrorCode unset. The next
		// gossipNotice tick would then redial the same self-looping
		// alias because the dial-gate consults health, not persistedMeta
		// alone. Route through the same applySelfIdentityCooldown the
		// managed session path and syncPeer use so the 24h window lands
		// in health and blocks further churn.
		if errors.Is(protocol.NoticeErrorFromFrame(welcome), protocol.ErrSelfIdentity) {
			s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		}
		return
	}
	// Local self-loopback guard on the same dial: an alternate alias
	// may reach a responder that does not emit the peer-banned notice
	// (older version, different role) but still welcomes us with our
	// own Ed25519 address. Signing auth_session with our own key and
	// sending push_notice is wasted work at best and a defence-in-depth
	// hole at worst — we would ingest our own welcome into the peer
	// caches via the auth_ok → recordOutboundAuthSuccess path. Abort
	// with the same cooldown the notice branch above applies so both
	// arrival shapes converge on one health record.
	if s.isSelfIdentity(domain.PeerIdentity(welcome.Address)) {
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", welcome.Listen).
			Msg("send_notice_self_identity_rejected")
		s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		return
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, connauth.SessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			return
		}
		// netcore-migration: §4.4 bootstrap exception (see sentinel above).
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
		// Outbound convergence success hook: auth_ok on this raw/
		// bootstrap path is the same evidence of a reachable dialable
		// peer as on the managed-session path. Without this call, peers
		// reached only through push_notice fan-out would never get an
		// announceable persistedMeta row or a trusted advertise triple,
		// so their state would diverge from the managed path and the
		// hostname / observed-IP sweep invariants would not apply here.
		//
		// The helper takes the wrapper-form "host:port" string. This
		// §4.4 raw path has no NetCore wrapper, so we read conn.RemoteAddr()
		// inline — routing_relay.go is already in the §2.9 net-import
		// whitelist for exactly these bootstrap edges. The defensive empty-
		// string fallthrough keeps the contract with the helper.
		var remoteAddr string
		if ra := conn.RemoteAddr(); ra != nil {
			remoteAddr = ra.String()
		}
		s.recordOutboundAuthSuccess(address, remoteAddr)
	}
	if welcome.Type == "error" {
		return
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return
	}
	// netcore-migration: §4.4 bootstrap exception (see sentinel above).
	_, _ = io.WriteString(conn, line)
	_, _ = readFrameLine(reader, maxResponseLineBytes)
}
