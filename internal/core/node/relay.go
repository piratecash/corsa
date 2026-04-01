package node

import (
	"sync"
	"time"

	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"

	"github.com/rs/zerolog/log"
)

const (
	// defaultMaxHops limits the maximum number of hops a relay message can
	// traverse. When hop_count >= max_hops, the message is dropped.
	defaultMaxHops = 10

	// relayStateTTLSeconds is the initial RemainingTTL for a relayForwardState
	// entry. Decremented every second by the cleanup ticker. When it reaches 0
	// the entry is removed. 180 seconds = 3 minutes.
	relayStateTTLSeconds = 180
)

// relayForwardState stores per-message forwarding context on an intermediate
// node. Each node knows only its own previous_hop and forwarded_to —
// no single message reveals the full path.
type relayForwardState struct {
	MessageID        string
	PreviousHop      domain.PeerAddress  // who sent this relay to me (transport address)
	ReceiptForwardTo domain.PeerAddress  // = PreviousHop (where to send receipt back)
	ForwardedTo      domain.PeerAddress  // who I forwarded to (for loop detection)
	Recipient        domain.PeerIdentity // final recipient identity (for hop_ack route confirmation)
	RouteOrigin      domain.PeerIdentity // route origin from routing decision (for triple-scoped hop_ack)
	HopCount         int                 // incremented on each hop
	RemainingTTL     int                 // seconds until cleanup (decremented by ticker)
}

// relayStateStore is a concurrency-safe store for relay forwarding state.
// It is embedded in Service but has its own mutex to avoid contention with
// the main Service.mu on high-throughput relay paths.
//
// Capacity limits: the store enforces maxRelayStates (global) and
// maxRelayStatesPerPeer (per PreviousHop) to prevent unbounded growth
// under relay floods. When a limit is hit, new entries are rejected —
// the relay message is silently dropped (same as dedupe).
type relayStateStore struct {
	mu       sync.Mutex
	states   map[string]*relayForwardState // keyed by message ID
	perPeer  map[domain.PeerAddress]int    // PreviousHop → count of active states
	stopCh   chan struct{}
	onEvict  func() // called after TTL ticker removes expired entries; set by Service for persistence
	rejected int64  // counter for monitoring: entries rejected due to capacity
}

func newRelayStateStore() *relayStateStore {
	return &relayStateStore{
		states:  make(map[string]*relayForwardState),
		perPeer: make(map[domain.PeerAddress]int),
		stopCh:  make(chan struct{}),
	}
}

// start launches the TTL decrement ticker. Call stop() to terminate.
func (rs *relayStateStore) start() {
	go rs.ttlTicker()
}

// stop terminates the TTL ticker goroutine.
func (rs *relayStateStore) stop() {
	close(rs.stopCh)
}

// ttlTicker decrements RemainingTTL every second and removes expired entries.
// Uses a local ticker — no wall-clock comparison between nodes.
// When entries are evicted and an onEvict callback is set, it is called to
// trigger durable persistence of the updated relay state.
func (rs *relayStateStore) ttlTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if rs.decrementTTLsAndReport() && rs.onEvict != nil {
				rs.onEvict()
			}
		case <-rs.stopCh:
			return
		}
	}
}

func (rs *relayStateStore) decrementTTLs() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for id, state := range rs.states {
		state.RemainingTTL--
		if state.RemainingTTL <= 0 {
			if state.PreviousHop != "" {
				rs.perPeer[state.PreviousHop]--
				if rs.perPeer[state.PreviousHop] <= 0 {
					delete(rs.perPeer, state.PreviousHop)
				}
			}
			delete(rs.states, id)
		}
	}
}

// hasSeen returns true if a relayForwardState already exists for this message
// ID — the message has already been relayed through this node.
func (rs *relayStateStore) hasSeen(messageID string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	_, ok := rs.states[messageID]
	return ok
}

// tryReserve atomically checks whether messageID has been seen and, if not,
// inserts a placeholder entry. Returns true when the caller wins the
// reservation (first claim). This replaces the racy hasSeen+store pattern
// by combining both into a single critical section.
//
// previousHop identifies the transport address of the peer that sent the
// relay. It is used for per-peer quota enforcement. Pass "" for origin
// entries (the node that created the relay message).
//
// Capacity enforcement: returns false (same as dedupe) when the global
// limit (maxRelayStates) or per-peer limit (maxRelayStatesPerPeer) is
// reached. The caller treats this identically to a duplicate — the relay
// message is silently dropped.
func (rs *relayStateStore) tryReserve(messageID string, previousHop domain.PeerAddress) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.states[messageID]; ok {
		return false
	}
	// Enforce global capacity.
	if len(rs.states) >= maxRelayStates {
		rs.rejected++
		return false
	}
	// Enforce per-peer capacity.
	if previousHop != "" && rs.perPeer[previousHop] >= maxRelayStatesPerPeer {
		rs.rejected++
		return false
	}
	rs.states[messageID] = &relayForwardState{
		MessageID:    messageID,
		PreviousHop:  previousHop,
		RemainingTTL: relayStateTTLSeconds,
	}
	if previousHop != "" {
		rs.perPeer[previousHop]++
	}
	return true
}

// release removes a previously reserved messageID. Used on error paths
// (max hops, client node, store rejection) so the slot does not block
// a legitimate retry from a different hop.
func (rs *relayStateStore) release(messageID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if state, ok := rs.states[messageID]; ok {
		if state.PreviousHop != "" {
			rs.perPeer[state.PreviousHop]--
			if rs.perPeer[state.PreviousHop] <= 0 {
				delete(rs.perPeer, state.PreviousHop)
			}
		}
		delete(rs.states, messageID)
	}
}

// updateState overwrites the placeholder created by tryReserve with a
// fully populated relayForwardState.
func (rs *relayStateStore) updateState(state *relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.states[state.MessageID] = state
}

// store saves forwarding state for a relayed message. Returns false if the
// message was already seen (dedupe) or the store is at capacity.
func (rs *relayStateStore) store(state *relayForwardState) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.states[state.MessageID]; ok {
		return false
	}
	if len(rs.states) >= maxRelayStates {
		rs.rejected++
		return false
	}
	if state.PreviousHop != "" && rs.perPeer[state.PreviousHop] >= maxRelayStatesPerPeer {
		rs.rejected++
		return false
	}
	rs.states[state.MessageID] = state
	if state.PreviousHop != "" {
		rs.perPeer[state.PreviousHop]++
	}
	return true
}

// lookupReceiptForwardTo returns the address to forward a receipt back to
// for the given message ID, or "" if unknown.
func (rs *relayStateStore) lookupReceiptForwardTo(messageID string) domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.ReceiptForwardTo
}

// lookupRecipient returns the final recipient identity for a relayed message,
// or "" if the message ID is unknown. Used by hop_ack processing to confirm
// the routing table entry for the recipient.
func (rs *relayStateStore) lookupRecipient(messageID string) domain.PeerIdentity {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.Recipient
}

// lookupRouteOrigin returns the route origin for a relayed message, or ""
// if the message ID is unknown or origin was not recorded. Used by hop_ack
// processing to scope route confirmation to the exact (Identity, Origin,
// NextHop) triple that carried the message.
func (rs *relayStateStore) lookupRouteOrigin(messageID string) domain.PeerIdentity {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.RouteOrigin
}

// snapshot returns a copy of all relay forward states for persistence.
func (rs *relayStateStore) snapshot() []relayForwardState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]relayForwardState, 0, len(rs.states))
	for _, state := range rs.states {
		out = append(out, *state)
	}
	return out
}

// restore loads relay forward states from persisted data. Rebuilds per-peer
// counters from the loaded entries.
func (rs *relayStateStore) restore(states []relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range states {
		s := states[i]
		if s.RemainingTTL > 0 {
			rs.states[s.MessageID] = &s
			if s.PreviousHop != "" {
				rs.perPeer[s.PreviousHop]++
			}
		}
	}
}

// count returns the number of active relay forward states.
func (rs *relayStateStore) count() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return len(rs.states)
}

// removed returns true if any entries were removed during the last
// decrementTTLs call. Used by the TTL ticker to trigger persistence
// only when state actually changed.
func (rs *relayStateStore) decrementTTLsAndReport() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	removed := false
	for id, state := range rs.states {
		state.RemainingTTL--
		if state.RemainingTTL <= 0 {
			if state.PreviousHop != "" {
				rs.perPeer[state.PreviousHop]--
				if rs.perPeer[state.PreviousHop] <= 0 {
					delete(rs.perPeer, state.PreviousHop)
				}
			}
			delete(rs.states, id)
			removed = true
		}
	}
	return removed
}

// persistRelayState takes a queue-state snapshot and persists it to disk.
// Called after every relay state mutation (store, forward, deliver) to ensure
// recently learned relay paths survive a restart. Without this, in-memory
// mutations would only be persisted when some unrelated persistence event
// happened to trigger a snapshot.
func (s *Service) persistRelayState() {
	s.mu.RLock()
	snapshot := s.queueStateSnapshotLocked()
	s.mu.RUnlock()
	s.persistQueueState(snapshot)
}

// handleRelayMessage processes an incoming relay_message frame and returns the
// semantic hop-ack status. The caller is responsible for delivering the ack to
// the sender — this function does NOT send an ack itself, because the delivery
// mechanism depends on context (direct conn write in handleJSONCommand vs
// enqueuePeerFrame in handlePeerSessionFrame).
//
// Return values:
//   - "delivered" — this node is the final recipient, message accepted and stored locally
//   - "forwarded" — message relayed to the next hop
//   - "stored"    — no capable next hop, message stored locally for later delivery
//   - ""          — message was dropped (dedupe, max hops, client node, rejected by
//     storeIncomingMessage); no ack needed
//
// handleRelayMessage processes an incoming relay_message frame.
//
// syncSession is an optional outbound peer session that can be reused for
// on-demand sender-key sync (fetch_contacts). When the relay arrives on an
// inbound connection, the caller looks up the outbound session to the same
// peer and passes it here — this avoids opening a new TCP connection and
// handles NATed peers whose transport address is not redialable. When the
// relay arrives on an outbound session that may be inside a
// peerSessionRequest read loop, the caller passes nil to avoid a deadlock
// on the single-reader inboxCh.
func (s *Service) handleRelayMessage(senderAddress domain.PeerAddress, syncSession *peerSession, frame protocol.Frame) string {
	messageID := frame.ID
	recipient := frame.Recipient
	hopCount := frame.HopCount
	maxHops := frame.MaxHops
	originSender := frame.Address // original message sender

	if maxHops <= 0 {
		maxHops = defaultMaxHops
	}

	// Relay is a DM-only mechanism. The origin path
	// (tryRelayToCapableFullNodes) only fires for topic "dm"; reject
	// anything else to prevent non-DM payloads from being stored via
	// a code path with different lifecycle semantics.
	if frame.Topic != "dm" {
		log.Debug().
			Str("id", messageID).
			Str("topic", frame.Topic).
			Msg("relay_drop_non_dm_topic")
		return ""
	}

	// Am I the final recipient?
	if recipient == s.identity.Address {
		// Dedupe at final hop: if relay state already exists, this is a
		// duplicate relay_message. Drop silently — the valid reverse-path
		// state from the first delivery must not be overwritten or released.
		if !s.relayStates.tryReserve(messageID, senderAddress) {
			log.Info().
				Str("id", messageID).
				Str("from", originSender).
				Msg("relay_drop_duplicate_final_hop")
			return ""
		}
		log.Info().Str("id", messageID).Str("sender", originSender).Str("sync_from", string(senderAddress)).Msg("relay_final_hop_delivering")
		// Upgrade the placeholder reservation to full relay state BEFORE
		// delivery so the receipt reverse path is available when
		// storeIncomingMessage fires emitDeliveryReceipt in a goroutine.
		s.relayStates.updateState(&relayForwardState{
			MessageID:        messageID,
			PreviousHop:      senderAddress,
			ReceiptForwardTo: senderAddress,
			ForwardedTo:      "",
			Recipient:        domain.PeerIdentity(recipient),
			HopCount:         hopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		if !s.deliverRelayedMessage(senderAddress, syncSession, frame) {
			s.relayStates.release(messageID)
			log.Warn().
				Str("id", messageID).
				Str("from", originSender).
				Msg("relay_deliver_rejected_locally")
			return ""
		}
		s.persistRelayState()
		log.Info().
			Str("id", messageID).
			Str("from", originSender).
			Int("hops", hopCount).
			Msg("relay_delivered_locally")
		return "delivered"
	}

	// Dedupe: atomically reserve this message_id. If another goroutine
	// already claimed it, drop silently. tryReserve inserts a placeholder
	// entry so no concurrent handler can pass this point for the same ID.
	if !s.relayStates.tryReserve(messageID, senderAddress) {
		log.Debug().
			Str("id", messageID).
			Str("from", originSender).
			Msg("relay_drop_duplicate")
		return ""
	}

	// Max hops exceeded? Release the reservation.
	if hopCount >= maxHops {
		s.relayStates.release(messageID)
		log.Debug().
			Str("id", messageID).
			Int("hop_count", hopCount).
			Int("max_hops", maxHops).
			Msg("relay_drop_max_hops")
		return ""
	}

	// Only forward if this node can relay (full node).
	// Client nodes may be senders or final recipients, but never
	// intermediate relay hops.
	if !s.CanForward() {
		s.relayStates.release(messageID)
		log.Debug().
			Str("id", messageID).
			Msg("relay_drop_client_node")
		return ""
	}

	newHopCount := hopCount + 1

	// Build the forwarded frame.
	forwardFrame := protocol.Frame{
		Type:        "relay_message",
		ID:          messageID,
		Address:     originSender,
		Recipient:   recipient,
		Topic:       frame.Topic,
		Body:        frame.Body,
		Flag:        frame.Flag,
		CreatedAt:   frame.CreatedAt,
		TTLSeconds:  frame.TTLSeconds,
		HopCount:    newHopCount,
		MaxHops:     maxHops,
		PreviousHop: string(s.identity.Address),
	}

	// Try direct peer first — is the recipient directly connected?
	// A recipient identity may have multiple session addresses (reconnects,
	// address changes), so we try all matching sessions until one succeeds.
	forwardedTo := s.tryForwardToDirectPeer(domain.PeerIdentity(recipient), forwardFrame)

	// Table-directed relay (Phase 1.2): if no direct peer, consult the
	// routing table for a next-hop. This enables multi-hop relay chains
	// A→B→D where B uses the table to find D even though D is not the
	// final recipient's direct peer on B. Exclude the sender (from
	// incoming frame.PreviousHop) to prevent sending the message back.
	var tableRouteOrigin domain.PeerIdentity
	if forwardedTo == "" {
		result := s.tryForwardViaRoutingTable(domain.PeerIdentity(recipient), forwardFrame, domain.PeerIdentity(frame.PreviousHop))
		forwardedTo = result.Address
		tableRouteOrigin = result.RouteOrigin
	}

	// If no direct peer and no table route, gossip to capable peers.
	if forwardedTo == "" {
		forwardedTo = s.relayViaGossip(forwardFrame, senderAddress)
	}

	if forwardedTo == "" {
		// No capable relay next hop available. deliverRelayedMessage
		// calls storeIncomingMessage, which stores the message AND
		// runs the normal routing path (gossip + push). This is
		// intentional: gossip runs unconditionally (INV-3) and may
		// reach the recipient through non-relay peers. The "stored"
		// status tells the previous hop that relay forwarding did not
		// happen, but the message is not stuck — gossip handles it.
		if !s.deliverRelayedMessage(senderAddress, syncSession, frame) {
			s.relayStates.release(messageID)
			log.Warn().
				Str("id", messageID).
				Str("recipient", recipient).
				Msg("relay_store_rejected_locally")
			return ""
		}
		// Update the reservation placeholder with full state so the
		// receipt reverse path works correctly.
		s.relayStates.updateState(&relayForwardState{
			MessageID:        messageID,
			PreviousHop:      senderAddress,
			ReceiptForwardTo: senderAddress,
			ForwardedTo:      "", // stored locally, not forwarded
			Recipient:        domain.PeerIdentity(recipient),
			HopCount:         newHopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		s.persistRelayState()
		log.Info().
			Str("id", messageID).
			Str("recipient", recipient).
			Msg("relay_stored_no_capable_peers")
		return "stored"
	}

	// Update the reservation placeholder with full forwarding state.
	// ReceiptForwardTo uses the transport address (senderAddress), not the
	// identity fingerprint from frame.PreviousHop. This is critical:
	// sessionHasCapability() and sendReceiptToPeer() look up sessions by
	// transport address, not by identity.
	s.relayStates.updateState(&relayForwardState{
		MessageID:        messageID,
		PreviousHop:      senderAddress,
		ReceiptForwardTo: senderAddress,
		ForwardedTo:      forwardedTo,
		Recipient:        domain.PeerIdentity(recipient),
		RouteOrigin:      tableRouteOrigin,
		HopCount:         newHopCount,
		RemainingTTL:     relayStateTTLSeconds,
	})
	s.persistRelayState()

	log.Info().
		Str("id", messageID).
		Str("from", originSender).
		Str("to", recipient).
		Str("forwarded_to", string(forwardedTo)).
		Int("hop_count", newHopCount).
		Msg("relay_forwarded")
	return "forwarded"
}

// deliverRelayedMessage converts a relay_message frame into a local message
// store operation. Reuses storeIncomingMessage to avoid duplicating
// deduplication, persistence, and UI notification logic.
//
// senderAddress is the transport address of the previous relay hop. When
// storeIncomingMessage rejects the DM with ErrCodeUnknownSenderKey, this
// function syncs keys from the previous hop (which likely knows the
// sender's keys, since it already processed the message) and retries —
// matching the existing push_message / request_inbox behavior.
//
// Returns true if the message was accepted (stored locally), false if it was
// rejected (parse error, invalid signature, unknown key after sync, etc.).
// The caller uses this to decide the hop-ack status: a rejected message must
// NOT be reported as "delivered" or "stored" to the previous hop.
// deliverRelayedMessage stores a relayed message locally. When the message
// sender's keys are unknown, it attempts on-demand key sync from the
// previous hop.
//
// syncSession is an optional outbound session to the relay peer that can be
// reused for fetch_contacts without opening a new TCP connection. Pass nil
// when the caller is inside a peerSessionRequest read loop on the same
// session (single-reader constraint on inboxCh would deadlock).
func (s *Service) deliverRelayedMessage(senderAddress domain.PeerAddress, syncSession *peerSession, frame protocol.Frame) bool {
	msg, err := incomingMessageFromFrame(protocol.Frame{
		ID:         frame.ID,
		Topic:      frame.Topic,
		Address:    frame.Address,
		Recipient:  frame.Recipient,
		Flag:       frame.Flag,
		CreatedAt:  frame.CreatedAt,
		TTLSeconds: frame.TTLSeconds,
		Body:       frame.Body,
	})
	if err != nil {
		log.Warn().Err(err).Str("id", frame.ID).Msg("relay_deliver_parse_error")
		return false
	}
	stored, _, errCode := s.storeIncomingMessage(msg, false)
	if !stored && errCode == protocol.ErrCodeUnknownSenderKey && senderAddress != "" {
		log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Msg("relay_key_sync_start")
		imported := s.syncSenderKeys(senderAddress, syncSession)
		if imported == 0 {
			log.Warn().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Msg("relay_key_sync_no_contacts_imported")
		}
		stored, _, errCode = s.storeIncomingMessage(msg, false)
		if stored {
			log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", string(senderAddress)).Int("imported", imported).Msg("relay_deliver_after_key_sync")
		} else {
			log.Warn().Str("id", frame.ID).Str("sender", frame.Address).Str("code", errCode).Int("imported", imported).Msg("relay_key_sync_retry_failed")
		}
	}
	if !stored && errCode != "" {
		log.Warn().Str("id", frame.ID).Str("code", errCode).Msg("relay_deliver_rejected")
		return false
	}
	return stored
}

// sendRelayHopAck sends a relay_hop_ack frame back to the peer that forwarded
// the relay message to us.
func (s *Service) sendRelayHopAck(peerAddress domain.PeerAddress, messageID, status string) {
	ackFrame := protocol.Frame{
		Type:   "relay_hop_ack",
		ID:     messageID,
		Status: status,
	}
	s.enqueuePeerFrame(peerAddress, ackFrame)
}

// handleRelayHopAck processes an incoming relay_hop_ack frame. Currently
// logged for diagnostics; future iterations may use ack data for route
// reputation scoring.
func (s *Service) handleRelayHopAck(senderAddress domain.PeerAddress, frame protocol.Frame) {
	log.Debug().
		Str("id", frame.ID).
		Str("from", string(senderAddress)).
		Str("status", frame.Status).
		Msg("relay_hop_ack_received")

	// Confirm route via hop_ack: if the ack indicates the message was
	// delivered or forwarded, promote the route through the peer that
	// actually sent the ack. We use senderAddress (the transport address
	// of the ack sender) rather than the stored ForwardedTo, because
	// gossip fan-out may send relay_message to multiple peers but only
	// record the first-winner — the ack can arrive from any of them.
	// The ack sender is the peer that provably carried the message.
	//
	// routeOrigin scopes the confirmation to the exact (Identity, Origin,
	// NextHop) triple that was used for the original send. If empty (gossip
	// path), the first matching NextHop in Lookup order is promoted.
	if frame.Status == "delivered" || frame.Status == "forwarded" {
		recipient := s.relayStates.lookupRecipient(frame.ID)
		if recipient != "" {
			routeOrigin := s.relayStates.lookupRouteOrigin(frame.ID)
			s.confirmRouteViaHopAck(recipient, senderAddress, routeOrigin)
		}
	}
}

// tryForwardToDirectPeer iterates all outbound sessions whose peerIdentity
// matches the recipient. For each match it checks mesh_relay_v1 capability
// and tries to enqueue the frame. Returns the address of the first session
// that accepted the frame, or "" if none did. This avoids the problem where
// a random non-capable or non-writable session shadows a healthy direct path.
func (s *Service) tryForwardToDirectPeer(recipient domain.PeerIdentity, frame protocol.Frame) domain.PeerAddress {
	s.mu.RLock()
	var candidates []domain.PeerAddress
	for address, session := range s.sessions {
		if session.peerIdentity == recipient {
			candidates = append(candidates, address)
		}
	}
	s.mu.RUnlock()

	for _, address := range candidates {
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if s.enqueuePeerFrame(address, frame) {
			return address
		}
	}
	return domain.PeerAddress("")
}

// relayViaGossip forwards a relay_message to the top-scored peers that
// have the mesh_relay_v1 capability. The senderAddress is excluded to
// avoid sending the message back to where it came from. Per-peer rate
// limiting is applied: if a target's token bucket is exhausted, the
// frame is silently skipped for that target.
func (s *Service) relayViaGossip(frame protocol.Frame, excludeAddress domain.PeerAddress) domain.PeerAddress {
	targets := s.routingTargets()
	var forwardedTo domain.PeerAddress
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) || address == excludeAddress {
			continue
		}
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		frame.PreviousHop = s.identity.Address
		if s.enqueuePeerFrame(address, frame) {
			if forwardedTo == "" {
				forwardedTo = address
			}
		}
	}
	return forwardedTo
}

// handleRelayReceipt processes a delivery receipt that may need to be
// forwarded back along the relay chain via receipt_forward_to lookup.
// Returns true if the receipt was forwarded via the relay chain (the caller
// should NOT additionally gossip it). Returns false if no relay path was
// found or the relay hop failed — the caller is responsible for gossip
// fallback in that case.
func (s *Service) handleRelayReceipt(receipt protocol.DeliveryReceipt) bool {
	forwardTo := s.relayStates.lookupReceiptForwardTo(string(receipt.MessageID))
	if forwardTo == "" {
		return false
	}

	// Forward the receipt one hop back toward the original sender.
	if s.sessionHasCapability(forwardTo, domain.CapMeshRelayV1) {
		if s.sendReceiptToPeer(forwardTo, receipt) {
			log.Debug().
				Str("message_id", string(receipt.MessageID)).
				Str("forward_to", string(forwardTo)).
				Msg("relay_receipt_forwarded")
			return true
		}
		log.Debug().
			Str("message_id", string(receipt.MessageID)).
			Str("forward_to", string(forwardTo)).
			Msg("relay_receipt_send_failed")
		return false
	}

	log.Debug().
		Str("message_id", string(receipt.MessageID)).
		Str("forward_to", string(forwardTo)).
		Msg("relay_receipt_hop_unavailable")
	return false
}

// tryRelayToCapableFullNodes sends relay_message to full-node peers with
// mesh_relay_v1 capability. This is a fire-and-forget optimization on top
// of the gossip baseline — the caller MUST still run gossip unconditionally.
//
// Only full nodes are targeted because client nodes cannot forward relay
// messages (handleRelayMessage drops at CanForward check). Receivers
// dedupe via seen[messageID] if gossip arrives first.
func (s *Service) tryRelayToCapableFullNodes(msg protocol.Envelope, targets []domain.PeerAddress) {
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			continue
		}
		if s.peerIsClientNode(address) {
			continue
		}
		if !s.relayLimiter.allow(address) {
			continue
		}
		s.sendRelayMessage(address, msg)
	}
}

// relayStatusFrame returns diagnostic information about the relay subsystem
// for the fetch_relay_status RPC command.
func (s *Service) relayStatusFrame() protocol.Frame {
	activeStates := s.relayStates.count()
	capablePeers := s.countCapablePeers(domain.CapMeshRelayV1)

	return protocol.Frame{
		Type:   "relay_status",
		Status: "ok",
		Count:  activeStates,
		Limit:  capablePeers,
	}
}

// countCapablePeers returns the number of unique connected peers (both
// outbound sessions and inbound connections) that have the specified
// capability. A peer that appears in both maps is counted once.
func (s *Service) countCapablePeers(cap domain.Capability) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[domain.PeerIdentity]struct{})

	for _, session := range s.sessions {
		for _, c := range session.capabilities {
			if c == cap {
				seen[session.peerIdentity] = struct{}{}
				break
			}
		}
	}

	for _, info := range s.connPeerInfo {
		if _, dup := seen[info.identity]; dup {
			continue
		}
		for _, c := range info.capabilities {
			if c == cap {
				seen[info.identity] = struct{}{}
				break
			}
		}
	}

	return len(seen)
}

// sendRelayMessage creates a relay_message frame from an envelope and sends
// it to the specified peer. Returns true if the frame was enqueued (live
// session) or queued (persistent fallback). Used when the routing decision
// indicates a relay-capable next hop.
func (s *Service) sendRelayMessage(address domain.PeerAddress, msg protocol.Envelope) bool {
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

	sent := s.enqueuePeerFrame(address, frame)
	if !sent {
		sent = s.queuePeerFrame(address, frame)
		if sent {
			log.Debug().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("peer", string(address)).
				Str("mode", "queued").
				Msg("relay_message_queued")
		}
	}

	if !sent {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return false
	}

	// Store forwarding state for dedupe and receipt routing.
	// This is the origin node (hop_count=1), so ReceiptForwardTo is empty:
	// the receipt terminates here — there is no previous hop to forward to.
	// On intermediate nodes, handleRelayMessage stores the sender's transport
	// address as ReceiptForwardTo.
	s.relayStates.store(&relayForwardState{
		MessageID:        string(msg.ID),
		PreviousHop:      "",
		ReceiptForwardTo: "",
		ForwardedTo:      address,
		Recipient:        domain.PeerIdentity(msg.Recipient),
		HopCount:         1,
		RemainingTTL:     relayStateTTLSeconds,
	})
	s.persistRelayState()
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("mode", "session").
		Msg("relay_message_sent")
	return true
}

// sendRelayMessageWithOrigin is the table-directed variant of sendRelayMessage.
// It stores the route Origin in relayForwardState so that hop_ack confirmation
// can match the exact (Identity, Origin, NextHop) triple that carried the
// message. The gossip path (sendRelayMessage) leaves RouteOrigin empty because
// gossip delivery is not scoped to a specific route triple.
func (s *Service) sendRelayMessageWithOrigin(address domain.PeerAddress, msg protocol.Envelope, routeOrigin domain.PeerIdentity) bool {
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

	sent := s.enqueuePeerFrame(address, frame)
	if !sent {
		sent = s.queuePeerFrame(address, frame)
		if sent {
			log.Debug().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("peer", string(address)).
				Str("mode", "queued").
				Msg("relay_message_queued")
		}
	}

	if !sent {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", string(address)).
			Str("mode", "dropped").
			Msg("relay_message_dropped")
		return false
	}

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
			Str("peer", string(address)).
			Msg("relay_state_store_failed_outbound")
		return false
	}
	s.persistRelayState()
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", string(address)).
		Str("origin", string(routeOrigin)).
		Str("mode", "session").
		Msg("relay_message_sent_table_directed")
	return true
}
