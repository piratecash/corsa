package node

import (
	"context"
	"sync"
	"time"

	"corsa/internal/core/protocol"

	"github.com/rs/zerolog/log"
)

const (
	// capMeshRelayV1 is the capability token that gates relay_message frames.
	capMeshRelayV1 = "mesh_relay_v1"

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
	PreviousHop      string // who sent this relay to me
	ReceiptForwardTo string // = PreviousHop (where to send receipt back)
	ForwardedTo      string // who I forwarded to (for loop detection)
	HopCount         int    // incremented on each hop
	RemainingTTL     int    // seconds until cleanup (decremented by ticker)
}

// relayStateStore is a concurrency-safe store for relay forwarding state.
// It is embedded in Service but has its own mutex to avoid contention with
// the main Service.mu on high-throughput relay paths.
type relayStateStore struct {
	mu      sync.Mutex
	states  map[string]*relayForwardState // keyed by message ID
	stopCh  chan struct{}
	onEvict func() // called after TTL ticker removes expired entries; set by Service for persistence
}

func newRelayStateStore() *relayStateStore {
	return &relayStateStore{
		states: make(map[string]*relayForwardState),
		stopCh: make(chan struct{}),
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
func (rs *relayStateStore) tryReserve(messageID string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.states[messageID]; ok {
		return false
	}
	rs.states[messageID] = &relayForwardState{
		MessageID:    messageID,
		RemainingTTL: relayStateTTLSeconds,
	}
	return true
}

// release removes a previously reserved messageID. Used on error paths
// (max hops, client node, store rejection) so the slot does not block
// a legitimate retry from a different hop.
func (rs *relayStateStore) release(messageID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.states, messageID)
}

// updateState overwrites the placeholder created by tryReserve with a
// fully populated relayForwardState.
func (rs *relayStateStore) updateState(state *relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.states[state.MessageID] = state
}

// store saves forwarding state for a relayed message. Returns false if the
// message was already seen (dedupe).
func (rs *relayStateStore) store(state *relayForwardState) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if _, ok := rs.states[state.MessageID]; ok {
		return false
	}
	rs.states[state.MessageID] = state
	return true
}

// lookupReceiptForwardTo returns the address to forward a receipt back to
// for the given message ID, or "" if unknown.
func (rs *relayStateStore) lookupReceiptForwardTo(messageID string) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.ReceiptForwardTo
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

// restore loads relay forward states from persisted data.
func (rs *relayStateStore) restore(states []relayForwardState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range states {
		s := states[i]
		if s.RemainingTTL > 0 {
			rs.states[s.MessageID] = &s
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
func (s *Service) handleRelayMessage(senderAddress string, frame protocol.Frame) string {
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
		if !s.relayStates.tryReserve(messageID) {
			log.Debug().
				Str("id", messageID).
				Str("from", originSender).
				Msg("relay_drop_duplicate_final_hop")
			return ""
		}
		// Upgrade the placeholder reservation to full relay state BEFORE
		// delivery so the receipt reverse path is available when
		// storeIncomingMessage fires emitDeliveryReceipt in a goroutine.
		s.relayStates.updateState(&relayForwardState{
			MessageID:        messageID,
			PreviousHop:      senderAddress,
			ReceiptForwardTo: senderAddress,
			ForwardedTo:      "",
			HopCount:         hopCount,
			RemainingTTL:     relayStateTTLSeconds,
		})
		if !s.deliverRelayedMessage(senderAddress, frame) {
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
	if !s.relayStates.tryReserve(messageID) {
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
		PreviousHop: s.identity.Address,
	}

	// Try direct peer first — is the recipient directly connected?
	// A recipient identity may have multiple session addresses (reconnects,
	// address changes), so we try all matching sessions until one succeeds.
	forwardedTo := s.tryForwardToDirectPeer(recipient, forwardFrame)

	// If no direct peer or enqueue failed, gossip to capable peers.
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
		if !s.deliverRelayedMessage(senderAddress, frame) {
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
		HopCount:         newHopCount,
		RemainingTTL:     relayStateTTLSeconds,
	})
	s.persistRelayState()

	log.Info().
		Str("id", messageID).
		Str("from", originSender).
		Str("to", recipient).
		Str("forwarded_to", forwardedTo).
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
func (s *Service) deliverRelayedMessage(senderAddress string, frame protocol.Frame) bool {
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
		// The previous hop likely has the sender's keys — sync and retry.
		refreshCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		s.syncPeer(refreshCtx, senderAddress)
		cancel()
		stored, _, errCode = s.storeIncomingMessage(msg, false)
		if stored {
			log.Info().Str("id", frame.ID).Str("sender", frame.Address).Str("synced_from", senderAddress).Msg("relay_deliver_after_key_sync")
		}
	}
	if !stored && errCode != "" {
		log.Debug().Str("id", frame.ID).Str("code", errCode).Msg("relay_deliver_rejected")
		return false
	}
	return stored
}

// sendRelayHopAck sends a relay_hop_ack frame back to the peer that forwarded
// the relay message to us.
func (s *Service) sendRelayHopAck(peerAddress, messageID, status string) {
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
func (s *Service) handleRelayHopAck(senderAddress string, frame protocol.Frame) {
	log.Debug().
		Str("id", frame.ID).
		Str("from", senderAddress).
		Str("status", frame.Status).
		Msg("relay_hop_ack_received")
}

// tryForwardToDirectPeer iterates all outbound sessions whose peerIdentity
// matches the recipient. For each match it checks mesh_relay_v1 capability
// and tries to enqueue the frame. Returns the address of the first session
// that accepted the frame, or "" if none did. This avoids the problem where
// a random non-capable or non-writable session shadows a healthy direct path.
func (s *Service) tryForwardToDirectPeer(recipient string, frame protocol.Frame) string {
	s.mu.RLock()
	var candidates []string
	for address, session := range s.sessions {
		if session.peerIdentity == recipient {
			candidates = append(candidates, address)
		}
	}
	s.mu.RUnlock()

	for _, address := range candidates {
		if !s.sessionHasCapability(address, capMeshRelayV1) {
			continue
		}
		if s.enqueuePeerFrame(address, frame) {
			return address
		}
	}
	return ""
}

// relayViaGossip forwards a relay_message to the top-scored peers that
// have the mesh_relay_v1 capability. The senderAddress is excluded to
// avoid sending the message back to where it came from.
func (s *Service) relayViaGossip(frame protocol.Frame, excludeAddress string) string {
	targets := s.routingTargets()
	var forwardedTo string
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) || address == excludeAddress {
			continue
		}
		if !s.sessionHasCapability(address, capMeshRelayV1) {
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
	if s.sessionHasCapability(forwardTo, capMeshRelayV1) {
		if s.sendReceiptToPeer(forwardTo, receipt) {
			log.Debug().
				Str("message_id", string(receipt.MessageID)).
				Str("forward_to", forwardTo).
				Msg("relay_receipt_forwarded")
			return true
		}
		log.Debug().
			Str("message_id", string(receipt.MessageID)).
			Str("forward_to", forwardTo).
			Msg("relay_receipt_send_failed")
		return false
	}

	log.Debug().
		Str("message_id", string(receipt.MessageID)).
		Str("forward_to", forwardTo).
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
func (s *Service) tryRelayToCapableFullNodes(msg protocol.Envelope, targets []string) {
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		if !s.sessionHasCapability(address, capMeshRelayV1) {
			continue
		}
		if s.peerIsClientNode(address) {
			continue
		}
		s.sendRelayMessage(address, msg)
	}
}

// relayStatusFrame returns diagnostic information about the relay subsystem
// for the fetch_relay_status RPC command.
func (s *Service) relayStatusFrame() protocol.Frame {
	activeStates := s.relayStates.count()
	capablePeers := s.countCapablePeers(capMeshRelayV1)

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
func (s *Service) countCapablePeers(cap string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, session := range s.sessions {
		for _, c := range session.capabilities {
			if c == cap {
				seen[session.address] = struct{}{}
				break
			}
		}
	}

	for _, info := range s.connPeerInfo {
		if _, dup := seen[info.address]; dup {
			continue
		}
		for _, c := range info.capabilities {
			if c == cap {
				seen[info.address] = struct{}{}
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
func (s *Service) sendRelayMessage(address string, msg protocol.Envelope) bool {
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
				Str("peer", address).
				Str("mode", "queued").
				Msg("relay_message_queued")
		}
	}

	if !sent {
		log.Debug().
			Str("id", string(msg.ID)).
			Str("recipient", msg.Recipient).
			Str("peer", address).
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
		HopCount:         1,
		RemainingTTL:     relayStateTTLSeconds,
	})
	s.persistRelayState()
	log.Debug().
		Str("id", string(msg.ID)).
		Str("recipient", msg.Recipient).
		Str("peer", address).
		Str("mode", "session").
		Msg("relay_message_sent")
	return true
}
