package node

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
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
	MessageID            string
	PreviousHop          domain.PeerAddress   // who sent this relay to me (transport address)
	ReceiptForwardTo     domain.PeerAddress   // = PreviousHop (where to send receipt back)
	ForwardedTo          domain.PeerAddress   // who I forwarded to (for loop detection)
	AbandonedForwardedTo []domain.PeerAddress // old ForwardedTo values from reroutes (stale ack rejection)
	Recipient            domain.PeerIdentity  // final recipient identity (for hop_ack route confirmation)
	RouteOrigin          domain.PeerIdentity  // route origin from routing decision (for triple-scoped hop_ack)
	HopCount             int                  // incremented on each hop
	RemainingTTL         int                  // seconds until cleanup (decremented by ticker)
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
	if existing, ok := rs.states[state.MessageID]; ok {
		// Idempotent upsert: refresh forwarding fields and TTL so that
		// pending queue retries after route churn use the current best
		// path instead of being rejected.
		// PreviousHop and ReceiptForwardTo are preserved from the original
		// store — they describe the upstream path and must not change.
		//
		// Both ForwardedTo and RouteOrigin are always updated. A resend
		// may use the same next-hop peer but a different route origin
		// (e.g. after the origin node re-announces with a new SeqNo),
		// so checking only ForwardedTo would miss same-peer reroutes.
		//
		// AbandonedForwardedTo accumulates all old ForwardedTo addresses
		// from previous reroutes. This lets the stale ack guard reject
		// late acks from any abandoned next-hop, not just the most recent
		// one (e.g. table via A → table via B → gossip via C: both A and
		// B are stale and must be rejected).
		if existing.ForwardedTo != state.ForwardedTo && existing.ForwardedTo != "" {
			existing.AbandonedForwardedTo = append(existing.AbandonedForwardedTo, existing.ForwardedTo)
		}
		existing.ForwardedTo = state.ForwardedTo
		existing.RouteOrigin = state.RouteOrigin
		existing.RemainingTTL = state.RemainingTTL
		return true
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

// lookupForwardedTo returns the address the message was forwarded to,
// or "" if unknown. Used by hop_ack processing to verify that the ack
// sender matches the current forwarding destination — a mismatch
// indicates a stale ack from a previous route and should be ignored.
func (rs *relayStateStore) lookupForwardedTo(messageID string) domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return ""
	}
	return state.ForwardedTo
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

// lookupAbandonedForwardedTo returns all ForwardedTo addresses from previous
// reroutes, or nil if the message was never rerouted.
func (rs *relayStateStore) lookupAbandonedForwardedTo(messageID string) []domain.PeerAddress {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	state, ok := rs.states[messageID]
	if !ok {
		return nil
	}
	return state.AbandonedForwardedTo
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
// mechanism depends on context (direct conn write in dispatchNetworkFrame vs
// enqueuePeerFrame in dispatchPeerSessionFrame).
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
		// Use s.runCtx as the owning lifecycle ctx for the narrow key sync.
		// Threading ctx through handleRelayMessage/deliverRelayedMessage is
		// out of scope for Step 5 (would touch 25+ call sites in tests); the
		// service-run context still provides cancellation on shutdown, which
		// is the property CLAUDE.md protects.
		imported := s.syncSenderKeys(s.runCtx, senderAddress, syncSession)
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
	// Duplicate (stored=false, errCode="") means the message was already
	// delivered earlier. Treat as success so the relay layer sends an ack
	// and the upstream stops retrying — otherwise duplicates cause an
	// infinite retry loop that wastes bandwidth.
	if !stored {
		log.Debug().Str("id", frame.ID).Msg("relay_deliver_duplicate_accepted")
		return true
	}
	return true
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
	// actually sent the ack.
	//
	// Stale ack guard (table-routed path): when RouteOrigin is set, the
	// message was sent via a specific routing table triple. After a
	// reroute, store() updates ForwardedTo and RouteOrigin to reflect the
	// new path. A late ack from the old next-hop would have a different
	// peer identity than ForwardedTo — we skip route confirmation to avoid
	// promoting a stale triple. The comparison is identity-based (not
	// transport address) because a single peer may have multiple sessions
	// (inbound + outbound) and the ack can arrive on any of them.
	//
	// Gossip fan-out: when RouteOrigin is empty, the message was broadcast
	// to multiple peers and ForwardedTo records only the first. An ack
	// from any gossip target is valid — skip the ForwardedTo check.
	//
	// Table→gossip fallback: when a message was first sent via table route
	// and then retried via gossip, store() clears RouteOrigin to empty.
	// A late ack from the old table-routed peer would bypass the stale
	// guard (routeOrigin == ""). AbandonedForwardedTo accumulates all
	// old next-hops across multiple reroutes so the fallback guard can
	// reject stale acks from any of them.
	if frame.Status == "delivered" || frame.Status == "forwarded" {
		recipient := s.relayStates.lookupRecipient(frame.ID)
		if recipient == "" {
			return
		}
		forwardedTo := s.relayStates.lookupForwardedTo(frame.ID)
		if forwardedTo == "" {
			// Message was stored locally, not forwarded to any downstream
			// hop. No route confirmation is appropriate — there is no
			// next-hop triple to promote.
			return
		}
		routeOrigin := s.relayStates.lookupRouteOrigin(frame.ID)
		ackIdentity := s.resolvePeerIdentity(senderAddress)
		// Stale ack guard (table-routed path): when RouteOrigin is set,
		// verify the ack sender matches the peer we forwarded to.
		// Comparison is identity-based because a peer may have multiple
		// transport sessions.
		if routeOrigin != "" {
			fwdIdentity := s.resolvePeerIdentity(forwardedTo)
			if fwdIdentity != "" && ackIdentity != "" && fwdIdentity != ackIdentity {
				log.Debug().
					Str("id", frame.ID).
					Str("ack_identity", string(ackIdentity)).
					Str("forwarded_identity", string(fwdIdentity)).
					Msg("relay_hop_ack_stale_sender_ignored")
				return
			}
		}
		// Stale ack guard (table→gossip fallback): when RouteOrigin is
		// empty and AbandonedForwardedTo is non-empty, one or more reroutes
		// occurred before falling back to gossip. Reject acks from any
		// abandoned next-hop — those are stale and must not promote an
		// old route. Covers chains like A → B → gossip(C) where both A
		// and B are stale.
		if routeOrigin == "" && ackIdentity != "" {
			for _, abandoned := range s.relayStates.lookupAbandonedForwardedTo(frame.ID) {
				abandonedIdentity := s.resolvePeerIdentity(abandoned)
				if abandonedIdentity != "" && abandonedIdentity == ackIdentity {
					log.Debug().
						Str("id", frame.ID).
						Str("ack_identity", string(ackIdentity)).
						Str("abandoned_forwarded_identity", string(abandonedIdentity)).
						Msg("relay_hop_ack_stale_prev_route_ignored")
					return
				}
			}
		}
		s.confirmRouteViaHopAck(recipient, senderAddress, routeOrigin)
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
//
// Activation gate: mirrors sendFrameToIdentity. An outbound session is
// inserted into s.sessions before markPeerConnected seeds s.health, and an
// inbound NetCore is registered before hello/auth populates identity and
// capabilities; during those windows the peer is not yet active. Require a
// present, Connected, non-stalled health entry so relay_status.limit does
// not include peers the rest of the PR treats as not yet active.
func (s *Service) countCapablePeers(cap domain.Capability) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()
	seen := make(map[domain.PeerIdentity]struct{})

	isActive := func(address domain.PeerAddress) bool {
		health := s.health[s.resolveHealthAddress(address)]
		if health == nil || !health.Connected {
			return false
		}
		return s.computePeerStateAtLocked(health, now) != peerStateStalled
	}

	for _, session := range s.sessions {
		if session == nil || !isActive(session.address) {
			continue
		}
		for _, c := range session.capabilities {
			if c == cap {
				seen[session.peerIdentity] = struct{}{}
				break
			}
		}
	}

	// Outbound NetCores are already counted via s.sessions above;
	// skip them here so pre-activation outbound entries cannot
	// inflate the capable-peer count.
	s.forEachInboundConnLocked(func(pc *netcore.NetCore) bool {
		if _, dup := seen[pc.Identity()]; dup {
			return true
		}
		if !isActive(pc.Address()) {
			return true
		}
		if pc.HasCapability(cap) {
			seen[pc.Identity()] = struct{}{}
		}
		return true
	})

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

func (s *Service) gossipReceipt(receipt protocol.DeliveryReceipt) {
	defer crashlog.DeferRecover()
	for _, address := range s.routingTargetsForRecipient(receipt.Recipient) {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		go s.sendReceiptToPeer(address, receipt)
	}
}

func (s *Service) sendReceiptToPeer(address domain.PeerAddress, receipt protocol.DeliveryReceipt) bool {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:        "relay_delivery_receipt",
		ID:          string(receipt.MessageID),
		Address:     receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "session").Str("status", receipt.Status).Msg("route_receipt_attempt")
		return true
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "queued").Str("status", receipt.Status).Msg("route_receipt_attempt")
		return true
	}
	log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("peer", string(address)).Str("mode", "dropped").Str("status", receipt.Status).Msg("route_receipt_attempt")
	return false
}

func (s *Service) retryRelayDeliveries() {
	if !s.CanForward() {
		return
	}

	now := time.Now().UTC()
	for _, msg := range s.retryableRelayMessages(now) {
		attempts := s.noteRelayAttempt(relayMessageKey(msg.ID), now)
		decision := s.router.Route(msg)
		targets := make([]string, len(decision.GossipTargets))
		for i, t := range decision.GossipTargets {
			targets[i] = string(t)
		}
		log.Debug().Str("node", s.identity.Address).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Int("attempts", attempts).Strs("gossip_targets", targets).Msg("relay_retry_message")
		go s.executeGossipTargets(msg, decision.GossipTargets)
		// Table-directed relay (Phase 1.2): mirror the logic in
		// storeIncomingMessage — use the routing table when a next-hop
		// is known, fall back to blind gossip relay otherwise.
		if decision.RelayNextHop != nil {
			s.sendTableDirectedRelay(msg, *decision.RelayNextHop, decision.RelayNextHopAddress, decision.RelayRouteOrigin, decision.RelayNextHopHops)
		} else {
			s.tryRelayToCapableFullNodes(msg, decision.GossipTargets)
		}
	}
	for _, receipt := range s.retryableRelayReceipts(now) {
		log.Debug().Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Int("attempts", s.noteRelayAttempt(relayReceiptKey(receipt), now)).Msg("relay_retry_receipt")
		if !s.handleRelayReceipt(receipt) {
			go s.gossipReceipt(receipt)
		}
	}
}

func (s *Service) retryableRelayMessages(now time.Time) []protocol.Envelope {
	s.mu.Lock()

	items := append([]protocol.Envelope(nil), s.topics["dm"]...)
	out := make([]protocol.Envelope, 0)
	beforeLen := len(s.relayRetry)
	for _, msg := range items {
		key := relayMessageKey(msg.ID)
		if msg.Recipient == "" || msg.Recipient == "*" || msg.Recipient == s.identity.Address {
			delete(s.relayRetry, key)
			continue
		}
		if s.messageDeliveryExpired(msg.CreatedAt, msg.TTLSeconds) {
			delete(s.relayRetry, key)
			continue
		}
		if s.hasReceiptForMessageLocked(msg.Sender, msg.ID) {
			delete(s.relayRetry, key)
			continue
		}
		if !shouldRetryRelayLocked(s.relayRetry, key, now) {
			continue
		}
		out = append(out, msg)
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return out
	}
	s.mu.Unlock()
	return out
}

func (s *Service) retryableRelayReceipts(now time.Time) []protocol.DeliveryReceipt {
	s.mu.Lock()

	out := make([]protocol.DeliveryReceipt, 0)
	beforeLen := len(s.relayRetry)
	for _, list := range s.receipts {
		for _, receipt := range list {
			key := relayReceiptKey(receipt)
			if receipt.Recipient == "" || receipt.Recipient == s.identity.Address {
				delete(s.relayRetry, key)
				continue
			}
			if !shouldRetryRelayLocked(s.relayRetry, key, now) {
				continue
			}
			out = append(out, receipt)
		}
	}
	afterLen := len(s.relayRetry)
	if beforeLen != afterLen {
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return out
	}
	s.mu.Unlock()
	return out
}

func shouldRetryRelayLocked(items map[string]relayAttempt, key string, now time.Time) bool {
	state, ok := items[key]
	if !ok {
		return false
	}
	firstSeen := state.FirstSeen
	if firstSeen.IsZero() {
		firstSeen = now
	}
	if now.Sub(firstSeen) > relayRetryTTL {
		delete(items, key)
		return false
	}
	if state.LastAttempt.IsZero() {
		return true
	}
	return now.Sub(state.LastAttempt) >= relayRetryBackoff(state.Attempts)
}

func relayRetryBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 5 * time.Second
	}
	backoff := 5 * time.Second
	for i := 1; i < attempts; i++ {
		backoff *= 2
		if backoff >= 30*time.Second {
			return 30 * time.Second
		}
	}
	return backoff
}

func (s *Service) noteRelayAttempt(key string, now time.Time) int {
	s.mu.Lock()
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		state.FirstSeen = now
	}
	state.LastAttempt = now
	state.Attempts++
	s.relayRetry[key] = state
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return state.Attempts
}

func (s *Service) trackRelayMessage(msg protocol.Envelope) {
	if msg.Topic != "dm" || msg.Recipient == "" || msg.Recipient == "*" {
		return
	}
	s.mu.Lock()
	key := relayMessageKey(msg.ID)
	state := s.relayRetry[key]
	if state.FirstSeen.IsZero() {
		// Enforce capacity: reject new entries when the retry map is full.
		if len(s.relayRetry) >= maxRelayRetryEntries {
			s.mu.Unlock()
			return
		}
		state.FirstSeen = time.Now().UTC()
		s.relayRetry[key] = state
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return
	}
	s.mu.Unlock()
}

func (s *Service) trackRelayReceipt(receipt protocol.DeliveryReceipt) {
	s.mu.Lock()
	key := relayReceiptKey(receipt)
	state := s.relayRetry[key]
	dirty := false
	if state.FirstSeen.IsZero() {
		// Enforce capacity: reject new entries when the retry map is full.
		// Deletions below (message key cleanup) still proceed.
		if len(s.relayRetry) >= maxRelayRetryEntries {
			// Still try to clean up the message key below before returning.
		} else {
			state.FirstSeen = time.Now().UTC()
			s.relayRetry[key] = state
			dirty = true
		}
	}
	if _, ok := s.relayRetry[relayMessageKey(receipt.MessageID)]; ok {
		delete(s.relayRetry, relayMessageKey(receipt.MessageID))
		dirty = true
	}
	if !dirty {
		s.mu.Unlock()
		return
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func relayMessageKey(id protocol.MessageID) string {
	return "msg|" + string(id)
}

func relayReceiptKey(receipt protocol.DeliveryReceipt) string {
	return "receipt|" + receipt.Recipient + "|" + string(receipt.MessageID) + "|" + receipt.Status
}

func (s *Service) hasReceiptForMessageLocked(originalSender string, messageID protocol.MessageID) bool {
	for _, receipt := range s.receipts[originalSender] {
		if receipt.MessageID == messageID {
			return true
		}
	}
	return false
}

func (s *Service) deleteBacklogMessageForRecipient(recipient string, messageID protocol.MessageID) int {
	s.mu.Lock()
	before := len(s.topics["dm"])
	filtered := s.topics["dm"][:0]
	for _, msg := range s.topics["dm"] {
		if msg.ID == messageID && msg.Recipient == recipient {
			delete(s.relayRetry, relayMessageKey(msg.ID))
			continue
		}
		filtered = append(filtered, msg)
	}
	if len(filtered) == 0 {
		delete(s.topics, "dm")
	} else {
		s.topics["dm"] = filtered
	}
	removed := before - len(filtered)
	if removed <= 0 {
		s.mu.Unlock()
		return 0
	}
	log.Debug().Str("node", s.identity.Address).Str("recipient", recipient).Str("id", string(messageID)).Int("before", before).Int("after", len(filtered)).Int("removed", removed).Msg("deleteBacklogMessageForRecipient")
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return removed
}

func (s *Service) deleteBacklogReceiptForRecipient(recipient string, messageID protocol.MessageID, status string) int {
	s.mu.Lock()
	list := s.receipts[recipient]
	if len(list) == 0 {
		s.mu.Unlock()
		return 0
	}
	filtered := list[:0]
	removed := 0
	for _, receipt := range list {
		if receipt.MessageID == messageID && receipt.Recipient == recipient && receipt.Status == status {
			delete(s.relayRetry, relayReceiptKey(receipt))
			removed++
			continue
		}
		filtered = append(filtered, receipt)
	}
	if len(filtered) == 0 {
		delete(s.receipts, recipient)
	} else {
		s.receipts[recipient] = filtered
	}
	if removed <= 0 {
		s.mu.Unlock()
		return 0
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
	return removed
}

func (s *Service) queueStateSnapshot() queueStateFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queueStateSnapshotLocked()
}

func (s *Service) queueStateSnapshotLocked() queueStateFile {
	pending := make(map[string][]pendingFrame, len(s.pending))
	for address, items := range s.pending {
		if len(items) == 0 {
			continue
		}
		frames := make([]pendingFrame, len(items))
		copy(frames, items)
		pending[string(address)] = frames
	}

	relayRetry := make(map[string]relayAttempt, len(s.relayRetry))
	for key, item := range s.relayRetry {
		relayRetry[key] = item
	}
	relayMessages := make([]protocol.Envelope, 0, len(s.topics["dm"]))
	for _, msg := range s.topics["dm"] {
		if _, ok := relayRetry[relayMessageKey(msg.ID)]; ok {
			relayMessages = append(relayMessages, msg)
		}
	}
	relayReceipts := make([]protocol.DeliveryReceipt, 0)
	for _, list := range s.receipts {
		for _, receipt := range list {
			if _, ok := relayRetry[relayReceiptKey(receipt)]; ok {
				relayReceipts = append(relayReceipts, receipt)
			}
		}
	}
	outbound := make(map[string]outboundDelivery, len(s.outbound))
	for key, item := range s.outbound {
		outbound[key] = item
	}

	orphaned := make(map[string][]pendingFrame, len(s.orphaned))
	for addr, items := range s.orphaned {
		orphaned[string(addr)] = append([]pendingFrame(nil), items...)
	}

	return queueStateFile{
		Version:            queueStateVersion,
		Pending:            pending,
		Orphaned:           orphaned,
		RelayRetry:         relayRetry,
		RelayMessages:      relayMessages,
		RelayReceipts:      relayReceipts,
		OutboundState:      outbound,
		RelayForwardStates: s.relayStates.snapshot(),
	}
}

func (s *Service) persistQueueState(snapshot queueStateFile) {
	path := s.cfg.EffectiveQueueStatePath()
	if err := saveQueueState(path, snapshot); err != nil {
		log.Error().Str("path", path).Err(err).Msg("queue state save failed")
	}
}

func sanitizeRelayState(items map[string]relayAttempt, messages []protocol.Envelope, receipts []protocol.DeliveryReceipt) {
	valid := make(map[string]struct{}, len(messages)+len(receipts))
	for _, msg := range messages {
		valid[relayMessageKey(msg.ID)] = struct{}{}
	}
	for _, receipt := range receipts {
		valid[relayReceiptKey(receipt)] = struct{}{}
	}
	for key := range items {
		if _, ok := valid[key]; !ok {
			delete(items, key)
		}
	}
}

func (s *Service) noteOutboundQueuedLocked(frame protocol.Frame, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = "queued"
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) markOutboundRetrying(frame protocol.Frame, queuedAt time.Time, retries int, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	s.mu.Lock()
	s.markOutboundRetryingLocked(frame, queuedAt, retries, errText)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

// markOutboundRetryingLocked updates outbound state to "retrying" without
// persisting. Caller must hold s.mu and is responsible for persisting
// afterwards. Used by drainPendingForIdentities to batch all state changes
// into a single persist at the end of the drain cycle.
func (s *Service) markOutboundRetryingLocked(frame protocol.Frame, queuedAt time.Time, retries int, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
	}
	if state.QueuedAt.IsZero() {
		state.QueuedAt = queuedAt
	}
	state.Status = "retrying"
	state.Retries = retries
	state.LastAttemptAt = time.Now().UTC()
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) markOutboundTerminal(frame protocol.Frame, status, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	s.mu.Lock()
	s.markOutboundTerminalLocked(frame, status, errText)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

// markOutboundTerminalLocked updates outbound state to a terminal status
// without persisting. Caller must hold s.mu and is responsible for
// persisting afterwards. Used by drainPendingForIdentities to batch all
// state changes into a single persist at the end of the drain cycle.
func (s *Service) markOutboundTerminalLocked(frame protocol.Frame, status, errText string) {
	if frame.Type != "send_message" || frame.ID == "" {
		return
	}
	state := s.outbound[frame.ID]
	if state.MessageID == "" {
		state.MessageID = frame.ID
		state.Recipient = frame.Recipient
		state.QueuedAt = time.Now().UTC()
	}
	state.Status = status
	state.LastAttemptAt = time.Now().UTC()
	if status == "failed" {
		state.Retries++
	}
	state.Error = errText
	s.outbound[frame.ID] = state
}

func (s *Service) clearOutboundQueued(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.outbound[messageID]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.outbound, messageID)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

// clearOutboundQueuedLocked removes outbound delivery state for a message
// without persisting. Caller must hold s.mu and is responsible for
// persisting afterwards. Used by drainPendingForIdentities to batch all
// state changes into a single persist at the end of the drain cycle.
func (s *Service) clearOutboundQueuedLocked(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	delete(s.outbound, messageID)
}

func (s *Service) clearRelayRetryForOutbound(frame protocol.Frame) {
	if frame.Type != "relay_delivery_receipt" || frame.ID == "" || frame.Recipient == "" || frame.Status == "" {
		return
	}

	key := relayReceiptKey(protocol.DeliveryReceipt{
		MessageID: protocol.MessageID(frame.ID),
		Recipient: frame.Recipient,
		Status:    frame.Status,
	})

	s.mu.Lock()
	if _, ok := s.relayRetry[key]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.relayRetry, key)
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
}

func (s *Service) emitDeliveryReceipt(msg incomingMessage) {
	defer crashlog.DeferRecover()
	receipt := protocol.DeliveryReceipt{
		MessageID:   msg.ID,
		Sender:      s.identity.Address,
		Recipient:   msg.Sender,
		Status:      protocol.ReceiptStatusDelivered,
		DeliveredAt: time.Now().UTC(),
	}
	s.storeDeliveryReceipt(receipt)
}

func receiptFrame(receipt protocol.DeliveryReceipt) protocol.ReceiptFrame {
	return protocol.ReceiptFrame{
		MessageID:   string(receipt.MessageID),
		Sender:      receipt.Sender,
		Recipient:   receipt.Recipient,
		Status:      receipt.Status,
		DeliveredAt: receipt.DeliveredAt.UTC().Format(time.RFC3339),
	}
}

func receiptFromFrame(frame protocol.Frame) (protocol.DeliveryReceipt, error) {
	if strings.TrimSpace(frame.ID) == "" || strings.TrimSpace(frame.Address) == "" || strings.TrimSpace(frame.Recipient) == "" || strings.TrimSpace(frame.Status) == "" || strings.TrimSpace(frame.DeliveredAt) == "" {
		return protocol.DeliveryReceipt{}, fmt.Errorf("missing delivery receipt fields")
	}
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}

	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}

	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.ID),
		Sender:      frame.Address,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
}

func receiptFromReceiptFrame(frame protocol.ReceiptFrame) (protocol.DeliveryReceipt, error) {
	if frame.Status != protocol.ReceiptStatusDelivered && frame.Status != protocol.ReceiptStatusSeen {
		return protocol.DeliveryReceipt{}, fmt.Errorf("invalid delivery receipt status")
	}
	deliveredAt, err := time.Parse(time.RFC3339, frame.DeliveredAt)
	if err != nil {
		return protocol.DeliveryReceipt{}, err
	}
	return protocol.DeliveryReceipt{
		MessageID:   protocol.MessageID(frame.MessageID),
		Sender:      frame.Sender,
		Recipient:   frame.Recipient,
		Status:      frame.Status,
		DeliveredAt: deliveredAt.UTC(),
	}, nil
}
