package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Transit-DM retention + hop budget — bounds for blind (gossip) delivery.
//
// Background: relays hold transit DMs (neither party is this node) in
// s.topics["dm"] for store-and-forward. Before this layer the backlog was
// IMMORTAL: ordinary DMs are sent with TTLSeconds=0 (no auto-delete flag),
// cleanupExpiredMessagesForce only handles MessageFlagAutoDeleteTTL, and
// relayRetryTTL (3 min) stops the retries but never removes the envelope.
// Every transit DM to a never-returning recipient pinned its parsed body
// in RAM forever — the third memory leak found in the June 2026 hunt
// (pprof: storeIncomingMessage / readFrameLine / ParseFrameLine all
// climbing in lock-step).
//
// The design follows what comparable meshes do (docs/_refs):
//
//   - bitchat: 24h store-and-forward TTL, 100 msgs/peer FIFO cap,
//     hop-count TTL 7 decremented per relay, dedup before relay.
//   - SimpleX: 21d queue TTL, 128 msgs per queue, QUOTA backpressure,
//     sender owns retries.
//   - Tox: no relay storage at all — sender retries end-to-end.
//
// Corsa's queue-persistence removal (v1.0.53) already declared relay
// storage best-effort ("the sender retries end-to-end"). This layer
// finishes that thought: NODES DO NOT STORE USER MESSAGES — transit is
// forwarding only. A transit envelope lives in s.topics solely as the
// in-flight buffer of its own forwarding operation (the 3-minute
// relayRetry cycle needs the payload to re-attempt sends to peers that
// were busy/reconnecting), and is unconditionally dropped once that
// window closes. Offline delivery is the SENDER's responsibility: the
// sender's own node keeps its outgoing messages (isLocal) and re-sends
// via its pending rings / retry when the recipient appears.
//
//   - transitInFlightWindow caps how long a transit envelope survives,
//     anchored at LOCAL StoredAt (sender-controlled CreatedAt cannot
//     dodge it). Sized as relayRetryTTL plus slack: exactly the
//     lifetime of the forwarding operation, NOT a mailbox.
//   - maxTransitPerRecipient caps any single recipient's in-flight
//     backlog (FIFO — oldest transit evicted first).
//   - maxTransitBacklogBytes is the global payload budget across all
//     transit messages — the last-resort defence against deliberate
//     RAM-occupation floods.
//
// Hop budget (the wire half, MessageFrame.Hops): every relay forwards
// with budget-1 and refuses to re-gossip below 1, bounding blind-
// delivery propagation radius the way bitchat's TTL=7 does. A frame
// without the field (legacy peer or local send) is treated AS IF THIS
// NODE ORIGINATED IT: full defaultMessageHopBudget. The value 0 is
// never emitted on the wire — a message whose decremented budget hits
// 0 is stored/delivered but not re-gossiped.
// ---------------------------------------------------------------------------

const (
	// defaultMessageHopBudget is the propagation budget stamped on
	// locally originated DMs and on frames arriving without the Hops
	// field (legacy peers). bitchat ships 7 on a BLE mesh; Corsa's
	// overlay diameters are comparable, +1 headroom for sparse
	// topologies.
	defaultMessageHopBudget = 8

	// transitInFlightWindow is how long a transit envelope (neither
	// sender nor recipient is this node) survives in s.topics: the
	// relayRetryTTL (3 min) forwarding-retry cycle plus slack. NOT a
	// store-and-forward TTL — relays do not store user messages; this
	// is the in-flight buffer of the forwarding operation itself.
	// Without it, a single failed send (peer queue full, reconnect in
	// progress) would be unrecoverable: the sender's own retry copy is
	// rejected by the bloom dedup as a duplicate, so the retry MUST be
	// driven from this hop while the window is open.
	transitInFlightWindow = relayRetryTTL + 2*time.Minute

	// maxTransitPerRecipient caps the per-recipient transit backlog
	// (SimpleX uses 128 per queue; bitchat 100 per peer). FIFO:
	// admission of message N+1 evicts that recipient's oldest.
	maxTransitPerRecipient = 128

	// maxTransitBacklogBytes is the global transit payload budget.
	// Admission beyond it evicts the globally oldest transit entries
	// (slice order in s.topics is chronological). 64 MB ≈ the point
	// where a relay should shed load rather than buffer it.
	maxTransitBacklogBytes = 64 << 20
)

// effectiveHopBudget resolves the wire Hops value at admission:
// absent/0 (legacy peer or local send) becomes the full default — the
// "treat it as if we created it" compatibility rule — and anything
// above the default is clamped so a hostile sender cannot buy extra
// propagation radius.
func effectiveHopBudget(wire int) int {
	if wire <= 0 || wire > defaultMessageHopBudget {
		return defaultMessageHopBudget
	}
	return wire
}

// isTransitEnvelope reports whether e is transit traffic on this node.
// EXACTLY the inverse of isLocalMessage's classification: only
// point-to-point DM-class messages addressed between two OTHER parties
// are transit. Global/broadcast topics and recipient="*" envelopes are
// local by contract (isLocalMessage returns true for them — their
// history must survive), so they are never subject to the in-flight
// window or the transit caps.
func (s *Service) isTransitEnvelope(e protocol.Envelope) bool {
	if !protocol.IsDMTopic(e.Topic) {
		return false
	}
	if e.Recipient == "*" || e.Recipient == "" {
		return false
	}
	if s.identity == nil {
		// Minimal fixtures (and a half-constructed Service) have no
		// identity to compare against — classify as local: never
		// expiring something we cannot attribute is the safe side.
		return false
	}
	self := s.identity.Address
	return e.Sender != self && e.Recipient != self
}

// envelopeEmitHops resolves the hop budget an envelope may stamp on
// outbound gossip. Envelopes that went through admission carry the
// decremented budget in Hops AND always have StoredAt set, so
// Hops==0 with a non-zero StoredAt means genuinely exhausted. An
// envelope with BOTH fields zero predates the hop budget entirely —
// legacy queue-state restores and hand-built test fixtures — and the
// wire contract applies: absent means "as if this node originated it",
// full default budget.
func envelopeEmitHops(e protocol.Envelope) int {
	if e.Hops > 0 {
		return e.Hops
	}
	if e.StoredAt.IsZero() {
		return defaultMessageHopBudget
	}
	return 0
}

// transitAdmissionScan is the single O(n) pass over a topic backlog
// performed at admission time (caller holds s.gossipMu writer). It
// answers everything the admission decision needs in one walk:
// exact-duplicate detection (the rotating bloom forgets IDs after
// 5–10 min; the backlog lives for up to transitInFlightWindow, and
// local messages longer — without the
// exact check, late re-injections such as pending-ring drains are
// re-admitted as duplicates), the recipient's transit count, and the
// global transit byte total.
type transitAdmissionScan struct {
	duplicate      bool
	recipientCount int
	totalBytes     int
}

func scanTopicForAdmission(backlog []protocol.Envelope, id protocol.MessageID, recipient string, isTransit func(protocol.Envelope) bool) transitAdmissionScan {
	var out transitAdmissionScan
	for _, e := range backlog {
		if e.ID == id {
			out.duplicate = true
		}
		if !isTransit(e) {
			continue
		}
		out.totalBytes += len(e.Payload)
		if e.Recipient == recipient {
			out.recipientCount++
		}
	}
	return out
}

// evictTransitOverflowLocked enforces the per-recipient FIFO cap and
// the global byte budget on backlog before `incoming` (a transit
// envelope of payloadLen bytes for `recipient`) is appended. It
// returns the filtered backlog and the IDs evicted — the caller must
// drop those IDs from relayRetry AFTER releasing gossipMu (canonical
// deliveryMu → gossipMu order forbids taking deliveryMu here), or the
// retry map keeps orphan entries for envelopes that no longer exist.
//
// Slice order in s.topics is chronological (every removal path
// rebuilds with order preserved), so "first transit match" == oldest.
//
// Caller must hold s.gossipMu writer.
func evictTransitOverflowLocked(backlog []protocol.Envelope, scan transitAdmissionScan, recipient string, payloadLen int, isTransit func(protocol.Envelope) bool) ([]protocol.Envelope, []protocol.MessageID) {
	needRecipientEvictions := 0
	if scan.recipientCount >= maxTransitPerRecipient {
		needRecipientEvictions = scan.recipientCount - maxTransitPerRecipient + 1
	}
	overBytes := scan.totalBytes + payloadLen - maxTransitBacklogBytes

	if needRecipientEvictions == 0 && overBytes <= 0 {
		return backlog, nil
	}

	var evicted []protocol.MessageID
	filtered := backlog[:0]
	for _, e := range backlog {
		if isTransit(e) {
			if needRecipientEvictions > 0 && e.Recipient == recipient {
				needRecipientEvictions--
				overBytes -= len(e.Payload)
				evicted = append(evicted, e.ID)
				continue
			}
			if overBytes > 0 {
				overBytes -= len(e.Payload)
				evicted = append(evicted, e.ID)
				continue
			}
		}
		filtered = append(filtered, e)
	}
	// Release evicted Envelopes left in the shared backing array so
	// their payloads are not pinned until the next append grows it.
	clear(backlog[len(filtered):])
	return filtered, evicted
}

// transitExpired reports whether a transit envelope has outlived its
// in-flight forwarding window. Anchored at StoredAt (local admission
// time); falls back to CreatedAt for legacy fixtures with a zero
// StoredAt.
func transitExpired(e protocol.Envelope, now time.Time) bool {
	anchor := e.StoredAt
	if anchor.IsZero() {
		anchor = e.CreatedAt
	}
	if anchor.IsZero() {
		return true // no usable anchor at all — unbounded otherwise
	}
	return now.Sub(anchor) > transitInFlightWindow
}

// dropRelayRetryEntries removes the relayRetry bookkeeping for
// messages that were evicted/expired out of s.topics. Without this the
// entries are orphaned forever: retryableRelayMessages iterates the
// TOPIC snapshot, so an entry whose envelope is gone is never visited
// and its lazy TTL delete never fires. Takes deliveryMu — caller must
// NOT hold gossipMu (sequential lock use only, per the canonical
// deliveryMu → gossipMu order).
func (s *Service) dropRelayRetryEntries(ids []protocol.MessageID) {
	if len(ids) == 0 {
		return
	}
	s.deliveryMu.Lock()
	for _, id := range ids {
		delete(s.relayRetry, relayMessageKey(id))
	}
	s.deliveryMu.Unlock()
}

// viaIdentityForAddress resolves the authenticated identity of an
// ingress peer address (dialOrigin-canonicalized), for stamping
// Envelope.ViaIdentity on paths that only know the transport address
// (deliverRelayedMessage). Takes peerMu.RLock — caller must NOT hold
// peerMu. Empty result when the identity is not (yet) established.
func (s *Service) viaIdentityForAddress(addr domain.PeerAddress) domain.PeerIdentity {
	if addr == "" {
		return domain.PeerIdentity{}
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.peerIDs[s.resolveHealthAddress(addr)]
}

// isIngressNextHop reports whether the resolved table-directed
// next-hop is the message's ingress peer. The authenticated identity
// is checked FIRST: inbound-only routes are keyed "inbound:<raw
// remote addr>" while Via carries the sanitized overlay address, and
// dialOrigin canonicalization cannot bridge the two forms — identity
// comparison can. The canonical-address comparison remains as the
// fallback for ingress links whose identity was not established at
// admission time.
func (s *Service) isIngressNextHop(msg protocol.Envelope, nextHopIdentity domain.PeerIdentity, address domain.PeerAddress) bool {
	if msg.ViaIdentity != "" && nextHopIdentity.String() == msg.ViaIdentity {
		return true
	}
	return msg.Via != "" && s.sameCanonicalAddress(address, domain.PeerAddress(msg.Via))
}

// relayChainGossipBudget maps a relay_message's chain position to the
// gossip hop budget presented at admission. It takes ONLY the chain
// fields — the top-level `hops` of a relay_message is untrusted input
// and deliberately has NO path into this computation: a hostile peer
// stamping hops:8 on a chain that is already at hop_count≈max_hops
// must not re-arm blind-gossip propagation through the "no capable
// next hop" stored+gossip fallback. Absent/invalid MaxHops normalizes
// to defaultMaxHops exactly like handleRelayMessage's chain
// forwarding; remaining+1 compensates the Via-decrement at admission,
// so the stored budget equals the chain's actual remaining allowance
// (then clamped by effectiveHopBudget).
func relayChainGossipBudget(hopCount, maxHops int) int {
	if maxHops <= 0 {
		maxHops = defaultMaxHops
	}
	if hopCount < 0 {
		hopCount = 0
	}
	remaining := maxHops - hopCount
	if remaining < 0 {
		remaining = 0
	}
	return remaining + 1
}

// wireHops is the on-wire form of envelopeEmitHops: the remaining
// budget with a floor of 1, because 0 is never emitted — an absent
// (omitempty) field means "legacy/origin, assign the full default" at
// the receiver, which would re-arm an exhausted budget. Used ONLY by
// the mesh fan-out builder (gossipPushFrame → push_message).
// messageFrame (fetch_messages / fetch_inbox / subscriber push)
// deliberately does NOT stamp hops — those are final-delivery
// surfaces and the local/query API contract expects the field absent
// (pinned by TestGossipPushFrameCarriesHops).
func wireHops(e protocol.Envelope) int {
	if h := envelopeEmitHops(e); h > 1 {
		return h
	}
	return 1
}

// sameCanonicalAddress reports whether two transport addresses refer
// to the same peer, collapsing fallback-port dial aliases through
// dialOrigin (a session dialed at host:64646 and the canonical
// host:7777 are the same ingress). Takes peerMu.RLock for the
// dialOrigin reads — the resolveHealthAddress contract requires the
// lock, and dialOrigin is mutated during session setup/teardown.
// Caller must NOT hold peerMu (RWMutex is not recursive).
func (s *Service) sameCanonicalAddress(a, b domain.PeerAddress) bool {
	if a == b {
		return true
	}
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.resolveHealthAddress(a) == s.resolveHealthAddress(b)
}

// filterGossipTargetsForEnvelope applies the propagation gates to a
// routing decision's gossip targets: ingress suppression (never echo a
// message straight back into the link it arrived on — compared on
// CANONICAL addresses, so a fallback-port alias of the Via peer is
// suppressed too) and the hop budget (no re-gossip once the
// decremented budget is exhausted). Returns nil when gossip must be
// skipped entirely.
//
// Takes peerMu.RLock for the alias resolution — caller must NOT hold
// peerMu (RWMutex is not recursive). Every current caller
// (storeIncomingMessage routing section, retryRelayDeliveries,
// sendTableDirectedRelay fallbacks) is outside the peer domain at the
// call point: routingTargetsForMessage acquires and RELEASES the lock
// before returning.
func (s *Service) filterGossipTargetsForEnvelope(e protocol.Envelope, targets []domain.PeerAddress) []domain.PeerAddress {
	if envelopeEmitHops(e) < 1 {
		return nil
	}
	if e.Via == "" {
		return targets
	}
	via := domain.PeerAddress(e.Via)
	filtered := targets[:0:0]
	s.peerMu.RLock()
	canonVia := s.resolveHealthAddress(via)
	for _, t := range targets {
		if t == via || s.resolveHealthAddress(t) == canonVia {
			continue
		}
		filtered = append(filtered, t)
	}
	s.peerMu.RUnlock()
	return filtered
}
