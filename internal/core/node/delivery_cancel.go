package node

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Sender-side delivery cancellation.
//
// Relays are forwarding-only and nothing in the mesh stores a user message
// for an offline recipient (transit_retention.go), so a DM that has not been
// acknowledged is held by exactly one party: the sender. This file is how the
// sender gives it up — every place that could still put the envelope on the
// wire is emptied in one cross-domain section, so no later flush, gossip
// fan-out or retry tick can resurrect a message the user withdrew.
//
// The application half of the contract is docs/dm-commands.md §"Withdrawing
// an unsent message": the chatlog row is deleted only AFTER this call
// succeeds, so a failure here leaves both sides intact instead of deleting a
// row whose delivery keeps running.
// ---------------------------------------------------------------------------

// CancelOutgoingDeliveryResult reports what the cancellation actually
// found. Every field is informational — an already-abandoned message
// (retry budget spent, queues swept) cancels to an all-zero result and
// that is a legitimate success, not a failure. Callers log it rather
// than branch on it.
type CancelOutgoingDeliveryResult struct {
	// BacklogRemoved is true when the envelope was still in the
	// store-and-forward backlog (s.topics) and has been dropped.
	BacklogRemoved bool

	// RetryCancelled is true when the sender-owned end-to-end retry
	// entry (awaitingDelivered) was still scheduled and is now gone.
	RetryCancelled bool

	// OutboundCleared is true when the per-message delivery bookkeeping
	// entry (s.outbound, the queued/retrying/failed status the UI
	// renders) has been dropped.
	OutboundCleared bool

	// PendingFrames is how many queued per-peer frames carrying the
	// message were dropped from the pending queues.
	PendingFrames int

	// NeverEmitted reports that the message provably never went out: the
	// sender-owned retry entry was still there and no attempt of it had
	// ever reached the wire. It is a positive claim, not the absence of
	// evidence — a message whose entry is already gone (delivered,
	// abandoned, evicted) answers false, because nothing here can rule
	// out that the recipient has it.
	//
	// The application uses it to skip asking a peer to delete something
	// they never received, which would tell them a message existed.
	NeverEmitted bool
}

// Total is the number of delivery hooks the cancellation removed. Used
// as the wire-visible count in the command reply.
func (r CancelOutgoingDeliveryResult) Total() int {
	total := r.PendingFrames
	for _, removed := range []bool{r.BacklogRemoved, r.RetryCancelled, r.OutboundCleared} {
		if removed {
			total++
		}
	}
	return total
}

// CancelOutgoingDelivery withdraws a DM this node originated: the envelope
// leaves the store-and-forward backlog and every queue that could still emit
// it — pending peer frames, the sender-owned end-to-end retry, the relay
// retry shadow and the outbound status entry.
//
// What it guarantees is "no further attempt", not "the peer never saw it". A
// dispatch already in flight when the call lands is on the wire and cannot be
// recalled, and a peer that received the message earlier keeps its copy. The
// caller owns that risk, which is why the application only withdraws a
// message the recipient has never confirmed (see domain.MessageDeleteRoute).
//
// The message id stays in the gossip dedup filter, so an echo of the
// withdrawn envelope is dropped for as long as that window lasts (5-10 min,
// well past the transit retention that bounds who else can still hold it).
//
// Refuses a message this node did not author: the id is only a UUID, and
// purging a transit envelope on behalf of some other sender is not a
// cancellation, it is a hole in the relay contract.
//
// Locking: peerMu(R) → deliveryMu → gossipMu → statusMu, canonical order
// (docs/locking.md). One section, because the sets must not diverge
// mid-cancellation: a backlog envelope dropped while a retry tick still holds
// its awaitingDelivered entry would re-enter the wire on the next tick.
// Event publication happens after every mutex is released.
func (s *Service) CancelOutgoingDelivery(messageID protocol.MessageID, recipient domain.PeerIdentity) (CancelOutgoingDeliveryResult, error) {
	var result CancelOutgoingDeliveryResult

	if strings.TrimSpace(string(messageID)) == "" {
		return result, fmt.Errorf("cancel outgoing delivery: %w: message id is required", protocol.ErrInvalidCancelDelivery)
	}
	if recipient.IsZero() {
		return result, fmt.Errorf("cancel outgoing delivery: %w: recipient is required", protocol.ErrInvalidCancelDelivery)
	}
	wireRecipient := recipient.String()

	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_wait").Str("msg_id", string(messageID)).Msg("peer_mu_reader")
	s.peerMu.RLock()
	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_held").Str("msg_id", string(messageID)).Msg("peer_mu_reader")
	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_wait").Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_held").Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.gossipMu.Lock()

	// Authorship gate first, on a read-only pass: the filter below shares
	// its backing array with the slice it walks, so a mid-filter bail-out
	// would leave the backlog truncated.
	if foreign := s.backlogEnvelopeIsForeign(messageID, wireRecipient); foreign {
		s.gossipMu.Unlock()
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_released_foreign").Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
		s.peerMu.RUnlock()
		log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_released_foreign").Str("msg_id", string(messageID)).Msg("peer_mu_reader")
		log.Warn().Str("message_id", string(messageID)).Str("recipient", wireRecipient).Msg("cancel_outgoing_delivery_rejected_foreign_envelope")
		return result, fmt.Errorf("cancel outgoing delivery %s: %w: message was not originated by this node", messageID, protocol.ErrInvalidCancelDelivery)
	}

	result.BacklogRemoved = s.dropBacklogEnvelopeLocked(messageID, wireRecipient)
	delete(s.relayRetry, relayMessageKey(messageID))

	if entry, awaiting := s.awaitingDelivered[messageID]; awaiting && entry.Envelope.Recipient == wireRecipient {
		result.NeverEmitted = !entry.Emitted
		delete(s.awaitingDelivered, messageID)
		result.RetryCancelled = true
	}
	// Shadow the id for a while: a backlog push that snapshotted the
	// inbox a moment ago still holds this envelope and would hand it over
	// after we have just told the caller it never went out.
	s.noteDeliveryCancelledLocked(messageID, time.Now().UTC())
	// A single-message delete freezes the id before it classifies, for
	// the same reason a wipe does. The withdrawal is what that freeze was
	// waiting for.
	delete(s.frozenDeliveries, messageID)
	if _, tracked := s.outbound[string(messageID)]; tracked {
		delete(s.outbound, string(messageID))
		result.OutboundCleared = true
	}

	result.PendingFrames = s.countPendingFramesLocked(messageID)
	affected := s.clearPendingMessageLocked(messageID)

	s.statusMu.Lock()
	if len(affected) > 0 {
		s.refreshAggregatePendingLocked()
	}
	aggregate := s.aggregateStatus
	s.statusMu.Unlock()

	s.gossipMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_released").Str("msg_id", string(messageID)).Msg("delivery_mu_writer")
	s.peerMu.RUnlock()
	log.Trace().Str("site", "CancelOutgoingDelivery").Str("phase", "lock_released").Str("msg_id", string(messageID)).Msg("peer_mu_reader")

	for _, delta := range affected {
		s.emitPeerPendingChanged(delta.Address, delta.Count)
	}
	if len(affected) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggregate)
	}

	log.Info().
		Str("message_id", string(messageID)).
		Str("recipient", wireRecipient).
		Bool("backlog_removed", result.BacklogRemoved).
		Bool("retry_cancelled", result.RetryCancelled).
		Bool("outbound_cleared", result.OutboundCleared).
		Bool("never_emitted", result.NeverEmitted).
		Int("pending_frames", result.PendingFrames).
		Msg("cancel_outgoing_delivery")

	return result, nil
}

// CancelConversationResult reports what a whole-conversation cancellation
// withdrew.
//
// NeverEmitted names the ids the node can PROVE never reached the wire —
// their sender-owned retry entry was still present and no attempt of it
// had ever gone out. The wipe uses it the way a single withdrawal uses the
// same claim: a message nobody has ever seen is not requested from the
// peer, because the request would be how they learn it existed.
type CancelConversationResult struct {
	Cancelled    int
	NeverEmitted map[protocol.MessageID]struct{}
}

// CancelOutgoingDeliveriesTo withdraws every DM this node originated for
// the recipient in one pass: the whole-conversation counterpart of
// CancelOutgoingDelivery, used when the user wipes a thread rather than a
// message.
//
// One pass, not a loop of single cancellations: each of those walks the
// backlog and takes the full lock stack, so a thread of a thousand
// messages would be a thousand scans of the same slice under a writer
// mutex. Reports how many messages it withdrew.
//
// `scope`, when non-nil, restricts the pass to the ids the caller erased.
// A wipe passes the rows it took: immutable rows survive it, and
// cancelling their delivery would strand a message the user can still see
// in "sending" with nothing left to send it.
//
// Same guarantee and the same limit as the single-message form: no
// further attempt for anything it found, and no claim about what the
// recipient already holds.
func (s *Service) CancelOutgoingDeliveriesTo(recipient domain.PeerIdentity, scope map[protocol.MessageID]struct{}) (CancelConversationResult, error) {
	var result CancelConversationResult
	if recipient.IsZero() {
		return result, fmt.Errorf("cancel outgoing deliveries: %w: recipient is required", protocol.ErrInvalidCancelDelivery)
	}
	wireRecipient := recipient.String()

	// A nil scope means "everything this node owes the recipient". A
	// non-nil one names exactly what the caller erased — which is not the
	// same set: a wipe leaves immutable rows standing, and cancelling
	// their delivery would leave a message the user can still see stuck
	// in "sending" forever.
	inScope := func(id protocol.MessageID) bool {
		if scope == nil {
			return true
		}
		_, ok := scope[id]
		return ok
	}

	log.Trace().Str("site", "CancelOutgoingDeliveriesTo").Str("phase", "lock_wait").Str("recipient", wireRecipient).Msg("peer_mu_reader")
	s.peerMu.RLock()
	s.deliveryMu.Lock()
	s.gossipMu.Lock()

	// Backlog first: it is the set that decides which ids this node still
	// owns for the recipient, and every other structure is keyed by those
	// ids. Foreign envelopes (transit traffic we merely forward) are left
	// alone — a wipe of OUR conversation is not a licence to purge other
	// people's messages that happen to be addressed to the same peer.
	withdrawn := make(map[protocol.MessageID]struct{})
	messages := s.topics["dm"]
	filtered := messages[:0]
	for _, envelope := range messages {
		if envelope.Recipient == wireRecipient && envelope.Sender == s.identity.Address && inScope(envelope.ID) {
			withdrawn[envelope.ID] = struct{}{}
			continue
		}
		filtered = append(filtered, envelope)
	}
	clear(messages[len(filtered):])
	if len(filtered) == 0 {
		delete(s.topics, "dm")
	} else {
		s.topics["dm"] = filtered
	}

	neverEmitted := make(map[protocol.MessageID]struct{})
	for id, entry := range s.awaitingDelivered {
		if entry.Envelope.Recipient == wireRecipient && entry.Envelope.Sender == s.identity.Address && inScope(id) {
			withdrawn[id] = struct{}{}
			if !entry.Emitted {
				// Same claim the single-message withdrawal makes, and
				// for the same reason: nobody has ever seen this
				// message, so asking the peer to delete it would be
				// how they learn it existed.
				neverEmitted[id] = struct{}{}
			}
			delete(s.awaitingDelivered, id)
		}
	}
	for id := range s.outbound {
		if s.outbound[id].Recipient == wireRecipient && inScope(protocol.MessageID(id)) {
			withdrawn[protocol.MessageID(id)] = struct{}{}
			delete(s.outbound, id)
		}
	}
	// Same shadow as the single-message withdrawal: a backlog push that
	// snapshotted this recipient's inbox a moment ago must not hand any
	// of it over now.
	cancelledAt := time.Now().UTC()
	for id := range withdrawn {
		s.noteDeliveryCancelledLocked(id, cancelledAt)
	}
	// The withdrawal is what a freeze was waiting for: these ids are gone
	// for good now, and the shadow above is what keeps a backlog push
	// built a moment ago from handing any of them over.
	//
	// Released by SCOPE, not by what was withdrawn. The freeze was taken
	// over the caller's whole scope, and most of a long thread has no
	// delivery state at all — old messages, confirmed long ago — so
	// releasing only what this pass found would leave a key per such
	// message frozen for the life of the process, with nothing left that
	// could ever release it.
	if scope != nil {
		s.clearFreezeLocked(scope)
	} else {
		s.clearFreezeLocked(withdrawn)
	}

	var affected []ebus.PeerPendingDelta
	for id := range withdrawn {
		delete(s.relayRetry, relayMessageKey(id))
		affected = mergePendingDeltas(affected, s.clearPendingMessageLocked(id))
	}

	s.statusMu.Lock()
	if len(affected) > 0 {
		s.refreshAggregatePendingLocked()
	}
	aggregate := s.aggregateStatus
	s.statusMu.Unlock()

	s.gossipMu.Unlock()
	s.deliveryMu.Unlock()
	s.peerMu.RUnlock()
	log.Trace().Str("site", "CancelOutgoingDeliveriesTo").Str("phase", "lock_released").Str("recipient", wireRecipient).Msg("peer_mu_reader")

	for _, delta := range affected {
		s.emitPeerPendingChanged(delta.Address, delta.Count)
	}
	if len(affected) > 0 {
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggregate)
	}

	log.Info().
		Str("recipient", wireRecipient).
		Int("messages", len(withdrawn)).
		Msg("cancel_outgoing_deliveries_to_recipient")

	result.Cancelled = len(withdrawn)
	result.NeverEmitted = neverEmitted
	return result, nil
}

// backlogEnvelopeIsForeign reports whether the backlog holds an envelope
// for (messageID, recipient) that some OTHER identity originated — a
// transit message we are merely forwarding. An absent envelope is not
// foreign: the message may legitimately have aged out of the backlog
// while its retry entry is still scheduled.
// Caller MUST hold s.gossipMu (reads s.topics).
func (s *Service) backlogEnvelopeIsForeign(messageID protocol.MessageID, recipient string) bool {
	for _, envelope := range s.topics["dm"] {
		if envelope.ID == messageID && envelope.Recipient == recipient {
			return envelope.Sender != s.identity.Address
		}
	}
	return false
}

// dropBacklogEnvelopeLocked removes our envelope for (messageID,
// recipient) from the store-and-forward backlog and reports whether it
// was there. Mirrors deleteBacklogMessageForRecipient's slice hygiene —
// the dropped payload is cleared out of the shared backing array so it
// is not pinned until some future append overwrites the tail.
// Caller MUST hold s.gossipMu.Lock (mutates s.topics).
func (s *Service) dropBacklogEnvelopeLocked(messageID protocol.MessageID, recipient string) bool {
	messages := s.topics["dm"]
	filtered := messages[:0]
	for _, envelope := range messages {
		if envelope.ID == messageID && envelope.Recipient == recipient {
			continue
		}
		filtered = append(filtered, envelope)
	}
	if len(filtered) == len(messages) {
		return false
	}
	clear(messages[len(filtered):])
	if len(filtered) == 0 {
		delete(s.topics, "dm")
	} else {
		s.topics["dm"] = filtered
	}
	return true
}

// countPendingFramesLocked counts the queued frames carrying the message
// across every peer queue. clearPendingMessageLocked reports the queue
// sizes it leaves behind, not what it removed, so the count is taken
// before it runs.
// Caller MUST hold s.deliveryMu (reads s.pending).
func (s *Service) countPendingFramesLocked(messageID protocol.MessageID) int {
	count := 0
	for _, items := range s.pending {
		for _, item := range items {
			if item.Frame.ID != string(messageID) {
				continue
			}
			if item.Frame.Type == "send_message" || item.Frame.Type == "relay_message" {
				count++
			}
		}
	}
	return count
}

// cancelMessageDeliveryFrame handles the local "cancel_message_delivery"
// command: the application asks this node to stop delivering one of its
// own DMs before it deletes the matching chatlog row. Local dispatch
// only — the command withdraws OUR message, so there is no remote
// caller it could ever be right for.
//
// The reply carries the number of delivery hooks removed so the caller
// can log what it actually cancelled (zero is a success — an
// already-abandoned message has nothing left to cancel), and, in Status,
// whether the message provably never went out.
func (s *Service) cancelMessageDeliveryFrame(frame protocol.Frame) protocol.Frame {
	messageID := protocol.MessageID(strings.TrimSpace(frame.ID))
	recipient := domain.PeerIdentityFromWire(strings.TrimSpace(frame.Recipient))

	result, err := s.CancelOutgoingDelivery(messageID, recipient)
	if err != nil {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidCancelDelivery,
			Error: err.Error(),
		}
	}
	return protocol.Frame{
		Type:      "delivery_cancelled",
		Topic:     "dm",
		ID:        string(messageID),
		Recipient: recipient.String(),
		Count:     result.Total(),
		Status:    cancelStatus(result),
	}
}

// cancelConversationDeliveryFrame handles the local
// "cancel_conversation_delivery" command: the application is wiping a
// thread and wants everything it still owes that peer stopped first.
func (s *Service) cancelConversationDeliveryFrame(frame protocol.Frame) protocol.Frame {
	recipient := domain.PeerIdentityFromWire(strings.TrimSpace(frame.Recipient))

	// The frame carries the ids the caller erased, when it has them; an
	// empty list means "everything this node owes the recipient".
	var scope map[protocol.MessageID]struct{}
	if len(frame.IDs) > 0 {
		scope = make(map[protocol.MessageID]struct{}, len(frame.IDs))
		for _, id := range frame.IDs {
			scope[protocol.MessageID(id)] = struct{}{}
		}
	}

	withdrawn, err := s.CancelOutgoingDeliveriesTo(recipient, scope)
	if err != nil {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidCancelDelivery,
			Error: err.Error(),
		}
	}
	// The frame carries the count and the ids that never went out, so a
	// caller on the other side of the RPC boundary can make the same
	// "do not tell them it existed" decision the in-process caller does.
	never := make([]string, 0, len(withdrawn.NeverEmitted))
	for id := range withdrawn.NeverEmitted {
		never = append(never, string(id))
	}
	sort.Strings(never)
	return protocol.Frame{
		Type:      "delivery_cancelled",
		Topic:     "dm",
		Recipient: recipient.String(),
		Count:     withdrawn.Cancelled,
		Status:    CancelStatusUnknown,
		IDs:       never,
	}
}

// Delivery-cancellation reply statuses.
const (
	// CancelStatusNeverEmitted means the envelope provably never reached
	// the wire, so the recipient cannot have it.
	CancelStatusNeverEmitted = "never_emitted"
	// CancelStatusUnknown means the cancellation stopped what it could
	// find, but whether the recipient already has the message is not
	// knowable from here.
	CancelStatusUnknown = "unknown"
)

func cancelStatus(result CancelOutgoingDeliveryResult) string {
	if result.NeverEmitted {
		return CancelStatusNeverEmitted
	}
	return CancelStatusUnknown
}
