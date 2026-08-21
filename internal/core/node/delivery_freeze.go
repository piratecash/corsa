package node

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// delivery_freeze.go is the first half of a two-phase conversation wipe.
//
// A wipe has to decide, per message, whether the peer can possibly hold it
// — and then delete the row, which destroys the evidence. Doing the two in
// either order leaves a window:
//
//   - classify, then delete: the message can go out in between, so a row
//     the transaction read as "never emitted" is already at the peer by
//     the time it is deleted, and no request is ever written for it;
//   - delete, then classify: the row is gone, so the only witness left is
//     the node's memory, which does not survive a restart and is emptied
//     when a retry runs out of attempts.
//
// Cancelling first would close the window but cannot be undone: if the
// transaction then fails, the deliveries are gone for good and the user is
// left with messages that are still on screen and will never be sent.
//
// A FREEZE is the reversible form. It stops every path that could put the
// named messages on the wire, without discarding anything, and reports
// what the node knows about them at that instant. The wipe then classifies
// against a state that cannot move under it, and either commits — in which
// case the cancellation withdraws the deliveries for real — or fails, in
// which case a thaw puts everything back.

// FrozenDeliveries is what the node knew at the moment it stopped sending.
type FrozenDeliveries struct {
	// NeverEmitted names the frozen ids that provably never reached the
	// wire. Authoritative for as long as the freeze holds: nothing can
	// emit them while frozen, so the answer cannot go stale between here
	// and the caller's transaction.
	NeverEmitted map[protocol.MessageID]struct{}
	// Frozen counts the ids now held back.
	Frozen int
}

// FreezeOutgoingDeliveriesTo stops dispatch of the named messages to the
// recipient without withdrawing them, and reports which of them never
// reached the wire. Every freeze MUST be ended by either
// CancelOutgoingDeliveriesTo (commit) or ThawOutgoingDeliveries (abort) —
// a frozen id is never sent and never expires on its own, deliberately:
// an expiry would be a deletion guarantee with a timeout attached.
func (s *Service) FreezeOutgoingDeliveriesTo(recipient domain.PeerIdentity, scope []protocol.MessageID) (FrozenDeliveries, error) {
	if recipient.IsZero() {
		return FrozenDeliveries{}, fmt.Errorf("freeze outgoing deliveries: %w: recipient is required", protocol.ErrInvalidCancelDelivery)
	}
	frozen := s.freezeDeliveries(scope)
	log.Info().
		Str("recipient", recipient.String()).
		Int("frozen", frozen.Frozen).
		Int("never_emitted", len(frozen.NeverEmitted)).
		Msg("delivery_freeze_taken")
	return frozen, nil
}

// freezeDeliveries is the recipient-less core: a single-message delete
// needs the same guarantee for one id, and derives its peer from the row
// rather than being given one.
func (s *Service) freezeDeliveries(scope []protocol.MessageID) FrozenDeliveries {
	var frozen FrozenDeliveries
	if len(scope) == 0 {
		return frozen
	}

	neverEmitted := make(map[protocol.MessageID]struct{}, len(scope))
	s.deliveryMu.Lock()
	if s.frozenDeliveries == nil {
		s.frozenDeliveries = make(map[protocol.MessageID]struct{}, len(scope))
	}
	for _, id := range scope {
		s.frozenDeliveries[id] = struct{}{}
		// An id with no entry left says nothing: it may have been
		// delivered, or dropped after its retries ran out. Only a live
		// entry that has never emitted is proof, and the caller ORs this
		// with the durable mark on the row, which covers the rest.
		if entry, awaiting := s.awaitingDelivered[id]; awaiting && !entry.Emitted {
			neverEmitted[id] = struct{}{}
		}
	}
	frozen.Frozen = len(scope)
	s.deliveryMu.Unlock()

	frozen.NeverEmitted = neverEmitted
	return frozen
}

// ThawOutgoingDeliveries ends a freeze that its caller did not commit —
// the wipe's transaction failed, so the messages are still the user's and
// must go out as before.
func (s *Service) ThawOutgoingDeliveries(ids []protocol.MessageID) {
	if len(ids) == 0 {
		return
	}
	s.deliveryMu.Lock()
	for _, id := range ids {
		delete(s.frozenDeliveries, id)
	}
	s.deliveryMu.Unlock()
	log.Info().Int("thawed", len(ids)).Msg("delivery_freeze_released")
}

// clearFreezeLocked forgets a freeze because the delivery it held back is
// now withdrawn for good. Caller MUST hold s.deliveryMu.Lock.
func (s *Service) clearFreezeLocked(ids map[protocol.MessageID]struct{}) {
	for id := range ids {
		delete(s.frozenDeliveries, id)
	}
}

// deliveryFrozenLocked reports whether the id is held back by a freeze.
// Caller MUST hold s.deliveryMu (read or write).
func (s *Service) deliveryFrozenLocked(id protocol.MessageID) bool {
	_, frozen := s.frozenDeliveries[id]
	return frozen
}

// freezeConversationDeliveryFrame handles the local
// "freeze_conversation_delivery" command: the application is about to wipe
// a thread and needs the node to stop sending while it decides.
func (s *Service) freezeConversationDeliveryFrame(frame protocol.Frame) protocol.Frame {
	recipient := domain.PeerIdentityFromWire(strings.TrimSpace(frame.Recipient))
	scope := make([]protocol.MessageID, 0, len(frame.IDs))
	for _, id := range frame.IDs {
		scope = append(scope, protocol.MessageID(id))
	}

	frozen, err := s.FreezeOutgoingDeliveriesTo(recipient, scope)
	if err != nil {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidCancelDelivery,
			Error: err.Error(),
		}
	}
	never := make([]string, 0, len(frozen.NeverEmitted))
	for id := range frozen.NeverEmitted {
		never = append(never, string(id))
	}
	sort.Strings(never)
	return protocol.Frame{
		Type:      "delivery_frozen",
		Topic:     "dm",
		Recipient: recipient.String(),
		Count:     frozen.Frozen,
		IDs:       never,
	}
}

// thawConversationDeliveryFrame handles the local
// "thaw_conversation_delivery" command: the wipe did not commit, so the
// messages are the user's again.
func (s *Service) thawConversationDeliveryFrame(frame protocol.Frame) protocol.Frame {
	ids := make([]protocol.MessageID, 0, len(frame.IDs))
	for _, id := range frame.IDs {
		ids = append(ids, protocol.MessageID(id))
	}
	s.ThawOutgoingDeliveries(ids)
	return protocol.Frame{
		Type:      "delivery_thawed",
		Topic:     "dm",
		Recipient: strings.TrimSpace(frame.Recipient),
		Count:     len(ids),
	}
}

// freezeMessageDeliveryFrame handles the local "freeze_message_delivery"
// command: a single-message delete is about to classify from the row, and
// the row's answer means nothing while the message can still go out.
func (s *Service) freezeMessageDeliveryFrame(frame protocol.Frame) protocol.Frame {
	id := protocol.MessageID(strings.TrimSpace(frame.ID))
	if id == "" {
		return protocol.Frame{
			Type:  "error",
			Code:  protocol.ErrCodeInvalidCancelDelivery,
			Error: "freeze message delivery: message id is required",
		}
	}
	frozen := s.freezeDeliveries([]protocol.MessageID{id})
	_, never := frozen.NeverEmitted[id]
	status := CancelStatusUnknown
	if never {
		status = CancelStatusNeverEmitted
	}
	return protocol.Frame{
		Type:   "delivery_frozen",
		Topic:  "dm",
		ID:     string(id),
		Count:  frozen.Frozen,
		Status: status,
	}
}
