package node

import (
	"fmt"
	"sort"
	"strings"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/logid"
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
	refs := make([]messageIdentity, 0, len(scope))
	for _, id := range scope {
		refs = append(refs, s.ownMessageIdentity(id, recipient.String()))
	}
	frozen := s.freezeMessageIdentities(refs)
	// A freeze is only ever taken for a deletion, so this line says the same
	// thing the deletion paths must not say: that this user is erasing a
	// conversation with this peer, right now. Behind the same gate as the rest
	// of them, and the recipient is a digest even there.
	logid.DeletionLog().Info().
		Str("recipient", logid.Of(recipient.String())).
		Int("frozen", frozen.Frozen).
		Int("never_emitted", len(frozen.NeverEmitted)).
		Msg("delivery_freeze_taken")
	return frozen, nil
}

// freezeDeliveries is the recipient-less core: a single-message delete
// needs the same guarantee for one id, and derives its peer from the row
// rather than being given one.
func (s *Service) freezeDeliveries(scope []protocol.MessageID) (FrozenDeliveries, error) {
	refs, err := s.resolveOwnMessageIDs(scope)
	if err != nil {
		return FrozenDeliveries{}, err
	}
	return s.freezeMessageIdentities(refs), nil
}

func (s *Service) freezeMessageIdentities(refs []messageIdentity) FrozenDeliveries {
	never := make(map[protocol.MessageID]struct{}, len(refs))
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	if s.frozenDeliveries == nil {
		s.frozenDeliveries = make(map[messageIdentity]struct{}, len(refs))
	}
	for _, ref := range refs {
		s.frozenDeliveries[ref] = struct{}{}
		if entry, exists := s.awaitingDelivered[ref]; exists && !entry.Emitted {
			never[ref.MessageID] = struct{}{}
		}
	}
	return FrozenDeliveries{Frozen: len(refs), NeverEmitted: never}
}

// resolveOwnMessageIDs is a compatibility resolver, not a wildcard. It never
// freezes/thaws a second recipient merely because the wire id was reused.
func (s *Service) resolveOwnMessageIDs(ids []protocol.MessageID) ([]messageIdentity, error) {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	s.gossipMu.RLock()
	defer s.gossipMu.RUnlock()
	refs := make([]messageIdentity, 0, len(ids))
	for _, id := range ids {
		candidates := make(map[messageIdentity]struct{})
		add := func(ref messageIdentity) {
			if ref.MessageID == id && s.identity != nil && ref.Sender == s.identity.Address {
				candidates[ref] = struct{}{}
			}
		}
		for ref := range s.awaitingDelivered {
			add(ref)
		}
		for ref := range s.frozenDeliveries {
			add(ref)
		}
		for ref := range s.markedNeverEmitted {
			add(ref)
		}
		for _, envelope := range s.topics["dm"] {
			add(messageIdentityOf(envelope))
		}
		if len(candidates) > 1 {
			return nil, domain.ErrAmbiguousMessage
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("message identity unavailable: %w", protocol.ErrInvalidCancelDelivery)
		}
		for ref := range candidates {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

// ThawOutgoingDeliveries ends a freeze that its caller did not commit —
// the wipe's transaction failed, so the messages are still the user's and
// must go out as before.
func (s *Service) ThawOutgoingDeliveries(ids []protocol.MessageID) error {
	refs, err := s.resolveOwnMessageIDs(ids)
	if err != nil {
		return err
	}
	s.thawMessageIdentities(refs)
	return nil
}

func (s *Service) ThawOutgoingDeliveriesTo(recipient domain.PeerIdentity, ids []protocol.MessageID) error {
	if recipient.IsZero() {
		return protocol.ErrInvalidCancelDelivery
	}
	refs := make([]messageIdentity, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, s.ownMessageIdentity(id, recipient.String()))
	}
	s.thawMessageIdentities(refs)
	return nil
}

func (s *Service) thawMessageIdentities(refs []messageIdentity) {
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	for _, ref := range refs {
		delete(s.frozenDeliveries, ref)
	}
}

// clearFreezeLocked forgets a freeze because the delivery it held back is
// now withdrawn for good. Caller MUST hold s.deliveryMu.Lock.
func (s *Service) clearFreezeLocked(ids map[messageIdentity]struct{}) {
	for id := range ids {
		delete(s.frozenDeliveries, id)
	}
}

// deliveryFrozenLocked reports whether the id is held back by a freeze.
// Caller MUST hold s.deliveryMu (read or write).
func (s *Service) deliveryFrozenLocked(id messageIdentity) bool {
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
	var err error
	if strings.TrimSpace(frame.Recipient) == "" {
		err = s.ThawOutgoingDeliveries(ids)
	} else {
		err = s.ThawOutgoingDeliveriesTo(domain.PeerIdentityFromWire(strings.TrimSpace(frame.Recipient)), ids)
	}
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidCancelDelivery, Error: err.Error()}
	}
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
	var frozen FrozenDeliveries
	var err error
	if peer := domain.PeerIdentityFromWire(strings.TrimSpace(frame.Recipient)); !peer.IsZero() {
		frozen, err = s.FreezeOutgoingDeliveriesTo(peer, []protocol.MessageID{id})
	} else {
		frozen, err = s.freezeDeliveries([]protocol.MessageID{id})
	}
	if err != nil {
		return protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidCancelDelivery, Error: err.Error()}
	}
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
