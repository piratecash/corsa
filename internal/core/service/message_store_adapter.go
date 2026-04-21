package service

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// MessageStoreAdapter satisfies node.MessageStore by forwarding writes to
// ChatlogGateway. It was split out of DesktopClient so the node→desktop
// persistence contract has a dedicated, single-purpose type.
//
// The adapter owns no state of its own; it composes an existing gateway
// with the local identity (needed for sender/recipient disambiguation in
// delivery-receipt updates).
type MessageStoreAdapter struct {
	chatlog *ChatlogGateway
	id      *identity.Identity
}

// NewMessageStoreAdapter binds a ChatlogGateway to a MessageStore surface.
// The returned adapter is ready to be handed to node.Service.RegisterMessageStore.
func NewMessageStoreAdapter(chatlog *ChatlogGateway, id *identity.Identity) *MessageStoreAdapter {
	return &MessageStoreAdapter{chatlog: chatlog, id: id}
}

// StoreMessage persists an inbound or outbound envelope and classifies the
// outcome so the node can decide whether it saw a new message or a
// duplicate. Matches the node.MessageStore contract.
func (a *MessageStoreAdapter) StoreMessage(envelope protocol.Envelope, isOutgoing bool) node.StoreResult {
	if a == nil || a.chatlog == nil {
		return node.StoreFailed
	}
	status := chatlog.StatusDelivered
	if isOutgoing {
		status = chatlog.StatusSent
	}
	entry := chatlog.Entry{
		ID:             string(envelope.ID),
		Sender:         envelope.Sender,
		Recipient:      envelope.Recipient,
		Body:           string(envelope.Payload),
		CreatedAt:      envelope.CreatedAt.Format(time.RFC3339Nano),
		Flag:           string(envelope.Flag),
		DeliveryStatus: status,
		TTLSeconds:     envelope.TTLSeconds,
	}
	inserted, err := a.chatlog.AppendReportNew(envelope.Topic, domain.PeerIdentity(a.id.Address), entry)
	if err != nil {
		log.Error().Str("topic", envelope.Topic).Str("id", string(envelope.ID)).Err(err).Msg("chatlog append failed")
		return node.StoreFailed
	}
	if !inserted {
		return node.StoreDuplicate
	}
	return node.StoreInserted
}

// UpdateDeliveryStatus applies a delivery receipt to the persisted record.
// The receipt sender is the message recipient (confirming delivery/seen),
// and the receipt recipient is the message sender (being notified). The
// chatlog peer is the other party relative to the local identity.
func (a *MessageStoreAdapter) UpdateDeliveryStatus(receipt protocol.DeliveryReceipt) bool {
	if a == nil || a.chatlog == nil {
		return false
	}
	var chatlogPeer domain.PeerIdentity
	if receipt.Sender == a.id.Address {
		chatlogPeer = domain.PeerIdentity(receipt.Recipient)
	} else if receipt.Recipient == a.id.Address {
		chatlogPeer = domain.PeerIdentity(receipt.Sender)
	}
	if chatlogPeer == "" {
		return true // not our message, nothing to update
	}
	if _, err := a.chatlog.UpdateStatus("dm", chatlogPeer, domain.MessageID(receipt.MessageID), receipt.Status); err != nil {
		log.Error().Str("message_id", string(receipt.MessageID)).Str("status", receipt.Status).Err(err).Msg("chatlog update status failed")
		return false
	}
	return true
}
