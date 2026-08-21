package service

import (
	"context"
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

	// refusals reports whether an id has been deleted recently enough
	// that a re-delivery of it must not be stored again. Taken in the
	// constructor rather than set afterwards: the node holds this
	// adapter from the moment it is registered, so a guard installed
	// later is a contract the type cannot check and a window nothing
	// closes. nil means no deletion subsystem is wired (SDK consumers).
	//
	// The check belongs HERE, at the only door into the chatlog, and not
	// in the router that reads the events afterwards: the node stores
	// first and only then decides whether the message is news. A replay
	// of an envelope still sitting in some relay's buffer is not news —
	// its id is already in the backlog — so the node returns early and
	// the router never sees an event, while the row it just wrote is
	// back in the database. Refusing at the door is the only place that
	// covers every path that stores.
	refusals *wipeTombstoneSet
}

// NewMessageStoreAdapter binds a ChatlogGateway to a MessageStore surface.
// The returned adapter is ready to be handed to node.Service.RegisterMessageStore.
func NewMessageStoreAdapter(chatlog *ChatlogGateway, id *identity.Identity, refusals *wipeTombstoneSet) *MessageStoreAdapter {
	return &MessageStoreAdapter{chatlog: chatlog, id: id, refusals: refusals}
}

// opContext is the context every repository call from this adapter runs with.
//
// This type is the boundary between the node and the chatlog repository, and
// the node.MessageStore / DeliveryOutbox / SeenAckJournal callbacks carry no
// context of their own: they are invoked from connection handlers and retry
// loops that predate this layer. Widening those interfaces is a node-side
// change, deliberately not made here.
//
// Background is safe for the shutdown contract regardless, because the
// composition root joins the node — Service.Run and then WaitBackground —
// BEFORE it closes the database, so these writes are finished rather than
// cancelled. The ordering is what protects them; cancellation would only make
// the same guarantee arrive sooner.
func (a *MessageStoreAdapter) opContext() context.Context {
	return context.Background()
}

// refusesDeletedID asks the refusal gate about the envelope, but only for
// the traffic the gate can possibly have an answer about.
//
// Every refusal names a CHAT ROW: they are written by a message delete, by
// a conversation wipe (both of which select `topic = 'dm'`) and by
// applyInboundDelete for an id we do not have — all DM. For any other
// topic the gate can only answer "not refused" or "cannot tell", and the
// second one is harmful: while the refusals are unreadable it would defer
// everything this node stores rather than the conversations the mechanism
// exists for.
//
// The deferral contract is the other half of the argument. "The sender
// keeps it and tries again" is a property of the DM delivery engine —
// sender-owned retry, awaitingDelivered, an attempt budget. A broadcast
// topic has none of it, so an unacknowledged frame there is not a retry,
// it is a loss: exactly the outcome the deferral exists to prevent.
//
// The topic arrives from the wire, so this is also what keeps a peer from
// making our reception depend on a table that has nothing to do with
// their messages.
//
// Bare "dm", not IsDMTopic: control DMs are never persisted (the node
// skips the store for them entirely), so they own no row a refusal could
// name.
func (a *MessageStoreAdapter) refusesDeletedID(envelope protocol.Envelope) (refused, known bool) {
	if envelope.Topic != "dm" {
		return false, true
	}
	return a.refusals.Refuses(domain.MessageID(envelope.ID), time.Now().UTC())
}

// StoreMessage persists an inbound or outbound envelope and classifies the
// outcome so the node can decide whether it saw a new message or a
// duplicate. Matches the node.MessageStore contract.
func (a *MessageStoreAdapter) StoreMessage(envelope protocol.Envelope, isOutgoing bool) node.StoreResult {
	if a == nil || a.chatlog == nil {
		return node.StoreFailed
	}
	refused, known := a.refusesDeletedID(envelope)
	switch {
	case refused:
		// Already deleted here, and recently enough that copies of it can
		// still be in flight. Reported as a duplicate rather than a
		// failure: it IS one — we had this message and destroyed it — and
		// the node's duplicate path re-sends the delivery receipt, which
		// stops the sender retrying a message we are never going to keep.
		log.Debug().
			Str("id", string(envelope.ID)).
			Str("topic", envelope.Topic).
			Msg("chatlog store refused a re-delivery of a deleted message")
		return node.StoreDuplicate
	case !known:
		// The refusals could not be read, so whether this message was
		// deleted here is unknown — and storing it on a guess is how a
		// row the user destroyed comes back for good, since a later
		// reload of the refusals does not re-delete it.
		//
		// DEFERRED, not failed: the two differ in who keeps the message.
		// A failure leaves it in the node's runtime backlog and still
		// acknowledges it to the sender, so the sender stops retrying a
		// message that is on no disk anywhere — and a restart loses it.
		// Deferred keeps the message with the SENDER and answers when
		// the database can give an answer.
		log.Warn().
			Str("id", string(envelope.ID)).
			Str("topic", envelope.Topic).
			Msg("chatlog store deferred a message: the refusals of deleted ids are unreadable")
		return node.StoreDeferred
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
	inserted, err := a.chatlog.AppendReportNew(a.opContext(), envelope.Topic, domain.PeerIdentityFromWire(a.id.Address), entry)
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
		chatlogPeer = domain.PeerIdentityFromWire(receipt.Recipient)
	} else if receipt.Recipient == a.id.Address {
		chatlogPeer = domain.PeerIdentityFromWire(receipt.Sender)
	}
	if chatlogPeer.IsZero() {
		return true // not our message, nothing to update
	}
	if _, err := a.chatlog.UpdateStatus(a.opContext(), "dm", chatlogPeer, domain.MessageID(receipt.MessageID), receipt.Status); err != nil {
		log.Error().Str("message_id", string(receipt.MessageID)).Str("status", receipt.Status).Err(err).Msg("chatlog update status failed")
		return false
	}
	return true
}

// UndeliveredOutgoing implements node.DeliveryOutbox: it returns the sealed
// envelopes of locally-sent DMs whose delivery status is still "sent", so
// the node can reseed its end-to-end retry scheduler after a restart.
func (a *MessageStoreAdapter) UndeliveredOutgoing() ([]node.OutboxEntry, error) {
	if a == nil || a.chatlog == nil {
		return nil, nil
	}
	entries, err := a.chatlog.UndeliveredOutgoing(a.opContext(), time.Now().UTC().Add(-reseedHorizon))
	if err != nil {
		return nil, err
	}
	rows := make([]node.OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		createdAt, err := time.Parse(time.RFC3339Nano, entry.CreatedAt)
		if err != nil {
			if createdAt, err = time.Parse(time.RFC3339, entry.CreatedAt); err != nil {
				log.Warn().Str("id", entry.ID).Str("created_at", entry.CreatedAt).Msg("undelivered outgoing entry has unparseable created_at — skipped")
				continue
			}
		}
		rows = append(rows, node.OutboxEntry{
			Envelope: protocol.Envelope{
				ID:         protocol.MessageID(entry.ID),
				Topic:      "dm",
				Sender:     entry.Sender,
				Recipient:  entry.Recipient,
				Flag:       protocol.MessageFlag(entry.Flag),
				TTLSeconds: entry.TTLSeconds,
				Payload:    []byte(entry.Body),
				CreatedAt:  createdAt.UTC(),
			},
			// The row carries the mark only when this node withheld the
			// message and never got it out; everything else — including
			// every row written before the mark existed — reads as
			// emitted.
			Emitted: !chatlog.NeverEmitted(entry.Metadata),
		})
	}
	return rows, nil
}

// MarkNeverEmitted implements node.DeliveryEmissionJournal.
func (a *MessageStoreAdapter) MarkNeverEmitted(ids []protocol.MessageID) error {
	return a.writeEmissionMarks(ids, a.chatlog.MarkNeverEmitted)
}

// ClearNeverEmitted implements node.DeliveryEmissionJournal.
func (a *MessageStoreAdapter) ClearNeverEmitted(ids []protocol.MessageID) error {
	return a.writeEmissionMarks(ids, a.chatlog.ClearNeverEmitted)
}

func (a *MessageStoreAdapter) writeEmissionMarks(ids []protocol.MessageID, write func(context.Context, []domain.MessageID) error) error {
	if a == nil || a.chatlog == nil || len(ids) == 0 {
		return nil
	}
	converted := make([]domain.MessageID, 0, len(ids))
	for _, id := range ids {
		converted = append(converted, domain.MessageID(id))
	}
	return write(a.opContext(), converted)
}

// reseedHorizon bounds how far back the startup reseed scans for BOTH the
// undelivered-DM bodies (UndeliveredOutgoing) and the unconfirmed seen
// receipts (UnconfirmedSeen). Without it a restart reseeds the entire history
// and re-injects ancient undelivered DMs into the mesh — the months-long
// zombie-DM storm. A week comfortably covers any realistic retry window: the
// scheduler caps a single message/receipt at ~3.5h of attempts, so anything
// older is already abandoned in practice.
const reseedHorizon = 7 * 24 * time.Hour

// seenReseedHorizon is retained as an alias so existing references keep
// compiling; both halves share the same window.
const seenReseedHorizon = reseedHorizon

// UnconfirmedSeen implements node.SeenAckJournal: the seen receipts this
// identity sent that the original senders have not confirmed with seen_ack.
func (a *MessageStoreAdapter) UnconfirmedSeen() ([]protocol.DeliveryReceipt, error) {
	if a == nil || a.chatlog == nil {
		return nil, nil
	}
	entries, err := a.chatlog.UnconfirmedSeen(a.opContext(), time.Now().UTC().Add(-seenReseedHorizon))
	if err != nil {
		return nil, err
	}
	receipts := make([]protocol.DeliveryReceipt, 0, len(entries))
	for _, entry := range entries {
		receipts = append(receipts, protocol.DeliveryReceipt{
			MessageID:   protocol.MessageID(entry.ID),
			Sender:      a.id.Address,
			Recipient:   entry.Sender,
			Status:      protocol.ReceiptStatusSeen,
			DeliveredAt: time.Now().UTC(),
		})
	}
	return receipts, nil
}

// MarkDeliveryFailed implements node.DeliveryFailureJournal.
func (a *MessageStoreAdapter) MarkDeliveryFailed(id protocol.MessageID) error {
	if a == nil || a.chatlog == nil {
		return nil
	}
	return a.chatlog.MarkDeliveryFailed(a.opContext(), string(id))
}

// MarkSeenConfirmed implements node.SeenAckJournal.
func (a *MessageStoreAdapter) MarkSeenConfirmed(id protocol.MessageID) error {
	if a == nil || a.chatlog == nil {
		return nil
	}
	return a.chatlog.MarkSeenConfirmed(a.opContext(), string(id))
}
