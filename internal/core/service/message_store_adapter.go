package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
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

	// removals names the conversations being removed right now. Checked
	// here for the same reason refusals are: this is the only door into the
	// chatlog, and the node writes through it before the router learns
	// anything. A message stored during a removal survives it — the
	// deletion cannot see a row that is not there yet — and rebuilds the
	// conversation on the next start.
	removals *removalGate

	// events is where the UI is told that a message landing made reactions
	// visible.
	//
	// An atomic pointer and not a plain field: the composition root sets it
	// (DesktopClient.RegisterConversationControl) while the node's ingress
	// goroutines read it, and the ordering that puts the write first is a
	// comment in the root rather than something this type can check. Nil until
	// it is set, and a nil one costs only a redraw that waits for the next
	// conversation change.
	events atomic.Pointer[ebus.Bus]
}

// NewMessageStoreAdapter binds a ChatlogGateway to a MessageStore surface.
// The returned adapter is ready to be handed to node.Service.RegisterMessageStore.
func NewMessageStoreAdapter(chatlog *ChatlogGateway, id *identity.Identity, refusals *wipeTombstoneSet, removals *removalGate) *MessageStoreAdapter {
	return &MessageStoreAdapter{chatlog: chatlog, id: id, refusals: refusals, removals: removals}
}

// attachEventBus gives the adapter somewhere to announce that a stored message
// made reactions visible.
//
// Unexported and called from exactly one place — DesktopClient's single
// conversation-control wiring step — so this is not a Set* initialiser callers
// can forget or reorder: the bus is built by the composition root alongside the
// client, while the adapter is built inside it, and one wiring call is the
// smallest thing that can bridge that.
func (a *MessageStoreAdapter) attachEventBus(events *ebus.Bus) {
	if a == nil {
		return
	}
	a.events.Store(events)
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

// admitWrite takes a lease on writing this envelope's conversation, held
// until the row is committed. It reports false when a removal of that
// conversation is running.
//
// A check would not do: between checking and appending, a whole removal can
// start and finish, and the row would land behind both of its history
// deletes — exactly the row the next startup rebuilds the deleted
// conversation from. The lease closes that gap from the other side: the
// removal waits for it.
func (a *MessageStoreAdapter) admitWrite(envelope protocol.Envelope) (func(), bool) {
	if a.removals == nil || envelope.Topic != "dm" || a.id == nil {
		return func() {}, true
	}
	self := a.id.Address
	peer := envelope.Sender
	if peer == self {
		peer = envelope.Recipient
	}
	return a.removals.admitWrite(domain.PeerIdentityFromWire(peer))
}

// sharedDeletePolicy is the deletion policy a direct message is STORED under,
// whatever the sender stamped on it.
//
// The flag decides who may have a message removed from this side, and the
// answer the product gives is "either participant" (any-delete). It has never
// been reachable from the interface, so `sender-delete` and the empty value are
// not choices anybody made — they are what an older build stamped. Migration
// 0007 rewrote the history that carried them; this is the same rule at the
// door, for the messages an un-updated peer is still sending today. Without it
// the old answer keeps arriving, and with it the refusal the user reads as
// "the peer refused to delete the message".
//
// `immutable` is a real decision and survives. `auto-delete-ttl` is an expiry
// contract rather than a deletion policy and is left alone. Anything outside a
// direct conversation has no second participant to grant this to.
func sharedDeletePolicy(topic string, flag protocol.MessageFlag) protocol.MessageFlag {
	if topic != "dm" {
		return flag
	}
	switch flag {
	case protocol.MessageFlagSenderDelete, "":
		return protocol.MessageFlagAnyDelete
	default:
		return flag
	}
}

// StoreMessage persists an inbound or outbound envelope and classifies the
// outcome so the node can decide whether it saw a new message or a
// duplicate. Matches the node.MessageStore contract.
func (a *MessageStoreAdapter) StoreMessage(envelope protocol.Envelope, isOutgoing bool) node.StoreResult {
	if a == nil || a.chatlog == nil {
		return node.StoreFailed
	}
	// The lease is taken before anything else this function does and held
	// until the row is committed, so a removal either sees this write and
	// waits for it, or starts after it and sweeps the row it left.
	releaseWrite, admitted := a.admitWrite(envelope)
	if !admitted {
		// DEFERRED, not stored and not dropped: the sender keeps the
		// message and re-delivers it once the removal is over, at which
		// point it is simply a new message to a conversation that no
		// longer exists — and opens it again, as a message from any
		// stranger would. Storing it now would leave a row the deletion
		// has already looked past.
		// Gated: the line says a conversation of this user's is being erased
		// right now, and it is written on the arrival path of every message
		// that meets a wipe in progress.
		deletionLog().Debug().
			Str("id", logID(string(envelope.ID))).
			Msg("chatlog store deferred a message: its conversation is being removed")
		return node.StoreDeferred
	}
	defer releaseWrite()

	refused, known := a.refusesDeletedID(envelope)
	switch {
	case refused:
		// Already deleted here, and recently enough that copies of it can
		// still be in flight. Reported as a duplicate rather than a
		// failure: it IS one — we had this message and destroyed it — and
		// the node's duplicate path re-sends the delivery receipt, which
		// stops the sender retrying a message we are never going to keep.
		// Gated for the same reason, and it is the sharper of the two: it says
		// that THIS id was deleted here, which is exactly the fact the database
		// no longer keeps.
		deletionLog().Debug().
			Str("id", logID(string(envelope.ID))).
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
			Str("id", logID(string(envelope.ID))).
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
		Flag:           string(sharedDeletePolicy(envelope.Topic, envelope.Flag)),
		DeliveryStatus: status,
		TTLSeconds:     envelope.TTLSeconds,
	}
	if isOutgoing {
		// An outgoing row is born NEVER EMITTED, in the same write that
		// creates it. Nothing has been handed to a writer at this point —
		// the node registers the delivery and only then starts the sends —
		// and the claim is withdrawn (ClearNeverEmitted) before any frame
		// goes out. It is never put back: the bit is monotone, and the
		// question the sender's badge asks is answered by the SEPARATE
		// on_wire stamp instead. See chatlog/emission.go.
		//
		// Writing it here rather than afterwards is what makes the durable
		// state true at every instant: a crash between the row and the
		// first attempt used to leave a message that never left the
		// machine claiming otherwise. It costs no extra write, because it
		// rides the insert the row needs anyway.
		entry.Metadata = chatlog.NeverEmittedMetadata
	}
	inserted, err := a.chatlog.AppendReportNew(a.opContext(), envelope.Topic, domain.PeerIdentityFromWire(a.id.Address), entry)
	if err != nil {
		log.Error().Str("topic", envelope.Topic).Str("id", string(envelope.ID)).Err(err).Msg("chatlog append failed")
		return node.StoreFailed
	}
	// Released on a duplicate too, and that is not belt-and-braces. The message
	// EXISTS in both cases, which is the only thing a held reaction is waiting
	// for; a re-delivery is in fact the common way to reach this line, because
	// the reaction and the message travel by different paths and a peer that
	// resends the message has usually resent the reaction with it. Releasing
	// only on the first insert left those facts pending until the sweep.
	a.releaseReactionsWaitingFor(domain.MessageID(envelope.ID), conversationPeer(a.id.Address, envelope))
	if !inserted {
		return node.StoreDuplicate
	}
	return node.StoreInserted
}

// releaseReactionsWaitingFor makes visible any reaction that arrived before the
// message it is about.
//
// It runs HERE because this is the moment the wait was for, and only on a row
// that was really inserted: a duplicate changes nothing, and a message that was
// refused or deferred is one whose reactions must keep waiting.
//
// A failure is logged and not propagated. The message itself is stored, which
// is the answer this function's caller returns; refusing the message because
// its reactions could not be un-hidden would trade a row the user can read for
// a chip they cannot see.
func (a *MessageStoreAdapter) releaseReactionsWaitingFor(id domain.MessageID, peer domain.PeerIdentity) {
	store := a.chatlog.Store()
	if store == nil || peer.IsZero() {
		// No conversation to release WITHIN. Message ids are chosen by their
		// sender, so releasing by id alone would make a stranger's held facts
		// visible the moment an unrelated message with the same id landed.
		return
	}
	// time.Now directly, and it stays that way: this stamp lands in
	// `updated_at`, which nothing compares — the sweep measures `first_seen_at`,
	// which this statement does not touch. It is a record of when the release
	// happened, not an input to any decision, so it is the infrastructural use
	// CLAUDE.md's rule on time permits. An injected clock here was tried and
	// removed: nothing ever set it.
	released, err := store.ReleaseHeldReactions(
		a.opContext(), domain.ReactionScopeForPeer(peer), id, time.Now().UTC())
	if err != nil {
		log.Warn().Err(err).Str("id", string(id)).Msg("held reactions could not be released")
		return
	}
	if released == 0 {
		return
	}
	log.Debug().Str("id", string(id)).Int("reactions", released).
		Msg("reactions that arrived before their message are now visible")
	// Told, not merely written: the chips are drawn from a per-conversation
	// cache the UI loads once, so a release nobody announces is a reaction the
	// user does not see until they leave the chat and come back — which is the
	// exact scenario holding the fact existed for.
	if events := a.events.Load(); events != nil {
		events.Publish(ebus.TopicReactionsChanged, peer)
	}
}

// conversationPeer is the other party to an envelope, relative to this node.
// Zero when the envelope names neither side as us, which is what a broadcast
// topic looks like — and a conversation nobody can name is one no reader is
// waiting on.
func conversationPeer(self string, envelope protocol.Envelope) domain.PeerIdentity {
	switch self {
	case envelope.Sender:
		return domain.PeerIdentityFromWire(envelope.Recipient)
	case envelope.Recipient:
		return domain.PeerIdentityFromWire(envelope.Sender)
	default:
		return domain.PeerIdentity{}
	}
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
	// The result is the answer, not just the error. No row matched means
	// this receipt is not about a message in that conversation — an id
	// from another thread, or a peer confirming someone else's message —
	// and reporting it as recorded would let the caller stop the retry for
	// a message its real recipient has said nothing about.
	updated, err := a.chatlog.UpdateStatus(a.opContext(), "dm", chatlogPeer, domain.MessageID(receipt.MessageID), receipt.Status)
	if err != nil {
		log.Error().Str("message_id", string(receipt.MessageID)).Str("status", receipt.Status).Err(err).Msg("chatlog update status failed")
		return false
	}
	if !updated {
		// Either the row is already at or past this status — an ordinary
		// duplicate, and nothing is owed — or there is no such row in that
		// conversation at all. The two are told apart by the id: ours or
		// not.
		if !a.knowsOutgoingMessage(receipt) {
			log.Warn().Str("message_id", string(receipt.MessageID)).Str("from", receipt.Sender).
				Msg("receipt ignored: no message with that id in that conversation")
			return false
		}
	}
	return true
}

// knowsOutgoingMessage reports whether the chatlog holds the message this
// receipt names in the conversation it claims to be about.
func (a *MessageStoreAdapter) knowsOutgoingMessage(receipt protocol.DeliveryReceipt) bool {
	if a == nil || a.chatlog == nil {
		return false
	}
	peer := receipt.Sender
	if receipt.Sender == a.id.Address {
		peer = receipt.Recipient
	}
	return a.chatlog.HasEntryInConversation(a.opContext(), peer, string(receipt.MessageID))
}

// SentMessageIDs implements node.DeliveryOutbox: the newest ids of DMs
// this node authored, so the solicited-receipt gate survives a restart.
//
// Bounded by COUNT, not by age. A receipt does not expire — the recipient
// may open a months-old conversation for the first time — so an age cutoff
// here would be a correctness limit dressed up as a read bound, rejecting
// a genuine `seen` as unsolicited. The count is the caller's LRU capacity,
// which is the real limit on what can be remembered anyway.
func (a *MessageStoreAdapter) SentMessageIDs(limit int) ([]protocol.MessageID, error) {
	if a == nil || a.chatlog == nil {
		return nil, nil
	}
	ids, err := a.chatlog.SentMessageIDs(a.opContext(), limit)
	if err != nil {
		return nil, err
	}
	sent := make([]protocol.MessageID, 0, len(ids))
	for _, id := range ids {
		sent = append(sent, protocol.MessageID(id))
	}
	return sent, nil
}

// UndeliveredOutgoing implements node.DeliveryOutbox: it returns the sealed
// envelopes of locally-sent DMs whose delivery status is still "sent", so
// the node can reseed its end-to-end retry scheduler after a restart.
func (a *MessageStoreAdapter) UndeliveredOutgoing() ([]node.OutboxEntry, error) {
	if a == nil || a.chatlog == nil {
		return nil, nil
	}
	entries, err := a.chatlog.UndeliveredOutgoing(a.opContext(), time.Time{}, time.Now().UTC())
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
			// The row carries the claim only while nothing has been
			// handed to a writer; everything else — including every row
			// written before the bit existed — reads as emitted, which
			// is the safe answer for its reader (the deletion).
			Emitted: !chatlog.NeverEmitted(entry.Metadata),
			// The OTHER bit, read separately because its reader is the
			// sender's badge and its safe answer points the other way.
			//
			// awaitingWire, not a plain negation: the answer is a
			// TRI-STATE. A row with the stamp is believed. A row from the
			// PREVIOUS release — never_emitted, no on_wire — was one this
			// node was holding, so it reopens as queued and is re-sent
			// once, which the recipient dedupes silently. A row from
			// before either bit says nothing about the wire and keeps its
			// persisted status, so an upgrade does not re-badge a whole
			// unreceipted history. See chatlog/emission.go.
			OnWire: !awaitingWire(entry.Metadata),
		})
	}
	return rows, nil
}

// MarkOnWire implements node.DeliveryEmissionJournal.
func (a *MessageStoreAdapter) MarkOnWire(ids []protocol.MessageID) error {
	return a.writeEmissionMarks(ids, a.chatlog.MarkOnWire)
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

// Undelivered outgoing DMs are reseeded with NO horizon: the scan reaches
// as far back as the chatlog holds. A message ends when the recipient
// confirms it, when its author withdraws it, or when its own TTL expires,
// and a restart is none of those — so a horizon here would be the node
// giving up on a message silently, just for having been closed and
// reopened. See the age comment in node/delivery_retry.go.
//
// The reason a horizon once existed was the months-long zombie-DM storm:
// a restart used to re-inject ancient undelivered DMs into the mesh by
// blind-gossiping them at a recipient nobody could reach. The reachability
// gate is what actually fixed that — a reseeded message now emits nothing
// at all until its recipient is genuinely reachable, at which point
// sending it is the correct thing to do rather than the storm.
//
// The remaining cost is a bounded one: the scan reads the rows still in
// "sent", which is the set the user themselves can see as undelivered.

// seenReseedHorizon still bounds the OTHER half of the reseed — the
// unconfirmed seen receipts. Those keep their attempt cap (a receipt is
// re-triggered by the far side, so an abandoned one repairs itself on the
// next arrival), and a week comfortably outlives it.
const seenReseedHorizon = 7 * 24 * time.Hour

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
