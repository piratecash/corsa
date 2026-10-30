package node

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// messageIdentity is what makes two direct messages the same message.
//
// The outer id alone is not it. The author picks the id, and the signature
// (directmsg.VerifyEnvelope) covers the sender, the recipient and the body but
// not the id, so any peer can put a live message's id on a message of its own.
// Every step that settles one message's delivery state — a receipt that
// confirms it, its author withdrawing it, the sender engine giving up on it,
// the pending ring deduplicating its frames — therefore names the message by
// all three fields. Matching by the id alone let a receipt for this node's own
// message, or a receipt between two other identities, end the onward relay of
// a peer's transit DM that happened to share the id, and let a withdrawal drop
// that DM's queued frames.
//
// Sender and Recipient stay the wire strings rather than domain.PeerIdentity,
// as in protocol.Envelope and receiptIdentity: these are the exact values the
// signature covers and the frames carry, and parsing them would fold every
// malformed address into the zero PeerIdentity — merging messages the strings
// keep apart.
type messageIdentity struct {
	MessageID protocol.MessageID
	Sender    string
	Recipient string
}

// messageIdentityOf is the identity of an envelope, or of the message a frame
// carries once frameEnvelope has read it out.
func messageIdentityOf(envelope protocol.Envelope) messageIdentity {
	return messageIdentity{
		MessageID: envelope.ID,
		Sender:    envelope.Sender,
		Recipient: envelope.Recipient,
	}
}

// madeByMessageRecipient reads the message out of a receipt its recipient
// made and addressed to its author: the two roles swap.
func madeByMessageRecipient(receipt protocol.DeliveryReceipt) messageIdentity {
	return messageIdentity{MessageID: receipt.MessageID, Sender: receipt.Recipient, Recipient: receipt.Sender}
}

// madeByMessageAuthor reads the message out of a receipt its author made and
// addressed to its recipient: the roles stay.
func madeByMessageAuthor(receipt protocol.DeliveryReceipt) messageIdentity {
	return messageIdentity{MessageID: receipt.MessageID, Sender: receipt.Sender, Recipient: receipt.Recipient}
}

// confirmingReceiptReaders maps every receipt status that confirms a message
// to the reader of that message. delivered and seen are the recipient's
// claims. seen_ack is the author acknowledging the recipient's seen
// (sendSeenAck: the author sends it to the seen receipt's author), so it too
// is evidence the recipient holds the message, and it travels the other way.
var confirmingReceiptReaders = map[string]func(protocol.DeliveryReceipt) messageIdentity{
	protocol.ReceiptStatusDelivered: madeByMessageRecipient,
	protocol.ReceiptStatusSeen:      madeByMessageRecipient,
	protocol.ReceiptStatusSeenAck:   madeByMessageAuthor,
}

// confirmedMessageOf returns the message a receipt confirms. The second
// result is false for a status that confirms no message.
func confirmedMessageOf(receipt protocol.DeliveryReceipt) (messageIdentity, bool) {
	read, confirms := confirmingReceiptReaders[receipt.Status]
	if !confirms {
		return messageIdentity{}, false
	}
	return read(receipt), true
}

// ownMessageIdentity names a message this node authored for recipient.
// Withdrawal and outbound-status paths know their message by id and recipient
// only; the author is implied, and spelling it out is what keeps them off a
// peer's message under the same unsigned id.
func (s *Service) ownMessageIdentity(id protocol.MessageID, recipient string) messageIdentity {
	return messageIdentity{MessageID: id, Sender: s.identity.Address, Recipient: recipient}
}

// frameCarriesMessage reports whether a queued frame carries exactly this
// message. The pending-ring clear and its count share it, and the ring's
// dedup key (pendingFrameKey) is built from the same identity, so none of
// them can disagree about which frames belong to the message.
func frameCarriesMessage(frame protocol.Frame, message messageIdentity) bool {
	envelope := frameEnvelope(frame)
	return envelope.Topic == "dm" && messageIdentityOf(envelope) == message
}

// admissionRef retains the topic namespace as well as both verified endpoints.
func admissionRef(msg incomingMessage) domain.MessageRef {
	return domain.MessageRef{Topic: msg.Topic, ID: string(msg.ID), Sender: msg.Sender, Recipient: msg.Recipient}
}

func envelopeRef(envelope protocol.Envelope) domain.MessageRef {
	return domain.MessageRef{Topic: envelope.Topic, ID: string(envelope.ID), Sender: envelope.Sender, Recipient: envelope.Recipient}
}

func (s *Service) ownMessageSet(ids map[protocol.MessageID]struct{}, recipient string) map[messageIdentity]struct{} {
	refs := make(map[messageIdentity]struct{}, len(ids))
	for id := range ids {
		refs[s.ownMessageIdentity(id, recipient)] = struct{}{}
	}
	return refs
}
