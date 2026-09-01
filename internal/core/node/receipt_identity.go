package node

import (
	"github.com/piratecash/corsa/internal/core/protocol"
)

// receiptIdentity is what makes two delivery receipts the same receipt.
//
// It exists because that answer used to be given separately at every site
// that needed it, and the sites disagreed. The dedup set keyed on
// recipient+id+status, the relay-retry map on the same three with a
// different separator, the backlog filter compared those three fields by
// hand, and the ack_delete frame carried only two of them. Each spelling
// was correct in isolation and wrong together: a receipt is a CLAIM by
// the peer that made it, so two claims about one message from two peers
// are two facts, and every place that treated them as one gave an
// attacker a way to have someone else's receipt discarded — by occupying
// the dedup key, by sharing a retry entry, or by having an ack for the
// forgery delete the genuine receipt out of the backlog.
//
// So identity is a type, built in exactly one of the constructors below,
// and every map key, filter and wire field is derived from it.
// TestReceiptIdentityIsBuiltInOnePlace fails the build for a literal
// assembled anywhere else — that is what stops the next reader of this
// code from inventing an eighth spelling with one field missing.
type receiptIdentity struct {
	// Recipient is who the receipt is FOR: the original message's
	// sender, waiting to hear that it arrived.
	Recipient string
	MessageID protocol.MessageID
	Status    string
	// Sender is who CLAIMS it — the peer that made the receipt. Absent
	// only on an ack from a peer too old to carry it; see senderKnown.
	Sender string
}

// identityOf is the identity of a receipt this node holds.
func identityOf(receipt protocol.DeliveryReceipt) receiptIdentity {
	return receiptIdentity{
		Recipient: receipt.Recipient,
		MessageID: receipt.MessageID,
		Status:    receipt.Status,
		Sender:    receipt.Sender,
	}
}

// identityFromRelayFrame is the identity a relay_delivery_receipt frame
// describes. Frame.Address is the receipt's author — receiptFromFrame
// reads the same field into DeliveryReceipt.Sender, and requires it
// non-empty, so a frame that parses always yields a complete identity.
func identityFromRelayFrame(frame protocol.Frame) receiptIdentity {
	return receiptIdentity{
		Recipient: frame.Recipient,
		MessageID: protocol.MessageID(frame.ID),
		Status:    frame.Status,
		Sender:    frame.Address,
	}
}

// identityFromAck is the identity an ack_delete frame points at. The
// acking peer is the receipt's RECIPIENT — it is their backlog entry —
// and frame.Address is verified against the authenticated session before
// this is called. ReceiptSender is empty when the acking peer predates
// ProtocolVersionReceiptSenderAck.
func identityFromAck(frame protocol.Frame) receiptIdentity {
	return receiptIdentity{
		Recipient: frame.Address,
		MessageID: protocol.MessageID(frame.ID),
		Status:    frame.Status,
		Sender:    frame.ReceiptSender,
	}
}

// senderKnown reports whether the identity names its author. False only
// for an ack from a peer that cannot carry the field, which is why the
// backlog filter has an explicit rule for that case rather than a
// silently wider match.
func (id receiptIdentity) senderKnown() bool { return id.Sender != "" }

// dedupKey is the key of the "already handled" set (seenReceipts).
func (id receiptIdentity) dedupKey() string {
	return id.Recipient + ":" + string(id.MessageID) + ":" + id.Status + ":" + id.Sender
}

// retryKey is the key of the relay-retry map. The "receipt|" prefix keeps
// it disjoint from relayMessageKey in the map they share.
func (id receiptIdentity) retryKey() string {
	return "receipt|" + id.Recipient + "|" + string(id.MessageID) + "|" + id.Status + "|" + id.Sender
}

// matches reports whether the receipt IS the one this identity names. An
// identity with no sender matches on the other three fields, so callers
// must decide what an ambiguous match means before acting on it — see
// deleteBacklogReceiptForRecipient.
func (id receiptIdentity) matches(receipt protocol.DeliveryReceipt) bool {
	if receipt.Recipient != id.Recipient || receipt.MessageID != id.MessageID || receipt.Status != id.Status {
		return false
	}
	return !id.senderKnown() || receipt.Sender == id.Sender
}

// receiptKeyOf is the dedup key of a receipt this node holds.
func receiptKeyOf(receipt protocol.DeliveryReceipt) string {
	return identityOf(receipt).dedupKey()
}

// relayReceiptKey is the relay-retry key of a receipt this node holds.
func relayReceiptKey(receipt protocol.DeliveryReceipt) string {
	return identityOf(receipt).retryKey()
}

// ackDelete is one ack_delete instruction: "I hold this, you may drop
// your copy."
//
// It is a type for the same reason receiptIdentity is. The three senders
// used to take (ackType, id, status) as loose arguments, and a receipt
// ack built that way cannot name which receipt it means — there is no
// field for the author. Now a receipt ack can only be built FROM a
// receipt, so the sender cannot be forgotten at a call site.
type ackDelete struct {
	// Type is the wire AckType: "dm" or "receipt".
	Type      string
	MessageID protocol.MessageID
	// Status and ReceiptSender are empty for a "dm" ack — a message has
	// neither.
	Status        string
	ReceiptSender string
}

func ackDeleteForMessage(id protocol.MessageID) ackDelete {
	return ackDelete{Type: "dm", MessageID: id}
}

func ackDeleteForReceipt(receipt protocol.DeliveryReceipt) ackDelete {
	return ackDelete{
		Type:          "receipt",
		MessageID:     receipt.MessageID,
		Status:        receipt.Status,
		ReceiptSender: receipt.Sender,
	}
}

// ackDeleteFromFrame recovers the instruction a queued ack_delete frame
// carries, so it can be re-stamped for the session it is finally written
// to. A frame that sat in the pending queue across a reconnect was
// signed for a peer that no longer exists.
func ackDeleteFromFrame(frame protocol.Frame) ackDelete {
	return ackDelete{
		Type:          frame.AckType,
		MessageID:     protocol.MessageID(frame.ID),
		Status:        frame.Status,
		ReceiptSender: frame.ReceiptSender,
	}
}
