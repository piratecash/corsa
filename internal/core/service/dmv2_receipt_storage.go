package service

import (
	"errors"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type directDMV2StoreOutcome uint8

const (
	directDMV2Pending directDMV2StoreOutcome = iota
	directDMV2Rejected
	directDMV2Inserted
	directDMV2AlreadyStored
)

// storeDirectDMV2ForReceipt is an inactive storage boundary. The caller owns
// transport capability, proved identity, freshness and envelope size checks.
// The supplied key must come from that authorized direct session's identity.
// Only Inserted and AlreadyStored provide evidence for a delivery receipt.
func (a *MessageStoreAdapter) storeDirectDMV2ForReceipt(envelope protocol.Envelope, network, authorPublicKey string) (directDMV2StoreOutcome, error) {
	if a == nil || a.id == nil || a.chatlog == nil || a.chatlog.Store() == nil {
		return directDMV2Pending, errors.New("direct DM storage unavailable")
	}
	if envelope.Topic != "dm" || envelope.Recipient != a.id.Address || envelope.Sender == a.id.Address {
		return directDMV2Rejected, errors.New("direct DM is outside this recipient path")
	}
	ref := domain.MessageRef{
		Topic: envelope.Topic, ID: string(envelope.ID),
		Sender: envelope.Sender, Recipient: envelope.Recipient,
	}
	binding := directmsg.V2Binding{
		Network: network, ID: ref.ID, Topic: ref.Topic,
		Sender: ref.Sender, Recipient: ref.Recipient, Flag: string(envelope.Flag),
	}
	encoded := string(envelope.Payload)
	opened, err := directmsg.OpenV2(a.id, binding, authorPublicKey, encoded)
	if err != nil {
		return directDMV2Rejected, err
	}
	if !opened.CreatedAt.Equal(envelope.CreatedAt) {
		return directDMV2Rejected, errors.New("direct DM creation time differs from its authenticated value")
	}

	// Reuse the removal gate, anti-resurrection check and commit path. A
	// successful insert here comes from the real adapter, never from the
	// generic node path's default in-memory result.
	envelope.Payload = []byte(encoded)
	switch a.StoreMessage(envelope, false) {
	case node.StoreInserted:
		return directDMV2Inserted, nil
	case node.StoreDuplicate:
		row, found, err := a.chatlog.Store().EntryByRef(a.opContext(), ref)
		if err != nil {
			return directDMV2Pending, err
		}
		// The exact same bytes have already passed OpenV2 above. The row's
		// status may have advanced to seen since the initial insert.
		if found && row.Body == encoded &&
			(row.DeliveryStatus == chatlog.StatusDelivered || row.DeliveryStatus == chatlog.StatusSeen) {
			return directDMV2AlreadyStored, nil
		}
		return directDMV2Pending, errors.New("direct DM duplicate lacks an exact stored envelope")
	default:
		return directDMV2Pending, errors.New("direct DM storage did not confirm an insert or duplicate")
	}
}

// StoreDirectDMV2ForReceipt bridges only the verified storage outcomes to Node.
func (a *MessageStoreAdapter) StoreDirectDMV2ForReceipt(envelope protocol.Envelope, network, authorPublicKey string) (node.DirectDMV2ReceiptOutcome, error) {
	outcome, err := a.storeDirectDMV2ForReceipt(envelope, network, authorPublicKey)
	switch outcome {
	case directDMV2Inserted:
		return node.DirectDMV2ReceiptInserted, err
	case directDMV2AlreadyStored:
		return node.DirectDMV2ReceiptDuplicate, err
	default:
		return node.DirectDMV2ReceiptPending, err
	}
}

// StoredDirectDMV2Outgoing confirms that a duplicate names the original row.
func (a *MessageStoreAdapter) StoredDirectDMV2Outgoing(ref domain.MessageRef, body string) (string, bool, error) {
	if a == nil || a.id == nil || a.id.Address != ref.Sender || a.chatlog == nil || a.chatlog.Store() == nil {
		return "", false, errors.New("direct dm-v2 outbox unavailable")
	}
	row, found, err := a.chatlog.Store().EntryByRef(a.opContext(), ref)
	if err != nil || !found || row.Body != body {
		return "", false, err
	}
	if row.DeliveryStatus == chatlog.StatusSent && awaitingWire(row.Metadata) {
		return protocol.MessageStatusQueued, true, nil
	}
	return "", true, nil
}
