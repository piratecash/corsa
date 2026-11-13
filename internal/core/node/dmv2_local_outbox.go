package node

import (
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type directDMV2OutgoingStore interface {
	MessageStore
	StoredDirectDMV2Outgoing(domain.MessageRef, string) (string, bool, error)
}

func (s *Service) storeLocalDirectDMV2(msg incomingMessage) protocol.Frame {
	reject := protocol.Frame{Type: "error", Code: protocol.ErrCodeInvalidSendMessage}
	failed := protocol.Frame{Type: "error", Code: protocol.ErrCodeStoreFailed}
	store, ok := s.messageStore.(directDMV2OutgoingStore)
	if !s.directDMV2ListenerTest || !ok || s.identity == nil || msg.Topic != "dm" ||
		msg.Sender != s.identity.Address || msg.Recipient == msg.Sender ||
		len(msg.Body) > maxPeerCommandBodyBytes || !domain.MessageID(msg.ID).IsValid() {
		return reject
	}
	recipient, err := domain.ParsePeerIdentity(msg.Recipient)
	if err != nil || recipient.IsZero() {
		return reject
	}
	binding := directmsg.V2Binding{
		Network: networkName, Topic: msg.Topic, ID: string(msg.ID), Flag: string(msg.Flag),
		Sender: msg.Sender, Recipient: msg.Recipient,
	}
	key := identity.PublicKeyBase64(s.identity.PublicKey)
	meta, err := directmsg.VerifyV2(binding, key, msg.Body)
	now := time.Now().UTC()
	if err != nil || !msg.CreatedAt.Equal(time.Unix(meta.CreatedAt, 0)) ||
		msg.CreatedAt.After(now.Add(s.effectiveClockDrift())) || meta.ExpiresAt <= now.Unix() ||
		meta.ExpiresAt-meta.CreatedAt != int64(msg.TTLSeconds) {
		return reject
	}
	envelope := protocol.Envelope{
		ID: msg.ID, Topic: msg.Topic, Sender: msg.Sender, Recipient: msg.Recipient,
		Flag: msg.Flag, CreatedAt: msg.CreatedAt, TTLSeconds: msg.TTLSeconds, Payload: []byte(msg.Body),
	}
	ref := envelopeRef(envelope)
	switch store.StoreMessage(envelope, true) {
	case StoreDuplicate:
		status, exact, err := store.StoredDirectDMV2Outgoing(ref, msg.Body)
		if err != nil || !exact {
			return failed
		}
		return protocol.Frame{Type: "message_known", Topic: msg.Topic, ID: ref.ID, Status: status}
	case StoreInserted:
		s.deliveryMu.Lock()
		s.registerAwaitingDeliveredLocked(envelope, now, true)
		s.deliveryMu.Unlock()
	default:
		return failed
	}
	s.emitLocalChange(protocol.LocalChangeEvent{
		Type: protocol.LocalChangeNewMessage, Topic: msg.Topic, MessageID: ref.ID,
		Sender: ref.Sender, Recipient: ref.Recipient, Body: msg.Body, Flag: string(msg.Flag),
		CreatedAt: msg.CreatedAt.Format(time.RFC3339), TTLSeconds: msg.TTLSeconds, Status: protocol.MessageStatusQueued,
	})
	return protocol.Frame{Type: "message_stored", Topic: msg.Topic, ID: ref.ID, Status: protocol.MessageStatusQueued}
}
