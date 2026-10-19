package node

import (
	"context"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

type messageOrigin uint8

const (
	messageOriginUntrusted messageOrigin = iota
	messageOriginOwnGlobal
)

// PublishGlobalMessage acknowledges the local store, never a remote replica.
// Only this typed local entry can grant client own-global origin authority.
func (s *Service) PublishGlobalMessage(ctx context.Context, command domain.OutgoingGlobalMessage) (domain.GlobalPublication, error) {
	if err := ctx.Err(); err != nil {
		return domain.GlobalPublication{}, err
	}
	body, err := domain.CanonicalGlobalMessageBody(string(command.Body))
	if err != nil {
		return domain.GlobalPublication{}, err
	}
	if len(body) > maxPeerCommandBodyBytes {
		return domain.GlobalPublication{}, domain.ErrInvalidGlobalMessage
	}
	if s.identity == nil || s.messageStore == nil {
		return domain.GlobalPublication{}, domain.ErrGlobalPublisherUnavailable
	}
	id, err := protocol.NewMessageID()
	if err != nil {
		return domain.GlobalPublication{}, err
	}
	at := s.deliveryClock().UTC()
	msg := incomingMessage{origin: messageOriginOwnGlobal, ID: id, Topic: string(domain.GlobalMessageTopic), Sender: s.identity.Address, Recipient: "*", Flag: protocol.MessageFlagImmutable, Body: string(body), CreatedAt: at}
	// Validate the same transport size before a row can be committed. Normal
	// gossip uses push_message; escaping can be larger than the plain body.
	env := protocol.Envelope{ID: id, Topic: msg.Topic, Sender: msg.Sender, Recipient: msg.Recipient, Flag: msg.Flag, Payload: []byte(body), CreatedAt: at}
	if _, err := protocol.MarshalFrameLine(gossipPushFrame(env)); err != nil {
		return domain.GlobalPublication{}, domain.ErrInvalidGlobalMessage
	}
	stored, _, code := s.storeIncomingMessage(msg, true)
	if code != "" {
		return domain.GlobalPublication{}, protocol.ErrorFromCode(code)
	}
	if !stored {
		return domain.GlobalPublication{}, domain.ErrGlobalPublisherUnavailable
	}
	return domain.GlobalPublication{Outcome: domain.GlobalStoredLocally, Message: domain.GlobalMessage{ID: domain.MessageID(id), Topic: domain.GlobalMessageTopic, ClaimedSender: domain.PeerIdentityFromWire(s.identity.Address), Body: body, CreatedAt: at}}, nil
}
