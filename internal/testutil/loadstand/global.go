package loadstand

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

type GlobalReplicaProvenance string

const UnsignedGlobalRuntimeStore GlobalReplicaProvenance = "unsigned_claim_runtime_store"

var ErrGlobalReplicaAbsent = errors.New("global replica not observed")

type GlobalPublisher interface {
	PublishGlobal(context.Context, MessageBody) (domain.GlobalPublication, error)
}
type GlobalObserver interface {
	GlobalReplica(context.Context, domain.MessageID) (domain.GlobalMessage, error)
}

func (s *ProcessSource) PublishGlobal(ctx context.Context, body MessageBody) (domain.GlobalPublication, error) {
	var raw json.RawMessage
	if err := s.client.Call(ctx, "publishGlobal", map[string]string{"body": string(body)}, &raw); err != nil {
		return domain.GlobalPublication{}, err
	}
	var publication domain.GlobalPublication
	if err := decodeDTO(raw, &publication); err != nil {
		return publication, err
	}
	if publication.Outcome != domain.GlobalStoredLocally || publication.Message.Topic != domain.GlobalMessageTopic || !publication.Message.ID.IsValid() || publication.Message.ClaimedSender.IsZero() || publication.Message.CreatedAt.IsZero() {
		return domain.GlobalPublication{}, ErrInvalidSample
	}
	canonical, err := domain.CanonicalGlobalMessageBody(string(body))
	if err != nil || canonical != publication.Message.Body {
		return domain.GlobalPublication{}, ErrInvalidSample
	}
	return publication, nil
}
func (s *ProcessSource) GlobalReplica(ctx context.Context, id domain.MessageID) (domain.GlobalMessage, error) {
	var raw json.RawMessage
	if err := s.client.Call(ctx, "fetchMessage", map[string]string{"topic": "global", "id": string(id)}, &raw); err != nil {
		return domain.GlobalMessage{}, err
	}
	return DecodeGlobalReplica(raw, id)
}

// DecodeGlobalReplica parses the real read-only fetchMessage schema, including
// known-missing. It proves a runtime replica, not durable SQL or sender signature.
func DecodeGlobalReplica(raw json.RawMessage, id domain.MessageID) (domain.GlobalMessage, error) {
	if !id.IsValid() {
		return domain.GlobalMessage{}, ErrInvalidConfiguration
	}
	fields, err := object(raw, "type")
	if err != nil {
		return domain.GlobalMessage{}, err
	}
	var kind string
	if err := json.Unmarshal(fields["type"], &kind); err != nil {
		return domain.GlobalMessage{}, ErrInvalidSample
	}
	if kind == "error" {
		fields, err := object(raw, "type", "code")
		if err != nil {
			return domain.GlobalMessage{}, err
		}
		var code string
		if err := json.Unmarshal(fields["code"], &code); err != nil {
			return domain.GlobalMessage{}, ErrInvalidSample
		}
		if code == "unknown-message-id" {
			return domain.GlobalMessage{}, ErrGlobalReplicaAbsent
		}
		return domain.GlobalMessage{}, ErrFamilyUnavailable
	}
	fields, err = object(raw, "type", "topic", "id", "item")
	if err != nil {
		return domain.GlobalMessage{}, err
	}
	itemFields, err := object(fields["item"], "id", "sender", "recipient", "body", "flag", "created_at")
	if err != nil {
		return domain.GlobalMessage{}, err
	}
	if _, present := itemFields["ttl_seconds"]; present {
		if _, err := object(fields["item"], "ttl_seconds"); err != nil {
			return domain.GlobalMessage{}, err
		}
	}
	var wire struct {
		Type  string              `json:"type"`
		Topic domain.MessageTopic `json:"topic"`
		ID    domain.MessageID    `json:"id"`
		Item  *struct {
			ID         domain.MessageID         `json:"id"`
			Sender     domain.PeerIdentity      `json:"sender"`
			Recipient  string                   `json:"recipient"`
			Body       domain.GlobalMessageBody `json:"body"`
			Flag       string                   `json:"flag"`
			CreatedAt  time.Time                `json:"created_at"`
			TTLSeconds int                      `json:"ttl_seconds,omitempty"`
		} `json:"item"`
	}
	if err := decodeDTO(raw, &wire); err != nil {
		return domain.GlobalMessage{}, err
	}
	if wire.Item == nil || kind != "message" || wire.Topic != domain.GlobalMessageTopic || wire.ID != id || wire.Item.ID != id || wire.Item.Sender.IsZero() || wire.Item.Recipient != "*" || wire.Item.Flag != "immutable" || wire.Item.TTLSeconds != 0 || wire.Item.CreatedAt.IsZero() {
		return domain.GlobalMessage{}, ErrInvalidSample
	}
	canonical, err := domain.CanonicalGlobalMessageBody(string(wire.Item.Body))
	if err != nil || canonical != wire.Item.Body {
		return domain.GlobalMessage{}, ErrInvalidSample
	}
	return domain.GlobalMessage{ID: id, Topic: wire.Topic, ClaimedSender: wire.Item.Sender, Body: wire.Item.Body, CreatedAt: wire.Item.CreatedAt}, nil
}
