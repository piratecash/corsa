package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

type MessageID string

type MessageFlag string

const (
	MessageFlagImmutable     MessageFlag = "immutable"
	MessageFlagSenderDelete  MessageFlag = "sender-delete"
	MessageFlagAnyDelete     MessageFlag = "any-delete"
	MessageFlagAutoDeleteTTL MessageFlag = "auto-delete-ttl"
	DefaultMessageTimeDrift              = 10 * time.Minute
)

type Envelope struct {
	ID         MessageID
	Topic      string
	Sender     string
	Recipient  string
	Flag       MessageFlag
	TTLSeconds int
	Payload    []byte
	CreatedAt  time.Time
}

type DeliveryReceipt struct {
	MessageID   MessageID
	Sender      string
	Recipient   string
	Status      string
	DeliveredAt time.Time
}

const (
	ReceiptStatusDelivered = "delivered"
	ReceiptStatusSeen      = "seen"
)

func NewMessageID() (MessageID, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("generate message uuid: %w", err)
	}

	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80

	return MessageID(fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hex.EncodeToString(data[0:4]),
		hex.EncodeToString(data[4:6]),
		hex.EncodeToString(data[6:8]),
		hex.EncodeToString(data[8:10]),
		hex.EncodeToString(data[10:16]),
	)), nil
}

func (f MessageFlag) Valid() bool {
	switch f {
	case MessageFlagImmutable, MessageFlagSenderDelete, MessageFlagAnyDelete, MessageFlagAutoDeleteTTL:
		return true
	default:
		return false
	}
}
