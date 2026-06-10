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

	// The fields below are NODE-LOCAL bookkeeping — Envelope is an
	// in-memory struct (no json tags); the wire shape is MessageFrame.
	// They exist for the transit-retention and hop-budget layers.

	// Hops is the remaining propagation budget THIS node may stamp on
	// outbound gossip for the message (already decremented at
	// admission for transit; full default budget for locally
	// originated messages). Hops < 1 means the message must not be
	// re-gossiped — store/deliver only.
	Hops int

	// Via is the transport address of the peer the message arrived
	// from (ingress link). Gossip fan-out excludes it so a message is
	// never echoed straight back where it came from. Empty for
	// locally originated messages.
	Via string

	// ViaIdentity is the authenticated identity (Ed25519 fingerprint)
	// of the ingress peer, when known. Address comparison alone cannot
	// suppress an inbound-only next-hop: the routing table keys those
	// sessions as "inbound:<raw remote addr>" while Via carries the
	// sanitized overlay address, and dialOrigin canonicalization does
	// not bridge the two forms. Identity comparison does. Empty for
	// local sends and for ingress links whose identity is not (yet)
	// established.
	ViaIdentity string

	// StoredAt is the LOCAL admission time, used by the transit
	// in-flight sweep (transitInFlightWindow anchors here, NOT at the
	// sender-controlled CreatedAt — a hostile sender could otherwise
	// post-date messages to dodge eviction). Zero only in legacy
	// test fixtures; the sweep falls back to CreatedAt then.
	StoredAt time.Time
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
