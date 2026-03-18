package protocol

import "time"

type MessageID string

type Envelope struct {
	ID        MessageID
	Topic     string
	Sender    string
	Recipient string
	Payload   []byte
	CreatedAt time.Time
}
