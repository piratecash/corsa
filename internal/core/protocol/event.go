package protocol

import "time"

// LocalChangeEvent describes changes in the chatlog. The desktop UI uses this to
// perform surgical cache updates instead of re-fetching and re-decrypting the
// entire conversation.
type LocalChangeEvent struct {
	Type LocalChangeType `json:"type"`

	Topic string `json:"topic,omitempty"`

	MessageID string `json:"message_id,omitempty"`

	Sender string `json:"sender,omitempty"`

	Recipient string `json:"recipient,omitempty"`

	// Encrypted message body (only for new messages).
	// The desktop client decrypts using the sender's box key.
	Body string `json:"body,omitempty"`

	// Only for new messages.
	Flag string `json:"flag,omitempty"`

	// Only for new messages.
	CreatedAt string `json:"created_at,omitempty"`

	// Only for receipt updates.
	Status string `json:"status,omitempty"`

	// Only for receipt updates.
	DeliveredAt time.Time `json:"delivered_at,omitempty"`

	// Only for new messages.
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

type LocalChangeType string

const (
	LocalChangeNewMessage    LocalChangeType = "new_message"
	LocalChangeReceiptUpdate LocalChangeType = "receipt_update"
)
