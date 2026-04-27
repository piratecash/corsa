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

	// LocalChangeNewControlMessage is emitted by node.Service when a DM
	// arrives on the dedicated control topic (TopicControlDM). It carries
	// the same wire-encrypted Body as LocalChangeNewMessage but is *not*
	// persisted to chatlog and is published on a separate ebus channel
	// (ebus.TopicMessageControl). Subscribers (DMRouter) decrypt it via
	// DMCrypto.DecryptIncomingControlMessage and dispatch on the inner
	// DMCommand value (message_delete, message_delete_ack, ...). See
	// docs/dm-commands.md for the wire and storage contracts.
	LocalChangeNewControlMessage LocalChangeType = "new_control_message"
)

// TopicControlDM is the dedicated wire topic for control DMs
// (message_delete, message_delete_ack). Frames on this topic bypass
// chatlog persistence on both the sender and recipient sides; they
// reach the DM router via LocalChangeNewControlMessage events instead
// of LocalChangeNewMessage. Relays see only the topic name and
// ciphertext length, not the inner command.
const TopicControlDM = "dm-control"

// IsDMTopic reports whether the given wire topic identifies a DM-class
// message — either a regular data DM ("dm") or a control DM
// (TopicControlDM). Routing target selection, push-subscriber gating,
// table-directed relay candidate filtering, and any other code that
// treats DMs as point-to-point (recipient-specific) traffic must use
// this predicate; comparing against the bare "dm" string drops control
// DMs even though they follow the same routing semantics.
//
// Use directly:
//
//	if protocol.IsDMTopic(envelope.Topic) { ... }
//
// instead of
//
//	if envelope.Topic == "dm" { ... }
func IsDMTopic(topic string) bool {
	return topic == "dm" || topic == TopicControlDM
}
