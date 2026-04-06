package domain

import "regexp"

// messageIDPattern matches a strict UUID v4 string:
//   - 8-4-4-4-12 lowercase hex digits separated by dashes
//   - version nibble (position 15) must be '4'
//   - variant nibble (position 20) must be '8', '9', 'a', or 'b'
//
// This matches the output of protocol.NewMessageID() which sets
// data[6] = (data[6] & 0x0f) | 0x40  and  data[8] = (data[8] & 0x3f) | 0x80.
var messageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// MessageIDMaxLen is the fixed length of a valid MessageID (UUID v4 = 36 chars).
const MessageIDMaxLen = 36

// MessageID is a UUID v4 identifying a single message in the system.
// Defined in domain (not protocol) so it can be used by value types
// without pulling in the wire-format package.
type MessageID string

// IsValid returns true if the MessageID matches UUID v4 format.
// Empty string is considered invalid — use IsValidOrEmpty for optional fields.
func (id MessageID) IsValid() bool {
	return len(id) == MessageIDMaxLen && messageIDPattern.MatchString(string(id))
}

// IsValidOrEmpty returns true if the MessageID is either empty or valid UUID v4.
// Use for optional fields like ReplyTo.
func (id MessageID) IsValidOrEmpty() bool {
	return id == "" || id.IsValid()
}

// DMRecipient holds the public parameters needed to encrypt a message
// for a peer. Separating these into a typed struct prevents accidental
// swaps between address and box key at call sites.
type DMRecipient struct {
	Address      PeerIdentity
	BoxKeyBase64 string
}

// OutgoingDM is the plaintext content to encrypt and send as a direct message.
// New fields (reactions, forwarding, etc.) are added here without changing
// function signatures across the call chain.
//
// For file_announce DMs: Command is set to FileActionAnnounce and
// CommandData holds the JSON-encoded FileAnnouncePayload. Body contains
// either a user-provided caption or FileDMBodySentinel.
type OutgoingDM struct {
	Body        string
	ReplyTo     MessageID
	Command     FileAction // e.g. FileActionAnnounce for file_announce DMs; empty for regular DMs
	CommandData string     // JSON-encoded payload (e.g. FileAnnouncePayload); empty for regular DMs
}
