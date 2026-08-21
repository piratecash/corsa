package domain

import (
	"encoding/json"
	"errors"
)

// DMCommand identifies a control or data action attached to a direct
// message. It is carried inside the encrypted DM envelope (so transit
// nodes never see it) and dispatched by the recipient's DM router after
// decryption.
//
// DMCommand is the discriminator stored in OutgoingDM.Command and
// service.DirectMessage.Command. It is deliberately separate from
// FileAction: FileAction now narrowly identifies file-transfer protocol
// frames (chunk_request, chunk_response, file_downloaded,
// file_downloaded_ack) carried inside FileCommandFrame — those never
// travel as DMs.
//
// The wire form (directmsg.PlainMessage.Command) remains string-typed —
// strings on the wire, typed at the domain boundary.
//
// Two classes of DM command exist:
//
//   - Data DM commands (currently DMCommandFileAnnounce) are persisted
//     to chatlog and surface in the chat thread.
//   - Control DM commands (message_delete / message_delete_ack and
//     decrypt_failed) are *not*
//     persisted to chatlog and never surface in the chat thread. They
//     travel on a dedicated wire topic (see docs/dm-commands.md). Use
//     IsControl to test for this class without string comparisons.
type DMCommand string

const (
	// DMCommandFileAnnounce is the data DM that announces an outbound
	// file transfer. The receiver records it in chatlog as a regular
	// DM and additionally registers a file-transfer mapping via
	// FileTransferBridge.
	DMCommandFileAnnounce DMCommand = "file_announce"

	// DMCommandMessageDelete is the control DM that asks the recipient
	// to remove a previously delivered data DM from their chatlog and
	// any state attached to it (receiver-side file-transfer mappings,
	// downloaded blobs, etc.). The request is honoured only if the
	// target message's MessageFlag permits the deletion for the
	// requesting peer; otherwise the receiver replies with status
	// MessageDeleteStatusDenied or MessageDeleteStatusImmutable and
	// leaves the chatlog entry intact.
	DMCommandMessageDelete DMCommand = "message_delete"

	// DMCommandMessageDeleteAck is the control DM the recipient sends
	// back to confirm how a message_delete was resolved. Carries
	// MessageDeleteAckPayload. Also a control DM and not stored.
	DMCommandMessageDeleteAck DMCommand = "message_delete_ack"

	// DMCommandDecryptFailed is the control DM a recipient sends after a
	// CONFIRMED crypto-fail (docs/protocol/identity-lookup.md §4.10): the
	// envelope authenticated — signature and both addresses verified — but
	// neither sealed part opened with the recipient's current keys, i.e.
	// the sender encrypted to a box key the recipient no longer holds.
	// Carries DecryptFailedPayload. Sent only AFTER a fresh
	// target_proof lookup of the sender, so the notice itself is encrypted
	// to keys that are provably current. Control DM, never stored.
	DMCommandDecryptFailed DMCommand = "decrypt_failed"
)

// Valid reports whether the command is a recognised DM command. An
// empty DMCommand is also valid — it represents a regular DM with no
// command attached.
func (c DMCommand) Valid() bool {
	switch c {
	case "",
		DMCommandFileAnnounce,
		DMCommandMessageDelete,
		DMCommandMessageDeleteAck,
		DMCommandDecryptFailed:
		return true
	default:
		return false
	}
}

// IsControl reports whether the command identifies a control DM
// (message_delete, message_delete_ack, decrypt_failed). Send and
// receive code paths branch on this predicate, not on string
// comparisons. An empty DMCommand is
// *not* a control DM — it represents a plain text DM.
func (c DMCommand) IsControl() bool {
	switch c {
	case DMCommandMessageDelete,
		DMCommandMessageDeleteAck,
		DMCommandDecryptFailed:
		return true
	default:
		return false
	}
}

// DecryptFailedPayload is the body of a DMCommandDecryptFailed notice: the
// id of the message the recipient could not open. The re-send recipient is
// NEVER taken from here — the sender re-reads it from its own chatlog row,
// or a crafted notice could redirect somebody else's plaintext.
type DecryptFailedPayload struct {
	MessageID MessageID `json:"message_id"`
}

// Valid enforces the payload contract.
func (p DecryptFailedPayload) Valid() bool { return p.MessageID.IsValid() }

// MarshalDecryptFailedPayload renders the payload for CommandData.
func MarshalDecryptFailedPayload(p DecryptFailedPayload) (string, error) {
	if !p.Valid() {
		return "", errors.New("decrypt_failed payload: invalid message id")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// MessageDeletePayload is the JSON-encoded body of OutgoingDM.CommandData
// for a DMCommandMessageDelete. It carries only the ID of the message
// the sender wants the recipient to remove.
type MessageDeletePayload struct {
	TargetID MessageID `json:"target_id"`
}

// Valid reports whether the payload references a syntactically valid
// MessageID. A zero TargetID is rejected — message_delete with no
// target has no meaning.
func (p MessageDeletePayload) Valid() bool {
	return p.TargetID.IsValid()
}

// MessageDeleteStatus is the outcome the recipient reports back to the
// sender of a message_delete. It is exhaustively defined: every
// authenticated message_delete resolves to exactly one of these values.
// All but MessageDeleteStatusError are terminal — see IsTransient.
type MessageDeleteStatus string

const (
	// MessageDeleteStatusDeleted reports that the target row was
	// present and is now gone. Cleanup hooks (file transfer, etc.)
	// have run.
	MessageDeleteStatusDeleted MessageDeleteStatus = "deleted"

	// MessageDeleteStatusNotFound reports that no row for the target
	// ID exists on the recipient's side (already deleted, never
	// received, wrong ID). Idempotent success — the sender should
	// stop retrying.
	MessageDeleteStatusNotFound MessageDeleteStatus = "not_found"

	// MessageDeleteStatusDenied reports that the target message's
	// MessageFlag did not authorize this peer to delete it. The
	// chatlog row remains. Terminal failure — the sender should stop
	// retrying and surface the outcome to the UI.
	MessageDeleteStatusDenied MessageDeleteStatus = "denied"

	// MessageDeleteStatusImmutable reports that the target message
	// carries the immutable flag and may never be deleted on the
	// wire. Terminal failure.
	MessageDeleteStatusImmutable MessageDeleteStatus = "immutable"

	// MessageDeleteStatusError reports that the recipient could not
	// even decide the request: its chatlog was unavailable or the
	// lookup/delete failed. NOT terminal — the row may well still be
	// there, and answering "not_found" would retire the sender's
	// intent over a transient database fault, leaving the message
	// alive on one side with nobody left to ask. The sender keeps the
	// intent and re-issues on the normal backoff.
	MessageDeleteStatusError MessageDeleteStatus = "error"
)

// Valid reports whether the status string is one of the recognised
// outcomes. An empty status is rejected.
func (s MessageDeleteStatus) Valid() bool {
	switch s {
	case MessageDeleteStatusDeleted,
		MessageDeleteStatusNotFound,
		MessageDeleteStatusDenied,
		MessageDeleteStatusImmutable,
		MessageDeleteStatusError:
		return true
	default:
		return false
	}
}

// IsTerminalSuccess reports whether the sender should treat this
// status as a successful delivery from the protocol's perspective —
// i.e. the recipient ended up consistent with our local state. Both
// "deleted" (target was present and removed) and "not_found" (target
// was never there or already gone) qualify.
func (s MessageDeleteStatus) IsTerminalSuccess() bool {
	return s == MessageDeleteStatusDeleted || s == MessageDeleteStatusNotFound
}

// IsTransient reports whether the recipient failed to decide rather
// than decided against us. Such an answer settles nothing, so the
// sender keeps its intent and asks again instead of reporting the
// deletion finished.
func (s MessageDeleteStatus) IsTransient() bool {
	return s == MessageDeleteStatusError
}

// MessageDeleteAckPayload is the JSON-encoded body of
// OutgoingDM.CommandData for a DMCommandMessageDeleteAck. It echoes
// the target ID so the sender can match the ack to a pending entry,
// and carries the terminal status.
type MessageDeleteAckPayload struct {
	TargetID MessageID           `json:"target_id"`
	Status   MessageDeleteStatus `json:"status"`
}

// Valid reports whether the ack payload is well-formed: the target ID
// is a syntactically valid MessageID and the status is one of the
// recognised values.
func (p MessageDeleteAckPayload) Valid() bool {
	return p.TargetID.IsValid() && p.Status.Valid()
}

// MarshalMessageDeletePayload encodes a MessageDeletePayload as JSON
// suitable for OutgoingDM.CommandData. The string return mirrors the
// CommandData type expected by the DM pipeline.
func MarshalMessageDeletePayload(p MessageDeletePayload) (string, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// MarshalMessageDeleteAckPayload encodes a MessageDeleteAckPayload as
// JSON suitable for OutgoingDM.CommandData.
func MarshalMessageDeleteAckPayload(p MessageDeleteAckPayload) (string, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ConversationDeleteRequestID identifies one "delete chat for everyone"
// gesture on THIS node. It never leaves the process: the wipe puts no
// bulk command on the wire, so there is nothing to bind an ack to. What
// it does bind is the two halves of the local operation —
// BeginConversationDelete latches the outgoing barrier and returns the
// id, CompleteConversationDelete presents it to claim the same latch —
// so a goroutine that arrives late cannot finish a wipe that a newer
// click has already replaced.
//
// Format mirrors MessageID (UUID v4) so the same generator can be used
// (protocol.NewMessageID() returns a UUID v4 string), but the type is
// distinct because the role is different: a MessageID identifies a
// stored message, this identifies one press of a button. Mixing them
// would let either be passed where the other is expected, defeating the
// domain typing rule.
type ConversationDeleteRequestID string
