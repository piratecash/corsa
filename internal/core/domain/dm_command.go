package domain

import "encoding/json"

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
//     conversation_delete / conversation_delete_ack) are *not*
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

	// DMCommandConversationDelete is the control DM that asks the
	// recipient to wipe the entire conversation with the requester.
	// It is the bulk counterpart of DMCommandMessageDelete: instead
	// of targeting a single message id, the recipient walks every
	// chatlog row of the conversation with the envelope sender and
	// deletes every NON-IMMUTABLE row regardless of authorship.
	//
	// Authorization deliberately diverges from message_delete: the
	// per-row authorizedToDelete matrix is NOT consulted. Reusing it
	// would refuse rows the requester did not author, so under the
	// default sender-delete flag every regular DM keeps half the
	// thread alive on each side after a "wipe everything" gesture —
	// directly contradicting the user-visible promise. The bulk
	// gesture is treated as mutual consent to forget the thread,
	// authorised by an explicit two-click UI confirmation; immutable
	// rows are the only carve-out and stay on both sides because
	// that flag is a hard "permanent record" promise that bulk
	// consent cannot override.
	//
	// The conversation peer is derived from the envelope sender after
	// signature verification, not from any field inside the payload.
	// Spoofing the target conversation is therefore not possible
	// without forging the wire signature.
	DMCommandConversationDelete DMCommand = "conversation_delete"

	// DMCommandConversationDeleteAck is the control DM the recipient
	// sends back to confirm a conversation_delete. Carries
	// ConversationDeleteAckPayload (status + number of rows actually
	// removed on the recipient side). Also a control DM and not
	// stored.
	DMCommandConversationDeleteAck DMCommand = "conversation_delete_ack"
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
		DMCommandConversationDelete,
		DMCommandConversationDeleteAck:
		return true
	default:
		return false
	}
}

// IsControl reports whether the command identifies a control DM
// (message_delete, message_delete_ack, conversation_delete,
// conversation_delete_ack). Send and receive code paths branch on
// this predicate, not on string comparisons. An empty DMCommand is
// *not* a control DM — it represents a plain text DM.
func (c DMCommand) IsControl() bool {
	switch c {
	case DMCommandMessageDelete,
		DMCommandMessageDeleteAck,
		DMCommandConversationDelete,
		DMCommandConversationDeleteAck:
		return true
	default:
		return false
	}
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

// MessageDeleteStatus is the terminal outcome the recipient reports
// back to the sender of a message_delete. It is exhaustively defined:
// every authenticated message_delete resolves to exactly one of these
// values.
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
)

// Valid reports whether the status string is one of the four
// recognised terminal outcomes. An empty status is rejected.
func (s MessageDeleteStatus) Valid() bool {
	switch s {
	case MessageDeleteStatusDeleted,
		MessageDeleteStatusNotFound,
		MessageDeleteStatusDenied,
		MessageDeleteStatusImmutable:
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
// four recognised values.
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

// ConversationDeleteRequestID is the typed identifier of a single
// conversation_delete request, used to bind a conversation_delete_ack
// to the exact pending wipe it was issued for. Without this binding a
// late ack from an abandoned earlier wipe (one that filled and spent
// the retry budget) would silently retire a fresh pending wipe for
// the same peer and trigger the local sweep before the new wipe was
// actually applied on the recipient.
//
// Format mirrors MessageID (UUID v4) so the same generator can be
// used (protocol.NewMessageID() returns a UUID v4 string), but the
// type is distinct because the semantic role is different — a
// MessageID identifies a stored message, a ConversationDeleteRequestID
// identifies a request. Mixing them would let either field be passed
// where the other is expected, defeating the domain typing rule.
type ConversationDeleteRequestID string

// IsValid reports whether the request id is a syntactically valid
// UUID v4. Empty is invalid — every conversation_delete must carry a
// non-empty id so the matching predicate in the ack handler always
// has something to compare against.
func (id ConversationDeleteRequestID) IsValid() bool {
	return MessageID(id).IsValid()
}

// ConversationDeletePayload is the JSON-encoded body of
// OutgoingDM.CommandData for a DMCommandConversationDelete. It
// carries only the request id; the conversation peer is derived
// from the verified envelope sender on the recipient side (a
// forged payload cannot redirect the wipe), and no row list
// travels on the wire.
//
// The two sides scope the wipe asymmetrically:
//
//   - Receiver: every non-immutable row of the conversation that
//     was present in chatlog at FIRST CONTACT. The candidate set
//     is gathered once on first apply and frozen in the per-
//     (peer, requestID) cache; retries sweep ONLY that frozen
//     set, never current chatlog. See "Receiver-side frozen
//     scope" in docs/dm-commands.md.
//   - Sender: the intersection of (a) the current chatlog and
//     (b) the click-time `localKnownIDs` snapshot captured by
//     `SendConversationDelete`. Rows that landed locally AFTER
//     the click are OUTSIDE the sender's mirror sweep scope —
//     the post-ack sender sweep deliberately leaves them in
//     place. Symmetric survival on the receiver side is NOT
//     guaranteed: a row authored by the originator that arrived
//     at the receiver BEFORE first-contact gather is in the
//     receiver's frozen scope and gets wiped, even if it lands
//     on the originator only AFTER the click (in-flight at click
//     time). Such rows survive only on the originator. The
//     scope sentence describes the SENDER's local sweep, not a
//     symmetric two-side outcome.
//
// RequestID binds the request to its eventual ack (echoed by the
// recipient in ConversationDeleteAckPayload.RequestID) so a late
// ack from an abandoned earlier wipe cannot silently retire a
// fresh pending wipe for the same peer.
//
// Trade-off: the wire payload stays compact regardless of chat
// history size (no relay body-size pressure, no chunking), but
// the asymmetric scope means a message that was IN-FLIGHT at
// click time and lands on the originator AFTER the receiver has
// already wiped its outgoing copy can survive only on the
// originator's side. The tombstone set in DMRouter cancels the
// resurrection class where the SAME envelope is re-delivered
// after being wiped (relay retry, network reorder); a brand-new
// in-flight message is OUT OF SCOPE for this protocol and stays
// visible only on the originator until they delete it manually.
// Documented in docs/dm-commands.md "Bulk wipe" → "Late delivery
// limitation".
type ConversationDeletePayload struct {
	RequestID ConversationDeleteRequestID `json:"request_id"`
}

// Valid reports whether the payload is well-formed: the request id
// is present and has the documented UUID v4 shape.
func (p ConversationDeletePayload) Valid() bool {
	return p.RequestID.IsValid()
}

// ConversationDeleteStatus is the recipient-reported wire status
// carried by a conversation_delete_ack. Unlike MessageDeleteStatus
// it has no per-row "denied / immutable / not_found" branches: the
// wipe runs a flat predicate (every non-immutable row of the
// conversation with the requester, regardless of authorship — see
// DMCommandConversationDelete for the bulk-vs-per-row authorisation
// rationale) and either succeeds (possibly removing zero rows) or
// fails to even attempt the work because of a transient backend
// error or a per-row chatlog DELETE failure. The number of rows
// actually removed is reported separately in
// ConversationDeleteAckPayload.Deleted so subscribers can
// differentiate "wipe applied, nothing to remove" from "wipe
// applied, N rows gone".
//
// Recognised wire status is a separate concept from sender-side
// terminal outcome (see IsTerminalSuccess). Only
// ConversationDeleteStatusApplied is treated as terminal success
// by the sender — the post-ack local mirror sweep then runs and
// the pending entry is retired. ConversationDeleteStatusError is a
// recognised wire value but the sender deliberately treats it as
// non-terminal: the pending entry stays in place, the retry loop
// keeps chasing the peer, and the only remaining sender-side
// terminal paths are an applied ack or retry-budget abandonment.
// No TopicConversationDeleteCompleted event is published on an
// error ack.
type ConversationDeleteStatus string

const (
	// ConversationDeleteStatusApplied reports that the recipient
	// has CONVERGED its frozen first-contact scope for this
	// requestID — every non-immutable row that was in chatlog at
	// the moment of first contact has either been removed
	// (DeleteByID succeeded) or was already gone (DeleteByID
	// removed=false; sender intent satisfied). The number actually
	// removed across all attempts of this requestID is in
	// ConversationDeleteAckPayload.Deleted (cumulativeDeleted on
	// the receiver-side cache; may be zero — empty thread, or all
	// rows already immutable / already wiped). Applied does NOT
	// promise that every non-immutable row of the CURRENT
	// conversation is gone: rows authored by either side AFTER
	// first contact are outside the receiver's frozen scope and
	// stay on the receiver side, and self-authored "sent"
	// outbound rows on the originator are deliberately excluded
	// from the sender's localKnownIDs snapshot (see
	// snapshotLocalKnownConversationIDs) and therefore stay on the
	// originator after the post-ack mirror sweep. Immutable rows
	// are the only universal carve-out — they stay on both sides
	// regardless of frozen scope. The sender treats Applied as
	// terminal success and stops retrying.
	ConversationDeleteStatusApplied ConversationDeleteStatus = "applied"

	// ConversationDeleteStatusError reports that the recipient could
	// not run the wipe because of a transient backend failure
	// (chatlog unavailable, SQL error before the first row was
	// processed, etc.). The sender treats this as a transient
	// failure and keeps retrying until the budget is exhausted.
	// Recipients should NOT use this status for routine "no rows
	// matched" cases — that is success with Deleted=0.
	ConversationDeleteStatusError ConversationDeleteStatus = "error"
)

// Valid reports whether the status string is one of the recognised
// wire values. An empty status is rejected. "Recognised on the wire"
// is a separate concept from "terminal for the sender": only
// ConversationDeleteStatusApplied is treated as a terminal success
// by the sender (see IsTerminalSuccess); ConversationDeleteStatusError
// is a transient failure that keeps the pending entry alive and the
// retry loop running, with the only sender-side terminal paths being
// either an applied ack or retry-budget abandonment.
func (s ConversationDeleteStatus) Valid() bool {
	switch s {
	case ConversationDeleteStatusApplied,
		ConversationDeleteStatusError:
		return true
	default:
		return false
	}
}

// IsTerminalSuccess reports whether the sender should treat this
// status as a terminal success (recipient is consistent with our
// intent and the sender's local mirror sweep can run). Only
// "applied" qualifies; "error" is a transient failure that does
// NOT terminate the request — the pending entry stays in place and
// the retry loop keeps chasing the peer until either an applied
// ack arrives or the retry budget is exhausted.
func (s ConversationDeleteStatus) IsTerminalSuccess() bool {
	return s == ConversationDeleteStatusApplied
}

// ConversationDeleteAckPayload is the JSON-encoded body of
// OutgoingDM.CommandData for a DMCommandConversationDeleteAck. It
// echoes the request id, the recipient-reported status, and the
// number of rows actually removed on the recipient side. The ack
// is matched to the sender's pending entry by (envelope sender,
// RequestID) rather than by envelope sender alone — this defends
// against a late ack from an abandoned earlier wipe being mistaken
// for the ack of a fresh wipe to the same peer. Status is one of
// the recognised wire values (Valid) but not necessarily terminal
// for the sender — see IsTerminalSuccess for that distinction.
type ConversationDeleteAckPayload struct {
	RequestID ConversationDeleteRequestID `json:"request_id"`
	Status    ConversationDeleteStatus    `json:"status"`
	Deleted   int                         `json:"deleted"`
}

// Valid reports whether the ack payload is well-formed: the request
// id is present and has the documented UUID v4 shape, the status is
// one of the recognised wire values, and Deleted is non-negative.
func (p ConversationDeleteAckPayload) Valid() bool {
	return p.RequestID.IsValid() && p.Status.Valid() && p.Deleted >= 0
}

// MarshalConversationDeletePayload encodes a ConversationDeletePayload
// as JSON suitable for OutgoingDM.CommandData.
func MarshalConversationDeletePayload(p ConversationDeletePayload) (string, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// MarshalConversationDeleteAckPayload encodes a
// ConversationDeleteAckPayload as JSON suitable for
// OutgoingDM.CommandData.
func MarshalConversationDeleteAckPayload(p ConversationDeleteAckPayload) (string, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
