package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ---------------------------------------------------------------------------
// Control DM wire path
// ---------------------------------------------------------------------------
//
// Control DMs (message_delete, message_delete_ack, ...) travel on the
// dedicated wire topic protocol.TopicControlDM. Their encrypted envelope
// shape is identical to a regular DM (directmsg.PlainMessage), but the
// node-level Frame.Type is "send_control_message" rather than
// "send_message" — see node.Service for the corresponding receive-side
// divert. The node skips chatlog persistence and the
// LocalChangeNewMessage event for this Frame.Type, which is what
// distinguishes control DMs from regular DMs at the storage layer.
//
// See docs/dm-commands.md for the full design.

// SendControlMessage encrypts a control DM (message_delete,
// message_delete_ack, ...) for recipient to and submits it through the
// local node on the dedicated control wire topic. It does NOT write to
// chatlog, does NOT return a DirectMessage echo, and does NOT cause a
// LocalChangeNewMessage event to fire on either side. Reliability is
// the caller's responsibility — the recipient is expected to send back
// a separate control DM (message_delete_ack) which the caller can match
// against pending state to retire retries.
//
// The body of a control DM is empty by contract; the action data lives
// in payload (JSON-encoded MessageDeletePayload, MessageDeleteAckPayload,
// ...). Body validation that SendDirectMessage applies to data DMs is
// intentionally skipped here.
func (d *DMCrypto) SendControlMessage(
	ctx context.Context,
	to domain.PeerIdentity,
	cmd domain.DMCommand,
	payload string,
) (domain.MessageID, error) {
	to = domain.PeerIdentity(strings.TrimSpace(string(to)))
	if to == "" {
		return "", fmt.Errorf("recipient is required")
	}
	if !cmd.IsControl() {
		return "", fmt.Errorf("SendControlMessage requires a control command (got %q)", cmd)
	}

	contact, err := d.ensureRecipientContact(ctx, string(to))
	if err != nil {
		return "", err
	}

	recipient := domain.DMRecipient{
		Address:      to,
		BoxKeyBase64: contact.BoxKey,
	}

	// The encrypted envelope reuses the regular PlainMessage layout —
	// Body is empty by contract, Command and CommandData carry the
	// dispatch-relevant payload. The recipient's
	// DecryptIncomingControlMessage decodes only Command/CommandData.
	envelope := domain.OutgoingDM{
		Body:        "",
		Command:     cmd,
		CommandData: payload,
	}
	ciphertext, err := directmsg.EncryptForParticipants(d.id, recipient, envelope)
	if err != nil {
		return "", fmt.Errorf("encrypt control DM: %w", err)
	}

	messageID, err := protocol.NewMessageID()
	if err != nil {
		return "", err
	}

	frame := buildControlMessageFrame(d.id.Address, string(to), string(messageID), ciphertext, time.Now().UTC())

	reply, err := d.rpc.LocalRequestFrameCtx(ctx, frame)
	if err != nil {
		return "", err
	}

	// The node funnels send_control_message through the same
	// storeMessageFrame / storeIncomingMessage path as send_message,
	// so a successful submission returns "message_stored" (or
	// "message_known" on dedup). We accept either as a wire-handoff
	// ack; an "error" reply is surfaced verbatim. Note that this is
	// NOT the recipient ack — the recipient ack is a separate
	// message_delete_ack control DM that arrives later.
	switch reply.Type {
	case "message_stored", "message_known":
		return domain.MessageID(messageID), nil
	case "error":
		return "", fmt.Errorf("control DM rejected by node (code=%s): %s", reply.Code, reply.Error)
	default:
		return "", fmt.Errorf("unexpected control send reply: %s", reply.Type)
	}
}

// buildControlMessageFrame assembles the Frame submitted to the local
// node for a control DM. Pulled out as a separate helper so tests can
// inspect the frame fields without spinning up rpc/identity/chatlog.
//
// Required fields mirror what incomingMessageFromFrame in node parses:
// ID, Topic, Address (sender), Recipient, Flag (must satisfy Flag.Valid),
// CreatedAt (RFC3339), and Body. Without all of these, storeMessageFrame
// would reject the frame with ErrCodeInvalidSendMessage before
// storeIncomingMessage ever runs the topic-aware control branch.
//
// Flag is set to MessageFlagSenderDelete: control DMs are sender-emitted
// and follow the same default policy as data DMs. This matches what
// SendDirectMessage stamps onto outbound frames.
func buildControlMessageFrame(senderAddress, recipient, messageID, ciphertext string, createdAt time.Time) protocol.Frame {
	return protocol.Frame{
		Type:       "send_control_message",
		Topic:      protocol.TopicControlDM,
		ID:         messageID,
		Address:    senderAddress,
		Recipient:  recipient,
		Flag:       string(protocol.MessageFlagSenderDelete),
		CreatedAt:  createdAt.Format(time.RFC3339),
		TTLSeconds: 0,
		Body:       ciphertext,
	}
}

// DecryptIncomingControlMessage turns a LocalChangeNewControlMessage
// event into a (DMCommand, commandData) pair. Returns ok=false for
// events that are not control DMs, fail signature verification, or
// carry a non-control command after decryption.
//
// Unlike DecryptIncomingMessage, this does not return a DirectMessage —
// control DMs are not chatlog rows. Callers (DMRouter) dispatch on the
// returned DMCommand and parse commandData via the typed payloads
// defined in domain (MessageDeletePayload, MessageDeleteAckPayload).
func (d *DMCrypto) DecryptIncomingControlMessage(event protocol.LocalChangeEvent) (cmd domain.DMCommand, commandData string, sender domain.PeerIdentity, ok bool) {
	if event.Type != protocol.LocalChangeNewControlMessage {
		return "", "", "", false
	}
	if event.Topic != protocol.TopicControlDM {
		return "", "", "", false
	}

	senderPubKey, ok := d.resolveSenderPubKey(event.Sender)
	if !ok {
		return "", "", "", false
	}

	msg, err := directmsg.DecryptForIdentity(d.id, event.Sender, senderPubKey, event.Recipient, event.Body)
	if err != nil {
		return "", "", "", false
	}

	parsed := domain.DMCommand(msg.Command)
	if !parsed.IsControl() {
		// Wire-decrypted but the inner Command is not a recognised
		// control command. This can happen when a peer ahead of us in
		// versioning sends a control DM type we do not yet implement —
		// drop quietly. A peer that wants to inject a data command
		// through the control wire would also land here; the same drop
		// closes that hole.
		return "", "", "", false
	}

	return parsed, msg.CommandData, domain.PeerIdentity(event.Sender), true
}

// resolveSenderPubKey returns the base64-encoded ed25519 public key for
// the given sender address, or ok=false if it cannot be resolved. Used
// by both DecryptIncomingMessage and DecryptIncomingControlMessage so
// the lookup logic stays in one place.
//
// Lookup order:
//  1. Self — return the local identity's public key directly.
//  2. Trusted contacts — preferred source.
//  3. All known contacts — fallback for relayed messages whose sender
//     has not yet been promoted to the trust store. This keeps inbound
//     decryption working during the on-demand sync window.
func (d *DMCrypto) resolveSenderPubKey(sender string) (string, bool) {
	if sender == d.id.Address {
		return identity.PublicKeyBase64(d.id.PublicKey), true
	}

	contactsReply, err := d.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return "", false
	}
	contacts := contactsFromFrame(contactsReply)
	if contact, found := contacts[sender]; found && contact.PubKey != "" {
		return contact.PubKey, true
	}

	allReply, err := d.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		return "", false
	}
	allContacts := contactsFromFrame(allReply)
	if allContact, found := allContacts[sender]; found && allContact.PubKey != "" {
		return allContact.PubKey, true
	}

	return "", false
}
