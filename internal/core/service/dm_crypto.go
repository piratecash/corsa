package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/logid"
	"github.com/piratecash/corsa/internal/core/node"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// ErrReplyToNotFound marks a send rejected by reply-reference validation:
// msg.ReplyTo names a message that is absent from the chatlog for that
// conversation at send time. The sentinel asserts ABSENCE, nothing more —
// it cannot distinguish "deleted after the user quoted it" from an ID
// that never existed or belongs to another conversation; the chatlog
// keeps no tombstones to tell those apart. Callers attach the meaning:
// DMRouter.SendMessage (the composer path, where ReplyTo provenance is a
// bubble the user clicked in THIS conversation) reads it as
// deleted-mid-compose and degrades to a plain send; direct
// DesktopClient/SDK callers get the error as-is and decide themselves.
// A chatlog lookup FAILURE is a different error, never this sentinel —
// see the validation block in SendDirectMessage.
var ErrReplyToNotFound = errors.New("reply_to message not found in conversation")

// DMCrypto owns all direct-message cryptographic operations split out of
// the former DesktopClient:
//
//   - encryption and send (SendDirectMessage)
//   - inbound decryption (DecryptIncomingMessage)
//   - contact bootstrap during decryption (fetchContactsForDecrypt)
//   - on-demand per-peer message sync (SyncDirectMessagesFromPeers)
//   - header-only contact import (importIncomingDMHeaderContacts)
//
// Dependencies are narrow and explicit: it uses the LocalRPCClient for node
// RPCs, the ChatlogGateway for reply-reference validation, and the local
// identity for key material. No direct reach into config, filetransfer,
// or chat persistence writes.
type DMCrypto struct {
	rpc     *LocalRPCClient
	chatlog *ChatlogGateway
	id      *identity.Identity
	// onDecryptFailure is the §4.10 single entry point: every decrypt path
	// (live, previews, batch) reports through it. Optional — nil means no
	// recovery subsystem is wired (SDK consumers). Set once at wiring time
	// by the DMRouter, before any decrypt path can run.
	onDecryptFailure func(DecryptFailure)

	// onDecryptSuccess is the §4.10 receiver-side qualifying chokepoint:
	// EVERY successfully decrypted incoming DM flows through it — the live
	// event path, the history load and the previews alike — because both
	// the established fact and the retry_of acceptance must not depend on
	// which chat the UI happens to have open when the message arrives.
	onDecryptSuccess func(*DirectMessage)

	// onSendSuccess is the sender-side qualifying chokepoint: EVERY
	// accepted user-authored outgoing DM flows through it — the composer,
	// the RPC send, the file-transfer bridge — because the established
	// fact must not depend on WHICH surface the user sent from.
	onSendSuccess func(peer string)
}

// defaultOutgoingMessageFlag is the deletion policy this build stamps on
// the direct messages it sends. It is the author's own answer to "may my
// counterpart erase this from their side too", and it decides, on the
// receiving end, whether deleting a message someone wrote to you also
// asks them to drop their copy (authorizedToDelete).
const defaultOutgoingMessageFlag = protocol.MessageFlagAnyDelete

// DecryptFailureClass separates the §4.10 outcome classes: only the
// CONFIRMED crypto-fail (authenticated envelope, unreadable sealed parts)
// leads to a notice; a missing sender key leads to a key lookup and a
// local retry; everything else is not a recovery matter at all.
type DecryptFailureClass uint8

const (
	// DecryptFailureMissingSenderKey — the sender's public key is unknown,
	// verification could not even start.
	DecryptFailureMissingSenderKey DecryptFailureClass = iota + 1
	// DecryptFailureSealedUnreadable — the envelope authenticated but no
	// sealed part opened with the current keys.
	DecryptFailureSealedUnreadable
)

// DecryptFailure describes one failed decrypt of a stored DM row.
type DecryptFailure struct {
	MessageID string
	Sender    string
	Recipient string
	Class     DecryptFailureClass
}

// noteDecryptFailure funnels a failed decrypt into the recovery hook,
// applying the §4.10 entry condition: only rows addressed TO this node
// FROM somebody else qualify — a failed decrypt of one's own outgoing row
// is a manual-fallback case, never a network recovery.
func (d *DMCrypto) noteDecryptFailure(class DecryptFailureClass, messageID, sender, recipient string) {
	if d.onDecryptFailure == nil || messageID == "" {
		return
	}
	if recipient != d.id.Address || sender == d.id.Address {
		return
	}
	d.onDecryptFailure(DecryptFailure{MessageID: messageID, Sender: sender, Recipient: recipient, Class: class})
}

// noteDecryptSuccess funnels a decrypted message into the success hook
// under the same entry condition as the failure side: incoming only.
func (d *DMCrypto) noteDecryptSuccess(msg *DirectMessage) {
	if d.onDecryptSuccess == nil || msg == nil {
		return
	}
	if msg.Recipient.String() != d.id.Address || msg.Sender.String() == d.id.Address {
		return
	}
	d.onDecryptSuccess(msg)
}

// notePreviewDecryptSuccess adapts a preview-path decrypt for the success
// hook: previews are a first-read surface like any other — a replacement
// first seen in the sidebar after a restart must close recovery, and a
// decrypted incoming DM must establish the peer, whether or not the chat
// ever opens.
func (d *DMCrypto) notePreviewDecryptSuccess(entry *chatlog.Entry, plain *directmsg.PlainMessage) {
	d.noteDecryptSuccess(&DirectMessage{
		ID:        entry.ID,
		Sender:    domain.PeerIdentityFromWire(entry.Sender),
		Recipient: domain.PeerIdentityFromWire(entry.Recipient),
		Body:      plain.Body,
		RetryOf:   sanitizedRetryOf(plain.RetryOf),
	})
}

// NewDMCrypto wires the three dependencies that the DM crypto path needs.
func NewDMCrypto(rpc *LocalRPCClient, chatlog *ChatlogGateway, id *identity.Identity) *DMCrypto {
	return &DMCrypto{rpc: rpc, chatlog: chatlog, id: id}
}

// SendDirectMessage encrypts msg for recipient to and submits it through
// the local node. Returns the decrypted echo so callers can render the
// message in the active conversation without a round-trip through the
// decrypt path.
func (d *DMCrypto) SendDirectMessage(ctx context.Context, to domain.PeerIdentity, msg domain.OutgoingDM) (*DirectMessage, error) {
	to = domain.PeerIdentityFromWire(strings.TrimSpace(to.String()))
	msg.Body = strings.TrimSpace(msg.Body)
	msg.ReplyTo = domain.MessageID(strings.TrimSpace(string(msg.ReplyTo)))

	// Control DMs (message_delete, message_delete_ack) must travel on the
	// dedicated control wire path that bypasses chatlog persistence and
	// the LocalChangeNewMessage event — the regular send_message path
	// would surface them as visible "[delete]" rows in the sender's chat
	// (the exact failure mode docs/dm-commands.md is designed to avoid).
	// This guard is the canonical chokepoint: every higher-level entry
	// point (DesktopClient.SendDirectMessage, DMRouter.SendMessage)
	// funnels through here, so a single check covers all of them.
	if msg.Command.IsControl() {
		return nil, fmt.Errorf("control DM (command=%s) must be sent through SendControlMessage, not SendDirectMessage", msg.Command)
	}

	if to.IsZero() || msg.Body == "" {
		return nil, fmt.Errorf("recipient and message are required")
	}
	if !msg.ReplyTo.IsValidOrEmpty() {
		return nil, fmt.Errorf("reply_to must be a valid message ID (UUID v4)")
	}
	if !msg.PresetID.IsValidOrEmpty() {
		return nil, fmt.Errorf("preset id must be a valid message ID (UUID v4)")
	}

	contact, err := d.ensureRecipientContact(ctx, to.String())
	if err != nil {
		return nil, err
	}

	// Reply-reference validation runs AFTER the contact fetch (the RPC
	// round-trip that dominates this function's latency) and immediately
	// before the reference is baked into the ciphertext. Checking at the
	// top of the function left the full contact-fetch window for a
	// concurrent message_delete / conversation wipe to remove the quoted
	// row after it had already passed validation — the send would then
	// carry a dead reply_to that the deleted-mid-compose degrade path
	// (DMRouter.SendMessage) never got to see. The residual window is
	// encrypt+dispatch; a deletion landing inside it yields a dangling
	// reference, which every renderer must tolerate anyway (the peer can
	// delete the quoted message seconds after a fully valid send).
	//
	// A store lookup failure is deliberately NOT ErrReplyToNotFound: the
	// sentinel asserts "the row is absent", and callers degrade to a
	// plain send on it — masking a broken chatlog behind that would
	// silently strip quotes from every reply while the DB is unhealthy.
	if msg.ReplyTo != "" && d.chatlog != nil && d.chatlog.Store() != nil {
		found, lookupErr := d.chatlog.Store().LookupEntryInConversation(ctx, to, domain.MessageID(msg.ReplyTo))
		if lookupErr != nil {
			return nil, fmt.Errorf("reply_to validation for %q: %w", msg.ReplyTo, lookupErr)
		}
		if !found {
			return nil, fmt.Errorf("reply_to message %q not found in conversation with %s: %w", msg.ReplyTo, to, ErrReplyToNotFound)
		}
	}

	recipient := domain.DMRecipient{
		Address:      to,
		BoxKeyBase64: contact.BoxKey,
	}

	ciphertext, err := directmsg.EncryptForParticipants(d.id, recipient, msg)
	if err != nil {
		return nil, err
	}

	messageID := protocol.MessageID(msg.PresetID)
	if messageID == "" {
		messageID, err = protocol.NewMessageID()
		if err != nil {
			return nil, err
		}
	}

	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339)

	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:      "send_message",
		Topic:     "dm",
		ID:        string(messageID),
		Address:   d.id.Address,
		Recipient: to.String(),
		// defaultOutgoingMessageFlag: either participant may have this
		// message removed
		// from BOTH sides. The flag is the author's own answer to "may
		// my counterpart erase this from their side too", and the
		// product answer is yes: a conversation is shared, and a user
		// deleting a message from it expects it gone, not gone from
		// their own screen while the other copy stands.
		//
		// It is the per-message form of an authority that already
		// exists one level up — the bulk wipe erases the whole
		// thread on both sides regardless of authorship — so refusing
		// it one message at a time meant the user could erase
		// everything but not one line.
		//
		// The flag travels with the row and is read on receipt
		// (authorizedToDelete), so this decides only messages sent from
		// here on. A row an older build stamped sender-delete stays
		// deletable by its author alone, and deleting our view of one
		// remains a local act.
		Flag:       string(defaultOutgoingMessageFlag),
		CreatedAt:  createdAt,
		TTLSeconds: 0,
		Body:       ciphertext,
	})
	if err != nil {
		return nil, err
	}

	if reply.Type != "message_stored" && reply.Type != "message_known" {
		return nil, sendRejection(reply)
	}

	if d.onSendSuccess != nil {
		d.onSendSuccess(to.String())
	}
	return &DirectMessage{
		ID:            string(messageID),
		Sender:        domain.PeerIdentityFromWire(d.id.Address),
		Recipient:     to,
		Body:          msg.Body, // plaintext — already known to us
		ReplyTo:       msg.ReplyTo,
		Command:       msg.Command,
		CommandData:   msg.CommandData,
		Timestamp:     now,
		ReceiptStatus: "sent",
	}, nil
}

// DeliveryCancellation is what the node could establish while stopping a
// delivery. HooksRemoved is for the log; NeverEmitted is the one the
// caller acts on.
type DeliveryCancellation struct {
	// NeverEmitted reports that the envelope provably never reached the
	// wire, so the recipient cannot hold a copy. False means "not
	// knowable", never "it was delivered".
	NeverEmitted bool
	// HooksRemoved counts the delivery hooks the node actually dropped.
	HooksRemoved int
}

// CancelMessageDelivery asks the node to stop delivering a DM we sent
// earlier: the envelope leaves the store-and-forward backlog and every
// queue that could still emit it (node.Service.CancelOutgoingDelivery).
//
// It is the undo of SendDirectMessage above, and lives beside it for that
// reason.
func (d *DMCrypto) CancelMessageDelivery(ctx context.Context, to domain.PeerIdentity, messageID domain.MessageID) (DeliveryCancellation, error) {
	if to.IsZero() {
		return DeliveryCancellation{}, fmt.Errorf("cancel delivery: recipient is required")
	}
	if !messageID.IsValid() {
		return DeliveryCancellation{}, fmt.Errorf("cancel delivery: message id %q is not a valid UUID v4", messageID)
	}

	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:      "cancel_message_delivery",
		Topic:     "dm",
		ID:        string(messageID),
		Recipient: to.String(),
	})
	if err != nil {
		return DeliveryCancellation{}, fmt.Errorf("cancel delivery of %s: %w", messageID, err)
	}
	if reply.Type == "error" {
		return DeliveryCancellation{}, fmt.Errorf("cancel delivery of %s: node refused: %s (%s)", messageID, reply.Error, reply.Code)
	}

	cancellation := DeliveryCancellation{
		NeverEmitted: reply.Status == node.CancelStatusNeverEmitted,
		HooksRemoved: reply.Count,
	}
	log.Debug().
		Str("message_id", logid.Of(string(messageID))).
		Str("recipient", logid.Of(to.String())).
		Int("hooks_removed", cancellation.HooksRemoved).
		Bool("never_emitted", cancellation.NeverEmitted).
		Msg("dm_crypto: outgoing delivery cancelled")
	return cancellation, nil
}

// FreezeMessageDelivery stops the node from sending one message without
// withdrawing it, and reports whether it provably never reached the wire.
//
// A single delete classifies from the ROW — its durable never-emitted mark
// — and that mark only means something while nothing can emit the message
// behind the reader's back. The freeze is ended by CancelMessageDelivery,
// which every route that takes it runs immediately afterwards.
func (d *DMCrypto) FreezeMessageDelivery(ctx context.Context, messageID domain.MessageID) (bool, error) {
	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:  "freeze_message_delivery",
		Topic: "dm",
		ID:    string(messageID),
	})
	if err != nil {
		return false, fmt.Errorf("freeze delivery of %s: %w", messageID, err)
	}
	if reply.Type == "error" {
		return false, fmt.Errorf("freeze delivery of %s: node refused: %s (%s)", messageID, reply.Error, reply.Code)
	}
	return reply.Status == node.CancelStatusNeverEmitted, nil
}

// CancelConversationDelivery asks the node to stop delivering everything
// we still owe the peer: the bulk form of CancelMessageDelivery, used
// when the user wipes the whole thread.
//
// Returns how many messages were withdrawn. The count is for the log —
// the wipe proceeds either way, for the reason given at its call site.
func (d *DMCrypto) CancelConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) (ConversationCancellation, error) {
	var result ConversationCancellation
	if peer.IsZero() {
		return result, fmt.Errorf("cancel conversation delivery: peer is required")
	}

	ids := make([]string, 0, len(scope))
	for _, id := range scope {
		ids = append(ids, string(id))
	}
	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:      "cancel_conversation_delivery",
		Topic:     "dm",
		Recipient: peer.String(),
		IDs:       ids,
	})
	if err != nil {
		return result, fmt.Errorf("cancel conversation delivery to %s: %w", peer, err)
	}
	if reply.Type == "error" {
		return result, fmt.Errorf("cancel conversation delivery to %s: node refused: %s (%s)", peer, reply.Error, reply.Code)
	}

	result.Cancelled = reply.Count
	result.NeverEmitted = make(map[domain.MessageID]struct{}, len(reply.IDs))
	for _, id := range reply.IDs {
		result.NeverEmitted[domain.MessageID(id)] = struct{}{}
	}

	log.Debug().
		Str("peer", logid.Of(peer.String())).
		Int("messages", result.Cancelled).
		Int("never_emitted", len(result.NeverEmitted)).
		Msg("dm_crypto: outgoing deliveries to the peer cancelled")
	return result, nil
}

// FreezeConversationDelivery asks the node to stop sending everything we
// still owe the peer WITHOUT withdrawing it, and reports which of the ids
// provably never reached the wire.
//
// The wipe classifies against this answer and against the rows' own marks,
// then either commits — CancelConversationDelivery withdraws for real — or
// aborts, and ThawConversationDelivery puts everything back. Freezing
// rather than cancelling up front is what makes the abort possible: a
// cancellation cannot be undone, so a transaction that then failed would
// leave the user with messages on screen that will never be sent.
func (d *DMCrypto) FreezeConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) (ConversationFreeze, error) {
	var result ConversationFreeze
	if peer.IsZero() {
		return result, fmt.Errorf("freeze conversation delivery: peer is required")
	}

	ids := make([]string, 0, len(scope))
	for _, id := range scope {
		ids = append(ids, string(id))
	}
	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:      "freeze_conversation_delivery",
		Topic:     "dm",
		Recipient: peer.String(),
		IDs:       ids,
	})
	if err != nil {
		return result, fmt.Errorf("freeze conversation delivery to %s: %w", peer, err)
	}
	if reply.Type == "error" {
		return result, fmt.Errorf("freeze conversation delivery to %s: node refused: %s (%s)", peer, reply.Error, reply.Code)
	}

	result.Frozen = reply.Count
	result.NeverEmitted = make(map[domain.MessageID]struct{}, len(reply.IDs))
	for _, id := range reply.IDs {
		result.NeverEmitted[domain.MessageID(id)] = struct{}{}
	}
	log.Debug().
		Str("peer", logid.Of(peer.String())).
		Int("frozen", result.Frozen).
		Int("never_emitted", len(result.NeverEmitted)).
		Msg("dm_crypto: deliveries to the peer frozen for a wipe")
	return result, nil
}

// ThawConversationDelivery ends a freeze the wipe did not commit.
func (d *DMCrypto) ThawConversationDelivery(ctx context.Context, peer domain.PeerIdentity, scope []domain.MessageID) error {
	if len(scope) == 0 {
		return nil
	}
	ids := make([]string, 0, len(scope))
	for _, id := range scope {
		ids = append(ids, string(id))
	}
	reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type:      "thaw_conversation_delivery",
		Topic:     "dm",
		Recipient: peer.String(),
		IDs:       ids,
	})
	if err != nil {
		return fmt.Errorf("thaw conversation delivery to %s: %w", peer, err)
	}
	if reply.Type == "error" {
		return fmt.Errorf("thaw conversation delivery to %s: node refused: %s (%s)", peer, reply.Error, reply.Code)
	}
	return nil
}

// ConversationFreeze is what the node knew when it stopped sending. The
// answer is authoritative for as long as the freeze holds: nothing can
// emit a frozen message, so it cannot go stale while the wipe decides.
type ConversationFreeze struct {
	Frozen       int
	NeverEmitted map[domain.MessageID]struct{}
}

// sendRejection turns a refused send into an error that keeps the node's
// REASON. Collapsing every refusal into "unexpected send reply: error"
// threw away the one bit that decides what to tell the user: whether the
// refusal is transient. The store-deferred case makes that visible —
// while the refusals of deleted ids cannot be read, the node declines to
// store anything in the conversation, sending included, and "try again in
// a moment" is a very different message from "this failed".
//
// The sentinel is wrapped, not returned bare, so errors.Is keeps working
// while the text still names what happened.
func sendRejection(reply protocol.Frame) error {
	if reply.Type != "error" {
		return fmt.Errorf("unexpected send reply: %s", reply.Type)
	}
	sentinel := protocol.ErrorFromCode(reply.Code)
	if detail := strings.TrimSpace(reply.Error); detail != "" {
		return fmt.Errorf("send refused: %s: %w", detail, sentinel)
	}
	return fmt.Errorf("send refused: %w", sentinel)
}

// ConversationCancellation is what a whole-conversation withdrawal took
// back. NeverEmitted names the ids the node can prove never reached the
// wire — the same claim a single withdrawal makes, and used for the same
// reason: a message nobody has ever seen is not requested from the peer,
// because the request would be how they learn it existed.
type ConversationCancellation struct {
	Cancelled    int
	NeverEmitted map[domain.MessageID]struct{}
}

// DecryptIncomingMessage turns a local-change event into a typed
// DirectMessage. Returns nil for events that are not DM-related or cannot
// be decrypted (missing sender key). Malformed reply_to references are
// silently cleared — a remote peer cannot inject dangling cross-thread
// reply links this way.
func (d *DMCrypto) DecryptIncomingMessage(ctx context.Context, event protocol.LocalChangeEvent) *DirectMessage {
	if event.Type != protocol.LocalChangeNewMessage || event.Topic != "dm" {
		return nil
	}

	senderPubKey, ok := d.resolveSenderPubKey(event.Sender)
	if !ok {
		// The §4.10 missing_sender_key class: the row stays undecrypted
		// until a key lookup lands, then re-decrypts locally on the next
		// read — no notice.
		d.noteDecryptFailure(DecryptFailureMissingSenderKey, event.MessageID, event.Sender, event.Recipient)
		return nil
	}

	msg, err := directmsg.DecryptForIdentity(d.id, event.Sender, senderPubKey, event.Recipient, event.Body)
	if err != nil {
		if errors.Is(err, directmsg.ErrSealedUnreadable) {
			// The CONFIRMED crypto-fail class: envelope authenticated, keys
			// rotated underneath it — the one case the notice exists for.
			d.noteDecryptFailure(DecryptFailureSealedUnreadable, event.MessageID, event.Sender, event.Recipient)
		}
		return nil
	}

	ts, parseErr := parseTimestamp(event.CreatedAt)
	if parseErr != nil {
		ts = time.Now().UTC()
	}

	status := "delivered"
	if event.Sender == d.id.Address {
		status = "sent"
	}

	replyTo := domain.MessageID(msg.ReplyTo)
	if replyTo != "" {
		if !replyTo.IsValid() {
			replyTo = ""
		} else if store := d.chatlog.Store(); store != nil {
			peerAddress := event.Sender
			if event.Sender == d.id.Address {
				peerAddress = event.Recipient
			}
			// Inline decryption has no error path to take — this builds one
			// message for the UI — so a lookup that FAILED keeps the
			// reference instead of dropping it. Dropping was the worse of the
			// two: a cancelled context or an unhealthy database silently
			// stripped a valid quote, while a kept reference is what every
			// renderer already tolerates (the peer can delete the quoted
			// message at any time).
			found, err := store.LookupEntryInConversation(ctx, domain.PeerIdentityFromWire(peerAddress), domain.MessageID(replyTo))
			if err != nil {
				log.Warn().Err(err).
					Str("message_id", string(event.MessageID)).
					Msg("reply reference lookup failed; keeping the reference")
			} else if !found {
				replyTo = ""
			}
		}
	}

	decrypted := &DirectMessage{
		ID:            event.MessageID,
		Sender:        domain.PeerIdentityFromWire(event.Sender),
		Recipient:     domain.PeerIdentityFromWire(event.Recipient),
		Body:          msg.Body,
		ReplyTo:       replyTo,
		RetryOf:       sanitizedRetryOf(msg.RetryOf),
		Command:       domain.DMCommand(msg.Command),
		CommandData:   msg.CommandData,
		Timestamp:     ts,
		ReceiptStatus: status,
	}
	d.noteDecryptSuccess(decrypted)
	return decrypted
}

// sanitizedRetryOf mirrors the ReplyTo treatment: a malformed id degrades
// to "no link" rather than poisoning downstream consumers.
func sanitizedRetryOf(raw string) domain.MessageID {
	retryOf := domain.MessageID(raw)
	if retryOf != "" && !retryOf.IsValid() {
		return ""
	}
	return retryOf
}

// SyncDirectMessagesFromPeers walks a list of peer addresses, pulls any
// DM IDs the local store does not have yet, and imports them under the
// supplied counterparty identity. Used by the sidebar sync trigger when the
// user clicks "fetch history".
func (d *DMCrypto) SyncDirectMessagesFromPeers(ctx context.Context, peerAddresses []string, counterparty string) (int, error) {
	counterparty = strings.TrimSpace(counterparty)
	if counterparty == "" {
		return 0, fmt.Errorf("counterparty is required")
	}

	imported := 0
	seenIDs := make(map[string]struct{})
	var firstErr error
	me := d.id.Address

	for _, peerAddress := range peerAddresses {
		peerAddress = strings.TrimSpace(peerAddress)
		if peerAddress == "" || peerAddress == d.rpc.LocalAddress() {
			continue
		}

		remoteConn, remoteReader, _, err := d.rpc.OpenSessionAt(ctx, peerAddress, "desktop")
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		idsFrame, err := d.rpc.RequestFrame(remoteConn, remoteReader, protocol.Frame{
			Type:  "fetch_message_ids",
			Topic: "dm",
		})
		if err != nil {
			_ = remoteConn.Close()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		for _, id := range idsFrame.IDs {
			if _, ok := seenIDs[id]; ok {
				continue
			}
			seenIDs[id] = struct{}{}

			messageFrame, err := d.rpc.RequestFrame(remoteConn, remoteReader, protocol.Frame{
				Type:  "fetch_message",
				Topic: "dm",
				ID:    id,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if messageFrame.Item == nil {
				continue
			}

			item := messageFrame.Item
			if !isConversationMessage(*item, me, counterparty) {
				continue
			}

			reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
				Type:       "import_message",
				Topic:      "dm",
				ID:         item.ID,
				Address:    item.Sender,
				Recipient:  item.Recipient,
				Flag:       item.Flag,
				CreatedAt:  item.CreatedAt,
				TTLSeconds: item.TTLSeconds,
				Body:       item.Body,
			})
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if reply.Type == "message_stored" {
				imported++
			}
		}

		_ = remoteConn.Close()
	}

	if imported == 0 && firstErr != nil {
		return 0, firstErr
	}
	return imported, nil
}

// FetchConversation loads the full chat history for a single peer from the
// local chatlog, decrypts it, and returns the messages. Called on demand
// when the user switches conversation rather than keeping every thread in
// memory.
func (d *DMCrypto) FetchConversation(ctx context.Context, peerAddress domain.PeerIdentity) ([]DirectMessage, error) {
	peerAddress = domain.PeerIdentityFromWire(strings.TrimSpace(peerAddress.String()))
	if peerAddress.IsZero() {
		return nil, fmt.Errorf("peer address is required")
	}
	store := d.chatlog.Store()
	if store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	entries, err := d.chatlog.Read(ctx, "dm", peerAddress)
	if err != nil {
		return nil, fmt.Errorf("chatlog read: %w", err)
	}

	senderSet := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		senderSet[entry.Sender] = struct{}{}
	}
	senders := make([]string, 0, len(senderSet))
	for s := range senderSet {
		senders = append(senders, s)
	}
	decryptContacts, err := d.fetchContactsForDecrypt(ctx, senders)
	if err != nil {
		return nil, err
	}

	receiptsReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_delivery_receipts", Recipient: d.id.Address})
	if err != nil {
		return nil, err
	}
	deliveryReceipts := receiptRecordsFromFrames(receiptsReply.Receipts)
	pendingReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_pending_messages", Topic: "dm"})
	if err != nil {
		return nil, err
	}
	pendingMessages := pendingMessagesFromFrame(pendingReply)

	records := make([]MessageRecord, 0, len(entries))
	for _, entry := range entries {
		ts, parseErr := parseTimestamp(entry.CreatedAt)
		if parseErr != nil {
			continue
		}
		records = append(records, MessageRecord{
			ID:              entry.ID,
			Flag:            entry.Flag,
			Timestamp:       ts.UTC(),
			Sender:          entry.Sender,
			Recipient:       entry.Recipient,
			Body:            entry.Body,
			PersistedStatus: entry.DeliveryStatus,
		})
	}

	messages := decryptDirectMessagesReporting(d.id, decryptContacts, records, deliveryReceipts, pendingMessages, d.onDecryptFailure, d.onDecryptSuccess)
	// A failed reply lookup fails the READ. Returning the messages anyway
	// handed the caller history with quotes silently removed and called it a
	// success.
	if err := sanitizeReplyReferences(ctx, messages, store, d.id.Address); err != nil {
		return nil, err
	}
	return messages, nil
}

// FetchConversationPreviews loads the last message for each DM thread and
// returns preview data for the sidebar. Combines chatlog summaries with
// receipt / pending state from the node so unread badges survive restart.
func (d *DMCrypto) FetchConversationPreviews(ctx context.Context) ([]ConversationPreview, error) {
	if d.chatlog.Store() == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	lastEntries, err := d.chatlog.ReadLastEntryPerPeer(ctx)
	if err != nil {
		return nil, fmt.Errorf("chatlog previews: %w", err)
	}

	unreadByPeer := make(map[string]int)
	summaries, err := d.chatlog.ListConversations(ctx)
	if err != nil {
		return nil, fmt.Errorf("chatlog summaries: %w", err)
	}
	for _, conv := range summaries {
		if conv.UnreadCount > 0 {
			unreadByPeer[conv.PeerAddress] = conv.UnreadCount
		}
	}

	senders := make([]string, 0, len(lastEntries))
	for _, entry := range lastEntries {
		senders = append(senders, entry.Sender)
	}
	decryptContacts, err := d.fetchContactsForDecrypt(ctx, senders)
	if err != nil {
		return nil, err
	}

	out := make([]ConversationPreview, 0, len(lastEntries))
	for peerAddr, entry := range lastEntries {
		peer := domain.PeerIdentityFromWire(peerAddr)
		senderRaw := entry.Sender
		sender := domain.PeerIdentityFromWire(senderRaw)

		var senderPubKey string
		if senderRaw == d.id.Address {
			senderPubKey = identity.PublicKeyBase64(d.id.PublicKey)
		} else {
			contact, ok := decryptContacts[senderRaw]
			if !ok || contact.PubKey == "" {
				// The sidebar render is one of the §4.10 entry points:
				// without this report, recovery for a never-opened chat
				// would not start at all.
				d.noteDecryptFailure(DecryptFailureMissingSenderKey, entry.ID, entry.Sender, entry.Recipient)
				ts, _ := parseTimestamp(entry.CreatedAt)
				out = append(out, ConversationPreview{
					PeerAddress: peer,
					Sender:      sender,
					Body:        "",
					Timestamp:   ts.UTC(),
					UnreadCount: unreadByPeer[peerAddr],
				})
				continue
			}
			senderPubKey = contact.PubKey
		}

		message, decryptErr := directmsg.DecryptForIdentity(d.id, senderRaw, senderPubKey, entry.Recipient, entry.Body)
		if decryptErr != nil {
			if errors.Is(decryptErr, directmsg.ErrSealedUnreadable) {
				d.noteDecryptFailure(DecryptFailureSealedUnreadable, entry.ID, entry.Sender, entry.Recipient)
			}
			ts, _ := parseTimestamp(entry.CreatedAt)
			out = append(out, ConversationPreview{
				PeerAddress: peer,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
				UnreadCount: unreadByPeer[peerAddr],
			})
			continue
		}

		d.notePreviewDecryptSuccess(&entry, message)
		ts, _ := parseTimestamp(entry.CreatedAt)
		out = append(out, ConversationPreview{
			PeerAddress: peer,
			Sender:      sender,
			Body:        message.Body,
			Timestamp:   ts.UTC(),
			UnreadCount: unreadByPeer[peerAddr],
		})
	}

	return out, nil
}

// FetchSinglePreview loads and decrypts the last message for a single
// conversation. Returns (nil, nil) when the conversation is empty.
func (d *DMCrypto) FetchSinglePreview(ctx context.Context, peerAddress domain.PeerIdentity) (*ConversationPreview, error) {
	peerAddress = domain.PeerIdentityFromWire(strings.TrimSpace(peerAddress.String()))
	if peerAddress.IsZero() {
		return nil, fmt.Errorf("peer address is required")
	}
	if d.chatlog.Store() == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	entry, err := d.chatlog.ReadLastEntry(ctx, "dm", peerAddress)
	if err != nil {
		return nil, fmt.Errorf("chatlog read last: %w", err)
	}
	if entry == nil {
		return nil, nil
	}

	senderRaw := entry.Sender
	sender := domain.PeerIdentityFromWire(senderRaw)
	contacts, err := d.fetchContactsForDecrypt(ctx, []string{senderRaw})
	if err != nil {
		return nil, err
	}

	var senderPubKey string
	if senderRaw == d.id.Address {
		senderPubKey = identity.PublicKeyBase64(d.id.PublicKey)
	} else {
		contact, ok := contacts[senderRaw]
		if !ok || contact.PubKey == "" {
			d.noteDecryptFailure(DecryptFailureMissingSenderKey, entry.ID, entry.Sender, entry.Recipient)
			ts, _ := parseTimestamp(entry.CreatedAt)
			return &ConversationPreview{
				PeerAddress: peerAddress,
				Sender:      sender,
				Body:        "",
				Timestamp:   ts.UTC(),
			}, nil
		}
		senderPubKey = contact.PubKey
	}

	ts, _ := parseTimestamp(entry.CreatedAt)

	message, decryptErr := directmsg.DecryptForIdentity(d.id, senderRaw, senderPubKey, entry.Recipient, entry.Body)
	if decryptErr != nil {
		if errors.Is(decryptErr, directmsg.ErrSealedUnreadable) {
			d.noteDecryptFailure(DecryptFailureSealedUnreadable, entry.ID, entry.Sender, entry.Recipient)
		}
		return &ConversationPreview{
			PeerAddress: peerAddress,
			Sender:      sender,
			Body:        "",
			Timestamp:   ts.UTC(),
		}, nil
	}

	d.notePreviewDecryptSuccess(entry, message)
	return &ConversationPreview{
		PeerAddress: peerAddress,
		Sender:      sender,
		Body:        message.Body,
		Timestamp:   ts.UTC(),
	}, nil
}

// MarkConversationSeen fires delivery-seen receipts for each unseen message
// sent by counterparty. Silently skips messages that are already at seen
// status or originated from the local identity.
func (d *DMCrypto) MarkConversationSeen(ctx context.Context, counterparty domain.PeerIdentity, messages []DirectMessage) error {
	counterparty = domain.PeerIdentityFromWire(strings.TrimSpace(counterparty.String()))
	if counterparty.IsZero() {
		return nil
	}

	var firstErr error
	seenAt := time.Now().UTC().Format(time.RFC3339)

	for _, message := range messages {
		if message.Sender != counterparty || message.Recipient != domain.PeerIdentityFromWire(d.id.Address) {
			continue
		}
		if message.ReceiptStatus == protocol.ReceiptStatusSeen {
			continue
		}

		reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
			Type:        "send_delivery_receipt",
			ID:          message.ID,
			Address:     d.id.Address,
			Recipient:   counterparty.String(),
			Status:      protocol.ReceiptStatusSeen,
			DeliveredAt: seenAt,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if reply.Type != "receipt_stored" && reply.Type != "receipt_known" {
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected receipt reply: %s", reply.Type)
			}
		}
	}

	return firstErr
}

// ErrRecipientKeysUnknown is the typed "cannot encrypt yet" failure: the
// recipient's box key has not reached this node through any channel. A
// distinguishable sentinel rather than error text, so the UI can show a
// human explanation ("keys not received yet — key lookup is running") and
// the send path can gate on it instead of string-matching.
var ErrRecipientKeysUnknown = errors.New("recipient's encryption keys are not known yet — waiting for key discovery")

// ensureRecipientContact makes sure the local trust store has an entry for
// recipient before encryption. Falls back to the wider fetch_contacts set
// and triggers a one-shot import when only network contacts know the key
// material.
func (d *DMCrypto) ensureRecipientContact(ctx context.Context, recipient string) (Contact, error) {
	trustedReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts := contactsFromFrame(trustedReply)
	if contact, ok := trustedContacts[recipient]; ok && contact.BoxKey != "" {
		return contact, nil
	}

	networkReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		return Contact{}, err
	}
	networkContacts := contactsFromFrame(networkReply)
	contact, ok := networkContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, ErrRecipientKeysUnknown
	}
	if contact.PubKey == "" || contact.BoxSignature == "" {
		return Contact{}, fmt.Errorf("recipient trust data is incomplete")
	}

	importReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: recipient,
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}},
	})
	if err != nil {
		return Contact{}, err
	}
	if importReply.Type != "contacts_imported" {
		return Contact{}, fmt.Errorf("unexpected contacts import reply: %s", importReply.Type)
	}

	trustedReply, err = d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts = contactsFromFrame(trustedReply)
	contact, ok = trustedContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, ErrRecipientKeysUnknown
	}
	return contact, nil
}

// fetchContactsForDecrypt loads trusted contacts and, if any sender in the
// supplied list is missing, supplements with network contacts. Trusted
// contacts always take precedence over network ones. This helper
// deduplicates the contact-fetching pattern used by FetchConversation,
// FetchConversationPreviews, and FetchSinglePreview.
//
// The local identity address is excluded from the missing-sender check
// because all callers already handle sender==self via the identity key,
// without needing contacts. This avoids a spurious fetch_contacts roundtrip
// on every conversation that contains outgoing messages.
func (d *DMCrypto) fetchContactsForDecrypt(ctx context.Context, senders []string) (map[string]Contact, error) {
	contactsReply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return nil, err
	}
	contacts := contactsFromFrame(contactsReply)

	myAddr := d.id.Address
	needNetwork := false
	for _, sender := range senders {
		if sender == myAddr {
			continue
		}
		if _, ok := contacts[sender]; !ok {
			needNetwork = true
			break
		}
	}
	if !needNetwork {
		return contacts, nil
	}

	networkReply, networkErr := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{Type: "fetch_contacts"})
	if networkErr != nil {
		return nil, fmt.Errorf("fetch network contacts: %w", networkErr)
	}
	networkContacts := contactsFromFrame(networkReply)
	merged := make(map[string]Contact, len(contacts)+len(networkContacts))
	for addr, nc := range networkContacts {
		merged[addr] = nc
	}
	for addr, tc := range contacts {
		merged[addr] = tc // trusted wins
	}
	return merged, nil
}

// importIncomingDMHeaderContacts imports contacts for DM senders that are
// not yet in the trusted store. Header-only variant — does not require
// decrypted message bodies. Used by NodeProber.ProbeNode when a fresh DM
// header arrives for an unknown counterparty.
func (d *DMCrypto) importIncomingDMHeaderContacts(trustedContacts, networkContacts map[string]Contact, headers []DMHeader) int {
	toImport := make(map[string]protocol.ContactFrame)

	for _, h := range headers {
		if h.Recipient != domain.PeerIdentityFromWire(d.id.Address) || h.Sender == domain.PeerIdentityFromWire(d.id.Address) {
			continue
		}
		if _, ok := trustedContacts[h.Sender.String()]; ok {
			continue
		}
		contact, ok := networkContacts[h.Sender.String()]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[h.Sender.String()] = protocol.ContactFrame{
			Address: h.Sender.String(),
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}
	}

	if len(toImport) == 0 {
		return 0
	}

	contacts := make([]protocol.ContactFrame, 0, len(toImport))
	addresses := make([]string, 0, len(toImport))
	for address := range toImport {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		contacts = append(contacts, toImport[address])
	}

	reply, err := d.rpc.LocalRequestFrame(protocol.Frame{
		Type:     "import_contacts",
		Contacts: contacts,
	})
	if err != nil {
		return 0
	}
	return reply.Count
}
