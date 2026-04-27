package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

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
	to = domain.PeerIdentity(strings.TrimSpace(string(to)))
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

	if to == "" || msg.Body == "" {
		return nil, fmt.Errorf("recipient and message are required")
	}
	if !msg.ReplyTo.IsValidOrEmpty() {
		return nil, fmt.Errorf("reply_to must be a valid message ID (UUID v4)")
	}

	if msg.ReplyTo != "" && d.chatlog != nil && d.chatlog.Store() != nil {
		if !d.chatlog.Store().HasEntryInConversation(to, domain.MessageID(msg.ReplyTo)) {
			return nil, fmt.Errorf("reply_to message %q not found in conversation with %s", msg.ReplyTo, to)
		}
	}

	contact, err := d.ensureRecipientContact(ctx, string(to))
	if err != nil {
		return nil, err
	}

	recipient := domain.DMRecipient{
		Address:      to,
		BoxKeyBase64: contact.BoxKey,
	}

	ciphertext, err := directmsg.EncryptForParticipants(d.id, recipient, msg)
	if err != nil {
		return nil, err
	}

	messageID, err := protocol.NewMessageID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339)

	reply, err := d.rpc.LocalRequestFrame(protocol.Frame{
		Type:       "send_message",
		Topic:      "dm",
		ID:         string(messageID),
		Address:    d.id.Address,
		Recipient:  string(to),
		Flag:       string(protocol.MessageFlagSenderDelete),
		CreatedAt:  createdAt,
		TTLSeconds: 0,
		Body:       ciphertext,
	})
	if err != nil {
		return nil, err
	}

	if reply.Type != "message_stored" && reply.Type != "message_known" {
		return nil, fmt.Errorf("unexpected send reply: %s", reply.Type)
	}

	return &DirectMessage{
		ID:            string(messageID),
		Sender:        domain.PeerIdentity(d.id.Address),
		Recipient:     to,
		Body:          msg.Body, // plaintext — already known to us
		ReplyTo:       msg.ReplyTo,
		Command:       msg.Command,
		CommandData:   msg.CommandData,
		Timestamp:     now,
		ReceiptStatus: "sent",
	}, nil
}

// DecryptIncomingMessage turns a local-change event into a typed
// DirectMessage. Returns nil for events that are not DM-related or cannot
// be decrypted (missing sender key). Malformed reply_to references are
// silently cleared — a remote peer cannot inject dangling cross-thread
// reply links this way.
func (d *DMCrypto) DecryptIncomingMessage(event protocol.LocalChangeEvent) *DirectMessage {
	if event.Type != protocol.LocalChangeNewMessage || event.Topic != "dm" {
		return nil
	}

	senderPubKey, ok := d.resolveSenderPubKey(event.Sender)
	if !ok {
		return nil
	}

	msg, err := directmsg.DecryptForIdentity(d.id, event.Sender, senderPubKey, event.Recipient, event.Body)
	if err != nil {
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
			if !store.HasEntryInConversation(domain.PeerIdentity(peerAddress), domain.MessageID(replyTo)) {
				replyTo = ""
			}
		}
	}

	return &DirectMessage{
		ID:            event.MessageID,
		Sender:        domain.PeerIdentity(event.Sender),
		Recipient:     domain.PeerIdentity(event.Recipient),
		Body:          msg.Body,
		ReplyTo:       replyTo,
		Command:       domain.DMCommand(msg.Command),
		CommandData:   msg.CommandData,
		Timestamp:     ts,
		ReceiptStatus: status,
	}
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

			reply, err := d.rpc.LocalRequestFrame(protocol.Frame{
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
	peerAddress = domain.PeerIdentity(strings.TrimSpace(string(peerAddress)))
	if peerAddress == "" {
		return nil, fmt.Errorf("peer address is required")
	}
	store := d.chatlog.Store()
	if store == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	entries, err := d.chatlog.ReadCtx(ctx, "dm", peerAddress)
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

	messages := decryptDirectMessages(d.id, decryptContacts, records, deliveryReceipts, pendingMessages)
	sanitizeReplyReferences(messages, store, d.id.Address)
	return messages, nil
}

// FetchConversationPreviews loads the last message for each DM thread and
// returns preview data for the sidebar. Combines chatlog summaries with
// receipt / pending state from the node so unread badges survive restart.
func (d *DMCrypto) FetchConversationPreviews(ctx context.Context) ([]ConversationPreview, error) {
	if d.chatlog.Store() == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	lastEntries, err := d.chatlog.ReadLastEntryPerPeerCtx(ctx)
	if err != nil {
		return nil, fmt.Errorf("chatlog previews: %w", err)
	}

	unreadByPeer := make(map[string]int)
	summaries, err := d.chatlog.ListConversationsCtx(ctx)
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
		peer := domain.PeerIdentity(peerAddr)
		senderRaw := entry.Sender
		sender := domain.PeerIdentity(senderRaw)

		var senderPubKey string
		if senderRaw == d.id.Address {
			senderPubKey = identity.PublicKeyBase64(d.id.PublicKey)
		} else {
			contact, ok := decryptContacts[senderRaw]
			if !ok || contact.PubKey == "" {
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
	peerAddress = domain.PeerIdentity(strings.TrimSpace(string(peerAddress)))
	if peerAddress == "" {
		return nil, fmt.Errorf("peer address is required")
	}
	if d.chatlog.Store() == nil {
		return nil, fmt.Errorf("chatlog not available")
	}

	entry, err := d.chatlog.ReadLastEntryCtx(ctx, "dm", peerAddress)
	if err != nil {
		return nil, fmt.Errorf("chatlog read last: %w", err)
	}
	if entry == nil {
		return nil, nil
	}

	senderRaw := entry.Sender
	sender := domain.PeerIdentity(senderRaw)
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
		return &ConversationPreview{
			PeerAddress: peerAddress,
			Sender:      sender,
			Body:        "",
			Timestamp:   ts.UTC(),
		}, nil
	}

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
	counterparty = domain.PeerIdentity(strings.TrimSpace(string(counterparty)))
	if counterparty == "" {
		return nil
	}

	var firstErr error
	seenAt := time.Now().UTC().Format(time.RFC3339)

	for _, message := range messages {
		if message.Sender != counterparty || message.Recipient != domain.PeerIdentity(d.id.Address) {
			continue
		}
		if message.ReceiptStatus == protocol.ReceiptStatusSeen {
			continue
		}

		reply, err := d.rpc.LocalRequestFrameCtx(ctx, protocol.Frame{
			Type:        "send_delivery_receipt",
			ID:          message.ID,
			Address:     d.id.Address,
			Recipient:   string(counterparty),
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

// ensureRecipientContact makes sure the local trust store has an entry for
// recipient before encryption. Falls back to the wider fetch_contacts set
// and triggers a one-shot import when only network contacts know the key
// material.
func (d *DMCrypto) ensureRecipientContact(ctx context.Context, recipient string) (Contact, error) {
	trustedReply, err := d.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts := contactsFromFrame(trustedReply)
	if contact, ok := trustedContacts[recipient]; ok && contact.BoxKey != "" {
		return contact, nil
	}

	networkReply, err := d.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_contacts"})
	if err != nil {
		return Contact{}, err
	}
	networkContacts := contactsFromFrame(networkReply)
	contact, ok := networkContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, fmt.Errorf("recipient box key is unknown")
	}
	if contact.PubKey == "" || contact.BoxSignature == "" {
		return Contact{}, fmt.Errorf("recipient trust data is incomplete")
	}

	importReply, err := d.rpc.LocalRequestFrame(protocol.Frame{
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

	trustedReply, err = d.rpc.LocalRequestFrame(protocol.Frame{Type: "fetch_trusted_contacts"})
	if err != nil {
		return Contact{}, err
	}
	trustedContacts = contactsFromFrame(trustedReply)
	contact, ok = trustedContacts[recipient]
	if !ok || contact.BoxKey == "" {
		return Contact{}, fmt.Errorf("recipient box key is unknown")
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
		if h.Recipient != domain.PeerIdentity(d.id.Address) || h.Sender == domain.PeerIdentity(d.id.Address) {
			continue
		}
		if _, ok := trustedContacts[string(h.Sender)]; ok {
			continue
		}
		contact, ok := networkContacts[string(h.Sender)]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[string(h.Sender)] = protocol.ContactFrame{
			Address: string(h.Sender),
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
