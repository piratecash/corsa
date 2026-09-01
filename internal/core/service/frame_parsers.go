package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// Package-level frame parsers and decode helpers shared by the sub-services
// that used to live on the former DesktopClient god-type (NodeProber,
// DMCrypto, MessageStoreAdapter, ChatlogGateway).
//
// Every function in this file is pure (no receiver, no I/O) — they exist to
// translate protocol frames / chatlog entries into service-layer structs
// and vice-versa. Keeping them side-effect-free lets the unit tests drive
// them in isolation.

// contactsFromFrame decodes a fetch_contacts / fetch_trusted_contacts reply
// into a flat address → Contact map consumed by the sub-services.
func contactsFromFrame(frame protocol.Frame) map[string]Contact {
	out := make(map[string]Contact, len(frame.Contacts))
	for _, contact := range frame.Contacts {
		out[contact.Address] = Contact{
			BoxKey:       contact.BoxKey,
			PubKey:       contact.PubKey,
			BoxSignature: contact.BoxSig,
			LastOnlineAt: parseOptionalTime(contact.LastOnlineAt),
		}
	}
	return out
}

// messageRecordsFromFrames converts a batch of MessageFrame values returned
// by fetch_messages / fetch_message into typed MessageRecord values. Frames
// that fail timestamp parsing are skipped silently — the caller treats
// decode misses the same way it treats a pure I/O error (empty list).
func messageRecordsFromFrames(messages []protocol.MessageFrame) []MessageRecord {
	out := make([]MessageRecord, 0, len(messages))
	for _, message := range messages {
		record, err := messageRecordFromFrame(message)
		if err != nil {
			continue
		}
		out = append(out, record)
	}
	return out
}

// messageRecordFromFrame converts a single wire-level MessageFrame into a
// MessageRecord. The only failure mode is an unparseable timestamp — the
// caller typically treats this as "skip the record".
func messageRecordFromFrame(message protocol.MessageFrame) (MessageRecord, error) {
	timestamp, err := time.Parse(time.RFC3339, message.CreatedAt)
	if err != nil {
		return MessageRecord{}, err
	}
	return MessageRecord{
		ID:         message.ID,
		Flag:       message.Flag,
		Timestamp:  timestamp.UTC(),
		TTLSeconds: message.TTLSeconds,
		Sender:     message.Sender,
		Recipient:  message.Recipient,
		Body:       message.Body,
	}, nil
}

// receiptRecordsFromFrames decodes the Receipts slice returned by
// fetch_delivery_receipts. Frames with unparseable DeliveredAt are dropped
// because a receipt without a valid timestamp cannot be ordered against
// the lifecycle.
func receiptRecordsFromFrames(receipts []protocol.ReceiptFrame) []DeliveryReceipt {
	out := make([]DeliveryReceipt, 0, len(receipts))
	for _, receipt := range receipts {
		deliveredAt, err := time.Parse(time.RFC3339, receipt.DeliveredAt)
		if err != nil {
			continue
		}
		out = append(out, DeliveryReceipt{
			MessageID:   receipt.MessageID,
			Sender:      domain.PeerIdentityFromWire(receipt.Sender),
			Recipient:   domain.PeerIdentityFromWire(receipt.Recipient),
			Status:      receipt.Status,
			DeliveredAt: deliveredAt.UTC(),
		})
	}
	return out
}

// Delivery statuses as the UI reads them. The three the chatlog persists
// plus MessageStatusQueued, which it does not: queued is the ABSENCE of
// the on-wire stamp (see decryptDirectMessagesReporting) and lives only in
// the view.
const (
	MessageStatusQueued    = "queued"
	MessageStatusSent      = "sent"
	MessageStatusDelivered = "delivered"
	MessageStatusSeen      = "seen"
)

// receiptStatusRank returns the monotonic rank of a delivery status. Higher
// rank = further along the lifecycle. Unknown/empty values map to 0 so they
// never overwrite a known status; MessageStatusQueued is deliberately one
// of them, because a queued message has not reached the wire and every
// other status is news.
func receiptStatusRank(status string) int {
	switch status {
	case MessageStatusSent:
		return 1
	case MessageStatusDelivered:
		return 2
	case MessageStatusSeen:
		return 3
	default:
		return 0
	}
}

// peerHealthFromFrame translates the wire-level PeerHealth slice into the
// service layer's typed representation. All timestamp fields are parsed
// through parseOptionalTime so an empty / unparseable value yields an
// invalid OptionalTime rather than a zero time.Time that would masquerade
// as a real timestamp.
func peerHealthFromFrame(frame protocol.Frame) []PeerHealth {
	items := make([]PeerHealth, 0, len(frame.PeerHealth))
	for _, item := range frame.PeerHealth {
		items = append(items, PeerHealth{
			Address:             item.Address,
			PeerID:              item.PeerID,
			ConnID:              item.ConnID,
			Direction:           item.Direction,
			ClientVersion:       item.ClientVersion,
			ClientBuild:         item.ClientBuild,
			ProtocolVersion:     item.ProtocolVersion,
			State:               item.State,
			Connected:           item.Connected,
			PendingCount:        item.PendingCount,
			LastConnectedAt:     parseOptionalTime(item.LastConnectedAt),
			LastDisconnectedAt:  parseOptionalTime(item.LastDisconnectedAt),
			LastPingAt:          parseOptionalTime(item.LastPingAt),
			LastPongAt:          parseOptionalTime(item.LastPongAt),
			LastUsefulSendAt:    parseOptionalTime(item.LastUsefulSendAt),
			LastUsefulReceiveAt: parseOptionalTime(item.LastUsefulReceiveAt),
			ConsecutiveFailures: item.ConsecutiveFailures,
			LastError:           item.LastError,
			Score:               item.Score,
			BannedUntil:         parseOptionalTime(item.BannedUntil),
			BytesSent:           item.BytesSent,
			BytesReceived:       item.BytesReceived,
			TotalTraffic:        item.TotalTraffic,
			SlotState:           item.SlotState,
			SlotRetryCount:      item.SlotRetryCount,
			SlotGeneration:      item.SlotGeneration,
			SlotConnectedAddr:   item.SlotConnectedAddr,

			LastErrorCode:               item.LastErrorCode,
			LastDisconnectCode:          item.LastDisconnectCode,
			IncompatibleVersionAttempts: item.IncompatibleVersionAttempts,
			LastIncompatibleVersionAt:   parseOptionalTime(item.LastIncompatibleVersionAt),
			ObservedPeerVersion:         item.ObservedPeerVersion,
			ObservedPeerMinimumVersion:  item.ObservedPeerMinimumVersion,
			VersionLockoutActive:        item.VersionLockoutActive,
		})
	}
	return items
}

// captureSessionsFromFrame extracts the subset of the fetch_peer_health
// response that describes active traffic-capture sessions. The node still
// carries Recording* fields per-connection on PeerHealthFrame for wire
// compatibility, so the probe-time merge uses the single RPC: only rows
// whose Recording flag is true produce a CaptureSession entry.
//
// Stopped sessions are not part of the probe snapshot — the probe only
// reports in-flight state. The capture ticker is responsible for surfacing
// terminal CaptureSession entries via TopicCaptureSessionStopped.
func captureSessionsFromFrame(frame protocol.Frame) map[domain.ConnID]CaptureSession {
	sessions := make(map[domain.ConnID]CaptureSession)
	for _, item := range frame.PeerHealth {
		if !item.Recording {
			continue
		}
		connID := domain.ConnID(item.ConnID)
		scope, _ := parseCaptureScope(item.RecordingScope)
		sessions[connID] = CaptureSession{
			ConnID:    connID,
			Address:   domain.PeerAddress(item.Address),
			PeerID:    domain.PeerIdentityFromWire(item.PeerID),
			Direction: domain.PeerDirection(item.Direction),
			FilePath:  item.RecordingFile,
			StartedAt: parseOptionalTime(item.RecordingStartedAt),
			Scope:     scope,
			// Format is not carried on the wire — the probe snapshot only
			// surfaces recordings that started before the monitor subscribed,
			// and CaptureFormatCompact is the default for all new sessions.
			// When the capture ticker fires again it will publish the real
			// format via TopicCaptureSessionStarted, which overwrites this.
			Format:        domain.CaptureFormatCompact,
			Active:        true,
			Error:         item.RecordingError,
			DroppedEvents: item.RecordingDroppedEvents,
		}
	}
	if len(sessions) == 0 {
		return nil
	}
	return sessions
}

// parseCaptureScope maps a wire scope string to its domain counterpart.
// Unknown values fall back to CaptureScopeConnID so the UI still labels
// the session rather than rendering an empty string.
func parseCaptureScope(s string) (domain.CaptureScope, bool) {
	scope := domain.CaptureScope(s)
	if scope.IsValid() {
		return scope, true
	}
	return domain.CaptureScopeConnID, false
}

// aggregateStatusFromFrame converts the protocol frame returned by
// fetch_aggregate_status into the service-layer AggregateStatus struct.
// Returns nil when the frame carries no aggregate status data (e.g. the
// node version does not support the command yet).
func aggregateStatusFromFrame(frame protocol.Frame) *AggregateStatus {
	if frame.AggregateStatus == nil {
		return nil
	}
	return &AggregateStatus{
		Status:          frame.AggregateStatus.Status,
		UsablePeers:     frame.AggregateStatus.UsablePeers,
		ConnectedPeers:  frame.AggregateStatus.ConnectedPeers,
		TotalPeers:      frame.AggregateStatus.TotalPeers,
		PendingMessages: frame.AggregateStatus.PendingMessages,

		UpdateAvailable:              frame.AggregateStatus.UpdateAvailable,
		UpdateReason:                 frame.AggregateStatus.UpdateReason,
		IncompatibleVersionReporters: frame.AggregateStatus.IncompatibleVersionReporters,
		MaxObservedPeerBuild:         frame.AggregateStatus.MaxObservedPeerBuild,
		MaxObservedPeerVersion:       frame.AggregateStatus.MaxObservedPeerVersion,
	}
}

// resourceUsageFromFrame maps a fetch_resource_usage reply onto the
// service-layer ResourceUsage view. Returns nil when the frame carries
// no payload (older node that does not support the command yet), so
// the Info tab degrades gracefully to omitting the rows.
func resourceUsageFromFrame(frame protocol.Frame) *ResourceUsage {
	if frame.ResourceUsage == nil {
		return nil
	}
	f := frame.ResourceUsage
	return &ResourceUsage{
		MemSysBytes:         f.MemSysBytes,
		MemSysHuman:         f.MemSysHuman,
		MemHeapAllocBytes:   f.MemHeapAllocBytes,
		MemHeapAllocHuman:   f.MemHeapAllocHuman,
		HeapInuseBytes:      f.HeapInuseBytes,
		HeapInuseHuman:      f.HeapInuseHuman,
		HeapIdleBytes:       f.HeapIdleBytes,
		HeapIdleHuman:       f.HeapIdleHuman,
		HeapReleasedBytes:   f.HeapReleasedBytes,
		HeapReleasedHuman:   f.HeapReleasedHuman,
		GCSysBytes:          f.GCSysBytes,
		GCSysHuman:          f.GCSysHuman,
		CgroupMemLimitBytes: f.CgroupMemLimitBytes,
		CgroupMemLimitHuman: f.CgroupMemLimitHuman,
		CgroupMemUsageBytes: f.CgroupMemUsageBytes,
		CgroupMemUsageHuman: f.CgroupMemUsageHuman,
		ConnectionCount:     f.ConnectionCount,
		UptimeSeconds:       f.UptimeSeconds,
		UptimeHuman:         f.UptimeHuman,
		SampledAt:           f.SampledAt,
	}
}

// resolveAggregateStatus interprets the fetch_aggregate_status RPC result
// and classifies it into one of four outcomes:
//
//  1. Normal success         → non-nil *AggregateStatus, empty warning.
//  2. unknown-command (legacy)→ nil *AggregateStatus, empty warning (expected fallback).
//  3. Unexpected RPC error   → nil *AggregateStatus, warning describing the failure.
//  4. Success but no payload → nil *AggregateStatus, warning about malformed reply.
//
// The function is intentionally side-effect-free so that the branching
// logic can be tested without a live node.
func resolveAggregateStatus(reply protocol.Frame, err error) (*AggregateStatus, string) {
	if err != nil {
		if errors.Is(err, protocol.ErrUnknownCommand) {
			// Case 2: older node — expected, silent fallback.
			return nil, ""
		}
		// Case 3: unexpected RPC error.
		return nil, fmt.Sprintf("fetch_aggregate_status failed unexpectedly: %v", err)
	}

	result := aggregateStatusFromFrame(reply)
	if result == nil {
		// Case 4: command succeeded but frame has no AggregateStatus payload.
		return nil, fmt.Sprintf("fetch_aggregate_status returned success but frame (type=%q) lacks AggregateStatus payload", reply.Type)
	}

	// Case 1: normal success.
	return result, ""
}

// parseOptionalTime parses an RFC3339 timestamp from the wire, returning
// an invalid OptionalTime when the value is empty or unparseable. Used at
// the wire→service boundary where the protocol carries timestamps as
// strings.
func parseOptionalTime(value string) domain.OptionalTime {
	value = strings.TrimSpace(value)
	if value == "" {
		return domain.OptionalTime{}
	}
	ts, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return domain.OptionalTime{}
	}
	return domain.TimeOf(ts)
}

// parseTimestamp tries RFC3339Nano first and falls back to RFC3339 for
// legacy / migrated records that were stored with second precision.
func parseTimestamp(s string) (time.Time, error) {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		ts, err = time.Parse(time.RFC3339, s)
	}
	return ts, err
}

// dmHeadersFromFrame decodes the fetch_dm_headers reply into DMHeader
// values. Entries with unparseable timestamps are skipped — a header
// without a timestamp cannot be ordered in the sidebar.
func dmHeadersFromFrame(frame protocol.Frame) []DMHeader {
	out := make([]DMHeader, 0, len(frame.DMHeaders))
	for _, h := range frame.DMHeaders {
		ts, err := parseTimestamp(h.CreatedAt)
		if err != nil {
			continue
		}
		out = append(out, DMHeader{
			ID:        h.ID,
			Sender:    domain.PeerIdentityFromWire(h.Sender),
			Recipient: domain.PeerIdentityFromWire(h.Recipient),
			Timestamp: ts.UTC(),
		})
	}
	return out
}

// missingDMHeaderContacts scans a batch of DM headers and returns the
// sender/recipient addresses that are not already in the trusted contacts
// map. Used by NodeProber to decide whether a follow-up fetch_contacts
// RPC is needed (the import is delegated to DMCrypto).
func missingDMHeaderContacts(self string, contacts map[string]Contact, headers []DMHeader) []string {
	missing := make(map[string]struct{})
	for _, h := range headers {
		for _, address := range []string{h.Sender.String(), h.Recipient.String()} {
			address = strings.TrimSpace(address)
			if address == "" || address == "*" || address == self {
				continue
			}
			if _, ok := contacts[address]; ok {
				continue
			}
			missing[address] = struct{}{}
		}
	}

	out := make([]string, 0, len(missing))
	for address := range missing {
		out = append(out, address)
	}
	return out
}

// pendingMessagesFromFrame decodes fetch_pending_messages into typed
// PendingMessage records. Used by DMCrypto.FetchConversation to layer
// per-message pending state onto the decrypted thread.
func pendingMessagesFromFrame(frame protocol.Frame) []PendingMessage {
	out := make([]PendingMessage, 0, len(frame.PendingMessages))
	for _, item := range frame.PendingMessages {
		queuedAt := parseOptionalTime(item.QueuedAt)
		lastAttemptAt := parseOptionalTime(item.LastAttemptAt)
		out = append(out, PendingMessage{
			ID:            item.ID,
			Recipient:     item.Recipient,
			Status:        item.Status,
			QueuedAt:      queuedAt,
			LastAttemptAt: lastAttemptAt,
			Retries:       item.Retries,
			Error:         item.Error,
		})
	}
	return out
}

// isConversationMessage reports whether message belongs to the bidirectional
// thread between self and counterparty. Used during on-demand peer sync to
// filter out unrelated messages that happen to live on the remote node.
func isConversationMessage(message protocol.MessageFrame, self, counterparty string) bool {
	return (message.Sender == self && message.Recipient == counterparty) ||
		(message.Sender == counterparty && message.Recipient == self)
}

// incomingContactsToTrust inspects a batch of already-decrypted incoming
// DirectMessages and returns the subset of their senders that should be
// promoted into the trust store. Senders already trusted, or whose network
// contact lacks the full key triple, are filtered out. The result is
// sorted by address for deterministic import order.
func incomingContactsToTrust(self string, trustedContacts, decryptContacts map[string]Contact, messages []DirectMessage) []protocol.ContactFrame {
	toImport := make(map[string]protocol.ContactFrame)

	for _, message := range messages {
		if message.Recipient != domain.PeerIdentityFromWire(self) || message.Sender == domain.PeerIdentityFromWire(self) {
			continue
		}
		if _, ok := trustedContacts[message.Sender.String()]; ok {
			continue
		}
		contact, ok := decryptContacts[message.Sender.String()]
		if !ok || contact.BoxKey == "" || contact.PubKey == "" || contact.BoxSignature == "" {
			continue
		}
		toImport[message.Sender.String()] = protocol.ContactFrame{
			Address: message.Sender.String(),
			PubKey:  contact.PubKey,
			BoxKey:  contact.BoxKey,
			BoxSig:  contact.BoxSignature,
		}
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
	return contacts
}

// decryptDirectMessages turns a batch of persisted MessageRecords into the
// decrypted DirectMessage shape the UI consumes, layering delivery-receipt
// and pending state on top of each thread entry.
//
// The flow per record is:
//
//  1. Resolve sender public key (self key for outgoing, contacts map for
//     inbound). Skip records whose sender key is unknown.
//  2. Decrypt the ciphertext; skip records that fail decryption (stale or
//     forged payloads).
//  3. Merge the in-memory receipt map (per MessageID) with the persisted
//     status so restart survives: the persisted "delivered" or "seen"
//     state is never downgraded to empty.
//  4. Synthesize DeliveredAt from the message timestamp for
//     delivered/seen states so UI checkmarks survive restart — the
//     in-memory receipt map is empty after a cold boot.
//  5. Clear any ReplyTo that fails UUID validation. Full cross-thread
//     validation is the caller's job (sanitizeReplyReferences).
func decryptDirectMessages(id *identity.Identity, contacts map[string]Contact, messages []MessageRecord, receipts []DeliveryReceipt, pendingItems []PendingMessage) []DirectMessage {
	return decryptDirectMessagesReporting(id, contacts, messages, receipts, pendingItems, nil, nil)
}

// decryptDirectMessagesReporting is the batch decrypt with the §4.10
// failure hook: history loads are one of the three entry points that must
// start recovery for rows they cannot open.
func decryptDirectMessagesReporting(id *identity.Identity, contacts map[string]Contact, messages []MessageRecord, receipts []DeliveryReceipt, pendingItems []PendingMessage, report func(DecryptFailure), reportSuccess func(*DirectMessage)) []DirectMessage {
	note := func(class DecryptFailureClass, messageID, sender, recipient string) {
		if report == nil || messageID == "" || recipient != id.Address || sender == id.Address {
			return
		}
		report(DecryptFailure{MessageID: messageID, Sender: sender, Recipient: recipient, Class: class})
	}
	noteSuccess := func(msg *DirectMessage) {
		// The same incoming-only entry condition as the failure side: the
		// §4.10 established fact and the retry_of acceptance must fire on
		// a HISTORY load too — a replacement read after a restart, or in a
		// background chat, closes recovery exactly like a live one.
		if reportSuccess == nil || msg.Recipient.String() != id.Address || msg.Sender.String() == id.Address {
			return
		}
		reportSuccess(msg)
	}
	receiptsByMessageID := make(map[string]DeliveryReceipt, len(receipts))
	for _, receipt := range receipts {
		existing, ok := receiptsByMessageID[receipt.MessageID]
		if !ok || receipt.Status == protocol.ReceiptStatusSeen || existing.Status != protocol.ReceiptStatusSeen {
			receiptsByMessageID[receipt.MessageID] = receipt
		}
	}
	pending := make(map[string]PendingMessage, len(pendingItems))
	for _, item := range pendingItems {
		pending[item.ID] = item
	}

	out := make([]DirectMessage, 0, len(messages))
	for _, item := range messages {
		sender := item.Sender
		recipient := item.Recipient
		ciphertext := item.Body

		// Resolve sender key. For outgoing messages (sender == self) use
		// the identity key directly instead of relying on the contacts
		// map, which typically does not contain the node's own address.
		// Mirrors DMCrypto.DecryptIncomingMessage.
		var senderPubKey string
		if sender == id.Address {
			senderPubKey = identity.PublicKeyBase64(id.PublicKey)
		} else {
			contact, ok := contacts[sender]
			if !ok || contact.PubKey == "" {
				note(DecryptFailureMissingSenderKey, item.ID, sender, recipient)
				continue
			}
			senderPubKey = contact.PubKey
		}

		message, err := directmsg.DecryptForIdentity(id, sender, senderPubKey, recipient, ciphertext)
		if err != nil {
			if errors.Is(err, directmsg.ErrSealedUnreadable) {
				note(DecryptFailureSealedUnreadable, item.ID, sender, recipient)
			}
			continue
		}

		var deliveredAt domain.OptionalTime

		// Start from the persisted status in SQLite (survives restart).
		receiptStatus := item.PersistedStatus

		// Layer in-memory receipt on top — but only if it advances the status.
		fromReceipt := false
		if receipt, ok := receiptsByMessageID[item.ID]; ok {
			deliveredAt = domain.TimeOf(receipt.DeliveredAt)
			fromReceipt = true
			if receiptStatusRank(receipt.Status) > receiptStatusRank(receiptStatus) {
				receiptStatus = receipt.Status
			}
		}

		// Synthesize DeliveredAt for persisted statuses that survive restart.
		// After a restart the in-memory receipt map is empty, so deliveredAt
		// would be invalid even though SQLite has a valid "delivered" or
		// "seen" status. Use the message timestamp as a reasonable
		// approximation so the UI can render status badges (checkmarks).
		if !deliveredAt.Valid() && (receiptStatus == "delivered" || receiptStatus == "seen") {
			deliveredAt = domain.TimeOf(item.Timestamp)
		}

		// For outgoing messages with no persisted or receipt status, check pending state.
		if receiptStatus == "" && item.Sender == id.Address {
			if pendingItem, ok := pending[item.ID]; ok {
				receiptStatus = pendingItem.Status
			} else {
				receiptStatus = MessageStatusSent
			}
		}

		// A message no sink has confirmed reads as QUEUED, not sent. The
		// chatlog cannot make this distinction — "sent" is the only status
		// it has below delivered, and it is the truthful one for the wire
		// — so the answer comes from the on-wire stamp instead. Applied
		// last and only over "sent": any receipt at all outranks it,
		// because a message the recipient has answered for plainly went
		// out.
		//
		// Read off THIS bit and not off the never-emitted claim. That
		// claim is withdrawn before the first frame is handed to a writer,
		// so it says "sent" from the moment an attempt begins — and an
		// attempt every writer then refused left the sender looking at a
		// message nothing had carried, which is the original bug.
		//
		// The sender check is EXPLICIT. The old claim was only ever true
		// for a row this node authored, so the domain guard came free with
		// the data; an incoming row is governed by the bit too (it simply
		// never gets a stamp), so without this check every message the
		// user received would read as queued.
		if receiptStatus == MessageStatusSent && item.AwaitingWire && item.Sender == id.Address {
			receiptStatus = MessageStatusQueued
		}

		replyTo := domain.MessageID(message.ReplyTo)
		if replyTo != "" && !replyTo.IsValid() {
			replyTo = ""
		}

		out = append(out, DirectMessage{
			ID:            item.ID,
			Sender:        domain.PeerIdentityFromWire(sender),
			Recipient:     domain.PeerIdentityFromWire(recipient),
			Body:          message.Body,
			ReplyTo:       replyTo,
			RetryOf:       sanitizedRetryOf(message.RetryOf),
			Command:       domain.DMCommand(message.Command),
			CommandData:   message.CommandData,
			Timestamp:     item.Timestamp,
			ReceiptStatus: receiptStatus,
			DeliveredAt:   deliveredAt,

			DeliveredAtFromReceipt: fromReceipt,
			Seq:                    item.Seq,
		})
		noteSuccess(&out[len(out)-1])
	}

	return out
}

// sanitizeReplyReferences drops ReplyTo links that point outside the
// conversation they appear in. Requires the persistent chatlog store —
// malformed UUIDs are already cleared during decryption.
//
// Without this check, a remote peer could inject cross-thread reply links
// by re-encrypting a foreign message into a fresh envelope that claims a
// ReplyTo outside the current thread.
func sanitizeReplyReferences(ctx context.Context, messages []DirectMessage, store *chatlog.Store, selfAddress string) error {
	if store == nil {
		return nil
	}
	for i := range messages {
		if messages[i].ReplyTo == "" {
			continue
		}
		peerAddr := messages[i].Sender
		if peerAddr == domain.PeerIdentityFromWire(selfAddress) {
			peerAddr = messages[i].Recipient
		}
		// A lookup that FAILED says nothing about the reference. The
		// bool-only form returned false for a cancelled context and for an
		// unhealthy database exactly as it did for a genuine miss, so a valid
		// quote was stripped from history and the caller was handed the
		// edited messages as a successful read.
		found, err := store.LookupEntryInConversation(ctx, peerAddr, domain.MessageID(messages[i].ReplyTo))
		if err != nil {
			return fmt.Errorf("reply reference lookup for message %s: %w", messages[i].ID, err)
		}
		if !found {
			messages[i].ReplyTo = ""
		}
	}
	return nil
}
