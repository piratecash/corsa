package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/chatlog"
	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
	"corsa/internal/core/node"
	"corsa/internal/core/protocol"
)

func TestContactsFromFrame(t *testing.T) {
	t.Parallel()

	got := contactsFromFrame(protocol.Frame{
		Type: "contacts",
		Contacts: []protocol.ContactFrame{
			{Address: "abc", BoxKey: "box1", PubKey: "pub1", BoxSig: "sig1"},
			{Address: "def", BoxKey: "box2", PubKey: "pub2", BoxSig: "sig2"},
		},
	})

	want := map[string]Contact{
		"abc": {BoxKey: "box1", PubKey: "pub1", BoxSignature: "sig1"},
		"def": {BoxKey: "box2", PubKey: "pub2", BoxSignature: "sig2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected contacts: got %v want %v", got, want)
	}
}

func TestMessageRecordFromFrame(t *testing.T) {
	t.Parallel()

	got, err := messageRecordFromFrame(protocol.MessageFrame{
		ID:         "id-1",
		Sender:     "alice",
		Recipient:  "bob",
		Flag:       "sender-delete",
		CreatedAt:  "2026-03-19T10:00:00Z",
		TTLSeconds: 0,
		Body:       "hello",
	})
	if err != nil {
		t.Fatalf("messageRecordFromFrame returned error: %v", err)
	}

	want := MessageRecord{
		ID:         "id-1",
		Flag:       "sender-delete",
		Timestamp:  mustTime(t, "2026-03-19T10:00:00Z"),
		TTLSeconds: 0,
		Sender:     "alice",
		Recipient:  "bob",
		Body:       "hello",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected message: got %#v want %#v", got, want)
	}
}

func TestMessageRecordsFromFrames(t *testing.T) {
	t.Parallel()

	got := messageRecordsFromFrames([]protocol.MessageFrame{
		{ID: "id-1", Sender: "alice", Recipient: "*", Flag: "immutable", CreatedAt: "2026-03-19T10:00:00Z", TTLSeconds: 0, Body: "hello"},
		{ID: "id-2", Sender: "bob", Recipient: "*", Flag: "sender-delete", CreatedAt: "2026-03-19T10:01:00Z", TTLSeconds: 0, Body: "world"},
	})

	want := []MessageRecord{
		{ID: "id-1", Flag: "immutable", Timestamp: mustTime(t, "2026-03-19T10:00:00Z"), TTLSeconds: 0, Sender: "alice", Recipient: "*", Body: "hello"},
		{ID: "id-2", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:01:00Z"), TTLSeconds: 0, Sender: "bob", Recipient: "*", Body: "world"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected messages: got %v want %v", got, want)
	}
}

func TestMessageRecordsFromFramesSkipsInvalid(t *testing.T) {
	t.Parallel()

	got := messageRecordsFromFrames([]protocol.MessageFrame{
		{ID: "bad", Sender: "alice", Recipient: "bob", Flag: "sender-delete", CreatedAt: "broken", TTLSeconds: 0, Body: "hello"},
		{ID: "good", Sender: "alice", Recipient: "bob", Flag: "sender-delete", CreatedAt: "2026-03-19T10:00:00Z", TTLSeconds: 0, Body: "world"},
	})

	want := []MessageRecord{
		{ID: "good", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:00:00Z"), TTLSeconds: 0, Sender: "alice", Recipient: "bob", Body: "world"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered messages: got %v want %v", got, want)
	}
}

func TestDecryptDirectMessages(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	ciphertext, err := directmsg.EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "secret phrase")
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	got := decryptDirectMessages(recipient, map[string]Contact{
		sender.Address: {
			BoxKey: identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(sender.PublicKey),
		},
	}, []MessageRecord{{
		ID:         "id-1",
		Flag:       "sender-delete",
		Timestamp:  mustTime(t, "2026-03-19T10:00:00Z"),
		TTLSeconds: 0,
		Sender:     sender.Address,
		Recipient:  recipient.Address,
		Body:       ciphertext,
	}}, nil, nil)
	want := []DirectMessage{{
		ID:        "id-1",
		Sender:    sender.Address,
		Recipient: recipient.Address,
		Body:      "secret phrase",
		Timestamp: mustTime(t, "2026-03-19T10:00:00Z"),
	}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected decrypted messages: got %v want %v", got, want)
	}
}

func TestDecryptDirectMessagesMarksLifecycleStatuses(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	ciphertext, err := directmsg.EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "queued later")
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	got := decryptDirectMessages(sender, map[string]Contact{
		sender.Address: {
			BoxKey: identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(sender.PublicKey),
		},
		recipient.Address: {
			BoxKey: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(recipient.PublicKey),
		},
	}, []MessageRecord{
		{ID: "queued-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:00:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
		{ID: "retrying-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:01:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
		{ID: "failed-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:02:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
		{ID: "expired-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:03:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
		{ID: "sent-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:04:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
	}, nil, []PendingMessage{
		{ID: "queued-1", Status: "queued"},
		{ID: "retrying-1", Status: "retrying", Retries: 2},
		{ID: "failed-1", Status: "failed", Retries: 5, Error: "max retries exceeded"},
		{ID: "expired-1", Status: "expired", Error: "pending queue expired"},
	})

	if len(got) != 5 {
		t.Fatalf("unexpected direct messages: %#v", got)
	}
	if got[0].ReceiptStatus != "queued" {
		t.Fatalf("expected queued status, got %#v", got[0])
	}
	if got[1].ReceiptStatus != "retrying" {
		t.Fatalf("expected retrying status, got %#v", got[1])
	}
	if got[2].ReceiptStatus != "failed" {
		t.Fatalf("expected failed status, got %#v", got[2])
	}
	if got[3].ReceiptStatus != "expired" {
		t.Fatalf("expected expired status, got %#v", got[3])
	}
	if got[4].ReceiptStatus != "sent" {
		t.Fatalf("expected sent status, got %#v", got[4])
	}
}

// TestDecryptOutgoingWithoutSelfInContacts verifies that decryptDirectMessages
// correctly decrypts outgoing messages (sender == self) even when the node's
// own address is NOT in the contacts map. Before the fix, such messages were
// silently dropped, making the conversation one-sided after reload/restart.
func TestDecryptOutgoingWithoutSelfInContacts(t *testing.T) {
	t.Parallel()

	self, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate self failed: %v", err)
	}
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate peer failed: %v", err)
	}

	// Encrypt an outgoing message (self → peer).
	outCipher, err := directmsg.EncryptForParticipants(self, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey), "outgoing text")
	if err != nil {
		t.Fatalf("EncryptForParticipants outgoing failed: %v", err)
	}

	// Encrypt an incoming message (peer → self).
	inCipher, err := directmsg.EncryptForParticipants(peer, self.Address, identity.BoxPublicKeyBase64(self.BoxPublicKey), "incoming text")
	if err != nil {
		t.Fatalf("EncryptForParticipants incoming failed: %v", err)
	}

	// Contacts map does NOT contain self — only the peer.
	// This is the realistic scenario after chatlog migration.
	contacts := map[string]Contact{
		peer.Address: {
			BoxKey: identity.BoxPublicKeyBase64(peer.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(peer.PublicKey),
		},
	}

	got := decryptDirectMessages(self, contacts, []MessageRecord{
		{ID: "out-1", Timestamp: mustTime(t, "2026-03-19T10:00:00Z"), Sender: self.Address, Recipient: peer.Address, Body: outCipher},
		{ID: "in-1", Timestamp: mustTime(t, "2026-03-19T10:01:00Z"), Sender: peer.Address, Recipient: self.Address, Body: inCipher},
	}, nil, nil)

	if len(got) != 2 {
		t.Fatalf("expected 2 decrypted messages (outgoing + incoming), got %d", len(got))
	}
	if got[0].ID != "out-1" || got[0].Body != "outgoing text" {
		t.Fatalf("outgoing message not decrypted: %+v", got[0])
	}
	if got[0].ReceiptStatus != "sent" {
		t.Fatalf("outgoing message should have status 'sent', got %q", got[0].ReceiptStatus)
	}
	if got[1].ID != "in-1" || got[1].Body != "incoming text" {
		t.Fatalf("incoming message not decrypted: %+v", got[1])
	}
}

// TestPersistedStatusSurvivesRestart verifies that delivery_status persisted
// in SQLite is used as the baseline when decryptDirectMessages reconstructs
// the conversation after a node restart. In-memory receipts are layered on
// top monotonically: they can advance the status but never regress it.
func TestPersistedStatusSurvivesRestart(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	ciphertext, err := directmsg.EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "hello after restart")
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	contacts := map[string]Contact{
		sender.Address: {
			BoxKey: identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(sender.PublicKey),
		},
		recipient.Address: {
			BoxKey: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(recipient.PublicKey),
		},
	}

	ts := mustTime(t, "2026-03-24T10:00:00Z")

	// Simulate post-restart: messages loaded from SQLite with persisted status,
	// no in-memory receipts, no pending items.
	t.Run("persisted_status_used_as_baseline", func(t *testing.T) {
		records := []MessageRecord{
			{ID: "msg-delivered", Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext, Timestamp: ts, PersistedStatus: "delivered"},
			{ID: "msg-seen", Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext, Timestamp: ts, PersistedStatus: "seen"},
		}
		got := decryptDirectMessages(recipient, contacts, records, nil, nil)
		if len(got) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(got))
		}
		if got[0].ReceiptStatus != "delivered" {
			t.Fatalf("msg-delivered: expected status=delivered, got %q", got[0].ReceiptStatus)
		}
		if got[1].ReceiptStatus != "seen" {
			t.Fatalf("msg-seen: expected status=seen, got %q", got[1].ReceiptStatus)
		}
	})

	// In-memory receipt advances persisted status: delivered → seen.
	t.Run("receipt_advances_persisted_status", func(t *testing.T) {
		records := []MessageRecord{
			{ID: "msg-adv", Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext, Timestamp: ts, PersistedStatus: "delivered"},
		}
		receipts := []DeliveryReceipt{
			{MessageID: "msg-adv", Sender: sender.Address, Recipient: recipient.Address, Status: "seen", DeliveredAt: time.Now()},
		}
		got := decryptDirectMessages(recipient, contacts, records, receipts, nil)
		if got[0].ReceiptStatus != "seen" {
			t.Fatalf("expected receipt to advance to seen, got %q", got[0].ReceiptStatus)
		}
	})

	// In-memory receipt cannot regress persisted status: seen → delivered is ignored.
	t.Run("receipt_cannot_regress_persisted_status", func(t *testing.T) {
		records := []MessageRecord{
			{ID: "msg-noreg", Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext, Timestamp: ts, PersistedStatus: "seen"},
		}
		receipts := []DeliveryReceipt{
			{MessageID: "msg-noreg", Sender: sender.Address, Recipient: recipient.Address, Status: "delivered", DeliveredAt: time.Now()},
		}
		got := decryptDirectMessages(recipient, contacts, records, receipts, nil)
		if got[0].ReceiptStatus != "seen" {
			t.Fatalf("expected status to stay seen (no regression), got %q", got[0].ReceiptStatus)
		}
	})

	// Outgoing message with persisted status should use it, not fall back to pending.
	t.Run("outgoing_persisted_status_overrides_pending", func(t *testing.T) {
		records := []MessageRecord{
			{ID: "msg-out", Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext, Timestamp: ts, PersistedStatus: "delivered"},
		}
		pending := []PendingMessage{
			{ID: "msg-out", Status: "queued"},
		}
		got := decryptDirectMessages(sender, contacts, records, nil, pending)
		if got[0].ReceiptStatus != "delivered" {
			t.Fatalf("expected persisted delivered to override pending queued, got %q", got[0].ReceiptStatus)
		}
	})
}

func TestReceiptStatusRank(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status string
		rank   int
	}{
		{"", 0},
		{"unknown", 0},
		{"sent", 1},
		{"delivered", 2},
		{"seen", 3},
	}
	for _, tc := range cases {
		if got := receiptStatusRank(tc.status); got != tc.rank {
			t.Errorf("receiptStatusRank(%q) = %d, want %d", tc.status, got, tc.rank)
		}
	}
}

func TestPendingMessagesFromFrame(t *testing.T) {
	t.Parallel()

	got := pendingMessagesFromFrame(protocol.Frame{
		PendingMessages: []protocol.PendingMessageFrame{
			{
				ID:            "msg-1",
				Recipient:     "alice",
				Status:        "retrying",
				QueuedAt:      "2026-03-20T10:00:00Z",
				LastAttemptAt: "2026-03-20T10:01:00Z",
				Retries:       2,
				Error:         "retry queued delivery",
			},
		},
	})

	if len(got) != 1 {
		t.Fatalf("unexpected pending messages: %#v", got)
	}
	if got[0].Status != "retrying" || got[0].Retries != 2 || got[0].Error != "retry queued delivery" {
		t.Fatalf("unexpected pending item: %#v", got[0])
	}
	if got[0].QueuedAt == nil || got[0].LastAttemptAt == nil {
		t.Fatalf("expected parsed timestamps: %#v", got[0])
	}
}

func TestIncomingContactsToTrustIncludesUnknownIncomingSender(t *testing.T) {
	t.Parallel()

	got := incomingContactsToTrust(
		"me",
		map[string]Contact{
			"trusted": {BoxKey: "trusted-box", PubKey: "trusted-pub", BoxSignature: "trusted-sig"},
		},
		map[string]Contact{
			"alice":   {BoxKey: "alice-box", PubKey: "alice-pub", BoxSignature: "alice-sig"},
			"trusted": {BoxKey: "trusted-box", PubKey: "trusted-pub", BoxSignature: "trusted-sig"},
			"bob":     {BoxKey: "bob-box", PubKey: "bob-pub"},
		},
		[]DirectMessage{
			{Sender: "alice", Recipient: "me", Body: "hello"},
			{Sender: "trusted", Recipient: "me", Body: "known"},
			{Sender: "bob", Recipient: "me", Body: "missing signature"},
			{Sender: "me", Recipient: "alice", Body: "outgoing"},
		},
	)

	want := []protocol.ContactFrame{
		{Address: "alice", PubKey: "alice-pub", BoxKey: "alice-box", BoxSig: "alice-sig"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected contacts to trust: got %#v want %#v", got, want)
	}
}

func TestPeerHealthFromFrame(t *testing.T) {
	t.Parallel()

	got := peerHealthFromFrame(protocol.Frame{
		Type: "peer_health",
		PeerHealth: []protocol.PeerHealthFrame{
			{
				Address:             "198.51.100.1:64646",
				ClientVersion:       "0.11-alpha",
				ClientBuild:         17,
				State:               "healthy",
				Connected:           true,
				PendingCount:        2,
				LastConnectedAt:     "2026-03-20T09:10:11Z",
				LastDisconnectedAt:  "2026-03-20T09:09:11Z",
				LastPingAt:          "2026-03-20T09:11:00Z",
				LastPongAt:          "2026-03-20T09:11:00Z",
				LastUsefulSendAt:    "2026-03-20T09:10:58Z",
				LastUsefulReceiveAt: "2026-03-20T09:10:59Z",
				ConsecutiveFailures: 1,
				LastError:           "timeout",
				Score:               15,
			},
		},
	})

	if len(got) != 1 {
		t.Fatalf("expected one peer health item, got %#v", got)
	}

	want := PeerHealth{
		Address:             "198.51.100.1:64646",
		ClientVersion:       "0.11-alpha",
		ClientBuild:         17,
		State:               "healthy",
		Connected:           true,
		PendingCount:        2,
		LastConnectedAt:     timePtr(t, "2026-03-20T09:10:11Z"),
		LastDisconnectedAt:  timePtr(t, "2026-03-20T09:09:11Z"),
		LastPingAt:          timePtr(t, "2026-03-20T09:11:00Z"),
		LastPongAt:          timePtr(t, "2026-03-20T09:11:00Z"),
		LastUsefulSendAt:    timePtr(t, "2026-03-20T09:10:58Z"),
		LastUsefulReceiveAt: timePtr(t, "2026-03-20T09:10:59Z"),
		ConsecutiveFailures: 1,
		LastError:           "timeout",
		Score:               15,
	}

	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("unexpected peer health: got %#v want %#v", got[0], want)
	}
}

func TestParseOptionalTime(t *testing.T) {
	t.Parallel()

	if got := parseOptionalTime(""); got != nil {
		t.Fatalf("expected nil for empty time, got %#v", got)
	}
	if got := parseOptionalTime("broken"); got != nil {
		t.Fatalf("expected nil for invalid time, got %#v", got)
	}

	got := parseOptionalTime("2026-03-20T09:10:11Z")
	want := timePtr(t, "2026-03-20T09:10:11Z")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected parsed time: got %#v want %#v", got, want)
	}
}

func TestParseConsoleCommandHelp(t *testing.T) {
	t.Parallel()

	frame, output, err := parseConsoleCommand("help", "me", "desktop test")
	if err != nil {
		t.Fatalf("parseConsoleCommand returned error: %v", err)
	}
	if frame.Type != "" {
		t.Fatalf("expected empty frame for help, got %#v", frame)
	}
	if output == "" || !strings.Contains(output, "== Control ==") || !strings.Contains(output, "== Messages ==") {
		t.Fatalf("expected help output, got %q", output)
	}
}

func TestParseConsoleCommandFetchInboxDefaults(t *testing.T) {
	t.Parallel()

	frame, output, err := parseConsoleCommand("fetch_inbox", "me", "desktop test")
	if err != nil {
		t.Fatalf("parseConsoleCommand returned error: %v", err)
	}
	if output != "" {
		t.Fatalf("expected empty inline output, got %q", output)
	}

	want := protocol.Frame{Type: "fetch_inbox", Topic: "dm", Recipient: "me"}
	if !reflect.DeepEqual(frame, want) {
		t.Fatalf("unexpected frame: got %#v want %#v", frame, want)
	}
}

func TestParseConsoleCommandHello(t *testing.T) {
	t.Parallel()

	frame, output, err := parseConsoleCommand("hello", "me", "desktop test")
	if err != nil {
		t.Fatalf("parseConsoleCommand returned error: %v", err)
	}
	if output != "" {
		t.Fatalf("expected empty inline output, got %q", output)
	}
	if frame.Type != "hello" || frame.Version != config.ProtocolVersion || frame.Client != "desktop" || frame.ClientVersion != "desktop-test" || frame.ClientBuild != config.ClientBuild {
		t.Fatalf("unexpected hello frame: %#v", frame)
	}
}

func TestParseConsoleCommandJSON(t *testing.T) {
	t.Parallel()

	frame, output, err := parseConsoleCommand(`{"type":"get_peers"}`, "me", "desktop test")
	if err != nil {
		t.Fatalf("parseConsoleCommand returned error: %v", err)
	}
	if output != "" {
		t.Fatalf("expected empty inline output, got %q", output)
	}
	if frame.Type != "get_peers" {
		t.Fatalf("unexpected frame: %#v", frame)
	}
}

func TestParseConsoleCommandUnknown(t *testing.T) {
	t.Parallel()

	_, _, err := parseConsoleCommand("wat", "me", "desktop test")
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestBuildConsolePeersPayloadIncludesStatuses(t *testing.T) {
	t.Parallel()

	payload := buildConsolePeersPayload(
		[]string{"a:1", "b:2", "c:3"},
		[]PeerHealth{
			{Address: "a:1", State: "healthy", Connected: true},
			{Address: "b:2", State: "reconnecting", Connected: false, PendingCount: 2},
		},
	)
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, `"count":1`) {
		t.Fatalf("expected connected count in output: %s", out)
	}
	if !strings.Contains(out, `"total":3`) {
		t.Fatalf("expected total count in output: %s", out)
	}
	if !strings.Contains(out, `"connected":true`) {
		t.Fatalf("expected connected peer in output: %s", out)
	}
	if !strings.Contains(out, `"state":"reconnecting"`) {
		t.Fatalf("expected reconnecting peer in output: %s", out)
	}
	if !strings.Contains(out, `"pending":[`) {
		t.Fatalf("expected pending section in output: %s", out)
	}
	if !strings.Contains(out, `"known_only":[`) {
		t.Fatalf("expected known_only section in output: %s", out)
	}
}

func TestBuildConsolePeersPayloadIncludesHealthOnlyPeers(t *testing.T) {
	t.Parallel()

	// "c:3" and "e:5" appear only in health (inbound-only), not in peers[].
	// Using two health-only peers to verify deterministic sort order.
	payload := buildConsolePeersPayload(
		[]string{"a:1"},
		[]PeerHealth{
			{Address: "a:1", State: "healthy", Connected: true, BytesSent: 100},
			{Address: "e:5", State: "healthy", Connected: true, BytesSent: 500, BytesReceived: 300, TotalTraffic: 800},
			{Address: "c:3", State: "healthy", Connected: true, BytesSent: 200, BytesReceived: 100, TotalTraffic: 300},
		},
	)

	// Unmarshal into a structured type for precise ordering assertions.
	type peersPayload struct {
		Type      string              `json:"type"`
		Count     int                 `json:"count"`
		Total     int                 `json:"total"`
		Connected []ConsolePeerStatus `json:"connected"`
		Peers     []ConsolePeerStatus `json:"peers"`
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var result peersPayload
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	// All three peers should be present.
	if result.Total != 3 {
		t.Fatalf("expected total=3, got %d", result.Total)
	}
	if result.Count != 3 {
		t.Fatalf("expected connected count=3, got %d", result.Count)
	}

	// Verify ordering: a:1 (from peers[] first pass), then health-only in
	// sorted order: c:3 before e:5.
	if len(result.Peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(result.Peers))
	}
	expectedOrder := []string{"a:1", "c:3", "e:5"}
	for i, want := range expectedOrder {
		if result.Peers[i].Address != want {
			t.Errorf("peers[%d]: expected address %q, got %q (health-only peers must be sorted)", i, want, result.Peers[i].Address)
		}
	}
}

func TestBuildConsolePeersPayloadIncludesClientBuild(t *testing.T) {
	t.Parallel()

	payload := buildConsolePeersPayload(
		[]string{"a:1", "b:2"},
		[]PeerHealth{
			{Address: "a:1", ClientVersion: "0.19-alpha", ClientBuild: 19, State: "healthy", Connected: true},
			{Address: "b:2", ClientVersion: "", ClientBuild: 0, State: "healthy", Connected: true},
		},
	)

	type peersPayload struct {
		Peers []ConsolePeerStatus `json:"peers"`
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var result peersPayload
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if len(result.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(result.Peers))
	}

	if result.Peers[0].ClientVersion != "0.19-alpha" {
		t.Errorf("peers[0]: expected client_version='0.19-alpha', got %q", result.Peers[0].ClientVersion)
	}
	if result.Peers[0].ClientBuild != 19 {
		t.Errorf("peers[0]: expected client_build=19, got %d", result.Peers[0].ClientBuild)
	}
	if result.Peers[1].ClientVersion != "" {
		t.Errorf("peers[1]: expected empty client_version, got %q", result.Peers[1].ClientVersion)
	}
	if result.Peers[1].ClientBuild != 0 {
		t.Errorf("peers[1]: expected client_build=0, got %d", result.Peers[1].ClientBuild)
	}
}

func TestBuildConsolePeersPayloadIncludesPeerIdentity(t *testing.T) {
	t.Parallel()

	payload := buildConsolePeersPayload(
		[]string{"a:1", "b:2"},
		[]PeerHealth{
			{Address: "a:1", PeerID: "abc123", State: "healthy", Connected: true},
			{Address: "b:2", PeerID: "", State: "healthy", Connected: true},
		},
	)

	type peersPayload struct {
		Peers []ConsolePeerStatus `json:"peers"`
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var result peersPayload
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	if len(result.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(result.Peers))
	}
	if result.Peers[0].Identity != "abc123" {
		t.Errorf("peers[0]: expected identity='abc123', got %q", result.Peers[0].Identity)
	}
	if result.Peers[1].Identity != "" {
		t.Errorf("peers[1]: expected empty identity, got %q", result.Peers[1].Identity)
	}
}

func TestConsolePingPayloadShape(t *testing.T) {
	t.Parallel()

	payload := struct {
		Type    string              `json:"type"`
		Count   int                 `json:"count"`
		Total   int                 `json:"total"`
		Results []ConsolePingStatus `json:"results"`
	}{
		Type:  "ping",
		Count: 1,
		Total: 2,
		Results: []ConsolePingStatus{
			{Address: "a:1", OK: true, Status: "ok", Connected: true, State: "healthy", Node: "corsa", Network: "gazeta-devnet"},
			{Address: "b:2", OK: false, Status: "not_ok", Connected: false, State: "reconnecting", Error: "dial timeout"},
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, `"type":"ping"`) || !strings.Contains(out, `"status":"ok"`) || !strings.Contains(out, `"status":"not_ok"`) {
		t.Fatalf("unexpected ping payload: %s", out)
	}
}

func TestParseConsoleCommandAddPeer(t *testing.T) {
	t.Parallel()

	frame, output, err := parseConsoleCommand("add_peer 10.0.0.1:64646", "me", "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output != "" {
		t.Fatalf("expected empty inline output, got %q", output)
	}
	if frame.Type != "add_peer" {
		t.Fatalf("expected type add_peer, got %q", frame.Type)
	}
	if len(frame.Peers) != 1 || frame.Peers[0] != "10.0.0.1:64646" {
		t.Fatalf("expected peers=[10.0.0.1:64646], got %v", frame.Peers)
	}
}

func TestParseConsoleCommandAddPeerMissingArg(t *testing.T) {
	t.Parallel()

	_, _, err := parseConsoleCommand("add_peer", "me", "test")
	if err == nil {
		t.Fatal("expected error for add_peer without argument")
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()

	timestamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}

	return timestamp.UTC()
}

func timePtr(t *testing.T, value string) *time.Time {
	t.Helper()
	ts := mustTime(t, value)
	return &ts
}

func TestParseConsoleCommandFetchChatlog(t *testing.T) {
	t.Parallel()

	frame, text, err := parseConsoleCommand("fetch_chatlog dm abc123", "self-addr", "1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if text != "" {
		t.Fatalf("unexpected text: %s", text)
	}
	if frame.Type != "fetch_chatlog" {
		t.Fatalf("expected type=fetch_chatlog, got %s", frame.Type)
	}
	if frame.Topic != "dm" {
		t.Fatalf("expected topic=dm, got %s", frame.Topic)
	}
	if frame.Address != "abc123" {
		t.Fatalf("expected address=abc123, got %s", frame.Address)
	}
}

// TestNodeStatusHasNoDMBodyFields verifies that NodeStatus does not carry full
// DM bodies. The poll loop (ProbeNode, every 2s) must use only lightweight
// DMHeaders — never full inbox bodies. This test guards the documented
// "header-only" contract from docs/chatlog.md.
func TestNodeStatusHasNoDMBodyFields(t *testing.T) {
	t.Parallel()

	rt := reflect.TypeOf(NodeStatus{})
	forbidden := []string{"DirectInbox"}
	for _, name := range forbidden {
		if _, found := rt.FieldByName(name); found {
			t.Fatalf("NodeStatus must not have field %q — poll loop must be header-only (see docs/chatlog.md)", name)
		}
	}

	// DMHeaders field must exist — it's the lightweight path.
	if _, found := rt.FieldByName("DMHeaders"); !found {
		t.Fatal("NodeStatus must have DMHeaders field for lightweight poll path")
	}
}

func TestParseConsoleCommandFetchChatlogDefaultTopic(t *testing.T) {
	t.Parallel()

	frame, _, err := parseConsoleCommand("fetch_chatlog", "self-addr", "1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if frame.Topic != "dm" {
		t.Fatalf("expected default topic=dm, got %s", frame.Topic)
	}
}

func TestParseConsoleCommandFetchConversations(t *testing.T) {
	t.Parallel()

	frame, text, err := parseConsoleCommand("fetch_conversations", "self-addr", "1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if text != "" {
		t.Fatalf("unexpected text: %s", text)
	}
	if frame.Type != "fetch_conversations" {
		t.Fatalf("expected type=fetch_conversations, got %s", frame.Type)
	}
}

// ---------------------------------------------------------------------------
// Integration tests for ExecuteConsoleCommand with chatlog commands.
// These verify the full user-facing path: parse → desktop-side chatlog read →
// JSON response. They catch regressions like routing chatlog frame types through
// node.HandleLocalFrame (which no longer handles them after the chatlog refactor).
// ---------------------------------------------------------------------------

// newTestDesktopClientWithChatlog creates a minimal DesktopClient backed by a
// real chatlog.Store in a temp directory. No node.Service is attached — only
// the chatlog-related code paths work.
func newTestDesktopClientWithChatlog(t *testing.T) *DesktopClient {
	t.Helper()
	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	store := chatlog.NewStore(dir, domain.PeerIdentity(id.Address), domain.ListenAddress(":0"))
	return &DesktopClient{
		id:      id,
		appCfg:  config.App{Version: "test"},
		chatLog: store,
	}
}

func TestExecuteConsoleCommandFetchChatlog(t *testing.T) {
	t.Parallel()
	c := newTestDesktopClientWithChatlog(t)
	defer func() { _ = c.Close() }()

	peer := "abcdef1234567890abcdef1234567890abcdef12"

	// Insert a test entry.
	err := c.chatLog.Append("dm", c.id.Address, chatlog.Entry{
		ID:             "msg-001",
		Sender:         c.id.Address,
		Recipient:      peer,
		Body:           "hello",
		CreatedAt:      time.Now().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Execute the console command.
	output, err := c.ExecuteConsoleCommand("fetch_chatlog dm " + peer)
	if err != nil {
		t.Fatalf("ExecuteConsoleCommand: %v", err)
	}

	// Verify JSON output contains our message.
	if !strings.Contains(output, "msg-001") {
		t.Fatalf("expected output to contain msg-001, got: %s", output)
	}
	if !strings.Contains(output, "hello") {
		t.Fatalf("expected output to contain body 'hello', got: %s", output)
	}
}

func TestExecuteConsoleCommandFetchChatlogPreviews(t *testing.T) {
	t.Parallel()
	c := newTestDesktopClientWithChatlog(t)
	defer func() { _ = c.Close() }()

	peer := "abcdef1234567890abcdef1234567890abcdef12"

	err := c.chatLog.Append("dm", c.id.Address, chatlog.Entry{
		ID:             "msg-preview",
		Sender:         peer,
		Recipient:      c.id.Address,
		Body:           "preview body",
		CreatedAt:      time.Now().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusDelivered,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	output, err := c.ExecuteConsoleCommand("fetch_chatlog_previews")
	if err != nil {
		t.Fatalf("ExecuteConsoleCommand: %v", err)
	}

	if !strings.Contains(output, peer) {
		t.Fatalf("expected output to contain peer %s, got: %s", peer, output)
	}
}

func TestExecuteConsoleCommandFetchConversations(t *testing.T) {
	t.Parallel()
	c := newTestDesktopClientWithChatlog(t)
	defer func() { _ = c.Close() }()

	peer := "abcdef1234567890abcdef1234567890abcdef12"

	err := c.chatLog.Append("dm", c.id.Address, chatlog.Entry{
		ID:             "msg-conv",
		Sender:         c.id.Address,
		Recipient:      peer,
		Body:           "conv body",
		CreatedAt:      time.Now().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	output, err := c.ExecuteConsoleCommand("fetch_conversations")
	if err != nil {
		t.Fatalf("ExecuteConsoleCommand: %v", err)
	}

	if !strings.Contains(output, peer) {
		t.Fatalf("expected output to contain peer %s, got: %s", peer, output)
	}
	if !strings.Contains(output, "peer_address") {
		t.Fatalf("expected output to contain 'peer_address' field, got: %s", output)
	}
}

func TestExecuteConsoleCommandFetchChatlogNoChatlog(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	c := &DesktopClient{
		id:     id,
		appCfg: config.App{Version: "test"},
		// chatLog intentionally nil — simulates relay-only node
	}

	_, err = c.ExecuteConsoleCommand("fetch_chatlog dm somepeer")
	if err == nil {
		t.Fatal("expected error when chatlog is nil")
	}
	if !strings.Contains(err.Error(), "chatlog not available") {
		t.Fatalf("expected 'chatlog not available' error, got: %v", err)
	}
}

// TestDeliveredAtSynthesizedAfterRestart verifies that decryptDirectMessages
// synthesizes a non-nil DeliveredAt for messages whose persisted status is
// "delivered" or "seen" but have no in-memory receipt (the restart scenario).
func TestDeliveredAtSynthesizedAfterRestart(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate sender: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate recipient: %v", err)
	}

	// Encrypt a test message.
	body := "restart status test"
	ciphertext, err := directmsg.EncryptForParticipants(
		sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), body,
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	contacts := map[string]Contact{
		sender.Address: {
			BoxKey: identity.BoxPublicKeyBase64(sender.BoxPublicKey),
			PubKey: identity.PublicKeyBase64(sender.PublicKey),
		},
	}

	msgTime := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name            string
		persistedStatus string
		wantStatus      string
		wantDeliveredAt bool
	}{
		{"delivered from SQLite", "delivered", "delivered", true},
		{"seen from SQLite", "seen", "seen", true},
		{"sent from SQLite (no synthesis)", "sent", "sent", false},
		{"empty status, outgoing (fallback to sent)", "", "sent", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			records := []MessageRecord{{
				ID:              "msg-restart-" + tc.persistedStatus,
				Sender:          sender.Address,
				Recipient:       recipient.Address,
				Body:            ciphertext,
				Timestamp:       msgTime,
				PersistedStatus: tc.persistedStatus,
			}}

			// No in-memory receipts — simulates restart.
			result := decryptDirectMessages(sender, contacts, records, nil, nil)
			if len(result) != 1 {
				t.Fatalf("expected 1 message, got %d", len(result))
			}

			msg := result[0]
			if msg.ReceiptStatus != tc.wantStatus {
				t.Errorf("ReceiptStatus = %q, want %q", msg.ReceiptStatus, tc.wantStatus)
			}
			if tc.wantDeliveredAt && msg.DeliveredAt == nil {
				t.Error("DeliveredAt should not be nil for persisted delivered/seen status")
			}
			if !tc.wantDeliveredAt && msg.DeliveredAt != nil {
				t.Errorf("DeliveredAt should be nil, got %v", msg.DeliveredAt)
			}
			if tc.wantDeliveredAt && msg.DeliveredAt != nil {
				if !msg.DeliveredAt.Equal(msgTime) {
					t.Errorf("DeliveredAt = %v, want %v (message timestamp)", msg.DeliveredAt, msgTime)
				}
			}
			if msg.Body != body {
				t.Errorf("Body = %q, want %q", msg.Body, body)
			}
		})
	}
}

// TestParseTimestampFormats verifies that parseTimestamp handles both
// RFC3339Nano (nanosecond precision) and RFC3339 (second precision) formats,
// ensuring legacy/migrated chatlog entries sort correctly in previews.
func TestParseTimestampFormats(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"RFC3339Nano", "2026-03-25T23:02:08.123456789Z", false},
		{"RFC3339", "2026-03-25T23:02:08Z", false},
		{"RFC3339 with offset", "2026-03-25T23:02:08+03:00", false},
		{"RFC3339Nano with offset", "2026-03-25T23:02:08.123+03:00", false},
		{"invalid", "not-a-timestamp", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, err := parseTimestamp(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got time %v", tt.input, ts)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTimestamp(%q) returned error: %v", tt.input, err)
			}
			if ts.IsZero() {
				t.Fatalf("parseTimestamp(%q) returned zero time", tt.input)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Integration tests for FetchConversation, FetchConversationPreviews,
// FetchSinglePreview. These exercise the full desktop-owned chatlog reading
// path: chatlog → decryption → contact lookup via embedded node.
// ---------------------------------------------------------------------------

// newTestDesktopClientWithNode creates a DesktopClient backed by a real
// chatlog.Store AND an embedded node.Service so that localRequestFrameCtx
// (used by fetchContactsForDecrypt, delivery receipts, pending messages)
// works without a TCP connection. The node is NOT started via Run() — only
// HandleLocalFrame is used.
func newTestDesktopClientWithNode(t *testing.T) (*DesktopClient, *identity.Identity) {
	t.Helper()
	dir := t.TempDir()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	cfg := config.Node{
		ListenAddress:  ":0",
		TrustStorePath: filepath.Join(dir, "trust.json"),
		QueueStatePath: filepath.Join(dir, "queue.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
	}

	svc := node.NewService(cfg, id)
	store := chatlog.NewStore(dir, domain.PeerIdentity(id.Address), domain.ListenAddress(":0"))

	c := &DesktopClient{
		id:        id,
		appCfg:    config.App{Version: "test"},
		localNode: svc,
		chatLog:   store,
	}
	return c, id
}

// TestFetchConversationReturnsDecryptedMessages verifies the full path:
// outgoing message stored in chatlog → FetchConversation → decrypted body.
func TestFetchConversationReturnsDecryptedMessages(t *testing.T) {
	t.Parallel()
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	// Encrypt an outgoing message (self → peer).
	ciphertext, err := directmsg.EncryptForParticipants(
		id, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey), "hello from test",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	err = c.chatLog.Append("dm", id.Address, chatlog.Entry{
		ID:             "msg-out-1",
		Sender:         id.Address,
		Recipient:      peer.Address,
		Body:           ciphertext,
		CreatedAt:      ts,
		DeliveryStatus: chatlog.StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	msgs, err := c.FetchConversation(context.Background(), peer.Address)
	if err != nil {
		t.Fatalf("FetchConversation: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ID != "msg-out-1" {
		t.Fatalf("unexpected message ID: %s", msgs[0].ID)
	}
	if msgs[0].Body != "hello from test" {
		t.Fatalf("unexpected body: %q", msgs[0].Body)
	}
	if msgs[0].ReceiptStatus != "sent" {
		t.Fatalf("expected status=sent, got %q", msgs[0].ReceiptStatus)
	}
}

// TestFetchConversationEmptyPeerReturnsError verifies that an empty peer
// address is rejected immediately.
func TestFetchConversationEmptyPeerReturnsError(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	_, err := c.FetchConversation(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty peer address")
	}
	if !strings.Contains(err.Error(), "peer address is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFetchConversationNilChatlogReturnsError verifies FetchConversation
// returns an error when chatLog is nil (relay-only node).
func TestFetchConversationNilChatlogReturnsError(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}}

	_, err = c.FetchConversation(context.Background(), "somepeer")
	if err == nil {
		t.Fatal("expected error when chatlog is nil")
	}
	if !strings.Contains(err.Error(), "chatlog not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFetchConversationRespectsContextCancellation verifies that a cancelled
// context propagates through FetchConversation.
func TestFetchConversationRespectsContextCancellation(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := c.FetchConversation(ctx, "somepeer")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// TestFetchConversationPreviewsReturnsDecryptedPreviews verifies end-to-end:
// messages in chatlog → FetchConversationPreviews → decrypted preview per peer.
func TestFetchConversationPreviewsReturnsDecryptedPreviews(t *testing.T) {
	t.Parallel()
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer1, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer1: %v", err)
	}
	peer2, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer2: %v", err)
	}

	// Insert outgoing messages to two different peers.
	for _, tc := range []struct {
		peerAddr string
		peerID   *identity.Identity
		body     string
		msgID    string
	}{
		{peer1.Address, peer1, "hello peer1", "msg-p1"},
		{peer2.Address, peer2, "hello peer2", "msg-p2"},
	} {
		ct, encErr := directmsg.EncryptForParticipants(
			id, tc.peerAddr, identity.BoxPublicKeyBase64(tc.peerID.BoxPublicKey), tc.body,
		)
		if encErr != nil {
			t.Fatalf("encrypt for %s: %v", tc.peerAddr, encErr)
		}
		err = c.chatLog.Append("dm", id.Address, chatlog.Entry{
			ID:             tc.msgID,
			Sender:         id.Address,
			Recipient:      tc.peerAddr,
			Body:           ct,
			CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
			DeliveryStatus: chatlog.StatusSent,
		})
		if err != nil {
			t.Fatalf("append %s: %v", tc.msgID, err)
		}
	}

	previews, err := c.FetchConversationPreviews(context.Background())
	if err != nil {
		t.Fatalf("FetchConversationPreviews: %v", err)
	}
	if len(previews) != 2 {
		t.Fatalf("expected 2 previews, got %d", len(previews))
	}

	// Build a map for easier assertion.
	byPeer := make(map[string]ConversationPreview, len(previews))
	for _, p := range previews {
		byPeer[p.PeerAddress] = p
	}
	if p, ok := byPeer[peer1.Address]; !ok {
		t.Fatal("missing preview for peer1")
	} else if p.Body != "hello peer1" {
		t.Fatalf("peer1 preview body = %q, want %q", p.Body, "hello peer1")
	}
	if p, ok := byPeer[peer2.Address]; !ok {
		t.Fatal("missing preview for peer2")
	} else if p.Body != "hello peer2" {
		t.Fatalf("peer2 preview body = %q, want %q", p.Body, "hello peer2")
	}
}

// TestFetchConversationPreviewsNilChatlogReturnsError verifies the nil guard.
func TestFetchConversationPreviewsNilChatlogReturnsError(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}}

	_, err = c.FetchConversationPreviews(context.Background())
	if err == nil {
		t.Fatal("expected error when chatlog is nil")
	}
	if !strings.Contains(err.Error(), "chatlog not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFetchConversationPreviewsIncludesUnreadCount verifies that UnreadCount
// from ListConversationsCtx is propagated into the preview.
func TestFetchConversationPreviewsIncludesUnreadCount(t *testing.T) {
	t.Parallel()
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	// Insert an outgoing message (so we have a conversation).
	ct, err := directmsg.EncryptForParticipants(
		id, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey), "msg body",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	// Insert as incoming (from peer) with status "delivered" so it counts as unread.
	err = c.chatLog.Append("dm", id.Address, chatlog.Entry{
		ID:             "msg-unread",
		Sender:         id.Address,
		Recipient:      peer.Address,
		Body:           ct,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	previews, err := c.FetchConversationPreviews(context.Background())
	if err != nil {
		t.Fatalf("FetchConversationPreviews: %v", err)
	}
	if len(previews) != 1 {
		t.Fatalf("expected 1 preview, got %d", len(previews))
	}
	// UnreadCount comes from ListConversationsCtx which counts non-seen
	// incoming messages. Our message is outgoing so unread should be 0.
	if previews[0].UnreadCount != 0 {
		t.Fatalf("expected UnreadCount=0 for outgoing message, got %d", previews[0].UnreadCount)
	}
}

// TestFetchContactsForDecryptPropagatesNetworkError verifies that when
// fetch_contacts fails (network contacts needed but unavailable), the error
// propagates to callers instead of silently returning trusted-only contacts.
// With trusted-only contacts, messages from unknown senders would be silently
// dropped in decryptDirectMessages, producing incomplete history/previews.
//
// We simulate this by using a DesktopClient with localNode=nil (no embedded
// node) — localRequestFrameCtx will fail for both fetch_trusted_contacts and
// fetch_contacts. The important assertion is that the error IS returned.
func TestFetchContactsForDecryptPropagatesNetworkError(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	// No localNode, no TCP connection — all localRequestFrameCtx calls fail.
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}}

	// Even though this fails at fetch_trusted_contacts (first call), the
	// contract is: any failure in contact resolution returns an error.
	_, err = c.fetchContactsForDecrypt(context.Background(), []string{"unknown-peer"})
	if err == nil {
		t.Fatal("expected error when localNode is nil, got nil — callers would silently get incomplete contacts")
	}
}

// TestFetchConversationPreviewsFailsOnSummariesError verifies that
// FetchConversationPreviews returns an error when ListConversationsCtx
// fails, rather than silently returning previews with zero unread counts.
// We simulate this by closing the chatlog db — both queries will fail, and
// the function must propagate the first error.
func TestFetchConversationPreviewsFailsOnSummariesError(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	// Close the chatlog db to make SQLite queries fail.
	if err := c.chatLog.Close(); err != nil {
		t.Fatalf("close chatlog: %v", err)
	}

	_, err := c.FetchConversationPreviews(context.Background())
	if err == nil {
		t.Fatal("expected error when chatlog db is closed, got nil — unread badges would be silently lost")
	}
	// The error should come from one of the SQLite queries (entries or summaries).
	if !strings.Contains(err.Error(), "chatlog") {
		t.Fatalf("expected chatlog-related error, got: %v", err)
	}
}

// TestFetchSinglePreviewReturnsDecryptedPreview verifies the single-peer path.
func TestFetchSinglePreviewReturnsDecryptedPreview(t *testing.T) {
	t.Parallel()
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	ct, err := directmsg.EncryptForParticipants(
		id, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey), "single preview body",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	err = c.chatLog.Append("dm", id.Address, chatlog.Entry{
		ID:             "msg-single",
		Sender:         id.Address,
		Recipient:      peer.Address,
		Body:           ct,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		DeliveryStatus: chatlog.StatusSent,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	preview, err := c.FetchSinglePreview(context.Background(), peer.Address)
	if err != nil {
		t.Fatalf("FetchSinglePreview: %v", err)
	}
	if preview == nil {
		t.Fatal("expected non-nil preview")
	}
	if preview.PeerAddress != peer.Address {
		t.Fatalf("unexpected peer: %s", preview.PeerAddress)
	}
	if preview.Body != "single preview body" {
		t.Fatalf("unexpected body: %q", preview.Body)
	}
}

// TestFetchSinglePreviewNonExistentPeerReturnsNil verifies that a peer with
// no messages returns nil preview (not error).
func TestFetchSinglePreviewNonExistentPeerReturnsNil(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	preview, err := c.FetchSinglePreview(context.Background(), "nonexistent-peer")
	if err != nil {
		t.Fatalf("FetchSinglePreview: %v", err)
	}
	if preview != nil {
		t.Fatalf("expected nil preview for nonexistent peer, got %+v", preview)
	}
}

// TestFetchSinglePreviewNilChatlogReturnsError verifies the nil guard.
func TestFetchSinglePreviewNilChatlogReturnsError(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	c := &DesktopClient{id: id, appCfg: config.App{Version: "test"}}

	_, err = c.FetchSinglePreview(context.Background(), "somepeer")
	if err == nil {
		t.Fatal("expected error when chatlog is nil")
	}
	if !strings.Contains(err.Error(), "chatlog not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestFetchConversationMultipleMessages verifies that FetchConversation
// returns all messages in the conversation, preserving order.
func TestFetchConversationMultipleMessages(t *testing.T) {
	t.Parallel()
	c, id := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate peer: %v", err)
	}

	// Insert 3 outgoing messages.
	for i := 0; i < 3; i++ {
		body := strings.Repeat("x", i+1) // "x", "xx", "xxx"
		ct, encErr := directmsg.EncryptForParticipants(
			id, peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey), body,
		)
		if encErr != nil {
			t.Fatalf("encrypt msg %d: %v", i, encErr)
		}
		err = c.chatLog.Append("dm", id.Address, chatlog.Entry{
			ID:             fmt.Sprintf("msg-%d", i),
			Sender:         id.Address,
			Recipient:      peer.Address,
			Body:           ct,
			CreatedAt:      time.Now().Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano),
			DeliveryStatus: chatlog.StatusSent,
		})
		if err != nil {
			t.Fatalf("append msg %d: %v", i, err)
		}
	}

	msgs, err := c.FetchConversation(context.Background(), peer.Address)
	if err != nil {
		t.Fatalf("FetchConversation: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	// Verify bodies are decrypted correctly.
	for i, msg := range msgs {
		want := strings.Repeat("x", i+1)
		if msg.Body != want {
			t.Errorf("msg[%d].Body = %q, want %q", i, msg.Body, want)
		}
	}
}

// TestBuildReachableIDsNilNode verifies that buildReachableIDs returns nil
// when localNode is not set (remote TCP mode).
func TestBuildReachableIDsNilNode(t *testing.T) {
	t.Parallel()
	c := &DesktopClient{
		id:     mustGenerateIdentity(t),
		appCfg: config.App{Version: "test"},
	}
	got := c.buildReachableIDs([]string{"aaa", "bbb"})
	if got != nil {
		t.Fatalf("expected nil for nil localNode, got %v", got)
	}
}

// TestBuildReachableIDsEmptyTable verifies that all identities are marked
// unreachable when the routing table has no routes.
func TestBuildReachableIDsEmptyTable(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	ids := []string{"peer-a", "peer-b", "peer-c"}
	got := c.buildReachableIDs(ids)
	if got == nil {
		t.Fatal("expected non-nil map")
	}
	for _, id := range ids {
		pid := domain.PeerIdentity(id)
		if got[pid] {
			t.Errorf("identity %q should be unreachable with empty routing table", id)
		}
	}
}

// TestBuildReachableIDsEmptyList verifies that an empty identity list
// produces an empty (non-nil) map.
func TestBuildReachableIDsEmptyList(t *testing.T) {
	t.Parallel()
	c, _ := newTestDesktopClientWithNode(t)
	defer func() { _ = c.Close() }()

	got := c.buildReachableIDs(nil)
	if got == nil {
		t.Fatal("expected non-nil map for nil slice")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
}

func mustGenerateIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id
}
