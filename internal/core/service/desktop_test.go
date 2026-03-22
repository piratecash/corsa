package service

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"corsa/internal/core/config"
	"corsa/internal/core/directmsg"
	"corsa/internal/core/identity"
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
	if frame.Type != "hello" || frame.Version != config.ProtocolVersion || frame.Client != "desktop" || frame.ClientVersion != "desktop-test" {
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
