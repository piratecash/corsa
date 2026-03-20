package service

import (
	"reflect"
	"testing"
	"time"

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

func TestDecryptDirectMessagesMarksQueuedAndSent(t *testing.T) {
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
		{ID: "sent-1", Flag: "sender-delete", Timestamp: mustTime(t, "2026-03-19T10:01:00Z"), Sender: sender.Address, Recipient: recipient.Address, Body: ciphertext},
	}, nil, []string{"queued-1"})

	if len(got) != 2 {
		t.Fatalf("unexpected direct messages: %#v", got)
	}
	if got[0].ReceiptStatus != "queued" {
		t.Fatalf("expected queued status, got %#v", got[0])
	}
	if got[1].ReceiptStatus != "sent" {
		t.Fatalf("expected sent status, got %#v", got[1])
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
				Address:             "65.108.204.190:64646",
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
			},
		},
	})

	if len(got) != 1 {
		t.Fatalf("expected one peer health item, got %#v", got)
	}

	want := PeerHealth{
		Address:             "65.108.204.190:64646",
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
