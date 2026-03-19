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
	}}, nil)
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

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()

	timestamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}

	return timestamp.UTC()
}
