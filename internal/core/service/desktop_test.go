package service

import (
	"reflect"
	"testing"

	"corsa/internal/core/directmsg"
	"corsa/internal/core/identity"
)

func TestParsePeers(t *testing.T) {
	t.Parallel()

	got, err := parsePeers("PEERS count=2 list=127.0.0.1:64646,127.0.0.1:64647")
	if err != nil {
		t.Fatalf("parsePeers returned error: %v", err)
	}

	want := []string{"127.0.0.1:64646", "127.0.0.1:64647"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected peers: got %v want %v", got, want)
	}
}

func TestParsePeersMissingList(t *testing.T) {
	t.Parallel()

	if _, err := parsePeers("PEERS count=1"); err == nil {
		t.Fatal("expected parsePeers to fail for missing list")
	}
}

func TestParseMessages(t *testing.T) {
	t.Parallel()

	got, err := parseMessages("MESSAGES topic=global count=2 list=hello|world")
	if err != nil {
		t.Fatalf("parseMessages returned error: %v", err)
	}

	want := []string{"hello", "world"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected messages: got %v want %v", got, want)
	}
}

func TestParseMessagesPreservesSpaces(t *testing.T) {
	t.Parallel()

	got, err := parseMessages("MESSAGES topic=dm count=1 list=alice>bob>My message was delivered ?")
	if err != nil {
		t.Fatalf("parseMessages returned error: %v", err)
	}

	want := []string{"alice>bob>My message was delivered ?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected messages with spaces: got %v want %v", got, want)
	}
}

func TestParseMessagesUnexpectedPrefix(t *testing.T) {
	t.Parallel()

	if _, err := parseMessages("PEERS count=1 list=x"); err == nil {
		t.Fatal("expected parseMessages to fail for unexpected prefix")
	}
}

func TestParseInbox(t *testing.T) {
	t.Parallel()

	got, err := parseInbox("INBOX topic=global recipient=abc count=1 list=sender>abc>hello")
	if err != nil {
		t.Fatalf("parseInbox returned error: %v", err)
	}

	want := []string{"sender>abc>hello"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected inbox: got %v want %v", got, want)
	}
}

func TestParseInboxPreservesSpaces(t *testing.T) {
	t.Parallel()

	got, err := parseInbox("INBOX topic=dm recipient=abc count=1 list=sender>abc>Мое сообщение доставилось ?")
	if err != nil {
		t.Fatalf("parseInbox returned error: %v", err)
	}

	want := []string{"sender>abc>Мое сообщение доставилось ?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected inbox with spaces: got %v want %v", got, want)
	}
}

func TestParseIdentities(t *testing.T) {
	t.Parallel()

	got, err := parseIdentities("IDENTITIES count=2 list=abc,def")
	if err != nil {
		t.Fatalf("parseIdentities returned error: %v", err)
	}

	want := []string{"abc", "def"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected identities: got %v want %v", got, want)
	}
}

func TestParseContacts(t *testing.T) {
	t.Parallel()

	got, err := parseContacts("CONTACTS count=2 list=abc@box1@pub1@sig1,def@box2@pub2@sig2")
	if err != nil {
		t.Fatalf("parseContacts returned error: %v", err)
	}

	want := map[string]Contact{
		"abc": {BoxKey: "box1", PubKey: "pub1", BoxSignature: "sig1"},
		"def": {BoxKey: "box2", PubKey: "pub2", BoxSignature: "sig2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected contacts: got %v want %v", got, want)
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
	}, []string{sender.Address + ">" + recipient.Address + ">" + ciphertext})
	want := []string{sender.Address + ">" + recipient.Address + ">secret phrase"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected decrypted messages: got %v want %v", got, want)
	}
}
