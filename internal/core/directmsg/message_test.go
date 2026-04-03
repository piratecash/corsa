package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
)

func TestEncryptDecryptForSenderAndRecipient(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "hello encrypted world"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	gotRecipient, err := DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("recipient decrypt failed: %v", err)
	}
	if gotRecipient.Body != "hello encrypted world" {
		t.Fatalf("unexpected recipient body: %q", gotRecipient.Body)
	}

	gotSender, err := DecryptForIdentity(
		sender,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("sender decrypt failed: %v", err)
	}
	if gotSender.Body != "hello encrypted world" {
		t.Fatalf("unexpected sender body: %q", gotSender.Body)
	}
}

func TestDecryptFailsForThirdParty(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	thirdParty, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate third party failed: %v", err)
	}

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "classified"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	if _, err := DecryptForIdentity(
		thirdParty,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	); err == nil {
		t.Fatal("expected third party decryption to fail")
	}
}

func TestDecryptFailsWhenSignatureIsTampered(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "tamper check"},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	raw, err := base64.RawURLEncoding.DecodeString(ciphertext)
	if err != nil {
		t.Fatalf("decode ciphertext failed: %v", err)
	}

	var envelope sealedEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	envelope.Signature = base64.RawURLEncoding.EncodeToString([]byte("bad-signature"))
	tamperedRaw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal tampered envelope failed: %v", err)
	}
	tampered := base64.RawURLEncoding.EncodeToString(tamperedRaw)

	if _, err := DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		tampered,
	); err == nil {
		t.Fatal("expected tampered signature verification to fail")
	}
}

func TestEncryptDecryptWithReplyTo(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate sender failed: %v", err)
	}

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("Generate recipient failed: %v", err)
	}

	expectedReplyTo := "a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5"
	ciphertext, err := EncryptForParticipants(
		sender,
		domain.DMRecipient{
			Address:      domain.PeerIdentity(recipient.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
		},
		domain.OutgoingDM{
			Body:    "hello with reply",
			ReplyTo: domain.MessageID(expectedReplyTo),
		},
	)
	if err != nil {
		t.Fatalf("EncryptForParticipants failed: %v", err)
	}

	gotRecipient, err := DecryptForIdentity(
		recipient,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("recipient decrypt failed: %v", err)
	}
	if gotRecipient.Body != "hello with reply" {
		t.Fatalf("unexpected recipient body: %q", gotRecipient.Body)
	}
	if gotRecipient.ReplyTo != expectedReplyTo {
		t.Fatalf("unexpected recipient replyTo: expected %q, got %q", expectedReplyTo, gotRecipient.ReplyTo)
	}

	gotSender, err := DecryptForIdentity(
		sender,
		sender.Address,
		identity.PublicKeyBase64(sender.PublicKey),
		recipient.Address,
		ciphertext,
	)
	if err != nil {
		t.Fatalf("sender decrypt failed: %v", err)
	}
	if gotSender.Body != "hello with reply" {
		t.Fatalf("unexpected sender body: %q", gotSender.Body)
	}
	if gotSender.ReplyTo != expectedReplyTo {
		t.Fatalf("unexpected sender replyTo: expected %q, got %q", expectedReplyTo, gotSender.ReplyTo)
	}
}
