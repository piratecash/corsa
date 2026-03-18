package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"testing"

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

	ciphertext, err := EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "hello encrypted world")
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

	ciphertext, err := EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "classified")
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

	ciphertext, err := EncryptForParticipants(sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey), "tamper check")
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
