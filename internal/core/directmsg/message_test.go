package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestDMV1SealedReferenceRemainsReadableByLegacyDecoder(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: "message-id", Sender: sender.Address, Recipient: recipient.Address}
	pubKey := identity.PublicKeyBase64(sender.PublicKey)
	encoded, err := EncryptForMessage(sender, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(recipient.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
	}, domain.OutgoingDM{Body: "hello"}, ref.Topic, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := verifyEnvelope(ref.Sender, pubKey, ref.Recipient, encoded)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := openLabeledPart(recipient, envelope.Recipient, dmKeyLabel)
	if err != nil || legacy.Body != "hello" {
		t.Fatalf("legacy decoder = %v, %v", legacy, err)
	}
	for _, participant := range []*identity.Identity{recipient, sender} {
		message, err := DecryptForMessage(participant, ref, pubKey, encoded)
		if err != nil || message.Body != "hello" {
			t.Fatalf("matching reference = %v, %v", message, err)
		}
	}
	wrongID := ref
	wrongID.ID = "another-message-id"
	wrongTopic := ref
	wrongTopic.Topic = "dm-control"
	for _, mismatch := range []domain.MessageRef{wrongID, wrongTopic} {
		if err := VerifyDataEnvelopeForRecipient(recipient, mismatch, pubKey, encoded); !errors.Is(err, ErrEnvelopeAuth) {
			t.Fatalf("reference mismatch returned %v", err)
		}
	}
}

func TestDMV1SealedReferenceRespectsLegacyRelayBodyLimit(t *testing.T) {
	t.Parallel()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	to := domain.DMRecipient{Address: domain.PeerIdentityFromWire(recipient.Address), BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey)}
	ref := v1MessageRef{Topic: "dm", ID: "f8139aa9-49df-4f51-80a2-40eaf440b6f1", Sender: sender.Address, Recipient: recipient.Address}
	for _, tc := range []struct {
		name   string
		length int
		limit  int
		bound  bool
	}{
		{"binding fits", 18000, domain.MaxRelayMessageBodyBytes, true},
		{"only legacy fits the relay", 18180, domain.MaxRelayMessageBodyBytes, false},
		{"legacy peer command body", 36500, 128 * 1024, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Repeat("a", tc.length)
			msg := domain.OutgoingDM{Body: body}
			legacy, err := EncryptForParticipants(sender, to, msg)
			if err != nil || len(legacy) > tc.limit {
				t.Fatalf("legacy body: %d bytes, %v", len(legacy), err)
			}
			bound, err := encryptV1(sender, to, msg, &ref)
			if err != nil || (len(bound) <= domain.MaxRelayMessageBodyBytes) != tc.bound {
				t.Fatalf("bound body: %d bytes, %v; expected to fit: %v", len(bound), err, tc.bound)
			}
			encoded, err := EncryptForMessage(sender, to, msg, ref.Topic, ref.ID)
			if err != nil || len(encoded) > tc.limit {
				t.Fatalf("encoded body: %d bytes, %v", len(encoded), err)
			}
			message, err := DecryptForMessage(recipient, domain.MessageRef(ref), identity.PublicKeyBase64(sender.PublicKey), encoded)
			if err != nil || message.Body != body {
				t.Fatalf("decode did not preserve body: %v", err)
			}
			wrong := domain.MessageRef(ref)
			wrong.ID = "another-message-id"
			_, err = DecryptForMessage(recipient, wrong, identity.PublicKeyBase64(sender.PublicKey), encoded)
			if (tc.bound && !errors.Is(err, ErrEnvelopeAuth)) || (!tc.bound && err != nil) {
				t.Fatalf("reference check returned %v; expected binding: %v", err, tc.bound)
			}
			t.Logf("encoded bytes: legacy=%d bound=%d sent=%d", len(legacy), len(bound), len(encoded))
		})
	}
}

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
			Address:      domain.PeerIdentityFromWire(recipient.Address),
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
			Address:      domain.PeerIdentityFromWire(recipient.Address),
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
			Address:      domain.PeerIdentityFromWire(recipient.Address),
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
			Address:      domain.PeerIdentityFromWire(recipient.Address),
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
