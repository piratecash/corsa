package directmsg

import (
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func newEnvelopeIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return id
}

// rotateBoxKey returns the same signing identity with a brand-new X25519
// pair — the §4.10 scenario: the address survives, the encryption key does
// not.
func rotateBoxKey(t *testing.T, id *identity.Identity) *identity.Identity {
	t.Helper()
	fresh, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate box key: %v", err)
	}
	rotated := *id
	rotated.BoxPrivateKey = fresh
	rotated.BoxPublicKey = fresh.PublicKey().Bytes()
	return &rotated
}

func sealTestEnvelope(t *testing.T, sender, recipient *identity.Identity, body string, retryOf string) string {
	t.Helper()
	ciphertext, err := EncryptForParticipants(sender, domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(recipient.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
	}, domain.OutgoingDM{Body: body, RetryOf: domain.MessageID(retryOf)})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return ciphertext
}

// TestDecryptErrorClasses pins the §4.10 typed classes: only the rotated
// key produces the confirmed crypto-fail; breakage and forgery are their
// own classes and must never be mistaken for it.
func TestDecryptErrorClasses(t *testing.T) {
	t.Parallel()
	sender := newEnvelopeIdentity(t)
	receiver := newEnvelopeIdentity(t)
	stranger := newEnvelopeIdentity(t)
	senderPub := identity.PublicKeyBase64(sender.PublicKey)
	ciphertext := sealTestEnvelope(t, sender, receiver, "hello", "")

	t.Run("rotated box key is the confirmed crypto-fail", func(t *testing.T) {
		rotated := rotateBoxKey(t, receiver)
		_, err := DecryptForIdentity(rotated, sender.Address, senderPub, receiver.Address, ciphertext)
		if !errors.Is(err, ErrSealedUnreadable) {
			t.Fatalf("err = %v, want ErrSealedUnreadable", err)
		}
	})

	t.Run("malformed bytes are not a crypto-fail", func(t *testing.T) {
		_, err := DecryptForIdentity(receiver, sender.Address, senderPub, receiver.Address, "%%%not-base64%%%")
		if !errors.Is(err, ErrEnvelopeMalformed) {
			t.Fatalf("err = %v, want ErrEnvelopeMalformed", err)
		}
	})

	t.Run("recipient mismatch is an auth failure", func(t *testing.T) {
		_, err := DecryptForIdentity(receiver, sender.Address, senderPub, stranger.Address, ciphertext)
		if !errors.Is(err, ErrEnvelopeAuth) {
			t.Fatalf("err = %v, want ErrEnvelopeAuth", err)
		}
	})

	t.Run("foreign signer is an auth failure", func(t *testing.T) {
		_, err := DecryptForIdentity(receiver, sender.Address, identity.PublicKeyBase64(stranger.PublicKey), receiver.Address, ciphertext)
		if err == nil {
			t.Fatal("foreign signer accepted")
		}
		if errors.Is(err, ErrSealedUnreadable) {
			t.Fatal("forgery classified as a confirmed crypto-fail — a notice would be spendable slander")
		}
	})

	t.Run("healthy decrypt still works", func(t *testing.T) {
		msg, err := DecryptForIdentity(receiver, sender.Address, senderPub, receiver.Address, ciphertext)
		if err != nil || msg.Body != "hello" {
			t.Fatalf("msg=%+v err=%v", msg, err)
		}
	})
}

// TestSenderOwnCopyAndRetryOf: the sender re-reads the plaintext from its
// OWN sealed copy (the §4.10 re-send source — plaintext is nowhere on
// disk), and retry_of survives the roundtrip inside the encrypted part.
func TestSenderOwnCopyAndRetryOf(t *testing.T) {
	t.Parallel()
	sender := newEnvelopeIdentity(t)
	receiver := newEnvelopeIdentity(t)
	const original = "0b7d81f2-9c48-4a6e-9d10-0000000000aa"
	ciphertext := sealTestEnvelope(t, sender, receiver, "recovered text", original)

	own, err := DecryptForIdentity(sender, sender.Address, identity.PublicKeyBase64(sender.PublicKey), receiver.Address, ciphertext)
	if err != nil {
		t.Fatalf("sender cannot read its own copy: %v", err)
	}
	if own.Body != "recovered text" || own.RetryOf != original {
		t.Fatalf("own copy = %+v", own)
	}

	their, err := DecryptForIdentity(receiver, sender.Address, identity.PublicKeyBase64(sender.PublicKey), receiver.Address, ciphertext)
	if err != nil || their.RetryOf != original {
		t.Fatalf("receiver copy = %+v err=%v", their, err)
	}
}
