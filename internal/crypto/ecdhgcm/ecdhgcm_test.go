package ecdhgcm

import (
	"bytes"
	"crypto/ecdh"
	"testing"
)

func generateTestKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestSealOpenRoundTrip(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)
	plaintext := []byte("corsa p2p messenger payload")
	label := "test-label-v1"

	box, err := Seal(recipient.PublicKey(), plaintext, label)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	got, err := Open(recipient, box, label)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if !bytes.Equal(got, plaintext) {
		t.Errorf("plaintext mismatch: got %q, want %q", got, plaintext)
	}
}

func TestSealProducesDifferentCiphertexts(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)
	plaintext := []byte("same input twice")
	label := "determinism-check"

	box1, err := Seal(recipient.PublicKey(), plaintext, label)
	if err != nil {
		t.Fatalf("Seal 1: %v", err)
	}

	box2, err := Seal(recipient.PublicKey(), plaintext, label)
	if err != nil {
		t.Fatalf("Seal 2: %v", err)
	}

	if bytes.Equal(box1.EphemeralPub, box2.EphemeralPub) {
		t.Error("two Seal calls produced identical ephemeral keys")
	}
	if bytes.Equal(box1.Ciphertext, box2.Ciphertext) {
		t.Error("two Seal calls produced identical ciphertexts")
	}
}

func TestOpenWrongKeyFails(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)
	attacker := generateTestKey(t)
	label := "wrong-key-test"

	box, err := Seal(recipient.PublicKey(), []byte("secret"), label)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	_, err = Open(attacker, box, label)
	if err == nil {
		t.Fatal("Open with wrong private key must fail")
	}
}

func TestDomainSeparation(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)
	plaintext := []byte("domain separation check")

	box, err := Seal(recipient.PublicKey(), plaintext, "label-alpha")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	_, err = Open(recipient, box, "label-beta")
	if err == nil {
		t.Fatal("Open with different label must fail (domain separation)")
	}

	got, err := Open(recipient, box, "label-alpha")
	if err != nil {
		t.Fatalf("Open with correct label: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("plaintext mismatch after correct-label Open")
	}
}

func TestSealedBoxFieldSizes(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)

	box, err := Seal(recipient.PublicKey(), []byte("size check"), "size-test")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if len(box.EphemeralPub) != 32 {
		t.Errorf("EphemeralPub length = %d, want 32", len(box.EphemeralPub))
	}
	if len(box.Nonce) != 12 {
		t.Errorf("Nonce length = %d, want 12", len(box.Nonce))
	}
	if len(box.Ciphertext) == 0 {
		t.Error("Ciphertext is empty")
	}
}

func TestOpenTamperedCiphertextFails(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)

	box, err := Seal(recipient.PublicKey(), []byte("tamper test"), "tamper-label")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	tampered := make([]byte, len(box.Ciphertext))
	copy(tampered, box.Ciphertext)
	tampered[0] ^= 0xff

	_, err = Open(recipient, &SealedBox{
		EphemeralPub: box.EphemeralPub,
		Nonce:        box.Nonce,
		Ciphertext:   tampered,
	}, "tamper-label")
	if err == nil {
		t.Fatal("Open with tampered ciphertext must fail")
	}
}

func TestOpenTamperedNonceFails(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)

	box, err := Seal(recipient.PublicKey(), []byte("nonce tamper"), "nonce-label")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	badNonce := make([]byte, len(box.Nonce))
	copy(badNonce, box.Nonce)
	badNonce[0] ^= 0xff

	_, err = Open(recipient, &SealedBox{
		EphemeralPub: box.EphemeralPub,
		Nonce:        badNonce,
		Ciphertext:   box.Ciphertext,
	}, "nonce-label")
	if err == nil {
		t.Fatal("Open with tampered nonce must fail")
	}
}

func TestOpenInvalidEphemeralKeyFails(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)

	box, err := Seal(recipient.PublicKey(), []byte("bad eph"), "eph-label")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	_, err = Open(recipient, &SealedBox{
		EphemeralPub: make([]byte, 31), // wrong size
		Nonce:        box.Nonce,
		Ciphertext:   box.Ciphertext,
	}, "eph-label")
	if err == nil {
		t.Fatal("Open with invalid ephemeral key size must fail")
	}
}

func TestEmptyPlaintext(t *testing.T) {
	t.Parallel()

	recipient := generateTestKey(t)

	box, err := Seal(recipient.PublicKey(), []byte{}, "empty-label")
	if err != nil {
		t.Fatalf("Seal empty: %v", err)
	}

	got, err := Open(recipient, box, "empty-label")
	if err != nil {
		t.Fatalf("Open empty: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("expected empty plaintext, got %d bytes", len(got))
	}
}
