// Package ecdhgcm implements ephemeral ECDH X25519 + AES-256-GCM
// authenticated encryption with domain-separated key derivation.
//
// All Corsa protocols that need sealed envelopes (DM, file commands, gazeta)
// share this primitive, differing only in the KDF label.
package ecdhgcm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
)

// SealedBox is the output of Seal: raw bytes ready for transport encoding.
type SealedBox struct {
	EphemeralPub []byte // 32 bytes X25519 public key
	Nonce        []byte // 12 bytes AES-GCM nonce
	Ciphertext   []byte // AES-GCM ciphertext + tag
}

// Seal encrypts plaintext for recipientPub using a fresh ephemeral X25519 key
// pair and AES-256-GCM. The label provides domain separation for the KDF so
// that ciphertexts from different protocols are not interchangeable.
func Seal(recipientPub *ecdh.PublicKey, plaintext []byte, label string) (*SealedBox, error) {
	curve := ecdh.X25519()

	ephemeralKey, err := curve.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral key: %w", err)
	}

	sharedSecret, err := ephemeralKey.ECDH(recipientPub)
	if err != nil {
		return nil, fmt.Errorf("derive shared secret: %w", err)
	}

	gcm, err := newGCM(sharedSecret, label)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)

	return &SealedBox{
		EphemeralPub: ephemeralKey.PublicKey().Bytes(),
		Nonce:        nonce,
		Ciphertext:   ciphertext,
	}, nil
}

// Open decrypts a SealedBox using the recipient's X25519 private key and the
// same label that was used during Seal.
func Open(privKey *ecdh.PrivateKey, box *SealedBox, label string) ([]byte, error) {
	curve := ecdh.X25519()

	ephPub, err := curve.NewPublicKey(box.EphemeralPub)
	if err != nil {
		return nil, fmt.Errorf("restore ephemeral public key: %w", err)
	}

	sharedSecret, err := privKey.ECDH(ephPub)
	if err != nil {
		return nil, fmt.Errorf("derive shared secret: %w", err)
	}

	gcm, err := newGCM(sharedSecret, label)
	if err != nil {
		return nil, err
	}

	plain, err := gcm.Open(nil, box.Nonce, box.Ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}

	return plain, nil
}

// newGCM derives an AES-256 key from the ECDH shared secret using SHA-256 with
// a domain-separation label and returns a ready-to-use GCM cipher.
func newGCM(sharedSecret []byte, label string) (cipher.AEAD, error) {
	key := sha256.Sum256(append([]byte(label), sharedSecret...))

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	return gcm, nil
}
