package directmsg

import (
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/crypto/ecdhgcm"
)

// fileCommandKeyLabel is the domain-separation string for file command
// payload encryption. Distinct from "corsa-dm-v1" to prevent cross-protocol
// key reuse.
const fileCommandKeyLabel = "corsa-file-cmd-v1"

// EncryptFileCommandPayload encrypts a FileCommandPayload for the recipient's
// box key using ephemeral ECDH + AES-256-GCM. The result is a base64-encoded
// ciphertext suitable for the Payload field of FileCommandFrame.
//
// The encryption is one-directional (sealed only for the recipient, not for
// the sender) because file commands are transient protocol traffic, not
// stored messages.
func EncryptFileCommandPayload(
	recipientBoxKeyBase64 string,
	payload domain.FileCommandPayload,
) (string, error) {
	recipientBoxKey, err := base64.StdEncoding.DecodeString(recipientBoxKeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode recipient box key: %w", err)
	}

	curve := ecdh.X25519()
	recipientKey, err := curve.NewPublicKey(recipientBoxKey)
	if err != nil {
		return "", fmt.Errorf("create recipient public key: %w", err)
	}

	plain, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal file command payload: %w", err)
	}

	box, err := ecdhgcm.Seal(recipientKey, plain, fileCommandKeyLabel)
	if err != nil {
		return "", err
	}

	// Wire format: ephemeral_pub(32) || nonce(12) || ciphertext(variable)
	// All concatenated then base64-encoded.
	combined := make([]byte, 0, len(box.EphemeralPub)+len(box.Nonce)+len(box.Ciphertext))
	combined = append(combined, box.EphemeralPub...)
	combined = append(combined, box.Nonce...)
	combined = append(combined, box.Ciphertext...)

	return base64.RawURLEncoding.EncodeToString(combined), nil
}

// DecryptFileCommandPayload decrypts the base64-encoded encrypted payload
// from a FileCommandFrame using the local identity's box private key.
func DecryptFileCommandPayload(
	id *identity.Identity,
	encodedPayload string,
) (*domain.FileCommandPayload, error) {
	combined, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil {
		return nil, fmt.Errorf("decode file command payload: %w", err)
	}

	// Wire format: ephemeral_pub(32) || nonce(12) || ciphertext(rest)
	const ephKeySize = 32
	const gcmNonceSize = 12 // standard AES-GCM nonce
	minLen := ephKeySize + gcmNonceSize + 1
	if len(combined) < minLen {
		return nil, fmt.Errorf("file command payload too short: %d bytes", len(combined))
	}

	box := &ecdhgcm.SealedBox{
		EphemeralPub: combined[:ephKeySize],
		Nonce:        combined[ephKeySize : ephKeySize+gcmNonceSize],
		Ciphertext:   combined[ephKeySize+gcmNonceSize:],
	}

	plain, err := ecdhgcm.Open(id.BoxPrivateKey, box, fileCommandKeyLabel)
	if err != nil {
		return nil, fmt.Errorf("decrypt file command payload: %w", err)
	}

	var payload domain.FileCommandPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal file command payload: %w", err)
	}

	return &payload, nil
}
