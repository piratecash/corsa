package gazeta

import (
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"corsa/internal/core/identity"
	"corsa/internal/crypto/ecdhgcm"
)

type Notice struct {
	ID         string
	Ciphertext string
	ExpiresAt  time.Time
}

type PlainNotice struct {
	Topic     string    `json:"topic"`
	From      string    `json:"from"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type sealedNotice struct {
	Ephemeral string `json:"ephemeral"`
	Nonce     string `json:"nonce"`
	Data      string `json:"data"`
}

const gazetaKeyLabel = "gazeta-v1"

func EncryptForRecipient(recipientBoxKeyBase64, topic, from, body string) (string, error) {
	recipientBoxKey, err := base64.StdEncoding.DecodeString(recipientBoxKeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode recipient box key: %w", err)
	}

	curve := ecdh.X25519()
	recipientKey, err := curve.NewPublicKey(recipientBoxKey)
	if err != nil {
		return "", fmt.Errorf("create recipient public key: %w", err)
	}

	plain, err := json.Marshal(PlainNotice{
		Topic:     topic,
		From:      from,
		Body:      body,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("marshal plain notice: %w", err)
	}

	box, err := ecdhgcm.Seal(recipientKey, plain, gazetaKeyLabel)
	if err != nil {
		return "", err
	}

	sealed, err := json.Marshal(sealedNotice{
		Ephemeral: base64.RawURLEncoding.EncodeToString(box.EphemeralPub),
		Nonce:     base64.RawURLEncoding.EncodeToString(box.Nonce),
		Data:      base64.RawURLEncoding.EncodeToString(box.Ciphertext),
	})
	if err != nil {
		return "", fmt.Errorf("marshal sealed notice: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func DecryptForIdentity(id *identity.Identity, encoded string) (*PlainNotice, error) {
	sealedBytes, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode sealed notice: %w", err)
	}

	var sealed sealedNotice
	if err := json.Unmarshal(sealedBytes, &sealed); err != nil {
		return nil, fmt.Errorf("unmarshal sealed notice: %w", err)
	}

	ephemeralBytes, err := base64.RawURLEncoding.DecodeString(sealed.Ephemeral)
	if err != nil {
		return nil, fmt.Errorf("decode ephemeral key: %w", err)
	}

	nonce, err := base64.RawURLEncoding.DecodeString(sealed.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}

	ciphertext, err := base64.RawURLEncoding.DecodeString(sealed.Data)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	box := &ecdhgcm.SealedBox{
		EphemeralPub: ephemeralBytes,
		Nonce:        nonce,
		Ciphertext:   ciphertext,
	}

	plain, err := ecdhgcm.Open(id.BoxPrivateKey, box, gazetaKeyLabel)
	if err != nil {
		return nil, fmt.Errorf("decrypt notice: %w", err)
	}

	var notice PlainNotice
	if err := json.Unmarshal(plain, &notice); err != nil {
		return nil, fmt.Errorf("unmarshal plain notice: %w", err)
	}

	return &notice, nil
}

func ID(ciphertext string) string {
	sum := sha256.Sum256([]byte(ciphertext))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}
