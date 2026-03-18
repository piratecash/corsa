package gazeta

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"corsa/internal/core/identity"
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

	ephemeralKey, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate ephemeral key: %w", err)
	}

	sharedSecret, err := ephemeralKey.ECDH(recipientKey)
	if err != nil {
		return "", fmt.Errorf("derive shared secret: %w", err)
	}

	block, err := aes.NewCipher(deriveKey(sharedSecret))
	if err != nil {
		return "", fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create gcm: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
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

	ciphertext := gcm.Seal(nil, nonce, plain, nil)
	sealed, err := json.Marshal(sealedNotice{
		Ephemeral: base64.RawURLEncoding.EncodeToString(ephemeralKey.PublicKey().Bytes()),
		Nonce:     base64.RawURLEncoding.EncodeToString(nonce),
		Data:      base64.RawURLEncoding.EncodeToString(ciphertext),
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

	curve := ecdh.X25519()
	ephemeralBytes, err := base64.RawURLEncoding.DecodeString(sealed.Ephemeral)
	if err != nil {
		return nil, fmt.Errorf("decode ephemeral key: %w", err)
	}

	ephemeralKey, err := curve.NewPublicKey(ephemeralBytes)
	if err != nil {
		return nil, fmt.Errorf("restore ephemeral key: %w", err)
	}

	sharedSecret, err := id.BoxPrivateKey.ECDH(ephemeralKey)
	if err != nil {
		return nil, fmt.Errorf("derive shared secret: %w", err)
	}

	block, err := aes.NewCipher(deriveKey(sharedSecret))
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create gcm: %w", err)
	}

	nonce, err := base64.RawURLEncoding.DecodeString(sealed.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(sealed.Data)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
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

func deriveKey(sharedSecret []byte) []byte {
	sum := sha256.Sum256(append([]byte("gazeta-v1"), sharedSecret...))
	return sum[:]
}
