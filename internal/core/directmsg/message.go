package directmsg

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"corsa/internal/core/identity"
)

type PlainMessage struct {
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type sealedEnvelope struct {
	Version   string     `json:"version"`
	From      string     `json:"from"`
	To        string     `json:"to"`
	Recipient sealedPart `json:"recipient"`
	Sender    sealedPart `json:"sender"`
	Signature string     `json:"signature"`
}

type sealedPart struct {
	Ephemeral string `json:"ephemeral"`
	Nonce     string `json:"nonce"`
	Data      string `json:"data"`
}

func EncryptForParticipants(sender *identity.Identity, recipientAddress, recipientBoxKeyBase64, body string) (string, error) {
	recipientBoxKey, err := base64.StdEncoding.DecodeString(recipientBoxKeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode recipient box key: %w", err)
	}

	curve := ecdh.X25519()
	recipientKey, err := curve.NewPublicKey(recipientBoxKey)
	if err != nil {
		return "", fmt.Errorf("create recipient public key: %w", err)
	}

	plain, err := json.Marshal(PlainMessage{
		Body:      body,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return "", fmt.Errorf("marshal direct message: %w", err)
	}

	recipientPart, err := sealForPublicKey(recipientKey, plain)
	if err != nil {
		return "", err
	}

	senderPart, err := sealForPublicKey(sender.BoxPrivateKey.PublicKey(), plain)
	if err != nil {
		return "", err
	}

	unsigned := sealedEnvelope{
		Version:   "dm-v1",
		From:      sender.Address,
		To:        recipientAddress,
		Recipient: recipientPart,
		Sender:    senderPart,
	}
	unsignedBytes, err := marshalUnsignedEnvelope(unsigned)
	if err != nil {
		return "", err
	}

	signature := ed25519.Sign(sender.PrivateKey, unsignedBytes)

	encoded, err := json.Marshal(sealedEnvelope{
		Version:   unsigned.Version,
		From:      unsigned.From,
		To:        unsigned.To,
		Recipient: unsigned.Recipient,
		Sender:    unsigned.Sender,
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	})
	if err != nil {
		return "", fmt.Errorf("marshal direct envelope: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func DecryptForIdentity(id *identity.Identity, senderAddress, senderPublicKeyBase64, recipientAddress, encoded string) (*PlainMessage, error) {
	envelope, err := verifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded)
	if err != nil {
		return nil, err
	}

	parts := []sealedPart{envelope.Recipient, envelope.Sender}
	for _, part := range parts {
		message, err := openPart(id, part)
		if err == nil {
			return message, nil
		}
	}

	return nil, fmt.Errorf("decrypt direct message: no readable payload")
}

func VerifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded string) error {
	_, err := verifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded)
	return err
}

func verifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded string) (sealedEnvelope, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return sealedEnvelope{}, fmt.Errorf("decode direct envelope: %w", err)
	}

	var envelope sealedEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return sealedEnvelope{}, fmt.Errorf("unmarshal direct envelope: %w", err)
	}

	if envelope.Version != "dm-v1" {
		return sealedEnvelope{}, fmt.Errorf("unsupported direct message version: %s", envelope.Version)
	}
	if envelope.From != senderAddress {
		return sealedEnvelope{}, fmt.Errorf("sender mismatch in envelope")
	}
	if envelope.To != recipientAddress {
		return sealedEnvelope{}, fmt.Errorf("recipient mismatch in envelope")
	}

	senderPublicKey, err := decodeSenderPublicKey(senderAddress, senderPublicKeyBase64)
	if err != nil {
		return sealedEnvelope{}, err
	}

	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return sealedEnvelope{}, fmt.Errorf("decode signature: %w", err)
	}

	unsignedBytes, err := marshalUnsignedEnvelope(sealedEnvelope{
		Version:   envelope.Version,
		From:      envelope.From,
		To:        envelope.To,
		Recipient: envelope.Recipient,
		Sender:    envelope.Sender,
	})
	if err != nil {
		return sealedEnvelope{}, err
	}

	if !ed25519.Verify(senderPublicKey, unsignedBytes, signature) {
		return sealedEnvelope{}, fmt.Errorf("invalid direct message signature")
	}

	return envelope, nil
}

func marshalUnsignedEnvelope(envelope sealedEnvelope) ([]byte, error) {
	payload, err := json.Marshal(sealedEnvelope{
		Version:   envelope.Version,
		From:      envelope.From,
		To:        envelope.To,
		Recipient: envelope.Recipient,
		Sender:    envelope.Sender,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal unsigned envelope: %w", err)
	}
	return payload, nil
}

func decodeSenderPublicKey(senderAddress, encoded string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode sender public key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid sender public key size: %d", len(raw))
	}

	publicKey := ed25519.PublicKey(raw)
	if identity.Fingerprint(publicKey) != senderAddress {
		return nil, fmt.Errorf("sender public key does not match sender address")
	}

	return publicKey, nil
}

func sealForPublicKey(publicKey *ecdh.PublicKey, plain []byte) (sealedPart, error) {
	curve := ecdh.X25519()
	ephemeralKey, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return sealedPart{}, fmt.Errorf("generate ephemeral key: %w", err)
	}

	sharedSecret, err := ephemeralKey.ECDH(publicKey)
	if err != nil {
		return sealedPart{}, fmt.Errorf("derive shared secret: %w", err)
	}

	block, err := aes.NewCipher(deriveKey(sharedSecret))
	if err != nil {
		return sealedPart{}, fmt.Errorf("create aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return sealedPart{}, fmt.Errorf("create gcm: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return sealedPart{}, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, plain, nil)
	return sealedPart{
		Ephemeral: base64.RawURLEncoding.EncodeToString(ephemeralKey.PublicKey().Bytes()),
		Nonce:     base64.RawURLEncoding.EncodeToString(nonce),
		Data:      base64.RawURLEncoding.EncodeToString(ciphertext),
	}, nil
}

func openPart(id *identity.Identity, part sealedPart) (*PlainMessage, error) {
	curve := ecdh.X25519()
	ephemeralBytes, err := base64.RawURLEncoding.DecodeString(part.Ephemeral)
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

	nonce, err := base64.RawURLEncoding.DecodeString(part.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}

	ciphertext, err := base64.RawURLEncoding.DecodeString(part.Data)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}

	var message PlainMessage
	if err := json.Unmarshal(plain, &message); err != nil {
		return nil, fmt.Errorf("unmarshal direct message: %w", err)
	}

	return &message, nil
}

func deriveKey(sharedSecret []byte) []byte {
	sum := sha256.Sum256(append([]byte("corsa-dm-v1"), sharedSecret...))
	return sum[:]
}
