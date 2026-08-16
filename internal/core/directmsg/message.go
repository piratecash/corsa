package directmsg

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/crypto/ecdhgcm"
)

type PlainMessage struct {
	Body        string    `json:"body"`
	CreatedAt   time.Time `json:"created_at"`
	ReplyTo     string    `json:"reply_to,omitempty"`
	Command     string    `json:"command,omitempty"`      // e.g. "file_announce"; empty for regular DMs
	CommandData string    `json:"command_data,omitempty"` // JSON-encoded payload; empty for regular DMs
	// RetryOf is the original message id this envelope re-sends after a
	// decrypt-failure recovery (docs/protocol/identity-lookup.md §4.10).
	// Inside the encrypted part, like ReplyTo, and for the same privacy
	// reason: the relation between two ciphertexts is nobody's business
	// but the two endpoints'.
	RetryOf string `json:"retry_of,omitempty"`
}

// Decrypt failures are three DIFFERENT situations with three different
// recoveries, and the §4.10 machinery may only ever act on the last one:
// malformed bytes and a failed authentication carry no proof of who sent
// them (no notice — it would be spendable slander), while an AUTHENTICATED
// envelope whose sealed parts will not open proves the counterparty's key
// changed — the one case worth a decrypt_failed notice.
var (
	// ErrEnvelopeMalformed — the bytes are not a well-formed dm-v1
	// envelope: base64/JSON breakage or an unknown version.
	ErrEnvelopeMalformed = errors.New("direct message: malformed envelope")

	// ErrEnvelopeAuth — structure is fine but authentication failed: the
	// addresses do not match or the signature does not verify.
	ErrEnvelopeAuth = errors.New("direct message: envelope authentication failed")

	// ErrSealedUnreadable — the envelope IS authentic (signature and both
	// addresses verified) but neither sealed part opens with this
	// identity's keys: the confirmed crypto-fail class of §4.10.
	ErrSealedUnreadable = errors.New("direct message: sealed payload unreadable with current keys")
)

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

func EncryptForParticipants(sender *identity.Identity, recipient domain.DMRecipient, msg domain.OutgoingDM) (string, error) {
	recipientBoxKey, err := base64.StdEncoding.DecodeString(recipient.BoxKeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode recipient box key: %w", err)
	}

	curve := ecdh.X25519()
	recipientKey, err := curve.NewPublicKey(recipientBoxKey)
	if err != nil {
		return "", fmt.Errorf("create recipient public key: %w", err)
	}

	plain, err := json.Marshal(PlainMessage{
		Body:        msg.Body,
		CreatedAt:   time.Now().UTC(),
		ReplyTo:     string(msg.ReplyTo),
		Command:     string(msg.Command),
		CommandData: msg.CommandData,
		RetryOf:     string(msg.RetryOf),
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
		To:        recipient.Address.String(),
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

	// The envelope authenticated above; only the sealed halves refused the
	// current keys — the typed class the recovery machinery keys on.
	return nil, ErrSealedUnreadable
}

func VerifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded string) error {
	_, err := verifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded)
	return err
}

func verifyEnvelope(senderAddress, senderPublicKeyBase64, recipientAddress, encoded string) (sealedEnvelope, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return sealedEnvelope{}, fmt.Errorf("%w: decode: %w", ErrEnvelopeMalformed, err)
	}

	var envelope sealedEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return sealedEnvelope{}, fmt.Errorf("%w: unmarshal: %w", ErrEnvelopeMalformed, err)
	}

	if envelope.Version != "dm-v1" {
		return sealedEnvelope{}, fmt.Errorf("%w: unsupported version %s", ErrEnvelopeMalformed, envelope.Version)
	}
	if envelope.From != senderAddress {
		return sealedEnvelope{}, fmt.Errorf("%w: sender mismatch", ErrEnvelopeAuth)
	}
	if envelope.To != recipientAddress {
		return sealedEnvelope{}, fmt.Errorf("%w: recipient mismatch", ErrEnvelopeAuth)
	}

	senderPublicKey, err := decodeSenderPublicKey(senderAddress, senderPublicKeyBase64)
	if err != nil {
		return sealedEnvelope{}, err
	}

	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return sealedEnvelope{}, fmt.Errorf("%w: decode signature: %w", ErrEnvelopeMalformed, err)
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
		return sealedEnvelope{}, fmt.Errorf("%w: invalid signature", ErrEnvelopeAuth)
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

const dmKeyLabel = "corsa-dm-v1"

func sealForPublicKey(publicKey *ecdh.PublicKey, plain []byte) (sealedPart, error) {
	box, err := ecdhgcm.Seal(publicKey, plain, dmKeyLabel)
	if err != nil {
		return sealedPart{}, err
	}

	return sealedPart{
		Ephemeral: base64.RawURLEncoding.EncodeToString(box.EphemeralPub),
		Nonce:     base64.RawURLEncoding.EncodeToString(box.Nonce),
		Data:      base64.RawURLEncoding.EncodeToString(box.Ciphertext),
	}, nil
}

func openPart(id *identity.Identity, part sealedPart) (*PlainMessage, error) {
	ephemeralBytes, err := base64.RawURLEncoding.DecodeString(part.Ephemeral)
	if err != nil {
		return nil, fmt.Errorf("decode ephemeral key: %w", err)
	}

	nonce, err := base64.RawURLEncoding.DecodeString(part.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode nonce: %w", err)
	}

	ciphertext, err := base64.RawURLEncoding.DecodeString(part.Data)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	box := &ecdhgcm.SealedBox{
		EphemeralPub: ephemeralBytes,
		Nonce:        nonce,
		Ciphertext:   ciphertext,
	}

	plain, err := ecdhgcm.Open(id.BoxPrivateKey, box, dmKeyLabel)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}

	var message PlainMessage
	if err := json.Unmarshal(plain, &message); err != nil {
		return nil, fmt.Errorf("unmarshal direct message: %w", err)
	}

	return &message, nil
}
