package directmsg

import (
	"bytes"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

const envelopeV2 = "dm-v2"

// V2EnvelopePrefix is the encoded prefix of a canonical dm-v2 envelope.
// It can classify the family but does not authenticate the envelope.
const V2EnvelopePrefix = "eyJwYXlsb2FkIjp7"

// HasV2EnvelopePrefix classifies the wire family without authenticating it.
// Use it for refusal or dispatch, never to establish sender identity.
func HasV2EnvelopePrefix(encoded []byte) bool {
	return bytes.HasPrefix(encoded, []byte(V2EnvelopePrefix))
}

// V2Binding identifies the author-controlled fields that must agree with
// the surrounding message. A relay's timestamps and hop counters are separate.
type V2Binding struct {
	Network   string `json:"network"`
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Flag      string `json:"flag"`
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
}

// V2Metadata carries the author's time and deadline in Unix seconds.
// Ingress owns the time policy. History readers can authenticate expired rows.
type V2Metadata struct {
	V2Binding
	CreatedAt int64 `json:"created_at"`
	ExpiresAt int64 `json:"expires_at"`
}

type v2Payload struct {
	Version   string     `json:"version"`
	Metadata  V2Metadata `json:"metadata"`
	Recipient sealedPart `json:"recipient"`
	Sender    sealedPart `json:"sender"`
}

type v2Envelope struct {
	Payload   v2Payload `json:"payload"`
	Signature string    `json:"signature"`
}

func (m V2Metadata) valid() bool {
	return m.Network != "" && m.ID != "" && m.Topic != "" && m.Flag != "" &&
		m.Sender != "" && m.Recipient != "" && m.CreatedAt > 0 && m.ExpiresAt > m.CreatedAt
}

func v2SealLabel(metadata V2Metadata, role string) (string, error) {
	context, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return "corsa-dm-v2-seal\x00" + role + "\x00" + string(context), nil
}

func v2SignatureInput(payload v2Payload) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return append([]byte("corsa-dm-v2-envelope\x00"), body...), nil
}

// SealV2 creates a signed envelope with independently sealed copies for
// the author and recipient.
func SealV2(sender *identity.Identity, recipient domain.DMRecipient, metadata V2Metadata, message PlainMessage) (string, error) {
	if !metadata.valid() || metadata.Sender != sender.Address || metadata.Recipient != recipient.Address.String() {
		return "", fmt.Errorf("%w: invalid dm-v2 metadata", ErrEnvelopeAuth)
	}
	rawKey, err := base64.StdEncoding.DecodeString(recipient.BoxKeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode recipient box key: %w", err)
	}
	key, err := ecdh.X25519().NewPublicKey(rawKey)
	if err != nil {
		return "", fmt.Errorf("create recipient box key: %w", err)
	}
	message.CreatedAt = time.Unix(metadata.CreatedAt, 0).UTC()
	plain, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	recipientLabel, err := v2SealLabel(metadata, "recipient")
	if err != nil {
		return "", err
	}
	recipientPart, err := sealPart(key, plain, recipientLabel)
	if err != nil {
		return "", err
	}
	senderLabel, err := v2SealLabel(metadata, "sender")
	if err != nil {
		return "", err
	}
	senderPart, err := sealPart(sender.BoxPrivateKey.PublicKey(), plain, senderLabel)
	if err != nil {
		return "", err
	}
	payload := v2Payload{Version: envelopeV2, Metadata: metadata, Recipient: recipientPart, Sender: senderPart}
	signed, err := v2SignatureInput(payload)
	if err != nil {
		return "", err
	}
	wire, err := json.Marshal(v2Envelope{Payload: payload, Signature: identity.SignPayload(sender, signed)})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(wire), nil
}

// VerifyV2 returns authenticated author metadata for the supplied binding.
func VerifyV2(expected V2Binding, senderPublicKeyBase64, encoded string) (V2Metadata, error) {
	payload, err := verifyV2(expected, senderPublicKeyBase64, encoded)
	return payload.Metadata, err
}

func verifyV2(expected V2Binding, senderPublicKeyBase64, encoded string) (v2Payload, error) {
	envelope, err := decodeV2(encoded)
	if err != nil {
		return v2Payload{}, err
	}
	return authenticateV2(expected, senderPublicKeyBase64, envelope)
}

// VerifyStoredV2 checks the canonical v2 family in an outbox row. The flag
// comes from the signed payload because the chatlog stores local policy.
// The boolean remains true when a recognized envelope fails verification.
func VerifyStoredV2(expected V2Binding, senderPublicKeyBase64, encoded string) (V2Metadata, bool, error) {
	if !strings.HasPrefix(encoded, V2EnvelopePrefix) {
		return V2Metadata{}, false, nil
	}
	payload, err := verifyV2WithStoredFlag(expected, senderPublicKeyBase64, encoded)
	return payload.Metadata, true, err
}

func verifyV2WithStoredFlag(expected V2Binding, senderPublicKeyBase64, encoded string) (v2Payload, error) {
	envelope, err := decodeV2(encoded)
	if err != nil {
		return v2Payload{}, err
	}
	expected.Flag = envelope.Payload.Metadata.Flag
	return authenticateV2(expected, senderPublicKeyBase64, envelope)
}

func decodeV2(encoded string) (v2Envelope, error) {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return v2Envelope{}, fmt.Errorf("%w: decode dm-v2: %v", ErrEnvelopeMalformed, err)
	}
	var envelope v2Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return v2Envelope{}, fmt.Errorf("%w: decode dm-v2 JSON: %v", ErrEnvelopeMalformed, err)
	}
	// Require the wire representation emitted by SealV2. This rules out
	// ambiguous encodings, including duplicate and unknown fields.
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(raw, canonical) || envelope.Payload.Version != envelopeV2 {
		return v2Envelope{}, fmt.Errorf("%w: noncanonical dm-v2 envelope", ErrEnvelopeMalformed)
	}
	return envelope, nil
}

func authenticateV2(expected V2Binding, senderPublicKeyBase64 string, envelope v2Envelope) (v2Payload, error) {
	payload := envelope.Payload
	if !payload.Metadata.valid() || payload.Metadata.V2Binding != expected {
		return v2Payload{}, fmt.Errorf("%w: dm-v2 binding mismatch", ErrEnvelopeAuth)
	}
	key, err := decodeSenderPublicKey(expected.Sender, senderPublicKeyBase64)
	if err != nil {
		return v2Payload{}, err
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(envelope.Signature)
	if err != nil {
		return v2Payload{}, fmt.Errorf("%w: decode dm-v2 signature: %v", ErrEnvelopeMalformed, err)
	}
	signed, err := v2SignatureInput(payload)
	if err != nil {
		return v2Payload{}, err
	}
	if !key.Verify(signed, signature) {
		return v2Payload{}, fmt.Errorf("%w: invalid dm-v2 signature", ErrEnvelopeAuth)
	}
	return payload, nil
}

// OpenV2 authenticates the envelope and opens the caller's role-specific copy.
// A self-addressed message uses the recipient copy.
func OpenV2(id *identity.Identity, expected V2Binding, senderPublicKeyBase64, encoded string) (*PlainMessage, error) {
	payload, err := verifyV2(expected, senderPublicKeyBase64, encoded)
	if err != nil {
		return nil, err
	}
	return openVerifiedV2(id, payload)
}

// OpenStoredV2 authenticates a stored envelope using its signed author flag.
// A caller supplies all other binding fields from the exact row reference.
func OpenStoredV2(id *identity.Identity, expected V2Binding, senderPublicKeyBase64, encoded string) (*PlainMessage, error) {
	payload, err := verifyV2WithStoredFlag(expected, senderPublicKeyBase64, encoded)
	if err != nil {
		return nil, err
	}
	return openVerifiedV2(id, payload)
}

func openVerifiedV2(id *identity.Identity, payload v2Payload) (*PlainMessage, error) {
	expected := payload.Metadata.V2Binding
	var role string
	var part sealedPart
	switch id.Address {
	case expected.Recipient:
		role, part = "recipient", payload.Recipient
	case expected.Sender:
		role, part = "sender", payload.Sender
	default:
		return nil, fmt.Errorf("%w: identity is outside dm-v2 pair", ErrEnvelopeAuth)
	}
	label, err := v2SealLabel(payload.Metadata, role)
	if err != nil {
		return nil, err
	}
	message, err := openLabeledPart(id, part, label)
	if err != nil {
		return nil, ErrSealedUnreadable
	}
	message.CreatedAt = time.Unix(payload.Metadata.CreatedAt, 0).UTC()
	return message, nil
}
