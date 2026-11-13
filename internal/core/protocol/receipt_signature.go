package protocol

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"errors"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

var ErrReceiptSignature = errors.New("delivery receipt authentication failed")

// receiptTranscript binds the original message and the receipt's direction.
// ObservedAt is local bookkeeping and is never signed.
func receiptTranscript(network domain.NetworkID, ref domain.MessageRef, receipt DeliveryReceipt) ([]byte, error) {
	if _, err := domain.ParseNetworkID(network.String()); err != nil {
		return nil, ErrReceiptSignature
	}
	if ref.Topic != "dm" || ref.ID == "" || ref.Sender == "" || ref.Recipient == "" ||
		string(receipt.MessageID) != ref.ID || !IsValidReceiptStatus(receipt.Status) ||
		receipt.DeliveredAt.IsZero() || receipt.DeliveredAt.Nanosecond() != 0 {
		return nil, ErrReceiptSignature
	}
	sender, recipient := ref.Recipient, ref.Sender
	if receipt.Status == ReceiptStatusSeenAck {
		sender, recipient = ref.Sender, ref.Recipient
	}
	if receipt.Sender != sender || receipt.Recipient != recipient {
		return nil, ErrReceiptSignature
	}

	var buf bytes.Buffer
	buf.WriteString("corsa-dm-receipt-v1\x00")
	for _, field := range []string{network.String(), ref.Topic, ref.ID, ref.Sender, ref.Recipient, receipt.Status} {
		writeLengthPrefixed(&buf, []byte(field))
	}
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(receipt.DeliveredAt.Unix()))
	writeLengthPrefixed(&buf, timestamp[:])
	return buf.Bytes(), nil
}

// SignDeliveryReceipt signs a receipt for the original message reference.
// DeliveredAt must have the second resolution preserved by receipt frames.
func SignDeliveryReceipt(network domain.NetworkID, ref domain.MessageRef, receipt DeliveryReceipt, signer *identity.Identity) (string, error) {
	transcript, err := receiptTranscript(network, ref, receipt)
	if err != nil || signer.Address != receipt.Sender {
		return "", ErrReceiptSignature
	}
	return identity.SignPayload(signer, transcript), nil
}

// VerifyDeliveryReceipt verifies the receipt author, even when the frame
// arrived through a different authenticated peer.
func VerifyDeliveryReceipt(network domain.NetworkID, ref domain.MessageRef, receipt DeliveryReceipt, publicKeyBase64, signatureBase64 string) error {
	// The signature also names the exact receipt in ack_delete. Require one
	// wire spelling, including no ignored line breaks, before any decoding.
	if len(publicKeyBase64) != base64.StdEncoding.EncodedLen(ed25519.PublicKeySize) ||
		len(signatureBase64) != base64.RawURLEncoding.EncodedLen(ed25519.SignatureSize) {
		return ErrReceiptSignature
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(publicKeyBase64)
	if err != nil {
		return ErrReceiptSignature
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(signatureBase64)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return ErrReceiptSignature
	}
	transcript, err := receiptTranscript(network, ref, receipt)
	if err != nil {
		return err
	}
	key, err := identity.ParsePublicKey(publicKey)
	if err != nil || key.Fingerprint() != receipt.Sender {
		return ErrReceiptSignature
	}
	if !key.Verify(transcript, signature) {
		return ErrReceiptSignature
	}
	return nil
}
