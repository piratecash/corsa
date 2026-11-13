package protocol

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestSignedDeliveryReceiptBindsOriginalMessage(t *testing.T) {
	generate := func() *identity.Identity {
		t.Helper()
		id, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	author, recipient, relay := generate(), generate(), generate()
	network, err := domain.ParseNetworkID("testnet")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.MessageRef{Topic: "dm", ID: "message-1", Sender: author.Address, Recipient: recipient.Address}
	for _, status := range []string{ReceiptStatusDelivered, ReceiptStatusSeen, ReceiptStatusSeenAck} {
		t.Run(status, func(t *testing.T) {
			signer, target := recipient, author
			if status == ReceiptStatusSeenAck {
				signer, target = author, recipient
			}
			r := DeliveryReceipt{MessageID: MessageID(ref.ID), Sender: signer.Address, Recipient: target.Address,
				Status: status, DeliveredAt: time.Unix(1700000000, 0)}
			signature, err := SignDeliveryReceipt(network, ref, r, signer)
			if err != nil {
				t.Fatal(err)
			}
			pub := identity.PublicKeyBase64(signer.PublicKey)
			r.ObservedAt = r.DeliveredAt.Add(time.Hour)
			if err := VerifyDeliveryReceipt(network, ref, r, pub, signature); err != nil {
				t.Fatal(err)
			}
			if err := VerifyDeliveryReceipt(network, ref, r, pub, signature+"\n"); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("non-canonical signature: %v", err)
			}
			if err := VerifyDeliveryReceipt(network, ref, r, pub+"\n", signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("non-canonical public key: %v", err)
			}
			if err := VerifyDeliveryReceipt(network, ref, r, identity.PublicKeyBase64(relay.PublicKey), signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("relay key: %v", err)
			}
			if _, err := SignDeliveryReceipt(network, ref, r, relay); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("relay signing: %v", err)
			}
			if err := VerifyDeliveryReceipt(network, ref, r, pub, ""); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("missing signature: %v", err)
			}
			wrongRef, wrongReceipt := ref, r
			wrongRef.ID, wrongReceipt.MessageID = "message-2", "message-2"
			if err := VerifyDeliveryReceipt(network, wrongRef, wrongReceipt, pub, signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("changed ID: %v", err)
			}
			wrongReceipt = r
			wrongReceipt.DeliveredAt = r.DeliveredAt.Add(time.Second)
			if err := VerifyDeliveryReceipt(network, ref, wrongReceipt, pub, signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("changed timestamp: %v", err)
			}
			wrongReceipt = r
			wrongReceipt.Status = ReceiptStatusDelivered
			if status == ReceiptStatusDelivered {
				wrongReceipt.Status = ReceiptStatusSeen
			}
			if err := VerifyDeliveryReceipt(network, ref, wrongReceipt, pub, signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("changed status: %v", err)
			}
			if err := VerifyDeliveryReceipt(domain.NetworkID("othernet"), ref, r, pub, signature); !errors.Is(err, ErrReceiptSignature) {
				t.Fatalf("changed network: %v", err)
			}
		})
	}
}

func TestReceiptProofFieldsRoundTrip(t *testing.T) {
	t.Run("legacy omits proof fields", func(t *testing.T) {
		frame := Frame{Type: "delivery_receipts", Receipts: []ReceiptFrame{{MessageID: "legacy"}}}
		line, err := MarshalFrameLine(frame)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, `"receipt_proof"`) || strings.Contains(line, `"signature"`) || strings.Contains(line, `"pubkey"`) {
			t.Fatal("legacy frame includes optional proof fields")
		}
		decoded, err := ParseFrameLine(line)
		if err != nil || len(decoded.Receipts) != 1 || decoded.Receipts[0].MessageID != "legacy" {
			t.Fatalf("legacy frame round trip: %+v, %v", decoded.Receipts, err)
		}
	})

	t.Run("signed receipt keeps proof fields", func(t *testing.T) {
		frame := Frame{Type: "push_delivery_receipt", Receipt: &ReceiptFrame{
			MessageID: "current", SignerPublicKey: "example-key", Signature: "example-proof",
		}}
		line, err := MarshalFrameLine(frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := ParseFrameLine(line)
		if err != nil || decoded.Receipt == nil || *decoded.Receipt != *frame.Receipt {
			t.Fatalf("signed receipt frame round trip: %+v, %v", decoded.Receipt, err)
		}
	})

	t.Run("ack keeps exact proof", func(t *testing.T) {
		frame := Frame{Type: "ack_delete", ID: "current", ReceiptSender: "example-sender", ReceiptProof: "example-proof"}
		line, err := MarshalFrameLine(frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := ParseFrameLine(line)
		if err != nil || decoded.ID != frame.ID || decoded.ReceiptSender != frame.ReceiptSender || decoded.ReceiptProof != frame.ReceiptProof {
			t.Fatalf("ack frame round trip: %+v, %v", decoded, err)
		}
	})
}
