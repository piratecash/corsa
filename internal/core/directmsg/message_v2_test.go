package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestV2EnvelopeBinding(t *testing.T) {
	newIdentity := func() *identity.Identity {
		t.Helper()
		id, err := identity.Generate()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	author, recipient, relay := newIdentity(), newIdentity(), newIdentity()
	binding := V2Binding{
		Network: "testnet", ID: "message-1", Topic: "dm", Flag: "sender-delete",
		Sender: author.Address, Recipient: recipient.Address,
	}
	metadata := V2Metadata{V2Binding: binding, CreatedAt: 1700000000, ExpiresAt: 1700000300}
	address := domain.DMRecipient{
		Address:      domain.PeerIdentityFromWire(recipient.Address),
		BoxKeyBase64: identity.BoxPublicKeyBase64(recipient.BoxPublicKey),
	}
	wire, err := SealV2(author, address, metadata, PlainMessage{Body: "author text"})
	if err != nil {
		t.Fatal(err)
	}
	pub := identity.PublicKeyBase64(author.PublicKey)

	t.Run("author and recipient", func(t *testing.T) {
		got, err := VerifyV2(binding, pub, wire)
		if err != nil || got != metadata {
			t.Fatalf("VerifyV2 = %+v, %v", got, err)
		}
		for _, id := range []*identity.Identity{author, recipient} {
			msg, err := OpenV2(id, binding, pub, wire)
			if err != nil || msg.Body != "author text" || !msg.CreatedAt.Equal(time.Unix(metadata.CreatedAt, 0)) {
				t.Fatalf("OpenV2 = %+v, %v", msg, err)
			}
		}
	})

	for name, alter := range map[string]func(*V2Binding){
		"network":   func(b *V2Binding) { b.Network = "other" },
		"id":        func(b *V2Binding) { b.ID = "message-2" },
		"topic":     func(b *V2Binding) { b.Topic = "dm-control" },
		"flag":      func(b *V2Binding) { b.Flag = "immutable" },
		"sender":    func(b *V2Binding) { b.Sender = relay.Address },
		"recipient": func(b *V2Binding) { b.Recipient = relay.Address },
	} {
		t.Run("changed "+name, func(t *testing.T) {
			expected := binding
			alter(&expected)
			if _, err := VerifyV2(expected, pub, wire); !errors.Is(err, ErrEnvelopeAuth) {
				t.Fatalf("VerifyV2 = %v, want authentication error", err)
			}
		})
	}

	raw, err := base64.RawURLEncoding.DecodeString(wire)
	if err != nil {
		t.Fatal(err)
	}
	var original v2Envelope
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	encode := func(t *testing.T, envelope v2Envelope) string {
		t.Helper()
		raw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}

	t.Run("changed deadline", func(t *testing.T) {
		modified := original
		modified.Payload.Metadata.ExpiresAt++
		if _, err := VerifyV2(binding, pub, encode(t, modified)); !errors.Is(err, ErrEnvelopeAuth) {
			t.Fatalf("VerifyV2 = %v, want authentication error", err)
		}
	})

	for _, reflect := range []bool{false, true} {
		name := "relay re-signs recipient copy"
		if reflect {
			name = "relay reflects author copy"
		}
		t.Run(name, func(t *testing.T) {
			forged := original
			target := recipient
			if reflect {
				target = author
				forged.Payload.Recipient = original.Payload.Sender
			}
			forged.Payload.Metadata.Sender = relay.Address
			forged.Payload.Metadata.Recipient = target.Address
			signed, err := v2SignatureInput(forged.Payload)
			if err != nil {
				t.Fatal(err)
			}
			forged.Signature = identity.SignPayload(relay, signed)
			relayKey := identity.PublicKeyBase64(relay.PublicKey)
			encoded := encode(t, forged)
			if _, err := VerifyV2(forged.Payload.Metadata.V2Binding, relayKey, encoded); err != nil {
				t.Fatalf("forgery fixture must have a valid outer signature: %v", err)
			}
			if _, err := OpenV2(target, forged.Payload.Metadata.V2Binding, relayKey, encoded); !errors.Is(err, ErrSealedUnreadable) {
				t.Fatalf("OpenV2 = %v, want unreadable sealed copy", err)
			}
		})
	}

	t.Run("duplicate JSON field", func(t *testing.T) {
		duplicate := append([]byte(`{"signature":"",`), raw[1:]...)
		if _, err := VerifyV2(binding, pub, base64.RawURLEncoding.EncodeToString(duplicate)); !errors.Is(err, ErrEnvelopeMalformed) {
			t.Fatalf("VerifyV2 = %v, want malformed envelope", err)
		}
	})

	t.Run("self message cannot swap roles", func(t *testing.T) {
		self := metadata
		self.Recipient = author.Address
		selfAddress := domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(author.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(author.BoxPublicKey),
		}
		wire, err := SealV2(author, selfAddress, self, PlainMessage{Body: "note"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenV2(author, self.V2Binding, pub, wire); err != nil {
			t.Fatal(err)
		}
		payload, err := verifyV2(self.V2Binding, pub, wire)
		if err != nil {
			t.Fatal(err)
		}
		payload.Recipient, payload.Sender = payload.Sender, payload.Recipient
		signed, err := v2SignatureInput(payload)
		if err != nil {
			t.Fatal(err)
		}
		swapped := encode(t, v2Envelope{Payload: payload, Signature: identity.SignPayload(author, signed)})
		if _, err := OpenV2(author, self.V2Binding, pub, swapped); !errors.Is(err, ErrSealedUnreadable) {
			t.Fatalf("OpenV2 = %v, want unreadable sealed copy", err)
		}
	})
}
