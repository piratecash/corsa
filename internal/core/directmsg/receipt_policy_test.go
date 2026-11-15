package directmsg

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestDMV1RequiredReceiptsAtRelayBodyLimit(t *testing.T) {
	t.Parallel()
	sender, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	to := domain.DMRecipient{Address: domain.PeerIdentityFromWire(reader.Address), BoxKeyBase64: identity.BoxPublicKeyBase64(reader.BoxPublicKey)}
	ref := v1MessageRef{Topic: "dm", ID: "f8139aa9-49df-4f51-80a2-40eaf440b6f1", Sender: sender.Address, Recipient: reader.Address}
	msgWithSize := func(size int) domain.OutgoingDM {
		return domain.OutgoingDM{Body: strings.Repeat("a", size)}
	}
	const limit = domain.MaxRelayMessageBodyBytes
	// Leave room for encoded timestamp lengths to vary between seals.
	boundary := sort.Search(limit, func(size int) bool {
		encoded, err := encryptV1WithReceiptPolicy(sender, to, msgWithSize(size), &ref, true)
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded) > limit-48
	})
	msg := msgWithSize(boundary - 1)
	encoded, err := EncryptForMessageWithSignedReceipts(sender, to, msg, ref.Topic, ref.ID, true)
	if err != nil || len(encoded) > limit || len(encoded) < limit-96 {
		t.Fatalf("protected envelope near limit: %d bytes, %v", len(encoded), err)
	}
	key := identity.PublicKeyBase64(sender.PublicKey)
	for _, participant := range []*identity.Identity{sender, reader} {
		plain, err := DecryptForMessage(participant, domain.MessageRef(ref), key, encoded)
		if err != nil || !plain.SignedReceiptRequired || plain.Body != msg.Body {
			t.Fatalf("protected payload or policy lost on decrypt: %v", err)
		}
	}
	wrongRef := domain.MessageRef(ref)
	wrongRef.ID = "different-message-id"
	if required, err := DataEnvelopeReceiptPolicy(reader, wrongRef, key, encoded); required || !errors.Is(err, ErrEnvelopeAuth) {
		t.Fatalf("receipt policy must be bound to the original message: required=%v, err=%v", required, err)
	}

	larger := msgWithSize(boundary + 32)
	legacy, err := EncryptForMessageWithSignedReceipts(sender, to, larger, ref.Topic, ref.ID, false)
	if err != nil || len(legacy) > limit {
		t.Fatalf("unprotected envelope must still fit: %d bytes, %v", len(legacy), err)
	}
	if encoded, err := EncryptForMessageWithSignedReceipts(sender, to, larger, ref.Topic, ref.ID, true); err == nil || encoded != "" {
		t.Fatalf("oversized protected envelope must fail explicitly: %d bytes, %v", len(encoded), err)
	}
}
