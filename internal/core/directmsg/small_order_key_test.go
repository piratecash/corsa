package directmsg

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// TestVerifyEnvelopeRefusesSmallOrderSender is the DM half of the universal
// forgery: an envelope "from" the neutral key's fingerprint, signed with the
// signature stdlib accepts under that key for any message. Before the key
// check, VerifyEnvelope accepted it, so anyone could post DMs as that address.
func TestVerifyEnvelopeRefusesSmallOrderSender(t *testing.T) {
	t.Parallel()
	recipient := newEnvelopeIdentity(t)
	senderKey := edforgery.NeutralPublicKey()
	senderAddress := identity.Fingerprint(senderKey)

	encoded, err := json.Marshal(sealedEnvelope{
		Version:   "dm-v1",
		From:      senderAddress,
		To:        recipient.Address,
		Recipient: sealedPart{Ephemeral: "e", Nonce: "n", Data: "d"},
		Sender:    sealedPart{Ephemeral: "e", Nonce: "n", Data: "d"},
		Signature: base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature()),
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	body := base64.RawURLEncoding.EncodeToString(encoded)

	err = VerifyEnvelope(senderAddress, base64.StdEncoding.EncodeToString(senderKey), recipient.Address, body)
	if !errors.Is(err, ErrEnvelopeAuth) || !errors.Is(err, identity.ErrInvalidPublicKey) {
		t.Fatalf("VerifyEnvelope(forged) = %v, want ErrEnvelopeAuth carrying ErrInvalidPublicKey", err)
	}
}
