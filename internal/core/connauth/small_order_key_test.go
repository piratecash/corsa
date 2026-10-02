package connauth

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// TestPrepareAuthRefusesSmallOrderKey is where the ban for a small-order key
// comes from: PrepareAuth is the one check of the peer's OWN key, and its
// error is what the responder charges banIncrementInvalidSig for. The hello
// is the self-consistent universal forgery, so nothing but the key check can
// refuse it — and before that check, nothing did.
func TestPrepareAuthRefusesSmallOrderKey(t *testing.T) {
	t.Parallel()
	key := edforgery.NeutralPublicKey()
	hello := protocol.Frame{
		Type:    "hello",
		Client:  "node",
		Address: identity.Fingerprint(key),
		PubKey:  base64.StdEncoding.EncodeToString(key),
		BoxKey:  base64.StdEncoding.EncodeToString(make([]byte, 32)),
		BoxSig:  base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature()),
	}
	state, err := PrepareAuth(hello)
	if !errors.Is(err, identity.ErrInvalidPublicKey) {
		t.Fatalf("PrepareAuth(small-order hello) = (%v, %v), want ErrInvalidPublicKey", state, err)
	}
}

// TestPrepareAuthAcceptsHonestKey is the compatibility control: the same path
// keeps accepting every honest identity.
func TestPrepareAuthAcceptsHonestKey(t *testing.T) {
	t.Parallel()
	_, hello := testIdentityHello(t)
	if _, err := PrepareAuth(hello); err != nil {
		t.Fatalf("PrepareAuth(honest hello) = %v", err)
	}
}
