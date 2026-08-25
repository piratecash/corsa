package dmcontrol

import (
	"bytes"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func sealTestParty(t *testing.T) (*identity.Identity, domain.PeerIdentity, string) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id, domain.PeerIdentityFromWire(id.Address), identity.BoxPublicKeyBase64(id.BoxPublicKey)
}

func TestSealRoundTrip(t *testing.T) {
	sender, senderID, _ := sealTestParty(t)
	recipient, recipientID, recipientKey := sealTestParty(t)
	_ = sender

	plain := []byte("the padded payload, whatever it says")
	sealed, err := Seal(senderID, recipientID, recipientKey, plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(sealed, plain) {
		t.Fatal("the plaintext is readable in the sealed bytes")
	}
	if want := len(plain) + SealOverheadBytes; len(sealed) != want {
		t.Fatalf("sealing %d bytes produced %d, want %d — the padded frame size depends on this being constant",
			len(plain), len(sealed), want)
	}

	opened, err := Open(recipient, senderID, recipientID, sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, plain) {
		t.Fatalf("opened %q, sealed %q", opened, plain)
	}
}

// The pair is bound into the key, so a ciphertext lifted out of A's frame and
// re-sent inside a frame the RELAY signs does not open. Without that binding the
// relay could not read what it was asserting but would not need to: the facts
// would land under its own key carrying A's clock values, and the merge keeps
// the highest clock.
func TestSealDoesNotOpenForAnotherPair(t *testing.T) {
	_, authorID, _ := sealTestParty(t)
	_, relayID, _ := sealTestParty(t)
	recipient, recipientID, recipientKey := sealTestParty(t)

	sealed, err := Seal(authorID, recipientID, recipientKey, []byte("facts"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := Open(recipient, relayID, recipientID, sealed); err == nil {
		t.Fatal("a ciphertext written for one sender opened as another's")
	}
	// And the recipient half of the label is bound too, so a payload addressed
	// elsewhere does not open here even when the box key happens to match.
	_, elsewhere, _ := sealTestParty(t)
	if _, err := Open(recipient, authorID, elsewhere, sealed); err == nil {
		t.Fatal("a ciphertext written for one conversation opened as another's")
	}
	// The genuine pair still opens, so the two refusals above are the binding
	// and not a broken fixture.
	if _, err := Open(recipient, authorID, recipientID, sealed); err != nil {
		t.Fatalf("the real pair could not open its own payload: %v", err)
	}
}

func TestSealRefusesWhatItCannotBind(t *testing.T) {
	_, senderID, _ := sealTestParty(t)
	recipient, recipientID, recipientKey := sealTestParty(t)

	if _, err := Seal(domain.PeerIdentity{}, recipientID, recipientKey, []byte("x")); err == nil {
		t.Fatal("sealed with no sender to bind to")
	}
	if _, err := Seal(senderID, domain.PeerIdentity{}, recipientKey, []byte("x")); err == nil {
		t.Fatal("sealed with no recipient to bind to")
	}
	if _, err := Seal(senderID, recipientID, "not base64", []byte("x")); err == nil {
		t.Fatal("sealed to a key that is not a key")
	}
	if _, err := Open(nil, senderID, recipientID, make([]byte, 128)); err == nil {
		t.Fatal("opened without a local identity")
	}
	// Too short to hold an ephemeral key, a nonce and a byte of ciphertext.
	// Refused by LENGTH and not handed to the AEAD, because the slice
	// expressions that split the three parts would run off the end first.
	// Exactly at the boundary, so the guard is what answers rather than the
	// cipher failing later for its own reasons.
	if _, err := Open(recipient, senderID, recipientID,
		make([]byte, ephemeralKeyBytes+nonceBytes)); err == nil {
		t.Fatal("opened a payload too short to contain one")
	}
}
