package dmcontrol

import (
	"crypto/ecdh"
	"encoding/base64"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/crypto/ecdhgcm"
)

// seal.go hides the payload from the relays that carry it, and binds it to the
// pair it was written for.
//
// # Why the ciphertext carries no signature but is still not transplantable
//
// A routed datagram is Ed25519-signed by its source, and the signed transcript
// covers the payload bytes (protocol.BuildDatagramTranscript). The receiver
// only ever sees a payload whose signature verified and whose signer matched
// src, and it reads the author from that signature.
//
// That alone is NOT enough, and the first cut of this file wrongly said it was.
// The two inputs are independent: a relay that carries A's frame to C can lift
// the ciphertext out, put it in a frame it signs ITSELF and send that to C. C
// would open it — the seal is to C's box key either way — and attribute every
// fact inside to the relay. It cannot read what it is asserting, but it does
// not need to: the facts land under its own key carrying A's clock values, and
// because the merge keeps the highest clock per key, one such frame silences
// that key for good.
//
// So the sender and the recipient are bound into the KDF label. A ciphertext
// written for the pair (A→C) derives a different key from one claimed as
// (relay→C), and the transplanted frame simply fails to open. Binding through
// the label rather than through a second signature keeps ONE answer to "who
// wrote this" — the frame signature — instead of two that can disagree.
//
// The seal is one-directional, for the recipient's box key and not also the
// sender's: a control command is transient traffic rather than a stored
// message, and sealing it to ourselves would double the frame for no reader.

// keyLabelPrefix is the domain separation string. Distinct from every other
// corsa label so a payload cannot be replayed into another protocol's opener.
const keyLabelPrefix = "corsa-dm-control-v1"

// sealLabel is the per-pair KDF label. The two identities are fixed-width hex
// and separated, so no pair of identities can produce another pair's label.
func sealLabel(sender, recipient domain.PeerIdentity) string {
	return keyLabelPrefix + "|" + sender.String() + "|" + recipient.String()
}

// ephemeralKeyBytes and nonceBytes are the fixed-width prefix of a sealed
// payload: ephemeral X25519 public key, then the AES-GCM nonce, then the
// ciphertext. Fixed width is what lets the reader split the three without a
// length field to get wrong.
const (
	ephemeralKeyBytes = 32
	nonceBytes        = 12
)

// SealOverheadBytes is what sealing adds to the plaintext: the ephemeral key,
// the nonce and the GCM tag. Constant, which is what makes the padded bucket
// (PayloadBucketBytes) a constant frame size rather than an approximate one.
const SealOverheadBytes = ephemeralKeyBytes + nonceBytes + gcmTagBytes

const gcmTagBytes = 16

// Seal encrypts a padded payload for one recipient's box key, bound to the pair
// it travels between.
//
// The result is raw bytes rather than base64: a datagram payload is a byte
// field, and the existing base64 helpers in directmsg exist because their
// transports carry JSON strings. Encoding here would inflate every frame by a
// third for no reader.
func Seal(sender, recipient domain.PeerIdentity, recipientBoxKeyBase64 string, plain []byte) ([]byte, error) {
	if sender.IsZero() || recipient.IsZero() {
		return nil, fmt.Errorf("dmcontrol: sealing needs both ends of the conversation")
	}
	rawKey, err := base64.StdEncoding.DecodeString(recipientBoxKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: decode recipient box key: %w", err)
	}
	recipientKey, err := ecdh.X25519().NewPublicKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: recipient box key: %w", err)
	}
	box, err := ecdhgcm.Seal(recipientKey, plain, sealLabel(sender, recipient))
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: seal payload: %w", err)
	}
	sealed := make([]byte, 0, len(box.EphemeralPub)+len(box.Nonce)+len(box.Ciphertext))
	sealed = append(sealed, box.EphemeralPub...)
	sealed = append(sealed, box.Nonce...)
	sealed = append(sealed, box.Ciphertext...)
	return sealed, nil
}

// Open decrypts a sealed payload with this node's box key.
//
// Sender is who the FRAME SIGNATURE says wrote it. A payload written for
// another pair derives another key and fails here, which is what stops a relay
// re-signing somebody else's ciphertext as its own.
func Open(id *identity.Identity, sender, recipient domain.PeerIdentity, sealed []byte) ([]byte, error) {
	if id == nil {
		return nil, fmt.Errorf("dmcontrol: opening a payload needs a local identity")
	}
	if sender.IsZero() || recipient.IsZero() {
		return nil, fmt.Errorf("dmcontrol: opening a payload needs both ends of the conversation")
	}
	if len(sealed) <= ephemeralKeyBytes+nonceBytes {
		return nil, fmt.Errorf("dmcontrol: sealed payload is %d bytes, too short to hold anything", len(sealed))
	}
	plain, err := ecdhgcm.Open(id.BoxPrivateKey, &ecdhgcm.SealedBox{
		EphemeralPub: sealed[:ephemeralKeyBytes],
		Nonce:        sealed[ephemeralKeyBytes : ephemeralKeyBytes+nonceBytes],
		Ciphertext:   sealed[ephemeralKeyBytes+nonceBytes:],
	}, sealLabel(sender, recipient))
	if err != nil {
		return nil, fmt.Errorf("dmcontrol: open payload: %w", err)
	}
	return plain, nil
}
