package protocol

import (
	"crypto/ecdh"
	"encoding/json"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/crypto/ecdhgcm"
)

// liveness_probe.go is the sealed half of a liveness probe: who is asking, in
// which epoch, and the reciprocity token that proves they hold the key of an
// identity the target knows.
//
// # Why any of this is sealed
//
// The wire form of `get_identity` already has a `requester` triple in the
// clear. Nothing fills it in today, and nothing should: a plaintext requester
// publishes the pair (asker, target) to every hop the frame crosses, which is
// a stronger leak than the `routed` mode the probe avoids for exactly that
// reason. Sealing is what makes it safe to say who is asking at all.
//
// The target's box key is the only key needed, so this works against any
// contact whose identity record we hold — no new handshake, no new key.
//
// # What binds what
//
// The SEAL is bound to the target only, not to the pair. It has to be: the
// frame's `src` is a one-time label rather than an identity, so the target
// cannot know who the asker is until this payload is open. The asker's name
// therefore travels inside, and the token — not the seal — is what makes
// claiming somebody else's name useless.
//
// The TOKEN is bound to the attempt label, and that is what stops the sealed
// blob from being a bearer credential. Before it was, and the hole was real:
// any hop on the path could copy the ciphertext into a get_identity of its
// own with a fresh label and harvest proofs for the rest of the epoch window.
// The claim is now valid for exactly one question.

// livenessProbeSealLabel domain-separates this ciphertext from every other
// sealed payload in the system.
const livenessProbeSealLabel = "corsa-liveness-probe-v1"

// LivenessProbeClaim is what the sealed part of a probe says.
type LivenessProbeClaim struct {
	// Asker is who says they are asking. Unverified by itself — the token
	// is what turns the name into a claim worth checking.
	Asker domain.PeerIdentity
	// Epoch is the token's time window.
	Epoch uint64
	// Token is the reciprocity MAC (see liveness_token.go).
	Token []byte
}

type livenessProbeWire struct {
	Asker string `json:"asker"`
	Epoch uint64 `json:"epoch"`
	Token []byte `json:"token"`
}

// SealLivenessProbe builds the sealed payload an asker attaches to a probe.
func SealLivenessProbe(
	ownBoxPrivate *ecdh.PrivateKey,
	network domain.NetworkID,
	asker, target domain.PeerIdentity,
	targetBoxKeyBase64 string,
	attemptLabel domain.PeerIdentity,
	at time.Time,
) ([]byte, error) {
	if attemptLabel.IsZero() {
		return nil, fmt.Errorf("liveness probe: a claim must be bound to an attempt label")
	}
	epoch := LivenessTokenEpochAt(at)
	token, err := BuildLivenessToken(ownBoxPrivate, network, targetBoxKeyBase64, asker, target, attemptLabel, epoch)
	if err != nil {
		return nil, err
	}
	plain, err := json.Marshal(livenessProbeWire{
		Asker: asker.String(),
		Epoch: epoch,
		Token: token,
	})
	if err != nil {
		return nil, fmt.Errorf("liveness probe: marshal claim: %w", err)
	}

	targetKey, err := decodeBoxPublicKey(targetBoxKeyBase64)
	if err != nil {
		return nil, err
	}
	box, err := ecdhgcm.Seal(targetKey, plain, livenessProbeSealLabel+"|"+target.String())
	if err != nil {
		return nil, fmt.Errorf("liveness probe: seal: %w", err)
	}
	sealed := make([]byte, 0, len(box.EphemeralPub)+len(box.Nonce)+len(box.Ciphertext))
	sealed = append(sealed, box.EphemeralPub...)
	sealed = append(sealed, box.Nonce...)
	sealed = append(sealed, box.Ciphertext...)
	return sealed, nil
}

// OpenLivenessProbe decrypts the sealed part with this node's box key.
//
// It does NOT verify the token — that needs the asker's public box key, which
// only the caller's contact store has. Opening and verifying are separate so
// the failure modes stay separate: an unopenable payload is a frame for
// somebody else, a bad token is somebody claiming a contact they are not.
func OpenLivenessProbe(id *identity.Identity, target domain.PeerIdentity, sealed []byte) (LivenessProbeClaim, error) {
	if id == nil || id.BoxPrivateKey == nil {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: no local box key")
	}
	if len(sealed) <= livenessSealPrefixBytes {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: sealed payload is %d bytes, too short", len(sealed))
	}
	plain, err := ecdhgcm.Open(id.BoxPrivateKey, &ecdhgcm.SealedBox{
		EphemeralPub: sealed[:livenessEphemeralKeyBytes],
		Nonce:        sealed[livenessEphemeralKeyBytes:livenessSealPrefixBytes],
		Ciphertext:   sealed[livenessSealPrefixBytes:],
	}, livenessProbeSealLabel+"|"+target.String())
	if err != nil {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: open: %w", err)
	}

	var wire livenessProbeWire
	if err := json.Unmarshal(plain, &wire); err != nil {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: parse claim: %w", err)
	}
	asker, err := domain.ParsePeerIdentity(wire.Asker)
	if err != nil || asker.IsZero() {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: claim names no asker")
	}
	if len(wire.Token) != livenessTokenBytes {
		return LivenessProbeClaim{}, fmt.Errorf("liveness probe: token is %d bytes", len(wire.Token))
	}
	return LivenessProbeClaim{Asker: asker, Epoch: wire.Epoch, Token: wire.Token}, nil
}

// Fixed-width prefix of a sealed payload: ephemeral X25519 public key, then the
// AES-GCM nonce, then the ciphertext. Fixed width is what lets the reader split
// the three without a length field to get wrong.
const (
	livenessEphemeralKeyBytes = 32
	livenessNonceBytes        = 12
	livenessSealPrefixBytes   = livenessEphemeralKeyBytes + livenessNonceBytes
)
