package protocol

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/hkdf"

	"github.com/piratecash/corsa/internal/core/domain"
)

// liveness_token.go builds the reciprocity token that gates a liveness probe.
//
// # What it proves, and what it deliberately does not
//
// The token is a MAC over (epoch, attempt label, asker, target) keyed by the X25519 shared
// secret of the two identities' box keys. Computing it requires ONE of the two
// private box keys, so presenting a valid one proves the asker holds the key
// material of an identity the target has as a contact — reciprocity, in the
// only sense this protocol can check without a round trip.
//
// It is NOT an authorisation token and must not be reused as one. It says
// "these two know each other", not "this request is permitted"; the target's
// own contact list decides the second question.
//
// # Why the shared secret and not a signature
//
// A signature would name the asker to anyone who sees the frame. The token is
// carried inside the sealed payload, and it is verified by RECOMPUTING it per
// contact rather than by reading an identity out of it — so the wire never
// carries who is asking, and a target with no matching contact simply finds no
// match. That property is the whole point: the probe must not publish the pair
// (asker, target) to the network, which is what the plaintext `requester`
// triple would do if it were ever filled in.
//
// # Epochs
//
// The token is bound to a coarse time window so a captured one expires. The
// window is deliberately wide compared to clock error: both sides compute it
// from wall-clock time with no negotiation, and the verifier accepts the
// neighbouring windows, so a few minutes of drift costs nothing. A narrower
// window would buy little — the token is already inside a sealed payload — and
// would start failing on ordinary clock skew.

const (
	// LivenessTokenEpoch is the width of one token window.
	LivenessTokenEpoch = 10 * time.Minute

	// livenessTokenLabel is the domain separator. Distinct from every other
	// corsa label so a token cannot be replayed into another protocol, and
	// so the shared secret this derives from cannot collide with the DM
	// sealing key derived from the same ECDH output.
	livenessTokenLabel = "corsa-liveness-token-v1"

	// livenessTokenBytes is the token width. Sixteen bytes is far past the
	// forgery budget of an online protocol — every attempt costs a round
	// trip to a node that answers at most a few probes a second — and keeps
	// the sealed payload small.
	livenessTokenBytes = 16
)

// ErrLivenessTokenMalformed is returned for a token that cannot be a token:
// wrong length, or built from key material that does not parse.
var ErrLivenessTokenMalformed = errors.New("liveness token malformed")

// LivenessTokenEpochAt is the epoch number covering t. Exported so both sides
// compute it the same way and so a test can pin a window.
func LivenessTokenEpochAt(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	seconds := t.UTC().Unix()
	if seconds < 0 {
		return 0
	}
	return uint64(seconds) / uint64(LivenessTokenEpoch.Seconds())
}

// BuildLivenessToken derives the token an asker sends to a target.
//
// ownBoxPrivate is the asker's X25519 private key; targetBoxKeyBase64 is the
// target's public box key as it appears in their identity record. asker and
// target are the two fingerprints, and they enter the MAC in a fixed order so
// the token for A→B cannot be replayed as B→A.
func BuildLivenessToken(
	ownBoxPrivate *ecdh.PrivateKey,
	network domain.NetworkID,
	targetBoxKeyBase64 string,
	asker, target domain.PeerIdentity,
	attemptLabel domain.PeerIdentity,
	epoch uint64,
) ([]byte, error) {
	if ownBoxPrivate == nil {
		return nil, fmt.Errorf("%w: no box private key", ErrLivenessTokenMalformed)
	}
	if asker.IsZero() || target.IsZero() {
		return nil, fmt.Errorf("%w: zero identity", ErrLivenessTokenMalformed)
	}
	peerKey, err := decodeBoxPublicKey(targetBoxKeyBase64)
	if err != nil {
		return nil, err
	}
	shared, err := ownBoxPrivate.ECDH(peerKey)
	if err != nil {
		return nil, fmt.Errorf("%w: ecdh: %w", ErrLivenessTokenMalformed, err)
	}
	return livenessTokenFromShared(shared, network, asker, target, attemptLabel, epoch)
}

// VerifyLivenessToken recomputes the token a given contact would have sent and
// compares it in constant time.
//
// ownBoxPrivate is the TARGET's private key and askerBoxKeyBase64 the
// candidate contact's public one; the ECDH output is the same on both sides,
// which is what lets the target check without learning anything new.
//
// The caller loops over its contacts. That is O(contacts) hashes per probe —
// tens of them, once per probe — and it is what keeps the asker's identity off
// the wire: there is nothing in the frame to look them up BY.
func VerifyLivenessToken(
	ownBoxPrivate *ecdh.PrivateKey,
	network domain.NetworkID,
	askerBoxKeyBase64 string,
	asker, target domain.PeerIdentity,
	attemptLabel domain.PeerIdentity,
	epoch uint64,
	token []byte,
) bool {
	if len(token) != livenessTokenBytes {
		return false
	}
	expected, err := BuildLivenessToken(ownBoxPrivate, network, askerBoxKeyBase64, asker, target, attemptLabel, epoch)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, token)
}

// LivenessTokenEpochsAccepted returns the epochs a verifier will accept at t:
// the current one and its immediate neighbours.
//
// Both sides read an unsynchronised wall clock, so a token built moments
// before a boundary arrives inside the next window. Accepting the neighbours
// turns that from a failure into a non-event, at the cost of widening the
// replay window to three epochs — which the sealed envelope and the round-trip
// nature of the probe already bound far more tightly than time does.
func LivenessTokenEpochsAccepted(t time.Time) []uint64 {
	current := LivenessTokenEpochAt(t)
	if current == 0 {
		return []uint64{0, 1}
	}
	return []uint64{current, current - 1, current + 1}
}

func livenessTokenFromShared(
	shared []byte,
	network domain.NetworkID,
	asker, target, attemptLabel domain.PeerIdentity,
	epoch uint64,
) ([]byte, error) {
	// The label carries the direction AND the network.
	//
	// Direction: A→B and B→A derive different keys from the same ECDH
	// output, so a token cannot be reflected back.
	//
	// Network: without it, one sealed claim is valid on every network the
	// same key pair appears on. The two identities are the same principals
	// there, so the ECDH output is identical, and a claim captured on one
	// network would be replayable on another inside the accepted epochs.
	// The network is a first-class part of what a token means — "these two
	// know each other HERE" — so it goes into the key derivation rather
	// than being checked alongside it, and a mismatch produces a token that
	// simply does not verify.
	label := livenessTokenLabel + "|" + network.String() + "|" + asker.String() + "|" + target.String()
	reader := hkdf.New(sha256.New, shared, []byte(network.String()), []byte(label))
	key := make([]byte, sha256.Size)
	if _, err := reader.Read(key); err != nil {
		return nil, fmt.Errorf("%w: hkdf: %w", ErrLivenessTokenMalformed, err)
	}

	// The MAC covers the attempt LABEL as well as the epoch, and that is what
	// makes a captured claim worthless.
	//
	// Without it the sealed blob was a bearer credential for the whole epoch
	// window: any hop on the path could copy it into a get_identity of its
	// own, with a fresh label, and harvest proofs — the target verified the
	// claim, saw a contact, and answered. Binding the label means a claim is
	// valid for exactly one question, and the question is the one whose
	// answer comes back to whoever asked it.
	var bound [8 + len(domain.PeerIdentity{})]byte
	binary.BigEndian.PutUint64(bound[:8], epoch)
	copy(bound[8:], attemptLabel[:])
	mac := hmac.New(sha256.New, key)
	mac.Write(bound[:])
	return mac.Sum(nil)[:livenessTokenBytes], nil
}

func decodeBoxPublicKey(encoded string) (*ecdh.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: box key is not base64: %w", ErrLivenessTokenMalformed, err)
	}
	key, err := ecdh.X25519().NewPublicKey(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: box key is not an X25519 point: %w", ErrLivenessTokenMalformed, err)
	}
	return key, nil
}
