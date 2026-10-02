package identity

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
)

// Errors of ParsePublicKey. Every one of them matches ErrInvalidPublicKey
// through errors.Is, so a caller that only needs "the key itself is
// unacceptable" — as opposed to "the signature under a good key is wrong" —
// tests for the umbrella and never for a message.
var (
	ErrInvalidPublicKey      = errors.New("invalid ed25519 public key")
	ErrPublicKeySize         = fmt.Errorf("%w: wrong size", ErrInvalidPublicKey)
	ErrPublicKeyEncoding     = fmt.Errorf("%w: undecodable", ErrInvalidPublicKey)
	ErrPublicKeyNonCanonical = fmt.Errorf("%w: non-canonical y coordinate", ErrInvalidPublicKey)
	ErrPublicKeySmallOrder   = fmt.Errorf("%w: small-order point", ErrInvalidPublicKey)
)

// PublicKey is an Ed25519 signing key that passed ParsePublicKey — the only
// way to obtain one holding a key. Verification of a peer's signature goes
// through PublicKey.Verify, never through crypto/ed25519 directly: stdlib
// accepts small-order keys, under which a signature can be produced without
// any private key (the neutral key 0100…00 with R = neutral, S = 0 verifies
// for EVERY message).
//
// The zero value holds no key and verifies nothing.
type PublicKey struct {
	key ed25519.PublicKey
}

// smallOrderEncodings is every 32-byte string the permissive stdlib decoder
// turns into a point of order 1, 2, 4 or 8: the ten canonical encodings of
// the eight torsion points (x = -0 is accepted, so the two points with x = 0
// have two each) and the four aliases with y = p and y = p + 1. The test
// suite re-derives the set from the curve equation.
var smallOrderEncodings = [...][ed25519.PublicKeySize]byte{
	{0x01},
	{0x01, 31: 0x80},
	{0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
	{0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{},
	{31: 0x80},
	{0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4, 0x89, 0xf2, 0xef, 0x98, 0xf0, 0xd5, 0xdf, 0xac, 0x05, 0xd3, 0xc6, 0x33, 0x39, 0xb1, 0x38, 0x02, 0x88, 0x6d, 0x53, 0xfc, 0x05},
	{0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4, 0x89, 0xf2, 0xef, 0x98, 0xf0, 0xd5, 0xdf, 0xac, 0x05, 0xd3, 0xc6, 0x33, 0x39, 0xb1, 0x38, 0x02, 0x88, 0x6d, 0x53, 0xfc, 0x85},
	{0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10, 0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac, 0x03, 0x7a},
	{0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10, 0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac, 0x03, 0xfa},
	{0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
	{0xed, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	{0xee, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
	{0xee, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
}

// ParsePublicKey is the single point where a peer's Ed25519 signing key is
// accepted. It refuses:
//
//   - a length other than 32 bytes;
//   - any of the 14 small-order encodings (smallOrderEncodings);
//   - a non-canonical y, i.e. (little-endian value with the sign bit
//     cleared) ≥ p = 2^255 − 19. stdlib reduces such a y silently, so one
//     point would have several "keys" — and several addresses.
//
// It deliberately does NOT refuse:
//
//   - a mixed-order key (prime-order point plus a torsion component).
//     Detecting one needs [L]·A = O, a scalar multiplication stdlib does not
//     expose; such a key harms only its own identity, because the address is
//     bound to the exact key bytes; and every node verifies with the same
//     cofactorless Go verifier, so no two nodes of this network disagree
//     about a signature under it;
//   - an encoding with no point behind it. Deciding that costs a field
//     exponentiation per key, and buys nothing: Verify decompresses the key
//     itself and refuses every signature under it.
func ParsePublicKey(raw []byte) (PublicKey, error) {
	if len(raw) != ed25519.PublicKeySize {
		return PublicKey{}, fmt.Errorf("%w: %d bytes", ErrPublicKeySize, len(raw))
	}
	if isSmallOrderEncoding(raw) {
		return PublicKey{}, ErrPublicKeySmallOrder
	}
	if !hasCanonicalY(raw) {
		return PublicKey{}, ErrPublicKeyNonCanonical
	}
	return PublicKey{key: append(ed25519.PublicKey(nil), raw...)}, nil
}

// ParsePublicKeyBase64 is ParsePublicKey for the standard-base64 form every
// frame carries a signing key in.
func ParsePublicKeyBase64(encoded string) (PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return PublicKey{}, fmt.Errorf("%w: %v", ErrPublicKeyEncoding, err)
	}
	return ParsePublicKey(raw)
}

// Verify reports whether signature is a valid Ed25519 signature of message
// under k.
func (k PublicKey) Verify(message, signature []byte) bool {
	if len(k.key) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(k.key, message, signature)
}

// Bytes returns a copy of the 32 key bytes; nil for the zero value.
func (k PublicKey) Bytes() []byte {
	if k.key == nil {
		return nil
	}
	return append([]byte(nil), k.key...)
}

// Fingerprint is the identity address this key certifies.
func (k PublicKey) Fingerprint() string {
	return Fingerprint(k.key)
}

func isSmallOrderEncoding(raw []byte) bool {
	for i := range smallOrderEncodings {
		if [ed25519.PublicKeySize]byte(raw) == smallOrderEncodings[i] {
			return true
		}
	}
	return false
}

// hasCanonicalY reports whether the 255-bit little-endian y is below
// p = 2^255 − 19, whose encoding is ed ff … ff 7f. Anything at or above it has
// bits 8..254 all set and a low byte ≥ 0xed.
func hasCanonicalY(raw []byte) bool {
	last := len(raw) - 1
	if raw[last]&0x7f != 0x7f {
		return true
	}
	for i := last - 1; i >= 1; i-- {
		if raw[i] != 0xff {
			return true
		}
	}
	return raw[0] < 0xed
}
