// Package edforgery holds Ed25519 material no honest node can produce, for
// tests that prove a verification site refuses it.
//
// The neutral element 0100…00 is a valid encoding that stdlib's
// crypto/ed25519 accepts as a public key, and under it the signature
// R = 0100…00, S = 0 verifies for EVERY message: the equation [S]B = R + [k]A
// collapses to O = O. Anyone can therefore "sign" anything as the identity
// whose address is the fingerprint of that key, without a private key. A test
// built from these two values is a forgery that is consistent everywhere — the
// address matches the key, the signature verifies under stdlib — so the only
// thing that can refuse it is the key check itself.
package edforgery

// NeutralPublicKey returns the encoding of the neutral element of Ed25519.
func NeutralPublicKey() []byte {
	key := make([]byte, 32)
	key[0] = 0x01
	return key
}

// UniversalSignature returns R = neutral, S = 0: a signature stdlib accepts
// under NeutralPublicKey for any message.
func UniversalSignature() []byte {
	sig := make([]byte, 64)
	sig[0] = 0x01
	return sig
}
