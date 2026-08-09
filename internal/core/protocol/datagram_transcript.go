package protocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// datagram_transcript.go builds the signed transcript of a routed
// datagram, derives its replay key and implements the two pure crypto
// helpers. Everything here is stateless: no clock, no trust store, no peer
// knowledge — a transit relay verifies authenticity from the frame bytes
// alone (§3.1, §3.2).

// ErrDatagramSignature marks a frame whose Ed25519 signature does not
// verify against the public key it carries.
var ErrDatagramSignature = errors.New("datagram: invalid signature")

// datagramTranscriptDomain is the domain separation tag. It is followed by
// a 0x00 byte so no other corsa signing context can ever share a prefix
// with a datagram transcript.
const datagramTranscriptDomain = "corsa-datagram-auth-v1"

// BuildDatagramTranscript returns the exact byte string signed by src, per
// §3.2. Every field with a binary meaning is length-prefixed in its BINARY
// form — 20-byte addresses, raw salt and pubkey, decoded payload — because
// an implementation that signed the textual form would silently produce a
// different transcript for the same frame.
//
// Covered: everything immutable. Excluded: ttl (changes every hop) and
// auth.sig (a signature cannot sign itself).
//
// The network id is a parameter rather than a constant of this package:
// the node owns the single declaration of the network name, and a second
// copy here could only ever drift out of sync with the one on the wire.
func BuildDatagramTranscript(d DatagramFrame, network domain.NetworkID) ([]byte, error) {
	if err := d.validate(false); err != nil {
		return nil, err
	}
	if d.Auth == nil {
		return nil, fmt.Errorf("%w: transcript requires auth", ErrDatagramAuth)
	}
	if _, err := domain.ParseNetworkID(network.String()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDatagramAuth, err)
	}

	var buf bytes.Buffer
	buf.WriteString(datagramTranscriptDomain)
	buf.WriteByte(0x00)
	writeLengthPrefixed(&buf, []byte{byte(d.Auth.AuthVersion)})
	writeLengthPrefixed(&buf, []byte(network.String()))
	writeLengthPrefixed(&buf, []byte{byte(d.Version)})
	writeLengthPrefixed(&buf, []byte(d.Mode.String()))
	writeLengthPrefixed(&buf, []byte(d.Class.String()))
	writeLengthPrefixed(&buf, []byte(d.RoutePolicy.String()))
	writeLengthPrefixed(&buf, d.Src[:])
	writeLengthPrefixed(&buf, d.Dst[:])
	writeLengthPrefixed(&buf, []byte(d.DType.String()))
	writeLengthPrefixed(&buf, []byte{d.Auth.MaxTTL})
	var unixTime [8]byte
	binary.BigEndian.PutUint64(unixTime[:], uint64(d.Auth.Time))
	writeLengthPrefixed(&buf, unixTime[:])
	writeLengthPrefixed(&buf, d.Auth.Salt)
	writeLengthPrefixed(&buf, d.Auth.PubKey)
	writeLengthPrefixed(&buf, d.Payload)
	return buf.Bytes(), nil
}

// DatagramReplayKey derives sha256(transcript) — the anti-replay cache key
// (§3.1). It is never carried on the wire: any node holding the frame can
// compute it, so a separate field could only add a value to forge.
//
// It takes the transcript rather than the frame so the key is provably
// derived from the same bytes the signature was verified over: a profile
// cannot substitute a key, and the pipeline cannot accidentally verify one
// byte string and cache another (§2.2).
func DatagramReplayKey(transcript []byte) domain.ReplayKey {
	return domain.ReplayKey(sha256.Sum256(transcript))
}

// SignDatagram returns a copy of d whose auth.sig covers the transcript.
// The public key is taken from privateKey, so the frame cannot claim a key
// it was not signed with; a pre-set mismatching key is an error rather
// than a silent overwrite.
func SignDatagram(d DatagramFrame, network domain.NetworkID, privateKey ed25519.PrivateKey) (DatagramFrame, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return DatagramFrame{}, fmt.Errorf("%w: private key %d bytes, want %d", ErrDatagramAuth, len(privateKey), ed25519.PrivateKeySize)
	}
	if d.Auth == nil {
		return DatagramFrame{}, fmt.Errorf("%w: signing requires auth", ErrDatagramAuth)
	}
	signed := d.Clone()
	publicKey := []byte(privateKey.Public().(ed25519.PublicKey))
	if len(signed.Auth.PubKey) > 0 && !bytes.Equal(signed.Auth.PubKey, publicKey) {
		return DatagramFrame{}, fmt.Errorf("%w: auth.pubkey does not match the signing key", ErrDatagramAuth)
	}
	signed.Auth.PubKey = publicKey
	transcript, err := BuildDatagramTranscript(signed, network)
	if err != nil {
		return DatagramFrame{}, err
	}
	signed.Auth.Sig = ed25519.Sign(privateKey, transcript)
	if err := signed.Validate(); err != nil {
		return DatagramFrame{}, err
	}
	return signed, nil
}

// VerifyDatagramSignature checks auth.sig against the public key carried in
// the frame. It proves INTEGRITY only: binding that key to src is
// DatagramSignerMatchesSrc, and authorization of the sender is the
// receiver's local trust policy. The three gates stay separate exactly as
// the file transport separated them (§3.1).
func VerifyDatagramSignature(d DatagramFrame, network domain.NetworkID) error {
	if d.Auth == nil {
		return fmt.Errorf("%w: verification requires auth", ErrDatagramAuth)
	}
	if len(d.Auth.Sig) != domain.DatagramSigBytes {
		return fmt.Errorf("%w: sig %d bytes, want %d", ErrDatagramEncoding, len(d.Auth.Sig), domain.DatagramSigBytes)
	}
	transcript, err := BuildDatagramTranscript(d, network)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(d.Auth.PubKey), transcript, d.Auth.Sig) {
		return ErrDatagramSignature
	}
	return nil
}

// DatagramSignerMatchesSrc reports whether the carried public key
// fingerprints to src. Without it an attacker could keep a stranger's src
// while substituting their own key and signature (§3.1). Pure: it derives
// the fingerprint, it does not look the sender up anywhere.
func DatagramSignerMatchesSrc(d DatagramFrame) bool {
	if d.Auth == nil || len(d.Auth.PubKey) != domain.DatagramPubKeyBytes {
		return false
	}
	fingerprint, err := domain.ParsePeerIdentity(identity.Fingerprint(ed25519.PublicKey(d.Auth.PubKey)))
	if err != nil {
		return false
	}
	return fingerprint == d.Src
}

func writeLengthPrefixed(buf *bytes.Buffer, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	buf.Write(length[:])
	buf.Write(value)
}
