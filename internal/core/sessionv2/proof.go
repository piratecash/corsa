package sessionv2

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

// proof.go is the identity half of the v2 session: a signature over the TLS
// exporter of THIS connection.
//
// Why the exporter. The v1 proof signed "challenge|own address", which names
// neither the verifier nor the connection, so a man in the middle could hand
// one victim's challenge to another and carry the signature across. The
// exporter is derived from this TLS session's handshake secret: a middleman
// running two TLS sessions holds two different exporters, and a signature
// over one never verifies on the other.

// Role is which end of the TLS session signs. It is taken from the LOCAL TLS
// side (client = dialer), never from a frame, so a proof cannot be reflected
// back to the end that made it.
type Role byte

const (
	RoleDialer   Role = 0x01
	RoleListener Role = 0x02
)

// peer is the role the other end of the session holds.
func (r Role) peer() Role {
	if r == RoleDialer {
		return RoleListener
	}
	return RoleDialer
}

const (
	// ExporterLabel and ExporterLength define E (RFC 8446 §7.5, empty
	// context).
	ExporterLabel  = "EXPORTER-corsa-session-v2"
	ExporterLength = 32

	proofDomainTag = "corsa-session-v2-proof"
	proofFrameType = "session_proof"
	// proofSignatureChars is a 64-byte signature in unpadded base64url.
	proofSignatureChars = 86
)

// ErrProofFrame is a session_proof frame that does not parse strictly.
var ErrProofFrame = errors.New("sessionv2: malformed session_proof frame")

// proofPayload is DOMAIN("corsa-session-v2-proof") ‖ role ‖ E, where
// DOMAIN(tag) = tag ‖ 0x00 ‖ uint16be(len(network)) ‖ network. The network
// keeps a proof made on one network from verifying on another.
func proofPayload(network domain.NetworkID, role Role, exporter []byte) []byte {
	name := network.String()
	payload := make([]byte, 0, len(proofDomainTag)+1+2+len(name)+1+len(exporter))
	payload = append(payload, proofDomainTag...)
	payload = append(payload, 0x00)
	payload = binary.BigEndian.AppendUint16(payload, uint16(len(name))) //nolint:gosec // bounded by maxNetworkName at the call sites
	payload = append(payload, name...)
	payload = append(payload, byte(role))
	return append(payload, exporter...)
}

// verifyProof reports whether signature proves key's holder signed THIS
// session (exporter) in role on network.
func verifyProof(key identity.PublicKey, network domain.NetworkID, role Role, exporter, signature []byte) bool {
	if len(exporter) != ExporterLength || len(signature) != ed25519.SignatureSize {
		return false
	}
	return key.Verify(proofPayload(network, role, exporter), signature)
}

// proofFrame is the whole session_proof frame; nothing else may ride in it.
type proofFrame struct {
	Type      string `json:"type"`
	Signature string `json:"signature"`
}

func marshalProofFrame(signature []byte) ([]byte, error) {
	if len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: a signature of %d bytes", ErrProofFrame, len(signature))
	}
	return json.Marshal(proofFrame{Type: proofFrameType, Signature: base64.RawURLEncoding.EncodeToString(signature)})
}

// parseProofFrame accepts exactly {"type":"session_proof","signature":S}
// with S an unpadded base64url encoding of 64 bytes, and returns the bytes.
func parseProofFrame(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var frame proofFrame
	if err := decoder.Decode(&frame); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProofFrame, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: data after the frame", ErrProofFrame)
	}
	if frame.Type != proofFrameType {
		return nil, fmt.Errorf("%w: type %q", ErrProofFrame, frame.Type)
	}
	if len(frame.Signature) != proofSignatureChars {
		return nil, fmt.Errorf("%w: a signature of %d characters", ErrProofFrame, len(frame.Signature))
	}
	signature, err := base64.RawURLEncoding.Strict().DecodeString(frame.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: the signature does not decode", ErrProofFrame)
	}
	return signature, nil
}
