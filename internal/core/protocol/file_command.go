package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"corsa/internal/core/domain"
)

// FileCommandFrameType is the type identifier used in the unified Frame
// dispatcher. Transit nodes see this type and route via FileRouter;
// the actual command (chunk_request, etc.) is inside the encrypted Payload.
const FileCommandFrameType = "file_command"

// FileCommandFrame is the wire format for file transfer protocol commands.
// Only SRC, DST, TTL, Time, Nonce, and Signature are cleartext — transit
// nodes route based on DST without knowing the command type.
//
// Wire layout (JSON):
//
//	{
//	  "type":      "file_command",
//	  "src":       "<sender PeerIdentity>",
//	  "dst":       "<recipient PeerIdentity>",
//	  "ttl":       10,
//	  "max_ttl":   10,
//	  "time":      1712345678,
//	  "nonce":     "<hex SHA-256 digest>",
//	  "signature": "<hex ed25519 signature over nonce>",
//	  "payload":   "<base64 encrypted JSON>"
//	}
//
// MaxTTL is set equal to TTL by the sender and included in the nonce hash.
// Relays decrement TTL but cannot change MaxTTL without invalidating the
// nonce → signature chain. Each processing node enforces TTL <= MaxTTL.
type FileCommandFrame struct {
	Type      string              `json:"type"`
	SRC       domain.PeerIdentity `json:"src"`
	DST       domain.PeerIdentity `json:"dst"`
	TTL       uint8               `json:"ttl"`
	MaxTTL    uint8               `json:"max_ttl"`
	Time      int64               `json:"time"`
	Nonce     string              `json:"nonce"`
	Signature string              `json:"signature"`
	Payload   string              `json:"payload"` // base64-encoded encrypted JSON
}

// ComputeNonce derives the anti-replay nonce by hashing the immutable frame
// fields: SHA256(SRC || DST || MaxTTL || Time || Payload). The nonce binds
// all fields except TTL (which is decremented per hop) and Signature.
// MaxTTL is included so that a relay cannot inflate TTL beyond the sender's
// original hop budget without invalidating the nonce → signature chain.
func ComputeNonce(src, dst domain.PeerIdentity, maxTTL uint8, unixTime int64, payload string) string {
	h := sha256.New()
	h.Write([]byte(src))
	h.Write([]byte(dst))
	h.Write([]byte{maxTTL})
	h.Write([]byte(strconv.FormatInt(unixTime, 10)))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// SignFileCommand signs the nonce with the sender's Ed25519 private key.
// The signature authenticates the sender and binds all immutable fields.
func SignFileCommand(nonce string, privateKey ed25519.PrivateKey) string {
	sig := ed25519.Sign(privateKey, []byte(nonce))
	return hex.EncodeToString(sig)
}

// VerifyFileCommandSignature checks the ed25519 signature of the nonce
// against the sender's public key.
func VerifyFileCommandSignature(nonce, signatureHex string, publicKey ed25519.PublicKey) error {
	sig, err := hex.DecodeString(signatureHex)
	if err != nil {
		return fmt.Errorf("decode file command signature: %w", err)
	}
	if !ed25519.Verify(publicKey, []byte(nonce), sig) {
		return fmt.Errorf("invalid file command signature")
	}
	return nil
}

// ValidateFileCommandFrame performs cleartext validation on a received
// FileCommandFrame without decrypting the payload. It checks:
//   - nonce binding (SHA-256 matches immutable fields)
//   - freshness (timestamp within allowed clock drift)
//
// TTL validation is intentionally excluded — the caller (file_router)
// performs it at the correct pipeline stage: after anti-replay and
// deliverability check, before expensive cryptographic operations.
//
// Signature verification requires the sender's public key and is done
// separately via VerifyFileCommandSignature.
func ValidateFileCommandFrame(f FileCommandFrame, now time.Time) error {
	if f.Type != FileCommandFrameType {
		return fmt.Errorf("unexpected frame type %q, expected %q", f.Type, FileCommandFrameType)
	}

	if f.SRC == "" {
		return fmt.Errorf("file command: empty SRC")
	}
	if f.DST == "" {
		return fmt.Errorf("file command: empty DST")
	}

	// MaxTTL enforcement: TTL must never exceed MaxTTL. A relay that
	// inflates TTL beyond the sender's original hop budget is detected
	// here. MaxTTL itself is signature-bound via the nonce.
	if f.MaxTTL == 0 {
		return fmt.Errorf("file command: MaxTTL is zero")
	}
	if f.TTL > f.MaxTTL {
		return fmt.Errorf("file command: TTL %d exceeds MaxTTL %d", f.TTL, f.MaxTTL)
	}

	// Nonce binding: recompute and compare.
	expected := ComputeNonce(f.SRC, f.DST, f.MaxTTL, f.Time, f.Payload)
	if f.Nonce != expected {
		return fmt.Errorf("file command: nonce mismatch")
	}

	// Freshness: reject if too far from local clock.
	drift := now.Unix() - f.Time
	if drift < 0 {
		drift = -drift
	}
	if drift > domain.FileCommandMaxClockDrift {
		return fmt.Errorf("file command: timestamp drift %ds exceeds limit %ds", drift, domain.FileCommandMaxClockDrift)
	}

	return nil
}

// DecrementTTL returns a copy of the frame with TTL reduced by 1.
// Used by relay nodes before forwarding. Returns error if TTL is already 0.
func (f FileCommandFrame) DecrementTTL() (FileCommandFrame, error) {
	if f.TTL == 0 {
		return f, fmt.Errorf("file command: cannot decrement TTL below zero")
	}
	out := f
	out.TTL = f.TTL - 1
	return out, nil
}

// MarshalFileCommandFrame serializes a FileCommandFrame to JSON bytes.
func MarshalFileCommandFrame(f FileCommandFrame) ([]byte, error) {
	return json.Marshal(f)
}

// UnmarshalFileCommandFrame deserializes a FileCommandFrame from JSON bytes.
func UnmarshalFileCommandFrame(data []byte) (FileCommandFrame, error) {
	var f FileCommandFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return FileCommandFrame{}, fmt.Errorf("unmarshal file command frame: %w", err)
	}
	return f, nil
}

// NewFileCommandFrame constructs a complete FileCommandFrame ready for
// transmission. It computes the nonce and signs it with the sender's key.
//
// Parameters:
//   - src: sender's identity
//   - dst: recipient's identity
//   - ttl: initial hop count (clamped to FileCommandMaxTTL)
//   - payload: base64-encoded encrypted command payload
//   - privateKey: sender's Ed25519 private key for signing
func NewFileCommandFrame(
	src, dst domain.PeerIdentity,
	ttl uint8,
	payload string,
	privateKey ed25519.PrivateKey,
) FileCommandFrame {
	if ttl == 0 {
		ttl = 10 // sensible default
	}
	if ttl > uint8(math.Min(float64(domain.FileCommandMaxTTL), 255)) {
		ttl = domain.FileCommandMaxTTL
	}

	now := time.Now().Unix()
	nonce := ComputeNonce(src, dst, ttl, now, payload)
	sig := SignFileCommand(nonce, privateKey)

	return FileCommandFrame{
		Type:      FileCommandFrameType,
		SRC:       src,
		DST:       dst,
		TTL:       ttl,
		MaxTTL:    ttl,
		Time:      now,
		Nonce:     nonce,
		Signature: sig,
		Payload:   payload,
	}
}
