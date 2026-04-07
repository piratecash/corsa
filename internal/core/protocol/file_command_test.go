package protocol

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

func TestComputeNonce(t *testing.T) {
	t.Parallel()

	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")
	now := int64(1712345678)
	payload := "encrypted-data-base64"
	maxTTL := uint8(10)

	n1 := ComputeNonce(src, dst, maxTTL, now, payload)
	n2 := ComputeNonce(src, dst, maxTTL, now, payload)

	if n1 != n2 {
		t.Error("same inputs must produce same nonce")
	}

	// Different payload → different nonce.
	n3 := ComputeNonce(src, dst, maxTTL, now, "different-payload")
	if n1 == n3 {
		t.Error("different payload must produce different nonce")
	}

	// Different time → different nonce.
	n4 := ComputeNonce(src, dst, maxTTL, now+1, payload)
	if n1 == n4 {
		t.Error("different time must produce different nonce")
	}

	// Different MaxTTL → different nonce.
	n5 := ComputeNonce(src, dst, maxTTL+1, now, payload)
	if n1 == n5 {
		t.Error("different MaxTTL must produce different nonce")
	}

	// Nonce is a hex SHA-256 hash = 64 chars.
	if len(n1) != 64 {
		t.Errorf("nonce length = %d, want 64", len(n1))
	}
}

func TestSignAndVerifyFileCommand(t *testing.T) {
	t.Parallel()

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	nonce := "test-nonce-value-1234567890abcdef"
	sig := SignFileCommand(nonce, priv)

	if err := VerifyFileCommandSignature(nonce, sig, pub); err != nil {
		t.Fatalf("verify valid signature: %v", err)
	}

	// Wrong nonce should fail.
	if err := VerifyFileCommandSignature("wrong-nonce", sig, pub); err == nil {
		t.Error("verify with wrong nonce should fail")
	}

	// Wrong key should fail.
	pub2, _, _ := ed25519.GenerateKey(nil)
	if err := VerifyFileCommandSignature(nonce, sig, pub2); err == nil {
		t.Error("verify with wrong key should fail")
	}
}

func TestValidateFileCommandFrame(t *testing.T) {
	t.Parallel()

	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")
	now := time.Now()
	payload := "test-payload"
	maxTTL := uint8(10)
	nonce := ComputeNonce(src, dst, maxTTL, now.Unix(), payload)

	valid := FileCommandFrame{
		Type:      FileCommandFrameType,
		SRC:       src,
		DST:       dst,
		TTL:       10,
		MaxTTL:    maxTTL,
		Time:      now.Unix(),
		Nonce:     nonce,
		Signature: "placeholder",
		Payload:   payload,
	}

	if err := ValidateFileCommandFrame(valid, now); err != nil {
		t.Fatalf("validate valid frame: %v", err)
	}

	// Wrong type.
	wrongType := valid
	wrongType.Type = "send_message"
	if err := ValidateFileCommandFrame(wrongType, now); err == nil {
		t.Error("wrong type should fail")
	}

	// Empty SRC.
	emptySRC := valid
	emptySRC.SRC = ""
	if err := ValidateFileCommandFrame(emptySRC, now); err == nil {
		t.Error("empty SRC should fail")
	}

	// Empty DST.
	emptyDST := valid
	emptyDST.DST = ""
	if err := ValidateFileCommandFrame(emptyDST, now); err == nil {
		t.Error("empty DST should fail")
	}

	// Bad nonce.
	badNonce := valid
	badNonce.Nonce = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := ValidateFileCommandFrame(badNonce, now); err == nil {
		t.Error("wrong nonce should fail")
	}

	// Stale timestamp.
	stale := valid
	stale.Time = now.Unix() - domain.FileCommandMaxClockDrift - 10
	stale.Nonce = ComputeNonce(src, dst, maxTTL, stale.Time, payload)
	if err := ValidateFileCommandFrame(stale, now); err == nil {
		t.Error("stale timestamp should fail")
	}

	// Future timestamp.
	future := valid
	future.Time = now.Unix() + domain.FileCommandMaxClockDrift + 10
	future.Nonce = ComputeNonce(src, dst, maxTTL, future.Time, payload)
	if err := ValidateFileCommandFrame(future, now); err == nil {
		t.Error("future timestamp should fail")
	}

	// TTL > MaxTTL should fail (relay inflation).
	inflated := valid
	inflated.TTL = maxTTL + 1
	if err := ValidateFileCommandFrame(inflated, now); err == nil {
		t.Error("TTL > MaxTTL should fail")
	}

	// MaxTTL = 0 should fail.
	zeroMax := valid
	zeroMax.MaxTTL = 0
	zeroMax.TTL = 0
	if err := ValidateFileCommandFrame(zeroMax, now); err == nil {
		t.Error("MaxTTL = 0 should fail")
	}

	// TTL = 0 with valid MaxTTL should pass validation (TTL is checked
	// by file_router, not ValidateFileCommandFrame).
	zeroTTL := valid
	zeroTTL.TTL = 0
	if err := ValidateFileCommandFrame(zeroTTL, now); err != nil {
		t.Errorf("zero TTL should pass validation (TTL is checked by file_router): %v", err)
	}
}

func TestNewFileCommandFrame(t *testing.T) {
	t.Parallel()

	_, priv, _ := ed25519.GenerateKey(nil)
	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")

	frame := NewFileCommandFrame(src, dst, 10, "payload-data", priv)

	if frame.Type != FileCommandFrameType {
		t.Errorf("Type = %q, want %q", frame.Type, FileCommandFrameType)
	}
	if frame.SRC != src {
		t.Errorf("SRC = %q, want %q", frame.SRC, src)
	}
	if frame.DST != dst {
		t.Errorf("DST = %q, want %q", frame.DST, dst)
	}
	if frame.TTL != 10 {
		t.Errorf("TTL = %d, want 10", frame.TTL)
	}
	if frame.MaxTTL != 10 {
		t.Errorf("MaxTTL = %d, want 10", frame.MaxTTL)
	}
	if frame.Payload != "payload-data" {
		t.Errorf("Payload mismatch")
	}

	// Validate the frame.
	if err := ValidateFileCommandFrame(frame, time.Now()); err != nil {
		t.Fatalf("validate new frame: %v", err)
	}
}

func TestDecrementTTL(t *testing.T) {
	t.Parallel()

	frame := FileCommandFrame{TTL: 5, MaxTTL: 10}
	dec, err := frame.DecrementTTL()
	if err != nil {
		t.Fatalf("decrement: %v", err)
	}
	if dec.TTL != 4 {
		t.Errorf("TTL = %d, want 4", dec.TTL)
	}
	if dec.MaxTTL != 10 {
		t.Errorf("MaxTTL should be unchanged: got %d, want 10", dec.MaxTTL)
	}

	// Decrementing from 0 should fail.
	zero := FileCommandFrame{TTL: 0}
	if _, err := zero.DecrementTTL(); err == nil {
		t.Error("decrement from 0 should fail")
	}
}

func TestMarshalUnmarshalFileCommandFrame(t *testing.T) {
	t.Parallel()

	_, priv, _ := ed25519.GenerateKey(nil)
	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")

	original := NewFileCommandFrame(src, dst, 10, "test-payload", priv)

	data, err := MarshalFileCommandFrame(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := UnmarshalFileCommandFrame(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type mismatch")
	}
	if decoded.SRC != original.SRC {
		t.Errorf("SRC mismatch")
	}
	if decoded.DST != original.DST {
		t.Errorf("DST mismatch")
	}
	if decoded.TTL != original.TTL {
		t.Errorf("TTL mismatch")
	}
	if decoded.MaxTTL != original.MaxTTL {
		t.Errorf("MaxTTL mismatch")
	}
	if decoded.Time != original.Time {
		t.Errorf("Time mismatch")
	}
	if decoded.Nonce != original.Nonce {
		t.Errorf("Nonce mismatch")
	}
	if decoded.Signature != original.Signature {
		t.Errorf("Signature mismatch")
	}
	if decoded.Payload != original.Payload {
		t.Errorf("Payload mismatch")
	}
}
