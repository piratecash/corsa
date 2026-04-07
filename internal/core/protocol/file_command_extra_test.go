package protocol

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// TestInvalidSignatureDropped verifies that a frame with a wrong signature
// is rejected by VerifyFileCommandSignature.
func TestInvalidSignatureDropped(t *testing.T) {
	t.Parallel()

	pub, _, _ := ed25519.GenerateKey(nil)
	_, otherPriv, _ := ed25519.GenerateKey(nil)

	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")
	payload := "test-payload"

	frame := NewFileCommandFrame(src, dst, 10, payload, otherPriv)

	// Nonce binding passes because the frame is internally consistent.
	if err := ValidateFileCommandFrame(frame, time.Now()); err != nil {
		t.Fatalf("validation should pass (nonce is correct for this frame): %v", err)
	}

	// Signature check with the real sender's key must fail — signed by wrong key.
	if err := VerifyFileCommandSignature(frame.Nonce, frame.Signature, pub); err == nil {
		t.Error("signature verification should fail when signed by wrong key")
	}
}

// TestStaleFrameDropped verifies that a frame older than MaxClockDrift is rejected.
func TestStaleFrameDropped(t *testing.T) {
	t.Parallel()

	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")
	payload := "test-payload"
	maxTTL := uint8(10)

	now := time.Now()
	staleTime := now.Add(-time.Duration(domain.FileCommandMaxClockDrift+60) * time.Second)

	nonce := ComputeNonce(src, dst, maxTTL, staleTime.Unix(), payload)
	frame := FileCommandFrame{
		Type:      FileCommandFrameType,
		SRC:       src,
		DST:       dst,
		TTL:       10,
		MaxTTL:    maxTTL,
		Time:      staleTime.Unix(),
		Nonce:     nonce,
		Signature: "placeholder",
		Payload:   payload,
	}

	if err := ValidateFileCommandFrame(frame, now); err == nil {
		t.Error("stale frame (>5 min drift) should fail validation")
	}
}

// TestTamperedSRCDropped verifies that changing SRC breaks nonce binding.
func TestTamperedSRCDropped(t *testing.T) {
	t.Parallel()

	_, priv, _ := ed25519.GenerateKey(nil)
	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")

	frame := NewFileCommandFrame(src, dst, 10, "payload", priv)

	// Tamper with SRC — nonce was computed with original SRC.
	frame.SRC = domain.PeerIdentity("cccc111122223333444455556666777788889999")

	if err := ValidateFileCommandFrame(frame, time.Now()); err == nil {
		t.Error("tampered SRC should fail nonce binding check")
	}
}

// TestTamperedPayloadDropped verifies that changing Payload breaks nonce binding.
func TestTamperedPayloadDropped(t *testing.T) {
	t.Parallel()

	_, priv, _ := ed25519.GenerateKey(nil)
	src := domain.PeerIdentity("aaaa111122223333444455556666777788889999")
	dst := domain.PeerIdentity("bbbb111122223333444455556666777788889999")

	frame := NewFileCommandFrame(src, dst, 10, "original-payload", priv)

	// Tamper with Payload — nonce was computed with original payload.
	frame.Payload = "tampered-payload"

	if err := ValidateFileCommandFrame(frame, time.Now()); err == nil {
		t.Error("tampered payload should fail nonce binding check")
	}
}

// TestTTLZeroDropped verifies that DecrementTTL fails on TTL=0 and that
// a frame with TTL=0 cannot be forwarded (the hop budget is exhausted).
func TestTTLZeroDropped(t *testing.T) {
	t.Parallel()

	frame := FileCommandFrame{TTL: 0, MaxTTL: 10}

	_, err := frame.DecrementTTL()
	if err == nil {
		t.Error("DecrementTTL from 0 should return error (hop budget exhausted)")
	}
}

// TestTTLDecrementedOnForward verifies that TTL is decremented by exactly 1
// while MaxTTL remains unchanged.
func TestTTLDecrementedOnForward(t *testing.T) {
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

	// Decrement again to verify sequential correctness.
	dec2, err := dec.DecrementTTL()
	if err != nil {
		t.Fatalf("second decrement: %v", err)
	}
	if dec2.TTL != 3 {
		t.Errorf("TTL = %d, want 3", dec2.TTL)
	}
}
