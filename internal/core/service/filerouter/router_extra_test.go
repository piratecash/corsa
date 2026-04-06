package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"corsa/internal/core/domain"
	"corsa/internal/core/protocol"
	"corsa/internal/core/routing"
)

// TestFileRouterUndeliverableDSTDropped verifies that a frame whose DST has
// no route in the snapshot and is not the local node is silently dropped
// before any expensive crypto checks. This ensures the cheap deliverability
// check (step 2) filters unroutable frames early.
func TestFileRouterUndeliverableDSTDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	unknownDST := domain.PeerIdentity("unknown-dst-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	// Empty routing snapshot — no routes to anyone.
	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, unknownDST, 5, "unroutable-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// No local delivery — DST != localID.
	if len(tr.localDeliveries()) != 0 {
		t.Error("undeliverable DST should not produce local delivery")
	}

	// No forwarding — no route exists.
	if len(tr.sentTo(unknownDST)) != 0 {
		t.Error("undeliverable DST should not be forwarded")
	}
}

// TestFileRouterTTLZeroAtRouterDropped verifies that a frame arriving with
// TTL=0 is dropped at the DecrementTTL check (step 3). The hop budget is
// exhausted, so the frame must not be delivered or forwarded.
func TestFileRouterTTLZeroAtRouterDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relay1, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{relay1: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	// Build a valid frame with MaxTTL=5, then manually set TTL=0 to simulate
	// a frame that has been forwarded through all its hops.
	// NewFileCommandFrame clamps TTL=0 to 10, so we construct manually.
	maxTTL := uint8(5)
	ts := time.Now().Unix()
	payload := "ttl-zero-payload"
	nonce := protocol.ComputeNonce(senderID, dstID, maxTTL, ts, payload)
	sig := protocol.SignFileCommand(nonce, priv)

	frame := protocol.FileCommandFrame{
		Type:      protocol.FileCommandFrameType,
		SRC:       senderID,
		DST:       dstID,
		TTL:       0, // hop budget exhausted
		MaxTTL:    maxTTL,
		Time:      ts,
		Nonce:     nonce,
		Signature: sig,
		Payload:   payload,
	}

	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw))

	// No local delivery — frame dropped at step 3 (DecrementTTL fails).
	if len(tr.localDeliveries()) != 0 {
		t.Error("frame with TTL=0 should not deliver locally")
	}

	// No forwarding — frame dropped at step 3.
	if len(tr.sentTo(relay1)) != 0 {
		t.Error("frame with TTL=0 should not be forwarded")
	}
}

// TestFileRouterStaleFrameDropped verifies that a frame with a timestamp older
// than MaxClockDrift is dropped at validation step 4 (freshness check).
func TestFileRouterStaleFrameDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Create a frame with a stale timestamp (10 minutes ago, well beyond 5-min drift).
	now := time.Now()
	staleTime := now.Add(-time.Duration(domain.FileCommandMaxClockDrift+300) * time.Second)

	nonce := protocol.ComputeNonce(senderID, localID, 10, staleTime.Unix(), "stale-payload")
	sig := protocol.SignFileCommand(nonce, priv)

	frame := protocol.FileCommandFrame{
		Type:      protocol.FileCommandFrameType,
		SRC:       senderID,
		DST:       localID,
		TTL:       10,
		MaxTTL:    10,
		Time:      staleTime.Unix(),
		Nonce:     nonce,
		Signature: sig,
		Payload:   "stale-payload",
	}

	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw))

	// Stale frame must be rejected — no local delivery.
	if len(tr.localDeliveries()) != 0 {
		t.Error("stale frame should not be delivered locally")
	}
}

// TestFileRouterInvalidSignatureDropped verifies that a frame signed by the
// wrong key is dropped at step 6 (signature verification).
func TestFileRouterInvalidSignatureDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, _, _ := ed25519.GenerateKey(nil)
	_, wrongPriv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Sign with wrong key — should fail at signature verification.
	frame := makeSignedFrame(senderID, localID, 5, "bad-sig-payload", wrongPriv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	if len(tr.localDeliveries()) != 0 {
		t.Error("frame with invalid signature should not be delivered")
	}
}

// TestFileRouterFullNodeRelaysToDestination verifies that a full node correctly
// relays a file command frame to the next hop when DST != localID.
func TestFileRouterFullNodeRelays(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relay1, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{relay1: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "relay-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// Should be forwarded to relay1.
	if len(tr.sentTo(relay1)) != 1 {
		t.Fatalf("expected 1 frame forwarded to relay1, got %d", len(tr.sentTo(relay1)))
	}

	// No local delivery.
	if len(tr.localDeliveries()) != 0 {
		t.Error("non-local DST should not produce local delivery")
	}
}

// TestFileRouterUnknownSenderKeyDropped verifies that a frame from a sender
// whose public key is not in the trust store is dropped at step 6.
func TestFileRouterUnknownSenderKeyDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("unknown-sender-identity-1234567")

	_, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	// Empty keys map — sender is unknown.
	keys := map[domain.PeerIdentity]ed25519.PublicKey{}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, "unknown-sender", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	if len(tr.localDeliveries()) != 0 {
		t.Error("frame from unknown sender should not be delivered")
	}
}

// TestFileRouterRejectsTTLInflation verifies that a frame with TTL inflated
// above MaxTTL by a malicious relay is rejected before the router applies
// its own decrement. Without the pre-decrement validation, TTL = MaxTTL + 1
// would be decremented to MaxTTL, passing the TTL <= MaxTTL check.
func TestFileRouterRejectsTTLInflation(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Create a valid signed frame with TTL=5 (MaxTTL=5).
	frame := makeSignedFrame(senderID, localID, 5, "ttl-inflation-test", priv)

	// Simulate malicious relay inflating TTL to MaxTTL + 1.
	frame.TTL = frame.MaxTTL + 1
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	if len(tr.localDeliveries()) != 0 {
		t.Error("frame with inflated TTL (MaxTTL+1) should be rejected, not delivered")
	}
}

// TestFileRouterAcceptsValidTTLBelowMaxTTL verifies that a frame with TTL
// legitimately below MaxTTL (e.g. after traversing intermediate hops) is
// accepted normally.
func TestFileRouterAcceptsValidTTLBelowMaxTTL(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Create a valid frame with TTL=5 (MaxTTL=5), then simulate 2 honest
	// hops that decremented TTL to 3.
	frame := makeSignedFrame(senderID, localID, 5, "valid-ttl-test", priv)
	frame.TTL = 3 // 2 hops already consumed
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	if len(tr.localDeliveries()) != 1 {
		t.Errorf("valid frame with TTL < MaxTTL should be delivered, got %d deliveries", len(tr.localDeliveries()))
	}
}
