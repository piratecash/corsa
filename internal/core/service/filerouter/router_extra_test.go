package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// TestFileRouterUndeliverableDSTDropped verifies that a frame whose DST has
// no route in the snapshot and is not the local node is silently dropped
// before any expensive crypto checks. This ensures the cheap deliverability
// check (step 2) filters unroutable frames early.
func TestFileRouterUndeliverableDSTDropped(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	unknownDST := domain.PeerIdentity("unknown-dst-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	// Empty routing snapshot — no routes to anyone.
	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, unknownDST, 5, "unroutable-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	pub, _, _ := ed25519.GenerateKey(nil)
	_, wrongPriv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Sign with wrong key — should fail at signature verification.
	// Wire-frame's SrcPubKey will reflect wrongPriv's pubkey (constructor
	// derives it from privateKey), so fingerprint(SrcPubKey) won't match
	// senderID — the frame is now dropped at the fingerprint check before
	// reaching signature verify. The test contract still holds: an
	// improperly signed frame is rejected and not delivered locally.
	frame := makeSignedFrame(senderID, localID, 5, "bad-sig-payload", wrongPriv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.localDeliveries()) != 0 {
		t.Error("frame with invalid signature should not be delivered")
	}
}

// TestFileRouterFullNodeRelaysToDestination verifies that a full node correctly
// relays a file command frame to the next hop when DST != localID.
func TestFileRouterFullNodeRelays(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.localDeliveries()) != 0 {
		t.Error("frame from unknown sender should not be delivered")
	}
}

// TestFileRouterDropsRoutesExpiredByWallClockButLiveInSnapshot is the
// regression test for the TTL fix in collectRouteCandidates: if a cached
// routing.Snapshot has a TakenAt that is older than the route's
// ExpiresAt — i.e. by snap.TakenAt the route was still alive — but
// wall-clock time is already past ExpiresAt, the route is dead. Earlier
// the filter compared IsExpired(snap.TakenAt) and let such routes
// through, and trySendToCandidates does not re-check expiry, so the
// file router would happily forward a frame down a dead path.
//
// Setup:
//
//   - snap.TakenAt = now - 30s
//   - route.ExpiresAt = now - 5s
//
// snap.TakenAt < route.ExpiresAt → IsExpired(snap.TakenAt) returns false.
// time.Now() > route.ExpiresAt → IsExpired(now) returns true.
// The current implementation evaluates against time.Now() and must drop
// this route. If a future refactor reverts collectRouteCandidates to
// compare against snap.TakenAt the candidate set will become non-empty,
// SessionSend will be invoked for the (dead) next-hop, and this test
// will fail at sentTo(relay1).
func TestFileRouterDropsRoutesExpiredByWallClockButLiveInSnapshot(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	now := time.Now()
	staleTakenAt := now.Add(-30 * time.Second)  // snap was published 30s ago
	routeExpiresAt := now.Add(-5 * time.Second) // route already wall-clock expired
	if !staleTakenAt.Before(routeExpiresAt) {
		t.Fatal("test fixture invariant: snap.TakenAt must precede route.ExpiresAt " +
			"so IsExpired(snap.TakenAt) returns false while IsExpired(time.Now()) returns true")
	}

	snap := routing.Snapshot{
		TakenAt: staleTakenAt,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{
					Identity:  dstID,
					NextHop:   relay1,
					Hops:      2,
					ExpiresAt: routeExpiresAt,
				},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	// Mark relay1 as reachable so a regression that lets the route
	// through cannot be masked by an unreachable next-hop: if the
	// router decides to forward, SessionSend will succeed and
	// sentTo(relay1) will be non-empty, making the regression visible.
	reachable := map[domain.PeerIdentity]bool{relay1: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "expired-route-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.sentTo(relay1); len(got) != 0 {
		t.Fatalf("file router forwarded a frame through a wall-clock-expired route; "+
			"the route is alive vs snap.TakenAt but dead vs time.Now(), so the candidate "+
			"set must be empty. sentTo(relay1) returned %d frame(s)", len(got))
	}
	if got := len(tr.localDeliveries()); got != 0 {
		t.Fatalf("non-local DST should not produce local delivery; got %d", got)
	}

	// The frame was un-deliverable: every cached route to DST was
	// wall-clock expired, so the early deliverability gate (step 2)
	// rejects it before signature verification, AND the relay path
	// (step 8) rejects it before the nonce commit. Either gate is
	// enough to keep the bounded LRU clean — but if a future refactor
	// reverts step 2 to the key-only check (`_, ok := snap.Routes[DST]`)
	// AND step 8 back to TryAdd-before-collect, the nonce ends up in
	// the cache for a frame the router never actually forwards. That
	// regression poisons the bounded LRU under attack: an adversary
	// produces authentic frames addressed to peers whose only routes
	// are stale-but-cached, and every such frame burns a slot. The
	// assertion below catches the regression by re-injecting the same
	// frame and asserting that step 1 did NOT see it as already-cached.
	if tr.nonceCache.Has(frame.Nonce) {
		t.Fatal("nonce was committed to the replay cache for an un-deliverable frame; " +
			"either the early deliverability gate or the relay collect-before-TryAdd " +
			"order regressed, allowing an attacker to evict bounded-LRU slots by sending " +
			"authentic frames addressed to peers whose only routes are wall-clock expired")
	}
}

// TestFileRouterDoesNotCommitNonceWhenSplitHorizonErasesOnlyCandidate is
// the full-pipeline regression test for the split-horizon leg of the
// nonce-leak fix in step 8 of HandleInbound. It synthesises the case
// that hasViableRoute (step 2) cannot detect by design:
//
//   - Cached snapshot has exactly one live, non-expired, non-self route
//     to DST. The early deliverability gate at step 2 sees it and lets
//     the frame proceed.
//   - That single route's NextHop equals the peer that delivered the
//     frame to us (incomingPeer). Step 8's collectRouteCandidates is
//     called with excludeVia == incomingPeer; split-horizon erases the
//     only entry, leaving the candidate slice empty.
//   - The relay path must drop the frame WITHOUT calling nonceCache.TryAdd.
//     Doing TryAdd before the collect (the pre-fix order) would commit
//     a nonce for a frame that never gets forwarded — exactly the
//     bounded-LRU eviction vector the fix closes.
//
// TestFileRouterDropsRoutesExpiredByWallClockButLiveInSnapshot covers
// the TTL-expiry leg of the same fix; this test covers the remaining
// leg (split-horizon). Together they pin both legs of the
// collect-before-TryAdd invariant.
//
// Existing TestFileRouterExcludeViaWinsOverHigherVersion in router_test.go
// asserts split-horizon at the collectRouteCandidates unit level, but
// does not run HandleInbound and therefore cannot observe the nonce
// commit decision. Adding the full-pipeline assertion here closes the
// gap: a future refactor that puts TryAdd back ahead of collect, OR
// drops the split-horizon excludeVia argument, would fail this test.
func TestFileRouterDoesNotCommitNonceWhenSplitHorizonErasesOnlyCandidate(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	// incomingPeer is BOTH the previous hop AND the only NextHop in our
	// routing table for DST. Split-horizon must therefore drop the only
	// candidate and the relay path must refuse to commit the nonce.
	incomingPeer := domain.PeerIdentity("incoming-neighbor-identity-12345")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				// Single live route, NextHop == incomingPeer. hasViableRoute
				// (step 2) sees this as viable — it does not know about
				// excludeVia. Only step 8's collectRouteCandidates with
				// excludeVia==incomingPeer can erase it.
				{
					Identity:  dstID,
					NextHop:   incomingPeer,
					Hops:      1,
					ExpiresAt: now.Add(time.Minute),
				},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	// Mark incomingPeer as reachable so a regression that lets the
	// frame through cannot be masked by an unreachable next-hop:
	// SessionSend would succeed and the test would observe a forwarded
	// frame, making the regression impossible to miss.
	reachable := map[domain.PeerIdentity]bool{incomingPeer: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "split-horizon-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	// Pass incomingPeer as the previous hop so HandleInbound applies
	// split-horizon against it.
	tr.router.HandleInbound(json.RawMessage(raw), incomingPeer)

	if got := tr.sentTo(incomingPeer); len(got) != 0 {
		t.Fatalf("file router reflected the frame back to the previous hop; "+
			"split-horizon must drop NextHop == incomingPeer regardless of how "+
			"attractive the route is. sentTo(incomingPeer) returned %d frame(s)", len(got))
	}
	if got := len(tr.localDeliveries()); got != 0 {
		t.Fatalf("non-local DST should not produce local delivery; got %d", got)
	}
	if tr.nonceCache.Has(frame.Nonce) {
		t.Fatal("nonce was committed to the replay cache for a frame whose only " +
			"forwarding candidate was erased by split-horizon. The relay path " +
			"must collect candidates BEFORE TryAdd; reverting to TryAdd-first " +
			"lets an attacker holding a route to himself evict bounded-LRU slots " +
			"by reflecting frames onto themselves")
	}
}

// TestFileRouterRejectsTTLInflation verifies that a frame with TTL inflated
// above MaxTTL by a malicious relay is rejected before the router applies
// its own decrement. Without the pre-decrement validation, TTL = MaxTTL + 1
// would be decremented to MaxTTL, passing the TTL <= MaxTTL check.
func TestFileRouterRejectsTTLInflation(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Create a valid signed frame with TTL=5 (MaxTTL=5).
	frame := makeSignedFrame(senderID, localID, 5, "ttl-inflation-test", priv)

	// Simulate malicious relay inflating TTL to MaxTTL + 1.
	frame.TTL = frame.MaxTTL + 1
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	// Create a valid frame with TTL=5 (MaxTTL=5), then simulate 2 honest
	// hops that decremented TTL to 3.
	frame := makeSignedFrame(senderID, localID, 5, "valid-ttl-test", priv)
	frame.TTL = 3 // 2 hops already consumed
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.localDeliveries()) != 1 {
		t.Errorf("valid frame with TTL < MaxTTL should be delivered, got %d deliveries", len(tr.localDeliveries()))
	}
}
