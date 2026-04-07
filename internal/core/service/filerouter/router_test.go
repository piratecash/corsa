package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// testNonceCache is a simple in-memory NonceCache for testing.
type testNonceCache struct {
	mu     sync.Mutex
	nonces map[string]struct{}
}

func newTestNonceCache() *testNonceCache {
	return &testNonceCache{nonces: make(map[string]struct{})}
}

func (c *testNonceCache) Has(nonce string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.nonces[nonce]
	return ok
}

func (c *testNonceCache) TryAdd(nonce string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.nonces[nonce]; ok {
		return false
	}
	c.nonces[nonce] = struct{}{}
	return true
}

// testFileRouter builds a Router with controllable stubs for testing.
type testFileRouter struct {
	router *Router

	mu             sync.Mutex
	sentFrames     map[domain.PeerIdentity][]json.RawMessage
	deliveredLocal []protocol.FileCommandFrame
}

func newTestFileRouter(
	localID domain.PeerIdentity,
	isFullNode bool,
	snap routing.Snapshot,
	senderPubKeys map[domain.PeerIdentity]ed25519.PublicKey,
	reachableHops map[domain.PeerIdentity]bool,
) *testFileRouter {
	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
	}

	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return isFullNode },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			k, ok := senderPubKeys[id]
			return k, ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			if reachableHops != nil {
				if !reachableHops[dst] {
					return false
				}
			}
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.deliveredLocal = append(tr.deliveredLocal, frame)
		},
	})

	return tr
}

func (tr *testFileRouter) sentTo(dst domain.PeerIdentity) []json.RawMessage {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.sentFrames[dst]
}

func (tr *testFileRouter) localDeliveries() []protocol.FileCommandFrame {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.deliveredLocal
}

// makeSignedFrame creates a valid, signed FileCommandFrame for testing.
func makeSignedFrame(
	src, dst domain.PeerIdentity,
	ttl uint8,
	payload string,
	priv ed25519.PrivateKey,
) protocol.FileCommandFrame {
	return protocol.NewFileCommandFrame(src, dst, ttl, payload, priv)
}

func TestFileRouterLocalDelivery(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, "test-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	deliveries := tr.localDeliveries()
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 local delivery, got %d", len(deliveries))
	}
	if deliveries[0].SRC != senderID {
		t.Errorf("delivered frame SRC = %q, want %q", deliveries[0].SRC, senderID)
	}
}

func TestFileRouterReplayRejection(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, "test-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))
	tr.router.HandleInbound(json.RawMessage(raw)) // replay

	deliveries := tr.localDeliveries()
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery (replay rejected), got %d", len(deliveries))
	}
}

func TestFileRouterForwardMultipleRoutes(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")
	relay2 := domain.PeerIdentity("relay2-node-identity-1234567890a")
	relay3 := domain.PeerIdentity("relay3-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relay1, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relay2, Hops: 3, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relay3, Hops: 4, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	// relay1 is unreachable, relay2 is reachable.
	reachable := map[domain.PeerIdentity]bool{
		relay1: false,
		relay2: true,
		relay3: true,
	}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "relay-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// Frame should have been forwarded to relay2 (first reachable, sorted by hops).
	sentToRelay1 := tr.sentTo(relay1)
	sentToRelay2 := tr.sentTo(relay2)
	sentToRelay3 := tr.sentTo(relay3)

	if len(sentToRelay1) != 0 {
		t.Error("should not have sent to unreachable relay1")
	}
	if len(sentToRelay2) != 1 {
		t.Fatalf("expected 1 frame sent to relay2, got %d", len(sentToRelay2))
	}
	if len(sentToRelay3) != 0 {
		t.Error("should not have sent to relay3 after relay2 succeeded")
	}
}

func TestFileRouterForwardAllRoutesFail(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")
	relay2 := domain.PeerIdentity("relay2-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relay1, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relay2, Hops: 3, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	// Both relays unreachable.
	reachable := map[domain.PeerIdentity]bool{
		relay1: false,
		relay2: false,
	}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "relay-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// No frames should have been sent.
	if len(tr.sentTo(relay1)) != 0 || len(tr.sentTo(relay2)) != 0 {
		t.Error("no frames should be delivered when all routes fail")
	}

	// No local delivery either.
	if len(tr.localDeliveries()) != 0 {
		t.Error("should not deliver locally when DST != localID")
	}
}

func TestFileRouterClientNodeNoRelay(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("client-node-identity-123456789a")
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

	// isFullNode = false → client node, should not relay.
	tr := newTestFileRouter(localID, false, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "client-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	if len(tr.sentTo(relay1)) != 0 {
		t.Error("client node should not relay file commands")
	}
}

func TestFileRouterSkipsSelfRoutes(t *testing.T) {
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
				// Self-route (our own announcement) should be skipped.
				{Identity: dstID, NextHop: localID, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				// Real route via relay1.
				{Identity: dstID, NextHop: relay1, Hops: 3, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{relay1: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "test-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw))

	// Should forward via relay1, not self.
	if len(tr.sentTo(relay1)) != 1 {
		t.Fatalf("expected 1 frame sent to relay1, got %d", len(tr.sentTo(relay1)))
	}
}

// TestFileRouterConcurrentDuplicateDelivery verifies the anti-replay guarantee
// under concurrent delivery: when the same valid frame is delivered
// simultaneously by N goroutines (simulating arrival via multiple peers),
// exactly one delivery succeeds. This is the regression test for the Has/Add
// TOCTOU race that the TryAdd commit step closes.
func TestFileRouterConcurrentDuplicateDelivery(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")

	pub, priv, _ := ed25519.GenerateKey(nil)

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	var deliveryCount atomic.Int32
	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			k, ok := keys[id]
			return k, ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			deliveryCount.Add(1)
		},
	})

	frame := makeSignedFrame(senderID, localID, 5, "concurrent-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	const goroutines = 50
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			router.HandleInbound(json.RawMessage(raw))
		}()
	}

	close(start)
	wg.Wait()

	if d := deliveryCount.Load(); d != 1 {
		t.Errorf("exactly 1 local delivery expected under concurrent duplicate frames, got %d", d)
	}
}
