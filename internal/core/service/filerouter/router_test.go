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
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			return time.Now(), true
		},
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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	tr.router.HandleInbound(json.RawMessage(raw), "")
	tr.router.HandleInbound(json.RawMessage(raw), "") // replay

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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

	tr.router.HandleInbound(json.RawMessage(raw), "")

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
			router.HandleInbound(json.RawMessage(raw), "")
		}()
	}

	close(start)
	wg.Wait()

	if d := deliveryCount.Load(); d != 1 {
		t.Errorf("exactly 1 local delivery expected under concurrent duplicate frames, got %d", d)
	}
}

// TestFileRouterSplitHorizonExcludesIncomingPeer verifies that when a
// transit node forwards a FileCommandFrame, the route whose NextHop
// equals the incomingPeer (the neighbor the frame arrived from) is
// excluded from the candidate set. Without this split-horizon, the
// transit node would reflect the frame straight back to the previous
// hop, causing a bandwidth-wasting ping-pong loop that continues
// until TTL expires.
func TestFileRouterSplitHorizonExcludesIncomingPeer(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	neighborA := domain.PeerIdentity("neighbor-a-identity-1234567890a")
	neighborB := domain.PeerIdentity("neighbor-b-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				// neighborA is the shortest route but is also the
				// incoming peer — must be excluded by split-horizon.
				{Identity: dstID, NextHop: neighborA, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: neighborB, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{
		neighborA: true,
		neighborB: true,
	}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "split-horizon-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	// neighborA is the previous hop — must NOT receive the forwarded frame.
	tr.router.HandleInbound(json.RawMessage(raw), neighborA)

	if len(tr.sentTo(neighborA)) != 0 {
		t.Error("split-horizon violation: frame was reflected back to incoming peer neighborA")
	}
	if len(tr.sentTo(neighborB)) != 1 {
		t.Fatalf("expected frame forwarded to neighborB, got %d", len(tr.sentTo(neighborB)))
	}
}

// TestFileRouterSplitHorizonAllRoutesExcluded verifies that when the only
// available route is back through the incoming peer, the frame is silently
// dropped rather than reflected.
func TestFileRouterSplitHorizonAllRoutesExcluded(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	onlyNeighbor := domain.PeerIdentity("only-neighbor-identity-12345678")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: onlyNeighbor, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{onlyNeighbor: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "dead-end-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	// The only route is via the incoming peer — frame must be dropped.
	tr.router.HandleInbound(json.RawMessage(raw), onlyNeighbor)

	if len(tr.sentTo(onlyNeighbor)) != 0 {
		t.Error("frame must not be reflected back to the only neighbor when it is the incoming peer")
	}
	if len(tr.localDeliveries()) != 0 {
		t.Error("should not deliver locally when DST != localID")
	}
}

// TestFileRouterEmptyIncomingPeerDisablesSplitHorizon confirms that passing
// an empty incomingPeer (locally-originated or test-injected frame) does
// not exclude any route candidates.
func TestFileRouterEmptyIncomingPeerDisablesSplitHorizon(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	neighborA := domain.PeerIdentity("neighbor-a-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: neighborA, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	reachable := map[domain.PeerIdentity]bool{neighborA: true}

	tr := newTestFileRouter(localID, true, snap, keys, reachable)

	frame := makeSignedFrame(senderID, dstID, 5, "no-exclusion-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	// Empty incomingPeer → no split-horizon, neighborA should receive the frame.
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.sentTo(neighborA)) != 1 {
		t.Fatalf("empty incomingPeer should not exclude any route; expected 1 send to neighborA, got %d", len(tr.sentTo(neighborA)))
	}
}

func TestFileRouterEqualHopsPrefersLongestConnectedPeer(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayOld := domain.PeerIdentity("relay-old-identity-1234567890ab")
	relayNew := domain.PeerIdentity("relay-new-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayNew, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayOld, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	connectedAt := map[domain.PeerIdentity]time.Time{
		relayOld: now.Add(-10 * time.Minute),
		relayNew: now.Add(-1 * time.Minute),
	}

	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
	}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			ts, ok := connectedAt[id]
			return ts, ok
		},
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			k, ok := keys[id]
			return k, ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "tie-break-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.sentTo(relayOld)) != 1 {
		t.Fatalf("expected file router to prefer longer-connected equal-hop peer relayOld, got %d sends", len(tr.sentTo(relayOld)))
	}
	if len(tr.sentTo(relayNew)) != 0 {
		t.Fatalf("expected relayNew not to be used after stable equal-hop route succeeded, got %d sends", len(tr.sentTo(relayNew)))
	}
}

func TestFileRouterSkipsUnusablePeerFromPeerUsableAt(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	stalledRelay := domain.PeerIdentity("relay-stalled-identity-123456789")
	healthyRelay := domain.PeerIdentity("relay-healthy-identity-123456789")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: stalledRelay, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: healthyRelay, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
	}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			if id == stalledRelay {
				return time.Time{}, false
			}
			if id == healthyRelay {
				return now.Add(-5 * time.Minute), true
			}
			return time.Time{}, false
		},
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			k, ok := keys[id]
			return k, ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "skip-stalled-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.sentTo(stalledRelay)) != 0 {
		t.Fatalf("stalled relay must be filtered out before send attempt, got %d sends", len(tr.sentTo(stalledRelay)))
	}
	if len(tr.sentTo(healthyRelay)) != 1 {
		t.Fatalf("healthy relay should receive the frame after stalled route is filtered, got %d sends", len(tr.sentTo(healthyRelay)))
	}
}

func TestSendFileCommandRouteTableFallbackPrefersLongestConnectedEqualHop(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayOld := domain.PeerIdentity("relay-old-identity-1234567890ab")
	relayNew := domain.PeerIdentity("relay-new-identity-1234567890ab")

	_, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayNew, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayOld, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	connectedAt := map[domain.PeerIdentity]time.Time{
		relayOld: now.Add(-10 * time.Minute),
		relayNew: now.Add(-1 * time.Minute),
	}

	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
	}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			ts, ok := connectedAt[id]
			return ts, ok
		},
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			return nil, false
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			if dst == dstID {
				return false
			}
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	err := tr.router.SendFileCommand(
		dstID,
		"recipient-box-key",
		domain.FileCommandPayload{Command: domain.FileActionChunkReq},
		priv,
		func(_ string, payload domain.FileCommandPayload) (string, error) {
			return string(payload.Command), nil
		},
	)
	if err != nil {
		t.Fatalf("SendFileCommand returned error: %v", err)
	}

	if len(tr.sentTo(dstID)) != 0 {
		t.Fatalf("direct destination send should fail and fall back to route table, got %d direct sends", len(tr.sentTo(dstID)))
	}
	if len(tr.sentTo(relayOld)) != 1 {
		t.Fatalf("expected route-table fallback to prefer relayOld, got %d sends", len(tr.sentTo(relayOld)))
	}
	if len(tr.sentTo(relayNew)) != 0 {
		t.Fatalf("expected relayNew not to be used after relayOld succeeded, got %d sends", len(tr.sentTo(relayNew)))
	}
}

func TestFileRouterDeduplicatesCandidatesByNextHop(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	senderID := domain.PeerIdentity("sender-node-identity-1234567890")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayA := domain.PeerIdentity("relay-a-identity-1234567890ab")
	relayB := domain.PeerIdentity("relay-b-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, Origin: relayA, NextHop: relayA, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, Origin: relayB, NextHop: relayA, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, Origin: relayB, NextHop: relayB, Hops: 3, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	attempts := make(map[domain.PeerIdentity]int)
	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
	}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerUsableAt: func(id domain.PeerIdentity) (time.Time, bool) {
			return now.Add(-time.Minute), true
		},
		PeerPubKey: func(id domain.PeerIdentity) (ed25519.PublicKey, bool) {
			k, ok := keys[id]
			return k, ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			attempts[dst]++
			return false
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "dedup-next-hop-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if attempts[relayA] != 1 {
		t.Fatalf("expected relayA to be attempted once despite duplicate next-hop routes, got %d", attempts[relayA])
	}
	if attempts[relayB] != 1 {
		t.Fatalf("expected relayB to be attempted once, got %d", attempts[relayB])
	}
}
