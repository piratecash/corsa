package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			// Default to a protocolVersion at the file-transfer
			// cutover (domain.FileCommandMinPeerProtocolVersion) so
			// existing tests that don't care about version still pass
			// the ranking gate. Tests that exercise the cutover (e.g.
			// TestRouter_RelaySkipsBelowMinProtocolVersionPeer) wire
			// their own PeerRouteMeta callback returning a lower
			// version explicitly.
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		// Authenticity is now self-contained in the wire frame
		// (FileCommandFrame.SrcPubKey + identity fingerprint check),
		// so the helper no longer wires a pubkey resolver. The
		// senderPubKeys map serves as the trust-store stand-in for
		// IsAuthorizedForLocalDelivery: presence of an identity in
		// the map means the local node is willing to accept files
		// from that SRC. Boundary tests below
		// (TestRouter_LocalDeliveryRejectsUntrustedSRC,
		// TestRouter_RelayForwardsUntrustedAuthenticatedSRC) drive
		// the resolver explicitly to pin the authenticity-vs-
		// authorization split.
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := senderPubKeys[id]
			return ok
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
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")
	relay2 := domain.PeerIdentity("relay2-node-identity-1234567890a")
	relay3 := domain.PeerIdentity("relay3-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relay1 := domain.PeerIdentity("relay1-node-identity-1234567890a")
	relay2 := domain.PeerIdentity("relay2-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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

	// Hold a reference to the nonce cache so we can assert what was
	// (or, in this case, was NOT) committed. After the SrcPubKey change
	// any peer can produce an authentic transit frame at near-zero CPU
	// cost; without the isFullNode gate sitting BEFORE TryAdd, every
	// such frame would burn a slot in the bounded LRU on a client that
	// will never forward — partial replay of the same denial-of-service
	// vector that the local-delivery auth-before-TryAdd fix neutralised.
	cache := newTestNonceCache()
	tr := &testFileRouter{sentFrames: make(map[domain.PeerIdentity][]json.RawMessage)}
	tr.router = NewRouter(RouterConfig{
		NonceCache: cache,
		LocalID:    localID,
		// isFullNode = false → client node, should not relay AND must
		// not consume replay-cache slots for the dropped frame.
		IsFullNode: func() bool { return false },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(domain.PeerIdentity) (PeerRouteMeta, bool) {
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.deliveredLocal = append(tr.deliveredLocal, frame)
		},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "client-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.sentTo(relay1)) != 0 {
		t.Error("client node should not relay file commands")
	}
	// Core invariant: a transit frame the client cannot forward MUST NOT
	// consume a slot in the bounded LRU. Otherwise an attacker producing
	// authentic-but-undeliverable frames at near-zero CPU cost can evict
	// legitimate nonces and re-open the replay window.
	if cache.Has(frame.Nonce) {
		t.Fatal("client node committed a nonce for a transit frame it cannot forward; replay-cache LRU is consumable by drive-by transit traffic")
	}
}

func TestFileRouterSkipsSelfRoutes(t *testing.T) {
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
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}

	var deliveryCount atomic.Int32
	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	neighborA := domain.PeerIdentity("neighbor-a-identity-1234567890a")
	neighborB := domain.PeerIdentity("neighbor-b-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	onlyNeighbor := domain.PeerIdentity("only-neighbor-identity-12345678")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	neighborA := domain.PeerIdentity("neighbor-a-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayOld := domain.PeerIdentity("relay-old-identity-1234567890ab")
	relayNew := domain.PeerIdentity("relay-new-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			ts, ok := connectedAt[id]
			if !ok {
				return PeerRouteMeta{}, false
			}
			return PeerRouteMeta{
				ConnectedAt:        ts,
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
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

func TestFileRouterSkipsUnusablePeerFromPeerRouteMeta(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	stalledRelay := domain.PeerIdentity("relay-stalled-identity-123456789")
	healthyRelay := domain.PeerIdentity("relay-healthy-identity-123456789")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			if id == stalledRelay {
				return PeerRouteMeta{}, false
			}
			if id == healthyRelay {
				return PeerRouteMeta{
					ConnectedAt:        now.Add(-5 * time.Minute),
					ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
					RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				}, true
			}
			return PeerRouteMeta{}, false
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			ts, ok := connectedAt[id]
			if !ok {
				return PeerRouteMeta{}, false
			}
			return PeerRouteMeta{
				ConnectedAt:        ts,
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			return false
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
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayA := domain.PeerIdentity("relay-a-identity-1234567890ab")
	relayB := domain.PeerIdentity("relay-b-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			return PeerRouteMeta{
				ConnectedAt:        now.Add(-time.Minute),
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
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

// TestRouteCandidateLessOrdering pins the comparator contract used by
// collectRouteCandidates. The order is: protocolVersion DESC, hops ASC,
// connectedAt ASC (older = longer uptime), nextHop lexicographic.
//
// This is the single source of truth for the file-router's "best route"
// definition; any change here must be reflected in docs/routing.md and
// in the comment block above the insertion sort in collectRouteCandidates.
func TestRouteCandidateLessOrdering(t *testing.T) {
	t.Parallel()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		a, b routeCandidate
		// wantALess is true when a is strictly preferred over b.
		wantALess bool
	}{
		{
			name:      "higher protocol version beats lower hops",
			a:         routeCandidate{nextHop: "a", hops: 5, protocolVersion: 7, connectedAt: base},
			b:         routeCandidate{nextHop: "b", hops: 1, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			wantALess: true,
		},
		{
			name:      "equal version: fewer hops wins",
			a:         routeCandidate{nextHop: "a", hops: 1, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: "b", hops: 2, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			wantALess: true,
		},
		{
			name:      "equal version and hops: longer uptime (older connectedAt) wins",
			a:         routeCandidate{nextHop: "a", hops: 2, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			b:         routeCandidate{nextHop: "b", hops: 2, protocolVersion: 6, connectedAt: base},
			wantALess: true,
		},
		{
			name:      "equal everything: lexicographic nextHop wins",
			a:         routeCandidate{nextHop: "a", hops: 2, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: "b", hops: 2, protocolVersion: 6, connectedAt: base},
			wantALess: true,
		},
		{
			name:      "known connectedAt beats zero connectedAt under equal version and hops",
			a:         routeCandidate{nextHop: "a", hops: 2, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: "b", hops: 2, protocolVersion: 6, connectedAt: time.Time{}},
			wantALess: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := routeCandidateLess(tc.a, tc.b); got != tc.wantALess {
				t.Fatalf("routeCandidateLess(a,b)=%v want %v", got, tc.wantALess)
			}
			// Antisymmetry sanity: swapping the inputs must flip the result
			// unless the values are equal under the comparator (no test case
			// here exercises strict equality, so the flip MUST hold).
			if got := routeCandidateLess(tc.b, tc.a); got == tc.wantALess {
				t.Fatalf("routeCandidateLess is not antisymmetric for case %q", tc.name)
			}
		})
	}
}

// TestFileRouterPrefersLegitOverInflatedVersion exercises the primary
// ranking key end-to-end through collectRouteCandidates: an eligible
// peer reporting the legit cutover version (RawProtocolVersion=12,
// ProtocolVersion=12) must win over an inflated-version peer
// (RawProtocolVersion>12, ProtocolVersion clamped to 0 by the node-side
// trustedFileRouteVersion helper) even when the inflated peer has
// fewer hops. Inflated peers stay eligible (Raw>=12 passes the cutover
// filter — see routeCandidate godoc) but the clamped ranking value
// pushes them to the bottom of the protocolVersion-DESC sort.
//
// This is the production-realistic shape of the legacy
// "higher-version-wins" test: with config.ProtocolVersion = 12, no
// peer can legitimately advertise a higher version, so the only way
// 13/14/etc. surface in PeerRouteMeta is the inflated-and-clamped
// case. ProtocolVersion=12 + RawProtocolVersion=12 represents a
// healthy peer at the cutover; ProtocolVersion=0 + RawProtocolVersion=13
// represents an inflated peer kept as last-resort.
func TestFileRouterPrefersLegitOverInflatedVersion(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayClose := domain.PeerIdentity("relay-close-identity-12345678901")
	relayFar := domain.PeerIdentity("relay-far-identity-1234567890123")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayClose, Hops: 2, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayFar, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// Legit: ProtocolVersion mirrors RawProtocolVersion at the cutover.
		relayClose: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
		// Inflated: peer claimed v13, clamped to ranking 0 by node-side
		// trustedFileRouteVersion. Eligible (Raw>=12) but ranked last.
		// Note: even though relayFar has fewer hops, the protocolVersion
		// DESC primary key wins (12 > 0).
		relayFar: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 0, RawProtocolVersion: 13},
	}

	tr := &testFileRouter{sentFrames: make(map[domain.PeerIdentity][]json.RawMessage)}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "legit-over-inflated-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if len(tr.sentTo(relayClose)) != 1 {
		t.Fatalf("expected file router to choose legit-version relayClose (PV=12) over inflated relayFar (PV=0 clamped, Raw=13), got %d sends", len(tr.sentTo(relayClose)))
	}
	if len(tr.sentTo(relayFar)) != 0 {
		t.Fatalf("expected inflated relayFar to be the last-resort candidate, not picked first; got %d sends", len(tr.sentTo(relayFar)))
	}
}

// TestFileRouterEqualVersionFallsBackToHopsThenUptime walks the secondary
// keys: equal version, then fewer hops, then longer uptime.
func TestFileRouterEqualVersionFallsBackToHopsThenUptime(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayClose := domain.PeerIdentity("relay-close-identity-12345678901")
	relayFar := domain.PeerIdentity("relay-far-identity-1234567890123")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayClose, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayFar, Hops: 3, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		relayClose: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 12, RawProtocolVersion: 12},
		relayFar:   {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
	}

	tr := &testFileRouter{sentFrames: make(map[domain.PeerIdentity][]json.RawMessage)}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			_, ok := keys[id]
			return ok
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "version-equal-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	// At equal version, hops dominates uptime; relayClose with fewer hops wins
	// even though relayFar has the longer uptime.
	if len(tr.sentTo(relayClose)) != 1 {
		t.Fatalf("at equal version fewer hops should win, got %d sends to relayClose", len(tr.sentTo(relayClose)))
	}
	if len(tr.sentTo(relayFar)) != 0 {
		t.Fatalf("relayFar (more hops) should be skipped at equal version, got %d sends", len(tr.sentTo(relayFar)))
	}
}

// TestRouterExplainRouteReturnsRankedPlan verifies that ExplainRoute is a
// faithful, read-only projection of the live route-selection path. The
// diagnostic surface (RPC explainFileRoute, console, CLI) relies on this
// property — if ExplainRoute and SendFileCommand ever disagreed on order,
// the diagnostic would lie about where the next byte will actually go.
func TestRouterExplainRouteReturnsRankedPlan(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	relayClose := domain.PeerIdentity("relay-close-identity-12345678901")
	relayFar := domain.PeerIdentity("relay-far-identity-1234567890123")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayClose, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayFar, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// relayClose is a healthy peer at the cutover (RawProtocolVersion=12,
		// ProtocolVersion=12). relayFar represents an inflated peer:
		// it claimed a version higher than config.ProtocolVersion=12, the
		// node-side trustedFileRouteVersion clamped its ranking value to 0
		// while keeping RawProtocolVersion at the actually-reported value
		// (13 here). The clamp pushes relayFar to the bottom of the
		// protocolVersion-DESC sort even though it is one hop closer; the
		// raw value (>= cutover) keeps relayFar eligible as last-resort.
		// This is the only production-realistic shape where
		// ProtocolVersion=0 and the candidate is still in the plan — see
		// PeerRouteMeta godoc and routeCandidate godoc.
		relayClose: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
		relayFar:   {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 0, RawProtocolVersion: 13},
	}

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})

	plan := router.ExplainRoute(dstID)
	if len(plan) != 2 {
		t.Fatalf("expected 2 entries in plan, got %d", len(plan))
	}
	if plan[0].NextHop != relayClose {
		t.Fatalf("expected best entry to be relayClose (legit cutover version PV=12), got %s", plan[0].NextHop)
	}
	if plan[0].ProtocolVersion != 12 {
		t.Fatalf("expected best entry version=12, got %d", plan[0].ProtocolVersion)
	}
	if plan[0].Hops != 1 {
		t.Fatalf("expected best entry hops=1, got %d", plan[0].Hops)
	}
	if plan[1].NextHop != relayFar {
		t.Fatalf("expected fall-back entry to be relayFar (inflated, ranking-clamped), got %s", plan[1].NextHop)
	}
	if plan[1].ProtocolVersion != 0 {
		t.Fatalf("expected fall-back entry to have ProtocolVersion=0 (clamped), got %d", plan[1].ProtocolVersion)
	}
	if plan[1].ConnectedAt.IsZero() {
		t.Fatal("expected fall-back entry to carry a connectedAt timestamp")
	}
}

// TestRouterExplainRoutePromotesDirectSession mirrors SendFileCommand's
// step-1 invariant: the direct session to dst is tried first,
// unconditionally, before any route-table ranking. ExplainRoute therefore
// MUST report the direct path as best=true even when the route-table
// candidate carries an exotic ranking key — otherwise the diagnostic
// would lie about where the next byte is actually going.
//
// Setup: dst has a usable direct session at the file-transfer cutover
// (ProtocolVersion=12, RawProtocolVersion=12). A relay also has a route
// to dst, hop-distance 2, with the inflated-version shape produced by
// the production node-side helper: ProtocolVersion=0 (ranking key
// clamped because the peer reported v > config.ProtocolVersion) but
// RawProtocolVersion=99 (eligibility passes the cutover filter as
// last-resort). Live SendFileCommand would hit the direct session first;
// ExplainRoute must agree, and the clamped relay must show up at the
// bottom of the plan with ProtocolVersion=0 as documented in
// docs/protocol/file_transfer.md "Inflated-version defence".
func TestRouterExplainRoutePromotesDirectSession(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890abc")
	dst := domain.PeerIdentity("destination-identity-1234567890a")
	relay := domain.PeerIdentity("relay-identity-1234567890aabbccdd")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dst: {
				{Identity: dst, NextHop: relay, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// dst has a direct session — that is the live send target.
		dst: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
		// relay reports an inflated version (Raw=99 > config=12). The
		// production node-side helper would clamp the ranking key to 0
		// while keeping RawProtocolVersion at 99 so the candidate stays
		// eligible (Raw>=12) but ranks at the bottom. Here the test
		// covers the direct-session promotion contract; relay's ranking
		// is therefore deliberately worst-case to make the promotion
		// outcome unambiguous.
		relay: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 0, RawProtocolVersion: 99},
	}

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})

	plan := router.ExplainRoute(dst)
	if len(plan) != 2 {
		t.Fatalf("expected 2 entries (direct + relay fall-back), got %d", len(plan))
	}
	if plan[0].NextHop != dst {
		t.Fatalf("expected direct entry at head of plan, got next_hop=%s", plan[0].NextHop)
	}
	if plan[0].Hops != 1 {
		t.Fatalf("direct entry must report hops=1, got %d", plan[0].Hops)
	}
	if plan[0].ProtocolVersion != 12 {
		t.Fatalf("direct entry version=12 expected, got %d", plan[0].ProtocolVersion)
	}
	if plan[1].NextHop != relay {
		t.Fatalf("expected relay as fall-back, got %s", plan[1].NextHop)
	}
}

// TestRouterExplainRouteDeduplicatesDirectAndRoutingTable ensures the
// synthetic direct entry is not double-listed when the routing table
// also carries a NextHop == dst entry (this happens when AddDirectPeer
// has run for dst). The direct candidate is the canonical first entry;
// the table-side duplicate must be filtered out.
func TestRouterExplainRouteDeduplicatesDirectAndRoutingTable(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890abc")
	dst := domain.PeerIdentity("destination-identity-1234567890a")
	relay := domain.PeerIdentity("relay-identity-1234567890aabbccdd")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dst: {
				// AddDirectPeer-style entry: NextHop == dst, hops == 1.
				{Identity: dst, NextHop: dst, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				// Independent relay path.
				{Identity: dst, NextHop: relay, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	meta := map[domain.PeerIdentity]PeerRouteMeta{
		dst:   {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
		relay: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 12, RawProtocolVersion: 12},
	}

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})

	plan := router.ExplainRoute(dst)
	if len(plan) != 2 {
		t.Fatalf("expected exactly 2 entries (direct + relay), got %d", len(plan))
	}
	if plan[0].NextHop != dst || plan[1].NextHop != relay {
		t.Fatalf("expected [direct, relay], got [%s, %s]", plan[0].NextHop, plan[1].NextHop)
	}
}

// TestSendFileCommandSkipsRoutingTableDirectAfterFailedDirectAttempt is
// a regression for the SendFileCommand fall-back path: when the live
// send tries direct and fails, the routing-table fall-back must NOT
// attempt the same dst a second time, even if AddDirectPeer added a
// next_hop == dst entry to the routing table. Otherwise the live send
// walks [direct, direct(again), relay] while the diagnostic ExplainRoute
// shows [direct, relay] — and the two surfaces drift.
//
// Setup:
//   - dst is reachable as a direct file-capable peer (peerRouteMeta returns ok).
//   - sessionSend(dst, …) returns false to force the fall-back.
//   - The routing table holds two entries: NextHop == dst (direct route)
//     and NextHop == relay.
//
// Expectation: sessionSend is called exactly once for dst (the step-1
// attempt) and once for relay (the fall-back), never twice for dst.
func TestSendFileCommandSkipsRoutingTableDirectAfterFailedDirectAttempt(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890abc")
	dst := domain.PeerIdentity("destination-identity-1234567890a")
	relay := domain.PeerIdentity("relay-identity-1234567890aabbccdd")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dst: {
				// AddDirectPeer-style entry: next_hop == dst, hops == 1.
				{Identity: dst, NextHop: dst, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				// Independent relay route.
				{Identity: dst, NextHop: relay, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	meta := map[domain.PeerIdentity]PeerRouteMeta{
		dst:   {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 12, RawProtocolVersion: 12},
		relay: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 12, RawProtocolVersion: 12},
	}

	_, priv, _ := ed25519.GenerateKey(nil)

	var mu sync.Mutex
	attempts := make(map[domain.PeerIdentity]int)

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend: func(target domain.PeerIdentity, _ []byte) bool {
			mu.Lock()
			defer mu.Unlock()
			attempts[target]++
			// Direct (target == dst) always fails so we exercise the
			// fall-back path. Relay accepts.
			return target != dst
		},
		LocalDeliver: func(protocol.FileCommandFrame) {},
	})

	err := router.SendFileCommand(
		dst,
		"recipient-box-key",
		domain.FileCommandPayload{Command: domain.FileActionChunkReq},
		priv,
		func(_ string, payload domain.FileCommandPayload) (string, error) {
			return string(payload.Command), nil
		},
	)
	if err != nil {
		t.Fatalf("SendFileCommand: %v", err)
	}

	mu.Lock()
	directAttempts := attempts[dst]
	relayAttempts := attempts[relay]
	mu.Unlock()

	if directAttempts != 1 {
		t.Fatalf("expected exactly 1 direct sessionSend(dst) attempt (the step-1 try), got %d", directAttempts)
	}
	if relayAttempts != 1 {
		t.Fatalf("expected exactly 1 relay sessionSend attempt, got %d", relayAttempts)
	}
}

// TestRouterExplainRouteEmptyWhenNoRoute guards the empty-result contract:
// callers (RPC, CLI) expect a nil/empty slice, not a panic, when no route
// to dst exists.
func TestRouterExplainRouteEmptyWhenNoRoute(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	unknown := domain.PeerIdentity("unknown-destination-1234567890ab")

	snap := routing.Snapshot{
		TakenAt: time.Now(),
		Routes:  map[domain.PeerIdentity][]routing.RouteEntry{},
	}

	router := NewRouter(RouterConfig{
		NonceCache:                   newTestNonceCache(),
		LocalID:                      localID,
		IsFullNode:                   func() bool { return true },
		RouteSnap:                    func() routing.Snapshot { return snap },
		PeerRouteMeta:                func(domain.PeerIdentity) (PeerRouteMeta, bool) { return PeerRouteMeta{}, false },
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})

	if plan := router.ExplainRoute(unknown); len(plan) != 0 {
		t.Fatalf("expected empty plan for unknown destination, got %d entries", len(plan))
	}
}

// TestFileRouterExcludeViaWinsOverHigherVersion guards the relay-loop
// invariant: split-horizon must drop the via-peer regardless of how
// attractive its protocol version is. Otherwise a transit node could
// reflect the frame back to the neighbour that just delivered it just
// because that neighbour speaks a newer version.
//
// The test calls collectRouteCandidates directly — there is no need to
// run a full HandleInbound pipeline (signature verification, nonce
// commit, …) to exercise the candidate filtering, and stripping the
// pipeline keeps the test focused on the one invariant it asserts.
func TestFileRouterExcludeViaWinsOverHigherVersion(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	dstID := domain.PeerIdentity("destination-identity-1234567890a")
	via := domain.PeerIdentity("incoming-neighbor-identity-12345")
	other := domain.PeerIdentity("other-relay-identity-12345678901")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: via, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: other, Hops: 4, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// via reports an inflated version (Raw=99, ranking clamped to 0)
		// AND has fewer hops — both keys would normally compete with
		// split-horizon. The test pins that split-horizon excludes via
		// regardless of how the rest of routeCandidateLess would score
		// it; choosing the inflated last-resort shape makes the outcome
		// independent of the ranking comparator and exercises the
		// production-realistic clamped state for high reported versions.
		via:   {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: 0, RawProtocolVersion: 99},
		other: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: 12, RawProtocolVersion: 12},
	}

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			m, ok := meta[id]
			return m, ok
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			return false
		},
		SessionSend:  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver: func(protocol.FileCommandFrame) {},
	})

	candidates := router.collectRouteCandidates(dstID, via)

	if len(candidates) != 1 {
		t.Fatalf("expected exactly one candidate after excludeVia, got %d", len(candidates))
	}
	if candidates[0].nextHop != other {
		t.Fatalf("expected only candidate to be %s, got %s", other, candidates[0].nextHop)
	}
}

// --- Authenticity vs authorization boundary ---
//
// The tests below pin the split between data-integrity authenticity
// (self-contained in the wire frame via SrcPubKey + signature) and
// destination-side authorization (IsAuthorizedForLocalDelivery). They
// cover the four cases that matter:
//
//                                 │ frame authenticity │ frame authenticity
//                                 │       valid        │      invalid
//   ──────────────────────────────┼────────────────────┼─────────────────────
//   DST != self (forward path)    │ Forward regardless │ Drop
//                                 │ of trust store     │
//   ──────────────────────────────┼────────────────────┼─────────────────────
//   DST == self (local delivery)  │ Authorize via      │ Drop
//                                 │ trust store; drop  │
//                                 │ if untrusted       │
//
// The "forward regardless of trust" property is the one that lets two
// NAT-ed peers exchange files through any public relay — earlier
// versions sourced pubkeys from the relay's own trust store, which is
// why relay forwarded only between trusted contacts.

func newTestFileRouterAuthorize(
	localID domain.PeerIdentity,
	isFullNode bool,
	snap routing.Snapshot,
	authorized map[domain.PeerIdentity]bool,
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
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool {
			return authorized != nil && authorized[id]
		},
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			if reachableHops != nil && !reachableHops[dst] {
				return false
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

// TestRouter_RelayForwardsAuthenticatedFrameUntrustedSRC is the
// regression guard for the production symptom: two NAT-ed peers could
// not exchange files through any public relay because no public node
// had either of them in its trust store. With self-contained
// authenticity (SrcPubKey + fingerprint check on the frame itself), a
// relay can verify the signature without any peer state at all and
// MUST forward the frame even when SRC is not authorized for local
// delivery (i.e. not in the relay's trust store).
func TestRouter_RelayForwardsAuthenticatedFrameUntrustedSRC(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	hop := domain.PeerIdentity("nexthop-identity-1234567890abcde")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))
	dstID := domain.PeerIdentity("destination-identity-1234567890a")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: hop, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	// Authorization map is empty — relay does NOT trust SRC for local
	// delivery. Forwarding must still succeed because authenticity is
	// self-contained.
	tr := newTestFileRouterAuthorize(localID, true, snap, nil, map[domain.PeerIdentity]bool{hop: true})

	frame := makeSignedFrame(senderID, dstID, 5, "relay-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.sentTo(hop); len(got) != 1 {
		t.Fatalf("relay must forward an authenticated frame regardless of local trust; sent=%d", len(got))
	}
	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("relay must NOT locally deliver a frame whose DST is not self, got %d deliveries", len(got))
	}
}

// TestRouter_DropsFrameWhenSrcPubKeyFingerprintMismatch pins that
// authenticity check rejects frames where SrcPubKey does not hash to
// SRC. Without this an attacker could lift a stranger's SRC and stamp
// their own pubkey to bypass any sender-binding.
func TestRouter_DropsFrameWhenSrcPubKeyFingerprintMismatch(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	_, priv, _ := ed25519.GenerateKey(nil)
	// Use a SRC that is NOT the fingerprint of this pubkey.
	wrongSRC := domain.PeerIdentity("not-the-fingerprint-of-pub-1234")

	snap := routing.Snapshot{TakenAt: time.Now()}
	authorized := map[domain.PeerIdentity]bool{wrongSRC: true} // even if "trusted"
	tr := newTestFileRouterAuthorize(localID, true, snap, authorized, nil)

	frame := makeSignedFrame(wrongSRC, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("frame with mismatched SrcPubKey/SRC must be dropped, got %d deliveries", len(got))
	}
}

// TestRouter_DropsFrameWhenSignatureInvalid pins that the signature is
// verified against the embedded SrcPubKey. A tampered signature must
// be dropped even when fingerprint matches.
func TestRouter_DropsFrameWhenSignatureInvalid(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	authorized := map[domain.PeerIdentity]bool{senderID: true}
	tr := newTestFileRouterAuthorize(localID, true, snap, authorized, nil)

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	// Tamper with signature — flip last hex char.
	if len(frame.Signature) > 0 {
		last := frame.Signature[len(frame.Signature)-1]
		var rep byte = 'a'
		if last == 'a' {
			rep = 'b'
		}
		frame.Signature = frame.Signature[:len(frame.Signature)-1] + string(rep)
	}
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("frame with tampered signature must be dropped, got %d deliveries", len(got))
	}
}

// TestRouter_LocalDeliveryRejectsUntrustedSRC is the authorization
// guard: even with a perfectly authentic frame, an untrusted SRC must
// not deposit files into the local inbox.
func TestRouter_LocalDeliveryRejectsUntrustedSRC(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	// Authorization map empty — SRC is authenticated but NOT trusted.
	tr := newTestFileRouterAuthorize(localID, true, snap, nil, nil)

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("local delivery must drop an authenticated-but-untrusted SRC, got %d deliveries", len(got))
	}
}

// TestRouter_LocalDeliveryUntrustedSRCDoesNotConsumeNonceSlot pins the
// replay-cache poisoning fix: with self-contained authenticity, any
// peer can produce a perfectly signed frame addressed to us using its
// own identity. If the local-delivery branch committed the nonce
// before the trust-store check, an untrusted-but-authentic SRC could
// burn slots in the bounded LRU and evict legitimate nonces, opening
// a cheap denial-of-service path on a node's anti-replay budget.
//
// The fix is order-of-operations: IsAuthorizedForLocalDelivery runs
// FIRST, and only authorized local frames reach TryAdd. We assert
// that contract directly by inspecting the nonce cache after the call.
// The relay path keeps the original authenticity-before-TryAdd order,
// covered by TestRouter_RelayForwardsAuthenticatedFrameUntrustedSRC.
func TestRouter_LocalDeliveryUntrustedSRCDoesNotConsumeNonceSlot(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}

	// Hold a reference to the nonce cache so we can assert what got
	// committed; the helpers wrap one inline and there is no public
	// accessor on Router.
	cache := newTestNonceCache()
	tr := &testFileRouter{sentFrames: make(map[domain.PeerIdentity][]json.RawMessage)}
	tr.router = NewRouter(RouterConfig{
		NonceCache: cache,
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(domain.PeerIdentity) (PeerRouteMeta, bool) {
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion),
			}, true
		},
		// SRC is authentic but NOT in the trust store — the exact
		// shape of an "any peer can sign a frame addressed to me"
		// attack the fix exists to neutralise.
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.deliveredLocal = append(tr.deliveredLocal, frame)
		},
	})

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("untrusted SRC must NOT be delivered locally, got %d deliveries", len(got))
	}
	// Core invariant: the rejected frame's nonce MUST NOT be in the
	// cache. If it were, an attacker could exhaust the LRU at near-zero
	// CPU cost (single signature per nonce) and evict legitimate
	// in-flight nonces from trusted peers.
	if cache.Has(frame.Nonce) {
		t.Fatal("untrusted local frame committed its nonce; replay-cache LRU is consumable by an authentic-but-untrusted SRC")
	}
}

// TestRouter_RelaySkipsBelowMinProtocolVersionPeer pins the protocol-
// version cutover (domain.FileCommandMinPeerProtocolVersion). A v11
// next-hop advertising file_transfer_v1 would receive a v2-formatted
// frame and drop it at its missing-SrcPubKey gate, so the file router
// must NOT pick that next-hop in the first place. The frame must also
// not silently disappear: when the only candidate route is below the
// minimum, forwardToNextHop reports "no active route".
func TestRouter_RelaySkipsBelowMinProtocolVersionPeer(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("relay-node-identity-1234567890ab")
	hop := domain.PeerIdentity("nexthop-identity-1234567890abcde")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))
	dstID := domain.PeerIdentity("destination-identity-1234567890a")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: hop, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	// peerRouteMeta reports v11 for the only candidate — below the cutover.
	belowMinVersion := domain.ProtocolVersion(domain.FileCommandMinPeerProtocolVersion - 1)

	tr := &testFileRouter{sentFrames: make(map[domain.PeerIdentity][]json.RawMessage)}
	tr.router = NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(domain.PeerIdentity) (PeerRouteMeta, bool) {
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    belowMinVersion,
				RawProtocolVersion: belowMinVersion,
			}, true
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.sentFrames[dst] = append(tr.sentFrames[dst], json.RawMessage(data))
			return true
		},
		LocalDeliver: func(frame protocol.FileCommandFrame) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			tr.deliveredLocal = append(tr.deliveredLocal, frame)
		},
	})

	frame := makeSignedFrame(senderID, dstID, 5, "relay-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.sentTo(hop); len(got) != 0 {
		t.Fatalf("relay must NOT pick a next-hop with protocolVersion < FileCommandMinPeerProtocolVersion; sent=%d", len(got))
	}
}

// TestRouter_LocalDeliveryAcceptsTrustedSRC is the happy-path: an
// authentic frame from a trusted SRC reaches the local inbox.
func TestRouter_LocalDeliveryAcceptsTrustedSRC(t *testing.T) {
	t.Parallel()

	localID := domain.PeerIdentity("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentity(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	authorized := map[domain.PeerIdentity]bool{senderID: true}
	tr := newTestFileRouterAuthorize(localID, true, snap, authorized, nil)

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), "")

	if got := tr.localDeliveries(); len(got) != 1 {
		t.Fatalf("authenticated trusted SRC must be delivered locally, got %d deliveries", len(got))
	}
}
