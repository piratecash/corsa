package filerouter

import (
	"crypto/ed25519"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// testFixturePeerProtocolVersion is the placeholder protocol version
// returned by default fixture PeerRouteMeta callbacks. It is pinned at
// the current production ProtocolVersion / MinimumProtocolVersion (14)
// so the default fixture describes a peer the live build would actually
// admit; ranking-DESC tests stay deterministic. Tests exercising
// specific ranking outcomes wire their own callback returning the
// version they need.
const testFixturePeerProtocolVersion = 14

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

	// nonceCache is exposed so tests that exercise the anti-replay
	// commit path (e.g. the "frame dropped without committing the
	// nonce" assertion in the wall-clock-expiry regression) can
	// observe whether a given nonce was actually inserted.
	nonceCache *testNonceCache

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
	nonceCache := newTestNonceCache()
	tr := &testFileRouter{
		sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
		nonceCache: nonceCache,
	}

	tr.router = NewRouter(RouterConfig{
		NonceCache: nonceCache,
		LocalID:    localID,
		IsFullNode: func() bool { return isFullNode },
		RouteSnap:  func() routing.Snapshot { return snap },
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			// Default to a fixed positive protocol version so existing
			// tests that don't care about version still rank
			// deterministically.
			return PeerRouteMeta{
				ConnectedAt:        time.Now(),
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	localID := domaintest.ID("local-node-identity-1234567890ab")
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, "test-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("local-node-identity-1234567890ab")
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	tr := newTestFileRouter(localID, true, snap, keys, nil)

	frame := makeSignedFrame(senderID, localID, 5, "test-payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{}) // replay

	deliveries := tr.localDeliveries()
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery (replay rejected), got %d", len(deliveries))
	}
}

func TestFileRouterForwardMultipleRoutes(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relay1 := domaintest.ID("relay1-node-identity-1234567890a")
	relay2 := domaintest.ID("relay2-node-identity-1234567890a")
	relay3 := domaintest.ID("relay3-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relay1 := domaintest.ID("relay1-node-identity-1234567890a")
	relay2 := domaintest.ID("relay2-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("client-node-identity-123456789a")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relay1 := domaintest.ID("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relay1 := domaintest.ID("relay1-node-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("local-node-identity-1234567890ab")
	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
			router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})
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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	neighborA := domaintest.ID("neighbor-a-identity-1234567890a")
	neighborB := domaintest.ID("neighbor-b-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	onlyNeighbor := domaintest.ID("only-neighbor-identity-12345678")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	neighborA := domaintest.ID("neighbor-a-identity-1234567890a")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if len(tr.sentTo(neighborA)) != 1 {
		t.Fatalf("empty incomingPeer should not exclude any route; expected 1 send to neighborA, got %d", len(tr.sentTo(neighborA)))
	}
}

func TestFileRouterEqualHopsPrefersLongestConnectedPeer(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayOld := domaintest.ID("relay-old-identity-1234567890ab")
	relayNew := domaintest.ID("relay-new-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if len(tr.sentTo(relayOld)) != 1 {
		t.Fatalf("expected file router to prefer longer-connected equal-hop peer relayOld, got %d sends", len(tr.sentTo(relayOld)))
	}
	if len(tr.sentTo(relayNew)) != 0 {
		t.Fatalf("expected relayNew not to be used after stable equal-hop route succeeded, got %d sends", len(tr.sentTo(relayNew)))
	}
}

func TestFileRouterSkipsUnusablePeerFromPeerRouteMeta(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	stalledRelay := domaintest.ID("relay-stalled-identity-123456789")
	healthyRelay := domaintest.ID("relay-healthy-identity-123456789")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
					ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
					RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if len(tr.sentTo(stalledRelay)) != 0 {
		t.Fatalf("stalled relay must be filtered out before send attempt, got %d sends", len(tr.sentTo(stalledRelay)))
	}
	if len(tr.sentTo(healthyRelay)) != 1 {
		t.Fatalf("healthy relay should receive the frame after stalled route is filtered, got %d sends", len(tr.sentTo(healthyRelay)))
	}
}

func TestSendFileCommandRouteTableFallbackPrefersLongestConnectedEqualHop(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("sender-node-identity-1234567890")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayOld := domaintest.ID("relay-old-identity-1234567890ab")
	relayNew := domaintest.ID("relay-new-identity-1234567890ab")

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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

// TestSendFileCommandUsesFreshRouteLookupWhenSnapshotIsStale pins the
// fix for the "route just arrived, send fails for ~1–1.5 s" regression
// (the cached routing snapshot's coalescing floor plus a refresh tick).
//
// Scenario: a route is accepted into the routing table at T=0. The
// hot-reads refresher's dirty-flag publish has not yet republished
// the cached snapshot, so RouteSnap still returns the empty pre-route
// view. The user clicks "send file" at T=10 ms. isPeerReachable
// (node-side) reads routing.Table.Lookup directly and reports the
// destination as reachable; without the fresh oracle, SendFileCommand
// would re-read the cached snapshot, find no candidates, and return
// "no route to <dst>" — even though the route is in fact in the
// table.
//
// The fix wires RouterConfig.RouteLookup to the same fresh
// per-destination Lookup the reachability gate uses. This test
// reproduces the disagreement: RouteSnap is empty, RouteLookup
// returns the route, and SendFileCommand must succeed via the relay.
//
// As a control, a sibling sub-test verifies that an unwired
// RouteLookup (RouterConfig field nil) preserves the pre-fix fallback
// to RouteSnap — a stale snapshot then surfaces the bug exactly as
// before. This is the failure mode any pre-fix or partially-wired
// caller would experience and it documents why the oracle is required
// at the wiring site.
func TestSendFileCommandUsesFreshRouteLookupWhenSnapshotIsStale(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("sender-node-identity-1234567890")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayID := domaintest.ID("relay-identity-1234567890abcdef")

	_, priv, _ := ed25519.GenerateKey(nil)

	now := time.Now()

	// Stale snapshot — does NOT contain a route to dstID. Models the
	// state right after the routing table accepted the route but
	// before the hot-reads refresher republished.
	staleSnap := routing.Snapshot{
		TakenAt: now.Add(-300 * time.Millisecond),
		Routes:  map[domain.PeerIdentity][]routing.RouteEntry{},
	}

	// Fresh per-destination view — contains the route as it lives in
	// the routing.Table at the moment of the SendFileCommand call.
	freshLookup := func(dst domain.PeerIdentity) []routing.RouteEntry {
		if dst != dstID {
			return nil
		}
		return []routing.RouteEntry{
			{
				Identity: dstID, Origin: dstID, NextHop: relayID,
				Hops: 2, ExpiresAt: now.Add(time.Minute),
				Source: routing.RouteSourceAnnouncement,
			},
		}
	}

	t.Run("fresh_lookup_wired_send_succeeds_via_relay", func(t *testing.T) {
		tr := &testFileRouter{
			sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
		}
		tr.router = NewRouter(RouterConfig{
			NonceCache:  newTestNonceCache(),
			LocalID:     localID,
			IsFullNode:  func() bool { return true },
			RouteSnap:   func() routing.Snapshot { return staleSnap },
			RouteLookup: freshLookup,
			PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
				if id != relayID {
					return PeerRouteMeta{}, false
				}
				return PeerRouteMeta{
					ConnectedAt:        now.Add(-time.Minute),
					ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
					RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
				}, true
			},
			IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool { return false },
			SessionSend: func(dst domain.PeerIdentity, data []byte) bool {
				tr.mu.Lock()
				defer tr.mu.Unlock()
				if dst == dstID {
					// No direct session — force the route-table fallback
					// path that this test is actually exercising.
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
			t.Fatalf("SendFileCommand should have succeeded via the fresh route, got %v", err)
		}

		if got := len(tr.sentTo(relayID)); got != 1 {
			t.Fatalf("expected exactly one frame to relay via the fresh route, got %d", got)
		}
	})

	t.Run("fresh_lookup_unset_falls_back_to_stale_snapshot_and_fails", func(t *testing.T) {
		// Control: leaving RouteLookup nil restores the pre-fix path.
		// SendFileCommand reads the empty cached snapshot, finds no
		// candidates, and returns "no route to <dst>". This is the
		// exact regression the fix closes; pinning it here documents
		// why every Router caller must wire RouteLookup.
		tr := &testFileRouter{
			sentFrames: make(map[domain.PeerIdentity][]json.RawMessage),
		}
		tr.router = NewRouter(RouterConfig{
			NonceCache: newTestNonceCache(),
			LocalID:    localID,
			IsFullNode: func() bool { return true },
			RouteSnap:  func() routing.Snapshot { return staleSnap },
			// RouteLookup intentionally nil.
			PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
				return PeerRouteMeta{
					ConnectedAt:        now.Add(-time.Minute),
					ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
					RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
				}, true
			},
			IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool { return false },
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
		if err == nil {
			t.Fatalf("expected SendFileCommand to fail with stale snapshot and no fresh oracle")
		}

		if got := len(tr.sentTo(relayID)); got != 0 {
			t.Fatalf("relay should not have received a frame when the cached snapshot is empty and no fresh oracle is wired, got %d", got)
		}
	})
}

// TestRouterExplainRouteUsesFreshRouteLookup pins that the diagnostic
// surface mirrors what SendFileCommand would actually do — both must
// read the same fresh per-destination oracle, otherwise an operator
// staring at ExplainRoute output would see a route the live send
// cannot use (or vice versa) during the cached snapshot's republish
// window. ExplainRoute is the "why did SendFileCommand pick this
// next-hop" tool, so the moment they read different sources, the tool
// stops being useful.
func TestRouterExplainRouteUsesFreshRouteLookup(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("sender-node-identity-1234567890")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayID := domaintest.ID("relay-identity-1234567890abcdef")

	now := time.Now()
	staleSnap := routing.Snapshot{
		TakenAt: now.Add(-300 * time.Millisecond),
		Routes:  map[domain.PeerIdentity][]routing.RouteEntry{},
	}

	router := NewRouter(RouterConfig{
		NonceCache: newTestNonceCache(),
		LocalID:    localID,
		IsFullNode: func() bool { return true },
		RouteSnap:  func() routing.Snapshot { return staleSnap },
		RouteLookup: func(dst domain.PeerIdentity) []routing.RouteEntry {
			if dst != dstID {
				return nil
			}
			return []routing.RouteEntry{
				{
					Identity: dstID, Origin: dstID, NextHop: relayID,
					Hops: 2, ExpiresAt: now.Add(time.Minute),
					Source: routing.RouteSourceAnnouncement,
				},
			}
		},
		PeerRouteMeta: func(id domain.PeerIdentity) (PeerRouteMeta, bool) {
			if id != relayID {
				return PeerRouteMeta{}, false
			}
			return PeerRouteMeta{
				ConnectedAt:        now.Add(-time.Minute),
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
			}, true
		},
		IsAuthorizedForLocalDelivery: func(id domain.PeerIdentity) bool { return false },
		SessionSend:                  func(dst domain.PeerIdentity, data []byte) bool { return false },
		LocalDeliver:                 func(frame protocol.FileCommandFrame) {},
	})

	plan := router.ExplainRoute(dstID)
	// peerRouteMeta(dstID) returns ok=false, so no synthetic direct
	// candidate is prepended; the plan should consist solely of the
	// relay candidate the fresh oracle reports.
	if len(plan) != 1 {
		t.Fatalf("expected ExplainRoute to surface the fresh route as the only plan entry, got %d entries: %#v", len(plan), plan)
	}
	if plan[0].NextHop != relayID {
		t.Fatalf("expected ExplainRoute to point at relay %q, got %q", relayID, plan[0].NextHop)
	}
}

func TestFileRouterDeduplicatesCandidatesByNextHop(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayA := domaintest.ID("relay-a-identity-1234567890ab")
	relayB := domaintest.ID("relay-b-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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
			a:         routeCandidate{nextHop: domaintest.ID("a"), hops: 5, protocolVersion: 7, connectedAt: base},
			b:         routeCandidate{nextHop: domaintest.ID("b"), hops: 1, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			wantALess: true,
		},
		{
			name:      "equal version: fewer hops wins",
			a:         routeCandidate{nextHop: domaintest.ID("a"), hops: 1, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: domaintest.ID("b"), hops: 2, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			wantALess: true,
		},
		{
			name:      "equal version and hops: longer uptime (older connectedAt) wins",
			a:         routeCandidate{nextHop: domaintest.ID("a"), hops: 2, protocolVersion: 6, connectedAt: base.Add(-time.Hour)},
			b:         routeCandidate{nextHop: domaintest.ID("b"), hops: 2, protocolVersion: 6, connectedAt: base},
			wantALess: true,
		},
		{
			name:      "equal everything: lexicographic nextHop wins",
			a:         routeCandidate{nextHop: domaintest.ID("a"), hops: 2, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: domaintest.ID("b"), hops: 2, protocolVersion: 6, connectedAt: base},
			wantALess: true,
		},
		{
			name:      "known connectedAt beats zero connectedAt under equal version and hops",
			a:         routeCandidate{nextHop: domaintest.ID("a"), hops: 2, protocolVersion: 6, connectedAt: base},
			b:         routeCandidate{nextHop: domaintest.ID("b"), hops: 2, protocolVersion: 6, connectedAt: time.Time{}},
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
// peer reporting the fixture-local version must NOT lose to an
// inflated-version peer (RawProtocolVersion > fixtureLocal,
// ProtocolVersion CAPPED at fixtureLocal by the node-side
// trustedFileRouteVersion helper) on the inflation lie alone. After
// the cap fix both peers tie on the primary key — neither wins via
// the lie — and the secondary keys (hops, uptime) decide. The test
// pins the legit peer's longer uptime as the tiebreaker so the
// assertion still reads as "legit wins over inflated"; the security
// guarantee is unchanged: an attacker cannot use a fake inflated
// version to capture file traffic.
//
// Earlier behaviour clamped ProtocolVersion to 0 instead of capping,
// which solved the inflation-attack ranking but ALSO sorted every
// legitimate upgraded peer to the bottom of the plan, breaking
// staged rollouts. The cap path is the production shape after that
// fix; see
// internal/core/node/file_integration_test.go's
// TestFileTransferPeerRouteMetaCapsInflatedVersionAtLocal for the
// node-layer regression that pins the cap behaviour where
// trustedFileRouteVersion lives — that test reads
// config.ProtocolVersion at runtime, the present test pins a
// fixture-local stand-in because the filerouter package has no
// access to that constant.
func TestFileRouterPrefersLegitOverInflatedVersion(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayClose := domaintest.ID("relay-close-identity-12345678901")
	relayFar := domaintest.ID("relay-far-identity-1234567890123")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

	now := time.Now()
	// Equal hops so the inflation lie cannot win via the hops
	// tie-break — uptime alone decides between two equal-version peers,
	// and the cap collapses both peers' primary keys to v=local.
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dstID: {
				{Identity: dstID, NextHop: relayClose, Hops: 1, ExpiresAt: now.Add(time.Minute)},
				{Identity: dstID, NextHop: relayFar, Hops: 1, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	keys := map[domain.PeerIdentity]ed25519.PublicKey{senderID: pub}
	// fixtureLocal is the local-version stand-in for this fixture
	// (filerouter has no access to config.ProtocolVersion, so we pin
	// a value here and treat it as the cap ceiling; production
	// config.ProtocolVersion may differ — what matters for the
	// comparator is only that both candidates' ProtocolVersion values
	// are equal so the primary key ties).
	const fixtureLocal = domain.ProtocolVersion(12)
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// Legit: ProtocolVersion mirrors RawProtocolVersion at fixtureLocal.
		relayClose: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal},
		// Inflated: peer claimed v=fixtureLocal+1, capped at ranking
		// fixtureLocal by node-side trustedFileRouteVersion. Tied with
		// relayClose on the primary key — uptime decides, and
		// relayClose has the longer one.
		relayFar: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal + 1},
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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if len(tr.sentTo(relayClose)) != 1 {
		t.Fatalf("expected file router to choose legit-uptime relayClose (PV=Raw=fixtureLocal) over inflated relayFar (PV=fixtureLocal capped, Raw=fixtureLocal+1) on uptime tie-break, got %d sends", len(tr.sentTo(relayClose)))
	}
	if len(tr.sentTo(relayFar)) != 0 {
		t.Fatalf("expected inflated relayFar to lose on uptime tie-break, not picked first; got %d sends", len(tr.sentTo(relayFar)))
	}
}

// TestFileRouterEqualVersionFallsBackToHopsThenUptime walks the secondary
// keys: equal version, then fewer hops, then longer uptime.
func TestFileRouterEqualVersionFallsBackToHopsThenUptime(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayClose := domaintest.ID("relay-close-identity-12345678901")
	relayFar := domaintest.ID("relay-far-identity-1234567890123")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	relayClose := domaintest.ID("relay-close-identity-12345678901")
	relayFar := domaintest.ID("relay-far-identity-1234567890123")

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

	// fixtureLocal is the ranking ceiling this test models. The
	// filerouter package has no access to config.ProtocolVersion (the
	// node layer is where the cap is applied); we hard-code a single
	// value here and treat it as "the local version" for the
	// purposes of this fixture. The actual config.ProtocolVersion in
	// the production build can be higher than 12 — what matters for
	// the comparator is only that both candidates' ProtocolVersion
	// values are equal so the primary key ties. RawProtocolVersion
	// is then free to differ on the inflated side.
	const fixtureLocal = domain.ProtocolVersion(12)
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// relayClose is a healthy peer at fixtureLocal
		// (Raw == PV == fixtureLocal). relayFar represents an
		// inflated peer: it claimed a higher version, and the
		// production node-side trustedFileRouteVersion CAPPED its
		// ranking value at fixtureLocal while keeping
		// RawProtocolVersion at the actually-reported value (here
		// fixtureLocal+1). Under the cap both peers tie on the
		// primary key; the secondary key (hops ASC) decides —
		// relayClose wins because it has fewer hops. ExplainRoute
		// mirrors that decision.
		relayClose: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal},
		relayFar:   {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal + 1},
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
		t.Fatalf("expected best entry to be relayClose (legit fixtureLocal version), got %s", plan[0].NextHop)
	}
	if plan[0].ProtocolVersion != fixtureLocal {
		t.Fatalf("expected best entry version=%d (fixtureLocal), got %d", fixtureLocal, plan[0].ProtocolVersion)
	}
	if plan[0].Hops != 1 {
		t.Fatalf("expected best entry hops=1, got %d", plan[0].Hops)
	}
	if plan[1].NextHop != relayFar {
		t.Fatalf("expected fall-back entry to be relayFar (inflated, ranking-capped), got %s", plan[1].NextHop)
	}
	if plan[1].ProtocolVersion != fixtureLocal {
		t.Fatalf("expected fall-back entry to have ProtocolVersion=%d (capped at fixtureLocal), got %d", fixtureLocal, plan[1].ProtocolVersion)
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
// Setup uses a fixture-local stand-in (`fixtureLocal`, declared inside
// the function body) for the cap ceiling, since the filerouter package
// has no access to config.ProtocolVersion. dst has a usable direct
// session at fixtureLocal (Raw == PV == fixtureLocal). A relay also has
// a route to dst, hop-distance 2, with the inflated-version shape
// produced by the production node-side helper after the cap fix —
// ProtocolVersion mirrors fixtureLocal (the cap ceiling for this
// fixture) while RawProtocolVersion stays at the actually-reported
// value (intentionally large to model an attacker / strongly inflated
// claim). Live SendFileCommand would hit the direct session first
// regardless of how the relay ranks; ExplainRoute must agree. See
// docs/protocol/file_transfer.md "Inflated-version defence" for the
// cap semantics.
func TestRouterExplainRoutePromotesDirectSession(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("local-node-identity-1234567890abc")
	dst := domaintest.ID("destination-identity-1234567890a")
	relay := domaintest.ID("relay-identity-1234567890aabbccdd")

	now := time.Now()
	snap := routing.Snapshot{
		TakenAt: now,
		Routes: map[domain.PeerIdentity][]routing.RouteEntry{
			dst: {
				{Identity: dst, NextHop: relay, Hops: 2, ExpiresAt: now.Add(time.Minute)},
			},
		},
	}

	// fixtureLocal is the local-version stand-in for this fixture;
	// the filerouter package has no access to config.ProtocolVersion,
	// so the test pins a single value and treats it as the cap
	// ceiling. The actual production config.ProtocolVersion may be
	// higher — the only invariant this test exercises is the
	// direct-session promotion contract, which is independent of the
	// absolute ranking value.
	const fixtureLocal = domain.ProtocolVersion(12)
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// dst has a direct session — that is the live send target.
		dst: {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal},
		// relay reports an inflated version (Raw=99 > fixtureLocal).
		// The production node-side helper caps the ranking key at the
		// local version while keeping RawProtocolVersion at 99, so
		// the candidate ranks tied with v=local peers on the primary
		// key. Here the test covers the direct-session promotion
		// contract; the direct entry wins independent of the relay's
		// ranking because step 1 of the send path (direct sessionSend
		// to dst) is unconditional.
		relay: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: fixtureLocal, RawProtocolVersion: 99},
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

	localID := domaintest.ID("local-node-identity-1234567890abc")
	dst := domaintest.ID("destination-identity-1234567890a")
	relay := domaintest.ID("relay-identity-1234567890aabbccdd")

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

	localID := domaintest.ID("local-node-identity-1234567890abc")
	dst := domaintest.ID("destination-identity-1234567890a")
	relay := domaintest.ID("relay-identity-1234567890aabbccdd")

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	unknown := domaintest.ID("unknown-destination-1234567890ab")

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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	dstID := domaintest.ID("destination-identity-1234567890a")
	via := domaintest.ID("incoming-neighbor-identity-12345")
	other := domaintest.ID("other-relay-identity-12345678901")

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

	// fixtureLocal is the local-version stand-in for this fixture
	// (filerouter has no access to config.ProtocolVersion, so we pin
	// a value here and treat it as the cap ceiling; production
	// config.ProtocolVersion may be higher).
	const fixtureLocal = domain.ProtocolVersion(12)
	meta := map[domain.PeerIdentity]PeerRouteMeta{
		// via reports an inflated version (Raw=99, ranking capped at
		// fixtureLocal) AND has fewer hops — both keys would normally
		// compete with split-horizon. The test pins that split-horizon
		// excludes via regardless of how the rest of routeCandidateLess
		// would score it; the production-realistic shape after the
		// inflation-version cap fix is PV=local with Raw>local — the
		// cap collapses the lie on the primary key without zeroing it.
		via:   {ConnectedAt: now.Add(-time.Hour), ProtocolVersion: fixtureLocal, RawProtocolVersion: 99},
		other: {ConnectedAt: now.Add(-time.Minute), ProtocolVersion: fixtureLocal, RawProtocolVersion: fixtureLocal},
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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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

	localID := domaintest.ID("relay-node-identity-1234567890ab")
	hop := domaintest.ID("nexthop-identity-1234567890abcde")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))
	dstID := domaintest.ID("destination-identity-1234567890a")

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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("local-node-identity-1234567890ab")

	_, priv, _ := ed25519.GenerateKey(nil)
	// Use a SRC that is NOT the fingerprint of this pubkey.
	wrongSRC := domaintest.ID("not-the-fingerprint-of-pub-1234")

	snap := routing.Snapshot{TakenAt: time.Now()}
	authorized := map[domain.PeerIdentity]bool{wrongSRC: true} // even if "trusted"
	tr := newTestFileRouterAuthorize(localID, true, snap, authorized, nil)

	frame := makeSignedFrame(wrongSRC, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("frame with mismatched SrcPubKey/SRC must be dropped, got %d deliveries", len(got))
	}
}

// TestRouter_DropsFrameWhenSignatureInvalid pins that the signature is
// verified against the embedded SrcPubKey. A tampered signature must
// be dropped even when fingerprint matches.
func TestRouter_DropsFrameWhenSignatureInvalid(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if got := tr.localDeliveries(); len(got) != 0 {
		t.Fatalf("frame with tampered signature must be dropped, got %d deliveries", len(got))
	}
}

// TestRouter_LocalDeliveryRejectsUntrustedSRC is the authorization
// guard: even with a perfectly authentic frame, an untrusted SRC must
// not deposit files into the local inbox.
func TestRouter_LocalDeliveryRejectsUntrustedSRC(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	// Authorization map empty — SRC is authenticated but NOT trusted.
	tr := newTestFileRouterAuthorize(localID, true, snap, nil, nil)

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

	localID := domaintest.ID("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

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
				ProtocolVersion:    domain.ProtocolVersion(testFixturePeerProtocolVersion),
				RawProtocolVersion: domain.ProtocolVersion(testFixturePeerProtocolVersion),
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
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

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

// TestRouter_LocalDeliveryAcceptsTrustedSRC is the happy-path: an
// authentic frame from a trusted SRC reaches the local inbox.
func TestRouter_LocalDeliveryAcceptsTrustedSRC(t *testing.T) {
	t.Parallel()

	localID := domaintest.ID("local-node-identity-1234567890ab")

	pub, priv, _ := ed25519.GenerateKey(nil)
	senderID := domain.PeerIdentityFromWire(identity.Fingerprint(pub))

	snap := routing.Snapshot{TakenAt: time.Now()}
	authorized := map[domain.PeerIdentity]bool{senderID: true}
	tr := newTestFileRouterAuthorize(localID, true, snap, authorized, nil)

	frame := makeSignedFrame(senderID, localID, 5, "payload", priv)
	raw, _ := protocol.MarshalFileCommandFrame(frame)
	tr.router.HandleInbound(json.RawMessage(raw), domain.PeerIdentity{})

	if got := tr.localDeliveries(); len(got) != 1 {
		t.Fatalf("authenticated trusted SRC must be delivered locally, got %d deliveries", len(got))
	}
}
