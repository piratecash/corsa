package node

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
)

// session_capability_scope_test.go pins ONE contract across every receive
// path that already holds the session a frame arrived on: the capability gate
// is answered by THAT session, never by whatever session the address map
// happens to hold at that instant.
//
// The two are not the same object during a reconnect. onCMSessionEstablished
// registers a replacement session under the same dial address while the
// previous session's goroutines are still unwinding (see the ownedCleanup
// comment in peer_sessions.go), so an address-keyed capability lookup taken
// from a frame that arrived on the OLD session answers about the NEW one.
// Both directions of that mistake are wrong and both are pinned here: a frame
// whose own session declared the capability must not be dropped, and a frame
// whose own session did NOT declare it must not be accepted.

// registerReplacementSession simulates the reconnect window: a NEW session for
// the SAME address, with a DIFFERENT negotiated capability set, takes over the
// map entry while the old session object is still live and still delivering.
func registerReplacementSession(
	t *testing.T,
	svc *Service,
	address domain.PeerAddress,
	identity domain.PeerIdentity,
	caps ...domain.Capability,
) *peerSession {
	t.Helper()
	replacement := &peerSession{
		address:      address,
		peerIdentity: identity,
		connID:       domain.ConnID(99001),
		capabilities: caps,
		sendCh:       make(chan peerSendItem, 4),
		inboxCh:      make(chan protocol.Frame, 8),
		errCh:        make(chan error, 4),
		authOK:       true,
	}
	svc.peerMu.Lock()
	svc.sessions[address] = replacement
	svc.peerMu.Unlock()
	return replacement
}

// ---------------------------------------------------------------------------
// Datagram receive path (dispatchSessionDatagramLine)
// ---------------------------------------------------------------------------

// TestDatagramJudgedByDeliveringSessionNotAddressHolder is the reconnect race
// in its acceptance direction: the frame arrived on a session that DID
// negotiate mesh_datagram_v1, and a replacement session that did not now owns
// the address. The frame must still reach the ingress — dropping it would
// discard a legitimate datagram because an unrelated socket handshook
// differently.
func TestDatagramJudgedByDeliveringSessionNotAddressHolder(t *testing.T) {
	svc, address, delivering := newDatagramOutboundFixture(t, domain.CapMeshDatagramV1)
	registerReplacementSession(t, svc, address, delivering.peerIdentity, domain.CapMeshRelayV1)

	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")
	svc.dispatchSessionDatagramLine(delivering, line)

	if got := datagramObservedCount(svc); got != 1 {
		t.Fatalf("the ingress observed %d frames, want 1: the gate must read the capabilities of the session that DELIVERED the datagram, not of the session that currently holds its address", got)
	}
}

// TestDatagramRefusedWhenDeliveringSessionLacksCapability is the same race in
// its admission direction, which is the one that matters for security: the
// frame arrived on a session that never negotiated mesh_datagram_v1, and a
// replacement session that did now owns the address. Accepting it would let a
// peer send frames behind a capability it never declared on that connection.
func TestDatagramRefusedWhenDeliveringSessionLacksCapability(t *testing.T) {
	svc, address, delivering := newDatagramOutboundFixture(t, domain.CapMeshRelayV1)
	registerReplacementSession(t, svc, address, delivering.peerIdentity, domain.CapMeshDatagramV1)

	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")
	svc.dispatchSessionDatagramLine(delivering, line)

	if got := datagramObservedCount(svc); got != 0 {
		t.Fatalf("the ingress observed %d frames, want 0: a datagram that arrived on a session without mesh_datagram_v1 must be dropped even when another session for the same address declared it", got)
	}
}

// TestSessionCapabilityGateRacesHandshakeWrite pins the ordering half of the
// contract, and only shows anything under -race: readPeerSession is started
// BEFORE applyWelcomeMetadata writes the negotiated set, so a peer that
// pipelines a datagram behind its own welcome has the reader gate on a field
// the handshake goroutine is writing at that instant. The gate must therefore
// read the set under the mutex the field is published with, not straight off
// the struct.
func TestSessionCapabilityGateRacesHandshakeWrite(t *testing.T) {
	svc, _, session := newDatagramOutboundFixture(t)
	line := strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n")

	welcome := protocol.Frame{
		Version:      config.ProtocolVersion,
		Address:      datagramTestDstHex,
		Capabilities: []string{string(domain.CapMeshDatagramV1)},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Mirrors the production handshake write exactly, lock included —
		// the test is worthless if it invents an ordering openPeerSession
		// does not have.
		svc.peerMu.Lock()
		applyWelcomeMetadata(session, welcome, false, datagramAdvertise{Endpoint: true})
		svc.peerMu.Unlock()
	}()
	go func() {
		defer wg.Done()
		svc.dispatchSessionDatagramLine(session, line)
	}()
	wg.Wait()
}

// ---------------------------------------------------------------------------
// file_command receive path (readPeerSession)
// ---------------------------------------------------------------------------

// recordingNonceCache is the first thing filerouter.Router.HandleInbound
// touches, which makes it the cheapest honest observable for "the frame got
// past the capability gate".
type recordingNonceCache struct {
	mu   sync.Mutex
	seen int
}

func (c *recordingNonceCache) Has(string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen++
	return true
}

func (c *recordingNonceCache) TryAdd(string) bool { return true }

func (c *recordingNonceCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen
}

// installRecordingFileRouter wires a router whose only job is to report that
// it was reached.
func installRecordingFileRouter(svc *Service) *recordingNonceCache {
	cache := &recordingNonceCache{}
	svc.fileMu.Lock()
	defer svc.fileMu.Unlock()
	svc.fileRouter = filerouter.NewRouter(filerouter.RouterConfig{
		NonceCache:                   cache,
		LocalID:                      domain.PeerIdentityFromWire(svc.identity.Address),
		IsFullNode:                   func() bool { return true },
		RouteSnap:                    func() routing.Snapshot { return routing.Snapshot{} },
		PeerRouteMeta:                func(domain.PeerIdentity) (filerouter.PeerRouteMeta, bool) { return filerouter.PeerRouteMeta{}, false },
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})
	return cache
}

func fileCommandWireLine(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(protocol.FileCommandFrame{
		Type:  "file_command",
		Nonce: "nonce-session-capability-scope",
		TTL:   4,
	})
	if err != nil {
		t.Fatalf("marshal file_command: %v", err)
	}
	return string(raw) + "\n"
}

// awaitFileRouterHits waits for the read loop to reach (or provably not reach)
// the file router. The loop is a goroutine, so a bare read would be a flake in
// whichever direction the scheduler chose.
func awaitFileRouterHits(cache *recordingNonceCache, want int) int {
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := cache.count()
		if got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestFileCommandJudgedByDeliveringSessionNotAddressHolder is the same
// reconnect race one branch above the datagram diversion in the SAME read
// loop: file_command is gated by an address-keyed lookup while the session it
// arrived on is a parameter of the enclosing function.
func TestFileCommandJudgedByDeliveringSessionNotAddressHolder(t *testing.T) {
	svc, peerEnd, session := newFileCommandReadFixture(t, domain.CapFileTransferV1)
	cache := installRecordingFileRouter(svc)
	registerReplacementSession(t, svc, session.address, session.peerIdentity, domain.CapMeshRelayV1)

	writeWireLine(t, peerEnd, fileCommandWireLine(t))

	if got := awaitFileRouterHits(cache, 1); got != 1 {
		t.Fatalf("file router reached %d times, want 1: the gate must read the capabilities of the session the file_command ARRIVED on", got)
	}
}

// TestFileCommandRefusedWhenDeliveringSessionLacksCapability is the admission
// direction of the same gate.
func TestFileCommandRefusedWhenDeliveringSessionLacksCapability(t *testing.T) {
	svc, peerEnd, session := newFileCommandReadFixture(t, domain.CapMeshRelayV1)
	cache := installRecordingFileRouter(svc)
	registerReplacementSession(t, svc, session.address, session.peerIdentity, domain.CapFileTransferV1)

	writeWireLine(t, peerEnd, fileCommandWireLine(t))
	// A frame that IS admitted reaches the router inside a few scheduler
	// ticks; the wait is what makes "never reached" a real observation
	// rather than a race the test happened to win.
	awaitFileRouterHits(cache, 1)

	if got := cache.count(); got != 0 {
		t.Fatalf("file router reached %d times, want 0: a file_command that arrived on a session without file_transfer_v1 must be dropped even when another session for the same address declared it", got)
	}
}

// newFileCommandReadFixture drives the PRODUCTION read loop over a pipe, with
// one outbound session whose negotiated set is exactly caps.
func newFileCommandReadFixture(t *testing.T, caps ...domain.Capability) (*Service, net.Conn, *peerSession) {
	t.Helper()
	svc, peerEnd, session := newReadPeerSessionFixture(t)
	svc.peerMu.Lock()
	session.capabilities = caps
	svc.peerMu.Unlock()
	return svc, peerEnd, session
}

// ---------------------------------------------------------------------------
// Outbound-session frame dispatch (dispatchPeerSessionFrame)
// ---------------------------------------------------------------------------

// TestAnnounceRoutesJudgedByDeliveringSession pins the same contract on the
// announce plane, where the defect has a second face: the switch arm not only
// gated on an address-keyed capability lookup but then re-resolved the session
// BY ADDRESS to read the peer identity the routes are attributed to. During a
// reconnect that identity belongs to the replacement session, so the routes of
// one peer were credited to another.
func TestAnnounceRoutesJudgedByDeliveringSession(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	svc.runCtx = t.Context()

	address := domain.PeerAddress("10.4.4.4:7777")
	delivering := &peerSession{
		address:      address,
		peerIdentity: idPeerB,
		connID:       domain.ConnID(4401),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
		sendCh:       make(chan peerSendItem, 4),
		inboxCh:      make(chan protocol.Frame, 8),
		errCh:        make(chan error, 4),
		authOK:       true,
	}
	svc.peerMu.Lock()
	svc.sessions[address] = delivering
	svc.health[address] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// The reconnect: a replacement session for the same address, a different
	// peer identity, and no routing capabilities at all.
	registerReplacementSession(t, svc, address, idPeerC)

	svc.dispatchPeerSessionFrame(address, delivering, protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: idTargetX.String(), Origin: idPeerB.String(), Hops: 1, SeqNo: 1},
		},
	})

	entries := svc.routingTable.Lookup(idTargetX)
	if len(entries) == 0 {
		t.Fatal("no route was accepted: the capability gate answered about the replacement session instead of the session the announce arrived on")
	}
	for _, entry := range entries {
		if entry.NextHop != idPeerB {
			t.Fatalf("route attributed to %s, want %s: the arm re-resolved the session by address and credited the replacement session's identity", entry.NextHop, idPeerB)
		}
	}
}
