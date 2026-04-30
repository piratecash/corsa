package node

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
	"github.com/piratecash/corsa/internal/core/service/filerouter"
)

// TestIsPeerReachable_DirectSessionWithCapability verifies that a peer with
// a direct outbound session carrying file_transfer_v1 is reported reachable.
func TestIsPeerReachable_DirectSessionWithCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		// v12 — at the file-transfer cutover. file_transfer_v1
		// capability alone is no longer sufficient (see
		// FileCommandMinPeerProtocolVersion); the negative coverage
		// for v11 / unknown-version sessions lives in
		// TestIsPeerReachable_DirectSessionBelowMinProtocolVersion
		// and TestIsPeerReachable_DirectSessionUnknownProtocolVersion.
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with direct v12 file_transfer_v1 session should be reachable")
	}
}

// TestIsPeerReachable_DirectSessionBelowMinProtocolVersion is the negative
// regression for the FileCommandMinPeerProtocolVersion cutover: a peer
// that has file_transfer_v1 capability but reports a known protocol
// version below the cutover MUST NOT be reachable. Otherwise we fall
// straight back into the v11 black hole — the peer would receive a
// v2-formatted frame and drop it on its trust-store-only pubkey lookup.
func TestIsPeerReachable_DirectSessionBelowMinProtocolVersion(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      domain.FileCommandMinPeerProtocolVersion - 1, // 11 — known pre-cutover
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with file_transfer_v1 but version below FileCommandMinPeerProtocolVersion must NOT be reachable")
	}
}

// TestIsPeerReachable_DirectSessionUnknownProtocolVersion is the second
// negative regression: a peer with file_transfer_v1 capability but with
// no observed protocol version (the default-zero value of peerSession.version)
// MUST NOT be reachable for file transfer. Capability alone is no longer
// proof that the peer speaks the v2 SrcPubKey wire format.
func TestIsPeerReachable_DirectSessionUnknownProtocolVersion(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		// version intentionally left zero — capability-only / pre-handshake
		// / not observed yet. Capability alone is insufficient evidence the
		// peer speaks the v2 wire format.
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with file_transfer_v1 but unknown protocol version must NOT be reachable")
	}
}

// TestIsPeerReachable_DirectSessionWithoutCapability verifies that a direct
// session lacking file_transfer_v1 is NOT considered reachable for file transfer.
func TestIsPeerReachable_DirectSessionWithoutCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with direct session but no file_transfer_v1 should NOT be reachable")
	}
}

// announceRouteVia is a test helper that injects a route to targetID via
// nextHopID into the service's routing table using handleAnnounceRoutes.
func announceRouteVia(svc *Service, nextHopID domain.PeerIdentity, targetID domain.PeerIdentity, hops int) {
	frame := protocol.Frame{
		Type: "announce_routes",
		AnnounceRoutes: []protocol.AnnounceRouteFrame{
			{Identity: string(targetID), Origin: string(nextHopID), Hops: hops, SeqNo: 1},
		},
	}
	svc.handleAnnounceRoutes(nextHopID, frame)
}

// TestIsPeerReachable_RouteViaFileCapableNextHop verifies that a routed peer
// is reachable when the next-hop has file_transfer_v1 capability.
func TestIsPeerReachable_RouteViaFileCapableNextHop(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Relay peer-B has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Target X is reachable via peer-B (2 hops after +1 at receiver).
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if !svc.isPeerReachable(idTargetX) {
		t.Fatal("target reachable via file-capable next-hop should be reachable")
	}
}

// TestIsPeerReachable_RouteViaNextHopWithoutFileCapability verifies that a
// routed peer is NOT reachable when the only next-hop lacks file_transfer_v1.
// This is the core regression test for the false-positive reachability bug.
func TestIsPeerReachable_RouteViaNextHopWithoutFileCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Relay peer-B has relay + routing but NOT file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapMeshRoutingV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Target X is reachable via peer-B.
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("target reachable only via non-file-capable next-hop should NOT be reachable")
	}
}

// TestIsPeerReachable_RouteViaBelowCutoverNextHop is the routed counterpart
// of TestIsPeerReachable_DirectSessionBelowMinProtocolVersion: a routed
// target whose only next-hop reports a protocol version below
// FileCommandMinPeerProtocolVersion MUST NOT be reachable. Otherwise
// isPeerReachable lies — it promises a path that the file router's own
// cutover gate (Router.collectRouteCandidates filtering on
// PeerRouteMeta.RawProtocolVersion) will reject, so FileTransferManager
// schedules a download that never actually leaves the node. The
// reachability layer and the send-path version gate must agree on
// eligibility, otherwise we're back to the original v11 black hole that
// motivated the cutover.
func TestIsPeerReachable_RouteViaBelowCutoverNextHop(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Relay peer-B advertises file_transfer_v1 capability but reports
	// version 11 — below the cutover. Capability alone is not proof that
	// the peer speaks the v2 SrcPubKey wire format; the version gate is
	// the actual contract.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      domain.FileCommandMinPeerProtocolVersion - 1,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Target X is reachable via peer-B at the announce layer, but the
	// next-hop is below the file-transfer cutover.
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("target via below-cutover next-hop must NOT be reachable; reachability would lie about a path the file router will reject")
	}
}

// TestIsPeerReachable_RouteViaUnknownVersionNextHop is the second routed
// negative regression: a next-hop with file_transfer_v1 capability but
// no observed protocol version (default-zero peerSession.version) MUST
// NOT make the routed target reachable. The pre-handshake / capability-
// only window is exactly the case the cutover refuses to admit on
// either tier — admitting it on the routed reachability path would
// reopen the same hole through the back door.
func TestIsPeerReachable_RouteViaUnknownVersionNextHop(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Relay peer-B advertises file_transfer_v1 capability with version
	// intentionally left at zero (capability-only / pre-handshake / not
	// observed yet). peerSession.version is the raw negotiated value,
	// not the capped-inflated ranking key, so 0 here always means
	// "unknown", never "demoted attacker".
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Target X is reachable via peer-B at the announce layer, but the
	// next-hop has no observed protocol version.
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("target via unknown-version next-hop must NOT be reachable; capability alone is not enough to clear the file-transfer cutover")
	}
}

// TestIsPeerReachable_MultipleRoutesOneCapable verifies that when multiple
// routes exist, the peer is reachable if at least one next-hop has file_transfer_v1.
func TestIsPeerReachable_MultipleRoutesOneCapable(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Peer-B: relay only, no file transfer.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Peer-C: has file_transfer_v1.
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		address:      domain.PeerAddress("addr-C"),
		peerIdentity: idPeerC,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-C")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Target X announced via both peers.
	announceRouteVia(svc, idPeerB, idTargetX, 1)
	announceRouteVia(svc, idPeerC, idTargetX, 2)

	if !svc.isPeerReachable(idTargetX) {
		t.Fatal("target should be reachable via the file-capable next-hop (peer-C)")
	}
}

// TestIsPeerReachable_NoPeerNoRoute verifies that a completely unknown peer
// is reported as unreachable.
func TestIsPeerReachable_NoPeerNoRoute(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRouting(t, idNodeA)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("unknown peer with no sessions or routes should NOT be reachable")
	}
}

// TestIsPeerReachable_SelfRouteSkipped verifies that routes where next_hop ==
// local identity are not considered when evaluating reachability, even when
// the local node has file_transfer_v1 capability.
func TestIsPeerReachable_SelfRouteSkipped(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// The local node has file_transfer_v1 — so it appears in fileCapable.
	svc.sessions[domain.PeerAddress("addr-A")] = &peerSession{
		address:      domain.PeerAddress("addr-A"),
		peerIdentity: domain.PeerIdentity(idNodeA),
		version:      12,
		capabilities: []domain.Capability{domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-A")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Inject a route to targetX with NextHop == localID directly via
	// UpdateRoute. handleAnnounceRoutes rejects own-origin routes, so
	// we use the routing table API to simulate a loop entry.
	_, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: idTargetX,
		Origin:   idPeerB,
		NextHop:  domain.PeerIdentity(idNodeA),
		Hops:     2,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	routes := svc.routingTable.Lookup(idTargetX)
	if len(routes) == 0 {
		t.Fatal("expected route to exist")
	}
	if routes[0].NextHop != domain.PeerIdentity(idNodeA) {
		t.Fatalf("expected self-route NextHop=%s, got %s", idNodeA, routes[0].NextHop)
	}

	// Even though local node is file-capable, sending to ourselves is a loop.
	if svc.isPeerReachable(idTargetX) {
		t.Fatal("self-route should not count as reachable")
	}
}

// TestIsPeerReachable_InboundConnectionWithCapability verifies that a peer
// connected via an inbound connection with file_transfer_v1 is reachable.
func TestIsPeerReachable_InboundConnectionWithCapability(t *testing.T) {
	t.Parallel()
	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	pipeLocal, pipeRemote := net.Pipe()
	defer func() { _ = pipeLocal.Close() }()
	defer func() { _ = pipeRemote.Close() }()

	pc := netcore.New(netcore.ConnID(1), pipeLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-B"),
		Identity:        domain.PeerIdentity(idPeerB),
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: 12,
	})
	svc.setTestConnEntryLocked(pipeLocal, &connEntry{core: pc})
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	if !svc.isPeerReachable(idPeerB) {
		t.Fatal("peer with inbound file_transfer_v1 connection should be reachable")
	}
}

func TestIsPeerReachable_DirectSessionStalled(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}

	if svc.isPeerReachable(idPeerB) {
		t.Fatal("stalled direct peer should NOT be reachable for file transfer")
	}
}

func TestIsPeerReachable_RouteViaStalledNextHop(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}
	announceRouteVia(svc, idPeerB, idTargetX, 1)

	if svc.isPeerReachable(idTargetX) {
		t.Fatal("route via stalled next-hop should NOT be considered reachable")
	}
}

// TestFileTransferPeerRouteMetaDescribesOneSession pins the new contract:
// fileTransferPeerRouteMeta must describe a single connection — both
// connectedAt and protocolVersion belong to the same session — instead
// of aggregating across all eligible sessions for the peer. The previous
// "max version, oldest connectedAt" aggregate let the file router rank
// peers by a version that the live send path would never actually use.
//
// Single-outbound case: the meta describes the only candidate and
// returns its own version + connectedAt.
func TestFileTransferPeerRouteMetaDescribesOneSession(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-out")] = &peerSession{
		address:      domain.PeerAddress("addr-out"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-10 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
	if !ok {
		t.Fatal("expected peer to be usable")
	}
	if !meta.ConnectedAt.Equal(now.Add(-10 * time.Minute)) {
		t.Fatalf("connectedAt = %v, want session's own LastConnectedAt", meta.ConnectedAt)
	}
	if meta.ProtocolVersion != domain.ProtocolVersion(12) {
		t.Fatalf("protocolVersion = %d, want 12 (the session's own negotiated version)", meta.ProtocolVersion)
	}
}

func TestFileTransferPeerRouteMetaRejectsStalledPeer(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-stalled")] = &peerSession{
		address:      domain.PeerAddress("addr-stalled"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-stalled")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-30 * time.Minute),
		LastUsefulReceiveAt: now.Add(-heartbeatInterval - pongStallTimeout - time.Second),
	}

	if meta, ok := svc.fileTransferPeerRouteMeta(idPeerB); ok {
		t.Fatalf("stalled peer must be unusable, got meta=%+v", meta)
	}
}

func TestFileTransferPeerRouteMetaRejectsPeerWithoutHealth(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	svc.sessions[domain.PeerAddress("addr-no-health")] = &peerSession{
		address:      domain.PeerAddress("addr-no-health"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	if meta, ok := svc.fileTransferPeerRouteMeta(idPeerB); ok {
		t.Fatalf("peer without health must be unusable, got meta=%+v", meta)
	}
}

// installTestFileRouter wires a minimal filerouter.Router into svc so the
// diagnostic surface (Service.ExplainFileRoute) can be exercised without
// pulling in the full initFileTransfer disk path. SessionSend / LocalDeliver
// are stubs because ExplainFileRoute never sends a frame — it only ranks
// candidates.
func installTestFileRouter(svc *Service) {
	svc.fileMu.Lock()
	defer svc.fileMu.Unlock()
	svc.fileRouter = filerouter.NewRouter(filerouter.RouterConfig{
		NonceCache: newDefaultNonceCache(),
		LocalID:    domain.PeerIdentity(svc.identity.Address),
		IsFullNode: func() bool { return true },
		RouteSnap: func() routing.Snapshot {
			if svc.routingTable == nil {
				return routing.Snapshot{}
			}
			return svc.routingTable.Snapshot()
		},
		PeerRouteMeta: func(id domain.PeerIdentity) (filerouter.PeerRouteMeta, bool) {
			return svc.fileTransferPeerRouteMeta(id)
		},
		IsAuthorizedForLocalDelivery: func(domain.PeerIdentity) bool { return false },
		SessionSend:                  func(domain.PeerIdentity, []byte) bool { return true },
		LocalDeliver:                 func(protocol.FileCommandFrame) {},
	})
}

// TestExplainFileRouteReturnsRankedJSON pins the wire format of the
// diagnostic command end-to-end: the ranked plan must come back as a
// JSON array, the first entry must be marked best, and each entry
// must carry next_hop, hops, protocol_version, connected_at and
// uptime_seconds. The behavioural ranking itself is covered by
// filerouter tests; this test owns the JSON contract.
func TestExplainFileRouteReturnsRankedJSON(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Two next-hops to the same target. Both peers run the cutover /
	// minimum file-command version (FileCommandMinPeerProtocolVersion
	// == 12). The previous "higher protocol version wins" fixture
	// using versions 6/7 became impossible after the file-transfer
	// cutover: versions below 12 are filtered out, and versions above
	// config.ProtocolVersion are capped at the local version by
	// trustedFileRouteVersion (defence against the traffic-capture
	// attack — the cap collapses the lie to the v=local tier instead
	// of zeroing it). We therefore exercise the secondary tiebreaker —
	// equal version, equal hops, longer uptime (older LastConnectedAt)
	// wins. relayB wins because it has held its session for ~30
	// minutes while relayC just connected a minute ago. The
	// behavioural ranking itself is covered exhaustively by filerouter
	// tests; this test owns the JSON wire contract.
	svc.sessions[domain.PeerAddress("addr-B")] = &peerSession{
		address:      domain.PeerAddress("addr-B"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.sessions[domain.PeerAddress("addr-C")] = &peerSession{
		address:      domain.PeerAddress("addr-C"),
		peerIdentity: idPeerC,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-B")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-30 * time.Minute),
		LastUsefulReceiveAt: now,
	}
	svc.health[domain.PeerAddress("addr-C")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// announceRouteVia uses the wire-side hop count; the receiver adds +1
	// when inserting into its own table (see docs/routing.md). Both
	// announces carry hops=1, so the local table has hops=2 via either
	// next-hop and the version DESC + hops ASC keys collapse to a tie —
	// uptime ASC (older LastConnectedAt above) decides the winner.
	announceRouteVia(svc, idPeerB, idTargetX, 1)
	announceRouteVia(svc, idPeerC, idTargetX, 1)
	const (
		hopsViaB = 2
		hopsViaC = 2
	)

	installTestFileRouter(svc)

	raw, err := svc.ExplainFileRoute(idTargetX)
	if err != nil {
		t.Fatalf("ExplainFileRoute returned error: %v", err)
	}

	var entries []map[string]interface{}
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("unmarshal plan: %v\npayload=%s", err, string(raw))
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in plan, got %d (payload=%s)", len(entries), string(raw))
	}

	first := entries[0]
	if first["next_hop"] != string(idPeerB) {
		t.Fatalf("expected best entry next_hop=%s, got %v", idPeerB, first["next_hop"])
	}
	if first["best"] != true {
		t.Fatalf("expected first entry to be marked best=true, got %v", first["best"])
	}
	if v, _ := first["protocol_version"].(float64); int(v) != 12 {
		t.Fatalf("expected best entry protocol_version=12, got %v", first["protocol_version"])
	}
	if v, _ := first["hops"].(float64); int(v) != hopsViaB {
		t.Fatalf("expected best entry hops=%d, got %v", hopsViaB, first["hops"])
	}
	if _, ok := first["connected_at"].(string); !ok {
		t.Fatalf("expected best entry to carry connected_at as RFC3339 string, got %v", first["connected_at"])
	}
	if uptime, _ := first["uptime_seconds"].(float64); uptime <= 0 {
		t.Fatalf("expected best entry uptime_seconds > 0, got %v", uptime)
	}

	second := entries[1]
	if second["next_hop"] != string(idPeerC) {
		t.Fatalf("expected fall-back entry next_hop=%s, got %v", idPeerC, second["next_hop"])
	}
	if second["best"] == true {
		t.Fatal("expected only the first entry to be marked best, got best=true on entry[1]")
	}
	if v, _ := second["hops"].(float64); int(v) != hopsViaC {
		t.Fatalf("expected fall-back entry hops=%d, got %v", hopsViaC, second["hops"])
	}
}

// TestExplainFileRouteEmptyArrayWhenNoRoute confirms that ExplainFileRoute
// returns the JSON array [] (not null, not an error) when no route exists.
// CLI / console renderers can then loop over the response without
// branching on a missing-key special case.
func TestExplainFileRouteEmptyArrayWhenNoRoute(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	installTestFileRouter(svc)

	raw, err := svc.ExplainFileRoute(idTargetX)
	if err != nil {
		t.Fatalf("ExplainFileRoute returned error: %v", err)
	}
	if string(raw) != "[]" {
		t.Fatalf("expected empty JSON array for unknown destination, got %s", string(raw))
	}
}

// TestFileTransferPeerRouteMetaCapsInflatedVersionAtLocal is the
// security regression for the inflated-version defence: a peer that
// claims a protocol version higher than this build's
// config.ProtocolVersion cannot actually be speaking the protocol we
// run, so its version is either a benign staged-rollout case (operator
// upgraded the peer ahead of this node) or a deliberate traffic-capture
// attack ("look at me, I'm newer, route everything through me").
//
// The meta layer demotes such a peer in the ranking by capping the
// reported version at the local protocol version. The cap collapses
// the inflated peer to the same primary-key tier as every legitimate
// v=local peer — neither wins the primary key on the lie. Earlier
// behaviour clamped the reported version to zero, which solved the
// inflation-attack ranking problem but ALSO pushed every legitimate
// upgraded peer (v=local+1) to the bottom of the plan, breaking
// staged rollouts. The cap fixes that without re-opening the attack:
// the attacker's claim of "newer" still cannot WIN against a legit
// v=local peer.
//
// Without this defence the file router would happily prefer a peer
// claiming version 99 over a real peer at the local version, and an
// attacker who can complete handshake could siphon all file traffic.
func TestFileTransferPeerRouteMetaCapsInflatedVersionAtLocal(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Outbound session reporting an impossibly-high version. The
	// constant config.ProtocolVersion is whatever this build ships;
	// adding a large fixed offset keeps the test future-proof against
	// version bumps without re-tuning the literal.
	localVersion := domain.ProtocolVersion(config.ProtocolVersion)
	inflated := localVersion + 100

	svc.sessions[domain.PeerAddress("addr-attacker")] = &peerSession{
		address:      domain.PeerAddress("addr-attacker"),
		peerIdentity: idPeerB,
		version:      int(inflated),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-attacker")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Hour),
		LastUsefulReceiveAt: now,
	}

	meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
	if !ok {
		t.Fatal("expected peer to remain usable — defence is a soft demote, not a hard reject")
	}
	if meta.ProtocolVersion != localVersion {
		t.Fatalf("expected inflated version (%d) to be capped at local (%d) for ranking, got %d", inflated, localVersion, meta.ProtocolVersion)
	}
	// RawProtocolVersion still mirrors the actually-reported value —
	// only the ranking projection is capped, the audit/eligibility
	// surface keeps the truth.
	if meta.RawProtocolVersion != inflated {
		t.Fatalf("RawProtocolVersion must report the actually-reported value, got %d (want %d)", meta.RawProtocolVersion, inflated)
	}
	// connected_at MUST still describe the chosen connection — the
	// defence only touches the ranking key, not the audit trail.
	if !meta.ConnectedAt.Equal(now.Add(-time.Hour)) {
		t.Fatalf("connected_at = %v, want it to come from the chosen session unchanged", meta.ConnectedAt)
	}
}

// TestExplainFileRouteCapsInflatedVersionAtLocal is the end-to-end
// counterpart: with one legit peer at the local version and one
// inflated-version peer (would-be attacker), the explain plan must
// list the legit peer first (best=true) and the inflated peer second.
// After the cap fix both entries report protocol_version equal to the
// local version (the inflated value is capped, not zeroed), so the
// primary key is tied between them and the secondary key (older
// connectedAt — longer uptime) decides. The legit peer is set up with
// the longer uptime so it wins on the tiebreak, exactly as the
// inflation defence intends: a peer claiming "newer" cannot win the
// ranking on the lie alone.
//
// Without ANY defence (cap or clamp) the inflated peer would win the
// primary key (version DESC) outright and become best, capturing all
// file routes. The earlier clamp-to-zero behaviour solved that but
// also broke staged rollouts; the cap-at-local behaviour solves both.
func TestExplainFileRouteCapsInflatedVersionAtLocal(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	legitVersion := domain.ProtocolVersion(config.ProtocolVersion)
	inflatedVersion := legitVersion + 100

	// Two distinct relays to the same target.
	svc.sessions[domain.PeerAddress("addr-legit")] = &peerSession{
		address:      domain.PeerAddress("addr-legit"),
		peerIdentity: idPeerB,
		version:      int(legitVersion),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.sessions[domain.PeerAddress("addr-attacker")] = &peerSession{
		address:      domain.PeerAddress("addr-attacker"),
		peerIdentity: idPeerC,
		version:      int(inflatedVersion),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	// Legit peer has the longer uptime — under the cap, the primary
	// key is tied at v=local for both peers and the secondary key
	// (connectedAt ASC) decides. The attacker cannot fake a longer
	// local-clock uptime, so this is the realistic shape of the
	// defence in production.
	svc.health[domain.PeerAddress("addr-legit")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-30 * time.Minute),
		LastUsefulReceiveAt: now,
	}
	svc.health[domain.PeerAddress("addr-attacker")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Both relays announce equal hops to the target so the primary
	// key tie under the cap is honest — only uptime decides.
	announceRouteVia(svc, idPeerB, idTargetX, 1)
	announceRouteVia(svc, idPeerC, idTargetX, 1)

	installTestFileRouter(svc)

	raw, err := svc.ExplainFileRoute(idTargetX)
	if err != nil {
		t.Fatalf("ExplainFileRoute: %v", err)
	}
	var entries []map[string]interface{}
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("unmarshal plan: %v\npayload=%s", err, string(raw))
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d (payload=%s)", len(entries), string(raw))
	}
	if entries[0]["next_hop"] != string(idPeerB) {
		t.Fatalf("expected legit peer at head of plan, got %v", entries[0]["next_hop"])
	}
	if entries[0]["best"] != true {
		t.Fatalf("expected legit peer to be best=true, got %v", entries[0]["best"])
	}
	if v, _ := entries[0]["protocol_version"].(float64); int(v) != int(legitVersion) {
		t.Fatalf("legit entry: expected protocol_version=%d, got %v", legitVersion, entries[0]["protocol_version"])
	}
	// Inflated peer is in the plan at fall-back position, with the
	// CAPPED ranking value (== legitVersion) — the wire surface
	// reports the cap, never the raw inflated value.
	if entries[1]["next_hop"] != string(idPeerC) {
		t.Fatalf("expected inflated peer at fall-back position, got %v", entries[1]["next_hop"])
	}
	if v, _ := entries[1]["protocol_version"].(float64); int(v) != int(legitVersion) {
		t.Fatalf("inflated entry: expected protocol_version=%d (capped at local), got %v", legitVersion, entries[1]["protocol_version"])
	}
}

// TestFileTransferPeerRouteMetaCapsNewerVersionAtLocal is the regression
// for the legitimate-newer-peer ranking inversion reported by the
// staged-rollout case (operator upgrades a single node ahead of the
// rest of the fleet). Before the fix, trustedFileRouteVersion clamped
// any peer reporting protocol_version > config.ProtocolVersion down to
// ranking value 0, which under protocolVersion DESC sorted the upgraded
// peer to the bottom of the candidate list — below every legacy v=local
// candidate. The user-visible symptom is "files never route through the
// upgraded node", because the file router prefers any v=local relay over
// the demoted v=local+1 one regardless of hops/uptime.
//
// The fix caps the ranking value at config.ProtocolVersion instead of
// zeroing. The inflation defence still holds — an attacker reporting
// v=local+100 cannot WIN the primary key over a legitimate v=local
// peer because the cap collapses both to the same ranking — but the
// upgraded peer keeps its place in the equal-version tier, where the
// secondary keys (hops ASC, connectedAt ASC) decide.
//
// RawProtocolVersion still mirrors the actually-reported value so the
// receive-side cutover gate and any future audit/diagnostic surfaces
// can distinguish "legitimately-newer peer" from "v=local peer".
func TestFileTransferPeerRouteMetaCapsNewerVersionAtLocal(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	localVersion := domain.ProtocolVersion(config.ProtocolVersion)
	upgradedVersion := localVersion + 1 // one release ahead — staged rollout

	svc.sessions[domain.PeerAddress("addr-upgraded")] = &peerSession{
		address:      domain.PeerAddress("addr-upgraded"),
		peerIdentity: idPeerB,
		version:      int(upgradedVersion),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-upgraded")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Hour),
		LastUsefulReceiveAt: now,
	}

	meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
	if !ok {
		t.Fatal("expected upgraded peer to remain usable — staged rollout must not break file transfer")
	}
	// Cap at local — NOT clamp to zero. Zero collapses the upgraded peer
	// to last-resort under protocolVersion DESC and re-creates the
	// production failure: master picks every v=local relay first, no
	// matter how many hops, and the upgraded next-hop is permanently
	// starved of file traffic.
	if meta.ProtocolVersion != localVersion {
		t.Fatalf("expected ProtocolVersion to be capped at local (%d), got %d", localVersion, meta.ProtocolVersion)
	}
	// RawProtocolVersion is the eligibility key and the audit trail —
	// it must report the actually-negotiated value, not the cap.
	if meta.RawProtocolVersion != upgradedVersion {
		t.Fatalf("RawProtocolVersion must report the actual reported value, got %d (want %d)", meta.RawProtocolVersion, upgradedVersion)
	}
}

// TestExplainFileRouteRanksUpgradedPeerByTiebreak is the end-to-end
// counterpart of the cap regression: when one next-hop is at the local
// version and another is one release ahead with FEWER hops, the
// upgraded peer must win the plan on the secondary key (hops ASC),
// because the cap brings both to the same primary key. Under the old
// clamp-to-zero behaviour the legacy v=local peer would have won
// regardless of hops — exactly the production failure mode the user
// reported as "newer protocol gets displaced".
func TestExplainFileRouteRanksUpgradedPeerByTiebreak(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	localVersion := domain.ProtocolVersion(config.ProtocolVersion)
	upgradedVersion := localVersion + 1

	// Legit peer: same version as local, but two hops to target.
	svc.sessions[domain.PeerAddress("addr-legit")] = &peerSession{
		address:      domain.PeerAddress("addr-legit"),
		peerIdentity: idPeerB,
		version:      int(localVersion),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	// Upgraded peer: one release ahead, one hop to target.
	svc.sessions[domain.PeerAddress("addr-upgraded")] = &peerSession{
		address:      domain.PeerAddress("addr-upgraded"),
		peerIdentity: idPeerC,
		version:      int(upgradedVersion),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-legit")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Hour),
		LastUsefulReceiveAt: now,
	}
	svc.health[domain.PeerAddress("addr-upgraded")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	announceRouteVia(svc, idPeerB, idTargetX, 2)
	announceRouteVia(svc, idPeerC, idTargetX, 1)

	installTestFileRouter(svc)

	raw, err := svc.ExplainFileRoute(idTargetX)
	if err != nil {
		t.Fatalf("ExplainFileRoute: %v", err)
	}
	var entries []map[string]interface{}
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("unmarshal: %v\npayload=%s", err, string(raw))
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d (payload=%s)", len(entries), string(raw))
	}
	// After the cap fix both peers tie at protocol_version = localVersion,
	// so hops ASC decides — upgraded peer (1 hop) wins over legit peer
	// (2 hops). Under the old clamp-to-0 behaviour the upgraded peer
	// would have ProtocolVersion=0 and would lose the primary key, so
	// the legit peer would incorrectly take the head of the plan.
	if entries[0]["next_hop"] != string(idPeerC) {
		t.Fatalf("expected upgraded peer (%s, 1 hop) to win the plan, got %v at head", idPeerC, entries[0]["next_hop"])
	}
	if entries[0]["best"] != true {
		t.Fatalf("expected upgraded peer to be best=true, got %v", entries[0]["best"])
	}
	// The protocol_version reported in the plan reflects the cap, not
	// the raw negotiated value — that is the contract of the ranking
	// surface (RawProtocolVersion is internal-only).
	if v, _ := entries[0]["protocol_version"].(float64); int(v) != int(localVersion) {
		t.Fatalf("upgraded entry: expected protocol_version=%d (capped), got %v", localVersion, entries[0]["protocol_version"])
	}
	if entries[1]["next_hop"] != string(idPeerB) {
		t.Fatalf("expected legit v=local peer at fall-back position, got %v", entries[1]["next_hop"])
	}
}

// TestFileTransferPeerRouteMetaInboundConnContributesNegotiatedVersion
// pins the regression that motivated wiring the negotiated version
// onto NetCore for the inbound direction. Before this fix, an inbound
// carve-out conn (in s.conns but not in s.sessions) collapsed to
// protocol_version 0 in the route-meta snapshot, so any peer with
// even a single outbound link automatically won the new
// protocolVersion DESC primary key. With the fix, an inbound conn
// whose hello.Version was folded onto the NetCore (via
// rememberConnPeerAddr's ApplyOpts) reports its real negotiated
// version to the file router.
//
// The meta now describes the *single* connection the live send path
// would try first (head of peerSendableConnectionsLocked), so this
// test asserts the protocol_version of THAT connection — not an
// aggregate / max across multiple sessions; the older "highestVersion"
// contract was removed when the meta switched to a single-connection
// snapshot.
func TestFileTransferPeerRouteMetaInboundConnContributesNegotiatedVersion(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	const inboundVersion = domain.ProtocolVersion(12)
	pc := netcore.New(netcore.ConnID(42), inLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-inbound"),
		Identity:        idPeerB,
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: inboundVersion,
	})
	defer pc.Close()
	svc.setTestConnEntryLocked(inLocal, &connEntry{core: pc, tracked: true})
	svc.health[domain.PeerAddress("addr-inbound")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-5 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
	if !ok {
		t.Fatal("expected inbound peer to be reported as usable")
	}
	if meta.ProtocolVersion != inboundVersion {
		t.Fatalf("expected ProtocolVersion=%d (from inbound NetCore), got %d", inboundVersion, meta.ProtocolVersion)
	}
}

// TestFileTransferPeerRouteMetaDeterministicAcrossMapIteration pins
// the within-tier ordering: when a peer has two healthy outbound
// sessions, the meta and the live send path must pick the SAME first
// candidate every time, regardless of Go map iteration randomisation.
//
// The previous implementation read sessions straight out of s.sessions
// (a map) and could surface a different first candidate on every call,
// so two sequential queries — one from the file router's diagnostic,
// one from sendFrameToIdentity — could disagree about which session
// "owns" the next-hop slot. The fix sorts both tiers by oldest
// connectedAt with an immutable tiebreak; we exercise that contract
// across many calls so a regression that re-introduced map order would
// be flagged probabilistically.
func TestFileTransferPeerRouteMetaDeterministicAcrossMapIteration(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Two outbound sessions to the same peer with different connectedAt.
	// addr-old has the longer uptime and must win the head slot in
	// every call, regardless of map iteration order.
	svc.sessions[domain.PeerAddress("addr-old")] = &peerSession{
		address:      domain.PeerAddress("addr-old"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.sessions[domain.PeerAddress("addr-new")] = &peerSession{
		address:      domain.PeerAddress("addr-new"),
		peerIdentity: idPeerB,
		version:      13,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-old")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-30 * time.Minute),
		LastUsefulReceiveAt: now,
	}
	svc.health[domain.PeerAddress("addr-new")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	// 64 iterations is comfortably above the typical Go map-iteration
	// shuffle period; if ordering depended on map order we would see at
	// least one mismatched call within this many tries.
	const iterations = 64
	for i := 0; i < iterations; i++ {
		meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
		if !ok {
			t.Fatalf("iter %d: expected peer to be usable", i)
		}
		if !meta.ConnectedAt.Equal(now.Add(-30 * time.Minute)) {
			t.Fatalf("iter %d: expected ConnectedAt=oldest, got %v", i, meta.ConnectedAt)
		}
		// version 12 belongs to addr-old (the oldest), so meta MUST
		// report it — not version 13 from the newer session.
		if meta.ProtocolVersion != domain.ProtocolVersion(12) {
			t.Fatalf("iter %d: expected ProtocolVersion=12 (addr-old's own), got %d", i, meta.ProtocolVersion)
		}
	}
}

// TestPeerSendableConnectionsDeterministicAcrossMapIteration covers the
// same invariant directly on the shared helper, including the inbound
// tiebreak path (ConnID) that the meta-only test above does not reach.
// Two outbound (different addresses, same connectedAt → tiebreak by
// address) and two inbound (different ConnIDs, same connectedAt →
// tiebreak by ConnID) — order must be stable across repeated calls.
func TestPeerSendableConnectionsDeterministicAcrossMapIteration(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	svc.sessions[domain.PeerAddress("addr-bb")] = &peerSession{
		address:      domain.PeerAddress("addr-bb"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapFileTransferV1},
	}
	svc.sessions[domain.PeerAddress("addr-aa")] = &peerSession{
		address:      domain.PeerAddress("addr-aa"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapFileTransferV1},
	}
	commonHealth := func() *peerHealth {
		return &peerHealth{
			Connected:           true,
			LastConnectedAt:     now.Add(-5 * time.Minute),
			LastUsefulReceiveAt: now,
		}
	}
	svc.health[domain.PeerAddress("addr-aa")] = commonHealth()
	svc.health[domain.PeerAddress("addr-bb")] = commonHealth()

	// Two inbound conns with identical connectedAt; tiebreak is by
	// ConnID (lower first). ConnID 13 must consistently win over 99.
	makeInbound := func(id netcore.ConnID, addr string) {
		local, remote := net.Pipe()
		t.Cleanup(func() { _ = local.Close() })
		t.Cleanup(func() { _ = remote.Close() })
		pc := netcore.New(id, local, netcore.Inbound, netcore.Options{
			Address:         domain.PeerAddress(addr),
			Identity:        idPeerB,
			Caps:            []domain.Capability{domain.CapFileTransferV1},
			ProtocolVersion: 12,
		})
		t.Cleanup(func() { pc.Close() })
		svc.setTestConnEntryLocked(local, &connEntry{core: pc, tracked: true})
		svc.health[domain.PeerAddress(addr)] = commonHealth()
	}
	makeInbound(99, "addr-in-99")
	makeInbound(13, "addr-in-13")

	const iterations = 64
	for i := 0; i < iterations; i++ {
		svc.peerMu.RLock()
		got := svc.peerSendableConnectionsLocked(idPeerB, domain.CapFileTransferV1, now)
		svc.peerMu.RUnlock()

		if len(got) != 4 {
			t.Fatalf("iter %d: expected 4 candidates (2 outbound + 2 inbound), got %d", i, len(got))
		}
		// Outbound block: addr-aa < addr-bb lexicographically.
		if got[0].outbound == nil || got[0].outbound.address != "addr-aa" {
			t.Fatalf("iter %d: candidates[0] expected outbound addr-aa, got %+v", i, got[0])
		}
		if got[1].outbound == nil || got[1].outbound.address != "addr-bb" {
			t.Fatalf("iter %d: candidates[1] expected outbound addr-bb, got %+v", i, got[1])
		}
		// Inbound block: ConnID 13 < 99.
		if got[2].outbound != nil || got[2].inboundID != 13 {
			t.Fatalf("iter %d: candidates[2] expected inbound ConnID=13, got %+v", i, got[2])
		}
		if got[3].outbound != nil || got[3].inboundID != 99 {
			t.Fatalf("iter %d: candidates[3] expected inbound ConnID=99, got %+v", i, got[3])
		}
	}
}

// TestFileTransferPeerRouteMetaPrefersOutboundOverInbound is the
// regression test for the meta-vs-send-path drift bug: when peer X has
// BOTH an outbound session (lower version) and an inbound conn (higher
// version), the meta MUST report the outbound's version because that
// is the connection sendFrameToIdentity will actually try first.
//
// Before the fix, fileTransferPeerRouteMetaLocked aggregated max
// version across all eligible sessions, so it would say "v9" while the
// real send path would route bytes through the outbound v5 session.
// The diagnostic and the file router's protocol_version DESC ranking
// would then prefer this peer based on a version no packet would
// actually traverse.
func TestFileTransferPeerRouteMetaPrefersOutboundOverInbound(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	// Outbound session with version 12 — this is the connection
	// sendFrameToIdentity tries first.
	svc.sessions[domain.PeerAddress("addr-out")] = &peerSession{
		address:      domain.PeerAddress("addr-out"),
		peerIdentity: idPeerB,
		version:      12,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	// Inbound conn for the same identity with version 13 — only the
	// fall-back tier; meta must NOT pretend bytes will go through it.
	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(7), inLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-inbound-mix"),
		Identity:        idPeerB,
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: 13,
	})
	defer pc.Close()
	svc.setTestConnEntryLocked(inLocal, &connEntry{core: pc, tracked: true})
	svc.health[domain.PeerAddress("addr-inbound-mix")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	meta, ok := svc.fileTransferPeerRouteMeta(idPeerB)
	if !ok {
		t.Fatal("expected peer to be usable")
	}
	if meta.ProtocolVersion != 12 {
		t.Fatalf("expected outbound version=12 (live send tries outbound first), got %d", meta.ProtocolVersion)
	}
	// connected_at must come from the SAME session as protocol_version,
	// not stitched together from a different inbound conn.
	if !meta.ConnectedAt.Equal(now.Add(-2 * time.Minute)) {
		t.Fatalf("connected_at = %v, want %v (the outbound session's own LastConnectedAt)", meta.ConnectedAt, now.Add(-2*time.Minute))
	}
}

// TestExplainFileRouteUninitializedSubsystem confirms that ExplainFileRoute
// returns errFileTransferNotInitialized when the file subsystem has not
// been wired yet — same contract as the rest of the file-domain API.
func TestExplainFileRouteUninitializedSubsystem(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	if _, err := svc.ExplainFileRoute(idTargetX); err != errFileTransferNotInitialized {
		t.Fatalf("expected errFileTransferNotInitialized, got %v", err)
	}
}

func TestSendFrameToIdentityFallsBackToInboundWhenOutboundBufferFull(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	outboundSendCh := make(chan protocol.Frame, 1)
	outboundSendCh <- protocol.Frame{Type: "pre-filled"}
	svc.sessions[domain.PeerAddress("addr-out")] = &peerSession{
		address:      domain.PeerAddress("addr-out"),
		peerIdentity: idPeerB,
		version:      12,
		conn:         outLocal,
		sendCh:       outboundSendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(1), inLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-in"),
		Identity:        idPeerB,
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: 12,
	})
	defer pc.Close()
	svc.setTestConnEntryLocked(inLocal, &connEntry{core: pc})
	svc.health[domain.PeerAddress("addr-in")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	frame := protocol.Frame{Type: "ping"}
	if !svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("expected inbound fallback to succeed when outbound sendCh is full")
	}

	reader := bufio.NewReader(inRemote)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	parsed, err := protocol.ParseFrameLine(line[:len(line)-1])
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if parsed.Type != "ping" {
		t.Fatalf("forwarded frame type = %q, want ping", parsed.Type)
	}
}

func TestSendFrameToIdentityFallsBackToInboundWhenOutboundSendChannelClosed(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)
	now := time.Now().UTC()

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	outboundSendCh := make(chan protocol.Frame, 1)
	close(outboundSendCh)
	svc.sessions[domain.PeerAddress("addr-out-closed")] = &peerSession{
		address:      domain.PeerAddress("addr-out-closed"),
		peerIdentity: idPeerB,
		version:      12,
		conn:         outLocal,
		sendCh:       outboundSendCh,
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}
	svc.health[domain.PeerAddress("addr-out-closed")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-2 * time.Minute),
		LastUsefulReceiveAt: now,
	}

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(2), inLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-in-fallback"),
		Identity:        idPeerB,
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: 12,
	})
	defer pc.Close()
	svc.setTestConnEntryLocked(inLocal, &connEntry{core: pc})
	svc.health[domain.PeerAddress("addr-in-fallback")] = &peerHealth{
		Connected:           true,
		LastConnectedAt:     now.Add(-time.Minute),
		LastUsefulReceiveAt: now,
	}

	frame := protocol.Frame{Type: "ping"}
	if !svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("expected inbound fallback to succeed when outbound sendCh is closed")
	}

	reader := bufio.NewReader(inRemote)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	parsed, err := protocol.ParseFrameLine(line[:len(line)-1])
	if err != nil {
		t.Fatalf("ParseFrameLine: %v", err)
	}
	if parsed.Type != "ping" {
		t.Fatalf("forwarded frame type = %q, want ping", parsed.Type)
	}
}

// TestSendFrameToIdentityRejectsOutboundBeforeActivation pins the activation
// boundary: outbound bring-up inserts into s.sessions before markPeerConnected
// seeds s.health, and during that window identity-routed frames must not be
// accepted. Regression for the pre-activation visibility leak in
// sendFrameToIdentity step 1 (outbound preferred path).
func TestSendFrameToIdentityRejectsOutboundBeforeActivation(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	outLocal, outRemote := net.Pipe()
	defer func() { _ = outLocal.Close() }()
	defer func() { _ = outRemote.Close() }()

	// Session is installed into s.sessions but markPeerConnected has NOT
	// been called yet — health is absent. This mirrors the window between
	// peer_management.go s.sessions[address] = session and the subsequent
	// markPeerConnected(...) call.
	svc.sessions[domain.PeerAddress("addr-out-preactivation")] = &peerSession{
		address:      domain.PeerAddress("addr-out-preactivation"),
		peerIdentity: idPeerB,
		version:      12,
		conn:         outLocal,
		sendCh:       make(chan protocol.Frame, 4),
		capabilities: []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
	}

	frame := protocol.Frame{Type: "ping"}
	if svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("sendFrameToIdentity accepted outbound session before markPeerConnected; pre-activation visibility leak")
	}
}

// TestSendFrameToIdentityRejectsInboundBeforeActivation pins the same
// activation boundary for the inbound fallback branch. registerInboundConn
// installs the NetCore before hello/auth; identity/capabilities populate
// before markPeerConnected. Identity-routed frames must not land on a
// partially-handshaken inbound NetCore.
func TestSendFrameToIdentityRejectsInboundBeforeActivation(t *testing.T) {
	t.Parallel()

	svc := newTestServiceWithRoutingAndHealth(t, idNodeA)

	inLocal, inRemote := net.Pipe()
	defer func() { _ = inLocal.Close() }()
	defer func() { _ = inRemote.Close() }()

	pc := netcore.New(netcore.ConnID(7), inLocal, netcore.Inbound, netcore.Options{
		Address:         domain.PeerAddress("addr-in-preactivation"),
		Identity:        idPeerB,
		Caps:            []domain.Capability{domain.CapMeshRelayV1, domain.CapFileTransferV1},
		ProtocolVersion: 12,
	})
	defer pc.Close()
	svc.setTestConnEntryLocked(inLocal, &connEntry{core: pc})
	// Intentionally no svc.health entry for addr-in-preactivation.

	frame := protocol.Frame{Type: "ping"}
	if svc.sendFrameToIdentity(idPeerB, frame, domain.CapFileTransferV1) {
		t.Fatal("sendFrameToIdentity accepted inbound NetCore before markPeerConnected; pre-activation visibility leak")
	}
}
