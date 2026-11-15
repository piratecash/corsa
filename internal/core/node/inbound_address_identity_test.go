package node

import (
	"net"
	"slices"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestInboundAdvertisedPortCannotReplaceProvenAddressIdentity(t *testing.T) {
	svc := newDialChoiceService(t)
	original := addressIdentity{identity: domain.PeerIdentity{1}, origin: domain.PeerIdentityProvenV2}
	other := addressIdentity{identity: domain.PeerIdentity{2}, origin: domain.PeerIdentityProvenV2}
	svc.addPeerID(dialChoiceAddress, original)
	svc.recordInboundPeer(dialChoiceAddress, protocol.Frame{
		Address: original.identity.String(), ClientVersion: "original", ClientBuild: 71, NodeType: "full",
	}, original.origin)
	svc.recordInboundPeer(dialChoiceAddress, protocol.Frame{
		Address: other.identity.String(), ClientVersion: "other", ClientBuild: 999, NodeType: "client",
	}, other.origin)
	if version, build, peerType := svc.peerVersions[dialChoiceAddress], svc.peerBuilds[dialChoiceAddress], svc.peerTypeForAddress(dialChoiceAddress); version != "original" || build != 71 || peerType != domain.NodeTypeFull {
		t.Fatalf("inbound claim replaced metadata: version=%q build=%d type=%v", version, build, peerType)
	}
	if got := svc.peerIDs[dialChoiceAddress]; got != original {
		t.Fatalf("inbound claim replaced proven address identity: %+v", got)
	}
	svc.addPeerID(dialChoiceAddress, other)
	if got := svc.peerIDs[dialChoiceAddress]; got != other {
		t.Fatalf("outbound proof did not update address identity: %+v", got)
	}
}

func TestInboundProvenSessionKeepsExistingEndpointIdentity(t *testing.T) {
	local := newTestService(t, config.NodeTypeFull)
	original := newTestService(t, config.NodeTypeFull)
	intruder := newTestService(t, config.NodeTypeFull)
	for _, svc := range []*Service{local, original, intruder} {
		svc.routeWithdrawalGracePeriodTest = -1
	}

	// First prove the original peer at the exact socket endpoint we dialled.
	server, client := tcpPair(t)
	firstOwner := newF1FixtureOwner()
	firstOwner.rawServer, firstOwner.rawClient = server, client
	t.Cleanup(func() { firstOwner.stopAndJoin(t) })
	first := f1PairOnRaw(t, false, nil, original, local, firstOwner, server, client)
	peer, err := first.dialedProof.Peer()
	if err != nil {
		t.Fatal(err)
	}
	proof, valid := peer.Proof()
	if proven, ok := proof.Identity(); !valid || !ok || proven.String() != original.Address() {
		t.Fatal("original outbound handshake did not prove its peer")
	}
	endpoint := first.outbound.address
	// Apply the original welcome through the normal outbound metadata path.
	local.learnIdentityFromWelcome(peer.Intro, endpoint, identityOriginOf(valid))
	local.peerMu.RLock()
	before := local.peerIDs[endpoint]
	version, build, peerType := local.peerVersions[endpoint], local.peerBuilds[endpoint], local.peerTypes[endpoint]
	local.peerMu.RUnlock()
	if before.identity != peer.Identity || before.origin != domain.PeerIdentityProvenV2 {
		t.Fatal("outbound proof did not establish the endpoint identity", before)
	}

	local.peerMu.Lock()
	health := local.ensurePeerHealthLocked(endpoint)
	health.Score = -23
	local.peerMu.Unlock()

	// Another identity on the same loopback host advertises that endpoint's port.
	portNumber := server.LocalAddr().(*net.TCPAddr).Port
	port := domain.PeerPort(portNumber)
	intruder.cfg.AdvertisePort = &port
	otherServer, otherClient := tcpPair(t)
	secondOwner := newF1FixtureOwner()
	secondOwner.rawServer, secondOwner.rawClient = otherServer, otherClient
	t.Cleanup(func() { secondOwner.stopAndJoin(t) })
	second := f1PairOnRaw(t, false, nil, local, intruder, secondOwner, otherServer, otherClient)
	auth := k3Auth(t, local, second.inbound.id)
	if claim := synthesiseAdvertisedFromObserved(otherServer.RemoteAddr().String(), auth.Hello.AdvertisePort); domain.PeerAddress(claim) != endpoint || !listenerEnabledFromFrame(auth.Hello) {
		t.Fatal("fixture did not advertise the conflicting endpoint", claim)
	}
	inboundProof, proven := auth.ProvenIdentity()
	if id, ok := inboundProof.Identity(); !proven || !ok || id.String() != intruder.Address() {
		t.Fatal("accepted session lost its actual peer proof")
	}
	local.peerMu.RLock()
	got := local.peerIDs[endpoint]
	gotScore, connected := local.health[endpoint].Score, local.health[endpoint].Connected
	gotVersion, gotBuild, gotType := local.peerVersions[endpoint], local.peerBuilds[endpoint], local.peerTypes[endpoint]
	local.peerMu.RUnlock()
	if gotScore != -23 || connected {
		t.Fatalf("inbound proof charged the previous endpoint owner: score=%d connected=%v", gotScore, connected)
	}
	if got != before || gotVersion != version || gotBuild != build || gotType != peerType {
		t.Fatalf("inbound proof replaced outbound endpoint metadata: identity=%+v version=%q build=%d type=%v", got, gotVersion, gotBuild, gotType)
	}
	tracked := local.trackedInboundPeerAddress(second.inbound.id)
	if tracked == "" || tracked == endpoint {
		t.Fatal("conflicting connection lacks its own health key", tracked)
	}
	if caps, found := local.peerRoutingCapabilities(tracked); !found || !slices.Equal(caps, local.connCapabilitiesForID(second.inbound.id)) {
		t.Fatalf("conflicting inbound connection lost routing capabilities: found=%v caps=%v", found, caps)
	}
	local.trackInboundDisconnectWithPresenceEvidence(second.inbound.id, tracked, nil)
	local.peerMu.RLock()
	unchanged := local.health[endpoint].Score == -23 && !local.health[endpoint].Connected
	remaining := local.inboundHealthRefs[tracked]
	local.peerMu.RUnlock()
	if !unchanged || remaining != 0 {
		t.Fatalf("disconnect affected the original endpoint or leaked a ref: unchanged=%v remaining=%d", unchanged, remaining)
	}
	// These connection-local rows must follow the existing orphan grace and
	// eviction, rather than accumulating once for every conflicting session.
	local.peerMu.Lock()
	local.health[tracked].LastDisconnectedAt = time.Now().Add(-2 * orphanedHealthEvictWindow)
	local.peerMu.Unlock()
	local.evictOrphanedHealthEntries()
	local.peerMu.RLock()
	_, healthRemains := local.health[tracked]
	_, identityRemains := local.peerIDs[tracked]
	_, versionRemains := local.peerVersions[tracked]
	_, buildRemains := local.peerBuilds[tracked]
	_, typeRemains := local.peerTypes[tracked]
	local.peerMu.RUnlock()
	if healthRemains || identityRemains || versionRemains || buildRemains || typeRemains {
		t.Fatalf("orphaned conflict metadata survived eviction: health=%v identity=%v version=%v build=%v type=%v", healthRemains, identityRemains, versionRemains, buildRemains, typeRemains)
	}
}

func TestInboundClaimCannotChangeProtectedEndpointMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		origin   domain.PeerIdentityOrigin
		pinned   bool
		accepted bool
	}{
		{"proven-before-claim", domain.PeerIdentityProvenV2, false, false},
		{"proven-and-pinned", domain.PeerIdentityProvenV2, true, false},
		{"pin-after-legacy-gate", domain.PeerIdentityClaimed, true, false},
		{"ordinary-legacy-refresh", domain.PeerIdentityClaimed, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newDialChoiceService(t)
			id := domain.PeerIdentity{1}
			svc.recordInboundPeer(dialChoiceAddress, protocol.Frame{
				Address: id.String(), ClientVersion: "original", ClientBuild: 71, NodeType: "full",
			}, tc.origin)
			if tc.pinned {
				if err := svc.secureSessions.store.noteProvenInbound(id); err != nil {
					t.Fatal(err)
				}
			}
			got := svc.recordInboundPeer(dialChoiceAddress, protocol.Frame{
				Address: id.String(), ClientVersion: "late-claim", ClientBuild: 999, NodeType: "client",
			}, domain.PeerIdentityClaimed)
			if got != tc.accepted {
				t.Errorf("claimed update accepted=%v, want %v", got, tc.accepted)
			}
			version, build, kind := svc.peerVersions[dialChoiceAddress], svc.peerBuilds[dialChoiceAddress], svc.peerTypes[dialChoiceAddress]
			if tc.accepted {
				if version != "late-claim" || build != 999 || kind != domain.NodeTypeClient {
					t.Fatalf("ordinary legacy refresh lost: %q %d %v", version, build, kind)
				}
			} else if version != "original" || build != 71 || kind != domain.NodeTypeFull {
				t.Fatalf("claim changed protected metadata: %q %d %v", version, build, kind)
			}
		})
	}
}
