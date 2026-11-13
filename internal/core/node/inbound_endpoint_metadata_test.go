package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestInboundEndpointRefreshKeepsAuthenticatedIdentity(t *testing.T) {
	svc := newDialChoiceService(t)
	peer := domain.PeerIdentityFromWire(svc.identity.Address)
	origin := domain.PeerIdentityProvenV2
	svc.addPeerID(dialChoiceAddress, addressIdentity{identity: peer, origin: origin})

	frame := protocol.Frame{Address: peer.String(), ClientVersion: "2.5.71", ClientBuild: 71, NodeType: "full"}
	if !svc.recordInboundPeer(dialChoiceAddress, frame, origin) {
		t.Fatal("authenticated endpoint metadata refresh refused")
	}

	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	if got := svc.peerIDs[dialChoiceAddress]; got != (addressIdentity{identity: peer, origin: origin}) {
		t.Fatalf("endpoint identity after refresh = %+v", got)
	}
	if got := svc.peerVersions[dialChoiceAddress]; got != frame.ClientVersion {
		t.Errorf("endpoint version = %q, want %q", got, frame.ClientVersion)
	}
	if got := svc.peerBuilds[dialChoiceAddress]; got != frame.ClientBuild {
		t.Errorf("endpoint build = %d, want %d", got, frame.ClientBuild)
	}
	if got := svc.peerTypes[dialChoiceAddress]; got != domain.NodeTypeFull {
		t.Errorf("endpoint type = %v, want full", got)
	}
}
