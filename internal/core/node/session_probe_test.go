package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// oldReleaseCapabilities is what a v2.4.70 full node with mesh routing v3
// and both datagram roles enabled puts into welcome (localCapabilities of
// that release).
var oldReleaseCapabilities = []string{
	string(domain.CapMeshRelayV1),
	string(domain.CapMeshRoutingV1),
	string(domain.CapMeshRoutingV2),
	string(domain.CapFileTransferV1),
	string(domain.CapMeshRouteProbeV1),
	string(domain.CapMeshRouteQueryV1),
	string(domain.CapMeshRouteSyncV1),
	string(domain.CapMeshRoutingV3),
	string(domain.CapMeshPoisonReverseV1),
	string(domain.CapMeshPoisonReverseV2),
	string(domain.CapMeshDatagramV1),
	string(domain.CapMeshDatagramTransitV1),
}

func oldReleaseWelcome(t *testing.T, id *identity.Identity, version int) protocol.Frame {
	t.Helper()
	dtypes := []string{
		string(domain.DTypeDMControl),
		string(domain.DTypeGetIdentity),
		string(domain.DTypePostIdentity),
		string(domain.DTypePushIdentity),
	}
	return protocol.Frame{
		Type:                   "welcome",
		Version:                version,
		MinimumProtocolVersion: 26,
		Node:                   nodeName,
		Network:                networkName,
		Listener:               "1",
		AdvertisePort:          64646,
		NodeType:               "full",
		ClientVersion:          "2.4.70",
		ClientBuild:            70,
		Address:                id.Address,
		PubKey:                 identity.PublicKeyBase64(id.PublicKey),
		BoxKey:                 identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSig:                 identity.SignBoxKeyBinding(id),
		ObservedAddress:        "127.0.0.1",
		Services:               []string{"identity", "contacts", "messages", "gazeta", "relay"},
		Capabilities:           append([]string(nil), oldReleaseCapabilities...),
		DTypes:                 &dtypes,
	}
}

func generateProbeIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id
}
