package node

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestRelayReceiptCapabilityProofBindsRequestAndBoxKey(t *testing.T) {
	t.Parallel()
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	network := domain.NetworkID(networkName)
	label := domain.PeerIdentity{1}
	request, err := protocol.BuildGetIdentityPayload(protocol.GetIdentityPayload{
		V: domain.IdentityLookupSchemaVersion, TargetProof: true,
		Required: []string{domain.LookupRequirementSignedDirectReceipts},
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := protocol.BuildSignedIdentityRecord(owner, protocol.IdentityRecordSpec{
		Network: network, Seq: 1, IssuedAt: uint64(time.Now().Unix()), DM: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := protocol.BuildPostIdentityPayload(protocol.PostIdentityPayload{
		V: domain.IdentityLookupSchemaVersion, Record: record,
		TargetProof: protocol.SignTargetProof(owner, network, label, request, record),
	})
	if err != nil {
		t.Fatal(err)
	}
	probe := relayReceiptProbe{
		target: domain.PeerIdentityFromWire(owner.Address), boxKey: identity.BoxPublicKeyBase64(owner.BoxPublicKey),
		hash: sha256.Sum256(request),
	}
	if !probe.verifies(network, label, response) {
		t.Fatal("recipient's exact proof was refused")
	}
	if probe.verifies(network, domain.PeerIdentity{2}, response) {
		t.Fatal("proof was accepted for a later attempt label")
	}
	wrongKey := probe
	wrongKey.boxKey = "different box key"
	if wrongKey.verifies(network, label, response) {
		t.Fatal("proof was accepted for a different recipient box key")
	}
	ordinary := protocol.GetIdentityPayload{V: domain.IdentityLookupSchemaVersion, TargetProof: true}
	request, err = protocol.BuildGetIdentityPayload(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	probe.hash = sha256.Sum256(request)
	if probe.verifies(network, label, response) {
		t.Fatal("capability proof was accepted for a normal identity question")
	}
}
