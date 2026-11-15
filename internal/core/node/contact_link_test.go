package node

import (
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/contactlink"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestPeerContactLinkRequiresCurrentVerifiedBinding(t *testing.T) {
	owner, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	address := owner.Address
	peer := domain.PeerIdentityFromWire(address)
	pub := identity.PublicKeyBase64(owner.PublicKey)
	box := identity.BoxPublicKeyBase64(owner.BoxPublicKey)
	sig := identity.SignBoxKeyBinding(owner)
	svc := &Service{
		pubKeys: map[string]string{address: pub},
		boxKeys: map[string]string{address: box},
		boxSigs: map[string]string{},
		trust:   &trustStore{contacts: make(map[string]trustedContact)},
	}
	if link := svc.PeerContactLink(peer); link != "" {
		t.Fatal("an unsigned key pair was exported")
	}
	svc.boxSigs[address] = strings.Repeat("A", len(sig))
	if link := svc.PeerContactLink(peer); link != "" {
		t.Fatal("an invalid key binding was exported")
	}
	svc.boxSigs[address] = sig
	checkLink := func() {
		t.Helper()
		link := svc.PeerContactLink(peer)
		parsed, err := contactlink.Parse(link, domain.NetworkID(networkName))
		if err != nil || parsed.Address != peer || string(parsed.BoxKey) != box {
			t.Fatalf("public keys did not round-trip: %v", err)
		}
	}
	checkLink()

	svc.trust.contacts[address] = trustedContact{Address: address, PubKey: pub, BoxKey: box}
	checkLink()
	newer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	svc.trust.contacts[address] = trustedContact{Address: address, PubKey: pub, BoxKey: identity.BoxPublicKeyBase64(newer.BoxPublicKey)}
	if link := svc.PeerContactLink(peer); link != "" {
		t.Fatal("the cached previous key was exported over the current trusted key")
	}

	delete(svc.boxSigs, address)
	svc.trust.contacts[address] = trustedContact{Address: address, PubKey: pub, BoxKey: box, BoxSignature: sig}
	checkLink()
}
