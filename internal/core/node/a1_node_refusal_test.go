package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
)

func TestTLSIdentityRefusalPreservesExistingEndpointPin(t *testing.T) {
	t.Parallel()

	other, address, stopOther := listenerNode(t, nil)
	defer stopOther()
	endpoint := domain.PeerAddress(normalizeAddress(address))
	expectedIdentity, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	expected := domain.PeerIdentityFromWire(expectedIdentity.Address)
	otherID := domain.PeerIdentityFromWire(other.Address())
	dialer, stopDialer := startQuietNode(t, func(s *Service) {
		if _, err := s.secureSessions.store.noteProvenOutbound(dialledAt(endpoint), expected, endpointProtectionNone()); err != nil {
			t.Fatalf("record existing endpoint pin: %v", err)
		}
	})
	defer stopDialer()

	intent, err := domain.DialToIdentity(expected)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	transport, err := dialer.dialPeerTransport(ctx, endpoint, intent, func(ctx context.Context) (net.Conn, error) {
		return dialer.dialPeerRawForCM(ctx, endpoint)
	})
	if transport != nil || !errors.Is(err, errEndpointOwnerMismatch) {
		t.Fatalf("dial with a named peer must refuse a different TLS identity, got %v", err)
	}

	for _, id := range []domain.PeerIdentity{expected, otherID} {
		if nodeAuthenticatedIdentity(dialer, id) || len(dialer.routingTable.Lookup(id)) != 0 {
			t.Fatal("refused TLS session published peer or route state")
		}
	}
	store := dialer.secureSessions.store
	if !store.identityPinned(expected) || !store.endpointsBoundTo(expected).covers(endpoint) ||
		store.identityPinned(otherID) || store.endpointsBoundTo(otherID).covers(endpoint) {
		t.Fatal("refused TLS session changed the existing identity or endpoint binding")
	}
}
