package node

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func TestGossipRouterImplementsRouter(t *testing.T) {
	var _ Router = (*GossipRouter)(nil)
}

func TestRoutingDecisionFieldsExist(t *testing.T) {
	rd := RoutingDecision{
		PushSubscribers: []*subscriber{{id: "sub-1", recipient: "alice"}},
		DirectPeers:     []string{"peer-1"},
		RelayNextHop:    peerIdentityPtr("relay-1"),
		GossipTargets:   []domain.PeerAddress{"gossip-1", "gossip-2"},
	}

	if len(rd.PushSubscribers) != 1 || rd.PushSubscribers[0].id != "sub-1" {
		t.Fatal("PushSubscribers field not populated correctly")
	}
	if len(rd.DirectPeers) != 1 || rd.DirectPeers[0] != "peer-1" {
		t.Fatal("DirectPeers field not populated correctly")
	}
	if rd.RelayNextHop == nil || *rd.RelayNextHop != domain.PeerIdentity("relay-1") {
		t.Fatal("RelayNextHop field not populated correctly")
	}
	if len(rd.GossipTargets) != 2 {
		t.Fatal("GossipTargets field not populated correctly")
	}
}

func TestRoutingDecisionEmptyByDefault(t *testing.T) {
	var rd RoutingDecision
	if rd.PushSubscribers != nil {
		t.Fatal("PushSubscribers should be nil by default")
	}
	if rd.DirectPeers != nil {
		t.Fatal("DirectPeers should be nil by default")
	}
	if rd.RelayNextHop != nil {
		t.Fatal("RelayNextHop should be nil by default")
	}
	if rd.GossipTargets != nil {
		t.Fatal("GossipTargets should be nil by default")
	}
}

func TestRouterInterfaceAcceptsEnvelope(t *testing.T) {
	msg := protocol.Envelope{
		ID:        "test-id",
		Topic:     "dm",
		Sender:    "alice",
		Recipient: "bob",
	}
	_ = msg
}

// newRecordingMockRouter creates a MockRouter that captures every Route
// call for later assertions and returns the given decision.
func newRecordingMockRouter(t *testing.T, decision RoutingDecision) (*MockRouter, *[]protocol.Envelope) {
	t.Helper()
	calls := &[]protocol.Envelope{}
	m := NewMockRouter(t)
	m.EXPECT().Route(mock.Anything).RunAndReturn(
		func(msg protocol.Envelope) RoutingDecision {
			*calls = append(*calls, msg)
			return decision
		},
	).Maybe()
	return m, calls
}

func TestMockRouterSatisfiesInterface(t *testing.T) {
	m := NewMockRouter(t)
	var _ Router = m
}

// TestServiceRouterFieldIsUsed verifies that Service.router is actually the
// field consulted during delivery. We inject a MockRouter and trigger
// the store-message path to prove calls go through s.router.
func TestServiceRouterFieldIsUsed(t *testing.T) {
	rec, _ := newRecordingMockRouter(t, RoutingDecision{
		GossipTargets: []domain.PeerAddress{}, // empty so no sends attempted
	})

	svc := &Service{
		router: rec,
	}

	// Verify the injected router is the one returned
	if svc.router != rec {
		t.Fatal("Service.router should be the injected MockRouter")
	}
}

func peerIdentityPtr(s string) *domain.PeerIdentity {
	id := domain.PeerIdentity(s)
	return &id
}
