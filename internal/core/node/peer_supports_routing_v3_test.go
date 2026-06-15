package node

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// peer_supports_routing_v3_test.go pins the Round-19 fix:
// peerSupportsRoutingV3 must return true ONLY for peers that have the
// FULL v3 wire triplet — mesh_routing_v1 + mesh_routing_v3 +
// mesh_relay_v1 — negotiated. The old helper checked only v1+v3 and
// could return true for a relay-less peer, which then silently
// failed the send-side dispatch gate in
// dispatchAnnouncePlaneFrameWithCaps with no legacy fallback (the
// full snapshot was dropped). Aligning the helper with the actual
// send-side gate keeps the connect-time / forced-full path on legacy
// SendAnnounceRoutes when any leg of the triplet is missing.

// TestPeerSupportsRoutingV3_OutboundTripletRequired walks the outbound
// session branch of the helper. Three test peers cover the missing-leg
// cases (no v3, no relay, no v1) plus the all-three positive.
func TestPeerSupportsRoutingV3_OutboundTripletRequired(t *testing.T) {
	cases := []struct {
		name string
		caps []domain.Capability
		want bool
	}{
		{
			name: "full_triplet_returns_true",
			caps: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
			want: true,
		},
		{
			name: "missing_v3_returns_false",
			caps: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1},
			want: false,
		},
		{
			name: "missing_relay_returns_false_round19_fix",
			caps: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3},
			want: false,
		},
		{
			name: "missing_v1_returns_false",
			caps: []domain.Capability{domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
			want: false,
		},
		{
			name: "empty_caps_returns_false",
			caps: nil,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestServiceWithRouting(t, idNodeA)
			addr := domain.PeerAddress("addr-X")
			svc.peerMu.Lock()
			svc.sessions[addr] = &peerSession{
				address:      addr,
				peerIdentity: idPeerB,
				capabilities: tc.caps,
			}
			svc.peerMu.Unlock()
			if got := svc.peerSupportsRoutingV3(addr); got != tc.want {
				t.Fatalf("peerSupportsRoutingV3(%v) = %v, want %v", tc.caps, got, tc.want)
			}
		})
	}
}

// TestPeerSupportsRoutingV3_NoLiveTransportReturnsFalse pins the
// "address resolves to nothing" fallback: a missing session must
// return false so the caller falls back to legacy rather than picking
// v3 and silently dropping the send.
func TestPeerSupportsRoutingV3_NoLiveTransportReturnsFalse(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	if got := svc.peerSupportsRoutingV3(domain.PeerAddress("addr-nonexistent")); got {
		t.Fatal("peerSupportsRoutingV3 on an unknown address must return false (legacy fallback contract)")
	}
}
