package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// classify_delta_mode_v3_test.go covers the Phase 4 extension to
// classifyDeltaMode: deltaModeV3 wins over v2 when both caps include
// the full v3 triplet (mesh_routing_v1 + mesh_routing_v3 +
// mesh_relay_v1 — Round-20 alignment), mixed disagreement on v3
// yields divergence, and a peer pair that lacks v3 falls back to the
// v2/v1 ladder unchanged. The triplet is mandatory because the send-
// side dispatch in node.dispatchAnnouncePlaneFrameWithCaps gates
// SendRouteAnnounceV3 on v1+v3+relay; classifying a relay-less peer
// as v3 would route to a sender that refuses the frame with no
// legacy fallback.

// capsV1V2 returns the v2 wire triplet (v1+v2+relay) — production
// shape for the v2-but-not-v3 deployment. Round-21 added relay
// alongside the Round-20 v3 triplet so classifier and dispatch agree.
func capsV1V2() []PeerCapability {
	return []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapMeshRelayV1,
	}
}

// capsV1V2NoRelay returns v1+v2 WITHOUT relay — the Round-21
// negative case: classifyDeltaMode must NOT pick v2 here because the
// send-side gate would refuse the frame with no legacy fallback.
func capsV1V2NoRelay() []PeerCapability {
	return []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	}
}

// capsV1V2V3 returns the full v3 triplet AND v2: the production-
// realistic deployed shape (localCapabilities lights up v1+v2+v3+relay
// when EnableMeshRoutingV3 is on).
func capsV1V2V3() []PeerCapability {
	return []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapMeshRoutingV3,
		domain.CapMeshRelayV1,
	}
}

// capsV1V3 returns the full v3 triplet (v1+v3+relay) WITHOUT v2 — the
// minimal v3-capable shape. Round-20: relay is part of the triplet.
func capsV1V3() []PeerCapability {
	return []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV3,
		domain.CapMeshRelayV1,
	}
}

// capsV1V3NoRelay returns v1+v3 WITHOUT relay — the Round-20 negative
// case: classifyDeltaMode / PeerSupportsV3 must NOT pick v3 here
// because the send-side dispatch gate would refuse the frame with no
// legacy fallback.
func capsV1V3NoRelay() []PeerCapability {
	return []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV3,
	}
}

func TestClassifyDeltaMode_BothV3_ReturnsV3(t *testing.T) {
	if got := classifyDeltaMode(capsV1V2V3(), capsV1V2V3()); got != deltaModeV3 {
		t.Fatalf("both v3: got %d want deltaModeV3", got)
	}
	// v1+v3 without v2 still picks v3 (v3 doesn't require v2).
	if got := classifyDeltaMode(capsV1V3(), capsV1V3()); got != deltaModeV3 {
		t.Fatalf("v1+v3 (no v2): got %d want deltaModeV3", got)
	}
}

func TestClassifyDeltaMode_V3DisagreementYieldsDivergence(t *testing.T) {
	// One side has v3, the other doesn't (but agrees on v2). The
	// classifier treats this as a state inconsistency and returns
	// divergence, matching the defensive contract for v2 disagreement.
	if got := classifyDeltaMode(capsV1V2V3(), capsV1V2()); got != deltaModeDivergence {
		t.Fatalf("v3 mismatch (state>target): got %d want deltaModeDivergence", got)
	}
	if got := classifyDeltaMode(capsV1V2(), capsV1V2V3()); got != deltaModeDivergence {
		t.Fatalf("v3 mismatch (target>state): got %d want deltaModeDivergence", got)
	}
}

func TestClassifyDeltaMode_V3WithoutV1IsLegacy(t *testing.T) {
	// v3 without v1 is treated as v1-absent (CapMeshRoutingV3 doc):
	// the legacy fallback gates on v1, so a peer advertising v3 alone
	// cannot speak the wire pair at all. Both sides identical → v1
	// path (which here means classifier picks v1 because both v2 and
	// v3 are absent).
	v3Only := []PeerCapability{domain.CapMeshRoutingV3}
	if got := classifyDeltaMode(v3Only, v3Only); got != deltaModeV1 {
		t.Fatalf("v3-only (no v1): got %d want deltaModeV1", got)
	}
}

func TestClassifyDeltaMode_NoV3PreservesLegacyLadder(t *testing.T) {
	// Sanity guard: removing v3 from both inputs must leave the
	// pre-Phase-4 v2/v1 ladder untouched. Regression net against an
	// accidental shadowing of the legacy paths by the new v3 branch.
	if got := classifyDeltaMode(capsV1V2(), capsV1V2()); got != deltaModeV2 {
		t.Fatalf("v1+v2 both sides: got %d want deltaModeV2", got)
	}
	v1Only := []PeerCapability{domain.CapMeshRoutingV1}
	if got := classifyDeltaMode(v1Only, v1Only); got != deltaModeV1 {
		t.Fatalf("v1-only both sides: got %d want deltaModeV1", got)
	}
}

func TestPeerSupportsV3_ExportedMatchesInternal(t *testing.T) {
	// PeerSupportsV3 is exported so any package picking the v3 wire
	// path uses the same predicate as the internal hasCapV3Triplet
	// that drives classifyDeltaMode. The node-layer connect-time path
	// does NOT call this helper directly — it owns a parallel
	// Service.peerSupportsRoutingV3 (capabilities.go) that walks the
	// session / inbound-conn cap snapshot under peerMu — but both
	// helpers MUST agree on the same triplet (Round-20: v1+v3+relay)
	// so the layer boundary doesn't silently disagree on what counts
	// as v3-capable. The node-side test is
	// peer_supports_routing_v3_test.go.
	cases := []struct {
		name string
		caps []PeerCapability
		want bool
	}{
		{"v1+v3+relay (triplet)", capsV1V3(), true},
		{"v1+v2+v3+relay (deployed shape)", capsV1V2V3(), true},
		{"v1+v3 missing relay (Round-20 negative)", capsV1V3NoRelay(), false},
		{"v1+v2 (no v3)", capsV1V2(), false},
		{"v3-only", []PeerCapability{domain.CapMeshRoutingV3}, false},
		{"v3+relay missing v1", []PeerCapability{domain.CapMeshRoutingV3, domain.CapMeshRelayV1}, false},
		{"empty", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := PeerSupportsV3(c.caps); got != c.want {
				t.Fatalf("PeerSupportsV3(%v) = %v want %v", c.caps, got, c.want)
			}
			if got := hasCapV3Triplet(c.caps); got != c.want {
				t.Fatalf("hasCapV3Triplet(%v) = %v want %v", c.caps, got, c.want)
			}
		})
	}
}

// TestClassifyDeltaMode_RelayMissingFallsAllTheWayToV1 pins the
// Round-20 + Round-21 fallback contract: BOTH the v3 ladder and the
// v2 ladder now require relay (the send-side dispatch in
// node.dispatchAnnouncePlaneFrameWithCaps gates SendRoutesUpdate and
// SendRouteAnnounceV3 on the relay cap). A relay-less peer therefore
// falls past v3 AND past v2 down to legacy v1, no matter how many
// upgrade caps it advertises. Without this fallback the loop would
// pick an upgraded path, the send-side gate would refuse the frame,
// and the cycle would silently drop the snapshot with no legacy
// fallback.
func TestClassifyDeltaMode_RelayMissingFallsAllTheWayToV1(t *testing.T) {
	// v1+v2+v3 without relay: both ladders gate on relay (Round-20
	// for v3, Round-21 for v2), so the classifier walks down past
	// v3 and v2 to legacy v1.
	relayless := []PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapMeshRoutingV3,
	}
	if got := classifyDeltaMode(relayless, relayless); got != deltaModeV1 {
		t.Fatalf("v1+v2+v3 without relay: got %d want deltaModeV1 (Round-20 + Round-21 both gate on relay)", got)
	}
	// v1+v3 without relay (or v2) — same fallback.
	relaylessV3 := capsV1V3NoRelay()
	if got := classifyDeltaMode(relaylessV3, relaylessV3); got != deltaModeV1 {
		t.Fatalf("v1+v3 without relay: got %d want deltaModeV1 (Round-20 fallback)", got)
	}
	// v1+v2 without relay — Round-21 fallback specifically.
	relaylessV2 := capsV1V2NoRelay()
	if got := classifyDeltaMode(relaylessV2, relaylessV2); got != deltaModeV1 {
		t.Fatalf("v1+v2 without relay: got %d want deltaModeV1 (Round-21 fallback)", got)
	}
}

// TestHasCapV2Triplet_PinsTriplet pins the Round-21 v2 triplet
// requirement explicitly for the helper (mirrors the v3 helper
// table-test for symmetry).
func TestHasCapV2Triplet_PinsTriplet(t *testing.T) {
	cases := []struct {
		name string
		caps []PeerCapability
		want bool
	}{
		{"v1+v2+relay (triplet)", capsV1V2(), true},
		{"v1+v2 missing relay (Round-21 negative)", capsV1V2NoRelay(), false},
		{"v1+relay missing v2", []PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRelayV1}, false},
		{"v2+relay missing v1", []PeerCapability{domain.CapMeshRoutingV2, domain.CapMeshRelayV1}, false},
		{"empty", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasCapV2Triplet(c.caps); got != c.want {
				t.Fatalf("hasCapV2Triplet(%v) = %v want %v", c.caps, got, c.want)
			}
		})
	}
}

func TestAnnouncePeerState_V3BaselineFlagLifecycle(t *testing.T) {
	r := NewAnnounceStateRegistry()
	pid := PeerIdentity(idV3EpochPeer)
	r.MarkReconnected(pid, nil)
	s := r.Get(pid)

	if s.HasSentWireBaselineV3() {
		t.Fatalf("fresh state must start with HasSentWireBaselineV3=false")
	}
	if v := s.View().HasSentWireBaselineV3; v {
		t.Fatalf("View().HasSentWireBaselineV3 must be false initially")
	}

	s.MarkWireBaselineV3Sent()
	if !s.HasSentWireBaselineV3() {
		t.Fatalf("MarkWireBaselineV3Sent must flip the flag")
	}
	if v := s.View().HasSentWireBaselineV3; !v {
		t.Fatalf("View().HasSentWireBaselineV3 must reflect the flip")
	}

	// Session boundary must reset the v3 flag (Phase 4 requirement: a
	// fresh session has no observable v3 baseline).
	r.MarkDisconnected(pid)
	r.MarkReconnected(pid, nil)
	s = r.Get(pid)
	if s.HasSentWireBaselineV3() {
		t.Fatalf("session boundary must clear HasSentWireBaselineV3")
	}
}
