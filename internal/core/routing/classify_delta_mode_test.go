package routing

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
)

// classifyDeltaMode is the pure capability-classification core of the v2
// mode selection. The announce loop reconciles persistent and target caps
// at cycle start (UpdateCapabilities), so in production the divergence
// path is unreachable — the loop calls classifyDeltaMode with the same
// caps in both arguments. The unit tests below pin the function-level
// contract (V1 / V2 / divergence) so that, if a future call site routes
// around the cycle-time reconciliation, the safety net keeps producing
// the documented outcome.

// TestClassifyDeltaMode_BothV1AndV2_ReturnsV2 pins the v2 path: when
// both capability inputs carry mesh_routing_v1 and mesh_routing_v2, the
// classifier picks the v2 delta wire frame.
func TestClassifyDeltaMode_BothV1AndV2_ReturnsV2(t *testing.T) {
	caps := []PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}
	if got := classifyDeltaMode(caps, caps); got != deltaModeV2 {
		t.Fatalf("classifyDeltaMode({v1,v2}, {v1,v2}) = %v, want deltaModeV2", got)
	}
}

// TestClassifyDeltaMode_BothLegacy_ReturnsV1 pins the v1 path: when
// neither input has mesh_routing_v2, the classifier picks the legacy
// delta wire frame. nil inputs are the legacy-peer case and must reach
// the same outcome.
func TestClassifyDeltaMode_BothLegacy_ReturnsV1(t *testing.T) {
	cases := []struct {
		name       string
		state, tgt []PeerCapability
	}{
		{name: "nil-nil", state: nil, tgt: nil},
		{name: "v1-only-both", state: []PeerCapability{domain.CapMeshRoutingV1}, tgt: []PeerCapability{domain.CapMeshRoutingV1}},
		{name: "relay-only-both", state: []PeerCapability{domain.CapMeshRelayV1}, tgt: []PeerCapability{domain.CapMeshRelayV1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDeltaMode(tc.state, tc.tgt); got != deltaModeV1 {
				t.Fatalf("classifyDeltaMode(%v, %v) = %v, want deltaModeV1", tc.state, tc.tgt, got)
			}
		})
	}
}

// TestClassifyDeltaMode_V2WithoutV1_TreatedAsV1 pins the protocol rule
// that mesh_routing_v2 is opt-in atop mesh_routing_v1. A peer that
// somehow advertised v2 without v1 must be classified as v1-only —
// the first-sync invariant gates v2 baseline delivery on the v1 wire
// frame, so v2-without-v1 is meaningless and must fall back to legacy.
func TestClassifyDeltaMode_V2WithoutV1_TreatedAsV1(t *testing.T) {
	caps := []PeerCapability{domain.CapMeshRoutingV2}
	if got := classifyDeltaMode(caps, caps); got != deltaModeV1 {
		t.Fatalf("classifyDeltaMode({v2}, {v2}) = %v, want deltaModeV1 (v2 without v1 is meaningless)", got)
	}
}

// TestClassifyDeltaMode_StateV2_TargetV1_Divergence pins the
// divergence path on the function level. The announce loop's
// cycle-time UpdateCapabilities normally prevents the inputs from
// disagreeing, but the function itself must still classify mismatched
// inputs as deltaModeDivergence so the safety net works for any future
// caller that does not run through the cycle-time sync.
func TestClassifyDeltaMode_StateV2_TargetV1_Divergence(t *testing.T) {
	stateCaps := []PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}
	targetCaps := []PeerCapability{domain.CapMeshRoutingV1}
	if got := classifyDeltaMode(stateCaps, targetCaps); got != deltaModeDivergence {
		t.Fatalf("classifyDeltaMode({v1,v2}, {v1}) = %v, want deltaModeDivergence", got)
	}
}

// TestClassifyDeltaMode_StateV1_TargetV2_Divergence is the symmetric
// disagreement case: persistent state lacks v2, target advertises it.
// Same outcome — the function does not pick a winner; it raises the
// flag and lets the loop decide on a fallback.
func TestClassifyDeltaMode_StateV1_TargetV2_Divergence(t *testing.T) {
	stateCaps := []PeerCapability{domain.CapMeshRoutingV1}
	targetCaps := []PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV2}
	if got := classifyDeltaMode(stateCaps, targetCaps); got != deltaModeDivergence {
		t.Fatalf("classifyDeltaMode({v1}, {v1,v2}) = %v, want deltaModeDivergence", got)
	}
}
