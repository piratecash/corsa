package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Mode-selection telemetry (docs/refactoring/dht/05-rollout-metrics.md).
//
// The property under test throughout: the REASON comes from the branch that
// decided the mode. Every case below fixes both halves at once, so a reason
// re-derived from the capabilities by a second rule would disagree with the
// mode it travels with and the table would go red.

func capsOf(list ...domain.Capability) []PeerCapability {
	return append([]PeerCapability(nil), list...)
}

// allLocalCaps is "this node advertises everything", the setting under which a
// downgrade can only be the neighbour's. Tests about the mode LADDER use it so
// the ladder is not silently entangled with local attribution; the local
// limitation has its own tests.
func allLocalCaps() []PeerCapability {
	return capsOf(
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapMeshRoutingV3,
		domain.CapMeshRelayV1,
	)
}

// TestClassifyDeltaModeReportsWhyEachModeWasChosen pins the delta ladder and
// its reasons together.
func TestClassifyDeltaModeReportsWhyEachModeWasChosen(t *testing.T) {
	v1 := domain.CapMeshRoutingV1
	v2 := domain.CapMeshRoutingV2
	v3 := domain.CapMeshRoutingV3
	relay := domain.CapMeshRelayV1

	cases := map[string]struct {
		state, target []PeerCapability
		wantMode      deltaMode
		wantReason    ModeReason
	}{
		"full v3 triplet on both sides is the top of the ladder": {
			state:      capsOf(v1, v2, v3, relay),
			target:     capsOf(v1, v2, v3, relay),
			wantMode:   deltaModeV3,
			wantReason: ModeReasonNegotiated,
		},
		"peer without v3 caps the ladder at v2, and v3 is what it lacks": {
			state:      capsOf(v1, v2, relay),
			target:     capsOf(v1, v2, relay),
			wantMode:   deltaModeV2,
			wantReason: ModeReasonMissingRoutingV3,
		},
		"peer without v2 falls to legacy, and v2 is what it lacks": {
			state:      capsOf(v1, relay),
			target:     capsOf(v1, relay),
			wantMode:   deltaModeV1,
			wantReason: ModeReasonMissingRoutingV2,
		},
		"peer without routing v1 is legacy-only and v1 is the reason": {
			state:      capsOf(relay),
			target:     capsOf(relay),
			wantMode:   deltaModeV1,
			wantReason: ModeReasonMissingRoutingV1,
		},
		"relay is part of the triplet, so its absence is the reason": {
			state:      capsOf(v1, v2),
			target:     capsOf(v1, v2),
			wantMode:   deltaModeV1,
			wantReason: ModeReasonMissingRelayV1,
		},
		"the two capability views disagreeing is its own outcome": {
			state:      capsOf(v1, v2, v3, relay),
			target:     capsOf(v1, v2, relay),
			wantMode:   deltaModeDivergence,
			wantReason: ModeReasonCapabilityDivergence,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Local set = everything, so every downgrade below is the
			// neighbour's. The local-limitation cases are pinned separately.
			mode, reason := classifyDeltaMode(allLocalCaps(), tc.state, tc.target)
			if mode != tc.wantMode {
				t.Fatalf("mode = %v, want %v", mode, tc.wantMode)
			}
			if reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestClassifyFullSyncModeHasTwoRungs pins that the full-sync ladder never
// reports v2: there is no v2 full frame on the wire, and a telemetry row for
// one would describe a send this protocol cannot make.
func TestClassifyFullSyncModeHasTwoRungs(t *testing.T) {
	v1 := domain.CapMeshRoutingV1
	v2 := domain.CapMeshRoutingV2
	v3 := domain.CapMeshRoutingV3
	relay := domain.CapMeshRelayV1

	local := capsOf(v1, v2, v3, relay)
	mode, reason := ClassifyFullSyncMode(local, capsOf(v1, v3, relay))
	if mode != AnnounceModeV3 || reason != ModeReasonNegotiated {
		t.Fatalf("full triplet = (%v, %v), want (v3, negotiated)", mode, reason)
	}

	// A peer with the complete v2 triplet is still a legacy FULL sync: v2 has
	// no full frame. The reason must name v3, not v2.
	mode, reason = ClassifyFullSyncMode(local, capsOf(v1, v2, relay))
	if mode != AnnounceModeV1 {
		t.Fatalf("v2 triplet mode = %v, want v1 (there is no v2 full frame)", mode)
	}
	if reason != ModeReasonMissingRoutingV3 {
		t.Fatalf("v2 triplet reason = %v, want missing_mesh_routing_v3", reason)
	}
}

// TestModeReasonSeparatesPeerIncompatibilityFromLocalState pins the class
// split the whole breakdown exists for: a downgrade caused by OUR missing wire
// baseline must not be readable as a neighbour that needs upgrading.
func TestModeReasonSeparatesPeerIncompatibilityFromLocalState(t *testing.T) {
	incompatible := []ModeReason{
		ModeReasonMissingRoutingV1,
		ModeReasonMissingRoutingV2,
		ModeReasonMissingRoutingV3,
		ModeReasonMissingRelayV1,
	}
	for _, reason := range incompatible {
		if !reason.IsPeerIncompatibility() {
			t.Fatalf("%v must count as peer incompatibility", reason)
		}
	}
	notPeer := []ModeReason{
		ModeReasonNegotiated,
		ModeReasonNoWireBaselineV2,
		ModeReasonNoWireBaselineV3,
		ModeReasonCapabilityDivergence,
		ModeReasonUnknown,
	}
	for _, reason := range notPeer {
		if reason.IsPeerIncompatibility() {
			t.Fatalf("%v must NOT count as peer incompatibility", reason)
		}
	}
}

// TestModeSelectionCountersRecordOneDecisionPerIncrement pins the additivity
// invariant: the sum over the published triples is the number of decisions.
func TestModeSelectionCountersRecordOneDecisionPerIncrement(t *testing.T) {
	started := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	counters := NewModeSelectionCounters(started)

	counters.Record(AnnounceOperationDelta, AnnounceModeV3, ModeReasonNegotiated)
	counters.Record(AnnounceOperationDelta, AnnounceModeV3, ModeReasonNegotiated)
	counters.Record(AnnounceOperationDelta, AnnounceModeV1, ModeReasonNoWireBaselineV3)
	counters.Record(AnnounceOperationFullSync, AnnounceModeV1, ModeReasonMissingRoutingV3)

	stats := counters.Snapshot()
	if !stats.StartedAt.Equal(started) {
		t.Fatalf("started_at = %v, want %v", stats.StartedAt, started)
	}

	var total uint64
	for _, decision := range stats.Decisions {
		if decision.Count == 0 {
			t.Fatalf("zero-count triple published: %+v", decision)
		}
		total += decision.Count
	}
	if total != 4 {
		t.Fatalf("decisions sum to %d, want 4 — one increment per recorded decision", total)
	}
	if len(stats.Decisions) != 3 {
		t.Fatalf("published %d triples, want 3 distinct ones", len(stats.Decisions))
	}

	// Deterministic order: operation, then mode, then reason. Two consecutive
	// reads must be diffable row by row.
	for i := 1; i < len(stats.Decisions); i++ {
		prev, cur := stats.Decisions[i-1], stats.Decisions[i]
		if prev.Operation > cur.Operation ||
			(prev.Operation == cur.Operation && prev.Mode > cur.Mode) ||
			(prev.Operation == cur.Operation && prev.Mode == cur.Mode && prev.Reason > cur.Reason) {
			t.Fatalf("decisions are not ordered: %+v before %+v", prev, cur)
		}
	}
}

// TestModeSelectionCountersAreNilSafe pins that a loop built without telemetry
// behaves exactly as it did before the counter existed. Without this the
// option would be mandatory in name only, and the first construction that
// forgot it would panic on a path unrelated to what it was testing.
func TestModeSelectionCountersAreNilSafe(t *testing.T) {
	var counters *ModeSelectionCounters
	counters.Record(AnnounceOperationDelta, AnnounceModeV1, ModeReasonUnknown)
	stats := counters.Snapshot()
	if len(stats.Decisions) != 0 || !stats.StartedAt.IsZero() {
		t.Fatalf("nil counters reported %+v, want an empty period", stats)
	}
}

// TestModeSelectionCountersFoldUnknownReasons pins that a mislabelled decision
// is still counted. Dropping it would break the one property the counter has —
// that the triples add up to the decisions made.
func TestModeSelectionCountersFoldUnknownReasons(t *testing.T) {
	counters := NewModeSelectionCounters(time.Unix(0, 0).UTC())
	counters.Record(AnnounceOperationDelta, AnnounceModeV1, ModeReason(200))

	stats := counters.Snapshot()
	if len(stats.Decisions) != 1 {
		t.Fatalf("published %d triples, want 1", len(stats.Decisions))
	}
	if stats.Decisions[0].Reason != ModeReasonUnknown {
		t.Fatalf("reason = %v, want unknown", stats.Decisions[0].Reason)
	}
}

// TestLocallyDisabledCapabilityIsNotBlamedOnTheNeighbour is the regression
// guard for the defect that would have sent operators to upgrade a fleet that
// was already up to date.
//
// The capability snapshots a send reasons about are INTERSECTIONS with the
// local advertised set. Turning mesh_routing_v3 off locally therefore empties
// it from every neighbour's negotiated set — and the first version of this
// telemetry reported that as `missing_mesh_routing_v3`, i.e. as the
// neighbour's fault, for the entire network at once.
func TestLocallyDisabledCapabilityIsNotBlamedOnTheNeighbour(t *testing.T) {
	v1 := domain.CapMeshRoutingV1
	v2 := domain.CapMeshRoutingV2
	v3 := domain.CapMeshRoutingV3
	relay := domain.CapMeshRelayV1

	// This build does not advertise v3, so the negotiated set cannot contain
	// it no matter how modern the neighbour is.
	localWithoutV3 := capsOf(v1, v2, relay)
	negotiated := capsOf(v1, v2, relay)

	mode, reason := classifyDeltaMode(localWithoutV3, negotiated, negotiated)
	if mode != deltaModeV2 {
		t.Fatalf("mode = %v, want v2", mode)
	}
	if reason != ModeReasonLocalRoutingV3Disabled {
		t.Fatalf("reason = %v, want local_mesh_routing_v3_disabled", reason)
	}
	if reason.IsPeerIncompatibility() {
		t.Fatal("a locally disabled capability must never count as peer incompatibility")
	}
	if !reason.IsLocalLimitation() {
		t.Fatal("a locally disabled capability must count as a local limitation")
	}

	// Full sync takes the same attribution through its own ladder.
	fullMode, fullReason := ClassifyFullSyncMode(localWithoutV3, negotiated)
	if fullMode != AnnounceModeV1 || fullReason != ModeReasonLocalRoutingV3Disabled {
		t.Fatalf("full sync = (%v, %v), want (v1, local_mesh_routing_v3_disabled)", fullMode, fullReason)
	}

	// And with v3 advertised locally, the SAME negotiated set is the
	// neighbour's shortfall — the two cases are told apart by the local set
	// and nothing else.
	localWithV3 := capsOf(v1, v2, v3, relay)
	_, peerReason := classifyDeltaMode(localWithV3, negotiated, negotiated)
	if peerReason != ModeReasonMissingRoutingV3 {
		t.Fatalf("reason = %v, want missing_mesh_routing_v3", peerReason)
	}
}

// TestUnknownLocalSetIsNotAttributedLocally pins the safe direction for a
// caller that could not read its own advertisement: attributing a downgrade to
// ourselves tells an operator to stop looking, so an unknown local set must
// not do that.
func TestUnknownLocalSetIsNotAttributedLocally(t *testing.T) {
	v1 := domain.CapMeshRoutingV1
	relay := domain.CapMeshRelayV1
	negotiated := capsOf(v1, relay)

	_, reason := classifyDeltaMode(nil, negotiated, negotiated)
	if reason != ModeReasonMissingRoutingV2 {
		t.Fatalf("reason = %v, want missing_mesh_routing_v2 when the local set is unknown", reason)
	}
}

// TestModeSelectionSnapshotStampsItsOwnReadTime pins that the counters carry
// the moment they were LOADED.
//
// Without it the only timestamp in the response is snapshot_at, which belongs
// to the cached routing snapshot and stands still while the routing table is
// unchanged: two answers would share a timestamp and carry different counts,
// and every rate computed between them would be wrong by however long the
// table stood still.
func TestModeSelectionSnapshotStampsItsOwnReadTime(t *testing.T) {
	started := time.Now().UTC().Add(-time.Hour) // a period that really has started
	counters := NewModeSelectionCounters(started)

	first := counters.Snapshot()
	if first.ReadAt.IsZero() {
		t.Fatal("read_at must be stamped even when nothing was recorded")
	}
	if first.ReadAt.Before(started) {
		t.Fatalf("read_at %v precedes started_at %v", first.ReadAt, started)
	}

	counters.Record(AnnounceOperationDelta, AnnounceModeV3, ModeReasonNegotiated)
	second := counters.Snapshot()
	if second.ReadAt.Before(first.ReadAt) {
		t.Fatalf("read_at went backwards: %v then %v", first.ReadAt, second.ReadAt)
	}
	if !second.StartedAt.Equal(first.StartedAt) {
		t.Fatal("started_at must not move between reads — it is the opening edge of the period")
	}
}
