package routing_test

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// UpdateCapabilities is the metadata-only refresh path for
// AnnouncePeerState.capabilities. It exists so that an overlapping
// reconnect — additional session 2 for the same identity that brings
// different caps than session 1 — can update the persistent capability
// snapshot read by classifyDeltaMode without touching the rest of the
// state. Without this method, MarkInvalid alone (which only flips
// needsFullResync) leaves the persistent caps stale and the peer is
// stuck in divergence/forced-full forever. The tests below pin both the
// happy path (replacement, idempotence, missing-state no-op) and the
// surrounding negative invariants (no leak into baseline, wire-baseline,
// or needsFullResync flags).

// TestAnnounceStateRegistry_UpdateCapabilities_ReplacesOnExistingState
// pins the primary contract: UpdateCapabilities replaces the persistent
// capabilities slice on an existing state record. The View snapshot
// reflects the change atomically.
func TestAnnounceStateRegistry_UpdateCapabilities_ReplacesOnExistingState(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("precondition: state must exist after MarkReconnected")
	}

	// Refresh with a v1+v2 cap set — simulating the overlapping reconnect
	// where session 2 brings v2 support that session 1 did not have.
	registry.UpdateCapabilities("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	view := state.View()
	wantSet := map[routing.PeerCapability]bool{
		domain.CapMeshRoutingV1: true,
		domain.CapMeshRoutingV2: true,
	}
	for _, c := range view.CapabilitiesSnapshot {
		if !wantSet[c] {
			t.Fatalf("unexpected capability in snapshot: %s", c)
		}
		delete(wantSet, c)
	}
	if len(wantSet) > 0 {
		t.Fatalf("missing capabilities in refreshed snapshot: %v", wantSet)
	}
}

// TestAnnounceStateRegistry_UpdateCapabilities_NoStateIsNoop pins that
// calling UpdateCapabilities for an identity with no state record is a
// silent no-op — never panics, never materialises a state record, never
// allocates a phantom AnnouncePeerState that subsequent MarkReconnected
// would have to overwrite.
func TestAnnounceStateRegistry_UpdateCapabilities_NoStateIsNoop(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()

	// No MarkReconnected / GetOrCreate before this call — state is absent.
	registry.UpdateCapabilities("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	if state := registry.Get("peer-A"); state != nil {
		t.Fatalf("UpdateCapabilities on missing state must NOT materialise a record, got %v", state)
	}
}

// TestAnnounceStateRegistry_UpdateCapabilities_DoesNotResetBaselineFlags
// pins the narrow contract of UpdateCapabilities: it touches the
// persistent capability slice and nothing else. Resetting either the
// receive-side baseline (hasReceivedBaseline) or the send-side
// wire-baseline (wireBaselineSentToPeer) on an additional session would
// force a needless request_resync round-trip and re-establish the
// baseline that the peer-identity layer already holds — caps are an
// identity-level property, not a session-level one.
func TestAnnounceStateRegistry_UpdateCapabilities_DoesNotResetBaselineFlags(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("precondition: state must exist")
	}

	// Establish both directions of the baseline gate.
	state.MarkBaselineReceived()
	state.MarkWireBaselineSent()
	if !state.HasReceivedBaseline() {
		t.Fatalf("precondition: HasReceivedBaseline must be true")
	}
	if !state.HasSentWireBaseline() {
		t.Fatalf("precondition: HasSentWireBaseline must be true")
	}

	registry.UpdateCapabilities("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	if !state.HasReceivedBaseline() {
		t.Fatalf("UpdateCapabilities must NOT reset HasReceivedBaseline")
	}
	if !state.HasSentWireBaseline() {
		t.Fatalf("UpdateCapabilities must NOT reset HasSentWireBaseline")
	}
}

// TestAnnounceStateRegistry_UpdateCapabilities_DoesNotTouchSyncState
// pins that UpdateCapabilities does not flip needsFullResync, does not
// touch lastSentSnapshot, and does not move any timestamp. Treating caps
// refresh as a sync-state event would force an unnecessary forced-full
// on every additional session — the routing table is unchanged, the
// receiver's view is unchanged, only the metadata snapshot moved.
func TestAnnounceStateRegistry_UpdateCapabilities_DoesNotTouchSyncState(t *testing.T) {
	now := time.Now()
	registry := routing.NewAnnounceStateRegistry()
	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("precondition: state must exist")
	}

	// Establish a baseline send so NeedsFullResync clears and
	// LastSuccessfulFullSyncAt is set to a known value.
	snapshot := &routing.AnnounceSnapshot{}
	state.RecordFullSyncSuccess(snapshot, now)
	view := state.View()
	if view.NeedsFullResync {
		t.Fatalf("precondition: NeedsFullResync must be false after RecordFullSyncSuccess")
	}
	if view.LastSentSnapshot != snapshot {
		t.Fatalf("precondition: LastSentSnapshot must point at the recorded snapshot")
	}
	priorLastSucc := view.LastSuccessfulFullSyncAt

	registry.UpdateCapabilities("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	view = state.View()
	if view.NeedsFullResync {
		t.Fatalf("UpdateCapabilities must NOT flip NeedsFullResync")
	}
	if view.LastSentSnapshot != snapshot {
		t.Fatalf("UpdateCapabilities must NOT replace LastSentSnapshot")
	}
	if !view.LastSuccessfulFullSyncAt.Equal(priorLastSucc) {
		t.Fatalf("UpdateCapabilities must NOT move LastSuccessfulFullSyncAt: was %v, now %v", priorLastSucc, view.LastSuccessfulFullSyncAt)
	}
}

// TestAnnounceStateRegistry_UpdateCapabilities_NilInputClearsCapabilities
// pins the corner case: a nil/empty caps slice is a legitimate "peer
// advertised no caps" signal and UpdateCapabilities must propagate it
// rather than silently keeping the prior snapshot. This matches
// MarkReconnected, which also accepts nil caps as the legacy-node case.
func TestAnnounceStateRegistry_UpdateCapabilities_NilInputClearsCapabilities(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	registry.MarkReconnected("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("precondition: state must exist")
	}
	if len(state.View().CapabilitiesSnapshot) == 0 {
		t.Fatalf("precondition: snapshot must be non-empty before nil refresh")
	}

	registry.UpdateCapabilities("peer-A", nil)

	if got := state.View().CapabilitiesSnapshot; got != nil {
		t.Fatalf("UpdateCapabilities(nil) must propagate as nil snapshot, got %v", got)
	}
}

// TestAnnounceStateRegistry_UpdateCapabilities_DefensiveCopy pins that
// the registry takes a defensive copy of the input caps slice so that
// post-call mutation of the caller's backing array cannot leak into
// AnnouncePeerState.capabilities. MarkReconnected has the same contract
// — UpdateCapabilities must match it so the two metadata-write paths are
// substitutable from a memory-safety standpoint.
func TestAnnounceStateRegistry_UpdateCapabilities_DefensiveCopy(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("precondition: state must exist")
	}

	caps := []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	}
	registry.UpdateCapabilities("peer-A", caps)

	// Mutate the caller's slice — the stored snapshot must NOT change.
	caps[0] = domain.CapMeshRelayV1
	caps[1] = domain.CapMeshRelayV1

	view := state.View()
	wantSet := map[routing.PeerCapability]bool{
		domain.CapMeshRoutingV1: true,
		domain.CapMeshRoutingV2: true,
	}
	for _, c := range view.CapabilitiesSnapshot {
		if !wantSet[c] {
			t.Fatalf("post-mutation snapshot leaked: unexpected %s", c)
		}
		delete(wantSet, c)
	}
	if len(wantSet) > 0 {
		t.Fatalf("post-mutation snapshot lost capabilities: %v", wantSet)
	}
}
