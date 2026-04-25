package routing_test

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/routing"
)

// The "baseline received" flag on AnnouncePeerState gates the v2 receive
// path: a routes_update arriving before any legacy announce_routes in the
// current session means the local state is desynced and the receiver must
// emit request_resync instead of silently applying the delta. The flag must
// therefore start at false on every fresh state and reset on every session
// boundary (MarkDisconnected / MarkReconnected). The tests below pin those
// transitions so a future refactor cannot regress the gate without a loud
// failure.

// TestAnnouncePeerState_Baseline_DefaultFalse pins that a freshly created
// per-peer state reports HasReceivedBaseline() == false. Without this default
// the very first session of a peer would unlock the v2 delta path before any
// baseline arrived — see handleRoutesUpdate in routing_announce.go for the
// gating contract.
func TestAnnouncePeerState_Baseline_DefaultFalse(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()

	state := registry.GetOrCreate("peer-A")
	if state.HasReceivedBaseline() {
		t.Fatalf("fresh AnnouncePeerState must have HasReceivedBaseline()==false")
	}

	view := state.View()
	if view.HasReceivedBaseline {
		t.Fatalf("fresh View must have HasReceivedBaseline==false")
	}
}

// TestAnnouncePeerState_MarkBaselineReceived_FlipsFlag pins the only writer
// of the baseline flag: MarkBaselineReceived flips it to true, and the View
// snapshot reflects the change atomically.
func TestAnnouncePeerState_MarkBaselineReceived_FlipsFlag(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")

	state.MarkBaselineReceived()

	if !state.HasReceivedBaseline() {
		t.Fatalf("after MarkBaselineReceived, HasReceivedBaseline() must be true")
	}
	if !state.View().HasReceivedBaseline {
		t.Fatalf("View().HasReceivedBaseline must reflect the flip")
	}

	// Idempotent: a second call must keep the flag set.
	state.MarkBaselineReceived()
	if !state.HasReceivedBaseline() {
		t.Fatalf("MarkBaselineReceived must be idempotent — flag must stay true")
	}
}

// TestAnnounceStateRegistry_MarkDisconnected_ResetsBaseline pins the session
// boundary semantics: when a session is torn down, the baseline flag resets
// to false so a future MarkReconnected does not silently inherit the prior
// session's baseline. Inheriting it would let stale per-peer SeqNo space
// become indistinguishable from a fresh session at the v2 receive gate.
func TestAnnounceStateRegistry_MarkDisconnected_ResetsBaseline(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")
	state.MarkBaselineReceived()
	if !state.HasReceivedBaseline() {
		t.Fatalf("precondition: flag must be true before disconnect")
	}

	registry.MarkDisconnected("peer-A")

	if state.HasReceivedBaseline() {
		t.Fatalf("MarkDisconnected must reset HasReceivedBaseline() to false")
	}
}

// TestAnnounceStateRegistry_MarkReconnected_ResetsBaseline_OnExistingState
// pins the idempotent reset path: MarkReconnected on a state that still
// holds a previous-session baseline must clear it, even if MarkDisconnected
// was never called (e.g. a teardown path that skipped the disconnect hook).
// Without this reset, the very first routes_update of the new session would
// pass the gate with no baseline ever delivered in this session.
func TestAnnounceStateRegistry_MarkReconnected_ResetsBaseline_OnExistingState(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")
	state.MarkBaselineReceived()
	if !state.HasReceivedBaseline() {
		t.Fatalf("precondition: flag must be true before MarkReconnected")
	}

	// Reconnect without a prior MarkDisconnected — simulating a teardown
	// path that lost the disconnect hook.
	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	if state.HasReceivedBaseline() {
		t.Fatalf("MarkReconnected must reset HasReceivedBaseline() to false on existing state")
	}
}

// TestAnnounceStateRegistry_MarkReconnected_FreshState_NoBaseline pins that
// MarkReconnected creating a brand-new state record never sets the baseline
// flag. The view-side snapshot must agree with the direct accessor.
func TestAnnounceStateRegistry_MarkReconnected_FreshState_NoBaseline(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()

	registry.MarkReconnected("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("MarkReconnected must materialise the per-peer state")
	}
	if state.HasReceivedBaseline() {
		t.Fatalf("MarkReconnected on fresh state must leave HasReceivedBaseline() false")
	}

	view := state.View()
	if view.HasReceivedBaseline {
		t.Fatalf("View on fresh reconnected state must report HasReceivedBaseline==false")
	}

	// Sanity: capabilities snapshot is what we passed in. This pins that
	// the v2 mode-selection path reads the same caps the session hook
	// captured.
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
		t.Fatalf("missing capabilities in snapshot: %v", wantSet)
	}
}

// TestAnnouncePeerState_Baseline_SurvivesRecordCalls pins that the regular
// send-side bookkeeping methods (RecordFullSyncSuccess, RecordFullSyncAttempt,
// RecordDeltaSendSuccess) do not flip the baseline flag in either direction.
// The flag is owned exclusively by MarkBaselineReceived / session-boundary
// resets — any leak through a Record* call would entangle the receive gate
// with the send-side cache and is a design regression.
func TestAnnouncePeerState_Baseline_SurvivesRecordCalls(t *testing.T) {
	now := time.Now()
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")

	// Baseline starts false; Record* calls must keep it that way.
	state.RecordFullSyncAttempt(now)
	if state.HasReceivedBaseline() {
		t.Fatalf("RecordFullSyncAttempt must not flip baseline to true")
	}
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now)
	if state.HasReceivedBaseline() {
		t.Fatalf("RecordFullSyncSuccess must not flip baseline to true")
	}
	state.RecordDeltaSendSuccess(&routing.AnnounceSnapshot{}, now)
	if state.HasReceivedBaseline() {
		t.Fatalf("RecordDeltaSendSuccess must not flip baseline to true")
	}

	// Now flip baseline manually — Record* calls must keep it true.
	state.MarkBaselineReceived()

	state.RecordFullSyncAttempt(now)
	if !state.HasReceivedBaseline() {
		t.Fatalf("RecordFullSyncAttempt must not clear baseline back to false")
	}
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now)
	if !state.HasReceivedBaseline() {
		t.Fatalf("RecordFullSyncSuccess must not clear baseline back to false")
	}
	state.RecordDeltaSendSuccess(&routing.AnnounceSnapshot{}, now)
	if !state.HasReceivedBaseline() {
		t.Fatalf("RecordDeltaSendSuccess must not clear baseline back to false")
	}
}

// The "wire baseline sent" flag is the symmetric send-side mirror of the
// receive-side baseline gate. The tests below pin its lifecycle: default
// false, single explicit writer (MarkWireBaselineSent), idempotent, and
// reset on every session boundary so a fresh session never inherits a
// prior session's wire baseline state. Without this contract the v2
// receive gate on the peer would drop the first delta of the new session
// and emit request_resync — see AnnounceLoop.announceToAllPeers for the
// gating override.

// TestAnnouncePeerState_WireBaselineSent_DefaultFalse pins the default:
// a freshly created state reports HasSentWireBaseline()==false on both the
// direct accessor and the View snapshot. Without this default, an empty
// connect-time baseline (which records a successful local baseline without
// emitting a wire frame) would leave the loop free to pick a v2 frame on
// the first non-empty delta and the peer would drop it.
func TestAnnouncePeerState_WireBaselineSent_DefaultFalse(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()

	state := registry.GetOrCreate("peer-A")
	if state.HasSentWireBaseline() {
		t.Fatalf("fresh AnnouncePeerState must have HasSentWireBaseline()==false")
	}
	if state.View().HasSentWireBaseline {
		t.Fatalf("fresh View must have HasSentWireBaseline==false")
	}
}

// TestAnnouncePeerState_MarkWireBaselineSent_FlipsFlag pins the only
// writer of the wire-baseline flag: MarkWireBaselineSent flips it to true
// and the View snapshot reflects the change atomically. Idempotent on
// subsequent calls.
func TestAnnouncePeerState_MarkWireBaselineSent_FlipsFlag(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")

	state.MarkWireBaselineSent()

	if !state.HasSentWireBaseline() {
		t.Fatalf("after MarkWireBaselineSent, HasSentWireBaseline() must be true")
	}
	if !state.View().HasSentWireBaseline {
		t.Fatalf("View().HasSentWireBaseline must reflect the flip")
	}

	// Idempotent: a second call must keep the flag set.
	state.MarkWireBaselineSent()
	if !state.HasSentWireBaseline() {
		t.Fatalf("MarkWireBaselineSent must be idempotent — flag must stay true")
	}
}

// TestAnnounceStateRegistry_MarkDisconnected_ResetsWireBaseline pins the
// session-boundary semantics for the send-side mirror: when a session is
// torn down, the wire-baseline flag resets so the next session's first
// delta cannot ride on the previous session's already-sent baseline. The
// receiver's session is also new on the peer side; without this reset the
// peer would drop the v2 frame and emit request_resync.
func TestAnnounceStateRegistry_MarkDisconnected_ResetsWireBaseline(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")
	state.MarkWireBaselineSent()
	if !state.HasSentWireBaseline() {
		t.Fatalf("precondition: flag must be true before disconnect")
	}

	registry.MarkDisconnected("peer-A")

	if state.HasSentWireBaseline() {
		t.Fatalf("MarkDisconnected must reset HasSentWireBaseline() to false")
	}
}

// TestAnnounceStateRegistry_MarkReconnected_ResetsWireBaseline_OnExistingState
// pins the idempotent reset path on the send side: MarkReconnected on a
// state that still holds a previous-session wire baseline must clear it,
// even if MarkDisconnected was never called (e.g. a teardown path that
// skipped the disconnect hook). Without this reset, the very first delta
// of the new session could pick the v2 wire frame against a peer that has
// not received any baseline frame in the new session.
func TestAnnounceStateRegistry_MarkReconnected_ResetsWireBaseline_OnExistingState(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")
	state.MarkWireBaselineSent()
	if !state.HasSentWireBaseline() {
		t.Fatalf("precondition: flag must be true before MarkReconnected")
	}

	registry.MarkReconnected("peer-A", []routing.PeerCapability{domain.CapMeshRoutingV1})

	if state.HasSentWireBaseline() {
		t.Fatalf("MarkReconnected must reset HasSentWireBaseline() to false on existing state")
	}
}

// TestAnnounceStateRegistry_MarkReconnected_FreshState_NoWireBaseline pins
// that MarkReconnected creating a brand-new state record never sets the
// wire-baseline flag. The view-side snapshot must agree with the direct
// accessor — both directions of the v2 baseline gate must start closed
// for a fresh session.
func TestAnnounceStateRegistry_MarkReconnected_FreshState_NoWireBaseline(t *testing.T) {
	registry := routing.NewAnnounceStateRegistry()

	registry.MarkReconnected("peer-A", []routing.PeerCapability{
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
	})

	state := registry.Get("peer-A")
	if state == nil {
		t.Fatalf("MarkReconnected must materialise the per-peer state")
	}
	if state.HasSentWireBaseline() {
		t.Fatalf("MarkReconnected on fresh state must leave HasSentWireBaseline() false")
	}
	if state.View().HasSentWireBaseline {
		t.Fatalf("View on fresh reconnected state must report HasSentWireBaseline==false")
	}
}

// TestAnnouncePeerState_WireBaseline_SurvivesRecordCalls pins that the
// regular send-side bookkeeping methods (RecordFullSyncSuccess,
// RecordFullSyncAttempt, RecordDeltaSendSuccess) do not flip the
// wire-baseline flag in either direction. The flag is owned exclusively
// by MarkWireBaselineSent and the session-boundary resets — a leak through
// any Record* method would entangle the wire-baseline gate with the
// snapshot-cache bookkeeping and is a design regression.
//
// Concretely: an empty-baseline branch calls RecordFullSyncSuccess WITHOUT
// having put a wire frame on the wire. If RecordFullSyncSuccess flipped
// the flag, the very class of bug this fix targets would silently
// reappear: the next delta would be eligible for v2 even though the peer
// never observed an announce_routes frame.
func TestAnnouncePeerState_WireBaseline_SurvivesRecordCalls(t *testing.T) {
	now := time.Now()
	registry := routing.NewAnnounceStateRegistry()
	state := registry.GetOrCreate("peer-A")

	// Default false; Record* calls must keep it that way.
	state.RecordFullSyncAttempt(now)
	if state.HasSentWireBaseline() {
		t.Fatalf("RecordFullSyncAttempt must not flip wire-baseline to true")
	}
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now)
	if state.HasSentWireBaseline() {
		t.Fatalf("RecordFullSyncSuccess must not flip wire-baseline to true (empty-baseline branch must leave it false)")
	}
	state.RecordDeltaSendSuccess(&routing.AnnounceSnapshot{}, now)
	if state.HasSentWireBaseline() {
		t.Fatalf("RecordDeltaSendSuccess must not flip wire-baseline to true on its own — the call site flips via MarkWireBaselineSent")
	}

	// Now flip the flag manually — Record* calls must keep it true.
	state.MarkWireBaselineSent()

	state.RecordFullSyncAttempt(now)
	if !state.HasSentWireBaseline() {
		t.Fatalf("RecordFullSyncAttempt must not clear wire-baseline back to false")
	}
	state.RecordFullSyncSuccess(&routing.AnnounceSnapshot{}, now)
	if !state.HasSentWireBaseline() {
		t.Fatalf("RecordFullSyncSuccess must not clear wire-baseline back to false")
	}
	state.RecordDeltaSendSuccess(&routing.AnnounceSnapshot{}, now)
	if !state.HasSentWireBaseline() {
		t.Fatalf("RecordDeltaSendSuccess must not clear wire-baseline back to false")
	}
}
