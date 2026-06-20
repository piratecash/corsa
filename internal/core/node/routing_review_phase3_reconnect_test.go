package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// routing_review_phase3_reconnect_test.go covers the Phase 3 review P1
// fix that makes the reconnect forced full sync actually suppressible by
// a digest match — the path the whole route_sync exchange exists for.
//
// The release-blocking gate behaviour (soft reconnect resync IS
// suppressed, hard request_resync is NOT) is asserted at the announce-
// loop level in the routing package
// (announce_resync_suppression_test.go). These node-level tests pin the
// two Service-side glue points the fix introduced:
//
//   - onPeerSessionEstablished arms a pending suppression window
//     BEFORE the reconnect TriggerUpdate fires, so the immediate cycle
//     holds off the full sync until the summary can land.
//   - handleRouteSyncSummary clears any active suppression on a
//     mismatch so the next cycle full-syncs promptly.

// TestOnPeerSessionEstablished_ReconnectArmsPendingSuppression — when a
// relay+routesync-capable peer reconnects and a cached digest exists,
// the hook must arm the pending suppression window. Without it the
// reconnect's own TriggerUpdate would full-sync the peer before the
// digest round-trip could complete.
func TestOnPeerSessionEstablished_ReconnectArmsPendingSuppression(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)

	// Seed a cached per-peer digest so ConsumePeerDigestSnapshot returns
	// ok=true inside the hook — the precondition for emitting a digest
	// and arming the pending window on reconnect.
	now := time.Now().UTC()
	svc.routingTable.RecordPeerDigestSnapshot(idPeerA, "deadbeef", nil, 3, now)

	svc.onPeerSessionEstablished(idPeerA, []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRouteSyncV1,
	})

	if !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("reconnect did not arm pending digest suppression; the immediate TriggerUpdate would full-sync ahead of the round-trip")
	}
}

// TestOnPeerSessionEstablished_NoCachedDigestArmsNothing — without a
// cached digest there is no prior view to compare against, so the hook
// must NOT arm suppression: the reconnect full sync is the correct
// behaviour.
func TestOnPeerSessionEstablished_NoCachedDigestArmsNothing(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)

	svc.onPeerSessionEstablished(idPeerA, []domain.Capability{
		domain.CapMeshRelayV1,
		domain.CapMeshRoutingV1,
		domain.CapMeshRouteSyncV1,
	})

	if svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("reconnect armed suppression with no cached digest; full sync must proceed")
	}
}

// TestHandleRouteSyncSummary_MismatchClearsPendingSuppression — a
// pending window armed on reconnect must be cleared by a Match=false
// summary so the next announce cycle full-syncs promptly instead of
// waiting out the grace.
func TestHandleRouteSyncSummary_MismatchClearsPendingSuppression(t *testing.T) {
	svc := newTestServiceWithRoutingAndHealth(t, idNodeB)

	// Arm a pending window directly, mirroring what the reconnect hook
	// does, then deliver a mismatch summary.
	svc.announceLoop.MarkPeerDigestPending(idPeerA, time.Now().UTC(), "deadbeef")
	if !svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("precondition: pending suppression must be active")
	}

	summary := protocol.RouteSyncSummaryFrame{
		Type:           protocol.RouteSyncSummaryFrameType,
		Digest:         "deadbeef",
		Match:          false,
		ExpectFullSync: true,
	}
	svc.handleRouteSyncSummary(idPeerA, summary)

	if svc.announceLoop.IsDigestSuppressionActiveForTest(idPeerA, time.Now().UTC()) {
		t.Fatal("mismatch summary did not clear pending suppression")
	}
}
