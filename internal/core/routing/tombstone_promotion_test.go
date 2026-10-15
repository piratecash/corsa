package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// Tombstone → live promotion at a newer SeqNo goes through cap-aware
// admission: route_store_mutation.go detaches the tombstone, calls
// AdmitNew with the candidate live claim and restores the tombstone
// on rejection. The K-live cap itself is pinned by
// TestRIBCap_TombstoneToLivePromotionRespectsKLiveCap; this file pins
// the restore half — a rejected promotion leaves the tombstone exactly
// as it was, so it neither loses its SeqNo guard nor blocks a later
// legitimate admission.

// K=1 with an un-evictable direct claim in the only live slot makes
// AdmitNew reject the promotion with AdmissionRejectedAllProtected.
// The tombstone must come back at its own SeqNo and still withdrawn,
// and once the direct claim leaves, the same candidate is admitted.
func TestTombstonePromotion_CapRejectionRestoresTombstoneAndAllowsRetry(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		dest = domaintest.ID("dest")
		uX   = domaintest.ID("u-X")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(domaintest.ID("node-A")),
		WithMaxNextHopsPerOrigin(1),
	)

	if _, err := tbl.AddDirectPeer(dest); err != nil {
		t.Fatalf("AddDirectPeer(dest): %v", err)
	}
	if !tbl.WithdrawRoute(dest, uX, 10) {
		t.Fatalf("WithdrawRoute must succeed to seed the tombstone")
	}

	candidate := RouteEntry{
		Identity: dest, Origin: uX, NextHop: uX,
		Hops: 4, SeqNo: 15, Source: RouteSourceAnnouncement,
	}
	if status := mustUpdate(t, tbl, candidate); status != RouteRejected {
		t.Fatalf("expected RouteRejected from the RejectedAllProtected cap path, got %v", status)
	}
	assertSlot(t, tbl, dest, uX, 10, true)

	if _, err := tbl.RemoveDirectPeer(dest); err != nil {
		t.Fatalf("RemoveDirectPeer(dest): %v", err)
	}

	if status := mustUpdate(t, tbl, candidate); status != RouteAccepted {
		t.Fatalf("retry after the live slot freed up must be accepted, got %v", status)
	}
	assertSlot(t, tbl, dest, uX, 15, false)
}

// assertSlot fails the test unless the (identity, uplink) slot holds
// a claim at seqNo with the given withdrawn state.
func assertSlot(t *testing.T, tbl *Table, identity, uplink PeerIdentity, seqNo uint64, withdrawn bool) {
	t.Helper()
	stored := tbl.InspectUplinkClaim(identity, uplink)
	if stored == nil {
		t.Fatalf("slot (%s, %s) is empty", identity, uplink)
	}
	if stored.SeqNo != seqNo || stored.IsWithdrawn() != withdrawn {
		t.Fatalf("slot (%s, %s) = {SeqNo:%d Withdrawn:%v}, want {SeqNo:%d Withdrawn:%v}",
			identity, uplink, stored.SeqNo, stored.IsWithdrawn(), seqNo, withdrawn)
	}
}
