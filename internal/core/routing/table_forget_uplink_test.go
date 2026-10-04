package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// table_forget_uplink_test.go pins ForgetUplink — the local, wire-silent purge
// of what an unproven connection may have written about an uplink before the
// identity proved itself over v2 (docs/refactoring/n1-legacy-residual.md §2).
// Unlike InvalidateTransitRoutes it leaves NO tombstone: a tombstone with an
// impostor's SeqNo is exactly the residue that would refuse the real
// identity's own announcements.

func TestForgetUplinkLeavesNoResidueAgainstTheRealUplink(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")), WithClock(fixedClock(now)))
	x, other := domaintest.ID("uplink-x"), domaintest.ID("uplink-other")
	target, withdrawn := domaintest.ID("target"), domaintest.ID("withdrawn")

	if _, err := tbl.AddDirectPeer(x); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	upsertClaim(t, tbl, target, x, 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, target, other, 3, RouteSourceAnnouncement)
	upsertClaim(t, tbl, withdrawn, x, 2, RouteSourceAnnouncement)
	if !tbl.WithdrawRoute(withdrawn, x, x, 1<<40) {
		t.Fatal("precondition: the withdrawal tombstoned the claim")
	}
	for i := 0; i < BlackHoleThreshold; i++ {
		tbl.MarkHopFailure(target, x)
	}

	if removed := tbl.ForgetUplink(x); removed != 2 {
		t.Fatalf("ForgetUplink removed %d claims, want 2 (the transit claim and the tombstone)", removed)
	}
	if got := tbl.InspectTriple(RouteTriple{Identity: withdrawn, Origin: withdrawn, NextHop: x}); got != nil {
		t.Fatalf("the tombstone survived: %+v", got)
	}
	routes := tbl.Lookup(target)
	if len(routes) != 1 || routes[0].NextHop != other {
		t.Fatalf("Lookup(target) = %+v, want only the claim via the other uplink", routes)
	}
	if direct := tbl.Lookup(x); len(direct) != 1 || direct[0].Source != RouteSourceDirect {
		t.Fatalf("the live direct route to the uplink itself must stay, got %+v", direct)
	}

	// The real uplink's own announcement at a LOW SeqNo is accepted again:
	// nothing the forgotten claims said stands in its way.
	upsertClaim(t, tbl, withdrawn, x, 2, RouteSourceAnnouncement)
	upsertClaim(t, tbl, target, x, 2, RouteSourceAnnouncement)
	if routes := tbl.Lookup(target); len(routes) != 2 {
		t.Fatalf("the relearned claim via x is still cooled down: Lookup(target) = %+v", routes)
	}
	if got := tbl.Lookup(withdrawn); len(got) != 1 {
		t.Fatalf("a low-SeqNo announcement after the purge was refused: %+v", got)
	}
}
