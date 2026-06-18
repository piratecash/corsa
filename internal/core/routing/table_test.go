package routing

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// fixedClock returns a clock function that always returns the given time.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// mustUpdate calls UpdateRoute and fails the test on error, returns status.
func mustUpdate(t *testing.T, tbl *Table, entry RouteEntry) RouteUpdateStatus {
	t.Helper()
	status, err := tbl.UpdateRoute(entry)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	return status
}

// mustAddDirect calls AddDirectPeer and fails the test on error.
func mustAddDirect(t *testing.T, tbl *Table, peerID PeerIdentity) RouteEntry {
	t.Helper()
	result, err := tbl.AddDirectPeer(peerID)
	if err != nil {
		t.Fatalf("AddDirectPeer(%q) unexpected error: %v", peerID, err)
	}
	return result.Entry
}

// mustRemoveDirect calls RemoveDirectPeer and fails the test on error.
func mustRemoveDirect(t *testing.T, tbl *Table, peerID PeerIdentity) RemoveDirectPeerResult {
	t.Helper()
	result, err := tbl.RemoveDirectPeer(peerID)
	if err != nil {
		t.Fatalf("RemoveDirectPeer(%q) unexpected error: %v", peerID, err)
	}
	return result
}

// --- Insertion & deduplication ---

func TestUpdateRouteInsertsNew(t *testing.T) {
	tbl := NewTable()

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("new route should be accepted")
	}
	if tbl.Size() != 1 {
		t.Fatalf("expected size=1, got %d", tbl.Size())
	}
}

func TestUpdateRouteDedupByTriple(t *testing.T) {
	tbl := NewTable()

	entry := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	}
	mustUpdate(t, tbl, entry)

	entry.SeqNo = 2
	entry.Hops = 2
	mustUpdate(t, tbl, entry)

	if tbl.Size() != 1 {
		t.Fatalf("same triple should dedup: size=%d", tbl.Size())
	}
}

// TestUpdateRouteDifferentUplinksCoexist documents the post-Phase-A
// dedup key: claims with the same Identity but different Uplinks
// (the relaying direct peer, was previously RouteEntry.NextHop) live
// side-by-side in the bucket. Origin no longer participates in
// dedup — it is consumed for anti-spoof on ingest and dropped.
//
// The pre-Phase-A version of this test pinned that two entries with
// the same NextHop but different Origins coexist (per-triple dedup).
// That invariant is intentionally gone: per the field-data analysis
// (12.7 origin lineages per identity, all routing-equivalent), Origin
// was redundant.
func TestUpdateRouteDifferentUplinksCoexist(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("alice"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("alice"), NextHop: domaintest.ID("dave"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if tbl.Size() != 2 {
		t.Fatalf("different uplinks should coexist: size=%d", tbl.Size())
	}
}

// --- Validation ---

func TestUpdateRouteRejectsEmptyIdentity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domain.PeerIdentity{}, Origin: domaintest.ID("bob"), NextHop: domaintest.ID("bob"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrEmptyIdentity {
		t.Fatalf("expected ErrEmptyIdentity, got %v", err)
	}
}

func TestUpdateRouteRejectsEmptyOrigin(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domain.PeerIdentity{}, NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrEmptyOrigin {
		t.Fatalf("expected ErrEmptyOrigin, got %v", err)
	}
}

func TestUpdateRouteRejectsEmptyNextHop(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domain.PeerIdentity{},
		Hops: 1, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrEmptyNextHop {
		t.Fatalf("expected ErrEmptyNextHop, got %v", err)
	}
}

func TestUpdateRouteRejectsZeroHops(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 0, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrInvalidHops {
		t.Fatalf("expected ErrInvalidHops for hops=0, got %v", err)
	}
}

func TestUpdateRouteRejectsHopsAboveInfinity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 17, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrInvalidHops {
		t.Fatalf("expected ErrInvalidHops for hops=17, got %v", err)
	}
}

func TestUpdateRouteAcceptsHopsInfinity(t *testing.T) {
	tbl := NewTable()
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("hops=16 (infinity/withdrawal) should be accepted")
	}
}

func TestMalformedEntryDoesNotPollute(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 0, SeqNo: 99, Source: RouteSourceAnnouncement,
	})
	if err == nil {
		t.Fatal("expected validation error for hops=0")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].SeqNo != 1 {
		t.Fatal("malformed entry with higher SeqNo should not replace valid entry")
	}
}

// --- Direct route validation ---

func TestDirectRouteRequiresHops1(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 2, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrDirectHopsMust1 {
		t.Fatalf("expected ErrDirectHopsMust1, got %v", err)
	}
}

func TestDirectRouteRequiresNextHopEqualsIdentity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrDirectNextHop {
		t.Fatalf("expected ErrDirectNextHop, got %v", err)
	}
}

func TestDirectRouteValidForm(t *testing.T) {
	tbl := NewTable()
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if status != RouteAccepted {
		t.Fatal("well-formed direct route should be accepted")
	}
}

// --- SeqNo comparison invariants ---

func TestOriginAwareSeqNoHigherWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("higher SeqNo should be accepted even with worse hops")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].SeqNo != 10 {
		t.Fatalf("expected SeqNo=10, got %+v", routes)
	}
}

func TestOriginAwareSeqNoLowerRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status == RouteAccepted {
		t.Fatal("lower SeqNo should be rejected")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].SeqNo != 10 {
		t.Fatalf("original route should remain: %+v", routes)
	}
}

// TestUpdateRoute_CrossOriginLineageShiftBetterHops reproduces the
// round-5k mixed-version ingest fix: a pre-A1 sender whose
// per-Identity winner shifts between Origin lineages across cycles
// may emit a wire frame with a numerically lower SeqNo (pre-A1
// SeqNos were per-Origin) but strictly better hops. Post-A1
// receiver compares SeqNo per-(Identity, Uplink) and would naively
// reject as stale; LastIngressOrigin detects the lineage shift and
// admits the new lineage when it strictly improves on the stored
// claim.
func TestUpdateRoute_CrossOriginLineageShiftBetterHops(t *testing.T) {
	tbl := NewTable()

	// Cycle 1 (pre-A1 sender's winner via Origin=A, hops=3, seq=10).
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Cycle 2: sender's winner shifted to Origin=B's lineage which
	// has better hops but a lower SeqNo in its own per-Origin
	// counter. Without the cross-Origin rule this would be rejected
	// as stale (SeqNo=5 < 10).
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatalf("cross-Origin lineage shift with better hops must be accepted, got status=%v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected single stored claim post-replace, got %d", len(routes))
	}
	if routes[0].Hops != 2 {
		t.Fatalf("expected the new better-hops claim (Hops=2) to win, got Hops=%d", routes[0].Hops)
	}
	if routes[0].SeqNo != 5 {
		t.Fatalf("expected per-uplink SeqNo namespace to reset to the new lineage's value (5), got %d", routes[0].SeqNo)
	}
}

// TestUpdateRoute_CrossOriginLineageShiftSameHopsRejected pins the
// flap-prevention side of the cross-Origin rule: when a pre-A1
// sender emits a different-Origin update with the same hops and the
// same trust rank, we reject to avoid SeqNo-namespace churn from
// equivalent lineage oscillations.
func TestUpdateRoute_CrossOriginLineageShiftSameHopsRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status == RouteAccepted {
		t.Fatal("cross-Origin with same hops + same trust should be rejected (anti-flap)")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].SeqNo != 10 {
		t.Fatalf("original lineage should remain: %+v", routes)
	}
}

// TestUpdateRoute_CrossOriginResurrectsAfterTombstone covers the
// most damaging mixed-version scenario: pre-A1 sender first
// withdraws Origin=A's lineage (creating a tombstone in our
// per-uplink storage), then sends a live update for Origin=B (with
// a lower SeqNo, since pre-A1 SeqNos are per-Origin). Without the
// cross-Origin rule the new lineage would be rejected by the
// tombstone-resurrection guard, and we would lose a working route
// to peer-X until the tombstone's TTL elapsed.
func TestUpdateRoute_CrossOriginResurrectsAfterTombstone(t *testing.T) {
	tbl := NewTable()

	// Live Origin=A claim.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Pre-A1 sender withdraws Origin=A (SeqNo bump on same lineage).
	if !tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("origA"), domaintest.ID("preA1-sender"), 11) {
		t.Fatal("WithdrawRoute(Origin=A) should have applied")
	}

	// Live Origin=B claim from same uplink, lower SeqNo. Without
	// the cross-Origin rule the tombstone (SeqNo=11) would reject
	// this (incoming SeqNo=5 < 11).
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 4, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatalf("cross-Origin live update against tombstone must be accepted, got status=%v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected the live Origin=B claim to replace the tombstone, got %d routes", len(routes))
	}
	if routes[0].Hops != 4 {
		t.Fatalf("expected the new claim's Hops=4, got %d", routes[0].Hops)
	}
}

// TestUpdateRoute_SameSeqNoCrossOriginAdvancesLineageMarker pins the
// LastIngressOrigin advance in the same-SeqNo path: an arrival from
// a new Origin at the SAME SeqNo (and same hops, so no replace
// branch fires) must still update the sticky lineage marker.
// Without the advance, a subsequent stale-SeqNo replay from that
// new Origin would trip the cross-Origin lineage branch as if it
// were a fresh lineage transition we had not yet observed and
// could be admitted on the "improves" criterion (e.g. better hops),
// destabilising the stored claim from what is in reality a stale
// replay on the same Origin's timeline.
func TestUpdateRoute_SameSeqNoCrossOriginAdvancesLineageMarker(t *testing.T) {
	tbl := NewTable()

	// Cycle 1: pre-A1 sender's winner via Origin=A.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Cycle 2: same uplink emits a frame with a different Origin
	// at the same SeqNo + same hops. The replace branches do not
	// fire (no trust uplift, no hops improvement), but the
	// lineage marker must move to origB so the next stale-SeqNo
	// replay from origB is not misclassified as a fresh cross-
	// Origin shift.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if status == RouteRejected {
		t.Fatalf("same-SeqNo same-hops re-announce should be reconfirmation, got %v", status)
	}

	// Cycle 3: stale-SeqNo replay from origB with better hops.
	// Without the advance in cycle 2, this would trip the cross-
	// Origin lineage branch (origB != stale LastIngressOrigin=origA)
	// and admit Hops=1 / SeqNo=8 over the stored Hops=3 / SeqNo=10.
	// With the advance, LastIngressOrigin=origB so cross-Origin
	// does not fire and the stale frame is rejected.
	status = mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 1, SeqNo: 8, Source: RouteSourceAnnouncement,
	})
	if status == RouteAccepted {
		t.Fatalf("stale-SeqNo replay from already-observed origB must be rejected, got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected one stored claim, got %d", len(routes))
	}
	if routes[0].Hops != 3 || routes[0].SeqNo != 10 {
		t.Fatalf("stored claim should be unchanged after stale replay: Hops=%d SeqNo=%d", routes[0].Hops, routes[0].SeqNo)
	}
}

// TestUpdateRoute_PerOriginHighWaterRejectsCrossOriginStaleReplay
// is the reviewer-flagged regression for the per-Origin high-water
// map (UplinkClaim.SeenOriginSeqs): A10 → B5 → A9. The genuine
// winner shift A→B is accepted via the cross-Origin lineage
// branch (B5 has strictly better hops), but the subsequent
// delayed in-flight A9 must be rejected — A has already been
// observed at SeqNo=10 on this uplink, so a SeqNo=9 frame from A
// is a stale replay on A's own timeline regardless of the current
// active lineage being B.
//
// Without the per-Origin high-water map, A9 lands in the
// `SeqNo > old.SeqNo` branch (9 > current B's 5) and overwrites
// the legitimate B5 lineage with stale A state.
func TestUpdateRoute_PerOriginHighWaterRejectsCrossOriginStaleReplay(t *testing.T) {
	tbl := NewTable()

	// Cycle 1: A's lineage at SeqNo=10, Hops=3.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Cycle 2: B's lineage at SeqNo=5, Hops=2 — winner shift A→B
	// accepted because of strictly better hops via cross-Origin
	// lineage branch.
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteAccepted {
		t.Fatalf("B5 cross-Origin lineage shift with better hops must be accepted, got %v", status)
	}

	// Cycle 3: delayed in-flight A9 from BEFORE the winner shift.
	// A's high-water on this uplink is 10 (recorded in cycle 1);
	// SeqNo=9 is stale on A's own timeline. Must be rejected.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 1, SeqNo: 9, Source: RouteSourceAnnouncement,
	})
	if status == RouteAccepted {
		t.Fatalf("A9 must be rejected as stale replay on A's high-water (10), got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected one stored claim, got %d", len(routes))
	}
	if routes[0].Hops != 2 || routes[0].SeqNo != 5 {
		t.Fatalf("stored claim must remain B5/Hops=2 after stale A9 replay: Hops=%d SeqNo=%d", routes[0].Hops, routes[0].SeqNo)
	}
}

// TestUpdateRoute_CrossOriginLivePromotesTombstoneAtEqualSeqNo
// pins the symmetric admission shape for the equal-SeqNo cross-
// Origin live-vs-tombstone case. Pre-A1 senders can shift their
// per-Identity winner from a now-withdrawn Origin (A) to a
// different LIVE Origin (B) while B's per-Origin counter
// coincidentally matches A's withdrawal SeqNo. The stale-SeqNo
// and newer-SeqNo branches already promote cross-Origin live
// against a tombstone (the round-5k cross-Origin lineage branch
// with old.IsWithdrawn → improves; the SeqNo > old tombstone-
// promotion path); equal-SeqNo must behave the same — the
// receiver otherwise stays on the tombstone until TTL, despite
// us having a perfectly good live alternative on the same
// uplink. Same-Origin equal-SeqNo against a tombstone IS still
// rejected (the hop_ack-resurrection guard) — only cross-Origin
// is admitted by the new branch.
func TestUpdateRoute_CrossOriginLivePromotesTombstoneAtEqualSeqNo(t *testing.T) {
	tbl := NewTable()

	// Withdraw Origin=A at SeqNo=10 to create a tombstone on
	// (alice, preA1-sender).
	if !tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("origA"), domaintest.ID("preA1-sender"), 10) {
		t.Fatal("WithdrawRoute(Origin=A, SeqNo=10) must apply")
	}

	// Cross-Origin live update from Origin=B at the SAME SeqNo.
	// Without the equal-SeqNo cross-Origin live-vs-tombstone
	// promotion branch this would land in the same-SeqNo
	// tombstone guard and reject.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("cross-Origin live at equal-SeqNo against tombstone must be accepted, got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected one live claim post-promotion, got %d", len(routes))
	}
	if routes[0].Hops != 3 || routes[0].SeqNo != 10 {
		t.Fatalf("promoted claim must be the B/Hops=3/SeqNo=10 input, got Hops=%d SeqNo=%d", routes[0].Hops, routes[0].SeqNo)
	}
}

// TestUpdateRoute_SameOriginEqualSeqNoTombstoneStillRejected pins
// the asymmetry of the equal-SeqNo cross-Origin promotion: the
// same-Origin tombstone-resurrection guard still protects against
// hop_ack-style replays from the lineage we just withdrew. Only
// cross-Origin equal-SeqNo arrivals are admitted by the new branch.
func TestUpdateRoute_SameOriginEqualSeqNoTombstoneStillRejected(t *testing.T) {
	tbl := NewTable()

	if !tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("origA"), domaintest.ID("preA1-sender"), 10) {
		t.Fatal("WithdrawRoute(Origin=A, SeqNo=10) must apply")
	}

	// Same Origin, same SeqNo, live shape — must be rejected by
	// the tombstone-resurrection guard.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if status == RouteAccepted {
		t.Fatalf("same-Origin live at equal-SeqNo against tombstone must be rejected, got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	// The lookup filters withdrawn claims out — the tombstone is in
	// the bucket but not visible via Lookup. Asserting the lookup
	// shape (no live claims) verifies the tombstone has not been
	// resurrected; an accepted promotion would have surfaced a live
	// row here.
	if len(routes) != 0 {
		t.Fatalf("tombstone must not be resurrected by same-Origin equal-SeqNo replay: got %d live routes", len(routes))
	}
}

// TestUpdateRoute_HopAckPromotionPreservesWireLineage pins the bug
// fix for the lineage-corruption hazard in confirmRouteViaHopAck's
// trust-uplift path. Without the fix, the synthetic Origin=
// localOrigin coming back from Lookup → toRouteEntry would replace
// the stored LastIngressOrigin and seed SeenOriginSeqs[localOrigin]
// at the existing SeqNo. The next forced-full-sync re-announce
// from the real upstream Origin would then trip the cross-Origin
// per-Origin high-water guard (incoming Origin != stored
// localOrigin; SeenOriginSeqs[real-origin] already at this SeqNo
// from the original announce) and be rejected before reaching the
// TTL-refresh path. Confirmed routes would age out by TTL despite
// being actively re-announced.
//
// Flow:
//
//   - peer-B announces alice via uplink=peer-B with Origin=foo,
//     SeqNo=10 (the real wire-frame Origin).
//   - confirmRouteViaHopAck promotes the route to Source=HopAck;
//     internally the entry's Origin field is the synthetic
//     localOrigin from Lookup, but the lineage-override at the top
//     of ApplyUpdate preserves the wire-observed lineage on the
//     stored claim.
//   - peer-B re-announces alice at SeqNo=10 (forced-full-sync TTL
//     refresh path; the per-peer delta is empty but the cadence
//     forces a full snapshot).
//   - Expectation: re-announce is NOT rejected by the cross-Origin
//     guard; the same-SeqNo reconfirmation path refreshes TTL.
func TestUpdateRoute_HopAckPromotionPreservesWireLineage(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("local-node")))

	// 1. Initial wire announce from peer-B claiming Origin=foo.
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foo"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	}); status != RouteAccepted {
		t.Fatalf("initial announce: expected RouteAccepted, got %v", status)
	}

	// 2. Simulate confirmRouteViaHopAck's promotion: reads the
	//    existing route via Lookup (which synthesises Origin=
	//    localOrigin) and submits with Source=HopAck.
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("local-node"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceHopAck,
	}); status != RouteAccepted {
		t.Fatalf("hop_ack promotion: expected RouteAccepted, got %v", status)
	}

	// 3. peer-B re-announces same SeqNo (forced-full-sync TTL
	//    refresh). Without the lineage-override fix this is
	//    rejected by the cross-Origin high-water guard.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foo"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if status == RouteRejected {
		t.Fatalf("forced-full-sync re-announce after hop_ack must NOT be rejected by cross-Origin guard, got %v", status)
	}

	// The route is still reachable and unchanged.
	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected one live claim post-cycle, got %d", len(routes))
	}
	if routes[0].SeqNo != 10 || routes[0].Hops != 3 {
		t.Fatalf("stored claim shape unexpectedly changed: Hops=%d SeqNo=%d", routes[0].Hops, routes[0].SeqNo)
	}
	if routes[0].Source != RouteSourceHopAck {
		t.Fatalf("stored Source must remain HopAck (promotion sticks), got %v", routes[0].Source)
	}
}

// TestUpdateRoute_HopAckPromotedRouteRefreshesTTL pins the
// reconfirmation-gate weakening: after a HopAck promotion the
// stored Source diverges from the wire-frame Source of subsequent
// re-announces (HopAck in storage vs Announcement on the wire).
// The TTL-refresh gate must accept same-SeqNo + same-Hops
// re-announces regardless of Source mismatch, otherwise confirmed
// routes age out by TTL even though forced-full-sync delivers
// re-announces correctly.
//
// Uses fake clock to advance time deterministically and inspect
// ExpiresAt before/after the re-announce.
func TestUpdateRoute_HopAckPromotedRouteRefreshesTTL(t *testing.T) {
	start := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)
	now := start
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("local-node")),
		WithClock(func() time.Time { return now }),
		WithDefaultTTL(120*time.Second),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foo"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Promote to HopAck.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("local-node"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceHopAck,
	})

	// Capture the post-promotion ExpiresAt.
	preRoutes := tbl.Lookup(domaintest.ID("alice"))
	if len(preRoutes) != 1 {
		t.Fatalf("expected one claim post-promotion, got %d", len(preRoutes))
	}
	preExpiresAt := preRoutes[0].ExpiresAt

	// Advance clock by 60s and re-announce (forced-full-sync
	// reconfirmation cycle).
	now = start.Add(60 * time.Second)
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foo"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	postRoutes := tbl.Lookup(domaintest.ID("alice"))
	if len(postRoutes) != 1 {
		t.Fatalf("expected one claim post-refresh, got %d", len(postRoutes))
	}
	postExpiresAt := postRoutes[0].ExpiresAt
	if !postExpiresAt.After(preExpiresAt) {
		t.Fatalf("forced-full-sync re-announce after hop_ack must refresh ExpiresAt: pre=%v post=%v", preExpiresAt, postExpiresAt)
	}
	expectedExpiresAt := now.Add(120 * time.Second)
	if !postExpiresAt.Equal(expectedExpiresAt) {
		t.Fatalf("ExpiresAt should equal now + defaultTTL: got %v expected %v", postExpiresAt, expectedExpiresAt)
	}
}

// TestUpdateRoute_PerOriginHighWaterAllowsForwardShift complements
// the stale-replay rejection test: a strictly newer SeqNo from a
// previously-observed Origin IS admitted (it is a genuine winner
// shift back to that Origin, not a replay). A10 → B5 → A11 should
// accept A11 as the new active lineage.
func TestUpdateRoute_PerOriginHighWaterAllowsForwardShift(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origB"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteAccepted {
		t.Fatalf("B5 setup: expected RouteAccepted, got %v", status)
	}

	// A11 strictly exceeds A's recorded high-water (10).
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origA"), NextHop: domaintest.ID("preA1-sender"),
		Hops: 4, SeqNo: 11, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("A11 forward-shift past A's high-water must be accepted, got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected one stored claim, got %d", len(routes))
	}
	if routes[0].SeqNo != 11 || routes[0].Hops != 4 {
		t.Fatalf("expected A11/Hops=4 as new active lineage, got Hops=%d SeqNo=%d", routes[0].Hops, routes[0].SeqNo)
	}
}

// TestSeqNoComparisonScopedToUplink documents the post-Phase-A
// per-(Identity, Uplink) SeqNo space: a low-SeqNo claim from one
// uplink does NOT collide with a high-SeqNo claim from a different
// uplink for the same Identity. Each uplink maintains its own
// SeqNo timeline.
//
// The pre-Phase-A version of this test pinned per-(Identity, Origin)
// SeqNo space — that invariant is intentionally gone (Origin was
// dropped from the dedup key; see uplink_claim.go's type-level
// doc-comment for the field-data motivation).
func TestSeqNoComparisonScopedToUplink(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("alice"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Different Uplink (NextHop=dave) → independent SeqNo space.
	// Same NextHop with SeqNo=1 would be rejected as stale; from a
	// fresh uplink, SeqNo=1 is the first observation and accepted.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("alice"), NextHop: domaintest.ID("dave"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("different uplink should have independent SeqNo space")
	}
	if tbl.Size() != 2 {
		t.Fatalf("expected 2 routes, got %d", tbl.Size())
	}
}

func TestSameSeqNoHigherTrustWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceHopAck,
	})

	if status != RouteAccepted {
		t.Fatal("same SeqNo but higher trust should be accepted")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if routes[0].Source != RouteSourceHopAck {
		t.Fatal("hop_ack should replace announcement at same SeqNo")
	}
}

func TestSameSeqNoLowerTrustRejected(t *testing.T) {
	tbl := NewTable()

	// Use a well-formed direct route: NextHop must equal Identity
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 5, Source: RouteSourceDirect,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status == RouteAccepted {
		t.Fatal("same SeqNo but lower trust should be rejected")
	}
}

func TestSameSeqNoSameTrustFewerHopsWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("same SeqNo and trust, fewer hops should be accepted")
	}
}

// --- RouteUnchanged status ---

func TestUpdateRouteUnchangedForAliveReconfirmation(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Insert initial route.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("initial route should be accepted")
	}

	// Re-announce the same route (same SeqNo, same trust, same hops).
	status = mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteUnchanged {
		t.Fatalf("same alive route re-announced should return RouteUnchanged, got %d", status)
	}
}

func TestUpdateRouteUnchangedNotReturnedForExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)), WithDefaultTTL(10*time.Second))

	// Insert route with short TTL.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// Advance clock past expiry.
	expired := now.Add(20 * time.Second)
	tbl.mu.Lock()
	tbl.clock = fixedClock(expired)
	tbl.mu.Unlock()

	// Same route re-announced after expiry — accepted as valid refresh.
	// The origin has not changed the route, it is simply re-confirming it.
	// An expired entry should not block same-SeqNo refreshes.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("expired route same-SeqNo reannounce should be RouteAccepted, got %d", status)
	}
}

// TestUpdateRouteRefreshesTTLOnUnchangedLiveRoute is a regression guard for the
// "learned routes silently expire between forced full syncs" bug. A periodic
// re-announcement of an unchanged route (same SeqNo, source, hops) that
// arrives BEFORE the existing entry expires must extend ExpiresAt to
// now+defaultTTL — otherwise the route ages out at its original deadline even
// though the origin keeps confirming it. The status remains RouteUnchanged
// because no field that callers use for delta/comparison has changed; only
// the lifetime is renewed.
func TestUpdateRouteRefreshesTTLOnUnchangedLiveRoute(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	key := RouteTriple{Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie")}
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: key.Identity, Origin: key.Origin, NextHop: key.NextHop,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteAccepted {
		t.Fatalf("initial insert: expected RouteAccepted, got %d", status)
	}

	initial := tbl.InspectTriple(key)
	if initial == nil {
		t.Fatalf("inserted route not found")
	}
	initialExpiry := initial.ExpiresAt

	// Advance clock by half the TTL — well within the alive window.
	current = current.Add(60 * time.Second)

	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: key.Identity, Origin: key.Origin, NextHop: key.NextHop,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteUnchanged {
		t.Fatalf("re-announce of alive route: expected RouteUnchanged, got %d", status)
	}

	refreshed := tbl.InspectTriple(key)
	if refreshed == nil {
		t.Fatalf("route disappeared after RouteUnchanged refresh")
	}
	expectedExpiry := current.Add(120 * time.Second)
	if !refreshed.ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("RouteUnchanged must refresh TTL: got ExpiresAt=%s, want %s (initial=%s, advanced 60s)",
			refreshed.ExpiresAt, expectedExpiry, initialExpiry)
	}
}

// TestUpdateRouteDirectInsertPreservesZeroExpiry guards the direct route
// lifecycle invariant at the UpdateRoute primary-normalization step: any
// caller (state restore, defensive write, the existing Direct-via-UpdateRoute
// tests) that supplies a RouteSourceDirect entry with ExpiresAt=zero must
// land in the table with ExpiresAt still zero. The default TTL bump that
// applies to learned routes (now+defaultTTL when ExpiresAt.IsZero()) would
// otherwise convert the entry into a 120s-ageing one and TickTTL would
// evict it while the underlying socket is still alive — exactly the class
// of races the AddDirectPeer/RemoveDirectPeer event-driven design avoids.
// A non-zero ExpiresAt supplied by a caller is also forced back to zero:
// the invariant is hard, not advisory, so no caller can accidentally pin
// a finite TTL onto a direct entry.
func TestUpdateRouteDirectInsertPreservesZeroExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithClock(func() time.Time { return current }),
		WithLocalOrigin(domaintest.ID("me")),
	)

	// Caller supplies the canonical zero ExpiresAt. Normalization must
	// leave it zero — anything else lets TickTTL evict an alive direct.
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	}); status != RouteAccepted {
		t.Fatalf("initial direct insert: expected RouteAccepted, got %d", status)
	}
	zeroIn := tbl.InspectTriple(RouteTriple{Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice")})
	if zeroIn == nil {
		t.Fatalf("direct route not found after insert")
	}
	if !zeroIn.ExpiresAt.IsZero() {
		t.Fatalf("zero-ExpiresAt direct insert must remain zero, got %s", zeroIn.ExpiresAt)
	}

	// Caller supplies a non-zero ExpiresAt. The invariant is hard — direct
	// routes are never TTL-managed — so the table must force ExpiresAt
	// back to zero rather than honour whatever the caller passed.
	current = current.Add(time.Hour)
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: current.Add(time.Hour),
	}); status != RouteAccepted {
		t.Fatalf("direct insert with non-zero ExpiresAt: expected RouteAccepted, got %d", status)
	}
	nonZeroIn := tbl.InspectTriple(RouteTriple{Identity: domaintest.ID("bob"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob")})
	if nonZeroIn == nil {
		t.Fatalf("direct route 'bob' not found after insert")
	}
	if !nonZeroIn.ExpiresAt.IsZero() {
		t.Fatalf("caller-supplied non-zero ExpiresAt must be forced to zero on direct insert, got %s",
			nonZeroIn.ExpiresAt)
	}

	// TickTTL must not evict an active direct route no matter how far the
	// clock has advanced — that is the whole point of ExpiresAt=zero.
	current = current.Add(10 * time.Hour)
	tbl.TickTTL()
	if got := tbl.InspectTriple(RouteTriple{Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice")}); got == nil {
		t.Fatalf("TickTTL evicted active direct route — invariant ExpiresAt=zero failed")
	}
}

// TestUpdateRouteUnchangedDirectRoutePreservesZeroExpiry guards the direct
// route lifecycle invariant from docs/routing.md "Direct route lifecycle":
// own-origin direct routes use ExpiresAt=zero (never expire by time) and
// their lifetime is managed entirely by AddDirectPeer / RemoveDirectPeer.
// IsExpired returns false for ExpiresAt.IsZero(), so an alive direct route
// reaches the RouteUnchanged refresh branch alongside learned routes; the
// branch must NOT overwrite the zero ExpiresAt with now+defaultTTL, because
// that would convert the never-expiring direct entry into a 120s-ageing one
// and let TickTTL evict it while the underlying socket is still connected.
func TestUpdateRouteUnchangedDirectRoutePreservesZeroExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithClock(func() time.Time { return current }),
		WithLocalOrigin(domaintest.ID("hub")),
	)

	direct := mustAddDirect(t, tbl, domaintest.ID("peer-A"))
	if !direct.ExpiresAt.IsZero() {
		t.Fatalf("AddDirectPeer must seed ExpiresAt=zero, got %s", direct.ExpiresAt)
	}

	// Advance the clock and re-apply the identical direct route via
	// UpdateRoute. This is the wire-shape of a forced full sync re-import
	// or a future state-restore path; the existing route is alive and
	// unchanged so the call lands on the RouteUnchanged branch.
	current = current.Add(60 * time.Second)
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("peer-A"), Origin: domaintest.ID("hub"), NextHop: domaintest.ID("peer-A"),
		Hops: 1, SeqNo: direct.SeqNo, Source: RouteSourceDirect,
	}); status != RouteUnchanged {
		t.Fatalf("re-apply of alive direct route: expected RouteUnchanged, got %d", status)
	}

	after := tbl.InspectTriple(RouteTriple{
		Identity: domaintest.ID("peer-A"), Origin: domaintest.ID("hub"), NextHop: domaintest.ID("peer-A"),
	})
	if after == nil {
		t.Fatalf("direct route disappeared after RouteUnchanged refresh")
	}
	if !after.ExpiresAt.IsZero() {
		t.Fatalf("RouteUnchanged refresh must preserve ExpiresAt=zero for direct routes, got %s",
			after.ExpiresAt)
	}
}

// TestUpdateRouteUnchangedWorseHopsDoesNotRefreshTTL guards against the
// "frozen optimistic route" regression: when an incoming announcement
// carries the same SeqNo as the existing entry but a strictly worse Hops
// (e.g. a transit's local path to the origin lengthened while the origin
// itself did not bump SeqNo), the call lands on the RouteUnchanged branch
// because nothing improves. Refreshing ExpiresAt in that case would let
// the optimistic earlier hops=N entry live indefinitely, blocking natural
// TTL-driven reconvergence onto the now-actual hops=N+1 path. Only an
// *exact* reconfirmation (same source AND same hops) qualifies as a
// liveness signal that warrants a TTL extension.
func TestUpdateRouteUnchangedWorseHopsDoesNotRefreshTTL(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	key := RouteTriple{Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie")}
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: key.Identity, Origin: key.Origin, NextHop: key.NextHop,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteAccepted {
		t.Fatalf("initial insert: expected RouteAccepted, got %d", status)
	}
	initial := tbl.InspectTriple(key)
	if initial == nil {
		t.Fatalf("inserted route not found")
	}
	initialExpiry := initial.ExpiresAt

	// Advance the clock and apply a WORSE-hops entry with the same SeqNo.
	// Same triple, same source, Hops=3 instead of 2. The trust check above
	// does not fire (same source), the strict-hops-less check does not
	// fire (Hops did not improve), so the call lands on the alive
	// RouteUnchanged branch. It must NOT refresh ExpiresAt — otherwise the
	// older hops=2 entry never ages out and reconvergence onto the
	// actually-current hops=3 path is impossible without a SeqNo bump
	// from the origin.
	current = current.Add(60 * time.Second)
	if status := mustUpdate(t, tbl, RouteEntry{
		Identity: key.Identity, Origin: key.Origin, NextHop: key.NextHop,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	}); status != RouteUnchanged {
		t.Fatalf("worse-hops same-SeqNo: expected RouteUnchanged, got %d", status)
	}

	after := tbl.InspectTriple(key)
	if after == nil {
		t.Fatalf("route disappeared after RouteUnchanged on worse-hops re-apply")
	}
	if !after.ExpiresAt.Equal(initialExpiry) {
		t.Fatalf("worse-hops re-application must NOT extend ExpiresAt: initial=%s after=%s",
			initialExpiry, after.ExpiresAt)
	}
	if after.Hops != 2 {
		t.Fatalf("worse-hops re-application must keep the better existing Hops=2, got %d",
			after.Hops)
	}
}

// --- Split horizon ---

func TestSplitHorizonOmitsRoutesFromPeer(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	announceable := tbl.Announceable(domaintest.ID("peer-A"))

	for _, r := range announceable {
		if r.NextHop == domaintest.ID("peer-A") {
			t.Fatal("split horizon: routes from peer-A must be excluded when announcing to peer-A")
		}
	}
	if len(announceable) != 1 || announceable[0].Identity != domaintest.ID("bob") {
		t.Fatalf("expected only bob's route, got %+v", announceable)
	}
}

func TestSplitHorizonDoesNotSendFakeWithdrawal(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable(domaintest.ID("peer-A"))

	for _, r := range announceable {
		if r.Identity == domaintest.ID("alice") && r.Hops == HopsInfinity {
			t.Fatal("split horizon must omit, not send fake withdrawal")
		}
	}
}

func TestAnnouncableExcludesWithdrawn(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-B"),
		Hops: HopsInfinity, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable(domaintest.ID("peer-A"))
	if len(announceable) != 0 {
		t.Fatal("withdrawn routes should not be announceable")
	}
}

// --- AnnounceTo (wire projection with split horizon) ---

func TestAnnounceToPreservesHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo(domaintest.ID("peer-A"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Hops != 2 {
		t.Fatalf("wire should carry sender's local hops: expected 2, got %d", entries[0].Hops)
	}
}

func TestAnnounceToAppliesSplitHorizon(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo(domaintest.ID("peer-A"))
	if len(entries) != 1 || entries[0].Identity != domaintest.ID("bob") {
		t.Fatalf("split horizon should exclude alice's route: got %+v", entries)
	}
}

// --- Identity filtering (don't send peer routes TO themselves) ---
//
// Post-Phase-A the storage no longer keeps the "transit Origin"
// information (who first announced the route). The only meaningful
// filter that remains is: skip routes whose Identity equals the peer
// we're announcing to — a "you can reach yourself via me" claim is
// always degenerate, and on pre-A1 receivers it would also trip the
// own-origin anti-spoof if Identity happened to equal that
// receiver's localOrigin. The pre-Phase-A "Origin == excludeVia for
// arbitrary transit Origin" filter is intentionally gone — the
// field-data motivation (Origin was protocol metadata, not
// routing-decision input) is captured in uplink_claim.go's
// type-level doc-comment.

func TestAnnounceableOmitsRoutesToSelfDestination(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Route TO peer-A, learned via peer-C (transit). When announcing
	// to peer-A, this would land on peer-A's side as
	// {Identity: peer-A, Origin: peer-A} (Phase A contract) which
	// peer-A's anti-spoof rejects as forged own-origin. Filter it
	// out at send time.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("peer-A"), Origin: domaintest.ID("peer-A"), NextHop: domaintest.ID("peer-C"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("peer-C"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable(domaintest.ID("peer-A"))
	for _, r := range announceable {
		if r.Identity == domaintest.ID("peer-A") {
			t.Fatal("identity filter: routes TO peer-A must be excluded when announcing to peer-A")
		}
	}
	if len(announceable) != 1 || announceable[0].Identity != domaintest.ID("bob") {
		t.Fatalf("expected only bob's route, got %+v", announceable)
	}
}

func TestAnnounceToOmitsRoutesToSelfDestination(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("peer-A"), Origin: domaintest.ID("peer-A"), NextHop: domaintest.ID("peer-C"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("peer-C"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo(domaintest.ID("peer-A"))
	if len(entries) != 1 || entries[0].Identity != domaintest.ID("bob") {
		t.Fatalf("identity filter should exclude peer-A's own route: got %+v", entries)
	}
}

// TestAnnounceToStripsInternalFields pins the wire-emit shape of
// AnnounceTo. The Phase A contract is "every wire emit (live or
// withdrawal) carries Origin = localOrigin", which keeps pre-A1
// receivers' (Identity, Origin, NextHop)-keyed storage finding the
// same triple for live and subsequent withdrawals — see
// uplink_claim.go for the migration rationale.
//
// Two sub-tests are required to cover the contract:
//
//   - PRODUCTION path: a real Service constructs the Table with
//     WithLocalOrigin set, and the synthesised Origin reflects
//     that value.
//   - Test-fixture fallback path: tables built without
//     WithLocalOrigin observe Origin = Identity (the fallback that
//     prevents Origin == "" from failing RouteEntry.Validate at
//     downstream consumers).
//
// Both behaviours come from toRouteEntry in uplink_claim.go.
func TestAnnounceToStripsInternalFields(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("with_local_origin_emits_local_origin", func(t *testing.T) {
		// Production path: every wire frame this sender emits carries
		// Origin = its own localOrigin, regardless of what transit
		// Origin the ingress entry had ("x" below — that value is
		// consumed for anti-spoof and dropped by storage).
		tbl := NewTable(WithClock(fixedClock(now)), WithLocalOrigin(domaintest.ID("node-A")))

		mustUpdate(t, tbl, RouteEntry{
			Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-B"),
			Hops: 2, SeqNo: 7, Source: RouteSourceHopAck,
		})

		entries := tbl.AnnounceTo(domaintest.ID("peer-C"))
		if len(entries) != 1 {
			t.Fatal("expected 1 entry")
		}
		e := entries[0]
		if e.Identity != domaintest.ID("alice") || e.Origin != domaintest.ID("node-A") || e.SeqNo != 7 {
			t.Fatalf("wire fields: expected Identity=alice, Origin=node-A (localOrigin), SeqNo=7, got %+v", e)
		}
		if e.Hops != 2 {
			t.Fatalf("expected Hops=2 (sender's local hops as-is), got %d", e.Hops)
		}
	})

	t.Run("without_local_origin_falls_back_to_identity", func(t *testing.T) {
		// Test-fixture fallback path: tables built without
		// WithLocalOrigin have routeStore.localOrigin == "" and would
		// emit Origin == "" (which downstream consumers reject via
		// RouteEntry.Validate). The toRouteEntry fallback substitutes
		// Identity to keep test-only call sites producing valid frames.
		tbl := NewTable(WithClock(fixedClock(now)))

		mustUpdate(t, tbl, RouteEntry{
			Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-B"),
			Hops: 2, SeqNo: 7, Source: RouteSourceHopAck,
		})

		entries := tbl.AnnounceTo(domaintest.ID("peer-A"))
		if len(entries) != 1 {
			t.Fatal("expected 1 entry")
		}
		e := entries[0]
		if e.Identity != domaintest.ID("alice") || e.Origin != domaintest.ID("alice") || e.SeqNo != 7 {
			t.Fatalf("wire fields: expected Identity=alice, Origin=alice (fallback), SeqNo=7, got %+v", e)
		}
	})

	// AnnounceEntry has no NextHop, Source, or ExpiresAt fields —
	// verified by type system.
}

func TestAnnounceToHops15SentAsIs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-B"),
		Hops: 15, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo(domaintest.ID("peer-A"))
	if len(entries) != 1 || entries[0].Hops != 15 {
		t.Fatalf("wire should carry 15 as-is (receiver does +1 to get infinity), got %d", entries[0].Hops)
	}
}

func TestAnnounceToIncludesOwnOriginTombstones(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin(domaintest.ID("node-A")),
		WithDefaultTTL(120*time.Second),
	)

	// Add a direct peer, then remove it to create an own-origin tombstone.
	if _, err := tbl.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	result, err := tbl.RemoveDirectPeer(domaintest.ID("peer-B"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Withdrawals) == 0 {
		t.Fatal("expected withdrawal entries")
	}

	// AnnounceTo should include the own-origin tombstone (hops=16).
	entries := tbl.AnnounceTo(domaintest.ID("peer-C"))
	found := false
	for _, e := range entries {
		if e.Identity == domaintest.ID("peer-B") && e.Hops == HopsInfinity {
			found = true
		}
	}
	if !found {
		t.Fatalf("own-origin tombstone should appear in AnnounceTo, got %+v", entries)
	}
}

func TestAnnounceToExcludesTransitTombstones(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin(domaintest.ID("node-A")),
		WithDefaultTTL(120*time.Second),
	)

	// Insert a transit route (origin != localOrigin), then withdraw it.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("peer-X"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// Withdraw with higher SeqNo.
	tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("peer-X"), domaintest.ID("peer-B"), 2)

	// AnnounceTo should NOT include the transit tombstone.
	entries := tbl.AnnounceTo(domaintest.ID("peer-C"))
	for _, e := range entries {
		if e.Identity == domaintest.ID("alice") && e.Hops >= HopsInfinity {
			t.Fatal("transit tombstone should NOT appear in AnnounceTo")
		}
	}
}

func TestAnnounceToOwnOriginTombstoneExpiresNaturally(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(domaintest.ID("node-A")),
		WithDefaultTTL(120*time.Second),
	)

	if _, err := tbl.AddDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.RemoveDirectPeer(domaintest.ID("peer-B")); err != nil {
		t.Fatal(err)
	}

	// Tombstone visible immediately.
	entries := tbl.AnnounceTo(domaintest.ID("peer-C"))
	hasTombstone := false
	for _, e := range entries {
		if e.Identity == domaintest.ID("peer-B") && e.Hops == HopsInfinity {
			hasTombstone = true
		}
	}
	if !hasTombstone {
		t.Fatal("tombstone should be visible before expiry")
	}

	// Advance clock past TTL — tombstone should disappear.
	now = now.Add(121 * time.Second)
	entries = tbl.AnnounceTo(domaintest.ID("peer-C"))
	for _, e := range entries {
		if e.Identity == domaintest.ID("peer-B") {
			t.Fatalf("tombstone should have expired, still present: %+v", e)
		}
	}
}

// --- Withdrawal ---

func TestWithdrawRouteSetHopsInfinity(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 6)
	if !ok {
		t.Fatal("withdrawal with higher SeqNo should succeed")
	}

	snap := tbl.Snapshot()
	routes := snap.Routes[domaintest.ID("alice")]
	if len(routes) != 1 || routes[0].Hops != HopsInfinity {
		t.Fatal("withdrawn route should have hops=16")
	}
	if routes[0].SeqNo != 6 {
		t.Fatal("withdrawal should update SeqNo")
	}
}

func TestWithdrawRouteEqualSeqNoRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 10)
	if ok {
		t.Fatal("withdrawal with equal SeqNo should be rejected — origin must increment")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should remain alive after rejected equal-seq withdrawal")
	}
}

func TestWithdrawRouteStaleSeqNoRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 5)
	if ok {
		t.Fatal("withdrawal with lower SeqNo should be rejected")
	}
}

func TestWithdrawRouteNonexistentCreatesTombstone(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	ok := tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 5)
	if !ok {
		t.Fatal("withdrawal of unseen route should succeed (tombstone created)")
	}

	// Tombstone should exist: withdrawn entry with HopsInfinity and SeqNo=5.
	snap := tbl.Snapshot()
	routes := snap.Routes[domaintest.ID("alice")]
	if len(routes) != 1 {
		t.Fatalf("expected 1 tombstone entry, got %d", len(routes))
	}
	if routes[0].Hops != HopsInfinity {
		t.Fatalf("tombstone should have hops=%d, got %d", HopsInfinity, routes[0].Hops)
	}
	if routes[0].SeqNo != 5 {
		t.Fatalf("tombstone should preserve SeqNo=5, got %d", routes[0].SeqNo)
	}

	// Verify Lookup returns nothing (withdrawn routes are filtered).
	active := tbl.Lookup(domaintest.ID("alice"))
	if len(active) != 0 {
		t.Fatal("tombstone should not appear in Lookup")
	}
}

func TestWithdrawTombstoneBlocksStaleAnnouncement(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Withdrawal arrives first with SeqNo=10 — creates tombstone.
	ok := tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 10)
	if !ok {
		t.Fatal("tombstone creation should succeed")
	}

	// Delayed stale announcement with SeqNo=8 arrives — must be rejected
	// because the tombstone's SeqNo (10) is higher.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 8, Source: RouteSourceAnnouncement,
	})
	if status == RouteAccepted {
		t.Fatal("stale announcement (SeqNo=8) should be rejected by tombstone (SeqNo=10)")
	}

	// Route should still be withdrawn in the table.
	active := tbl.Lookup(domaintest.ID("alice"))
	if len(active) != 0 {
		t.Fatal("no active route should exist after tombstone blocks stale announcement")
	}
}

func TestWithdrawTombstoneSupersededByNewerAnnouncement(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Tombstone at SeqNo=5.
	tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 5)

	// Newer announcement with SeqNo=7 — should be accepted.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("newer announcement (SeqNo=7) should supersede tombstone (SeqNo=5)")
	}

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should be active after tombstone superseded")
	}
}

func TestWithdrawnRouteRejectsSameSeqNoHigherTrustUpdate(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Active announcement route at SeqNo=5.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// Withdraw at SeqNo=6 (strictly greater than active SeqNo=5) — sets Hops=HopsInfinity.
	if !tbl.WithdrawRoute(domaintest.ID("alice"), domaintest.ID("bob"), domaintest.ID("charlie"), 6) {
		t.Fatal("withdraw should succeed")
	}

	// Same-SeqNo hop_ack (higher trust rank) must NOT resurrect the
	// withdrawn entry. Only a strictly newer SeqNo may do that.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 1, SeqNo: 6, Source: RouteSourceHopAck,
	})
	if status == RouteAccepted {
		t.Fatal("same-SeqNo hop_ack must not resurrect a withdrawn route")
	}

	// Verify the route is still withdrawn.
	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 0 {
		t.Fatal("withdrawn route should not appear in Lookup")
	}

	// A strictly newer SeqNo should succeed.
	status = mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("newer SeqNo=7 should supersede withdrawn SeqNo=6")
	}
	routes = tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should be active after superseding withdrawal")
	}
}

// --- UpdateRoute: direct route origin guard ---

func TestUpdateRouteRejectsDirectWithForeignOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foreign-node"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrDirectForeignOrigin {
		t.Fatalf("expected ErrDirectForeignOrigin, got %v", err)
	}

	// Table should remain empty — the route was rejected.
	if tbl.Size() != 0 {
		t.Fatalf("rejected direct route should not be stored, size=%d", tbl.Size())
	}
}

func TestUpdateRouteAcceptsDirectWithOwnOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if status != RouteAccepted {
		t.Fatal("direct route with own origin should be accepted")
	}
}

func TestUpdateRouteDirectSkipsCheckWithoutLocalOrigin(t *testing.T) {
	// When localOrigin is not set, the guard is skipped — backward compat
	// for tables used without WithLocalOrigin (e.g., pure read-only).
	tbl := NewTable()

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("any-origin"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if status != RouteAccepted {
		t.Fatal("without localOrigin, direct route with any origin should be accepted")
	}
}

func TestForeignDirectRouteCannotOutrankAnnouncement(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Legitimate announcement route.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foreign-node"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// Attempt to inject a foreign-origin direct route that would outrank it.
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("foreign-node"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 2, Source: RouteSourceDirect,
	})
	if err != ErrDirectForeignOrigin {
		t.Fatalf("foreign-origin direct injection should be rejected: %v", err)
	}

	// Only the announcement should remain.
	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 || routes[0].Source != RouteSourceAnnouncement {
		t.Fatal("announcement should be the only route after rejecting foreign direct")
	}
}

// --- AddDirectPeer ---

func TestAddDirectPeerCreatesRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	result, err := tbl.AddDirectPeer(domaintest.ID("peer-A"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry := result.Entry
	// Phase A migration contract: boundary RouteEntry carries
	// Origin = localOrigin ("sender originated route"), matching the
	// wire-emit Origin used by AnnounceTo / InvalidateAllVia. This
	// preserves the pre-A1 expectation for own-direct routes
	// (Origin = our localOrigin) and gives pre-A1 receivers a
	// consistent value for live + withdrawal anti-spoof. Storage
	// internally no longer keeps Origin — see route_store_lookup.go
	// for the synthesis contract.
	if entry.Identity != domaintest.ID("peer-A") || entry.Origin != domaintest.ID("me") || entry.NextHop != domaintest.ID("peer-A") {
		t.Fatalf("unexpected entry fields: %+v", entry)
	}
	if entry.Hops != 1 || entry.Source != RouteSourceDirect {
		t.Fatalf("expected hops=1 direct, got hops=%d source=%s", entry.Hops, entry.Source)
	}
	if entry.SeqNo != 1 {
		t.Fatalf("first AddDirectPeer should produce SeqNo=1, got %d", entry.SeqNo)
	}
	if tbl.Size() != 1 {
		t.Fatalf("expected 1 route, got %d", tbl.Size())
	}
	if result.Penalized {
		t.Fatal("first connection should not be penalized")
	}
}

func TestAddDirectPeerIncrementsSeqNo(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	e1 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))
	// Simulate disconnect + reconnect by removing then re-adding
	mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))
	e2 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	if e2.SeqNo <= e1.SeqNo {
		t.Fatalf("reconnected peer should get higher SeqNo: first=%d, second=%d", e1.SeqNo, e2.SeqNo)
	}
}

func TestAddDirectPeerRequiresLocalOrigin(t *testing.T) {
	tbl := NewTable()

	_, err := tbl.AddDirectPeer(domaintest.ID("peer-A"))
	if err != ErrNoLocalOrigin {
		t.Fatalf("expected ErrNoLocalOrigin, got %v", err)
	}
}

func TestAddDirectPeerRejectsEmptyPeerID(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	_, err := tbl.AddDirectPeer(domain.PeerIdentity{})
	if err != ErrEmptyPeerID {
		t.Fatalf("expected ErrEmptyPeerID, got %v", err)
	}
}

func TestAddDirectPeerIdempotentWhenAlreadyActive(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	e1 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))
	e2 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	if e2.SeqNo != e1.SeqNo {
		t.Fatalf("repeat AddDirectPeer should not bump SeqNo: first=%d, second=%d",
			e1.SeqNo, e2.SeqNo)
	}
	if tbl.Size() != 1 {
		t.Fatalf("should still be 1 route, got %d", tbl.Size())
	}
}

func TestAddDirectPeerBumpsSeqNoAfterWithdrawal(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	e1 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))
	mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))
	e2 := mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	if e2.SeqNo <= e1.SeqNo {
		t.Fatalf("reconnect after withdrawal must bump SeqNo: first=%d, second=%d",
			e1.SeqNo, e2.SeqNo)
	}
}

func TestAddDirectPeerIdempotentOnRepeat(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(30 * time.Second)
	current := t1
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(60*time.Second),
	)

	res1, err := tbl.AddDirectPeer(domaintest.ID("peer-A"))
	if err != nil {
		t.Fatalf("first AddDirectPeer failed: %v", err)
	}
	current = t2
	res2, err := tbl.AddDirectPeer(domaintest.ID("peer-A"))
	if err != nil {
		t.Fatalf("repeat AddDirectPeer failed: %v", err)
	}

	// Direct routes have ExpiresAt=zero (event-driven lifecycle).
	// Repeat call is idempotent — same entry, no SeqNo bump.
	if !res1.Entry.ExpiresAt.IsZero() {
		t.Fatalf("direct route should have zero ExpiresAt, got %v", res1.Entry.ExpiresAt)
	}
	if res2.Entry.SeqNo != res1.Entry.SeqNo {
		t.Fatalf("repeat AddDirectPeer should not bump SeqNo: got %d, want %d",
			res2.Entry.SeqNo, res1.Entry.SeqNo)
	}
}

// --- RemoveDirectPeer ---

func TestRemoveDirectPeerSeparatesDirectFromTransit(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Direct route via AddDirectPeer
	mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	// Transit route: learned from peer-A via announcement
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// Transit route: confirmed via hop_ack
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("carol"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})
	// Unrelated route via different peer
	mustAddDirect(t, tbl, domaintest.ID("dave"))

	result := mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))

	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 wire withdrawal, got %d", len(result.Withdrawals))
	}
	w := result.Withdrawals[0]
	if w.Identity != domaintest.ID("peer-A") || w.Origin != domaintest.ID("me") || w.Hops != HopsInfinity {
		t.Fatalf("withdrawal should be wire-ready: %+v", w)
	}
	if w.SeqNo <= 1 {
		t.Fatalf("withdrawal SeqNo should be incremented beyond AddDirectPeer's seq: got %d", w.SeqNo)
	}
	if result.TransitInvalidated != 2 {
		t.Fatalf("expected 2 transit invalidations, got %d", result.TransitInvalidated)
	}
	if tbl.ActiveSize() != 1 {
		t.Fatalf("only dave's route should remain active, got %d", tbl.ActiveSize())
	}
}

func TestRemoveDirectPeerSkipsAlreadyWithdrawn(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	result := mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))

	if len(result.Withdrawals) != 0 {
		t.Fatal("already-withdrawn routes should not appear in Withdrawals")
	}
	if result.TransitInvalidated != 0 {
		t.Fatal("already-withdrawn routes should not be counted as TransitInvalidated")
	}
}

func TestRemoveDirectPeerIdempotent(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))
	result := mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))

	if result.TransitInvalidated != 0 {
		t.Fatal("second disconnect should not re-invalidate")
	}
}

func TestRemoveDirectPeerRequiresLocalOrigin(t *testing.T) {
	tbl := NewTable()

	_, err := tbl.RemoveDirectPeer(domaintest.ID("peer-A"))
	if err != ErrNoLocalOrigin {
		t.Fatalf("expected ErrNoLocalOrigin, got %v", err)
	}
}

func TestRemoveDirectPeerWireReadyWithdrawals(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	mustAddDirect(t, tbl, domaintest.ID("peer-A"))
	mustAddDirect(t, tbl, domaintest.ID("peer-B"))

	result := mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))

	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 withdrawal, got %d", len(result.Withdrawals))
	}
	w := result.Withdrawals[0]

	// Verify the withdrawal is wire-ready (AnnounceEntry format)
	if w.Hops != HopsInfinity {
		t.Fatalf("withdrawal must have hops=%d, got %d", HopsInfinity, w.Hops)
	}
	if w.Origin != domaintest.ID("me") {
		t.Fatalf("withdrawal origin must be localOrigin, got %q", w.Origin)
	}

	// Verify peer-B is unaffected
	routes := tbl.Lookup(domaintest.ID("peer-B"))
	if len(routes) != 1 || routes[0].Hops != 1 {
		t.Fatal("unrelated peer should be unaffected")
	}
}

// --- InvalidateTransitRoutes ---

func TestInvalidateTransitRoutesSkipsDirectRoutes(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Direct route for peer-A (source=direct, origin=me).
	mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	// Transit route learned through peer-A (source=announcement).
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("target-X"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	})
	if err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	invalidated, exposed := tbl.InvalidateTransitRoutes(domaintest.ID("peer-A"))
	if invalidated != 1 {
		t.Fatalf("expected 1 transit route invalidated, got %d", invalidated)
	}

	// No backup route exists for target-X, so nothing should be exposed.
	if len(exposed) != 0 {
		t.Fatalf("expected no exposed backups (no alternative route), got %v", exposed)
	}

	// Direct route should be untouched — Lookup returns active routes.
	routes := tbl.Lookup(domaintest.ID("peer-A"))
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("direct route should not be invalidated")
	}

	// Transit route should be withdrawn. Lookup() filters withdrawn entries,
	// so use Snapshot() to inspect raw table state.
	snap := tbl.Snapshot()
	snapRoutes := snap.Routes[domaintest.ID("target-X")]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot for target-X, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be invalidated (hops=infinity)")
	}

	// Lookup must return empty for withdrawn routes.
	if len(tbl.Lookup(domaintest.ID("target-X"))) != 0 {
		t.Fatal("Lookup should return empty for withdrawn transit route")
	}
}

func TestInvalidateTransitRoutesEvictsHealthEagerly(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))
	target := domaintest.ID("target-X")
	peer := domaintest.ID("peer-A")

	// Transit route to target-X via peer-A — UpdateRoute admits it and creates
	// a health state for (target-X, peer-A) via ensureLocked.
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: target, Origin: target, NextHop: peer,
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	if tbl.health.getLocked(target, peer) == nil {
		t.Fatal("expected a health state for the admitted transit route")
	}

	// Invalidating the peer's transit routes must evict the backing health
	// state immediately — not leave it lingering until the tombstone TTL
	// elapses, CompactExpired removes the claim, and reconcileHealthLocked runs.
	tbl.InvalidateTransitRoutes(peer)

	if tbl.health.getLocked(target, peer) != nil {
		t.Fatal("health state must be evicted eagerly on transit invalidation")
	}
}

func TestRemoveDirectPeerEvictsHealthEagerly(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))
	target := domaintest.ID("target-X")
	peer := domaintest.ID("peer-A")

	// Direct route to peer-A plus a transit route to target-X via peer-A; both
	// get health states (peer-A,peer-A) and (target-X,peer-A) via ensureLocked.
	mustAddDirect(t, tbl, peer)
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: target, Origin: target, NextHop: peer,
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	}); err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}
	if tbl.health.getLocked(target, peer) == nil {
		t.Fatal("expected a health state for the transit route via peer-A")
	}

	// Removing the direct peer invalidates all routes via it and must evict
	// their health states immediately, not after the tombstone TTL.
	if _, err := tbl.RemoveDirectPeer(peer); err != nil {
		t.Fatalf("RemoveDirectPeer failed: %v", err)
	}

	if tbl.health.getLocked(target, peer) != nil {
		t.Fatal("transit health via the removed direct peer must be evicted eagerly")
	}
}

func TestInvalidateTransitRoutesNoMatch(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("target-X"), NextHop: domaintest.ID("peer-B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	})
	if err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// Invalidate for peer-A — should not touch peer-B's route.
	invalidated, exposed := tbl.InvalidateTransitRoutes(domaintest.ID("peer-A"))
	if invalidated != 0 {
		t.Fatalf("expected 0 invalidated, got %d", invalidated)
	}
	if len(exposed) != 0 {
		t.Fatalf("expected no exposed backups, got %v", exposed)
	}
}

// --- Disconnect exposed backups ---

func TestRemoveDirectPeerExposesBackupRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Direct route to peer-A (will be withdrawn on disconnect).
	mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	// peer-A also serves as next-hop for a transit route to target-X.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("target-X"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	// Backup route to target-X via peer-B (survives disconnect).
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("peer-B"), NextHop: domaintest.ID("peer-B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	result, err := tbl.RemoveDirectPeer(domaintest.ID("peer-A"))
	if err != nil {
		t.Fatalf("RemoveDirectPeer failed: %v", err)
	}

	// target-X should be exposed because the backup via peer-B survives.
	if len(result.ExposedBackups) != 1 || result.ExposedBackups[0] != domaintest.ID("target-X") {
		t.Fatalf("expected [target-X] exposed, got %v", result.ExposedBackups)
	}
}

func TestRemoveDirectPeerNoBackupNoExposed(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	mustAddDirect(t, tbl, domaintest.ID("peer-A"))

	// Only route to target-X goes through peer-A — no backup.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("target-X"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	result, err := tbl.RemoveDirectPeer(domaintest.ID("peer-A"))
	if err != nil {
		t.Fatalf("RemoveDirectPeer failed: %v", err)
	}

	// No backup survives — nothing exposed.
	if len(result.ExposedBackups) != 0 {
		t.Fatalf("expected no exposed backups, got %v", result.ExposedBackups)
	}
}

func TestInvalidateTransitRoutesExposesBackupRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Transit route to target-X via peer-A (will be invalidated).
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("origin-A"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	// Backup route to target-X via peer-B (survives invalidation).
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("target-X"), Origin: domaintest.ID("origin-B"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	invalidated, exposed := tbl.InvalidateTransitRoutes(domaintest.ID("peer-A"))
	if invalidated != 1 {
		t.Fatalf("expected 1 invalidated, got %d", invalidated)
	}
	if len(exposed) != 1 || exposed[0] != domaintest.ID("target-X") {
		t.Fatalf("expected [target-X] exposed, got %v", exposed)
	}

	// Backup via peer-B should still be reachable.
	routes := tbl.Lookup(domaintest.ID("target-X"))
	if len(routes) != 1 || routes[0].NextHop != domaintest.ID("peer-B") {
		t.Fatalf("expected backup via peer-B, got %v", routes)
	}
}

// --- TTL ---

func TestTickTTLRemovesExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Second),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: now.Add(time.Hour),
	})

	tbl.TickTTL()

	if tbl.Size() != 1 {
		t.Fatalf("expected 1 route after tick, got %d", tbl.Size())
	}
}

func TestTickTTLCleansUpEmptyIdentities(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Second),
	})

	tbl.TickTTL()

	snap := tbl.Snapshot()
	if _, exists := snap.Routes[domaintest.ID("alice")]; exists {
		t.Fatal("identity with no routes should be removed from map")
	}
}

func TestTickTTLRemovesWithdrawnRoutes(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := start
	tbl := NewTable(
		WithClock(func() time.Time { return now }),
		WithLocalOrigin(domaintest.ID("me")),
		WithDefaultTTL(120*time.Second),
	)

	// Add a direct route and a transit route through domaintest.ID("alice"), plus
	// a healthy announcement through domaintest.ID("carol").
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: start.Add(2 * time.Hour),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("alice"), NextHop: domaintest.ID("alice"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: start.Add(2 * time.Hour),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("carol"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("carol"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: start.Add(2 * time.Hour),
	})

	if tbl.Size() != 3 {
		t.Fatalf("expected 3 routes before withdraw, got %d", tbl.Size())
	}

	// Simulate peer domaintest.ID("alice") disconnecting. RemoveDirectPeer sets
	// Hops=HopsInfinity and ExpiresAt=now+defaultTTL on both the
	// direct and transit routes.
	result, err := tbl.RemoveDirectPeer(domaintest.ID("alice"))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 withdrawal, got %d", len(result.Withdrawals))
	}
	if result.TransitInvalidated != 1 {
		t.Fatalf("expected 1 transit invalidated, got %d", result.TransitInvalidated)
	}

	// Immediately after withdraw: routes are still in the table (tombstones
	// protect against delayed lower-SeqNo announcements).
	tbl.TickTTL()
	if tbl.Size() != 3 {
		t.Fatalf("expected 3 entries immediately after withdraw (tombstones alive), got %d", tbl.Size())
	}

	// Advance clock past defaultTTL — tombstones expire.
	now = start.Add(121 * time.Second)
	tbl.TickTTL()

	// After TTL expiry: withdrawn routes are removed, carol survives.
	if tbl.Size() != 1 {
		t.Fatalf("expected 1 route after TTL expiry (withdrawn cleaned), got %d", tbl.Size())
	}
	snap := tbl.Snapshot()
	if _, exists := snap.Routes[domaintest.ID("carol")]; !exists {
		t.Fatal("carol's route should survive tick")
	}
	if _, exists := snap.Routes[domaintest.ID("alice")]; exists {
		t.Fatal("alice's withdrawn direct route should be removed after TTL")
	}
	if _, exists := snap.Routes[domaintest.ID("bob")]; exists {
		t.Fatal("bob's invalidated transit route should be removed after TTL")
	}
}

func TestDefaultTTLAppliedOnInsert(t *testing.T) {
	// Direct routes are excluded by design — UpdateRoute forces ExpiresAt=zero
	// for RouteSourceDirect regardless of caller intent (see "Direct route
	// lifecycle" in docs/routing.md and TestUpdateRouteDirectInsertPreservesZeroExpiry).
	// The defaultTTL contract this test exercises applies only to learned
	// (announcement / hop_ack) entries, so the fixture uses RouteSourceAnnouncement.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithDefaultTTL(60*time.Second),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	snap := tbl.Snapshot()
	r := snap.Routes[domaintest.ID("alice")][0]
	expectedExpiry := now.Add(60 * time.Second)
	if !r.ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("expected ExpiresAt=%v, got %v", expectedExpiry, r.ExpiresAt)
	}
}

func TestExplicitExpiresAtPreserved(t *testing.T) {
	// Same lifecycle constraint as TestDefaultTTLAppliedOnInsert: an
	// explicitly supplied ExpiresAt is preserved for learned routes only;
	// direct entries are forced to zero so this contract uses an
	// announcement entry.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	custom := now.Add(999 * time.Second)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: custom,
	})

	snap := tbl.Snapshot()
	if !snap.Routes[domaintest.ID("alice")][0].ExpiresAt.Equal(custom) {
		t.Fatal("explicitly set ExpiresAt should be preserved")
	}
}

// --- SeqNo counter sync ---

func TestSeqCounterSyncsWithExternalOwnOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Simulate table restored from snapshot with own-origin route at SeqNo=50.
	// This bypasses AddDirectPeer, so seqCounters starts at 0.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 50, Source: RouteSourceDirect,
	})

	// Now RemoveDirectPeer must produce SeqNo > 50, not SeqNo=1.
	result, err := tbl.RemoveDirectPeer(domaintest.ID("alice"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 withdrawal, got %d", len(result.Withdrawals))
	}
	if result.Withdrawals[0].SeqNo <= 50 {
		t.Fatalf("withdrawal SeqNo must be > 50 (synced from existing route), got %d",
			result.Withdrawals[0].SeqNo)
	}
}

func TestSeqCounterSyncsOnlyOwnOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")))

	// Foreign-origin route with high SeqNo — must NOT affect our counter.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("other-node"), NextHop: domaintest.ID("bob"),
		Hops: 2, SeqNo: 999, Source: RouteSourceAnnouncement,
	})

	// AddDirectPeer for alice should start from 1, not 999+1.
	entry := mustAddDirect(t, tbl, domaintest.ID("alice"))
	if entry.SeqNo != 1 {
		t.Fatalf("foreign-origin SeqNo should not affect own counter: expected 1, got %d",
			entry.SeqNo)
	}
}

// --- Lookup ordering ---

func TestLookupSortsBySourceThenHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Phase 2 changed Lookup ranking from "source-tier first, then
	// hops" to a single CompositeScore (base − hops×10 + RTTBonus +
	// healthPenalty + sourceBonus). With equal hops the source bonus
	// alone drives the ordering, which reproduces the original
	// source-priority contract this test asserts. With unequal hops
	// the new ranking is additive — see
	// docs/protocol/route_health.md §4.2 for the rationale.

	// announcement with 1 hop — lowest trust bonus (+0)
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"),
		Hops: 1, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// direct with 1 hop — highest trust bonus (+20)
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	// hop_ack with 1 hop — middle trust bonus (+10)
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("z"), NextHop: domaintest.ID("n3"),
		Hops: 1, SeqNo: 1, Source: RouteSourceHopAck,
	})

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 3 {
		t.Fatalf("expected 3 routes, got %d", len(routes))
	}
	// Source priority at equal hops: direct > hop_ack > announcement.
	if routes[0].Source != RouteSourceDirect {
		t.Fatalf("first route should be direct, got %s", routes[0].Source)
	}
	if routes[1].Source != RouteSourceHopAck {
		t.Fatalf("second route should be hop_ack, got %s", routes[1].Source)
	}
	if routes[2].Source != RouteSourceAnnouncement {
		t.Fatalf("third route should be announcement, got %s", routes[2].Source)
	}
}

func TestLookupTiebreaksByHopsWithinSameSource(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"),
		Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("n2"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
	if routes[0].Hops != 2 || routes[1].Hops != 5 {
		t.Fatalf("same source should sort by hops: got %d, %d", routes[0].Hops, routes[1].Hops)
	}
}

func TestLookupExcludesWithdrawnAndExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Withdrawn announcement (hops=infinity) — Lookup must exclude.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"),
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// Already-expired learned route (caller-supplied ExpiresAt in the past) —
	// Lookup must exclude. Uses RouteSourceAnnouncement, not Direct: direct
	// routes are not TTL-managed and UpdateRoute forces ExpiresAt=zero on
	// them (see "Direct route lifecycle" in docs/routing.md and
	// TestUpdateRouteDirectInsertPreservesZeroExpiry), so a "caller-passed
	// expired direct" scenario is not representable. The invariant under
	// test here — Lookup excludes expired entries — is independent of the
	// source enum, the announcement entry exercises it just as faithfully.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("n2"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Second),
	})
	// Alive hop_ack route — Lookup must include.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("z"), NextHop: domaintest.ID("n3"),
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})

	routes := tbl.Lookup(domaintest.ID("alice"))
	if len(routes) != 1 {
		t.Fatalf("expected 1 active route, got %d", len(routes))
	}
	if routes[0].NextHop != domaintest.ID("n3") {
		t.Fatal("only the non-withdrawn, non-expired route should remain")
	}
}

// --- Snapshot immutability ---

func TestSnapshotIsImmutable(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	snap := tbl.Snapshot()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 2, Source: RouteSourceDirect,
	})

	if snap.Routes[domaintest.ID("alice")][0].Hops != 2 {
		t.Fatal("snapshot should not be affected by later table mutations")
	}
}

// --- NextHop is peer identity (not transport address) ---

func TestNextHopIsPeerIdentity(t *testing.T) {
	tbl := NewTable()

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("ed25519:abc123"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("ed25519:abc123"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	if status != RouteAccepted {
		t.Fatal("route with identity-style NextHop should be accepted")
	}

	routes := tbl.Lookup(domaintest.ID("ed25519:abc123"))
	if routes[0].NextHop != domaintest.ID("ed25519:abc123") {
		t.Fatal("NextHop should be preserved as peer identity")
	}
}

// --- Trust hierarchy per triple ---

func TestTrustHierarchyPerTripleNotPerNextHop(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("C"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("D"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	if tbl.Size() != 2 {
		t.Fatal("different origins should be independent")
	}

	snap := tbl.Snapshot()
	for _, r := range snap.Routes[domaintest.ID("alice")] {
		if r.Origin == domaintest.ID("C") && r.Source != RouteSourceAnnouncement {
			t.Fatal("upgrading one triple's trust should not affect another triple")
		}
	}
}

// --- HopAck confirms specific route only ---

func TestHopAckScopedToTriple(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("C"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("D"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("C"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})

	snap := tbl.Snapshot()
	for _, r := range snap.Routes[domaintest.ID("alice")] {
		if r.Origin == domaintest.ID("D") && r.Source != RouteSourceAnnouncement {
			t.Fatal("hop_ack for (alice,C,B) should not upgrade (alice,D,B)")
		}
		if r.Origin == domaintest.ID("C") && r.Source != RouteSourceHopAck {
			t.Fatal("hop_ack for (alice,C,B) should upgrade it")
		}
	}
}

// --- Edge cases ---

func TestLookupUnknownIdentity(t *testing.T) {
	tbl := NewTable()
	routes := tbl.Lookup(domaintest.ID("nonexistent"))
	if routes != nil {
		t.Fatal("unknown identity should return nil")
	}
}

func TestEmptyTableSnapshot(t *testing.T) {
	tbl := NewTable()
	snap := tbl.Snapshot()
	if len(snap.Routes) != 0 {
		t.Fatal("empty table snapshot should have no routes")
	}
}

func TestSizeCountsAllEntries(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("n2"),
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if tbl.Size() != 2 {
		t.Fatal("Size should count all entries including withdrawn")
	}
	if tbl.ActiveSize() != 1 {
		t.Fatal("ActiveSize should exclude withdrawn entries")
	}
}

// --- Flap detection and hold-down ---

func TestFlapDetectionTriggersHoldDown(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(30*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Three rapid connect/disconnect cycles within the flap window.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, domaintest.ID("flappy"))
		mustRemoveDirect(t, tbl, domaintest.ID("flappy"))
		current = current.Add(5 * time.Second)
	}

	// Fourth reconnect should be penalized — hold-down is active.
	result, err := tbl.AddDirectPeer(domaintest.ID("flappy"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Penalized {
		t.Fatal("peer should be penalized after exceeding flap threshold")
	}

	// Direct routes are event-driven: ExpiresAt stays zero even when penalized.
	// The Penalized flag signals the caller to delay or suppress the announcement,
	// not to shorten the route's lifetime.
	if !result.Entry.ExpiresAt.IsZero() {
		t.Fatalf("penalized direct route should still have zero ExpiresAt, got %v", result.Entry.ExpiresAt)
	}
}

func TestBelowFlapThresholdNoPenalty(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Two disconnects — below threshold of 3.
	for i := 0; i < 2; i++ {
		mustAddDirect(t, tbl, domaintest.ID("stable"))
		mustRemoveDirect(t, tbl, domaintest.ID("stable"))
		current = current.Add(5 * time.Second)
	}

	result, err := tbl.AddDirectPeer(domaintest.ID("stable"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Penalized {
		t.Fatal("peer should not be penalized when below flap threshold")
	}
}

func TestHoldDownExpiresAllowsNormalReconnect(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(30*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, domaintest.ID("flappy"))
		mustRemoveDirect(t, tbl, domaintest.ID("flappy"))
		current = current.Add(2 * time.Second)
	}

	// Advance past hold-down duration (30s) AND past flap window (60s)
	// so previous withdrawal events are stale.
	current = current.Add(90 * time.Second)

	result, err := tbl.AddDirectPeer(domaintest.ID("flappy"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Penalized {
		t.Fatal("peer should not be penalized after hold-down expires")
	}
}

func TestFlapWindowSlidingExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(30*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(10*time.Second),
		WithPenalizedTTL(10*time.Second),
	)

	// Two disconnects early.
	for i := 0; i < 2; i++ {
		mustAddDirect(t, tbl, domaintest.ID("peer"))
		mustRemoveDirect(t, tbl, domaintest.ID("peer"))
		current = current.Add(2 * time.Second)
	}

	// Wait for first two disconnects to fall outside the 30s window.
	current = current.Add(35 * time.Second)

	// One more disconnect — only 1 in window, below threshold.
	mustAddDirect(t, tbl, domaintest.ID("peer"))
	mustRemoveDirect(t, tbl, domaintest.ID("peer"))

	result, err := tbl.AddDirectPeer(domaintest.ID("peer"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Penalized {
		t.Fatal("old flap events outside window should not contribute to threshold")
	}
}

func TestTickTTLCleansFlapState(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(30*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(10*time.Second),
		WithPenalizedTTL(10*time.Second),
	)

	// Trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, domaintest.ID("peer"))
		mustRemoveDirect(t, tbl, domaintest.ID("peer"))
		current = current.Add(1 * time.Second)
	}

	// Advance past both hold-down (10s) and flap window (30s).
	current = current.Add(60 * time.Second)
	tbl.TickTTL()

	// Internal flap state should be cleaned — verify by reconnecting
	// and checking that it's not penalized (no stale state lingering).
	result, err := tbl.AddDirectPeer(domaintest.ID("peer"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Penalized {
		t.Fatal("TickTTL should have cleaned stale flap state")
	}
}

func TestFlapDetectionPerPeerIsolation(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
	)

	// Flap peer-A three times.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, domaintest.ID("peer-A"))
		mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))
		current = current.Add(1 * time.Second)
	}

	// peer-B connects for the first time — should not be penalized.
	result, err := tbl.AddDirectPeer(domaintest.ID("peer-B"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Penalized {
		t.Fatal("peer-B should not be penalized by peer-A's flapping")
	}
}

func TestFlapSnapshotFiltersStaleEntries(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("me")),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
		WithHoldDownDuration(30*time.Second),
	)

	// Flap peer-A three times to trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, domaintest.ID("peer-A"))
		mustRemoveDirect(t, tbl, domaintest.ID("peer-A"))
		current = current.Add(5 * time.Second)
	}

	// Immediately after flapping: should appear in snapshot.
	snap := tbl.FlapSnapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 flap entry, got %d", len(snap))
	}
	if snap[0].PeerIdentity != domaintest.ID("peer-A") {
		t.Errorf("expected peer-A, got %s", snap[0].PeerIdentity)
	}
	if snap[0].RecentWithdrawals != 3 {
		t.Errorf("expected 3 recent withdrawals, got %d", snap[0].RecentWithdrawals)
	}
	if !snap[0].InHoldDown {
		t.Error("expected peer-A to be in hold-down")
	}

	// Advance past hold-down (30s) but within flap window (60s).
	current = current.Add(35 * time.Second)

	snap = tbl.FlapSnapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 flap entry (withdrawals still in window), got %d", len(snap))
	}
	if snap[0].InHoldDown {
		t.Error("hold-down should have expired")
	}
	if snap[0].RecentWithdrawals != 3 {
		t.Errorf("expected 3 recent withdrawals still in window, got %d", snap[0].RecentWithdrawals)
	}

	// Advance past flap window — all withdrawals are stale.
	current = current.Add(60 * time.Second)

	snap = tbl.FlapSnapshot()
	if len(snap) != 0 {
		t.Errorf("expected 0 flap entries after window expiry, got %d", len(snap))
	}
}

// --- Direct route event-driven lifecycle ---

func TestDirectRouteNeverExpiresByTime(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	current := now

	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, domaintest.ID("peer1"))

	// Verify ExpiresAt is zero — direct routes are event-driven.
	routes := tbl.Lookup(domaintest.ID("peer1"))
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if !routes[0].ExpiresAt.IsZero() {
		t.Fatalf("direct route ExpiresAt should be zero, got %v", routes[0].ExpiresAt)
	}

	// Advance far beyond any TTL — route must still be alive.
	current = now.Add(24 * time.Hour)

	routes = tbl.Lookup(domaintest.ID("peer1"))
	if len(routes) != 1 {
		t.Fatalf("direct route must survive indefinitely, got %d routes", len(routes))
	}

	// TickTTL must not remove it.
	tbl.TickTTL()
	routes = tbl.Lookup(domaintest.ID("peer1"))
	if len(routes) != 1 {
		t.Fatalf("TickTTL must not remove live direct route, got %d routes", len(routes))
	}
}

func TestDirectRouteRemovedOnlyBySocketEvent(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	current := now

	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, domaintest.ID("peer1"))

	// RemoveDirectPeer (socket close) withdraws and sets tombstone TTL.
	mustRemoveDirect(t, tbl, domaintest.ID("peer1"))

	routes := tbl.Lookup(domaintest.ID("peer1"))
	if len(routes) != 0 {
		t.Fatalf("withdrawn direct route should not appear in Lookup, got %d", len(routes))
	}

	// Tombstone exists with finite ExpiresAt — TickTTL will clean it up.
	current = current.Add(121 * time.Second)
	tbl.TickTTL()
	if tbl.Size() != 0 {
		t.Fatalf("tombstone should be cleaned by TickTTL, size=%d", tbl.Size())
	}
}

func TestDirectRouteReconnectAfterWithdrawal(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	current := now

	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, domaintest.ID("peer1"))
	mustRemoveDirect(t, tbl, domaintest.ID("peer1"))

	// Peer reconnects — new direct route replaces tombstone.
	current = current.Add(5 * time.Second)
	mustAddDirect(t, tbl, domaintest.ID("peer1"))

	routes := tbl.Lookup(domaintest.ID("peer1"))
	if len(routes) != 1 {
		t.Fatalf("expected 1 route after reconnect, got %d", len(routes))
	}
	if routes[0].IsWithdrawn() {
		t.Fatal("reconnected route must not be withdrawn")
	}
	if !routes[0].ExpiresAt.IsZero() {
		t.Fatalf("reconnected direct route ExpiresAt should be zero, got %v", routes[0].ExpiresAt)
	}
}

// --- TickTTL exposed identities ---

func TestTickTTLReturnsExposedIdentities(t *testing.T) {
	// Scenario: identity domaintest.ID("alice") has two routes — a primary (expires soon) and
	// a backup (long TTL, non-withdrawn). When the primary expires, TickTTL
	// should return domaintest.ID("alice") because a usable backup route was exposed.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(WithClock(func() time.Time { return current }))

	// Primary route — about to expire.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origin-A"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})
	// Backup route — lives much longer.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("origin-B"), NextHop: domaintest.ID("peer-B"),
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(2 * time.Hour),
	})

	// Before expiry: nothing exposed.
	result := tbl.TickTTL()
	if len(result.Exposed) != 0 {
		t.Fatalf("expected no exposed identities before expiry, got %v", result.Exposed)
	}

	// Advance past primary's TTL.
	current = now.Add(11 * time.Second)
	result = tbl.TickTTL()

	if len(result.Exposed) != 1 || result.Exposed[0] != domaintest.ID("alice") {
		t.Fatalf("expected [alice] exposed, got %v", result.Exposed)
	}
	// Backup route still alive.
	if tbl.Size() != 1 {
		t.Fatalf("expected 1 surviving route, got %d", tbl.Size())
	}
}

func TestTickTTLNoExposedWhenAllSurvivorsWithdrawn(t *testing.T) {
	// If the only survivors after expiry are withdrawn (tombstones), the
	// identity should NOT appear in exposed — there's no usable route.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(
		WithClock(func() time.Time { return current }),
		WithLocalOrigin(domaintest.ID("me")),
		WithDefaultTTL(120*time.Second),
	)

	// Non-withdrawn route — will expire.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("origin-A"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})
	// Withdrawn tombstone — survives longer but is not usable.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("origin-B"), NextHop: domaintest.ID("peer-B"),
		Hops: HopsInfinity, SeqNo: 5, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(2 * time.Hour),
	})

	current = now.Add(11 * time.Second)
	result := tbl.TickTTL()

	if len(result.Exposed) != 0 {
		t.Fatalf("withdrawn-only survivors should not be exposed, got %v", result.Exposed)
	}
}

func TestTickTTLNoExposedWhenNothingRemoved(t *testing.T) {
	// If no routes expired, nothing is exposed.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("carol"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("peer-C"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(time.Hour),
	})

	result := tbl.TickTTL()
	if len(result.Exposed) != 0 {
		t.Fatalf("no expiry means no exposed, got %v", result.Exposed)
	}
}

func TestTickTTLNoExposedWhenAllRoutesExpire(t *testing.T) {
	// If ALL routes for an identity expire (identity deleted), it should NOT
	// appear in exposed — there's nothing to drain to.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(WithClock(func() time.Time { return current }))

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("dave"), Origin: domaintest.ID("origin-A"), NextHop: domaintest.ID("peer-A"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})

	current = now.Add(11 * time.Second)
	result := tbl.TickTTL()

	if len(result.Exposed) != 0 {
		t.Fatalf("all-expired identity should not be exposed, got %v", result.Exposed)
	}
	if tbl.Size() != 0 {
		t.Fatalf("expected empty table, got %d", tbl.Size())
	}
}

// TestUpdateRoutePreservesExtra pins the documented public
// RouteEntry.Extra contract (see types.go): forward-compatible
// wire fields MUST round-trip unchanged through storage so
// re-announced routes carry unknown extensions onward. The
// Phase A storage swap preserved this by adding UplinkClaim.Extra
// and threading it through toUplinkClaim / toRouteEntry.
func TestUpdateRoutePreservesExtra(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("local")))
	extra := json.RawMessage(`{"onion_box":"deadbeef"}`)
	entry := RouteEntry{
		Identity: domaintest.ID("X"), Origin: domaintest.ID("A"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		Extra: extra,
	}
	status := mustUpdate(t, tbl, entry)
	if status != RouteAccepted {
		t.Fatalf("expected accepted, got %v", status)
	}

	routes := tbl.Lookup(domaintest.ID("X"))
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if string(routes[0].Extra) != string(extra) {
		t.Fatalf("Extra lost after UpdateRoute: got %q, want %q",
			string(routes[0].Extra), string(extra))
	}
}

func TestToAnnounceEntryPreservesExtra(t *testing.T) {
	extra := json.RawMessage(`{"onion_box":"deadbeef","future":true}`)
	entry := RouteEntry{
		Identity: domaintest.ID("X"), Origin: domaintest.ID("A"), NextHop: domaintest.ID("B"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		Extra: extra,
	}
	ae := entry.ToAnnounceEntry()
	if string(ae.Extra) != string(extra) {
		t.Fatalf("Extra lost in ToAnnounceEntry: got %s", string(ae.Extra))
	}
}

func TestToAnnounceEntryNilExtraForLocalRoute(t *testing.T) {
	entry := RouteEntry{
		Identity: domaintest.ID("X"), Origin: domaintest.ID("A"), NextHop: domaintest.ID("A"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	}
	ae := entry.ToAnnounceEntry()
	if ae.Extra != nil {
		t.Fatalf("expected nil Extra for local route, got %s", string(ae.Extra))
	}
}

// --- Self-route (local identity) tests ---

func TestLookupOwnIdentityReturnsSelfRoute(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	routes := tbl.Lookup(domaintest.ID("nodeA"))
	if len(routes) == 0 {
		t.Fatal("Lookup for own identity must return at least one route")
	}

	self := routes[0]
	if self.Identity != domaintest.ID("nodeA") {
		t.Fatalf("expected Identity=nodeA, got %s", self.Identity)
	}
	if self.Origin != domaintest.ID("nodeA") {
		t.Fatalf("expected Origin=nodeA, got %s", self.Origin)
	}
	if self.NextHop != domaintest.ID("nodeA") {
		t.Fatalf("expected NextHop=nodeA, got %s", self.NextHop)
	}
	if self.Hops != 0 {
		t.Fatalf("expected Hops=0 for self-route, got %d", self.Hops)
	}
	if self.Source != RouteSourceLocal {
		t.Fatalf("expected Source=local, got %s", self.Source)
	}
}

func TestLookupOwnIdentityEmptyTable(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	// Even with a completely empty table, self-route must be present.
	routes := tbl.Lookup(domaintest.ID("nodeA"))
	if len(routes) != 1 {
		t.Fatalf("expected exactly 1 route (self), got %d", len(routes))
	}
	if routes[0].Source != RouteSourceLocal {
		t.Fatalf("expected local source, got %s", routes[0].Source)
	}
}

func TestLookupOwnIdentitySelfRouteHasHighestPriority(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	// Add an announcement route for the same identity via a remote peer.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("nodeA"), Origin: domaintest.ID("peerB"), NextHop: domaintest.ID("peerB"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	routes := tbl.Lookup(domaintest.ID("nodeA"))
	if len(routes) < 2 {
		t.Fatalf("expected at least 2 routes, got %d", len(routes))
	}

	// Self-route must be first (highest priority).
	if routes[0].Source != RouteSourceLocal {
		t.Fatalf("self-route must be first in sorted results, got %s", routes[0].Source)
	}
}

func TestLookupOtherIdentityDoesNotInjectSelfRoute(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	routes := tbl.Lookup(domaintest.ID("nodeB"))
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes for unknown identity, got %d", len(routes))
	}
}

func TestSnapshotContainsSelfRoute(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	snap := tbl.Snapshot()

	selfRoutes, ok := snap.Routes[domaintest.ID("nodeA")]
	if !ok || len(selfRoutes) == 0 {
		t.Fatal("snapshot must contain self-route for own identity")
	}

	found := false
	for _, r := range selfRoutes {
		if r.Source == RouteSourceLocal && r.Hops == 0 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("snapshot must contain a RouteSourceLocal entry with Hops=0")
	}

	// Counters describe persisted table state only — the synthetic
	// self-route is NOT counted to stay consistent with ActiveSize().
	if snap.TotalEntries != 0 {
		t.Fatalf("TotalEntries must not count synthetic self-route, got %d", snap.TotalEntries)
	}
	if snap.ActiveEntries != 0 {
		t.Fatalf("ActiveEntries must not count synthetic self-route, got %d", snap.ActiveEntries)
	}
}

func TestSnapshotBestRouteReturnsSelfForOwnIdentity(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	snap := tbl.Snapshot()
	best := snap.BestRoute(domaintest.ID("nodeA"))
	if best == nil {
		t.Fatal("BestRoute must return non-nil for own identity")
	}
	if best.Source != RouteSourceLocal {
		t.Fatalf("expected local source as best, got %s", best.Source)
	}
}

func TestSelfRouteNeverExpires(t *testing.T) {
	now := time.Now()
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(now)),
	)

	entry := tbl.localRouteEntry()
	if entry.IsExpired(now.Add(365 * 24 * time.Hour)) {
		t.Fatal("self-route must never expire")
	}
	if entry.IsWithdrawn() {
		t.Fatal("self-route must never be withdrawn")
	}
}

func TestAnnounceToDoesNotIncludeSelfRoute(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	// Add a direct peer so there's at least one real route.
	mustAddDirect(t, tbl, domaintest.ID("peerB"))

	entries := tbl.AnnounceTo(domaintest.ID("peerB"))
	for _, e := range entries {
		if e.Identity == domaintest.ID("nodeA") && e.Hops == 0 {
			t.Fatal("self-route (Hops=0) must not be included in announcements")
		}
	}
}

func TestLookupWithoutLocalOriginNoSelfRoute(t *testing.T) {
	tbl := NewTable()

	routes := tbl.Lookup(domaintest.ID("someNode"))
	if len(routes) != 0 {
		t.Fatalf("without localOrigin, no self-route should be injected, got %d", len(routes))
	}
}

func TestSnapshotCountersMatchActiveSize(t *testing.T) {
	now := time.Now()
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(now)),
	)

	// Empty table — counters must agree and both be zero.
	snap := tbl.Snapshot()
	if snap.ActiveEntries != tbl.ActiveSize() {
		t.Fatalf("empty table: ActiveEntries=%d != ActiveSize()=%d",
			snap.ActiveEntries, tbl.ActiveSize())
	}

	// Add a real direct peer.
	mustAddDirect(t, tbl, domaintest.ID("peerB"))

	snap = tbl.Snapshot()
	if snap.ActiveEntries != tbl.ActiveSize() {
		t.Fatalf("with direct peer: ActiveEntries=%d != ActiveSize()=%d",
			snap.ActiveEntries, tbl.ActiveSize())
	}
	if snap.ActiveEntries != 1 {
		t.Fatalf("expected ActiveEntries=1 (one real route), got %d", snap.ActiveEntries)
	}

	// Self-route must be visible in Routes map but not in counters.
	selfRoutes := snap.Routes[domaintest.ID("nodeA")]
	hasSelfRoute := false
	for _, r := range selfRoutes {
		if r.Source == RouteSourceLocal {
			hasSelfRoute = true
		}
	}
	if !hasSelfRoute {
		t.Fatal("self-route missing from Snapshot().Routes")
	}
}

func TestUpdateRouteRejectsRouteSourceLocal(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("nodeA")),
		WithClock(fixedClock(time.Now())),
	)

	// Attempt to persist a RouteSourceLocal entry for a remote identity.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("remoteNode"),
		Origin:   domaintest.ID("remoteNode"),
		NextHop:  domaintest.ID("remoteNode"),
		Hops:     0,
		SeqNo:    1,
		Source:   RouteSourceLocal,
	})
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected for RouteSourceLocal, got %v", status)
	}
	if err != ErrLocalSourceReserved {
		t.Fatalf("expected ErrLocalSourceReserved, got %v", err)
	}

	// Also reject for own identity — local source must stay synthetic.
	status, err = tbl.UpdateRoute(RouteEntry{
		Identity: domaintest.ID("nodeA"),
		Origin:   domaintest.ID("nodeA"),
		NextHop:  domaintest.ID("nodeA"),
		Hops:     0,
		SeqNo:    1,
		Source:   RouteSourceLocal,
	})
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected for own identity RouteSourceLocal, got %v", status)
	}
	if err != ErrLocalSourceReserved {
		t.Fatalf("expected ErrLocalSourceReserved, got %v", err)
	}

	// Verify no entries were persisted.
	if tbl.Size() != 0 {
		t.Fatalf("expected empty table after rejections, got size=%d", tbl.Size())
	}
}
