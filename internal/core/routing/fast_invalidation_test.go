package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// Phase 1 P3 — fast invalidation regression suite.
//
// The fast-invalidation path admits incoming claims with hops above
// MaxSaneHops as tombstones at the observed SeqNo, dropping them
// from Lookup immediately while preserving the SeqNo-resurrection
// guard for recovery. These tests pin the contract:
//
//   - hops > MaxSaneHops admits a tombstone, not a live row;
//   - LookupActive excludes the invalidated claim immediately;
//   - AnnounceProjectionFor does NOT wire-emit the transit tombstone
//     (own-direct tombstone passthrough is the only wire-emit path);
//   - a subsequent valid announce with strictly newer SeqNo resurrects
//     the claim via the standard tombstone-promotion path.
//
// See docs/routing.md "RIB compaction" / "fast invalidation" for
// the permanent contract; the inline ApplyUpdate branch in
// route_store_mutation.go documents the per-branch behaviour.

// TestFastInvalidation_NewClaimAcceptedAsTombstone documents the
// idx<0 branch: a fresh ingest with hops>MaxSaneHops never reaches
// the live cap-admission flow; it lands as a tombstone outside the
// K-counted slots.
func TestFastInvalidation_NewClaimAcceptedAsTombstone(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
	)

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 10, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("fast-invalidated new claim must be accepted as tombstone, got %v", status)
	}

	if got := tbl.CapStats().FastInvalidations; got != 1 {
		t.Fatalf("FastInvalidations counter=%d after first invalidation, want 1", got)
	}

	// LookupActive must NOT return the invalidated claim — it is a
	// tombstone, filtered by IsWithdrawn.
	if routes := tbl.Lookup(dest); len(routes) != 0 {
		t.Fatalf("Lookup must skip fast-invalidated tombstone, got %+v", routes)
	}

	// InspectTriple must still see the row as a withdrawn entry —
	// the SeqNo-resurrection guard depends on it being present.
	entry := tbl.InspectTriple(RouteTriple{Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink})
	if entry == nil {
		t.Fatalf("InspectTriple must return the tombstone")
	}
	if !entry.IsWithdrawn() {
		t.Fatalf("invalidated claim must be marked withdrawn, got Hops=%d", entry.Hops)
	}
	if entry.SeqNo != 5 {
		t.Fatalf("tombstone must preserve incoming SeqNo (NO bump) for resurrection guard, got SeqNo=%d want 5", entry.SeqNo)
	}
}

// TestFastInvalidation_ReplacesLiveClaimWithStrictlyNewerSeqNo
// covers the idx>=0 branch where a live claim already exists and
// the bad-hops announce arrives at a strictly newer SeqNo. The
// stored live row must be retired (replaced by tombstone).
func TestFastInvalidation_ReplacesLiveClaimWithStrictlyNewerSeqNo(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
	)

	// Live claim at hops=3, SeqNo=10.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Bad-hops update at strictly newer SeqNo — must invalidate
	// the stored live row.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 12, SeqNo: 11, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("strictly-newer bad-hops claim must replace live with tombstone, got %v", status)
	}

	if routes := tbl.Lookup(dest); len(routes) != 0 {
		t.Fatalf("Lookup must drop invalidated route, got %+v", routes)
	}
	if got := tbl.CapStats().FastInvalidations; got != 1 {
		t.Fatalf("FastInvalidations=%d, want 1", got)
	}
}

// TestFastInvalidation_StaleBadHopsClaimRejected documents the
// stale-SeqNo branch: a bad-hops claim whose SeqNo is not strictly
// newer than the stored row is dropped without bumping the counter
// — the stored claim was already validated at a higher SeqNo and
// the upstream's recovery path (a fresh legit advance) is the only
// legitimate way to undo a live → tombstone transition.
func TestFastInvalidation_StaleBadHopsClaimRejected(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 3, SeqNo: 20, Source: RouteSourceAnnouncement,
	})

	// Stale bad-hops sample at SeqNo=15 < stored 20.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 12, SeqNo: 15, Source: RouteSourceAnnouncement,
	})
	if status != RouteRejected {
		t.Fatalf("stale bad-hops claim must be rejected, got %v", status)
	}
	if got := tbl.CapStats().FastInvalidations; got != 0 {
		t.Fatalf("FastInvalidations=%d on stale rejection, want 0", got)
	}

	// Stored row must still be live and unchanged.
	routes := tbl.Lookup(dest)
	if len(routes) != 1 || routes[0].Hops != 3 || routes[0].SeqNo != 20 {
		t.Fatalf("Lookup after stale bad-hops must keep stored live row, got %+v", routes)
	}
}

// TestFastInvalidation_RecoveryByNewerValidAnnounce verifies the
// recovery contract: a subsequent valid announce on the same
// (Identity, Uplink) with strictly newer SeqNo and Hops <=
// MaxSaneHops resurrects the route through the standard
// ApplyUpdate tombstone-promotion path.
func TestFastInvalidation_RecoveryByNewerValidAnnounce(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
		WithMaxNextHopsPerOrigin(4),
	)

	// Invalidate (lands as tombstone at SeqNo=10).
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 12, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if routes := tbl.Lookup(dest); len(routes) != 0 {
		t.Fatalf("Lookup must drop fast-invalidated route, got %+v", routes)
	}

	// Resurrect with a valid announce at strictly newer SeqNo.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 3, SeqNo: 11, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("recovery by strictly-newer valid announce must succeed, got %v", status)
	}

	routes := tbl.Lookup(dest)
	if len(routes) != 1 || routes[0].Hops != 3 || routes[0].SeqNo != 11 {
		t.Fatalf("Lookup after recovery must return resurrected live row, got %+v", routes)
	}
}

// TestFastInvalidation_NotEmittedOnWire pins the wire-emit
// suppression: AnnounceProjectionFor only emits own-direct
// tombstones (Source == RouteSourceDirect), so a fast-invalidated
// transit claim (Source == RouteSourceAnnouncement) naturally
// never leaves the node. We are not the route's origin and have
// no authority to broadcast a count-to-infinity withdrawal on
// the upstream's behalf.
func TestFastInvalidation_NotEmittedOnWire(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
		viewer  = domaintest.ID("peer-Z")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 12, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	for _, e := range tbl.AnnounceTo(viewer) {
		if e.Identity == dest {
			t.Fatalf("fast-invalidated transit claim must NOT be wire-emitted, got entry=%+v", e)
		}
	}
}

// TestFastInvalidation_WireHopsBoundaryClampedToHopsInfinity pins
// the review-flagged regression: the receive path in
// `node/routing_announce.go` adds `+1` to the wire hop count and
// clamps overflow back to `HopsInfinity` (16). A wire announce
// with `Hops=15` therefore reaches `UpdateRoute` as `Hops=16`,
// `toUplinkClaim` then derives `Withdrawn=true` from the Hops
// sentinel, and the pre-fix P3 trigger's `!incoming.Withdrawn`
// guard silently exempted the case — the bad-hops claim landed as
// a "real" tombstone WITHOUT bumping `FastInvalidations` or
// emitting `routing_fast_invalidation`, contradicting the
// phase-doc requirement to count / log every `Hops > MaxSaneHops`
// event.
//
// Genuine wire withdrawals route through `Table.WithdrawRoute`,
// NOT `UpdateRoute`, so any `UpdateRoute` call with
// `incoming.Hops == HopsInfinity` is by construction a loop-
// boundary signal (wire.Hops was 15-or-greater, clamped during
// receive). The post-fix P3 trigger drops the `!incoming.Withdrawn`
// guard so both tiers (live bad-hops AND landed-at-HopsInfinity-
// by-clamp) bump the counter and log line.
//
// The fixture below simulates the receive-path conversion directly
// (the unit test exercises Table.UpdateRoute with the post-clamp
// shape, since the receive path itself lives in the node package).
func TestFastInvalidation_WireHopsBoundaryClampedToHopsInfinity(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
	)

	// Wire announce with Hops=15 → receive path applies +1 →
	// reaches UpdateRoute with Hops=16 (HopsInfinity, clamped).
	// toUplinkClaim then sets Withdrawn=true from the sentinel.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: HopsInfinity, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("clamped wire-15 boundary must reach P3 fast-invalidation as RouteAccepted (tombstone-at-incoming.SeqNo), got %v", status)
	}

	// FastInvalidations MUST bump — the observability contract
	// demands one counter increment per accepted invalidation,
	// including the wire-boundary case.
	if got := tbl.CapStats().FastInvalidations; got != 1 {
		t.Fatalf("FastInvalidations=%d after wire-15 boundary invalidation, want 1 (pre-fix this stayed at 0 because the `!incoming.Withdrawn` exempt silently routed the case past P3)", got)
	}

	// Stored claim must be the tombstone at the observed SeqNo for
	// the SeqNo-resurrection guard.
	entry := tbl.InspectTriple(RouteTriple{Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink})
	if entry == nil {
		t.Fatalf("InspectTriple must return the tombstone")
	}
	if !entry.IsWithdrawn() {
		t.Fatalf("invalidated claim must be marked withdrawn, got Hops=%d", entry.Hops)
	}
	if entry.SeqNo != 5 {
		t.Fatalf("tombstone must preserve incoming SeqNo (NO bump) so legit recovery must exceed the observed bad SeqNo, got %d want 5", entry.SeqNo)
	}
}

// TestFastInvalidation_CrossOriginStaleReplayRejected pins the
// regression caught in review: when a stale bad-hops claim arrives
// from a previously-observed Origin lineage that has a recorded
// high-water EXCEEDING the incoming SeqNo, the P3 fast-invalidation
// branch MUST honour the per-Origin stale-replay guard (the same
// guard the standard branches at the bottom of ApplyUpdate apply).
// Without the guard duplicated inside P3, the scenario
//
//	A10  →  B5  →  A9 with Hops > MaxSaneHops
//
// would slip past the standard guard (P3 returns early before
// reaching it) and tombstone the legitimate B lineage purely on the
// `entry.SeqNo > old.SeqNo` check (9 > 5), even though A's
// previously observed high-water of 10 explicitly marks A9 as a
// stale replay on A's own timeline.
//
// Reference: the per-Origin high-water guard in
// route_store_mutation.go ApplyUpdate (the SeenOriginSeqs[origin]
// pre-check at the top of the idx>=0 path), duplicated inside the
// P3 fast-invalidation branch for the same reason.
func TestFastInvalidation_CrossOriginStaleReplayRejected(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
		originA = domaintest.ID("origin-A")
		originB = domaintest.ID("origin-B")
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxSaneHops(8),
		WithMaxNextHopsPerOrigin(4),
	)

	// A10: live claim from Origin A at SeqNo=10.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: originA, NextHop: uplink,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// B5: cross-Origin lineage shift from B at strictly better hops.
	// Lower SeqNo (5) is admitted by the cross-Origin lineage branch
	// in the stale-SeqNo path because hops improve. Records B's
	// observation (SeenOriginSeqs[B]=5) and shifts LastIngressOrigin
	// to B.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: originB, NextHop: uplink,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// A9 with bad hops: a stale replay from A. The per-Origin
	// high-water guard MUST reject this — A's recorded high-water
	// is 10, and 9 <= 10. Without the guard inside P3,
	// fast-invalidation would tombstone the legitimate B lineage
	// because `entry.SeqNo (9) > old.SeqNo (5)`.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: originA, NextHop: uplink,
		Hops: 12, SeqNo: 9, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("cross-Origin stale-replay bad-hops claim must be rejected by per-Origin high-water guard inside P3, got status=%v", status)
	}
	if got := tbl.CapStats().FastInvalidations; got != 0 {
		t.Fatalf("rejected cross-Origin stale replay must NOT bump FastInvalidations, got %d", got)
	}

	// The legitimate B lineage must still be in place — Lookup
	// returns the B-shaped route (Hops=2), proving the bad A9
	// claim did not poison storage.
	routes := tbl.Lookup(dest)
	if len(routes) != 1 || routes[0].Hops != 2 || routes[0].SeqNo != 5 {
		t.Fatalf("B lineage must survive the rejected A9 fast-invalidation, got routes=%+v", routes)
	}
}

// TestFastInvalidation_DisabledCapShortCircuits pins the
// maxSaneHops<=0 contract: tables built without WithMaxSaneHops
// admit bad-hops announces through the standard branches
// (Hops 15 < HopsInfinity=16 is still a valid live claim per
// RouteEntry.Validate). RouteCapStats.FastInvalidations stays at
// zero.
func TestFastInvalidation_DisabledCapShortCircuits(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	var (
		localID = domaintest.ID("node-A")
		dest    = domaintest.ID("dest")
		uplink  = domaintest.ID("u-1")
	)

	// No WithMaxSaneHops → cap disabled (default 0).
	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
	)

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: domaintest.ID("src-x"), NextHop: uplink,
		Hops: 14, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("with maxSaneHops disabled, hops=14 (<HopsInfinity) must be accepted live, got %v", status)
	}

	routes := tbl.Lookup(dest)
	if len(routes) != 1 || routes[0].Hops != 14 {
		t.Fatalf("Lookup must keep live row when cap disabled, got %+v", routes)
	}
	if got := tbl.CapStats().FastInvalidations; got != 0 {
		t.Fatalf("FastInvalidations=%d with disabled cap, want 0", got)
	}
}
