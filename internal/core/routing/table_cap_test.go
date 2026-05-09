package routing

import (
	"testing"
	"time"
)

// --- §9.1 admission rules ---

// TestRIBCap_AcceptsUpToK_PerOriginPair verifies the trivial cap
// invariant: while the (Identity, Origin) bucket is below the cap,
// every new announcement triple is accepted as-is — no eviction, no
// rejection. The bucket grows linearly until it hits maxNextHopsPerOrigin.
func TestRIBCap_AcceptsUpToK_PerOriginPair(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(3),
	)

	// Three distinct next-hops on the same (Identity, Origin) pair.
	for i, nh := range []PeerIdentity{"hop-a", "hop-b", "hop-c"} {
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: nh,
			Hops: 2 + i, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("entry %d (%s): expected RouteAccepted, got %v", i, nh, status)
		}
	}

	snap := tbl.Snapshot()
	got := len(snap.Routes["alice"])
	if got != 3 {
		t.Fatalf("bucket should hold 3 entries up to cap; got %d", got)
	}
}

// TestRIBCap_RejectsExcessAnnouncement_WhenWorseThanWorst verifies the
// "cap floor" semantics: once a bucket is saturated with K announcements,
// a new announcement that is NOT strictly better than the worst existing
// candidate is dropped. The table state stays at exactly K entries.
func TestRIBCap_RejectsExcessAnnouncement_WhenWorseThanWorst(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)

	// Saturate the bucket with two well-ranked announcements.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-a",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-b",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// New candidate is strictly worse (more hops than the worst, same trust).
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-c",
		Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected for worse-than-worst announcement, got %v", status)
	}

	snap := tbl.Snapshot()
	if got := len(snap.Routes["alice"]); got != 2 {
		t.Fatalf("bucket size after rejection should remain at cap=2, got %d", got)
	}
	// hop-c must NOT have ended up in the bucket.
	for _, r := range snap.Routes["alice"] {
		if r.NextHop == "hop-c" {
			t.Fatal("hop-c should have been dropped by the cap, but it is in the table")
		}
	}
}

// TestRIBCap_EvictsWorstAnnouncement_ByTrust_ThenHops_ThenFreshness
// drives all three legs of the EVICTION KEY (which bucket member to
// drop once admission has decided to evict): trust tier first, hops
// second, expiry timestamp third. Each sub-test isolates one leg by
// keeping the other two equal across the bucket so the chosen victim
// is unambiguous.
//
// Note: the cap admission FLOOR (whether to evict at all) is
// intentionally narrower than the eviction key — it omits ExpiresAt
// because every re-announce arrives with a strictly-later ExpiresAt
// and including it would thrash the bucket on each cycle. See
// isStrictlyBetterForCapLocked for the asymmetry rationale. The
// expiry sub-test below therefore engineers admission via a
// strict-better tier (lower hops) and verifies that, GIVEN the cap
// has decided to evict, the eviction key picks the earliest-expiring
// row among equal-trust + equal-hops incumbents.
func TestRIBCap_EvictsWorstAnnouncement_ByTrust_ThenHops_ThenFreshness(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("trust_lowest_evicted_first", func(t *testing.T) {
		// Bucket: one announcement (trust 0) + one hop_ack (trust 1).
		// New incoming: hop_ack with worse hops than the existing
		// hop_ack but better than nothing. Must evict the
		// announcement, NOT the hop_ack, even though the announcement
		// has fewer hops.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-ann",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-ack",
			Hops: 5, SeqNo: 1, Source: RouteSourceHopAck,
		})

		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-new",
			Hops: 4, SeqNo: 1, Source: RouteSourceHopAck,
		})
		if status != RouteAccepted {
			t.Fatalf("hop_ack incoming with announcement victim available: expected RouteAccepted, got %v", status)
		}

		snap := tbl.Snapshot()
		nextHops := nextHopsByOrigin(t, snap, "alice", "bob")
		if _, ok := nextHops["hop-ann"]; ok {
			t.Fatal("announcement should have been evicted as the lowest-trust candidate")
		}
		if _, ok := nextHops["hop-ack"]; !ok {
			t.Fatal("existing hop_ack must survive the eviction of the announcement")
		}
		if _, ok := nextHops["hop-new"]; !ok {
			t.Fatal("incoming hop_ack must occupy the freed slot")
		}
	})

	t.Run("hops_highest_evicted_among_same_trust", func(t *testing.T) {
		// Bucket: two announcements at the same trust tier, one with
		// fewer hops, one with more. Incoming announcement must evict
		// the higher-hop one and keep the better-hops survivor.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-low",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-high",
			Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
		})

		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-mid",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("expected RouteAccepted (evicts highest-hops), got %v", status)
		}

		nextHops := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
		if _, ok := nextHops["hop-high"]; ok {
			t.Fatal("highest-hops candidate should have been evicted")
		}
		if _, ok := nextHops["hop-low"]; !ok {
			t.Fatal("lowest-hops candidate must survive")
		}
		if _, ok := nextHops["hop-mid"]; !ok {
			t.Fatal("incoming candidate must occupy the freed slot")
		}
	})

	t.Run("expiry_earliest_evicted_among_same_trust_and_hops", func(t *testing.T) {
		// Bucket: two announcements at the same trust AND same hops,
		// distinguished only by ExpiresAt. The eviction-key contract
		// (isWorseForEvictionLocked) still uses ExpiresAt as the final
		// tie-break — the earlier-expiring row is the cheaper eviction
		// victim because TickTTL would reclaim it soon anyway. To
		// trigger admission at all under the cap floor, however, the
		// candidate must improve a real tier (live > expired, trust,
		// or hops) — see isStrictlyBetterForCapLocked. We therefore
		// engineer admission via strictly-fewer-hops on the candidate
		// and verify that the eviction-key picks the earliest-expiring
		// of the two equal-trust + equal-hops incumbents as the
		// victim.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-soon",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			// Explicit ExpiresAt to bypass UpdateRoute's defaultTTL
			// fill-in and pin the comparison.
			ExpiresAt: now.Add(30 * time.Second),
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-late",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			ExpiresAt: now.Add(120 * time.Second),
		})

		// Hops=2 is strictly better than incumbents' Hops=3 → the cap
		// floor admits, then the eviction key picks the worst victim
		// among the equal-trust + equal-hops bucket members by
		// ExpiresAt.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-new",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
			ExpiresAt: now.Add(180 * time.Second),
		})
		if status != RouteAccepted {
			t.Fatalf("expected RouteAccepted (Hops=2 strictly better than incumbents' Hops=3), got %v", status)
		}

		nextHops := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
		if _, ok := nextHops["hop-soon"]; ok {
			t.Fatal("earliest-expiring incumbent should have been evicted (eviction key uses ExpiresAt as final tie-break)")
		}
		if _, ok := nextHops["hop-late"]; !ok {
			t.Fatal("longer-lived incumbent must survive")
		}
		if _, ok := nextHops["hop-new"]; !ok {
			t.Fatal("incoming candidate (lower hops) must occupy the freed slot")
		}
	})
}

// TestRIBCap_DirectAndLocalNeverEvicted pins the protection invariant:
// even when the bucket is at the cap and every direct/local entry is
// "worse" than the incoming announcement by ranking, the cap MUST NOT
// retire them. Direct routes correspond to live sockets and only
// RemoveDirectPeer may release them.
func TestRIBCap_DirectAndLocalNeverEvicted(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin("self"),
		WithMaxNextHopsPerOrigin(1),
	)

	// Lay down a direct route via "neighbor" — Origin must be the
	// local identity, NextHop must equal Identity (direct neighbour).
	mustAddDirect(t, tbl, "neighbor")

	// Incoming: a strictly higher-trust candidate (impossible without
	// RouteSourceLocal which is reserved, so use hop_ack) on the same
	// bucket. Even though the new candidate is strictly better by the
	// generic ranking, the protection rule keeps direct in place and
	// the new candidate is rejected because no evictable victim
	// exists.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "neighbor", Origin: "self", NextHop: "transit",
		Hops: 1, SeqNo: 99, Source: RouteSourceHopAck,
	})
	if status != RouteRejected {
		t.Fatalf("incoming hop_ack against a direct-only saturated bucket: "+
			"expected RouteRejected (all entries protected), got %v", status)
	}

	// Direct entry must still be present after the rejected admission.
	snap := tbl.Snapshot()
	nextHops := nextHopsByOrigin(t, snap, "neighbor", "self")
	if _, ok := nextHops["neighbor"]; !ok {
		t.Fatal("direct route was retired by cap admission — protection invariant broken")
	}
}

// TestRIBCap_HopAckPromotionEvictsAnnouncement_EvenIfMoreHops verifies
// the trust-tier promotion rule: when an authentic hop_ack arrives and
// the bucket is saturated with announcements that share the (Identity,
// Origin) pair, the cap MUST evict the worst announcement to make
// room — even if the incoming hop_ack has more hops than the worst
// announcement victim. Trust ordering wins over hop count for the cap
// admission decision, mirroring the relay path's own ranking.
func TestRIBCap_HopAckPromotionEvictsAnnouncement_EvenIfMoreHops(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-low",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-mid",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// Incoming hop_ack with WORSE hop count than either announcement.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-promo",
		Hops: 7, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if status != RouteAccepted {
		t.Fatalf("hop_ack promotion against announcements: expected RouteAccepted, got %v", status)
	}

	nextHops := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
	if _, ok := nextHops["hop-promo"]; !ok {
		t.Fatal("hop_ack candidate must be admitted regardless of hop count")
	}
	// One announcement must have been displaced (the higher-hops one).
	if _, ok := nextHops["hop-mid"]; ok {
		t.Fatal("highest-hops announcement should be the eviction victim")
	}
	if _, ok := nextHops["hop-low"]; !ok {
		t.Fatal("lowest-hops announcement must survive")
	}
}

// TestRIBCap_HopAckPromotionRejected_IfAllKAreHopAckOrDirect_AndNotStrictlyBetter
// covers the negative case of the same trust-tier rule: if every entry
// in a saturated bucket is already at hop_ack (or higher), the cap
// requires the incoming hop_ack to be strictly better by hops. The
// admission floor (isStrictlyBetterForCapLocked) intentionally
// excludes ExpiresAt as a tiebreaker so equal-trust + equal-hops
// re-announces cannot churn the best-K winners; a worse-hops or
// equal-hops hop_ack is dropped, and the bucket cannot be flooded by
// progressively worse-or-equal paths.
func TestRIBCap_HopAckPromotionRejected_IfAllKAreHopAckOrDirect_AndNotStrictlyBetter(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-a",
		Hops: 3, SeqNo: 1, Source: RouteSourceHopAck,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-b",
		Hops: 4, SeqNo: 1, Source: RouteSourceHopAck,
	})

	// Worse-hops hop_ack — must be rejected.
	statusWorse := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-c",
		Hops: 6, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if statusWorse != RouteRejected {
		t.Fatalf("hop_ack with worse hops than worst hop_ack: expected RouteRejected, got %v", statusWorse)
	}

	// Equal-hops hop_ack — also rejected. The cap floor is
	// irreflexive on the (live, trust, hops) tiers and
	// intentionally excludes ExpiresAt, so no equal-trust +
	// equal-hops candidate is ever "strictly better" regardless of
	// when it arrived.
	statusEqual := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-d",
		Hops: 4, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if statusEqual != RouteRejected {
		t.Fatalf("hop_ack equal in trust+hops: expected RouteRejected, got %v", statusEqual)
	}

	// Strictly-better hop_ack (fewer hops) — must be accepted.
	statusBetter := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-e",
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if statusBetter != RouteAccepted {
		t.Fatalf("hop_ack strictly better than worst: expected RouteAccepted, got %v", statusBetter)
	}

	nextHops := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
	if _, ok := nextHops["ack-c"]; ok {
		t.Fatal("worse-hops hop_ack must not be present after rejection")
	}
	if _, ok := nextHops["ack-d"]; ok {
		t.Fatal("equal hop_ack must not be present after rejection")
	}
	if _, ok := nextHops["ack-e"]; !ok {
		t.Fatal("strictly-better hop_ack must be present after acceptance")
	}
}

// TestRIBCap_ExpiredAndWithdrawnAcceptLiveAnnouncement pins the two
// distinct mechanisms that let a fresh live announcement reach a
// "dead" (Lookup-empty) bucket:
//
//   - Expired-but-not-withdrawn rows (Hops < HopsInfinity, ExpiresAt
//     in the past) are still cap-counted, and the LIVENESS PRE-TIER
//     in isWorseForEvictionLocked makes them the first eviction
//     target. A bucket of K expired hop_acks would otherwise
//     dominate a live announcement on trust alone, the announcement
//     would be rejected as "worse than worst", and Lookup — which
//     filters expired entries on the read side — would return zero
//     usable routes until TickTTL reclaimed the rows. Liveness as
//     the first tier makes the cap evict a stale hop_ack and admit
//     the live announcement at the same K.
//
//   - Withdrawn rows (Hops >= HopsInfinity) are NOT cap-counted at
//     all — they bypass the cap entirely and live alongside the
//     K-live slots until TTL reclaims them. So a bucket of K
//     withdrawn hop_acks (e.g. via WithdrawRoute) has bucketCount=0
//     for cap purposes, and a fresh live announcement is admitted
//     into a free slot without any eviction. The withdrawn rows
//     remain in the slice as SeqNo resurrection guards.
//
// Both sub-scenarios end with Lookup returning the live announcement,
// but the path through admitNewLocked is different — the test
// exercises both to pin the design.
func TestRIBCap_ExpiredAndWithdrawnAcceptLiveAnnouncement(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	tbl := NewTable(
		WithClock(clock.nowFunc),
		WithMaxNextHopsPerOrigin(2),
		// Direct routes never enter this scenario — bucket is
		// announcement / hop_ack only.
	)

	// Saturate the bucket with two live hop_acks (trust > announcement
	// so the new announcement loses on trust alone if the liveness
	// tier is missing).
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-a",
		Hops:      3,
		SeqNo:     1,
		Source:    RouteSourceHopAck,
		ExpiresAt: now.Add(60 * time.Second),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-b",
		Hops:      4,
		SeqNo:     1,
		Source:    RouteSourceHopAck,
		ExpiresAt: now.Add(60 * time.Second),
	})

	// Advance the clock past both hop_acks' ExpiresAt — the bucket is
	// now full of expired rows. Lookup would return zero results
	// (it filters expired/withdrawn).
	clock.advance(120 * time.Second)
	if got := len(tbl.Lookup("alice")); got != 0 {
		t.Fatalf("test setup invariant: Lookup should return 0 usable routes when both hop_acks are expired, got %d", got)
	}

	// Incoming: a live announcement (lower trust, distinct NextHop).
	// Without the liveness pre-tier the cap rejects this on trust;
	// with the fix the cap evicts one expired hop_ack to admit the
	// live announcement.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "fresh",
		Hops:      5,
		SeqNo:     1,
		Source:    RouteSourceAnnouncement,
		ExpiresAt: now.Add(120 * time.Second).Add(60 * time.Second),
	})
	if status != RouteAccepted {
		t.Fatalf("live announcement against bucket of expired hop_acks: expected RouteAccepted, got %v "+
			"(cap eviction order missing the liveness pre-tier — dead hop_acks dominated by trust)", status)
	}

	// Lookup must now return the live announcement — the bucket is
	// usable again without waiting for TickTTL.
	live := tbl.Lookup("alice")
	if len(live) == 0 {
		t.Fatal("after admitting the live announcement, Lookup should return at least one usable route")
	}
	foundFresh := false
	for _, r := range live {
		if r.NextHop == "fresh" {
			foundFresh = true
			break
		}
	}
	if !foundFresh {
		t.Fatalf("live announcement was not in Lookup output; got %+v", live)
	}

	// Also exercise the withdrawn-row branch separately: a bucket
	// holding K withdrawn hop_acks. Unlike the expired branch above,
	// withdrawn rows are NOT cap-counted, so the live announcement
	// goes through the "bucket below cap" path (no eviction needed)
	// rather than the liveness-pre-tier replace path. Both
	// withdrawn rows remain in the slice as SeqNo guards.
	tbl2 := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)
	// Two live hop_acks, then withdraw both.
	mustUpdate(t, tbl2, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-a",
		Hops: 3, SeqNo: 1, Source: RouteSourceHopAck,
	})
	mustUpdate(t, tbl2, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "ack-b",
		Hops: 4, SeqNo: 1, Source: RouteSourceHopAck,
	})
	if !tbl2.WithdrawRoute("alice", "bob", "ack-a", 2) {
		t.Fatal("WithdrawRoute(ack-a) returned false")
	}
	if !tbl2.WithdrawRoute("alice", "bob", "ack-b", 2) {
		t.Fatal("WithdrawRoute(ack-b) returned false")
	}
	if got := len(tbl2.Lookup("alice")); got != 0 {
		t.Fatalf("test setup invariant: Lookup should return 0 usable routes when both hop_acks are withdrawn, got %d", got)
	}

	statusW := mustUpdate(t, tbl2, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "fresh",
		Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if statusW != RouteAccepted {
		t.Fatalf("live announcement against bucket of withdrawn hop_acks: expected RouteAccepted, got %v",
			statusW)
	}

	// Both withdrawn hop_acks must still be in the slice — they are
	// resurrection guards, not cap-counted competitors.
	bucket := tbl2.Snapshot().Routes["alice"]
	withdrawnCount := 0
	for _, r := range bucket {
		if r.IsWithdrawn() {
			withdrawnCount++
		}
	}
	if withdrawnCount != 2 {
		t.Fatalf("withdrawn rows should survive a live admission (cap-bypass), got %d withdrawn", withdrawnCount)
	}
}

// TestRIBCap_WithdrawalTombstonesBypassCap pins the design that
// withdrawal tombstones live OUTSIDE the K-counted slots: the cap
// counts only non-tombstone entries, and tombstones are appended
// unconditionally on the WithdrawRoute idx<0 path. This is what keeps
// the SeqNo resurrection guard intact under cap pressure — see
// TestRIBCap_TombstoneSurvivesCapPressure_BlocksResurrection for the
// negative case.
//
// The cap invariant after this design is "at most K LIVE
// (non-tombstone) entries per (Identity, Origin)". The total number
// of rows in the slice can temporarily exceed K when recent
// withdrawals contributed tombstones; TickTTL reclaims them after
// defaultTTL.
func TestRIBCap_WithdrawalTombstonesBypassCap(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("tombstone_admitted_even_when_K_live_routes_saturate_bucket", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// Saturate the bucket with two live announcements.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		statsBefore := tbl.CapStats()

		// Wire withdrawal arrives for a NextHop the bucket has never
		// seen. The tombstone bypasses the cap and lands as an
		// extra row alongside the two live entries.
		applied := tbl.WithdrawRoute("alice", "bob", "hop-c", 5)
		if !applied {
			t.Fatal("WithdrawRoute should return true — tombstones bypass the cap and always land")
		}

		// Total slice size = K live + 1 tombstone.
		bucket := tbl.Snapshot().Routes["alice"]
		if got := len(bucket); got != 3 {
			t.Fatalf("expected K=2 live + 1 tombstone = 3 rows, got %d", got)
		}

		// And the cap REJECTION counters are NOT advanced — the
		// tombstone never went through admitNewLocked.
		statsAfter := tbl.CapStats()
		if statsAfter.RejectedFull != statsBefore.RejectedFull {
			t.Fatalf("RejectedFull should not advance for tombstone admissions; %d → %d",
				statsBefore.RejectedFull, statsAfter.RejectedFull)
		}
		if statsAfter.RejectedAllProtected != statsBefore.RejectedAllProtected {
			t.Fatalf("RejectedAllProtected should not advance for tombstones; %d → %d",
				statsBefore.RejectedAllProtected, statsAfter.RejectedAllProtected)
		}
		if statsAfter.Accepted != statsBefore.Accepted {
			t.Fatalf("Accepted should not advance for tombstones (they bypass admitNewLocked); %d → %d",
				statsBefore.Accepted, statsAfter.Accepted)
		}
	})

	t.Run("live_admission_after_tombstones_still_caps_at_K_live", func(t *testing.T) {
		// Tombstones don't count against the cap, but live
		// admissions still do — so a bucket with 1 live + 2
		// tombstones still has K-1 = 1 free live slot at K=2.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		// Two tombstones via WithdrawRoute idx<0.
		if !tbl.WithdrawRoute("alice", "bob", "hop-x", 1) {
			t.Fatal("setup: tombstone hop-x not appended")
		}
		if !tbl.WithdrawRoute("alice", "bob", "hop-y", 1) {
			t.Fatal("setup: tombstone hop-y not appended")
		}

		// Bucket: 1 live + 2 tombstones = 3 rows, but cap-counted
		// (live) = 1, so the cap admits one more live route.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 4, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("second live announcement should fit alongside tombstones at cap=K=2 live; got %v", status)
		}

		// A THIRD live announcement must be rejected — cap is
		// counted by live entries.
		status = mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if status != RouteRejected {
			t.Fatalf("third live announcement at K=2 live cap should be rejected, got %v", status)
		}
	})
}

// TestRIBCap_UpdateRouteWithdrawalBypassesCapForNewTriple pins the
// invariant that the public Table.UpdateRoute API entry point — a
// withdrawal entry (Hops = HopsInfinity) for an (Identity, Origin,
// NextHop) triple not yet in the bucket — bypasses admitNewLocked
// exactly like the internal WithdrawRoute idx<0 path. Without the
// short-circuit, a saturated live bucket would route the tombstone
// through admitNewLocked, which counts only live entries (rule 1
// strips the tombstone from bucketCount) and rejects the new entry
// on RejectedFull or RejectedAllProtected — silently dropping the
// SeqNo resurrection guard for the new triple even though the cap
// model leaves room for tombstones outside the K-counted slots.
//
// Companion to TestRIBCap_WithdrawalTombstonesBypassCap, which
// exercises the same invariant via the internal WithdrawRoute API.
// The two paths must be symmetric — there is no public/private split
// in the tombstone-bypass model.
func TestRIBCap_UpdateRouteWithdrawalBypassesCapForNewTriple(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("tombstone_admitted_via_UpdateRoute_when_K_live_routes_saturate_bucket", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// Saturate the bucket with K=2 live announcements.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		statsBefore := tbl.CapStats()

		// Direct UpdateRoute with a tombstone for a brand-new NextHop.
		// The idx<0 short-circuit must accept it without consulting
		// the cap.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: HopsInfinity, SeqNo: 10, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("UpdateRoute with HopsInfinity for new triple at saturated K live: expected RouteAccepted, got %v", status)
		}

		// Total slice = K live + 1 tombstone.
		bucket := tbl.Snapshot().Routes["alice"]
		if got := len(bucket); got != 3 {
			t.Fatalf("expected K=2 live + 1 tombstone = 3 rows, got %d", got)
		}
		liveCount, tombstoneCount := 0, 0
		for _, r := range bucket {
			if r.IsWithdrawn() {
				tombstoneCount++
			} else {
				liveCount++
			}
		}
		if liveCount != 2 || tombstoneCount != 1 {
			t.Fatalf("expected 2 live + 1 tombstone, got %d live + %d tombstones", liveCount, tombstoneCount)
		}

		// Cap counters must NOT advance — the tombstone short-circuited
		// out of admitNewLocked, so neither acceptance nor rejection
		// counters should change.
		statsAfter := tbl.CapStats()
		if statsAfter.Accepted != statsBefore.Accepted {
			t.Fatalf("Accepted should not advance for the tombstone path; %d → %d",
				statsBefore.Accepted, statsAfter.Accepted)
		}
		if statsAfter.AcceptedReplaced != statsBefore.AcceptedReplaced {
			t.Fatalf("AcceptedReplaced should not advance; %d → %d",
				statsBefore.AcceptedReplaced, statsAfter.AcceptedReplaced)
		}
		if statsAfter.RejectedFull != statsBefore.RejectedFull {
			t.Fatalf("RejectedFull should not advance — the tombstone bypassed the cap, not got rejected; %d → %d",
				statsBefore.RejectedFull, statsAfter.RejectedFull)
		}
		if statsAfter.RejectedAllProtected != statsBefore.RejectedAllProtected {
			t.Fatalf("RejectedAllProtected should not advance; %d → %d",
				statsBefore.RejectedAllProtected, statsAfter.RejectedAllProtected)
		}
	})

	t.Run("tombstone_via_UpdateRoute_blocks_delayed_lower_SeqNo_announcement", func(t *testing.T) {
		// The point of routing tombstones through UpdateRoute idx<0
		// at all (rather than forcing every caller through
		// WithdrawRoute) is to preserve the SeqNo resurrection guard
		// for the new triple. If admitNewLocked had rejected the
		// tombstone, a delayed announcement with a lower SeqNo for
		// the same (Identity, Origin, NextHop) would not find a
		// tombstone via the idx>=0 path in UpdateRoute and could
		// resurrect the lineage through cap admission alone.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})

		// Origin issues a withdrawal at SeqNo=10 for a brand-new
		// NextHop. Goes through the public UpdateRoute API (the path
		// under test), not WithdrawRoute. The tombstone short-circuit
		// must accept it despite the saturated live bucket.
		if status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: HopsInfinity, SeqNo: 10, Source: RouteSourceAnnouncement,
		}); status != RouteAccepted {
			t.Fatalf("setup: tombstone should be accepted via UpdateRoute idx<0 short-circuit, got %v", status)
		}

		// Delayed live announcement for the same triple at LOWER
		// SeqNo. UpdateRoute now finds the tombstone via idx>=0
		// (DedupKey match), and the stale-SeqNo branch rejects it.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 2, SeqNo: 4, Source: RouteSourceAnnouncement,
		})
		if status != RouteRejected {
			t.Fatalf("delayed lower-SeqNo announcement should be blocked by tombstone, got %v", status)
		}

		// Bucket unchanged: 2 live + 1 tombstone; NextHop=hop-c is
		// still a tombstone (no resurrection).
		bucket := tbl.Snapshot().Routes["alice"]
		var hopCEntry *RouteEntry
		for i := range bucket {
			if bucket[i].NextHop == "hop-c" {
				hopCEntry = &bucket[i]
				break
			}
		}
		if hopCEntry == nil {
			t.Fatal("hop-c entry vanished from the bucket")
		}
		if !hopCEntry.IsWithdrawn() {
			t.Fatalf("hop-c should still be a tombstone after blocked resurrection; Hops=%d", hopCEntry.Hops)
		}
		if hopCEntry.SeqNo != 10 {
			t.Fatalf("hop-c SeqNo should still be 10 (tombstone preserved), got %d", hopCEntry.SeqNo)
		}
	})
}

// TestRIBCap_TombstoneToLivePromotionRespectsKLiveCap pins the
// invariant that resurrecting a tombstone via a newer-SeqNo live
// announcement still goes through cap admission. Without the fix, the
// SeqNo>old.SeqNo branch in UpdateRoute would overwrite the tombstone
// slot in place — which doesn't increase the slice length but DOES
// flip a non-cap-counted row (the tombstone) into a cap-counted row
// (the live entry), pushing live count from K to K+1.
//
// The promotion now temporarily detaches the tombstone slot, runs
// admitNewLocked on the live entry, and either lets it append/evict
// like any fresh admission or restores the tombstone on rejection
// (preserving the SeqNo guard).
func TestRIBCap_TombstoneToLivePromotionRespectsKLiveCap(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("admitted_when_bucket_below_K_live", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// One live route + one tombstone (different NextHop). Live
		// count = 1, K = 2 — there's room for one more live.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if !tbl.WithdrawRoute("alice", "bob", "hop-c", 5) {
			t.Fatal("setup: tombstone hop-c not appended")
		}

		// Newer-SeqNo live announcement for hop-c — promotion path.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 4, SeqNo: 7, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("tombstone-to-live promotion with bucket below K live: expected RouteAccepted, got %v", status)
		}

		// Bucket now has 2 live entries (hop-a + hop-c) and zero
		// tombstones (the tombstone slot was consumed by the
		// promotion).
		liveCount := 0
		tombstoneCount := 0
		for _, r := range tbl.Snapshot().Routes["alice"] {
			if r.IsWithdrawn() {
				tombstoneCount++
			} else {
				liveCount++
			}
		}
		if liveCount != 2 {
			t.Fatalf("expected 2 live entries after promotion, got %d", liveCount)
		}
		if tombstoneCount != 0 {
			t.Fatalf("expected the tombstone slot to be consumed by the promotion, got %d tombstones", tombstoneCount)
		}
	})

	t.Run("rejected_when_K_live_and_promotion_not_strictly_better", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// K=2 live + 1 tombstone (different NextHop).
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if !tbl.WithdrawRoute("alice", "bob", "hop-c", 5) {
			t.Fatal("setup: tombstone hop-c not appended")
		}

		// Promotion attempt — incoming Hops=10 is worse than worst
		// live (hop-b, Hops=3), so admitNewLocked rejects with
		// AdmissionRejectedFull.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 10, SeqNo: 7, Source: RouteSourceAnnouncement,
		})
		if status != RouteRejected {
			t.Fatalf("promotion to live worse than worst at K=2 live: expected RouteRejected, got %v", status)
		}

		// Live count is still K=2; tombstone for hop-c was restored
		// by the rejection branch — SeqNo guard is back in place.
		liveCount := 0
		var tombstoneSeq uint64
		var foundTombstone bool
		for _, r := range tbl.Snapshot().Routes["alice"] {
			if r.IsWithdrawn() {
				if r.NextHop == "hop-c" {
					foundTombstone = true
					tombstoneSeq = r.SeqNo
				}
			} else {
				liveCount++
			}
		}
		if liveCount != 2 {
			t.Fatalf("live count after rejected promotion = %d, want 2 (cap invariant violated)", liveCount)
		}
		if !foundTombstone {
			t.Fatal("tombstone for hop-c was lost on rejection — SeqNo guard dropped")
		}
		// Tombstone keeps its original SeqNo, NOT the rejected
		// promotion's higher SeqNo. The promotion never landed.
		if tombstoneSeq != 5 {
			t.Fatalf("tombstone SeqNo after rejected promotion = %d, want 5 (original)", tombstoneSeq)
		}
	})

	t.Run("admitted_with_eviction_when_K_live_and_promotion_strictly_better", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// K=2 live with WORSE hops than the upcoming promotion + 1 tombstone.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 5, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 6, SeqNo: 5, Source: RouteSourceAnnouncement,
		})
		if !tbl.WithdrawRoute("alice", "bob", "hop-c", 5) {
			t.Fatal("setup: tombstone hop-c not appended")
		}

		// Promotion with Hops=2 — strictly better than worst live
		// (hop-b, Hops=6). admitNewLocked evicts hop-b and admits
		// hop-c as live.
		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("promotion strictly better than worst: expected RouteAccepted, got %v", status)
		}

		// Live count still K=2 — promotion ate the worst live slot
		// AND consumed the tombstone slot. hop-a survives, hop-c is
		// the new live, hop-b is gone.
		next := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
		if _, ok := next["hop-a"]; !ok {
			t.Fatal("hop-a (best live) should survive promotion-eviction")
		}
		if _, ok := next["hop-b"]; ok {
			t.Fatal("hop-b (worst live) should have been evicted by the promotion")
		}
		if r, ok := next["hop-c"]; !ok || r.IsWithdrawn() {
			t.Fatalf("hop-c should now be live (promoted), got %+v / present=%v", r, ok)
		}
		// Live count is K=2.
		liveCount := 0
		for _, r := range tbl.Snapshot().Routes["alice"] {
			if !r.IsWithdrawn() {
				liveCount++
			}
		}
		if liveCount != 2 {
			t.Fatalf("live count after promotion-with-eviction = %d, want 2", liveCount)
		}
	})
}

// TestRIBCap_TombstoneSurvivesCapPressure_BlocksResurrection is the
// negative regression for the SeqNo resurrection guard. With
// withdrawal tombstones bypassing the cap, a stale lower-SeqNo
// announcement that arrives later for the same (Identity, Origin,
// NextHop) triple hits the idx>=0 path in UpdateRoute, finds the
// tombstone, and gets rejected via the existing
// "tombstone-protects-against-stale-SeqNo" branch.
//
// If the cap design ever regresses to admit-cap-tombstones-or-drop,
// this test fails on the second UpdateRoute: the stale announcement
// would be admitted via cap eviction (its Hops are strictly better
// than the worst retained live route's), restoring a withdrawn
// lineage in violation of the SeqNo invariant in docs/routing.md.
func TestRIBCap_TombstoneSurvivesCapPressure_BlocksResurrection(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)

	// Saturate the live-bucket with K=2 routes that have WORSE
	// hops than the stale announcement we will inject below — so
	// without the resurrection guard the cap would happily evict
	// one of them in favour of the lower-SeqNo announcement.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-a",
		Hops: 5, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-b",
		Hops: 6, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// Origin issues a withdrawal for a NextHop the bucket has
	// never seen. Tombstone bypasses the cap and lands as a third
	// row in the slice. SeqNo=10 — this is the guard.
	if !tbl.WithdrawRoute("alice", "bob", "hop-c", 10) {
		t.Fatal("setup: WithdrawRoute(hop-c, seq=10) returned false")
	}

	// A delayed lower-SeqNo announcement arrives for the same
	// (Identity, Origin, NextHop=hop-c). Hops=2 is BETTER than
	// any of the live entries (5, 6), so without the tombstone
	// the cap floor would happily evict hop-b. With the
	// tombstone, UpdateRoute hits idx>=0 and the
	// SeqNo-based stale-vs-tombstone check kicks in: incoming
	// SeqNo=4 < tombstone SeqNo=10 → RouteRejected.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "hop-c",
		Hops: 2, SeqNo: 4, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("delayed announcement: unexpected validation error: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("delayed lower-SeqNo announcement should be rejected by the tombstone "+
			"(SeqNo guard), got %v — a regression that lets cap pressure drop the "+
			"tombstone lets stale announcements resurrect withdrawn lineages", status)
	}

	// And the stale announcement must NOT be in the live set.
	for _, r := range tbl.Lookup("alice") {
		if r.NextHop == "hop-c" {
			t.Fatalf("stale announcement leaked through; Lookup returned %+v", r)
		}
	}
}

// --- §9.2 system behaviour ---

// TestRIBCap_LookupReturnsAtMostK_SortedByTrustHopsFreshness verifies
// that the read path observes the cap end-to-end: regardless of how
// many announcements arrived, Lookup returns at most K entries, and
// they are sorted by the existing Lookup contract (trust DESC, hops
// ASC). The sort step is exercised by Lookup itself; the cap simply
// ensures the slice it sorts is bounded.
func TestRIBCap_LookupReturnsAtMostK_SortedByTrustHopsFreshness(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(3),
	)

	// Inject many announcements; the cap should retain the best three
	// by (trust, hops, expiry). All have the same trust and expiry, so
	// the surviving three are the three lowest-hops candidates.
	for i, hop := range []struct {
		nh   PeerIdentity
		hops int
	}{
		{"hop-a", 7},
		{"hop-b", 2},
		{"hop-c", 5},
		{"hop-d", 3},
		{"hop-e", 9},
	} {
		_ = i
		_, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: hop.nh,
			Hops: hop.hops, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("UpdateRoute %s: %v", hop.nh, err)
		}
	}

	got := tbl.Lookup("alice")
	if len(got) > 3 {
		t.Fatalf("Lookup returned %d entries; cap=3 must clamp the slice to at most 3", len(got))
	}
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 surviving routes, got %d: %+v", len(got), got)
	}
	// Sort contract: trust DESC, hops ASC. All three are announcements
	// with the same trust, so the order is hops-ascending: 2, 3, 5.
	wantHops := []int{2, 3, 5}
	for i, want := range wantHops {
		if got[i].Hops != want {
			t.Fatalf("Lookup result[%d].Hops = %d, want %d (full slice: %+v)", i, got[i].Hops, want, got)
		}
	}
}

// TestRIBCap_RemoveDirectPeer_ExposesBackupsFromK verifies that the
// ExposedBackups signal still fires correctly when the cap is in
// effect. RemoveDirectPeer walks the routes for the affected
// destination and reports it as exposed if any non-withdrawn,
// non-expired survivor exists. With cap=4 holding 1 direct + 3
// announcements, removing the direct peer must surface the
// announcements as backups exactly the same way as without the cap.
func TestRIBCap_RemoveDirectPeer_ExposesBackupsFromK(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin("self"),
		WithMaxNextHopsPerOrigin(4),
	)

	// Direct route via "alice" (Origin must be local for direct).
	mustAddDirect(t, tbl, "alice")

	// Three transit routes to "alice" via different next-hops, learned
	// through transit announcements with a different Origin so they
	// share neither bucket nor protected status with the direct entry.
	for i, nh := range []PeerIdentity{"hop-x", "hop-y", "hop-z"} {
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "transit-origin", NextHop: nh,
			Hops: 2 + i, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
	}

	result := mustRemoveDirect(t, tbl, "alice")

	// The direct route was withdrawn (own-origin → wire withdrawal).
	if len(result.Withdrawals) == 0 {
		t.Fatal("RemoveDirectPeer should emit at least one wire withdrawal for the own-origin direct route")
	}

	// At least one transit route survived, so the destination must be
	// reported as exposed.
	exposedFound := false
	for _, id := range result.ExposedBackups {
		if id == "alice" {
			exposedFound = true
			break
		}
	}
	if !exposedFound {
		t.Fatalf("RemoveDirectPeer with surviving transit backups did not expose 'alice' "+
			"in ExposedBackups; got %v", result.ExposedBackups)
	}
}

// TestRIBCap_AnnounceWireSchemaUnchanged_ContentReflectsBestK pins the
// part of the wire contract the cap actually preserves: the wire
// SCHEMA is unchanged — `BuildAnnounceSnapshot` still produces exactly
// one `AnnounceEntry` per Identity regardless of K (post-hot-fix
// stage-4 aggregation collapses multiple Origin lineages to one winner
// per destination), and the surviving entry's Identity agrees between
// capped and cap-disabled tables. The wire CONTENT (Hops/Extra of the
// surviving entry) reflects whatever the cap's best-K rows happen to
// be — see TestRIBCap_AnnounceContentDiffersWhenCapDropsLowerHopsWireWinner
// for the case where the two projections actually disagree on Hops.
//
// This test uses a single Origin per Identity so the post-hot-fix
// per-Identity aggregation is observationally identical to the
// pre-hot-fix per-(Identity, Origin) aggregation — both produce 1
// entry. The pin still holds.
//
// This test uses a homogeneous announcement bucket — every entry shares
// the same Source (RouteSourceAnnouncement), so cap-rank and wire-rank
// agree on which two rows to keep, and the surviving wire content
// matches between the two pipelines. That is the easy half of the
// contract; the trust-mismatch sibling test below covers the hard
// half.
func TestRIBCap_AnnounceWireSchemaUnchanged_ContentReflectsBestK(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	// Build two tables, one with cap, one without — feed each the same
	// announce sequence, then compare the wire projection.
	makeTable := func(cap int) *Table {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(cap),
		)
		for i, nh := range []PeerIdentity{"hop-a", "hop-b", "hop-c", "hop-d"} {
			_, err := tbl.UpdateRoute(RouteEntry{
				Identity: "alice", Origin: "bob", NextHop: nh,
				Hops: 2 + i, SeqNo: 1, Source: RouteSourceAnnouncement,
			})
			if err != nil {
				t.Fatalf("seed UpdateRoute %s: %v", nh, err)
			}
		}
		return tbl
	}

	withCap := makeTable(2).AnnounceTo("excluded")
	withoutCap := makeTable(0).AnnounceTo("excluded")

	// AnnounceTo row counts MUST track the cap — the wire pipeline's
	// aggregation step is what collapses these into one entry, not
	// AnnounceTo itself.
	if len(withCap) != 2 {
		t.Fatalf("cap=2 AnnounceTo should produce one row per stored RouteEntry (2), got %d", len(withCap))
	}
	if len(withoutCap) != 4 {
		t.Fatalf("cap=0 AnnounceTo should produce one row per stored RouteEntry (4), got %d", len(withoutCap))
	}

	// Run the production wire-aggregation pass. After BuildAnnounceSnapshot
	// both projections must collapse to exactly one live winner entry per
	// Identity (per-Identity Stage 4 aggregation hot-fix) — no own-origin
	// tombstones in this scenario because all entries are live announcements.
	// That single live winner is the wire schema invariant the cap preserves.
	withCapWire := BuildAnnounceSnapshot(withCap)
	withoutCapWire := BuildAnnounceSnapshot(withoutCap)
	if got := len(withCapWire.Entries); got != 1 {
		t.Fatalf("cap=2 wire-aggregated snapshot should have 1 live winner entry per Identity, got %d: %+v",
			got, withCapWire.Entries)
	}
	if got := len(withoutCapWire.Entries); got != 1 {
		t.Fatalf("cap=0 wire-aggregated snapshot should have 1 live winner entry per Identity, got %d: %+v",
			got, withoutCapWire.Entries)
	}
	// The (Identity, Origin) of the surviving entry agrees — wire
	// schema invariant. Single-Origin scenario, so per-Identity Stage 4
	// aggregation reduces to per-(Identity, Origin) collapse — same
	// observable result as pre-hot-fix.
	if withCapWire.Entries[0].Identity != withoutCapWire.Entries[0].Identity ||
		withCapWire.Entries[0].Origin != withoutCapWire.Entries[0].Origin {
		t.Fatalf("cap-vs-uncapped wire projection diverges on (Identity, Origin): cap=%+v, uncapped=%+v",
			withCapWire.Entries[0], withoutCapWire.Entries[0])
	}
	// In this homogeneous-trust scenario the SURVIVING Hops also agree:
	// cap-rank picks {Hops=2, Hops=3}, BuildAnnounceSnapshot picks min
	// (Hops=2); cap=0 keeps all four, BuildAnnounceSnapshot still picks
	// Hops=2. Pin this to make the contrast with the trust-mismatch
	// test explicit.
	if withCapWire.Entries[0].Hops != withoutCapWire.Entries[0].Hops {
		t.Fatalf("homogeneous-trust scenario should produce identical Hops in the surviving wire entry: "+
			"cap=%d, uncapped=%d", withCapWire.Entries[0].Hops, withoutCapWire.Entries[0].Hops)
	}
}

// TestRIBCap_AnnounceContentDiffersWhenCapDropsLowerHopsWireWinner pins
// the part of the wire contract the cap deliberately does NOT preserve:
// when the cap eviction order (`live > dead` → trust → hops → expiry)
// disagrees with the wire-aggregation order (min Hops, Source-blind),
// a tight cap can keep the hop_ack with worse Hops and drop the
// announcement that would have been the wire winner. Capped and
// uncapped nodes then announce DIFFERENT Hops for the same
// `(Identity, Origin)` pair.
//
// This is the explicit trade-off the cap makes — confidence (trust
// tier) over hop count for routing decisions, mirroring Lookup's
// ordering, even at the cost of announcing a longer-but-confirmed
// path. The test pins the trade-off so a future eviction-order
// refactor that "fixes" this by collapsing to wire-rank cannot land
// without explicit reconsideration.
func TestRIBCap_AnnounceContentDiffersWhenCapDropsLowerHopsWireWinner(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	makeTable := func(cap int) *Table {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(cap),
		)
		// Both rows share (Identity, Origin, SeqNo) — they only
		// differ in NextHop / Source / Hops. This is the case the
		// wire pipeline aggregates with min Hops.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-ack",
			Hops:   7,
			SeqNo:  1,
			Source: RouteSourceHopAck,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-announce",
			Hops:   2,
			SeqNo:  1,
			Source: RouteSourceAnnouncement,
		})
		return tbl
	}

	// Cap=1: cap-rank prefers hop_ack (higher trust) → bucket holds
	// only the hop_ack with Hops=7.
	withCap := makeTable(1)
	if got := len(withCap.Snapshot().Routes["alice"]); got != 1 {
		t.Fatalf("cap=1 bucket should hold exactly one entry after admission ranking, got %d", got)
	}
	withCapWire := BuildAnnounceSnapshot(withCap.AnnounceTo("excluded"))
	if got := len(withCapWire.Entries); got != 1 {
		t.Fatalf("cap=1 wire snapshot should have 1 entry, got %d", got)
	}
	if got := withCapWire.Entries[0].Hops; got != 7 {
		t.Fatalf("cap=1 should announce the hop_ack the cap retained (Hops=7), got %d", got)
	}

	// Cap=0: both rows live in the table, BuildAnnounceSnapshot picks
	// min Hops (the announcement) for the wire because Source is not
	// on the wire and tier-1 of the wire-rank is min Hops.
	withoutCap := makeTable(0)
	if got := len(withoutCap.Snapshot().Routes["alice"]); got != 2 {
		t.Fatalf("cap=0 bucket should hold both rows, got %d", got)
	}
	withoutCapWire := BuildAnnounceSnapshot(withoutCap.AnnounceTo("excluded"))
	if got := len(withoutCapWire.Entries); got != 1 {
		t.Fatalf("cap=0 wire snapshot should have 1 entry, got %d", got)
	}
	if got := withoutCapWire.Entries[0].Hops; got != 2 {
		t.Fatalf("cap=0 should announce the announcement (min Hops=2), got %d", got)
	}

	// The two pipelines AGREE on (Identity, Origin) — wire schema is
	// preserved — but DISAGREE on Hops, which is the documented
	// content trade-off.
	if withCapWire.Entries[0].Identity != withoutCapWire.Entries[0].Identity ||
		withCapWire.Entries[0].Origin != withoutCapWire.Entries[0].Origin {
		t.Fatalf("schema invariant violated: cap=%+v, uncapped=%+v",
			withCapWire.Entries[0], withoutCapWire.Entries[0])
	}
	if withCapWire.Entries[0].Hops == withoutCapWire.Entries[0].Hops {
		t.Fatalf("expected Hops to diverge between capped and uncapped wire content "+
			"(cap=trust-first vs wire=hops-first), but both got Hops=%d",
			withCapWire.Entries[0].Hops)
	}
}

// TestRIBCap_MixedVersionMesh_WorksWithUnboundedPeers simulates a
// receiver running the cap against a sender that does not — the
// "mixed-version mesh" scenario. The cap is a LOCAL ingestion policy
// that exercises the same UpdateRoute entry point the wire path uses,
// so a burst of announcements from a hypothetical uncapped peer must
// converge to at most K stored entries on the capped receiver without
// any wire-level coordination.
//
// The burst sweeps Hops from 3 up to a value still inside the
// validator's 1..HopsInfinity range — RouteEntry.Validate rejects
// Hops > 16 outright, so we cap the burst at 13 iterations
// (Hops 3..15). The cap floor is still exercised: the first two
// admitted entries (Hops 3 and 4) saturate the bucket, the remaining
// 11 are strictly worse and must be rejected.
func TestRIBCap_MixedVersionMesh_WorksWithUnboundedPeers(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithMaxNextHopsPerOrigin(2),
	)

	// 13 announcements arriving from an "uncapped peer" — same
	// (Identity, Origin), distinct next-hops, monotonically worse
	// hops in [3, 15]. The cap must stabilize the bucket at exactly
	// K=2 regardless of the burst size.
	const burst = 13
	for i := 0; i < burst; i++ {
		hops := 3 + i // 3..15 — all valid (RouteEntry rejects Hops > HopsInfinity=16).
		_, err := tbl.UpdateRoute(RouteEntry{
			Identity: "alice", Origin: "bob",
			// Construct a fresh next-hop identity per iteration; the
			// exact bytes don't matter, only that the dedup triple
			// changes each call so we exercise the new-triple path.
			NextHop: PeerIdentity("hop-" + string(rune('a'+i%26)) + string(rune('a'+i/26))),
			Hops:    hops,
			SeqNo:   1,
			Source:  RouteSourceAnnouncement,
		})
		if err != nil {
			t.Fatalf("burst UpdateRoute #%d (Hops=%d): %v", i, hops, err)
		}
	}

	got := len(tbl.Snapshot().Routes["alice"])
	if got != 2 {
		t.Fatalf("after a %d-entry burst the bucket must equal cap=2, got %d", burst, got)
	}

	// And the survivors must be the two lowest-hops candidates —
	// monotonically ascending hops mean the first two admitted were
	// the best, and the cap floor protects them from later worse
	// candidates.
	for _, r := range tbl.Snapshot().Routes["alice"] {
		if r.Hops > 4 {
			t.Fatalf("survivor has Hops=%d but the two best candidates had Hops=3 and Hops=4 — cap floor leaked",
				r.Hops)
		}
	}
}

// TestRIBCap_StrictBetterFloor_EqualHopsEqualTrust_KeepsIncumbent
// pins the cap stability invariant against ExpiresAt-based churn.
//
// Root cause it fixes: an earlier version of
// isStrictlyBetterForCapLocked used `cand.ExpiresAt.After(worst.ExpiresAt)`
// as the final tiebreaker, mirroring the eviction key. Every
// re-announce arrives with `ExpiresAt = now + defaultTTL`, strictly
// later than any existing entry's ExpiresAt, so on every announce
// cycle the floor admitted the re-announce, displaced an incumbent,
// and the bucket churned through every available next-hop. In a
// dense mesh this produced 90%+ eviction-rate (a node with K=4 and
// 22 direct peers saw ~165 displacement decisions per second on
// the routing-table cap), routes flapped between Lookup() reads, and
// the file router could not pick stable paths.
//
// The fix narrows the floor: equal-liveness + equal-trust +
// equal-hops returns false (incumbent wins). ExpiresAt remains in
// the eviction key (isWorseForEvictionLocked) for the orthogonal
// "which bucket member to evict GIVEN admission decided to evict"
// question.
//
// Sub-tests cover both halves of the contract: (a) ExpiresAt-only
// "betterness" must not displace, (b) real improvements (lower
// hops, higher trust) still displace.
func TestRIBCap_StrictBetterFloor_EqualHopsEqualTrust_KeepsIncumbent(t *testing.T) {
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	t.Run("equal_trust_equal_hops_later_expires_rejected", func(t *testing.T) {
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)

		// Saturate K=2 with announcements at Hops=3, equal trust.
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})

		// Read incumbent's ExpiresAt and craft a candidate with
		// strictly-later ExpiresAt — simulates the wall-clock-
		// advanced re-announce that produced the production
		// thrashing in dense meshes.
		var incumbentExpires time.Time
		for _, r := range tbl.Snapshot().Routes["alice"] {
			if r.NextHop == "hop-a" {
				incumbentExpires = r.ExpiresAt
				break
			}
		}
		if incumbentExpires.IsZero() {
			t.Fatal("setup: incumbent ExpiresAt should have been filled by UpdateRoute")
		}

		statsBefore := tbl.CapStats()

		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
			ExpiresAt: incumbentExpires.Add(time.Second),
		})
		if status != RouteRejected {
			t.Fatalf("expected RouteRejected — equal-trust equal-hops candidate must not displace incumbent on ExpiresAt alone, got %v", status)
		}

		statsAfter := tbl.CapStats()
		if statsAfter.AcceptedReplaced != statsBefore.AcceptedReplaced {
			t.Fatalf("AcceptedReplaced should NOT advance on equal-trust equal-hops re-announce; %d → %d",
				statsBefore.AcceptedReplaced, statsAfter.AcceptedReplaced)
		}
		if statsAfter.RejectedFull != statsBefore.RejectedFull+1 {
			t.Fatalf("RejectedFull should advance by exactly 1; %d → %d",
				statsBefore.RejectedFull, statsAfter.RejectedFull)
		}

		bucket := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
		if _, hasA := bucket["hop-a"]; !hasA {
			t.Fatal("hop-a (incumbent) should still be in bucket")
		}
		if _, hasB := bucket["hop-b"]; !hasB {
			t.Fatal("hop-b (incumbent) should still be in bucket")
		}
		if _, hasC := bucket["hop-c"]; hasC {
			t.Fatal("hop-c should NOT have been admitted on ExpiresAt-only 'betterness'")
		}
		if got := len(bucket); got != 2 {
			t.Fatalf("bucket should still hold the original K=2 incumbents, got %d entries", got)
		}
	})

	t.Run("strictly_lower_hops_still_displaces", func(t *testing.T) {
		// Sanity: real improvements still pass the floor — the
		// fix narrows the floor only for the equal-on-all-tiers
		// case.
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
		})

		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if status != RouteAccepted {
			t.Fatalf("expected RouteAccepted — Hops=2 strictly better than incumbents' Hops=5, got %v", status)
		}

		bucket := nextHopsByOrigin(t, tbl.Snapshot(), "alice", "bob")
		if _, hasC := bucket["hop-c"]; !hasC {
			t.Fatal("hop-c should have been admitted via strict-better-by-hops")
		}
		liveCount := 0
		for _, r := range bucket {
			if !r.IsWithdrawn() {
				liveCount++
			}
		}
		if liveCount != 2 {
			t.Fatalf("expected K=2 live entries after displacement, got %d", liveCount)
		}
	})

	t.Run("strictly_higher_trust_still_displaces", func(t *testing.T) {
		// Same sanity from the trust tier: hop_ack displaces
		// announcement at equal hops because trust strictly wins
		// (hop_ack > announcement on the trust ladder).
		tbl := NewTable(
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(2),
		)
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-a",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-b",
			Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		})

		status := mustUpdate(t, tbl, RouteEntry{
			Identity: "alice", Origin: "bob", NextHop: "hop-c",
			Hops: 3, SeqNo: 1, Source: RouteSourceHopAck,
		})
		if status != RouteAccepted {
			t.Fatalf("expected RouteAccepted — hop_ack strictly better than announcement at equal hops, got %v", status)
		}
	})
}

// TestRIBCap_AddDirectPeerRespectsCap pins the invariant that
// AddDirectPeer goes through cap admission so the bucket never holds
// more than K live entries per (Identity, Origin) — even though
// direct admission itself must always succeed.
//
// Direct routes have NextHop == Identity (Validate enforces this), so
// the (peer, self) bucket can in principle accumulate one direct row
// (NextHop=peer) plus K-1 non-direct rows (announcement / hop_ack
// confirmations to the same destination via different intermediates).
// Without cap-aware admission, AddDirectPeer's bare append/in-place
// overwrite was the last public Table API path that could push live
// count from K to K+1: the regular UpdateRoute path was already
// migrated to admitNewLocked in earlier rounds, but
// AddDirectPeer/idx<0 and AddDirectPeer/tombstone-reactivation were
// not.
//
// admitDirectLocked closes that gap: it never rejects (a direct
// entry models a session the OS already holds open, refusing to
// record it would silently desync the table from the transport
// layer), but it evicts the worst evictable row when the bucket is
// saturated. The cap counter `acceptedReplaced` is incremented on
// the displacement path so operators see the cap actually fired.
func TestRIBCap_AddDirectPeerRespectsCap(t *testing.T) {
	const localOrigin PeerIdentity = "self-node-identity"
	const peerID PeerIdentity = "peer-node-identity"
	const otherHop PeerIdentity = "other-hop-identity"

	t.Run("new_direct_evicts_worst_evictable_when_bucket_saturated", func(t *testing.T) {
		now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
		tbl := NewTable(
			WithLocalOrigin(localOrigin),
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(1),
		)

		// Pre-seed (peerID, self) bucket with one live announcement
		// via a different next-hop. Bucket is now at K=1 live.
		mustUpdate(t, tbl, RouteEntry{
			Identity: peerID, Origin: localOrigin, NextHop: otherHop,
			Hops: 5, SeqNo: 7, Source: RouteSourceAnnouncement,
		})

		statsBefore := tbl.CapStats()

		// AddDirectPeer must succeed and evict the announcement.
		result, err := tbl.AddDirectPeer(peerID)
		if err != nil {
			t.Fatalf("AddDirectPeer unexpected error: %v", err)
		}
		if result.Entry.NextHop != peerID || result.Entry.Source != RouteSourceDirect {
			t.Fatalf("expected direct entry with NextHop=peer, got %#v", result.Entry)
		}

		bucket := nextHopsByOrigin(t, tbl.Snapshot(), peerID, localOrigin)
		liveCount := 0
		for _, r := range bucket {
			if !r.IsWithdrawn() {
				liveCount++
			}
		}
		if liveCount != 1 {
			t.Fatalf("expected K=1 live entry after direct admission, got %d", liveCount)
		}
		if _, hasDirect := bucket[peerID]; !hasDirect {
			t.Fatalf("direct entry (NextHop=peer) missing from bucket: %#v", bucket)
		}
		if _, hasAnnouncement := bucket[otherHop]; hasAnnouncement {
			t.Fatalf("announcement via %s should have been evicted by direct admission", otherHop)
		}

		statsAfter := tbl.CapStats()
		if statsAfter.AcceptedReplaced != statsBefore.AcceptedReplaced+1 {
			t.Fatalf("expected acceptedReplaced++ on cap-driven displacement; %d → %d",
				statsBefore.AcceptedReplaced, statsAfter.AcceptedReplaced)
		}
		if statsAfter.RejectedFull != statsBefore.RejectedFull {
			t.Fatalf("RejectedFull should not advance — direct admission never rejects; %d → %d",
				statsBefore.RejectedFull, statsAfter.RejectedFull)
		}
		if statsAfter.RejectedAllProtected != statsBefore.RejectedAllProtected {
			t.Fatalf("RejectedAllProtected should not advance; %d → %d",
				statsBefore.RejectedAllProtected, statsAfter.RejectedAllProtected)
		}
	})

	t.Run("reactivation_from_tombstone_evicts_worst_evictable_when_bucket_saturated", func(t *testing.T) {
		// User-reported scenario: K=1, direct peer was previously
		// disconnected (tombstone for (peerID, self, peerID)), and
		// the bucket additionally has one live transit entry for
		// (peerID, self, otherHop). On reconnect, AddDirectPeer
		// must NOT leave the bucket with 2 live rows at K=1.
		now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
		tbl := NewTable(
			WithLocalOrigin(localOrigin),
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(1),
		)

		// Initial direct add — establishes the (peerID, self,
		// peerID) triple and bumps the SeqNo counter.
		if _, err := tbl.AddDirectPeer(peerID); err != nil {
			t.Fatalf("setup AddDirectPeer unexpected error: %v", err)
		}
		// Disconnect — tombstone for (peerID, self, peerID).
		if _, err := tbl.RemoveDirectPeer(peerID); err != nil {
			t.Fatalf("setup RemoveDirectPeer unexpected error: %v", err)
		}
		// Live transit entry for the same (peerID, self) bucket
		// via a different next-hop. Tombstones bypass the cap
		// (rule 1) so this still admits despite K=1.
		mustUpdate(t, tbl, RouteEntry{
			Identity: peerID, Origin: localOrigin, NextHop: otherHop,
			Hops: 5, SeqNo: 100, Source: RouteSourceAnnouncement,
		})

		// Sanity check: bucket has 1 live + 1 tombstone before reconnect.
		preBucket := nextHopsByOrigin(t, tbl.Snapshot(), peerID, localOrigin)
		preLive, preTomb := 0, 0
		for _, r := range preBucket {
			if r.IsWithdrawn() {
				preTomb++
			} else {
				preLive++
			}
		}
		if preLive != 1 || preTomb != 1 {
			t.Fatalf("setup expected 1 live + 1 tombstone, got %d live + %d tombstones", preLive, preTomb)
		}

		// Reconnect — AddDirectPeer takes the tombstone-reactivation
		// path. It must displace the otherHop announcement, NOT
		// leave a 2-live bucket at K=1.
		if _, err := tbl.AddDirectPeer(peerID); err != nil {
			t.Fatalf("reconnect AddDirectPeer unexpected error: %v", err)
		}

		bucket := nextHopsByOrigin(t, tbl.Snapshot(), peerID, localOrigin)
		liveCount, tombstoneCount := 0, 0
		for _, r := range bucket {
			if r.IsWithdrawn() {
				tombstoneCount++
			} else {
				liveCount++
			}
		}
		if liveCount != 1 {
			t.Fatalf("expected K=1 live after reconnect, got %d live entries: %#v", liveCount, bucket)
		}
		if tombstoneCount != 0 {
			t.Fatalf("expected tombstone slot consumed by reactivation, got %d tombstones", tombstoneCount)
		}
		direct, hasDirect := bucket[peerID]
		if !hasDirect || direct.Source != RouteSourceDirect {
			t.Fatalf("direct entry missing or wrong source after reconnect: %#v", bucket)
		}
		if _, hasAnnouncement := bucket[otherHop]; hasAnnouncement {
			t.Fatalf("transit announcement via %s should have been evicted by direct reactivation at K=1", otherHop)
		}
	})

	t.Run("new_direct_below_K_does_not_touch_cap_counters", func(t *testing.T) {
		// Sanity: at K=4 with an empty bucket the new direct just
		// appends. acceptedReplaced must stay flat — the operator
		// metric is "cap actually fired", and a normal direct
		// add should not show up there.
		now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
		tbl := NewTable(
			WithLocalOrigin(localOrigin),
			WithClock(fixedClock(now)),
			WithMaxNextHopsPerOrigin(4),
		)

		statsBefore := tbl.CapStats()
		if _, err := tbl.AddDirectPeer(peerID); err != nil {
			t.Fatalf("AddDirectPeer unexpected error: %v", err)
		}
		statsAfter := tbl.CapStats()

		if statsAfter != statsBefore {
			t.Fatalf("expected cap counters unchanged on below-K direct admission; %#v → %#v",
				statsBefore, statsAfter)
		}
	})
}

// --- helpers ---

// nextHopsByOrigin slices a Snapshot down to a NextHop→RouteEntry map
// for the entries belonging to the given (Identity, Origin) pair. Tests
// use this to assert presence/absence and to read out the surviving
// fields after a cap admission decision.
func nextHopsByOrigin(t *testing.T, snap Snapshot, identity, origin PeerIdentity) map[PeerIdentity]RouteEntry {
	t.Helper()
	out := make(map[PeerIdentity]RouteEntry)
	for _, r := range snap.Routes[identity] {
		if r.Origin == origin {
			out[r.NextHop] = r
		}
	}
	return out
}
