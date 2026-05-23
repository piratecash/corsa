package routing

import (
	"testing"
	"time"
)

// Phase 1 P0 Phase A — tombstone-promotion cap-aware admission +
// SeenOriginSeqs aliasing guards.
//
// The three cap-aware tombstone-promotion paths in route_store_
// mutation.go (newer-SeqNo, equal-SeqNo cross-Origin, stale-SeqNo
// cross-Origin) all share the same shape: detach the tombstone,
// call AdmitNew with the candidate live claim, restore the
// tombstone on rejection. Two invariants the suite pins:
//
//  1. The K-live cap is honoured on EVERY tombstone-promotion
//     branch — a bare `bucket[idx] = incoming` (live claim into
//     tombstone slot) would silently flip a non-counted slot into
//     a counted live slot and balloon the bucket past K.
//
//  2. The tombstone's SeenOriginSeqs map is NOT mutated by a
//     rejected promotion — the merge into the candidate's map
//     uses the copy-on-write helper so the restored tombstone
//     retains exactly the high-water observations it had before.
//     Without this, a future legit retry at the rejected
//     Origin/SeqNo would be misclassified as a stale replay by
//     the per-Origin high-water guard at the top of ApplyUpdate.

// countLiveClaims walks the bucket for identity in the published
// snapshot and counts non-withdrawn, non-expired entries.
func countLiveClaims(t *testing.T, tbl *Table, identity PeerIdentity) int {
	t.Helper()
	snap := tbl.Snapshot()
	n := 0
	for _, e := range snap.Routes[identity] {
		if !e.IsWithdrawn() && !e.IsExpired(snap.TakenAt) {
			n++
		}
	}
	return n
}

// TestStaleSeqNoCrossOriginTombstonePromotion_RespectsCap pins the
// review-flagged regression: the stale-SeqNo cross-Origin branch
// in ApplyUpdate must apply the cap-aware detach + AdmitNew +
// restore-on-reject pattern when it would promote a tombstone slot
// to a live claim. The scenario:
//
//   - Bucket has K live claims (Hops=3 each) on u1..uK.
//   - Tombstone on uplink-X at SeqNo=10 from Origin A (the
//     SeqNo-resurrection guard).
//   - Cross-Origin stale-SeqNo claim arrives from B at SeqNo=4
//     on uplink-X with Hops=4 (worse than the live K).
//
// Pre-fix: the branch did `bucket[idx] = incoming` because
// `improves = old.IsWithdrawn()` is true regardless of the cap
// state. The bucket grew from K live + 1 tombstone to K+1 live,
// violating the "at most K live UplinkClaim per Identity"
// invariant.
//
// Post-fix: cap-aware promotion. AdmitNew sees K live (the
// tombstone is detached) and either evicts a worse evictable
// candidate OR rejects. With Hops=4 > existing 3, AdmitNew
// rejects; the tombstone is restored and the live count stays
// at K.
//
// Reference: AdmitNew rule 1 in route_store_admission.go —
// "Withdrawn claims are EXCLUDED from the cap counter and from the
// eviction-search entirely" — together with docs/routing.md "RIB
// compaction" → rule 0.
func TestStaleSeqNoCrossOriginTombstonePromotion_RespectsCap(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	const (
		localID PeerIdentity = "node-A"
		dest    PeerIdentity = "dest"
		u1      PeerIdentity = "u-1"
		u2      PeerIdentity = "u-2"
		uX      PeerIdentity = "u-X"
		originA PeerIdentity = "origin-A"
		originB PeerIdentity = "origin-B"
	)
	const K = 2

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxNextHopsPerOrigin(K),
	)

	// Fill the bucket with K live claims at Hops=3 from Origin A.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: originA, NextHop: u1,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: originA, NextHop: u2,
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	// Add a tombstone on uplink-X via the wire-withdrawal path.
	// This creates a slot at (dest, uX) with Withdrawn=true,
	// SeqNo=10, LastIngressOrigin=originA.
	if !tbl.WithdrawRoute(dest, originA, uX, 10) {
		t.Fatalf("WithdrawRoute must succeed to seed the tombstone")
	}

	// Bucket now: K=2 live (u1, u2) + 1 tombstone (uX). Live
	// count exactly K — at cap.
	if got := countLiveClaims(t, tbl, dest); got != K {
		t.Fatalf("setup: live count must equal K=%d before promotion attempt, got %d", K, got)
	}

	// Cross-Origin stale-SeqNo claim on uplink-X. incoming.SeqNo=4
	// is stale against the tombstone's SeqNo=10, cross-Origin
	// (originB != stored originA), and `improves=true` purely
	// because `old.IsWithdrawn()` is true. Hops=4 is WORSE than
	// the existing live K (Hops=3), so AdmitNew will reject
	// AdmissionRejectedFull and the tombstone must be restored.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: originB, NextHop: uX,
		Hops: 4, SeqNo: 4, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	// Either RouteAccepted (cap evicted something) or RouteRejected
	// (cap floor); the K-live invariant must hold in BOTH outcomes.
	_ = status

	if got := countLiveClaims(t, tbl, dest); got > K {
		t.Fatalf("stale-SeqNo cross-Origin tombstone promotion broke K-live invariant: got live=%d, cap=%d", got, K)
	}
}

// TestTombstonePromotion_RejectedDoesNotPoisonSeenOriginSeqs pins
// the review-flagged P1.2 regression: when AdmitNew rejects a
// tombstone-promotion candidate, the saved tombstone's
// SeenOriginSeqs map must NOT contain the rejected Origin/SeqNo
// observation. The original implementation mutated the saved
// tombstone's map in place via the shared-reference
// `mergeOriginObservation`, leaving the restored tombstone
// remembering an observation that was never accepted. A future
// legit retry from the same Origin at the same SeqNo then hit the
// per-Origin high-water guard at the top of ApplyUpdate and was
// rejected as a "stale replay" — even though it was the FIRST
// successful observation from that Origin.
//
// Fix: tombstone-promotion paths use mergeOriginObservationCopy
// so the candidate's map is independent of the saved tombstone's
// map.
//
// Test fixture: K=1 with a direct claim filling the single live
// slot (un-evictable), plus a tombstone on a different uplink.
// AdmitNew on the tombstone-promotion path returns
// AdmissionRejectedAllProtected (direct is the only live row and
// cannot be evicted), so the candidate is dropped and the
// tombstone is restored. The assertion peeks into the restored
// tombstone's SeenOriginSeqs and verifies the rejected Origin is
// absent.
func TestTombstonePromotion_RejectedDoesNotPoisonSeenOriginSeqs(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	const (
		localID PeerIdentity = "node-A"
		dest    PeerIdentity = "dest"
		uX      PeerIdentity = "u-X"
		originA PeerIdentity = "origin-A"
		originB PeerIdentity = "origin-B"
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxNextHopsPerOrigin(1), // K=1 forces saturated bucket
	)

	// Fill the single K=1 slot with a DIRECT claim — direct is
	// not evictable, so AdmitNew will return RejectedAllProtected
	// on the tombstone-promotion path below.
	if _, err := tbl.AddDirectPeer(dest); err != nil {
		t.Fatalf("AddDirectPeer(dest): %v", err)
	}

	// Seed a tombstone on uplink-X at SeqNo=10 from Origin A.
	// The tombstone's SeenOriginSeqs is {A:10}.
	if !tbl.WithdrawRoute(dest, originA, uX, 10) {
		t.Fatalf("WithdrawRoute must succeed to seed the tombstone")
	}

	// Cross-Origin newer-SeqNo tombstone-promotion candidate from
	// B at SeqNo=15. Path: SeqNo>old (15>10), old.IsWithdrawn,
	// detach + AdmitNew. Bucket post-detach has 1 direct claim
	// (Uplink=dest), at K=1 cap, un-evictable → AdmitNew returns
	// RejectedAllProtected. Tombstone restored.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: originB, NextHop: uX,
		Hops: 4, SeqNo: 15, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected from RejectedAllProtected cap path, got %v", status)
	}

	// Peek at the restored tombstone's SeenOriginSeqs map. Pre-fix
	// it would contain {A:10, B:15}; post-fix it must contain only
	// {A:10}.
	tbl.mu.RLock()
	bucket := tbl.store.buckets[dest]
	tbl.mu.RUnlock()

	var tombstone *UplinkClaim
	for i := range bucket {
		if bucket[i].Uplink == uX && bucket[i].Withdrawn {
			c := bucket[i]
			tombstone = &c
			break
		}
	}
	if tombstone == nil {
		t.Fatalf("tombstone for uplink-X must survive rejection; bucket=%+v", bucket)
	}

	if _, poisoned := tombstone.SeenOriginSeqs[originB]; poisoned {
		t.Fatalf("rejected promotion poisoned tombstone's SeenOriginSeqs with %s: got %+v", originB, tombstone.SeenOriginSeqs)
	}
	if seq, ok := tombstone.SeenOriginSeqs[originA]; !ok || seq != 10 {
		t.Fatalf("tombstone must retain original SeenOriginSeqs[%s]=10, got %+v", originA, tombstone.SeenOriginSeqs)
	}
}

// TestEqualSeqNoCrossOriginTombstonePromotion_DoesNotPoisonSeenOriginSeqs
// is the sibling regression for the equal-SeqNo cross-Origin
// tombstone-promotion path (the second of the three branches that
// share the cap-aware detach + AdmitNew + restore shape). Same
// invariant: a rejected promotion must not mutate the tombstone's
// SeenOriginSeqs map.
//
// Equal-SeqNo cross-Origin fires when: incoming.SeqNo == old.SeqNo,
// old is withdrawn, incoming is live, and entry.Origin differs from
// old.LastIngressOrigin. The branch reaches AdmitNew after the
// cross-Origin stale-replay guard at the top of ApplyUpdate passes.
func TestEqualSeqNoCrossOriginTombstonePromotion_DoesNotPoisonSeenOriginSeqs(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	const (
		localID PeerIdentity = "node-A"
		dest    PeerIdentity = "dest"
		uX      PeerIdentity = "u-X"
		originA PeerIdentity = "origin-A"
		originB PeerIdentity = "origin-B"
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxNextHopsPerOrigin(1),
	)

	if _, err := tbl.AddDirectPeer(dest); err != nil {
		t.Fatalf("AddDirectPeer(dest): %v", err)
	}

	// Tombstone on u-X at SeqNo=10 from Origin A.
	if !tbl.WithdrawRoute(dest, originA, uX, 10) {
		t.Fatalf("WithdrawRoute must succeed to seed the tombstone")
	}

	// Equal-SeqNo cross-Origin live-vs-tombstone promotion
	// candidate from B at SeqNo=10. The cross-Origin stale-replay
	// guard at the top of ApplyUpdate does NOT fire (B has no
	// recorded high-water in the map). The equal-SeqNo branch
	// enters the cap-aware detach + AdmitNew + restore path;
	// AdmitNew rejects with AdmissionRejectedAllProtected (direct
	// is the only live row).
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: originB, NextHop: uX,
		Hops: 4, SeqNo: 10, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected from cap rejection, got %v", status)
	}

	tbl.mu.RLock()
	bucket := tbl.store.buckets[dest]
	tbl.mu.RUnlock()

	var tombstone *UplinkClaim
	for i := range bucket {
		if bucket[i].Uplink == uX && bucket[i].Withdrawn {
			c := bucket[i]
			tombstone = &c
			break
		}
	}
	if tombstone == nil {
		t.Fatalf("tombstone for uplink-X must survive rejection; bucket=%+v", bucket)
	}

	if _, poisoned := tombstone.SeenOriginSeqs[originB]; poisoned {
		t.Fatalf("equal-SeqNo rejected promotion poisoned tombstone's SeenOriginSeqs with %s: got %+v", originB, tombstone.SeenOriginSeqs)
	}
}

// TestStaleSeqNoCrossOriginTombstonePromotion_DoesNotPoisonSeenOriginSeqs
// is the third sibling: the stale-SeqNo cross-Origin tombstone-
// promotion path (added by the same review round that flagged
// P1.1) must also use the copy-on-write merge so rejection does
// not corrupt the tombstone's high-water map.
func TestStaleSeqNoCrossOriginTombstonePromotion_DoesNotPoisonSeenOriginSeqs(t *testing.T) {
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	const (
		localID PeerIdentity = "node-A"
		dest    PeerIdentity = "dest"
		uX      PeerIdentity = "u-X"
		originA PeerIdentity = "origin-A"
		originB PeerIdentity = "origin-B"
	)

	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin(localID),
		WithMaxNextHopsPerOrigin(1),
	)

	if _, err := tbl.AddDirectPeer(dest); err != nil {
		t.Fatalf("AddDirectPeer(dest): %v", err)
	}

	// Tombstone on u-X at SeqNo=20 from Origin A. Higher SeqNo so
	// the cross-Origin claim below lands in the stale-SeqNo branch.
	if !tbl.WithdrawRoute(dest, originA, uX, 20) {
		t.Fatalf("WithdrawRoute must succeed to seed the tombstone")
	}

	// Stale-SeqNo cross-Origin claim from B at SeqNo=5. Path:
	// SeqNo<old (5<20), cross-Origin, improves (old.IsWithdrawn),
	// cap>0 → cap-aware detach+AdmitNew+restore. AdmitNew rejects
	// (only direct live).
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: dest, Origin: originB, NextHop: uX,
		Hops: 4, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if err != nil {
		t.Fatalf("UpdateRoute err: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("expected RouteRejected from cap rejection, got %v", status)
	}

	tbl.mu.RLock()
	bucket := tbl.store.buckets[dest]
	tbl.mu.RUnlock()

	var tombstone *UplinkClaim
	for i := range bucket {
		if bucket[i].Uplink == uX && bucket[i].Withdrawn {
			c := bucket[i]
			tombstone = &c
			break
		}
	}
	if tombstone == nil {
		t.Fatalf("tombstone for uplink-X must survive rejection; bucket=%+v", bucket)
	}

	if _, poisoned := tombstone.SeenOriginSeqs[originB]; poisoned {
		t.Fatalf("stale-SeqNo cross-Origin rejected promotion poisoned tombstone's SeenOriginSeqs with %s: got %+v", originB, tombstone.SeenOriginSeqs)
	}
}
