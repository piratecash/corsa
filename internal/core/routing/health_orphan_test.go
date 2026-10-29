package routing

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// orphanHealthCount returns the number of health entries whose
// (Identity, Uplink) pair has no claim (live or tombstone) in storage.
// The invariant under test is "health keys ⊆ storage keys": every
// path that physically drops a claim must drop its health entry in
// the same mutation, or the health map grows with the history of the
// node instead of its current route set.
func orphanHealthCount(tbl *Table) int {
	tbl.mu.RLock()
	defer tbl.mu.RUnlock()
	return tbl.health.orphanCountLocked(tbl.store.hasClaimLocked)
}

func transitEntry(target, uplink PeerIdentity, hops, seq int, expires time.Time) RouteEntry {
	return RouteEntry{
		Identity: target, Origin: target, NextHop: uplink,
		Hops: hops, SeqNo: uint64(seq), Source: RouteSourceAnnouncement,
		ExpiresAt: expires,
	}
}

// Cap replacement evicts the losing claim in place. The health entry of
// the evicted (target, uplink) pair must go with it — this was the
// production shape: route_health 28 510 against route_claims 3 585.
func TestCapReplacementEvictsHealthOfDisplacedUplink(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithMaxNextHopsPerOrigin(1), WithClock(fixedClock(now)))
	target := domaintest.ID("target-X")
	uplinkA := domaintest.ID("uplink-A")
	uplinkB := domaintest.ID("uplink-B")

	mustUpdate(t, tbl, transitEntry(target, uplinkA, 4, 1, now.Add(DefaultTTL)))
	if got := tbl.health.lenLocked(); got != 1 {
		t.Fatalf("health entries after first admission = %d, want 1", got)
	}

	if status := mustUpdate(t, tbl, transitEntry(target, uplinkB, 2, 1, now.Add(DefaultTTL))); status != RouteAccepted {
		t.Fatalf("better uplink must replace the worse one under cap=1, got %v", status)
	}
	if tbl.health.getLocked(target, uplinkA) != nil {
		t.Fatal("health for the cap-evicted uplink A must be dropped with its claim")
	}
	if tbl.health.getLocked(target, uplinkB) == nil {
		t.Fatal("health for the surviving uplink B must exist")
	}
	if got := orphanHealthCount(tbl); got != 0 {
		t.Fatalf("orphan health entries = %d, want 0", got)
	}
}

// A constant number of routes whose uplink keeps changing must keep a
// constant number of health entries, regardless of whether TTL ever
// removes anything (the winner is refreshed, so TickTTL is a no-op).
func TestRepeatedUplinkReplacementKeepsHealthBounded(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	clk := now
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithMaxNextHopsPerOrigin(1), WithClock(func() time.Time { return clk }))
	target := domaintest.ID("target-X")

	const rounds = 500
	for i := range rounds {
		uplink := domaintest.ID(fmt.Sprintf("uplink-%d", i))
		// Strictly fewer hops each round is impossible past a point, so
		// let the previous winner expire (still stored, not compacted)
		// and admit the newcomer: the liveness pre-tier evicts the
		// expired row in place — the second production shape, where
		// every replacement of an expired claim leaked one entry.
		clk = clk.Add(DefaultTTL + time.Second)
		mustUpdate(t, tbl, transitEntry(target, uplink, 2, i+1, clk.Add(DefaultTTL)))
		if removed := tbl.TickTTL().Removed; removed != 0 {
			t.Fatalf("round %d: TickTTL removed %d claims, the replacement must have happened in place", i, removed)
		}
		tbl.TickHealth()
	}
	if got := tbl.health.lenLocked(); got != 1 {
		t.Fatalf("health entries after %d replacements = %d, want 1", rounds, got)
	}
	if got := orphanHealthCount(tbl); got != 0 {
		t.Fatalf("orphan health entries = %d, want 0", got)
	}
}

// Disconnecting the evicted uplink must not be the only thing that
// cleans up, and it must not leave the entry behind either: after the
// eviction there is nothing in storage for RemoveDirectPeer to find.
func TestDisconnectOfEvictedUplinkLeavesNoOrphanHealth(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithMaxNextHopsPerOrigin(1), WithClock(fixedClock(now)))
	target := domaintest.ID("target-X")
	uplinkA := domaintest.ID("uplink-A")
	uplinkB := domaintest.ID("uplink-B")

	mustAddDirect(t, tbl, uplinkA)
	mustUpdate(t, tbl, transitEntry(target, uplinkA, 4, 1, now.Add(DefaultTTL)))
	mustUpdate(t, tbl, transitEntry(target, uplinkB, 2, 1, now.Add(DefaultTTL)))
	mustRemoveDirect(t, tbl, uplinkA)
	tbl.InvalidateTransitRoutes(uplinkA)

	if tbl.health.getLocked(target, uplinkA) != nil {
		t.Fatal("health for (target, uplink-A) must not survive eviction + disconnect")
	}
	if got := orphanHealthCount(tbl); got != 0 {
		t.Fatalf("orphan health entries = %d, want 0", got)
	}
}

// Direct admission on a saturated bucket displaces the worst transit
// claim; its health entry must follow.
func TestDirectAdmissionEvictsHealthOfDisplacedUplink(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithMaxNextHopsPerOrigin(1), WithClock(fixedClock(now)))
	peer := domaintest.ID("peer-A")
	uplinkB := domaintest.ID("uplink-B")

	mustUpdate(t, tbl, transitEntry(peer, uplinkB, 2, 1, now.Add(DefaultTTL)))
	mustAddDirect(t, tbl, peer)

	if tbl.health.getLocked(peer, uplinkB) != nil {
		t.Fatal("health for the transit claim displaced by the direct registration must be dropped")
	}
	if got := orphanHealthCount(tbl); got != 0 {
		t.Fatalf("orphan health entries = %d, want 0", got)
	}
}

// TTL compaction is the third physical removal path; health goes with
// the claim in the same mutation, not in a separate reconcile pass.
func TestCompactExpiredEvictsHealthOfRemovedClaims(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	clk := now
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithClock(func() time.Time { return clk }))
	target := domaintest.ID("target-X")
	uplinkA := domaintest.ID("uplink-A")
	uplinkB := domaintest.ID("uplink-B")

	mustUpdate(t, tbl, transitEntry(target, uplinkA, 2, 1, now.Add(time.Minute)))
	mustUpdate(t, tbl, transitEntry(target, uplinkB, 3, 1, now.Add(time.Hour)))
	clk = now.Add(2 * time.Minute)
	if removed := tbl.TickTTL().Removed; removed != 1 {
		t.Fatalf("TickTTL removed %d claims, want 1", removed)
	}
	if tbl.health.getLocked(target, uplinkA) != nil {
		t.Fatal("health for the expired claim must be dropped by compaction")
	}
	if tbl.health.getLocked(target, uplinkB) == nil {
		t.Fatal("health for the surviving claim must stay")
	}

	clk = now.Add(2 * time.Hour)
	tbl.TickTTL()
	if got := tbl.health.lenLocked(); got != 0 {
		t.Fatalf("health entries after the whole bucket expired = %d, want 0", got)
	}
}

// CompactExpired shrinks the bucket slice in place; the vacated tail
// must be zeroed so the backing array stops pinning the removed
// claims' Extra / signature allocations.
func TestCompactExpiredClearsVacatedTail(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	s := newRouteStore()
	target := domaintest.ID("target-X")
	live := UplinkClaim{Uplink: domaintest.ID("uplink-live"), Hops: 2, SeqNo: 1, ExpiresAt: now.Add(time.Hour), Source: RouteSourceAnnouncement}
	expired := UplinkClaim{
		Uplink: domaintest.ID("uplink-old"), Hops: 2, SeqNo: 1, ExpiresAt: now.Add(-time.Second), Source: RouteSourceAnnouncement,
		AttestedSig: []byte{0x01, 0x02},
	}
	s.buckets[target] = []UplinkClaim{live, expired}

	if removed, _, _ := s.CompactExpired(now); removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	bucket := s.buckets[target]
	if len(bucket) != 1 || bucket[0].Uplink != live.Uplink {
		t.Fatalf("bucket = %+v, want only the live claim", bucket)
	}
	tail := bucket[:cap(bucket)][1]
	if tail.AttestedSig != nil || !tail.Uplink.IsZero() {
		t.Fatalf("vacated tail slot still holds the removed claim: %+v", tail)
	}
}

// Common resource polling must report the cheap total, not walk every health
// pair to audit an invariant. Absence of the expensive series is not zero.
func TestUsageOmitsExpensiveOrphanAudit(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(WithLocalOrigin(domaintest.ID("me")), WithClock(fixedClock(now)))
	target := domaintest.ID("target-X")
	uplink := domaintest.ID("uplink-A")
	mustUpdate(t, tbl, transitEntry(target, uplink, 2, 1, now.Add(time.Hour)))
	tbl.mu.Lock()
	tbl.health.ensureLocked(target, domaintest.ID("uplink-ghost"), now)
	tbl.mu.Unlock()
	var health uint64
	for _, gauge := range tbl.Usage().Gauges() {
		if gauge.Name() == "route_health_orphans" {
			t.Fatal("common resource polling exposed the expensive orphan audit")
		}
		if gauge.Name() == "route_health" {
			health = gauge.Count()
		}
	}
	if health != 2 {
		t.Fatalf("route_health=%d, want the complete container count 2", health)
	}
	if count := orphanHealthCount(tbl); count != 1 {
		t.Fatalf("test-only invariant audit=%d, want 1 (not a fabricated zero)", count)
	}
}

// This full invariant audit is test-only. It must not be called while normal
// resource polling holds the production table read lock.
func (s *healthStore) orphanCountLocked(hasClaim func(identity, uplink domain.PeerIdentity) bool) int {
	orphans := 0
	for key := range s.states {
		if !hasClaim(key.Identity, key.Uplink) {
			orphans++
		}
	}
	return orphans
}
