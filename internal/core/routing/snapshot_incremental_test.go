package routing

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// snapshotsEqual compares the route-projection-relevant parts of two
// snapshots: the Routes map and the Total/Active counters. FlapState /
// Health / CapStats are fully copied by both paths (incremental does not
// touch them), so the projection contract is fully captured by these three.
func snapshotsEqual(t *testing.T, want, got Snapshot) {
	t.Helper()
	if want.TotalEntries != got.TotalEntries {
		t.Fatalf("TotalEntries: full=%d incremental=%d", want.TotalEntries, got.TotalEntries)
	}
	if want.ActiveEntries != got.ActiveEntries {
		t.Fatalf("ActiveEntries: full=%d incremental=%d", want.ActiveEntries, got.ActiveEntries)
	}
	if !reflect.DeepEqual(want.Routes, got.Routes) {
		t.Fatalf("Routes diverged between full and incremental snapshot\nfull=%#v\nincremental=%#v",
			want.Routes, got.Routes)
	}
}

// TestSnapshotIncrementalMatchesFullUnderRandomMutations is the core safety
// net for the copy-on-write projection: after every mutation through the
// public Table API, the incremental projection (which reuses unchanged
// buckets) must be byte-for-byte identical to a fresh full Snapshot. A
// missed dirty-mark in any mutation wrapper makes the incremental snapshot
// reuse a stale bucket, which this comparison catches immediately (without
// relying on the periodic forceFull self-heal).
func TestSnapshotIncrementalMatchesFullUnderRandomMutations(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(clock),
		WithDefaultTTL(120*time.Second),
	)

	identities := make([]PeerIdentity, 0, 8)
	for i := 0; i < 8; i++ {
		identities = append(identities, domaintest.ID(fmt.Sprintf("id-%02d", i)))
	}
	uplinks := []PeerIdentity{domaintest.ID("up-a"), domaintest.ID("up-b"), domaintest.ID("up-c")}

	rng := rand.New(rand.NewSource(42))
	seq := uint64(1)

	// compare runs an incremental snapshot followed by a full one against
	// the same frozen clock and asserts they agree.
	compare := func() {
		t.Helper()
		inc, _ := tbl.SnapshotIncremental(false)
		full := tbl.Snapshot()
		snapshotsEqual(t, full, inc)
	}

	// Prime the incremental cache once so subsequent calls exercise reuse.
	compare()

	directPeers := map[PeerIdentity]bool{}

	for step := 0; step < 400; step++ {
		id := identities[rng.Intn(len(identities))]
		up := uplinks[rng.Intn(len(uplinks))]
		seq++

		switch rng.Intn(8) {
		case 0, 1:
			// Learn / improve a transit route.
			_, _ = tbl.UpdateRoute(RouteEntry{
				Identity: id, Origin: domaintest.ID("self"), NextHop: up,
				Hops: 1 + rng.Intn(4), SeqNo: seq, Source: RouteSourceAnnouncement,
			})
		case 2:
			// Withdraw a specific (identity, uplink) route from the wire.
			tbl.WithdrawRoute(id, domaintest.ID("self"), up, seq)
		case 3:
			// Poison-reverse style single-claim invalidation.
			tbl.InvalidateUplinkClaim(id, up)
		case 4:
			// Add a direct peer (own-origin Direct route).
			if _, err := tbl.AddDirectPeer(id); err == nil {
				directPeers[id] = true
			}
		case 5:
			// Confirm a hop-ack (may promote Announcement -> HopAck).
			tbl.ConfirmHopAck(id, up, time.Duration(rng.Intn(50))*time.Millisecond)
		case 6:
			// Remove a direct peer if one exists (bulk invalidation path).
			if directPeers[id] {
				_, _ = tbl.RemoveDirectPeer(id)
				delete(directPeers, id)
			} else {
				tbl.InvalidateTransitRoutes(up)
			}
		case 7:
			// Advance the clock and run a TTL sweep so finite-TTL routes
			// expire — exercises the time-driven active-count recompute and
			// the bulk full-dirty path.
			now = now.Add(time.Duration(10+rng.Intn(60)) * time.Second)
			tbl.TickTTL()
		}

		compare()
	}
}

// TestSnapshotIncrementalReusesCleanBuckets verifies the optimisation
// actually fires: a bucket untouched since the previous snapshot is reused
// by identity (same backing array), while a mutated bucket is freshly
// copied. Without reuse the change would be correct but pointless — this
// pins the allocation-elision contract that motivates the whole change.
func TestSnapshotIncrementalReusesCleanBuckets(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(func() time.Time { return now }),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("self"), NextHop: domaintest.ID("up-a"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("self"), NextHop: domaintest.ID("up-b"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	first, _ := tbl.SnapshotIncremental(false)
	aliceFirst := first.Routes[domaintest.ID("alice")]
	bobFirst := first.Routes[domaintest.ID("bob")]

	// Mutate only bob.
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("bob"), Origin: domaintest.ID("self"), NextHop: domaintest.ID("up-b"),
		Hops: 1, SeqNo: 2, Source: RouteSourceAnnouncement,
	})

	second, _ := tbl.SnapshotIncremental(false)

	// alice was untouched: the published slice must be the same backing
	// array (reused, not re-allocated).
	if !sameBackingArray(aliceFirst, second.Routes[domaintest.ID("alice")]) {
		t.Fatal("clean identity alice was re-copied instead of reused")
	}
	// bob changed: must be a fresh copy reflecting the new hops/seq.
	if sameBackingArray(bobFirst, second.Routes[domaintest.ID("bob")]) {
		t.Fatal("dirty identity bob reused stale backing array")
	}
	if got := second.Routes[domaintest.ID("bob")][0].SeqNo; got != 2 {
		t.Fatalf("bob projection stale: SeqNo=%d want 2", got)
	}
}

// TestSnapshotIncrementalForceFullRecopies verifies the self-heal pass:
// forceFull=true re-copies even clean buckets, so a hypothetical missed
// dirty-mark cannot survive a forced rebuild.
func TestSnapshotIncrementalForceFullRecopies(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	tbl := NewTable(
		WithLocalOrigin(domaintest.ID("self")),
		WithClock(func() time.Time { return now }),
	)
	mustUpdate(t, tbl, RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("self"), NextHop: domaintest.ID("up-a"),
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// First call has no reuse cache yet, so it is necessarily a full copy.
	first, firstFull := tbl.SnapshotIncremental(false)
	if !firstFull {
		t.Fatal("first snapshot (cold cache) should report a full re-copy")
	}
	aliceFirst := first.Routes[domaintest.ID("alice")]

	// No mutation; a non-forced pass reuses the slice and reports NOT full.
	reused, reusedFull := tbl.SnapshotIncremental(false)
	if reusedFull {
		t.Fatal("clean non-forced pass should report incremental (not full)")
	}
	if !sameBackingArray(aliceFirst, reused.Routes[domaintest.ID("alice")]) {
		t.Fatal("expected reuse on clean non-forced pass")
	}

	// Forced pass must re-copy despite no dirty mark, and report full.
	forced, forcedFull := tbl.SnapshotIncremental(true)
	if !forcedFull {
		t.Fatal("forceFull pass should report a full re-copy")
	}
	if sameBackingArray(aliceFirst, forced.Routes[domaintest.ID("alice")]) {
		t.Fatal("forceFull did not re-copy the clean bucket")
	}
	// Content must still be identical to a full snapshot.
	snapshotsEqual(t, tbl.Snapshot(), forced)
}

// sameBackingArray reports whether two RouteEntry slices share the same
// underlying array (i.e. one was reused rather than re-allocated). Empty
// slices are treated as distinct because reuse is unobservable and
// irrelevant for them.
func sameBackingArray(a, b []RouteEntry) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return &a[0] == &b[0]
}
