package routing

import (
	"encoding/json"
	"testing"
	"time"
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
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

func TestUpdateRouteDifferentTriplesCoexist(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "dave", NextHop: "charlie",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if tbl.Size() != 2 {
		t.Fatalf("different origins should coexist: size=%d", tbl.Size())
	}
}

// --- Validation ---

func TestUpdateRouteRejectsEmptyIdentity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "", Origin: "bob", NextHop: "bob",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrEmptyIdentity {
		t.Fatalf("expected ErrEmptyIdentity, got %v", err)
	}
}

func TestUpdateRouteRejectsEmptyOrigin(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrEmptyOrigin {
		t.Fatalf("expected ErrEmptyOrigin, got %v", err)
	}
}

func TestUpdateRouteRejectsEmptyNextHop(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "",
		Hops: 1, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrEmptyNextHop {
		t.Fatalf("expected ErrEmptyNextHop, got %v", err)
	}
}

func TestUpdateRouteRejectsZeroHops(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 0, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrInvalidHops {
		t.Fatalf("expected ErrInvalidHops for hops=0, got %v", err)
	}
}

func TestUpdateRouteRejectsHopsAboveInfinity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 17, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if err != ErrInvalidHops {
		t.Fatalf("expected ErrInvalidHops for hops=17, got %v", err)
	}
}

func TestUpdateRouteAcceptsHopsInfinity(t *testing.T) {
	tbl := NewTable()
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("hops=16 (infinity/withdrawal) should be accepted")
	}
}

func TestMalformedEntryDoesNotPollute(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 0, SeqNo: 99, Source: RouteSourceAnnouncement,
	})
	if err == nil {
		t.Fatal("expected validation error for hops=0")
	}

	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].SeqNo != 1 {
		t.Fatal("malformed entry with higher SeqNo should not replace valid entry")
	}
}

// --- Direct route validation ---

func TestDirectRouteRequiresHops1(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 2, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrDirectHopsMust1 {
		t.Fatalf("expected ErrDirectHopsMust1, got %v", err)
	}
}

func TestDirectRouteRequiresNextHopEqualsIdentity(t *testing.T) {
	tbl := NewTable()
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "bob",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if err != ErrDirectNextHop {
		t.Fatalf("expected ErrDirectNextHop, got %v", err)
	}
}

func TestDirectRouteValidForm(t *testing.T) {
	tbl := NewTable()
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("higher SeqNo should be accepted even with worse hops")
	}

	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].SeqNo != 10 {
		t.Fatalf("expected SeqNo=10, got %+v", routes)
	}
}

func TestOriginAwareSeqNoLowerRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status == RouteAccepted {
		t.Fatal("lower SeqNo should be rejected")
	}

	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].SeqNo != 10 {
		t.Fatalf("original route should remain: %+v", routes)
	}
}

func TestSeqNoComparisonScopedToOrigin(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "dave", NextHop: "charlie",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if status != RouteAccepted {
		t.Fatal("different origin should have independent SeqNo space")
	}
	if tbl.Size() != 2 {
		t.Fatalf("expected 2 routes, got %d", tbl.Size())
	}
}

func TestSameSeqNoHigherTrustWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceHopAck,
	})

	if status != RouteAccepted {
		t.Fatal("same SeqNo but higher trust should be accepted")
	}

	routes := tbl.Lookup("alice")
	if routes[0].Source != RouteSourceHopAck {
		t.Fatal("hop_ack should replace announcement at same SeqNo")
	}
}

func TestSameSeqNoLowerTrustRejected(t *testing.T) {
	tbl := NewTable()

	// Use a well-formed direct route: NextHop must equal Identity
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 5, Source: RouteSourceDirect,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if status == RouteAccepted {
		t.Fatal("same SeqNo but lower trust should be rejected")
	}
}

func TestSameSeqNoSameTrustFewerHopsWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("initial route should be accepted")
	}

	// Re-announce the same route (same SeqNo, same trust, same hops).
	status = mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatalf("expired route same-SeqNo reannounce should be RouteAccepted, got %d", status)
	}
}

// --- Split horizon ---

func TestSplitHorizonOmitsRoutesFromPeer(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "me", NextHop: "bob",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	announceable := tbl.Announceable("peer-A")

	for _, r := range announceable {
		if r.NextHop == "peer-A" {
			t.Fatal("split horizon: routes from peer-A must be excluded when announcing to peer-A")
		}
	}
	if len(announceable) != 1 || announceable[0].Identity != "bob" {
		t.Fatalf("expected only bob's route, got %+v", announceable)
	}
}

func TestSplitHorizonDoesNotSendFakeWithdrawal(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable("peer-A")

	for _, r := range announceable {
		if r.Identity == "alice" && r.Hops == HopsInfinity {
			t.Fatal("split horizon must omit, not send fake withdrawal")
		}
	}
}

func TestAnnouncableExcludesWithdrawn(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-B",
		Hops: HopsInfinity, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable("peer-A")
	if len(announceable) != 0 {
		t.Fatal("withdrawn routes should not be announceable")
	}
}

// --- AnnounceTo (wire projection with split horizon) ---

func TestAnnounceToPreservesHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo("peer-A")
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
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "y", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo("peer-A")
	if len(entries) != 1 || entries[0].Identity != "bob" {
		t.Fatalf("split horizon should exclude alice's route: got %+v", entries)
	}
}

// --- Origin filtering (don't send peer its own originated routes) ---

func TestAnnounceableOmitsRoutesOriginatedByPeer(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Route originated by peer-A, learned via peer-C (different NextHop).
	// Split horizon alone would NOT filter this, but origin filter must.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "peer-A", NextHop: "peer-C",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "peer-B", NextHop: "peer-C",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	announceable := tbl.Announceable("peer-A")
	for _, r := range announceable {
		if r.Origin == "peer-A" {
			t.Fatal("origin filter: routes originated by peer-A must be excluded when announcing to peer-A")
		}
	}
	if len(announceable) != 1 || announceable[0].Identity != "bob" {
		t.Fatalf("expected only bob's route, got %+v", announceable)
	}
}

func TestAnnounceToOmitsRoutesOriginatedByPeer(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Route originated by peer-A, learned via peer-C.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "peer-A", NextHop: "peer-C",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "peer-B", NextHop: "peer-C",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo("peer-A")
	if len(entries) != 1 || entries[0].Identity != "bob" {
		t.Fatalf("origin filter should exclude alice's route: got %+v", entries)
	}
}

func TestAnnounceToStripsInternalFields(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-B",
		Hops: 2, SeqNo: 7, Source: RouteSourceHopAck,
	})

	entries := tbl.AnnounceTo("peer-A")
	if len(entries) != 1 {
		t.Fatal("expected 1 entry")
	}
	e := entries[0]
	if e.Identity != "alice" || e.Origin != "x" || e.SeqNo != 7 {
		t.Fatal("wire fields should be preserved")
	}
	// AnnounceEntry has no NextHop, Source, or ExpiresAt fields — verified by type system.
}

func TestAnnounceToHops15SentAsIs(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-B",
		Hops: 15, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	entries := tbl.AnnounceTo("peer-A")
	if len(entries) != 1 || entries[0].Hops != 15 {
		t.Fatalf("wire should carry 15 as-is (receiver does +1 to get infinity), got %d", entries[0].Hops)
	}
}

func TestAnnounceToIncludesOwnOriginTombstones(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin("node-A"),
		WithDefaultTTL(120*time.Second),
	)

	// Add a direct peer, then remove it to create an own-origin tombstone.
	if _, err := tbl.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	result, err := tbl.RemoveDirectPeer("peer-B")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Withdrawals) == 0 {
		t.Fatal("expected withdrawal entries")
	}

	// AnnounceTo should include the own-origin tombstone (hops=16).
	entries := tbl.AnnounceTo("peer-C")
	found := false
	for _, e := range entries {
		if e.Identity == "peer-B" && e.Hops == HopsInfinity {
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
		WithLocalOrigin("node-A"),
		WithDefaultTTL(120*time.Second),
	)

	// Insert a transit route (origin != localOrigin), then withdraw it.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "peer-X", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// Withdraw with higher SeqNo.
	tbl.WithdrawRoute("alice", "peer-X", "peer-B", 2)

	// AnnounceTo should NOT include the transit tombstone.
	entries := tbl.AnnounceTo("peer-C")
	for _, e := range entries {
		if e.Identity == "alice" && e.Hops >= HopsInfinity {
			t.Fatal("transit tombstone should NOT appear in AnnounceTo")
		}
	}
}

func TestAnnounceToOwnOriginTombstoneExpiresNaturally(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	tbl := NewTable(
		WithClock(clock),
		WithLocalOrigin("node-A"),
		WithDefaultTTL(120*time.Second),
	)

	if _, err := tbl.AddDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.RemoveDirectPeer("peer-B"); err != nil {
		t.Fatal(err)
	}

	// Tombstone visible immediately.
	entries := tbl.AnnounceTo("peer-C")
	hasTombstone := false
	for _, e := range entries {
		if e.Identity == "peer-B" && e.Hops == HopsInfinity {
			hasTombstone = true
		}
	}
	if !hasTombstone {
		t.Fatal("tombstone should be visible before expiry")
	}

	// Advance clock past TTL — tombstone should disappear.
	now = now.Add(121 * time.Second)
	entries = tbl.AnnounceTo("peer-C")
	for _, e := range entries {
		if e.Identity == "peer-B" {
			t.Fatalf("tombstone should have expired, still present: %+v", e)
		}
	}
}

// --- Withdrawal ---

func TestWithdrawRouteSetHopsInfinity(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 6)
	if !ok {
		t.Fatal("withdrawal with higher SeqNo should succeed")
	}

	snap := tbl.Snapshot()
	routes := snap.Routes["alice"]
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
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 10)
	if ok {
		t.Fatal("withdrawal with equal SeqNo should be rejected — origin must increment")
	}

	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should remain alive after rejected equal-seq withdrawal")
	}
}

func TestWithdrawRouteStaleSeqNoRejected(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 5)
	if ok {
		t.Fatal("withdrawal with lower SeqNo should be rejected")
	}
}

func TestWithdrawRouteNonexistentCreatesTombstone(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 5)
	if !ok {
		t.Fatal("withdrawal of unseen route should succeed (tombstone created)")
	}

	// Tombstone should exist: withdrawn entry with HopsInfinity and SeqNo=5.
	snap := tbl.Snapshot()
	routes := snap.Routes["alice"]
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
	active := tbl.Lookup("alice")
	if len(active) != 0 {
		t.Fatal("tombstone should not appear in Lookup")
	}
}

func TestWithdrawTombstoneBlocksStaleAnnouncement(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Withdrawal arrives first with SeqNo=10 — creates tombstone.
	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 10)
	if !ok {
		t.Fatal("tombstone creation should succeed")
	}

	// Delayed stale announcement with SeqNo=8 arrives — must be rejected
	// because the tombstone's SeqNo (10) is higher.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 8, Source: RouteSourceAnnouncement,
	})
	if status == RouteAccepted {
		t.Fatal("stale announcement (SeqNo=8) should be rejected by tombstone (SeqNo=10)")
	}

	// Route should still be withdrawn in the table.
	active := tbl.Lookup("alice")
	if len(active) != 0 {
		t.Fatal("no active route should exist after tombstone blocks stale announcement")
	}
}

func TestWithdrawTombstoneSupersededByNewerAnnouncement(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Tombstone at SeqNo=5.
	tbl.WithdrawRoute("alice", "bob", "charlie", 5)

	// Newer announcement with SeqNo=7 — should be accepted.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("newer announcement (SeqNo=7) should supersede tombstone (SeqNo=5)")
	}

	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should be active after tombstone superseded")
	}
}

func TestWithdrawnRouteRejectsSameSeqNoHigherTrustUpdate(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// Active announcement route at SeqNo=5.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// Withdraw at SeqNo=6 (strictly greater than active SeqNo=5) — sets Hops=HopsInfinity.
	if !tbl.WithdrawRoute("alice", "bob", "charlie", 6) {
		t.Fatal("withdraw should succeed")
	}

	// Same-SeqNo hop_ack (higher trust rank) must NOT resurrect the
	// withdrawn entry. Only a strictly newer SeqNo may do that.
	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 1, SeqNo: 6, Source: RouteSourceHopAck,
	})
	if status == RouteAccepted {
		t.Fatal("same-SeqNo hop_ack must not resurrect a withdrawn route")
	}

	// Verify the route is still withdrawn.
	routes := tbl.Lookup("alice")
	if len(routes) != 0 {
		t.Fatal("withdrawn route should not appear in Lookup")
	}

	// A strictly newer SeqNo should succeed.
	status = mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	})
	if status != RouteAccepted {
		t.Fatal("newer SeqNo=7 should supersede withdrawn SeqNo=6")
	}
	routes = tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].Hops != 2 {
		t.Fatal("route should be active after superseding withdrawal")
	}
}

// --- UpdateRoute: direct route origin guard ---

func TestUpdateRouteRejectsDirectWithForeignOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "foreign-node", NextHop: "alice",
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
	tbl := NewTable(WithLocalOrigin("me"))

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
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
		Identity: "alice", Origin: "any-origin", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if status != RouteAccepted {
		t.Fatal("without localOrigin, direct route with any origin should be accepted")
	}
}

func TestForeignDirectRouteCannotOutrankAnnouncement(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	// Legitimate announcement route.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "foreign-node", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	// Attempt to inject a foreign-origin direct route that would outrank it.
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "alice", Origin: "foreign-node", NextHop: "alice",
		Hops: 1, SeqNo: 2, Source: RouteSourceDirect,
	})
	if err != ErrDirectForeignOrigin {
		t.Fatalf("foreign-origin direct injection should be rejected: %v", err)
	}

	// Only the announcement should remain.
	routes := tbl.Lookup("alice")
	if len(routes) != 1 || routes[0].Source != RouteSourceAnnouncement {
		t.Fatal("announcement should be the only route after rejecting foreign direct")
	}
}

// --- AddDirectPeer ---

func TestAddDirectPeerCreatesRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	result, err := tbl.AddDirectPeer("peer-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry := result.Entry
	if entry.Identity != "peer-A" || entry.Origin != "me" || entry.NextHop != "peer-A" {
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
	tbl := NewTable(WithLocalOrigin("me"))

	e1 := mustAddDirect(t, tbl, "peer-A")
	// Simulate disconnect + reconnect by removing then re-adding
	mustRemoveDirect(t, tbl, "peer-A")
	e2 := mustAddDirect(t, tbl, "peer-A")

	if e2.SeqNo <= e1.SeqNo {
		t.Fatalf("reconnected peer should get higher SeqNo: first=%d, second=%d", e1.SeqNo, e2.SeqNo)
	}
}

func TestAddDirectPeerRequiresLocalOrigin(t *testing.T) {
	tbl := NewTable()

	_, err := tbl.AddDirectPeer("peer-A")
	if err != ErrNoLocalOrigin {
		t.Fatalf("expected ErrNoLocalOrigin, got %v", err)
	}
}

func TestAddDirectPeerRejectsEmptyPeerID(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	_, err := tbl.AddDirectPeer("")
	if err != ErrEmptyPeerID {
		t.Fatalf("expected ErrEmptyPeerID, got %v", err)
	}
}

func TestAddDirectPeerIdempotentWhenAlreadyActive(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	e1 := mustAddDirect(t, tbl, "peer-A")
	e2 := mustAddDirect(t, tbl, "peer-A")

	if e2.SeqNo != e1.SeqNo {
		t.Fatalf("repeat AddDirectPeer should not bump SeqNo: first=%d, second=%d",
			e1.SeqNo, e2.SeqNo)
	}
	if tbl.Size() != 1 {
		t.Fatalf("should still be 1 route, got %d", tbl.Size())
	}
}

func TestAddDirectPeerBumpsSeqNoAfterWithdrawal(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	e1 := mustAddDirect(t, tbl, "peer-A")
	mustRemoveDirect(t, tbl, "peer-A")
	e2 := mustAddDirect(t, tbl, "peer-A")

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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(60*time.Second),
	)

	res1, err := tbl.AddDirectPeer("peer-A")
	if err != nil {
		t.Fatalf("first AddDirectPeer failed: %v", err)
	}
	current = t2
	res2, err := tbl.AddDirectPeer("peer-A")
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
	tbl := NewTable(WithLocalOrigin("me"))

	// Direct route via AddDirectPeer
	mustAddDirect(t, tbl, "peer-A")

	// Transit route: learned from peer-A via announcement
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "x", NextHop: "peer-A",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// Transit route: confirmed via hop_ack
	mustUpdate(t, tbl, RouteEntry{
		Identity: "carol", Origin: "y", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})
	// Unrelated route via different peer
	mustAddDirect(t, tbl, "dave")

	result := mustRemoveDirect(t, tbl, "peer-A")

	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 wire withdrawal, got %d", len(result.Withdrawals))
	}
	w := result.Withdrawals[0]
	if w.Identity != "peer-A" || w.Origin != "me" || w.Hops != HopsInfinity {
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
	tbl := NewTable(WithLocalOrigin("me"))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	result := mustRemoveDirect(t, tbl, "peer-A")

	if len(result.Withdrawals) != 0 {
		t.Fatal("already-withdrawn routes should not appear in Withdrawals")
	}
	if result.TransitInvalidated != 0 {
		t.Fatal("already-withdrawn routes should not be counted as TransitInvalidated")
	}
}

func TestRemoveDirectPeerIdempotent(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustRemoveDirect(t, tbl, "peer-A")
	result := mustRemoveDirect(t, tbl, "peer-A")

	if result.TransitInvalidated != 0 {
		t.Fatal("second disconnect should not re-invalidate")
	}
}

func TestRemoveDirectPeerRequiresLocalOrigin(t *testing.T) {
	tbl := NewTable()

	_, err := tbl.RemoveDirectPeer("peer-A")
	if err != ErrNoLocalOrigin {
		t.Fatalf("expected ErrNoLocalOrigin, got %v", err)
	}
}

func TestRemoveDirectPeerWireReadyWithdrawals(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	mustAddDirect(t, tbl, "peer-A")
	mustAddDirect(t, tbl, "peer-B")

	result := mustRemoveDirect(t, tbl, "peer-A")

	if len(result.Withdrawals) != 1 {
		t.Fatalf("expected 1 withdrawal, got %d", len(result.Withdrawals))
	}
	w := result.Withdrawals[0]

	// Verify the withdrawal is wire-ready (AnnounceEntry format)
	if w.Hops != HopsInfinity {
		t.Fatalf("withdrawal must have hops=%d, got %d", HopsInfinity, w.Hops)
	}
	if w.Origin != "me" {
		t.Fatalf("withdrawal origin must be localOrigin, got %q", w.Origin)
	}

	// Verify peer-B is unaffected
	routes := tbl.Lookup("peer-B")
	if len(routes) != 1 || routes[0].Hops != 1 {
		t.Fatal("unrelated peer should be unaffected")
	}
}

// --- InvalidateTransitRoutes ---

func TestInvalidateTransitRoutesSkipsDirectRoutes(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	// Direct route for peer-A (source=direct, origin=me).
	mustAddDirect(t, tbl, "peer-A")

	// Transit route learned through peer-A (source=announcement).
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	})
	if err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	invalidated, exposed := tbl.InvalidateTransitRoutes("peer-A")
	if invalidated != 1 {
		t.Fatalf("expected 1 transit route invalidated, got %d", invalidated)
	}

	// No backup route exists for target-X, so nothing should be exposed.
	if len(exposed) != 0 {
		t.Fatalf("expected no exposed backups (no alternative route), got %v", exposed)
	}

	// Direct route should be untouched — Lookup returns active routes.
	routes := tbl.Lookup("peer-A")
	if len(routes) != 1 || routes[0].IsWithdrawn() {
		t.Fatal("direct route should not be invalidated")
	}

	// Transit route should be withdrawn. Lookup() filters withdrawn entries,
	// so use Snapshot() to inspect raw table state.
	snap := tbl.Snapshot()
	snapRoutes := snap.Routes["target-X"]
	if len(snapRoutes) != 1 {
		t.Fatalf("expected 1 route in snapshot for target-X, got %d", len(snapRoutes))
	}
	if !snapRoutes[0].IsWithdrawn() {
		t.Fatal("transit route should be invalidated (hops=infinity)")
	}

	// Lookup must return empty for withdrawn routes.
	if len(tbl.Lookup("target-X")) != 0 {
		t.Fatal("Lookup should return empty for withdrawn transit route")
	}
}

func TestInvalidateTransitRoutesNoMatch(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(DefaultTTL),
	})
	if err != nil {
		t.Fatalf("UpdateRoute failed: %v", err)
	}

	// Invalidate for peer-A — should not touch peer-B's route.
	invalidated, exposed := tbl.InvalidateTransitRoutes("peer-A")
	if invalidated != 0 {
		t.Fatalf("expected 0 invalidated, got %d", invalidated)
	}
	if len(exposed) != 0 {
		t.Fatalf("expected no exposed backups, got %v", exposed)
	}
}

// --- Disconnect exposed backups ---

func TestRemoveDirectPeerExposesBackupRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	// Direct route to peer-A (will be withdrawn on disconnect).
	mustAddDirect(t, tbl, "peer-A")

	// peer-A also serves as next-hop for a transit route to target-X.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	// Backup route to target-X via peer-B (survives disconnect).
	mustUpdate(t, tbl, RouteEntry{
		Identity: "target-X", Origin: "peer-B", NextHop: "peer-B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	result, err := tbl.RemoveDirectPeer("peer-A")
	if err != nil {
		t.Fatalf("RemoveDirectPeer failed: %v", err)
	}

	// target-X should be exposed because the backup via peer-B survives.
	if len(result.ExposedBackups) != 1 || result.ExposedBackups[0] != "target-X" {
		t.Fatalf("expected [target-X] exposed, got %v", result.ExposedBackups)
	}
}

func TestRemoveDirectPeerNoBackupNoExposed(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	mustAddDirect(t, tbl, "peer-A")

	// Only route to target-X goes through peer-A — no backup.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "target-X", Origin: "target-X", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	result, err := tbl.RemoveDirectPeer("peer-A")
	if err != nil {
		t.Fatalf("RemoveDirectPeer failed: %v", err)
	}

	// No backup survives — nothing exposed.
	if len(result.ExposedBackups) != 0 {
		t.Fatalf("expected no exposed backups, got %v", result.ExposedBackups)
	}
}

func TestInvalidateTransitRoutesExposesBackupRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	// Transit route to target-X via peer-A (will be invalidated).
	mustUpdate(t, tbl, RouteEntry{
		Identity: "target-X", Origin: "origin-A", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	// Backup route to target-X via peer-B (survives invalidation).
	mustUpdate(t, tbl, RouteEntry{
		Identity: "target-X", Origin: "origin-B", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: time.Now().Add(2 * time.Hour),
	})

	invalidated, exposed := tbl.InvalidateTransitRoutes("peer-A")
	if invalidated != 1 {
		t.Fatalf("expected 1 invalidated, got %d", invalidated)
	}
	if len(exposed) != 1 || exposed[0] != "target-X" {
		t.Fatalf("expected [target-X] exposed, got %v", exposed)
	}

	// Backup via peer-B should still be reachable.
	routes := tbl.Lookup("target-X")
	if len(routes) != 1 || routes[0].NextHop != "peer-B" {
		t.Fatalf("expected backup via peer-B, got %v", routes)
	}
}

// --- TTL ---

func TestTickTTLRemovesExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Second),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "me", NextHop: "bob",
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
		Identity: "alice", Origin: "x", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(-time.Second),
	})

	tbl.TickTTL()

	snap := tbl.Snapshot()
	if _, exists := snap.Routes["alice"]; exists {
		t.Fatal("identity with no routes should be removed from map")
	}
}

func TestTickTTLRemovesWithdrawnRoutes(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := start
	tbl := NewTable(
		WithClock(func() time.Time { return now }),
		WithLocalOrigin("me"),
		WithDefaultTTL(120*time.Second),
	)

	// Add a direct route and a transit route through "alice", plus
	// a healthy announcement through "carol".
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: start.Add(2 * time.Hour),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "alice", NextHop: "alice",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: start.Add(2 * time.Hour),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "carol", Origin: "x", NextHop: "carol",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: start.Add(2 * time.Hour),
	})

	if tbl.Size() != 3 {
		t.Fatalf("expected 3 routes before withdraw, got %d", tbl.Size())
	}

	// Simulate peer "alice" disconnecting. RemoveDirectPeer sets
	// Hops=HopsInfinity and ExpiresAt=now+defaultTTL on both the
	// direct and transit routes.
	result, err := tbl.RemoveDirectPeer("alice")
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
	if _, exists := snap.Routes["carol"]; !exists {
		t.Fatal("carol's route should survive tick")
	}
	if _, exists := snap.Routes["alice"]; exists {
		t.Fatal("alice's withdrawn direct route should be removed after TTL")
	}
	if _, exists := snap.Routes["bob"]; exists {
		t.Fatal("bob's invalidated transit route should be removed after TTL")
	}
}

func TestDefaultTTLAppliedOnInsert(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithDefaultTTL(60*time.Second),
	)

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	snap := tbl.Snapshot()
	r := snap.Routes["alice"][0]
	expectedExpiry := now.Add(60 * time.Second)
	if !r.ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("expected ExpiresAt=%v, got %v", expectedExpiry, r.ExpiresAt)
	}
}

func TestExplicitExpiresAtPreserved(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	custom := now.Add(999 * time.Second)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: custom,
	})

	snap := tbl.Snapshot()
	if !snap.Routes["alice"][0].ExpiresAt.Equal(custom) {
		t.Fatal("explicitly set ExpiresAt should be preserved")
	}
}

// --- SeqNo counter sync ---

func TestSeqCounterSyncsWithExternalOwnOrigin(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	// Simulate table restored from snapshot with own-origin route at SeqNo=50.
	// This bypasses AddDirectPeer, so seqCounters starts at 0.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 50, Source: RouteSourceDirect,
	})

	// Now RemoveDirectPeer must produce SeqNo > 50, not SeqNo=1.
	result, err := tbl.RemoveDirectPeer("alice")
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
	tbl := NewTable(WithLocalOrigin("me"))

	// Foreign-origin route with high SeqNo — must NOT affect our counter.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "other-node", NextHop: "bob",
		Hops: 2, SeqNo: 999, Source: RouteSourceAnnouncement,
	})

	// AddDirectPeer for alice should start from 1, not 999+1.
	entry := mustAddDirect(t, tbl, "alice")
	if entry.SeqNo != 1 {
		t.Fatalf("foreign-origin SeqNo should not affect own counter: expected 1, got %d",
			entry.SeqNo)
	}
}

// --- Lookup ordering ---

func TestLookupSortsBySourceThenHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	// announcement with 1 hop — fewest hops but lowest trust
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "n1",
		Hops: 1, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	// direct with 1 hop — highest trust
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	// hop_ack with 3 hops — middle trust, most hops
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "z", NextHop: "n3",
		Hops: 3, SeqNo: 1, Source: RouteSourceHopAck,
	})

	routes := tbl.Lookup("alice")
	if len(routes) != 3 {
		t.Fatalf("expected 3 routes, got %d", len(routes))
	}
	// Source priority: direct > hop_ack > announcement
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
		Identity: "alice", Origin: "x", NextHop: "n1",
		Hops: 5, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "y", NextHop: "n2",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	routes := tbl.Lookup("alice")
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

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "n1",
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
		ExpiresAt: now.Add(-time.Second),
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "z", NextHop: "n3",
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})

	routes := tbl.Lookup("alice")
	if len(routes) != 1 {
		t.Fatalf("expected 1 active route, got %d", len(routes))
	}
	if routes[0].NextHop != "n3" {
		t.Fatal("only the non-withdrawn, non-expired route should remain")
	}
}

// --- Snapshot immutability ---

func TestSnapshotIsImmutable(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "x", NextHop: "n1",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	snap := tbl.Snapshot()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 2, Source: RouteSourceDirect,
	})

	if snap.Routes["alice"][0].Hops != 2 {
		t.Fatal("snapshot should not be affected by later table mutations")
	}
}

// --- NextHop is peer identity (not transport address) ---

func TestNextHopIsPeerIdentity(t *testing.T) {
	tbl := NewTable()

	status := mustUpdate(t, tbl, RouteEntry{
		Identity: "ed25519:abc123", Origin: "me", NextHop: "ed25519:abc123",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	if status != RouteAccepted {
		t.Fatal("route with identity-style NextHop should be accepted")
	}

	routes := tbl.Lookup("ed25519:abc123")
	if routes[0].NextHop != "ed25519:abc123" {
		t.Fatal("NextHop should be preserved as peer identity")
	}
}

// --- Trust hierarchy per triple ---

func TestTrustHierarchyPerTripleNotPerNextHop(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "C", NextHop: "B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "D", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	if tbl.Size() != 2 {
		t.Fatal("different origins should be independent")
	}

	snap := tbl.Snapshot()
	for _, r := range snap.Routes["alice"] {
		if r.Origin == "C" && r.Source != RouteSourceAnnouncement {
			t.Fatal("upgrading one triple's trust should not affect another triple")
		}
	}
}

// --- HopAck confirms specific route only ---

func TestHopAckScopedToTriple(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "C", NextHop: "B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "D", NextHop: "B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "C", NextHop: "B",
		Hops: 2, SeqNo: 1, Source: RouteSourceHopAck,
	})

	snap := tbl.Snapshot()
	for _, r := range snap.Routes["alice"] {
		if r.Origin == "D" && r.Source != RouteSourceAnnouncement {
			t.Fatal("hop_ack for (alice,C,B) should not upgrade (alice,D,B)")
		}
		if r.Origin == "C" && r.Source != RouteSourceHopAck {
			t.Fatal("hop_ack for (alice,C,B) should upgrade it")
		}
	}
}

// --- Edge cases ---

func TestLookupUnknownIdentity(t *testing.T) {
	tbl := NewTable()
	routes := tbl.Lookup("nonexistent")
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
		Identity: "alice", Origin: "x", NextHop: "n1",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "y", NextHop: "n2",
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(30*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Three rapid connect/disconnect cycles within the flap window.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, "flappy")
		mustRemoveDirect(t, tbl, "flappy")
		current = current.Add(5 * time.Second)
	}

	// Fourth reconnect should be penalized — hold-down is active.
	result, err := tbl.AddDirectPeer("flappy")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Two disconnects — below threshold of 3.
	for i := 0; i < 2; i++ {
		mustAddDirect(t, tbl, "stable")
		mustRemoveDirect(t, tbl, "stable")
		current = current.Add(5 * time.Second)
	}

	result, err := tbl.AddDirectPeer("stable")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(60*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(30*time.Second),
		WithPenalizedTTL(15*time.Second),
	)

	// Trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, "flappy")
		mustRemoveDirect(t, tbl, "flappy")
		current = current.Add(2 * time.Second)
	}

	// Advance past hold-down duration (30s) AND past flap window (60s)
	// so previous withdrawal events are stale.
	current = current.Add(90 * time.Second)

	result, err := tbl.AddDirectPeer("flappy")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(30*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(10*time.Second),
		WithPenalizedTTL(10*time.Second),
	)

	// Two disconnects early.
	for i := 0; i < 2; i++ {
		mustAddDirect(t, tbl, "peer")
		mustRemoveDirect(t, tbl, "peer")
		current = current.Add(2 * time.Second)
	}

	// Wait for first two disconnects to fall outside the 30s window.
	current = current.Add(35 * time.Second)

	// One more disconnect — only 1 in window, below threshold.
	mustAddDirect(t, tbl, "peer")
	mustRemoveDirect(t, tbl, "peer")

	result, err := tbl.AddDirectPeer("peer")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapWindow(30*time.Second),
		WithFlapThreshold(3),
		WithHoldDownDuration(10*time.Second),
		WithPenalizedTTL(10*time.Second),
	)

	// Trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, "peer")
		mustRemoveDirect(t, tbl, "peer")
		current = current.Add(1 * time.Second)
	}

	// Advance past both hold-down (10s) and flap window (30s).
	current = current.Add(60 * time.Second)
	tbl.TickTTL()

	// Internal flap state should be cleaned — verify by reconnecting
	// and checking that it's not penalized (no stale state lingering).
	result, err := tbl.AddDirectPeer("peer")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
	)

	// Flap peer-A three times.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, "peer-A")
		mustRemoveDirect(t, tbl, "peer-A")
		current = current.Add(1 * time.Second)
	}

	// peer-B connects for the first time — should not be penalized.
	result, err := tbl.AddDirectPeer("peer-B")
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
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithFlapThreshold(3),
		WithFlapWindow(60*time.Second),
		WithHoldDownDuration(30*time.Second),
	)

	// Flap peer-A three times to trigger hold-down.
	for i := 0; i < 3; i++ {
		mustAddDirect(t, tbl, "peer-A")
		mustRemoveDirect(t, tbl, "peer-A")
		current = current.Add(5 * time.Second)
	}

	// Immediately after flapping: should appear in snapshot.
	snap := tbl.FlapSnapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 flap entry, got %d", len(snap))
	}
	if snap[0].PeerIdentity != "peer-A" {
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
		WithLocalOrigin("self"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, "peer1")

	// Verify ExpiresAt is zero — direct routes are event-driven.
	routes := tbl.Lookup("peer1")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if !routes[0].ExpiresAt.IsZero() {
		t.Fatalf("direct route ExpiresAt should be zero, got %v", routes[0].ExpiresAt)
	}

	// Advance far beyond any TTL — route must still be alive.
	current = now.Add(24 * time.Hour)

	routes = tbl.Lookup("peer1")
	if len(routes) != 1 {
		t.Fatalf("direct route must survive indefinitely, got %d routes", len(routes))
	}

	// TickTTL must not remove it.
	tbl.TickTTL()
	routes = tbl.Lookup("peer1")
	if len(routes) != 1 {
		t.Fatalf("TickTTL must not remove live direct route, got %d routes", len(routes))
	}
}

func TestDirectRouteRemovedOnlyBySocketEvent(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	current := now

	tbl := NewTable(
		WithLocalOrigin("self"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, "peer1")

	// RemoveDirectPeer (socket close) withdraws and sets tombstone TTL.
	mustRemoveDirect(t, tbl, "peer1")

	routes := tbl.Lookup("peer1")
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
		WithLocalOrigin("self"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(120*time.Second),
	)

	mustAddDirect(t, tbl, "peer1")
	mustRemoveDirect(t, tbl, "peer1")

	// Peer reconnects — new direct route replaces tombstone.
	current = current.Add(5 * time.Second)
	mustAddDirect(t, tbl, "peer1")

	routes := tbl.Lookup("peer1")
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
	// Scenario: identity "alice" has two routes — a primary (expires soon) and
	// a backup (long TTL, non-withdrawn). When the primary expires, TickTTL
	// should return "alice" because a usable backup route was exposed.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(WithClock(func() time.Time { return current }))

	// Primary route — about to expire.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "origin-A", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})
	// Backup route — lives much longer.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "origin-B", NextHop: "peer-B",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(2 * time.Hour),
	})

	// Before expiry: nothing exposed.
	exposed := tbl.TickTTL()
	if len(exposed) != 0 {
		t.Fatalf("expected no exposed identities before expiry, got %v", exposed)
	}

	// Advance past primary's TTL.
	current = now.Add(11 * time.Second)
	exposed = tbl.TickTTL()

	if len(exposed) != 1 || exposed[0] != "alice" {
		t.Fatalf("expected [alice] exposed, got %v", exposed)
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
		WithLocalOrigin("me"),
		WithDefaultTTL(120*time.Second),
	)

	// Non-withdrawn route — will expire.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "origin-A", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})
	// Withdrawn tombstone — survives longer but is not usable.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "bob", Origin: "origin-B", NextHop: "peer-B",
		Hops: HopsInfinity, SeqNo: 5, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(2 * time.Hour),
	})

	current = now.Add(11 * time.Second)
	exposed := tbl.TickTTL()

	if len(exposed) != 0 {
		t.Fatalf("withdrawn-only survivors should not be exposed, got %v", exposed)
	}
}

func TestTickTTLNoExposedWhenNothingRemoved(t *testing.T) {
	// If no routes expired, nothing is exposed.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "carol", Origin: "x", NextHop: "peer-C",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(time.Hour),
	})

	exposed := tbl.TickTTL()
	if len(exposed) != 0 {
		t.Fatalf("no expiry means no exposed, got %v", exposed)
	}
}

func TestTickTTLNoExposedWhenAllRoutesExpire(t *testing.T) {
	// If ALL routes for an identity expire (identity deleted), it should NOT
	// appear in exposed — there's nothing to drain to.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	current := now
	tbl := NewTable(WithClock(func() time.Time { return current }))

	mustUpdate(t, tbl, RouteEntry{
		Identity: "dave", Origin: "origin-A", NextHop: "peer-A",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		ExpiresAt: now.Add(10 * time.Second),
	})

	current = now.Add(11 * time.Second)
	exposed := tbl.TickTTL()

	if len(exposed) != 0 {
		t.Fatalf("all-expired identity should not be exposed, got %v", exposed)
	}
	if tbl.Size() != 0 {
		t.Fatalf("expected empty table, got %d", tbl.Size())
	}
}

func TestUpdateRoutePreservesExtra(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("local"))
	extra := json.RawMessage(`{"onion_box":"deadbeef"}`)
	entry := RouteEntry{
		Identity: "X", Origin: "A", NextHop: "B",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		Extra: extra,
	}
	status := mustUpdate(t, tbl, entry)
	if status != RouteAccepted {
		t.Fatalf("expected accepted, got %v", status)
	}

	routes := tbl.Lookup("X")
	if len(routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(routes))
	}
	if string(routes[0].Extra) != string(extra) {
		t.Fatalf("Extra lost after UpdateRoute: got %s", string(routes[0].Extra))
	}
}

func TestToAnnounceEntryPreservesExtra(t *testing.T) {
	extra := json.RawMessage(`{"onion_box":"deadbeef","future":true}`)
	entry := RouteEntry{
		Identity: "X", Origin: "A", NextHop: "B",
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
		Identity: "X", Origin: "A", NextHop: "A",
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	routes := tbl.Lookup("nodeA")
	if len(routes) == 0 {
		t.Fatal("Lookup for own identity must return at least one route")
	}

	self := routes[0]
	if self.Identity != "nodeA" {
		t.Fatalf("expected Identity=nodeA, got %s", self.Identity)
	}
	if self.Origin != "nodeA" {
		t.Fatalf("expected Origin=nodeA, got %s", self.Origin)
	}
	if self.NextHop != "nodeA" {
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	// Even with a completely empty table, self-route must be present.
	routes := tbl.Lookup("nodeA")
	if len(routes) != 1 {
		t.Fatalf("expected exactly 1 route (self), got %d", len(routes))
	}
	if routes[0].Source != RouteSourceLocal {
		t.Fatalf("expected local source, got %s", routes[0].Source)
	}
}

func TestLookupOwnIdentitySelfRouteHasHighestPriority(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	// Add an announcement route for the same identity via a remote peer.
	mustUpdate(t, tbl, RouteEntry{
		Identity: "nodeA", Origin: "peerB", NextHop: "peerB",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	routes := tbl.Lookup("nodeA")
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	routes := tbl.Lookup("nodeB")
	if len(routes) != 0 {
		t.Fatalf("expected 0 routes for unknown identity, got %d", len(routes))
	}
}

func TestSnapshotContainsSelfRoute(t *testing.T) {
	tbl := NewTable(
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	snap := tbl.Snapshot()

	selfRoutes, ok := snap.Routes["nodeA"]
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	snap := tbl.Snapshot()
	best := snap.BestRoute("nodeA")
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
		WithLocalOrigin("nodeA"),
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	// Add a direct peer so there's at least one real route.
	mustAddDirect(t, tbl, "peerB")

	entries := tbl.AnnounceTo("peerB")
	for _, e := range entries {
		if e.Identity == "nodeA" && e.Hops == 0 {
			t.Fatal("self-route (Hops=0) must not be included in announcements")
		}
	}
}

func TestLookupWithoutLocalOriginNoSelfRoute(t *testing.T) {
	tbl := NewTable()

	routes := tbl.Lookup("someNode")
	if len(routes) != 0 {
		t.Fatalf("without localOrigin, no self-route should be injected, got %d", len(routes))
	}
}

func TestSnapshotCountersMatchActiveSize(t *testing.T) {
	now := time.Now()
	tbl := NewTable(
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(now)),
	)

	// Empty table — counters must agree and both be zero.
	snap := tbl.Snapshot()
	if snap.ActiveEntries != tbl.ActiveSize() {
		t.Fatalf("empty table: ActiveEntries=%d != ActiveSize()=%d",
			snap.ActiveEntries, tbl.ActiveSize())
	}

	// Add a real direct peer.
	mustAddDirect(t, tbl, "peerB")

	snap = tbl.Snapshot()
	if snap.ActiveEntries != tbl.ActiveSize() {
		t.Fatalf("with direct peer: ActiveEntries=%d != ActiveSize()=%d",
			snap.ActiveEntries, tbl.ActiveSize())
	}
	if snap.ActiveEntries != 1 {
		t.Fatalf("expected ActiveEntries=1 (one real route), got %d", snap.ActiveEntries)
	}

	// Self-route must be visible in Routes map but not in counters.
	selfRoutes := snap.Routes["nodeA"]
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
		WithLocalOrigin("nodeA"),
		WithClock(fixedClock(time.Now())),
	)

	// Attempt to persist a RouteSourceLocal entry for a remote identity.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: "remoteNode",
		Origin:   "remoteNode",
		NextHop:  "remoteNode",
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
		Identity: "nodeA",
		Origin:   "nodeA",
		NextHop:  "nodeA",
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
