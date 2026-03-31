package routing

import (
	"testing"
	"time"
)

// fixedClock returns a clock function that always returns the given time.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// mustUpdate calls UpdateRoute and fails the test on error, returns accepted.
func mustUpdate(t *testing.T, tbl *Table, entry RouteEntry) bool {
	t.Helper()
	accepted, err := tbl.UpdateRoute(entry)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	return accepted
}

// mustAddDirect calls AddDirectPeer and fails the test on error.
func mustAddDirect(t *testing.T, tbl *Table, peerID string) RouteEntry {
	t.Helper()
	entry, err := tbl.AddDirectPeer(peerID)
	if err != nil {
		t.Fatalf("AddDirectPeer(%q) unexpected error: %v", peerID, err)
	}
	return entry
}

// mustRemoveDirect calls RemoveDirectPeer and fails the test on error.
func mustRemoveDirect(t *testing.T, tbl *Table, peerID string) RemoveDirectPeerResult {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if !accepted {
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
	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: HopsInfinity, SeqNo: 1, Source: RouteSourceAnnouncement,
	})
	if !accepted {
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
	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})
	if !accepted {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, SeqNo: 10, Source: RouteSourceAnnouncement,
	})

	if !accepted {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if accepted {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "dave", NextHop: "charlie",
		Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement,
	})

	if !accepted {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceHopAck,
	})

	if !accepted {
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if accepted {
		t.Fatal("same SeqNo but lower trust should be rejected")
	}
}

func TestSameSeqNoSameTrustFewerHopsWins(t *testing.T) {
	tbl := NewTable()

	mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	if !accepted {
		t.Fatal("same SeqNo and trust, fewer hops should be accepted")
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

func TestWithdrawRouteNonexistent(t *testing.T) {
	tbl := NewTable()

	ok := tbl.WithdrawRoute("alice", "bob", "charlie", 1)
	if ok {
		t.Fatal("withdrawal of nonexistent route should return false")
	}
}

// --- AddDirectPeer ---

func TestAddDirectPeerCreatesRoute(t *testing.T) {
	tbl := NewTable(WithLocalOrigin("me"))

	entry, err := tbl.AddDirectPeer("peer-A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

func TestAddDirectPeerRefreshesTTLOnRepeat(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(30 * time.Second)
	current := t1
	tbl := NewTable(
		WithLocalOrigin("me"),
		WithClock(func() time.Time { return current }),
		WithDefaultTTL(60*time.Second),
	)

	mustAddDirect(t, tbl, "peer-A")
	current = t2
	mustAddDirect(t, tbl, "peer-A")

	snap := tbl.Snapshot()
	r := snap.Routes["peer-A"][0]
	expected := t2.Add(60 * time.Second)
	if !r.ExpiresAt.Equal(expected) {
		t.Fatalf("repeat AddDirectPeer should refresh TTL: expected %v, got %v",
			expected, r.ExpiresAt)
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

	accepted := mustUpdate(t, tbl, RouteEntry{
		Identity: "ed25519:abc123", Origin: "me", NextHop: "ed25519:abc123",
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	})

	if !accepted {
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
