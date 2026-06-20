package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// sync_vector_test.go covers Table.ReconcileSyncVector — the Phase-0
// version-vector receive path that replaces the all-or-nothing digest
// hash compare. The headline property is that a route the receiver did
// NOT adopt under its K-cap (an identity in the peer's vector we do not
// hold via that peer) must NOT spoil the match, so a converged pair can
// fall silent even when the peer offers routes we rejected — exactly the
// case a single whole-table digest could never match.

// TestReconcileSyncVector_MatchUnderCapRejectedOffer — we hold X via P at
// the SeqNo the peer announces, plus the peer offers Y we never adopted
// (cap-rejected: no via-P claim for Y). Match must be true (Y is the
// peer's offer we legitimately did not keep) and X must be refreshed.
func TestReconcileSyncVector_MatchUnderCapRejectedOffer(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	x := domaintest.ID("x")
	y := domaintest.ID("y")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})

	// Peer P's vector: X@5 (which we hold via P) plus Y@9 (which we do NOT
	// hold via P — its offer lost our cap or never landed).
	want := []DigestEntry{{Identity: x, MaxSeqNo: 5}, {Identity: y, MaxSeqNo: 9}}
	match, refreshed := tbl.ReconcileSyncVector(peerP, want, clk)
	if !match {
		t.Fatal("match=false despite holding exactly X@5 via P; a cap-rejected offer (Y) must not spoil the match")
	}
	if refreshed != 1 {
		t.Fatalf("refreshed=%d, want 1 (only X via P)", refreshed)
	}
}

// TestReconcileSyncVector_RefreshesMatchingSeqNoTTL — a matching entry
// renews the route's TTL so it survives past its original expiry.
func TestReconcileSyncVector_RefreshesMatchingSeqNoTTL(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	peerZ := domaintest.ID("z") // split-horizon-distinct announce target
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(10 * time.Second)})

	clk = clk.Add(9 * time.Second)
	match, refreshed := tbl.ReconcileSyncVector(peerP, []DigestEntry{{Identity: x, MaxSeqNo: 5}}, clk)
	if !match || refreshed != 1 {
		t.Fatalf("match=%v refreshed=%d, want true/1", match, refreshed)
	}

	// Past the ORIGINAL 10s expiry but inside the renewed DefaultTTL window.
	clk = clk.Add(5 * time.Second) // t=14s
	tbl.TickTTL()
	entries := tbl.AnnounceTo(peerZ)
	defer tbl.ReleaseAnnounceEntries(entries)
	if _, ok := entryByIdentity(entries)[x]; !ok {
		t.Fatal("route x aged out despite a matching vector entry renewing its TTL")
	}
}

// TestReconcileSyncVector_StaleSeqNoNoMatchNoRefresh — we hold X@5 via P
// but the peer now announces X@7. The route is stale on our side: not
// refreshed (it ages out), and match=false so the sender escalates a
// full.
func TestReconcileSyncVector_StaleSeqNoNoMatchNoRefresh(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	peerZ := domaintest.ID("z")
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(10 * time.Second)})

	clk = clk.Add(9 * time.Second)
	match, refreshed := tbl.ReconcileSyncVector(peerP, []DigestEntry{{Identity: x, MaxSeqNo: 7}}, clk)
	if match {
		t.Fatal("match=true on a stale SeqNo; want false so the sender reconciles")
	}
	if refreshed != 0 {
		t.Fatalf("refreshed=%d, want 0 (stale route must not be extended)", refreshed)
	}

	// The un-refreshed route ages out at its ORIGINAL expiry.
	clk = clk.Add(5 * time.Second) // t=14s, past the 10s expiry
	tbl.TickTTL()
	entries := tbl.AnnounceTo(peerZ)
	defer tbl.ReleaseAnnounceEntries(entries)
	if _, ok := entryByIdentity(entries)[x]; ok {
		t.Fatal("stale route x survived; an unconfirmed route must be left to age out")
	}
}

// TestReconcileSyncVector_AbsentIdentityNoMatch — we hold X via P but the
// peer's vector does not mention X at all (it withdrew/replaced it). That
// is a stale route on our side: match=false, no refresh.
func TestReconcileSyncVector_AbsentIdentityNoMatch(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})

	match, refreshed := tbl.ReconcileSyncVector(peerP, nil, clk)
	if match {
		t.Fatal("match=true while holding X via P that the empty vector does not confirm; want false")
	}
	if refreshed != 0 {
		t.Fatalf("refreshed=%d, want 0", refreshed)
	}
}

// TestReconcileSyncVector_DeadMatchResurrectsQuestionable — a matching
// vector entry is the same "peer is still announcing this route" liveness
// signal as a wire-identical UpdateRoute refresh, so a Dead via-peer route
// the vector confirms must be evicted + reseeded Questionable (mirroring
// the UpdateRoute Dead-resurrection contract / PR 11.21 P2#1), not just
// kept Dead-but-fresh and filtered from Lookup until the slow safety full.
func TestReconcileSyncVector_DeadMatchResurrectsQuestionable(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})
	tbl.ForceHealthForTest(x, peerP, HealthDead)

	// Precondition: a Dead transit route is filtered out of Lookup.
	if routes := tbl.Lookup(x); len(routes) != 0 {
		t.Fatalf("setup: Lookup returned %d entries with Dead health, want 0", len(routes))
	}

	match, refreshed := tbl.ReconcileSyncVector(peerP, []DigestEntry{{Identity: x, MaxSeqNo: 5}}, clk)
	if !match || refreshed != 1 {
		t.Fatalf("match=%v refreshed=%d, want true/1", match, refreshed)
	}

	// The confirmed Dead pair must be resurrected to Questionable and
	// surface in Lookup again.
	routes := tbl.Lookup(x)
	if len(routes) != 1 {
		t.Fatalf("Dead route not resurrected by a matching vector: Lookup returned %d, want 1", len(routes))
	}
}

// TestReconcileSyncVector_ResurrectionJournalsDelta — resurrecting a Dead
// route makes it announceable again, which is a real wire-projection
// change, so it must be journaled: a peer with a current cursor receives
// the re-added route via AnnounceDeltaTo, not only after a full sync.
func TestReconcileSyncVector_ResurrectionJournalsDelta(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p") // uplink the route is learned through
	peerZ := domaintest.ID("z") // delta target (split-horizon distinct)
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})
	tbl.ForceHealthForTest(x, peerP, HealthDead)

	// Capture the change-log head AFTER X went Dead — a delta from here must
	// be empty until the resurrection re-journals X.
	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	tbl.ReleaseAnnounceEntries(base)

	if match, refreshed := tbl.ReconcileSyncVector(peerP, []DigestEntry{{Identity: x, MaxSeqNo: 5}}, clk); !match || refreshed != 1 {
		t.Fatalf("ReconcileSyncVector match=%v refreshed=%d, want true/1", match, refreshed)
	}

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	defer tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("resurrection forced needFull instead of a cursor delta")
	}
	if _, ok := entryByIdentity(delta)[x]; !ok {
		t.Fatalf("resurrected route x not emitted in the cursor delta: %+v", delta)
	}
}

// TestReconcileSyncVector_OnlyTouchesViaPeer — a route via a DIFFERENT
// uplink (Q) is neither refreshed nor allowed to affect P's match verdict.
func TestReconcileSyncVector_OnlyTouchesViaPeer(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	peerQ := domaintest.ID("q")
	x := domaintest.ID("x") // via P
	z := domaintest.ID("z") // via Q

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})
	mustUpdateRoute(t, tbl, RouteEntry{Identity: z, Origin: origin, NextHop: peerQ, Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(time.Hour)})

	// P's vector confirms only X. Z via Q must be ignored entirely.
	match, refreshed := tbl.ReconcileSyncVector(peerP, []DigestEntry{{Identity: x, MaxSeqNo: 5}}, clk)
	if !match {
		t.Fatal("match=false; a route via a different uplink (Q) must not spoil P's verdict")
	}
	if refreshed != 1 {
		t.Fatalf("refreshed=%d, want 1 (only X via P)", refreshed)
	}
}
