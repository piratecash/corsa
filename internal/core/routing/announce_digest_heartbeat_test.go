package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_digest_heartbeat_test.go covers the digest-as-heartbeat freshness
// path: the periodic deadline emits a cheap route_sync_digest_v1 instead of an
// unconditional full sync, the receiver renews TTL on a match
// (Table.RefreshRoutesVia), and a full rebuild fires only when the previous
// heartbeat did not confirm a match. See the "Digest-as-heartbeat refresh"
// section in docs/routing.md.

// --- send-side state machine: AnnounceLoop.digestHeartbeatStatus ---

func TestDigestHeartbeatStatus_NoEntryBootstrapsDueUnconfirmed(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	due, prevConfirmed := a.digestHeartbeatStatus(domaintest.ID("peer"), now)
	if !due || prevConfirmed {
		t.Fatalf("no entry: got due=%v prevConfirmed=%v, want true,false (bootstrap: full + first heartbeat)", due, prevConfirmed)
	}
}

func TestDigestHeartbeatStatus_PendingWithinCadenceNotDueUnconfirmed(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second} // cadence 20s
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("peer"), now, "dig")

	due, prevConfirmed := a.digestHeartbeatStatus(domaintest.ID("peer"), now.Add(5*time.Second))
	if due || prevConfirmed {
		t.Fatalf("pending within cadence, no match yet: got due=%v prevConfirmed=%v, want false,false", due, prevConfirmed)
	}
}

func TestDigestHeartbeatStatus_ConfirmedMatchReportedNotDue(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second} // cadence 20s
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("peer"), now, "dig")
	if !a.ConfirmPeerDigestMatch(domaintest.ID("peer"), "dig", now.Add(time.Second)) {
		t.Fatal("precondition: correlated match should be accepted")
	}

	due, prevConfirmed := a.digestHeartbeatStatus(domaintest.ID("peer"), now.Add(2*time.Second))
	if due || !prevConfirmed {
		t.Fatalf("confirmed within cadence: got due=%v prevConfirmed=%v, want false,true (skip full, not yet re-due)", due, prevConfirmed)
	}
}

func TestDigestHeartbeatStatus_DueAfterCadenceElapsed(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	cadence := EffectiveForcedFullSyncInterval(a.interval) // 20s
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("peer"), now, "dig")

	// A full cadence since the last heartbeat emit → due again (next tick),
	// independent of LastSuccessfulFullSyncAt which freezes on the stable path.
	due, _ := a.digestHeartbeatStatus(domaintest.ID("peer"), now.Add(cadence))
	if !due {
		t.Fatalf("heartbeat must be due once a full cadence (%v) elapsed since the last emit", cadence)
	}
}

// TestDigestHeartbeatStatus_LostDigestEscalatesToFull pins the freshness-parity
// rule: a heartbeat that armed but never confirmed (silence / lost digest)
// reports prevConfirmed=false, so the deadline escalates to a full — exactly as
// a dropped forced-full did in the pre-digest design.
func TestDigestHeartbeatStatus_LostDigestEscalatesToFull(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("peer"), now, "dig") // emitted, never confirmed

	_, prevConfirmed := a.digestHeartbeatStatus(domaintest.ID("peer"), now.Add(time.Minute))
	if prevConfirmed {
		t.Fatal("an unconfirmed (lost/silent) heartbeat must report prevConfirmed=false so the deadline escalates to a full")
	}
}

// --- receiver-side TTL refresh: Table.RefreshRoutesVia ---

func TestRefreshRoutesVia_RenewsLearnedRouteTTL(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p") // uplink the route is learned through
	peerZ := domaintest.ID("z") // announce target (split-horizon distinct)
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(10 * time.Second)})

	// A matching digest from peerP just before the original expiry renews the TTL.
	clk = clk.Add(9 * time.Second)
	if refreshed := tbl.RefreshRoutesVia(peerP, clk); refreshed != 1 {
		t.Fatalf("RefreshRoutesVia refreshed=%d, want 1", refreshed)
	}

	// Past the ORIGINAL 10s expiry but well inside the renewed DefaultTTL window.
	clk = clk.Add(5 * time.Second) // t=14s
	tbl.TickTTL()
	entries := tbl.AnnounceTo(peerZ)
	defer tbl.ReleaseAnnounceEntries(entries)
	if _, ok := entryByIdentity(entries)[x]; !ok {
		t.Fatal("route x aged out despite RefreshRoutesVia renewing its TTL")
	}
}

// TestRefreshRoutesVia_DoesNotJournalDelta pins that a TTL bump is NOT a wire
// change: it must not synthesise an announce delta, or every heartbeat match
// would defeat the no-op suppression the cursor path relies on.
func TestRefreshRoutesVia_DoesNotJournalDelta(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	peerZ := domaintest.ID("z")
	x := domaintest.ID("x")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(1 * time.Hour)})

	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	tbl.ReleaseAnnounceEntries(base)

	tbl.RefreshRoutesVia(peerP, clk)

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	defer tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("a TTL refresh must not force needFull")
	}
	if len(delta) != 0 {
		t.Fatalf("a TTL refresh journaled a spurious delta: %+v", delta)
	}
}

// TestRefreshRoutesVia_OnlyMatchingUplink pins the split: only routes learned
// through the named peer are renewed.
func TestRefreshRoutesVia_OnlyMatchingUplink(t *testing.T) {
	origin := domaintest.ID("self")
	peerP := domaintest.ID("p")
	peerQ := domaintest.ID("q")
	x := domaintest.ID("x")
	y := domaintest.ID("y")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x, Origin: origin, NextHop: peerP, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(1 * time.Hour)})
	mustUpdateRoute(t, tbl, RouteEntry{Identity: y, Origin: origin, NextHop: peerQ, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(1 * time.Hour)})

	if refreshed := tbl.RefreshRoutesVia(peerP, clk); refreshed != 1 {
		t.Fatalf("RefreshRoutesVia(peerP) refreshed=%d, want 1 (only the via-peerP route)", refreshed)
	}
}

// --- leak guard: reconcileDigestSuppression bounds the map to live peers ---

func TestReconcileDigestSuppression_EvictsAbsentPeers(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	peerA := domaintest.ID("a")
	peerB := domaintest.ID("b")
	a.MarkPeerDigestPending(peerA, now, "da")
	a.MarkPeerDigestPending(peerB, now, "db")

	// Only peerA is still in the live set this cycle.
	a.reconcileDigestSuppression(map[PeerIdentity]struct{}{peerA: {}})

	if !a.isDigestSuppressionActive(peerA, now) {
		t.Fatal("live peerA window must survive reconcile")
	}
	if a.isDigestSuppressionActive(peerB, now) {
		t.Fatal("absent peerB window must be evicted by reconcile (unbounded-growth leak guard)")
	}
}

// TestConsumeDigestMismatch pins the single-shot, atomic correlate+consume the
// summary handler uses so a correlated mismatch counts exactly once per
// heartbeat even under replay.
func TestConsumeDigestMismatch(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second} // cadence 20s, grace 5s
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	peer := domaintest.ID("p")

	if a.ConsumeDigestMismatch(peer, "d", now) {
		t.Fatal("no pending digest must not correlate")
	}
	a.MarkPeerDigestPending(peer, now, "d")
	if a.ConsumeDigestMismatch(peer, "stale-or-spoofed", now) {
		t.Fatal("a different echo must not correlate (anti-spoof / anti-stale)")
	}
	if a.ConsumeDigestMismatch(peer, "d", now.Add(time.Hour)) {
		t.Fatal("an echo after the window elapsed must not correlate")
	}
	// First correlated mismatch consumes the entry...
	if !a.ConsumeDigestMismatch(peer, "d", now) {
		t.Fatal("the echo of our pending digest within the window must correlate")
	}
	// ...so a replayed/duplicate mismatch finds it gone and is NOT counted again.
	if a.ConsumeDigestMismatch(peer, "d", now) {
		t.Fatal("a replayed mismatch must not correlate again after consume (single-shot)")
	}

	// A digest already consumed by a confirmed MATCH must not then be counted as
	// a mismatch (the !confirmed gate).
	a.MarkPeerDigestPending(peer, now, "d2")
	if !a.ConfirmPeerDigestMatch(peer, "d2", now) {
		t.Fatal("precondition: first correlated match should confirm")
	}
	if a.ConsumeDigestMismatch(peer, "d2", now) {
		t.Fatal("a confirmed (already-consumed) digest must not be counted as a mismatch")
	}
}

func TestReconcileDigestSuppression_EmptyLiveSetClearsAll(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("a"), now, "da")

	// An isolated node with zero live peers must reclaim every window.
	a.reconcileDigestSuppression(nil)

	if a.isDigestSuppressionActive(domaintest.ID("a"), now) {
		t.Fatal("empty live set must evict all digest windows")
	}
}
