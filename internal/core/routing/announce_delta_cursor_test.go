package routing

import (
	"bytes"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_delta_cursor_test.go pins the deploy-2 cursor-authoritative
// projection (AnnounceDeltaTo / projectChangedFor): it must produce the SAME
// wire entries — including the per-receiver SeqNo — that the full
// rebuild-and-diff path produces for the same set of identities.

// buildDeltaTestTable applies an identical set of live transit routes so two
// independent tables can be compared without the SeqNo double-advance that
// projecting the same table twice would cause.
func buildDeltaTestTable(t *testing.T, origin PeerIdentity, ids, uplinks []PeerIdentity) *Table {
	t.Helper()
	tbl := NewTable(WithLocalOrigin(origin))
	for i := range ids {
		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: ids[i], Origin: origin, NextHop: uplinks[i],
			Hops: i + 1, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if err != nil || status != RouteAccepted {
			t.Fatalf("UpdateRoute(%d) status=%v err=%v", i, status, err)
		}
	}
	return tbl
}

func entryByIdentity(es []AnnounceEntry) map[PeerIdentity]AnnounceEntry {
	m := make(map[PeerIdentity]AnnounceEntry, len(es))
	for _, e := range es {
		m[e.Identity] = e
	}
	return m
}

func mustUpdateRoute(t *testing.T, tbl *Table, e RouteEntry) {
	t.Helper()
	if status, err := tbl.UpdateRoute(e); err != nil || status != RouteAccepted {
		t.Fatalf("UpdateRoute(%s) status=%v err=%v", e.Identity, status, err)
	}
}

// TestAnnounceDeltaTo_MatchesFullProjection: with cursor 0 the journal covers
// every mutated identity, so the cursor delta must emit the identical entry set
// (identity, origin, hops, SeqNo, attested sig) as the full projection.
func TestAnnounceDeltaTo_MatchesFullProjection(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	ids := []PeerIdentity{domaintest.ID("x1"), domaintest.ID("x2"), domaintest.ID("x3")}
	uplinks := []PeerIdentity{domaintest.ID("u1"), domaintest.ID("u2"), domaintest.ID("u3")}

	tblFull := buildDeltaTestTable(t, origin, ids, uplinks)
	tblDelta := buildDeltaTestTable(t, origin, ids, uplinks)

	full, _ := tblFull.AnnounceToWithChangeHead(peerZ)
	defer tblFull.ReleaseAnnounceEntries(full)

	delta, _, needFull := tblDelta.AnnounceDeltaTo(peerZ, 0)
	defer tblDelta.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("cursor 0 over a small journal must not need a full sync")
	}

	fm, dm := entryByIdentity(full), entryByIdentity(delta)
	if len(fm) != len(dm) {
		t.Fatalf("entry count mismatch: full=%d delta=%d", len(fm), len(dm))
	}
	for id, fe := range fm {
		de, ok := dm[id]
		if !ok {
			t.Fatalf("identity %s present in full projection but missing from cursor delta", id)
		}
		if de.Origin != fe.Origin || de.Hops != fe.Hops || de.SeqNo != fe.SeqNo {
			t.Fatalf("entry mismatch for %s:\n full  %+v\n delta %+v", id, fe, de)
		}
	}
}

// TestAnnounceDeltaTo_IncrementalSelectsOnlyChanged: after a baseline sync, a
// route mutation that changes wire content (a hops improvement) must produce a
// delta containing ONLY that identity, at the improved hops and a strictly
// newer wire SeqNo (the journal drove the selection).
func TestAnnounceDeltaTo_IncrementalSelectsOnlyChanged(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	x1, x2 := domaintest.ID("x1"), domaintest.ID("x2")
	u1, u2 := domaintest.ID("u1"), domaintest.ID("u2")

	tbl := NewTable(WithLocalOrigin(origin))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 3, SeqNo: 1, Source: RouteSourceAnnouncement})
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x2, Origin: origin, NextHop: u2, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})

	// Baseline full sync to peerZ; capture the cursor head and x1's wire SeqNo.
	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	baseSeq := entryByIdentity(base)[x1].SeqNo
	tbl.ReleaseAnnounceEntries(base)

	// Improve only x1's hops (3→2): changes the wire content sig, so the
	// projection re-emits at a strictly newer wire SeqNo. (A native-SeqNo-only
	// bump with identical wire content would NOT change the wire SeqNo — the
	// content cache would hit — so it is not a meaningful per-receiver change.)
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 2, Source: RouteSourceAnnouncement})

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	defer tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("single mutation within ring must be served incrementally")
	}
	if len(delta) != 1 {
		t.Fatalf("delta must contain only the mutated identity, got %d entries: %+v", len(delta), delta)
	}
	if delta[0].Identity != x1 {
		t.Fatalf("delta carried %s, expected the mutated x1", delta[0].Identity)
	}
	if delta[0].Hops != 2 {
		t.Fatalf("delta must carry the improved hops 2, got %d", delta[0].Hops)
	}
	if delta[0].SeqNo <= baseSeq {
		t.Fatalf("wire-content change must carry a strictly newer wire SeqNo: base=%d got=%d", baseSeq, delta[0].SeqNo)
	}
}

// TestAnnounceDeltaTo_TTLRefreshIsNotADelta: a same-SeqNo/same-hops
// reconfirmation refreshes ExpiresAt (RouteUnchanged, mutated) but leaves the
// wire AnnounceEntry byte-identical, so it must NOT be journaled — otherwise the
// cursor path would re-emit an unchanged route and fan it out across the mesh on
// every TTL refresh.
func TestAnnounceDeltaTo_TTLRefreshIsNotADelta(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	x1, u1 := domaintest.ID("x1"), domaintest.ID("u1")

	tbl := NewTable(WithLocalOrigin(origin))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})

	// Baseline sync; cursor at the matching head.
	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	tbl.ReleaseAnnounceEntries(base)

	// Re-apply the SAME route (same SeqNo/hops) — a TTL-only reconfirmation.
	status, err := tbl.UpdateRoute(RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})
	if err != nil {
		t.Fatalf("TTL refresh UpdateRoute: %v", err)
	}
	if status != RouteUnchanged {
		t.Fatalf("same-SeqNo refresh: status=%v, want RouteUnchanged", status)
	}

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("TTL refresh must not force a full sync")
	}
	if len(delta) != 0 {
		t.Fatalf("TTL-only refresh must not produce a delta, got %d: %+v", len(delta), delta)
	}
}

// TestAnnounceDeltaTo_SigUpgradeIsADelta: a same-SeqNo/same-hops reconfirmation
// that upgrades AttestedSig changes wire content (AttestedSig is on the wire), so
// it MUST be journaled and reach the peer as a cursor delta — not wait for the
// forced full. Pins the Round-14 sig-only-upgrade-flows-through-delta contract in
// cursor-authoritative mode.
func TestAnnounceDeltaTo_SigUpgradeIsADelta(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	x1, u1 := domaintest.ID("x1"), domaintest.ID("u1")

	tbl := NewTable(WithLocalOrigin(origin))
	// Initial route, unsigned.
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})

	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	tbl.ReleaseAnnounceEntries(base)

	// Same SeqNo/hops, now carrying a verified signature — a sig-only wire upgrade.
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1,
		Source:      RouteSourceAnnouncement,
		AttestedSig: []byte("sig-A"), AttestedSigVerified: true,
	})
	if err != nil {
		t.Fatalf("sig upgrade UpdateRoute: %v", err)
	}
	if status != RouteUnchanged {
		t.Fatalf("sig upgrade: status=%v, want RouteUnchanged", status)
	}

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	defer tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("sig upgrade must not force a full sync")
	}
	if len(delta) != 1 || delta[0].Identity != x1 {
		t.Fatalf("sig upgrade must produce a delta for x1, got %d: %+v", len(delta), delta)
	}
	if !bytes.Equal(delta[0].AttestedSig, []byte("sig-A")) {
		t.Fatalf("delta must carry the upgraded sig, got %q", delta[0].AttestedSig)
	}
}

// TestAnnounceDeltaTo_CapRejectionIsNotADelta: a cap-rejected announcement bumps
// RouteCapStats (mutated, snapshot re-copy) but leaves buckets — and therefore
// the wire projection — unchanged, so it must NOT be journaled. Otherwise a cap
// reject storm would re-emit the current unchanged route on every rejection.
func TestAnnounceDeltaTo_CapRejectionIsNotADelta(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	alice, bob := domaintest.ID("alice"), domaintest.ID("bob")

	tbl := NewTable(WithLocalOrigin(origin), WithMaxNextHopsPerOrigin(2))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: alice, Origin: bob, NextHop: domaintest.ID("hop-a"), Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})
	mustUpdateRoute(t, tbl, RouteEntry{Identity: alice, Origin: bob, NextHop: domaintest.ID("hop-b"), Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})

	base, head := tbl.AnnounceToWithChangeHead(peerZ)
	tbl.ReleaseAnnounceEntries(base)

	// A worse-than-worst announcement on a full per-origin bucket → cap reject.
	status, err := tbl.UpdateRoute(RouteEntry{Identity: alice, Origin: bob, NextHop: domaintest.ID("hop-c"), Hops: 9, SeqNo: 1, Source: RouteSourceAnnouncement})
	if err != nil {
		t.Fatalf("cap reject UpdateRoute: %v", err)
	}
	if status != RouteRejected {
		t.Fatalf("cap reject: status=%v, want RouteRejected", status)
	}

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	tbl.ReleaseAnnounceEntries(delta)
	if needFull {
		t.Fatal("cap reject must not force a full sync")
	}
	if len(delta) != 0 {
		t.Fatalf("cap-rejected update must not produce a delta, got %d: %+v", len(delta), delta)
	}
}

// TestAnnounceDeltaTo_NeedFullOnBulkReset: a cursor below a bulk reset
// (markSnapFullDirtyLocked → recordFullLocked) cannot be served incrementally.
func TestAnnounceDeltaTo_NeedFullOnBulkReset(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	tbl := buildDeltaTestTable(t, origin, []PeerIdentity{domaintest.ID("x1")}, []PeerIdentity{domaintest.ID("u1")})

	_, head := tbl.AnnounceToWithChangeHead(peerZ)

	tbl.mu.Lock()
	tbl.markSnapFullDirtyLocked() // bulk reset: cursor==head is now below fullResetSeq
	tbl.mu.Unlock()

	delta, _, needFull := tbl.AnnounceDeltaTo(peerZ, head)
	tbl.ReleaseAnnounceEntries(delta)
	if !needFull {
		t.Fatal("a cursor below a bulk reset must force a full sync")
	}
}
