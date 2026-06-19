package routing

import (
	"fmt"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_identity_hex_memo_test.go pins the identity-hex memo: the
// projection must carry a hex string byte-identical to PeerIdentity.String()
// onto every emitted AnnounceEntry, the memo must be pruned in lockstep with
// the bucket it describes (the leak-free contract), and IdentityHexString must
// fall back to Identity.String() for entries built outside the projection.

func TestProjection_PopulatesIdentityHexMemo(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	x1 := domaintest.ID("x1")
	u1 := domaintest.ID("u1")

	tbl := NewTable(WithLocalOrigin(origin))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement})

	entries := tbl.AnnounceTo(peerZ)
	defer tbl.ReleaseAnnounceEntries(entries)

	got, ok := entryByIdentity(entries)[x1]
	if !ok {
		t.Fatalf("expected x1 in projection; entries=%+v", entries)
	}
	// The carried hex must equal the canonical wire form exactly — the
	// memo is a pure allocation optimisation, never a behavioural change.
	if got.IdentityHex != x1.String() {
		t.Fatalf("IdentityHex=%q, want %q", got.IdentityHex, x1.String())
	}
	if got.IdentityHexString() != x1.String() {
		t.Fatalf("IdentityHexString()=%q, want %q", got.IdentityHexString(), x1.String())
	}
	// The store memo must now hold the entry so later cycles reuse it.
	if cached, ok := tbl.store.identityHex[x1]; !ok || cached != x1.String() {
		t.Fatalf("memo miss after projection: ok=%v cached=%q", ok, cached)
	}
}

func TestIdentityHexMemo_PrunedOnBucketRemoval(t *testing.T) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")
	x1 := domaintest.ID("x1")
	u1 := domaintest.ID("u1")

	clk := time.Now()
	tbl := NewTable(WithLocalOrigin(origin), WithClock(func() time.Time { return clk }))
	mustUpdateRoute(t, tbl, RouteEntry{Identity: x1, Origin: origin, NextHop: u1, Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement, ExpiresAt: clk.Add(1 * time.Second)})

	// Project to populate the memo.
	tbl.ReleaseAnnounceEntries(tbl.AnnounceTo(peerZ))
	if _, ok := tbl.store.identityHex[x1]; !ok {
		t.Fatal("memo should be populated after projection")
	}

	// Expire the only claim and sweep — the bucket is deleted, so the memo
	// entry must be pruned in lockstep (bounds the memo to live identities).
	clk = clk.Add(2 * time.Second)
	tbl.TickTTL()

	if _, ok := tbl.store.identityHex[x1]; ok {
		t.Fatal("memo entry must be pruned when its bucket is removed (leak-free contract)")
	}
}

func TestAnnounceEntry_IdentityHexString_FallsBackWhenEmpty(t *testing.T) {
	x1 := domaintest.ID("x1")

	// Entry built outside the projection (e.g. ToAnnounceEntry, fixtures)
	// carries no memo — IdentityHexString must recompute from Identity.
	noMemo := AnnounceEntry{Identity: x1}
	if got := noMemo.IdentityHexString(); got != x1.String() {
		t.Fatalf("fallback IdentityHexString()=%q, want %q", got, x1.String())
	}

	// A populated memo is returned verbatim.
	withMemo := AnnounceEntry{Identity: x1, IdentityHex: "deadbeef"}
	if got := withMemo.IdentityHexString(); got != "deadbeef" {
		t.Fatalf("memoized IdentityHexString()=%q, want %q", got, "deadbeef")
	}
}

// BenchmarkAnnounceTo_IdentityHexMemo projects a populated table repeatedly to
// the same peer. Run with -benchmem: after the first cycle warms the memo, the
// per-cycle hex.EncodeToString allocations are gone, so allocs/op drops versus
// the pre-memo projection (the whole point of the optimisation).
func BenchmarkAnnounceTo_IdentityHexMemo(b *testing.B) {
	origin := domaintest.ID("self")
	peerZ := domaintest.ID("z")

	tbl := NewTable(WithLocalOrigin(origin))
	const n = 512
	for i := 0; i < n; i++ {
		id := domaintest.ID(fmt.Sprintf("dst-%d", i))
		uplink := domaintest.ID(fmt.Sprintf("up-%d", i%32))
		status, err := tbl.UpdateRoute(RouteEntry{
			Identity: id, Origin: origin, NextHop: uplink,
			Hops: 2, SeqNo: 1, Source: RouteSourceAnnouncement,
		})
		if err != nil || status != RouteAccepted {
			b.Fatalf("UpdateRoute(%d) status=%v err=%v", i, status, err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := tbl.AnnounceTo(peerZ)
		tbl.ReleaseAnnounceEntries(entries)
	}
}
