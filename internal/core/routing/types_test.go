package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

func TestRouteSourceString(t *testing.T) {
	tests := []struct {
		source RouteSource
		want   string
	}{
		{RouteSourceDirect, "direct"},
		{RouteSourceHopAck, "hop_ack"},
		{RouteSourceAnnouncement, "announcement"},
		{RouteSource(99), "unknown(99)"},
	}
	for _, tt := range tests {
		if got := tt.source.String(); got != tt.want {
			t.Errorf("RouteSource(%d).String() = %q, want %q", tt.source, got, tt.want)
		}
	}
}

func TestRouteSourceTrustRank(t *testing.T) {
	if RouteSourceDirect.TrustRank() <= RouteSourceHopAck.TrustRank() {
		t.Fatal("direct must outrank hop_ack")
	}
	if RouteSourceHopAck.TrustRank() <= RouteSourceAnnouncement.TrustRank() {
		t.Fatal("hop_ack must outrank announcement")
	}
}

func TestRouteEntryIsWithdrawn(t *testing.T) {
	r := RouteEntry{Hops: HopsInfinity}
	if !r.IsWithdrawn() {
		t.Fatal("hops=16 should be withdrawn")
	}

	r.Hops = 1
	if r.IsWithdrawn() {
		t.Fatal("hops=1 should not be withdrawn")
	}
}

func TestRouteEntryIsExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	r := RouteEntry{ExpiresAt: now.Add(-time.Second)}
	if !r.IsExpired(now) {
		t.Fatal("entry past expiry should be expired")
	}

	r.ExpiresAt = now.Add(time.Hour)
	if r.IsExpired(now) {
		t.Fatal("entry with future expiry should not be expired")
	}

	r.ExpiresAt = time.Time{}
	if r.IsExpired(now) {
		t.Fatal("zero ExpiresAt should not be treated as expired")
	}
}

func TestDedupKey(t *testing.T) {
	r := RouteEntry{Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie")}
	key := r.DedupKey()
	if key.Identity != domaintest.ID("alice") || key.Origin != domaintest.ID("bob") || key.NextHop != domaintest.ID("charlie") {
		t.Fatalf("unexpected dedup key: %+v", key)
	}
}

func TestSnapshotBestRoute(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	snap := Snapshot{
		TakenAt: now,
		Routes: map[PeerIdentity][]RouteEntry{
			domaintest.ID("alice"): {
				{Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"), Hops: 3, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)},
				{Identity: domaintest.ID("alice"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("n2"), Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
				{Identity: domaintest.ID("alice"), Origin: domaintest.ID("z"), NextHop: domaintest.ID("n3"), Hops: HopsInfinity, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
			},
		},
	}

	best := snap.BestRoute(domaintest.ID("alice"))
	if best == nil {
		t.Fatal("expected a best route")
	}
	if best.Hops != 1 || best.NextHop != domaintest.ID("n2") {
		t.Fatalf("expected 1-hop route via n2, got hops=%d via %s", best.Hops, best.NextHop)
	}
}

func TestSnapshotBestRoutePrefersTrustOverHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	snap := Snapshot{
		TakenAt: now,
		Routes: map[PeerIdentity][]RouteEntry{
			domaintest.ID("alice"): {
				{Identity: domaintest.ID("alice"), Origin: domaintest.ID("x"), NextHop: domaintest.ID("n1"), Hops: 1, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)},
				{Identity: domaintest.ID("alice"), Origin: domaintest.ID("y"), NextHop: domaintest.ID("alice"), Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
			},
		},
	}

	best := snap.BestRoute(domaintest.ID("alice"))
	if best == nil {
		t.Fatal("expected a best route")
	}
	if best.Source != RouteSourceDirect {
		t.Fatalf("direct should win over announcement even at same hops, got source=%s", best.Source)
	}
}

func TestSnapshotBestRouteSkipsExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	snap := Snapshot{
		TakenAt: now,
		Routes: map[PeerIdentity][]RouteEntry{
			domaintest.ID("bob"): {
				{Identity: domaintest.ID("bob"), Hops: 1, ExpiresAt: now.Add(-time.Second)},
			},
		},
	}

	if snap.BestRoute(domaintest.ID("bob")) != nil {
		t.Fatal("expired route should not be returned as best")
	}
}

func TestSnapshotBestRouteNone(t *testing.T) {
	snap := Snapshot{
		TakenAt: time.Now(),
		Routes:  map[PeerIdentity][]RouteEntry{},
	}
	if snap.BestRoute(domaintest.ID("unknown")) != nil {
		t.Fatal("unknown identity should return nil")
	}
}

func TestSnapshotReachableIdentitiesWithTransitClassifiesSources(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	transit := domaintest.ID("classified-transit")
	direct := domaintest.ID("classified-direct")
	mixed := domaintest.ID("classified-mixed")
	dead := domaintest.ID("classified-dead")
	local := domaintest.ID("classified-local")
	deadHop := domaintest.ID("classified-dead-hop")

	snap := Snapshot{
		TakenAt: now,
		Routes: map[PeerIdentity][]RouteEntry{
			transit: {{Identity: transit, NextHop: domaintest.ID("transit-hop"), Hops: 2, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)}},
			direct:  {{Identity: direct, NextHop: direct, Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)}},
			mixed: {
				{Identity: mixed, NextHop: mixed, Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
				{Identity: mixed, NextHop: domaintest.ID("mixed-hop"), Hops: 2, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)},
			},
			dead:  {{Identity: dead, NextHop: deadHop, Hops: 2, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)}},
			local: {{Identity: local, NextHop: local, Hops: 0, Source: RouteSourceLocal}},
		},
		Health: []RouteHealthState{
			{Identity: dead, Uplink: deadHop, Health: HealthDead},
			// A stale health row must not hide a session-bound direct route.
			{Identity: direct, Uplink: direct, Health: HealthDead},
		},
	}

	got := snap.ReachableIdentitiesWithTransit()
	if hasTransit, ok := got[direct]; !ok || hasTransit {
		t.Fatalf("direct classification = (%v, %v), want reachable without transit", hasTransit, ok)
	}
	for _, identity := range []PeerIdentity{transit, mixed} {
		if hasTransit, ok := got[identity]; !ok || !hasTransit {
			t.Fatalf("%s classification = (%v, %v), want reachable with transit", identity, hasTransit, ok)
		}
	}
	for _, identity := range []PeerIdentity{dead, local} {
		if _, ok := got[identity]; ok {
			t.Fatalf("%s unexpectedly classified reachable: %v", identity, got)
		}
	}
}

// --- Validate ---

func TestValidateRejectsDirectWithWrongHops(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 3, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != ErrDirectHopsMust1 {
		t.Fatalf("expected ErrDirectHopsMust1, got %v", err)
	}
}

func TestValidateRejectsDirectWithWrongNextHop(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("bob"),
		Hops: 1, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != ErrDirectNextHop {
		t.Fatalf("expected ErrDirectNextHop, got %v", err)
	}
}

func TestValidateAcceptsWellFormedDirect(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("well-formed direct should pass: %v", err)
	}
}

func TestValidateAcceptsAnnouncement(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 3, Source: RouteSourceAnnouncement,
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("announcement should pass: %v", err)
	}
}

// --- ToAnnounceEntry ---

func TestToAnnounceEntryPreservesHops(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	}
	a := e.ToAnnounceEntry()
	if a.Hops != 2 {
		t.Fatalf("wire should carry sender's local hops as-is: expected 2, got %d", a.Hops)
	}
	if a.Identity != domaintest.ID("alice") || a.Origin != domaintest.ID("bob") || a.SeqNo != 7 {
		t.Fatal("wire fields should be preserved")
	}
}

func TestToAnnounceEntryWithdrawalPassthrough(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("bob"), NextHop: domaintest.ID("charlie"),
		Hops: HopsInfinity, SeqNo: 10, Source: RouteSourceAnnouncement,
	}
	a := e.ToAnnounceEntry()
	if a.Hops != HopsInfinity {
		t.Fatalf("withdrawal hops should pass through as 16, got %d", a.Hops)
	}
}

func TestToAnnounceEntryDirectRouteHops1OnWire(t *testing.T) {
	e := RouteEntry{
		Identity: domaintest.ID("alice"), Origin: domaintest.ID("me"), NextHop: domaintest.ID("alice"),
		Hops: 1, SeqNo: 1, Source: RouteSourceDirect,
	}
	a := e.ToAnnounceEntry()
	if a.Hops != 1 {
		t.Fatalf("direct route should appear as hops=1 on wire, got %d", a.Hops)
	}
}

func TestIsBetterPrefersHigherTrust(t *testing.T) {
	a := &RouteEntry{Hops: 3, Source: RouteSourceDirect}
	b := &RouteEntry{Hops: 1, Source: RouteSourceAnnouncement}
	if !isBetter(a, b) {
		t.Fatal("higher trust source should be preferred even with worse hops")
	}
}

func TestIsBetterTiebreaksByHops(t *testing.T) {
	a := &RouteEntry{Hops: 1, Source: RouteSourceAnnouncement}
	b := &RouteEntry{Hops: 3, Source: RouteSourceAnnouncement}
	if !isBetter(a, b) {
		t.Fatal("same trust: fewer hops should win")
	}
	if isBetter(b, a) {
		t.Fatal("same trust: more hops should lose")
	}
}
