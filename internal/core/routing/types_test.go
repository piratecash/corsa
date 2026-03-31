package routing

import (
	"testing"
	"time"
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
	r := RouteEntry{Identity: "alice", Origin: "bob", NextHop: "charlie"}
	key := r.DedupKey()
	if key.Identity != "alice" || key.Origin != "bob" || key.NextHop != "charlie" {
		t.Fatalf("unexpected dedup key: %+v", key)
	}
}

func TestSnapshotBestRoute(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	snap := Snapshot{
		TakenAt: now,
		Routes: map[string][]RouteEntry{
			"alice": {
				{Identity: "alice", Origin: "x", NextHop: "n1", Hops: 3, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)},
				{Identity: "alice", Origin: "y", NextHop: "n2", Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
				{Identity: "alice", Origin: "z", NextHop: "n3", Hops: HopsInfinity, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
			},
		},
	}

	best := snap.BestRoute("alice")
	if best == nil {
		t.Fatal("expected a best route")
	}
	if best.Hops != 1 || best.NextHop != "n2" {
		t.Fatalf("expected 1-hop route via n2, got hops=%d via %s", best.Hops, best.NextHop)
	}
}

func TestSnapshotBestRoutePrefersTrustOverHops(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	snap := Snapshot{
		TakenAt: now,
		Routes: map[string][]RouteEntry{
			"alice": {
				{Identity: "alice", Origin: "x", NextHop: "n1", Hops: 1, Source: RouteSourceAnnouncement, ExpiresAt: now.Add(time.Hour)},
				{Identity: "alice", Origin: "y", NextHop: "alice", Hops: 1, Source: RouteSourceDirect, ExpiresAt: now.Add(time.Hour)},
			},
		},
	}

	best := snap.BestRoute("alice")
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
		Routes: map[string][]RouteEntry{
			"bob": {
				{Identity: "bob", Hops: 1, ExpiresAt: now.Add(-time.Second)},
			},
		},
	}

	if snap.BestRoute("bob") != nil {
		t.Fatal("expired route should not be returned as best")
	}
}

func TestSnapshotBestRouteNone(t *testing.T) {
	snap := Snapshot{
		TakenAt: time.Now(),
		Routes:  map[string][]RouteEntry{},
	}
	if snap.BestRoute("unknown") != nil {
		t.Fatal("unknown identity should return nil")
	}
}

// --- Validate ---

func TestValidateRejectsDirectWithWrongHops(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 3, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != ErrDirectHopsMust1 {
		t.Fatalf("expected ErrDirectHopsMust1, got %v", err)
	}
}

func TestValidateRejectsDirectWithWrongNextHop(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "bob",
		Hops: 1, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != ErrDirectNextHop {
		t.Fatalf("expected ErrDirectNextHop, got %v", err)
	}
}

func TestValidateAcceptsWellFormedDirect(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
		Hops: 1, Source: RouteSourceDirect,
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("well-formed direct should pass: %v", err)
	}
}

func TestValidateAcceptsAnnouncement(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 3, Source: RouteSourceAnnouncement,
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("announcement should pass: %v", err)
	}
}

// --- ToAnnounceEntry ---

func TestToAnnounceEntryPreservesHops(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: 2, SeqNo: 7, Source: RouteSourceAnnouncement,
	}
	a := e.ToAnnounceEntry()
	if a.Hops != 2 {
		t.Fatalf("wire should carry sender's local hops as-is: expected 2, got %d", a.Hops)
	}
	if a.Identity != "alice" || a.Origin != "bob" || a.SeqNo != 7 {
		t.Fatal("wire fields should be preserved")
	}
}

func TestToAnnounceEntryWithdrawalPassthrough(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "bob", NextHop: "charlie",
		Hops: HopsInfinity, SeqNo: 10, Source: RouteSourceAnnouncement,
	}
	a := e.ToAnnounceEntry()
	if a.Hops != HopsInfinity {
		t.Fatalf("withdrawal hops should pass through as 16, got %d", a.Hops)
	}
}

func TestToAnnounceEntryDirectRouteHops1OnWire(t *testing.T) {
	e := RouteEntry{
		Identity: "alice", Origin: "me", NextHop: "alice",
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
