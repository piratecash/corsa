package node

import (
	"fmt"
	"testing"
)

// known_identities_test.go pins the bounded-set contract that replaced the
// unbounded s.known map: dedup-aware Add, membership, snapshot, and LRU
// eviction at capacity (the memory bound) with recency promotion on re-Add.

func TestBoundedKnownIdentities_AddReportsNovelty(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	if !b.Add("a") {
		t.Fatal("first Add must report novel (true)")
	}
	if b.Add("a") {
		t.Fatal("duplicate Add must report not-novel (false)")
	}
	if !b.Has("a") {
		t.Fatal("Has must report a present member")
	}
	if b.Has("missing") {
		t.Fatal("Has must report absent for an unknown key")
	}
}

func TestBoundedKnownIdentities_Snapshot(t *testing.T) {
	b := newBoundedKnownIdentities(maxKnownIdentities)
	b.Add("a")
	b.Add("b")
	b.Add("a") // duplicate, no effect
	snap := b.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot must contain the 2 distinct members, got %d", len(snap))
	}
	seen := map[string]bool{}
	for _, s := range snap {
		seen[s] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("snapshot must contain both a and b, got %v", snap)
	}
}

func TestBoundedKnownIdentities_EvictsLeastRecentlyUsedAtCapacity(t *testing.T) {
	b := newBoundedKnownIdentities(3)
	b.Add("1")
	b.Add("2")
	b.Add("3")
	// At capacity. Adding a 4th evicts the least-recently-used ("1").
	b.Add("4")
	if b.Len() != 3 {
		t.Fatalf("membership must never exceed capacity, got %d", b.Len())
	}
	if b.Has("1") {
		t.Fatal("least-recently-used entry must be evicted at capacity")
	}
	for _, k := range []string{"2", "3", "4"} {
		if !b.Has(k) {
			t.Fatalf("newer entry %q must survive eviction", k)
		}
	}
}

func TestBoundedKnownIdentities_ReAddPromotesRecency(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Add("1")
	b.Add("2")
	// Touch "1": re-Add promotes it to most-recently-used, so "2" is now the
	// least-recently-used and must be the one evicted next.
	if b.Add("1") {
		t.Fatal("re-Add of a present key must report not-novel (false)")
	}
	b.Add("3") // capacity 2 → evict LRU, which is now "2", not "1"
	if !b.Has("1") {
		t.Fatal("a recently re-observed identity must NOT be evicted")
	}
	if b.Has("2") {
		t.Fatal("the least-recently-used identity (2) must be evicted")
	}
	if !b.Has("3") {
		t.Fatal("the newest identity must be present")
	}
}

func TestBoundedKnownIdentities_ReAddAfterEviction(t *testing.T) {
	b := newBoundedKnownIdentities(2)
	b.Add("1")
	b.Add("2")
	b.Add("3") // evicts "1"
	if b.Has("1") {
		t.Fatal("1 should have been evicted")
	}
	if !b.Add("1") {
		t.Fatal("re-adding an evicted key must report novel again")
	}
	if b.Len() != 2 {
		t.Fatalf("membership must stay bounded after re-add, got %d", b.Len())
	}
}

func TestBoundedKnownIdentities_StaysBoundedUnderChurn(t *testing.T) {
	const cap = 100
	b := newBoundedKnownIdentities(cap)
	for i := 0; i < 10_000; i++ {
		b.Add(fmt.Sprintf("id-%d", i))
		if b.Len() > cap {
			t.Fatalf("membership exceeded capacity during churn: %d", b.Len())
		}
	}
	if b.Len() != cap {
		t.Fatalf("after sustained churn the set must sit at capacity, got %d", b.Len())
	}
	// Oldest must be gone, newest must be present.
	if b.Has("id-0") {
		t.Fatal("oldest churned key must have been evicted")
	}
	if !b.Has("id-9999") {
		t.Fatal("most-recent key must be present")
	}
}
