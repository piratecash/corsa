package desktop

import (
	"testing"

	"github.com/piratecash/corsa/internal/core/service"
)

// TestPeerCachesPerGeneration verifies both derived caches (counts and active
// rows) are recomputed only when RouterSnapshot.Generation advances. The peers
// tab renders at 60 fps while peers are connected; without the cache,
// activeRowsForTab plus the two counters allocated fresh on every frame (the
// top desktop-side allocator in the heap-churn profile).
func TestPeerCachesPerGeneration(t *testing.T) {
	c := &ConsoleWindow{}

	gen5 := service.RouterSnapshot{
		Generation: 5,
		NodeStatus: service.NodeStatus{
			PeerHealth: []service.PeerHealth{
				{Address: "1.2.3.4:5555", PeerID: "peer-a", Connected: true},
			},
		},
	}
	rows := c.activePeerRows(gen5)
	connected, unique := c.peerCounts(gen5)
	if len(rows) != 1 || connected != 1 || unique != 1 {
		t.Fatalf("first derive: rows=%d connected=%d unique=%d, want 1/1/1", len(rows), connected, unique)
	}

	// Same generation, mutated payload: a cache HIT must return the
	// already-derived values and ignore the new (larger) PeerHealth set.
	gen5.NodeStatus = service.NodeStatus{
		PeerHealth: []service.PeerHealth{
			{Address: "1.2.3.4:5555", PeerID: "peer-a", Connected: true},
			{Address: "5.6.7.8:5555", PeerID: "peer-b", Connected: true},
		},
	}
	rows2 := c.activePeerRows(gen5)
	connected2, unique2 := c.peerCounts(gen5)
	if len(rows2) != 1 || connected2 != 1 || unique2 != 1 {
		t.Fatalf("same-generation derive must be a cache hit: rows=%d connected=%d unique=%d, want 1/1/1", len(rows2), connected2, unique2)
	}

	// New generation: recompute and observe the updated payload.
	gen6 := service.RouterSnapshot{Generation: 6, NodeStatus: gen5.NodeStatus}
	rows3 := c.activePeerRows(gen6)
	connected3, unique3 := c.peerCounts(gen6)
	if len(rows3) != 2 || connected3 != 2 || unique3 != 2 {
		t.Fatalf("generation bump must recompute: rows=%d connected=%d unique=%d, want 2/2/2", len(rows3), connected3, unique3)
	}
}

// TestActivePeerRows_ReusesSliceWithinGeneration pins the allocation contract:
// repeated calls within one generation return the SAME backing slice, proving
// no re-derivation (and no per-frame allocation) happens on a cache hit.
func TestActivePeerRows_ReusesSliceWithinGeneration(t *testing.T) {
	c := &ConsoleWindow{}
	snap := service.RouterSnapshot{
		Generation: 1,
		NodeStatus: service.NodeStatus{
			PeerHealth: []service.PeerHealth{
				{Address: "1.2.3.4:5555", PeerID: "peer-a", Connected: true},
			},
		},
	}

	rowsA := c.activePeerRows(snap)
	rowsB := c.activePeerRows(snap)
	if len(rowsA) == 0 || len(rowsB) == 0 {
		t.Fatalf("expected non-empty rows, got %d and %d", len(rowsA), len(rowsB))
	}
	if &rowsA[0] != &rowsB[0] {
		t.Fatal("cache hit must reuse the same backing slice, not re-derive a fresh one")
	}
}

// TestPeerCounts_IndependentOfActiveRowsCache verifies the info-tab path
// (peerCounts) never depends on the active-rows cache being built — the two
// caches are keyed independently so the info tab does not pay for the
// active-rows merge.
func TestPeerCounts_IndependentOfActiveRowsCache(t *testing.T) {
	c := &ConsoleWindow{}
	snap := service.RouterSnapshot{
		Generation: 7,
		NodeStatus: service.NodeStatus{
			PeerHealth: []service.PeerHealth{
				{Address: "1.2.3.4:5555", PeerID: "peer-a", Connected: true},
				{Address: "5.6.7.8:5555", PeerID: "peer-b", Connected: true},
			},
		},
	}
	connected, unique := c.peerCounts(snap)
	if connected != 2 || unique != 2 {
		t.Fatalf("peerCounts = %d/%d, want 2/2", connected, unique)
	}
	if c.peerRows.rowsValid {
		t.Fatal("peerCounts must not populate the active-rows cache")
	}
}
