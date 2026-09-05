package routing

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// usage_footprint_test.go is the bench half of the measurement step: it fills
// the route plane to the sizes the network is expected to reach and measures
// what it actually costs, so the numbers in 13-measurements.md come from a run
// rather than from a model.
//
// It is a TEST and not a benchmark on purpose. A benchmark reports what an
// operation ALLOCATES, and the question here is what the structures RETAIN —
// which is measured by holding them alive across a GC and reading the heap,
// not by counting the garbage produced on the way.
//
// What it asserts is the one property everything downstream rests on: the
// floor reported by Usage() must really be a floor. If the measured retention
// ever falls below it, a per-entry cost is being computed from the wrong
// struct and every budget derived from it is fiction. The RATIO between the
// two is logged rather than asserted — it depends on Go's map load factor and
// on how much of a claim's payload is referenced rather than inlined, and
// pinning it would be pinning the allocator.
//
// Reference: docs/refactoring/dht/13-measurements.md §2, §5.

// footprintOf measures what building a structure RETAINS.
//
// Both readings are taken after a forced GC so unreferenced intermediates are
// gone from both, and the built object is kept alive across the second one —
// without that the compiler is free to collect the very thing being weighed.
func footprintOf(build func() any) uint64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	built := build()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(built)

	if after.HeapAlloc < before.HeapAlloc {
		return 0
	}
	return after.HeapAlloc - before.HeapAlloc
}

// footprintIdentity derives a deterministic identity from two counters, so a
// run is reproducible and two sizes of the same shape overlap in their first
// identities.
func footprintIdentity(prefix byte, index int) PeerIdentity {
	var id PeerIdentity
	id[0] = prefix
	id[1] = byte(index >> 24)
	id[2] = byte(index >> 16)
	id[3] = byte(index >> 8)
	id[4] = byte(index)
	return id
}

// buildFootprintTable fills a table with identities × uplinks and then emits an
// announce projection to each peer, which is what populates the per-receiver
// SeqNo watermark — the container that grows as the PRODUCT and is the reason
// this measurement exists at all.
func buildFootprintTable(identities, uplinks, peers int) *Table {
	local := footprintIdentity('L', 0)
	table := NewTable(
		WithLocalOrigin(local),
		WithMaxNextHopsPerOrigin(DefaultMaxNextHopsPerOrigin),
	)
	expires := time.Now().UTC().Add(time.Hour)

	peerIDs := make([]PeerIdentity, peers)
	for p := range peerIDs {
		peerIDs[p] = footprintIdentity('P', p)
		if _, err := table.AddDirectPeer(peerIDs[p]); err != nil {
			panic(err)
		}
	}
	for i := range identities {
		identity := footprintIdentity('D', i)
		for u := range uplinks {
			// Uplinks are drawn from the live peer set: a claim whose next hop
			// is not a neighbour is not a shape the node ever holds.
			uplink := peerIDs[(i+u)%len(peerIDs)]
			_, err := table.UpdateRoute(RouteEntry{
				Identity:  identity,
				Origin:    uplink,
				NextHop:   uplink,
				Hops:      2 + u,
				SeqNo:     uint64(i + 1),
				Source:    RouteSourceAnnouncement,
				ExpiresAt: expires,
			})
			if err != nil {
				panic(err)
			}
		}
	}
	// One projection per peer: this is the announce cycle's own work, and it
	// is what writes outboundPeerMax and outboundContent.
	for _, peer := range peerIDs {
		table.AnnounceTo(peer)
	}
	// The publisher's snapshot is part of the resident cost too — it holds a
	// second, projected copy of the table.
	table.SnapshotIncremental(true)
	return table
}

// TestRoutePlaneFootprintAtMeshSizes measures the route plane at the three
// sizes the roadmap plans against and proves the reported floor is one.
//
// The 64k case is skipped in -short: it holds a real 64k-identity table, which
// is the point of the number and also several seconds and several hundred
// megabytes.
func TestRoutePlaneFootprintAtMeshSizes(t *testing.T) {
	cases := []struct {
		name       string
		identities int
		uplinks    int
		peers      int
		long       bool
	}{
		{name: "1k identities, 8 peers", identities: 1_000, uplinks: 2, peers: 8},
		{name: "10k identities, 8 peers", identities: 10_000, uplinks: 2, peers: 8},
		{name: "10k identities, 64 peers", identities: 10_000, uplinks: 2, peers: 64, long: true},
		{name: "64k identities, 8 peers", identities: 64_000, uplinks: 2, peers: 8, long: true},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.long && testing.Short() {
				t.Skip("holds a full-size table; -short runs the small shapes only")
			}

			var table *Table
			retained := footprintOf(func() any {
				table = buildFootprintTable(testCase.identities, testCase.uplinks, testCase.peers)
				return table
			})

			usage := table.Usage()
			floor := usage.FloorBytes()
			if floor == 0 {
				t.Fatal("the route plane reported a zero floor for a populated table")
			}
			if retained < floor {
				t.Fatalf("measured retention %d B is BELOW the reported floor %d B: a per-entry cost is derived from the wrong struct, and every budget built on it is fiction",
					retained, floor)
			}

			t.Logf("route plane: %s", footprintReport(retained, usage))
		})
	}
}

// TestAnnouncePlaneFootprintGrowsWithPeers measures the other half of the
// product — what the announce loop retains PER PEER — and pins the shape of
// that growth.
//
// The assertion is not a byte figure but the relationship: doubling the peers
// over one table must roughly double what the announce plane holds, because
// each peer keeps its own full snapshot of the projection. That relationship
// is the argument for the whole overlay roadmap, and if it ever stops holding,
// the roadmap's premise has changed rather than the test.
func TestAnnouncePlaneFootprintGrowsWithPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two populated registries")
	}

	const identities = 5_000
	few := announceFootprint(t, identities, 8)
	many := announceFootprint(t, identities, 32)

	if few == 0 {
		t.Fatal("the announce plane reported nothing for a populated registry")
	}
	// Four times the peers over the same table. Anything below three times the
	// entries would mean the snapshots are being shared rather than held per
	// peer, which is a different memory model than the one being budgeted.
	if many < 3*few {
		t.Fatalf("announce entries grew from %d to %d for 4× the peers: per-peer snapshots are no longer per peer",
			few, many)
	}
	t.Logf("announce plane entries: 8 peers = %d, 32 peers = %d", few, many)
}

// announceFootprint reports how many announce entries are retained on peers'
// behalf for one table shape.
func announceFootprint(t *testing.T, identities, peers int) uint64 {
	t.Helper()

	registry := NewAnnounceStateRegistry()
	table := buildFootprintTable(identities, 2, peers)
	now := time.Now().UTC()
	for p := range peers {
		peer := footprintIdentity('P', p)
		state := registry.GetOrCreate(peer)
		entries, cursor := table.AnnounceToWithChangeHead(peer)
		state.RecordFullSyncSuccess(&AnnounceSnapshot{Entries: entries}, cursor, now)
	}

	usage := registry.Usage()
	for _, gauge := range usage.Gauges() {
		if gauge.Name() == "last_sent_entries" {
			return gauge.Count()
		}
	}
	return 0
}

// footprintReport renders one measurement for the log: what was measured, what
// was claimed, and by how much reality exceeds the claim.
func footprintReport(retained uint64, usage domain.SubsystemUsage) string {
	floor := usage.FloorBytes()
	report := fmt.Sprintf("retained %s, floor %s (×%.2f)",
		footprintBytes(retained), footprintBytes(floor), float64(retained)/float64(floor))
	for _, gauge := range usage.Gauges() {
		if gauge.Count() == 0 {
			continue
		}
		report += fmt.Sprintf("\n    %-24s %9d × %3d B = %s",
			gauge.Name(), gauge.Count(), gauge.EntryBytes(), footprintBytes(gauge.FloorBytes()))
	}
	return report
}

// footprintBytes renders a byte count for the log in the largest unit that
// keeps the integer part small.
func footprintBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	value := float64(b)
	units := []string{"KB", "MB", "GB"}
	idx := -1
	for value >= unit && idx < len(units)-1 {
		value /= unit
		idx++
	}
	return fmt.Sprintf("%.2f %s", value, units[idx])
}
