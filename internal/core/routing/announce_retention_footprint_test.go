package routing

import (
	"runtime"
	"testing"
	"time"
)

// announce_retention_footprint_test.go weighs what the announce plane HOLDS
// once a peer has been reconciled — the question step 14 asks of it.
//
// It measures the heap, not a gauge. The earlier harness in
// usage_footprint_test.go read `last_sent_entries` back out of Usage() and
// reported that count as the footprint, which measures the node's own claim
// about itself: if the claim were wrong the measurement would repeat the error
// instead of catching it. Here the registry is built inside a GC-bracketed
// window and weighed, so the number comes from the allocator.
//
// It also follows the sequence the node really runs
// (node/routing_announce.go sendConnectTimeFullSync): project, build the
// snapshot, RETURN THE POOLED PROJECTION BUFFER, then commit. The old harness
// wrapped the pooled buffer in a snapshot and handed it to the registry —
// a shape production never holds, and one that announce_builder.go's strict
// ownership rule forbids outright (the pool would later hand that backing
// array to another projection).

// skipIfMeasurementIsInstrumented steps aside under -race, where a heap-delta
// reading stops meaning what these tests read it as.
//
// The race detector allocates shadow state beside every object and defers
// releases, so the same registry that weighs single-digit kilobytes in a normal
// build reads as megabytes here and the gap between two table sizes shrinks
// into the noise — one run of the per-peer test even measured a NEGATIVE delta,
// which footprintOf clamps to zero. What would be reported is the
// instrumentation, not the structure.
//
// The behaviour tests are the ones that must survive -race, and they do
// (announce_baseline_lifecycle_test.go, run with -race -count=2). What is given
// up here is a measurement under instrumentation that could not be believed
// anyway; the condition is read from the build the toolchain actually produced
// rather than predicted (race_detector_on_test.go).
func skipIfMeasurementIsInstrumented(t *testing.T) {
	t.Helper()

	if raceDetectorEnabled {
		t.Skip("heap-delta measurement is not meaningful under -race: the detector's shadow state dominates the reading")
	}
}

// settledFootprintOf is footprintOf with the sync.Pool caches drained on both
// sides of the measurement.
//
// One GC is not enough for a pool. A cycle moves a pool's contents to its
// victim cache and only the NEXT cycle drops them, so announce_builder.go's
// table-sized scratch maps — allocated inside the window and returned to the
// pool before it closes — were still reachable at the second reading. That is
// how one run in six reported 3.39 MB for a registry holding kilobytes: the
// warm-up outside the window pins the pools only while they survive, and
// whether they survive into the window is not something the test controls.
//
// Two consecutive GCs make it deterministic, and they make the number more
// honest as well: what is being weighed is what the REGISTRY holds, not what an
// allocator cache happened to be keeping warm on its behalf.
//
// It is separate from footprintOf rather than a change to it, because that one
// is shared with the route-plane test, whose assertion is that measured
// retention sits ABOVE the reported floor — squeezing its reading downward
// would weaken a check that has nothing to do with this one.
func settledFootprintOf(build func() any) uint64 {
	settle := func() {
		runtime.GC()
		runtime.GC()
	}

	settle()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	built := build()

	settle()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(built)

	if after.HeapAlloc < before.HeapAlloc {
		return 0
	}
	return after.HeapAlloc - before.HeapAlloc
}

// announceStateRetention reports what the announce registry retains after every
// peer has been reconciled against a table of the given size.
//
// The table is built and the WHOLE sequence is run once OUTSIDE the measured
// window. Two different things would otherwise be charged to the announce
// plane, both of them table-proportional and neither of them per-peer state:
//
//   - the route store's own outbound caches, which the first projection to a
//     peer populates;
//   - announce_builder.go's sync.Pool scratch (the seen/best/groups maps and
//     the projection buffer), which is sized by the table it last served and
//     survives a single GC cycle in the pool's victim cache.
//
// Leaving either inside the window made the measurement report the builder's
// working set as the registry's retention: it fell with the change (the
// snapshots really did go) but still tracked the table, which would have read
// as "the copy is still there" and is a different, wrong conclusion.
func announceStateRetention(t *testing.T, identities, peers int) uint64 {
	t.Helper()

	table := buildFootprintTable(identities, 2, peers)
	warmup := NewAnnounceStateRegistry()
	now := time.Now().UTC()
	for p := range peers {
		peer := footprintIdentity('P', p)
		routes, head := table.AnnounceToWithChangeHead(peer)
		BuildAnnounceSnapshot(routes)
		table.ReleaseAnnounceEntries(routes)
		warmup.GetOrCreate(peer).RecordFullSyncSuccess(head, now)
	}

	var registry *AnnounceStateRegistry
	retained := settledFootprintOf(func() any {
		registry = NewAnnounceStateRegistry()
		for p := range peers {
			peer := footprintIdentity('P', p)
			state := registry.GetOrCreate(peer)
			routes, head := table.AnnounceToWithChangeHead(peer)
			snapshot := BuildAnnounceSnapshot(routes)
			table.ReleaseAnnounceEntries(routes)
			// The snapshot is built and sent, then dropped — exactly what the
			// node does. Asserting it is non-empty keeps the measurement from
			// silently degrading into "nothing was projected, so nothing was
			// retained", which would pass every check below for the wrong
			// reason.
			if len(snapshot.Entries) == 0 {
				t.Fatalf("peer %d projected an empty table: the measurement would prove nothing", p)
			}
			state.RecordFullSyncSuccess(head, now)
		}
		return registry
	})

	runtime.KeepAlive(table)
	runtime.KeepAlive(warmup)
	return retained
}

// BenchmarkAnnounceFullSyncCommit measures the OTHER half of the trade, the
// one a retention win is usually paid for with: what one peer's full-sync cycle
// allocates.
//
// It is here because "we stopped holding it" and "we stopped allocating it" are
// different claims, and only the first one is true. The projection is still
// built — it has to be, it is what goes on the wire — so the bytes are still
// allocated. What changed is their lifetime: they were live until the next
// forced full sync and are now garbage as soon as the frame is sent. Reporting
// the retention drop without this number would be selling a change of lifetime
// as a change of cost.
func BenchmarkAnnounceFullSyncCommit(b *testing.B) {
	const (
		identities = 1_000
		peers      = 8
	)
	table := buildFootprintTable(identities, 2, peers)
	registry := NewAnnounceStateRegistry()
	now := time.Now().UTC()
	peerIDs := make([]PeerIdentity, peers)
	for p := range peers {
		peerIDs[p] = footprintIdentity('P', p)
		registry.GetOrCreate(peerIDs[p])
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		peer := peerIDs[i%peers]
		routes, head := table.AnnounceToWithChangeHead(peer)
		snapshot := BuildAnnounceSnapshot(routes)
		table.ReleaseAnnounceEntries(routes)
		registry.GetOrCreate(peer).RecordFullSyncSuccess(head, now)
		runtime.KeepAlive(snapshot)
	}
}

// TestAnnounceStateRetentionDoesNotScaleWithTheTable pins the property that
// makes the per-peer announce state cheap: what a peer's send state holds is
// the fact that a baseline was established, not the table that was sent.
//
// The two shapes differ only in the size of the table each peer was reconciled
// against — same peer count, same code path. A per-peer state that keeps the
// projection grows with the table by construction; one that keeps a mark does
// not.
//
// The comparison is a DIFFERENCE against an absolute envelope, not a ratio. A
// first version asserted `large < 2 × small` and was flaky: once the projection
// is gone both readings are single-digit kilobytes, where allocator noise is
// the same order as the value, so a ratio between them measures the noise. The
// difference is the honest quantity — before the change it was 35 MB, after it
// is a few kilobytes — and it stays honest however small the two readings get.
func TestAnnounceStateRetentionDoesNotScaleWithTheTable(t *testing.T) {
	skipIfMeasurementIsInstrumented(t)
	if testing.Short() {
		t.Skip("builds two populated tables")
	}

	const peers = 32
	small := announceStateRetention(t, 500, peers)
	large := announceStateRetention(t, 5_000, peers)

	t.Logf("announce state retained by %d peers: 500 identities = %s, 5000 identities = %s",
		peers, footprintBytes(small), footprintBytes(large))

	// Ten times the table over the same peers. Holding the projection would put
	// roughly identities × peers × announceEntryBytes between the two readings;
	// the envelope below is three orders of magnitude under that and still far
	// above the noise the two now sit in.
	envelope := uint64(peers) * announcePeerBytes * 64
	if large > small+envelope {
		t.Fatalf("announce state grew by %s (from %s to %s) for 10× the table, above the %s envelope: the per-peer state still retains the projection it sent",
			footprintBytes(large-small), footprintBytes(small), footprintBytes(large), footprintBytes(envelope))
	}
}

// TestAnnounceStateRetentionPerPeerIsBounded states the remaining growth in the
// only terms that are left: the registry costs one record per peer, and that
// record does not carry the table.
//
// Without this the test above could be satisfied by a state that holds nothing
// useful at all; here the same table is reconciled to four times the peers and
// the cost is required to stay within a small per-peer envelope.
func TestAnnounceStateRetentionPerPeerIsBounded(t *testing.T) {
	skipIfMeasurementIsInstrumented(t)
	if testing.Short() {
		t.Skip("builds two populated tables")
	}

	const identities = 5_000
	few := announceStateRetention(t, identities, 8)
	many := announceStateRetention(t, identities, 32)

	t.Logf("announce state over %d identities: 8 peers = %s, 32 peers = %s",
		identities, footprintBytes(few), footprintBytes(many))

	// 24 extra peers. announcePeerBytes is the measured floor of one record;
	// a generous multiple of it leaves room for the registry map's own growth
	// while still failing loudly if a table-sized copy came back per peer.
	const envelope = 64
	if budget := uint64(24 * announcePeerBytes * envelope); many > few+budget {
		t.Fatalf("24 more peers added %s on top of %s, above the %s envelope for per-peer records: the per-peer state is carrying more than a mark",
			footprintBytes(many-few), footprintBytes(few), footprintBytes(budget))
	}
}
