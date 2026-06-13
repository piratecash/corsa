package node

import (
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/routing"
)

// routingSnapshot wraps routing.Snapshot inside an atomic.Pointer so RPC
// handlers, the file router and the desktop reachability path can read
// the routing view with zero locking — completely decoupled from any
// writer holding routing.Table.t.mu.
//
// The snapshot is immutable once stored: rebuildRoutingSnapshot
// allocates a fresh routing.Snapshot value (with its own routes map)
// and replaces the pointer atomically. Callers MUST treat the returned
// snapshot as read-only — the underlying routes map is shared between
// every concurrent reader of the same generation, so any mutation
// would corrupt observers downstream and the next refresh would
// publish a value that shadows but does not undo the in-place change.
//
// Bounded-staleness contract for structural changes (route accepted/
// withdrawn/replaced, direct peer added/removed, flap burst arming
// hold-down, flap-state cleanup after a writer touched the table)
// matches the other hot-read snapshots (network_stats, peer_health,
// peers_exchange, cm_slots): worst-case staleness equals
// networkStatsSnapshotInterval plus the time the refresher needs to
// acquire routing.Table.t.mu.RLock.
//
// Time-derived state is bounded more loosely — the dirty-flag publisher
// only republishes when a writer touches the table, but wall-clock
// timestamps that drive these fields advance without writer events:
//
//   - A finite-TTL route silently aging out is not a writer event until
//     TickTTL rewrites it (every 10 s). IsExpired(snap.TakenAt) and
//     ttl_seconds therefore can lag up to TickTTL_interval (≈10 s)
//     plus one refresh tick.
//   - FlapEntry.InHoldDown flipping from true to false is also driven
//     by wall-clock — fs.holdDownUntil elapsing. TickTTL clears the
//     deadline on its 10 s cadence and marks the table dirty, so the
//     transition is published within TickTTL_interval + one refresh.
//     Hold-down arming is a writer event (the disconnect that crossed
//     the flap threshold) and is reflected within the structural
//     bound. Both Snapshot and FlapSnapshot normalize HoldDownUntil
//     to zero whenever InHoldDown is false, so consumers never see a
//     past-timestamp deadline paired with InHoldDown=false even if
//     TickTTL has not yet cleared fs.holdDownUntil.
//
// Consumers needing strict freshness for any time-derived field must
// read the table directly — see file_integration.isPeerReachable,
// which uses routing.Table.Lookup() for a per-destination fresh view,
// and docs/routing.md "Snapshot freshness" for the full contract.
//
// Under writer pressure on the routing table the refresher itself may
// stall, but every consumer continues to serve the last good snapshot —
// the same invariant that decoupled fetch_network_stats from s.peerMu
// writers in the original phase-1 carve-out.
type routingSnapshot struct {
	// snap is the routing.Snapshot captured at the most recent rebuild.
	// Stored as a struct value (not a pointer) so the atomic.Pointer
	// alone owns the lifecycle — readers receive a value copy with the
	// same shared routes map that other readers of the same generation
	// see, and the next rebuild produces a brand-new routes map under
	// a new pointer rather than mutating in place.
	snap routing.Snapshot
}

// loadRoutingSnapshot atomically retrieves the last-published routing
// snapshot. Returns an empty (zero-value) routing.Snapshot when the
// publisher has not yet run — the same shape RPC handlers used to
// produce on a freshly constructed Service before any peer connected,
// so downstream JSON serialisation paths do not need a nil-check.
//
// Once Run() has executed primeHotReadSnapshots() before opening the
// listener, the publisher is guaranteed to have run at least once and
// the returned snapshot reflects whatever routing state existed at
// service start. Until that point (unit tests that bypass Run() and
// read the snapshot directly), the empty snapshot is the safe default.
func (s *Service) loadRoutingSnapshot() routing.Snapshot {
	if s.routingTable == nil {
		return routing.Snapshot{}
	}
	rs := s.routingSnap.Load()
	if rs == nil {
		return routing.Snapshot{}
	}
	return rs.snap
}

// rebuildRoutingSnapshot publishes a fresh routing.Snapshot if the
// table has been mutated since the previous publish OR no snapshot has
// been published yet (cold start primed by Run()). Skipping the
// rebuild when the table is clean keeps the worst-case CPU cost of the
// refresher proportional to actual writer activity rather than a fixed
// 500 ms tick — important once routing tables grow into the tens of
// thousands of entries on a 1000-node mesh.
//
// The dirty/consume protocol intentionally races with concurrent
// writers. A writer that sets dirty=true after ConsumeDirty has
// already returned true (and the rebuild has started) leaves the next
// refresh tick observing dirty=true again and producing one more
// rebuild — a single missed mutation is therefore visible no later
// than 2 × networkStatsSnapshotInterval, well inside the bounded-
// staleness contract.
//
// routing.Table.Snapshot acquires t.mu.RLock for the deep copy. If a
// writer storm queues on t.mu.Lock the refresher itself stalls, but
// the previously published snapshot continues to satisfy every reader
// — the RPC path is statically decoupled from t.mu writers, the same
// invariant that motivated the network_stats / peer_health phase-1
// carve-out.
// routingSnapshotMinInterval coalesces routingSnap rebuilds: even when the
// table stays dirty on every tick (steady route-announce stream), the full
// deep-copy snapshot is rebuilt at most this often. Routing decisions consume
// the snapshot with bounded staleness, so 1s of lag is acceptable while it
// cuts the dominant per-tick churn. Tunable; kept >= the 500ms ticker so it
// actually throttles.
const routingSnapshotMinInterval = 1 * time.Second

func (s *Service) rebuildRoutingSnapshot() {
	if s.routingTable == nil {
		return
	}

	// Coalesce: throttle the (expensive) deep copy to once per
	// routingSnapshotMinInterval even under a dirty-every-tick announce
	// stream. Checked BEFORE ConsumeDirty so the dirty bit is preserved for
	// the next eligible tick — a throttled rebuild must not swallow a change.
	// The cold-start case (no prior publish) bypasses the throttle so the
	// first RPC after Run() sees real data immediately.
	if s.routingSnap.Load() != nil {
		last := s.lastRoutingSnapAtNanos.Load()
		if last != 0 && time.Since(time.Unix(0, last)) < routingSnapshotMinInterval {
			return
		}
	}

	// Skip the deep copy when the table is clean AND a previous
	// publish exists. The cold-start case (no prior publish) must
	// always rebuild so loadRoutingSnapshot returns real data on the
	// first RPC immediately after Run() opens the listener.
	if !s.routingTable.ConsumeDirty() && s.routingSnap.Load() != nil {
		return
	}

	log.Trace().Msg("routing_snapshot_refresh_begin")
	snap := s.routingTable.Snapshot()
	s.routingSnap.Store(&routingSnapshot{snap: snap})
	s.lastRoutingSnapAtNanos.Store(time.Now().UnixNano())
	log.Trace().
		Int("total", snap.TotalEntries).
		Int("active", snap.ActiveEntries).
		Msg("routing_snapshot_refresh_end")
}

// routingSnapPtr wraps atomic.Pointer[routingSnapshot] — mirror of
// networkStatsSnapPtr / peerHealthSnapPtr / peersExchangeSnapPtr /
// cmSlotsSnapPtr. Zero value is usable: .Load() returns nil until the
// first publish.
type routingSnapPtr = atomic.Pointer[routingSnapshot]
