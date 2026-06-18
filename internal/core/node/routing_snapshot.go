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
// hold-down, flap-state cleanup after a writer touched the table): unlike
// the other hot-read snapshots (network_stats, peer_health, peers_exchange,
// cm_slots) the routing rebuild is additionally coalesced by
// routingSnapshotMinInterval (1 s), checked BEFORE ConsumeDirty. Worst-case
// staleness is therefore that 1 s floor plus the next refresh tick that
// crosses it (~500 ms) plus the time the refresher needs to acquire
// routing.Table.t.mu (now an exclusive Lock for the incremental projection
// build, see rebuildRoutingSnapshot) — on the order of 1–1.5 s, the churn
// reduction routing trades for the tighter ~500 ms bound of the others.
//
// Time-derived state is bounded more loosely — the dirty-flag publisher
// only republishes when a writer touches the table, but wall-clock
// timestamps that drive these fields advance without writer events:
//
//   - A finite-TTL route silently aging out is not a writer event until
//     TickTTL rewrites it (every 10 s). IsExpired(snap.TakenAt) and
//     ttl_seconds therefore can lag up to TickTTL_interval (≈10 s) plus
//     the structural publish bound — TickTTL's dirty mark still flows
//     through the routingSnapshotMinInterval floor + a refresh tick
//     (~1–1.5 s), so ≈11–11.5 s total.
//   - FlapEntry.InHoldDown flipping from true to false is also driven
//     by wall-clock — fs.holdDownUntil elapsing. TickTTL clears the
//     deadline on its 10 s cadence and marks the table dirty, so the
//     transition is published within TickTTL_interval + the structural
//     publish bound (~1–1.5 s), i.e. ≈11–11.5 s.
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
// eligible refresh observing dirty=true again and producing one more
// rebuild — a single missed mutation is therefore visible no later than
// the next rebuild that passes the routingSnapshotMinInterval (1 s) floor
// plus one refresh tick, well inside the bounded-staleness contract above.
//
// routing.Table.SnapshotIncremental acquires t.mu.Lock (exclusive — it
// consumes the per-identity snap dirty-set and updates the reuse cache,
// both writes) for the projection build. The build reuses the unchanged
// route slices of the previous snapshot and re-copies only the churned
// identities, so the exclusive hold is shorter than the old full deep
// copy even though it now also blocks the few direct t.mu.RLock readers
// (the Lookup callsites) for its duration. If a writer storm queues on
// t.mu the refresher itself stalls, but the previously published snapshot
// continues to satisfy every cached reader — the RPC path is statically
// decoupled from t.mu writers, the same invariant that motivated the
// network_stats / peer_health phase-1 carve-out.
// routingSnapshotMinInterval coalesces routingSnap rebuilds: even when the
// table stays dirty on every tick (steady route-announce stream), the
// incremental snapshot is rebuilt at most this often. Routing decisions
// consume the snapshot with bounded staleness, so a couple of seconds of
// lag is acceptable while it cuts the dominant per-tick churn.
//
// Raised 1s→2s: each rebuild deep-copies the full health set
// (healthStore.snapshotLocked) and the flap set on top of the
// copy-on-write route projection, so on a dense node (250+ peers) the
// per-rebuild cost is large and dominated alloc_space churn. Halving the
// rebuild cadence halves that churn at the cost of one extra second of
// snapshot staleness, well inside the bounded-staleness contract above.
// Tunable; kept >= the 500ms ticker so it actually throttles.
const routingSnapshotMinInterval = 2 * time.Second

// routingSnapshotFullInterval upgrades a rebuild that is happening anyway
// (the table was dirty) to a FULL (non-incremental) re-copy once this much
// wall-clock time has elapsed since the last full one. It is the self-heal
// safety net for the copy-on-write projection (SnapshotIncremental): the
// route-mutation surface in route_store_*.go is broad, and a site that
// changes a claim in place without marking its identity dirty would leave
// that identity's projection stale in the reuse cache. A full re-copy
// re-projects every bucket, healing such a stale entry.
//
// Crucially this does NOT wake a clean table: a missed snap-mark can only
// produce a stale published snapshot if a rebuild ran at all (the coarse
// `dirty` flag was set but the per-identity mark was missed), so the heal
// only needs to ride along on a future dirty rebuild — an idle/headless
// node with no route churn and nothing to heal is still skipped entirely,
// matching the sibling hot-read snapshots. The residual gap (a mis-marked
// mutation that is the very last one before the table goes permanently
// idle) is accepted: any live node sees dirty activity well within this
// interval via TickTTL / route announces. Wall-clock-gated (not a pass
// counter) so the cadence does not drift with the refresher tick rate.
const routingSnapshotFullInterval = 60 * time.Second

func (s *Service) rebuildRoutingSnapshot() {
	if s.routingTable == nil {
		return
	}

	// Coalesce: throttle the projection build to once per
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

	// Skip entirely when the table is clean AND a previous publish exists:
	// a steady-state idle/headless node pays nothing (no wake), the same
	// invariant honoured by the sibling hot-read snapshots. Cold start (no
	// prior publish) always proceeds so the first RPC after Run() sees data.
	dirty := s.routingTable.ConsumeDirty()
	if !dirty && s.routingSnap.Load() != nil {
		return
	}

	// A rebuild is warranted (dirty or cold start). Upgrade it to a full
	// re-copy when the self-heal interval has elapsed since the last full —
	// this never wakes an idle table; it only periodically makes one of the
	// rebuilds that are happening anyway a full one. Cold start (lastFull==0)
	// is full regardless.
	lastFull := s.lastRoutingFullSnapAtNanos.Load()
	forceFull := lastFull == 0 || time.Since(time.Unix(0, lastFull)) >= routingSnapshotFullInterval

	log.Trace().Msg("routing_snapshot_refresh_begin")
	// wasFull reflects whether the build actually re-copied every bucket —
	// true for our forceFull, but ALSO when a bulk mutation set snapFullDirty
	// or on cold start. Timestamp the self-heal cadence off the real full so
	// a bulk-driven full resets the interval and we do not redundantly force
	// another one shortly after.
	snap, wasFull := s.routingTable.SnapshotIncremental(forceFull)
	s.routingSnap.Store(&routingSnapshot{snap: snap})
	now := time.Now()
	s.lastRoutingSnapAtNanos.Store(now.UnixNano())
	if wasFull {
		s.lastRoutingFullSnapAtNanos.Store(now.UnixNano())
	}
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
