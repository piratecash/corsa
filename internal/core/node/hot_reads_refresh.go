package node

import (
	"context"
	"sync"
	"time"
)

// primeHotReadSnapshots builds every atomic snapshot that the hot RPC path
// depends on exactly once.  Called from Run() on the main goroutine BEFORE
// the listener starts accepting RPC connections — this establishes the
// invariant that every hot-path handler (peerHealthFrames,
// buildPeerExchangeResponse, networkStatsFrame, fetchRouteTable) observes a
// non-nil snapshot on its first load.  With that invariant the handlers
// can drop their synchronous rebuild fallbacks entirely, which used to
// reach cm.mu.RLock, s.peerMu.RLock and routing.Table.t.mu.RLock on the
// RPC goroutine and re-coupled the "lock-free hot path" contract to the
// very locks it was built to bypass.
//
// Running five rebuilds back-to-back at startup is cheap: the node has no
// peers yet, so each is O(1).
func (s *Service) primeHotReadSnapshots() {
	s.rebuildNetworkStatsSnapshot()
	s.rebuildPeerHealthSnapshot()
	s.rebuildPeersExchangeSnapshot()
	s.rebuildCMSlotsSnapshot()
	s.rebuildRoutingSnapshot()
}

// hotReadsRefreshLoop periodically rebuilds every atomic snapshot that the
// hot RPC path depends on: network_stats, peer_health, peers_exchange, the
// ConnectionManager slots view, and the routing table.  Each snapshot is
// refreshed by its own goroutine with its own ticker so a slow rebuild in
// one path does not delay the others.  Per-snapshot lock footprint (see
// docs/locking.md §"Reader path invariants" for the authoritative
// breakdown):
//
//   - network_stats / peer_health — short s.peerMu.RLock (peer-domain
//     placeholder during Phase 2 transition); no IP-state callbacks.
//     Both snapshots gate the periodic rebuild on recent-reader activity
//     (maybeRebuildNetworkStatsSnapshot / maybeRebuildPeerHealthSnapshot):
//     while no consumer is polling the corresponding RPC, a headless node
//     stops paying for the 2x/s per-peer copies.
//   - peers_exchange — s.peerMu.RLock for persistedMeta/health, then
//     peerProvider.Candidates() whose callbacks reach BannedIPsFn
//     (ipStateMu.RLock) and RemoteBannedFn (s.peerMu.RLock → ipStateMu.RLock
//     in the canonical order).  A burst of IP-state writers can delay
//     this specific rebuild even with other domains quiet.
//   - cm_slots — cm.mu.RLock only (separate mutex inside
//     ConnectionManager, not covered by the Service domain split).
//   - routing — routing.Table.t.mu.Lock (exclusive: SnapshotIncremental
//     consumes the per-identity snap dirty-set and updates the reuse cache)
//     on a separate mutex inside the routing package, not covered by the
//     Service domain split.  Skipped entirely when ConsumeDirty returns
//     false and a previous publish exists — the projection build runs only
//     when the table actually mutated since the last refresh; a clean
//     idle/headless node is never woken.  The build reuses the unchanged
//     route slices of the previous snapshot and re-copies only the churned
//     identities, so it is no longer a full deep copy of the whole table on
//     every publish.  Periodically (routingSnapshotFullInterval) a rebuild
//     that is already happening because the table was dirty is upgraded to
//     a full re-copy as a self-heal net — this rides an existing dirty
//     rebuild, it does NOT wake a clean table.
//
// The snapshots feed different UI panels and there is no correctness
// relationship between them, so this fan-out is safe.
//
// Worst-case staleness for any single snapshot is bounded by
// networkStatsSnapshotInterval plus the time the refresher needs to
// acquire its locks per the footprint above.  Under a writer storm the
// refresher itself may be delayed, but every RPC continues to return
// the last good snapshot — unblocking the UI during the same
// reader-starvation conditions that used to freeze hot local RPCs for
// the full command timeout.
//
// One caveat applies specifically to the routing snapshot: this bound
// covers structural changes only. Routing-specific structural events
// are route accepted/withdrawn/replaced, direct peer added/removed,
// flap burst arming hold-down (the disconnect that crossed the flap
// threshold IS a writer event), and flap-state cleanup that ran
// inside a TickTTL pass which already touched the table. Time-derived
// fields are bounded more loosely — the dirty-flag publisher only
// republishes when a writer touches the table, but wall-clock
// timestamps that drive these fields advance without writer events:
//
//   - A finite-TTL route silently aging out is not a writer event
//     until TickTTL rewrites it (every 10 s). `IsExpired` against
//     `snap.TakenAt` and `ttl_seconds` therefore can lag up to
//     TickTTL_interval (≈10 s) plus the structural publish bound
//     (routingSnapshotMinInterval floor + a refresh tick, ~1–1.5 s),
//     i.e. ≈11–11.5 s.
//   - `FlapEntry.InHoldDown` flipping from true to false on hold-down
//     expiry is also driven by wall-clock — `fs.holdDownUntil`
//     elapsing. TickTTL clears the deadline on its 10 s cadence and
//     marks the table dirty, so the transition is published within
//     TickTTL_interval + the structural publish bound (~1–1.5 s), i.e.
//     ≈11–11.5 s. (Hold-down ARMING is structural and falls under the
//     ~1–1.5 s bound; the false→true transition is a writer event.)
//
// Consumers that depend on strict freshness for any time-derived field
// must read the table directly via `routing.Table.Snapshot()` or
// `routing.Table.Lookup()` (see docs/routing.md "Snapshot freshness").
//
// The initial "prime" rebuild is NOT done here.  It is performed
// synchronously by primeHotReadSnapshots() from Run() before the listener
// opens, so RPC handlers never observe a nil snapshot and therefore never
// need a fallback rebuild path that would re-couple them to cm.mu / s.peerMu /
// routing.Table.t.mu.
//
// The function returns when every per-snapshot goroutine has exited, so the
// caller's close(hotReadsDone) still happens-after the final rebuild for
// each path.
func (s *Service) hotReadsRefreshLoop(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(5)
	// network_stats and peer_health use reader-gated variants: the periodic
	// tick rebuilds only while a consumer is actively polling the matching
	// RPC, so a headless node stops paying for 2x/s peer-domain snapshots.
	// Startup priming still publishes initial snapshots unconditionally.
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.maybeRebuildNetworkStatsSnapshot) }()
	// peer-state-change eager rebuilds still call the unconditional
	// rebuildPeerHealthSnapshot — see maybeRebuildPeerHealthSnapshot.
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.maybeRebuildPeerHealthSnapshot) }()
	// peers_exchange is likewise reader-gated (maybeRebuildPeersExchangeSnapshot):
	// its rebuild allocates persistedMeta/health maps and calls
	// peerProvider.Candidates(), so a node nobody is calling get_peers on stops
	// paying for the 2×/s rebuild. Startup priming still calls the unconditional
	// rebuildPeersExchangeSnapshot.
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.maybeRebuildPeersExchangeSnapshot) }()
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildCMSlotsSnapshot) }()
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildRoutingSnapshot) }()
	wg.Wait()
}

// runSnapshotTicker drives one snapshot rebuild on its own ticker until ctx
// is cancelled.  Isolated into a helper so every hot-read snapshot loop
// (network_stats, peer_health, peers_exchange, cm_slots, routing) shares
// the same shape without duplicating the select.
//
// A second ctx.Done() check is performed AFTER the ticker fires but BEFORE
// invoking rebuild().  Without it the two cases race at shutdown: if the
// ticker tick and ctx cancel arrive close enough together that the runtime
// picks ticker.C, the rebuild runs to completion before the next loop
// iteration observes ctx.Done().  With CM.shutdown concurrently firing a
// disconnect storm that queues writers on s.peerMu, that trailing rebuild
// can stall on s.peerMu.RLock long enough to push hotReadsDone past Run's
// caller-side shutdown budget (test harness: 5 s).  The post-tick check is
// free in the common case and cheap insurance at shutdown.
func (s *Service) runSnapshotTicker(ctx context.Context, rebuild func()) {
	ticker := time.NewTicker(networkStatsSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case <-ctx.Done():
				return
			default:
			}
			rebuild()
		}
	}
}

// refreshHotReadSnapshotsAfterPeerStateChange forces an immediate rebuild of
// the peer-health snapshot so UI pollers that call fetch_peer_health right
// after a state transition observe it without waiting for the next
// hotReadsRefreshLoop tick (up to networkStatsSnapshotInterval late).
//
// Called by markPeerConnected / markPeerDisconnected AFTER releasing
// s.peerMu.Lock.  peer_health rebuild takes its own s.peerMu.RLock briefly; with
// the writer just released the RLock is immediate on the common path.
// Under a writer storm another writer may be queued — the rebuild stalls
// on RLock but the hot RPC path continues serving the previous snapshot,
// preserving the bounded-staleness contract.
//
// Only peer_health is rebuilt here.  peers_exchange is intentionally left
// to the periodic ticker because rebuildPeersExchangeSnapshot calls
// peerProvider.Candidates(), whose callbacks reach s.peerMu.RLock and
// ipStateMu.RLock (BannedIPsFn, RemoteBannedFn) — doing that on the
// caller's goroutine (session write loop, shutdown drain, etc.) couples
// those paths to both peer-domain and IP-state contention.  The 500 ms
// staleness window on get_peers is acceptable: that RPC feeds gossip
// propagation, not the click-to-render UI paths that fetch_peer_health
// serves.  networkStats is likewise skipped — it tracks aggregate traffic
// counters that change continuously and do not need step-synchronous
// visibility on peer transitions.
//
// Skipped entirely when s.runCtx is done: during graceful shutdown every
// session-close fires markPeerDisconnected, and each eager rebuild
// would stall the session-teardown goroutines on s.peerMu contention from
// other tearing-down writers, pushing shutdown past its budget.  The UI
// is not polling during shutdown anyway, so skipping the rebuild is
// correct — the final snapshot that was published before shutdown is
// still served to any lingering reader.
func (s *Service) refreshHotReadSnapshotsAfterPeerStateChange() {
	if s.runCtx != nil {
		select {
		case <-s.runCtx.Done():
			return
		default:
		}
	}
	s.rebuildPeerHealthSnapshot()
}
