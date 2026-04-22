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
// buildPeerExchangeResponse, networkStatsFrame) observes a non-nil snapshot
// on its first load.  With that invariant the handlers can drop their
// synchronous rebuild fallbacks entirely, which used to reach cm.mu.RLock
// and s.mu.RLock on the RPC goroutine and re-coupled the "lock-free hot
// path" contract to the very locks it was built to bypass.
//
// Running four rebuilds back-to-back at startup is cheap: the node has no
// peers yet, so each is O(1).
func (s *Service) primeHotReadSnapshots() {
	s.rebuildNetworkStatsSnapshot()
	s.rebuildPeerHealthSnapshot()
	s.rebuildPeersExchangeSnapshot()
	s.rebuildCMSlotsSnapshot()
}

// hotReadsRefreshLoop periodically rebuilds every atomic snapshot that the
// hot RPC path depends on: network_stats, peer_health, peers_exchange, and
// the ConnectionManager slots view.  Each snapshot is refreshed by its own
// goroutine with its own ticker so a slow rebuild in one path (notably
// peers_exchange, whose rebuild calls peerProvider.Candidates() which
// re-acquires s.mu.RLock per callback, and cm_slots, which takes cm.mu.RLock)
// does not delay the others.  The snapshots feed different UI panels and
// there is no correctness relationship between them, so this fan-out is
// safe.
//
// Worst-case staleness for any single snapshot is bounded by
// networkStatsSnapshotInterval plus the time the refresher needs to acquire
// the relevant lock (s.mu.RLock for the first three, cm.mu.RLock for
// cm_slots).  Under a writer storm the refresher itself may be delayed, but
// every RPC continues to return the last good snapshot — unblocking the UI
// during the same reader-starvation conditions that used to freeze hot
// local RPCs for the full command timeout.
//
// The initial "prime" rebuild is NOT done here.  It is performed
// synchronously by primeHotReadSnapshots() from Run() before the listener
// opens, so RPC handlers never observe a nil snapshot and therefore never
// need a fallback rebuild path that would re-couple them to cm.mu / s.mu.
//
// The function returns when every per-snapshot goroutine has exited, so the
// caller's close(hotReadsDone) still happens-after the final rebuild for
// each path.
func (s *Service) hotReadsRefreshLoop(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildNetworkStatsSnapshot) }()
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildPeerHealthSnapshot) }()
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildPeersExchangeSnapshot) }()
	go func() { defer wg.Done(); s.runSnapshotTicker(ctx, s.rebuildCMSlotsSnapshot) }()
	wg.Wait()
}

// runSnapshotTicker drives one snapshot rebuild on its own ticker until ctx
// is cancelled.  Isolated into a helper so each of the three hot-read
// snapshots gets an identical loop shape without duplicating the select.
//
// A second ctx.Done() check is performed AFTER the ticker fires but BEFORE
// invoking rebuild().  Without it the two cases race at shutdown: if the
// ticker tick and ctx cancel arrive close enough together that the runtime
// picks ticker.C, the rebuild runs to completion before the next loop
// iteration observes ctx.Done().  With CM.shutdown concurrently firing a
// disconnect storm that queues writers on s.mu, that trailing rebuild can
// stall on s.mu.RLock long enough to push hotReadsDone past Run's caller-
// side shutdown budget (test harness: 5 s).  The post-tick check is free
// in the common case and cheap insurance at shutdown.
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
// s.mu.Lock.  peer_health rebuild takes its own s.mu.RLock briefly; with
// the writer just released the RLock is immediate on the common path.
// Under a writer storm another writer may be queued — the rebuild stalls
// on RLock but the hot RPC path continues serving the previous snapshot,
// preserving the bounded-staleness contract.
//
// Only peer_health is rebuilt here.  peers_exchange is intentionally left
// to the periodic ticker because rebuildPeersExchangeSnapshot calls
// peerProvider.Candidates() which re-acquires s.mu.RLock via its
// callbacks — doing that on the caller's goroutine (session write loop,
// shutdown drain, etc.) couples those paths to s.mu contention.  The
// 500 ms staleness window on get_peers is acceptable: that RPC feeds
// gossip propagation, not the click-to-render UI paths that fetch_peer_health
// serves.  networkStats is likewise skipped — it tracks aggregate traffic
// counters that change continuously and do not need step-synchronous
// visibility on peer transitions.
//
// Skipped entirely when s.runCtx is done: during graceful shutdown every
// session-close fires markPeerDisconnected, and each eager rebuild
// would stall the session-teardown goroutines on s.mu contention from
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
