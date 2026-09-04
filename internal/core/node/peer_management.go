package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

func (s *Service) Peers() []transport.Peer {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	return s.peersSnapshotLocked()
}

// peersSnapshotLocked returns a shallow copy of the peers slice.
// Reads s.peers, which is peer-domain state — caller MUST hold
// s.peerMu (read or write).
func (s *Service) peersSnapshotLocked() []transport.Peer {
	out := make([]transport.Peer, len(s.peers))
	copy(out, s.peers)
	return out
}

func (s *Service) peerHealthFrame() protocol.Frame {
	items := s.peerHealthFrames()
	return protocol.Frame{
		Type:       "peer_health",
		Count:      len(items),
		PeerHealth: items,
	}
}

func (s *Service) bootstrapLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Signal ConnectionManager that bootstrap loading is complete.
	// CM will call fill() on receipt, triggering the first outbound dials.
	s.connManager.NotifyBootstrapReady()

	for {
		select {
		case <-ctx.Done():
			s.flushPeerState()
			return
		case <-ticker.C:
			s.cleanupExpiredMessagesForce()
			s.cleanupExpiredNotices()
			s.evictStalePeers()
			s.evictOrphanedHealthEntries()
			s.evictOrphanedPeerMetadata()
			s.evictStaleInboundConns()
			// Raw-address activity throttle map: no other sweep reaches
			// it (raw keys never match the health-address keys above).
			s.evictStalePeerActivity(time.Now().UnixNano())
			// Terminal outbound delivery states have no event-driven
			// delete (no receipt ever arrives for a failed delivery).
			s.sweepTerminalOutbound(time.Now().UTC())
			// Periodic cleanup for the route-quarantine state machine:
			// drop expired peerQuarantine entries whose recidivism
			// window has elapsed, and prune peerDisconnectHistory
			// entries that no longer contain any in-window events.
			// Without this, both maps slowly grow for long-running
			// nodes (every observed peer leaves a residual record).
			// See routing_route_quarantine.go.
			s.purgeRouteQuarantineState()
			// Same hygiene for the ban/blacklist domain: expired
			// local IP bans (s.bans), IP-wide bans (bannedIPSet),
			// remote IP bans (remoteBannedIPs) and stale setup-
			// failure counters previously shrank only lazily — on
			// reconnect of the same IP or recovery of the same
			// peer — so every transient offender left a permanent
			// residue. See ban_purge.go.
			s.purgeExpiredBanState()
			s.retryRelayDeliveries()
			// Sender-side end-to-end retry (delivery_retry.go): the 2s
			// tick is the resolution; the per-entry exponential schedule
			// provides the actual pacing.
			s.retryDueDeliveries(time.Now().UTC())
			s.relayLimiter.cleanup(5 * time.Minute)
			if s.announceLimiter != nil {
				// Phase 4 13.7: drop per-peer announce buckets for
				// identities idle past the cleanup horizon. The
				// idle window is longer than the relay limiter's
				// because reconnect bursts to the same identity
				// must keep accumulating abuse history (a peer
				// flipping every 6 min would otherwise get a
				// fresh bucket each time).
				s.announceLimiter.cleanup(announceLimiterCleanupAge)
			}
			s.maybeSavePeerState()
			s.refreshAggregateStatus()
			s.emitTrafficDeltas()
		}
	}
}

// markPeerStateDirty records that persisted peer state changed and lets the
// next bootstrapLoop tick coalesce the change into a single debounced flush.
// It replaces the synchronous per-event flushPeerState() calls on every path
// that mutates persisted peer state outside the periodic catch-all:
//
//   - add_peer (operator add and startup bootstrap priming),
//   - a newly learned peers.json v3 address→identity binding (addPeerID sets
//     the flag directly because it already holds peerMu),
//   - remote-ban RECORD on a peer-banned notice (handlePeerBannedNotice),
//   - remote-ban CLEAR on auth success (clearRemoteBansOnAuth).
//
// Those used to fire a full O(peers) snapshot+marshal+disk write for every
// single event, so startup bootstrap priming (dozens of back-to-back
// add_peer) cost O(peers^2) allocations and writes. Any NEW call site that
// changes a persisted peer field must use this, not a synchronous
// flushPeerState(). Takes peerMu.Lock; callers MUST NOT already hold peerMu.
func (s *Service) markPeerStateDirty() {
	s.peerMu.Lock()
	s.peerStateDirty = true
	s.peerMu.Unlock()
}

// maybeSavePeerState is the single coalescing flush point, driven by the 2s
// bootstrapLoop tick. It flushes when EITHER:
//
//   - peer state was marked dirty (markPeerStateDirty) and at least
//     peerStateDebounceSeconds has elapsed since the last save — collapses a
//     burst of add_peer / ban-clear events into one flush; or
//   - the periodic catch-all interval (peerStateSaveMinutes) has elapsed —
//     captures slowly-evolving persisted fields (health, score, version
//     policy) that mutate without an explicit dirty mark.
//
// flushPeerState clears the dirty flag, so a clean node performs no
// snapshot/marshal/disk work on the dirty path at all.
func (s *Service) maybeSavePeerState() {
	s.peerMu.RLock()
	elapsed := time.Since(s.lastPeerSave)
	dirty := s.peerStateDirty
	s.peerMu.RUnlock()

	debounceElapsed := dirty && elapsed >= time.Duration(peerStateDebounceSeconds)*time.Second
	periodicElapsed := elapsed >= time.Duration(peerStateSaveMinutes)*time.Minute
	if !debounceElapsed && !periodicElapsed {
		return
	}
	s.flushPeerState()
}

// flushPeerState builds a snapshot from in-memory state and writes it to disk.
func (s *Service) flushPeerState() {
	// Snapshot peerProvider metadata OUTSIDE s.peerMu.  PeerProvider.Candidates()
	// runs under pp.mu.RLock and calls back into Service via RemoteBannedFn
	// / BannedIPsFn, which take s.peerMu.RLock and s.ipStateMu.RLock — that
	// is the existing pp.mu → s.peerMu edge.  Calling peerProvider.KnownPeerStatic
	// from inside buildPeerEntriesLocked (which runs under s.peerMu.Lock) would
	// close the cycle and, under Go's writer-preferring RWMutex, deadlock
	// in a three-goroutine interleaving: refresher holds pp.mu.RLock and
	// waits on s.peerMu; a concurrent pp.mu.Lock writer (Promote/Add) queues
	// up; this path holds s.peerMu.Lock and is then blocked from becoming a
	// new pp.mu reader by the queued writer.  Capturing the snapshot first
	// keeps the lock graph acyclic.
	var providerSnap map[domain.PeerAddress]knownPeer
	if s.peerProvider != nil {
		providerSnap = s.peerProvider.StaticSnapshotAll()
	}

	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_held").Msg("peer_mu_writer")
	// Clear the dirty flag at snapshot time, not after the disk write: a
	// mutation that lands AFTER we copy entries but BEFORE the write
	// finishes must re-mark the state so the next tick flushes again,
	// rather than being swallowed by a clear that runs post-write.
	s.peerStateDirty = false
	entries := s.buildPeerEntriesLocked(providerSnap)
	path := s.peersStatePath

	// Snapshot IP-wide bans, filtering out expired entries.  bannedIPSet
	// and remoteBannedIPs live in the IP/advertise domain; nest
	// s.ipStateMu inside the already-held s.peerMu per the canonical
	// peerMu → ipStateMu order documented in docs/locking.md.
	now := time.Now().UTC()
	var bannedIPs []bannedIPStateEntry
	var remoteBannedIPs []remoteBannedIPStateEntry
	s.ipStateMu.RLock()
	for ip, entry := range s.bannedIPSet {
		if entry.BannedUntil.After(now) {
			affected := make([]string, len(entry.AffectedPeers))
			for i, a := range entry.AffectedPeers {
				affected[i] = string(a)
			}
			bannedIPs = append(bannedIPs, bannedIPStateEntry{
				IP:            ip,
				BannedUntil:   entry.BannedUntil,
				BanOrigin:     string(entry.BanOrigin),
				BanReason:     entry.BanReason,
				AffectedPeers: affected,
			})
		}
	}
	// Snapshot remote IP-wide bans ("they banned our egress IP"),
	// filtering out expired entries. Persisted so a blacklisted-reason
	// peer-banned notice survives restart and the dialler does not
	// resurrect the retry storm the notice was supposed to end.
	for ip, entry := range s.remoteBannedIPs {
		if entry.Until.After(now) {
			remoteBannedIPs = append(remoteBannedIPs, remoteBannedIPStateEntry{
				IP:     ip,
				Until:  entry.Until,
				Reason: entry.Reason,
			})
		}
	}
	s.ipStateMu.RUnlock()
	s.peerMu.Unlock()
	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_released_mid").Msg("peer_mu_writer")

	sortPeerEntries(entries)
	entries = trimPeerEntries(entries)

	// The guard set is read here rather than pushed in by its owner: this is
	// the single writer of the file, and a second writer would be how two
	// halves of it start clobbering each other. Reading OUTSIDE the peer
	// mutex, because the set takes its own leaf lock.
	state := peerStateFile{
		Version:             peerStateVersion,
		Peers:               entries,
		BannedIPs:           bannedIPs,
		RemoteBannedIPs:     remoteBannedIPs,
		FirstHopGuards:      firstHopGuardRows(s.firstHopGuardEntries()),
		FirstHopGuardsOwner: s.firstHopGuardOwner(),
	}
	if err := savePeerState(path, state); err != nil {
		log.Error().Str("path", path).Err(err).Msg("peer state save failed")
		// Re-mark dirty so the next tick retries on the debounce path
		// instead of waiting out the full periodic interval. lastPeerSave
		// is deliberately left untouched (the failed write did not happen),
		// so periodicElapsed also still holds as a second safety net.
		s.peerMu.Lock()
		s.peerStateDirty = true
		s.peerMu.Unlock()
		return
	}

	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_wait_tail").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_held_tail").Msg("peer_mu_writer")
	s.lastPeerSave = time.Now()
	s.peerMu.Unlock()
	log.Trace().Str("site", "flushPeerState").Str("phase", "lock_released_tail").Msg("peer_mu_writer")
}

// evictStalePeers removes in-memory peers whose score has dropped below
// peerEvictScoreThreshold and that have not successfully connected within
// peerEvictStaleWindow.  Bad addresses are purged so they stop consuming dial attempts
// and make room for fresh peer-exchange discoveries.
// Bootstrap peers are never evicted — they act as permanent seeds.
func (s *Service) evictStalePeers() {
	s.peerMu.RLock()
	elapsed := time.Since(s.lastPeerEvict)
	s.peerMu.RUnlock()
	if elapsed < peerEvictInterval {
		return
	}

	now := time.Now()

	// ---------------------------------------------------------------
	// Phase 1 (RLock): identify eviction candidates without holding
	// the write lock. This keeps the write-lock window short, reducing
	// contention with ProbeNode RPCs that need RLock.
	// ---------------------------------------------------------------
	s.peerMu.RLock()
	candidates := make(map[domain.PeerAddress]struct{})
	for _, peer := range s.peers {
		health := s.health[peer.Address]
		if health == nil {
			continue
		}
		if peer.Source == domain.PeerSourceBootstrap {
			continue
		}
		if health.Connected {
			continue
		}
		if pm := s.persistedMeta[peer.Address]; pm != nil && pm.VersionLockout.IsActive() {
			continue
		}
		// Evict if score is terrible AND last successful connection (or
		// first discovery, if never connected) was more than staleWindow ago.
		// Importantly, LastDisconnectedAt is NOT used here — it refreshes on
		// every failed retry and would prevent eviction of perpetually-failing
		// peers.  Only LastConnectedAt (actual success) matters for eviction.
		if health.Score <= peerEvictScoreThreshold {
			lastSuccess := health.LastConnectedAt
			if lastSuccess.IsZero() {
				if pm := s.persistedMeta[peer.Address]; pm != nil && pm.AddedAt != nil {
					lastSuccess = *pm.AddedAt
				}
			}
			if !lastSuccess.IsZero() && now.Sub(lastSuccess) > peerEvictStaleWindow {
				candidates[peer.Address] = struct{}{}
			}
		}
	}
	s.peerMu.RUnlock()

	if len(candidates) == 0 {
		// Still update the timestamp under a short lock so the interval
		// check does not re-run the scan on every tick.
		log.Trace().Str("site", "evictStalePeers_noop").Str("phase", "lock_wait").Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "evictStalePeers_noop").Str("phase", "lock_held").Msg("peer_mu_writer")
		s.lastPeerEvict = now
		s.peerMu.Unlock()
		log.Trace().Str("site", "evictStalePeers_noop").Str("phase", "lock_released").Msg("peer_mu_writer")
		return
	}

	// ---------------------------------------------------------------
	// Phase 2 (Lock): apply evictions. Re-check each candidate under
	// write lock — state may have changed between phases (peer
	// reconnected, score improved, etc.).
	// ---------------------------------------------------------------
	var evicted []domain.PeerAddress
	var evictIdentities []domain.PeerIdentity

	log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.lastPeerEvict = now

	kept := make([]transport.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		if _, candidate := candidates[peer.Address]; !candidate {
			kept = append(kept, peer)
			continue
		}
		// Re-validate under write lock: conditions may have changed.
		health := s.health[peer.Address]
		if health == nil || health.Connected {
			kept = append(kept, peer)
			continue
		}
		if peer.Source == domain.PeerSourceBootstrap {
			kept = append(kept, peer)
			continue
		}
		if pm := s.persistedMeta[peer.Address]; pm != nil && pm.VersionLockout.IsActive() {
			kept = append(kept, peer)
			continue
		}
		if health.Score <= peerEvictScoreThreshold {
			lastSuccess := health.LastConnectedAt
			if lastSuccess.IsZero() {
				if pm := s.persistedMeta[peer.Address]; pm != nil && pm.AddedAt != nil {
					lastSuccess = *pm.AddedAt
				}
			}
			if !lastSuccess.IsZero() && now.Sub(lastSuccess) > peerEvictStaleWindow {
				delete(s.health, peer.Address)
				delete(s.peerTypes, peer.Address)
				if id := s.peerIDs[peer.Address]; !id.IsZero() {
					evictIdentities = append(evictIdentities, id)
				}
				delete(s.peerIDs, peer.Address)
				delete(s.peerVersions, peer.Address)
				delete(s.peerBuilds, peer.Address)
				delete(s.persistedMeta, peer.Address)
				delete(s.metaOrphanFirstSeen, peer.Address)
				evicted = append(evicted, peer.Address)
				continue
			}
		}
		kept = append(kept, peer)
	}
	s.peers = kept
	// Filter to identities whose LAST peerIDs anchor was just removed —
	// must happen under the same peerMu hold as the deletions, before
	// the identity → address association is unrecoverable.
	evictIdentities = s.orphanedIdentitiesLocked(evictIdentities)
	s.peerMu.Unlock()
	log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_released").Msg("peer_mu_writer")

	// Observed-IP hint history and observed-address votes are
	// ipState-domain — drop the evicted addresses' entries (and fully
	// orphaned identities' votes) under a standalone ipStateMu hold
	// (taken after peerMu is released; never the reverse order). Without
	// this the hint history outlived every other per-address record
	// (memory-leak audit, 2026-06), and observedAddrs votes outlived
	// the peerIDs entry that markPeerDisconnected needs to resolve them.
	if len(evicted) > 0 {
		s.ipStateMu.Lock()
		for _, addr := range evicted {
			delete(s.observedIPHistoryByPeer, addr)
		}
		for _, id := range evictIdentities {
			delete(s.observedAddrs, id)
		}
		s.ipStateMu.Unlock()
	}

	// Drop the evicted peers' pending rings. Eviction requires 24 h
	// (peerEvictStaleWindow) without a successful connection while every
	// queued frame is dead after pendingFrameTTL (5 min) anyway, so the
	// frames are undeliverable — but without this drop the rings (and
	// their s.pendingKeys entries, counted against maxPendingFramesTotal)
	// survive forever: flushPendingPeerFrames only prunes on a session
	// (re-)establish that will never come, and evictOrphanedHealthEntries
	// only scans s.health rows, which this sweep just deleted.
	// deliveryMu is taken standalone after peerMu is released (canonical
	// order peerMu → deliveryMu → statusMu).
	if len(evicted) > 0 {
		log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_wait").Msg("delivery_mu_writer")
		s.deliveryMu.Lock()
		log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_held").Msg("delivery_mu_writer")
		dropped := false
		for _, addr := range evicted {
			if _, ok := s.pending[addr]; ok {
				s.capPendingRingLocked(addr, 0)
				dropped = true
			}
		}
		if dropped {
			s.statusMu.Lock()
			s.refreshAggregatePendingLocked()
			s.statusMu.Unlock()
		}
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_released").Msg("delivery_mu_writer")
	}

	// Remove evicted peers from PeerProvider so they no longer
	// appear in Candidates() and stop consuming dial attempts.
	if s.peerProvider != nil {
		for _, addr := range evicted {
			s.peerProvider.Remove(addr)
		}
	}
}

// orphanedIdentitiesLocked returns the subset of ids that no longer
// resolve through ANY surviving s.peerIDs entry. Caller must hold
// s.peerMu (write) and must have already applied its peerIDs deletions —
// this is the last moment the identity → vote association is knowable,
// because the only event-driven observedAddrs delete
// (markPeerDisconnected) resolves the identity through s.peerIDs.
// Identities still anchored by another live address keep their vote.
// The caller then drops the returned identities' observedAddrs entries
// under its ipStateMu section.
func (s *Service) orphanedIdentitiesLocked(ids []domain.PeerIdentity) []domain.PeerIdentity {
	if len(ids) == 0 {
		return nil
	}
	live := make(map[domain.PeerIdentity]struct{}, len(s.peerIDs))
	for _, id := range s.peerIDs {
		live[id] = struct{}{}
	}
	orphaned := ids[:0]
	for _, id := range ids {
		if _, ok := live[id]; !ok {
			orphaned = append(orphaned, id)
		}
	}
	return orphaned
}

// evictOrphanedHealthEntries removes health map entries for inbound-only
// peers that are no longer connected and have no outbound peer-list entry.
//
// These "orphaned" entries accumulate from ephemeral inbound connections
// (e.g. 127.0.0.1:<random_port>) that connected once, disconnected, and
// will never be dialled because they have no persistent address in s.peers.
// Without cleanup the reconnecting count in computeAggregateStatusLocked
// grows unboundedly, inflating TotalPeers and degrading the aggregate
// status signal.
//
// The sweep runs on the same tick as evictStalePeers (bootstrapLoop, every
// 2 s) but the inner scan is throttled to peerEvictInterval.
func (s *Service) evictOrphanedHealthEntries() {
	now := time.Now().UTC()

	// ---------------------------------------------------------------
	// Phase 1 (RLock): build the set of addresses owned by s.peers,
	// then identify orphaned health entries outside that set.
	// ---------------------------------------------------------------
	s.peerMu.RLock()
	peerAddrs := make(map[domain.PeerAddress]struct{}, len(s.peers))
	for _, p := range s.peers {
		peerAddrs[p.Address] = struct{}{}
	}

	var candidates []domain.PeerAddress
	for addr, health := range s.health {
		if _, inPeers := peerAddrs[addr]; inPeers {
			continue // has an outbound peer entry — handled by evictStalePeers
		}
		if health.Connected {
			continue // still alive
		}
		if s.inboundHealthRefs[addr] > 0 {
			continue // active inbound TCP session(s) exist
		}
		// Require a staleness window so that a brief disconnect + immediate
		// reconnect does not lose the health row mid-cycle.
		if health.LastDisconnectedAt.IsZero() || now.Sub(health.LastDisconnectedAt) < orphanedHealthEvictWindow {
			continue
		}
		candidates = append(candidates, addr)
	}
	s.peerMu.RUnlock()

	if len(candidates) == 0 {
		return
	}

	// ---------------------------------------------------------------
	// Phase 2 (Lock): apply evictions. Re-check each candidate under
	// write lock — state may have changed between phases (a peer-exchange
	// frame may have added the address to s.peers).
	// Cross-domain: peer fields (health/peerTypes/etc) under peerMu,
	// s.pending under deliveryMu, refreshAggregateStatusLocked writes
	// s.aggregateStatus under statusMu.  Canonical order
	// peerMu → deliveryMu → statusMu.
	// ---------------------------------------------------------------
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_held").Msg("peer_mu_writer")
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_wait").Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_held").Msg("delivery_mu_writer")

	// Rebuild peerAddrs under write lock — s.peers may have grown.
	peerAddrs = make(map[domain.PeerAddress]struct{}, len(s.peers))
	for _, p := range s.peers {
		peerAddrs[p.Address] = struct{}{}
	}

	var evictedAddrs []domain.PeerAddress
	var evictIdentities []domain.PeerIdentity
	for _, addr := range candidates {
		health := s.health[addr]
		if health == nil || health.Connected {
			continue
		}
		if _, inPeers := peerAddrs[addr]; inPeers {
			continue
		}
		if s.inboundHealthRefs[addr] > 0 {
			continue
		}
		if health.LastDisconnectedAt.IsZero() || now.Sub(health.LastDisconnectedAt) < orphanedHealthEvictWindow {
			continue
		}
		delete(s.health, addr)
		delete(s.peerTypes, addr)
		if id := s.peerIDs[addr]; !id.IsZero() {
			evictIdentities = append(evictIdentities, id)
		}
		delete(s.peerIDs, addr)
		delete(s.peerVersions, addr)
		delete(s.peerBuilds, addr)
		delete(s.persistedMeta, addr)
		// Empty the pending ring through the shared helper rather than a
		// bare delete(s.pending, addr): every queued frame owns an entry
		// in the s.pendingKeys dedup set (and send_message frames an
		// s.outbound record). A wholesale map delete strands those keys
		// forever — no later path can reach them once the queue is gone —
		// permanently eroding the maxPendingFramesTotal budget and making
		// a returning peer's re-queue hit the "already queued" branch
		// without any frame actually queued.
		s.capPendingRingLocked(addr, 0)
		delete(s.metaOrphanFirstSeen, addr)
		evictedAddrs = append(evictedAddrs, addr)
	}
	evicted := len(evictedAddrs)
	if evicted > 0 {
		s.statusMu.Lock()
		s.refreshAggregateStatusLocked()
		s.statusMu.Unlock()
	}
	// Must run under the peerMu hold, while the surviving s.peerIDs
	// entries are still consistent with the deletions above.
	evictIdentities = s.orphanedIdentitiesLocked(evictIdentities)
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_released").Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_released").Msg("peer_mu_writer")

	// Observed-IP hint history and observed-address votes are
	// ipState-domain — standalone hold AFTER peerMu/deliveryMu are
	// released (canonical order keeps ipStateMu innermost; a standalone
	// acquisition avoids adding a deliveryMu↔ipStateMu edge entirely).
	// See the memory-leak audit note on evictOrphanedPeerMetadata.
	if evicted > 0 {
		s.ipStateMu.Lock()
		for _, addr := range evictedAddrs {
			delete(s.observedIPHistoryByPeer, addr)
		}
		for _, id := range evictIdentities {
			delete(s.observedAddrs, id)
		}
		s.ipStateMu.Unlock()
	}

	if evicted > 0 {
		log.Info().Int("evicted", evicted).Int("candidates", len(candidates)).Msg("evicted_orphaned_health_entries")
	}
}

// evictOrphanedPeerMetadata sweeps the per-address metadata maps
// (peerTypes / peerIDs / peerVersions / peerBuilds /
// observedIPHistoryByPeer) for entries whose address has NO owning
// lifecycle anchor anywhere: not in s.peers (evictStalePeers owns
// those), no health row (evictOrphanedHealthEntries owns those), no
// active inbound refs, no outbound session, no persisted row
// (version-lockout and other persisted metadata must survive).
//
// Why this exists (memory-leak audit, 2026-06): the metadata maps
// gain entries on paths that never create a health row — inbound
// hello rejected for version incompatibility, handshake failures
// after the version was already learned, CM dial failures that
// stored the remote version. Inbound addresses carry an EPHEMERAL
// source port, so every reconnect mints a fresh key; on a node that
// has lived through tens of thousands of connections these maps
// grow monotonically and nothing ever deletes the entries.
//
// Deletion uses a two-phase grace via metaOrphanFirstSeen: an
// address must be CONTINUOUSLY orphaned for at least
// orphanedHealthEvictWindow before its metadata is dropped. The
// grace protects the in-flight handshake window where peerIDs /
// peerVersions are written moments before trackInboundConnect
// creates the health row — losing those entries mid-handshake would
// be harmless but noisy (version re-learned from the next frame),
// and the grace removes the race entirely. An address that regains
// any anchor drops its orphan mark.
//
// Lock contract: peerMu (writer) for the peer-domain maps;
// observedIPHistoryByPeer is ipState-domain and is touched under
// ipStateMu nested inside peerMu (canonical peerMu → ipStateMu
// order, same as applyAdvertiseValidationResultLocked). Runs from
// bootstrapLoop next to evictOrphanedHealthEntries.
func (s *Service) evictOrphanedPeerMetadata() {
	now := time.Now().UTC()

	log.Trace().Str("site", "evictOrphanedPeerMetadata").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "evictOrphanedPeerMetadata").Str("phase", "lock_released").Msg("peer_mu_writer")
	}()

	// Throttle: the bootstrapLoop tick is 2 s, the grace window is
	// minutes — a once-a-minute union-scan loses nothing. The
	// metaOrphanSweepInterval also defines the MINIMUM spacing
	// between the marking pass and the deleting pass, on top of the
	// orphanedHealthEvictWindow grace.
	if !s.lastMetaOrphanSweep.IsZero() && now.Sub(s.lastMetaOrphanSweep) < metaOrphanSweepInterval {
		return
	}
	s.lastMetaOrphanSweep = now

	peerAddrs := make(map[domain.PeerAddress]struct{}, len(s.peers))
	for _, p := range s.peers {
		peerAddrs[p.Address] = struct{}{}
	}

	// Candidate set: union of every metadata map's keys.
	candidates := make(map[domain.PeerAddress]struct{})
	for a := range s.peerTypes {
		candidates[a] = struct{}{}
	}
	for a := range s.peerIDs {
		candidates[a] = struct{}{}
	}
	for a := range s.peerVersions {
		candidates[a] = struct{}{}
	}
	for a := range s.peerBuilds {
		candidates[a] = struct{}{}
	}
	s.ipStateMu.Lock()
	for a := range s.observedIPHistoryByPeer {
		candidates[a] = struct{}{}
	}
	s.ipStateMu.Unlock()

	if s.metaOrphanFirstSeen == nil {
		s.metaOrphanFirstSeen = make(map[domain.PeerAddress]time.Time)
	}

	var evictObserved []domain.PeerAddress
	// Identities whose LAST address anchor is evicted below must also drop
	// their observedAddrs vote: the only other delete site
	// (markPeerDisconnected) resolves the identity through s.peerIDs[addr],
	// which this sweep is about to remove — after that the vote is
	// unreachable forever. Seen in practice via the CM setup-failure path,
	// which records the vote (onCMSessionEstablished) but never reaches
	// markPeerDisconnected.
	var evictIdentities []domain.PeerIdentity
	evicted := 0
	for addr := range candidates {
		anchored := false
		if _, ok := peerAddrs[addr]; ok {
			anchored = true
		} else if s.health[addr] != nil {
			anchored = true
		} else if s.inboundHealthRefs[addr] > 0 {
			anchored = true
		} else if s.sessions[addr] != nil {
			anchored = true
		} else if s.persistedMeta[addr] != nil {
			anchored = true
		}
		if anchored {
			delete(s.metaOrphanFirstSeen, addr)
			continue
		}
		first, seen := s.metaOrphanFirstSeen[addr]
		if !seen {
			s.metaOrphanFirstSeen[addr] = now
			continue
		}
		if now.Sub(first) < orphanedHealthEvictWindow {
			continue
		}
		delete(s.peerTypes, addr)
		if id := s.peerIDs[addr]; !id.IsZero() {
			evictIdentities = append(evictIdentities, id)
		}
		delete(s.peerIDs, addr)
		delete(s.peerVersions, addr)
		delete(s.peerBuilds, addr)
		delete(s.metaOrphanFirstSeen, addr)
		evictObserved = append(evictObserved, addr)
		evicted++
	}
	if len(evictObserved) > 0 {
		// Keep votes for identities that still resolve through another
		// (surviving) address — only fully orphaned identities lose theirs.
		evictIdentities = s.orphanedIdentitiesLocked(evictIdentities)
		s.ipStateMu.Lock()
		for _, addr := range evictObserved {
			delete(s.observedIPHistoryByPeer, addr)
		}
		for _, id := range evictIdentities {
			delete(s.observedAddrs, id)
		}
		s.ipStateMu.Unlock()
	}

	// Drop stale orphan marks for addresses whose metadata is already
	// gone (e.g. deleted by the health-driven sweeps between our
	// passes) — without this the tracker itself would leak the very
	// keys it exists to reclaim.
	for addr := range s.metaOrphanFirstSeen {
		if _, ok := candidates[addr]; !ok {
			delete(s.metaOrphanFirstSeen, addr)
		}
	}

	if evicted > 0 {
		log.Info().Int("evicted", evicted).Msg("evicted_orphaned_peer_metadata")
	}
}

// buildPeerEntriesLocked snapshots all known peers with their health metadata.
// Stable metadata (NodeType, Source, AddedAt) is read from persistedMeta so
// that values loaded from disk survive a restart+flush cycle without being
// overwritten by transient runtime state.  Only truly new peers (not yet in
// persistedMeta) derive these fields from runtime maps.
//
// providerSnap is a pre-captured snapshot of PeerProvider.known taken by
// the caller OUTSIDE any s.peerMu hold.  This function never calls back
// into peerProvider: doing so would close the peerMu → pp.mu edge and —
// combined with the existing pp.mu → peerMu edge via Candidates()
// callbacks — deadlock under Go's writer-preferring RWMutex.  nil is
// permitted (tests that construct a bare Service without a provider can
// pass nil; in that case the persistedMeta row is treated as
// authoritative for Source / AddedAt).
//
// Must be called with s.peerMu held (write lock required — updates
// persistedMeta for newly discovered peers).
func (s *Service) buildPeerEntriesLocked(providerSnap map[domain.PeerAddress]knownPeer) []peerEntry {
	entries := make([]peerEntry, 0, len(s.peers))
	now := time.Now().UTC()
	for _, peer := range s.peers {
		if peer.Address == "" {
			continue
		}
		// Never persist loopback / private LAN peers unless the node is
		// explicitly configured for private peering. A manual
		// `addpeer 192.168.x` is a runtime-only dial intent (immediate
		// EmitSlot(ManualPeerRequested)); it must not survive a restart in
		// peers.json nor be re-selected as a candidate from disk. This
		// mirrors the candidate-side filter (shouldSkipPersistedPrivatePeer),
		// keeping the persisted set and the dial-candidate set consistent.
		if !s.cfg.AllowPrivatePeers {
			if host, _, ok := splitHostPort(string(peer.Address)); ok {
				if isManualLocalDialIP(net.ParseIP(host)) {
					continue
				}
			}
		}
		var entry peerEntry
		if pm := s.persistedMeta[peer.Address]; pm != nil {
			// Preserve stable metadata from the persisted snapshot.
			entry = peerEntry{
				Address:  peer.Address,
				Identity: pm.Identity,
				NodeType: pm.NodeType,
				Network:  pm.Network,
				Source:   pm.Source,
				AddedAt:  pm.AddedAt,
			}
			// If runtime has a fresher NodeType (e.g. from a hello/welcome),
			// prefer it over the persisted value.
			if rt := s.peerTypes[peer.Address]; rt != "" {
				entry.NodeType = rt
			}
			// PeerProvider is the runtime authority for Source and AddedAt.
			// Add(bootstrap) upgrades Source; Promote() refreshes both
			// Source and AddedAt. persistedMeta still holds the on-disk
			// values, so we prefer the provider's copy to ensure
			// promotions round-trip through peers.json.  We read from the
			// pre-captured providerSnap — never call back into peerProvider
			// from under s.peerMu (see deadlock note above).
			if kp, ok := providerSnap[peer.Address]; ok {
				entry.Source = kp.Source
				t := kp.AddedAt
				entry.AddedAt = &t
			}
		} else {
			// New peer discovered at runtime — derive from live state.
			entry = peerEntry{
				Address:  peer.Address,
				Identity: s.peerIDs[peer.Address],
				NodeType: s.peerTypes[peer.Address],
				Network:  classifyAddress(peer.Address),
				Source:   peer.Source,
				AddedAt:  &now,
			}
			// Store so that subsequent flushes are stable.
			clone := entry
			s.persistedMeta[peer.Address] = &clone
		}
		if identity := s.peerIDs[peer.Address]; !identity.IsZero() {
			entry.Identity = identity
		}
		if health := s.health[peer.Address]; health != nil {
			if !health.LastConnectedAt.IsZero() {
				t := health.LastConnectedAt
				entry.LastConnectedAt = &t
			}
			if !health.LastDisconnectedAt.IsZero() {
				t := health.LastDisconnectedAt
				entry.LastDisconnectedAt = &t
			}
			entry.ConsecutiveFailures = health.ConsecutiveFailures
			entry.LastError = health.LastError
			entry.Score = health.Score
			if !health.BannedUntil.IsZero() {
				t := health.BannedUntil
				entry.BannedUntil = &t
			}
			// Machine-readable version diagnostics — persisted so the
			// operator-visible snapshot survives restarts.
			entry.LastErrorCode = health.LastErrorCode
			entry.LastDisconnectCode = health.LastDisconnectCode
			entry.IncompatibleVersionAttempts = health.IncompatibleVersionAttempts
			if !health.LastIncompatibleVersionAt.IsZero() {
				t := health.LastIncompatibleVersionAt
				entry.LastIncompatibleVersionAt = &t
			}
			entry.ObservedPeerVersion = health.ObservedPeerVersion
			entry.ObservedPeerMinimumVersion = health.ObservedPeerMinimumVersion
		}
		// Preserve version lockout from persistedMeta (set by
		// penalizeOldProtocolPeer when version-evidence confirms
		// that our protocol version is too old for this peer).
		if pm := s.persistedMeta[peer.Address]; pm != nil && pm.VersionLockout.IsActive() {
			entry.VersionLockout = pm.VersionLockout
		}
		// Preserve a still-active per-peer remote ban from persistedMeta
		// (set by recordRemoteBanLocked when a peer-banned notice scoped to
		// this address arrives). Without this copy the ban would be
		// in-memory only — buildPeerEntriesLocked rebuilds each entry from
		// scratch, so the next flush would rewrite peers.json without the
		// window and a restart would forget the ban, resuming the retry
		// storm the notice was meant to end. Expired windows are NOT copied
		// (so stale rows do not accumulate); clearRemoteBanLocked's nil-out
		// on successful-handshake recovery is reflected by the same absence.
		if pm := s.persistedMeta[peer.Address]; pm != nil && pm.RemoteBannedUntil != nil && pm.RemoteBannedUntil.After(now) {
			t := *pm.RemoteBannedUntil
			entry.RemoteBannedUntil = &t
			entry.RemoteBanReason = pm.RemoteBanReason
		}
		// Fall back to runtime classification only when persisted Network
		// is absent (new peer or pre-Network peers.json). Persisted value
		// takes priority — it may have been set by PeerProvider or restored
		// from a migration.
		if entry.Network == "" {
			entry.Network = classifyAddress(entry.Address)
		}
		entries = append(entries, entry)
	}
	return entries
}

// peerSource infers the source tag from a legacy peer ID prefix.
// Kept for backward compatibility with peers.json files written before
// the typed PeerSource migration. New code should read transport.Peer.Source directly.
func peerSource(id string) domain.PeerSource {
	switch {
	case len(id) >= 9 && id[:9] == "bootstrap":
		return domain.PeerSourceBootstrap
	case len(id) >= 9 && id[:9] == "persisted":
		return domain.PeerSourcePersisted
	default:
		return domain.PeerSourcePeerExchange
	}
}

func (s *Service) ensurePeerSessions(ctx context.Context) {
	for _, candidate := range s.peerDialCandidates() {
		// Cross-domain: s.upstream lives under s.deliveryMu, s.dialOrigin
		// under s.peerMu.  Canonical s.peerMu OUTER → s.deliveryMu INNER.
		log.Trace().Str("site", "ensurePeerSessions_register").Str("phase", "lock_wait").Str("address", string(candidate.address)).Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "ensurePeerSessions_register").Str("phase", "lock_held").Str("address", string(candidate.address)).Msg("peer_mu_writer")
		s.deliveryMu.Lock()
		if _, ok := s.upstream[candidate.address]; ok {
			s.deliveryMu.Unlock()
			s.peerMu.Unlock()
			log.Trace().Str("site", "ensurePeerSessions_register").Str("phase", "lock_released_dup").Str("address", string(candidate.address)).Msg("peer_mu_writer")
			continue
		}
		s.upstream[candidate.address] = struct{}{}
		// Record the mapping from dial address to primary peer address
		// so that health updates (markPeerConnected/Disconnected) always
		// accumulate on the primary entry, even when a fallback port is used.
		if candidate.primary != candidate.address {
			s.dialOrigin[candidate.address] = candidate.primary
		}
		s.deliveryMu.Unlock()
		s.peerMu.Unlock()
		log.Trace().Str("site", "ensurePeerSessions_register").Str("phase", "lock_released").Str("address", string(candidate.address)).Msg("peer_mu_writer")
		// lifecycle: per-DIAL goroutine, owned by the session it opens. It ends
		// when that session's serve loop ends, and Run joins those through
		// connWg and the ConnectionManager's own dial group — one goroutine,
		// one owner.
		go func(c peerDialCandidate) {
			defer func() {
				log.Trace().Str("site", "ensurePeerSessions_cleanup").Str("phase", "lock_wait").Str("address", string(c.address)).Msg("peer_mu_writer")
				s.peerMu.Lock()
				log.Trace().Str("site", "ensurePeerSessions_cleanup").Str("phase", "lock_held").Str("address", string(c.address)).Msg("peer_mu_writer")
				s.deliveryMu.Lock()
				// Best-effort cmdLimiter cleanup. Normally
				// runPeerSession's error path (peer_sessions.go) has
				// already deleted the session and dropped the bucket
				// before we land here, so s.sessions[c.address] is
				// usually nil. We still try in case a session was
				// installed by a code path that never hit
				// runPeerSession's cleanup (defence in depth — the
				// helper no-ops on connID==0). All authoritative
				// bucket drops live at the three session-removal
				// sites; see dropOutboundControlFrameBucket.
				var leftoverConnID domain.ConnID
				if sess := s.sessions[c.address]; sess != nil {
					leftoverConnID = sess.connID
				}
				delete(s.sessions, c.address)
				delete(s.upstream, c.address)
				delete(s.dialOrigin, c.address)
				s.deliveryMu.Unlock()
				s.peerMu.Unlock()
				s.dropOutboundControlFrameBucket(leftoverConnID)
				log.Trace().Str("site", "ensurePeerSessions_cleanup").Str("phase", "lock_released").Str("address", string(c.address)).Msg("peer_mu_writer")
			}()
			s.runPeerSession(ctx, c.address)
		}(candidate)
	}
}

// connectedHostsLocked returns the set of hosts (IP addresses or
// hostnames) that already have an active connection — either an
// outbound peer session or an inbound connection. Used by
// peerDialCandidates to avoid dialing hosts we are already connected
// to, since the goal is fault tolerance across distinct hosts.
//
// For inbound connections the actual TCP remote IP is used instead of the
// peer's self-reported overlay address.  A NATed peer or a peer that
// advertises a different endpoint would not reserve its real host if we
// relied on NetCore.Address(), allowing a second outbound connection
// to the same machine.
// Cross-domain read: s.upstream is delivery-domain (s.deliveryMu);
// inbound conn state is peer-domain (s.peerMu).  Caller MUST hold
// s.peerMu AND s.deliveryMu at least for read, acquired in canonical
// s.peerMu OUTER → s.deliveryMu INNER order.
func (s *Service) connectedHostsLocked() map[string]struct{} {
	hosts := make(map[string]struct{})

	// Outbound sessions.
	for addr := range s.upstream {
		if host, _, ok := splitHostPort(string(addr)); ok {
			hosts[host] = struct{}{}
		}
	}

	// Inbound connections — use the real transport-level remote IP.
	// Skip connections that have not received any frame for longer than
	// the stall threshold. Uses per-connection lastActivity instead of
	// shared health state to avoid conflating NATed peers that advertise
	// the same listen address.
	//
	// The unified registry now carries both directions;
	// filter to Inbound so outbound NetCores are not double-counted here —
	// they are already represented via s.upstream above.
	now := time.Now().UTC()
	stallThreshold := heartbeatInterval + pongStallTimeout
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if !info.lastActivity.IsZero() && now.Sub(info.lastActivity) >= stallThreshold {
			return true
		}
		if ip := remoteIPFromString(info.remoteAddr); ip != "" {
			hosts[ip] = struct{}{}
		}
		return true
	})

	return hosts
}

// peerDialCandidate is a scored candidate for outgoing connection attempts.
type peerDialCandidate struct {
	address domain.PeerAddress // actual address to dial (may be a fallback port variant)
	primary domain.PeerAddress // primary peer address in s.peers (health/score are tracked here)
	score   int
	index   int // insertion order for stable tie-breaking (preserves bootstrap-first ordering)
}

func (s *Service) peerDialCandidates() []peerDialCandidate {
	// Cross-domain: reads s.peers/s.health (peer-domain, under s.peerMu)
	// and s.upstream (delivery-domain, under s.deliveryMu).  Canonical
	// s.peerMu OUTER → s.deliveryMu INNER; both held for the whole scan
	// so the candidate set stays consistent with the current upstream
	// registry.
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()

	limit := s.cfg.EffectiveMaxOutgoingPeers()
	active := len(s.upstream)
	if limit > 0 && active >= limit {
		return nil
	}

	connectedHosts := s.connectedHostsLocked()

	now := time.Now()
	var scored []peerDialCandidate
	seen := make(map[domain.PeerAddress]struct{})
	for _, peer := range s.peers {
		primaryAddr := domain.PeerAddress(strings.TrimSpace(string(peer.Address)))

		// Look up health/score/cooldown from the primary address — the one
		// stored in s.peers and tracked by markPeerConnected/Disconnected.
		// Fallback dial variants (e.g. same host with default port) share
		// the primary's reputation so that cooldown cannot be bypassed by
		// dialling an alternative port.
		primaryHealth := s.health[primaryAddr]
		peerScore := 0
		if primaryHealth != nil {
			peerScore = primaryHealth.Score
			// Temporary ban: skip the peer entirely until the ban expires.
			// Applied when a peer runs an incompatible protocol version.
			if !primaryHealth.BannedUntil.IsZero() && now.Before(primaryHealth.BannedUntil) {
				continue
			}
			// Exponential cooldown: skip ALL dial variants for this peer
			// while the backoff window is active.  A single failure does
			// NOT trigger cooldown — the peer gets an immediate retry
			// on the next bootstrapLoop tick.  This avoids stalling
			// reconnection when a peer was simply not started yet.
			if primaryHealth.ConsecutiveFailures > 1 && !primaryHealth.LastDisconnectedAt.IsZero() {
				cooldown := peerCooldownDuration(primaryHealth.ConsecutiveFailures - 1)
				if now.Sub(primaryHealth.LastDisconnectedAt) < cooldown {
					continue
				}
			}
		}
		// Version lockout: skip peers that already confirmed our
		// protocol version is too old. A futile dial wastes resources
		// and risks remote-side ban escalation. Lockout is cleared
		// when the local version changes (startup path).
		if s.isPeerVersionLockedOutLocked(primaryAddr) {
			continue
		}

		for _, address := range s.dialAttemptAddressesLocked(primaryAddr) {
			if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
				continue
			}
			// Skip addresses in network groups we cannot reach (e.g.
			// .onion without a proxy, I2P without a tunnel, etc.).
			if !s.canReach(address) {
				continue
			}
			if _, ok := s.upstream[address]; ok {
				continue
			}
			// Skip hosts that already have an active connection
			// (outbound or inbound). The goal is fault tolerance
			// across distinct hosts, not accumulating multiple
			// connections to the same IP.
			if host, _, ok := splitHostPort(string(address)); ok {
				if _, connected := connectedHosts[host]; connected {
					continue
				}
			}
			if _, ok := seen[address]; ok {
				continue
			}
			seen[address] = struct{}{}
			scored = append(scored, peerDialCandidate{address: address, primary: primaryAddr, score: peerScore, index: len(scored)})
		}
	}

	// Sort by score descending so the healthiest peers
	// are dialled first and degraded peers sink to the bottom.
	// Stable tie-breaker by insertion index preserves bootstrap-first ordering.
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].index < scored[j].index
	})

	needed := len(scored)
	if limit > 0 && active+needed > limit {
		needed = limit - active
	}
	if needed > len(scored) {
		needed = len(scored)
	}
	if needed < len(scored) {
		scored = scored[:needed]
	}
	return scored
}

// syncReplySkipBudget bounds how many interleaved frames readSyncReply
// discards while waiting for the expected reply type on a syncPeer
// bootstrap connection.
//
// After auth_ok the responder's FIFO writer may already hold frames
// enqueued by auth-time side effects: the inbox backlog replay
// (pushBacklogToSubscriber fires right after auth_ok — including the
// very DMs whose unknown sender key triggered this sync dial) and the
// announce-plane baseline kicked off by trackInboundConnect. All of
// those land on the wire BEFORE the reply to any request we send next.
// The auth_ok read has tolerated this interleave since its "skip up to
// 5 unexpected frames" loop was added, but the get_peers /
// fetch_contacts reads predated the interleave and read exactly ONE
// frame — so a single backlog push_message parsed as a frame with an
// empty Contacts list, the sync reported zero imported contacts
// (sync_peer_no_new_contacts), and sender-key recovery for relayed DMs
// from a first-contact sender wedged permanently: every retry redialed
// the relay, retriggered the backlog replay, and re-read a push frame
// instead of the contacts reply.
//
// The FLOOR budget is sized for the worst DEFAULT pre-reply flood, not
// for a handful of frames: the backlog replay alone can carry up to
// maxReceiptBacklogPerRecipient (4096) receipt frames plus the pending
// ring (default maxPendingFramesPerPeer=200) plus a chunked v3 announce
// baseline. An undersized budget re-creates the original wedge one
// level up: the reader gives up before the reply, the sync fails, and
// the next retry redials into the same (still un-acked, so replayed
// again) backlog. When the operator raises PendingRingSize the flood
// scales with it, so the effective budget is computed from the live
// config (syncReplySkipBudgetFor) rather than pinned here;
// syncReplyDrainCap and the per-read idle deadline still bound the
// drain in time regardless.
const syncReplySkipBudgetFloor = 8192

// syncReplySkipBudgetFor returns the drain frame-budget for this node's
// current config: the floor, or (receipt backlog + configured pending
// ring + announce-baseline slack) when a raised PendingRingSize makes
// the worst legitimate flood exceed the floor. Without this an operator
// who raised CORSA_PENDING_RING_SIZE past ~4000 could see recovery give
// up before the contacts reply again.
func (s *Service) syncReplySkipBudgetFor() int {
	worst := maxReceiptBacklogPerRecipient + s.pendingRingSize() + 2048
	if worst < syncReplySkipBudgetFloor {
		return syncReplySkipBudgetFloor
	}
	return worst
}

// syncReplyDrainCap bounds the total wall time readSyncReply may spend
// draining interleaved frames for ONE reply. The dial's initial
// absolute deadline (syncHandshakeTimeout = 1.5s for the whole
// handshake) is far too small to drain a full receipt backlog over a
// WAN link, so readSyncReply refreshes the connection deadline before
// every read (idle bound per frame) and enforces this cap on the
// total. Long drains are affordable because sender-key recovery no
// longer blocks any session loop: unknown-sender-key handlers schedule
// the sync through triggerSenderKeySyncAsync (a background,
// single-flight goroutine), so the only party waiting on this cap is
// the recovery goroutine itself.
const syncReplyDrainCap = 10 * time.Second

// syncRecoveryTimeout bounds one whole background sender-key recovery
// dial: TCP dial + hello/welcome/auth (each read individually bounded
// by the syncHandshakeTimeout idle deadline) + up to two
// syncReplyDrainCap reply drains. It is the ctx budget
// syncSenderKeys attaches to its fresh-dial fallback; readSyncReply
// observes the ctx per iteration and the deadline-refresh closure
// clamps to the ctx deadline, so a cancelled or expired ctx actually
// stops the socket reads instead of being out-lived by the sliding
// deadline.
const syncRecoveryTimeout = 25 * time.Second

// readSyncReply reads frames from a syncPeer bootstrap connection until
// one with the wanted type — or a terminal "error" frame — arrives,
// skipping up to syncReplySkipBudget interleaved frames within a
// syncReplyDrainCap wall budget. refreshDeadline is invoked before
// every read to extend the connection deadline by one idle interval:
// steady progress (a backlog flood still arriving) must not be killed
// by the dial's initial absolute deadline, while a silent peer still
// times out after a single idle interval. An interleaved line that
// fails protocol.ParseFrameLine is skipped too (still consuming
// budget): announce-plane lines are marshalled from their own frame
// structs and are not guaranteed to round-trip through the generic
// Frame parser.
//
// Discarded interleaved frames are NOT all recoverable, and that is a
// pre-existing property of the ephemeral sync dial, not of this reader.
// Interleaved frames are DROPPED, deliberately. The responder treats an
// authenticated sync dial as a routable node connection and may flush
// fire-and-forget pending frames into it (deleting them from its own
// ring at send time), so this drop is a real — but PRE-EXISTING and
// bounded — loss window: it predates this reader and is what every
// deployed node already does. Processing them here was tried and
// reverted: acting on frames before auth_ok requires buffering them,
// and any buffer either bounds memory (a hostile endpoint can send
// 8 MiB frames) or avoids dropping — never both. The durable fix is
// responder-side: do not treat a one-shot recovery dial as a routable
// node connection (see the follow-up note on syncPeer).
//
// Returns the matching frame, or an error when the underlying read
// fails, either budget is exhausted, or ctx is done. ctx is observed
// per iteration (and the refreshDeadline closure is expected to clamp
// the socket deadline to the ctx deadline), so lifecycle cancellation
// is honoured mid-drain — the sliding deadline must never out-live the
// owning context (CLAUDE.md: ctx is the cancellation authority). The
// caller distinguishes the wanted type from "error" via frame.Type.
func readSyncReply(ctx context.Context, reader *bufio.Reader, wantType string, budget int, refreshDeadline func()) (protocol.Frame, error) {
	start := time.Now()
	for skipped := 0; ; skipped++ {
		if err := ctx.Err(); err != nil {
			return protocol.Frame{}, fmt.Errorf("sync reply wait for %s aborted: %w", wantType, err)
		}
		if skipped >= budget {
			return protocol.Frame{}, fmt.Errorf("no %s reply within %d frames", wantType, budget)
		}
		if time.Since(start) > syncReplyDrainCap {
			return protocol.Frame{}, fmt.Errorf("no %s reply within %v (drained %d frames)", wantType, syncReplyDrainCap, skipped)
		}
		refreshDeadline()
		line, err := readFrameLine(reader, maxResponseLineBytes)
		if err != nil {
			return protocol.Frame{}, err
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		if err != nil {
			log.Debug().Err(err).Str("want", wantType).Int("skipped", skipped+1).Msg("sync_reply_skip_unparseable_frame")
			continue
		}
		if frame.Type == wantType || frame.Type == "error" {
			return frame, nil
		}
		log.Debug().Str("want", wantType).Str("type", frame.Type).Int("skipped", skipped+1).Msg("sync_reply_skip_interleaved_frame")
	}
}

// syncPeer opens a fresh TCP connection to the given address, performs
// a full handshake (hello → welcome → auth if required), optionally
// requests the peer list (when requestPeers is true), fetches contacts,
// and imports any verified contacts into local state. Returns the number
// of contacts successfully imported.
//
// The requestPeers parameter is passed by the caller rather than derived
// internally from shouldRequestPeers(): forced-refresh paths (sender-key
// recovery) deliberately skip peer exchange even when the aggregate status
// would otherwise allow it, because those paths are a narrow contact/key
// sync, not a bootstrap/recovery dial. See
// docs/peer-discovery-conditional-get-peers.ru.md Step 5.
//
// A fresh connection is used instead of reusing the active session
// because syncPeer is called from dispatchPeerSessionFrame when a
// push_message fails with ErrCodeUnknownSenderKey. That handler runs
// inline inside peerSessionRequest. Reusing the session would call
// syncPeerSession → peerSessionRequest on the same inboxCh, consuming
// frames meant for the outer caller and causing a 12-second stall
// (peerRequestTimeout).
//
// The contacts leg of this dial is deprecated: superseded by the
// get_identity datagram lookup, kept as the epidemic bridge for peers
// without the layer. TODO(fetch-contacts-floor): remove the leg when
// nothing is left to bridge — see docs/protocol/identity-lookup.md.
func (s *Service) syncPeer(ctx context.Context, address domain.PeerAddress, requestPeers bool) int {
	conn, err := s.dialPeer(ctx, address, syncHandshakeTimeout)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_dial_failed")
		return 0
	}

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	// Per-read idle-deadline refresh for the filtered reply reads
	// (readSyncReply): each successfully arriving frame proves the peer
	// is alive and draining its post-auth flood, so the deadline slides
	// forward by one idle interval per read — but NEVER past the owning
	// ctx deadline. Without the clamp the sliding refresh would out-live
	// a cancelled/expired ctx and keep the socket alive for up to the
	// drain cap, violating the ctx-first cancellation contract. Total
	// wall time is bounded by syncReplyDrainCap inside readSyncReply.
	refreshSyncDeadline := func() {
		deadline := time.Now().Add(syncHandshakeTimeout)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		_ = conn.SetDeadline(deadline)
	}

	// Wrap the one-shot conn in a bootstrap NetCore so every write on this
	// handshake path goes through the managed writer — no raw io.WriteString
	// remains on this probe. pc.Close() closes conn and waits for the writer
	// goroutine to drain, so no separate defer conn.Close() is needed.
	// writeDeadline matches the outer handshake budget (syncHandshakeTimeout)
	// so the wrapper does not silently extend per-write timing past what the
	// caller guaranteed with SetDeadline above.
	pc := netcore.NewBootstrap(conn, syncHandshakeTimeout)
	defer pc.Close()

	skipBudget := s.syncReplySkipBudgetFor()

	// NOTE (accepted limitation): the responder flushes its pending
	// fire-and-forget frames and mailbox backlog into this dial once it
	// authenticates us, and readSyncReply drops them. That loss window
	// is pre-existing (every deployed node behaves this way) and is NOT
	// fixable from this side: acting on pre-auth_ok frames needs a
	// buffer, and no finite buffer both bounds memory against 8 MiB
	// frames and avoids dropping under a raised PendingRingSize. The
	// durable fix is responder-side — stop treating a one-shot recovery
	// dial as a routable node connection (do not install the node-route
	// subscriber / flush pending for it) — which needs both ends
	// updated and rides the ProtocolVersion 27 floor raise.
	if st := pc.SendRawSyncBlocking([]byte(s.nodeHelloJSONLine())); st != netcore.SendOK {
		log.Warn().Str("peer", string(address)).Str("status", st.String()).Msg("sync_peer_hello_write_failed")
		return 0
	}
	welcomeLine, err := readFrameLine(reader, maxResponseLineBytes)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_welcome_read_failed")
		return 0
	}
	welcome, err := protocol.ParseFrameLine(strings.TrimSpace(welcomeLine))
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_welcome_parse_failed")
		return 0
	}
	// Advertise convergence: peer closed the dial with a connection_notice
	// instead of welcoming us. Record the observed IP hint (when present
	// and routable) and abort this sync pass — the corrected advertise
	// will be emitted on the next hello.
	if welcome.Type == protocol.FrameTypeConnectionNotice {
		s.handleConnectionNotice(address, welcome)
		log.Info().Str("peer", string(address)).Str("code", welcome.Code).Msg("sync_peer_connection_notice")
		// Remote-first self-loopback discovery: when our dial lands on
		// ourselves via NAT hairpin / peer-exchange mirror / fallback
		// alias, the responder (which IS us) detects the collision at
		// the inbound hello handler and sends back
		// connection_notice{code=peer-banned, reason=self-identity}.
		// NoticeErrorFromFrame resolves this to ErrSelfIdentity, which
		// tells syncPeer to apply the same 24h local cooldown the
		// managed outbound paths produce. Without this branch,
		// handlePeerBannedNotice would only write the per-peer remote
		// ban record in persistedMeta using the notice's (potentially
		// empty) `until` — health.BannedUntil would stay zero and
		// LastErrorCode would not surface the self-identity signal to
		// the monitor/UI, leaving the fresh-dial recovery callers
		// (syncSenderKeys, unknown-sender recovery) free to re-enter
		// the churn loop on the next tick.
		if errors.Is(protocol.NoticeErrorFromFrame(welcome), protocol.ErrSelfIdentity) {
			s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		}
		return 0
	}
	// Self-loopback guard on the sender-key / forced-refresh dial path.
	// Abort the sync before auth_session is signed with our own key and
	// learnIdentityFromWelcome runs; `defer pc.Close()` above tears down
	// through the NetCore wrapper, so no raw socket operation is needed.
	//
	// The managed outbound paths (openPeerSession, openPeerSessionForCM)
	// surface the collision as *selfIdentityError which onCMDialFailed
	// converts into a 24h cooldown via applySelfIdentityCooldown. syncPeer
	// is a standalone one-shot dial — its callers (syncSenderKeys, the
	// unknown-sender recovery in handleInboundPushMessage) do not run
	// through the connection-manager failure hook, so returning 0 without
	// persisting a cooldown would let the next sender-key refresh hammer
	// the same self-looping address on the very next tick. Route through
	// applySelfIdentityCooldown directly to converge on the same wall-
	// clock ban window the CM paths produce.
	if s.isSelfIdentity(domain.PeerIdentityFromWire(welcome.Address)) {
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", welcome.Listen).
			Msg("sync_peer_self_identity_rejected")
		s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		return 0
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, connauth.SessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_marshal_failed")
			return 0
		}
		if st := pc.SendRawSyncBlocking([]byte(authLine)); st != netcore.SendOK {
			log.Warn().Str("peer", string(address)).Str("status", st.String()).Msg("sync_peer_auth_write_failed")
			return 0
		}
		// After auth_session the remote may interleave non-auth frames
		// BEFORE the auth_ok reply: handleAuthSession runs
		// trackInboundConnect (which can flush up to a full pending ring
		// of fire-and-forget frames — default maxPendingFramesPerPeer,
		// operator-raisable) plus route-sync/announce output before the
		// caller even enqueues auth_ok. The historical "skip up to 5"
		// loop broke the recovery dial exactly when it mattered — a
		// loaded responder with a backlog for us — so the auth wait uses
		// the same drain-tolerant filtered reader as the contacts/peers
		// replies (budget syncReplySkipBudget, wall cap
		// syncReplyDrainCap, ctx-clamped sliding idle deadline).
		frame, err := readSyncReply(ctx, reader, "auth_ok", skipBudget, refreshSyncDeadline)
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_read_failed")
			return 0
		}
		if frame.Type != "auth_ok" {
			log.Warn().Str("peer", string(address)).Str("type", frame.Type).Str("code", frame.Code).Msg("sync_peer_auth_rejected")
			return 0
		}
		// Outbound convergence success hook: syncPeer is a legacy fresh-
		// dial path (sender-key recovery, forced refresh) that completes
		// the same hello → welcome → auth_ok exchange as the managed-
		// session and raw/bootstrap paths. Without this call, peers
		// reached only through syncPeer would never get
		// announce_state=announceable or a trusted advertise triple,
		// so convergence state would depend on which outbound path
		// happened to reach the peer.
		//
		// pc is the bootstrap *netcore.NetCore wrapper created above; its
		// RemoteAddr() is the same "host:port" form that the managed-
		// session path feeds in, so both paths project onto persistedMeta
		// through one writer.
		s.recordOutboundAuthSuccess(address, pc.RemoteAddr())
	}
	s.learnIdentityFromWelcome(welcome, address)
	s.peerMu.RLock()
	syncHealthKey := s.resolveHealthAddress(address)
	s.peerMu.RUnlock()
	s.addPeerVersion(syncHealthKey, welcome.ClientVersion)
	s.addPeerBuild(syncHealthKey, welcome.ClientBuild)

	// Peer exchange policy is decided by the caller (see requestPeers param).
	// Forced-refresh paths pass false to keep the sync narrow. Other future
	// callers that want the legacy behaviour must evaluate shouldRequestPeers()
	// themselves before calling. See
	// docs/peer-discovery-conditional-get-peers.ru.md Steps 4 and 5.
	if requestPeers {
		if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "get_peers"}); err == nil {
			if st := pc.SendRawSyncBlocking([]byte(line)); st != netcore.SendOK {
				log.Warn().Str("peer", string(address)).Str("status", st.String()).Msg("sync_peer_get_peers_failed")
				return 0
			}
			// Frame-type-filtered read: the responder may interleave
			// backlog/announce frames ahead of the peers reply (see
			// syncReplySkipBudget). A bare one-line read here would
			// mistake the first interleaved frame for the reply.
			frame, err := readSyncReply(ctx, reader, "peers", skipBudget, refreshSyncDeadline)
			if err != nil {
				log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_get_peers_read_failed")
				return 0
			}
			if frame.Type == "peers" {
				peersImported := 0
				for _, peer := range frame.Peers {
					if s.addPeerAddress(domain.PeerAddress(peer), "", domain.PeerIdentity{}) {
						peersImported++
					}
				}
				s.logPeerExchangeExecuted(peerExchangePathLegacyDial, address, len(frame.Peers), peersImported)
			} else {
				// Terminal "error" frame from the responder. Keep going:
				// the contacts fetch below is the actual purpose of the
				// recovery dial and may still succeed.
				log.Warn().Str("peer", string(address)).Str("code", frame.Code).Msg("sync_peer_get_peers_rejected")
			}
		} else {
			return 0
		}
	}

	imported := 0
	if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "fetch_contacts"}); err == nil {
		if st := pc.SendRawSyncBlocking([]byte(line)); st != netcore.SendOK {
			log.Warn().Str("peer", string(address)).Str("status", st.String()).Msg("sync_peer_fetch_contacts_failed")
			return 0
		}
		// Frame-type-filtered read. This is the read whose bare one-line
		// predecessor wedged sender-key recovery for relayed DMs: right
		// after auth_ok the responder replays the recipient's backlog —
		// including the undeliverable DMs that triggered this very dial —
		// and pushes announce baselines, so the first line was virtually
		// never the contacts reply (see syncReplySkipBudget).
		frame, err := readSyncReply(ctx, reader, "contacts", skipBudget, refreshSyncDeadline)
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_contacts_read_failed")
			return 0
		}
		if frame.Type != "contacts" {
			// Terminal "error" frame — the responder refused the fetch.
			log.Warn().Str("peer", string(address)).Str("code", frame.Code).Msg("sync_peer_contacts_rejected")
			return 0
		}
		// Same two-stage admission as the session path, against the SAME
		// per-remote bucket — the reader here is readSyncReply, which accepts
		// maxResponseLineBytes just like the session reader does, so an
		// uncapped loop would be the identical hole on a connection that has no
		// admission ledger at all.
		//
		// The budget is node-scoped and keyed on this connection's endpoint
		// (contact_verify_budget.go), never on the dial: this dial is scheduled
		// by an attacker-supplied `sender` fingerprint, and the three gates
		// around it (per-sender cooldown, per-hop slot,
		// maxConcurrentSenderKeySyncPasses) bound CONCURRENCY, not the total —
		// so a per-dial budget was one the remote could re-buy at will.
		//
		// There is no ledger to score against and no ban surface on an outbound
		// dial (addBanScore keys on the IP of an ACCEPTED connection), so the
		// punishment is the refusal itself: the reply is dropped, the dial
		// returns nothing, and the connection closes.
		budget := s.contactVerifyBudgetFor(contactVerifyKeyFromEndpoint(pc.RemoteAddr(), address))
		report := s.importAdvertisedContacts(budget, frame.Contacts)
		switch report.Outcome {
		case contactImportRefusedCountCap:
			log.Warn().
				Str("peer", string(address)).
				Int("contacts", report.Offered).
				Int("cap", maxContactsPerResponse).
				Msg("sync_peer_contacts_count_cap_exceeded")
			return 0
		case contactImportBudgetExhausted:
			// NOT a violation, and the same disposition the session path
			// makes: the verified prefix is kept — the entries this node paid
			// for are the entries it gets — and the rest arrives on a later
			// pass, once the remote's bucket has refilled. Logged because on
			// THIS path it is also the signal an operator needs to see a
			// recovery flood: a remote that keeps arriving here is one whose
			// dials are being scheduled faster than its budget refills.
			log.Warn().
				Str("peer", string(address)).
				Int("offered", report.Offered).
				Int("verified", report.Verified).
				Int("imported", report.Imported).
				Msg("sync_peer_contact_verification_budget_exhausted")
		}
		imported = report.Imported
	}

	if imported > 0 {
		log.Info().Str("peer", string(address)).Int("imported", imported).Msg("sync_peer_contacts_imported")
	} else {
		log.Warn().Str("peer", string(address)).Msg("sync_peer_no_new_contacts")
	}
	return imported
}

// recordOutboundAuthSuccess is the shared post-auth_ok writer used by
// every outbound path that completes the handshake:
//   - the managed-session path via authenticatePeerSession
//     (passing session.netCore.RemoteAddr()),
//   - the raw/bootstrap push_notice fallback in sendNoticeToPeer,
//   - the legacy fresh-dial path in syncPeer (sender-key recovery,
//     forced refresh; passing pc.RemoteAddr()).
//
// Consolidating all three through one writer prevents convergence
// state from depending on which outbound path happened to reach the
// peer first.
//
// remoteAddr is the "host:port" string as reported by the connection
// wrapper (*netcore.NetCore.RemoteAddr()) — NOT from peerAddress, which
// may carry a hostname for DNS / manual bootstrap peers. The raw TCP
// host/port is extracted here and a canonical (IP, port) pair is fed to
// recordOutboundConfirmed. A hostname reaching TrustedAdvertiseIP would
// silently break the observed-IP downgrade sweep that compares the
// field against canonical IPs from inbound TCP endpoints.
//
// Accepting a string (rather than net.Conn) keeps the helper out of
// the frozen §2.6.26 net.Conn carve-out: this function is not a
// boundary translator, does not create/destroy an (id, conn) binding
// and does not evaluate pre-registration IP policy, so it has no right
// to speak net.Conn in its signature.
//
// Side effects are gated on successfully deriving an IP:port pair.
// If remoteAddr is empty (unit tests, wrapper not yet published), or
// the IP is unparseable, nothing is written — auth itself is still
// considered OK by the caller.
func (s *Service) recordOutboundAuthSuccess(peerAddress domain.PeerAddress, remoteAddr string) {
	if peerAddress == "" || remoteAddr == "" {
		return
	}
	// dialedIP is the canonical IP form derived from the raw RemoteAddr().
	// The legacy misadvertise repay path also kept the raw (non-canonical)
	// form to look the peer up in s.bans, but that path was removed in the
	// v12 cleanup phase, so only the canonical form survives here.
	dialedIP := canonicalIPFromHost(remoteIPFromString(remoteAddr))
	if dialedIP == "" {
		return
	}
	_, dialedPortStr, ok := splitHostPort(remoteAddr)
	if !ok || dialedPortStr == "" {
		return
	}
	// RemoteAddr() strings always carry a decimal port — if the parse
	// fails or the value is out of PeerPort range the call becomes a
	// no-op inside recordOutboundConfirmed, which is the correct
	// behaviour for a malformed transport report.
	dialedPortInt, err := strconv.Atoi(dialedPortStr)
	if err != nil {
		return
	}
	// dialedIP is an already-canonical IP string (canonicalIPFromHost,
	// non-empty checked above), so ParsePeerIP cannot fail; error discarded.
	dialedPeerIP, _ := domain.ParsePeerIP(dialedIP)
	s.recordOutboundConfirmed(peerAddress, dialedPeerIP, domain.PeerPort(dialedPortInt))
	// A successful handshake clears any remote-ban window we had recorded
	// against this peer: the responder just let us in, so the prior
	// peer-banned notice is no longer authoritative. Without this clear,
	// the PeerProvider.RemoteBannedFn gate would keep skipping the peer
	// on subsequent passes even though it is now willing to talk to us —
	// a stale suppression indistinguishable from the ebus storm the ban
	// window was introduced to end.
	//
	// The clear is symmetric with record. A blacklisted notice may touch
	// the per-peer row (offender), the volatile remoteIPBanOffenders set,
	// and — once enough distinct live offenders accumulate — the IP-wide
	// row; peer-ban touches only the per-peer row. Recovery only needs to
	// unwind the two PERSISTED tables (the offender set is volatile and
	// dropped as a side effect of clearRemoteIPBanLocked):
	//   (a) clearRemoteBanLocked(peerAddress) drops THIS peer's own
	//       per-peer record unconditionally — the handshake itself is
	//       direct proof this address accepts us again, regardless of
	//       which reason wrote the record;
	//   (b) clearRemoteIPBanLocked(dialedIP) drops the IP-wide entry so
	//       every sibling behind that egress IP is dialable again.
	// No mirror walk is needed: a reason=blacklisted notice now writes a
	// per-peer row for the OFFENDER that carried it (cleared by (a)
	// above, unconditionally) plus the IP-wide escalation counter — it
	// does NOT write per-peer rows on OTHER siblings, so there is none to
	// keep in sync. Per-peer rows with reason=peer-ban on other siblings
	// are standalone responder decisions on specific addresses and must
	// stay untouched — a handshake with a sibling is not proof the
	// responder has forgiven them. dialedIP is the canonical form used by
	// recordRemoteIPBanLocked, so (b) hashes into the same map key as the
	// original write and also drops the pre-escalation offender set
	// (clearRemoteIPBanLocked).
	// Cross-domain write: peer-domain (persistedMeta via clearRemoteBan)
	// + ipState-domain (remoteBannedIPs via clearRemoteIPBanLocked).
	// Canonical lock order per docs/locking.md: s.peerMu → s.ipStateMu.
	// clearRemoteIPBanLocked takes s.ipStateMu itself below.
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	// Resolve a generated fallback dial address to its canonical primary
	// before clearing the per-peer ban: the ban was recorded against the
	// primary (handlePeerBannedNotice resolves the same way), so clearing
	// the raw fallback — which has no persistedMeta row — would be a no-op
	// and leave a stale primary RemoteBannedUntil that re-suppresses the
	// peer after this session closes. Symmetric with the record side.
	canonical := s.resolveHealthAddress(peerAddress)
	remoteBanCleared := s.clearRemoteBanLocked(canonical)
	s.ipStateMu.Lock()
	// Clear the IP-wide scope under BOTH keys: the resolved TCP IP
	// (dialedIP) and the key the notice was actually recorded under
	// (remoteIPBanKey of the canonical address). For an IP-literal peer
	// these coincide; for a DNS / manual peer the ban is keyed on the
	// hostname, which dialedIP (a numeric IP) would never match — so
	// without the second clear a hostname-keyed IP-wide ban (and its
	// volatile offender bucket) would survive a successful handshake.
	remoteIPBanCleared := s.clearRemoteIPBanLocked(dialedIP)
	if key, ok := remoteIPBanKey(canonical); ok && key != dialedIP {
		if s.clearRemoteIPBanLocked(key) {
			remoteIPBanCleared = true
		}
	}
	s.ipStateMu.Unlock()
	s.peerMu.Unlock()
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	// Mirror handlePeerBannedNotice: mark dirty when any scope mutated so
	// the cleared state survives a crash/restart. The next bootstrapLoop
	// tick coalesces it into a debounced flush (markPeerStateDirty) — do
	// NOT call flushPeerState synchronously here; that O(peers) snapshot
	// per auth is exactly the churn the dirty-flag replaced. Without
	// persisting the clear at all, peers.json would be re-read with stale
	// per-peer RemoteBannedUntil or remote_banned_ips entries and the gate
	// would keep suppressing this peer (and any siblings behind the same
	// IP) until the old window elapses, even though the responder has
	// already accepted us.
	if remoteBanCleared || remoteIPBanCleared {
		s.markPeerStateDirty()
	}
}

func scorePeerTargetLocked(health *peerHealth) int64 {
	stateWeight := int64(0)
	switch health.State {
	case peerStateHealthy:
		stateWeight = 4
	case peerStateDegraded:
		stateWeight = 2
	case peerStateReconnecting:
		stateWeight = 1
	default:
		stateWeight = 0
	}

	lastUseful := health.LastUsefulReceiveAt
	if lastUseful.IsZero() {
		lastUseful = health.LastPongAt
	}
	recency := int64(0)
	if !lastUseful.IsZero() {
		recency = lastUseful.Unix()
	}

	return stateWeight*1_000_000_000_000 + recency - int64(health.ConsecutiveFailures*1000) - int64(len(health.LastError))
}

func (s *Service) learnPeerFromFrame(observedAddr string, frame protocol.Frame) {
	if listenerEnabledFromFrame(frame) {
		// v12 wire contract: the only authoritative port is
		// frame.AdvertisePort; hello.Listen carries no truth (host or
		// port) and must not leak through, even when a legacy / mixed-
		// network fixture still echoes it. Synthesising from observed
		// TCP host + AdvertisePort unconditionally guarantees that a
		// peer behind a NAT port-forward (where the listen port and
		// advertise port differ) is gossipped under the dialable
		// advertise_port, never under the legacy listen port. An empty
		// synthesis result means the observed address is not a
		// parseable host:port (non-IP transport, malformed wrapper
		// output) — in that case we skip the announce-learning side-
		// effects but still fall through to the identity / key
		// material caches below.
		if advertised := synthesiseAdvertisedFromObserved(observedAddr, frame.AdvertisePort); advertised != "" {
			if normalizedAddr, ok := s.normalizePeerAddress(domain.PeerAddress(observedAddr), domain.PeerAddress(advertised)); ok {
				s.promotePeerAddress(normalizedAddr)
				s.rememberPeerType(normalizedAddr, frame.NodeType)
				s.addPeerID(normalizedAddr, domain.PeerIdentityFromWire(frame.Address))
				s.addPeerVersion(normalizedAddr, frame.ClientVersion)
				s.addPeerBuild(normalizedAddr, frame.ClientBuild)
			}
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(domain.PeerIdentityFromWire(frame.Address))
	}
	s.learnWireIdentityKeys(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig)
}

// learnWireIdentityKeys is the single validated ingest point for key
// material carried by handshake frames (hello / welcome). Nothing is
// cached unless it self-certifies:
//
//   - the signing key must be present, within the length cap, and
//     fingerprint-match the address (identity.VerifyPublicKeyFingerprint)
//     — without this gate a hostile (and, on the welcome path,
//     still-unauthenticated) frame could both store multi-megabyte
//     strings under arbitrary addresses AND poison a legitimate
//     sender's cached pubkey with garbage, wedging that sender's DM
//     verification;
//   - the box pair is cached only complete AND binding-valid
//     (VerifyBoxKeyBinding also enforces the 32-byte X25519 size); a
//     partial or unverifiable pair is dropped, never stored half-way.
//
// The historical "store whatever fields arrived when the triple is
// incomplete" behaviour is gone deliberately: every deployed peer sends
// all four identity fields (session auth requires them), so partial
// frames are either ancient or hostile — and neither may write to the
// contact plane. Poisoning an EXISTING entry is impossible through
// here: a fingerprint-matching pubkey is byte-identical to the cached
// one, and a binding-valid box pair requires the sender's private
// signing key.
func (s *Service) learnWireIdentityKeys(address, pubKey, boxKey, boxSig string) {
	if address == "" || pubKey == "" {
		return
	}
	if len(pubKey) > maxAttachedKeyFieldLen ||
		len(boxKey) > maxAttachedKeyFieldLen ||
		len(boxSig) > maxAttachedKeyFieldLen {
		log.Warn().Str("address", address).Msg("wire_identity_keys_oversized_dropped")
		return
	}
	if err := identity.VerifyPublicKeyFingerprint(address, pubKey); err != nil {
		log.Warn().Err(err).Str("address", address).Msg("wire_identity_pubkey_rejected")
		return
	}
	s.addKnownPubKey(address, pubKey)
	if boxKey != "" && boxSig != "" &&
		identity.VerifyBoxKeyBinding(address, pubKey, boxKey, boxSig) == nil {
		s.addKnownBoxKey(address, boxKey)
		s.addKnownBoxSig(address, boxSig)
	}
	s.notifyIdentityKeysImported(address)
}

// peerListenAddress extracts the legacy advertised listen address from a
// hello frame. Returns empty string if the peer does not accept inbound
// connections OR if the peer is following the v12 wire contract that
// removes Listen from the wire entirely.
//
// NOTE: kept as a test helper for parsing legacy frames (mixed-network
// fixtures, regression tests for the pre-v12 wire shape). Production
// code does NOT use this — the announce / gossip path goes through
// observedAnnounceAddressFromHello, which trusts only the observed TCP
// host plus the self-reported advertise_port.
func peerListenAddress(hello protocol.Frame) string {
	if !listenerEnabledFromFrame(hello) {
		return ""
	}
	return strings.TrimSpace(hello.Listen)
}

// observedAnnounceAddressFromHello returns the peer address we should gossip
// to neighbours via announce_peer: observed TCP source host combined with
// the self-reported advertise_port. Under the v12 wire contract this is
// the ONLY truth source — hello.Listen carries no authoritative
// information, neither host nor port, even when a legacy / mixed-network
// fixture still echoes it.
//
// Trust model:
//   - host — taken from the observed TCP remote address, because it is the
//     only host we cryptographically trust (the peer cannot lie about where
//     packets actually come from without hijacking routing).
//   - port — taken from hello.advertise_port (collapsed to
//     config.DefaultPeerPort on absent / invalid wire value). The TCP
//     source port is an ephemeral NAT mapping that no neighbour could
//     dial into, so it is never used. The legacy hello.Listen port is
//     intentionally ignored: if a peer behind a NAT port-forward sets
//     listen=":<bind-port>" but advertise_port=":<external-port>", the
//     announce path must gossip the externally dialable port, never the
//     internal bind port.
//
// Returns ("", false) when:
//   - the peer disabled its listener (Listener="0");
//   - the observed address is empty (unregistered ConnID, post-close read);
//   - the observed address has no usable host part — synthesis from
//     observed host + advertise_port cannot proceed without a host;
//   - normalizePeerAddress rejects the observed host (forbidden range,
//     self-address, ::/::1 unspecified, etc.).
//
// All downstream filtering is delegated to normalizePeerAddress so the
// announce path shares the exact same trust rules as the learn path.
func (s *Service) observedAnnounceAddressFromHello(observedAddr string, hello protocol.Frame) (domain.PeerAddress, bool) {
	if !listenerEnabledFromFrame(hello) {
		return "", false
	}
	if strings.TrimSpace(observedAddr) == "" {
		return "", false
	}
	advertised := synthesiseAdvertisedFromObserved(observedAddr, hello.AdvertisePort)
	if advertised == "" {
		return "", false
	}
	return s.normalizePeerAddress(domain.PeerAddress(observedAddr), domain.PeerAddress(advertised))
}

// synthesiseAdvertisedFromObserved builds a host:port form for the
// peer's advertised endpoint when hello.Listen is empty (the v12 wire
// shape). Host is taken from the verified TCP remote so it survives
// even an attacker that lies on the wire; port is the self-reported
// advertise_port collapsed to DefaultPeerPort on absent/invalid wire
// value, matching extractAdvertisePort's contract. Returns an empty
// string when the observed address is not a host:port pair (non-IP
// transport, malformed RemoteAddr) — the caller treats that as
// "cannot synthesise" and bails out without learning.
func synthesiseAdvertisedFromObserved(observedAddr string, advertisePort domain.PeerPort) string {
	host := remoteIPFromString(observedAddr)
	if host == "" || !isIPHost(host) {
		// remoteIPFromString falls back to the raw input when the
		// host:port split fails, so a malformed observed address like
		// "not-a-host" survives the empty check. Reject anything that
		// is not a parseable IP literal — keying announce / learn
		// state under "not-a-host:64646" would persist garbage.
		return ""
	}
	port := config.DefaultPeerPort
	if advertisePort.IsValid() {
		port = strconv.Itoa(int(advertisePort))
	}
	return net.JoinHostPort(host, port)
}

// announcePeerToSessions sends an announce_peer frame with a single new
// peer address and its node type to every active outbound session.  The
// announcement is non-recursive: recipients learn the address but do not
// relay it further.
func (s *Service) announcePeerToSessions(peerAddress, nodeType string) {
	defer crashlog.DeferRecover()

	log.Trace().Str("peer", peerAddress).Str("node_type", nodeType).Msg("announce_peer_to_sessions_begin")

	log.Trace().Str("peer", peerAddress).Msg("announce_peer_to_sessions_before_rlock")
	s.peerMu.RLock()
	log.Trace().Str("peer", peerAddress).Msg("announce_peer_to_sessions_rlock_acquired")
	sessions := make([]*peerSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.peerMu.RUnlock()
	log.Trace().Str("peer", peerAddress).Int("sessions", len(sessions)).Msg("announce_peer_to_sessions_rlock_released")

	frame := protocol.Frame{
		Type:     "announce_peer",
		Peers:    []string{peerAddress},
		NodeType: nodeType,
	}
	for _, session := range sessions {
		if !s.enqueueSessionSendItem(session, legacyPeerSendItem(frame)) {
			// Refused at the admission — peer gone, sendCh full or queue
			// already fenced. Queue for delivery after the next drain.
			s.queuePeerFrame(session.address, frame)
		}
	}
	log.Debug().Str("peer", peerAddress).Str("node_type", nodeType).Int("sessions", len(sessions)).Msg("announce_peer sent to neighbors")
}

// addPeerMode selects the full semantics applied to a just-added peer.
// The two callers differ on three correlated axes, so a single mode drives
// all of them rather than a bare dial flag:
//
//   - source tag stamped in s.peers / persistedMeta / PeerProvider
//     (Manual is an operator assertion; bootstrap entries must stay
//     Bootstrap or list_peers / peers.json would misreport their origin);
//   - whether the automated penalty state (cooldown, ban, version
//     lockout) is cleared — that is an explicit operator override and
//     must NOT fire for an automatic startup prime;
//   - how the dial is enqueued: immediate ManualPeerRequested (bypasses
//     Candidates() and with it the subnet-diversity gate — /24 for public
//     IPv4, /64 for public IPv6) versus the NewPeersDiscovered hint (CM
//     fills through Candidates() with every gate applied).
type addPeerMode int

const (
	// addPeerModeOperator is an explicit operator add_peer: stamp Source
	// Manual, override automated penalties, and dial immediately past the
	// subnet-diversity gate. Operator intent is authoritative.
	addPeerModeOperator addPeerMode = iota

	// addPeerModeBootstrap is automatic startup priming of compiled/default
	// bootstrap peers: stamp Source Bootstrap, leave penalty state intact,
	// and dial through Candidates() so the subnet-diversity gate applies
	// (a default list may legitimately hold many nodes in one /24).
	addPeerModeBootstrap
)

// operatorOverride reports whether the mode carries operator authority to
// rewrite source and clear automated penalties.
func (m addPeerMode) operatorOverride() bool { return m == addPeerModeOperator }

// peerSource is the source tag this mode stamps on the added peer.
func (m addPeerMode) peerSource() domain.PeerSource {
	if m == addPeerModeBootstrap {
		return domain.PeerSourceBootstrap
	}
	return domain.PeerSourceManual
}

// addPeerFrame handles the operator "add_peer" command (console / RPC).
// Operator intent dials immediately and bypasses candidate filtering.
func (s *Service) addPeerFrame(frame protocol.Frame) protocol.Frame {
	return s.applyAddPeer(frame, addPeerModeOperator)
}

func (s *Service) applyAddPeer(frame protocol.Frame, mode addPeerMode) protocol.Frame {
	if len(frame.Peers) == 0 || strings.TrimSpace(frame.Peers[0]) == "" {
		return protocol.Frame{Type: "error", Error: "address is required"}
	}
	peerSource := mode.peerSource()
	operatorOverride := mode.operatorOverride()
	address := strings.TrimSpace(frame.Peers[0])

	// Ensure host:port format.
	if _, _, ok := splitHostPort(address); !ok {
		address = net.JoinHostPort(address, config.DefaultPeerPort)
	}

	// Apply the same validation as the network peer-exchange path so
	// that manually added peers cannot bypass forbidden-IP, self-address,
	// or unreachable-network checks.
	peerAddress := domain.PeerAddress(address)
	if s.isSelfAddress(peerAddress) {
		return protocol.Frame{Type: "error", Error: "cannot add self as peer"}
	}
	if s.shouldSkipDialAddress(peerAddress) {
		// A forbidden address is normally rejected, but a loopback / RFC1918
		// / ULA LAN target is admitted as a RUNTIME-ONLY dial intent. For an
		// operator add_peer (addPeerModeOperator) the immediate connect flows
		// via EmitSlot(ManualPeerRequested) below; for startup bootstrap
		// priming (addPeerModeBootstrap) it is offered to Candidates() — but
		// shouldSkipPersistedPrivatePeer there keeps private addresses out of
		// auto-selection, so in practice only an operator add actually dials
		// one. Either way these addresses stay excluded from announce
		// (classifyAddress==local guard in the auth path), peer exchange
		// (shouldHidePeerExchangeAddress), and peers.json persistence
		// (buildPeerEntriesLocked) — a private LAN address is never leaked to
		// neighbours. Structurally undialable forbidden addresses (link-local,
		// unspecified, multicast) and non-IP hosts remain rejected. When the
		// node already runs in private-peer mode the address is not forbidden
		// and never reaches this branch.
		host, _, _ := splitHostPort(address)
		if !isManualLocalDialIP(net.ParseIP(host)) {
			return protocol.Frame{Type: "error", Error: fmt.Sprintf("address %s is in a forbidden IP range", address)}
		}
		log.Info().Str("address", address).Str("source", string(peerSource)).Msg("add_peer_local_allowed")
	}
	if !s.canReach(peerAddress) {
		return protocol.Frame{Type: "error", Error: fmt.Sprintf("address %s is in an unreachable network group (%s)", address, classifyAddress(peerAddress))}
	}

	log.Trace().Str("site", "applyAddPeer").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "applyAddPeer").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")

	now := time.Now().UTC()

	// If already known, stamp the mode's source. Operator add_peer also
	// moves the entry to the front (explicit prioritisation); bootstrap
	// priming is automatic and must not reorder the user's queue.
	found := false
	for i, peer := range s.peers {
		if peer.Address == peerAddress {
			s.peers[i].Source = peerSource
			if operatorOverride && i > 0 {
				moved := s.peers[i]
				copy(s.peers[1:i+1], s.peers[:i])
				s.peers[0] = moved
				s.peers[0].Source = peerSource
			}
			found = true
			break
		}
	}

	if !found {
		s.peers = append(s.peers, transport.Peer{})
		copy(s.peers[1:], s.peers[:len(s.peers)-1])
		s.peers[0] = transport.Peer{
			Address: peerAddress,
			Source:  peerSource,
		}
		s.peerTypes[peerAddress] = domain.NodeTypeFull
	}

	// Stamp source — Manual for operator add_peer, Bootstrap for startup
	// priming — whether the peer is new or was previously discovered.
	if pm := s.persistedMeta[peerAddress]; pm != nil {
		pm.Source = peerSource
	} else {
		s.persistedMeta[peerAddress] = &peerEntry{
			Address:  peerAddress,
			NodeType: domain.NodeTypeFull,
			Source:   peerSource,
			AddedAt:  &now,
		}
	}

	// The penalty-clearing block below is an explicit operator override:
	// it resets cooldown/ban/version-lockout so the peer is dialled
	// immediately regardless of prior failures. Automatic bootstrap
	// priming has no such authority — a peer that earned a penalty must
	// keep it across a restart — so the entire block is gated on
	// operatorOverride. Bootstrap relies on the normal Candidates() gates
	// instead.
	if operatorOverride {
		// Reset cooldown, ban, and version lockout so the peer is dialled
		// immediately. A manual add_peer is an explicit operator action that
		// overrides any automated penalty (incompatible protocol, exponential
		// backoff, version lockout).
		if h := s.health[peerAddress]; h != nil {
			resetPeerHealthForRecoveryLocked(h)
		}

		// Clear persisted version lockout — the operator explicitly wants
		// to retry this peer regardless of prior incompatibility evidence.
		// Identity-wide clearing: lockouts are propagated across all addresses
		// of the same identity (see setVersionLockoutLocked), so clearing must
		// also be identity-wide. Otherwise sibling addresses remain suppressed
		// and can keep the lockout-based update signal alive unexpectedly.
		peerID := s.peerIDs[peerAddress]
		if pm := s.persistedMeta[peerAddress]; pm != nil {
			if pm.VersionLockout.IsActive() {
				log.Info().
					Str("peer", string(peerAddress)).
					Str("identity", peerID.String()).
					Str("reason", string(pm.VersionLockout.Reason)).
					Msg("version_lockout_cleared_operator_override")
				pm.VersionLockout = domain.VersionLockoutSnapshot{}
			}
		}
		if !peerID.IsZero() {
			// Remove from the incompatible-reporter dedup set so the
			// operator override also reduces the reporter count.
			// statusMu guards s.versionPolicy (INNERMOST — acquired while
			// peerMu is held per canonical order).
			if s.versionPolicy != nil {
				s.statusMu.Lock()
				delete(s.versionPolicy.incompatibleReporters, peerID)
				s.statusMu.Unlock()
			}
			// Clear lockout, health diagnostics, and dial-suppression state
			// for all sibling addresses of the same identity. Without this,
			// stale ban/cooldown fields keep siblings out of candidate
			// selection even though the operator explicitly overrode the
			// penalty on the primary address.
			//
			// Cross-domain: sibling loop reads peer-domain state
			// (peerIDs, persistedMeta, health) under peerMu and mutates
			// ipState-domain bannedIPSet.  Canonical order
			// peerMu → ipStateMu.
			s.ipStateMu.Lock()
			for otherAddr, otherID := range s.peerIDs {
				if otherAddr == peerAddress || otherID != peerID {
					continue
				}
				if otherEntry, ok := s.persistedMeta[otherAddr]; ok && otherEntry.VersionLockout.IsActive() {
					log.Info().
						Str("peer", string(otherAddr)).
						Str("peer_identity", peerID.String()).
						Str("source_address", string(peerAddress)).
						Msg("version_lockout_cleared_by_identity_on_operator_override")
					otherEntry.VersionLockout = domain.VersionLockoutSnapshot{}
				}
				if siblingHealth := s.health[otherAddr]; siblingHealth != nil {
					resetPeerHealthForRecoveryLocked(siblingHealth)
				}
				// Clear the IP-wide ban for the sibling's IP.
				if ip, _, ok := splitHostPort(string(otherAddr)); ok {
					delete(s.bannedIPSet, ip)
				}
			}
			s.ipStateMu.Unlock()
		}

		// Recompute version policy since we may have removed lockouts and/or
		// a reporter that were contributing to the update_available signal.
		// statusMu is INNERMOST per canonical peerMu → statusMu order.
		s.statusMu.Lock()
		s.recomputeVersionPolicyLocked(time.Now().UTC())
		s.statusMu.Unlock()

		// Also clear the IP-wide ban — without this, buildBannedIPsSet still
		// excludes the peer from Candidates() even though per-address health
		// is unbanned.  Short ipStateMu section nested inside s.peerMu.
		if ip, _, ok := splitHostPort(string(peerAddress)); ok {
			s.ipStateMu.Lock()
			delete(s.bannedIPSet, ip)
			s.ipStateMu.Unlock()
		}
	}

	s.peerMu.Unlock()
	log.Trace().Str("site", "applyAddPeer").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")

	// Mark the freshly added peer (operator or bootstrap) for persistence.
	// The next bootstrapLoop tick coalesces this with any sibling adds into
	// one debounced flush (markPeerStateDirty) — a synchronous flush here
	// made startup priming O(peers^2) in snapshot+marshal+disk cost. The
	// durability window is peerStateDebounceSeconds; a peer lost in that
	// window is re-primed on the next start.
	s.markPeerStateDirty()

	// Register in PeerProvider so the CM can pick it up as a candidate.
	// Operator add_peer uses Promote (re-stamps Source=Manual and refreshes
	// AddedAt); bootstrap priming uses Add with Source=Bootstrap so it does
	// not overwrite the origin tag with an operator assertion it never made.
	// Neither call reorders Candidates() — operator dial priority comes from
	// the ManualPeerRequested bypass below, not from this registration.
	if s.peerProvider != nil {
		if operatorOverride {
			s.peerProvider.Promote(peerAddress, domain.PeerSourceManual)
		} else {
			s.peerProvider.Add(peerAddress, domain.PeerSourceBootstrap)
		}
	}
	// Enqueue the dial according to caller intent.
	if s.connManager != nil {
		switch mode {
		case addPeerModeOperator:
			// ManualPeerRequested creates a slot directly, bypassing the
			// Candidates() round-trip (and every gate in it, including
			// subnet diversity) that NewPeersDiscovered would use. Uses
			// EmitSlot (blocking) to guarantee delivery.
			//
			// Build the same primary+fallback dial address list that
			// Candidates() would produce. Without this, a manual add of a
			// non-default-port peer (e.g. 1.2.3.4:7777) would never attempt
			// the standard fallback port (1.2.3.4:64646), making manual
			// recovery strictly weaker than ordinary candidate dialing.
			dialAddrs := []domain.PeerAddress{peerAddress}
			if s.peerProvider != nil {
				dialAddrs = s.peerProvider.BuildDialAddresses(peerAddress)
			}
			s.connManager.EmitSlot(ManualPeerRequested{
				Address:       peerAddress,
				DialAddresses: dialAddrs,
			})
		case addPeerModeBootstrap:
			// Hint-only: the next fill() picks the peer up through
			// Candidates() with full gating. Pre-bootstrap the hint is
			// dropped by design — NotifyBootstrapReady triggers the first
			// fill() right after startup priming completes, so the peer
			// is still picked up without a special case.
			s.connManager.EmitHint(NewPeersDiscovered{Count: 1})
		}
	}

	action := "added"
	if found {
		action = "already known"
		if operatorOverride {
			action = "already known, moved to front"
		}
	}
	log.Info().Str("address", address).Str("network", classifyAddress(peerAddress).String()).Str("action", action).Str("source", string(peerSource)).Msg("add_peer")

	return protocol.Frame{
		Type:   "ok",
		Peers:  []string{address},
		Status: fmt.Sprintf("peer %s %s (network: %s)", address, action, classifyAddress(domain.PeerAddress(address))),
	}
}

// applyStartupBootstrapPeer adds a compiled/default bootstrap peer through
// the shared add-peer body in addPeerModeBootstrap. That mode reuses the
// add-peer bookkeeping (validation, s.peers / persistedMeta / PeerProvider
// registration, persistence) but withholds every operator-only behaviour:
// the peer is stamped Source=Bootstrap (not Manual), its automated penalties
// are left intact, and the dial goes through Candidates() so the
// subnet-diversity gate applies. Without the mode split a default peer list
// with several nodes in one /24 would open that many immediate outbound
// dials, and bootstrap entries would masquerade as operator-added peers in
// list_peers / peers.json while silently clearing their own penalties.
func (s *Service) applyStartupBootstrapPeer(address string) {
	address = strings.TrimSpace(address)
	if address == "" {
		return
	}

	frame := s.applyAddPeer(protocol.Frame{Type: "add_peer", Peers: []string{address}}, addPeerModeBootstrap)
	if frame.Type == "error" {
		log.Debug().Str("address", address).Str("error", frame.Error).Msg("startup bootstrap peer skipped")
		return
	}
}

// PrimeBootstrapPeers applies compiled/default bootstrap peers once at startup.
// Injection happens later in Run(), after ConnectionManager is ready, so the
// shared add-peer path can register the peers and emit a NewPeersDiscovered
// hint (addPeerModeBootstrap) instead of being called before the CM exists.
func (s *Service) PrimeBootstrapPeers() {
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.primeBootstrapOnRun = true
	s.peerMu.Unlock()
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_released").Msg("peer_mu_writer")
}

// primeStartupBootstrapPeers applies compiled/default bootstrap peers once the
// ConnectionManager is running. Peers already restored from persisted state
// are left untouched; bootstrap-only entries reuse the add-peer bookkeeping
// (validation, registration, persistence) but in addPeerModeBootstrap — they
// keep Source=Bootstrap, retain any automated penalty (no health/ban reset),
// and dial through the regular candidate path. See applyStartupBootstrapPeer
// for why the operator-only ManualPeerRequested bypass must not apply here.
func (s *Service) primeStartupBootstrapPeers() {
	// Snapshot the "already restored from persisted state" decision for the
	// WHOLE list BEFORE applying any peer. applyStartupBootstrapPeer ->
	// applyAddPeer writes a persistedMeta row for the peer it adds (and
	// flushPeerState persists the rest), so reading persistedMeta inside the
	// loop would let the first applied peer flip the skip condition for every
	// later bootstrap address — they would look restoredFromState and be
	// dropped. Computing the set up front keeps the decision stable across
	// the mutations the loop itself causes.
	restored := make(map[domain.PeerAddress]struct{}, len(s.cfg.BootstrapPeers))
	s.peerMu.RLock()
	for _, address := range s.cfg.BootstrapPeers {
		peerAddress := domain.PeerAddress(strings.TrimSpace(address))
		if peerAddress == "" {
			continue
		}
		if _, ok := s.persistedMeta[peerAddress]; ok {
			restored[peerAddress] = struct{}{}
		}
	}
	s.peerMu.RUnlock()

	for _, address := range s.cfg.BootstrapPeers {
		peerAddress := domain.PeerAddress(strings.TrimSpace(address))
		if peerAddress == "" {
			continue
		}
		if _, ok := restored[peerAddress]; ok {
			continue
		}

		s.applyStartupBootstrapPeer(string(peerAddress))
	}
}

// addPeerAddress stores a peer-exchange-discovered address. Returns true only
// when a brand-new address was actually appended to s.peers (first time seen,
// not a self address, not a local/blocked destination, not collapsed onto an
// existing same-IP entry). Observability callers (peer_exchange_executed log)
// rely on this signal to report peers actually imported rather than raw
// response size — see docs/peer-discovery-conditional-get-peers.ru.md Step 6.
func (s *Service) addPeerAddress(address domain.PeerAddress, nodeType string, peerID domain.PeerIdentity) bool {
	if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
		return false
	}

	log.Trace().Str("site", "addPeerAddress").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerAddress").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	if existing, exists := s.findKnownPeerByIPLocked(address); exists {
		if existing == address {
			// Peer already known — do not overwrite its node type.
			// The type was set from a trusted self-report or local source
			// (bootstrap config, manual add, direct hello/welcome). Allowing
			// any network-discovered path to retag it would let senders
			// downgrade a "full" peer to "client" and break routing.
			if !peerID.IsZero() {
				s.peerIDs[address] = peerID
			}
			s.peerMu.Unlock()
			log.Trace().Str("site", "addPeerAddress").Str("phase", "lock_released_exists").Str("address", string(address)).Msg("peer_mu_writer")
			return false
		}
		// Peer exchange keeps only one stored address per IP. Alternative
		// ports learned from the network are ignored to avoid peer-list
		// poisoning via many addresses on the same host.
		s.peerMu.Unlock()
		log.Trace().Str("site", "addPeerAddress").Str("phase", "lock_released_sameIP").Str("address", string(address)).Msg("peer_mu_writer")
		return false
	}

	s.peers = append(s.peers, transport.Peer{
		Address: address,
		Source:  domain.PeerSourcePeerExchange,
	})
	if peerType, ok := domain.ParseNodeType(nodeType); ok {
		s.peerTypes[address] = peerType
	}
	if !peerID.IsZero() {
		s.peerIDs[address] = peerID
	}
	// Eagerly populate persistedMeta so that AddedAt is available for
	// eviction decisions immediately, without waiting for a flush cycle.
	if _, ok := s.persistedMeta[address]; !ok {
		now := time.Now().UTC()
		s.persistedMeta[address] = &peerEntry{
			Address:  address,
			NodeType: parseKnownPeerNodeType(nodeType),
			Source:   domain.PeerSourcePeerExchange,
			AddedAt:  &now,
		}
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "addPeerAddress").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	if s.peerProvider != nil {
		s.peerProvider.Add(address, domain.PeerSourcePeerExchange)
	}
	return true
}

// promotePeerAddress learns a network-discovered peer address without
// trusting third-party metadata such as node type. Freshness boosting is
// applied only on the first successful insertion.
func (s *Service) promotePeerAddress(address domain.PeerAddress) {
	if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
		return
	}

	shouldMarkFresh := false
	log.Trace().Str("site", "promotePeerAddress").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "promotePeerAddress").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	existing, found := s.findKnownPeerByIPLocked(address)
	if found && existing != address {
		// Network-learned/promoted peers keep a single stored address per IP.
		// Manual/bootstrap paths are intentionally exempt and do not call here.
		s.peerMu.Unlock()
		log.Trace().Str("site", "promotePeerAddress").Str("phase", "lock_released_sameIP").Str("address", string(address)).Msg("peer_mu_writer")
		return
	}

	if !found {
		now := time.Now().UTC()
		s.peers = append(s.peers, transport.Peer{
			Address: address,
			Source:  domain.PeerSourceAnnounce,
		})
		if _, ok := s.persistedMeta[address]; !ok {
			s.persistedMeta[address] = &peerEntry{
				Address:  address,
				NodeType: domain.NodeTypeUnknown,
				Source:   domain.PeerSourceAnnounce,
				AddedAt:  &now,
			}
		}
		shouldMarkFresh = true
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "promotePeerAddress").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	if s.peerProvider != nil {
		s.peerProvider.Add(address, domain.PeerSourceAnnounce)
		if shouldMarkFresh {
			s.peerProvider.MarkFresh(address, freshPeerTTL)
		}
	}
}

func (s *Service) findKnownPeerByIPLocked(address domain.PeerAddress) (domain.PeerAddress, bool) {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return "", false
	}
	for _, peer := range s.peers {
		peerHost, _, ok := splitHostPort(string(peer.Address))
		if ok && peerHost == host {
			return peer.Address, true
		}
	}
	return "", false
}

// addPeerID associates a peer identity (fingerprint) with a dial address.
// Safe to call multiple times; empty values are ignored.
func (s *Service) addPeerID(address domain.PeerAddress, peerID domain.PeerIdentity) {
	if address == "" || peerID.IsZero() {
		return
	}
	log.Trace().Str("site", "addPeerID").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerID").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	if s.peerIDs[address] != peerID {
		s.peerIDs[address] = peerID
		if persisted := s.persistedMeta[address]; persisted != nil {
			persisted.Identity = peerID
		}
		// The binding now belongs to peers.json v3. Let bootstrapLoop
		// debounce the full snapshot/write instead of waiting for the periodic
		// catch-all; reconnects with the same identity do not re-mark it.
		s.peerStateDirty = true
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "addPeerID").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
}

func (s *Service) addPeerVersion(address domain.PeerAddress, clientVersion string) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	clientVersion = strings.TrimSpace(clientVersion)
	if address == "" || clientVersion == "" {
		return
	}

	log.Trace().Str("site", "addPeerVersion").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerVersion").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerVersions[address] = clientVersion
	s.peerMu.Unlock()
	log.Trace().Str("site", "addPeerVersion").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
}

func (s *Service) addPeerBuild(address domain.PeerAddress, build int) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	if address == "" || build == 0 {
		return
	}

	log.Trace().Str("site", "addPeerBuild").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerBuild").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerBuilds[address] = build
	s.peerMu.Unlock()
	log.Trace().Str("site", "addPeerBuild").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
}

func parseKnownPeerNodeType(raw string) domain.NodeType {
	if t, ok := domain.ParseNodeType(raw); ok {
		return t
	}
	return domain.NodeTypeUnknown
}

func (s *Service) rememberPeerType(address domain.PeerAddress, raw string) {
	peerType, ok := domain.ParseNodeType(raw)
	if !ok {
		return
	}
	log.Trace().Str("site", "rememberPeerType").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "rememberPeerType").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerTypes[address] = peerType
	s.peerMu.Unlock()
	log.Trace().Str("site", "rememberPeerType").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
}

func (s *Service) peerTypeForAddress(address domain.PeerAddress) domain.NodeType {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.peerTypeForAddressLocked(address)
}

func (s *Service) peerTypeForAddressLocked(address domain.PeerAddress) domain.NodeType {
	if peerType, ok := s.peerTypes[address]; ok {
		return peerType
	}
	// Unknown is the safe default for network-learned peers without a
	// direct self-report. Callers that want to exclude non-relay peers
	// should check IsClient(); treating unknown as full would silently
	// trust third-party gossip.
	return domain.NodeTypeUnknown
}

func (s *Service) peerIsClientNode(address domain.PeerAddress) bool {
	return s.peerTypeForAddress(address).IsClient()
}

func (s *Service) readPeerSession(reader *bufio.Reader, session *peerSession) {
	defer crashlog.DeferRecover()
	for {
		// The RAW per-neighbour budget is applied AROUND this read, not after
		// it: readAdmittedSessionLine stops at protocol.MaxFrameLine unless the
		// line's claim, its entitlement and the neighbour's remaining budget
		// have earned more, and charges the bytes and the frame before anything
		// is classified or parsed. Reading first and judging afterwards — what
		// this loop used to do with a flat maxResponseLineBytes — meant eight
		// megabytes were read and copied before the node could say it never
		// wanted them.
		//
		// ONE type is returned UNCHARGED: a datagram is metered by the §5
		// per-neighbour budget of its own plane instead, charged on these same
		// bytes at the diversion below. The two budgets replace each other rather
		// than stack, so a datagram stream cannot empty the bucket the other
		// planes on this session are paying from.
		line, err := s.readAdmittedSessionLine(reader, session)
		if errors.Is(err, errPeerSessionLineRefused) {
			// One line consumed and dropped on admission grounds. The violation
			// is already recorded and logged; the session survives, because a
			// single violation must cost the peer a frame and not a reconnect.
			continue
		}
		if err != nil {
			if err == io.EOF {
				log.Debug().Str("peer", string(session.address)).
					Msg("peer_session_read: remote closed connection (EOF)")
			} else if errors.Is(err, errFrameTooLarge) {
				// Capture frame_too_large diagnostic event.
				s.captureOutboundRecvFrameTooLarge(session.connID)
				log.Debug().Str("peer", string(session.address)).
					Msg("peer_session_read: frame exceeds max response size")
			} else {
				log.Debug().Err(err).Str("peer", string(session.address)).
					Msg("peer_session_read: read error")
			}
			select {
			case session.errCh <- err:
			default:
			}
			return
		}

		// Capture tap: record raw outbound-session recv line before
		// parsing (plan §7.2). Strip only the transport newline — leading
		// whitespace is part of the wire payload for diagnostics.
		s.captureOutboundRecv(session.connID, strings.TrimRight(line, "\r\n"))

		// ADMISSION FIRST (§4.1 step 1). The line is classified by
		// classifyFrameLine — a single bounded scan that never builds a JSON
		// value — and BOTH a line that has not earned the wide
		// maxResponseLineBytes budget and a line whose type the scan cannot
		// resolve are refused BEFORE protocol.ParseFrameLine gets to copy and
		// decode it. The order is the whole point on this reader: it accepts up
		// to 8 MiB, so a gate that only ran below the parser would let a peer
		// make this node unmarshal eight megabytes of JSON per frame and only
		// then be told the frame was never admissible.
		dropped, fatal := s.refuseUnadmissibleFrameLine(session, line)
		if fatal != nil {
			select {
			case session.errCh <- fatal:
			default:
			}
			return
		}
		if dropped {
			continue
		}

		// A DATAGRAM NEVER REACHES protocol.ParseFrameLine. §4.1 step 1 charges
		// the neighbour's byte and frame budget "before any decoding", and the
		// universal parser is decoding: running it first let a neighbour impose
		// a full JSON unmarshal of every datagram-shaped line for free, and the
		// layer only found out afterwards. Diverting here — the same shape
		// file_command already uses below — puts the budget first and skips the
		// universal parse entirely, because the strict parser of §3.4 has to
		// read the ORIGINAL bytes anyway.
		//
		// This is also the ONLY meter such a line meets on this reader: the
		// response-plane budget above admitted it without charging, on the same
		// classification this predicate answers from
		// (sessionDatagramPaysItsOwnBudget), so the §5 charge inside the ingress
		// is what makes the line cost its sender anything at all. The two
		// predicates are one call for exactly that reason.
		if isDatagramWireLine(line) {
			s.dispatchSessionDatagramLine(session, line)
			continue
		}

		trimmed := strings.TrimSpace(line)
		frame, err := protocol.ParseFrameLine(trimmed)
		if err != nil {
			continue
		}

		// The authoritative half of the budget gate. The pre-parse scan
		// declines to classify a line that names its type ambiguously, so such
		// a line reaches this point with the strict budget unapplied; here the
		// type is the parsed one and no classification trick survives.
		if s.refuseOversizeFrameLine(session.address, frame.Type, line) {
			continue
		}

		// RawLine bypass preservation. protocol.ParseFrameLine does not
		// populate Frame.RawLine, but Phase 2+ / Phase 4 frames
		// (route_sync_digest_v1, route_sync_summary_v1,
		// route_announce_v3, route_poison_v1) keep their wire payload
		// out of the universal Frame struct and re-parse it from
		// Frame.RawLine in the dispatcher. Without this assignment the
		// dispatch calls Unmarshal*Frame([]byte("")) and silently drops
		// the frame, which on the outbound session path means
		// route_announce_v3 / route_poison_v1 effectively disappear in
		// one direction of the wire. See isRawLineBackedFrameType for
		// the explicit list of types that use the bypass.
		if isRawLineBackedFrameType(frame.Type) {
			frame.RawLine = rawLineForDispatch(frame.Type, line, trimmed)
		}

		// file_command frames use their own wire format (FileCommandFrame)
		// and require the raw JSON for decryption and routing. Dispatch them
		// directly to the file router instead of going through the
		// inboxCh → dispatchPeerSessionFrame path, which only has access to
		// the parsed protocol.Frame (missing src/dst/payload fields).
		//
		// The gate reads the capabilities of THIS session — the reader owns the
		// object the line came off, and an address-keyed lookup would answer
		// about a reconnect's replacement session instead (sessionHasCapability).
		//
		// The type is compared against the constant, not a literal, because the
		// budget one gate earlier decides the file plane's exemption from the
		// SAME name (sessionFileCommandIsAdmissible): a line exempted there and
		// not dispatched here would be metered by nobody.
		if frame.Type == protocol.FileCommandFrameType {
			s.markPeerRead(session.address, frame)
			s.markPeerUsefulReceive(session.address)
			if s.sessionHasCapability(session, domain.CapFileTransferV1) {
				// Outbound session carries the peer identity directly;
				// pass it so the file router can split-horizon forward
				// and never reflect the frame back to this same peer.
				// Read through the accessor for the same ordering reason
				// the gate is: this loop is running before the handshake
				// writes the field.
				s.handleFileCommandFrame(json.RawMessage(trimmed), s.sessionPeerIdentity(session))
			}
			continue
		}

		select {
		case session.inboxCh <- frame:
		default:
			select {
			case session.errCh <- fmt.Errorf("%w for %s", errPeerSessionInboxOverflow, session.address):
			default:
			}
			return
		}
	}
}

// refuseUnadmissibleFrameLine is admission (§4.1 step 1) on the peer-session
// reader: it runs on the CLASSIFICATION, before anything is parsed, and reports
// whether the line was dropped.
//
// See admitFrameLinePreParse for the rule. The two refusals are separate calls
// rather than one because they are different facts about the neighbour and an
// operator has to be able to tell them apart: one peer sent a frame too large
// for its type, the other sent a line whose type cannot be read at all.
// It reports the drop and, separately, the error that ENDS the session: a
// refusal is one dropped frame, but a neighbour that keeps producing them has
// discovered that violations are free, and the second return value is where
// that stops (peerSessionViolationBudget).
//
// Both refusals here stay scored, and neither carries the datagram carve-out —
// deliberately, and for different reasons:
//
//   - an AMBIGUOUS line names no plane at all (§3.4 refuses a duplicate
//     top-level key on this one outright), so it is not a frame of the datagram
//     plane whose rules could apply to it;
//   - the OVER-BUDGET verdict is unreachable for a datagram. The wide response
//     budget is bought at the read, from the line's first bytes, and `datagram`
//     is not a type that can buy it (hasWideFrameLineBudget) — so a line this
//     branch sees as a datagram would have had to be stopped mid-read, where
//     refuseOverBudgetSessionLine applies the plane's own silent-drop rule. What
//     reaches here is a line that bought the wide budget under another name, and
//     that decoy is the sender's own doing.
func (s *Service) refuseUnadmissibleFrameLine(session *peerSession, line string) (bool, error) {
	claimed, verdict := admitFrameLinePreParse(line)
	switch verdict {
	case preParseRefuseAmbiguous:
		// The dial address, not session.peerIdentity: it is the key this
		// direction can defend (datagram.AdmissionKeySpace), and it is also the
		// one field of the session this reader may read without peerMu — it is
		// assigned in the struct literal and never written again, while
		// peerIdentity is written by the handshake goroutine after this loop has
		// already started.
		s.dropAmbiguousFrameLine(datagram.DialedAddressKey(session.address), string(session.address), line)
		return true, sessionAdmissionFatal(s.punishSessionAdmission(session, "frame_line_ambiguous", claimed, wireLineBudget(line)))
	case preParseRefuseOverBudget:
		s.dropOversizeFrameLine(session.address, oversizeRefusalAttribution(claimed, line), line)
		return true, sessionAdmissionFatal(s.punishSessionAdmission(session, "strict_budget_exceeded", claimed, wireLineBudget(line)))
	}
	// A reply nobody asked for is refused here rather than one gate earlier,
	// because at this point the type is the CLASSIFIED one and the check costs
	// nothing: the classification has already been computed for the budget.
	if s.refuseUnsolicitedReplyLine(session, claimed) {
		return true, nil
	}
	// preParseAdmit, and every verdict a future revision forgets to handle:
	// falling through to the refusals would be the safer default only if the
	// classification could not admit, and it can.
	return false, nil
}

// sessionAdmissionFatal maps the verdict of punishSessionAdmission to the
// "session is over" half of the caller's contract: the ordinary drop sentinel
// is not an error to propagate, everything else is.
func sessionAdmissionFatal(err error) error {
	if errors.Is(err, errPeerSessionLineRefused) {
		return nil
	}
	return err
}

// oversizeRefusalAttribution picks the type name an oversize refusal is
// REPORTED under.
//
// The DECISION belongs to the classification, which is why it is not this
// function's business; the ATTRIBUTION is best-effort and falls back to
// peekFrameType, because a line refused without ever naming itself still has to
// land on the drop counter of the plane it CLAIMED to belong to, or §10's
// "dropped by reason" ledger loses exactly the refusals the widest reader on
// this node produces.
func oversizeRefusalAttribution(claimed, line string) string {
	if claimed != "" {
		return claimed
	}
	return peekFrameType(line)
}

// refuseOversizeFrameLine is the AUTHORITATIVE half: a peer-session line that
// breaches the budget for the type it really parsed as.
//
// The refusal is silent on the wire and does NOT tear the session down:
// announce_routes / routes_update / request_resync and `datagram` are all
// dispatched through the same code path the remote's inbound TCP plane uses,
// so any peer must respect MaxFrameLine for them, even though this reader
// itself accepts up to maxResponseLineBytes (8 MiB). Without the gate a buggy
// or hostile peer pushes a multi-megabyte announce_routes frame through the
// wider response-plane reader, has it accepted into the local routing table,
// and then watches our own size-aware sender silently drop the same route on
// re-announce — sender and receiver diverging on which routes the peer "knows
// about". For a datagram the same hole would make "a frame is smaller than
// 128 KiB" stop being a property of reception (§2.3).
//
// It is DEFENCE IN DEPTH now rather than a second decision: admission refuses
// every oversize line whose type is not frameLineNamed-and-entitled, and a
// frameLineNamed line yields the same type to protocol.ParseFrameLine by
// construction. It stays because the equivalence is an argument about two
// scanners, and a gate that costs one integer comparison is the cheapest place
// to keep that argument honest.
func (s *Service) refuseOversizeFrameLine(address domain.PeerAddress, frameType, line string) bool {
	if !exceedsStrictFrameLineBudget(frameType, line) {
		return false
	}
	return s.dropOversizeFrameLine(address, frameType, line)
}

// dropOversizeFrameLine logs and counts one refused line. Both gates funnel
// through it so a violation is reported identically whichever of them caught
// it — two log shapes for one drop reason is how a "dropped by reason" ledger
// stops adding up.
func (s *Service) dropOversizeFrameLine(address domain.PeerAddress, frameType, line string) bool {
	log.Warn().
		Str("peer", string(address)).
		Str("type", frameType).
		Int("size", wireLineBudget(line)).
		Int("limit", protocol.MaxFrameLine).
		Msg("strict_frame_budget_frame_too_large_dropped")
	s.countOversizeDatagramRefusal(frameType)
	return true
}

// dropAmbiguousFrameLine refuses a line whose type this node cannot resolve
// without parsing it, and charges the neighbour for the bytes it made this node
// scan. It always reports true — the line is dropped, whichever way the charge
// went.
//
// # Why the neighbour is charged for a refusal
//
// §4.1 step 1 puts admission before any decoding, and an unmetered refusal is a
// free load channel: the cheapest verdict this reader has would also be its only
// uncharged one, so a peer would hold the node at line rate for nothing by
// sending garbage that refuses to name itself. The same argument is already
// recorded on the closed-barrier branch of handleDatagramFrame, which charges
// before refusing for exactly this reason. The charge decides nothing here —
// the line is refused either way — it only makes the refusal cost the sender
// what it cost the receiver.
//
// # Why the DATAGRAM budget is the one charged
//
// Because it is the only per-NEIGHBOUR byte budget this node has (§5): the
// announce plane counts routes, the command limiter counts frames per socket and
// does not run on this reader at all. Charging it is not a claim that the line
// was a datagram — it cannot be, since the whole point is that nothing can say
// what it was — it is the node billing the neighbour that sent the bytes. With no
// layer there is nothing to charge, and the line is simply dropped; that is
// strictly what happened before this gate existed.
//
// # Why a KEY and not an identity
//
// Because this helper serves both readers, and the two prove different things
// about the neighbour: the key is what each of them can defend (see
// datagram.AdmissionKeySpace). Taking the key rather than deriving it here also
// keeps the charge on THIS bucket identical to the one the ingress takes, which
// is the only way one neighbour cannot end up with two budgets.
//
// # How it is counted
//
// Through countAmbiguousDatagramRefusal, on the same best-effort attribution
// every other pre-parse refusal on this reader uses: a line the peek calls a
// datagram lands on DropMalformed, which is the verdict the strict parser of
// §3.4 would have reached one step later for a duplicate top-level key. The
// Warn line carries the peeked type for every other case and says plainly that
// it is a hint.
func (s *Service) dropAmbiguousFrameLine(budgetKey datagram.AdmissionKey, peerLabel, line string) bool {
	if layer := s.datagramLayer(); layer != nil {
		// The verdict is ignored deliberately: over budget or inside it, the
		// line is refused. What the call is here for is the charge. A zero key
		// is refused inside Admit — nobody to bill, nothing charged.
		_ = layer.admission.Admit(budgetKey, len(line))
	}
	s.countAmbiguousDatagramRefusal(line)
	log.Warn().
		Str("peer", peerLabel).
		Str("peeked_type_hint", peekFrameType(line)).
		Int("size", wireLineBudget(line)).
		Msg("frame_line_type_ambiguous_dropped")
	return true
}

// errPeerSessionInboxOverflow marks a session teardown initiated by the
// LOCAL node: the per-session inbox channel saturated while the serve
// loop was busy (typically blocked inside a synchronous
// peerSessionRequest against a slow socket). It is a local
// slow-consumer eviction, not evidence of peer instability —
// sessionCloseCauseFromError matches against this sentinel via
// errors.Is to keep such teardowns out of the peer's disconnect_storm
// quarantine accounting.
var errPeerSessionInboxOverflow = errors.New("peer session inbox overflow")

// isUnsolicitedSessionFrame classifies frames that may legitimately
// land on an outbound session at ANY moment, independent of any
// request/reply in flight: the fire-and-forget classes plus
// push_delivery_receipt and announce_peer (dispatcher-handled but not
// part of isFireAndForgetFrame's SEND-side contract). The request-wait
// loop dispatches these before its reply match — see the ordering
// comment there.
func isUnsolicitedSessionFrame(frameType string) bool {
	return isFireAndForgetFrame(frameType) ||
		frameType == "push_delivery_receipt" ||
		frameType == "announce_peer"
}

func (s *Service) peerSessionRequest(session *peerSession, frame protocol.Frame, expectedType string, hello bool) (protocol.Frame, error) {
	// All outbound writes route through the managed single-writer path on
	// NetCore so that deadline, back-pressure and ordering match inbound.
	// SendRawSyncBlocking blocks until the writer goroutine flushes the
	// bytes to the socket, preserving the "write completed before we wait
	// for reply" contract the request loop relies on.
	if session.netCore == nil {
		return protocol.Frame{}, fmt.Errorf("peerSessionRequest: outbound session missing NetCore")
	}
	if expectedType != "" {
		// The ONLY record that a reply is outstanding, and it has to be shared
		// state rather than a local: the reader goroutine — not this one — is
		// what decides whether an arriving `contacts` is a reply it may spend
		// eight megabytes on or an unsolicited frame it must refuse before
		// reading (grantFrameLineExtension). Registered BEFORE the write, so a
		// reply that overtakes the return of SendRawSyncBlocking still finds it.
		defer session.admission.expectReply(expectedType)()
	}
	var payload []byte
	if hello {
		payload = []byte(s.nodeHelloJSONLine())
		s.markPeerWrite(session.address, protocol.Frame{Type: "hello"})
	} else {
		// Peer-session write: receiver dispatches through readPeerSession
		// bound by maxResponseLineBytes (8 MiB) — wider than the inbound
		// command-plane budget because response frames legally batch
		// many DM bodies (contacts, messages, inbox). Use the matching
		// MaxResponseLine budget so legitimate batched responses pass
		// while still rejecting genuinely oversize frames as a self-bug.
		line, err := protocol.MarshalFrameLineWithLimit(frame, protocol.MaxResponseLine)
		if err != nil {
			return protocol.Frame{}, fmt.Errorf("peerSessionRequest: %w", err)
		}
		payload = []byte(line)
		s.markPeerWrite(session.address, frame)
	}
	// Outbound control-plane: block on full queue rather than fast-fail,
	// so relay traffic backlog cannot starve handshake /
	// heartbeat writes. Inbound error paths keep the fast-fail sendRawSync
	// contract via enqueueFrameSync.
	if st := session.netCore.SendRawSyncBlocking(payload); st != netcore.SendOK {
		return protocol.Frame{}, fmt.Errorf("peerSessionRequest: send failed: %s", st.String())
	}

	// Use a longer read deadline for ping so that the heartbeat timeout
	// (pongStallTimeout) governs stall detection instead of the generic
	// peerRequestTimeout.
	readTimeout := peerRequestTimeout
	if frame.Type == "ping" {
		readTimeout = pongStallTimeout
	}
	_ = session.conn.SetReadDeadline(time.Now().Add(readTimeout))

	for {
		select {
		case err := <-session.errCh:
			return protocol.Frame{}, err
		case incoming := <-session.inboxCh:
			s.markPeerRead(session.address, incoming)
			if incoming.Type == protocol.FrameTypeConnectionNotice {
				// Advertise convergence feedback from the inbound side.
				// The peer is closing the connection immediately after
				// sending this notice (Status="closing"), so we record
				// the observed IP and surface the sentinel error so the
				// caller tears down the session.
				//
				// NoticeErrorFromFrame (NOT ErrorFromCode) is used here
				// because connection_notice{peer-banned} carries a
				// details.reason that discriminates how the caller must
				// react: `self-identity` MUST route through
				// applySelfIdentityCooldown (24h suppression) via the
				// shared tryApplySelfIdentityCooldown helper consulted
				// by every outbound failure hook (onCMDialFailed,
				// runPeerSession), while `peer-ban` / `blacklisted` go
				// through the standard peer-ban cooldown path. Collapsing
				// every reason to ErrPeerBanned (what ErrorFromCode would
				// do) defeats the whole point of detecting self-loopback
				// on the wire — the dialler would re-enter the churn loop
				// this notice was explicitly designed to break.
				s.handleConnectionNotice(session.address, incoming)
				return protocol.Frame{}, protocol.NoticeErrorFromFrame(incoming)
			}
			if incoming.Type == "error" {
				return protocol.Frame{}, protocol.ErrorFromCode(incoming.Code)
			}
			if incoming.Type == "ping" {
				pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
				// Route through the injected Network surface (visible to
				// netcoretest.Backend); on ErrUnknownConn the helper falls
				// back to session.netCore via enqueueSessionFrame, which is
				// the carve-out for a live session whose registry entry
				// has been reaped or was never populated.
				// writerLoop applies its own per-write deadline based on
				// Direction, so no manual SetWriteDeadline is needed.
				_ = s.sendSessionFrameViaNetwork(s.runCtx, session, pongFrame)
				s.markPeerWrite(session.address, pongFrame)
				continue
			}
			// Unsolicited traffic FIRST — before the reply match. The
			// order is load-bearing: requests issued with
			// expectedType == "" (send_message / publish_notice /
			// ack_delete — expectedReplyType returns "" for every
			// type) accept "the next reply-class frame" as their
			// answer, and checking the reply arm first would let the
			// first stray relay_message or receipt be returned to the
			// caller as that "reply" and vanish. Classification-based
			// dispatch keeps every unsolicited type flowing to its
			// real handler: the historical per-type allowlist silently
			// dropped whatever it did not enumerate (relay_message —
			// burning one bounded upstream retry per occurrence, with
			// a 12s recovery sync in flight enough to exhaust the
			// budget and lose the DM; relay_delivery_receipt; the
			// announce-plane trio; poison frames...). Dispatch from
			// inside the request wait is reentrancy-safe: handlers
			// only enqueue onto sendCh / other sessions, and the
			// recovery trigger is asynchronous — nothing here reads
			// this session's inboxCh. (file_command never reaches this
			// loop — readPeerSession diverts it before inboxCh.)
			if isUnsolicitedSessionFrame(incoming.Type) {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if expectedType == "" || incoming.Type == expectedType {
				_ = session.conn.SetReadDeadline(time.Time{})
				return incoming, nil
			}
			// Non-matching reply-class frame (e.g. a stale contacts
			// reply from an earlier timed-out request while expecting
			// pong): hand to the dispatcher, whose switch has no cases
			// for reply types — a deliberate no-op equivalent to the
			// historical drop, kept as one uniform sink.
			s.dispatchPeerSessionFrame(session.address, session, incoming)
			continue
		}
	}
}

// syncPeerSession performs peer exchange (conditional) and contact sync over
// an existing authenticated peer session.
//
// requestPeers controls whether get_peers is sent. The caller must evaluate
// shouldRequestPeers() BEFORE any side-effects that alter the aggregate
// status for the current connection (e.g. markPeerConnected). This ensures
// both session-based call sites (openPeerSession / initPeerSession) see a
// consistent aggregate snapshot that does not yet include the peer being
// set up.
//
// path identifies the caller for observability (peerExchangePathSessionOutbound
// for the legacy openPeerSession path, peerExchangePathSessionCM for the
// ConnectionManager initPeerSession path). See
// docs/peer-discovery-conditional-get-peers.ru.md Step 6.
func (s *Service) syncPeerSession(session *peerSession, requestPeers bool, path peerExchangePath) error {
	if requestPeers {
		peersFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "get_peers"}, "peers", false)
		if err != nil {
			return err
		}
		peersImported := 0
		for _, peer := range peersFrame.Peers {
			if s.addPeerAddress(domain.PeerAddress(peer), "", domain.PeerIdentity{}) {
				peersImported++
			}
		}

		// Notify CM that new peers were discovered from peer exchange.
		if len(peersFrame.Peers) > 0 && s.connManager != nil {
			s.connManager.EmitHint(NewPeersDiscovered{Count: len(peersFrame.Peers)})
		}

		s.logPeerExchangeExecuted(path, session.address, len(peersFrame.Peers), peersImported)
	}

	_, err := s.syncContactsViaSession(session)
	return err
}

// syncContactsViaSession fetches and imports contacts over an existing
// authenticated peer session. Returns the number of newly imported contacts.
// Unlike syncPeer (which opens a fresh TCP connection), this reuses the
// session's connection and avoids a full handshake — critical for NATed or
// inbound-only peers whose transport address is not redialable.
//
// Caller must ensure the session is not currently busy with another
// peerSessionRequest (single-reader constraint on inboxCh).
//
// Deprecated: superseded by the get_identity datagram lookup and the initial
// push_identity of the identity-discovery layer; kept as the epidemic bridge
// for peers without the layer. TODO(fetch-contacts-floor): remove when
// nothing is left to bridge — see docs/protocol/identity-lookup.md.
func (s *Service) syncContactsViaSession(session *peerSession) (int, error) {
	contactsFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "fetch_contacts"}, "contacts", false)
	if err != nil {
		return 0, err
	}
	// The verification budget is the REMOTE's, not the session's: a bucket that
	// lived on peerSessionAdmission was born full with every reconnect, which is
	// the same reset the fresh-dial path used to hand out. Both importers charge
	// one node-scoped bucket keyed on the connection's endpoint, so the two
	// paths cannot be alternated for two budgets (contact_verify_budget.go).
	report := s.importAdvertisedContacts(
		s.contactVerifyBudgetFor(sessionContactVerifyKey(session)),
		contactsFrame.Contacts,
	)
	switch report.Outcome {
	case contactImportRefusedCountCap:
		return 0, s.refuseOversizeContactsReply(session, report.Offered)
	case contactImportBudgetExhausted:
		// NOT a violation. This is the neighbour answering more replies than the
		// sustained budget covers, which a legitimate peer reaches only when this
		// node itself asked for several syncs in quick succession. The verified
		// prefix is kept — the entries this node paid for are the entries it
		// gets — and the rest comes back on the next pass.
		log.Warn().
			Str("peer", string(session.address)).
			Int("offered", report.Offered).
			Int("verified", report.Verified).
			Int("imported", report.Imported).
			Msg("contact_verification_budget_exhausted")
	}
	return report.Imported, nil
}

// contactImportOutcome says how a `contacts` reply was disposed of. It is an
// enumeration rather than a pair of bools because the three answers are mutually
// exclusive and each one is a different decision at the call site.
type contactImportOutcome uint8

const (
	// contactImportCompleted means every entry was walked.
	contactImportCompleted contactImportOutcome = iota + 1
	// contactImportRefusedCountCap means the reply carried more entries than
	// maxContactsPerResponse and NOTHING was verified or imported.
	contactImportRefusedCountCap
	// contactImportBudgetExhausted means the neighbour's verification budget
	// ran out part-way; the verified prefix was imported.
	contactImportBudgetExhausted
)

// String returns the outcome name used in logs.
func (o contactImportOutcome) String() string {
	switch o {
	case contactImportCompleted:
		return "completed"
	case contactImportRefusedCountCap:
		return "refused_count_cap"
	case contactImportBudgetExhausted:
		return "budget_exhausted"
	default:
		return "invalid"
	}
}

// contactImportReport is what one `contacts` reply produced.
type contactImportReport struct {
	// Offered is how many entries the reply carried.
	Offered int
	// Verified counts the entries that reached identity.VerifyBoxKeyBinding —
	// the work the budget exists to bound, whatever the verdict was.
	Verified int
	// Imported counts the entries whose binding verified and entered the
	// knowledge maps.
	Imported int
	Outcome  contactImportOutcome
}

// contactVerificationBudget is the WORK budget one `contacts` reply is verified
// against: one token per signature check, taken immediately before the check.
//
// It is an interface so importAdvertisedContacts states what it needs and
// nothing else — the production implementation is one node-scoped, per-remote
// refilling bucket shared by BOTH import paths (contact_verify_budget.go), and
// tests substitute a counter. There is deliberately no per-connection
// implementation any more: the fresh recovery dial used to build its own
// non-refilling budget per dial, on the argument that the connection carries
// exactly one reply — but the dial is scheduled by a wire field the remote
// writes, so "one budget per connection" meant "as many budgets as the remote
// cares to ask for".
type contactVerificationBudget interface {
	// ChargeContactVerify takes one token and reports whether the caller may
	// perform the verification. False means the entry must be skipped WITHOUT
	// verifying it.
	ChargeContactVerify() bool
}

// importAdvertisedContacts is the ONE place a peer-advertised contact list is
// verified and imported, and the two-stage admission of §5 applied to it.
//
// # Why it is metered at all
//
// Every entry costs one identity.VerifyBoxKeyBinding — an Ed25519 verification,
// ~50 µs — and the array is attacker-sized. The response plane meters the BYTES
// a neighbour makes this node read, but bytes are not what this loop spends: at
// ~approximateContactWireBytes per entry a single maximum-size reply is tens of
// thousands of signature checks, and the byte burst admits two of them back to
// back. So the count is capped first, and what survives the cap is charged
// against a budget one token at a time.
//
// # The order of the two stages
//
// The count cap is read from len() BEFORE the walk, so a reply past it costs
// zero verifications — refusing entry by entry would still let the reply buy a
// full budget's worth of crypto. The token is then charged immediately before
// each check and after the structural test, which is where §5 puts it: an
// incomplete entry never reaches a signature check, so it must not spend a token
// either, or an attacker would drain the budget with entries that are free to
// refuse and starve the entries behind them.
//
// Network-discovered contacts are stored in memory only and are NOT written to
// the trust store; that distinction is preserved by fetch_trusted_contacts
// (encryption.md: signed box-key advertisement).
func (s *Service) importAdvertisedContacts(
	budget contactVerificationBudget,
	contacts []protocol.ContactFrame,
) contactImportReport {
	report := contactImportReport{Offered: len(contacts), Outcome: contactImportCompleted}
	if len(contacts) > maxContactsPerResponse {
		report.Outcome = contactImportRefusedCountCap
		return report
	}
	for _, contact := range contacts {
		if contact.Address == "" || contact.PubKey == "" || contact.BoxKey == "" || contact.BoxSig == "" {
			continue
		}
		if !budget.ChargeContactVerify() {
			report.Outcome = contactImportBudgetExhausted
			return report
		}
		report.Verified++
		if identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig) != nil {
			continue
		}
		s.addKnownIdentity(domain.PeerIdentityFromWire(contact.Address))
		s.addKnownBoxKey(contact.Address, contact.BoxKey)
		s.addKnownPubKey(contact.Address, contact.PubKey)
		s.addKnownBoxSig(contact.Address, contact.BoxSig)
		s.notifyIdentityKeysImported(contact.Address)
		report.Imported++
	}
	return report
}

// syncSenderKeys imports unknown sender keys from the peer at senderAddress.
// It prefers syncing over an existing authenticated outbound session (no new
// TCP connection, works for NATed/inbound-only peers) and falls back to a
// fresh dial only when no reusable session is available.
//
// The fallback fresh-dial path deliberately skips get_peers. Sender-key
// recovery is a narrow contact/key sync, not a bootstrap/recovery dial, so
// it must not trigger peer exchange even when the aggregate status would
// otherwise allow it. See docs/peer-discovery-conditional-get-peers.ru.md
// Step 5.
//
// The ctx parameter is the owning lifecycle context (service run / peer
// session). It is used both to bound the fresh-dial handshake and to cancel
// the recovery when the owning lifecycle is torn down. A local
// context.Background() here would lose that cancellation — see
// CLAUDE.md: context.Context is passed as the first argument.
//
// The syncSession parameter, when non-nil, is used directly instead of
// looking up a session by address. Callers pass nil when the only candidate
// session is currently inside a peerSessionRequest read loop (e.g.,
// dispatchPeerSessionFrame dispatched during a ping), because the
// single-reader constraint on inboxCh would cause a deadlock.
// Deprecated: superseded by the get_identity datagram lookup; kept as the
// epidemic bridge for peers without the layer.
// TODO(fetch-contacts-floor): remove the fan-out when nothing is left to
// bridge — see docs/protocol/identity-lookup.md.
func (s *Service) syncSenderKeys(ctx context.Context, senderAddress domain.PeerAddress, syncSession *peerSession) int {
	if syncSession != nil {
		// Narrow contact/key recovery over an existing authenticated session:
		// peer exchange is never initiated here. Logged as a narrow-recovery
		// skip so this branch is visible in the peer_exchange_skipped stream
		// alongside the fresh-dial fallback — otherwise operators would see
		// contact recovery happen with no corresponding skip record and could
		// not tell a consciously-narrow sync from silent observability drift.
		// See docs/peer-discovery-conditional-get-peers.ru.md Step 6.
		s.logPeerExchangeSkipped(peerExchangePathSenderKeyViaSession, senderAddress, peerExchangeSkipByNarrowRecovery)
		imported, err := s.syncContactsViaSession(syncSession)
		if err == nil {
			if imported > 0 {
				log.Info().Str("peer", string(senderAddress)).Int("imported", imported).Msg("sync_sender_keys_via_session")
			}
			return imported
		}
		log.Warn().Err(err).Str("peer", string(senderAddress)).Msg("sync_sender_keys_session_failed")
	}

	// Fall back to a fresh TCP connection. Derive a timeout from the owning
	// ctx so the whole recovery is bounded but still cancels on lifecycle
	// shutdown. The budget is syncRecoveryTimeout, not syncHandshakeTimeout:
	// the dial and each handshake read are individually bounded by the
	// connection's idle deadline, while the post-auth reply drains
	// (readSyncReply) legitimately need up to syncReplyDrainCap each —
	// a 1.5s overall ctx would cancel a healthy drain mid-flight.
	dialCtx, cancel := context.WithTimeout(ctx, syncRecoveryTimeout)
	defer cancel()
	// requestPeers=false: narrow contact/key recovery only. Logged as a
	// narrow-recovery skip so operators can tell this path apart from a
	// steady-state (healthy) policy skip. See
	// docs/peer-discovery-conditional-get-peers.ru.md Step 6.
	s.logPeerExchangeSkipped(peerExchangePathSenderKeyFreshDial, senderAddress, peerExchangeSkipByNarrowRecovery)
	return s.syncPeer(dialCtx, senderAddress, false)
}

// senderKeySyncFanout caps how many ADDITIONAL connected peers (beyond
// the previous hop) one background recovery pass may query for the
// target sender's contact. The previous hop is only the best FIRST
// guess: a pure transit relay never needed the origin's keys to
// forward (the fast path is crypto-free), and a NATed / inbound-only
// previous hop may not be fresh-dialable at all — so a recovery scoped
// to it alone can never terminate. Peers we hold live OUTBOUND
// sessions to are dialable by construction (we already dialed them),
// and contact knowledge spreads between full nodes at session setup
// (syncPeerSession), which makes them genuinely useful second guesses.
const senderKeySyncFanout = 3

// senderKeySyncCooldown rate-limits recovery passes per SENDER: after a
// completed pass — successful or not — no new pass starts for this long.
// Upstream retry cadence would otherwise re-trigger a full fan-out
// (1 + senderKeySyncFanout dials) every few seconds for a sender whose
// keys genuinely aren't available anywhere yet.
const senderKeySyncCooldown = 30 * time.Second

// maxConcurrentSenderKeySyncPasses is the GLOBAL cap on simultaneously
// running recovery passes. Per-sender single-flight alone does not
// bound the network cost: DM frames carry an attacker-chosen sender
// fingerprint, so a burst of frames with DISTINCT fabricated senders
// would otherwise spawn one pass each — every pass holding a goroutine
// for up to its 2×syncRecoveryTimeout budget and opening up to
// 1+senderKeySyncFanout outbound dials. With the cap, excess triggers
// are dropped outright (not queued): the dropped message's upstream
// retry re-triggers later, when a slot and the per-sender cooldown
// allow, so legitimate recovery degrades to "later" while a fabricated
// flood degrades to "never past 3 concurrent passes".
const maxConcurrentSenderKeySyncPasses = 3

// triggerSenderKeySyncAsync schedules a background, single-flight
// contact recovery for ONE sender identity, starting at the previous
// hop and fanning out to up to senderKeySyncFanout other peers with
// live outbound sessions until the sender's signing key appears. This
// is the ONLY way the unknown-sender-key recovery is invoked from
// frame handlers: running syncSenderKeys inline there blocks the
// session dispatch loop (dispatchPeerSessionFrame /
// handleInboundPushMessage) for up to a full recovery budget while the
// socket reader keeps filling the bounded inboxCh — a large enough
// backlog then overflows the buffer and tears down a HEALTHY session
// (inbox overflow is fatal by design, see peer_sessions.go). The
// message that triggered the recovery is NOT retried here: it is
// rejected for this attempt and redelivered by the existing retry
// contours (previous-hop relay retry, sender e2e retry), which find
// the imported keys on the next attempt.
//
// Keying: single-flight AND cooldown are per sender fingerprint — the
// goal of a pass is "obtain THIS sender's keys", and a burst of
// undeliverable messages from that sender (the exact wedge scenario)
// must produce one pass, not one per message. When the caller has no
// sender identity (defensive), the previous-hop address keys the
// single-flight instead.
//
// ownedSession, when non-nil, is a live OUTBOUND session to the
// previous hop and is tried FIRST — through the session OWNER
// (requestOwnedContactSync serialises the fetch_contacts via the serve
// loop's contactSyncCh), never by reading inboxCh from this goroutine.
// This is the recovery path for a previous hop we already hold a
// session to but cannot freshly re-dial (NAT rebinding, unstable or
// aliased address, listener temporarily unreachable): the existing
// session is the only wire to it. Calling peerSessionRequest directly
// from here would race the serve loop for inbox frames — the pre-async
// inline code that passed syncSession from the inbound dispatch path
// had exactly that latent race; owner serialisation is the sanctioned
// replacement. After the owned attempt (or when no session is
// available) the pass falls back to a fresh dial of the previous hop,
// then the fan-out.
//
// Honest scope note: a peer connected to us ONLY inbound (we hold no
// outbound session at all) has no recovery wire in this design —
// deployed peers do not serve fetch_contacts on their outbound-session
// dispatcher, so a request sent back over the inbound connection would
// go unanswered. Such recipients rely on the v27 attached keys or the
// fan-out; serving fetch_contacts on the peer-session dispatcher is a
// both-ends protocol addition deferred to the floor-raise era.
func (s *Service) triggerSenderKeySyncAsync(prevHop domain.PeerAddress, sender string, ownedSession *peerSession) {
	// Canonical-format gate on the wire-supplied sender string BEFORE it
	// can become a map key: a keyless DM frame carries an arbitrary
	// attacker-chosen sender that may approach the 8 MiB frame budget,
	// and holding such strings in senderKeySyncInFlight/LastRun would be
	// a memory sink. A non-canonical sender also cannot succeed — no
	// contact plane entry can ever match it — so it degrades to the
	// address-keyed pass (the previous hop is still worth syncing).
	if sender != "" && !identity.IsValidAddress(sender) {
		log.Debug().Int("sender_len", len(sender)).Str("prev_hop", string(prevHop)).Msg("sender_key_sync_invalid_sender_ignored")
		sender = ""
	}
	if prevHop == "" && sender == "" {
		return
	}
	key := sender
	if key == "" {
		key = "addr:" + string(prevHop)
	}
	// Fairness-slot key: authenticated identity when known, transport
	// address otherwise. Resolved BEFORE taking senderKeySyncMu —
	// viaIdentityForAddress touches peer-domain state under its own
	// lock, and this mutex must never nest over other domains.
	hopKey := ""
	if prevHop != "" {
		hopKey = "addr:" + string(prevHop)
		if ownedSession != nil && !ownedSession.peerIdentity.IsZero() {
			hopKey = "id:" + ownedSession.peerIdentity.String()
		} else if id := s.viaIdentityForAddress(prevHop); !id.IsZero() {
			hopKey = "id:" + id.String()
		}
	}

	s.senderKeySyncMu.Lock()
	if s.senderKeySyncInFlight == nil {
		// Defensive for struct-literal test Services that bypass NewService.
		s.senderKeySyncInFlight = make(map[string]struct{})
	}
	if s.senderKeySyncLastRun == nil {
		s.senderKeySyncLastRun = make(map[string]time.Time)
	}
	if _, busy := s.senderKeySyncInFlight[key]; busy {
		s.senderKeySyncMu.Unlock()
		return
	}
	if last, ok := s.senderKeySyncLastRun[key]; ok && time.Since(last) < senderKeySyncCooldown {
		s.senderKeySyncMu.Unlock()
		return
	}
	// Global concurrency cap — per-sender single-flight alone does not
	// bound a flood of DISTINCT fabricated senders (see the constant
	// doc). Dropped, not queued: upstream retries re-trigger later.
	if len(s.senderKeySyncInFlight) >= maxConcurrentSenderKeySyncPasses {
		s.senderKeySyncMu.Unlock()
		log.Debug().Str("sender", sender).Str("prev_hop", string(prevHop)).Msg("sender_key_sync_pass_cap_reached")
		return
	}
	// Per-hop fairness slot: ONE concurrent pass per previous hop. A
	// hostile peer feeding unique well-formed sender fingerprints could
	// otherwise keep all global slots busy and starve recovery for
	// messages arriving via other hops. One slot per hop costs
	// legitimate traffic nothing: a pass runs fetch_contacts against
	// that hop and imports its WHOLE contact plane, so distinct real
	// senders behind the same hop are covered by one pass anyway.
	// The slot keys on the AUTHENTICATED peer identity when resolvable
	// (owned session first, then the via-identity map) and falls back
	// to the transport address: one identity holding several
	// connections under different IPs / advertise ports / dial aliases
	// must still occupy a single slot, not one per address.
	if hopKey != "" {
		if s.senderKeySyncHopInFlight == nil {
			s.senderKeySyncHopInFlight = make(map[string]struct{})
		}
		if _, hopBusy := s.senderKeySyncHopInFlight[hopKey]; hopBusy {
			s.senderKeySyncMu.Unlock()
			log.Debug().Str("sender", sender).Str("hop_key", hopKey).Msg("sender_key_sync_hop_slot_busy")
			return
		}
		s.senderKeySyncHopInFlight[hopKey] = struct{}{}
	}
	s.senderKeySyncInFlight[key] = struct{}{}
	s.senderKeySyncMu.Unlock()

	parent := s.runCtx
	if parent == nil {
		parent = context.Background()
	}
	// lifecycle: fire-and-forget. One sender-key sync exchange, bounded by the
	// dial and handshake timeouts of the send it performs, not a loop.
	s.goBackground(func() {
		defer func() {
			s.senderKeySyncMu.Lock()
			delete(s.senderKeySyncInFlight, key)
			if hopKey != "" {
				delete(s.senderKeySyncHopInFlight, hopKey)
			}
			s.senderKeySyncLastRun[key] = time.Now()
			s.pruneSenderKeySyncLastRunLocked()
			s.senderKeySyncMu.Unlock()
		}()
		// One overall budget for the whole pass (previous hop + fan-out);
		// each syncSenderKeys call additionally clamps itself to
		// syncRecoveryTimeout, so the total is min-bounded twice.
		ctx, cancel := context.WithTimeout(parent, 2*syncRecoveryTimeout)
		defer cancel()

		// Owner-serialised sync over the live outbound session to the
		// previous hop, when the caller had one — the only wire to a
		// hop that cannot be freshly re-dialed. Bounded by the pass ctx:
		// lifecycle shutdown must not be held hostage by the request
		// timers (goBackground → WaitBackground waits for this
		// goroutine).
		if ownedSession != nil {
			imported, ok := s.requestOwnedContactSync(ctx, ownedSession, peerRequestTimeout)
			log.Info().Str("peer", string(ownedSession.address)).Str("sender", sender).Int("imported", imported).Bool("executed", ok).Msg("sender_key_sync_owned_session_pass")
			if sender != "" && s.hasSenderPubKey(sender) {
				log.Info().Str("sender", sender).Msg("sender_key_sync_async_satisfied")
				return
			}
		}

		candidates := make([]domain.PeerAddress, 0, 1+senderKeySyncFanout)
		if prevHop != "" {
			candidates = append(candidates, prevHop)
		}
		candidates = append(candidates, s.senderKeySyncCandidates(prevHop)...)

		for _, addr := range candidates {
			if ctx.Err() != nil {
				return
			}
			// Already recovered (by an earlier candidate, a parallel
			// handshake, or a v27 frame that arrived meanwhile)?
			if sender != "" && s.hasSenderPubKey(sender) {
				log.Info().Str("sender", sender).Msg("sender_key_sync_async_satisfied")
				return
			}
			imported := s.syncSenderKeys(ctx, addr, nil)
			log.Info().Str("peer", string(addr)).Str("sender", sender).Int("imported", imported).Msg("sender_key_sync_async_pass")
		}
		if sender != "" && !s.hasSenderPubKey(sender) {
			log.Warn().Str("sender", sender).Int("candidates", len(candidates)).Msg("sender_key_sync_async_exhausted")
		}
	})
}

// requestOwnedContactSync asks a session's OWNER loop (servePeerSession)
// to run a fetch_contacts exchange on the caller's behalf and reports
// the imported-contact count. Returns (0, false) when the request could
// not be executed: nil session, a manually-built session without the
// channel, a loop that is dead / too busy to accept within timeout, or
// a done ctx. ctx is the caller's lifecycle/budget context — both
// waits observe it so a shutdown (WaitBackground) or an exhausted
// recovery budget is never held hostage by the request timers. The
// reply channel is buffered so the owner's response never blocks the
// serve loop, and contactSyncCh itself is unbuffered so a send
// succeeds only when a live loop is actually receiving.
func (s *Service) requestOwnedContactSync(ctx context.Context, session *peerSession, timeout time.Duration) (int, bool) {
	if session == nil || session.contactSyncCh == nil || ctx.Err() != nil {
		return 0, false
	}
	reply := make(chan int, 1)
	deliver := time.NewTimer(timeout)
	defer deliver.Stop()
	select {
	case session.contactSyncCh <- reply:
	case <-deliver.C:
		return 0, false
	case <-ctx.Done():
		return 0, false
	}
	// The owner is now committed to answering; its own
	// peerSessionRequest is bounded by peerRequestTimeout, so wait one
	// timeout beyond that for the result.
	wait := time.NewTimer(timeout + peerRequestTimeout)
	defer wait.Stop()
	select {
	case imported := <-reply:
		return imported, true
	case <-wait.C:
		return 0, false
	case <-ctx.Done():
		return 0, false
	}
}

// maxSenderKeySyncLastRunEntries hard-caps the cooldown-stamp map.
// Prune runs at PASS COMPLETION (not per trigger — a frame flood must
// never pay a map scan per frame), and pass completion rate is itself
// bounded by maxConcurrentSenderKeySyncPasses, so the scan frequency is
// structurally low. Expired entries go first; if a flood of unique
// valid senders keeps every stamp younger than the cooldown, arbitrary
// entries are evicted down to the cap — losing a cooldown stamp only
// means one extra (globally-capped) recovery pass may run early, which
// is strictly cheaper than an unbounded map.
const maxSenderKeySyncLastRunEntries = 1024

// pruneSenderKeySyncLastRunLocked bounds senderKeySyncLastRun. Caller
// must hold senderKeySyncMu.
func (s *Service) pruneSenderKeySyncLastRunLocked() {
	if len(s.senderKeySyncLastRun) <= maxSenderKeySyncLastRunEntries {
		return
	}
	cutoff := time.Now().Add(-senderKeySyncCooldown)
	for k, ts := range s.senderKeySyncLastRun {
		if ts.Before(cutoff) {
			delete(s.senderKeySyncLastRun, k)
		}
	}
	// Still over the cap (flood of young stamps): evict arbitrarily.
	for k := range s.senderKeySyncLastRun {
		if len(s.senderKeySyncLastRun) <= maxSenderKeySyncLastRunEntries {
			break
		}
		delete(s.senderKeySyncLastRun, k)
	}
}

// hasSenderPubKey reports whether the sender's signing key is present
// in the knowledge maps — the fan-out's termination condition.
func (s *Service) hasSenderPubKey(sender string) bool {
	s.knowledgeMu.RLock()
	defer s.knowledgeMu.RUnlock()
	return s.pubKeys[sender] != ""
}

// senderKeySyncCandidates snapshots up to senderKeySyncFanout addresses
// of peers with live, connected OUTBOUND sessions, excluding the
// previous hop (already tried first) AND deduplicating by peer
// IDENTITY: several sessions to the same identity (reconnect aliases,
// multi-homed peer) contribute at most one candidate, so the fan-out
// budget buys senderKeySyncFanout DISTINCT peers to ask rather than
// three connections to possibly one peer. Outbound addresses are
// dialable by construction; inbound-only peers are skipped — a fresh
// dial to an unroutable source address would only burn the pass budget.
func (s *Service) senderKeySyncCandidates(exclude domain.PeerAddress) []domain.PeerAddress {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	excludeID := domain.PeerIdentity{}
	if sess := s.sessions[exclude]; sess != nil {
		excludeID = sess.peerIdentity
	}
	seenID := make(map[domain.PeerIdentity]struct{})
	out := make([]domain.PeerAddress, 0, senderKeySyncFanout)
	for addr, session := range s.sessions {
		if session == nil || addr == exclude {
			continue
		}
		if !session.peerIdentity.IsZero() {
			if session.peerIdentity == excludeID {
				continue
			}
			if _, dup := seenID[session.peerIdentity]; dup {
				continue
			}
		}
		health := s.health[s.resolveHealthAddress(addr)]
		if health == nil || !health.Connected {
			continue
		}
		if !session.peerIdentity.IsZero() {
			seenID[session.peerIdentity] = struct{}{}
		}
		out = append(out, addr)
		if len(out) >= senderKeySyncFanout {
			break
		}
	}
	return out
}

// outboundControlFrameLimitKey is the cmdLimiter bucket key used for
// per-session command-rate limiting on the outbound peer-session
// path. Inbound uses RemoteAddr().String(); outbound has no
// equivalent because we initiated the connection, so we key by the
// peer-session connID (monotonic, unique per session) under an
// "outbound:" namespace to avoid collisions with inbound buckets.
//
// connID-keying means a peer that loses the session and reconnects
// gets a fresh bucket. That's the same property the inbound key
// (RemoteAddr) already has implicitly (new src port → new key);
// matching it here keeps the contract symmetric.
func outboundControlFrameLimitKey(connID domain.ConnID) string {
	return "outbound:" + strconv.FormatUint(uint64(connID), 10)
}

// outboundControlFrameAllowed mirrors the inbound cmdLimiter
// coverage for control-class announce-plane frames
// (request_resync / route_poison_v1) on the outbound peer-session
// dispatcher. Returns true when the frame may be handled, false
// when the per-session bucket is exhausted and the frame must be
// dropped.
//
// Why this exists: the inbound read loop (service.go) charges
// cmdLimiter tokens for every non-exempt frame BEFORE dispatch, so
// a peer flooding inbound TCP with request_resync / route_poison_v1
// gets throttled at 100 burst / 30 cmd/s. The outbound peer-session
// dispatcher had NO such throttle — a peer that waits for our
// outbound dial could flood the same control frames over the
// established session, bypass cmdLimiter entirely, and fall back
// only to the loose 200-token/s announceLimiter route bucket. This
// helper closes that asymmetry.
//
// Bulk announce frames (announce_routes / routes_update /
// route_announce_v3) are deliberately NOT routed through this
// helper: they are governed by announceLimiter (route-count, all
// bulk frames) and — for DELTA frames only (routes_update / v3
// kind="delta") — the chatty_routes quarantine (frames/sec). Full
// baselines are bounded by the route bucket, not chatty (see
// recordInboundAnnounceAndMaybeArm). Together they bound CPU without
// the cmd limiter's 100/30 cap that would truncate a legitimate
// chunked full-sync. See isAnnouncePlaneBulkFrameType in
// routing_announce.go.
func (s *Service) outboundControlFrameAllowed(session *peerSession) bool {
	if s.cmdLimiter == nil {
		return true
	}
	return s.cmdLimiter.allowCommand(outboundControlFrameLimitKey(session.connID))
}

// dropOutboundControlFrameBucket drops the per-session cmdLimiter
// bucket for the given connID. Called from every place that
// removes an outbound session entry from s.sessions:
//
//   - runPeerSession error-cleanup (legacy non-CM path)
//   - onCMSessionEstablished ownedCleanup (CM-managed path)
//   - onCMSessionTeardown (CM-initiated close)
//   - ensurePeerSessions outer defer (best-effort fallback)
//
// commandRateLimiter.cleanup is NOT wired to any periodic sweep,
// so without an explicit removal at each site the bucket lives
// forever — one entry per ever-opened outbound connection. A
// long-running node would accumulate buckets at the rate of new
// outbound sessions until restart, plus a stale connID could
// shadow a future session that wraps to the same monotonic ID
// (only theoretical at 64-bit, but free to defend against).
//
// Safe to call with connID == 0 or when cmdLimiter is nil
// (test fixtures); both are no-ops.
func (s *Service) dropOutboundControlFrameBucket(connID domain.ConnID) {
	if s.cmdLimiter == nil || connID == 0 {
		return
	}
	s.cmdLimiter.removeConn(outboundControlFrameLimitKey(connID))
}

func (s *Service) dispatchPeerSessionFrame(address domain.PeerAddress, session *peerSession, frame protocol.Frame) {
	// Respond to inbound pings on outbound sessions so the remote
	// heartbeat monitor receives a timely pong. Without this the
	// remote side closes the connection after pongStallTimeout.
	// Pings are not "useful" application traffic, only keep-alive.
	if frame.Type == "ping" {
		if session != nil {
			pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
			// Route through the injected Network surface; helper falls back
			// to session.netCore on ErrUnknownConn so live sessions with
			// reaped or absent (tests) registry entries still get the pong
			// out without a spurious unregistered_write log. writerLoop
			// owns the per-direction write deadline.
			_ = s.sendSessionFrameViaNetwork(s.runCtx, session, pongFrame)
			s.markPeerWrite(address, pongFrame)
		}
		return
	}

	s.markPeerUsefulReceive(address)
	switch frame.Type {
	case "push_message":
		if frame.Item == nil {
			return
		}
		if len(frame.Item.Body) > maxPeerCommandBodyBytes {
			return
		}

		msg, err := incomingMessageFromFrame(protocol.Frame{
			ID:         frame.Item.ID,
			Topic:      frame.Topic,
			Address:    frame.Item.Sender,
			Recipient:  frame.Item.Recipient,
			Flag:       frame.Item.Flag,
			CreatedAt:  frame.Item.CreatedAt,
			TTLSeconds: frame.Item.TTLSeconds,
			Hops:       frame.Item.Hops,
			Body:       frame.Item.Body,
			// Attached PUBLIC sender keys ride the top-level frame
			// fields (see attachKnownSenderKeys); validated on import.
			PubKey: frame.PubKey,
			BoxKey: frame.BoxKey,
			BoxSig: frame.BoxSig,
		})
		if err != nil {
			return
		}
		// Ingress link for hop accounting + echo suppression
		// (transit_retention.go).
		if session != nil {
			msg.Via = session.address
			msg.ViaIdentity = session.peerIdentity
		}

		// Non-DM sender verification: reject messages whose sender is not
		// a known identity. DM-class messages — both data DMs ("dm") and
		// control DMs (TopicControlDM) — have cryptographic verification
		// in storeIncomingMessage (VerifyEnvelope); this gate targets
		// only topics where no per-message signature exists.
		peerID := domain.PeerIdentity{}
		if session != nil {
			peerID = session.peerIdentity
		}
		if !protocol.IsDMTopic(msg.Topic) && !s.isVerifiedSender(msg.Sender, peerID) {
			log.Warn().
				Str("node", s.identity.Address).
				Str("peer", string(address)).
				Str("id", string(msg.ID)).
				Str("sender", msg.Sender).
				Str("topic", msg.Topic).
				Msg("push_message rejected: non-DM sender identity not verified (outbound)")
			return
		}

		stored, _, errCode := s.storeIncomingMessage(msg, true)
		if !stored && errCode == protocol.ErrCodeUnknownSenderKey {
			// Legacy keyless frame — schedule a BACKGROUND single-flight
			// contact sync and reject this attempt. Running the recovery
			// inline here blocked the session event loop for the whole
			// dial (up to syncRecoveryTimeout with the drain-tolerant
			// reader) while the socket reader kept filling the bounded
			// inboxCh — overflow there is fatal and tore down a healthy
			// session precisely on the legacy/keyless route. No
			// ack_delete goes out for this store result, so the sender's
			// retry redelivers the message once the keys are imported.
			log.Info().
				Str("peer", string(address)).
				Str("id", string(msg.ID)).
				Str("sender", msg.Sender).
				Str("recipient", msg.Recipient).
				Msg("push_message_key_sync_scheduled")
			s.triggerSenderKeySyncAsync(address, msg.Sender, session)
		}
		// Ack policy: see shouldAckOnStoreResult — stored=true OR the
		// dedup branch (stored=false && errCode=="") both mean "we have
		// this message, sender can stop retrying". errCode!="" leaves
		// the peer to retry once it addresses the underlying failure
		// (unknown_sender_key triggers a sync upstream; other codes
		// surface in the warn log).
		if shouldAckOnStoreResult(stored, errCode) {
			s.enqueueAckDeleteOnSession(session, address, ackDeleteForMessage(msg.ID))
			if !stored {
				log.Debug().Str("node", s.identity.Address).Str("peer", string(address)).Str("id", string(msg.ID)).Msg("push_message_dedup_acked")
			}
		} else {
			log.Warn().Str("node", s.identity.Address).Str("peer", string(address)).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Str("err_code", errCode).Msg("push_message_store_failed_no_ack_delete")
		}
		log.Info().Str("node", s.identity.Address).Str("peer", string(address)).Str("id", string(msg.ID)).Str("sender", msg.Sender).Str("recipient", msg.Recipient).Bool("stored", stored).Msg("received pushed message")
	case "push_delivery_receipt":
		if frame.Receipt == nil {
			return
		}
		receipt, err := receiptFromReceiptFrame(*frame.Receipt)
		if err != nil {
			return
		}
		// Identity gate: the pushed receipt's Recipient must match our own
		// identity or an identity we actively subscribe to (full-node relay).
		// Without this check a peer could push a receipt with arbitrary
		// Sender/Recipient and corrupt delivery state for foreign conversations.
		if receipt.Recipient != s.identity.Address && !s.hasSubscriber(receipt.Recipient) {
			log.Warn().
				Str("peer", string(address)).
				Str("message_id", string(receipt.MessageID)).
				Str("receipt_recipient", receipt.Recipient).
				Str("local_identity", s.identity.Address).
				Msg("push_delivery_receipt rejected: recipient does not match local identity or active subscriber")
			return
		}
		// Acking tells the peer to delete their copy, and their copy is
		// the only place this fact survives if our own write failed.
		if outcome := s.storeDeliveryReceipt(receipt); outcome.ackable {
			s.enqueueAckDeleteOnSession(session, address, ackDeleteForReceipt(receipt))
		} else {
			log.Warn().Str("peer", string(address)).Str("message_id", string(receipt.MessageID)).
				Msg("receipt not acked: its delivery status could not be recorded, so the peer must keep it")
		}
		log.Info().Str("peer", string(address)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received pushed delivery receipt")
	case "relay_delivery_receipt":
		// Gossip receipt path using flat Frame fields. Three paths mirror
		// handleInboundRelayDeliveryReceipt: local delivery, transit
		// forwarding via relay chain, or gossip fallback. No ban scoring.
		// Dedupe marking is deferred until delivery succeeds — see
		// gossipTransitReceipt and markTransitReceiptSeen comments.
		receipt, err := receiptFromFrame(frame)
		if err != nil {
			return
		}
		// Fast path: receipt is addressed to this node or an active subscriber.
		if receipt.Recipient == s.identity.Address || s.hasSubscriber(receipt.Recipient) {
			s.storeDeliveryReceipt(receipt)
			log.Info().Str("peer", string(address)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received relay_delivery_receipt")
			return
		}
		// Fast-path dedupe: read-only check suppresses already-delivered receipts.
		if s.isTransitReceiptSeen(receipt) {
			log.Debug().
				Str("peer", string(address)).
				Str("message_id", string(receipt.MessageID)).
				Str("recipient", receipt.Recipient).
				Msg("relay_delivery_receipt dropped: duplicate transit receipt (session)")
			return
		}
		// Transit path: forward the receipt along the relay chain.
		// On success, mark as seen to suppress duplicates. On failure,
		// fall back to gossip — consistent with the contract in
		// handleRelayReceipt and the pattern in retryRelayDeliveries.
		if s.handleRelayReceipt(receipt) {
			s.markTransitReceiptSeen(receipt)
			log.Info().
				Str("peer", string(address)).
				Str("message_id", string(receipt.MessageID)).
				Str("recipient", receipt.Recipient).
				Msg("relay_delivery_receipt forwarded via relay chain (session)")
			return
		}
		// Gossip fallback: no reverse relay path or send failed.
		// Pre-mark so rapid-fire duplicate receipts from the same peer are
		// suppressed. gossipTransitReceipt unmarks on complete failure to
		// preserve retry eligibility.
		if s.markTransitReceiptSeen(receipt) {
			log.Debug().
				Str("peer", string(address)).
				Str("message_id", string(receipt.MessageID)).
				Str("recipient", receipt.Recipient).
				Msg("relay_delivery_receipt dropped: duplicate transit receipt pre-gossip (session)")
			return
		}
		// Must run in a goroutine: gossipTransitReceipt fans out
		// receipts via sendReceiptToPeer → queuePeerFrame, which
		// publishes on ebus and touches per-peer pending state under
		// s.peerMu.  The per-peer writer contention alone stalls the read
		// loop long enough to miss heartbeat pong replies and cause
		// the remote side to disconnect on pong-stall timeout; a
		// fire-and-forget hop keeps that per-peer write off the read loop.
		// Track via backgroundWg so WaitBackground() blocks until the
		// gossip fan-out completes — prevents TempDir cleanup races
		// in tests.
		s.goBackground(func() { s.gossipTransitReceipt(receipt) })
		log.Debug().
			Str("peer", string(address)).
			Str("message_id", string(receipt.MessageID)).
			Str("receipt_recipient", receipt.Recipient).
			Str("local_identity", s.identity.Address).
			Msg("relay_delivery_receipt gossip fallback: no relay path or send failed (session)")
	case "push_notice":
		s.handleInboundPushNotice(frame)
	case "announce_peer":
		nodeType := frame.NodeType
		// node_type is validated for protocol compatibility only. announce_peer
		// is third-party gossip, so the sender cannot set the announced peer's
		// local role in our state.
		if !isKnownNodeType(nodeType) {
			return
		}
		peers := frame.Peers
		if len(peers) > maxAnnouncePeers {
			peers = peers[:maxAnnouncePeers]
		}
		added := 0
		for _, peer := range peers {
			if peer == "" || classifyAddress(domain.PeerAddress(peer)) == domain.NetGroupLocal {
				continue
			}
			s.promotePeerAddress(domain.PeerAddress(peer))
			added++
			log.Info().Str("peer", peer).Str("node_type", nodeType).Str("from", string(address)).Msg("learned peer from announce")
		}
		// Notify CM that new peers were discovered from announce.
		if added > 0 && s.connManager != nil {
			s.connManager.EmitHint(NewPeersDiscovered{Count: added})
		}
	case "relay_message":
		if admit := admitRelayFrame(s.sessionHasCapability(session, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			return
		}
		// Passing THIS session is safe post-owner-serialisation: the
		// recovery goroutine never reads inboxCh — it only enqueues an
		// owned-sync request on contactSyncCh, which the serve loop
		// picks up after this dispatch returns (the historical nil was
		// a guard against the recovery re-entering peerSessionRequest
		// on the same single-reader inboxCh). For a previous hop whose
		// address is not freshly dialable, this session IS the wire
		// key recovery arrives on.
		if ackStatus := s.handleRelayMessage(domain.PeerAddress(address), session, frame); ackStatus != "" {
			s.sendRelayHopAck(domain.PeerAddress(address), frame.ID, ackStatus)
		}
	case "relay_hop_ack":
		if admit := admitRelayFrame(s.sessionHasCapability(session, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			return
		}
		s.handleRelayHopAck(domain.PeerAddress(address), frame)
	case "announce_routes":
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV1) {
			return
		}
		// Routing-only peer (no mesh_relay_v1) — routes through it are
		// data-plane unusable. See inbound dispatch for full rationale.
		if !s.sessionHasCapability(session, domain.CapMeshRelayV1) {
			return
		}
		if session != nil {
			s.handleAnnounceRoutes(session.peerIdentity, frame)
		}
	case "routes_update":
		// v2 wire path on the outbound session. Capability gates mirror the
		// inbound dispatcher: v1 is the baseline, v2 the opt-in refinement,
		// relay is the data-plane requirement. Missing any of the three
		// collapses the delta into silent drop — the peer MUST NOT have
		// sent this frame in the first place (v2 is per-session opt-in),
		// so arriving here means the peer misread its own capability set.
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV1) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV2) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshRelayV1) {
			return
		}
		if session != nil {
			s.handleRoutesUpdate(session.peerIdentity, address, frame)
		}
	case "request_resync":
		// v2-only control frame — see inbound dispatcher for contract.
		// No payload, no capability beyond v2 required: the arrival is
		// the signal to clear per-peer announce state and let the next
		// cycle re-issue a legacy baseline.
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV2) {
			return
		}
		if session == nil {
			return
		}
		// Per-session cmd-rate limit. Mirrors the inbound read-loop
		// coverage so a peer flooding request_resync over an
		// outbound session cannot bypass cmdLimiter via the
		// dispatcher and fall back only to the loose route bucket.
		// See outboundControlFrameAllowed for the rationale.
		if !s.outboundControlFrameAllowed(session) {
			log.Debug().
				Str("peer", string(address)).
				Str("frame_type", "request_resync").
				Msg("outbound_session: control frame cmd rate limit exceeded")
			return
		}
		s.handleRequestResync(session.peerIdentity)
	case "route_sync_digest_v1":
		// Phase 3 PR 12.5 — incremental-sync digest arriving on an
		// outbound session. sendFrameToIdentity prefers outbound
		// candidates, so a peer that received OUR digest replies on
		// the same TCP, and the reply reader is THIS dispatcher.
		// Without an explicit case the summary would be silently
		// dropped and the digest match would almost never arm
		// AnnounceLoop suppression on the production path.
		if !s.sessionHasCapability(session, domain.CapMeshRouteSyncV1) {
			return
		}
		digestFrame, err := protocol.UnmarshalRouteSyncDigestFrame([]byte(frame.RawLine))
		if err != nil {
			log.Debug().
				Err(err).
				Str("peer", string(address)).
				Msg("peer_session: route_sync_digest parse failed")
			return
		}
		if session == nil {
			return
		}
		// The handler answers via sendFrameViaNetwork with connID,
		// but on the outbound-session arrival path we do not have a
		// connID — the session's own writer goroutine takes the
		// summary via sendCh. Build the summary inline here so the
		// reply mirrors the digest-handler logic without needing
		// connID plumbing through the outbound side. The compare,
		// TTL refresh and digests_compared/compare_match counters are
		// shared with the inbound path via compareInboundDigest so this
		// outbound arrival is no longer second-class (it previously
		// skipped both the receiver-side TTL refresh and the counters).
		localDigest, localCount, match := s.compareInboundDigest(session.peerIdentity, digestFrame.Digest, digestFrame.Entries)
		summary := protocol.RouteSyncSummaryFrame{
			Type:           protocol.RouteSyncSummaryFrameType,
			Digest:         digestFrame.Digest,
			Match:          match,
			ExpectFullSync: !match,
		}
		raw, marshalErr := protocol.MarshalRouteSyncSummaryFrame(summary)
		if marshalErr != nil {
			log.Warn().
				Err(marshalErr).
				Str("peer_identity", session.peerIdentity.String()).
				Msg("route_sync_summary_marshal_failed_outbound_reply")
			return
		}
		_ = s.sendSessionFrameViaNetwork(s.runCtx, session, protocol.Frame{
			Type:    protocol.RouteSyncSummaryFrameType,
			RawLine: string(raw) + "\n",
		})
		log.Debug().
			Str("peer_identity", session.peerIdentity.String()).
			Str("their_digest", digestFrame.Digest).
			Uint32("their_count", digestFrame.KnownIdentitiesCount).
			Str("our_digest", localDigest).
			Uint32("our_count", localCount).
			Bool("match", match).
			Msg("route_sync_digest_compared_outbound")
	case "route_sync_summary_v1":
		// Phase 3 PR 12.5 — incremental-sync summary arriving on an
		// outbound session (the common case: we initiated the digest
		// from the session-open hook). Without this case Match=true
		// summaries are dropped and the suppression is never armed.
		if !s.sessionHasCapability(session, domain.CapMeshRouteSyncV1) {
			return
		}
		summaryFrame, err := protocol.UnmarshalRouteSyncSummaryFrame([]byte(frame.RawLine))
		if err != nil {
			log.Debug().
				Err(err).
				Str("peer", string(address)).
				Msg("peer_session: route_sync_summary parse failed")
			return
		}
		if session != nil {
			s.handleRouteSyncSummary(session.peerIdentity, summaryFrame)
		}
	case "route_poison_v1":
		// Phase 4 single-hop poison-reverse arriving on an outbound
		// session. Same capability pair (v1 + poison_reverse) and
		// RawLine parse pattern as the inbound dispatcher.
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV1) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshPoisonReverseV1) {
			return
		}
		if session == nil {
			return
		}
		// Per-session cmd-rate limit BEFORE the Unmarshal +
		// (downstream) base64+ed25519 verify path so a hostile
		// peer cannot soak CPU on signature work via outbound
		// flood. Mirrors the inbound read-loop coverage —
		// without this, the loose 200-token/s announceLimiter
		// route bucket was the only outbound defence. See
		// outboundControlFrameAllowed for the rationale.
		if !s.outboundControlFrameAllowed(session) {
			log.Debug().
				Str("peer", string(address)).
				Str("frame_type", "route_poison_v1").
				Msg("outbound_session: control frame cmd rate limit exceeded")
			return
		}
		poison, err := protocol.UnmarshalRoutePoisonFrame([]byte(frame.RawLine))
		if err != nil {
			log.Debug().
				Err(err).
				Str("peer", string(address)).
				Msg("peer_session: route_poison_v1 parse failed")
			return
		}
		s.handleRoutePoison(session.peerIdentity, poison)
	case "route_poison_v2":
		// Batched poison-reverse on an outbound session. Same cap pair as v1
		// but with mesh_poison_reverse_v2, same per-session cmd-rate gate.
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV1) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshPoisonReverseV2) {
			return
		}
		if session == nil {
			return
		}
		if !s.outboundControlFrameAllowed(session) {
			log.Debug().Str("peer", string(address)).Str("frame_type", "route_poison_v2").Msg("outbound_session: control frame cmd rate limit exceeded")
			return
		}
		poisonBatch, err := protocol.UnmarshalRoutePoisonV2Frame([]byte(frame.RawLine))
		if err != nil {
			log.Debug().Err(err).Str("peer", string(address)).Msg("peer_session: route_poison_v2 parse failed")
			return
		}
		s.handleRoutePoisonV2(session.peerIdentity, poisonBatch)
	case "route_announce_v3":
		// Phase 4 compact announce arriving on an outbound session. Same
		// capability triplet as the inbound dispatcher (v1 + v3 + relay)
		// and the same parse-from-RawLine pattern as the route_sync frames.
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV1) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshRoutingV3) {
			return
		}
		if !s.sessionHasCapability(session, domain.CapMeshRelayV1) {
			return
		}
		v3, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(frame.RawLine))
		if err != nil {
			log.Debug().
				Err(err).
				Str("peer", string(address)).
				Msg("peer_session: route_announce_v3 parse failed")
			return
		}
		if session != nil {
			s.handleRouteAnnounceV3(session.peerIdentity, address, v3)
		}
	case "datagram":
		// UNREACHABLE, and kept as the assertion of that fact. readPeerSession
		// classifies every line before the parser runs: a line classifyFrameLine
		// names `datagram` is diverted straight to the ingress, and a line it
		// cannot resolve is refused unparsed (§4.1 step 1). What used to arrive
		// here — a duplicate or case-variant `type` key the parser resolved and
		// the scan could not — no longer reaches protocol.ParseFrameLine at all,
		// so a frame in this branch means the two readers disagree on a line
		// neither refused. That is a classifier bug, not a delivery decision,
		// and delivering it would put the universal parse back in front of the
		// neighbour's budget.
		s.reportDatagramResidueUnreachable("outbound_session", string(address))
	case "error":
		// Remote sent an explicit error frame before closing the connection.
		// Log at Warn so it stands out from the subsequent EOF line that
		// carries no context about the disconnect reason.
		log.Warn().
			Str("peer", string(address)).
			Str("code", frame.Code).
			Str("error", frame.Error).
			Msg("peer_session: remote reported error")
	}
}

// shouldAckOnStoreResult returns true when storeIncomingMessage's
// outcome should trigger an ack_delete back to the sender. ack_delete is
// a BACKLOG-CLEANUP signal — "this hop has the message, release the
// per-hop push/backlog resource" — NOT an end-to-end delivery
// confirmation: sender-side retry stops only on the delivered/seen
// receipt. The two ack-worthy outcomes are:
//
//   - stored=true: the message was newly stored on this hop.
//   - stored=false && errCode=="": the message was a duplicate
//     (already in the dedup index), so this hop has it.
//
// stored=false && errCode!="" is a real failure (unknown sender key,
// timestamp out of range, etc.) — the previous hop should re-attempt
// once it has addressed the underlying cause. Returning false on that
// path leaves the dedup-and-re-push policy intact while keeping the
// duplicate path from looping forever, which is one of the
// reconnect-storm amplifiers tracked in CLAUDE.md.
func shouldAckOnStoreResult(stored bool, errCode string) bool {
	return stored || errCode == ""
}

func (s *Service) sendAckDeleteToPeer(address domain.PeerAddress, ack ackDelete) {
	session := s.peerSession(address)
	if session == nil || !session.authOK {
		return
	}
	// Built full and stamped at the door it leaves by — the session
	// resolved here is not necessarily the one that writes it, and the
	// queued fallback below may be written after a reconnect.
	frame := s.buildAckDeleteFrame(ack)
	if s.enqueuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ack.Type).Str("id", string(ack.MessageID)).Str("status", ack.Status).Str("mode", "session").Msg("ack_delete_send")
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ack.Type).Str("id", string(ack.MessageID)).Str("status", ack.Status).Str("mode", "queued").Msg("ack_delete_send")
	}
}

// enqueueAckDeleteOnSession writes an ack_delete frame directly to the
// session's sendCh, bypassing the s.sessions lookup used by
// sendAckDeleteToPeer. This is needed because dispatchPeerSessionFrame
// processes frames during initPeerSession (Phase 1), before the session
// is registered in s.sessions (Phase 2). Using sendAckDeleteToPeer in
// that window silently drops the ack because peerSession(address)
// returns nil.
//
// It is the ONE outbound-session enqueue that deliberately does NOT go through
// enqueueSessionSendItem, and the exception is the same bring-up window: the
// peer-state gate refuses a session whose peer is not yet marked connected, and
// during Phase 1 no peer is. The gate would be answering about the ADDRESS
// while this frame is an ack on the very socket that just delivered the message
// — a socket whose reader is provably alive, because it is the one that handed
// us the frame being acked. The queue's own fence still applies through
// enqueueSend, so a session that died is still refused and still falls through
// to the pending queue below.
func (s *Service) enqueueAckDeleteOnSession(session *peerSession, address domain.PeerAddress, ack ackDelete) {
	if session == nil {
		return
	}
	// The FULL frame is what this node knows; the stamp is what THIS
	// session may receive. Keeping them apart matters at the fallback
	// below: a downgraded copy in the pending queue would have thrown the
	// receipt's author away for good, and the peer that eventually drains
	// it may be a current one — which then gets an ack it cannot act on
	// precisely, keeps both contested receipts, and re-pushes them.
	//
	// This is also the one path that writes to a session without passing
	// enqueueSessionSendItem (see the comment above), so it applies the
	// same stamp itself.
	full := s.buildAckDeleteFrame(ack)
	if session.enqueueSend(s.stampAckDeleteForSession(session, legacyPeerSendItem(full))) {
		log.Debug().Str("peer", string(address)).Str("type", ack.Type).Str("id", string(ack.MessageID)).Str("status", ack.Status).Str("mode", "session_direct").Msg("ack_delete_send")
		return
	}
	// sendCh full or already fenced — fall back to pending queue for later
	// drain, holding the full frame: the drain stamps it for whatever
	// session finally carries it.
	if s.queuePeerFrame(address, full) {
		log.Debug().Str("peer", string(address)).Str("type", ack.Type).Str("id", string(ack.MessageID)).Str("status", ack.Status).Str("mode", "queued").Msg("ack_delete_send")
	}
}

// buildAckDeleteFrame constructs a signed ack_delete frame. Extracted from
// sendAckDeleteToPeer so the same frame can be sent on either an outbound
// session (enqueuePeerFrame) or an inbound connection (sendAckDeleteByID).
// buildAckDeleteFrame builds the FULL ack — everything this node knows
// about the receipt, signed. What a particular peer may receive is not
// decided here: see stampAckDeleteForSession. Building the full form
// keeps the author in the frame while it waits in the pending queue, so
// a peer that turns out to support it still gets it.
func (s *Service) buildAckDeleteFrame(ack ackDelete) protocol.Frame {
	frame := protocol.Frame{
		Type:          "ack_delete",
		Address:       s.identity.Address,
		AckType:       ack.Type,
		ID:            string(ack.MessageID),
		Status:        ack.Status,
		ReceiptSender: ack.ReceiptSender,
	}
	frame.Signature = identity.SignPayload(s.identity, ackDeletePayloadForFrame(frame))
	return frame
}

// stampAckDeleteForSession settles the SHAPE of one of our ack_delete
// frames against the session it is about to be written to, and re-signs
// it there.
//
// The version decides what is signed, not merely whether a field is set:
// the receipt's author is inside the payload, so a peer that rebuilds the
// older payload finds a signature it cannot reproduce and scores it as
// forgery — ban points, not a warning. Deciding that where the frame is
// BUILT was wrong twice over: a frame can wait in the pending queue
// across a reconnect and be written to a session that did not exist when
// it was signed, and the build site could only ask about the address,
// while frames are written to a session.
//
// So the shape is settled at the one door every session write passes,
// next to the delivery reference, and for the same reason: a producer
// cannot forget what it never had to remember.
func (s *Service) stampAckDeleteForSession(session *peerSession, item peerSendItem) peerSendItem {
	if session == nil || item.Type != "ack_delete" || item.Address != s.identity.Address {
		return item
	}
	item.Frame = s.buildAckDeleteFrameFor(ackDeleteFromFrame(item.Frame), session.version >= config.ProtocolVersionReceiptSenderAck)
	return item
}

// buildAckDeleteFrameFor is buildAckDeleteFrame narrowed to what one
// destination can verify. Below the floor the frame is byte-identical to
// what this node sent before the field existed.
func (s *Service) buildAckDeleteFrameFor(ack ackDelete, peerCarriesReceiptSender bool) protocol.Frame {
	if !peerCarriesReceiptSender {
		ack.ReceiptSender = ""
	}
	return s.buildAckDeleteFrame(ack)
}

// sendAckDeleteByID writes an ack_delete frame directly on the inbound
// connection identified by connID. This is the inbound-path counterpart of
// sendAckDeleteToPeer: when we receive a push_message on an inbound
// connection and there is no outbound session to that peer, we acknowledge
// on the same conn that delivered the message. The ack is silently dropped
// if the connection has already been unregistered.
func (s *Service) sendAckDeleteByID(connID domain.ConnID, ack ackDelete) {
	core := s.netCoreForID(connID)
	if core == nil {
		return
	}
	frame := s.buildAckDeleteFrameFor(ack, int(core.ProtocolVersion()) >= config.ProtocolVersionReceiptSenderAck)
	// Fire-and-forget inbound write — route through the Network interface
	// so a test backend can intercept it. s.runCtx tracks Service lifecycle;
	// see network_consumer.go for the full outcome-tree contract.
	_ = s.sendFrameViaNetwork(s.runCtx, connID, frame)
	log.Debug().Str("addr", core.RemoteAddr()).Str("type", ack.Type).Str("id", string(ack.MessageID)).Str("status", ack.Status).Str("mode", "inbound_conn").Msg("ack_delete_send")
}

// hasOutboundSessionForInbound checks whether an active outbound session
// already exists for the given inbound peer address. Used during the inbound
// hello handshake to detect duplicate connections — when two nodes dial each
// other simultaneously, both end up with an inbound and an outbound TCP
// connection to the same peer. Keeping both wastes resources and causes
// duplicate entries in diagnostics.
//
// sessions is keyed by the dial address, which may be a fallback-port
// variant (e.g. 10.0.0.1:64647) while the inbound peer declares the
// primary address (10.0.0.1:64646). A direct map lookup would miss
// that match. Instead we resolve both the inbound address and every
// session key through resolveHealthAddress (which maps fallback →
// primary via dialOrigin) and compare the canonical health-tracking
// keys.
//
// Returns true when the inbound connection should be rejected because an
// outbound session already covers this peer.
func (s *Service) hasOutboundSessionForInbound(address domain.PeerAddress) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	target := s.resolveHealthAddress(address)
	for dialAddr := range s.sessions {
		if s.resolveHealthAddress(dialAddr) == target {
			return true
		}
	}
	return false
}

// penalizeOldProtocolPeer applies a score penalty and accumulates ban score
// for a peer whose protocol version is below MinimumProtocolVersion. The
// penalty increments towards the ban threshold — on the 4th incompatible
// attempt the peer gets banned for peerBanIncompatible (24 h).
//
// peerVersion and peerMinimum carry the remote peer's version evidence
// when available (from the wire error frame or welcome); pass 0 when unknown.
func (s *Service) penalizeOldProtocolPeer(address domain.PeerAddress, peerVersion, peerMinimum domain.ProtocolVersion) {
	log.Trace().Str("site", "penalizeOldProtocolPeer").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "penalizeOldProtocolPeer").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)

	// Machine-readable error codes. A pre-handshake incompatible reject
	// supersedes any prior post-handshake disconnect code — keeping an old
	// LastDisconnectCode (e.g. "frame-too-large") alongside the new
	// LastErrorCode ("incompatible-protocol-version") creates a mixed
	// diagnostic snapshot that misrepresents the peer's current state.
	health.LastErrorCode = protocol.ErrCodeIncompatibleProtocol
	health.LastError = "protocol version too old"
	health.LastDisconnectCode = ""
	health.IncompatibleVersionAttempts++
	health.LastIncompatibleVersionAt = time.Now().UTC()

	// Store observed version evidence for diagnostics.
	if peerVersion > 0 {
		health.ObservedPeerVersion = peerVersion
	}
	if peerMinimum > 0 {
		health.ObservedPeerMinimumVersion = peerMinimum
	}

	// Accumulating overlay-level penalty: each incompatible attempt
	// adds peerScoreOldProtocol to the peer quality score.
	health.Score = clampScore(health.Score + peerScoreOldProtocol)
	health.ConsecutiveFailures++

	// Accumulating ban: peerBanIncrementIncompatible per attempt,
	// ban activates when cumulative penalty reaches the overlay ban threshold.
	overlayPenalty := int(health.IncompatibleVersionAttempts) * peerBanIncrementIncompatible
	if overlayPenalty >= peerBanThresholdIncompatible {
		bannedUntil := time.Now().UTC().Add(peerBanIncompatible)
		health.BannedUntil = bannedUntil

		// Propagate ban to the IP level so that other ports on the same
		// host are also excluded from dial candidates.
		//
		// bannedIPSet lives in the IP/advertise domain; nest s.ipStateMu
		// inside the already-held s.peerMu per the canonical peerMu →
		// ipStateMu order documented in docs/locking.md.  s.peers is
		// peer-domain so the sibling enumeration stays outside the
		// ipStateMu window — the map write is the only ipState
		// mutation here.
		if ip, _, ok := splitHostPort(string(address)); ok {
			var affected []domain.PeerAddress
			for _, p := range s.peers {
				if pIP, _, ok2 := splitHostPort(string(p.Address)); ok2 && pIP == ip {
					affected = append(affected, p.Address)
				}
			}
			s.ipStateMu.Lock()
			s.bannedIPSet[ip] = domain.BannedIPEntry{
				IP:            ip,
				BannedUntil:   bannedUntil,
				BanOrigin:     address,
				BanReason:     "incompatible_protocol",
				AffectedPeers: affected,
			}
			s.ipStateMu.Unlock()
		}

		log.Info().
			Str("peer", string(address)).
			Time("banned_until", bannedUntil).
			Int("attempts", int(health.IncompatibleVersionAttempts)).
			Msg("peer_banned_incompatible_protocol")
	} else {
		log.Info().
			Str("peer", string(address)).
			Int("attempts", int(health.IncompatibleVersionAttempts)).
			Int("overlay_penalty", overlayPenalty).
			Int("threshold", peerBanThresholdIncompatible).
			Msg("peer_incompatible_version_penalty_accumulated")
	}

	// Record observation for node-owned update policy and set persisted
	// lockout — but ONLY when the remote peer's minimum exceeds our local
	// protocol version. This enforces two invariants simultaneously:
	//   Invariant A: no evidence → no lockout (peerMinimum must confirm
	//                incompatibility before suppressing dials).
	//   Invariant C: direction guard — only the "they think we're old"
	//                direction feeds the reporter set and lockout. When a
	//                remote peer is below OUR minimum (inbound reject of
	//                an old peer), the ban scoring above still applies, but
	//                the observation must NOT feed the reporter set —
	//                otherwise old peers connecting to us would incorrectly
	//                trigger the "you need to upgrade" signal.
	now := time.Now().UTC()
	if peerMinimum > domain.ProtocolVersion(config.ProtocolVersion) {
		peerID := s.peerIDs[address]
		// statusMu guards s.versionPolicy for both
		// recordIncompatibleObservationLocked and the trailing
		// recomputeVersionPolicyLocked. Acquired INNERMOST per canonical
		// peerMu → statusMu order; held across setVersionLockoutLocked
		// too so the reporter update and the lockout-based signal stay
		// consistent inside a single status-domain section.
		s.statusMu.Lock()
		s.recordIncompatibleObservationLocked(peerID, peerVersion, peerMinimum, now)

		peerClientVer := domain.ClientVersion(s.peerVersions[address])
		s.setVersionLockoutLocked(address, peerVersion, peerMinimum, peerClientVer)

		// Recompute after lockout write: the persisted lockout contributes
		// to update_available via the lockoutSignal path, but
		// recordIncompatibleObservationLocked recomputed before the lockout
		// existed. Without this second recompute the snapshot would be
		// stale until the next unrelated event.
		s.recomputeVersionPolicyLocked(now)
		s.statusMu.Unlock()
	}

	// emitPeerHealthDeltaLocked reads s.pending (delivery-domain, under
	// s.deliveryMu).  Canonical order s.peerMu OUTER → s.deliveryMu INNER.
	s.deliveryMu.RLock()
	s.emitPeerHealthDeltaLocked(health)
	s.deliveryMu.RUnlock()
	s.peerMu.Unlock()
	log.Trace().Str("site", "penalizeOldProtocolPeer").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
}

// markPeerStateInterval is the minimum interval between full state
// recomputes (computePeerStateAtLocked + possible ebus emit +
// aggregate refresh) for the same peer in markPeerWrite / markPeerRead.
//
// The throttle is enforced via peerActivityNanos (sync.Map of
// *atomic.Int64) which lives entirely outside s.peerMu. On the fast path
// (< 1s since last recompute) the functions return immediately with
// zero locking. Only when the interval elapses does the slow path
// acquire s.peerMu.Lock(), flush timestamps into health, and run the
// state machine. This eliminates the continuous writer pressure that
// previously starved s.peerMu.RLock() callers (loadConversation,
// fetch_network_stats).
const markPeerStateInterval = time.Second

// peerActivityNeedsRecompute checks the per-peer atomic timestamp in
// peerActivityNanos and returns true only when the recompute interval
// has elapsed since the last recompute. Uses CAS to guarantee exactly
// one goroutine wins the race for a given peer in the same interval.
// No locks are acquired — the check is fully lock-free.
//
// The interval defaults to markPeerStateInterval (1 s). Tests can set
// markPeerStateIntervalTest to -1 to disable throttling entirely.
func (s *Service) peerActivityNeedsRecompute(address domain.PeerAddress, nowNano int64) bool {
	interval := int64(markPeerStateInterval)
	if s.markPeerStateIntervalTest < 0 {
		return true // test mode: always recompute
	}
	if s.markPeerStateIntervalTest > 0 {
		interval = int64(s.markPeerStateIntervalTest)
	}

	v, _ := s.peerActivityNanos.LoadOrStore(address, &atomic.Int64{})
	last := v.(*atomic.Int64)
	prev := last.Load()
	if nowNano-prev < interval {
		return false
	}
	return last.CompareAndSwap(prev, nowNano)
}

// peerActivityEvictInterval / peerActivityEvictWindow drive
// evictStalePeerActivity: at most one full sweep per interval, dropping
// entries idle longer than the window. The window only has to be long
// enough that an ACTIVE peer's entry is never churned (useful traffic
// refreshes the timestamp on every recompute, i.e. at least once per
// markPeerStateInterval while frames flow); idle entries — dominated by
// never-returning ephemeral inbound addresses — are pure residue.
const (
	peerActivityEvictInterval = time.Minute
	peerActivityEvictWindow   = 10 * time.Minute
)

// evictStalePeerActivity reclaims peerActivityNanos entries whose last
// recompute is older than peerActivityEvictWindow. The map is keyed by
// RAW address (see the field doc), so ephemeral inbound ports accumulate
// one entry per reconnect and no other sweep covers them. Runs lock-free,
// matching the domain: a racing peerActivityNeedsRecompute may lose its
// CAS'd timestamp to our Delete, which merely forces that peer's next
// recompute one interval early.
func (s *Service) evictStalePeerActivity(nowNano int64) {
	lastSweep := s.peerActivitySweepNanos.Load()
	if nowNano-lastSweep < int64(peerActivityEvictInterval) ||
		!s.peerActivitySweepNanos.CompareAndSwap(lastSweep, nowNano) {
		return
	}
	cutoff := nowNano - int64(peerActivityEvictWindow)
	s.peerActivityNanos.Range(func(key, value any) bool {
		if value.(*atomic.Int64).Load() < cutoff {
			s.peerActivityNanos.Delete(key)
		}
		return true
	})
}

func (s *Service) markPeerWrite(address domain.PeerAddress, frame protocol.Frame) {
	log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", string(address)).
		Str("direction", "send").
		Str("command", frame.Type).
		Bool("accepted", true).
		Msg("protocol_trace")

	now := time.Now().UTC()
	// Ping/pong frames bypass the throttle — they are low-frequency
	// (~30 s) and critical for health: suppressing LastPingAt updates
	// makes computePeerStateAtLocked see a stale timestamp, degrading
	// the peer and eventually killing the connection.
	if frame.Type != "ping" && !s.peerActivityNeedsRecompute(address, now.UnixNano()) {
		return
	}

	log.Trace().Str("site", "markPeerWrite").Str("phase", "lock_wait").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "markPeerWrite").Str("phase", "lock_held").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "markPeerWrite").Str("phase", "lock_released").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	}()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	if frame.Type == "ping" {
		health.LastPingAt = now
	} else if frame.Type != "" {
		health.LastUsefulSendAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
}

func (s *Service) markPeerRead(address domain.PeerAddress, frame protocol.Frame) {
	accepted := frame.Type != "error"
	ev := log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", string(address)).
		Str("direction", "recv").
		Str("command", frame.Type).
		Bool("accepted", accepted)
	if frame.Type == "error" {
		ev = ev.Str("code", frame.Code).Str("error", frame.Error)
	}
	ev.Msg("protocol_trace")

	now := time.Now().UTC()
	// Pong frames bypass the throttle — they are low-frequency (~30 s)
	// and critical for health: suppressing LastPongAt updates makes
	// computePeerStateAtLocked see a stale timestamp, degrading the
	// peer and eventually killing the connection.
	if frame.Type != "pong" && !s.peerActivityNeedsRecompute(address, now.UnixNano()) {
		return
	}

	log.Trace().Str("site", "markPeerRead").Str("phase", "lock_wait").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "markPeerRead").Str("phase", "lock_held").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "markPeerRead").Str("phase", "lock_released").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	}()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	if frame.Type == "pong" {
		health.LastPongAt = now
	} else if frame.Type != "" {
		health.LastUsefulReceiveAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
}

func (s *Service) markPeerUsefulReceive(address domain.PeerAddress) {
	now := time.Now().UTC()
	if !s.peerActivityNeedsRecompute(address, now.UnixNano()) {
		return
	}

	log.Trace().Str("site", "markPeerUsefulReceive").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "markPeerUsefulReceive").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	defer func() {
		s.peerMu.Unlock()
		log.Trace().Str("site", "markPeerUsefulReceive").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	}()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	health.LastUsefulReceiveAt = now
	s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
}

// nextConnIDLocked returns a monotonically increasing connection ID.
// Mutates s.connIDCounter, which is peer-domain state — caller MUST
// hold s.peerMu write lock.
func (s *Service) nextConnIDLocked() domain.ConnID {
	s.connIDCounter++
	return domain.ConnID(s.connIDCounter)
}

// inboundConnIDsLocked returns the connection IDs for all active inbound
// connections that declared the given overlay address in their hello frame.
// Reads peer-domain inbound-conn state — caller MUST hold s.peerMu
// (read or write).
func (s *Service) inboundConnIDsLocked(address domain.PeerAddress) []uint64 {
	var ids []uint64
	// Lightweight scan: connID-by-address needs no capabilities, so skip the
	// snapshotEntryLocked cloneCaps copy (forEachInboundConnIDLocked). The
	// registry holds both directions; outbound NetCores are filtered out by
	// the helper (Dir != Inbound) and surface through s.sessions once active.
	s.forEachInboundConnIDLocked(func(id domain.ConnID, addr domain.PeerAddress, _ bool) bool {
		if addr == address {
			ids = append(ids, uint64(id))
		}
		return true
	})
	return ids
}

// inboundConnIDForAddressLocked returns the ConnID of an authenticated
// inbound connection for the given overlay address, or zero value and
// false if none exists. When multiple connections are active, any one of
// them is returned (all are equally valid for fire-and-forget writes).
// Reads peer-domain inbound-conn state — caller MUST hold s.peerMu
// (read or write). ConnID-first (PR 10.6): callers resolve the
// transport through the registry rather than holding a raw net.Conn
// across the lock boundary.
func (s *Service) inboundConnIDForAddressLocked(address domain.PeerAddress) (domain.ConnID, bool) {
	var result domain.ConnID
	var found bool
	// Lightweight scan (no cloneCaps): this is the gossip-relay send hot
	// path, called per message — it only needs id/address/tracked, never
	// capabilities. See forEachInboundConnIDLocked.
	s.forEachInboundConnIDLocked(func(id domain.ConnID, addr domain.PeerAddress, tracked bool) bool {
		// Only return tracked connections for the given address.
		if addr != address {
			return true
		}
		// The walker already filters to inbound; the tracked flag is the
		// single source of truth here, no second registry hop required.
		if tracked {
			result = id
			found = true
			return false // Stop iteration
		}
		return true
	})
	return result, found
}

func (s *Service) ensurePeerHealthLocked(address domain.PeerAddress) *peerHealth {
	health := s.health[address]
	if health == nil {
		health = &peerHealth{
			Address: address,
			State:   peerStateReconnecting,
		}
		s.health[address] = health
	}
	return health
}

// resetPeerHealthForRecoveryLocked clears all failure-related fields on a
// peerHealth entry. This is the single source of truth for the "compatibility
// recovery" contract: every code path that proves a peer is compatible
// (markPeerConnected) or explicitly overrides penalties (addPeerFrame)
// calls this helper instead of resetting fields inline.
//
// Cleared fields and their effects:
//   - LastError, LastErrorCode, LastDisconnectCode — diagnostic strings
//     (LastError depresses ranking via len(health.LastError) in rankPeerHealth)
//   - IncompatibleVersionAttempts, LastIncompatibleVersionAt — version ban counters
//   - ObservedPeerVersion, ObservedPeerMinimumVersion — stale version evidence
//   - ConsecutiveFailures, LastDisconnectedAt — exponential cooldown inputs
//   - BannedUntil — address-level ban
//   - Score floor to 0 — neutralises stale peerScoreOldProtocol penalties
//     without inflating peers that already have positive scores
//
// The caller remains responsible for IP-wide ban clearing and score bonuses
// (e.g. peerScoreConnect on handshake) because those depend on call-site context.
func resetPeerHealthForRecoveryLocked(h *peerHealth) {
	h.LastError = ""
	h.LastErrorCode = ""
	h.LastDisconnectCode = ""
	h.IncompatibleVersionAttempts = 0
	h.LastIncompatibleVersionAt = time.Time{}
	h.ObservedPeerVersion = 0
	h.ObservedPeerMinimumVersion = 0
	h.ConsecutiveFailures = 0
	h.LastDisconnectedAt = time.Time{}
	h.BannedUntil = time.Time{}
	if h.Score < 0 {
		h.Score = 0
	}
}

// updatePeerStateLocked transitions a peer's State field and fans out side
// effects (aggregate status recomputation + ebus delta).
//
// Caller MUST hold s.peerMu.Lock.  This function internally acquires
// s.deliveryMu.RLock for the full body — both the pending log line and
// publishAggregateStatusChangedLocked need to read s.pending.
// It also nests s.statusMu.Lock around publishAggregateStatusChangedLocked
// because s.aggregateStatus / s.lastPublishedAggregateStatus /
// s.lastAggregateStatusPublishAt live in the status domain.
//
// Canonical order: peerMu → deliveryMu → statusMu with statusMu INNERMOST.
func (s *Service) updatePeerStateLocked(health *peerHealth, next string) {
	s.deliveryMu.RLock()
	defer s.deliveryMu.RUnlock()
	if health.State == next {
		// State unchanged — still emit the delta so that timestamp updates
		// (LastPongAt, LastUsefulReceiveAt) reach ebus subscribers.
		// Callers are already throttled by peerActivityNeedsRecompute
		// (~1 event/sec/peer), so this does not create writer pressure.
		s.emitPeerHealthDeltaLocked(health)
		return
	}
	if health.State != "" {
		pendingCount := len(s.pending[health.Address])
		log.Info().Str("peer", string(health.Address)).Str("from", health.State).Str("to", next).Int("pending", pendingCount).Int("failures", health.ConsecutiveFailures).Msg("peer_state_change")
	}
	health.State = next

	// Any per-peer state transition may shift the aggregate network status
	// (e.g. the last usable peer going stalled moves aggregate from healthy
	// to limited). Recompute, store the materialized snapshot, and publish
	// TopicAggregateStatusChanged only when the semantic payload differs —
	// the helper owns the no-op gate that keeps peer-storm bursts from
	// stampeding the UI with byte-identical snapshots.
	s.statusMu.Lock()
	s.publishAggregateStatusChangedLocked()
	s.statusMu.Unlock()

	s.emitPeerHealthDeltaLocked(health)
}

// emitPeerHealthDeltaLocked publishes a full PeerHealthDelta for the given
// peer. Single point of construction for all health-delta events — called
// from updatePeerStateLocked (state transitions and timestamp updates) and
// penalizeOldProtocolPeer (incompatible version handling).
//
// Callers are rate-limited by peerActivityNeedsRecompute (~1 call/sec/peer)
// and pong bypass (~30 s interval), so emission frequency is bounded and
// does not create writer pressure on s.peerMu.
//
// Caller MUST hold s.peerMu at least for read (peer-domain fields
// s.peerIDs / s.peerVersions / s.peerBuilds / s.persistedMeta are read
// here) AND s.deliveryMu at least for read (s.pending feeds PendingCount).
// statusMu is NOT required — this helper reads no status-domain fields.
// Keeping the delivery RLock in the caller avoids nested s.deliveryMu.RLock
// recursion, which is unsafe on a writer-preferring RWMutex when another
// writer is queued.
func (s *Service) emitPeerHealthDeltaLocked(health *peerHealth) {
	pendingCount := len(s.pending[health.Address])
	delta := ebus.PeerHealthDelta{
		Address:             health.Address,
		PeerID:              s.peerIDs[health.Address],
		Direction:           health.Direction,
		ClientVersion:       s.peerVersions[health.Address],
		ClientBuild:         s.peerBuilds[health.Address],
		State:               health.State,
		Connected:           health.Connected,
		Score:               health.Score,
		PendingCount:        pendingCount,
		ConsecutiveFailures: health.ConsecutiveFailures,
		LastConnectedAt:     ebus.TimePtr(health.LastConnectedAt),
		LastDisconnectedAt:  ebus.TimePtr(health.LastDisconnectedAt),
		LastPingAt:          ebus.TimePtr(health.LastPingAt),
		LastPongAt:          ebus.TimePtr(health.LastPongAt),
		LastUsefulSendAt:    ebus.TimePtr(health.LastUsefulSendAt),
		LastUsefulReceiveAt: ebus.TimePtr(health.LastUsefulReceiveAt),
		LastError:           health.LastError,

		// Diagnostic fields — mirror peerHealthFrames() so operator-facing
		// UI stays current after the switch to one-shot FetchAndSeed().
		// Without these, ban clears, handshake rejections, and recovery
		// events reach the UI only on the startup probe and never again.
		BannedUntil:                 ebus.TimePtr(health.BannedUntil),
		LastErrorCode:               health.LastErrorCode,
		LastDisconnectCode:          health.LastDisconnectCode,
		IncompatibleVersionAttempts: health.IncompatibleVersionAttempts,
		LastIncompatibleVersionAt:   ebus.TimePtr(health.LastIncompatibleVersionAt),
		ObservedPeerVersion:         health.ObservedPeerVersion,
		ObservedPeerMinimumVersion:  health.ObservedPeerMinimumVersion,
		VersionLockoutActive:        s.isPeerVersionLockedOutLocked(health.Address),
	}
	if session := s.resolveSessionLocked(health.Address); session != nil {
		delta.ConnID = uint64(session.connID)
		delta.ProtocolVersion = session.version
	}
	// Snapshot active inbound connections so the monitor can reconcile
	// per-ConnID rows — creating rows for new inbound connections and
	// pruning rows for connections that no longer exist.
	delta.InboundConnIDs = s.inboundConnIDsLocked(health.Address)
	s.eventBus.Publish(ebus.TopicPeerHealthChanged, delta)
}

// emitPeerPendingChanged publishes a lightweight TopicPeerPendingChanged event.
// Called after queue mutations (enqueue, flush, expiry) so subscribers can
// update the per-peer pending badge without waiting for the next state
// transition. Must be called WITHOUT any Service mutex held (Publish is
// non-blocking but the publisher must not retain peer-domain locks around
// unbounded downstream handlers).
func (s *Service) emitPeerPendingChanged(address domain.PeerAddress, count int) {
	s.eventBus.Publish(ebus.TopicPeerPendingChanged, ebus.PeerPendingDelta{
		Address: address,
		Count:   count,
	})
}

// peerHealthFrames returns the fetch_peer_health RPC body.
//
// The hot path is statically decoupled from both s.peerMu and cm.mu.
// All per-peer state is read from s.peerHealthSnap; all CM-slot fields
// are read from s.cmSlotsSnap.  Both snapshots are rebuilt every
// networkStatsSnapshotInterval by hotReadsRefreshLoop (see
// peer_health_snapshot.go, cm_slots_snapshot.go) and primed synchronously
// by primeHotReadSnapshots() from Run() before the listener opens — so
// this handler performs only atomic loads, never synchronously rebuilds,
// and therefore never reaches cm.mu.RLock or s.peerMu.RLock on the RPC
// goroutine.  A writer holding s.peerMu for many seconds can stall the
// refresher ticks but cannot stall this handler.
//
// If either atomic load ever returns nil (refresher goroutine crashed or
// a unit test that bypasses Run() invokes the handler without priming),
// the handler returns nil rather than falling back to a synchronous
// rebuild: taking the locks here would reintroduce the starvation shape
// the snapshot infrastructure exists to eliminate.  Tests that invoke
// this handler directly must prime the snapshots explicitly (mirrors the
// pattern in peer_health_snapshot_test.go).
func (s *Service) peerHealthFrames() []protocol.PeerHealthFrame {
	// Record that a consumer read peer-health now.  maybeRebuildPeerHealthSnapshot
	// consults this so the periodic 500 ms rebuild — a top allocator on a
	// headless server where nobody polls fetch_peer_health — is skipped while
	// no reader is active and resumes the moment one returns.  See
	// peerHealthRebuildIdleAfter.
	s.peerHealthAccessNanos.Store(time.Now().UnixNano())

	// Load the cached ConnectionManager slots view instead of calling
	// cm.Slots() on the RPC path.  Slots() takes cm.mu.RLock which, under
	// writer-preferring semantics, would queue behind any CM writer and
	// re-introduce the reader-starvation shape the s.peerMu decoupling
	// eliminated.  See cm_slots_snapshot.go for the contract.
	slotsSnap := s.loadCMSlotsSnapshot()

	// Capture snapshots — independent of s.peerMu; captureManager owns
	// its own mutex.  Done before loading the health snapshot so the two
	// views are as closely aligned as possible in wall-clock time.
	captureByConn := make(map[domain.ConnID]captureSnap)
	if cm := s.captureManager; cm != nil {
		for _, snap := range cm.AllSessionSnapshots() {
			captureByConn[snap.ConnID] = captureSnap{
				Recording:  snap.Recording,
				File:       snap.FilePath,
				StartedAt:  snap.StartedAt.UTC().Format(time.RFC3339Nano),
				Scope:      snap.Scope.String(),
				Error:      snap.Error,
				DroppedEvt: snap.DroppedEvents,
			}
		}
	}

	snap := s.loadPeerHealthSnapshot()
	if snap == nil {
		// No primed snapshot and no refresher tick yet — should not happen
		// in production because Run() calls primeHotReadSnapshots() before
		// the listener opens.  Return nil rather than synchronously
		// rebuilding (which would take s.peerMu.RLock on the RPC goroutine and
		// break the lock-free contract).
		return nil
	}

	items := make([]protocol.PeerHealthFrame, 0, len(snap.records)+len(snap.inboundOnly))
	for _, rec := range snap.records {
		h := &rec.health
		phf := protocol.PeerHealthFrame{
			Address:             string(h.Address),
			PeerID:              rec.peerID.String(),
			Network:             classifyAddress(h.Address).String(),
			Direction:           string(h.Direction),
			ClientVersion:       rec.clientVersion,
			ClientBuild:         rec.clientBuild,
			State:               h.State,
			Connected:           h.Connected,
			PendingCount:        rec.pendingCount,
			LastConnectedAt:     formatTime(h.LastConnectedAt),
			LastDisconnectedAt:  formatTime(h.LastDisconnectedAt),
			LastPingAt:          formatTime(h.LastPingAt),
			LastPongAt:          formatTime(h.LastPongAt),
			LastUsefulSendAt:    formatTime(h.LastUsefulSendAt),
			LastUsefulReceiveAt: formatTime(h.LastUsefulReceiveAt),
			ConsecutiveFailures: h.ConsecutiveFailures,
			LastError:           h.LastError,
			Score:               h.Score,
			BannedUntil:         formatTime(h.BannedUntil),
			BytesSent:           h.BytesSent,
			BytesReceived:       h.BytesReceived,
			TotalTraffic:        h.BytesSent + h.BytesReceived,
			Capabilities:        rec.capabilities,

			// Machine-readable disconnect diagnostics.
			LastErrorCode:               h.LastErrorCode,
			LastDisconnectCode:          h.LastDisconnectCode,
			IncompatibleVersionAttempts: int(h.IncompatibleVersionAttempts),
			LastIncompatibleVersionAt:   formatTime(h.LastIncompatibleVersionAt),
			ObservedPeerVersion:         int(h.ObservedPeerVersion),
			ObservedPeerMinimumVersion:  int(h.ObservedPeerMinimumVersion),
			VersionLockoutActive:        rec.versionLockoutActive,

			ProtocolVersion: rec.sessionVersion,
			ConnID:          uint64(rec.sessionConnID),
		}
		// Enrich with CM slot lifecycle data if this peer has an outbound
		// slot.  Read from the cached snapshot's byAddress index — no
		// cm.mu.RLock on the RPC path.
		if slotsSnap != nil {
			if sl, ok := slotsSnap.byAddress[h.Address]; ok {
				// PeerHealthFrame.SlotState is a wire-format string; convert
				// from the typed domain.SlotState at the transport boundary.
				phf.SlotState = string(sl.State)
				phf.SlotRetryCount = sl.RetryCount
				phf.SlotGeneration = sl.Generation
				if sl.ConnectedAddress != nil {
					phf.SlotConnectedAddr = string(*sl.ConnectedAddress)
				}
			}
		}
		// When an outbound session exists, emit a single row with the
		// outbound ConnID — even if inbound connections coexist (both
		// directions are now allowed for simultaneous dials). For
		// inbound-only peers, emit one row per active TCP connection so
		// UI/diagnostics can distinguish multiple sessions to the same
		// overlay address.
		if phf.ConnID != 0 {
			enrichCaptureFields(&phf, captureByConn)
			items = append(items, phf)
		} else {
			if len(rec.inboundConnIDs) > 0 {
				for _, cid := range rec.inboundConnIDs {
					row := phf
					row.ConnID = cid
					row.Direction = string(peerDirectionInbound)
					enrichCaptureFields(&row, captureByConn)
					items = append(items, row)
				}
			} else {
				items = append(items, phf)
			}
		}
	}

	// Inbound-only peers — live traffic without a health entry yet.
	for _, ilr := range snap.inboundOnly {
		inboundPHF := protocol.PeerHealthFrame{
			Address:         string(ilr.address),
			PeerID:          ilr.peerID.String(),
			Network:         classifyAddress(ilr.address).String(),
			Direction:       string(peerDirectionInbound),
			ClientVersion:   ilr.clientVersion,
			ClientBuild:     ilr.clientBuild,
			State:           peerStateHealthy,
			Connected:       true,
			BytesSent:       ilr.sent,
			BytesReceived:   ilr.received,
			TotalTraffic:    ilr.sent + ilr.received,
			Capabilities:    ilr.capabilities,
			ProtocolVersion: ilr.sessionVersion,
		}
		if len(ilr.inboundConnIDs) > 0 {
			for _, cid := range ilr.inboundConnIDs {
				row := inboundPHF
				row.ConnID = cid
				enrichCaptureFields(&row, captureByConn)
				items = append(items, row)
			}
		} else {
			items = append(items, inboundPHF)
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Address < items[j].Address
	})
	return items
}

// captureSnap is the pre-fetched, lock-free view of a capture session
// used to enrich PeerHealthFrame without holding s.peerMu and m.mu
// together.
type captureSnap struct {
	Recording  bool
	File       string
	StartedAt  string
	Scope      string
	Error      string
	DroppedEvt int64
}

// enrichCaptureFields populates the recording_* fields on a PeerHealthFrame
// from the pre-fetched capture snapshot map. ConnID must already be set.
func enrichCaptureFields(phf *protocol.PeerHealthFrame, snaps map[domain.ConnID]captureSnap) {
	if phf.ConnID == 0 {
		return
	}
	snap, ok := snaps[domain.ConnID(phf.ConnID)]
	if !ok {
		return
	}
	phf.Recording = snap.Recording
	phf.RecordingFile = snap.File
	phf.RecordingStartedAt = snap.StartedAt
	phf.RecordingScope = snap.Scope
	phf.RecordingError = snap.Error
	phf.RecordingDroppedEvents = snap.DroppedEvt
}

// peerCapabilitiesFromIndexLocked returns the negotiated capabilities for a
// peer as wire-format strings for PeerHealthFrame.  An active outbound
// session's capabilities win; otherwise it falls back to the inbound
// capabilities supplied via the pre-built address→caps index.
//
// The index is the point of this helper: rebuildPeerHealthSnapshot builds it
// in a single s.conns pass and calls this per peer, so the inbound fallback
// is an O(1) map lookup instead of a full forEachInboundConnLocked scan (with
// a capability clone per entry) per peer — the old per-peer form was
// O(peers × conns) and a top allocator in profiling.  Caller MUST hold
// s.peerMu; the index MUST have been built under the same lock hold so the
// session and inbound views are consistent.
func (s *Service) peerCapabilitiesFromIndexLocked(address domain.PeerAddress, inboundCaps map[domain.PeerAddress][]string) []string {
	if session := s.resolveSessionLocked(address); session != nil && len(session.capabilities) > 0 {
		return domain.CapabilityStrings(session.capabilities)
	}
	return inboundCaps[address]
}

func (s *Service) computePeerStateAtLocked(health *peerHealth, now time.Time) string {
	if !health.Connected {
		return peerStateReconnecting
	}

	lastUseful := health.LastUsefulReceiveAt
	// A pong response is proof of liveness — use the most recent of
	// LastUsefulReceiveAt and LastPongAt. Previously LastPongAt was
	// only a fallback when LastUsefulReceiveAt was zero, which caused
	// idle-but-responsive peers to drift into stalled after ~2:45.
	if health.LastPongAt.After(lastUseful) {
		lastUseful = health.LastPongAt
	}
	if lastUseful.IsZero() {
		return peerStateDegraded
	}

	// Thresholds are based on heartbeatInterval (~2 min).
	// degraded: no useful response for longer than one heartbeat cycle + stall timeout buffer.
	// stalled:  no useful response for two full heartbeat cycles — peer is unreachable.
	age := now.Sub(lastUseful)
	switch {
	case age >= heartbeatInterval+pongStallTimeout:
		return peerStateStalled
	case age >= heartbeatInterval:
		return peerStateDegraded
	default:
		return peerStateHealthy
	}
}

// computePeerStateLocked is a convenience wrapper for infra code paths that
// already want "state as of now". Business logic that coordinates multiple
// decisions in one flow should prefer computePeerStateAtLocked with an
// explicit shared timestamp.
func (s *Service) computePeerStateLocked(health *peerHealth) string {
	return s.computePeerStateAtLocked(health, time.Now().UTC())
}

// peerHealthAcceptsOutboundFramesLocked is THE predicate behind "may this
// connection still be handed a frame". Every send path that picks a connection
// and every queue that admits one asks it and nothing else, so the selection
// and the hand-over cannot disagree about which sockets are alive.
//
// It is a WHITELIST of the states that may be sent to, and that shape is the
// whole finding it fixes rather than a stylistic preference. The blacklist it
// replaces named peerStateStalled and only that, while a session that just died
// leaves its peer in peerStateReconnecting: markPeerDisconnected flips the
// health entry and servePeerSession fences the queue one deferred call LATER,
// so in that window the frame passed the gate, landed in a queue nobody would
// ever drain to the socket, and the producer read the acceptance as a delivery
// — the datagram emitter stops its candidate walk on it, so the frame was lost
// INSTEAD of going to the peer's next connection. A state added later refuses
// by default instead of repeating that.
//
// A missing health entry refuses for the reason the outbound tier of
// peerSendableConnectionsLocked already documents: bring-up inserts the session
// into s.sessions BEFORE markPeerConnected, and in that window the session is
// not authoritative for sends.
//
// Caller must hold s.peerMu (read or write).
func (s *Service) peerHealthAcceptsOutboundFramesLocked(health *peerHealth, now time.Time) bool {
	if health == nil {
		return false
	}
	switch s.computePeerStateAtLocked(health, now) {
	case peerStateHealthy, peerStateDegraded:
		return true
	default:
		// peerStateReconnecting (health.Connected == false) and
		// peerStateStalled both mean the socket behind this address is not
		// carrying traffic any more.
		return false
	}
}

// peerAcceptsOutboundFrames asks the same question of an ADDRESS, resolving it
// to its canonical health entry first.
//
// Takes s.peerMu.RLock itself, so callers must hold no domain mutex — every
// call site is a queue admission, which by the rule in docs/locking.md happens
// outside every domain mutex anyway.
func (s *Service) peerAcceptsOutboundFrames(address domain.PeerAddress) bool {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	health := s.health[s.resolveHealthAddress(address)]
	return s.peerHealthAcceptsOutboundFramesLocked(health, time.Now().UTC())
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func (s *Service) enqueuePeerFrame(address domain.PeerAddress, frame protocol.Frame) bool {
	return s.enqueuePeerSendItem(address, legacyPeerSendItem(frame))
}

// enqueuePeerSendItem is the admission point into the upper outbound queue for
// a caller that knows only the peer's ADDRESS: it resolves the address to the
// session currently registered under it and hands the item on. A caller that
// already holds the session must go straight to enqueueSessionSendItem — see
// the contract there for why.
func (s *Service) enqueuePeerSendItem(address domain.PeerAddress, item peerSendItem) bool {
	session, ok := s.activePeerSession(address)
	if !ok || session == nil {
		return false
	}
	return s.enqueueSessionSendItem(session, item)
}

// enqueueSessionSendItem is the admission into the upper outbound queue of ONE
// session, and it is the whole of that admission: the peer-state gate and the
// queue fence stay in this one place for EVERY entry point. Anything that puts
// a frame on an outbound session goes through here — the address-keyed helper
// above, the tracked senders below, the announce fan-out, the pending-ring
// flush and the identity-addressed walk — so a state that must not be sent to
// is refused once rather than in each of them.
//
// The two gates answer different questions and both are needed. The peer-state
// gate (peerAcceptsOutboundFrames) says the PEER is no longer carrying traffic;
// the fence inside enqueueSend says THIS SESSION's queue is dead. Neither
// implies the other: a peer whose session died is disconnected for a whole
// window before the serve loop runs its deferred closeSendQueue, and a session
// retired by a reconnect sits under an address whose health has meanwhile gone
// back to connected.
//
// It takes the SESSION and not the address because the two stop naming the same
// socket the moment the peer reconnects: s.sessions[address] is rebound to a
// fresh peerSession with a fresh ConnID, and a caller that picked its
// connection earlier — the datagram emitter picks one per frame, after gating
// its handshake — would silently have its frame delivered over the successor.
// A session that died in the meantime refuses through its own fence
// (enqueueSend), which is the answer such a caller wants: the walk moves on to
// the next connection instead of being handed a substitute.
func (s *Service) enqueueSessionSendItem(session *peerSession, item peerSendItem) bool {
	if session == nil {
		return false
	}
	// A frame carrying one of OUR messages is completed with the dispatch
	// it belongs to HERE, so the serve loop's pre-wire gate and its
	// confirmation apply to every producer without any of them having to
	// remember. A producer that already knows the dispatch — the retry
	// tick, which fans one attempt out to several sinks — keeps its own.
	// See outbound_delivery_gate.go: this is the reason the reference
	// travels with the frame instead of being an argument every send site
	// could forget to pass.
	item = s.withDeliveryRef(item, time.Now().UTC())
	// An ack we authored is signed for the version of the session that
	// actually writes it — see stampAckDeleteForSession.
	item = s.stampAckDeleteForSession(session, item)
	// peerAcceptsOutboundFrames resolves a dial address onto its canonical
	// primary, so asking with the session's own address reaches the same health
	// entry the address-keyed caller above would have reached.
	if !s.peerAcceptsOutboundFrames(session.address) {
		return false
	}
	s.runSendAdmissionBarrier()
	return session.enqueueSend(item)
}

// runSendAdmissionBarrier fires the test-only synchronisation point described
// on the sendAdmissionBarrier field. Both tiers call it at the same point of
// their admission — after the peer-state gate answered, before the frame is
// offered to a queue — because that is the one window in which a teardown can
// still turn a queue that would have sent the frame into one that discards it.
func (s *Service) runSendAdmissionBarrier() {
	if s.sendAdmissionBarrier != nil {
		s.sendAdmissionBarrier()
	}
}

// runPeerTeardownBarrier fires the test-only synchronisation point described on
// the peerTeardownBarrier field. Both teardowns that own a queue call it at the
// same point — between the fence and the disconnect publication — because that
// is the only place from which the ORDER of those two is observable at all.
func (s *Service) runPeerTeardownBarrier() {
	if s.peerTeardownBarrier != nil {
		s.peerTeardownBarrier()
	}
}

// sendTrackedFrameToSession enqueues frame on the upper outbound queue of THIS
// session with an outbound contract attached: a send deadline re-checked by
// the writer immediately before the socket write, and a per-frame write grace.
//
// The ticket is MINTED BY THE CALLER, and that is the contract: one ticket
// belongs to one queue element, so a caller that offers the same frame to
// several connections mints one per offer. A nil ticket is the legacy,
// untracked frame.
//
// Returns false when the frame was refused (fenced queue, a peer that no
// longer accepts outbound frames, saturated queue). The frame provably never
// started a write in that case, so the caller is free to try another
// connection.
func (s *Service) sendTrackedFrameToSession(
	session *peerSession,
	frame protocol.Frame,
	ticket *netcore.WriteTicket,
	writeAck chan struct{},
) bool {
	return s.enqueueSessionSendItem(session, peerSendItem{Frame: frame, ticket: ticket, writeAck: writeAck})
}

// sendTrackedFrameToConn is the accepted-connection twin of
// sendTrackedFrameToSession: it attaches the same outbound contract to a frame
// destined for the connection registered under id.
//
// The inbound direction has NO peerSession and therefore only ONE queue — the
// NetCore writer's. That asymmetry is the whole reason the contract travels
// with the queue element instead of living in a peerSession-side table: the
// same ticket type has to work where the upper queue does not exist at all.
//
// It is keyed on the ConnID for the same reason the outbound twin is keyed on
// the session object. The remote ADDRESS of an accepted connection outlives the
// connection — a peer reconnecting from the same host:port produces a second
// one under the same key — so resolving the write address by it would hand the
// frame to the successor of the socket the caller chose.
//
// The ticket is minted by the caller for the same reason as in
// sendTrackedFrameToSession: one ticket, one queue element.
//
// The peer-state gate is the SAME one the outbound twin applies, and it is here
// for the same reason: peerSendableConnectionsLocked judged this connection
// under peerMu, the lock was released before the hand-over, and a peer that went
// away in between must send the caller on to its next candidate rather than
// have the frame accepted by a queue whose socket is finished. Asking exactly
// what the selection asked — and not a second, stricter question of this tier's
// own — is what keeps "the metadata describes the connection the send will try"
// true across the gap.
//
// Returns false when the connection is unknown, when its peer no longer accepts
// outbound frames, or when its queue answered anything other than SendOK.
// Unlike the outbound twin above, that false is not a proof: the outbound queue
// refuses at the door, while this one also answers a frame that is already in it
// and read a shut gate — a frame written just before a LATER frame killed the
// link lands there. The caller walks on to the next connection of the same peer,
// so the cost of the imprecise half is a duplicate the receiving side drops, not
// a lost frame.
func (s *Service) sendTrackedFrameToConn(
	id domain.ConnID,
	frame protocol.Frame,
	ticket *netcore.WriteTicket,
	writeAck chan struct{},
) bool {
	core := s.netCoreForID(id)
	if core == nil {
		return false
	}
	if !s.peerAcceptsOutboundFrames(core.Address()) {
		return false
	}
	s.runSendAdmissionBarrier()
	return core.SendTrackedObserved(frame, ticket, writeAck) == netcore.SendOK
}

// pendingRingSize resolves the per-peer pending ring capacity: the operator
// override (CORSA_PENDING_RING_SIZE / config.Node.PendingRingSize) when
// positive, otherwise the built-in maxPendingFramesPerPeer default.
func (s *Service) pendingRingSize() int {
	if n := s.cfg.PendingRingSize; n > 0 {
		return n
	}
	return maxPendingFramesPerPeer
}

// capPendingRingLocked enforces the per-peer pending RING bound on
// s.pending[primary]: if it holds more than ringSize frames, the OLDEST
// excess (by QueuedAt) are evicted and their pendingKeys + outbound
// bookkeeping released. This is the single chokepoint every write path into
// s.pending must funnel through so the hard memory cap holds regardless of HOW
// the queue grew — direct enqueue (queuePeerFrame), the flushPendingPeerFrames
// re-queue of undelivered frames, and the drainPendingForIdentities merge-back
// can each re-append frames past the bound, and without this they would let a
// peer's queue grow unbounded across reconnect/route-churn cycles.
//
// Eviction is by QueuedAt (not slice position) because the re-queue / merge
// paths do not preserve append-order==age; ties keep input order (stable) so
// the choice is deterministic. Survivor order is preserved — delivery happens
// in slice order — and survivors are copied into a fresh ringSize-capacity
// slice so the larger old backing array is released to the GC instead of being
// retained behind a reslice. Caller MUST hold s.deliveryMu.Lock. A ringSize of
// 0 (or below) empties the peer's queue entirely.
func (s *Service) capPendingRingLocked(primary domain.PeerAddress, ringSize int) {
	if ringSize < 0 {
		ringSize = 0
	}
	queue := s.pending[primary]
	if len(queue) <= ringSize {
		return
	}
	evictCount := len(queue) - ringSize

	// Fast path — the hot one: enqueue into a full queue evicts EXACTLY one
	// frame. queuePeerFrame hits this on every append into a full ring, so it
	// must not allocate. Find the single oldest (by QueuedAt; ties → lowest
	// index, matching the stable order of the bulk path) with a linear scan,
	// then remove it in place (shift-left + zero tail). No index slice, no
	// eviction map, no sort, no allocation.
	if evictCount == 1 {
		oldest := 0
		for i := 1; i < len(queue); i++ {
			if queue[i].QueuedAt.Before(queue[oldest].QueuedAt) {
				oldest = i
			}
		}
		s.releaseEvictedPendingLocked(primary, queue[oldest].Frame, ringSize)
		copy(queue[oldest:], queue[oldest+1:])
		queue[len(queue)-1] = pendingFrame{} // release the freed tail slot
		if ringSize == 0 {
			delete(s.pending, primary)
			return
		}
		s.pending[primary] = queue[:len(queue)-1]
		return
	}

	// Bulk path — restore trim / large re-queue overflow evicting many at once.
	// Select the evictCount oldest by QueuedAt with a stable sort, then compact
	// the survivors into a fresh ringSize-capacity slice (releasing the larger
	// backing array). Not hot, so the extra allocation is acceptable here.
	order := make([]int, len(queue))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return queue[order[a]].QueuedAt.Before(queue[order[b]].QueuedAt)
	})
	evicted := make(map[int]struct{}, evictCount)
	for i := 0; i < evictCount; i++ {
		idx := order[i]
		evicted[idx] = struct{}{}
		s.releaseEvictedPendingLocked(primary, queue[idx].Frame, ringSize)
	}
	if ringSize == 0 {
		delete(s.pending, primary)
		return
	}
	kept := make([]pendingFrame, 0, ringSize)
	for i, item := range queue {
		if _, drop := evicted[i]; drop {
			continue
		}
		kept = append(kept, item)
	}
	s.pending[primary] = kept
}

// releaseEvictedPendingLocked drops the bookkeeping for a pending frame the
// ring is evicting: its dedup key and, ONLY for send_message frames, its
// outbound delivery state. Other pending types (relay_delivery_receipt,
// ack_delete, relay_message, push_message, announce_peer) carry an ID from a
// DIFFERENT namespace, and noteOutboundQueuedLocked only creates s.outbound
// entries for send_message — so deleting s.outbound by a non-send_message
// frame's raw ID could wrongly drop the delivery state of an unrelated,
// still-in-flight DM that happens to share the ID. Caller MUST hold
// s.deliveryMu.Lock. ringSize is passed through for the diagnostic log only.
func (s *Service) releaseEvictedPendingLocked(primary domain.PeerAddress, frame protocol.Frame, ringSize int) {
	delete(s.pendingKeys, pendingFrameKey(primary, frame))
	if frame.Type == "send_message" && frame.ID != "" {
		delete(s.outbound, frame.ID)
	}
	log.Debug().
		Str("address", string(primary)).
		Str("evicted_id", frame.ID).
		Int("ring_size", ringSize).
		Msg("pending_ring_evicted_oldest")
}

// queuePeerFrame is cross-domain:
//   - s.resolveHealthAddress touches peer-domain fields → s.peerMu.Lock.
//   - s.pending / s.pendingKeys / noteOutboundQueuedLocked / outbound live in the
//     delivery domain → s.deliveryMu.Lock.
//   - refreshAggregatePendingLocked + read of s.aggregateStatus touch the
//     status domain → s.statusMu.Lock (INNERMOST).
//
// Canonical order: peerMu → deliveryMu → statusMu.
func (s *Service) queuePeerFrame(address domain.PeerAddress, frame protocol.Frame) bool {
	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_wait").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_held").Str("address", string(address)).Str("frame_type", frame.Type).Msg("peer_mu_writer")
	primary := s.resolveHealthAddress(address)

	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_wait").Str("address", string(address)).Str("frame_type", frame.Type).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_held").Str("address", string(address)).Str("frame_type", frame.Type).Msg("delivery_mu_writer")

	key := pendingFrameKey(primary, frame)
	if !key.IsValid() {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_nokey").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_nokey").Str("address", string(address)).Msg("peer_mu_writer")
		return false
	}

	if _, exists := s.pendingKeys[key]; exists {
		// Key already queued. For frames where metadata can change
		// between queuing and drain (e.g. announce_peer node_type),
		// replace the payload so the receiver sees the latest state.
		for i := range s.pending[primary] {
			if pendingFrameKey(primary, s.pending[primary][i].Frame) == key {
				s.pending[primary][i].Frame = frame
				break
			}
		}
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_dup").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_dup").Str("address", string(address)).Msg("peer_mu_writer")
		return true
	}

	// Per-peer RING (bounded, hard memory cap). At capacity we evict the
	// OLDEST queued frame for this peer instead of rejecting the new one, so
	// a reconnecting peer always receives the most RECENT pendingRingSize
	// frames and RAM stays bounded at ~pendingRingSize × connected-peers
	// regardless of churn. The ring IS the bound — nothing spills to disk.
	// The shared helper
	// capPendingRingLocked enforces the same bound on EVERY write-back path
	// (here, flushPendingPeerFrames re-queue, drainPendingForIdentities
	// merge); here we pre-trim to ringSize-1 so the append below lands exactly
	// at the cap.
	ringSize := s.pendingRingSize()
	s.capPendingRingLocked(primary, ringSize-1)
	// Global ceiling stays a hard reject: it is the absolute aggregate backstop
	// across ALL peers (the per-peer ring only bounds one peer at a time).
	if len(s.pendingKeys) >= maxPendingFramesTotal {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_globalfull").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_globalfull").Str("address", string(address)).Msg("peer_mu_writer")
		return false
	}

	s.pending[primary] = append(s.pending[primary], pendingFrame{
		Frame:    frame,
		QueuedAt: time.Now().UTC(),
	})
	s.pendingKeys[key] = struct{}{}
	s.noteOutboundQueuedLocked(frame, "")
	pendingCount := len(s.pending[primary])
	// statusMu is INNERMOST — nest inside the peerMu/deliveryMu section
	// to update the materialised aggregate pending count and snapshot
	// the value for the post-unlock ebus publish.
	s.statusMu.Lock()
	s.refreshAggregatePendingLocked()
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released").Str("address", string(address)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	s.emitPeerPendingChanged(primary, pendingCount)
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	return true
}

// pendingKey is the comparable struct key for the s.pendingKeys dedup set.
// Using a value struct as the map key instead of a concatenated string
// eliminates the per-call string allocation that the old pendingFrameKey
// produced on the queue / flush / drain hot paths — a top allocation-churn
// source in profiling. Map lookup / insert / delete with a struct key does not
// allocate. The zero value (Type == "") means "not a queueable frame" — the
// analogue of the old empty-string return; use IsValid to test it.
//
// A, B, C hold the type-specific key fields (their meaning varies by Type —
// see pendingFrameKey). All fields are strings/PeerAddress so the struct stays
// comparable and usable as a map key.
type pendingKey struct {
	Address domain.PeerAddress
	Type    string
	A, B, C string
}

// IsValid reports whether the key identifies a queueable frame (non-zero).
func (k pendingKey) IsValid() bool { return k.Type != "" }

func pendingFrameKey(address domain.PeerAddress, frame protocol.Frame) pendingKey {
	switch frame.Type {
	case "send_message":
		return pendingKey{Address: address, Type: "send_message", A: frame.ID, B: frame.Recipient}
	case "push_message":
		// Gossip path: fields live in Item, not flat Frame fields.
		if frame.Item == nil {
			return pendingKey{}
		}
		return pendingKey{Address: address, Type: "push_message", A: frame.Item.ID, B: frame.Item.Recipient}
	case "relay_delivery_receipt":
		// frame.Address is the receipt's AUTHOR here, and it belongs in the
		// key: two receipts about one message from two peers are two frames,
		// and collapsing them queues only whichever arrived first.
		return pendingKey{Address: address, Type: "relay_delivery_receipt", A: frame.ID, B: frame.Recipient, C: frame.Status + "|" + frame.Address}
	case "ack_delete":
		// ack_delete must be queueable so that sendAckDeleteToPeer's
		// fallback to queuePeerFrame works when the session's sendCh is
		// full. Without this, a transient channel back-pressure silently
		// drops the ack, the remote peer never clears its backlog, and
		// the receipt is re-pushed on every reconnect.
		return pendingKey{Address: address, Type: "ack_delete", A: frame.AckType, B: frame.ID, C: frame.Status + "|" + frame.ReceiptSender}
	case "announce_peer":
		if len(frame.Peers) > 0 {
			return pendingKey{Address: address, Type: "announce_peer", A: frame.Peers[0]}
		}
		return pendingKey{}
	case "relay_message":
		// relay_message must be queueable so sendRelayMessage's fallback
		// to queuePeerFrame actually works when the session is unavailable.
		// Keyed by message ID + recipient to dedupe identical relay attempts.
		return pendingKey{Address: address, Type: "relay_message", A: frame.ID, B: frame.Recipient}
	default:
		return pendingKey{}
	}
}

// flushPendingPeerFrames is cross-domain:
//   - s.resolveHealthAddress touches peer-domain fields → s.peerMu.Lock.
//   - s.pending / s.pendingKeys → s.deliveryMu.
//   - refreshAggregatePendingLocked + read of s.aggregateStatus touch the
//     status domain → s.statusMu.Lock (INNERMOST).
//
// Each lock section below uses the canonical order
// peerMu → deliveryMu → statusMu.
func (s *Service) flushPendingPeerFrames(address domain.PeerAddress) {
	session, ok := s.activePeerSession(address)
	if !ok || session == nil {
		return
	}

	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	primary := s.resolveHealthAddress(address)
	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_wait").Str("address", string(address)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_held").Str("address", string(address)).Msg("delivery_mu_writer")
	frames := append([]pendingFrame(nil), s.pending[primary]...)
	delete(s.pending, primary)
	for _, frame := range frames {
		delete(s.pendingKeys, pendingFrameKey(primary, frame.Frame))
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_released").Str("address", string(address)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "flushPendingPeerFrames_take").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	remaining := make([]pendingFrame, 0)
	now := time.Now().UTC()
	for _, item := range frames {
		if s.pendingFrameExpired(item.Frame, item.QueuedAt, now) {
			s.markOutboundTerminal(item.Frame, "expired", "message delivery expired")
			continue
		}
		if item.Frame.Type != "send_message" && now.Sub(item.QueuedAt) > pendingFrameTTL {
			s.markOutboundTerminal(item.Frame, "expired", "pending queue expired")
			continue
		}
		// A parked frame is a delivery like any other: wrapped with its
		// dispatch so the session's writer takes the pre-wire gate and
		// confirms it. Wrapping it as a legacy item left a frame that
		// really did go out unconfirmed, so it read as queued and was
		// re-sent on the next tick.
		if s.enqueueSessionSendItem(session, legacyPeerSendItem(item.Frame)) {
			s.clearOutboundQueued(item.Frame.ID)
			continue
		}
		item.Retries++
		if item.Retries >= maxPendingFrameRetries {
			s.markOutboundTerminal(item.Frame, "failed", "max retries exceeded")
			continue
		}
		s.markOutboundRetrying(item.Frame, item.QueuedAt, item.Retries, "retry queued delivery")
		remaining = append(remaining, item)
	}
	if len(remaining) == 0 {
		// refreshAggregatePendingLocked reads s.pending (deliveryMu)
		// and writes s.aggregateStatus (statusMu, INNERMOST).
		// Canonical order: peerMu → deliveryMu → statusMu.
		log.Trace().Str("site", "flushPendingPeerFrames_drain_empty").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "flushPendingPeerFrames_drain_empty").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
		s.deliveryMu.RLock()
		s.statusMu.Lock()
		s.refreshAggregatePendingLocked()
		aggSnap := s.aggregateStatus
		s.statusMu.Unlock()
		s.deliveryMu.RUnlock()
		s.peerMu.Unlock()
		log.Trace().Str("site", "flushPendingPeerFrames_drain_empty").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
		s.emitPeerPendingChanged(primary, 0)
		s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
		return
	}

	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_wait").Str("address", string(address)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_held").Str("address", string(address)).Msg("delivery_mu_writer")
	s.pending[primary] = append(s.pending[primary], remaining...)
	for _, item := range remaining {
		s.pendingKeys[pendingFrameKey(primary, item.Frame)] = struct{}{}
	}
	// Re-appending the undelivered frames on top of any frames that arrived
	// during the unlocked delivery window can push the queue past the ring
	// bound — enforce it here too (evict-oldest) so the re-queue path cannot
	// grow s.pending unbounded across reconnect cycles.
	s.capPendingRingLocked(primary, s.pendingRingSize())
	pendingCount := len(s.pending[primary])
	// statusMu is INNERMOST per canonical peerMu → deliveryMu → statusMu
	// order — refreshAggregatePendingLocked writes s.aggregateStatus.
	s.statusMu.Lock()
	s.refreshAggregatePendingLocked()
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_released").Str("address", string(address)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "flushPendingPeerFrames_requeue").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	s.emitPeerPendingChanged(primary, pendingCount)
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
}

// flushPendingFireAndForget drains fire-and-forget frames (push_message,
// push_notice) from the pending queue for the given address and writes them
// directly to the provided inbound connection.
//
// This is the inbound-path counterpart of flushPendingPeerFrames (which
// uses outbound sessions). When a node has no outbound session to a peer
// (e.g. CM slot full), queued fire-and-forget frames would sit forever in
// s.pending. This function writes them on the inbound conn established by
// the peer, ensuring gossip propagation across the relay chain even without
// a symmetric outbound session.
//
// Only fire-and-forget frames are flushed here — request/reply frames
// (send_message, relay_message) must go through the outbound session to
// avoid interleaving with the peer's inbound request dispatch loop.
//
// Cross-domain:
//   - refreshAggregatePendingLocked writes s.aggregateStatus → s.statusMu.Lock.
//   - s.pending / s.pendingKeys → s.deliveryMu.
//   - peer-domain iteration stays under s.peerMu.
//
// Canonical order: peerMu → deliveryMu → statusMu with statusMu INNERMOST.
func (s *Service) flushPendingFireAndForget(id domain.ConnID, address domain.PeerAddress) {
	// If there is already an active outbound session, let flushPendingPeerFrames
	// handle it to avoid double delivery.
	if session, ok := s.activePeerSession(address); ok && session != nil {
		return
	}

	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_wait").Str("address", string(address)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_held").Str("address", string(address)).Msg("delivery_mu_writer")
	frames := s.pending[address]
	if len(frames) == 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released_empty").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released_empty").Str("address", string(address)).Msg("peer_mu_writer")
		return
	}

	// Extract only gossip fire-and-forget frames; leave others in place.
	// Only push_message and push_notice are safe to flush here — they have
	// no external retry mechanisms that could cause double delivery. Relay
	// frames (relay_message, relay_hop_ack) have their own relayRetryLoop.
	var toSend []pendingFrame
	remaining := make([]pendingFrame, 0, len(frames))
	for _, item := range frames {
		if item.Frame.Type == "push_message" || item.Frame.Type == "push_notice" {
			toSend = append(toSend, item)
			delete(s.pendingKeys, pendingFrameKey(address, item.Frame))
		} else {
			remaining = append(remaining, item)
		}
	}
	if len(toSend) == 0 {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released_none").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released_none").Str("address", string(address)).Msg("peer_mu_writer")
		return
	}
	pendingCount := len(remaining)
	if len(remaining) > 0 {
		s.pending[address] = remaining
	} else {
		delete(s.pending, address)
	}
	// statusMu is INNERMOST per canonical peerMu → deliveryMu → statusMu
	// order — refreshAggregatePendingLocked writes s.aggregateStatus.
	s.statusMu.Lock()
	s.refreshAggregatePendingLocked()
	aggSnap := s.aggregateStatus
	s.statusMu.Unlock()
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released").Str("address", string(address)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "flushPendingFireAndForget").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	s.emitPeerPendingChanged(address, pendingCount)
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)

	remoteAddr := s.Network().RemoteAddr(id)
	flushedAt := time.Now().UTC()
	for _, item := range toSend {
		// Fire-and-forget per-item flush — Network-routed for test
		// observability; ctx is Service lifecycle (s.runCtx).
		// The ring is a SINK like any other, so it takes the same two
		// steps in the same order as every other writer: the pre-wire
		// gate first — freeze, withdrawal, then the durable claim — and
		// the confirmation after, on the SAME stamp, so one flush counts
		// as one attempt. A frame that is not one of ours passes the gate
		// untouched and confirms nothing.
		// The gate, the write and the confirmation are one step here — see
		// writeDeliveryFrameToInbound. The ring is a SINK like any other
		// and takes exactly what every other sink takes.
		if err := s.writeDeliveryFrameToInbound(id, item.Frame, s.deliveryRefForFrame(item.Frame, flushedAt)); err != nil {
			log.Debug().Str("addr", remoteAddr).Str("type", item.Frame.Type).Err(err).
				Msg("pending_fire_and_forget_flush_failed_inbound")
			continue
		}
		log.Debug().Str("addr", remoteAddr).Str("type", item.Frame.Type).Msg("pending_fire_and_forget_flushed_inbound")
	}
}

func (s *Service) peerSession(address domain.PeerAddress) *peerSession {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.resolveSessionLocked(address)
}

func (s *Service) activePeerSession(address domain.PeerAddress) (*peerSession, bool) {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	session := s.resolveSessionLocked(address)
	if session == nil {
		return nil, false
	}
	health := s.health[s.resolveHealthAddress(address)]
	if health == nil || !health.Connected {
		return nil, false
	}
	return session, true
}

// heartbeatDuration is the interval this Service pings inbound peers at, with
// the production schedule standing in for the zero override.
//
// It is a field rather than the bare function for the same reason the datagram
// maintenance cadence is: the JOIN this loop now takes part in cannot be
// observed by a test that has to wait out a thirty-second first tick, and a
// contract nobody can enter is a contract nobody checks.
func (s *Service) heartbeatDuration() time.Duration {
	if s.heartbeatIntervalOverride > 0 {
		return s.heartbeatIntervalOverride
	}
	return nextHeartbeatDuration()
}

func nextHeartbeatDuration() time.Duration {
	jitter := time.Duration(time.Now().UTC().UnixNano()%15) * time.Second
	return heartbeatInterval + jitter
}

// inboundHeartbeat periodically pings an inbound peer to independently verify
// liveness. The pong reply is handled by handleCommand which calls markPeerRead.
// If the peer does not respond within pongStallTimeout after a ping, the
// connection is closed — same semantics as outbound session heartbeats.
func (s *Service) inboundHeartbeat(id domain.ConnID, address domain.PeerAddress, stop <-chan struct{}) {
	defer crashlog.DeferRecover()
	timer := time.NewTimer(s.heartbeatDuration())
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			pingFrame := protocol.Frame{Type: "ping", Node: nodeName, Network: networkName}
			// inboundHeartbeat is a long-lived goroutine: loop
			// termination is driven by <-stop, so the ctx handed to the
			// Network helper is Service lifecycle (s.runCtx) rather than
			// a per-iteration value. The ctx is only a cancellation
			// boundary for the underlying SendFrame call — it must not
			// double as the loop-exit signal.
			_ = s.sendFrameViaNetwork(s.runCtx, id, pingFrame)
			s.markPeerWrite(address, pingFrame)

			// Record the time we sent the ping and wait for pongStallTimeout.
			// If LastPongAt has not advanced past our ping time by then,
			// the peer is unresponsive — close the connection.
			sentAt := time.Now().UTC()

			select {
			case <-stop:
				return
			case <-time.After(pongStallTimeout):
			}

			s.peerMu.RLock()
			health := s.health[s.resolveHealthAddress(address)]
			pongReceived := health != nil && !health.LastPongAt.IsZero() && health.LastPongAt.After(sentAt)
			connected := health != nil && health.Connected
			s.peerMu.RUnlock()

			if !connected {
				return
			}
			if !pongReceived {
				log.Warn().Str("peer", string(address)).Msg("inbound heartbeat failed, peer stalled — closing connection")
				// Force-disconnect via netcore.Network so the registry
				// (live bridge or netcoretest.Backend) owns the close;
				// the previous *netcore.NetCore.Close() bypassed test
				// observability.
				_ = s.Network().Close(s.runCtx, id)
				return
			}

			timer.Reset(nextHeartbeatDuration())
		}
	}
}

// evictStaleInboundConns force-closes inbound TCP connections that have
// not received any frame for longer than heartbeatInterval + pongStallTimeout.
// When internet drops, the underlying TCP socket may linger in the OS for
// much longer than the heartbeat timeout (TCP retransmission timeouts).
// These zombie connections occupy a slot in s.conns and block outbound
// dial attempts to the same host via connectedHostsLocked. By actively
// closing them we free the slot so the remote peer's retry loop can
// re-establish the connection faster.
//
// Uses per-connection lastActivity (updated on every received frame) instead
// of shared health state. This prevents NATed peers that advertise the same
// listen address (e.g. 127.0.0.1:64646) from being evicted due to an
// unrelated outbound session going stale.
func (s *Service) evictStaleInboundConns() {
	now := time.Now().UTC()
	stallThreshold := heartbeatInterval + pongStallTimeout

	type staleEntry struct {
		id     domain.ConnID
		addr   domain.PeerAddress
		ident  domain.PeerIdentity
		remote string
	}

	s.peerMu.RLock()
	var stale []staleEntry
	s.forEachInboundConnLocked(func(info connInfo) bool {
		if info.lastActivity.IsZero() {
			return true
		}
		if now.Sub(info.lastActivity) >= stallThreshold {
			stale = append(stale, staleEntry{
				id:     info.id,
				addr:   info.address,
				ident:  info.identity,
				remote: info.remoteAddr,
			})
		}
		return true
	})
	s.peerMu.RUnlock()

	ctx := context.Background()
	network := s.Network()
	for _, e := range stale {
		log.Warn().Str("peer", string(e.addr)).Str("identity", e.ident.String()).Str("remote", e.remote).Msg("force-closing stale inbound connection")
		_ = network.Close(ctx, e.id)
	}
}

// touchConnActivity updates the per-connection last activity timestamp.
func (s *Service) touchConnActivity(id domain.ConnID) {
	if pc := s.netCoreForID(id); pc != nil {
		pc.SetLastActivity(time.Now().UTC())
	}
}

// externalListenAddress returns the loopback-reachable form of our listen
// address, memoised. cfg is immutable after construction, so the value is
// computed once on first call and cached in an atomic pointer — every later
// call (this runs per candidate in the routing/gossip target-selection loops
// via isSelfAddress) is an atomic load + deref with no allocation, instead of
// re-synthesising "127.0.0.1"+ListenAddress each time. See externalListenCached.
func (s *Service) externalListenAddress() string {
	if p := s.externalListenCached.Load(); p != nil {
		return *p
	}
	v := computeExternalListenAddress(s.cfg)
	// Idempotent: concurrent first-callers all compute the same value; the
	// atomic store makes the publication race-free (last writer wins).
	s.externalListenCached.Store(&v)
	return v
}

func computeExternalListenAddress(cfg config.Node) string {
	if !cfg.EffectiveListenerEnabled() {
		return ""
	}
	if strings.HasPrefix(cfg.ListenAddress, ":") {
		return "127.0.0.1" + cfg.ListenAddress
	}
	return cfg.ListenAddress
}

func (s *Service) isSelfAddress(address domain.PeerAddress) bool {
	// Compare against the two stable local self-identifiers:
	//   - cfg.ListenAddress: what we actually bind on (may be ":port"
	//     wildcard, in which case the host check below catches it via
	//     the IP filter);
	//   - externalListenAddress(): "127.0.0.1:<port>" synthesised from
	//     ListenAddress for tests / single-host clusters that dial
	//     loopback.
	// The legacy cfg.AdvertiseAddress field was removed in the v12
	// cleanup phase: under the v12 wire contract no peer ever sees a
	// "configured advertise host" of ours, so a peer dialling our
	// configured advertise host literally cannot happen any more.
	if address == domain.PeerAddress(s.externalListenAddress()) || address == domain.PeerAddress(s.cfg.ListenAddress) {
		return true
	}
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return s.isSelfDialIP(ip)
}

func (s *Service) normalizePeerAddress(observedAddr, advertisedAddr domain.PeerAddress) (domain.PeerAddress, bool) {
	observedHost, _, observedOK := splitHostPort(string(observedAddr))
	advertisedHost, advertisedPort, advertisedOK := splitHostPort(string(advertisedAddr))
	if advertisedPort == "" {
		advertisedPort = config.DefaultPeerPort
	}

	// .onion addresses are accepted as-is from the advertised field;
	// the observed TCP address is meaningless for Tor connections.
	if advertisedOK && isOnionAddress(advertisedHost) {
		return domain.PeerAddress(net.JoinHostPort(advertisedHost, advertisedPort)), true
	}
	// Reject any .onion-suffixed hostname that failed the strict validator
	// above (wrong length, invalid base32 chars, etc.) so it cannot leak
	// through the generic hostname branches below.
	if advertisedOK && strings.HasSuffix(strings.ToLower(advertisedHost), ".onion") {
		return "", false
	}

	switch {
	case advertisedOK && observedOK:
		// Normalise both hosts through canonicalIPFromHost so IPv4-mapped
		// IPv6 (::ffff:1.2.3.4) compares equal to the bare IPv4 form and
		// we never produce a false mismatch between observed and advertised.
		// Host-form strings (names, DNS) pass through unchanged.
		normObserved := observedHost
		if canon := canonicalIPFromHost(observedHost); canon != "" {
			normObserved = canon
		}
		normAdvertised := advertisedHost
		if canon := canonicalIPFromHost(advertisedHost); canon != "" {
			normAdvertised = canon
		}
		observedIP := net.ParseIP(normObserved)
		advertisedIP := net.ParseIP(normAdvertised)

		if advertisedIP != nil && !isForbiddenAdvertisedIP(advertisedIP) && normAdvertised == normObserved {
			return domain.PeerAddress(net.JoinHostPort(normAdvertised, advertisedPort)), true
		}
		if observedIP != nil && !s.isForbiddenDialIP(observedIP) {
			if advertisedIP != nil && isForbiddenAdvertisedIP(advertisedIP) {
				// When both hosts match the peer is genuinely local (e.g.
				// loopback-to-loopback in tests or a single-machine
				// cluster), so its self-reported port is authoritative.
				// Only fall back to DefaultPeerPort when the hosts differ,
				// meaning the advertised IP was likely spoofed.
				if normAdvertised == normObserved {
					return domain.PeerAddress(net.JoinHostPort(normObserved, advertisedPort)), true
				}
				return domain.PeerAddress(net.JoinHostPort(normObserved, config.DefaultPeerPort)), true
			}
			return domain.PeerAddress(net.JoinHostPort(normObserved, advertisedPort)), true
		}
		return "", false
	case advertisedOK:
		advertisedIP := net.ParseIP(advertisedHost)
		if advertisedIP != nil && (isForbiddenAdvertisedIP(advertisedIP) || s.isForbiddenDialIP(advertisedIP)) {
			return "", false
		}
		return domain.PeerAddress(net.JoinHostPort(advertisedHost, advertisedPort)), true
	case observedOK:
		// The observed remote port is an ephemeral source port, not a
		// stable listening endpoint. Without a valid advertised port we
		// should not learn a dialable peer address from RemoteAddr alone.
		return "", false
	}

	return "", false
}

// isOnionAddress returns true if the host is a valid Tor .onion address.
// Tor v3 addresses are 56 base32 characters + ".onion" (62 total).
// Tor v2 addresses (deprecated) are 16 base32 characters + ".onion" (22 total).
func isOnionAddress(host string) bool {
	lower := strings.ToLower(host)
	if !strings.HasSuffix(lower, ".onion") {
		return false
	}
	name := lower[:len(lower)-6] // strip ".onion"
	if len(name) != 56 && len(name) != 16 {
		return false
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

// isI2PAddress returns true if the host is an I2P .b32.i2p address.
// I2P base32 addresses are 52 base32 characters + ".b32.i2p".
func isI2PAddress(host string) bool {
	return strings.HasSuffix(strings.ToLower(host), ".b32.i2p")
}

func splitHostPort(address string) (string, string, bool) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || host == "" || port == "" {
		return "", "", false
	}
	return host, port, true
}

// nonRoutableCIDRs enumerates the IPv4 / IPv6 ranges that are never
// routable in the public Internet. advertise validation, announce
// filtering, peer normalization and forbidden dial checks all share this
// single list so partial subsets never diverge between code paths.
//
// The list is deliberately wider than classic RFC 1918 because carrier-
// grade NAT (100.64/10) and link-local (169.254/16, fe80::/10) ranges
// are just as non-routable from the public Internet and must be excluded
// from world-reachable advertise/announce decisions. Loopback (127/8,
// ::1/128) and IPv6 ULA (fc00::/7) complete the set.
//
// Canonical non-routable IPv4/IPv6 ranges shared by advertise /
// announce / normalize / dial filters.
var nonRoutableCIDRs = []string{
	// IPv4.
	"127.0.0.0/8",    // loopback
	"10.0.0.0/8",     // RFC 1918
	"172.16.0.0/12",  // RFC 1918
	"192.168.0.0/16", // RFC 1918
	"100.64.0.0/10",  // carrier-grade NAT (RFC 6598)
	"169.254.0.0/16", // IPv4 link-local (RFC 3927)
	// IPv6.
	"::1/128",   // loopback
	"fc00::/7",  // unique local addresses (RFC 4193)
	"fe80::/10", // link-local
}

// nonRoutableBlocks is nonRoutableCIDRs parsed ONCE at process start.
// isNonRoutableIP is a hot predicate (PeerProvider.Candidates per candidate,
// announce filtering per route entry, peer normalize, dial-side forbidden
// checks), and re-parsing the constant CIDR strings via net.ParseCIDR on every
// call allocated ~one *net.IPNet per range per call — a top allocation-churn
// source in profiling. Parsing into *net.IPNet once and using block.Contains
// makes the predicate allocation-free.
var nonRoutableBlocks = func() []*net.IPNet {
	blocks := make([]*net.IPNet, 0, len(nonRoutableCIDRs))
	for _, cidr := range nonRoutableCIDRs {
		if _, block, err := net.ParseCIDR(cidr); err == nil && block != nil {
			blocks = append(blocks, block)
		}
	}
	return blocks
}()

// canonicalIPFromHost returns the canonical textual form of an IP
// address for compare/storage purposes. IPv4-mapped IPv6 such as
// ::ffff:1.2.3.4 collapses to the bare IPv4 form so observed vs
// advertised comparisons never give a false mismatch. Returns an empty
// string when host is not a parseable IP — callers treat this as
// "not an IP address" and fall through to their own branch.
//
// Unified IP normalization helper — IPv4-mapped IPv6 collapses.
func canonicalIPFromHost(host string) string {
	trimmed := strings.TrimSpace(host)
	if trimmed == "" {
		return ""
	}
	ip := net.ParseIP(trimmed)
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}
	return ip.String()
}

// isNonRoutableIP returns true if ip belongs to any of nonRoutableCIDRs
// or to the loopback / link-local classes enforced by the stdlib. This
// is the single shared predicate used by advertise validation, announce
// filtering, peer normalization and dial-side forbidden checks.
func isNonRoutableIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, block := range nonRoutableBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

// isForbiddenAdvertisedIP returns true for IPs that must never be accepted
// as self-reported advertised addresses from remote peers. The list is
// the shared nonRoutableCIDRs set so RFC 1918 private ranges, CGNAT
// (100.64/10), IPv4 link-local (169.254/16), IPv6 loopback (::1), IPv6
// ULA (fc00::/7) and IPv6 link-local (fe80::/10) are all rejected the
// same way in advertise / announce / normalize / dial paths. These
// addresses can still be used for manual `addpeer` connections — the
// guard applies only to the auto-learning / announce path.
func isForbiddenAdvertisedIP(ip net.IP) bool {
	return isNonRoutableIP(ip)
}

// isUnspecifiedIPHost reports whether the host string parses as the
// IPv4/IPv6 wildcard bind (0.0.0.0 or ::). Text-based companion to
// net.IP.IsUnspecified for call sites (advertise convergence) that
// work on host strings and must stay off the `net` import so the
// §2.9 Gate 12 whitelist does not grow.
func isUnspecifiedIPHost(host string) bool {
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsUnspecified()
}

// isNonRoutableIPHost parses host as an IP and reports whether it is
// non-routable. Returns true for unparseable hosts as well — callers
// use this predicate to gate "observed IP must be world-reachable";
// a non-IP observation is, by definition, not a routable world-IP
// and fails the gate the same way. Text-based companion to
// isNonRoutableIP for call sites that must stay off the `net` import.
func isNonRoutableIPHost(host string) bool {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return true
	}
	return isNonRoutableIP(ip)
}

// isIPHost reports whether host parses as a valid IP literal (IPv4
// or IPv6). Returns false for hostnames, .onion and empty input —
// callers use this to distinguish IP-form advertises from name-form
// ones without importing `net` outside the Gate 12 whitelist.
func isIPHost(host string) bool {
	return net.ParseIP(strings.TrimSpace(host)) != nil
}

// joinHostPort is the text-only wrapper over net.JoinHostPort.
// advertise_convergence.go composes self-advertise endpoints and
// must not import `net` itself (§2.9 Gate 12 whitelist); routing it
// through this helper keeps the call site expressive while the
// `net` import stays contained.
func joinHostPort(host, port string) string {
	return net.JoinHostPort(host, port)
}

// isManualLocalDialIP reports whether ip is a local-network address that an
// operator may legitimately target with a manual `addpeer` even though the
// automatic dial filters treat it as forbidden. Loopback and private/ULA LAN
// ranges qualify; link-local, unspecified and multicast addresses are
// structurally undialable and never qualify. A nil ip (non-IP host such as a
// hostname or overlay address) does not qualify — manual local peering is an
// IP-literal LAN convenience only.
func isManualLocalDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate()
}

func (s *Service) isForbiddenDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	// Link-local and unspecified addresses are structurally undialable
	// regardless of operator configuration; reject up front so the
	// AllowPrivatePeers / allowLoopbackPeers branches below cannot
	// accidentally re-enable them.
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	if s.cfg.AllowPrivatePeers {
		return false
	}
	if ip.IsLoopback() {
		return !s.allowLoopbackPeers()
	}
	// Everything else goes through the shared non-routable predicate so
	// advertise / announce / dial filters stay synchronised on one list.
	return isNonRoutableIP(ip)
}

// allowLoopbackPeers returns true only when the node is **explicitly**
// configured to listen on a loopback interface (e.g. "127.0.0.1:64646").
// Wildcard binds like ":64646" are NOT loopback — externalListenAddress()
// synthesises "127.0.0.1:<port>" for those as a test-dialing convenience,
// but that does not mean the node intends to operate in a local-dev
// cluster.
//
// Without this distinction, every production node that binds ":port"
// has allowLoopbackPeers=true, which disables self-detection for
// 127.0.0.1. Inbound connections from localhost then learn ephemeral
// loopback addresses as dial candidates, and the ConnectionManager
// enters a connect→EOF→re-dial storm that pollutes health/peer-list/
// routing.
func (s *Service) allowLoopbackPeers() bool {
	host, _, ok := splitHostPort(s.cfg.ListenAddress)
	if !ok {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Service) isSelfDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, address := range []string{s.externalListenAddress(), s.cfg.ListenAddress} {
		host, _, ok := splitHostPort(address)
		if !ok {
			continue
		}
		selfIP := net.ParseIP(host)
		if selfIP == nil {
			continue
		}
		if selfIP.Equal(ip) {
			if ip.IsLoopback() && s.allowLoopbackPeers() {
				return false
			}
			return true
		}
	}
	return false
}

func (s *Service) shouldSkipDialAddress(address domain.PeerAddress) bool {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return true
	}
	ip := net.ParseIP(host)
	return s.isForbiddenDialIP(ip)
}

func (s *Service) dialAttemptAddressesLocked(address domain.PeerAddress) []domain.PeerAddress {
	host, port, ok := splitHostPort(string(address))
	if !ok {
		return nil
	}
	addresses := []domain.PeerAddress{domain.PeerAddress(net.JoinHostPort(host, port))}
	ip := net.ParseIP(host)
	if port != config.DefaultPeerPort && ip != nil && !s.isForbiddenDialIP(ip) && !ip.IsLoopback() {
		addresses = append(addresses, domain.PeerAddress(net.JoinHostPort(host, config.DefaultPeerPort)))
	}
	return addresses
}

func enableTCPKeepAlive(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
}

// resolveHealthAddress returns the primary peer address to use as the
// health map key.  When a dial candidate is a fallback variant (e.g.
// host:defaultPort instead of the original host:customPort), the origin
// map translates back to the primary address so that score/cooldown
// accumulate on a single entry regardless of which port was dialled.
// Reads s.dialOrigin, which is peer-domain state — caller MUST hold
// s.peerMu (read or write).
func (s *Service) resolveHealthAddress(address domain.PeerAddress) domain.PeerAddress {
	if origin, ok := s.dialOrigin[address]; ok {
		return origin
	}
	return address
}

// resolveSessionLocked finds the peerSession for address, handling
// fallback-port aliases. Direct lookup by exact key is tried first;
// if that misses, the caller may be using the canonical (primary)
// address while the session is stored under a fallback dial address.
// In that case we scan dialOrigin (fallback→primary) to find the
// reverse mapping.
// Reads s.sessions and s.dialOrigin, which are peer-domain state —
// caller MUST hold s.peerMu (read or write).
func (s *Service) resolveSessionLocked(address domain.PeerAddress) *peerSession {
	if session := s.sessions[address]; session != nil {
		return session
	}
	// Reverse lookup: address is the primary, session keyed by fallback.
	for dialAddr, primary := range s.dialOrigin {
		if primary == address {
			if session := s.sessions[dialAddr]; session != nil {
				return session
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// ConnectionManager integration callbacks (Stage 3)
// ---------------------------------------------------------------------------

// dialForCM performs TCP connect + handshake for a ConnectionManager dial worker.
// It tries addresses in order (primary, then fallback) and returns the session
// and the actual address that succeeded.
//
// When a fallback address succeeds, dialOrigin[fallback] = primary is
// registered so resolveHealthAddress maps all health/metering operations
// back to the single canonical entry. The legacy path (ensurePeerSessions)
// does the same registration; without it markPeerConnected, peerHealthFrames
// and ActiveSessionLost would all operate on the wrong key.
func (s *Service) dialForCM(ctx context.Context, addresses []domain.PeerAddress) (DialResult, error) {
	if len(addresses) == 0 {
		return DialResult{}, errors.New("no addresses to dial")
	}

	primary := addresses[0]
	var lastErr error
	for _, address := range addresses {
		// Register the fallback→primary mapping BEFORE the handshake, not
		// after success. A connection_notice{peer-banned} can arrive
		// DURING the handshake (runPeerSession's read loop), and its
		// handler resolves the notice address to its canonical primary via
		// resolveHealthAddress (which reads dialOrigin). Without the
		// pre-registration, a blacklisted/peer-ban notice that lands on a
		// generated fallback variant would record against an address with
		// no persistedMeta row — recordRemoteBanLocked ignores unknown
		// addresses — and the per-peer remote-ban window would be lost
		// (the old "first notice = whole IP" behaviour used to mask this;
		// the scoped contract no longer does). The mapping also collapses
		// health/metering onto the single canonical entry regardless of
		// which port the TCP connection used.
		if address != primary {
			s.peerMu.Lock()
			s.dialOrigin[address] = primary
			s.peerMu.Unlock()
		}
		session, err := s.openPeerSessionForCM(ctx, address)
		if err != nil {
			// Drop the speculative alias if this fallback never produced a
			// live session, so a failed dial does not leave a stale
			// fallback→primary mapping behind. Guarded on s.sessions so a
			// concurrently-established session (same key) is never orphaned.
			if address != primary {
				s.peerMu.Lock()
				if s.sessions[address] == nil {
					delete(s.dialOrigin, address)
				}
				s.peerMu.Unlock()
			}
			lastErr = err
			continue
		}
		return DialResult{
			Session:          session,
			ConnectedAddress: address,
		}, nil
	}
	return DialResult{}, lastErr
}

// buildPeerExchangeResponse merges CM Active slots and PeerProvider Candidates,
// deduplicates by IP (active has priority), and optionally filters by caller's
// network groups. Used for both remote peer exchange and local RPC enrichment.
// ActivePeersJSON returns a JSON-encoded snapshot of ConnectionManager slots
// plus active capture recordings (plan §8.3).
// Implements rpc.ConnectionDiagnosticProvider.
func (s *Service) ActivePeersJSON() (json.RawMessage, error) {
	type recordingEntry struct {
		ConnID    domain.ConnID `json:"conn_id"`
		RemoteIP  string        `json:"remote_ip"`
		PeerDir   string        `json:"peer_direction"`
		Format    string        `json:"format"`
		Scope     string        `json:"scope"`
		FilePath  string        `json:"file_path"`
		StartedAt string        `json:"started_at"`
		Error     string        `json:"error,omitempty"`
		Dropped   int64         `json:"dropped_events,omitempty"`
	}
	type response struct {
		Slots      []SlotInfo       `json:"slots"`
		Count      int              `json:"count"`
		MaxSlots   int              `json:"max_slots"`
		Recordings []recordingEntry `json:"recordings,omitempty"`
	}

	var slots []SlotInfo
	if s.connManager != nil {
		slots = s.connManager.Slots()
	}

	var recordings []recordingEntry
	if cm := s.captureManager; cm != nil {
		for _, snap := range cm.AllSessionSnapshots() {
			recordings = append(recordings, recordingEntry{
				ConnID:    snap.ConnID,
				RemoteIP:  snap.RemoteIP.String(),
				PeerDir:   snap.PeerDirection.String(),
				Format:    snap.Format.String(),
				Scope:     snap.Scope.String(),
				FilePath:  snap.FilePath,
				StartedAt: snap.StartedAt.UTC().Format(time.RFC3339Nano),
				Error:     snap.Error,
				Dropped:   snap.DroppedEvents,
			})
		}
	}

	resp := response{
		Slots:      slots,
		Count:      len(slots),
		MaxSlots:   s.cfg.EffectiveMaxOutgoingPeers(),
		Recordings: recordings,
	}
	return json.Marshal(resp)
}

// ListPeersJSON returns a JSON-encoded list of all known peers from
// PeerProvider with ExcludeReasons for diagnostic purposes.
// Implements rpc.ConnectionDiagnosticProvider.
func (s *Service) ListPeersJSON() (json.RawMessage, error) {
	type peerEntry struct {
		Address        string                 `json:"address"`
		Source         string                 `json:"source"`
		AddedAt        string                 `json:"added_at"`
		Network        string                 `json:"network"`
		Score          int                    `json:"score"`
		Failures       int                    `json:"failures"`
		BannedUntil    string                 `json:"banned_until,omitempty"`
		Connected      bool                   `json:"connected"`
		ExcludeReasons []domain.ExcludeReason `json:"exclude_reasons,omitempty"`
	}
	type response struct {
		Peers []peerEntry `json:"peers"`
		Count int         `json:"count"`
	}

	var known []domain.KnownPeerInfo
	if s.peerProvider != nil {
		known = s.peerProvider.KnownPeers()
	}
	entries := make([]peerEntry, 0, len(known))
	for _, k := range known {
		e := peerEntry{
			Address:        string(k.Address),
			Source:         string(k.Source),
			AddedAt:        k.AddedAt.UTC().Format(time.RFC3339),
			Network:        k.Network.String(),
			Score:          k.Score,
			Failures:       k.Failures,
			Connected:      k.Connected,
			ExcludeReasons: k.ExcludeReasons,
		}
		if !k.BannedUntil.IsZero() {
			e.BannedUntil = k.BannedUntil.UTC().Format(time.RFC3339)
		}
		entries = append(entries, e)
	}

	resp := response{Peers: entries, Count: len(entries)}
	return json.Marshal(resp)
}

// ListBannedJSON returns a JSON-encoded list of banned IPs from
// PeerProvider for diagnostic purposes.
// Implements rpc.ConnectionDiagnosticProvider.
func (s *Service) ListBannedJSON() (json.RawMessage, error) {
	type bannedEntry struct {
		IP            string   `json:"ip"`
		BannedUntil   string   `json:"banned_until"`
		BanOrigin     string   `json:"ban_origin"`
		BanReason     string   `json:"ban_reason"`
		AffectedPeers []string `json:"affected_peers"`
	}
	type response struct {
		BannedIPs []bannedEntry `json:"banned_ips"`
		Count     int           `json:"count"`
	}

	var banned []domain.BannedIPInfo
	if s.peerProvider != nil {
		banned = s.peerProvider.BannedIPs()
	}
	entries := make([]bannedEntry, 0, len(banned))
	for _, b := range banned {
		affected := make([]string, len(b.AffectedPeers))
		for i, a := range b.AffectedPeers {
			affected[i] = string(a)
		}
		entries = append(entries, bannedEntry{
			IP:            b.IP,
			BannedUntil:   b.BannedUntil.UTC().Format(time.RFC3339),
			BanOrigin:     string(b.BanOrigin),
			BanReason:     b.BanReason,
			AffectedPeers: affected,
		})
	}

	resp := response{BannedIPs: entries, Count: len(entries)}
	return json.Marshal(resp)
}

// isActiveConnectionFrame is the single source of truth for "this peer
// health frame represents a live, established connection" — the exact
// filter ActiveConnectionsJSON (getActiveConnections) applies. Shared so
// the connection COUNT (activeConnectionCount, surfaced by
// getResourceUsage) and the connection LIST cannot drift apart.
//
// A frame counts when: Connected, a non-zero ConnID, a health state in
// {healthy, degraded, stalled} (reconnecting means Connected==false in
// practice), and a slot state that is empty / active / initializing
// (queued / dialing / retry_wait / reconnecting mean no established
// transport). PeerID is deliberately NOT required — an authenticated
// transport without a completed handshake is still a live connection;
// that matches getActiveConnections (which lists it) and differs from
// connectedPeerCount (which counts distinct relay-ready identities).
func isActiveConnectionFrame(f protocol.PeerHealthFrame) bool {
	return isActiveConnectionState(f.Connected, f.ConnID, f.State, domain.SlotState(f.SlotState))
}

func isActiveConnectionState(connected bool, connID uint64, state string, slotState domain.SlotState) bool {
	if !connected || connID == 0 {
		return false
	}
	if state != peerStateHealthy && state != peerStateDegraded && state != peerStateStalled {
		return false
	}
	if slotState != "" && slotState != domain.SlotStateActive && slotState != domain.SlotStateInitializing {
		return false
	}
	return true
}

// activeConnectionCount returns the number of live peer connections — the same
// set getActiveConnections lists (see isActiveConnectionFrame), counted from
// the lock-free peer-health / CM-slots snapshots. Surfaced by getResourceUsage
// as connection_count.
//
// This intentionally does NOT call peerHealthFrames(): getResourceUsage is
// commonly polled by headless monitoring, and calling peerHealthFrames would
// record a fetch_peer_health reader access, re-arming the expensive
// peer-health periodic rebuild even when nobody is actually viewing that
// diagnostic panel.
func (s *Service) activeConnectionCount() int {
	snap := s.loadPeerHealthSnapshot()
	if snap == nil {
		return 0
	}
	slotsSnap := s.loadCMSlotsSnapshot()

	n := 0
	for _, rec := range snap.records {
		h := rec.health
		slotState := domain.SlotState("")
		if slotsSnap != nil {
			if sl, ok := slotsSnap.byAddress[h.Address]; ok {
				slotState = sl.State
			}
		}
		if rec.sessionConnID != 0 {
			if isActiveConnectionState(h.Connected, uint64(rec.sessionConnID), h.State, slotState) {
				n++
			}
			continue
		}
		for _, cid := range rec.inboundConnIDs {
			if isActiveConnectionState(h.Connected, cid, h.State, slotState) {
				n++
			}
		}
	}
	for _, ilr := range snap.inboundOnly {
		for _, cid := range ilr.inboundConnIDs {
			if isActiveConnectionState(true, cid, peerStateHealthy, "") {
				n++
			}
		}
	}
	return n
}

// ActiveConnectionsJSON returns a JSON-encoded snapshot of all currently
// live peer connections (both inbound and outbound).
// Implements rpc.ConnectionDiagnosticProvider.
func (s *Service) ActiveConnectionsJSON() (json.RawMessage, error) {
	// Internal domain model for a live connection entry.
	type activeConnection struct {
		PeerAddress   domain.PeerAddress
		RemoteAddress domain.PeerAddress
		Identity      domain.PeerIdentity
		Direction     domain.PeerDirection
		Network       domain.NetGroup
		State         string
		ConnID        domain.ConnID
		SlotState     domain.SlotState
	}

	// Wire DTO projected at serialization boundary.
	type activeConnectionJSON struct {
		PeerAddress   string        `json:"peer_address"`
		RemoteAddress string        `json:"remote_address"`
		Identity      string        `json:"identity"`
		Direction     string        `json:"direction"`
		Network       string        `json:"network"`
		State         string        `json:"state"`
		ConnID        domain.ConnID `json:"conn_id"`
		SlotState     string        `json:"slot_state,omitempty"`
	}

	type activeConnectionsResponse struct {
		Version     int                    `json:"version"`
		Connections []activeConnectionJSON `json:"connections"`
		Count       int                    `json:"count"`
	}

	frames := s.peerHealthFrames()

	var connections []activeConnection
	for _, f := range frames {
		// Shared predicate — keeps the connection LIST and the
		// connection COUNT (activeConnectionCount / connection_count)
		// filtering identically. See isActiveConnectionFrame for the
		// rationale behind each clause.
		if !isActiveConnectionFrame(f) {
			continue
		}
		state := f.State
		slotState := domain.SlotState(f.SlotState)

		addr := domain.PeerAddress(f.Address)
		remoteAddr := addr
		if f.SlotConnectedAddr != "" {
			remoteAddr = domain.PeerAddress(f.SlotConnectedAddr)
		}

		dir := domain.PeerDirectionInbound
		if f.Direction == string(domain.PeerDirectionOutbound) {
			dir = domain.PeerDirectionOutbound
		}

		net, _ := domain.ParseNetGroup(f.Network)

		connections = append(connections, activeConnection{
			PeerAddress:   addr,
			RemoteAddress: remoteAddr,
			Identity:      domain.PeerIdentityFromWire(f.PeerID),
			Direction:     dir,
			Network:       net,
			State:         state,
			ConnID:        domain.ConnID(f.ConnID),
			SlotState:     slotState,
		})
	}

	// Deterministic sort: direction (outbound first), then peer_address,
	// then remote_address, then conn_id.
	sort.Slice(connections, func(i, j int) bool {
		di, dj := connections[i].Direction, connections[j].Direction
		if di != dj {
			// outbound < inbound
			return di == domain.PeerDirectionOutbound
		}
		if connections[i].PeerAddress != connections[j].PeerAddress {
			return string(connections[i].PeerAddress) < string(connections[j].PeerAddress)
		}
		if connections[i].RemoteAddress != connections[j].RemoteAddress {
			return string(connections[i].RemoteAddress) < string(connections[j].RemoteAddress)
		}
		return connections[i].ConnID < connections[j].ConnID
	})

	// Project domain model to wire DTO.
	entries := make([]activeConnectionJSON, len(connections))
	for i, c := range connections {
		entries[i] = activeConnectionJSON{
			PeerAddress:   string(c.PeerAddress),
			RemoteAddress: string(c.RemoteAddress),
			Identity:      c.Identity.String(),
			Direction:     c.Direction.String(),
			Network:       c.Network.String(),
			State:         c.State,
			ConnID:        c.ConnID,
			SlotState:     string(c.SlotState),
		}
	}

	resp := activeConnectionsResponse{
		Version:     1,
		Connections: entries,
		Count:       len(entries),
	}
	return json.Marshal(resp)
}

func (s *Service) buildPeerExchangeResponse(callerGroups map[domain.NetGroup]struct{}) []domain.PeerAddress {
	seenIPs := make(map[string]struct{})

	// Read the announce-state gate + inbound list from the cached
	// peers_exchange snapshot.  The snapshot is rebuilt every
	// networkStatsSnapshotInterval by hotReadsRefreshLoop under a short
	// s.peerMu.RLock (see peers_exchange_snapshot.go); this handler never
	// acquires s.peerMu, so get_peers is not serialised behind a queued
	// writer on the peer-domain lock.  The snapshot is primed
	// synchronously by primeHotReadSnapshots() before the listener
	// opens, so pxSnap is non-nil in production; any nil observed here
	// comes from a test that bypasses Run() without priming.
	//
	// Any peer whose persistedMeta.AnnounceState is NOT announceable
	// must be excluded from peer exchange — the advertise-convergence
	// contract forbids relaying direct-only knowledge to third parties.
	// Peers without any persistedMeta row fall back to "allow" so
	// bootstrap/manual peers that have never been through a handshake
	// still propagate (snap.isAnnounceable encodes this fallback).
	// Record that a consumer read peers-exchange now so the periodic rebuild
	// (a top allocator: persistedMeta/health maps + peerProvider.Candidates())
	// is skipped while no one is calling get_peers — see
	// maybeRebuildPeersExchangeSnapshot.
	s.peersExchangeAccessNanos.Store(time.Now().UnixNano())
	pxSnap := s.loadPeersExchangeSnapshot()
	isAnnounceable := func(addr domain.PeerAddress) bool {
		if pxSnap == nil {
			// No primed snapshot — default to "allow" so bootstrap/manual
			// peers still propagate on startup corner cases, matching the
			// fallback the snapshot itself encodes for addresses with no
			// persisted meta.
			return true
		}
		return pxSnap.isAnnounceable(addr)
	}

	// 1. Active connections first — verified by live TCP, highest priority.
	//
	// Iterates the cached cm_slots snapshot rather than calling
	// s.connManager.Slots() directly.  Slots() takes cm.mu.RLock, and
	// Go's RWMutex is writer-preferring: a queued CM writer (slot state
	// transition, dial completion, eviction) would block this RPC reader
	// exactly the way the pre-split s.mu used to.  The snapshot is rebuilt every
	// networkStatsSnapshotInterval by hotReadsRefreshLoop and primed
	// synchronously in Run() before the listener opens, so this handler
	// performs only atomic loads and never acquires cm.mu.  When the
	// atomic load returns nil (only possible from tests that bypass
	// Run()), the active branch is skipped rather than falling back to a
	// synchronous rebuild — the fallback would reach cm.mu.RLock and
	// break the lock-free contract the snapshot infrastructure enforces.
	slotsSnap := s.loadCMSlotsSnapshot()
	var active []domain.PeerAddress
	if slotsSnap != nil {
		for _, slot := range slotsSnap.all {
			if slot.State != domain.SlotStateActive {
				continue
			}
			// The convergence decision for a CM-managed peer may be
			// keyed either on the canonical slot.Address (when the
			// first dial reached the configured endpoint) or on
			// slot.ConnectedAddress (when a fallback variant won the
			// race and got persisted by the connect writer). A slot
			// must be excluded from peer exchange if *either* key
			// has a persisted decision that is not announceable —
			// without this, a direct_only peer reached via a fallback
			// port slips through because persistedMeta is empty for
			// the fallback key and the filter falls through to
			// "allow unknown".
			canonical := slot.Address
			connected := canonical
			if slot.ConnectedAddress != nil {
				connected = *slot.ConnectedAddress
			}
			// The emitted address is the endpoint that is actually
			// reachable right now (connected, which equals canonical
			// when no fallback won). That's the dial target other
			// peers will store.
			if shouldHidePeerExchangeAddress(connected) {
				continue
			}
			if !isAnnounceable(canonical) || !isAnnounceable(connected) {
				continue
			}
			ip, _, ok := splitHostPort(string(connected))
			if ok {
				if _, exists := seenIPs[ip]; !exists {
					seenIPs[ip] = struct{}{}
					active = append(active, connected)
				}
			}
		}
	}

	// 2. Inbound-only peers — authenticated but not in CM (CM tracks
	// outbound).  Without this, live inbound peers would be invisible
	// to get_peers because Candidates() excludes connected IPs via
	// ConnectedFn.  Inbound list comes from the cached snapshot —
	// already filtered to Direction==inbound && Connected==true inside
	// the refresher, so this handler only applies the pure-function
	// filters (shouldHidePeerExchangeAddress, isAnnounceable) and the
	// per-IP dedup.
	var inbound []domain.PeerAddress
	if pxSnap != nil {
		for _, addr := range pxSnap.inboundConnected {
			if shouldHidePeerExchangeAddress(addr) {
				continue
			}
			if !isAnnounceable(addr) {
				continue
			}
			ip, _, ok := splitHostPort(string(addr))
			if ok {
				if _, exists := seenIPs[ip]; !exists {
					seenIPs[ip] = struct{}{}
					inbound = append(inbound, addr)
				}
			}
		}
	}

	// 3. Supplement with candidates from the peers_exchange snapshot.  The
	// list was produced by peerProvider.Candidates() at refresh time
	// (already sorted by score descending) and baked into the snapshot
	// precisely because Candidates() re-enters s.peerMu.RLock via its callbacks
	// — iterating it directly here would recouple get_peers to s.peerMu
	// and reintroduce the writer-storm starvation (see peer_management.go
	// buildPeerExchangeResponse comment and peers_exchange_snapshot.go).
	var candidates []domain.PeerAddress
	if pxSnap != nil {
		for _, addr := range pxSnap.candidateAddresses {
			if shouldHidePeerExchangeAddress(addr) {
				continue
			}
			if !isAnnounceable(addr) {
				continue
			}
			ip, _, ok := splitHostPort(string(addr))
			if ok {
				if _, exists := seenIPs[ip]; !exists {
					seenIPs[ip] = struct{}{}
					candidates = append(candidates, addr)
				}
			}
		}
	}

	// 4. Filter by caller's network groups and build final result:
	// active outbound first, then inbound, then candidates (preserving score order).
	filterFn := func(addr domain.PeerAddress) bool {
		if callerGroups == nil {
			return true
		}
		g := classifyAddress(addr)
		if !g.IsRoutable() {
			return false
		}
		_, ok := callerGroups[g]
		return ok
	}

	var addresses []domain.PeerAddress
	for _, addr := range active {
		if filterFn(addr) {
			addresses = append(addresses, addr)
		}
	}
	for _, addr := range inbound {
		if filterFn(addr) {
			addresses = append(addresses, addr)
		}
	}
	for _, addr := range candidates {
		if filterFn(addr) {
			addresses = append(addresses, addr)
		}
	}

	return addresses
}

// shouldHidePeerExchangeAddress reports whether an address must never be
// surfaced in a get_peers response. It uses the shared non-routable predicate
// (loopback, RFC1918, CGNAT, link-local, IPv6 ULA) rather than an IPv4-only
// check: a manually added LAN peer (addPeerFrame) may be an IPv6 loopback/ULA
// target (::1, fd00::/8), and those must be hidden from peer exchange just as
// IPv4 private addresses are. Aligning with isNonRoutableIP keeps the
// peer-exchange filter consistent with the announce/advertise/dial filters so
// no non-routable address can leak to neighbours through any one path.
func shouldHidePeerExchangeAddress(address domain.PeerAddress) bool {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return false
	}
	return isNonRoutableIP(net.ParseIP(host))
}
