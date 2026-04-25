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
			s.evictStaleInboundConns()
			s.retryRelayDeliveries()
			s.relayLimiter.cleanup(5 * time.Minute)
			s.maybeSavePeerState()
			s.refreshAggregateStatus()
			s.emitTrafficDeltas()
		}
	}
}

// maybeSavePeerState persists peer addresses if enough time has elapsed
// since the last flush.
func (s *Service) maybeSavePeerState() {
	s.peerMu.RLock()
	elapsed := time.Since(s.lastPeerSave)
	s.peerMu.RUnlock()

	if elapsed < time.Duration(peerStateSaveMinutes)*time.Minute {
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

	state := peerStateFile{
		Version:         peerStateVersion,
		Peers:           entries,
		BannedIPs:       bannedIPs,
		RemoteBannedIPs: remoteBannedIPs,
	}
	if err := savePeerState(path, state); err != nil {
		log.Error().Str("path", path).Err(err).Msg("peer state save failed")
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
				delete(s.peerIDs, peer.Address)
				delete(s.peerVersions, peer.Address)
				delete(s.peerBuilds, peer.Address)
				delete(s.persistedMeta, peer.Address)
				evicted = append(evicted, peer.Address)
				continue
			}
		}
		kept = append(kept, peer)
	}
	s.peers = kept
	s.peerMu.Unlock()
	log.Trace().Str("site", "evictStalePeers").Str("phase", "lock_released").Msg("peer_mu_writer")

	// Remove evicted peers from PeerProvider so they no longer
	// appear in Candidates() and stop consuming dial attempts.
	if s.peerProvider != nil {
		for _, addr := range evicted {
			s.peerProvider.Remove(addr)
		}
	}
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

	var evicted int
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
		delete(s.peerIDs, addr)
		delete(s.peerVersions, addr)
		delete(s.peerBuilds, addr)
		delete(s.persistedMeta, addr)
		delete(s.pending, addr)
		evicted++
	}
	if evicted > 0 {
		s.statusMu.Lock()
		s.refreshAggregateStatusLocked()
		s.statusMu.Unlock()
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_released").Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "evictOrphanedHealthEntries").Str("phase", "lock_released").Msg("peer_mu_writer")

	if evicted > 0 {
		log.Info().Int("evicted", evicted).Int("candidates", len(candidates)).Msg("evicted_orphaned_health_entries")
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
		var entry peerEntry
		if pm := s.persistedMeta[peer.Address]; pm != nil {
			// Preserve stable metadata from the persisted snapshot.
			entry = peerEntry{
				Address:  peer.Address,
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
				NodeType: s.peerTypes[peer.Address],
				Network:  classifyAddress(peer.Address),
				Source:   peer.Source,
				AddedAt:  &now,
			}
			// Store so that subsequent flushes are stable.
			clone := entry
			s.persistedMeta[peer.Address] = &clone
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
		go func(c peerDialCandidate) {
			defer func() {
				log.Trace().Str("site", "ensurePeerSessions_cleanup").Str("phase", "lock_wait").Str("address", string(c.address)).Msg("peer_mu_writer")
				s.peerMu.Lock()
				log.Trace().Str("site", "ensurePeerSessions_cleanup").Str("phase", "lock_held").Str("address", string(c.address)).Msg("peer_mu_writer")
				s.deliveryMu.Lock()
				delete(s.sessions, c.address)
				delete(s.upstream, c.address)
				delete(s.dialOrigin, c.address)
				s.deliveryMu.Unlock()
				s.peerMu.Unlock()
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
func (s *Service) syncPeer(ctx context.Context, address domain.PeerAddress, requestPeers bool) int {
	conn, err := s.dialPeer(ctx, address, syncHandshakeTimeout)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_dial_failed")
		return 0
	}

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	// Wrap the one-shot conn in a bootstrap NetCore so every write on this
	// handshake path goes through the managed writer — no raw io.WriteString
	// remains on this probe. pc.Close() closes conn and waits for the writer
	// goroutine to drain, so no separate defer conn.Close() is needed.
	// writeDeadline matches the outer handshake budget (syncHandshakeTimeout)
	// so the wrapper does not silently extend per-write timing past what the
	// caller guaranteed with SetDeadline above.
	pc := netcore.NewBootstrap(conn, syncHandshakeTimeout)
	defer pc.Close()

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
	if s.isSelfIdentity(domain.PeerIdentity(welcome.Address)) {
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
		// (e.g., announce_routes triggered by trackInboundConnect) before
		// the auth_ok reply. Skip up to 5 unexpected frames to tolerate
		// this race without breaking the handshake.
		var frame protocol.Frame
		for skipped := 0; skipped < 5; skipped++ {
			authReply, err := readFrameLine(reader, maxResponseLineBytes)
			if err != nil {
				log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_read_failed")
				return 0
			}
			frame, err = protocol.ParseFrameLine(strings.TrimSpace(authReply))
			if err != nil {
				log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_parse_failed")
				return 0
			}
			if frame.Type == "auth_ok" || frame.Type == "error" {
				break
			}
			log.Debug().Str("peer", string(address)).Str("type", frame.Type).Int("skipped", skipped+1).Msg("sync_peer_auth_skip_interleaved_frame")
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
	s.learnIdentityFromWelcome(welcome)
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
			reply, err := readFrameLine(reader, maxResponseLineBytes)
			if err != nil {
				log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_get_peers_read_failed")
				return 0
			}
			frame, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
			if err == nil {
				peersImported := 0
				for _, peer := range frame.Peers {
					if s.addPeerAddress(domain.PeerAddress(peer), "", "") {
						peersImported++
					}
				}
				s.logPeerExchangeExecuted(peerExchangePathLegacyDial, address, len(frame.Peers), peersImported)
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
		contactsReply, err := readFrameLine(reader, maxResponseLineBytes)
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_contacts_read_failed")
			return 0
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(contactsReply))
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_contacts_parse_failed")
			return 0
		}
		for _, contact := range frame.Contacts {
			// Verify box key binding before accepting peer-advertised contacts.
			if contact.Address == "" || contact.PubKey == "" || contact.BoxKey == "" || contact.BoxSig == "" {
				continue
			}
			if identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig) != nil {
				continue
			}
			s.addKnownIdentity(domain.PeerIdentity(contact.Address))
			s.addKnownBoxKey(contact.Address, contact.BoxKey)
			s.addKnownPubKey(contact.Address, contact.PubKey)
			s.addKnownBoxSig(contact.Address, contact.BoxSig)
			imported++
		}
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
	// rawBanIP is the key addBanScore stores under — raw host from the
	// wrapper's RemoteAddr() string, not the canonicalised form. The
	// refund must look the peer up under the exact same key, otherwise
	// hostname-based peers get their peer-level bucket refunded while
	// their IP-level ban score silently accumulates toward banThreshold.
	rawBanIP := domain.PeerIP(remoteIPFromString(remoteAddr))
	dialedIP := canonicalIPFromHost(string(rawBanIP))
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
	s.recordOutboundConfirmed(peerAddress, domain.PeerIP(dialedIP), domain.PeerPort(dialedPortInt))
	// Forgivable misadvertise bucket repays a fixed amount on every
	// successful auth — keeps honest but flaky peers from drifting
	// toward a ban after transient NAT-remap events. rawBanIP mirrors
	// the refund onto s.bans; empty rawBanIP (never happens here
	// because canonicalIPFromHost already rejected it above) would
	// only move the peer-level bucket.
	//
	// A successful handshake also clears any remote-ban window we had
	// recorded against this peer: the responder just let us in, so the
	// prior peer-banned notice is no longer authoritative. Without this
	// clear, the PeerProvider.RemoteBannedFn gate would keep skipping
	// the peer on subsequent passes even though it is now willing to
	// talk to us — a stale suppression indistinguishable from the ebus
	// storm the ban window was introduced to end.
	//
	// The clear is symmetric with record. handlePeerBannedNotice writes
	// into exactly one table per notice (scope driven by reason), so
	// recovery touches exactly two tables:
	//   (a) clearRemoteBanLocked(peerAddress) drops THIS peer's own
	//       per-peer record unconditionally — the handshake itself is
	//       direct proof this address accepts us again, regardless of
	//       which reason wrote the record;
	//   (b) clearRemoteIPBanLocked(dialedIP) drops the IP-wide entry so
	//       every sibling behind that egress IP is dialable again.
	// No mirror walk is needed: reason=blacklisted writes ONLY the
	// IP-wide entry, so there is no per-peer blacklisted row on other
	// siblings to keep in sync. Per-peer rows with reason=peer-ban on
	// other siblings are standalone responder decisions on specific
	// addresses and must stay untouched — a handshake with a sibling
	// is not proof the responder has forgiven them. dialedIP is the
	// canonical form used by recordRemoteIPBanLocked, so (b) hashes
	// into the same map key as the original write.
	// Cross-domain write: peer-domain (persistedMeta via clearRemoteBan /
	// repayMisadvertisePenalty) + ipState-domain (remoteBannedIPs via
	// clearRemoteIPBanLocked, and bans via repayMisadvertisePenalty's
	// inner ipStateMu section).  Canonical lock order per docs/locking.md:
	// s.peerMu → s.ipStateMu.  clearRemoteIPBanLocked takes s.ipStateMu
	// itself below.
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	_ = s.repayMisadvertisePenaltyOnAuthLocked(peerAddress, rawBanIP)
	remoteBanCleared := s.clearRemoteBanLocked(peerAddress)
	s.ipStateMu.Lock()
	remoteIPBanCleared := s.clearRemoteIPBanLocked(dialedIP)
	s.ipStateMu.Unlock()
	s.peerMu.Unlock()
	log.Trace().Str("site", "clearRemoteBansOnAuth").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	// Mirror handlePeerBannedNotice: flush immediately when any scope
	// mutated so the cleared state survives a crash/restart. Without
	// this, peers.json would be re-read with stale per-peer
	// RemoteBannedUntil or remote_banned_ips entries and the gate would
	// keep suppressing this peer (and any siblings behind the same IP)
	// until the old window elapses, even though the responder has
	// already accepted us.
	if remoteBanCleared || remoteIPBanCleared {
		s.flushPeerState()
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
		if normalizedAddr, ok := s.normalizePeerAddress(domain.PeerAddress(observedAddr), domain.PeerAddress(frame.Listen)); ok {
			s.promotePeerAddress(normalizedAddr)
			s.rememberPeerType(normalizedAddr, frame.NodeType)
			s.addPeerID(normalizedAddr, domain.PeerIdentity(frame.Address))
			s.addPeerVersion(normalizedAddr, frame.ClientVersion)
			s.addPeerBuild(normalizedAddr, frame.ClientBuild)
		}
	}
	if frame.Address != "" {
		s.addKnownIdentity(domain.PeerIdentity(frame.Address))
	}
	// When all key fields are present, verify the box key binding before storing.
	// If verification fails the keys are discarded; if any field is absent the
	// existing behaviour is preserved for backward compatibility.
	if frame.Address != "" && frame.PubKey != "" && frame.BoxKey != "" && frame.BoxSig != "" {
		if identity.VerifyBoxKeyBinding(frame.Address, frame.PubKey, frame.BoxKey, frame.BoxSig) != nil {
			return
		}
	}
	s.addKnownBoxKey(frame.Address, frame.BoxKey)
	s.addKnownPubKey(frame.Address, frame.PubKey)
	s.addKnownBoxSig(frame.Address, frame.BoxSig)
}

// peerListenAddress extracts the advertised listen address from a hello frame.
// Returns empty string if the peer does not accept inbound connections.
//
// NOTE: the returned value is the peer's self-reported claim; it must not
// be used to gossip the peer's address to neighbours. Use
// observedAnnounceAddressFromHello for the announce path so we broadcast
// the observed TCP source host combined with the advertised port.
func peerListenAddress(hello protocol.Frame) string {
	if !listenerEnabledFromFrame(hello) {
		return ""
	}
	return strings.TrimSpace(hello.Listen)
}

// observedAnnounceAddressFromHello returns the peer address we should gossip
// to neighbours via announce_peer: observed TCP source host combined with
// the advertised listen port — falling back to config.DefaultPeerPort when
// hello.Listen carries a bare host without an explicit port.
//
// Trust model:
//   - host — taken from the observed TCP remote address, because it is the
//     only host we cryptographically trust (the peer cannot lie about where
//     packets actually come from without hijacking routing).
//   - port — taken from hello.Listen if it carries one; otherwise
//     config.DefaultPeerPort. The TCP source port is an ephemeral NAT
//     mapping that no neighbour could dial into, so it is never used.
//     The bare-host fallback matches the add_peer RPC contract
//     (addPeerFrame applies the same default).
//
// Returns ("", false) when:
//   - the peer disabled its listener (Listener="0");
//   - the observed address is empty (unregistered ConnID, post-close read);
//   - hello.Listen is empty / whitespace-only;
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
	advertised := strings.TrimSpace(hello.Listen)
	if advertised == "" {
		return "", false
	}
	// When hello.Listen carries a bare host without an explicit port,
	// attach config.DefaultPeerPort so downstream normalization has a
	// dialable endpoint to work with. Without this fallback a legitimate
	// peer that advertises its listen host without a port (older clients,
	// simple configs) would never appear in gossip — the observed IP is
	// still trusted, we just need a canonical port to pair it with.
	if _, _, ok := splitHostPort(advertised); !ok {
		advertised = net.JoinHostPort(advertised, config.DefaultPeerPort)
	}
	return s.normalizePeerAddress(domain.PeerAddress(observedAddr), domain.PeerAddress(advertised))
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
		select {
		case session.sendCh <- frame:
		default:
			// sendCh full — queue for delivery after drain.
			s.queuePeerFrame(session.address, frame)
		}
	}
	log.Debug().Str("peer", peerAddress).Str("node_type", nodeType).Int("sessions", len(sessions)).Msg("announce_peer sent to neighbors")
}

func (s *Service) addPeerFrame(frame protocol.Frame) protocol.Frame {
	if len(frame.Peers) == 0 || strings.TrimSpace(frame.Peers[0]) == "" {
		return protocol.Frame{Type: "error", Error: "address is required"}
	}
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
		return protocol.Frame{Type: "error", Error: fmt.Sprintf("address %s is in a forbidden IP range", address)}
	}
	if !s.canReach(peerAddress) {
		return protocol.Frame{Type: "error", Error: fmt.Sprintf("address %s is in an unreachable network group (%s)", address, classifyAddress(peerAddress))}
	}

	log.Trace().Str("site", "addPeerFrame").Str("phase", "lock_wait").Str("address", string(peerAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerFrame").Str("phase", "lock_held").Str("address", string(peerAddress)).Msg("peer_mu_writer")

	now := time.Now().UTC()

	// If already known, move to front and update source to manual.
	found := false
	for i, peer := range s.peers {
		if peer.Address == peerAddress {
			s.peers[i].Source = domain.PeerSourceManual
			if i > 0 {
				copy(s.peers[1:i+1], s.peers[:i])
				s.peers[0] = peer
				s.peers[0].Source = domain.PeerSourceManual
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
			Source:  domain.PeerSourceManual,
		}
		s.peerTypes[peerAddress] = domain.NodeTypeFull
	}

	// Always stamp source as manual — whether the peer is new or was
	// previously discovered via bootstrap/peer_exchange.
	if pm := s.persistedMeta[peerAddress]; pm != nil {
		pm.Source = domain.PeerSourceManual
	} else {
		s.persistedMeta[peerAddress] = &peerEntry{
			Address:  peerAddress,
			NodeType: domain.NodeTypeFull,
			Source:   domain.PeerSourceManual,
			AddedAt:  &now,
		}
	}

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
				Str("identity", string(peerID)).
				Str("reason", string(pm.VersionLockout.Reason)).
				Msg("version_lockout_cleared_operator_override")
			pm.VersionLockout = domain.VersionLockoutSnapshot{}
		}
	}
	if peerID != "" {
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
					Str("peer_identity", string(peerID)).
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

	s.peerMu.Unlock()
	log.Trace().Str("site", "addPeerFrame").Str("phase", "lock_released").Str("address", string(peerAddress)).Msg("peer_mu_writer")

	// Flush immediately so the manual peer survives a crash.
	s.flushPeerState()

	// Register in PeerProvider so the CM can pick it up as a candidate.
	// Promote (not just Add) so that an already-known peer gets Source=manual
	// and a refreshed AddedAt, moving it to the front of Candidates().
	if s.peerProvider != nil {
		s.peerProvider.Promote(peerAddress, domain.PeerSourceManual)
	}
	// Enqueue an immediate dial — ManualPeerRequested creates a slot directly,
	// bypassing the Candidates() round-trip that NewPeersDiscovered would use.
	// Uses EmitSlot (blocking) to guarantee delivery.
	//
	// Build the same primary+fallback dial address list that Candidates()
	// would produce. Without this, a manual add of a non-default-port peer
	// (e.g. 1.2.3.4:7777) would never attempt the standard fallback port
	// (1.2.3.4:64646), making manual recovery strictly weaker than ordinary
	// candidate dialing.
	if s.connManager != nil {
		dialAddrs := []domain.PeerAddress{peerAddress}
		if s.peerProvider != nil {
			dialAddrs = s.peerProvider.BuildDialAddresses(peerAddress)
		}
		s.connManager.EmitSlot(ManualPeerRequested{
			Address:       peerAddress,
			DialAddresses: dialAddrs,
		})
	}

	action := "added"
	if found {
		action = "already known, moved to front"
	}
	log.Info().Str("address", address).Str("network", classifyAddress(peerAddress).String()).Str("action", action).Msg("add_peer")

	return protocol.Frame{
		Type:   "ok",
		Peers:  []string{address},
		Status: fmt.Sprintf("peer %s %s (network: %s)", address, action, classifyAddress(domain.PeerAddress(address))),
	}
}

// applyStartupBootstrapPeer adds a compiled/default bootstrap peer using the
// same local command path as CommandTable.addPeer:
// addPeer -> HandleLocalFrame -> add_peer -> addPeerFrame.
func (s *Service) applyStartupBootstrapPeer(address string) {
	address = strings.TrimSpace(address)
	if address == "" {
		return
	}

	frame := s.HandleLocalFrame(protocol.Frame{Type: "add_peer", Peers: []string{address}})
	if frame.Type == "error" {
		log.Debug().Str("address", address).Str("error", frame.Error).Msg("startup bootstrap peer skipped")
		return
	}
}

// PrimeBootstrapPeers applies compiled/default bootstrap peers once at startup.
// Injection happens later in Run(), after ConnectionManager is ready, so the
// add_peer path can enqueue immediate dials instead of being called too early.
func (s *Service) PrimeBootstrapPeers() {
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_wait").Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_held").Msg("peer_mu_writer")
	s.primeBootstrapOnRun = true
	s.peerMu.Unlock()
	log.Trace().Str("site", "PrimeBootstrapPeers").Str("phase", "lock_released").Msg("peer_mu_writer")
}

// primeStartupBootstrapPeers applies compiled/default bootstrap peers once the
// ConnectionManager is running. Peers already restored from persisted state are
// left untouched; bootstrap-only entries are promoted through the manual-add
// path so startup behaves like an operator-issued add_peer.
func (s *Service) primeStartupBootstrapPeers() {
	for _, address := range s.cfg.BootstrapPeers {
		peerAddress := domain.PeerAddress(strings.TrimSpace(address))
		if peerAddress == "" {
			continue
		}

		s.peerMu.RLock()
		_, restoredFromState := s.persistedMeta[peerAddress]
		s.peerMu.RUnlock()

		if restoredFromState {
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
			if peerID != "" {
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
	if peerID != "" {
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
	if address == "" || peerID == "" {
		return
	}
	log.Trace().Str("site", "addPeerID").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "addPeerID").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerIDs[address] = peerID
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

func (s *Service) peerIdentityForAddress(address domain.PeerAddress) domain.PeerIdentity {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	return s.peerIDs[address]
}

func (s *Service) readPeerSession(reader *bufio.Reader, session *peerSession) {
	defer crashlog.DeferRecover()
	for {
		line, err := readFrameLine(reader, maxResponseLineBytes)
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

		trimmed := strings.TrimSpace(line)
		frame, err := protocol.ParseFrameLine(trimmed)
		if err != nil {
			continue
		}

		// file_command frames use their own wire format (FileCommandFrame)
		// and require the raw JSON for decryption and routing. Dispatch them
		// directly to the file router instead of going through the
		// inboxCh → dispatchPeerSessionFrame path, which only has access to
		// the parsed protocol.Frame (missing src/dst/payload fields).
		if frame.Type == "file_command" {
			s.markPeerRead(session.address, frame)
			s.markPeerUsefulReceive(session.address)
			if s.sessionHasCapability(session.address, domain.CapFileTransferV1) {
				// Outbound session carries the peer identity directly;
				// pass it so the file router can split-horizon forward
				// and never reflect the frame back to this same peer.
				s.handleFileCommandFrame(json.RawMessage(trimmed), session.peerIdentity)
			}
			continue
		}

		select {
		case session.inboxCh <- frame:
		default:
			select {
			case session.errCh <- fmt.Errorf("peer session inbox overflow for %s", session.address):
			default:
			}
			return
		}
	}
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
	var payload []byte
	if hello {
		payload = []byte(s.nodeHelloJSONLine())
		s.markPeerWrite(session.address, protocol.Frame{Type: "hello"})
	} else {
		line, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			return protocol.Frame{}, err
		}
		payload = []byte(line)
		s.markPeerWrite(session.address, frame)
	}
	// Outbound control-plane: block on full queue rather than fast-fail,
	// so relay traffic backlog cannot starve handshake / subscribe_inbox /
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
				// through the generic advertise-mismatch path.
				// Collapsing every reason to ErrPeerBanned (what
				// ErrorFromCode would do) defeats the whole point of
				// detecting self-loopback on the wire — the dialler
				// would re-enter the churn loop this notice was
				// explicitly designed to break.
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
			if incoming.Type == "push_message" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "push_delivery_receipt" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "push_notice" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "announce_peer" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "request_inbox" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "subscribe_inbox" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if incoming.Type == "relay_hop_ack" {
				s.dispatchPeerSessionFrame(session.address, session, incoming)
				continue
			}
			if expectedType == "" || incoming.Type == expectedType {
				_ = session.conn.SetReadDeadline(time.Time{})
				return incoming, nil
			}
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
			if s.addPeerAddress(domain.PeerAddress(peer), "", "") {
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
func (s *Service) syncContactsViaSession(session *peerSession) (int, error) {
	contactsFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "fetch_contacts"}, "contacts", false)
	if err != nil {
		return 0, err
	}
	imported := 0
	for _, contact := range contactsFrame.Contacts {
		// Verify box key binding before accepting keys from third-party contacts
		// advertised by peers (encryption.md: signed box-key advertisement).
		// Network-discovered contacts are stored in-memory only and are NOT
		// written to the trust store; that distinction is preserved by fetch_trusted_contacts.
		if contact.Address == "" || contact.PubKey == "" || contact.BoxKey == "" || contact.BoxSig == "" {
			continue
		}
		if identity.VerifyBoxKeyBinding(contact.Address, contact.PubKey, contact.BoxKey, contact.BoxSig) != nil {
			continue
		}
		s.addKnownIdentity(domain.PeerIdentity(contact.Address))
		s.addKnownBoxKey(contact.Address, contact.BoxKey)
		s.addKnownPubKey(contact.Address, contact.PubKey)
		s.addKnownBoxSig(contact.Address, contact.BoxSig)
		imported++
	}
	return imported, nil
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
	// ctx so the dial is bounded but still cancels on lifecycle shutdown.
	dialCtx, cancel := context.WithTimeout(ctx, syncHandshakeTimeout)
	defer cancel()
	// requestPeers=false: narrow contact/key recovery only. Logged as a
	// narrow-recovery skip so operators can tell this path apart from a
	// steady-state (healthy) policy skip. See
	// docs/peer-discovery-conditional-get-peers.ru.md Step 6.
	s.logPeerExchangeSkipped(peerExchangePathSenderKeyFreshDial, senderAddress, peerExchangeSkipByNarrowRecovery)
	return s.syncPeer(dialCtx, senderAddress, false)
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
			Body:       frame.Item.Body,
		})
		if err != nil {
			return
		}

		// Non-DM sender verification: reject messages whose sender is not
		// a known identity. DM messages have cryptographic verification in
		// storeIncomingMessage (VerifyEnvelope); this gate targets non-DM
		// topics where no per-message signature exists.
		peerID := domain.PeerIdentity("")
		if session != nil {
			peerID = session.peerIdentity
		}
		if msg.Topic != "dm" && !s.isVerifiedSender(msg.Sender, peerID) {
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
			// Pass nil for syncSession: this handler runs on the outbound
			// session event loop and may be inside peerSessionRequest
			// (single-reader constraint). syncSenderKeys falls back to a
			// fresh TCP dial.
			//
			// Propagate s.runCtx rather than synthesising context.Background():
			// dispatchPeerSessionFrame is invoked from servePeerSession which
			// was started with s.runCtx, so this is the effective incoming
			// context for this session. Threading ctx through the full
			// dispatch signature is deferred to a separate task to keep this
			// change focused on Step 5 policy (see CLAUDE.md: scope).
			imported := s.syncSenderKeys(s.runCtx, address, nil)
			stored, _, errCode = s.storeIncomingMessage(msg, true)
			if !stored && errCode == protocol.ErrCodeUnknownSenderKey {
				log.Warn().
					Str("peer", string(address)).
					Str("id", string(msg.ID)).
					Str("sender", msg.Sender).
					Str("recipient", msg.Recipient).
					Int("keys_imported", imported).
					Msg("push_message: sender key still unknown after sync — message dropped")
			}
		}
		if stored {
			s.enqueueAckDeleteOnSession(session, address, "dm", msg.ID, "")
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
		s.storeDeliveryReceipt(receipt)
		s.enqueueAckDeleteOnSession(session, address, "receipt", receipt.MessageID, receipt.Status)
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
		// the remote side to disconnect on pong-stall timeout; with a
		// fire-and-forget hop the disk write path never blocks here
		// (the queue-state persister absorbs the MarkDirty into a
		// coalesced background write).
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
	case "request_inbox":
		s.respondToInboxRequest(session)
	case "subscribe_inbox":
		if session != nil {
			reply, sub := s.subscribeInboxFrame(session.connID, frame)
			// Session-local reply: route through the injected Network
			// surface so test backends observe the subscribe_inbox reply,
			// with the session.netCore fallback (via enqueueSessionFrame)
			// preserving the carve-out for live sessions whose s.conns
			// entry is reaped or never populated (tests).
			_ = s.sendSessionFrameViaNetwork(s.runCtx, session, reply)
			if sub != nil {
				s.goBackground(func() { s.pushBacklogToSubscriber(sub) })
			}
		}
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
		if admit := admitRelayFrame(s.sessionHasCapability(address, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			return
		}
		// Pass nil for syncSession: this handler may be dispatched from
		// inside peerSessionRequest (e.g., relay_message arriving during
		// a ping round-trip). Reusing the same session would deadlock on
		// the single-reader inboxCh. syncSenderKeys falls back to a
		// fresh TCP connection for key sync.
		if ackStatus := s.handleRelayMessage(domain.PeerAddress(address), nil, frame); ackStatus != "" {
			s.sendRelayHopAck(domain.PeerAddress(address), frame.ID, ackStatus)
		}
	case "relay_hop_ack":
		if admit := admitRelayFrame(s.sessionHasCapability(address, domain.CapMeshRelayV1), len(frame.Body)); admit != relayAdmitOK {
			return
		}
		s.handleRelayHopAck(domain.PeerAddress(address), frame)
	case "announce_routes":
		if !s.sessionHasCapability(address, domain.CapMeshRoutingV1) {
			return
		}
		// Routing-only peer (no mesh_relay_v1) — routes through it are
		// data-plane unusable. See inbound dispatch for full rationale.
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			return
		}
		s.peerMu.RLock()
		session := s.sessions[address]
		s.peerMu.RUnlock()
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
		if !s.sessionHasCapability(address, domain.CapMeshRoutingV1) {
			return
		}
		if !s.sessionHasCapability(address, domain.CapMeshRoutingV2) {
			return
		}
		if !s.sessionHasCapability(address, domain.CapMeshRelayV1) {
			return
		}
		s.peerMu.RLock()
		session := s.sessions[address]
		s.peerMu.RUnlock()
		if session != nil {
			s.handleRoutesUpdate(session.peerIdentity, address, frame)
		}
	case "request_resync":
		// v2-only control frame — see inbound dispatcher for contract.
		// No payload, no capability beyond v2 required: the arrival is
		// the signal to clear per-peer announce state and let the next
		// cycle re-issue a legacy baseline.
		if !s.sessionHasCapability(address, domain.CapMeshRoutingV2) {
			return
		}
		s.peerMu.RLock()
		session := s.sessions[address]
		s.peerMu.RUnlock()
		if session != nil {
			s.handleRequestResync(session.peerIdentity)
		}
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

// respondToInboxRequest is called by the outbound session when the remote
// inbound side sends a request_inbox frame after authentication.  Messages
// and receipts are stored by identity fingerprint (peerIdentity), not by
// transport/dial address.  The function pushes any locally stored messages
// and delivery receipts for the peer back over the outbound connection.
func (s *Service) respondToInboxRequest(session *peerSession) {
	if session == nil {
		return
	}
	peerID := string(session.peerIdentity)
	if peerID == "" {
		peerID = string(session.address)
	}

	inbox := s.fetchInboxFrame("dm", peerID)
	for _, item := range inbox.Messages {
		if createdAt, err := time.Parse(time.RFC3339, item.CreatedAt); err == nil && s.messageDeliveryExpired(createdAt.UTC(), item.TTLSeconds) {
			continue
		}
		msgFrame := item
		// Session-local push: route through the injected Network surface;
		// helper falls back to session.netCore via enqueueSessionFrame on
		// ErrUnknownConn so live sessions with absent registry entries
		// (tests) keep delivering without a spurious unregistered_write.
		_ = s.sendSessionFrameViaNetwork(s.runCtx, session, protocol.Frame{
			Type:      "push_message",
			Topic:     "dm",
			Recipient: peerID,
			Item:      &msgFrame,
		})
	}

	receipts := s.fetchDeliveryReceiptsFrame(peerID)
	for _, item := range receipts.Receipts {
		receiptFrame := item
		// Session-local push: route through the injected Network surface
		// (see sendSessionFrameViaNetwork doc for the carve-out fallback).
		_ = s.sendSessionFrameViaNetwork(s.runCtx, session, protocol.Frame{
			Type:      "push_delivery_receipt",
			Recipient: peerID,
			Receipt:   &receiptFrame,
		})
	}
	log.Info().Str("peer", string(session.address)).Str("identity", peerID).Int("messages", len(inbox.Messages)).Int("receipts", len(receipts.Receipts)).Msg("responded to request_inbox")
}

func (s *Service) sendAckDeleteToPeer(address domain.PeerAddress, ackType string, id protocol.MessageID, status string) {
	session := s.peerSession(address)
	if session == nil || !session.authOK {
		return
	}
	frame := s.buildAckDeleteFrame(ackType, id, status)
	if s.enqueuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "session").Msg("ack_delete_send")
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "queued").Msg("ack_delete_send")
	}
}

// enqueueAckDeleteOnSession writes an ack_delete frame directly to the
// session's sendCh, bypassing the s.sessions lookup used by
// sendAckDeleteToPeer. This is needed because dispatchPeerSessionFrame
// processes frames during initPeerSession (Phase 1), before the session
// is registered in s.sessions (Phase 2). Using sendAckDeleteToPeer in
// that window silently drops the ack because peerSession(address)
// returns nil.
func (s *Service) enqueueAckDeleteOnSession(session *peerSession, address domain.PeerAddress, ackType string, id protocol.MessageID, status string) {
	if session == nil {
		return
	}
	frame := s.buildAckDeleteFrame(ackType, id, status)
	select {
	case session.sendCh <- frame:
		log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "session_direct").Msg("ack_delete_send")
	default:
		// sendCh full — fall back to pending queue for later drain.
		if s.queuePeerFrame(address, frame) {
			log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "queued").Msg("ack_delete_send")
		}
	}
}

// buildAckDeleteFrame constructs a signed ack_delete frame. Extracted from
// sendAckDeleteToPeer so the same frame can be sent on either an outbound
// session (enqueuePeerFrame) or an inbound connection (sendAckDeleteByID).
func (s *Service) buildAckDeleteFrame(ackType string, id protocol.MessageID, status string) protocol.Frame {
	return protocol.Frame{
		Type:      "ack_delete",
		Address:   s.identity.Address,
		AckType:   ackType,
		ID:        string(id),
		Status:    status,
		Signature: identity.SignPayload(s.identity, ackDeletePayload(s.identity.Address, ackType, string(id), status)),
	}
}

// sendAckDeleteByID writes an ack_delete frame directly on the inbound
// connection identified by connID. This is the inbound-path counterpart of
// sendAckDeleteToPeer: when we receive a push_message on an inbound
// connection and there is no outbound session to that peer, we acknowledge
// on the same conn that delivered the message. The ack is silently dropped
// if the connection has already been unregistered.
func (s *Service) sendAckDeleteByID(connID domain.ConnID, ackType string, msgID protocol.MessageID, status string) {
	core := s.netCoreForID(connID)
	if core == nil {
		return
	}
	frame := s.buildAckDeleteFrame(ackType, msgID, status)
	// Fire-and-forget inbound write — route through the Network interface
	// so a test backend can intercept it. s.runCtx tracks Service lifecycle;
	// see network_consumer.go for the full outcome-tree contract.
	_ = s.sendFrameViaNetwork(s.runCtx, connID, frame)
	log.Debug().Str("addr", core.RemoteAddr()).Str("type", ackType).Str("id", string(msgID)).Str("status", status).Str("mode", "inbound_conn").Msg("ack_delete_send")
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
	s.forEachInboundConnLocked(func(info connInfo) bool {
		// The registry holds both directions. Outbound
		// NetCores must stay invisible on inbound-only lookup paths —
		// they are surfaced through s.sessions once activated.
		if info.address == address {
			ids = append(ids, uint64(info.id))
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
	s.forEachInboundConnLocked(func(info connInfo) bool {
		// Only return tracked connections for the given address.
		if info.address != address {
			return true
		}
		// The walker already filters to inbound; the snapshot's tracked
		// flag is the single source of truth here, no second registry
		// hop required.
		if info.tracked {
			result = info.id
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
// publishAggregateStatusChangedLocked need to read s.pending / s.orphaned.
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
			PeerID:              string(rec.peerID),
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
			PeerID:          string(ilr.peerID),
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

// peerCapabilitiesLocked returns the negotiated capabilities for a peer
// as wire-format strings for PeerHealthFrame.  Checks outbound sessions
// first, then falls back to inbound NetCores.
// Reads s.sessions and peer-domain inbound-conn state — caller MUST
// hold s.peerMu (read or write).
func (s *Service) peerCapabilitiesLocked(address domain.PeerAddress) []string {
	if session := s.resolveSessionLocked(address); session != nil && len(session.capabilities) > 0 {
		return domain.CapabilityStrings(session.capabilities)
	}
	var result []string
	s.forEachInboundConnLocked(func(info connInfo) bool {
		// Outbound NetCores surface their capabilities via s.sessions
		// (checked above); skip them here so a pre-activation outbound
		// entry cannot answer on the inbound fallback path.
		if info.address == address && len(info.capabilities) > 0 {
			result = domain.CapabilityStrings(info.capabilities)
			return false // Stop iteration
		}
		return true
	})
	return result
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

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}

func (s *Service) enqueuePeerFrame(address domain.PeerAddress, frame protocol.Frame) bool {
	session, ok := s.activePeerSession(address)
	if !ok {
		return false
	}
	if s.peerState(address) == peerStateStalled {
		return false
	}
	if session == nil {
		return false
	}

	select {
	case session.sendCh <- frame:
		return true
	default:
		return false
	}
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
	if key == "" {
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
		s.queuePersist.MarkDirty()
		return true
	}

	// Enforce per-peer and global capacity limits.
	if len(s.pending[primary]) >= maxPendingFramesPerPeer {
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_peerfull").Str("address", string(address)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "queuePeerFrame").Str("phase", "lock_released_peerfull").Str("address", string(address)).Msg("peer_mu_writer")
		return false
	}
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
	s.queuePersist.MarkDirty()
	s.emitPeerPendingChanged(primary, pendingCount)
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)
	return true
}

func pendingFrameKey(address domain.PeerAddress, frame protocol.Frame) string {
	switch frame.Type {
	case "send_message":
		return string(address) + "|send_message|" + frame.ID + "|" + frame.Recipient
	case "push_message":
		// Gossip path: fields live in Item, not flat Frame fields.
		if frame.Item == nil {
			return ""
		}
		return string(address) + "|push_message|" + frame.Item.ID + "|" + frame.Item.Recipient
	case "relay_delivery_receipt":
		return string(address) + "|relay_delivery_receipt|" + frame.ID + "|" + frame.Recipient + "|" + frame.Status
	case "ack_delete":
		// ack_delete must be queueable so that sendAckDeleteToPeer's
		// fallback to queuePeerFrame works when the session's sendCh is
		// full. Without this, a transient channel back-pressure silently
		// drops the ack, the remote peer never clears its backlog, and
		// the receipt is re-pushed on every reconnect.
		return string(address) + "|ack_delete|" + frame.AckType + "|" + frame.ID + "|" + frame.Status
	case "announce_peer":
		if len(frame.Peers) > 0 {
			return string(address) + "|announce_peer|" + frame.Peers[0]
		}
		return ""
	case "relay_message":
		// relay_message must be queueable so sendRelayMessage's fallback
		// to queuePeerFrame actually works when the session is unavailable.
		// Keyed by message ID + recipient to dedupe identical relay attempts.
		return string(address) + "|relay_message|" + frame.ID + "|" + frame.Recipient
	default:
		return ""
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
		select {
		case session.sendCh <- item.Frame:
			s.clearOutboundQueued(item.Frame.ID)
		default:
			item.Retries++
			if item.Retries >= maxPendingFrameRetries {
				s.markOutboundTerminal(item.Frame, "failed", "max retries exceeded")
				continue
			}
			s.markOutboundRetrying(item.Frame, item.QueuedAt, item.Retries, "retry queued delivery")
			remaining = append(remaining, item)
		}
	}
	if len(remaining) == 0 {
		// refreshAggregatePendingLocked reads s.pending / s.orphaned
		// (deliveryMu) and writes s.aggregateStatus (statusMu, INNERMOST).
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
		s.queuePersist.MarkDirty()
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
	s.queuePersist.MarkDirty()
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
	s.queuePersist.MarkDirty()
	s.emitPeerPendingChanged(address, pendingCount)
	s.eventBus.Publish(ebus.TopicAggregateStatusChanged, aggSnap)

	remoteAddr := s.Network().RemoteAddr(id)
	for _, item := range toSend {
		// Fire-and-forget per-item flush — Network-routed for test
		// observability; ctx is Service lifecycle (s.runCtx).
		_ = s.sendFrameViaNetwork(s.runCtx, id, item.Frame)
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

func (s *Service) peerState(address domain.PeerAddress) string {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()
	health := s.health[s.resolveHealthAddress(address)]
	if health == nil {
		return peerStateReconnecting
	}
	now := time.Now().UTC()
	return s.computePeerStateAtLocked(health, now)
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
	timer := time.NewTimer(nextHeartbeatDuration())
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
		log.Warn().Str("peer", string(e.addr)).Str("identity", string(e.ident)).Str("remote", e.remote).Msg("force-closing stale inbound connection")
		_ = network.Close(ctx, e.id)
	}
}

// touchConnActivity updates the per-connection last activity timestamp.
func (s *Service) touchConnActivity(id domain.ConnID) {
	if pc := s.netCoreForID(id); pc != nil {
		pc.SetLastActivity(time.Now().UTC())
	}
}

func (s *Service) externalListenAddress() string {
	if !s.cfg.EffectiveListenerEnabled() {
		return ""
	}
	if strings.HasPrefix(s.cfg.ListenAddress, ":") {
		return "127.0.0.1" + s.cfg.ListenAddress
	}
	return s.cfg.ListenAddress
}

func (s *Service) isSelfAddress(address domain.PeerAddress) bool {
	if address == domain.PeerAddress(s.cfg.AdvertiseAddress) || address == domain.PeerAddress(s.externalListenAddress()) || address == domain.PeerAddress(s.cfg.ListenAddress) {
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
	for _, cidr := range nonRoutableCIDRs {
		if inCIDR(ip, cidr) {
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

func inCIDR(ip net.IP, cidr string) bool {
	_, block, err := net.ParseCIDR(cidr)
	if err != nil || block == nil {
		return false
	}
	return block.Contains(ip)
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
// configured to listen or advertise on a loopback interface (e.g.
// "127.0.0.1:64646"). Wildcard binds like ":64646" are NOT loopback —
// externalListenAddress() synthesises "127.0.0.1:<port>" for those as a
// test-dialing convenience, but that does not mean the node intends to
// operate in a local-dev cluster.
//
// Without this distinction, every production node that binds ":port" has
// allowLoopbackPeers=true, which disables self-detection for 127.0.0.1.
// Inbound connections from localhost then learn ephemeral loopback
// addresses as dial candidates, and the ConnectionManager enters a
// connect→EOF→re-dial storm that pollutes health/peer-list/routing.
func (s *Service) allowLoopbackPeers() bool {
	for _, address := range []string{s.cfg.AdvertiseAddress, s.cfg.ListenAddress} {
		host, _, ok := splitHostPort(address)
		if !ok {
			continue
		}
		ip := net.ParseIP(host)
		if ip != nil && ip.IsLoopback() {
			return true
		}
	}
	return false
}

func (s *Service) isSelfDialIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, address := range []string{s.cfg.AdvertiseAddress, s.externalListenAddress(), s.cfg.ListenAddress} {
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
		session, err := s.openPeerSessionForCM(ctx, address)
		if err != nil {
			lastErr = err
			continue
		}
		// Register fallback→primary mapping so that resolveHealthAddress
		// collapses health updates onto one entry regardless of which port
		// the TCP connection used.
		if address != primary {
			log.Trace().Str("site", "dialForCM_registerOrigin").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
			s.peerMu.Lock()
			log.Trace().Str("site", "dialForCM_registerOrigin").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
			s.dialOrigin[address] = primary
			s.peerMu.Unlock()
			log.Trace().Str("site", "dialForCM_registerOrigin").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
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
		if !f.Connected {
			continue
		}
		if f.ConnID == 0 {
			continue
		}

		state := f.State
		// Only healthy/degraded/stalled are valid for active connections.
		// reconnecting means Connected==false in practice, but guard anyway.
		if state != peerStateHealthy && state != peerStateDegraded && state != peerStateStalled {
			continue
		}

		// Filter slot_state: only active and initializing are allowed for
		// live connections. Other slot states (queued, dialing, retry_wait,
		// reconnecting) mean no established transport.  The wire field is
		// a raw string (protocol.PeerHealthFrame) — lift it into the typed
		// domain.SlotState at the decoding boundary so downstream callers
		// compile-compare against the enum constants instead of literals.
		slotState := domain.SlotState(f.SlotState)
		if slotState != "" && slotState != domain.SlotStateActive && slotState != domain.SlotStateInitializing {
			continue
		}

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
			Identity:      domain.PeerIdentity(f.PeerID),
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
			Identity:      string(c.Identity),
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

func shouldHidePeerExchangeAddress(address domain.PeerAddress) bool {
	host, _, ok := splitHostPort(string(address))
	if !ok {
		return false
	}
	return isLoopbackOrPrivateIPv4(net.ParseIP(host))
}

func isLoopbackOrPrivateIPv4(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		return ipv4[0] == 127 || ip.IsPrivate()
	}
	return false
}
