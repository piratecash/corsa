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
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/transport"
)

func (s *Service) Peers() []transport.Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.peersSnapshotLocked()
}

// peersSnapshotLocked returns a shallow copy of the peers slice.
// Must be called with s.mu held (read or write).
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
			s.cleanupExpiredMessages()
			s.cleanupExpiredNotices()
			s.evictStalePeers()
			s.evictStaleInboundConns()
			s.retryRelayDeliveries()
			s.relayLimiter.cleanup(5 * time.Minute)
			s.maybeSavePeerState()
			s.refreshAggregateStatus()
		}
	}
}

// maybeSavePeerState persists peer addresses if enough time has elapsed
// since the last flush.
func (s *Service) maybeSavePeerState() {
	s.mu.RLock()
	elapsed := time.Since(s.lastPeerSave)
	s.mu.RUnlock()

	if elapsed < time.Duration(peerStateSaveMinutes)*time.Minute {
		return
	}
	s.flushPeerState()
}

// flushPeerState builds a snapshot from in-memory state and writes it to disk.
func (s *Service) flushPeerState() {
	s.mu.Lock()
	entries := s.buildPeerEntriesLocked()
	path := s.peersStatePath

	// Snapshot IP-wide bans, filtering out expired entries.
	now := time.Now().UTC()
	var bannedIPs []bannedIPStateEntry
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
	s.mu.Unlock()

	sortPeerEntries(entries)
	entries = trimPeerEntries(entries)

	state := peerStateFile{
		Version:   peerStateVersion,
		Peers:     entries,
		BannedIPs: bannedIPs,
	}
	if err := savePeerState(path, state); err != nil {
		log.Error().Str("path", path).Err(err).Msg("peer state save failed")
		return
	}

	s.mu.Lock()
	s.lastPeerSave = time.Now()
	s.mu.Unlock()
}

// evictStalePeers removes in-memory peers whose score has dropped below
// peerEvictScoreThreshold and that have not successfully connected within
// peerEvictStaleWindow.  Bad addresses are purged so they stop consuming dial attempts
// and make room for fresh peer-exchange discoveries.
// Bootstrap peers are never evicted — they act as permanent seeds.
func (s *Service) evictStalePeers() {
	s.mu.RLock()
	elapsed := time.Since(s.lastPeerEvict)
	s.mu.RUnlock()
	if elapsed < peerEvictInterval {
		return
	}

	now := time.Now()

	var evicted []domain.PeerAddress

	s.mu.Lock()
	s.lastPeerEvict = now

	kept := make([]transport.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		health := s.health[peer.Address]
		if health == nil {
			// No health info yet — keep; might be a freshly discovered peer.
			kept = append(kept, peer)
			continue
		}
		// Never evict bootstrap peers.
		if peer.Source == domain.PeerSourceBootstrap {
			kept = append(kept, peer)
			continue
		}
		// Never evict currently connected peers.
		if health.Connected {
			kept = append(kept, peer)
			continue
		}
		// Evict if score is terrible AND last successful connection (or
		// first discovery, if never connected) was more than staleWindow ago.
		// Importantly, LastDisconnectedAt is NOT used here — it refreshes on
		// every failed retry and would prevent eviction of perpetually-failing
		// peers.  Only LastConnectedAt (actual success) matters for eviction.
		if health.Score <= peerEvictScoreThreshold {
			lastSuccess := health.LastConnectedAt
			// If peer was never successfully connected, fall back to AddedAt
			// (the time it was first discovered via peer exchange or config).
			if lastSuccess.IsZero() {
				if pm := s.persistedMeta[peer.Address]; pm != nil && pm.AddedAt != nil {
					lastSuccess = *pm.AddedAt
				}
			}
			if !lastSuccess.IsZero() && now.Sub(lastSuccess) > peerEvictStaleWindow {
				// Evict: clean up associated state.
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
	s.mu.Unlock()

	// Remove evicted peers from PeerProvider so they no longer
	// appear in Candidates() and stop consuming dial attempts.
	if s.peerProvider != nil {
		for _, addr := range evicted {
			s.peerProvider.Remove(addr)
		}
	}
}

// buildPeerEntriesLocked snapshots all known peers with their health metadata.
// Stable metadata (NodeType, Source, AddedAt) is read from persistedMeta so
// that values loaded from disk survive a restart+flush cycle without being
// overwritten by transient runtime state.  Only truly new peers (not yet in
// persistedMeta) derive these fields from runtime maps.
// Must be called with s.mu held (write lock required — updates persistedMeta
// for newly discovered peers).
func (s *Service) buildPeerEntriesLocked() []peerEntry {
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
			// promotions round-trip through peers.json.
			if s.peerProvider != nil {
				if kp := s.peerProvider.KnownPeerStatic(peer.Address); kp != nil {
					entry.Source = kp.Source
					t := kp.AddedAt
					entry.AddedAt = &t
				}
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
		s.mu.Lock()
		if _, ok := s.upstream[candidate.address]; ok {
			s.mu.Unlock()
			continue
		}
		s.upstream[candidate.address] = struct{}{}
		// Record the mapping from dial address to primary peer address
		// so that health updates (markPeerConnected/Disconnected) always
		// accumulate on the primary entry, even when a fallback port is used.
		if candidate.primary != candidate.address {
			s.dialOrigin[candidate.address] = candidate.primary
		}
		s.mu.Unlock()
		go func(c peerDialCandidate) {
			defer func() {
				s.mu.Lock()
				delete(s.sessions, c.address)
				delete(s.upstream, c.address)
				delete(s.dialOrigin, c.address)
				s.mu.Unlock()
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
// Caller must hold s.mu at least for read.
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
	s.forEachInboundConnLocked(func(core *netcore.NetCore) bool {
		if la := core.LastActivity(); !la.IsZero() && now.Sub(la) >= stallThreshold {
			return true
		}
		if ip := remoteIPFromString(core.RemoteAddr()); ip != "" {
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
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	}
	s.learnIdentityFromWelcome(welcome)
	s.mu.RLock()
	syncHealthKey := s.resolveHealthAddress(address)
	s.mu.RUnlock()
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
			s.addKnownIdentity(contact.Address)
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

func (s *Service) runPeerSession(ctx context.Context, address domain.PeerAddress) {
	defer crashlog.DeferRecover()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		connected, err := s.openPeerSession(ctx, address)
		if err != nil {
			s.mu.Lock()
			closedSession := s.sessions[address]
			delete(s.sessions, address)
			s.mu.Unlock()

			// Routing table: deregister direct peer on session close.
			// The hasRelayCap flag must match what was passed to
			// onPeerSessionEstablished for balanced accounting.
			if closedSession != nil && closedSession.peerIdentity != "" {
				hadRelay := sessionHasCap(closedSession.capabilities, domain.CapMeshRelayV1)
				s.onPeerSessionClosed(closedSession.peerIdentity, hadRelay)
			}
			// servePeerSession calls markPeerDisconnected before
			// returning its error.  For all other error paths (dial
			// failure, handshake, subscribe, sync) we must call it here
			// so that Score decreases and cooldown engages.
			if connected {
				s.mu.RLock()
				h := s.health[s.resolveHealthAddress(address)]
				stillConnected := h != nil && h.Connected
				s.mu.RUnlock()
				if stillConnected {
					s.markPeerDisconnected(address, err)
				}
			} else {
				s.markPeerDisconnected(address, err)
			}

			// Incompatible protocol — no point retrying until the peer
			// upgrades.  Exit the loop; peerDialCandidates will re-try
			// this peer after cooldown expires (exponential backoff via
			// ConsecutiveFailures) so it gets another chance eventually.
			if errors.Is(err, errIncompatibleProtocol) {
				log.Warn().Str("peer", string(address)).Msg("peer_session_stopped_incompatible_protocol")
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		return
	}
}

func (s *Service) openPeerSession(ctx context.Context, address domain.PeerAddress) (bool, error) {
	rawConn, err := s.dialPeer(ctx, address, dialTimeout)
	if err != nil {
		return false, err
	}
	conn := netcore.NewMeteredConn(rawConn)
	var session *peerSession
	defer func() {
		s.accumulateSessionTraffic(address, conn)
		if session != nil {
			_ = session.Close()
			return
		}
		_ = conn.Close()
	}()
	enableTCPKeepAlive(rawConn)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	reader := bufio.NewReader(conn)
	s.mu.Lock()
	cid := s.nextConnIDLocked()
	s.mu.Unlock()
	session = &peerSession{
		address:      address,
		peerIdentity: "",
		connID:       cid,
		conn:         conn,
		metered:      conn,
		sendCh:       make(chan protocol.Frame, 64),
		inboxCh:      make(chan protocol.Frame, 64),
		errCh:        make(chan error, 1),
	}
	s.attachOutboundNetCore(session)
	go s.readPeerSession(reader, session)

	welcome, err := s.peerSessionRequest(session, protocol.Frame{}, "welcome", true)
	if err != nil {
		return false, err
	}
	if err := validateProtocolHandshake(welcome); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Int("version", welcome.Version).Msg("outbound_peer_protocol_too_old")
		s.penalizeOldProtocolPeer(address)
		return false, fmt.Errorf("%w: %v", errIncompatibleProtocol, err)
	}
	session.version = welcome.Version
	session.peerIdentity = domain.PeerIdentity(welcome.Address)
	session.capabilities = intersectCapabilities(localCapabilities(), welcome.Capabilities)
	// Mirror the negotiated handshake state onto the outbound NetCore so
	// that s.conns (which now holds both directions) is a
	// symmetric registry: helpers that read pc.Address()/Identity()/
	// Capabilities() get the real values instead of an anonymous entry.
	//
	// pc.Address must be the peer's advertised listen address (the canonical
	// overlay key), not the dial address — for fallback-port variants the
	// two can differ. Mirror the inbound pattern from rememberConnPeerAddr:
	// prefer welcome.Listen, fall back to welcome.Address (identity) when
	// the peer does not advertise a listen address.
	if session.netCore != nil {
		advertised := domain.PeerAddress(strings.TrimSpace(welcome.Listen))
		if advertised == "" {
			advertised = domain.PeerAddress(strings.TrimSpace(welcome.Address))
		}
		session.netCore.SetAddress(advertised)
		session.netCore.SetIdentity(session.peerIdentity)
		session.netCore.SetCapabilities(session.capabilities)
	}
	s.learnIdentityFromWelcome(welcome)
	// learnIdentityFromWelcome stores version/build keyed by the normalized
	// listen address, which may differ from the dial address (or be absent
	// entirely when the peer omits the listen field). Store by the
	// health-tracking key (resolveHealthAddress) so peerHealthFrames can
	// always find the version — even for fallback-port dial variants.
	s.mu.RLock()
	healthKey := s.resolveHealthAddress(address)
	s.mu.RUnlock()
	s.addPeerID(healthKey, session.peerIdentity)
	s.addPeerVersion(healthKey, welcome.ClientVersion)
	s.addPeerBuild(healthKey, welcome.ClientBuild)
	if err := s.authenticatePeerSession(session, welcome); err != nil {
		return false, err
	}
	// Record observed address only after authentication succeeds so that
	// an unauthenticated responder cannot influence NAT consensus.
	s.recordObservedAddress(domain.PeerIdentity(welcome.Address), welcome.ObservedAddress)

	// Evaluate peer exchange policy BEFORE markPeerConnected so that the
	// aggregate status snapshot does not yet include the peer being set up.
	// This normalizes the decision with the CM path (initPeerSession),
	// where the session is not yet registered either.
	requestPeers := s.shouldRequestPeers()
	if !requestPeers {
		s.logPeerExchangeSkipped(peerExchangePathSessionOutbound, address, peerExchangeSkipByAggregateHealthy)
	}

	s.mu.Lock()
	s.sessions[address] = session
	s.mu.Unlock()
	s.markPeerConnected(address, peerDirectionOutbound)

	if _, err := s.peerSessionRequest(session, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  s.identity.Address,
		Subscriber: s.cfg.AdvertiseAddress,
	}, "subscribed", false); err != nil {
		return true, err
	}
	_ = conn.SetDeadline(time.Time{})
	log.Info().Str("peer", string(address)).Str("recipient", s.identity.Address).Msg("upstream subscription established")

	if err := s.syncPeerSession(session, requestPeers, peerExchangePathSessionOutbound); err != nil {
		return true, err
	}

	s.flushPendingPeerFrames(address)

	// Routing table: register direct peer. The relay capability flag
	// ensures only peers that can accept relay_message become direct
	// routes — see onPeerSessionEstablished for the full rationale.
	s.onPeerSessionEstablished(session.peerIdentity, s.sessionHasCapability(address, domain.CapMeshRelayV1))

	// Send full table sync to the new peer (Phase 1.2: full sync on connect).
	// Both capabilities required: mesh_routing_v1 (understands announce_routes)
	// and mesh_relay_v1 (can carry relay traffic). A routing-only peer would
	// learn routes it cannot deliver on the data plane.
	if session.peerIdentity != "" && s.sessionHasCapability(address, domain.CapMeshRoutingV1) && s.sessionHasCapability(address, domain.CapMeshRelayV1) {
		routes := s.routingTable.AnnounceTo(session.peerIdentity)
		if len(routes) > 0 {
			if !s.SendAnnounceRoutes(address, routes) {
				log.Warn().
					Str("peer", string(session.peerIdentity)).
					Str("address", string(address)).
					Int("routes", len(routes)).
					Msg("routing_outbound_full_sync_failed")
			}
		}
	}

	return true, s.servePeerSession(ctx, session)
}

func (s *Service) servePeerSession(ctx context.Context, session *peerSession) error {
	log.Debug().Str("node", s.identity.Address).Str("peer", string(session.address)).Int("inbox_buffered", len(session.inboxCh)).Msg("servePeerSession: entering Phase 3 main loop")

	// Event-driven pending queue drain: a direct route was added by
	// onPeerSessionEstablished before we entered the main loop. Now that
	// inboxCh is actively read (preventing overflow), drain any pending
	// frames (push_message, etc.) that target this peer's identity.
	if session.peerIdentity != "" {
		s.mu.RLock()
		hasPending := len(s.pending) > 0
		s.mu.RUnlock()
		if hasPending {
			go s.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{
				session.peerIdentity: {},
			})
		}
	}

	pingTimer := time.NewTimer(nextHeartbeatDuration())
	defer pingTimer.Stop()

	// writerDone signals that the managed writer goroutine has exited —
	// typically after a socket write failure, but also on normal teardown
	// where the context watcher closes the socket (which unblocks the
	// writer's Write with an error and then closes writerDone). If both
	// ctx.Done() and writerDone become ready simultaneously, Go's select
	// picks randomly; we must not report a clean shutdown as a peer write
	// failure. The case below re-checks ctx.Err() after writerDone fires
	// and returns nil on local cancellation, reserving markPeerDisconnected
	// for genuine remote-side failures. Without this case, a fire-and-forget
	// Send() that returns netcore.SendOK for a frame enqueued moments before the
	// writer dies (writer hits the write error post-enqueue) would leave
	// servePeerSession idle until the next heartbeat.
	var writerDoneCh <-chan struct{}
	if session.netCore != nil {
		writerDoneCh = session.netCore.WriterDone()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-writerDoneCh:
			if err := ctx.Err(); err != nil {
				// Local shutdown closed the socket, which killed the
				// writer. Treat as a clean teardown.
				return nil
			}
			writeErr := fmt.Errorf("managed writer exited (socket write failed)")
			s.markPeerDisconnected(session.address, writeErr)
			return writeErr
		case err := <-session.errCh:
			log.Info().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Err(err).Msg("upstream subscription closed")
			s.markPeerDisconnected(session.address, err)
			return err
		case frame := <-session.inboxCh:
			s.markPeerRead(session.address, frame)
			s.dispatchPeerSessionFrame(session.address, session, frame)
		case <-pingTimer.C:
			if _, err := s.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong", false); err != nil {
				log.Warn().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Err(err).Msg("heartbeat failed, peer stalled")
				s.markPeerDisconnected(session.address, err)
				return err
			}
			pingTimer.Reset(nextHeartbeatDuration())
		case outbound := <-session.sendCh:
			if isFireAndForgetFrame(outbound.Type) {
				// Fire-and-forget frames (relay_message, relay_hop_ack) are
				// enqueued on the managed writer without waiting for a reply.
				// Buffer-full means the peer is not draining fast enough —
				// evict it (slow-peer semantics) so upstream retry logic can
				// pick a different route.
				if session.netCore == nil {
					return fmt.Errorf("fire_and_forget: outbound session missing NetCore")
				}
				s.markPeerWrite(session.address, outbound)
				switch st := session.netCore.Send(outbound); st {
				case netcore.SendOK:
					// enqueued
				case netcore.SendBufferFull:
					writeErr := fmt.Errorf("fire_and_forget: send buffer full")
					log.Warn().Str("peer", string(session.address)).Str("type", outbound.Type).Msg("fire_and_forget_buffer_full")
					s.markPeerDisconnected(session.address, writeErr)
					return writeErr
				case netcore.SendChanClosed, netcore.SendWriterDone:
					writeErr := fmt.Errorf("fire_and_forget: connection closing (%s)", st.String())
					s.markPeerDisconnected(session.address, writeErr)
					return writeErr
				case netcore.SendMarshalError:
					log.Warn().Str("peer", string(session.address)).Str("type", outbound.Type).Msg("fire_and_forget_marshal_error")
					continue
				default:
					writeErr := fmt.Errorf("fire_and_forget: unexpected send status %s", st.String())
					log.Error().Str("peer", string(session.address)).Str("type", outbound.Type).Str("status", st.String()).Msg("fire_and_forget_unexpected_status")
					s.markPeerDisconnected(session.address, writeErr)
					return writeErr
				}
			} else if _, err := s.peerSessionRequest(session, outbound, expectedReplyType(outbound.Type), false); err != nil {
				log.Error().Str("peer", string(session.address)).Str("type", outbound.Type).Err(err).Msg("peer session send failed")
				s.markPeerDisconnected(session.address, err)
				return err
			}
			s.clearRelayRetryForOutbound(outbound)
			// After a successful send, drain any pending frames that were
			// queued when sendCh was full.  This replaces the old periodic
			// sync ticker and keeps delivery event-driven.
			s.flushPendingPeerFrames(session.address)
		}
	}
}

func (s *Service) authenticatePeerSession(session *peerSession, welcome protocol.Frame) error {
	if strings.TrimSpace(welcome.Challenge) == "" {
		return protocol.ErrAuthRequired
	}
	reply, err := s.peerSessionRequest(session, protocol.Frame{
		Type:      "auth_session",
		Address:   s.identity.Address,
		Signature: identity.SignPayload(s.identity, connauth.SessionAuthPayload(welcome.Challenge, s.identity.Address)),
	}, "auth_ok", false)
	if err != nil {
		return err
	}
	if reply.Type != "auth_ok" {
		return protocol.ErrAuthRequired
	}
	session.authOK = true
	return nil
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
		s.addKnownIdentity(frame.Address)
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
func peerListenAddress(hello protocol.Frame) string {
	if !listenerEnabledFromFrame(hello) {
		return ""
	}
	return strings.TrimSpace(hello.Listen)
}

// announcePeerToSessions sends an announce_peer frame with a single new
// peer address and its node type to every active outbound session.  The
// announcement is non-recursive: recipients learn the address but do not
// relay it further.
func (s *Service) announcePeerToSessions(peerAddress, nodeType string) {
	defer crashlog.DeferRecover()

	s.mu.RLock()
	sessions := make([]*peerSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.mu.RUnlock()

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

	s.mu.Lock()

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

	// Reset cooldown and ban so the peer is dialled immediately.
	// A manual add_peer is an explicit operator action that overrides
	// any automated penalty (incompatible protocol, exponential backoff).
	if h := s.health[peerAddress]; h != nil {
		h.ConsecutiveFailures = 0
		h.LastDisconnectedAt = time.Time{}
		h.BannedUntil = time.Time{}
	}

	// Also clear the IP-wide ban — without this, buildBannedIPsSet still
	// excludes the peer from Candidates() even though per-address health
	// is unbanned.
	if ip, _, ok := splitHostPort(string(peerAddress)); ok {
		delete(s.bannedIPSet, ip)
	}

	s.mu.Unlock()

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
	s.mu.Lock()
	s.primeBootstrapOnRun = true
	s.mu.Unlock()
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

		s.mu.RLock()
		_, restoredFromState := s.persistedMeta[peerAddress]
		s.mu.RUnlock()

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

	s.mu.Lock()
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
			s.mu.Unlock()
			return false
		}
		// Peer exchange keeps only one stored address per IP. Alternative
		// ports learned from the network are ignored to avoid peer-list
		// poisoning via many addresses on the same host.
		s.mu.Unlock()
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
	s.mu.Unlock()

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
	s.mu.Lock()
	existing, found := s.findKnownPeerByIPLocked(address)
	if found && existing != address {
		// Network-learned/promoted peers keep a single stored address per IP.
		// Manual/bootstrap paths are intentionally exempt and do not call here.
		s.mu.Unlock()
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
	s.mu.Unlock()

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
	s.mu.Lock()
	s.peerIDs[address] = peerID
	s.mu.Unlock()
}

func (s *Service) addPeerVersion(address domain.PeerAddress, clientVersion string) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	clientVersion = strings.TrimSpace(clientVersion)
	if address == "" || clientVersion == "" {
		return
	}

	s.mu.Lock()
	s.peerVersions[address] = clientVersion
	s.mu.Unlock()
}

func (s *Service) addPeerBuild(address domain.PeerAddress, build int) {
	address = domain.PeerAddress(strings.TrimSpace(string(address)))
	if address == "" || build == 0 {
		return
	}

	s.mu.Lock()
	s.peerBuilds[address] = build
	s.mu.Unlock()
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
	s.mu.Lock()
	s.peerTypes[address] = peerType
	s.mu.Unlock()
}

func (s *Service) peerTypeForAddress(address domain.PeerAddress) domain.NodeType {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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
		s.addKnownIdentity(contact.Address)
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
		// Gossip receipt path using flat Frame fields (mirrors the inbound
		// TCP handler handleInboundRelayDeliveryReceipt).
		receipt, err := receiptFromFrame(frame)
		if err != nil {
			return
		}
		// Identity gate: same as push_delivery_receipt — the receipt's
		// Recipient must match our identity or an active subscriber.
		if receipt.Recipient != s.identity.Address && !s.hasSubscriber(receipt.Recipient) {
			log.Warn().
				Str("peer", string(address)).
				Str("message_id", string(receipt.MessageID)).
				Str("receipt_recipient", receipt.Recipient).
				Str("local_identity", s.identity.Address).
				Msg("relay_delivery_receipt rejected: recipient does not match local identity or active subscriber")
			return
		}
		s.storeDeliveryReceipt(receipt)
		log.Info().Str("peer", string(address)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received relay_delivery_receipt")
	case "push_notice":
		s.handleInboundPushNotice(frame)
	case "request_inbox":
		s.respondToInboxRequest(session)
	case "subscribe_inbox":
		if session != nil {
			reply, sub := s.subscribeInboxFrame(session.connID, session.netCore, frame)
			// Session-local reply: route through the injected Network
			// surface so test backends observe the subscribe_inbox reply,
			// with the session.netCore fallback (via enqueueSessionFrame)
			// preserving the carve-out for live sessions whose s.conns
			// entry is reaped or never populated (tests).
			_ = s.sendSessionFrameViaNetwork(s.runCtx, session, reply)
			if sub != nil {
				go s.pushBacklogToSubscriber(sub)
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
		s.mu.RLock()
		session := s.sessions[address]
		s.mu.RUnlock()
		if session != nil {
			s.handleAnnounceRoutes(session.peerIdentity, frame)
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.resolveHealthAddress(address)
	for dialAddr := range s.sessions {
		if s.resolveHealthAddress(dialAddr) == target {
			return true
		}
	}
	return false
}

func (s *Service) markPeerConnected(address domain.PeerAddress, direction domain.PeerDirection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = true
	health.Direction = direction
	health.LastConnectedAt = now
	// Treat the TCP handshake itself as proof of liveness so that
	// computePeerStateLocked does not immediately return "degraded"
	// before any ping/pong or data exchange has occurred.
	health.LastUsefulReceiveAt = now
	s.updatePeerStateLocked(health, peerStateHealthy)
	health.LastError = ""
	health.ConsecutiveFailures = 0
	health.BannedUntil = time.Time{} // successful handshake proves compatibility
	health.Score = clampScore(health.Score + peerScoreConnect)

	// A completed handshake proves the peer (and its IP) is compatible.
	// Clear the IP-wide ban so sibling ports are also unblocked in
	// Candidates() and list_banned no longer reports a stale entry.
	if ip, _, ok := splitHostPort(string(address)); ok {
		delete(s.bannedIPSet, ip)
	}
}

func (s *Service) markPeerDisconnected(address domain.PeerAddress, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = false
	health.Direction = ""
	s.updatePeerStateLocked(health, peerStateReconnecting)
	health.LastDisconnectedAt = now
	if err != nil {
		health.ConsecutiveFailures++
		health.LastError = err.Error()
		health.Score = clampScore(health.Score + peerScoreFailure)
	} else {
		health.ConsecutiveFailures = 0
		health.Score = clampScore(health.Score + peerScoreDisconnect)
	}
	if peerID := s.peerIDs[address]; peerID != "" {
		delete(s.observedAddrs, peerID)
	}
}

// penalizeOldProtocolPeer applies a heavy score penalty and a temporary ban
// to a peer whose protocol version is below MinimumProtocolVersion. The ban
// prevents the dial loop from retrying the peer for peerBanIncompatible
// (24 h). After the ban expires the peer is retried — it may have upgraded.
func (s *Service) penalizeOldProtocolPeer(address domain.PeerAddress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	health.Score = clampScore(health.Score + peerScoreOldProtocol)
	health.ConsecutiveFailures++
	health.LastError = "protocol version too old"
	bannedUntil := time.Now().UTC().Add(peerBanIncompatible)
	health.BannedUntil = bannedUntil

	// Propagate ban to the IP level so that other ports on the same
	// host are also excluded from dial candidates.
	if ip, _, ok := splitHostPort(string(address)); ok {
		// Snapshot affected peer addresses at ban time so the list
		// round-trips through peers.json even if peers are later
		// trimmed from the top-500 list.
		var affected []domain.PeerAddress
		for _, p := range s.peers {
			if pIP, _, ok2 := splitHostPort(string(p.Address)); ok2 && pIP == ip {
				affected = append(affected, p.Address)
			}
		}
		s.bannedIPSet[ip] = domain.BannedIPEntry{
			IP:            ip,
			BannedUntil:   bannedUntil,
			BanOrigin:     address,
			BanReason:     "incompatible_protocol",
			AffectedPeers: affected,
		}
	}

	log.Info().Str("peer", string(address)).Time("banned_until", bannedUntil).Msg("peer_banned_incompatible_protocol")
}

func (s *Service) markPeerWrite(address domain.PeerAddress, frame protocol.Frame) {
	log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", string(address)).
		Str("direction", "send").
		Str("command", frame.Type).
		Bool("accepted", true).
		Msg("protocol_trace")

	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
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

	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	if frame.Type == "pong" {
		health.LastPongAt = now
		s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
		return
	}
	if frame.Type != "" {
		health.LastUsefulReceiveAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
}

func (s *Service) markPeerUsefulReceive(address domain.PeerAddress) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.LastUsefulReceiveAt = now
	s.updatePeerStateLocked(health, s.computePeerStateAtLocked(health, now))
}

// nextConnIDLocked returns a monotonically increasing connection ID.
// Must be called with s.mu held (write lock).
func (s *Service) nextConnIDLocked() domain.ConnID {
	s.connIDCounter++
	return domain.ConnID(s.connIDCounter)
}

// inboundConnIDsLocked returns the connection IDs for all active inbound
// connections that declared the given overlay address in their hello frame.
// Must be called with s.mu held (read lock).
func (s *Service) inboundConnIDsLocked(address domain.PeerAddress) []uint64 {
	var ids []uint64
	s.forEachInboundConnLocked(func(core *netcore.NetCore) bool {
		// The registry holds both directions. Outbound
		// NetCores must stay invisible on inbound-only lookup paths —
		// they are surfaced through s.sessions once activated.
		if core.Address() == address {
			ids = append(ids, core.ConnIDNum())
		}
		return true
	})
	return ids
}

// inboundConnIDForAddressLocked returns the ConnID of an authenticated
// inbound connection for the given overlay address, or zero value and
// false if none exists. When multiple connections are active, any one of
// them is returned (all are equally valid for fire-and-forget writes).
// Must be called with s.mu held (read lock). ConnID-first (PR 10.6):
// callers resolve the transport through the registry rather than holding
// a raw net.Conn across the lock boundary.
func (s *Service) inboundConnIDForAddressLocked(address domain.PeerAddress) (domain.ConnID, bool) {
	var result domain.ConnID
	var found bool
	s.forEachInboundConnLocked(func(core *netcore.NetCore) bool {
		// Only return tracked connections for the given address
		if core.Address() != address {
			return true
		}
		// Check if this connection is tracked
		if s.isInboundTrackedByIDLocked(core.ConnID()) {
			result = core.ConnID()
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

func (s *Service) updatePeerStateLocked(health *peerHealth, next string) {
	if health.State == next {
		return
	}
	if health.State != "" {
		log.Info().Str("peer", string(health.Address)).Str("from", health.State).Str("to", next).Int("pending", len(s.pending[health.Address])).Int("failures", health.ConsecutiveFailures).Msg("peer_state_change")
	}
	health.State = next

	// Any per-peer state transition may shift the aggregate network status
	// (e.g. the last usable peer going stalled moves aggregate from healthy
	// to limited). Recompute and store the materialized snapshot so that
	// policy helpers and the Desktop UI always see a consistent value.
	s.refreshAggregateStatusLocked()
}

func (s *Service) peerHealthFrames() []protocol.PeerHealthFrame {
	// Collect CM slot snapshots before taking s.mu to avoid nested locking
	// (connManager.Slots() acquires cm.mu independently).
	type slotSnapshot struct {
		State         string
		RetryCount    int
		Generation    uint64
		ConnectedAddr string
	}
	slotByAddr := make(map[domain.PeerAddress]slotSnapshot)
	if s.connManager != nil {
		for _, si := range s.connManager.Slots() {
			snap := slotSnapshot{
				State:      si.State,
				RetryCount: si.RetryCount,
				Generation: si.Generation,
			}
			if si.ConnectedAddress != nil {
				snap.ConnectedAddr = string(*si.ConnectedAddress)
			}
			slotByAddr[si.Address] = snap
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()

	live := s.liveTrafficLocked()

	seen := make(map[domain.PeerAddress]struct{}, len(s.health))
	items := make([]protocol.PeerHealthFrame, 0, len(s.health)+len(live))
	for _, health := range s.health {
		seen[health.Address] = struct{}{}
		sent := health.BytesSent
		recv := health.BytesReceived
		if lv, ok := live[health.Address]; ok {
			sent += lv.sent
			recv += lv.received
		}
		phf := protocol.PeerHealthFrame{
			Address:             string(health.Address),
			PeerID:              string(s.peerIDs[health.Address]),
			Network:             classifyAddress(health.Address).String(),
			Direction:           string(health.Direction),
			ClientVersion:       s.peerVersions[health.Address],
			ClientBuild:         s.peerBuilds[health.Address],
			State:               s.computePeerStateAtLocked(health, now),
			Connected:           health.Connected,
			PendingCount:        len(s.pending[health.Address]),
			LastConnectedAt:     formatTime(health.LastConnectedAt),
			LastDisconnectedAt:  formatTime(health.LastDisconnectedAt),
			LastPingAt:          formatTime(health.LastPingAt),
			LastPongAt:          formatTime(health.LastPongAt),
			LastUsefulSendAt:    formatTime(health.LastUsefulSendAt),
			LastUsefulReceiveAt: formatTime(health.LastUsefulReceiveAt),
			ConsecutiveFailures: health.ConsecutiveFailures,
			LastError:           health.LastError,
			Score:               health.Score,
			BannedUntil:         formatTime(health.BannedUntil),
			BytesSent:           sent,
			BytesReceived:       recv,
			TotalTraffic:        sent + recv,
			Capabilities:        s.peerCapabilitiesLocked(health.Address),
		}
		// sessions is keyed by dial address which may differ from the
		// health address when a fallback port was used. Iterate to find
		// the matching session by resolved health key.
		for dialAddr, session := range s.sessions {
			if s.resolveHealthAddress(dialAddr) == health.Address {
				phf.ProtocolVersion = session.version
				// PeerHealthFrame.ConnID is the wire-level uint64; convert
				// from the typed session identifier before serialising.
				phf.ConnID = uint64(session.connID)
				break
			}
		}
		// Enrich with CM slot lifecycle data if this peer has an outbound slot.
		if snap, ok := slotByAddr[health.Address]; ok {
			phf.SlotState = snap.State
			phf.SlotRetryCount = snap.RetryCount
			phf.SlotGeneration = snap.Generation
			phf.SlotConnectedAddr = snap.ConnectedAddr
		}
		// When an outbound session exists, emit a single row with the
		// outbound ConnID — even if inbound connections coexist (both
		// directions are now allowed for simultaneous dials). For inbound-only
		// peers, emit one row per active TCP connection so that
		// UI/diagnostics can distinguish multiple sessions to the same
		// overlay address.
		if phf.ConnID != 0 {
			// Outbound session present — single authoritative row.
			items = append(items, phf)
		} else {
			inboundConns := s.inboundConnIDsLocked(health.Address)
			if len(inboundConns) > 0 {
				for _, cid := range inboundConns {
					row := phf
					row.ConnID = cid
					row.Direction = string(peerDirectionInbound)
					items = append(items, row)
				}
			} else {
				items = append(items, phf)
			}
		}
	}

	// Include inbound-only peers that have live traffic but no health entry yet.
	for addr, lv := range live {
		if _, ok := seen[addr]; ok {
			continue
		}
		inboundPHF := protocol.PeerHealthFrame{
			Address:       string(addr),
			PeerID:        string(s.peerIDs[addr]),
			Network:       classifyAddress(addr).String(),
			Direction:     string(peerDirectionInbound),
			ClientVersion: s.peerVersions[addr],
			ClientBuild:   s.peerBuilds[addr],
			State:         peerStateHealthy,
			Connected:     true,
			BytesSent:     lv.sent,
			BytesReceived: lv.received,
			TotalTraffic:  lv.sent + lv.received,
			Capabilities:  s.peerCapabilitiesLocked(addr),
		}
		if session, ok := s.sessions[addr]; ok {
			inboundPHF.ProtocolVersion = session.version
		}
		items = append(items, inboundPHF)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Address < items[j].Address
	})
	return items
}

// peerCapabilitiesLocked returns the negotiated capabilities for a peer
// as wire-format strings for PeerHealthFrame.  Checks outbound sessions
// first, then falls back to inbound NetCores.
// Must be called while holding s.mu at least for read.
func (s *Service) peerCapabilitiesLocked(address domain.PeerAddress) []string {
	if session := s.resolveSessionLocked(address); session != nil && len(session.capabilities) > 0 {
		return domain.CapabilityStrings(session.capabilities)
	}
	var result []string
	s.forEachInboundConnLocked(func(core *netcore.NetCore) bool {
		// Outbound NetCores surface their capabilities via s.sessions
		// (checked above); skip them here so a pre-activation outbound
		// entry cannot answer on the inbound fallback path.
		if core.Address() == address && len(core.Capabilities()) > 0 {
			result = domain.CapabilityStrings(core.Capabilities())
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

func pendingStatusFromFrame(item pendingFrame) string {
	if item.Retries > 0 {
		return "retrying"
	}
	return "queued"
}

func outboundLastAttemptLocked(items map[string]outboundDelivery, id string) time.Time {
	if item, ok := items[id]; ok {
		return item.LastAttemptAt
	}
	return time.Time{}
}

func outboundRetriesLocked(items map[string]outboundDelivery, id string, fallback int) int {
	if item, ok := items[id]; ok && item.Retries > fallback {
		return item.Retries
	}
	return fallback
}

func outboundErrorLocked(items map[string]outboundDelivery, id string) string {
	if item, ok := items[id]; ok {
		return item.Error
	}
	return ""
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

func (s *Service) queuePeerFrame(address domain.PeerAddress, frame protocol.Frame) bool {
	s.mu.Lock()
	primary := s.resolveHealthAddress(address)

	key := pendingFrameKey(primary, frame)
	if key == "" {
		s.mu.Unlock()
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
		snapshot := s.queueStateSnapshotLocked()
		s.mu.Unlock()
		s.persistQueueState(snapshot)
		return true
	}

	// Enforce per-peer and global capacity limits.
	if len(s.pending[primary]) >= maxPendingFramesPerPeer {
		s.mu.Unlock()
		return false
	}
	if len(s.pendingKeys) >= maxPendingFramesTotal {
		s.mu.Unlock()
		return false
	}

	s.pending[primary] = append(s.pending[primary], pendingFrame{
		Frame:    frame,
		QueuedAt: time.Now().UTC(),
	})
	s.pendingKeys[key] = struct{}{}
	s.noteOutboundQueuedLocked(frame, "")
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
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

func (s *Service) flushPendingPeerFrames(address domain.PeerAddress) {
	session, ok := s.activePeerSession(address)
	if !ok || session == nil {
		return
	}

	s.mu.Lock()
	primary := s.resolveHealthAddress(address)
	frames := append([]pendingFrame(nil), s.pending[primary]...)
	delete(s.pending, primary)
	for _, frame := range frames {
		delete(s.pendingKeys, pendingFrameKey(primary, frame.Frame))
	}
	s.mu.Unlock()

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
		snapshot := s.queueStateSnapshot()
		s.persistQueueState(snapshot)
		return
	}

	s.mu.Lock()
	s.pending[primary] = append(s.pending[primary], remaining...)
	for _, item := range remaining {
		s.pendingKeys[pendingFrameKey(primary, item.Frame)] = struct{}{}
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)
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
func (s *Service) flushPendingFireAndForget(id domain.ConnID, core *netcore.NetCore, address domain.PeerAddress) {
	if core == nil {
		return
	}
	_ = id
	// If there is already an active outbound session, let flushPendingPeerFrames
	// handle it to avoid double delivery.
	if session, ok := s.activePeerSession(address); ok && session != nil {
		return
	}

	s.mu.Lock()
	frames := s.pending[address]
	if len(frames) == 0 {
		s.mu.Unlock()
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
		s.mu.Unlock()
		return
	}
	if len(remaining) > 0 {
		s.pending[address] = remaining
	} else {
		delete(s.pending, address)
	}
	snapshot := s.queueStateSnapshotLocked()
	s.mu.Unlock()
	s.persistQueueState(snapshot)

	connID := core.ConnID()
	for _, item := range toSend {
		// Fire-and-forget per-item flush — Network-routed for test
		// observability; ctx is Service lifecycle (s.runCtx).
		_ = s.sendFrameViaNetwork(s.runCtx, connID, item.Frame)
		log.Debug().Str("addr", core.RemoteAddr()).Str("type", item.Frame.Type).Msg("pending_fire_and_forget_flushed_inbound")
	}
}

func (s *Service) peerSession(address domain.PeerAddress) *peerSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resolveSessionLocked(address)
}

func (s *Service) activePeerSession(address domain.PeerAddress) (*peerSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
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
func (s *Service) inboundHeartbeat(id domain.ConnID, core *netcore.NetCore, address domain.PeerAddress, stop <-chan struct{}) {
	defer crashlog.DeferRecover()
	if core == nil {
		return
	}
	timer := time.NewTimer(nextHeartbeatDuration())
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			pingFrame := protocol.Frame{Type: "ping", Node: nodeName, Network: networkName}
			// inboundHeartbeat is a goroutine loop with its own stop
			// channel; per §2.6.32 invariant the ctx passed to the
			// Network helper is Service lifecycle (s.runCtx), not a
			// per-iteration ctx. Heartbeat termination remains driven
			// by <-stop, the helper's ctx is purely a cancellation
			// boundary for the underlying SendFrame call.
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

			s.mu.RLock()
			health := s.health[s.resolveHealthAddress(address)]
			pongReceived := health != nil && !health.LastPongAt.IsZero() && health.LastPongAt.After(sentAt)
			connected := health != nil && health.Connected
			s.mu.RUnlock()

			if !connected {
				return
			}
			if !pongReceived {
				log.Warn().Str("peer", string(address)).Msg("inbound heartbeat failed, peer stalled — closing connection")
				core.Close()
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

	s.mu.RLock()
	var stale []staleEntry
	s.forEachInboundConnLocked(func(core *netcore.NetCore) bool {
		la := core.LastActivity()
		if la.IsZero() {
			return true
		}
		if now.Sub(la) >= stallThreshold {
			stale = append(stale, staleEntry{
				id:     core.ConnID(),
				addr:   core.Address(),
				ident:  core.Identity(),
				remote: core.RemoteAddr(),
			})
		}
		return true
	})
	s.mu.RUnlock()

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
		observedIP := net.ParseIP(observedHost)
		advertisedIP := net.ParseIP(advertisedHost)

		if advertisedIP != nil && !isForbiddenAdvertisedIP(advertisedIP) && advertisedHost == observedHost {
			return domain.PeerAddress(net.JoinHostPort(advertisedHost, advertisedPort)), true
		}
		if observedIP != nil && !s.isForbiddenDialIP(observedIP) {
			if advertisedIP != nil && isForbiddenAdvertisedIP(advertisedIP) {
				// When both hosts match the peer is genuinely local (e.g.
				// loopback-to-loopback in tests or a single-machine
				// cluster), so its self-reported port is authoritative.
				// Only fall back to DefaultPeerPort when the hosts differ,
				// meaning the advertised IP was likely spoofed.
				if advertisedHost == observedHost {
					return domain.PeerAddress(net.JoinHostPort(observedHost, advertisedPort)), true
				}
				return domain.PeerAddress(net.JoinHostPort(observedHost, config.DefaultPeerPort)), true
			}
			return domain.PeerAddress(net.JoinHostPort(observedHost, advertisedPort)), true
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

func isForbiddenAdvertisedIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if inCIDR(ip, "192.168.0.0/16") {
		return true
	}
	if inCIDR(ip, "172.16.0.0/21") {
		return true
	}
	return false
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
	if ip.IsLoopback() {
		return !s.allowLoopbackPeers()
	}
	if isForbiddenAdvertisedIP(ip) || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	return false
}

func (s *Service) allowLoopbackPeers() bool {
	for _, address := range []string{s.cfg.AdvertiseAddress, s.externalListenAddress(), s.cfg.ListenAddress} {
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
// Must be called with s.mu held (read lock sufficient).
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
// Must be called with s.mu held (read lock sufficient).
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
			s.mu.Lock()
			s.dialOrigin[address] = primary
			s.mu.Unlock()
		}
		return DialResult{
			Session:          session,
			ConnectedAddress: address,
		}, nil
	}
	return DialResult{}, lastErr
}

// openPeerSessionForCM opens a TCP connection, performs the protocol
// handshake (hello/welcome) and authentication, then returns the
// transport-ready session WITHOUT registering it in Service maps or
// running any application-level exchanges (subscribe_inbox, sync).
//
// All side-effects — session registration, metadata writes, subscribe,
// sync, routing — are deferred to onCMSessionEstablished which runs
// only AFTER the CM event loop validates the slot generation. This
// ensures a stale dial (generation mismatch) is discarded with zero
// externally visible mutations.
func (s *Service) openPeerSessionForCM(ctx context.Context, address domain.PeerAddress) (*peerSession, error) {
	rawConn, err := s.dialPeer(ctx, address, dialTimeout)
	if err != nil {
		return nil, err
	}
	conn := netcore.NewMeteredConn(rawConn)

	// Guard goroutine: close the connection if the parent context is cancelled
	// during the handshake phase, or if we signal abort via closeConn.
	// On success we close the abort channel WITHOUT closing conn — the CM
	// event loop takes ownership.
	abort := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-abort:
			// Handshake completed (success or handled error).
			// Do NOT close conn — caller manages lifetime.
		}
	}()

	enableTCPKeepAlive(rawConn)

	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	reader := bufio.NewReader(conn)
	s.mu.Lock()
	cid := s.nextConnIDLocked()
	s.mu.Unlock()
	session := &peerSession{
		address:      address,
		peerIdentity: "",
		connID:       cid,
		conn:         conn,
		metered:      conn,
		sendCh:       make(chan protocol.Frame, 64),
		inboxCh:      make(chan protocol.Frame, 64),
		errCh:        make(chan error, 1),
	}
	s.attachOutboundNetCore(session)

	// closeOnError tears down the session (including the outbound NetCore
	// registration) and stops the guard goroutine. Must be called on every
	// error return path; success exits via close(abort) below, leaving the
	// session live for the CM caller to own.
	closeOnError := func() {
		_ = session.Close()
		close(abort)
	}

	go s.readPeerSession(reader, session)

	welcome, err := s.peerSessionRequest(session, protocol.Frame{}, "welcome", true)
	if err != nil {
		closeOnError()
		return nil, err
	}
	if err := validateProtocolHandshake(welcome); err != nil {
		closeOnError()
		log.Warn().Err(err).Str("peer", string(address)).Int("version", welcome.Version).Msg("outbound_peer_protocol_too_old")
		// Do NOT call penalizeOldProtocolPeer here — the caller (CM dial worker)
		// propagates errIncompatibleProtocol to onCMDialFailed, which applies the
		// penalty exactly once.
		return nil, fmt.Errorf("%w: %v", errIncompatibleProtocol, err)
	}
	session.version = welcome.Version
	session.peerIdentity = domain.PeerIdentity(welcome.Address)
	session.capabilities = intersectCapabilities(localCapabilities(), welcome.Capabilities)
	// Mirror the negotiated handshake state onto the outbound NetCore so
	// that s.conns (which now holds both directions) is a
	// symmetric registry: helpers that read pc.Address()/Identity()/
	// Capabilities() get the real values instead of an anonymous entry.
	//
	// pc.Address must be the peer's advertised listen address (canonical
	// overlay key), not the dial address — for fallback-port variants the
	// two can differ. Mirror the inbound pattern from rememberConnPeerAddr.
	if session.netCore != nil {
		advertised := domain.PeerAddress(strings.TrimSpace(welcome.Listen))
		if advertised == "" {
			advertised = domain.PeerAddress(strings.TrimSpace(welcome.Address))
		}
		session.netCore.SetAddress(advertised)
		session.netCore.SetIdentity(session.peerIdentity)
		session.netCore.SetCapabilities(session.capabilities)
	}

	// Stash welcome metadata for deferred application in
	// onCMSessionEstablished after the generation check passes.
	session.welcomeMeta = &peerWelcomeMeta{
		welcome:         welcome,
		clientVersion:   welcome.ClientVersion,
		clientBuild:     welcome.ClientBuild,
		observedAddress: welcome.ObservedAddress,
	}

	if err := s.authenticatePeerSession(session, welcome); err != nil {
		closeOnError()
		return nil, err
	}

	// Handshake succeeded — stop the guard goroutine without closing conn.
	// From here on, the CM event loop owns session lifetime.
	// NOTE: session is NOT registered in s.sessions and no subscribe/sync
	// has run. readPeerSession goroutine buffers incoming frames in
	// inboxCh (capacity 64) until onCMSessionEstablished drains them.
	close(abort)

	return session, nil
}

// onCMSessionEstablished is called by ConnectionManager when a slot
// transitions to Active — i.e., AFTER the generation check passes.
//
// The callback runs in the CM event loop, so it MUST NOT perform any
// blocking I/O. All socket-level work (subscribe_inbox, syncPeerSession,
// servePeerSession) is launched in a dedicated goroutine.
//
// openPeerSessionForCM intentionally performs ONLY TCP handshake + auth
// and returns a transport-ready session with no Service-level mutations.
// If the generation check rejects the session, CM closes it with zero
// side-effects.
func (s *Service) onCMSessionEstablished(info SessionInfo) {
	session := info.Session
	if session == nil {
		return
	}
	// dialAddress is the actual TCP address used for the connection (may be
	// a fallback port). slotAddress is the CM slot's canonical address.
	// When a fallback port was used, dialOrigin[dialAddress]=slotAddress
	// was registered by dialForCM so resolveHealthAddress collapses to the
	// canonical entry. ActiveSessionLost must use slotAddress so CM can
	// match the event back to its slot.
	dialAddress := session.address
	slotAddress := info.Address

	// A completed handshake proves the peer speaks the current protocol.
	// Clear the IP-wide compatibility ban immediately — even if the later
	// application-level setup (initPeerSession) fails, the protocol is
	// compatible. Without this, a subscribe/sync failure would leave sibling
	// ports on the same host incorrectly excluded from Candidates().
	if ip, _, ok := splitHostPort(string(dialAddress)); ok {
		s.mu.Lock()
		delete(s.bannedIPSet, ip)
		s.mu.Unlock()
	}

	// Apply welcome metadata — pure map writes, no I/O.
	// Session is NOT yet registered in s.sessions / s.upstream — that
	// happens only after initPeerSession succeeds (Phase 2).
	// The CM slot is in slotStateInitializing (not Active) until
	// SessionInitReady is emitted, so Slots(), ActiveCount(), and
	// buildPeerExchangeResponse() do not expose this peer during setup.
	if wm := session.welcomeMeta; wm != nil {
		s.learnIdentityFromWelcome(wm.welcome)
		s.mu.RLock()
		healthKey := s.resolveHealthAddress(dialAddress)
		s.mu.RUnlock()
		s.addPeerID(healthKey, session.peerIdentity)
		s.addPeerVersion(healthKey, wm.clientVersion)
		s.addPeerBuild(healthKey, wm.clientBuild)
		s.recordObservedAddress(session.peerIdentity, wm.observedAddress)
		session.welcomeMeta = nil // release for GC
	}

	// Launch the session goroutine. All blocking I/O (subscribe, sync,
	// serve loop) runs here, never in the CM event loop.
	savedGen := info.SlotGeneration
	go func() {
		defer crashlog.DeferRecover()

		// --- Phase 1: application-level setup (blocking I/O) ---
		if err := s.initPeerSession(session); err != nil {
			log.Warn().Err(err).Str("peer", string(dialAddress)).Msg("cm_session_setup_failed")
			// Session was never registered in s.sessions / s.upstream, so
			// only dialOrigin (set by dialForCM for fallback ports) needs cleanup.
			s.mu.Lock()
			delete(s.dialOrigin, dialAddress)
			s.mu.Unlock()
			_ = session.Close()
			// Notify CM so it can reconnect with backoff.
			s.connManager.EmitSlot(ActiveSessionLost{
				Address:        slotAddress,
				Identity:       session.peerIdentity,
				Error:          err,
				WasHealthy:     false,
				SlotGeneration: savedGen,
			})
			return
		}

		// --- Phase 2: session is fully operational ---
		// Promote CM slot from Initializing → Active so Slots(),
		// ActiveCount(), and buildPeerExchangeResponse() start exposing
		// this peer. Until this event, the slot is invisible to peer
		// exchange and diagnostics.
		s.connManager.EmitSlot(SessionInitReady{
			Address:        slotAddress,
			SlotGeneration: savedGen,
		})

		// Register session in Service maps. This is the first point where
		// the session becomes visible to routingTargets, enqueuePeerFrame,
		// connectedHostsLocked, and other lookups.
		s.mu.Lock()
		s.sessions[dialAddress] = session
		s.upstream[dialAddress] = struct{}{}
		s.mu.Unlock()

		s.markPeerConnected(dialAddress, peerDirectionOutbound)
		s.flushPendingPeerFrames(dialAddress)

		// Routing table: register direct peer.
		s.onPeerSessionEstablished(session.peerIdentity, s.sessionHasCapability(dialAddress, domain.CapMeshRelayV1))

		// Send full table sync to the new peer (Phase 1.2).
		if session.peerIdentity != "" && s.sessionHasCapability(dialAddress, domain.CapMeshRoutingV1) && s.sessionHasCapability(dialAddress, domain.CapMeshRelayV1) {
			routes := s.routingTable.AnnounceTo(session.peerIdentity)
			if len(routes) > 0 {
				if !s.SendAnnounceRoutes(dialAddress, routes) {
					log.Warn().
						Str("peer", string(session.peerIdentity)).
						Str("address", string(dialAddress)).
						Int("routes", len(routes)).
						Msg("routing_outbound_full_sync_failed")
				}
			}
		}

		// --- Phase 3: main session loop ---
		err := s.servePeerSession(s.runCtx, session)

		// servePeerSession already called markPeerDisconnected on some paths.
		// Clean up session from Service maps — but only if the map entry
		// still belongs to THIS session. A replacement session for the same
		// dial address may have been registered by a concurrent reconnect
		// before this goroutine finished unwinding.
		//
		// ownedCleanup tracks whether THIS goroutine won the race to remove
		// the session entry. The winner is responsible for routing
		// deregistration (onPeerSessionClosed). If onCMSessionTeardown
		// already deleted the entry (CM-initiated close), the goroutine
		// must NOT call onPeerSessionClosed a second time — that would
		// double-decrement the identity session counter and could remove
		// a live route belonging to a replacement session.
		s.mu.Lock()
		ownedCleanup := s.sessions[dialAddress] == session
		if ownedCleanup {
			delete(s.sessions, dialAddress)
			delete(s.upstream, dialAddress)
			delete(s.dialOrigin, dialAddress)
		}
		s.mu.Unlock()

		// Routing table: deregister direct peer only if this goroutine
		// owns the cleanup (i.e. onCMSessionTeardown did not run first).
		if ownedCleanup && session.peerIdentity != "" {
			hadRelay := sessionHasCap(session.capabilities, domain.CapMeshRelayV1)
			s.onPeerSessionClosed(session.peerIdentity, hadRelay)
		}

		// Accumulate traffic metrics from the metered connection.
		if session.metered != nil {
			s.accumulateSessionTraffic(dialAddress, session.metered)
		}

		// Emit ActiveSessionLost using the slot's canonical address so
		// CM.findSlotLocked can match it. When a fallback port was used,
		// dialAddress != slotAddress; using dialAddress here would cause
		// CM to treat the event as unknown and never reconnect.
		s.connManager.EmitSlot(ActiveSessionLost{
			Address:        slotAddress,
			Identity:       session.peerIdentity,
			Error:          err,
			WasHealthy:     true,
			SlotGeneration: savedGen,
		})
	}()
}

// onCMSessionTeardown is called by ConnectionManager when an active slot
// is deactivated (replace or shutdown). Cleans up Service-level state.
// Note: the CM has already closed the TCP transport at this point, so
// servePeerSession will detect EOF and exit — it calls markPeerDisconnected
// itself, so we must NOT call it here (that would be a double penalty).
// The goroutine emits a stale ActiveSessionLost that the generation guard
// suppresses.
func (s *Service) onCMSessionTeardown(info SessionInfo) {
	// Session is registered under DialAddress (the actual TCP address used,
	// which may be a fallback port). Use DialAddress for cleanup, falling
	// back to the canonical Address when DialAddress is empty (pre-activation
	// teardown paths where no session was established).
	addr := info.DialAddress
	if addr == "" {
		addr = info.Address
	}

	// Whoever removes the session entry from s.sessions owns routing
	// deregistration. When a peer-initiated EOF causes servePeerSession to
	// exit, the goroutine cleanup may win this race and delete the entry
	// before onCMSessionTeardown runs (via the stale ActiveSessionLost →
	// deactivateSlot path). In that case the goroutine already called
	// onPeerSessionClosed, and calling it again here would double-decrement
	// the identity session counter.
	s.mu.Lock()
	ownedCleanup := info.Session != nil && s.sessions[addr] == info.Session
	if ownedCleanup {
		delete(s.sessions, addr)
		delete(s.upstream, addr)
		delete(s.dialOrigin, addr)
	}
	s.mu.Unlock()

	// Routing table: deregister direct peer only if we owned the entry.
	// Pointer-compare ensures we don't accidentally delete or deregister
	// a replacement session that was registered for the same address
	// between deactivateSlotLocked and this callback.
	if ownedCleanup && info.Identity != "" {
		hadRelay := sessionHasCap(info.Capabilities, domain.CapMeshRelayV1)
		s.onPeerSessionClosed(info.Identity, hadRelay)
	}
}

// initPeerSession performs the application-level setup for an already
// authenticated session: subscribes to the peer's inbox relay and runs
// the initial sync (conditionally get_peers, then fetch_contacts).
// Whether get_peers is sent depends on the aggregate network status at
// the moment of entry — see shouldRequestPeers() for the policy.
// This is the blocking I/O phase that must NOT run in the CM event
// loop — call it from a dedicated goroutine only.
//
// On success the session is ready for the servePeerSession read loop.
// On error the caller is responsible for session cleanup and CM notification.
func (s *Service) initPeerSession(session *peerSession) error {
	// Evaluate peer exchange policy before subscribe_inbox so that any
	// network I/O done during subscribe cannot shift the aggregate status
	// between the decision point and its use. In the CM path the session
	// is not yet registered in s.health, so the snapshot naturally excludes
	// the peer being set up — matching the legacy path (openPeerSession).
	requestPeers := s.shouldRequestPeers()
	if !requestPeers {
		s.logPeerExchangeSkipped(peerExchangePathSessionCM, session.address, peerExchangeSkipByAggregateHealthy)
	}

	if _, err := s.peerSessionRequest(session, protocol.Frame{
		Type:       "subscribe_inbox",
		Topic:      "dm",
		Recipient:  s.identity.Address,
		Subscriber: s.cfg.AdvertiseAddress,
	}, "subscribed", false); err != nil {
		return fmt.Errorf("subscribe_inbox: %w", err)
	}
	_ = session.conn.SetDeadline(time.Time{})
	log.Info().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Msg("upstream subscription established")

	if err := s.syncPeerSession(session, requestPeers, peerExchangePathSessionCM); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	return nil
}

// onCMStaleSession is called by ConnectionManager when handleDialSucceeded
// detects a generation mismatch and discards the session.
//
// After the refactor, openPeerSessionForCM no longer registers the session
// in s.sessions (that happens in onCMSessionEstablished, which never runs
// for stale generations). However, dialForCM still registers
// dialOrigin[fallback]=primary BEFORE the generation check. This callback
// cleans up that leaked entry.
func (s *Service) onCMStaleSession(session *peerSession) {
	s.mu.Lock()
	// Clean up dialOrigin if it was registered for this address.
	// No pointer-compare needed — dialOrigin maps addresses, not sessions.
	delete(s.dialOrigin, session.address)
	s.mu.Unlock()
}

// onCMDialFailed is called by ConnectionManager when a dial attempt fails.
// Updates health/ban state BEFORE fill() re-queries Candidates().
func (s *Service) onCMDialFailed(address domain.PeerAddress, err error, incompatible bool) {
	if incompatible {
		s.penalizeOldProtocolPeer(address)
	} else {
		s.markPeerDisconnected(address, err)
	}
}

// buildPeerExchangeResponse merges CM Active slots and PeerProvider Candidates,
// deduplicates by IP (active has priority), and optionally filters by caller's
// network groups. Used for both remote peer exchange and local RPC enrichment.
// ActivePeersJSON returns a JSON-encoded snapshot of ConnectionManager slots.
// Implements rpc.ConnectionDiagnosticProvider.
func (s *Service) ActivePeersJSON() (json.RawMessage, error) {
	type response struct {
		Slots    []SlotInfo `json:"slots"`
		Count    int        `json:"count"`
		MaxSlots int        `json:"max_slots"`
	}

	var slots []SlotInfo
	if s.connManager != nil {
		slots = s.connManager.Slots()
	}
	resp := response{
		Slots:    slots,
		Count:    len(slots),
		MaxSlots: s.cfg.EffectiveMaxOutgoingPeers(),
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

func (s *Service) buildPeerExchangeResponse(callerGroups map[domain.NetGroup]struct{}) []domain.PeerAddress {
	seenIPs := make(map[string]struct{})

	// 1. Active connections first — verified by live TCP, highest priority.
	var active []domain.PeerAddress
	if s.connManager != nil {
		for _, slot := range s.connManager.Slots() {
			if slot.State != "active" {
				continue
			}
			addr := slot.Address
			if slot.ConnectedAddress != nil {
				addr = *slot.ConnectedAddress
			}
			if shouldHidePeerExchangeAddress(addr) {
				continue
			}
			ip, _, ok := splitHostPort(string(addr))
			if ok {
				if _, exists := seenIPs[ip]; !exists {
					seenIPs[ip] = struct{}{}
					active = append(active, addr)
				}
			}
		}
	}

	// 2. Inbound-only peers — authenticated but not in CM (CM tracks outbound).
	// Without this, live inbound peers would be invisible to get_peers because
	// Candidates() excludes connected IPs via ConnectedFn.
	var inbound []domain.PeerAddress
	s.mu.RLock()
	for addr, h := range s.health {
		if h.Direction != peerDirectionInbound || !h.Connected {
			continue
		}
		if shouldHidePeerExchangeAddress(addr) {
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
	s.mu.RUnlock()

	// 3. Supplement with candidates from PeerProvider (already sorted by
	// score descending inside Candidates()).
	var candidates []domain.PeerAddress
	if s.peerProvider != nil {
		for _, candidate := range s.peerProvider.Candidates() {
			if shouldHidePeerExchangeAddress(candidate.Address) {
				continue
			}
			ip, _, ok := splitHostPort(string(candidate.Address))
			if ok {
				if _, exists := seenIPs[ip]; !exists {
					seenIPs[ip] = struct{}{}
					candidates = append(candidates, candidate.Address)
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
