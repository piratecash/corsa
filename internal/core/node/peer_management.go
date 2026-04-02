package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"corsa/internal/core/config"
	"corsa/internal/core/crashlog"
	"corsa/internal/core/domain"
	"corsa/internal/core/identity"
	"corsa/internal/core/protocol"
	"corsa/internal/core/transport"
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

	s.ensurePeerSessions(ctx)
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
			s.ensurePeerSessions(ctx)
			s.retryRelayDeliveries()
			s.relayLimiter.cleanup(5 * time.Minute)
			s.maybeSavePeerState()
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
	s.mu.Unlock()

	sortPeerEntries(entries)
	entries = trimPeerEntries(entries)

	state := peerStateFile{
		Version: peerStateVersion,
		Peers:   entries,
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
				continue
			}
		}
		kept = append(kept, peer)
	}
	s.peers = kept
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
				Source:   pm.Source,
				AddedAt:  pm.AddedAt,
			}
			// If runtime has a fresher NodeType (e.g. from a hello/welcome),
			// prefer it over the persisted value.
			if rt := s.peerTypes[peer.Address]; rt != "" {
				entry.NodeType = rt
			}
		} else {
			// New peer discovered at runtime — derive from live state.
			entry = peerEntry{
				Address:  peer.Address,
				NodeType: s.peerTypes[peer.Address],
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
		entry.Network = classifyAddress(entry.Address)
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
// relied on connPeerInfo.address, allowing a second outbound connection
// to the same machine.
// Caller must hold s.mu at least for read.
func (s *Service) connectedHostsLocked() map[string]struct{} {
	hosts := make(map[string]struct{}, len(s.upstream)+len(s.inboundConns))

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
	now := time.Now().UTC()
	stallThreshold := heartbeatInterval + pongStallTimeout
	for conn := range s.inboundConns {
		if info := s.connPeerInfo[conn]; info != nil {
			if !info.lastActivity.IsZero() && now.Sub(info.lastActivity) >= stallThreshold {
				continue
			}
		}
		if ip := remoteIP(conn.RemoteAddr()); ip != "" {
			hosts[ip] = struct{}{}
		}
	}

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
// a full handshake (hello → welcome → auth if required), fetches peer
// addresses and contacts, and imports any verified contacts into local
// state. Returns the number of contacts successfully imported.
//
// A fresh connection is used instead of reusing the active session
// because syncPeer is called from handlePeerSessionFrame when a
// push_message fails with ErrCodeUnknownSenderKey. That handler runs
// inline inside peerSessionRequest. Reusing the session would call
// syncPeerSession → peerSessionRequest on the same inboxCh, consuming
// frames meant for the outer caller and causing a 12-second stall
// (peerRequestTimeout).
func (s *Service) syncPeer(ctx context.Context, address domain.PeerAddress) int {
	conn, err := s.dialPeer(ctx, address, syncHandshakeTimeout)
	if err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_dial_failed")
		return 0
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_hello_write_failed")
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
			Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_marshal_failed")
			return 0
		}
		if _, err := io.WriteString(conn, authLine); err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_auth_write_failed")
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

	if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "get_peers"}); err == nil {
		if _, err := io.WriteString(conn, line); err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_get_peers_failed")
			return 0
		}
		reply, err := readFrameLine(reader, maxResponseLineBytes)
		if err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_get_peers_read_failed")
			return 0
		}
		frame, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
		if err == nil {
			for _, peer := range frame.Peers {
				s.addPeerAddress(domain.PeerAddress(peer), "", "")
			}
		}
	} else {
		return 0
	}

	imported := 0
	if line, err := protocol.MarshalFrameLine(protocol.Frame{Type: "fetch_contacts"}); err == nil {
		if _, err := io.WriteString(conn, line); err != nil {
			log.Warn().Err(err).Str("peer", string(address)).Msg("sync_peer_fetch_contacts_failed")
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
	conn := NewMeteredConn(rawConn)
	defer func() {
		s.accumulateSessionTraffic(address, conn)
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
	s.learnIdentityFromWelcome(welcome)
	// learnIdentityFromWelcome stores version/build keyed by the normalized
	// listen address, which may differ from the dial address (or be absent
	// entirely when the peer omits the listen field). Store by the
	// health-tracking key (resolveHealthAddress) so peerHealthFrames can
	// always find the version — even for fallback-port dial variants.
	s.mu.RLock()
	healthKey := s.resolveHealthAddress(address)
	s.mu.RUnlock()
	s.addPeerVersion(healthKey, welcome.ClientVersion)
	s.addPeerBuild(healthKey, welcome.ClientBuild)
	if err := s.authenticatePeerSession(session, welcome); err != nil {
		return false, err
	}
	// Record observed address only after authentication succeeds so that
	// an unauthenticated responder cannot influence NAT consensus.
	s.recordObservedAddress(domain.PeerIdentity(welcome.Address), welcome.ObservedAddress)
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

	if err := s.syncPeerSession(session); err != nil {
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
	// Event-driven pending queue drain: a direct route was added by
	// onPeerSessionEstablished before we entered the main loop. Now that
	// inboxCh is actively read (preventing overflow), drain any pending
	// send_message frames that target this peer's identity.
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

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-session.errCh:
			log.Info().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Err(err).Msg("upstream subscription closed")
			s.markPeerDisconnected(session.address, err)
			return err
		case frame := <-session.inboxCh:
			s.markPeerRead(session.address, frame)
			s.handlePeerSessionFrame(session.address, frame)
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
				// written directly without waiting for a response. The remote
				// side may or may not send an ack — we don't block the session
				// waiting for one.
				line, err := protocol.MarshalFrameLine(outbound)
				if err != nil {
					continue
				}
				_ = session.conn.SetWriteDeadline(time.Now().Add(sessionWriteTimeout))
				s.markPeerWrite(session.address, outbound)
				if _, writeErr := io.WriteString(session.conn, line); writeErr != nil {
					log.Warn().Str("peer", string(session.address)).Str("type", outbound.Type).Err(writeErr).Msg("fire_and_forget_write_failed")
					_ = session.conn.SetWriteDeadline(time.Time{})
					s.markPeerDisconnected(session.address, writeErr)
					return writeErr
				}
				_ = session.conn.SetWriteDeadline(time.Time{})
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
		Signature: identity.SignPayload(s.identity, sessionAuthPayload(welcome.Challenge, s.identity.Address)),
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
			// Use promotePeerAddress: this is a direct hello from the peer
			// itself, so its self-reported node_type is authoritative and
			// must overwrite any earlier value (which could have come from
			// an unauthenticated announce_peer with a wrong type).
			s.promotePeerAddress(normalizedAddr, frame.NodeType)
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

	s.mu.Unlock()

	// Flush immediately so the manual peer survives a crash.
	s.flushPeerState()

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

func (s *Service) addPeerAddress(address domain.PeerAddress, nodeType string, peerID domain.PeerIdentity) {
	if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, peer := range s.peers {
		if peer.Address == address {
			// Peer already known — do not overwrite its node type.
			// The type was set from a trusted source (bootstrap config,
			// auth handshake, or authenticated announce_peer). Allowing
			// any caller to retag it would let unauthenticated senders
			// downgrade a "full" peer to "client" and break routing.
			if peerID != "" {
				s.peerIDs[address] = peerID
			}
			return
		}
	}

	s.peers = append(s.peers, transport.Peer{
		Address: address,
		Source:  domain.PeerSourcePeerExchange,
	})
	s.peerTypes[address] = normalizePeerNodeType(nodeType)
	if peerID != "" {
		s.peerIDs[address] = peerID
	}
	// Eagerly populate persistedMeta so that AddedAt is available for
	// eviction decisions immediately, without waiting for a flush cycle.
	if _, ok := s.persistedMeta[address]; !ok {
		now := time.Now().UTC()
		s.persistedMeta[address] = &peerEntry{
			Address:  address,
			NodeType: normalizePeerNodeType(nodeType),
			Source:   domain.PeerSourcePeerExchange,
			AddedAt:  &now,
		}
	}
}

// promotePeerAddress adds or updates a peer learned from an authenticated
// announce_peer. Unlike addPeerAddress it is allowed to update the node
// type and reset cooldown for an already-known peer, because the sender
// has been verified.
//
// The peer is NOT moved to the front of the dial list. Letting remote
// peers control dial priority would allow an attacker to flood the list
// with fake addresses and push real peers down. The dial order is managed
// solely by the local bootstrap/health logic.
func (s *Service) promotePeerAddress(address domain.PeerAddress, nodeType string) {
	if address == "" || s.isSelfAddress(address) || s.shouldSkipDialAddress(address) {
		return
	}

	peerType := normalizePeerNodeType(nodeType)

	s.mu.Lock()
	defer s.mu.Unlock()

	found := false
	for _, peer := range s.peers {
		if peer.Address == address {
			found = true
			break
		}
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
				NodeType: peerType,
				Source:   domain.PeerSourceAnnounce,
				AddedAt:  &now,
			}
		}
	}

	s.peerTypes[address] = peerType

	// Reset cooldown so the peer is dialled on the next bootstrap tick.
	// Use resolveHealthAddress to find the primary entry: if this address
	// is a known fallback variant, reset cooldown on the primary too so
	// the shared reputation invariant is maintained.
	healthAddr := s.resolveHealthAddress(address)
	if h := s.health[healthAddr]; h != nil {
		h.ConsecutiveFailures = 0
		h.LastDisconnectedAt = time.Time{}
		h.BannedUntil = time.Time{}
	}
	// Also reset the direct address entry if it differs from primary
	// (the announced address itself may have accumulated failures).
	if healthAddr != address {
		if h := s.health[address]; h != nil {
			h.ConsecutiveFailures = 0
			h.LastDisconnectedAt = time.Time{}
			h.BannedUntil = time.Time{}
		}
	}
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

func normalizePeerNodeType(raw string) domain.NodeType {
	if t, ok := domain.ParseNodeType(raw); ok {
		return t
	}
	return domain.NodeTypeFull
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
	return domain.NodeTypeFull
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
			select {
			case session.errCh <- err:
			default:
			}
			return
		}

		frame, err := protocol.ParseFrameLine(strings.TrimSpace(line))
		if err != nil {
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
	_ = session.conn.SetWriteDeadline(time.Now().Add(sessionWriteTimeout))
	if hello {
		line := s.nodeHelloJSONLine()
		s.markPeerWrite(session.address, protocol.Frame{Type: "hello"})
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	} else {
		line, err := protocol.MarshalFrameLine(frame)
		if err != nil {
			return protocol.Frame{}, err
		}
		s.markPeerWrite(session.address, frame)
		if _, err := io.WriteString(session.conn, line); err != nil {
			return protocol.Frame{}, err
		}
	}
	_ = session.conn.SetWriteDeadline(time.Time{})

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
				_ = session.conn.SetWriteDeadline(time.Now().Add(sessionWriteTimeout))
				pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
				s.writeJSONFrame(session.conn, pongFrame)
				_ = session.conn.SetWriteDeadline(time.Time{})
				s.markPeerWrite(session.address, pongFrame)
				continue
			}
			if incoming.Type == "push_message" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "push_delivery_receipt" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "announce_peer" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "request_inbox" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "subscribe_inbox" {
				s.handlePeerSessionFrame(session.address, incoming)
				continue
			}
			if incoming.Type == "relay_hop_ack" {
				s.handlePeerSessionFrame(session.address, incoming)
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

func (s *Service) syncPeerSession(session *peerSession) error {
	peersFrame, err := s.peerSessionRequest(session, protocol.Frame{Type: "get_peers"}, "peers", false)
	if err != nil {
		return err
	}
	for _, peer := range peersFrame.Peers {
		s.addPeerAddress(domain.PeerAddress(peer), "", "")
	}

	_, err = s.syncContactsViaSession(session)
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
// The syncSession parameter, when non-nil, is used directly instead of
// looking up a session by address. Callers pass nil when the only candidate
// session is currently inside a peerSessionRequest read loop (e.g.,
// handlePeerSessionFrame dispatched during a ping), because the
// single-reader constraint on inboxCh would cause a deadlock.
func (s *Service) syncSenderKeys(senderAddress domain.PeerAddress, syncSession *peerSession) int {
	if syncSession != nil {
		imported, err := s.syncContactsViaSession(syncSession)
		if err == nil {
			if imported > 0 {
				log.Info().Str("peer", string(senderAddress)).Int("imported", imported).Msg("sync_sender_keys_via_session")
			}
			return imported
		}
		log.Warn().Err(err).Str("peer", string(senderAddress)).Msg("sync_sender_keys_session_failed")
	}

	// Fall back to a fresh TCP connection.
	ctx, cancel := context.WithTimeout(context.Background(), syncHandshakeTimeout)
	defer cancel()
	return s.syncPeer(ctx, senderAddress)
}

func (s *Service) handlePeerSessionFrame(address domain.PeerAddress, frame protocol.Frame) {
	// Respond to inbound pings on outbound sessions so the remote
	// heartbeat monitor receives a timely pong. Without this the
	// remote side closes the connection after pongStallTimeout.
	// Pings are not "useful" application traffic, only keep-alive.
	if frame.Type == "ping" {
		s.mu.RLock()
		session := s.sessions[address]
		s.mu.RUnlock()
		if session != nil {
			_ = session.conn.SetWriteDeadline(time.Now().Add(sessionWriteTimeout))
			pongFrame := protocol.Frame{Type: "pong", Node: nodeName, Network: networkName}
			s.writeJSONFrame(session.conn, pongFrame)
			_ = session.conn.SetWriteDeadline(time.Time{})
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

		stored, _, errCode := s.storeIncomingMessage(msg, true)
		if !stored && errCode == protocol.ErrCodeUnknownSenderKey {
			// Pass nil for syncSession: this handler runs on the outbound
			// session event loop and may be inside peerSessionRequest
			// (single-reader constraint). syncSenderKeys falls back to a
			// fresh TCP dial.
			s.syncSenderKeys(address, nil)
			stored, _, _ = s.storeIncomingMessage(msg, true)
		}
		if stored {
			s.sendAckDeleteToPeer(address, "dm", msg.ID, "")
		} else {
			log.Warn().Str("peer", string(address)).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Msg("push_message_store_failed_no_ack_delete")
		}
		log.Info().Str("peer", string(address)).Str("id", string(msg.ID)).Str("recipient", msg.Recipient).Bool("stored", stored).Msg("received pushed message")
	case "push_delivery_receipt":
		if frame.Receipt == nil {
			return
		}
		receipt, err := receiptFromReceiptFrame(*frame.Receipt)
		if err != nil {
			return
		}
		s.storeDeliveryReceipt(receipt)
		s.sendAckDeleteToPeer(address, "receipt", receipt.MessageID, receipt.Status)
		log.Info().Str("peer", string(address)).Str("message_id", string(receipt.MessageID)).Str("recipient", receipt.Recipient).Str("status", receipt.Status).Msg("received pushed delivery receipt")
	case "request_inbox":
		s.mu.RLock()
		session := s.sessions[address]
		s.mu.RUnlock()
		s.respondToInboxRequest(session)
	case "subscribe_inbox":
		s.mu.RLock()
		session := s.sessions[address]
		s.mu.RUnlock()
		if session != nil {
			reply, sub := s.subscribeInboxFrame(session.conn, frame)
			s.writeJSONFrame(session.conn, reply)
			if sub != nil {
				go s.pushBacklogToSubscriber(sub)
			}
		}
	case "announce_peer":
		nodeType := frame.NodeType
		if !isKnownNodeType(nodeType) {
			return
		}
		peers := frame.Peers
		if len(peers) > maxAnnouncePeers {
			peers = peers[:maxAnnouncePeers]
		}
		for _, peer := range peers {
			if peer == "" || classifyAddress(domain.PeerAddress(peer)) == domain.NetGroupLocal {
				continue
			}
			s.promotePeerAddress(domain.PeerAddress(peer), nodeType)
			log.Info().Str("peer", peer).Str("node_type", nodeType).Str("from", string(address)).Msg("learned peer from announce")
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
		s.writeJSONFrame(session.conn, protocol.Frame{
			Type:      "push_message",
			Topic:     "dm",
			Recipient: peerID,
			Item:      &msgFrame,
		})
	}

	receipts := s.fetchDeliveryReceiptsFrame(peerID)
	for _, item := range receipts.Receipts {
		receiptFrame := item
		s.writeJSONFrame(session.conn, protocol.Frame{
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
	frame := protocol.Frame{
		Type:      "ack_delete",
		Address:   s.identity.Address,
		AckType:   ackType,
		ID:        string(id),
		Status:    status,
		Signature: identity.SignPayload(s.identity, ackDeletePayload(s.identity.Address, ackType, string(id), status)),
	}
	if s.enqueuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "session").Msg("ack_delete_send")
		return
	}
	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("peer", string(address)).Str("type", ackType).Str("id", string(id)).Str("status", status).Str("mode", "queued").Msg("ack_delete_send")
	}
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
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

func (s *Service) markPeerRead(address domain.PeerAddress, frame protocol.Frame) {
	accepted := frame.Type != "error"
	log.Debug().
		Str("protocol", "json/tcp").
		Str("addr", string(address)).
		Str("direction", "recv").
		Str("command", frame.Type).
		Bool("accepted", accepted).
		Msg("protocol_trace")

	s.mu.Lock()
	defer s.mu.Unlock()

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	if frame.Type == "pong" {
		health.LastPongAt = now
		s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
		return
	}
	if frame.Type != "" {
		health.LastUsefulReceiveAt = now
	}
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

func (s *Service) markPeerUsefulReceive(address domain.PeerAddress) {
	s.mu.Lock()
	defer s.mu.Unlock()

	health := s.ensurePeerHealthLocked(address)
	health.LastUsefulReceiveAt = time.Now().UTC()
	s.updatePeerStateLocked(health, s.computePeerStateLocked(health))
}

// nextConnIDLocked returns a monotonically increasing connection ID.
// Must be called with s.mu held (write lock).
func (s *Service) nextConnIDLocked() uint64 {
	s.connIDCounter++
	return s.connIDCounter
}

// inboundConnIDsLocked returns the connection IDs for all active inbound
// connections that declared the given overlay address in their hello frame.
// Must be called with s.mu held (read lock).
func (s *Service) inboundConnIDsLocked(address domain.PeerAddress) []uint64 {
	var ids []uint64
	for _, info := range s.connPeerInfo {
		if info != nil && info.address == address {
			ids = append(ids, info.connID)
		}
	}
	return ids
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
}

func (s *Service) peerHealthFrames() []protocol.PeerHealthFrame {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
			State:               s.computePeerStateLocked(health),
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
				phf.ConnID = session.connID
				break
			}
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
// first, then falls back to inbound connPeerInfo.
// Must be called while holding s.mu at least for read.
func (s *Service) peerCapabilitiesLocked(address domain.PeerAddress) []string {
	if session, ok := s.sessions[address]; ok && len(session.capabilities) > 0 {
		return domain.CapabilityStrings(session.capabilities)
	}
	for _, info := range s.connPeerInfo {
		if info.address == address && len(info.capabilities) > 0 {
			return domain.CapabilityStrings(info.capabilities)
		}
	}
	return nil
}

func (s *Service) computePeerStateLocked(health *peerHealth) string {
	if !health.Connected {
		return peerStateReconnecting
	}

	now := time.Now().UTC()
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
	case "send_delivery_receipt":
		return string(address) + "|send_delivery_receipt|" + frame.ID + "|" + frame.Recipient + "|" + frame.Status
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

func (s *Service) peerSession(address domain.PeerAddress) *peerSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[address]
}

func (s *Service) activePeerSession(address domain.PeerAddress) (*peerSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session := s.sessions[address]
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
	return s.computePeerStateLocked(health)
}

func nextHeartbeatDuration() time.Duration {
	jitter := time.Duration(time.Now().UTC().UnixNano()%15) * time.Second
	return heartbeatInterval + jitter
}

// inboundHeartbeat periodically pings an inbound peer to independently verify
// liveness. The pong reply is handled by handleCommand which calls markPeerRead.
// If the peer does not respond within pongStallTimeout after a ping, the
// connection is closed — same semantics as outbound session heartbeats.
func (s *Service) inboundHeartbeat(conn net.Conn, address domain.PeerAddress, stop <-chan struct{}) {
	defer crashlog.DeferRecover()
	timer := time.NewTimer(nextHeartbeatDuration())
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			pingFrame := protocol.Frame{Type: "ping", Node: nodeName, Network: networkName}
			s.writeJSONFrame(conn, pingFrame)
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
				_ = conn.Close()
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
// These zombie connections occupy a slot in inboundConns and block outbound
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

	s.mu.RLock()
	var stale []net.Conn
	for conn := range s.inboundConns {
		info := s.connPeerInfo[conn]
		if info == nil {
			continue
		}
		if info.lastActivity.IsZero() {
			continue
		}
		if now.Sub(info.lastActivity) >= stallThreshold {
			stale = append(stale, conn)
		}
	}
	s.mu.RUnlock()

	for _, conn := range stale {
		var addr domain.PeerAddress
		var ident domain.PeerIdentity
		s.mu.RLock()
		if info := s.connPeerInfo[conn]; info != nil {
			addr = info.address
			ident = info.identity
		}
		s.mu.RUnlock()
		log.Warn().Str("peer", string(addr)).Str("identity", string(ident)).Str("remote", conn.RemoteAddr().String()).Msg("force-closing stale inbound connection")
		_ = conn.Close()
	}
}

// touchConnActivity updates the per-connection last activity timestamp.
func (s *Service) touchConnActivity(conn net.Conn) {
	s.mu.Lock()
	if info := s.connPeerInfo[conn]; info != nil {
		info.lastActivity = time.Now().UTC()
	}
	s.mu.Unlock()
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
