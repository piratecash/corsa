package node

// peer_sessions.go hosts the session lifecycle adapters: outbound
// session open/serve, the ConnectionManager callbacks
// (onCMSessionEstablished / onCMSessionTeardown / onCMStaleSession /
// onCMDialFailed), and the markPeerConnected / markPeerDisconnected
// transitions driven by those events. The file was split out of
// peer_management.go because this subsystem is the hot edit zone for
// routing-announce v2 and capability propagation, and keeping the
// lifecycle in its own file keeps that churn from masking unrelated
// changes in the rest of peer_management.go.
//
// Contract references:
//   - connection_manager.go (ConnectionManager.Config doc comments on
//     OnSessionEstablished / OnSessionTeardown / OnStaleSession /
//     OnDialFailed) — authoritative contract for the CM callbacks
//     implemented in this file.
//   - docs/locking.md — lock ordering for markPeerConnected /
//     markPeerDisconnected (peerMu -> ipStateMu -> statusMu) and the
//     rule that side-effects run after domain mutexes are released.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// applyWelcomeMetadata copies the negotiated handshake state from a
// validated welcome frame into session and mirrors it onto the outbound
// NetCore so s.conns (which holds both directions) is a symmetric
// registry — helpers reading pc.Address()/Identity()/Capabilities()
// observe the real peer values instead of an anonymous entry.
//
// NetCore.Address is set to the peer's advertised listen address
// (canonical overlay key), NOT the dial address — for fallback-port
// variants those two can differ. When the peer omits welcome.Listen we
// fall back to welcome.Address (identity), mirroring the inbound pattern
// from rememberConnPeerAddr.
//
// This helper is pure metadata assignment: it does not touch
// learnIdentityFromWelcome, peerHealth maps or welcomeMeta — each
// outbound path (legacy openPeerSession vs CM openPeerSessionForCM)
// decides separately when to run those side-effects because the CM path
// defers them until the generation check passes.
func applyWelcomeMetadata(session *peerSession, welcome protocol.Frame) {
	session.version = welcome.Version
	session.peerIdentity = domain.PeerIdentity(welcome.Address)
	session.capabilities = intersectCapabilities(localCapabilities(), welcome.Capabilities)
	if session.netCore == nil {
		return
	}
	advertised := domain.PeerAddress(strings.TrimSpace(welcome.Listen))
	if advertised == "" {
		advertised = domain.PeerAddress(strings.TrimSpace(welcome.Address))
	}
	session.netCore.SetAddress(advertised)
	session.netCore.SetIdentity(session.peerIdentity)
	session.netCore.SetCapabilities(session.capabilities)
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
			log.Trace().Str("site", "runPeerSession_cleanup").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
			s.peerMu.Lock()
			log.Trace().Str("site", "runPeerSession_cleanup").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
			closedSession := s.sessions[address]
			delete(s.sessions, address)
			s.peerMu.Unlock()
			log.Trace().Str("site", "runPeerSession_cleanup").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

			// Routing table: deregister direct peer on session close.
			// The capability slice must match what was passed to
			// onPeerSessionEstablished for balanced accounting — the session
			// object still owns it here since we just removed it from
			// s.sessions and no other goroutine mutates session.capabilities
			// after applyWelcomeMetadata.
			if closedSession != nil && closedSession.peerIdentity != "" {
				s.onPeerSessionClosed(closedSession.peerIdentity, closedSession.capabilities)
			}
			// Self-identity short-circuit — must run BEFORE the generic
			// markPeerDisconnected penalty. openPeerSession surfaces the
			// self-loopback in two shapes: (a) *selfIdentityError when
			// the welcome carries our Ed25519 address (line 1044), and
			// (b) protocol.ErrSelfIdentity bubbled up from peerSessionRequest
			// when the remote answered with connection_notice{peer-banned,
			// reason=self-identity}. Both must land on
			// applySelfIdentityCooldown (24h BannedUntil + LastErrorCode =
			// self-identity) instead of the generic transient-disconnect
			// penalty, and the loop must return so this address is NOT
			// retried after 2s — the CM path and syncPeer already
			// converge on this contract; runPeerSession was the last
			// legacy path churning against self-aliases otherwise.
			if s.tryApplySelfIdentityCooldown(address, err) {
				log.Warn().Str("peer", string(address)).Msg("peer_session_stopped_self_identity")
				return
			}
			// servePeerSession calls markPeerDisconnected before
			// returning its error.  For all other error paths (dial
			// failure, handshake, subscribe, sync) we must call it here
			// so that Score decreases and cooldown engages.
			if connected {
				s.peerMu.RLock()
				h := s.health[s.resolveHealthAddress(address)]
				stillConnected := h != nil && h.Connected
				s.peerMu.RUnlock()
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
	log.Trace().Str("site", "openPeerSession_nextCID").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "openPeerSession_nextCID").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	cid := s.nextConnIDLocked()
	s.peerMu.Unlock()
	log.Trace().Str("site", "openPeerSession_nextCID").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
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
		// Pre-populate the client version cache so that
		// penalizeOldProtocolPeer → setVersionLockoutLocked writes the
		// complete diagnostics. On the failure path, learnIdentityFromWelcome /
		// addPeerVersion never run, so without this the lockout would store
		// an empty ObservedClientVersion.
		if welcome.ClientVersion != "" {
			log.Trace().Str("site", "openPeerSession_preLockoutVersion").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
			s.peerMu.Lock()
			log.Trace().Str("site", "openPeerSession_preLockoutVersion").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
			healthKey := s.resolveHealthAddress(address)
			s.peerVersions[healthKey] = welcome.ClientVersion
			s.peerMu.Unlock()
			log.Trace().Str("site", "openPeerSession_preLockoutVersion").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
		}
		s.penalizeOldProtocolPeer(address, domain.ProtocolVersion(welcome.Version), domain.ProtocolVersion(welcome.MinimumProtocolVersion))
		return false, fmt.Errorf("%w: %v", errIncompatibleProtocol, err)
	}
	// Self-loopback guard on the legacy outbound path. Deferred cleanup
	// (session.Close via the defer block at the top of openPeerSession)
	// owns wrapper teardown — returning here routes through session.Close()
	// → NetCore.Close(), no raw socket operations. The typed error is
	// propagated to the caller so health/peer_provider penalty paths can
	// detect self-identity via errors.As and apply the cooldown.
	if s.isSelfIdentity(domain.PeerIdentity(welcome.Address)) {
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", welcome.Listen).
			Msg("outbound_self_identity_rejected")
		return false, s.newSelfIdentityError(address, welcome.Listen)
	}
	applyWelcomeMetadata(session, welcome)
	s.learnIdentityFromWelcome(welcome)
	// learnIdentityFromWelcome stores version/build keyed by the normalized
	// listen address, which may differ from the dial address (or be absent
	// entirely when the peer omits the listen field). Store by the
	// health-tracking key (resolveHealthAddress) so peerHealthFrames can
	// always find the version — even for fallback-port dial variants.
	s.peerMu.RLock()
	healthKey := s.resolveHealthAddress(address)
	s.peerMu.RUnlock()
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

	log.Trace().Str("site", "openPeerSession_register").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "openPeerSession_register").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	s.sessions[address] = session
	s.peerMu.Unlock()
	log.Trace().Str("site", "openPeerSession_register").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
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

	// Routing table: register direct peer. The hook itself decides which
	// capabilities are actionable (relay-cap gates direct-route creation,
	// the full list propagates into AnnouncePeerState for v2 mode
	// decisions). session.capabilities is the authoritative set captured by
	// applyWelcomeMetadata — passing it raw keeps CommandTable / RPC /
	// wire semantics consistent across transport paths.
	s.onPeerSessionEstablished(session.peerIdentity, session.capabilities)

	// Send full table sync to the new peer (Phase 1.2: full sync on connect).
	// Both capabilities required: mesh_routing_v1 (understands announce_routes)
	// and mesh_relay_v1 (can carry relay traffic). A routing-only peer would
	// learn routes it cannot deliver on the data plane.
	if session.peerIdentity != "" && s.sessionHasCapability(address, domain.CapMeshRoutingV1) && s.sessionHasCapability(address, domain.CapMeshRelayV1) {
		s.sendOutboundFullTableSync(ctx, session.peerIdentity, address)
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
		// Pure-delivery probe: s.pending lives under deliveryMu.
		s.deliveryMu.RLock()
		hasPending := len(s.pending) > 0
		s.deliveryMu.RUnlock()
		if hasPending {
			peerID := session.peerIdentity
			s.goBackground(func() {
				s.drainPendingForIdentities(map[domain.PeerIdentity]struct{}{
					peerID: {},
				})
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
	var remoteAddr string
	if session.netCore != nil {
		remoteAddr = session.netCore.RemoteAddr()
	}
	s.recordOutboundAuthSuccess(session.address, remoteAddr)
	return nil
}

func (s *Service) markPeerConnected(address domain.PeerAddress, direction domain.PeerDirection) {
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	peerID := s.peerIDs[address]
	now := time.Now().UTC()
	health.Connected = true
	health.Direction = direction
	health.LastConnectedAt = now
	// Treat the TCP handshake itself as proof of liveness so that
	// computePeerStateLocked does not immediately return "degraded"
	// before any ping/pong or data exchange has occurred.
	health.LastUsefulReceiveAt = now
	s.updatePeerStateLocked(health, peerStateHealthy)
	// Clear all failure-related health state. A completed handshake is
	// definitive proof that the peer is compatible — stale error strings,
	// version counters, bans, cooldowns, and score penalties are all invalid.
	resetPeerHealthForRecoveryLocked(health)
	health.Score = clampScore(health.Score + peerScoreConnect)

	// A completed handshake proves the peer (and its IP) is compatible.
	// Clear the IP-wide ban so sibling ports are also unblocked in
	// Candidates() and list_banned no longer reports a stale entry.
	//
	// bannedIPSet lives in the IP/advertise domain.  Nest s.ipStateMu
	// inside the already-held s.peerMu per the canonical peerMu →
	// ipStateMu order documented in docs/locking.md.  A single ipStateMu
	// section covers both the primary-address clear and the sibling
	// sweep below so the expensive peerIDs iteration does not flap the
	// mutex.
	s.ipStateMu.Lock()
	if ip, _, ok := splitHostPort(string(address)); ok {
		delete(s.bannedIPSet, ip)
	}

	// Remove from the incompatible-reporter dedup set if present.
	// statusMu guards s.versionPolicy (INNERMOST — acquired while peerMu
	// and ipStateMu are held; nested self-contained section).
	if peerID != "" && s.versionPolicy != nil {
		s.statusMu.Lock()
		delete(s.versionPolicy.incompatibleReporters, peerID)
		s.statusMu.Unlock()
	}

	// Clear persisted version lockout — the peer just proved compatibility.
	// Identity-wide clearing: lockouts are propagated across all addresses
	// of the same identity (see setVersionLockoutLocked), so clearing must
	// also be identity-wide. Otherwise sibling addresses remain locked out
	// even though the peer has proven compatibility.
	if pm := s.persistedMeta[address]; pm != nil && pm.VersionLockout.IsActive() {
		log.Info().
			Str("peer", string(address)).
			Str("identity", string(peerID)).
			Str("reason", string(pm.VersionLockout.Reason)).
			Msg("version_lockout_cleared_by_successful_handshake")
		pm.VersionLockout = domain.VersionLockoutSnapshot{}
	}
	// Propagate lockout and health clearing to all sibling addresses of
	// the same identity — mirrors setVersionLockoutLocked propagation.
	if peerID != "" {
		for otherAddr, otherID := range s.peerIDs {
			if otherAddr == address || otherID != peerID {
				continue
			}
			if otherEntry, ok := s.persistedMeta[otherAddr]; ok && otherEntry.VersionLockout.IsActive() {
				log.Info().
					Str("peer", string(otherAddr)).
					Str("peer_identity", string(peerID)).
					Str("source_address", string(address)).
					Msg("version_lockout_cleared_by_identity_on_handshake")
				otherEntry.VersionLockout = domain.VersionLockoutSnapshot{}
			}
			if siblingHealth := s.health[otherAddr]; siblingHealth != nil {
				resetPeerHealthForRecoveryLocked(siblingHealth)
			}
			// Clear the IP-wide ban for the sibling's IP so that
			// Candidates() and list_banned are consistent.
			if ip, _, ok := splitHostPort(string(otherAddr)); ok {
				delete(s.bannedIPSet, ip)
			}
		}
	}
	s.ipStateMu.Unlock()

	// Recompute version policy to reflect the cleared evidence.
	// statusMu is INNERMOST per canonical peerMu → statusMu order.
	s.statusMu.Lock()
	s.recomputeVersionPolicyLocked(now)
	s.statusMu.Unlock()

	s.peerMu.Unlock()
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	// Refresh peer-health snapshot immediately so UI pollers that call
	// fetch_peer_health right after a connection transition observe the
	// new state without waiting for the 500 ms refresher tick.  Rebuild
	// acquires s.peerMu.RLock only — the writer was just released.  See
	// refreshHotReadSnapshotsAfterPeerStateChange for why peers_exchange
	// is intentionally left to the ticker.
	s.refreshHotReadSnapshotsAfterPeerStateChange()

	// Emit peer-connected event after releasing lock.
	// TopicAggregateStatusChanged and TopicPeerHealthChanged are already
	// emitted by updatePeerStateLocked (called above).
	ebus.PublishPeerConnected(s.eventBus, address, peerID)
}

func (s *Service) markPeerDisconnected(address domain.PeerAddress, err error) {
	log.Trace().Str("site", "markPeerDisconnected").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "markPeerDisconnected").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")

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
		// Set machine-readable disconnect code when the error wraps a
		// known protocol error (e.g. frame-too-large, rate-limited).
		// The generic "protocol-error" sentinel is excluded — it carries
		// no diagnostic value beyond "something went wrong".
		if code := protocol.ErrorCode(err); code != protocol.ErrCodeProtocol {
			health.LastDisconnectCode = code
		} else {
			health.LastDisconnectCode = ""
		}
	} else {
		// Clean disconnect — clear the code so stale values from a
		// previous error-disconnect do not persist in diagnostics.
		health.ConsecutiveFailures = 0
		health.LastDisconnectCode = ""
		health.Score = clampScore(health.Score + peerScoreDisconnect)
	}
	peerID := s.peerIDs[address]
	if peerID != "" {
		// observedAddrs is IP/advertise-domain state; nest s.ipStateMu
		// inside the already-held s.peerMu per the canonical peerMu →
		// ipStateMu order documented in docs/locking.md.
		s.ipStateMu.Lock()
		delete(s.observedAddrs, peerID)
		s.ipStateMu.Unlock()
	}

	// Remove session-scoped metadata so disconnected peers do not
	// influence the build-based update signal or stale version checks.
	// These maps are repopulated on the next successful handshake.
	delete(s.peerBuilds, address)
	delete(s.peerVersions, address)

	// Recompute version policy so that the build signal immediately
	// reflects the loss of this peer's vote.
	// statusMu is INNERMOST per canonical peerMu → statusMu order.
	s.statusMu.Lock()
	s.recomputeVersionPolicyLocked(time.Now().UTC())
	s.statusMu.Unlock()

	s.peerMu.Unlock()
	log.Trace().Str("site", "markPeerDisconnected").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	// Refresh peer-health snapshot immediately so UI pollers observe the
	// disconnect without waiting for the 500 ms refresher tick.  Same
	// rationale as markPeerConnected.  Skipped during shutdown — see
	// refreshHotReadSnapshotsAfterPeerStateChange.
	s.refreshHotReadSnapshotsAfterPeerStateChange()

	// Emit peer-disconnected event after releasing lock.
	// TopicAggregateStatusChanged and TopicPeerHealthChanged are already
	// emitted by updatePeerStateLocked (called above).
	ebus.PublishPeerDisconnected(s.eventBus, address, peerID)
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
	log.Trace().Str("site", "openPeerSessionForCM_nextCID").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "openPeerSessionForCM_nextCID").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	cid := s.nextConnIDLocked()
	s.peerMu.Unlock()
	log.Trace().Str("site", "openPeerSessionForCM_nextCID").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
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
		// propagates the error to onCMDialFailed, which applies the penalty
		// exactly once. The structured error carries confirmed version evidence
		// so the penalty path can set lockouts and feed the reporter set.
		return nil, &incompatibleProtocolError{
			PeerVersion:   domain.ProtocolVersion(welcome.Version),
			PeerMinimum:   domain.ProtocolVersion(welcome.MinimumProtocolVersion),
			ClientVersion: welcome.ClientVersion,
			Cause:         err,
		}
	}
	// Self-loopback guard: if the welcome carries our own Ed25519
	// identity we just dialled ourselves. Tear down the session
	// through the wrapper (closeOnError → session.Close() → NetCore.Close())
	// and surface a structured selfIdentityError so the CM dial-fail
	// path applies an address-level cooldown instead of retrying.
	if s.isSelfIdentity(domain.PeerIdentity(welcome.Address)) {
		closeOnError()
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", welcome.Listen).
			Msg("outbound_self_identity_rejected")
		return nil, s.newSelfIdentityError(address, welcome.Listen)
	}
	applyWelcomeMetadata(session, welcome)

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
	//
	// s.ipStateMu, not s.peerMu: bannedIPSet is IP-domain state.
	if ip, _, ok := splitHostPort(string(dialAddress)); ok {
		log.Trace().Str("site", "onCMSessionEstablished_clearIPBan").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("ip_state_mu_writer")
		s.ipStateMu.Lock()
		log.Trace().Str("site", "onCMSessionEstablished_clearIPBan").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("ip_state_mu_writer")
		delete(s.bannedIPSet, ip)
		s.ipStateMu.Unlock()
		log.Trace().Str("site", "onCMSessionEstablished_clearIPBan").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("ip_state_mu_writer")
	}

	// Apply welcome metadata — pure map writes, no I/O.
	// Session is NOT yet registered in s.sessions / s.upstream — that
	// happens only after initPeerSession succeeds (Phase 2).
	// The CM slot is in domain.SlotStateInitializing (not Active) until
	// SessionInitReady is emitted, so Slots(), ActiveCount(), and
	// buildPeerExchangeResponse() do not expose this peer during setup.
	if wm := session.welcomeMeta; wm != nil {
		s.learnIdentityFromWelcome(wm.welcome)
		s.peerMu.RLock()
		healthKey := s.resolveHealthAddress(dialAddress)
		s.peerMu.RUnlock()
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
			log.Trace().Str("site", "onCMSessionEstablished_setupFailedCleanup").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
			s.peerMu.Lock()
			log.Trace().Str("site", "onCMSessionEstablished_setupFailedCleanup").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
			delete(s.dialOrigin, dialAddress)
			s.peerMu.Unlock()
			log.Trace().Str("site", "onCMSessionEstablished_setupFailedCleanup").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")
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
		//
		// Cross-domain: s.sessions belongs to peer domain (s.peerMu),
		// s.upstream belongs to delivery domain (s.deliveryMu).  Canonical
		// s.peerMu OUTER → s.deliveryMu INNER.
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		s.deliveryMu.Lock()
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		s.sessions[dialAddress] = session
		s.upstream[dialAddress] = struct{}{}
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "onCMSessionEstablished_register").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")

		s.markPeerConnected(dialAddress, peerDirectionOutbound)
		s.flushPendingPeerFrames(dialAddress)

		// Routing table: register direct peer. session.capabilities is
		// stable by this point (applyWelcomeMetadata ran in
		// openPeerSessionForCM before the CM event loop validated the
		// slot generation), so we pass the raw slice and let the hook
		// flatten it for the relay-cap direct-route gate.
		s.onPeerSessionEstablished(session.peerIdentity, session.capabilities)

		// Send full table sync to the new peer (Phase 1.2).
		if session.peerIdentity != "" && s.sessionHasCapability(dialAddress, domain.CapMeshRoutingV1) && s.sessionHasCapability(dialAddress, domain.CapMeshRelayV1) {
			s.sendOutboundFullTableSync(s.runCtx, session.peerIdentity, dialAddress)
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
		//
		// Cross-domain cleanup: s.sessions / s.dialOrigin → s.peerMu;
		// s.upstream → s.deliveryMu.  Canonical s.peerMu OUTER →
		// s.deliveryMu INNER.
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
		s.peerMu.Lock()
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		s.deliveryMu.Lock()
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		ownedCleanup := s.sessions[dialAddress] == session
		if ownedCleanup {
			delete(s.sessions, dialAddress)
			delete(s.upstream, dialAddress)
			delete(s.dialOrigin, dialAddress)
		}
		s.deliveryMu.Unlock()
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
		s.peerMu.Unlock()
		log.Trace().Str("site", "onCMSessionEstablished_ownedCleanup").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")

		// Routing table: deregister direct peer only if this goroutine
		// owns the cleanup (i.e. onCMSessionTeardown did not run first).
		if ownedCleanup && session.peerIdentity != "" {
			s.onPeerSessionClosed(session.peerIdentity, session.capabilities)
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
	//
	// Cross-domain: s.sessions / s.dialOrigin → s.peerMu; s.upstream →
	// s.deliveryMu.  Canonical s.peerMu OUTER → s.deliveryMu INNER.
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_wait").Str("address", string(addr)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_held").Str("address", string(addr)).Msg("peer_mu_writer")
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_wait").Str("address", string(addr)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_held").Str("address", string(addr)).Msg("delivery_mu_writer")
	ownedCleanup := info.Session != nil && s.sessions[addr] == info.Session
	if ownedCleanup {
		delete(s.sessions, addr)
		delete(s.upstream, addr)
		delete(s.dialOrigin, addr)
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_released").Str("address", string(addr)).Msg("delivery_mu_writer")
	s.peerMu.Unlock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_released").Str("address", string(addr)).Msg("peer_mu_writer")

	// Routing table: deregister direct peer only if we owned the entry.
	// Pointer-compare ensures we don't accidentally delete or deregister
	// a replacement session that was registered for the same address
	// between deactivateSlotLocked and this callback.
	if ownedCleanup && info.Identity != "" {
		s.onPeerSessionClosed(info.Identity, info.Capabilities)
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
	log.Trace().Str("site", "onCMStaleSession").Str("phase", "lock_wait").Str("address", string(session.address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onCMStaleSession").Str("phase", "lock_held").Str("address", string(session.address)).Msg("peer_mu_writer")
	// Clean up dialOrigin if it was registered for this address.
	// No pointer-compare needed — dialOrigin maps addresses, not sessions.
	delete(s.dialOrigin, session.address)
	s.peerMu.Unlock()
	log.Trace().Str("site", "onCMStaleSession").Str("phase", "lock_released").Str("address", string(session.address)).Msg("peer_mu_writer")
}

// onCMDialFailed is called by ConnectionManager when a dial attempt fails.
// Updates health/ban state BEFORE fill() re-queries Candidates().
func (s *Service) onCMDialFailed(address domain.PeerAddress, err error, incompatible bool) {
	// Self-identity is a terminal failure against an address: the
	// remote answering at that endpoint IS us. Apply the long cooldown
	// once (no accumulation) so the dial loop stops thrashing the
	// loopback path, then return without re-feeding markPeerDisconnected
	// which would blend self-identity into generic disconnect diagnostics.
	// The shared helper covers both arrival shapes (structured
	// *selfIdentityError from the local outbound guard and bare
	// protocol.ErrSelfIdentity from the remote-first notice decode) so
	// every dial-failure dispatcher converges on one penalty path.
	if s.tryApplySelfIdentityCooldown(address, err) {
		return
	}
	if incompatible {
		// Extract confirmed version evidence from the structured error
		// returned by openPeerSessionForCM. Without evidence, the penalty
		// path still applies ban scoring but correctly skips the reporter
		// set and persisted lockout (direction guard requires non-zero
		// peerMinimum > local protocol version).
		var peerVersion, peerMinimum domain.ProtocolVersion
		var ipe *incompatibleProtocolError
		if errors.As(err, &ipe) {
			peerVersion = ipe.PeerVersion
			peerMinimum = ipe.PeerMinimum
			// Pre-populate the peerVersions cache so that
			// penalizeOldProtocolPeer → setVersionLockoutLocked can read the
			// client version for persisted diagnostics. On the CM path the
			// handshake failed before onCMSessionEstablished could populate
			// these maps from the welcome frame.
			if ipe.ClientVersion != "" {
				log.Trace().Str("site", "onCMDialFailed_storeVersion").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
				s.peerMu.Lock()
				log.Trace().Str("site", "onCMDialFailed_storeVersion").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
				s.peerVersions[address] = ipe.ClientVersion
				s.peerMu.Unlock()
				log.Trace().Str("site", "onCMDialFailed_storeVersion").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
			}
		}
		s.penalizeOldProtocolPeer(address, peerVersion, peerMinimum)
	} else {
		s.markPeerDisconnected(address, err)
	}
}

// applySelfIdentityCooldown sets a long BannedUntil window on the
// address that resolved to our own Ed25519 identity so the CM dial
// loop stops redialing the self-loopback. Writes LastErrorCode /
// LastError to the health snapshot so diagnostics surface the real
// cause instead of a generic disconnect reason. Runs under s.peerMu.
//
// No-accumulation contract (matches the peerBanSelfIdentity comment
// in peer_state.go and the onCMDialFailed caller comment): the
// address-level Score penalty and ConsecutiveFailures increment are
// applied ONLY on the first observation — defined as "the prior
// LastErrorCode wasn't self-identity, or the prior ban window has
// already expired". Before this guard every outbound path that
// resurfaced the same self-alias inside the 24h window re-entered
// this function and stacked peerScoreOldProtocol onto Score and
// bumped ConsecutiveFailures unboundedly, even though self-identity
// is binary evidence that a single mark-down should cover. The
// current-status fields (Connected, Direction, state, LastError*,
// LastDisconnectedAt, BannedUntil) are always refreshed because
// they reflect "right now" and must not stale-pin a prior classification.
func (s *Service) applySelfIdentityCooldown(address domain.PeerAddress, selfErr *selfIdentityError) {
	log.Trace().Str("site", "applySelfIdentityCooldown").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "applySelfIdentityCooldown").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	bannedUntil := now.Add(peerBanSelfIdentity)
	// First-observation gate for the accumulators. Evaluate BEFORE
	// rewriting LastErrorCode / BannedUntil below so the decision
	// reflects the PRIOR state of the record, not the one we are
	// about to write. A prior self-identity classification whose ban
	// window has already expired still counts as a fresh observation
	// — the address recovered and re-collided, so the penalty should
	// re-apply once.
	firstObservation := health.LastErrorCode != protocol.ErrCodeSelfIdentity || !health.BannedUntil.After(now)
	health.Connected = false
	health.Direction = ""
	s.updatePeerStateLocked(health, peerStateReconnecting)
	health.LastDisconnectedAt = now
	health.LastError = selfErr.Error()
	health.LastErrorCode = protocol.ErrCodeSelfIdentity
	health.LastDisconnectCode = ""
	if health.BannedUntil.Before(bannedUntil) {
		health.BannedUntil = bannedUntil
	}
	if firstObservation {
		health.Score = clampScore(health.Score + peerScoreOldProtocol)
		health.ConsecutiveFailures++
	}
	// emitPeerHealthDeltaLocked reads s.pending (delivery-domain, under
	// s.deliveryMu).  Canonical order s.peerMu OUTER → s.deliveryMu INNER.
	s.deliveryMu.RLock()
	s.emitPeerHealthDeltaLocked(health)
	s.deliveryMu.RUnlock()
	s.peerMu.Unlock()
	log.Trace().Str("site", "applySelfIdentityCooldown").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	log.Warn().
		Str("peer", string(address)).
		Str("local_identity", string(selfErr.LocalIdentity)).
		Str("welcome_listen", string(selfErr.PeerListen)).
		Time("banned_until", bannedUntil).
		Bool("first_observation", firstObservation).
		Msg("self_identity_cooldown_applied")
}
