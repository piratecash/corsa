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
//     markPeerDisconnected (peerMu outer; deliveryMu.RLock / ipStateMu /
//     statusMu nested), the rule that side-effects run after domain
//     mutexes are released, and the
//     "peerSession.sendMu — the outbound queue fence" section, which
//     names per death path who finalises which of the two outbound
//     queues.
//   - peer_send_queue.go — the upper outbound queue whose sole consumer
//     is this file's serve loop. servePeerSession fences and drains it
//     from one defer, so a newly added return site cannot leak frames.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/netcore"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// applyWelcomeMetadata copies the negotiated handshake state from a
// validated welcome frame into session and mirrors it onto the outbound
// NetCore so s.conns (which holds both directions) is a symmetric
// registry — helpers reading pc.Address()/Identity()/Capabilities()
// observe the real peer values instead of an anonymous entry.
//
// The welcome carries no host, so NetCore.Address is taken
// UNCONDITIONALLY from session.address — the host:port we successfully
// dialed. This is the canonical overlay key for the outbound side:
// pc.Address() callers see a host:port string, never a bare Ed25519
// identity, and nothing the responder writes into its welcome can move
// the registry away from the dialable address.
//
// This helper is pure metadata assignment: it does not touch
// learnIdentityFromWelcome, peerHealth maps or welcomeMeta — each
// outbound path (legacy openPeerSession vs CM openPeerSessionForCM)
// decides separately when to run those side-effects because the CM path
// defers them until the generation check passes.
//
// What it publishes is NEGOTIATED but not yet IN FORCE. auth_ok has not
// arrived when this runs — both call sites invoke it one round trip earlier —
// and readPeerSession has been dispatching since before the welcome. The
// receive path therefore asks sessionHasCapability, which additionally requires
// the completed handshake (markSessionHandshakeComplete); see its doc for the
// window that would otherwise stay open between this write and auth_ok.
func applyWelcomeMetadata(session *peerSession, welcome protocol.Frame, local []domain.Capability) {
	session.version = welcome.Version
	session.peerIdentity = domain.PeerIdentityFromWire(welcome.Address)
	session.capabilities = intersectCapabilities(local, welcome.Capabilities)
	// The raw declarations (docs/protocol/datagram.md §2.3 raw capability
	// names, docs/protocol/datagram.md §6.1 dtypes) are mirrored beside the
	// typed set, derived from the SAME welcome frame, for the same reason the
	// typed set is mirrored at all: the outbound dispatcher is addressed by
	// PeerAddress and has no ConnID with which to reach NetCore. Deriving both
	// here, in one pass, is what stops the two views of one handshake from ever
	// disagreeing.
	session.declarations = declarationsFromHandshake(welcome)
	if session.netCore == nil {
		return
	}
	// session.address — the host:port we actually dialed — is the only
	// trusted overlay key for the outbound side.
	advertised := session.address
	if advertised == "" {
		// Defensive fallback for unit-test fixtures that set up a
		// session without a dial address. Production outbound paths
		// always have one.
		advertised = domain.PeerAddress(strings.TrimSpace(welcome.Address))
	}
	session.netCore.SetAddress(advertised)
	session.netCore.SetIdentity(session.peerIdentity)
	session.netCore.SetCapabilities(session.capabilities)
	session.netCore.SetDeclarations(session.declarations)
	// Mirror the negotiated protocol version onto the outbound NetCore
	// for symmetry with the rest of the welcome-derived metadata
	// (Address / Identity / Capabilities). The file router does NOT
	// read this value on the outbound path — fileTransferPeerRouteMetaLocked
	// reads sess.version directly for outbound entries. The inbound
	// ranking fix lives in rememberConnPeerAddr (service.go), where
	// hello.Version is folded into the inbound NetCore via ApplyOpts;
	// that NetCore is the one snapshotEntryLocked reads when populating
	// connInfo.protocolVersion for the inbound carve-out.
	//
	// We still set it here so any future code that introspects an
	// outbound NetCore through the same accessor (logs, diagnostics,
	// future iterators) sees a consistent value instead of zero.
	// Removing this call without also reworking those consumers would
	// silently regress them.
	session.netCore.SetProtocolVersion(domain.ProtocolVersion(welcome.Version))
}

// peerSessionSendBuffer / peerSessionInboxBuffer size the per-session
// frame channels created by openPeerSession / openPeerSessionForCM.
//
// sendCh overflow degrades gracefully — enqueuePeerFrame fast-fails and
// the frame lands in the pending ring for a later retry — so it stays
// small. inboxCh overflow is fatal for the session (slow-consumer
// eviction in readPeerSession), and the serve loop legitimately stops
// draining inboxCh for up to peerRequestTimeout while a synchronous
// peerSessionRequest waits on a slow socket — 256 gives the reader
// that much headroom under gossip/announce bursts, where the previous
// 64 overflowed and tore down otherwise healthy sessions.
const (
	peerSessionSendBuffer  = 64
	peerSessionInboxBuffer = 256
)

// sessionCloseCauseFromError classifies a session teardown error for
// the disconnect_storm quarantine accounting (see sessionCloseCause in
// routing_session.go). Local evictions are:
//
//   - errors wrapping errPeerSessionInboxOverflow — slow-consumer
//     eviction chosen by the local node;
//   - nil — a clean local shutdown. The ONLY nil-returning exits of
//     servePeerSession are local context cancellation (the ctx.Done
//     case, and the writerDone case re-checked against ctx.Err), and
//     the runPeerSession cleanup never runs with a nil error at all.
//     Without the nil→local mapping a node shutdown recorded a
//     disconnect_storm event against every connected peer. (A CM session
//     the ConnectionManager evicted or shut down is attributed from its
//     recorded close reason before this is consulted — see
//     deregisterServedCMSessionRouting.)
//
// Everything else is peer-initiated.
func sessionCloseCauseFromError(err error) sessionCloseCause {
	if err == nil || errors.Is(err, errPeerSessionInboxOverflow) {
		return sessionCloseLocalEviction
	}
	return sessionClosePeerInitiated
}

// sessionCloseProvidesPeerOfflineEvidence is deliberately narrower than
// sessionCloseCauseFromError. The quarantine classifier treats timeouts and
// protocol failures as peer instability, but those failures do not prove that
// the remote identity went offline: our own interface, route, or writer may
// have failed. Durable last_online_at is therefore attributed only when the
// transport reports a clean remote close. Explicit lifecycle callers that
// already know the peer initiated the close pass that evidence directly via
// onPeerSessionClosedWithCause.
func sessionCloseProvidesPeerOfflineEvidence(err error) bool {
	if err == nil || errors.Is(err, errPeerSessionInboxOverflow) {
		return false
	}
	return errors.Is(err, io.EOF)
}

func (s *Service) runPeerSession(ctx context.Context, address domain.PeerAddress) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
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
			adLocked = true
			adTicket = s.beginK3AdvertisedPeerLocked()
			log.Trace().Str("site", "runPeerSession_cleanup").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
			closedSession := s.sessions[address]
			var k3 k3RegisteredTicket
			if s.k3Registered != nil {
				k3 = s.k3BeginSessionLocked(address)
			}
			s.k3AdvertisedSessionChangingLocked(address, nil)
			publication := s.beginRegistryPublication()
			delete(s.sessions, address)
			if publication != nil {
				publication.Complete()
			}
			if s.k3Registered != nil {
				s.k3FinishSessionLocked(k3)
			}
			s.finishK3AdvertisedPeerLocked(adTicket)
			adTicket = nil
			adLocked = false
			s.peerMu.Unlock()
			log.Trace().Str("site", "runPeerSession_cleanup").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

			// Routing table: deregister direct peer on session close.
			// The capability slice must match what was passed to
			// onPeerSessionEstablished for balanced accounting — the session
			// object still owns it here since we just removed it from
			// s.sessions and no other goroutine mutates session.capabilities
			// after applyWelcomeMetadata.
			if closedSession != nil && !closedSession.peerIdentity.IsZero() {
				s.onPeerSessionClosedWithError(closedSession.peerIdentity, closedSession.penaltySubject(), closedSession.capabilities, err)
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
			// An end that says nothing against the address
			// (isUnchargedDialRefusal: a refusal this node made, a held
			// dial, a listener's refusal, another owner than the dial named,
			// anything that ended before the peer was authenticated, a dial
			// this node ended itself) is not a failure of the address
			// (onCMDialFailed): nothing is charged, and the loop stops — a
			// refusal by pin already holds the fall back to v1 at the
			// address, and this path has no v2 to try instead.
			if isUnchargedDialRefusal(err) {
				log.Warn().Err(err).Str("peer", string(address)).Msg("peer_session_stopped_dial_refused_not_charged")
				return
			}
			// Incompatible protocol — no point retrying until the peer
			// upgrades. The reaction already ran where the version was
			// read (reactToIncompatibleDial): a claimed version only paces
			// the address and must not be charged here on top of it.
			if errors.Is(err, errIncompatibleProtocol) {
				log.Warn().Str("peer", string(address)).Msg("peer_session_stopped_incompatible_protocol")
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

func (s *Service) openPeerSession(ctx context.Context, address domain.PeerAddress) (connected bool, outcomeErr error) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	// Budgeted. This is the legacy session path (ensurePeerSessions), inert
	// while the connection manager is wired — but a dial path that is dead
	// today and unbudgeted tomorrow is how a ceiling quietly stops holding.
	// It speaks v1 only, so on a node that speaks v2 it opens v1 only where
	// the probe allows it (dialLegacyTransport), the full hello following on
	// the probe's connection.
	transport, err := s.dialLegacyTransport(ctx, address, s.budgetedSessionDialer(address))
	if err != nil {
		s.reactToIncompatibleDial(address, err)
		return false, err
	}
	rawConn, conn := transport.raw, transport.metered
	var session *peerSession
	defer func() {
		s.accumulateSessionTraffic(address, conn)
		if session != nil {
			_ = session.Close()
			session.k10FinishReported(outcomeErr)
			// The session ends here on every exit — an error, or the
			// serve loop returning on a cancelled context — and this
			// goroutine dispatched all of its frames, so the bucket they
			// charged ends here too. See dropOutboundControlFrameBucket.
			s.dropOutboundControlFrameBucket(session.connID)
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
	session = &peerSession{
		readiness:    newPeerSessionReadiness(ctx),
		address:      address,
		peerIdentity: domain.PeerIdentity{},
		connID:       transport.connID,
		conn:         conn,
		metered:      conn,
		sendCh:       make(chan peerSendItem, peerSessionSendBuffer),
		inboxCh:      make(chan protocol.Frame, peerSessionInboxBuffer),
		errCh:        make(chan error, 1),
		// Owner-serialised contact sync (sender-key recovery). Unbuffered:
		// see the peerSession field doc.
		contactSyncCh: make(chan chan int),
	}
	if err := s.attachOutboundNetCore(session); err != nil {
		return false, err
	}
	go s.readPeerSession(transport.reader, session)

	welcome, err := s.peerSessionRequest(session, protocol.Frame{}, "welcome", true)
	if err != nil {
		s.forgetLegacyObservedWithoutWelcome(address, transport)
		return false, err
	}
	if err := s.refuseFullLegacyWelcome(address, transport, welcome); err != nil {
		return false, err
	}
	// This path speaks v1 only, so every welcome it reads is a v1 welcome:
	// one naming an identity that requires v2 is refused before anything is
	// signed, as on the connection manager's path.
	if err := s.refuseLegacyWelcome(address, domain.PeerIdentityFromWire(welcome.Address)); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Msg("outbound_v1_refused_identity_pinned_to_v2")
		return false, err
	}
	if err := s.refuseIncompatibleWelcome(address, welcome, versionClaimed); err != nil {
		log.Warn().Err(err).Str("peer", string(address)).Int("version", welcome.Version).Msg("outbound_peer_protocol_too_old")
		return false, err
	}
	// Self-loopback guard on the legacy outbound path. Deferred cleanup
	// (session.Close via the defer block at the top of openPeerSession)
	// owns wrapper teardown — returning here routes through session.Close()
	// → NetCore.Close(), no raw socket operations. The typed error is
	// propagated to the caller so health/peer_provider penalty paths can
	// detect self-identity via errors.As and apply the cooldown.
	if s.isSelfIdentity(domain.PeerIdentityFromWire(welcome.Address)) {
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Msg("outbound_self_identity_rejected")
		return false, s.newSelfIdentityError(address)
	}
	// peerMu around the metadata write, not just around the map insert below:
	// readPeerSession is already running (it delivered the welcome that got us
	// here) and reads the same fields on its capability gates, so a peer that
	// pipelines a frame behind its own welcome puts a reader on
	// session.capabilities at the instant this line assigns it. The field doc
	// on peerSession already declares peerMu as its guard — this is the write
	// side honouring it. The typed local set is resolved BEFORE the lock: it
	// consults the datagram layer, and no domain mutex is held across that.
	local := s.localHandshakeCapabilities()
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	if s.k3Advertised != nil {
		s.applyK3AdvertisedWelcomeLocked(session, welcome, local)
	} else {
		applyWelcomeMetadata(session, welcome, local)
	}
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	_, proven := session.provenIdentity()
	s.learnIdentityFromWelcome(welcome, address, identityOriginOf(proven))
	// learnIdentityFromWelcome stores version/build keyed by the dial
	// address (the only trusted host:port for the outbound side).
	// Mirror them under the health-tracking key resolveHealthAddress
	// returns so peerHealthFrames can always
	// find the version — even for fallback-port dial variants whose
	// canonical health key differs from the dial target.
	s.peerMu.RLock()
	healthKey := s.resolveHealthAddress(address)
	s.peerMu.RUnlock()
	s.addPeerID(healthKey, addressIdentity{identity: session.peerIdentity, origin: identityOriginOf(proven)})
	s.addPeerVersion(healthKey, welcome.ClientVersion)
	s.addPeerBuild(healthKey, welcome.ClientBuild)
	if err := s.authenticatePeerSession(session, welcome); err != nil {
		return false, err
	}
	// Record observed address only after authentication succeeds so that
	// an unauthenticated responder cannot influence NAT consensus.
	s.recordObservedAddress(domain.PeerIdentityFromWire(welcome.Address), welcome.ObservedAddress)

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
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "openPeerSession_register").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")
	var k3 k3RegisteredTicket
	if s.k3Registered != nil {
		k3 = s.k3BeginSessionLocked(address)
	}
	s.k3AdvertisedSessionChangingLocked(address, session)
	publication := s.beginRegistryPublication()
	s.sessions[address] = session
	if publication != nil {
		publication.Complete()
	}
	if s.k3Registered != nil {
		s.k3FinishSessionLocked(k3)
	}
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "openPeerSession_register").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")
	s.markPeerConnected(address, peerDirectionOutbound)

	// Inbox subscription is established by the responder at auth time
	// (handleAuthSession → registerHelloRoute + backlog replay) — the
	// MinimumProtocolVersion floor guarantees every peer supports it, so no
	// explicit subscription round-trip happens here.
	_ = conn.SetDeadline(time.Time{})

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
	if !session.peerIdentity.IsZero() && s.sessionHasCapability(session, domain.CapMeshRoutingV1) && s.sessionHasCapability(session, domain.CapMeshRelayV1) {
		s.sendOutboundFullTableSync(ctx, session.peerIdentity, address)
	}

	return true, s.servePeerSession(ctx, session)
}

func (s *Service) servePeerSession(ctx context.Context, session *peerSession) error {
	log.Debug().Str("node", s.identity.Address).Str("peer", string(session.address)).Int("inbox_buffered", len(session.inboxCh)).Msg("servePeerSession: entering Phase 3 main loop")

	// The serve loop is the only consumer of the upper outbound queue, so
	// its exit — for ANY reason: ctx cancel, writer death, peer error,
	// heartbeat stall — is the moment nothing will ever move those frames
	// to NetCore again. Fence the queue and finalise the residue here
	// rather than at each return site: there are eight of them and a new
	// one would silently leak.
	defer session.discardSendQueue()

	// Event-driven pending queue drain: a direct route was added by
	// onPeerSessionEstablished before we entered the main loop. Now that
	// inboxCh is actively read (preventing overflow), drain any pending
	// frames (push_message, etc.) that target this peer's identity.
	if !session.peerIdentity.IsZero() {
		peerID := session.peerIdentity
		reachable := map[domain.PeerIdentity]struct{}{peerID: {}}
		// The peer's identity just became directly reachable: re-arm any held
		// sender-owned delivery retry for it (unconditional — held retries
		// live in awaitingDelivered, not s.pending).
		s.kickDeliveryRetriesForReachable(reachable)
		// The kick answers for messages that never left; this answers for
		// the ones that did and are still unacknowledged. See
		// wakeOverdueForReturningPeer.
		s.wakeOverdueForReturningPeer(peerID, time.Now().UTC())
		// Pure-delivery probe: s.pending lives under deliveryMu.
		s.deliveryMu.RLock()
		hasPending := len(s.pending) > 0
		s.deliveryMu.RUnlock()
		if hasPending {
			s.goBackground(func() {
				s.drainPendingForIdentities(reachable)
			})
		}
	}

	liveness := s.sessionLiveness()
	pingTimer := time.NewTimer(liveness.diallerPingDelay())
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
			session.endProofTransport()
			return nil
		case <-writerDoneCh:
			session.endProofTransport()
			if err := ctx.Err(); err != nil {
				// Local shutdown closed the socket, which killed the
				// writer. Treat as a clean teardown.
				return nil
			}
			writeErr := fmt.Errorf("managed writer exited (socket write failed)")
			s.retirePeerSession(session, writeErr)
			return writeErr
		case err := <-session.errCh:
			session.endProofTransport()
			log.Info().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Err(err).Msg("upstream subscription closed")
			s.retirePeerSession(session, err)
			return err
		case frame := <-session.inboxCh:
			s.markPeerRead(session.address, frame)
			s.dispatchPeerSessionFrame(session.address, session, frame)
		case reply := <-session.contactSyncCh:
			// Owner-serialised contact sync (sender-key recovery,
			// requestOwnedContactSync): the serve loop is the single
			// legal reader of inboxCh, so the fetch_contacts
			// request/reply runs HERE, on the loop, never in the
			// recovery goroutine. While peerSessionRequest waits for
			// the contacts reply it keeps consuming inbox frames
			// (dispatching fire-and-forget types), so the inbox cannot
			// overflow — same posture as the heartbeat ping below. The
			// reply channel is buffered (1) by the requester, so this
			// send never blocks the loop.
			imported, err := s.syncContactsViaSession(session)
			if err != nil {
				// A peerSessionRequest error is fatal for the session —
				// identical to the heartbeat contract below. In
				// particular a reply timeout means readPeerSession hit
				// the read deadline, pushed its single error and
				// EXITED: continuing the loop here would leave a zombie
				// session whose next heartbeat blocks forever (the
				// errCh error was already consumed, no reader remains
				// to produce another, and peerSessionRequest has no ctx
				// branch). Answer the requester first (buffered chan —
				// never blocks), then tear the session down.
				session.endProofTransport()
				reply <- 0
				log.Warn().Str("peer", string(session.address)).Err(err).Msg("owned_contact_sync_failed, tearing down session")
				s.retirePeerSession(session, err)
				return err
			}
			reply <- imported
		case <-pingTimer.C:
			if _, err := s.peerSessionRequest(session, protocol.Frame{Type: "ping"}, "pong", false); err != nil {
				session.endProofTransport()
				log.Warn().Str("peer", string(session.address)).Str("recipient", s.identity.Address).Err(err).Msg("heartbeat failed, peer stalled")
				s.retirePeerSession(session, err)
				return err
			}
			pingTimer.Reset(liveness.diallerPingDelay())
		case item := <-session.sendCh:
			outbound := item.Frame
			// A tracked item always takes the managed writer path: it
			// carries a send deadline and expects no reply, while the
			// request/reply path below would hold this loop waiting for an
			// answer that is never coming.
			if item.tracked() || isFireAndForgetFrame(outbound.Type) {
				// Fire-and-forget frames (relay_message, relay_hop_ack) are
				// enqueued on the managed writer without waiting for a reply.
				// Buffer-full drops the frame and keeps the session — see
				// the SendBufferFull case below for the contract.
				if session.netCore == nil {
					session.endProofTransport()
					return fmt.Errorf("fire_and_forget: outbound session missing NetCore")
				}
				// A frame can sit in this queue long enough for its author
				// to recall it, and the recall classifies a message nobody
				// has taken as never-emitted — so writing it afterwards
				// hands the recipient a message the sender was told was
				// withdrawn. The gate also withdraws the durable
				// never-emitted claim, which has to land BEFORE the write.
				if !s.clearedToWrite(item.delivery, time.Now().UTC()) {
					log.Info().Str("peer", string(session.address)).
						Str("message_id", string(item.delivery.Envelope.ID)).
						Msg("fire_and_forget_withheld_by_delivery_gate")
					continue
				}
				switch st := session.netCore.SendTrackedObserved(outbound, item.ticket, item.writeAck); st {
				case netcore.SendOK:
					// Enqueued. Account the write only now: a frame
					// rejected by the queue never reaches the wire, so
					// tracing it as accepted=true and bumping
					// LastUsefulSendAt before Send would keep a
					// backpressured peer looking healthy and falsify
					// protocol_trace under sustained drops.
					s.markPeerWrite(session.address, outbound)
					// This — not the session's own sendCh — is where a
					// frame stops being ours. sendCh is an in-process
					// queue whose remainder is discarded when the session
					// closes, so confirming a delivery on entry to it
					// called a message sent that a teardown could still
					// throw away.
					//
					// WHICH delivery and WHICH attempt both travel on the
					// item: reading the clock here instead made every sink
					// of one dispatch look like a separate attempt.
					if item.carriesDelivery() {
						s.confirmEnvelopeOnWire(item.delivery.Envelope, item.delivery.DispatchedAt)
					}
				case netcore.SendBufferFull:
					// Best-effort frame on a saturated write queue: drop
					// the frame, keep the session. The previous slow-peer
					// eviction (markPeerDisconnected + teardown) converted
					// transient backpressure into a reconnect storm: every
					// teardown discarded the whole write queue, forced a
					// redial, a connect-time full table sync and a baseline
					// resync — far more wire traffic than the one dropped
					// frame — and the self-inflicted disconnects fed the
					// disconnect_storm quarantine of an innocent peer.
					// Fire-and-forget frames are best-effort by contract:
					// gossip/relay payloads retry end-to-end through the
					// delivery subsystem, announce-plane frames self-heal
					// via the forced periodic full sync and route TTL. A
					// genuinely dead socket is still detected by the
					// heartbeat ping and by the writer-goroutine exit
					// (writerDoneCh above). Relay retry state is
					// deliberately NOT cleared so the relay-retry
					// subsystem can pick a different route.
					session.logFireAndForgetDrop(outbound.Type)
					s.recordDeliveryRefusedByWriter(item, st)
					continue
				case netcore.SendChanClosed, netcore.SendWriterDone:
					session.endProofTransport()
					writeErr := fmt.Errorf("fire_and_forget: connection closing (%s)", st.String())
					s.recordDeliveryRefusedByWriter(item, st)
					s.retirePeerSession(session, writeErr)
					return writeErr
				case netcore.SendMarshalError:
					log.Warn().Str("peer", string(session.address)).Str("type", outbound.Type).Msg("fire_and_forget_marshal_error")
					s.recordDeliveryRefusedByWriter(item, st)
					continue
				default:
					session.endProofTransport()
					writeErr := fmt.Errorf("fire_and_forget: unexpected send status %s", st.String())
					log.Error().Str("peer", string(session.address)).Str("type", outbound.Type).Str("status", st.String()).Msg("fire_and_forget_unexpected_status")
					s.retirePeerSession(session, writeErr)
					return writeErr
				}
			} else if _, err := s.peerSessionRequest(session, outbound, expectedReplyType(outbound.Type), false); err != nil {
				if errors.Is(err, protocol.ErrFrameTooLarge) {
					// Self-bug: our outbound frame exceeded the wire size budget.
					// Disconnecting the peer would treat it as a flap and drag
					// both sides into a reconnect loop — instead we drop the
					// offending frame, log loudly, and let the session
					// continue. The upstream layer that built the oversize
					// frame must shrink it (announce_routes pagination, peers
					// cap, etc.).
					log.Error().Str("peer", string(session.address)).Str("type", outbound.Type).Err(err).Msg("outbound_frame_too_large_dropped")
					s.clearRelayRetryForOutbound(outbound)
					s.flushPendingPeerFrames(session.address)
					continue
				}
				session.endProofTransport()
				log.Error().Str("peer", string(session.address)).Str("type", outbound.Type).Err(err).Msg("peer session send failed")
				s.retirePeerSession(session, err)
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

// fireAndForgetDropWarnInterval rate-limits the per-session
// fire_and_forget_buffer_full warn. During sustained backpressure the
// drop site fires for nearly every gossip frame, and per-event warn
// logging (JSON encode + runtime.Caller) was itself a measurable
// CPU/IO cost during storms. One line per interval carrying the
// aggregated drop count preserves the operator signal.
const fireAndForgetDropWarnInterval = 30 * time.Second

// logFireAndForgetDrop records a best-effort frame drop on a saturated
// write queue and emits the rate-limited fire_and_forget_buffer_full
// warn. Only the servePeerSession goroutine touches the drop counters,
// so no lock is required.
func (session *peerSession) logFireAndForgetDrop(frameType string) {
	session.ffDropsSinceWarn++
	now := time.Now()
	if !session.ffLastDropWarnAt.IsZero() && now.Sub(session.ffLastDropWarnAt) < fireAndForgetDropWarnInterval {
		return
	}
	log.Warn().
		Str("peer", string(session.address)).
		Str("type", frameType).
		Int("dropped", session.ffDropsSinceWarn).
		Msg("fire_and_forget_buffer_full")
	session.ffLastDropWarnAt = now
	session.ffDropsSinceWarn = 0
}

func (s *Service) authenticatePeerSession(session *peerSession, welcome protocol.Frame) error {
	authSession, err := s.authSessionFor(welcome)
	if err != nil {
		return err
	}
	reply, err := s.peerSessionRequest(session, authSession, "auth_ok", false)
	if err != nil {
		return err
	}
	if reply.Type != "auth_ok" {
		return protocol.ErrAuthRequired
	}
	return s.completeOutboundAuth(session)
}

// completeOutboundAuth is everything an outbound session gets once its peer
// is proven — by auth_ok on v1 (authenticatePeerSession) or by session_proof
// on v2 (openPeerSessionForCM): the negotiated welcome comes into force, the
// success is recorded, and the initial push_identity and reaction re-offer
// start.
func (s *Service) completeOutboundAuth(session *peerSession) error {
	if err := s.markSessionHandshakeComplete(session); err != nil {
		return err
	}
	// Registry attach has consumed the original P/T/R/W tuple. Auth is now
	// committed and seed/group facts are already owned; retain no full TLS proof
	// for a dialer, including peers that negotiated no FindNode capability.
	if l := session.common; l != nil {
		l.mu.Lock()
		if l.peer == session && l.entry != nil && l.nin == nil {
			l.evidence = nil
		}
		l.mu.Unlock()
	}
	var remoteAddr string
	if session.netCore != nil {
		remoteAddr = session.netCore.RemoteAddr()
	}
	s.recordOutboundAuthSuccess(session.address, remoteAddr)

	// Mandatory initial push_identity of the identity-discovery layer: right
	// after auth each side of a datagram-capable session hands the other its
	// own signed record. Fire-and-forget — the send path itself decides
	// between "peer without the plane, skip" and "enqueue fault, close".
	if peerID := s.sessionPeerIdentity(session); !peerID.IsZero() {
		// A new session is the only moment at which what this peer can receive
		// may have changed: the declared dtype set is fixed for a session's
		// whole lifetime, so a peer that updated its build becomes willing to
		// take conversation-control commands exactly here.
		s.forgetDMControlRefusal(peerID)
		// And offer this conversation's own reactions again. A session coming up
		// is the only moment this node has to work with: nothing on this
		// transport reports that a fact ARRIVED, so re-offering from the durable
		// record is the delivery guarantee (see reofferReactions).
		//
		// lifecycle: joined by backgroundWg (WaitBackground). One bounded read
		// plus one enqueue into the debounce buffer.
		s.goBackground(func() { s.reofferReactions(s.runCtx, peerID) })
		initialOwner := captureSessionTransportOwner(s, session)
		// lifecycle: joined by backgroundWg (WaitBackground), not by Run.
		// The initial push has at most 20 ctx-aware no_route attempts with
		// 250ms delays; its session-close callback starts no goroutine.
		s.goBackground(func() { s.sendInitialIdentityPushOwned(s.runCtx, peerID, func() { _ = session.Close() }, initialOwner) })
	}
	return nil
}

// markSessionHandshakeComplete records that auth_ok arrived: everything the
// welcome negotiated on this outbound session is from now on IN FORCE, and the
// receive path may act on it (sessionHasCapability).
//
// peerMu is the publication and not decoration. readPeerSession has been running
// since before the welcome — it is the goroutine that delivered it — and it
// consults this flag on every capability-gated frame it dispatches, so an
// unsynchronised write here races that reader as well as leaving it unordered.
// The peerSession field doc names peerMu as the guard of every session field
// that outlives the handshake; this is the write side honouring it, exactly as
// applyWelcomeMetadata's call sites do for the capability set.
//
// It stays a one-way transition: the handshake completes once per session, and a
// session that has to renegotiate is a new session with a new socket.
func (s *Service) markSessionHandshakeComplete(session *peerSession) error {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if session.readiness == nil {
		publication := s.beginRegistryPublication()
		session.authOK = true
		if publication != nil {
			publication.Complete()
		}
		return nil
	}
	session.readiness.mu.Lock()
	defer session.readiness.mu.Unlock()
	if err := session.readiness.prepareReadyLocked(); err != nil {
		return fmt.Errorf("%w: outbound handshake readiness: %w", errDialAbortedLocally, err)
	}
	publication := s.beginRegistryPublication()
	session.authOK = true
	if publication != nil {
		publication.Complete()
	}
	session.readiness.publishReadyLocked()
	return nil
}

func (s *Service) markPeerConnected(address domain.PeerAddress, direction domain.PeerDirection) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")

	s.noteAddressSessionLocked(address, direction)
	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	peerID := s.peerIDs.identityOf(address)
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
	if !peerID.IsZero() && s.versionPolicy != nil {
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
			Str("identity", peerID.String()).
			Str("reason", string(pm.VersionLockout.Reason)).
			Msg("version_lockout_cleared_by_successful_handshake")
		pm.VersionLockout = domain.VersionLockoutSnapshot{}
	}
	// Propagate lockout and health clearing to all sibling addresses of
	// the same identity — mirrors setVersionLockoutLocked propagation.
	if !peerID.IsZero() {
		for otherAddr, otherID := range s.peerIDs {
			if otherAddr == address || otherID.identity != peerID {
				continue
			}
			if otherEntry, ok := s.persistedMeta[otherAddr]; ok && otherEntry.VersionLockout.IsActive() {
				log.Info().
					Str("peer", string(otherAddr)).
					Str("peer_identity", peerID.String()).
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

	s.finishK3AdvertisedPeerLocked(adTicket)

	adTicket = nil
	adLocked = false

	s.peerMu.Unlock()
	log.Trace().Str("site", "markPeerConnected").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	s.markPeerStateDirty()

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

// retirePeerSession is the teardown of ONE outbound session that still owns a
// live upper queue, and it is the only shape in which a session-holding caller
// may publish its peer as disconnected.
//
// It exists because the two facts a producer reads are published by different
// objects and only their ORDER makes the pair honest. A producer is admitted by
// the peer-state gate (peerAcceptsOutboundFrames) and then hands its frame to
// the session's queue; the frame is discarded by closeSendQueue. Publishing
// "disconnected" first left a window in which a producer that had just been
// admitted still deposited its frame into a queue about to be drained, and the
// emitter read that acceptance as a delivery and STOPPED walking the peer's
// remaining connections — so the frame reached neither this socket nor the next
// candidate.
//
// Fencing first inverts the pair: once the disconnect is visible, the queue has
// provably already stopped accepting, so a producer that passed the gate on
// stale health is refused at the door and walks on. It does not — and cannot —
// promise that an ACCEPTED frame is written: a session may die right after a
// successful enqueue, which is the ordinary asynchronous case the receiving
// side's anti-replay covers. What it removes is the case where the queue
// answers "accepted" for a frame the same teardown has already condemned.
//
// Takes no domain mutex: closeSendQueue holds only the session's leaf sendMu,
// the session's Close takes peerMu only for its own NetCore unregistration,
// and markPeerDisconnected takes peerMu afterwards, so the sections are
// sequential and the fence stays outside the canonical lock order
// (docs/locking.md, "peerSession.sendMu — the outbound queue fence"). The
// test-only peerTeardownBarrier sits BETWEEN the two publications and is the
// only place the order is observable from; it must stay there, and it must stay
// outside both locks.
//
// What the disconnect charges depends on who closed the session first. The
// session is closed here, after the fence, before that is read: the first close
// wins closeOnce, so either this records that the serve loop gave up on the
// peer, or it returns after a ConnectionManager eviction that came first and
// makes that reason visible. A session the ConnectionManager evicted is
// published as disconnected without being charged — the read error the serve
// loop saw on the closed socket is this node's doing.
//
// The first closer is not necessarily the first cause: when the peer's own
// failure is already waiting in errCh and an eviction wins closeOnce first,
// that failure goes uncharged. Accepted — the eviction was closing the session
// regardless, and the error guarded against here is the opposite one, charging
// the peer for this node's own close.
func (s *Service) retirePeerSession(session *peerSession, err error) {
	if session == nil {
		return
	}
	session.discardSendQueue()
	// Every caller is about to return from the serve loop with err; the
	// close error adds nothing to it.
	_ = session.Close()
	s.runPeerTeardownBarrier()
	if session.closedByLocalDecision() {
		s.markPeerDisconnectedByLocalDecision(session.address)
		return
	}
	s.markPeerDisconnected(session.address, err)
}

// markPeerDisconnected publishes the peer at address as no longer carrying
// traffic: it is the write behind peerAcceptsOutboundFrames, the gate every
// producer passes before it hands a frame to a queue.
//
// ORDERING CONTRACT for callers that are tearing a connection down: the
// connection's queue must have STOPPED ACCEPTING before this is called, never
// after. A producer reads this publication and the queue's own answer as one
// decision, and only the fence-first order makes the pair honest — otherwise a
// producer admitted a moment before the publication still deposits its frame
// into a queue the same teardown discards, and its "accepted" ends the caller's
// walk over the peer's remaining connections. The two teardowns that own a
// queue do it: retirePeerSession above (closeSendQueue) on the dialled tier and
// trackInboundDisconnect (NetCore.ShutSendQueue) on the accepted one. Callers
// with NO live queue behind the address — a failed dial, a self-identity
// cooldown — have nothing to fence and call this directly.
//
// A non-nil err charges the peer a failure; nil records a clean disconnect.
func (s *Service) markPeerDisconnected(address domain.PeerAddress, err error) {
	s.publishPeerDisconnected(address, peerDisconnectChargeFromError(err))
}

// markPeerDisconnectedByLocalDecision publishes the peer at address as
// disconnected after THIS node closed the connection on its own decision (a
// ConnectionManager eviction or shutdown). The peer did nothing, so neither its
// failure counter, its last error nor its score moves; the same ordering
// contract as markPeerDisconnected applies.
func (s *Service) markPeerDisconnectedByLocalDecision(address domain.PeerAddress) {
	s.publishPeerDisconnected(address, localDecisionDisconnect{})
}

// peerDisconnectCharge is what a disconnect does to the peer's health record
// beyond publishing it as disconnected. Caller holds s.peerMu.
type peerDisconnectCharge interface {
	applyLocked(health *peerHealth)
}

// peerDisconnectChargeFromError maps a disconnect error onto its charge.
func peerDisconnectChargeFromError(err error) peerDisconnectCharge {
	if err != nil {
		return failedDisconnect{err: err}
	}
	return cleanDisconnect{}
}

// failedDisconnect: the connection failed; the peer is charged for it.
type failedDisconnect struct {
	err error
}

func (c failedDisconnect) applyLocked(health *peerHealth) {
	health.ConsecutiveFailures++
	health.LastError = c.err.Error()
	health.Score = clampScore(health.Score + peerScoreFailure)
	// Set machine-readable disconnect code when the error wraps a
	// known protocol error (e.g. frame-too-large, rate-limited).
	// The generic "protocol-error" sentinel is excluded — it carries
	// no diagnostic value beyond "something went wrong".
	if code := protocol.ErrorCode(c.err); code != protocol.ErrCodeProtocol {
		health.LastDisconnectCode = code
	} else {
		health.LastDisconnectCode = ""
	}
}

// cleanDisconnect: the connection ended without an error.
type cleanDisconnect struct{}

func (cleanDisconnect) applyLocked(health *peerHealth) {
	// Clean disconnect — clear the code so stale values from a
	// previous error-disconnect do not persist in diagnostics.
	health.ConsecutiveFailures = 0
	health.LastDisconnectCode = ""
	health.Score = clampScore(health.Score + peerScoreDisconnect)
}

// localDecisionDisconnect: this node closed the connection itself. It is
// neither a failure nor a clean departure of the peer, so the record keeps
// whatever it said about the peer before.
type localDecisionDisconnect struct{}

func (localDecisionDisconnect) applyLocked(*peerHealth) {}

// publishPeerDisconnected is the body shared by markPeerDisconnected and
// markPeerDisconnectedByLocalDecision; charge is the only part that differs.
func (s *Service) publishPeerDisconnected(address domain.PeerAddress, charge peerDisconnectCharge) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "markPeerDisconnected").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "markPeerDisconnected").Str("phase", "lock_held").Str("address", string(address)).Msg("peer_mu_writer")

	address = s.resolveHealthAddress(address)
	health := s.ensurePeerHealthLocked(address)
	now := time.Now().UTC()
	health.Connected = false
	health.Direction = ""
	s.updatePeerStateLocked(health, peerStateReconnecting)
	health.LastDisconnectedAt = now
	charge.applyLocked(health)
	peerID := s.peerIDs.identityOf(address)
	if !peerID.IsZero() {
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

	s.finishK3AdvertisedPeerLocked(adTicket)

	adTicket = nil
	adLocked = false

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
// running any application-level exchanges (sync).
//
// All side-effects — session registration, metadata writes, subscribe,
// sync, routing — are deferred to onCMSessionEstablished which runs
// only AFTER the CM event loop validates the slot generation. This
// ensures a stale dial (generation mismatch) is discarded with zero
// externally visible mutations.
func (s *Service) openPeerSessionForCM(ctx context.Context, address domain.PeerAddress) (*peerSession, error) {
	// TLS first; v1 only on the connection of a probe that allowed it
	// (session_dial_choice.go). A transport error
	// comes back wrapped in errPeerDialTransport, so rollout telemetry still
	// separates "never reached them" from a handshake refusal by type.
	transport, err := s.dialPeerTransportForCM(ctx, address)
	if err != nil {
		return nil, err
	}
	return s.finishPeerSessionForCM(ctx, address, transport)
}

func (s *Service) finishPeerSessionForCM(ctx context.Context, address domain.PeerAddress, transport *dialledTransport) (result *peerSession, outcomeErr error) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	// Capture the ORIGINAL transport binding before readiness/auth I/O waits.
	var outgoing r2aHopPublication
	if s.r2OwnDMOwner != nil {
		outgoing, _ = s.captureR2aHopPublication(transport)
	}
	var err error
	rawConn, metered, conn := transport.raw, transport.metered, transport.wire

	// Guard goroutine: close the connection if the parent context is cancelled
	// before readiness commits. A later dial cancellation must not close the
	// session already handed to CM; explicit session teardown still does.
	// On success we close the abort channel WITHOUT closing conn — the CM
	// event loop takes ownership.
	//
	// Both goroutines this function starts are on runLoopsWg, and the Add is
	// legal while stopRunLifecycle waits: this function runs only as the CM's
	// DialFn, on a dial worker that the CM event loop — itself a member —
	// joins (cm.dialWg) before it returns, so the counter is never zero here.
	readiness := newPeerSessionReadiness(ctx)
	abort := make(chan struct{})
	s.goRunLoop(func() {
		select {
		case <-ctx.Done():
			if readiness.abort(context.Cause(ctx)) != nil {
				_ = conn.Close()
			}
		case <-abort:
			// Handshake completed (success or handled error).
			// Do NOT close conn — caller manages lifetime.
		}
	})

	enableTCPKeepAlive(rawConn)

	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	// The transport's reader: on a v1 connection the probe opened, it holds
	// what the listener sent behind the probe's answer.
	reader := transport.reader
	session := &peerSession{
		r2OwnDMHop:   outgoing.sealed,
		common:       transport.common,
		readiness:    readiness,
		address:      address,
		peerIdentity: domain.PeerIdentity{},
		connID:       transport.connID,
		proven:       transport.proven,
		conn:         conn,
		metered:      metered,
		sendCh:       make(chan peerSendItem, peerSessionSendBuffer),
		inboxCh:      make(chan protocol.Frame, peerSessionInboxBuffer),
		errCh:        make(chan error, 1),
		// Owner-serialised contact sync (sender-key recovery). Unbuffered:
		// see the peerSession field doc.
		contactSyncCh: make(chan chan int),
	}
	defer func() {
		if result == nil {
			if outcomeErr != nil {
				session.k10FinishReported(outcomeErr)
			} else {
				session.k10MissingIfUnfinished()
			}
		}
	}() // Success hands the SAME capsule to CM; no premature final outcome.
	if outgoing.sealed != nil {
		outgoing.sealed.session = session
	}
	if session.common != nil {
		l := session.common
		l.mu.Lock()
		if !l.ended && l.peer == nil && l.id == session.connID {
			publication := s.beginRegistryPublication()
			l.peer = session
			if publication != nil {
				publication.Complete()
			}
		}
		l.mu.Unlock()
	}
	// closeOnError tears down the session (including the outbound NetCore
	// registration) and stops the guard goroutine. Must be called on every
	// error return path; success exits via close(abort) below, leaving the
	// session live for the CM caller to own.
	closeOnError := func() {
		_ = session.Close()
		close(abort)
	}
	if err := s.attachOutboundNetCore(session); err != nil {
		closeOnError()
		return nil, err
	}

	// The reader outlives this function: it serves the session for its whole
	// life, and it does work of its own off the serve loop — file_command
	// frames go straight to the file router, datagrams to the datagram plane.
	// Run must therefore not return, nor stop the file-transfer subsystem,
	// while it can still be inside one of those. It ends when the socket does,
	// and every outbound session socket is closed by the time the join waits:
	// the CM closes the sessions of its slots and of the dial results it
	// discards in its own shutdown, and every error path above closes its own.
	if transport.proven == nil {
		s.goRunLoop(func() { s.readPeerSession(reader, session) })
	}

	// v1 asks for the welcome; v2 already holds it, verified, inside the
	// proven session — the listener's intro.
	var welcome protocol.Frame
	if transport.proven != nil {
		welcome = transport.proven.Intro
	} else if welcome, err = s.peerSessionRequest(session, protocol.Frame{}, "welcome", true); err != nil {
		closeOnError()
		s.forgetLegacyObservedWithoutWelcome(address, transport)
		return nil, err
	} else if err := s.refuseFullLegacyWelcome(address, transport, welcome); err != nil {
		closeOnError()
		return nil, err
	} else if err := s.refuseLegacyWelcome(address, domain.PeerIdentityFromWire(welcome.Address)); err != nil {
		closeOnError()
		log.Warn().Err(err).Str("peer", string(address)).Msg("outbound_v1_refused_identity_pinned_to_v2")
		return nil, err
	}
	if err := validateProtocolHandshake(welcome); err != nil {
		closeOnError()
		log.Warn().Err(err).Str("peer", string(address)).Int("version", welcome.Version).Msg("outbound_peer_protocol_too_old")
		// Do NOT react here — the caller (CM dial worker) propagates the
		// error to onCMDialFailed, which reacts exactly once
		// (reactToIncompatibleDial). The structured error carries the version
		// evidence and what stands behind it: the intro of a proven v2
		// session may set lockouts and feed the reporter set, a v1 welcome
		// before auth_ok is a claim and only paces the address.
		return nil, incompatibleVersionOf(welcome, versionEvidenceOf(transport.proven != nil), err)
	}
	// Self-loopback guard: if the welcome carries our own Ed25519
	// identity we just dialled ourselves. Tear down the session
	// through the wrapper (closeOnError → session.Close() → NetCore.Close())
	// and surface a structured selfIdentityError so the CM dial-fail
	// path applies an address-level cooldown instead of retrying.
	if s.isSelfIdentity(domain.PeerIdentityFromWire(welcome.Address)) {
		closeOnError()
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Msg("outbound_self_identity_rejected")
		return nil, s.newSelfIdentityError(address)
	}
	// peerMu around the metadata write, not just around the map insert below:
	// readPeerSession is already running (it delivered the welcome that got us
	// here) and reads the same fields on its capability gates, so a peer that
	// pipelines a frame behind its own welcome puts a reader on
	// session.capabilities at the instant this line assigns it. The field doc
	// on peerSession already declares peerMu as its guard — this is the write
	// side honouring it. The typed local set is resolved BEFORE the lock: it
	// consults the datagram layer, and no domain mutex is held across that.
	local := s.localHandshakeCapabilities()
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	if s.k3Advertised != nil {
		s.applyK3AdvertisedWelcomeLocked(session, welcome, local)
	} else {
		applyWelcomeMetadata(session, welcome, local)
	}
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()

	// Stash welcome metadata for deferred application in
	// onCMSessionEstablished after the generation check passes.
	session.welcomeMeta = &peerWelcomeMeta{
		welcome:         welcome,
		clientVersion:   welcome.ClientVersion,
		clientBuild:     welcome.ClientBuild,
		observedAddress: welcome.ObservedAddress,
	}

	if transport.proven != nil {
		// Proven intro and negotiated metadata are ready before its reader starts:
		// the listener may pipeline data immediately behind session_proof.
		if err := s.completeOutboundAuth(session); err != nil {
			closeOnError()
			return nil, err
		}
		session.selectedProof = s.captureSelectedProof(transport)
		s.goRunLoop(func() { s.readPeerSession(reader, session) })
	} else if err := s.authenticatePeerSession(session, welcome); err != nil {
		closeOnError()
		return nil, err
	}

	// Handshake succeeded — stop the guard goroutine without closing conn.
	// From here on, the CM event loop owns session lifetime.
	// NOTE: session is NOT registered in s.sessions and no subscribe/sync
	// has run. readPeerSession goroutine buffers incoming frames in
	// inboxCh (capacity peerSessionInboxBuffer) until
	// onCMSessionEstablished drains them.
	close(abort)

	return session, nil
}

// onCMSessionEstablished is called by ConnectionManager when a slot has
// entered Initializing — i.e., AFTER the generation check passes and after
// cm.mu is released. The slot becomes Active only once the session
// goroutine below emits SessionInitReady.
//
// The callback runs in the CM event loop, so it MUST NOT perform any
// blocking I/O. All socket-level work (syncPeerSession,
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
	s.peerMu.Lock()
	receipt := s.prepareFindNodeCMAcceptedLocked(info)
	publication := s.beginRegistryPublication()
	session.selectedGeneration = info.SlotGeneration
	session.terminal.bindGeneration(info.SlotGeneration)
	if receipt != nil {
		session.findNodeReceipt = receipt
	}
	if publication != nil {
		publication.Complete()
	}
	s.peerMu.Unlock()
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
	// SessionInitReady is emitted, so ActiveCount() does not count this
	// peer and buildPeerExchangeResponse() does not advertise it during
	// setup; Slots() reports it only as Initializing.
	if wm := session.welcomeMeta; wm != nil {
		_, proven := session.provenIdentity()
		s.learnIdentityFromWelcome(wm.welcome, dialAddress, identityOriginOf(proven))
		s.peerMu.RLock()
		healthKey := s.resolveHealthAddress(dialAddress)
		s.peerMu.RUnlock()
		s.addPeerID(healthKey, addressIdentity{identity: session.peerIdentity, origin: identityOriginOf(proven)})
		s.addPeerVersion(healthKey, wm.clientVersion)
		s.addPeerBuild(healthKey, wm.clientBuild)
		s.recordObservedAddress(session.peerIdentity, wm.observedAddress)
		session.welcomeMeta = nil // release for GC
	}

	// Launch the session goroutine. All blocking I/O (subscribe, sync,
	// serve loop) runs here, never in the CM event loop.
	//
	// Joined by runLoopsWg (stopRunLifecycle). The goroutine serves
	// frames — storing messages, scheduling receipts and key syncs on
	// goBackground — until its session closes, and Run must not return while
	// it still does: a bare `go` here let it keep working on a node whose
	// caller had already been told it stopped. It ends on the lifecycle
	// context (servePeerSession) and on the socket the ConnectionManager
	// closes in its own shutdown, so the join is bounded. Adding to runLoopsWg
	// from here is legal even while stopRunLifecycle waits: this callback runs
	// on the CM event loop, which is itself on runLoopsWg, so the counter is
	// never zero at this Add.
	s.goRunLoop(func() { s.runCMSession(session, slotAddress, info.SlotGeneration) })
}

// cmSessionSetupOutcome is how the application-level setup of a CM session
// ended, from the point of view of who answers for it.
type cmSessionSetupOutcome int

const (
	// cmSessionSetupSucceeded: initPeerSession succeeded on a session the
	// ConnectionManager had not evicted when the outcome was classified.
	cmSessionSetupSucceeded cmSessionSetupOutcome = iota + 1
	// cmSessionSetupFailed: initPeerSession failed and the peer answers for
	// it.
	cmSessionSetupFailed
	// cmSessionSetupAbortedByLocalEviction: the ConnectionManager evicted the
	// slot — or shut down — while setup ran, and closed the session. Whatever
	// initPeerSession returned, nobody may charge the peer for it or publish
	// the session.
	cmSessionSetupAbortedByLocalEviction
)

// classifyCMSessionSetup decides the outcome of initPeerSession. A local
// eviction outranks the error: the ConnectionManager closed the transport
// under the setup, so the error is the echo of that close, not a failure of
// the peer. When initErr is non-nil the caller must have closed the session
// itself first (see closedByLocalEviction for why that makes the read sound).
// A success is re-checked under peerMu when the session is registered.
func classifyCMSessionSetup(session *peerSession, initErr error) cmSessionSetupOutcome {
	switch {
	case session.closedByLocalEviction():
		return cmSessionSetupAbortedByLocalEviction
	case initErr != nil:
		return cmSessionSetupFailed
	default:
		return cmSessionSetupSucceeded
	}
}

// runCMSession is the session goroutine of an outbound ConnectionManager
// session: application-level setup, publication, the serve loop and the
// withdrawal after it. slotAddress is the CM slot's canonical address and
// slotGeneration the generation the slot entered Initializing with; both go
// back to the CM in SessionInitReady / ActiveSessionLost.
//
// The goroutine alone accounts for the peer behind the session — setup
// failures, route quarantine, connected/disconnected health, routing
// registration and deregistration. onCMSessionTeardown only withdraws the
// session from the session maps. Each charge is decided by who closed the
// session first (peerSession.closedBy): a session the ConnectionManager closed
// on its own decision is never charged to the peer and never published.
func (s *Service) runCMSession(session *peerSession, slotAddress domain.PeerAddress, slotGeneration uint64) {
	defer session.k10MissingIfUnfinished()
	if session.releaseAddressDial != nil {
		defer session.releaseAddressDial()
	}
	// --- Phase 1: application-level setup (blocking I/O) ---
	initErr := s.initPeerSession(session)
	if initErr != nil {
		// Close BEFORE classifying: the first close wins closeOnce, so this
		// either records that the goroutine gave up on the peer, or returns
		// after the ConnectionManager's close that came first and makes its
		// reason visible. The close error adds nothing to the setup error.
		_ = session.Close()
		session.k10FinishReported(initErr)
	}

	switch classifyCMSessionSetup(session, initErr) {
	case cmSessionSetupAbortedByLocalEviction:
		s.abortLocallyEvictedCMSession(session, slotAddress, initErr)
	case cmSessionSetupFailed:
		s.failCMSessionSetup(session, slotAddress, slotGeneration, initErr)
	case cmSessionSetupSucceeded:
		s.publishAndServeCMSession(session, slotAddress, slotGeneration)
	}
}

// abortLocallyEvictedCMSession ends the goroutine of a session the
// ConnectionManager closed on its own decision while setup ran. The peer is not
// charged — no setup failure, no route quarantine — and the session is not
// published; a session that was torn down proves nothing about the peer
// either, so the setup-failure counter is not cleared.
//
// Only what the session left behind without being published is released: the
// dialOrigin entry dialForCM registered for a fallback port, and the
// control-frame bucket frames dispatched during setup may have created.
// Nothing is reported to the ConnectionManager: every local eviction
// removes the slot (or clears the whole table at shutdown), so there is no slot
// left at this generation for an event to reach.
func (s *Service) abortLocallyEvictedCMSession(session *peerSession, slotAddress domain.PeerAddress, initErr error) {
	dialAddress := session.address
	log.Trace().Str("site", "abortLocallyEvictedCMSession").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked := true
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(s.k3AdvertisedActive)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "abortLocallyEvictedCMSession").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.forgetUnpublishedDialOriginLocked(session)
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "abortLocallyEvictedCMSession").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.dropOutboundControlFrameBucket(session.connID)

	log.Info().
		Err(initErr).
		Str("peer", string(dialAddress)).
		Str("slot", string(slotAddress)).
		Uint64("conn_id", uint64(session.connID)).
		Msg("cm_session_setup_aborted_local_eviction")
}

// failCMSessionSetup charges a failed initPeerSession to the peer and reports
// it to the ConnectionManager, which retries with backoff or replaces the slot.
// The session is already closed (runCMSession).
func (s *Service) failCMSessionSetup(session *peerSession, slotAddress domain.PeerAddress, slotGeneration uint64, initErr error) {
	dialAddress := session.address
	if !localAddressDialOutcome(initErr) && s.runCtx.Err() == nil {
		s.recordAddressDialFailure(slotAddress)
	}
	log.Warn().Err(initErr).Str("peer", string(dialAddress)).Msg("cm_session_setup_failed")
	// Session was never registered in s.sessions / s.upstream, so only
	// what it left unpublished needs cleanup: dialOrigin (set by dialForCM
	// for fallback ports) and the control-frame bucket.
	// Also record the setup failure in the local per-address counter so
	// PeerProvider.SetupFailureBannedFn can mute the address once the
	// threshold is reached — breaks the reconnect storm against peers
	// whose handshake reply keeps failing during session setup.
	log.Trace().Str("site", "failCMSessionSetup").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked := true
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(s.k3AdvertisedActive)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "failCMSessionSetup").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.forgetUnpublishedDialOriginLocked(session)
	// Record failure against the slot's canonical address (slotAddress)
	// rather than dialAddress so siblings of a fallback-port peer share
	// the cooldown — matches the per-slot semantics of CM retry budget.
	now := time.Now()
	s.recordSetupFailureLocked(slotAddress, now)
	// Setup-failure quarantine trigger: when the per-address
	// failure threshold is crossed AND we know the peer's
	// identity (auth completed before initPeerSession failed),
	// arm route quarantine on the session's penalty subject —
	// the identity only when a v2 session proved it, the dialled
	// address otherwise: the welcome of the threshold-crossing
	// dial names whatever identity the dialled peer chose (see
	// penalty_subject.go). B1 already mutes
	// outbound dial attempts to the address; quarantine
	// additionally mutes inbound routing-snapshot trust and
	// transit usage for the same peer. Two complementary
	// brakes on the same root signal. See
	// routing_route_quarantine.go and
	// docs/protocol/network_security.md §13.
	if !session.peerIdentity.IsZero() && s.setupFailureExceedsThresholdLocked(slotAddress) {
		s.armRouteQuarantineLocked(session.penaltySubject(), quarantineReasonSetupFailureCycle, now)
	}
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "failCMSessionSetup").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.dropOutboundControlFrameBucket(session.connID)
	// Notify CM so it can reconnect with backoff.
	s.connManager.EmitSlot(ActiveSessionLost{
		Address:        slotAddress,
		Identity:       session.peerIdentity,
		Error:          initErr,
		WasHealthy:     false,
		SlotGeneration: slotGeneration,
	})
}

// forgetUnpublishedDialOriginLocked removes the dialOrigin entry dialForCM
// registered for a session that is ending without ever having been published.
// The entry is keyed by the dial address alone, so it is left alone when
// another session is registered under that address: that entry is the other
// session's, and deleting it would fold its fallback-port health back onto the
// wrong key. Caller holds s.peerMu.Lock.
func (s *Service) forgetUnpublishedDialOriginLocked(session *peerSession) {
	adTicket := s.beginK3AdvertisedPeerLocked()
	if adTicket != nil {
		defer s.guardK3AdvertisedPeerLocked(adTicket)
	}
	if registered, ok := s.sessions[session.address]; ok && registered != session {
		s.finishK3AdvertisedPeerLocked(adTicket)
		return
	}
	delete(s.dialOrigin, session.address)
	s.finishK3AdvertisedPeerLocked(adTicket)
}

// registerCMSession enters a session whose setup succeeded into s.sessions /
// s.upstream, unless the ConnectionManager has closed it on its own decision
// in the meantime. Returns false when it did not register.
//
// The check and the registration share one peerMu section, and that is what
// makes them race-free against the ConnectionManager: it records the close
// reason before it calls onCMSessionTeardown, which removes the entry under
// peerMu. If the teardown's section came first, this section sees the reason
// and does not register; if this section came first, the teardown finds the
// entry and withdraws it. A closed session is never left registered.
//
// What the check does NOT cover is an eviction that lands after this section
// and before publishAndServeCMSession's markPeerConnected /
// onPeerSessionEstablished. That session IS published — registered, marked
// connected, counted — and unwound moments later, when the serve loop sees the
// closed socket, by this goroutine's own withdrawal. Guaranteed even then: the
// accounting balances (this goroutine registers and deregisters, in that
// order, exactly once), and the peer is not charged (closedBy says local
// eviction). Not guaranteed: that no reader sees the session as live during
// that window. A re-check before each later step would only narrow it — the
// eviction can land after any of them — so none is made.
//
// Cross-domain: s.sessions and setupFailures belong to the peer domain
// (s.peerMu), s.upstream to the delivery domain (s.deliveryMu). Canonical
// s.peerMu OUTER → s.deliveryMu INNER.
func (s *Service) registerCMSession(session *peerSession, slotAddress domain.PeerAddress) bool {
	adNormalReturn := false
	adDeliveryLocked := false
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	dialAddress := session.address
	log.Trace().Str("site", "registerCMSession").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "registerCMSession").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	defer func() {
		if adNormalReturn {
			s.finishK3AdvertisedPeerLocked(adTicket)
		} else {
			s.guardK3AdvertisedPeerLocked(adTicket)
		}
		if s.k3Advertised != nil && adDeliveryLocked {
			s.deliveryMu.Unlock()
			adDeliveryLocked = false
		}
		adTicket = nil
		adLocked = false
		s.peerMu.Unlock()
		log.Trace().Str("site", "registerCMSession").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	}()
	if session.closedByLocalEviction() {
		adNormalReturn = true
		return false
	}

	log.Trace().Str("site", "registerCMSession").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	adDeliveryLocked = true
	log.Trace().Str("site", "registerCMSession").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	var k3 k3RegisteredTicket
	if s.k3Registered != nil {
		k3 = s.k3BeginSessionLocked(dialAddress)
	}
	s.k3AdvertisedSessionChangingLocked(dialAddress, session)
	publication := s.beginRegistryPublication()
	s.sessions[dialAddress] = session
	s.upstream[dialAddress] = struct{}{}
	if publication != nil {
		publication.Complete()
	}
	if s.k3Registered != nil {
		s.k3FinishSessionLocked(k3)
	}
	adDeliveryLocked = false
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "registerCMSession").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	adNormalReturn = true
	return true
}

// runCMSessionRegisterBarrier fires the test-only synchronisation point
// described on the cmSessionRegisterBarrier field.
func (s *Service) runCMSessionRegisterBarrier() {
	if s.cmSessionRegisterBarrier != nil {
		s.cmSessionRegisterBarrier()
	}
}

// publishAndServeCMSession publishes a session whose setup succeeded, serves
// it until it ends and withdraws it again. A session the ConnectionManager
// closed before it could be registered is aborted instead.
func (s *Service) publishAndServeCMSession(session *peerSession, slotAddress domain.PeerAddress, slotGeneration uint64) {
	dialAddress := session.address

	s.runCMSessionRegisterBarrier()

	// Register session in Service maps. This is the first point where
	// the session becomes visible to routingTargets, enqueuePeerFrame,
	// connectedHostsLocked, and other lookups.
	if !s.registerCMSession(session, slotAddress) {
		s.abortLocallyEvictedCMSession(session, slotAddress, nil)
		return
	}

	// The welcome was checked against the pin during the dial; the
	// identity may have proved itself over v2 since, and the sweep that
	// closes legacy sessions (v2_identity_claims.go) only sees sessions
	// already registered. Checking AFTER registering leaves no window: a
	// proof that came first is seen here, one that comes later sweeps
	// this session. A refused session is withdrawn before anything is
	// published for it, and charged to nobody (isUnchargedDialRefusal).
	_, proven := session.provenIdentity()
	if !proven {
		if err := s.refuseLegacyWelcome(dialAddress, session.peerIdentity); err != nil {
			log.Warn().Err(err).Str("peer", string(dialAddress)).Msg("outbound_v1_refused_identity_proved_v2_during_setup")
			s.recordAddressDialFailure(slotAddress)
			s.withdrawRefusedCMSession(session, slotAddress, slotGeneration, err)
			return
		}
	}

	// Only a setup that ends with a published session proves the peer
	// healthy, so the setup-failure counter is cleared once the session has
	// passed the pin — a session refused above is not published.
	s.peerMu.Lock()
	s.clearSetupFailuresLocked(slotAddress)
	s.peerMu.Unlock()

	// The session counts as an outbound session of its kind only now that it
	// is going to be published: a setup that failed, a slot the
	// ConnectionManager evicted under the setup and a refusal by the pin
	// above all end before this line, and none of them served the peer.
	s.sessionKinds.outbound().note(proven, domain.ProtocolVersion(session.version))

	// --- Phase 2: session is fully operational ---
	// Promote CM slot from Initializing → Active so ActiveCount() and
	// buildPeerExchangeResponse() start counting and advertising this
	// peer, and Slots() reports it as Active. Until this event the slot
	// is not advertised to peer exchange. Emitted only once the session is
	// registered and has passed the pin, so a slot is never promoted for a
	// session the Service did not publish.
	s.publishFindNodeCM(session, slotAddress, slotGeneration)
	s.connManager.EmitSlot(SessionInitReady{
		Address:        slotAddress,
		SlotGeneration: slotGeneration,
	})
	s.publishR2OwnDMHop(session)

	s.markPeerConnected(dialAddress, peerDirectionOutbound)
	if s.selectedPins != nil {
		s.selectedPins.notify()
	}
	s.flushPendingPeerFrames(dialAddress)

	// Routing table: register direct peer. session.capabilities is
	// stable by this point (applyWelcomeMetadata ran in
	// openPeerSessionForCM before the CM event loop validated the
	// slot generation), so we pass the raw slice and let the hook
	// flatten it for the relay-cap direct-route gate. This goroutine is
	// the only one that deregisters it (withdrawServedCMSession), so the
	// registration and its deregistration can never run in the wrong
	// order.
	s.onPeerSessionEstablished(session.peerIdentity, session.capabilities)

	// Send full table sync to the new peer (Phase 1.2).
	if !session.peerIdentity.IsZero() && s.sessionHasCapability(session, domain.CapMeshRoutingV1) && s.sessionHasCapability(session, domain.CapMeshRelayV1) {
		s.sendOutboundFullTableSync(s.runCtx, session.peerIdentity, dialAddress)
	}

	// --- Phase 3: main session loop ---
	err := s.servePeerSession(s.runCtx, session)
	// Close before withdrawing, for the same reason as after a failed
	// setup: the withdrawal reads who closed the session first. Every
	// exit of the serve loop leaves the session dead, so closing it here
	// changes nothing for the peer; the close error adds nothing to err.
	_ = session.Close()
	session.k10FinishReported(err)

	s.withdrawServedCMSession(session, slotAddress, slotGeneration, err)
}

// withdrawRefusedCMSession takes back a session that registerCMSession
// entered and the pin then refused: its identity proved v2 between the dial
// and the registration. Nothing else was published for it — no connected
// health, no route, no sync — so only the registration is undone. The close
// is recorded as this node's decision, and the loss is reported to the
// ConnectionManager as a failed setup carrying the refusal, which
// onCMDialFailed does not charge to the address.
func (s *Service) withdrawRefusedCMSession(session *peerSession, slotAddress domain.PeerAddress, slotGeneration uint64, refusal error) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	dialAddress := session.address
	_ = session.closeAs(peerSessionClosedByLegacyRefusal)
	// Cross-domain: s.sessions / s.dialOrigin → s.peerMu; s.upstream →
	// s.deliveryMu. Canonical s.peerMu OUTER → s.deliveryMu INNER.
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	s.deliveryMu.Lock()
	if s.sessions[dialAddress] == session {
		var k3 k3RegisteredTicket
		if s.k3Registered != nil {
			k3 = s.k3BeginSessionLocked(dialAddress)
		}
		s.k3AdvertisedSessionChangingLocked(dialAddress, nil)
		publication := s.beginRegistryPublication()
		delete(s.sessions, dialAddress)
		delete(s.upstream, dialAddress)
		delete(s.dialOrigin, dialAddress)
		if publication != nil {
			publication.Complete()
		}
		if s.k3Registered != nil {
			s.k3FinishSessionLocked(k3)
		}
	}
	s.deliveryMu.Unlock()
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	s.dropOutboundControlFrameBucket(session.connID)
	if session.metered != nil {
		s.accumulateSessionTraffic(dialAddress, session.metered)
	}
	s.connManager.EmitSlot(ActiveSessionLost{
		Address:        slotAddress,
		Identity:       session.peerIdentity,
		Error:          refusal,
		WasHealthy:     false,
		SlotGeneration: slotGeneration,
	})
}

// withdrawServedCMSession undoes publishAndServeCMSession once the serve loop
// has returned with err, and reports the loss to the ConnectionManager.
//
// The session-map entry is removed only if it still belongs to THIS session:
// onCMSessionTeardown may have removed it first (a ConnectionManager close),
// and a replacement session for the same dial address may have been registered
// since. Routing deregistration does not depend on that: this goroutine
// registered the session with onPeerSessionEstablished, so it deregisters it,
// exactly once — the teardown never does. The close is attributed to this node
// when the ConnectionManager closed the session on its own decision, and from
// the serve loop's error otherwise.
func (s *Service) withdrawServedCMSession(session *peerSession, slotAddress domain.PeerAddress, slotGeneration uint64, err error) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	dialAddress := session.address

	// Cross-domain cleanup: s.sessions / s.dialOrigin → s.peerMu;
	// s.upstream → s.deliveryMu.  Canonical s.peerMu OUTER →
	// s.deliveryMu INNER.
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("peer_mu_writer")
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_wait").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_held").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	ownedCleanup := s.sessions[dialAddress] == session
	if ownedCleanup {
		var k3 k3RegisteredTicket
		if s.k3Registered != nil {
			k3 = s.k3BeginSessionLocked(dialAddress)
		}
		s.k3AdvertisedSessionChangingLocked(dialAddress, nil)
		publication := s.beginRegistryPublication()
		delete(s.sessions, dialAddress)
		delete(s.upstream, dialAddress)
		delete(s.dialOrigin, dialAddress)
		if publication != nil {
			publication.Complete()
		}
		if s.k3Registered != nil {
			s.k3FinishSessionLocked(k3)
		}
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("delivery_mu_writer")
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "withdrawServedCMSession").Str("phase", "lock_released").Str("address", string(dialAddress)).Msg("peer_mu_writer")

	// Drop the per-session cmdLimiter bucket whether or not this
	// goroutine still owned the map entry. The bucket belongs to the
	// connID, not to the entry: onCMSessionTeardown may have withdrawn
	// the entry while the serve loop was still dispatching buffered
	// inbox frames, and a control frame dispatched after that
	// recreated the bucket. The serve loop has returned, so no frame
	// of this session is dispatched after this line, and the connID is
	// never reused, so no other session can be using the key. See
	// dropOutboundControlFrameBucket for the lifecycle contract.
	s.dropOutboundControlFrameBucket(session.connID)

	if !session.peerIdentity.IsZero() {
		s.deregisterServedCMSessionRouting(session, err)
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
		SlotGeneration: slotGeneration,
	})
}

// deregisterServedCMSessionRouting removes the direct route the session
// registered. A ConnectionManager eviction or shutdown is a local decision and
// is attributed as one, with no presence evidence: classifying it from the
// serve loop's error would read the read error on the socket the
// ConnectionManager closed as the peer hanging up.
func (s *Service) deregisterServedCMSessionRouting(session *peerSession, err error) {
	if session.closedByLocalDecision() {
		s.onPeerSessionClosedWithCause(session.peerIdentity, session.penaltySubject(), session.capabilities, sessionCloseLocalEviction)
		return
	}
	s.onPeerSessionClosedWithError(session.peerIdentity, session.penaltySubject(), session.capabilities, err)
}

// onCMSessionTeardown is called by ConnectionManager, on its event loop,
// whenever it deactivates an active or initializing slot. The CM has already
// closed the TCP transport and recorded on the session why (closedBy).
//
// It only withdraws the session from the session maps, so no producer picks a
// closed session and a replacement registered for the same address cannot
// collide with it. It charges the peer nothing and touches no routing state:
// the session goroutine (runCMSession) owns all of that. The goroutine sees
// the closed transport, reads the recorded reason, and either charges the
// peer — when the loss was the peer's — or, for a local eviction or shutdown,
// records neither a setup failure nor a failed disconnect and attributes the
// route withdrawal to this node. Its ActiveSessionLost, if any, is stale and
// suppressed by the generation guard.
func (s *Service) onCMSessionTeardown(info SessionInfo) {
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	// Session is registered under DialAddress (the actual TCP address used,
	// which may be a fallback port). Use DialAddress for cleanup, falling
	// back to the canonical Address when DialAddress is empty (pre-activation
	// teardown paths where no session was established).
	addr := info.DialAddress
	if addr == "" {
		addr = info.Address
	}

	// The entry is removed only if it still belongs to this session: the
	// session goroutine may have withdrawn it already (a peer-initiated
	// close it noticed first), or never registered it (setup still running,
	// or aborted on the recorded local eviction — see registerCMSession for
	// why the two sides cannot both miss each other).
	//
	// Cross-domain: s.sessions / s.dialOrigin → s.peerMu; s.upstream →
	// s.deliveryMu.  Canonical s.peerMu OUTER → s.deliveryMu INNER.
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_wait").Str("address", string(addr)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_held").Str("address", string(addr)).Msg("peer_mu_writer")
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_wait").Str("address", string(addr)).Msg("delivery_mu_writer")
	s.deliveryMu.Lock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_held").Str("address", string(addr)).Msg("delivery_mu_writer")
	ownedCleanup := info.Session != nil && s.sessions[addr] == info.Session
	if ownedCleanup {
		var k3 k3RegisteredTicket
		if s.k3Registered != nil {
			k3 = s.k3BeginSessionLocked(addr)
		}
		s.k3AdvertisedSessionChangingLocked(addr, nil)
		publication := s.beginRegistryPublication()
		delete(s.sessions, addr)
		delete(s.upstream, addr)
		delete(s.dialOrigin, addr)
		if publication != nil {
			publication.Complete()
		}
		if s.k3Registered != nil {
			s.k3FinishSessionLocked(k3)
		}
	}
	s.deliveryMu.Unlock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_released").Str("address", string(addr)).Msg("delivery_mu_writer")
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "onCMSessionTeardown").Str("phase", "lock_released").Str("address", string(addr)).Msg("peer_mu_writer")

	// The control-frame bucket is NOT dropped here: the session's
	// goroutine may still be dispatching frames it had buffered, and
	// it drops the bucket itself once it can dispatch no more (the
	// end of runCMSession — see dropOutboundControlFrameBucket).
}

// initPeerSession performs the application-level setup for an already
// authenticated session: runs the initial sync (conditionally get_peers,
// then fetch_contacts). Inbox subscription needs no setup here — the
// responder auto-registers it and replays the backlog at auth time
// (handleAuthSession → registerHelloRoute).
// Whether get_peers is sent depends on the aggregate network status at
// the moment of entry — see shouldRequestPeers() for the policy.
// This is the blocking I/O phase that must NOT run in the CM event
// loop — call it from a dedicated goroutine only.
//
// On success the session is ready for the servePeerSession read loop.
// On error the caller is responsible for session cleanup and CM notification.
func (s *Service) initPeerSession(session *peerSession) error {
	// Evaluate peer exchange policy before any session-setup I/O so that it
	// cannot shift the aggregate status between the decision point and its
	// use. In the CM path the session is not yet registered in s.health, so
	// the snapshot naturally excludes the peer being set up — matching the
	// legacy path (openPeerSession).
	requestPeers := s.shouldRequestPeers()
	if !requestPeers {
		s.logPeerExchangeSkipped(peerExchangePathSessionCM, session.address, peerExchangeSkipByAggregateHealthy)
	}

	_ = session.conn.SetDeadline(time.Time{})

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
	if session.releaseAddressDial != nil {
		session.releaseAddressDial()
	}
	log.Trace().Str("site", "onCMStaleSession").Str("phase", "lock_wait").Str("address", string(session.address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked := true
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(s.k3AdvertisedActive)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "onCMStaleSession").Str("phase", "lock_held").Str("address", string(session.address)).Msg("peer_mu_writer")
	// Clean up the dialOrigin entry the stale dial registered — unless a
	// newer session for the same dial address is registered, whose entry it
	// now is. The same rule as every other unpublished-session exit.
	s.forgetUnpublishedDialOriginLocked(session)
	adLocked = false
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
	// A refusal that says nothing against the address
	// (isUnchargedDialRefusal) is not charged. A v1 answer refused because
	// the peer requires v2 — or the v1 dial held after such a refusal — is
	// this node's decision, and a man in the middle can provoke it at will:
	// charging the address would let him push it out of rotation and keep
	// v2 away from it, so the only consequence is the hold of the v1 dial,
	// set where the refusal was made; the refusal counters are kept by the
	// store. A busy listener and an endpoint that changed hands say nothing
	// against the address either.
	if isUnchargedDialRefusal(err) {
		log.Debug().Err(err).Str("peer", string(address)).Msg("cm_dial_refused_not_charged")
		return
	}
	if incompatible {
		// The structured error returned by openPeerSessionForCM carries the
		// version evidence and what stands behind it. Only a version proven
		// inside a v2 session is penalised; a claimed one — the probe's
		// answer, a v1 welcome before auth_ok, or no structured evidence at
		// all — only paces the address (reactToIncompatibleDial).
		s.reactToIncompatibleDial(address, err)
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
	var adTicket *k3AdvertisedTicket
	var adLocked bool
	if s.k3Advertised != nil {
		defer func() {
			if adLocked {
				s.guardK3AdvertisedPeerLocked(adTicket)
				s.peerMu.Unlock()
			}
		}()
	}
	log.Trace().Str("site", "applySelfIdentityCooldown").Str("phase", "lock_wait").Str("address", string(address)).Msg("peer_mu_writer")
	s.peerMu.Lock()
	adLocked = true
	adTicket = s.beginK3AdvertisedPeerLocked()
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
	s.finishK3AdvertisedPeerLocked(adTicket)
	adTicket = nil
	adLocked = false
	s.peerMu.Unlock()
	log.Trace().Str("site", "applySelfIdentityCooldown").Str("phase", "lock_released").Str("address", string(address)).Msg("peer_mu_writer")

	log.Warn().
		Str("peer", string(address)).
		Str("local_identity", selfErr.LocalIdentity.String()).
		Time("banned_until", bannedUntil).
		Bool("first_observation", firstObservation).
		Msg("self_identity_cooldown_applied")
}
