// routing_session.go hosts routing-related session lifecycle hooks:
// onPeerSessionEstablished / onPeerSessionClosed that register or withdraw
// direct-peer routes, publish RouteTableChanged events, and coordinate the
// per-peer announce state registry across connect/disconnect transitions.
// inboundPeerIdentity lives here because it is the routing-side bridge
// from ConnID to peer identity used by session-tied code paths.
//
// The disconnect path in onPeerSessionClosed is the site that invokes the
// parallel withdrawal fan-out documented in the "Disconnect withdrawal
// fan-out (parallel contract)" section of docs/routing.md; the fan-out
// helper itself (fanoutAnnounceRoutes) lives in routing_announce.go so
// that the announce-plane write path is in one place.
//
// Companion files: announce-plane wire path in routing_announce.go,
// relay-plane forwarding in routing_relay.go, address↔identity resolution
// in routing_resolver.go, hop_ack route confirmation in routing_hop_ack.go,
// pending-queue drain in routing_drain.go. The per-file scope table is
// the durable record — see the "internal/core/node/" section of the
// file map in docs/routing.md.
package node

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// inboundPeerIdentity returns the peer identity (Ed25519 fingerprint)
// for an inbound connection, derived from the hello frame's Address field.
// This is distinct from NetCore.Address() which stores the listen
// address (transport) for health tracking. NATed peers advertise a
// non-routable listen address (e.g. 127.0.0.1:64646) that must never
// be used as a routing identity.
func (s *Service) inboundPeerIdentity(id domain.ConnID) domain.PeerIdentity {
	pc := s.netCoreForID(id)
	if pc == nil {
		return domain.PeerIdentity{}
	}
	return pc.Identity()
}

// onPeerSessionEstablished is called when a session is fully established,
// for both outbound sessions (after handshake, auth, and sync) and inbound
// connections (after hello exchange). It registers the peer as a direct
// route in the routing table and triggers an immediate announcement cycle.
//
// caps is the peer's full negotiated capability set captured at session
// establishment. The hook flattens it to a relay-cap boolean for its own
// direct-route decision, but also forwards the full list to the announce
// state registry so routing-announce v2 can make mode decisions without a
// second s.peerMu round-trip. Only relay-capable peers become direct routes
// — a direct route advertises that this node can deliver relay_message to
// the destination. Without relay capability the route is non-deliverable;
// announcing it would cause other routing-capable nodes to learn a path
// that fails at the last hop. When relay cap is absent, session counting
// still occurs (so that onPeerSessionClosed can safely decrement) but no
// routing table entry is created and no announcement is triggered.
//
// Multi-session awareness: AddDirectPeer is called only on the 0→1
// transition (first session for this identity). Subsequent sessions for
// the same identity increment the session count but do not touch the
// routing table — AddDirectPeer is idempotent at the model level, but
// the session-count gate here is the primary defense against churn.
func (s *Service) onPeerSessionEstablished(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	if peerIdentity.IsZero() {
		return
	}

	hasRelayCap := sessionHasCap(caps, domain.CapMeshRelayV1)

	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_wait").Str("peer_identity", peerIdentity.String()).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_held").Str("peer_identity", peerIdentity.String()).Msg("peer_mu_writer")
	s.identitySessions[peerIdentity]++

	if hasRelayCap {
		s.identityRelaySessions[peerIdentity]++
	}
	firstRelay := s.identityRelaySessions[peerIdentity] == 1
	s.peerMu.Unlock()
	log.Trace().Str("site", "onPeerSessionEstablished").Str("phase", "lock_released").Str("peer_identity", peerIdentity.String()).Msg("peer_mu_writer")

	if !hasRelayCap {
		log.Debug().
			Str("peer", peerIdentity.String()).
			Msg("routing_peer_no_relay_cap_skip_direct_route")
		return
	}

	if !firstRelay {
		// Routing table is unchanged (the direct route already exists for
		// this identity), no announce trigger fires, and the persistent
		// capability snapshot in AnnouncePeerState is intentionally NOT
		// touched here. Caps are reconciled at cycle start in
		// AnnounceLoop.announceToAllPeers, where the registry snapshot is
		// synced to the per-cycle AnnounceTarget caps (i.e. the same
		// session routingCapablePeers actually picked). Updating caps in
		// this hook from caps unrelated to that target — e.g. a relay-only
		// session that routingCapablePeers cannot pick, or an overlapping
		// session with different v2 support than the older one that ends
		// up being chosen — only adds a window of inconsistency between
		// state.capabilities and the cycle-time target. The cycle-time
		// sync is the single point of truth for persistent caps; no
		// lifecycle-hook variant can match it without re-implementing the
		// same selection rule routingCapablePeers uses.
		log.Debug().
			Str("peer", peerIdentity.String()).
			Msg("routing_additional_session_no_table_update")
		return
	}

	// Withdrawal grace probation: if the previous session for this
	// identity closed within the grace window and the deferred
	// withdrawal has not fired yet, the direct route is still in
	// the routing table. Cancelling the timer leaves it there
	// untouched — no AddDirectPeer call, no seqno bump, no announce
	// trigger, no wire traffic. The peer is silently re-attached
	// from the routing layer's point of view; everything else
	// (session counters in identitySessions / identityRelaySessions)
	// was updated above under peerMu.
	//
	// See routing_withdrawal_grace.go for the contract and
	// docs/refactoring/route-withdrawal-grace-period.md for the
	// design rationale.
	if s.tryCancelPendingWithdrawal(peerIdentity) {
		// The route never left the table, but hop-ack failures
		// accumulated against the dead session during the grace
		// window may have armed the black-hole cooldown for the
		// (peer, peer) pair — which would keep the still-present
		// direct route hidden from Lookup / fetchRouteLookup for
		// up to BlackHoleCooldown ("peer online, route count=0").
		// The confirmed new session is positive liveness evidence
		// on par with an organic late hop-ack, so lift the arm.
		// The non-grace reconnect path gets the same clear inside
		// AddDirectPeer below.
		// Nil guard mirrors executeDeferredWithdrawal: bare test
		// fixtures may run without a routing table.
		if s.routingTable != nil && s.routingTable.ClearDirectPairCooldown(peerIdentity) {
			log.Info().
				Str("peer", peerIdentity.String()).
				Msg("routing_direct_pair_cooldown_cleared_on_grace_reconnect")
		}
		return
	}

	result, err := s.routingTable.AddDirectPeer(peerIdentity)
	if err != nil {
		log.Error().Err(err).Str("peer", peerIdentity.String()).Msg("routing_add_direct_peer_failed")
		return
	}

	log.Info().
		Str("peer", peerIdentity.String()).
		Uint64("seq", result.Entry.SeqNo).
		Bool("penalized", result.Penalized).
		Msg("routing_direct_peer_added")

	s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason:   domain.RouteChangeDirectPeerAdded,
		PeerID:   peerIdentity,
		Accepted: 1,
	})

	// Register or reset per-peer announce state for this routing-capable peer.
	// The registry takes a defensive copy of caps so subsequent mutation of
	// the session's capability slice cannot leak into AnnouncePeerState.
	s.announceLoop.StateRegistry().MarkReconnected(peerIdentity, caps)

	// When flap detection penalizes a peer, skip the triggered announce.
	// The route will be picked up by the next periodic announce cycle,
	// giving the link time to stabilize.
	if result.Penalized {
		log.Warn().
			Str("peer", peerIdentity.String()).
			Msg("routing_peer_in_holddown_skipping_announce")
		return
	}

	// Phase 3 PR 12.5 — incremental sync on reconnect. If we cached a
	// per-peer digest from the prior session AND the new session
	// negotiated mesh_route_sync_v1, emit the digest so the peer can
	// short-circuit the reconnect forced full sync on a match.
	//
	// Ordering matters: this block runs BEFORE TriggerUpdate so that
	// MarkPeerDigestPending arms a short suppression window first.
	if sessionHasCap(caps, domain.CapMeshRouteSyncV1) {
		now := time.Now().UTC()
		if digest, count, generatedAt, ok := s.routingTable.ConsumePeerDigestSnapshot(peerIdentity, now); ok {
			s.announceLoop.MarkPeerDigestPending(peerIdentity, now, digest)
			s.emitRouteSyncDigest(peerIdentity, digest, count, generatedAt)
		}
	}

	s.announceLoop.TriggerUpdate()
}

// sessionCloseCause distinguishes WHO initiated a session teardown,
// for the disconnect_storm quarantine accounting in
// onPeerSessionClosedWithCause.
//
// The distinction exists because the quarantine counts disconnects as
// evidence of PEER instability. A teardown the local node chose itself
// (inbox-overflow slow-consumer eviction, ConnectionManager slot
// replacement) says nothing about the peer's stability; counting it
// used to let a busy local node quarantine — and transit-tombstone —
// perfectly healthy neighbours, amplifying route churn across the mesh.
type sessionCloseCause int

const (
	// sessionClosePeerInitiated covers remote-side failures: EOF,
	// connection reset, read/write timeouts, protocol errors. These
	// are genuine peer-instability evidence and feed the
	// disconnect_storm sliding window.
	sessionClosePeerInitiated sessionCloseCause = iota
	// sessionCloseLocalEviction covers teardowns the local node
	// initiated deliberately (inbox-overflow slow-consumer eviction,
	// CM slot replacement/shutdown). Excluded from disconnect_storm
	// accounting; all other close-path effects (route withdrawal,
	// transit invalidation, counters) run unchanged.
	sessionCloseLocalEviction
)

// onPeerSessionClosed is called when a session for a peer identity closes.
// caps must describe the same capability set that was passed to
// onPeerSessionEstablished for this session so that the relay-capable
// session counter stays balanced.
//
// The close is attributed to the peer (sessionClosePeerInitiated) —
// cleanup paths that know the teardown was a local decision must call
// onPeerSessionClosedWithCause directly.
//
// Multi-session awareness: RemoveDirectPeer is called on the relay-session
// 1→0 transition (last relay-capable session for this identity). Total
// session count cleanup happens on the total-session 1→0 transition.
//
// On disconnect, own-origin direct routes are withdrawn on the wire
// (returned as wire-ready AnnounceEntry items) and transit routes
// learned through this peer are silently invalidated locally.
func (s *Service) onPeerSessionClosed(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	s.onPeerSessionClosedWithCause(peerIdentity, caps, sessionClosePeerInitiated)
}

// onPeerSessionClosedWithCause is onPeerSessionClosed with an explicit
// teardown attribution. cause only gates the disconnect_storm
// quarantine accounting; every other close-path effect is identical
// for both causes.
func (s *Service) onPeerSessionClosedWithCause(peerIdentity domain.PeerIdentity, caps []domain.Capability, cause sessionCloseCause) {
	if peerIdentity.IsZero() {
		return
	}

	hadRelayCap := sessionHasCap(caps, domain.CapMeshRelayV1)

	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_wait").Str("peer_identity", peerIdentity.String()).Msg("peer_mu_writer")
	s.peerMu.Lock()
	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_held").Str("peer_identity", peerIdentity.String()).Msg("peer_mu_writer")
	count := s.identitySessions[peerIdentity]
	if count <= 0 {
		s.peerMu.Unlock()
		log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_released").Str("peer_identity", peerIdentity.String()).Str("reason", "no_session").Msg("peer_mu_writer")
		return
	}
	s.identitySessions[peerIdentity]--
	isLastTotal := s.identitySessions[peerIdentity] == 0
	if isLastTotal {
		delete(s.identitySessions, peerIdentity)
	}

	lastRelay := false
	if hadRelayCap {
		relayCount := s.identityRelaySessions[peerIdentity]
		if relayCount > 0 {
			s.identityRelaySessions[peerIdentity]--
			lastRelay = s.identityRelaySessions[peerIdentity] == 0
			if lastRelay {
				delete(s.identityRelaySessions, peerIdentity)
			}
		}
	}
	// Route quarantine trigger: on the last-relay transition (peer
	// genuinely lost its relay capability for now), record the
	// disconnect timestamp and arm quarantine if the rate exceeds
	// the threshold. The quarantine map and history are guarded by
	// the same peerMu we already hold.  See routing_route_quarantine.go.
	//
	// Local evictions are excluded: a teardown the local node chose
	// itself is not evidence of peer instability, and counting it
	// let a backpressured local node quarantine healthy neighbours
	// (see sessionCloseCause).
	if lastRelay && cause == sessionClosePeerInitiated {
		s.maybeArmRouteQuarantineOnCloseLocked(peerIdentity, time.Now())
	}
	s.peerMu.Unlock()
	log.Trace().Str("site", "onPeerSessionClosed").Str("phase", "lock_released").Str("peer_identity", peerIdentity.String()).Bool("last_total", isLastTotal).Bool("last_relay", lastRelay).Msg("peer_mu_writer")

	// Phase 3 PR 12.5 — record the per-peer digest snapshot BEFORE
	// any storage-mutating cleanup below (RemoveDirectPeer /
	// InvalidateTransitRoutes) so the cached digest reflects what
	// we knew through the peer at the moment the session closed. A
	// recording past the cleanup would observe an emptied bucket
	// for this peer and stash a misleading digest that any
	// reconnect within SessionDigestCacheTTL would compare against
	// the peer's fresh full view and falsely mismatch.
	//
	// We record only on the last-total-session transition (peer
	// truly disappeared) and only when the peer's negotiated set
	// ever advertised mesh_route_sync_v1; without the capability
	// any future emit would be silently dropped by
	// sendFrameToIdentity's gate so the cache entry would never
	// be consumed.
	if isLastTotal && sessionHasCap(caps, domain.CapMeshRouteSyncV1) {
		s.recordPeerDigestOnSessionClose(peerIdentity)
	}

	if !lastRelay {
		// No relay sessions left (or never had one). If this is also the
		// last total session, invalidate any transit routes learned through
		// this peer. This covers the routing-only peer case (mesh_routing_v1
		// without mesh_relay_v1): the receive-path gate blocks new
		// announcements, but defense-in-depth ensures stale lineages are
		// cleaned up on disconnect.
		if isLastTotal {
			invalidated, exposed := s.routingTable.InvalidateTransitRoutes(peerIdentity)
			if invalidated > 0 {
				log.Info().
					Str("peer", peerIdentity.String()).
					Int("transit_invalidated", invalidated).
					Msg("routing_transit_routes_invalidated_on_disconnect")

				s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
					Reason:    domain.RouteChangeTransitInvalidated,
					PeerID:    peerIdentity,
					Withdrawn: invalidated,
				})

				// Trigger an immediate announce cycle so neighbors learn the
				// invalidated routes are withdrawn. Without this, stale routes
				// persist until the next periodic cycle (up to 30s).
				s.announceLoop.TriggerUpdate()
			}
			s.triggerDrainForExposed(exposed)
		} else {
			log.Debug().
				Str("peer", peerIdentity.String()).
				Int("remaining", count-1).
				Msg("routing_session_closed_others_remain")
		}
		return
	}

	// B3-equivalent: route withdrawal grace period. The entire
	// removal sequence (RemoveDirectPeer + fanout + trigger + poison
	// reverse) is delegated to maybeScheduleDeferredWithdrawal which
	// — depending on s.effectiveWithdrawalGracePeriod() — either
	// fires it inline (legacy / test mode) or schedules it after the
	// grace period. A reconnect inside the window cancels the timer
	// (see tryCancelPendingWithdrawal in onPeerSessionEstablished),
	// the direct route stays in the table, and no flap-signal frames
	// reach the wire. See routing_withdrawal_grace.go.
	s.maybeScheduleDeferredWithdrawal(peerIdentity, caps)
}

// poisonReverseToOtherPeers emits route_poison_v1 about each lost
// transit identity to every direct peer EXCEPT lostUplink itself
// (the lost peer is, by definition, gone). Reason is always
// uplink_lost — the trigger that called this helper is the lost
// session; the more granular health-dead / loop-detected reasons
// have their own emit sites (see TODO in 13.3-B's phase-4 doc
// entry). Capability gating happens inside SendRoutePoison; peers
// without mesh_poison_reverse_v1 silently skip.
//
// Scaling: this emits len(targets) × len(other-peers) frames. For a
// 100-identity, 8-peer mesh that is ~700 frames per disconnect —
// each is small (<200B). A future batch-frame extension can
// collapse this; current single-identity-per-frame matches the
// spec.
func (s *Service) poisonReverseToOtherPeers(ctx context.Context, lostUplink domain.PeerIdentity, targets []routing.PeerIdentity) {
	// Snapshot the routing-capable peer set once; SendRoutePoison
	// applies its own capability filter (v1 + poison_reverse) so
	// peers without mesh_poison_reverse_v1 will see SendRoutePoison
	// return false without enqueueing any frame.
	peers := s.routingCapablePeers()
	if len(peers) == 0 {
		return
	}
	for _, target := range targets {
		identity := domain.PeerIdentity(target)
		// Skip the lost uplink's own identity — the legacy withdrawal
		// path (fanoutAnnounceRoutes on result.Withdrawals above)
		// already withdraws claims about the disconnected peer
		// itself. Emitting poison about the same identity would be
		// a redundant signal handled identically by the receiver
		// (both end up withdrawing claims[lostUplink][us]); the
		// transit-identity poisons are the unique value-add of this
		// helper.
		if identity == lostUplink {
			continue
		}
		for _, peer := range peers {
			if peer.Identity == lostUplink {
				continue
			}
			s.SendRoutePoison(ctx, peer.Address, identity, protocol.RoutePoisonReasonUplinkLost)
		}
	}
}
