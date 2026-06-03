package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/routing"
)

// ---------------------------------------------------------------------------
// Route withdrawal grace period (probation on close)
// ---------------------------------------------------------------------------
//
// Background and rationale: see docs/refactoring/route-withdrawal-grace-period.md
//
// Summary:
//   - onPeerSessionClosed schedules executeDeferredWithdrawal after
//     routeWithdrawalGracePeriod instead of running it inline.
//   - onPeerSessionEstablished checks for a pending timer and, if
//     found, stops it and short-circuits the establish path — the
//     direct route is already in the table, the peer is back, no
//     seqno bump needed.
//   - When the timer fires (peer did not reconnect in time) the
//     deferred path executes the full RemoveDirectPeer + fanout +
//     trigger + poison sequence — byte-identical to the pre-grace
//     behaviour.
//
// Test seam: set routeWithdrawalGracePeriodTest to a non-zero value
// to override the production constant. Set it to a negative value
// to disable the grace period entirely and run withdrawals
// synchronously inline — every existing routing test that asserts
// fanout-by-the-time-onPeerSessionClosed-returns relies on this.

// cmReconnectRetryBudget is the total time the connection manager
// spends in backoff across its full reconnect cycle:
// reconnectMaxRetries attempts with exponential backoff
// (backoffDuration: 2+4+8 = 14s with the current constants). The
// withdrawal grace period is DERIVED from this value so the two
// subsystems cannot silently drift apart — a previous revision
// hard-coded 10s here while claiming to absorb the 14s retry cycle,
// which let the deferred RemoveDirectPeer fire BEFORE the third
// retry reconnected: seqno bump, withdrawal fanout, poison reverse,
// then re-add traffic — the exact storm the grace period exists to
// smooth.
func cmReconnectRetryBudget() time.Duration {
	var total time.Duration
	for attempt := 1; attempt <= reconnectMaxRetries; attempt++ {
		total += backoffDuration(attempt)
	}
	return total
}

// routeWithdrawalGraceSlack covers what the raw backoff sum does
// not: per-attempt dial + handshake latency, the pacer gate that
// each retry passes AFTER its backoff elapses (retryAfterBackoff),
// and general scheduler jitter. Sized generously — a grace period
// that is slightly too long only delays a genuine withdrawal by a
// few seconds, while one that is too short re-introduces the
// withdraw/re-add churn.
const routeWithdrawalGraceSlack = 6 * time.Second

// routeWithdrawalGracePeriod is how long onPeerSessionClosed waits
// before actually removing the direct route and broadcasting the
// withdrawal. A reconnect inside this window cancels the pending
// withdrawal entirely — no seqno bump, no wire traffic. Derived
// from the CM reconnect retry budget (see cmReconnectRetryBudget)
// plus slack for dial/pacer/scheduling latency, so a peer that
// reconnects on the LAST retry of the CM cycle still lands inside
// the window. Sessions that stay closed for longer are treated as
// genuinely lost and trigger the full withdrawal path.
var routeWithdrawalGracePeriod = cmReconnectRetryBudget() + routeWithdrawalGraceSlack

// effectiveWithdrawalGracePeriod returns the active grace duration.
//
// Resolution order:
//  1. routeWithdrawalGracePeriodTest > 0 → use the override (probation
//     tests set a few hundred ms to keep test runtime short).
//  2. routeWithdrawalGracePeriodTest < 0 → disable grace (synchronous
//     legacy path). Explicit opt-out for tests that want pre-grace
//     behaviour.
//  3. pendingWithdrawals == nil → service was built as a minimal
//     &Service{...} literal (every routing test pre-grace). Treat
//     as legacy sync — they assert fanout-by-the-time-close-returns
//     and the grace map is the production-only initialisation signal.
//  4. otherwise → use the production value routeWithdrawalGracePeriod
//     (derived from the CM retry budget at package init).
func (s *Service) effectiveWithdrawalGracePeriod() time.Duration {
	if s.routeWithdrawalGracePeriodTest > 0 {
		return s.routeWithdrawalGracePeriodTest
	}
	if s.routeWithdrawalGracePeriodTest < 0 {
		return 0
	}
	if s.pendingWithdrawals == nil {
		return 0
	}
	return routeWithdrawalGracePeriod
}

// maybeScheduleDeferredWithdrawal is the entry point from
// onPeerSessionClosed for the lastRelay branch. When the grace
// period is non-zero, an AfterFunc timer is armed and the function
// returns immediately; the caller proceeds with NO RemoveDirectPeer
// / fanout / trigger / poison. When the grace period is zero (legacy
// path, used by every pre-grace test), the deferred body executes
// synchronously here.
//
// Idempotent on repeat close: if a timer already exists for this
// identity it is NOT reset — the first close starts the clock and
// subsequent close events from the same identity reuse it.
func (s *Service) maybeScheduleDeferredWithdrawal(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	grace := s.effectiveWithdrawalGracePeriod()
	if grace <= 0 {
		// Legacy synchronous path — call the body inline.
		s.executeDeferredWithdrawal(peerIdentity, caps)
		return
	}

	s.peerMu.Lock()
	if s.pendingWithdrawals == nil {
		s.pendingWithdrawals = make(map[domain.PeerIdentity]*pendingWithdrawal)
	}
	if _, alreadyPending := s.pendingWithdrawals[peerIdentity]; alreadyPending {
		// Another close already armed the timer for this identity.
		// Keep the original timer running — restarting it would
		// indefinitely defer withdrawal under a chatty close pattern.
		s.peerMu.Unlock()
		log.Trace().Str("peer", string(peerIdentity)).
			Msg("routing_withdrawal_grace_already_pending")
		return
	}
	captured := peerIdentity
	capturedCaps := append([]domain.Capability(nil), caps...)
	timer := time.AfterFunc(grace, func() {
		s.firePendingWithdrawal(captured, capturedCaps)
	})
	s.pendingWithdrawals[peerIdentity] = &pendingWithdrawal{
		timer: timer,
		caps:  capturedCaps,
	}
	s.peerMu.Unlock()

	log.Info().
		Str("peer", string(peerIdentity)).
		Dur("grace", grace).
		Msg("routing_direct_peer_withdrawal_deferred")
}

// pendingWithdrawal is the entry stored in s.pendingWithdrawals.
// The caps slice is captured at close time so the timer callback
// has all the inputs it needs without re-reading session state.
type pendingWithdrawal struct {
	timer *time.Timer
	caps  []domain.Capability
}

// firePendingWithdrawal is the timer callback invoked when the
// grace period elapses without a reconnect. It first claims the
// pending entry under peerMu — if the entry is no longer present
// (the timer race: shutdown / tryCancel deleted it while the
// callback was already scheduled and timer.Stop returned false),
// the callback returns WITHOUT running executeDeferredWithdrawal.
//
// Without this atomic claim, cancelAllPendingWithdrawalsForShutdown
// could delete the entry and unlock peerMu before this callback
// acquires it, but the callback would still proceed to perform
// fanoutAnnounceRoutes / TriggerUpdate / poisonReverseToOtherPeers
// against a Service whose routing-layer subsystems may already be
// tearing down. The pending entry doubles as the live-or-not flag
// for the timer-fired path.
func (s *Service) firePendingWithdrawal(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	s.peerMu.Lock()
	if _, ok := s.pendingWithdrawals[peerIdentity]; !ok {
		s.peerMu.Unlock()
		log.Debug().
			Str("peer", string(peerIdentity)).
			Msg("routing_withdrawal_grace_callback_no_entry — cancelled by shutdown/tryCancel race; nothing to do")
		return
	}
	delete(s.pendingWithdrawals, peerIdentity)
	s.peerMu.Unlock()

	log.Info().
		Str("peer", string(peerIdentity)).
		Msg("routing_direct_peer_withdrawal_grace_elapsed")
	s.executeDeferredWithdrawal(peerIdentity, caps)
}

// tryCancelPendingWithdrawal is called from onPeerSessionEstablished
// (firstRelay branch). Returns true if a pending withdrawal was
// found and the route should stay in the table — the caller MUST
// then short-circuit and NOT call AddDirectPeer/MarkReconnected/
// TriggerUpdate, because the route was never removed.
//
// Cancellation contract:
//
//	The atomic claim (delete from s.pendingWithdrawals under peerMu)
//	is what actually cancels the withdrawal — NOT timer.Stop. The
//	timer callback (firePendingWithdrawal) re-checks the map under
//	peerMu at the top and aborts when the entry is absent. So once
//	we have observed ok==true and called delete(...) under peerMu,
//	the deferred body WILL NOT run, regardless of timer.Stop's
//	return value:
//
//	  - Stop() == true: timer was still queued; we cancelled it
//	    cleanly.
//	  - Stop() == false: timer already fired and the callback is
//	    either (a) blocked on peerMu waiting for us, or (b) past
//	    its absent-entry guard. In case (a), the callback acquires
//	    the lock after us, finds no entry, logs the cancel-by-race
//	    branch and returns — RemoveDirectPeer is NOT called. Case
//	    (b) means the callback had already deleted the entry before
//	    we tried to claim it, so ok would have been false above and
//	    we would have returned false to the caller.
//
//	Therefore: returning true purely on the basis of the atomic
//	claim is CORRECT. The previous code returned false on Stop()
//	== false, sending the caller into AddDirectPeer for a route
//	that was never removed — a needless seqno bump + wire fanout
//	for a still-present direct route, plus MarkReconnected
//	disrupting the announce loop's view of a peer that never
//	actually disconnected.
func (s *Service) tryCancelPendingWithdrawal(peerIdentity domain.PeerIdentity) bool {
	s.peerMu.Lock()
	entry, ok := s.pendingWithdrawals[peerIdentity]
	if !ok {
		s.peerMu.Unlock()
		return false
	}
	delete(s.pendingWithdrawals, peerIdentity)
	s.peerMu.Unlock()

	// Best-effort Stop: if true, frees the timer immediately; if
	// false, the callback's absent-entry guard above already covers
	// us. Either way we have already claimed the cancel — never
	// fall through to AddDirectPeer for a still-present route.
	stopped := entry.timer.Stop()
	log.Info().
		Str("peer", string(peerIdentity)).
		Bool("timer_stopped", stopped).
		Msg("routing_direct_peer_reconnect_within_grace — withdrawal cancelled, route stays")
	return true
}

// cancelAllPendingWithdrawalsForShutdown stops every pending timer
// and clears the map. Called from Service shutdown so timers do not
// fire against a stopped Service. The withdrawals that would have
// fired are NOT executed — clean shutdown owns its own broadcast
// path (closeAllInboundConns / etc) and these probation entries
// are discarded as part of shutdown.
func (s *Service) cancelAllPendingWithdrawalsForShutdown() {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	for peer, entry := range s.pendingWithdrawals {
		entry.timer.Stop()
		delete(s.pendingWithdrawals, peer)
	}
}

// executeDeferredWithdrawal is the body that was previously inline
// in onPeerSessionClosed lastRelay branch. Moved here so both the
// timer callback (firePendingWithdrawal) and the synchronous legacy
// path (when grace == 0, from maybeScheduleDeferredWithdrawal) call
// the same code with the same semantics.
//
// caps is captured at close time and matches what
// onPeerSessionClosed received — so MarkDisconnected sees the same
// capability set the matching MarkReconnected was called with.
//
// Ownership contract:
//
//	The CALLER owns claiming and deleting the pendingWithdrawals
//	entry (firePendingWithdrawal does so atomically under peerMu;
//	the sync path never creates one). This helper only consults
//	peerMu to re-check session counters and to serialise the
//	routingTable mutation against a concurrent
//	onPeerSessionEstablished. It MUST NOT touch pendingWithdrawals
//	because (a) the timer-fire path already deleted it, and (b)
//	the sync path never had one to begin with.
//
// Atomicity vs concurrent reconnect:
//
//	The active-session re-check and routingTable.RemoveDirectPeer
//	run inside a SINGLE peerMu.Lock critical section. A concurrent
//	onPeerSessionEstablished is forced to serialise on the same
//	peerMu — it either:
//	  (a) acquires the lock FIRST: bumps identityRelaySessions[peer],
//	      releases the lock. When we then acquire the lock we see
//	      identityRelaySessions[peer] > 0 and abort the withdrawal
//	      — the route stays in the table.
//	  (b) acquires the lock AFTER us: when our critical section
//	      releases, RemoveDirectPeer has already committed.
//	      onPeerSessionEstablished sees no pending entry (claim
//	      happened in firePendingWithdrawal), takes the normal
//	      AddDirectPeer path, and the route is added back fresh.
//
//	Lock-order: peerMu → routingTable.mu. Safe because the routing
//	package never reaches back into peerMu (see
//	internal/core/routing/).
func (s *Service) executeDeferredWithdrawal(peerIdentity domain.PeerIdentity, caps []domain.Capability) {
	s.peerMu.Lock()

	// Re-check: did a new RELAY-capable session for this identity
	// appear between the timer scheduling and the callback running?
	// Direct route is established (and meaningful) only while a
	// relay-capable session exists — see onPeerSessionEstablished's
	// firstRelay branch. A non-relay session that survives is
	// irrelevant to the direct-route lifecycle, so checking
	// identitySessions (the total counter) would wrongly abort the
	// withdrawal when only a legacy/non-relay session remains and
	// leave a dangling direct route forever.
	if s.identityRelaySessions[peerIdentity] > 0 {
		s.peerMu.Unlock()
		log.Info().
			Str("peer", string(peerIdentity)).
			Msg("routing_withdrawal_aborted_peer_reconnected")
		return
	}

	// Defensive: minimal Service fixtures may skip routingTable
	// init. Caller (firePendingWithdrawal or the synchronous
	// maybeScheduleDeferredWithdrawal path) owns pendingWithdrawals
	// cleanup; we just exit.
	if s.routingTable == nil {
		s.peerMu.Unlock()
		return
	}

	// Phase 4 13.3-B: snapshot transit identities BEFORE
	// RemoveDirectPeer invalidates storage (see original inline
	// comment in onPeerSessionClosed for the full rationale).
	poisonTargets := s.routingTable.IdentitiesViaUplink(routing.PeerIdentity(peerIdentity))

	result, err := s.routingTable.RemoveDirectPeer(peerIdentity)
	s.peerMu.Unlock()

	if err != nil {
		log.Error().Err(err).Str("peer", string(peerIdentity)).Msg("routing_remove_direct_peer_failed")
		return
	}

	if s.announceLoop != nil {
		s.announceLoop.StateRegistry().MarkDisconnected(peerIdentity)
	}

	var sent, dropped int
	if len(result.Withdrawals) > 0 {
		sent, dropped = s.fanoutAnnounceRoutes(s.runCtx, s.routingCapablePeers(), result.Withdrawals)
	}

	log.Info().
		Str("peer", string(peerIdentity)).
		Int("withdrawals", len(result.Withdrawals)).
		Int("transit_invalidated", result.TransitInvalidated).
		Int("fanout_sent", sent).
		Int("fanout_dropped", dropped).
		Msg("routing_direct_peer_removed")

	if s.eventBus != nil {
		s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
			Reason:    domain.RouteChangeDirectPeerRemoved,
			PeerID:    peerIdentity,
			Withdrawn: len(result.Withdrawals),
		})
	}

	if s.announceLoop != nil {
		s.announceLoop.TriggerUpdate()
	}
	s.triggerDrainForExposed(result.ExposedBackups)

	if len(poisonTargets) > 0 {
		s.poisonReverseToOtherPeers(s.runCtx, peerIdentity, poisonTargets)
	}

	// Suppress unused-variable warning for caps — kept in the
	// signature because the future quarantine extension needs it
	// (cap-aware decisions about whether to emit withdrawal at all).
	_ = caps
}
