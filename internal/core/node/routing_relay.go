// routing_relay.go hosts the relay forwarding and gossip dispatch paths:
// tryForwardViaRoutingTable / sendTableDirectedRelay / sendRelayToAddress
// for table-directed relay forwarding; sendFrameToAddress as the unified
// inbound/outbound send dispatch; executeGossipTargets / routingTargets*
// for gossip fanout target selection; sendGossipFrameToPeer / gossipNotice /
// sendNoticeToPeer for push_message and push_notice delivery;
// resolveRouteNextHopAddress as the hop-role-aware resolver wrapper.
//
// This file also owns the three §4.4 bootstrap handshake raw-write edges
// inside sendNoticeToPeer (node-hello, auth challenge, challenge-response)
// and is therefore in the §2.9 net-import whitelist enforced by
// scripts/enforce-netcore-boundary.sh.
//
// This is the relay-plane counterpart to the announce-plane logic in
// routing_announce.go. Address↔identity resolution lives in
// routing_resolver.go, session-lifecycle hooks in routing_session.go,
// hop_ack route confirmation in routing_hop_ack.go, and event-driven
// pending-queue drain in routing_drain.go. The per-file scope table is
// the durable record — see the "internal/core/node/" section of the
// file map in docs/routing.md (both the English and Russian
// component-map sections).
package node

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// nextRelayShapingHint returns a monotonically advancing per-
// Service counter used as the `hint` argument to
// routing.Table.LookupForRelay. The hint feeds the Phase 3 PR
// 12.6 multi-path traffic shaping rotation: LookupForRelay
// reorders the ranked result roughly once every
// routing.ShapingProbeRatio calls (when the conditions for
// shaping are met). Keeping the counter on Service (rather than
// inside routing.Table) lets the table stay stateless w.r.t.
// shaping cadence — tests deterministically invoke
// LookupForRelay(identity, 0) for the rotation arm and any
// non-multiple-of-ratio hint for the plain-Lookup arm.
//
// Safe for concurrent callers: atomic.Uint64.Add is the only
// synchronisation needed.
func (s *Service) nextRelayShapingHint() uint64 {
	return s.relayShapingHint.Add(1)
}

// tableForwardResult describes the outcome of tryForwardViaRoutingTable.
type tableForwardResult struct {
	// Address is the transport address the frame was sent to. Empty if no
	// table route could be used.
	Address domain.PeerAddress

	// RouteOrigin is the Origin field from the RouteEntry that was selected.
	// Retained for plumbing-stability across the relay → hop_ack path, but
	// IGNORED by route promotion post-Phase-A: confirmRouteViaHopAck matches
	// on (Identity, NextHop) only because the routing table no longer keys
	// on Origin. See routing_hop_ack.go for the migration note.
	RouteOrigin domain.PeerIdentity
}

// onRelayHopAckTimeout is the Phase 3 PR 12.2 callback fired by the
// relay state store's TTL ticker when a forwarded relay_message's
// hop-ack budget (defaultHopAckBudgetSeconds) elapses without a
// matching relay_hop_ack arrival. It resolves the abandoned
// next-hop's transport address to its peer identity and records the
// failure on the per-(Identity, Uplink) reputation primitive via
// routing.Table.MarkHopFailure (PR 12.1 reputation surface).
//
// The state argument is a value copy snapshotted by
// relayStateStore.tickHopAckBudgets BEFORE the callback fired; no
// mutation here propagates back to the live store entry, and the
// store's HopAckObserved flag has already been flipped to true so a
// late relay_hop_ack arriving after this callback still runs through
// handleRelayHopAck → confirmRouteViaHopAck (which calls
// applyHopAckSuccess and clears the cooldown the failure may have
// armed). See handleRelayHopAck's doc-comment for the recovery
// contract.
//
// Empty ForwardedTo means there was no downstream hop to ack
// (final-recipient path, stored-locally fallback) — those entries
// must NOT have a tracked budget in the first place
// (HopAckRemainingTicks=0 keeps the ticker from arming them), so a
// callback firing with ForwardedTo="" indicates a logic bug; we
// defensively no-op rather than fire a phantom MarkHopFailure on a
// resolved identity that would be empty anyway.
//
// Identity resolution mirrors confirmRouteViaHopAck: ask the peer
// registry first, fall back to treating the address as an identity
// only when the session has already torn down. An empty resolved
// identity means we have no way to attribute the failure to a routing
// pair, so the callback is a no-op for that case as well — the
// reputation primitive would refuse it via the
// Table.MarkHopFailure(identity="", ...) guard anyway, but bailing
// out here keeps the structured log honest.
//
// Lock contract: this method takes routing.Table.mu via
// MarkHopFailure (briefly, W mode). The store-side caller already
// released relayStateStore.mu BEFORE invoking us (see
// relayStateStore.ttlTicker), so the two mutexes never nest.
func (s *Service) onRelayHopAckTimeout(state relayForwardState) {
	if state.ForwardedTo == "" {
		// Defensive: see doc-comment above. A state with empty
		// ForwardedTo should never have a non-zero
		// HopAckRemainingTicks, so reaching this branch is a sign
		// the call-site stamping logic drifted. Log so we notice.
		log.Debug().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Msg("relay_hop_ack_timeout_empty_forwarded_to")
		return
	}
	if state.Recipient == "" {
		log.Debug().
			Str("id", state.MessageID).
			Str("forwarded_to", string(state.ForwardedTo)).
			Msg("relay_hop_ack_timeout_empty_recipient")
		return
	}

	nextHopIdentity := s.resolvePeerIdentity(state.ForwardedTo)
	if nextHopIdentity == "" {
		// Session may have closed between forward and timeout —
		// resolvePeerIdentity returned "". Treat the address as
		// the identity directly only when it parses as one; the
		// "inbound:..." form would fail MarkHopFailure's empty
		// check anyway. We log either way so the operator sees a
		// "we timed out but couldn't blame anyone" trail.
		nextHopIdentity = domain.PeerIdentity(state.ForwardedTo)
		if nextHopIdentity == "" {
			log.Debug().
				Str("id", state.MessageID).
				Str("recipient", string(state.Recipient)).
				Str("forwarded_to", string(state.ForwardedTo)).
				Msg("relay_hop_ack_timeout_unresolved_uplink")
			return
		}
	}

	s.routingTable.MarkHopFailure(state.Recipient, nextHopIdentity)

	log.Debug().
		Str("id", state.MessageID).
		Str("recipient", string(state.Recipient)).
		Str("next_hop", string(nextHopIdentity)).
		Int("budget_seconds", defaultHopAckBudgetSeconds).
		Msg("routing_hop_ack_timeout_uplink_marked_failure")

	// Phase 3 PR 12.3 — multi-path failover. Try to re-send the
	// original frame through an alternative uplink before falling
	// back to the existing periodic retryRelayDeliveries / gossip
	// path. Bounded by MaxFailoverRetries; respects the Phase 0
	// overload gate so a node already shedding work does not
	// amplify outbound traffic.
	s.tryFailoverRelay(state, nextHopIdentity)
}

// tryFailoverRelay is the Phase 3 PR 12.3 multi-path failover step
// fired from onRelayHopAckTimeout after the reputation primitive
// records the hop-ack failure. It re-sends the same relay_message
// frame (snapshotted on relayForwardState.FrameLine at original
// forward time) through the highest-ranked alternative uplink that
// is NOT the just-failed peer, NOT the previous-hop incoming
// sender (split horizon), and NOT in the AbandonedForwardedTo list
// (already-tried retries).
//
// Bounded by MaxFailoverRetries (=1) so the per-message latency
// budget stays predictable: at most one extra hop-ack budget
// window (defaultHopAckBudgetSeconds) before gossip fallback or
// periodic retryRelayDeliveries takes over. Each successful retry
// advances RetryAttempt by exactly one via
// relayStateStore.recordFailoverRetry, which also re-arms the
// hop-ack budget and clears HopAckObserved so the new uplink gets
// a fresh deadline.
//
// Skipped silently (with a structured log) when:
//
//   - FrameLine is empty (origin/forward path failed to marshal
//     the frame at stamp time, or this is a final-recipient /
//     stored-locally entry that never had a downstream forward to
//     retry).
//   - RetryAttempt has reached MaxFailoverRetries.
//   - The Phase 0 overload gate is engaged — symmetric to the
//     probe loop's overload skip (Phase 2 §4.7 decision #4).
//   - The frame line fails to parse back to a protocol.Frame
//     (defensive guard; should not happen for our own marshal
//     output but stays safe under future format changes).
//   - Table.Lookup returns no alternative uplinks after the
//     skip-failed / split-horizon / abandoned filters.
//
// Lock contract: the caller (onRelayHopAckTimeout) is invoked from
// the relay state TTL ticker AFTER rs.mu has been released. This
// function acquires routing.Table.mu briefly via Lookup (R mode)
// and the relay state store rs.mu separately via
// retryAttemptCountFor / recordFailoverRetry (W mode). The two
// mutexes are never held simultaneously.
func (s *Service) tryFailoverRelay(state relayForwardState, failedUplink domain.PeerIdentity) {
	if state.FrameLine == "" {
		log.Debug().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Msg("relay_failover_skipped_no_frame")
		return
	}
	// Re-read RetryAttempt from the live store — the snapshot the
	// ticker captured is stale once we cross the rs.mu boundary,
	// and a concurrent recordFailoverRetry from a different
	// hop-ack-timeout fire (different MessageID, same address)
	// could have advanced our limit while we were not looking.
	currentAttempt, present := s.relayStates.retryAttemptCountFor(state.MessageID)
	if !present {
		// TTL eviction beat us between callback fire and now.
		return
	}
	if currentAttempt >= MaxFailoverRetries {
		log.Debug().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Int("retry_attempt", currentAttempt).
			Int("max", MaxFailoverRetries).
			Msg("relay_failover_skipped_max_retries")
		// Phase 3 PR 12.3 (review fix): retry budget exhausted is
		// the same "we cannot reach the recipient via any table
		// route" terminal state as the no-alternative branch below.
		// Without gossip fallback an intermediate-hop message that
		// burned its retry budget would just stop — the periodic
		// retryRelayDeliveries path holds no FrameLine for intermediate
		// hops and cannot rescue it. Fire gossip on the stashed wire
		// bytes so receivers reachable via the broader routing-target
		// fan-out still have a chance to deliver.
		s.failoverGossipFallback(state, failedUplink)
		return
	}
	if s.overloadMonitor != nil && s.overloadMonitor.IsOverloaded() {
		// Symmetric to probeTick — under overload we skip
		// active retries and let the slower passive timelines
		// converge. Phase 0 overload gate documented in
		// docs/cluster-mesh-architecture-plan.md §0.2.
		log.Debug().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Msg("relay_failover_skipped_overload")
		return
	}

	frame, err := protocol.ParseFrameLine(state.FrameLine)
	if err != nil {
		log.Warn().
			Err(err).
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Msg("relay_failover_skipped_unparseable_frame")
		return
	}

	// Resolve the previous-hop incoming sender so the split-
	// horizon skip applies by identity (a peer may have multiple
	// transport sessions). state.PreviousHop is empty on the
	// origin path (we created the message ourselves), in which
	// case there is no incoming-sender identity to skip.
	previousHopIdentity := domain.PeerIdentity("")
	if state.PreviousHop != "" {
		previousHopIdentity = s.resolvePeerIdentity(state.PreviousHop)
	}

	// Resolve abandoned addresses to identities once so the
	// per-route loop below stays O(routes × abandoned) without
	// re-resolving on each comparison.
	abandonedIdentities := make(map[domain.PeerIdentity]struct{}, len(state.AbandonedForwardedTo))
	for _, addr := range state.AbandonedForwardedTo {
		if id := s.resolvePeerIdentity(addr); id != "" {
			abandonedIdentities[id] = struct{}{}
		}
	}

	// Lookup returns CompositeScore-ranked entries with HealthDead
	// and (PR 12.4 wire-up) CooldownUntil already filtered out.
	// The failover path therefore picks the next best uplink
	// without having to re-implement the scoring logic.
	routes := s.routingTable.Lookup(state.Recipient)
	for _, route := range routes {
		if route.NextHop == failedUplink {
			continue
		}
		if previousHopIdentity != "" && route.NextHop == previousHopIdentity {
			continue
		}
		if _, abandoned := abandonedIdentities[route.NextHop]; abandoned {
			continue
		}
		// Route quarantine: skip transit through quarantined peers
		// (direct destinations pass — see routeIsBlockedByQuarantine).
		// Without this gate the failover path could re-pick a peer
		// we already decided not to trust as transit and undo the
		// "transit through P skipped" contract that the normal
		// forwarding and TableRouter paths uphold.
		if s.routeIsBlockedByQuarantine(route.NextHop, route.Hops) {
			log.Debug().
				Str("id", state.MessageID).
				Str("recipient", string(state.Recipient)).
				Str("next_hop", string(route.NextHop)).
				Int("hops", route.Hops).
				Msg("relay_failover_skip_quarantined_transit_next_hop")
			continue
		}
		address := s.resolveRouteNextHopAddress(route.NextHop, route.Hops)
		if address == "" {
			continue
		}
		if !s.sendFrameToAddress(s.runCtx, address, frame) {
			continue
		}
		if !s.relayStates.recordFailoverRetry(state.MessageID, address) {
			// TTL evicted between send and bookkeeping. The
			// frame is already on the wire; the receiver will
			// dedupe by ID if the original arrived too.
			log.Debug().
				Str("id", state.MessageID).
				Str("recipient", string(state.Recipient)).
				Msg("relay_failover_state_evicted_after_send")
			return
		}
		log.Info().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Str("failed_uplink", string(failedUplink)).
			Str("retry_uplink", string(route.NextHop)).
			Str("retry_address", string(address)).
			Int("retry_attempt", currentAttempt+1).
			Int("hops", route.Hops).
			Msg("relay_failover_retried")
		return
	}

	log.Debug().
		Str("id", state.MessageID).
		Str("recipient", string(state.Recipient)).
		Str("failed_uplink", string(failedUplink)).
		Int("candidates_considered", len(routes)).
		Msg("relay_failover_no_alternative")
	// Phase 3 PR 12.3 (review fix): no table alternative AND we
	// already used FrameLine to know the wire bytes. Fire gossip
	// fallback on those bytes so the message has one more chance
	// to reach the recipient through the broader routing-target
	// fan-out. retryRelayDeliveries does NOT rescue intermediate-
	// hop messages (no FrameLine in topics["dm"]), so without
	// this fallback the message would simply stop here.
	s.failoverGossipFallback(state, failedUplink)
}

// failoverGossipFallback parses the stashed FrameLine and fans the
// frame out through relayViaGossip with the failed uplink's transport
// address as the exclude. Used by tryFailoverRelay's terminal branches
// (retry budget exhausted, no table alternative) to give the message
// one final chance via the broader routing-target set before it
// genuinely stops.
//
// Receivers dedupe by message ID, so any peer that may already have
// received the original forward simply drops the gossip copy. The
// exclude is best-effort: relayViaGossip takes a single exclude
// address, so abandoned uplinks earlier in the failover chain may
// still receive a gossip copy and dedupe it.
//
// No-op when FrameLine is empty (origin marshal-failure path) or the
// parse fails — the message has nothing recoverable to gossip.
func (s *Service) failoverGossipFallback(state relayForwardState, failedUplink domain.PeerIdentity) {
	if state.FrameLine == "" {
		return
	}
	frame, err := protocol.ParseFrameLine(state.FrameLine)
	if err != nil {
		log.Debug().
			Err(err).
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Msg("relay_failover_gossip_skipped_unparseable_frame")
		return
	}
	// Gossip exclude follows the canonical relay split-horizon: on
	// an intermediate hop the message arrived FROM PreviousHop, so
	// gossiping back to them would loop (and they already have the
	// message). relayViaGossip takes a single exclude, so we pass
	// PreviousHop when set. On the origin path (PreviousHop empty —
	// we created the message) there is no upstream to protect, so
	// we exclude the just-failed downstream instead to avoid an
	// immediate pointless re-send to the peer that just timed out.
	// The failed downstream may still be reached via gossip on the
	// intermediate-hop branch — that is acceptable: the timeout
	// could have been transient and the receiver dedupes by
	// message ID regardless.
	excludeAddr := state.PreviousHop
	if excludeAddr == "" {
		excludeAddr = state.ForwardedTo
	}
	forwardedTo := s.relayViaGossip(frame, excludeAddr)
	if forwardedTo == "" {
		log.Debug().
			Str("id", state.MessageID).
			Str("recipient", string(state.Recipient)).
			Str("failed_uplink", string(failedUplink)).
			Msg("relay_failover_gossip_no_candidates")
		return
	}
	log.Info().
		Str("id", state.MessageID).
		Str("recipient", string(state.Recipient)).
		Str("failed_uplink", string(failedUplink)).
		Str("gossip_first_target", string(forwardedTo)).
		Msg("relay_failover_gossip_fired")
}

// tryForwardViaRoutingTable consults the routing table for a next-hop to
// the given recipient and tries to forward the relay frame there. Returns
// a tableForwardResult with the transport address and the route origin from
// the selected entry. Called from intermediate relay hops (handleRelayMessage)
// to enable multi-hop table-directed relay chains.
//
// excludeIdentity is the identity of the peer that sent the relay to us —
// we must not send back to them (split horizon on relay path).
//
// ctx is the caller's request/cycle context. It is threaded into
// sendFrameToAddress so that a pre-cancelled or mid-flight cancelled ctx
// aborts the inbound sync-flush wait instead of letting the NetCore writer
// burn the full syncFlushTimeout. Outbound enqueue is non-blocking and
// does not observe ctx beyond the pre-entry check.
func (s *Service) tryForwardViaRoutingTable(ctx context.Context, recipient domain.PeerIdentity, frame protocol.Frame, excludeIdentity domain.PeerIdentity) tableForwardResult {
	// Phase 3 PR 12.6 — LookupForRelay applies the multi-path
	// traffic shaping rotation on top of the standard ranked
	// Lookup result. The plain Lookup ordering still applies
	// to most relay forwards; only roughly one in
	// routing.ShapingProbeRatio calls rotate when the top
	// candidate qualifies as a shaping candidate AND a non-
	// shaping alternative is available. See LookupForRelay's
	// doc-comment for the precise contract.
	routes := s.routingTable.LookupForRelay(recipient, s.nextRelayShapingHint())
	if len(routes) == 0 {
		// Phase 2 on-demand recovery: nudge route discovery for
		// recipient. Per-target rate limit inside SendRouteQuery
		// caps the fan-out at 3 emissions per 30 s.
		s.triggerRouteQueryAsync(recipient)
		return tableForwardResult{}
	}

	for _, route := range routes {
		// Don't send back to where it came from.
		if route.NextHop == excludeIdentity {
			continue
		}
		// Route quarantine: skip transit through quarantined peers
		// (direct destinations pass — see routeIsBlockedByQuarantine).
		if s.routeIsBlockedByQuarantine(route.NextHop, route.Hops) {
			log.Debug().
				Str("id", frame.ID).
				Str("recipient", string(recipient)).
				Str("next_hop", string(route.NextHop)).
				Int("hops", route.Hops).
				Msg("relay_forward_skip_quarantined_transit_next_hop")
			continue
		}
		// Direct destination (hops=1) only needs relay cap; transit needs both.
		address := s.resolveRouteNextHopAddress(route.NextHop, route.Hops)
		if address == "" {
			continue
		}
		if s.sendFrameToAddress(ctx, address, frame) {
			log.Debug().
				Str("id", frame.ID).
				Str("recipient", string(recipient)).
				Str("next_hop", string(route.NextHop)).
				Str("address", string(address)).
				Str("origin", string(route.Origin)).
				Int("hops", route.Hops).
				Msg("relay_forward_via_routing_table")
			// PR 11.35 P2 — fast recovery when the selected route
			// is HealthBad. Lookup is CompositeScore-ranked with
			// Dead filtered (Direct exempt), so the top candidate
			// being Bad means every alternative is also Bad or
			// worse. Fire route_query in the background to
			// discover fresher uplinks; the send through the Bad
			// route still happened above as best-effort. Without
			// this signal the relay path would wait for a Dead
			// transition or send failure before triggering
			// recovery, prolonging the unhealthy window.
			// triggerRouteQueryAsync is rate-limited and
			// spawn-throttled so this is safe under sustained
			// traffic — see TableRouter.Route for the symmetric
			// trigger on the directed-relay path.
			if health, tracked := s.routingTable.HealthFor(recipient, route.NextHop); tracked && health == routing.HealthBad {
				s.triggerRouteQueryAsync(recipient)
			}
			return tableForwardResult{Address: address, RouteOrigin: route.Origin}
		}
	}

	// All routes exhausted without a successful send — nudge
	// route discovery before falling back to gossip. Phase 2
	// on-demand recovery (overview §7.5).
	s.triggerRouteQueryAsync(recipient)
	return tableForwardResult{}
}

// sendFrameToAddress sends a protocol frame to the given address, handling
// both outbound sessions (plain address) and inbound connections ("inbound:"
// prefixed key). This is the unified send dispatch for all table-directed
// relay and forwarding paths.
//
// ctx is propagated to the inbound sync-write path so cancellation from
// request-scoped contexts actually interrupts the send wait. Outbound
// enqueue is non-blocking and does not wait on a transport flush.
func (s *Service) sendFrameToAddress(ctx context.Context, address domain.PeerAddress, frame protocol.Frame) bool {
	if strings.HasPrefix(string(address), "inbound:") {
		return s.writeFrameToInbound(ctx, address, frame)
	}
	return s.enqueuePeerFrame(address, frame)
}

// sendTableDirectedRelay sends a relay_message to the table-selected next-hop.
// The validatedAddress was obtained from TableRouter at route-decision time
// and already had the correct capability check (relay-only for destinations,
// both caps for transit). Using it directly avoids re-resolution, which
// could pick a different session for the same identity with weaker capabilities.
//
// routeOrigin is the Origin field from the selected RouteEntry. It is
// retained on the call signature for caller-stability — relayForwardState
// still stores it and the hop_ack confirmation path still receives it —
// but it is IGNORED for route promotion post-Phase-A. The routing table
// no longer keys on Origin (per-(Identity, Uplink) storage), so
// confirmRouteViaHopAck matches on (Identity, NextHop) only. See
// routing_hop_ack.go for the migration note.
//
// nextHopHops is the hop count from the selected RouteEntry. Used by the
// retry path to re-resolve with correct capability requirements: hops=1
// (destination) needs only relay cap; hops>1 (transit) needs both caps.
//
// If validatedAddress is empty (e.g., retry path without cached address),
// falls back to resolveRouteNextHopAddress with the original hop role.
//
// Handles both outbound sessions and inbound connections ("inbound:" prefix).
//
// ctx is the caller's request/cycle context and is propagated to
// sendRelayToAddress so the inbound sync-flush wait honours cancellation.
func (s *Service) sendTableDirectedRelay(ctx context.Context, msg protocol.Envelope, nextHopIdentity domain.PeerIdentity, validatedAddress domain.PeerAddress, routeOrigin domain.PeerIdentity, nextHopHops int) {
	address := validatedAddress
	if address == "" {
		// Retry path or caller without cached address — re-resolve with
		// the correct capability requirements for the hop role.
		address = s.resolveRouteNextHopAddress(nextHopIdentity, nextHopHops)
	}

	if address == "" {
		// Session gone — gossip fallback. Same propagation gates as
		// every other blind path (transit_retention.go): hop budget
		// and ingress suppression apply, or an exhausted/echoing
		// message sneaks out through this side door.
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", string(nextHopIdentity)).
			Msg("table_relay_no_session_fallback_gossip")
		targets := s.filterGossipTargetsForEnvelope(msg, s.routingTargetsForMessage(msg))
		s.tryRelayToCapableFullNodes(msg, targets)
		return
	}

	// Ingress suppression for the DIRECTED path: when the routing table
	// says "recipient via B" and the message arrived FROM B, sending a
	// relay_message back to B is a guaranteed no-op round trip — B has
	// already seen the message (its dedup rejects the store) and made
	// its own delivery decision. isIngressNextHop checks the
	// authenticated identity first (inbound-only routes are keyed
	// "inbound:<raw>" and defeat address canonicalization), then falls
	// back to the canonical-address comparison. Fall back to
	// (Via-filtered) gossip instead of echoing; covers both the
	// admission call and every 2s retry.
	if s.isIngressNextHop(msg, nextHopIdentity, address) {
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", string(nextHopIdentity)).
			Str("address", string(address)).
			Msg("table_relay_ingress_suppressed_fallback_gossip")
		targets := s.filterGossipTargetsForEnvelope(msg, s.routingTargetsForMessage(msg))
		s.tryRelayToCapableFullNodes(msg, targets)
		return
	}

	if !s.sendRelayToAddress(ctx, address, msg, routeOrigin) {
		// Send failed — gossip fallback (same gates as above).
		log.Debug().
			Str("recipient", msg.Recipient).
			Str("next_hop", string(nextHopIdentity)).
			Str("address", string(address)).
			Msg("table_relay_send_failed_fallback_gossip")
		targets := s.filterGossipTargetsForEnvelope(msg, s.routingTargetsForMessage(msg))
		s.tryRelayToCapableFullNodes(msg, targets)
		return
	}

	log.Info().
		Str("recipient", msg.Recipient).
		Str("next_hop", string(nextHopIdentity)).
		Str("address", string(address)).
		Msg("table_relay_sent")
}

// sendRelayToAddress sends a relay_message to the given address, handling
// both outbound sessions and inbound connections. For outbound sessions it
// delegates to sendRelayMessage (which persists relayForwardState internally).
// For inbound connections it builds the frame, writes via writeFrameToInbound,
// and persists relayForwardState explicitly so that hop_ack and receipt
// routing work identically regardless of connection direction.
//
// routeOrigin is the Origin field from the routing decision. Stored in
// relayForwardState for plumbing-stability; IGNORED by post-Phase-A
// hop_ack promotion, which matches on (Identity, NextHop) only. See
// routing_hop_ack.go for the migration note.
//
// ctx is the caller's request/cycle context and is propagated into
// writeFrameToInbound so the inbound sync-flush wait honours cancellation.
// Outbound enqueue is non-blocking and does not observe ctx beyond the
// pre-entry check inside the network bridge.
func (s *Service) sendRelayToAddress(ctx context.Context, address domain.PeerAddress, msg protocol.Envelope, routeOrigin domain.PeerIdentity) bool {
	if strings.HasPrefix(string(address), "inbound:") {
		frame := protocol.Frame{
			Type:        "relay_message",
			ID:          string(msg.ID),
			Address:     msg.Sender,
			Recipient:   msg.Recipient,
			Topic:       msg.Topic,
			Flag:        string(msg.Flag),
			CreatedAt:   msg.CreatedAt.UTC().Format(time.RFC3339),
			TTLSeconds:  msg.TTLSeconds,
			Body:        string(msg.Payload),
			HopCount:    1,
			MaxHops:     defaultMaxHops,
			PreviousHop: s.identity.Address,
		}
		if !s.writeFrameToInbound(ctx, address, frame) {
			return false
		}
		// Phase 3 PR 12.2 / PR 12.3 (review fix) — symmetric with
		// sendRelayMessage{,WithOrigin} on the outbound side: arm
		// the hop-ack budget AND stash the wire bytes so
		// onRelayHopAckTimeout's reputation + failover path treats
		// inbound-directed origin relays the same as their outbound
		// counterparts. Without these stamps a relay forwarded
		// through an inbound-only peer would never enter Phase 3
		// timeout/failover at all — the ticker would skip the
		// entry (HopAckRemainingTicks=0 = not tracking) and the
		// failover gossip fallback would have no FrameLine to
		// re-emit.
		originLine, marshalErr := protocol.MarshalFrameLine(frame)
		if marshalErr != nil {
			originLine = ""
			log.Warn().
				Err(marshalErr).
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("address", string(address)).
				Msg("relay_origin_frame_marshal_failed_inbound_retry_disabled")
		}
		// Persist relay state — mirrors what sendRelayMessage does for outbound.
		stored := s.relayStates.store(&relayForwardState{
			MessageID:            string(msg.ID),
			PreviousHop:          "",
			ReceiptForwardTo:     "",
			ForwardedTo:          address,
			Recipient:            domain.PeerIdentity(msg.Recipient),
			RouteOrigin:          routeOrigin,
			HopCount:             1,
			RemainingTTL:         relayStateTTLSeconds,
			HopAckRemainingTicks: defaultHopAckBudgetSeconds,
			FrameLine:            originLine,
		})
		if !stored {
			log.Warn().
				Str("id", string(msg.ID)).
				Str("recipient", msg.Recipient).
				Str("address", string(address)).
				Msg("relay_state_store_failed_inbound")
			return false
		}
		s.persistRelayState()
		return true
	}
	return s.sendRelayMessageWithOrigin(address, msg, routeOrigin)
}

// resolveRouteNextHopAddress finds a transport address for a route's
// next-hop, applying the right capability check based on hop count:
//   - hops == 1 (direct peer, destination): only mesh_relay_v1 required
//   - hops >  1 (transit node): both mesh_relay_v1 and mesh_routing_v1
//
// This is the unified resolver used by TableRouter and tryForwardViaRoutingTable.
// Returns outbound session address or "inbound:" prefixed key.
func (s *Service) resolveRouteNextHopAddress(peerIdentity domain.PeerIdentity, hops int) domain.PeerAddress {
	if hops <= 1 {
		return s.resolveRelayAddress(peerIdentity)
	}
	return s.resolveRoutableAddress(peerIdentity)
}

// executeGossipTargets sends a message to pre-computed gossip targets from a
// RoutingDecision. The target list is provided by the Router — targets are not
// recomputed here.
func (s *Service) executeGossipTargets(msg protocol.Envelope, targets []domain.PeerAddress) {
	defer crashlog.DeferRecover()
	if len(targets) == 0 {
		// Hop budget exhausted / all targets ingress-suppressed
		// (filterGossipTargetsForEnvelope) — nothing to build.
		return
	}
	// Build the push_message frame ONCE and share it (read-only) across every
	// gossip target instead of rebuilding it — and re-copying the ciphertext
	// into a fresh MessageFrame — per target. Gossip fan-out recreating the
	// identical frame N times was the top allocation-churn source in profiling
	// (the per-target sends). The frame value is copied at each channel send /
	// call, so only the *MessageFrame is shared, and it is never mutated on any
	// send path (session marshal, queue store, inbound write are all reads).
	frame := gossipPushFrame(msg)
	msgID := string(msg.ID)
	recipient := msg.Recipient
	for _, address := range targets {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		s.dispatchGossipSend(func() { s.sendGossipFrameToPeer(address, frame, msgID, recipient) })
	}
}

// gossipPushFrame builds the push_message wire frame for a gossiped DM. The
// returned frame is safe to share read-only across the per-target send
// goroutines (its *MessageFrame is never mutated downstream).
func gossipPushFrame(msg protocol.Envelope) protocol.Frame {
	// Hop budget on the wire: wireHops (transit_retention.go) resolves
	// the budget THIS node may stamp — decremented at admission for
	// transit, full default for local sends and legacy pre-field
	// envelopes, floored at 1 because 0 is never emitted (an absent
	// omitempty field means "assign the FULL default as if originated
	// here" on the receiver, which would re-arm an exhausted budget).
	return protocol.Frame{
		Type:  "push_message",
		Topic: msg.Topic,
		Item: &protocol.MessageFrame{
			ID:         string(msg.ID),
			Sender:     msg.Sender,
			Recipient:  msg.Recipient,
			Flag:       string(msg.Flag),
			CreatedAt:  msg.CreatedAt.UTC().Format(time.RFC3339),
			TTLSeconds: msg.TTLSeconds,
			Hops:       wireHops(msg),
			Body:       string(msg.Payload),
		},
	}
}

func (s *Service) routingTargets() []domain.PeerAddress {
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, _ domain.PeerIdentity, _ time.Time) bool {
		return !peerType.IsClient()
	})
}

func (s *Service) routingTargetsForMessage(msg protocol.Envelope) []domain.PeerAddress {
	// Both data DMs ("dm") and control DMs (TopicControlDM) are
	// recipient-specific: their target set must include the recipient
	// even when the recipient is registered as a client peer (clients
	// are otherwise excluded from gossip targets to prevent them from
	// becoming relay hops). Without protocol.IsDMTopic here, control
	// DMs would silently fall back to s.routingTargets() (the
	// non-DM/broadcast set) and never reach a directly-connected
	// recipient client.
	if !protocol.IsDMTopic(msg.Topic) || msg.Recipient == "*" {
		return s.routingTargets()
	}
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity, now time.Time) bool {
		isRecipient := string(peerID) == msg.Recipient
		if peerType.IsClient() && !isRecipient {
			return false
		}
		// Route quarantine: skip a peer when it would be used as a
		// TRANSIT hop for someone else AND it is quarantined for a
		// transit-blocking reason (disconnect_storm /
		// setup_failure_cycle). A chatty_routes peer stays usable as
		// transit — see isPeerTransitQuarantinedLocked. The recipient
		// itself always stays addressable — quarantine suppresses our
		// trust in the peer's view of the network, not direct
		// delivery to that peer. Without this gate, the gossip
		// fallback would happily pick the peer and re-introduce the
		// transit path the table-routing gate (routeIsBlockedByQuarantine)
		// and failover gate just closed. See
		// docs/refactoring/route-withdrawal-grace-period.md.
		//
		// MUST use the lock-held helper (isPeerTransitQuarantinedLocked)
		// because routingTargetsFiltered already holds peerMu.RLock
		// when invoking this callback. Calling the public
		// IsPeerTransitQuarantined here would take peerMu.RLock a
		// second time on the same goroutine — Go's RWMutex is NOT
		// recursive and the second RLock deadlocks if a writer is
		// queued between the two acquisitions (writer starves the
		// new RLock; the goroutine still holds the outer one).
		if !isRecipient && s.isPeerTransitQuarantinedLocked(peerID, now) {
			return false
		}
		return true
	})
}

func (s *Service) routingTargetsForRecipient(recipient string) []domain.PeerAddress {
	return s.routingTargetsFiltered(func(_ domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity, now time.Time) bool {
		isRecipient := string(peerID) == recipient
		if peerType.IsClient() && !isRecipient {
			return false
		}
		// Route quarantine: same gate as routingTargetsForMessage.
		// The recipient stays reachable; a peer quarantined for a
		// transit-blocking reason (NOT chatty_routes) is excluded from
		// being a transit hop. See the comment in routingTargetsForMessage
		// above for why we MUST use isPeerTransitQuarantinedLocked here
		// rather than the public IsPeerTransitQuarantined.
		if !isRecipient && s.isPeerTransitQuarantinedLocked(peerID, now) {
			return false
		}
		return true
	})
}

// routingTargetsFiltered builds the gossip/relay fanout target set
// under peerMu.RLock. The `allow` callback is invoked WHILE the lock
// is held, so callbacks MUST NOT acquire peerMu (any flavor) again —
// Go's RWMutex is not recursive, and a writer queued between the
// outer and inner Lock attempts will deadlock both sides. `now` is
// passed in so callbacks can reuse the same wall-clock anchor the
// scoring loop uses (consistency) and so callers don't have to
// reach back into Service for time. Use the *Locked variants of any
// helper the callback needs (e.g. isPeerInRouteQuarantineLocked).
func (s *Service) routingTargetsFiltered(allow func(address domain.PeerAddress, peerType domain.NodeType, peerID domain.PeerIdentity, now time.Time) bool) []domain.PeerAddress {
	s.peerMu.RLock()
	now := time.Now().UTC()

	// Phase 1: collect scored session targets (preferred — health-checked,
	// low-latency delivery via enqueuePeerFrame).
	type scoredTarget struct {
		address domain.PeerAddress
		score   int64
	}
	sessionSeen := make(map[domain.PeerAddress]struct{}, len(s.sessions)*2)
	scored := make([]scoredTarget, 0, len(s.sessions))
	for address := range s.sessions {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		sessionSeen[address] = struct{}{}
		primaryAddr := s.resolveHealthAddress(address)
		// Also mark the canonical (primary) address as seen. When the
		// session connected via a fallback port (e.g. host:64646 instead of
		// host:7777), the session key and the canonical peer address differ.
		// Without this, Phase 2 treats the canonical peer as "no session"
		// and adds it as a pending target — wasting a fanout slot on the
		// same physical host that already has an active session.
		if primaryAddr != address {
			sessionSeen[primaryAddr] = struct{}{}
		}
		peerType := s.peerTypeForAddressLocked(primaryAddr)
		peerID := s.peerIDs[primaryAddr]
		if !allow(address, peerType, peerID, now) {
			continue
		}
		health := s.health[primaryAddr]
		if health == nil || !health.Connected {
			continue
		}
		if s.computePeerStateAtLocked(health, now) == peerStateStalled {
			continue
		}
		scored = append(scored, scoredTarget{
			address: address,
			score:   scorePeerTargetLocked(health),
		})
	}

	// maxTargets is the gossip fanout cap applied in Phase 3 when scored
	// session targets exist.
	const maxTargets = 3

	// Phase 2: collect non-session peers that pass the filter. These peers
	// don't have an active outbound session yet, but sendGossipFrameToPeer can
	// still reach them via queuePeerFrame → pending queue → drain on session
	// establishment. Without this, a message arriving before all sessions
	// are up gets gossipped only to existing sessions (possibly back to the
	// sender) and never reaches peers whose sessions haven't started yet.
	//
	// SKIP this entirely when the scored session targets already fill the
	// fanout cap: Phase 3 would discard every pending peer we collect here, so
	// peersSnapshotLocked() (a full peer-list copy) and the scan below are pure
	// waste on a connected node that steady-state has >= maxTargets healthy
	// sessions — profiling flagged both as top allocators. When there are NO
	// sessions (len(scored)==0 < maxTargets) Phase 2 still runs and returns the
	// full peer set uncapped (bootstrap).
	var pendingPeers []domain.PeerAddress
	if len(scored) < maxTargets {
		// Iterate s.peers directly under the RLock we already hold (this loop
		// runs BEFORE the RUnlock below) instead of peersSnapshotLocked()'s
		// defensive copy — the copy only exists for callers that iterate after
		// unlocking, and copying the whole peer list per call was a top
		// allocation source.
		for _, peer := range s.peers {
			if peer.Address == "" || s.isSelfAddress(peer.Address) {
				continue
			}
			if _, ok := sessionSeen[peer.Address]; ok {
				continue
			}
			// We keep peerMu.RLock held across this callback so that the
			// quarantine/identity/type lookups all use *Locked helpers
			// instead of re-taking RLock. The pre-quarantine version of
			// this loop called peerTypeForAddress/peerIdentityForAddress
			// (each of which takes peerMu.RLock internally) AFTER an
			// explicit RUnlock; that worked when the callback never
			// re-entered peerMu, but now the route-quarantine callback
			// needs to consult s.peerQuarantine, which is also guarded
			// by peerMu. Holding RLock through Phase 2 avoids both a
			// recursive-RLock path AND the small TOCTOU between phases.
			if !allow(peer.Address, s.peerTypeForAddressLocked(peer.Address), s.peerIDs[peer.Address], now) {
				continue
			}
			pendingPeers = append(pendingPeers, peer.Address)
		}
	}
	s.peerMu.RUnlock()

	// Phase 3: merge — session targets first (scored), then pending peers
	// (alphabetical, lower priority).
	//
	// When session targets exist, cap total at 3 (gossip fanout limit).
	// When no sessions exist at all, return all matching peers uncapped
	// (bootstrap scenario — identical to the pre-merge behavior).
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return string(scored[i].address) < string(scored[j].address)
		}
		return scored[i].score > scored[j].score
	})
	sort.Slice(pendingPeers, func(i, j int) bool {
		return string(pendingPeers[i]) < string(pendingPeers[j])
	})

	if len(scored) > 0 {
		// Session targets exist — cap at gossip fanout limit (maxTargets),
		// fill remaining slots with pending peers so messages reach
		// not-yet-connected nodes.
		targets := make([]domain.PeerAddress, 0, maxTargets)
		for _, item := range scored {
			if len(targets) >= maxTargets {
				break
			}
			targets = append(targets, item.address)
		}
		for _, addr := range pendingPeers {
			if len(targets) >= maxTargets {
				break
			}
			targets = append(targets, addr)
		}
		return targets
	}

	// No active sessions — return all matching peers uncapped (bootstrap).
	targets := make([]domain.PeerAddress, 0, len(pendingPeers))
	targets = append(targets, pendingPeers...)
	return targets
}

// sendGossipFrameToPeer delivers a pre-built gossip push_message frame to one
// peer: outbound session first, then a direct write to an authenticated
// inbound connection, then the pending ring. The frame is shared read-only
// with the other per-target send goroutines (see executeGossipTargets);
// msgID/recipient are passed separately only for the log lines so the shared
// frame's Item need not be re-read. push_message is fire-and-forget gossip
// relay — no outbound delivery tracking (that lives on the local send_message
// path via markOutboundTerminal/clearOutboundQueued).
func (s *Service) sendGossipFrameToPeer(address domain.PeerAddress, frame protocol.Frame, msgID, recipient string) {
	defer crashlog.DeferRecover()
	if s.enqueuePeerFrame(address, frame) {
		log.Info().Str("node", s.identity.Address).Str("id", msgID).Str("recipient", recipient).Str("peer", string(address)).Str("mode", "session").Msg("gossip_message_attempt")
		return
	}

	// No outbound session — try writing directly to an authenticated inbound
	// connection from this peer. This covers the common case where the peer
	// has connected to us (inbound) but we have no outbound session (CM slot
	// full). push_message is fire-and-forget so it is safe to interleave on
	// the inbound conn without disrupting request/reply traffic.
	// resolveHealthAddress reads dialOrigin (peer-domain state mutated
	// during session setup/teardown) — it belongs INSIDE the RLock.
	s.peerMu.RLock()
	resolved := s.resolveHealthAddress(address)
	inboundID, haveInbound := s.inboundConnIDForAddressLocked(resolved)
	s.peerMu.RUnlock()
	if haveInbound {
		// Fire-and-forget gossip inbound-direct fallback — Network-routed
		// so a test backend can intercept it; ctx is Service lifecycle.
		_ = s.sendFrameViaNetwork(s.runCtx, inboundID, frame)
		log.Info().Str("node", s.identity.Address).Str("id", msgID).Str("recipient", recipient).Str("peer", string(address)).Str("mode", "inbound_direct").Msg("gossip_message_attempt")
		return
	}

	if s.queuePeerFrame(address, frame) {
		log.Debug().Str("id", msgID).Str("recipient", recipient).Str("peer", string(address)).Str("mode", "queued").Msg("gossip_message_attempt")
		return
	}
	log.Debug().Str("id", msgID).Str("recipient", recipient).Str("peer", string(address)).Str("mode", "dropped").Msg("gossip_message_attempt")
}

func (s *Service) gossipNotice(ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	for _, address := range s.routingTargets() {
		if address == "" || s.isSelfAddress(address) {
			continue
		}
		// Dedicated notice lane: push_notice fan-out has no retry cycle
		// behind it (unlike DM gossip, which retryRelayDeliveries
		// re-covers), so it must not share a lossy queue with DM
		// storms. Its own lane only sheds under a notice flood — abuse
		// traffic by definition (inbound notices re-gossip here).
		s.dispatchGossipNoticeSend(func() { s.sendNoticeToPeer(address, ttl, ciphertext) })
	}
}

func (s *Service) sendNoticeToPeer(address domain.PeerAddress, ttl time.Duration, ciphertext string) {
	defer crashlog.DeferRecover()
	frame := protocol.Frame{
		Type:       "push_notice",
		TTLSeconds: int(ttl.Seconds()),
		Ciphertext: ciphertext,
	}
	if s.enqueuePeerFrame(address, frame) {
		return
	}

	// Try authenticated inbound connection before expensive TCP dial.
	// resolveHealthAddress reads dialOrigin (peer-domain state) — it
	// belongs INSIDE the RLock.
	s.peerMu.RLock()
	resolved := s.resolveHealthAddress(address)
	inboundID, haveInbound := s.inboundConnIDForAddressLocked(resolved)
	s.peerMu.RUnlock()
	if haveInbound {
		// Fire-and-forget push_notice inbound-direct fallback — see
		// gossip-message counterpart above for the same ctx rationale.
		_ = s.sendFrameViaNetwork(s.runCtx, inboundID, frame)
		return
	}

	conn, err := net.DialTimeout("tcp", string(address), syncHandshakeTimeout)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(syncHandshakeTimeout))
	reader := bufio.NewReader(conn)

	// netcore-migration: §4.4 bootstrap exception. This raw write pre-dates
	// the NetCore managed-write path and is pending architectural review of
	// the inbound-absent dial fallback itself. A repository-wide forbidden-
	// list grep gate whitelists this write by explicit line number using
	// the exact prefix `netcore-migration: §4.4 bootstrap exception` as the
	// whitelist token — removing the sentinel without removing the raw
	// write triggers the gate. Same applies to the two writes below in
	// this function body.
	if _, err := io.WriteString(conn, s.nodeHelloJSONLine()); err != nil {
		return
	}
	welcomeLine, err := readFrameLine(reader, maxResponseLineBytes)
	if err != nil {
		return
	}
	welcome, err := protocol.ParseFrameLine(strings.TrimSpace(welcomeLine))
	if err != nil {
		return
	}
	// Bootstrap fan-out path: the inbound side may respond with a
	// connection_notice (currently only peer-banned) instead of welcome,
	// meaning it is about to close. Forward to handleConnectionNotice so
	// the dialler records the responder-supplied ban window, then abort
	// this bootstrap write.
	if welcome.Type == protocol.FrameTypeConnectionNotice {
		s.handleConnectionNotice(address, welcome)
		// Remote-first self-loopback discovery on the raw push_notice
		// fan-out path. routingTargets() can route onto an alternate
		// alias (alternate listen port, onion/clearnet mirror) that
		// slips past the isSelfAddress() gate in gossipNotice, landing
		// on our own inbound hello handler which answers with
		// connection_notice{code=peer-banned, reason=self-identity}.
		// Without this branch handleConnectionNotice would only record
		// the (possibly empty) remote ban-until in persistedMeta,
		// leaving health.BannedUntil and LastErrorCode unset. The next
		// gossipNotice tick would then redial the same self-looping
		// alias because the dial-gate consults health, not persistedMeta
		// alone. Route through the same applySelfIdentityCooldown the
		// managed session path and syncPeer use so the 24h window lands
		// in health and blocks further churn.
		if errors.Is(protocol.NoticeErrorFromFrame(welcome), protocol.ErrSelfIdentity) {
			s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		}
		return
	}
	// Local self-loopback guard on the same dial: an alternate alias
	// may reach a responder that does not emit the peer-banned notice
	// (older version, different role) but still welcomes us with our
	// own Ed25519 address. Signing auth_session with our own key and
	// sending push_notice is wasted work at best and a defence-in-depth
	// hole at worst — we would ingest our own welcome into the peer
	// caches via the auth_ok → recordOutboundAuthSuccess path. Abort
	// with the same cooldown the notice branch above applies so both
	// arrival shapes converge on one health record.
	if s.isSelfIdentity(domain.PeerIdentity(welcome.Address)) {
		log.Warn().
			Str("peer", string(address)).
			Str("local_identity", s.identity.Address).
			Str("welcome_listen", welcome.Listen).
			Msg("send_notice_self_identity_rejected")
		s.applySelfIdentityCooldown(address, s.newSelfIdentityError(address, welcome.Listen))
		return
	}
	if strings.TrimSpace(welcome.Challenge) != "" {
		authLine, err := protocol.MarshalFrameLine(protocol.Frame{
			Type:      "auth_session",
			Address:   s.identity.Address,
			Signature: identity.SignPayload(s.identity, connauth.SessionAuthPayload(welcome.Challenge, s.identity.Address)),
		})
		if err != nil {
			return
		}
		// netcore-migration: §4.4 bootstrap exception (see sentinel above).
		if _, err := io.WriteString(conn, authLine); err != nil {
			return
		}
		reply, err := readFrameLine(reader, maxResponseLineBytes)
		if err != nil {
			return
		}
		authReply, err := protocol.ParseFrameLine(strings.TrimSpace(reply))
		if err != nil || authReply.Type != "auth_ok" {
			return
		}
		// Outbound convergence success hook: auth_ok on this raw/
		// bootstrap path is the same evidence of a reachable dialable
		// peer as on the managed-session path. Without this call, peers
		// reached only through push_notice fan-out would never get an
		// announceable persistedMeta row or a trusted advertise triple,
		// so their state would diverge from the managed path and the
		// hostname / observed-IP sweep invariants would not apply here.
		//
		// The helper takes the wrapper-form "host:port" string. This
		// §4.4 raw path has no NetCore wrapper, so we read conn.RemoteAddr()
		// inline — routing_relay.go is already in the §2.9 net-import
		// whitelist for exactly these bootstrap edges. The defensive empty-
		// string fallthrough keeps the contract with the helper.
		var remoteAddr string
		if ra := conn.RemoteAddr(); ra != nil {
			remoteAddr = ra.String()
		}
		s.recordOutboundAuthSuccess(address, remoteAddr)
	}
	if welcome.Type == "error" {
		return
	}

	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		return
	}
	// netcore-migration: §4.4 bootstrap exception (see sentinel above).
	_, _ = io.WriteString(conn, line)
	_, _ = readFrameLine(reader, maxResponseLineBytes)
}
