// routing_probe.go hosts the receive-path for Phase 2 reachability
// probes (route_probe_v1 / route_probe_ack_v1 wire frames defined in
// docs/protocol/route_health.md and overview §7.6).
//
// Two responsibilities live here:
//
//   - handleRouteProbe answers an inbound route_probe_v1 by performing
//     a local routing.Table.Lookup on the queried target and emitting
//     a route_probe_ack_v1 that mirrors whether the probe sender's
//     uplink (us) can currently reach the target.
//   - handleRouteProbeAck ingests an inbound route_probe_ack_v1 into
//     the per-(Identity, Uplink) RouteHealthState via
//     routing.Table.MarkProbeAck.
//
// Both paths are wired into dispatchNetworkFrame in service.go behind
// a CapMeshRouteProbeV1 capability gate. The probe sender goroutine
// (the ticker that schedules probes for Questionable pairs and the
// outstanding-probe registry that correlates sends with acks) lives
// in routing_probe_loop.go; this file is the receive side. The RTT
// sample threaded into MarkProbeAck is the sender-measured round-trip
// from probeRegistry.ResolveMatching — the registry stamps the send
// time at Register, and the ack-receive timestamp minus that send
// time becomes the EWMA input via UpdateRTT.
//
// Wire-vs-RPC separation, single-writer invariant, and the
// "no direct conn.Write" rule from CLAUDE.md are respected:
// responses go through s.sendFrameViaNetwork, the only sanctioned
// write path that funnels through the per-connection designated
// writer goroutine.
package node

import (
	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// handleRouteProbe answers an inbound route_probe_v1 frame. The
// responder looks up the queried target identity in its local
// routing.Table and emits a route_probe_ack_v1 with
// reachable=(len(routes) > 0). No forwarding is performed — probes
// are strictly single-hop, exactly like route_query_v1 (see overview
// §7.5/§7.6).
//
// The ack's rtt_ms field is left at zero. It is reserved as an
// informational hint from responder to sender; per the zero-trust
// budget (overview §4.2) senders MUST measure RTT locally via the
// probe send-to-ack round-trip and never trust this field for their
// own ranking decisions. PR 11.3c may populate rtt_ms from the
// responder's own EWMA estimate for the (target, downstream-uplink)
// pair, but the wire contract does not require it.
//
// Single-writer invariant: the ack send goes through
// sendFrameViaNetwork, which funnels into the per-connection
// designated writer goroutine. No conn.Write call site is introduced.
//
// Split horizon: claims whose NextHop equals senderIdentity are
// filtered out of the Lookup result before deciding `reachable`.
// Without this filter the responder could answer reachable=true
// using the path X-via-sender, which forms a sender↔responder
// routing loop once the sender's health for that pair is
// promoted on the strength of the ack. This mirrors the
// split-horizon rule already enforced by
// `Table.AnnounceableFor(excludeVia)` on the announce-plane.
func (s *Service) handleRouteProbe(connID domain.ConnID, senderIdentity domain.PeerIdentity, probe protocol.RouteProbeFrame) {
	// Local Lookup determines reachable. Lookup returns the post-
	// Phase-2 CompositeScore-ranked selectable routes; HealthDead
	// claims are already filtered out. After Lookup we apply
	// split-horizon: any claim whose NextHop equals the probe
	// sender would form a routing loop if accepted, so we strip
	// such claims before deciding reachable.
	routes := filterSplitHorizon(s.routingTable.Lookup(probe.TargetIdentity), senderIdentity)
	reachable := len(routes) > 0

	ack := protocol.RouteProbeAckFrame{
		Type:      protocol.RouteProbeAckFrameType,
		ProbeID:   probe.ProbeID,
		Reachable: reachable,
		// RTTMs left at 0 — informational only; sender measures
		// its own round-trip.
	}
	raw, err := protocol.MarshalRouteProbeAckFrame(ack)
	if err != nil {
		// Marshal failures are programming errors in our own helper —
		// log and drop the ack so we do not blow up the connection.
		log.Warn().
			Err(err).
			Uint64("probe_id", probe.ProbeID).
			Str("target_identity", string(probe.TargetIdentity)).
			Msg("route_probe_ack_marshal_failed")
		return
	}

	// RawLine fast-path (matches FileCommandFrame send pattern):
	// append the newline expected by the wire codec, wrap in a
	// Frame with the capability-gated Type so the receive-side
	// dispatcher routes correctly.
	rawLine := string(raw) + "\n"
	_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{
		Type:    protocol.RouteProbeAckFrameType,
		RawLine: rawLine,
	})
}

// filterSplitHorizon strips routes whose NextHop equals the
// excluded peer from a Lookup result. Used by handleRouteProbe and
// handleRouteQuery so a responder never advertises a path back to
// the requester via the requester itself — the classic split-
// horizon rule that prevents loops when the requester ingests
// the response as fresh routing state.
//
// Returns the input slice unmodified when no entry needs removal,
// otherwise a fresh slice. Callers MUST NOT assume the returned
// slice aliases the input.
func filterSplitHorizon(routes []routing.RouteEntry, excludeVia domain.PeerIdentity) []routing.RouteEntry {
	if excludeVia == "" || len(routes) == 0 {
		return routes
	}
	// Fast path: scan first to avoid allocating when nothing
	// matches. Most Lookup results in a healthy mesh have one or
	// two uplinks, so the linear scan is cheap.
	hit := false
	for _, r := range routes {
		if r.NextHop == excludeVia {
			hit = true
			break
		}
	}
	if !hit {
		return routes
	}
	out := make([]routing.RouteEntry, 0, len(routes))
	for _, r := range routes {
		if r.NextHop == excludeVia {
			continue
		}
		out = append(out, r)
	}
	return out
}

// handleRouteProbeAck ingests an inbound route_probe_ack_v1 into
// RouteHealthState[(target, senderIdentity)] via
// routing.Table.MarkProbeAck.
//
// senderIdentity is the immediate peer that produced the ack — i.e.,
// the uplink we probed. The target identity is recovered from the
// outstanding-probe registry by probe ID (the wire ack does not
// carry target_identity; PR 11.3c bookkeeping closes that loop).
//
// RTT is measured locally as registry.Resolve's now−SentAt
// duration, NOT taken from ack.RTTMs — per the zero-trust budget
// (overview §4.2) senders ignore responder-supplied RTT for their
// own ranking. ack.RTTMs is informational only and is logged but
// not folded into the EWMA.
//
// Stray acks (probe_id not in registry — either timeout already
// fired and removed the entry, or a malicious peer fabricated an
// ack) are silently dropped with a debug log. They cannot poison
// health state because Resolve returns ok=false.
//
// Uplink-mismatch acks (registry says the probe was sent to
// uplink U, but this ack arrived from a different identity V) are
// also dropped: a peer can only acknowledge probes we sent to it.
// This is the receive-side zero-trust guard that prevents one peer
// from spoofing acks for another peer's probes.
func (s *Service) handleRouteProbeAck(senderIdentity domain.PeerIdentity, ack protocol.RouteProbeAckFrame) {
	if s.probeRegistry == nil {
		// Service initialised without probe registry (legacy test
		// fixtures pre-PR-11.3c). Defensive no-op.
		log.Debug().
			Uint64("probe_id", ack.ProbeID).
			Str("sender_identity", string(senderIdentity)).
			Msg("route_probe_ack_dropped_no_registry")
		return
	}

	// ResolveMatching is uplink-aware: a mismatched ack
	// (probe_id present but expectedUplink != registered)
	// returns ok=false and LEAVES the pending entry intact.
	// This is the receive-side zero-trust guard against an
	// attacker who guesses a monotonic probe_id and tries to
	// consume someone else's outstanding probe — the real
	// uplink's ack (or timeout) still fires unchanged.
	target, rtt, ok := s.probeRegistry.ResolveMatching(ack.ProbeID, senderIdentity)
	if !ok {
		// Two failure modes collapse into one log line because
		// the registry intentionally hides which one occurred:
		// either the probe_id was unknown (stale ack after
		// timeout) or the uplink mismatched (potential spoof).
		// Both outcomes are "drop without state mutation" —
		// the distinction is not useful for the receive path.
		log.Debug().
			Uint64("probe_id", ack.ProbeID).
			Str("sender_identity", string(senderIdentity)).
			Msg("route_probe_ack_dropped_unknown_or_mismatched")
		return
	}

	s.routingTable.MarkProbeAck(target, senderIdentity, ack.Reachable, rtt)
}
