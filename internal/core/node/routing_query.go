// routing_query.go hosts the receive-path for Phase 2 targeted
// route queries (route_query_v1 / route_query_response_v1 wire
// frames defined in docs/protocol/route_health.md
// and overview §7.5).
//
// Two responsibilities live here:
//
//   - handleRouteQuery answers an inbound route_query_v1 by
//     consulting the local routing.Table for the queried target
//     and emitting a route_query_response_v1 with the best uplink,
//     hop count and SeqNo (or Found=false when no route exists).
//     No forwarding — the protocol is strictly single-hop.
//   - handleRouteQueryResponse ingests an inbound
//     route_query_response_v1 into the routing.Table as a
//     RouteSourceAnnouncement claim. Found=false is a no-op. The
//     standard admission rules (SeqNo monotonicity, anti-spoof,
//     fast invalidation, cap) apply unchanged — a query response
//     is just a peer-attested route claim, the same trust class as
//     announce_routes.
//
// Both paths are wired into dispatchNetworkFrame in service.go
// behind a CapMeshRouteQueryV1 capability gate (PR 11.4b). The
// sender side (on-demand query when all uplinks Bad/Dead, rate
// limit 3 per target per 30s, outstanding-query bookkeeping) lands
// in PR 11.4c.
//
// Architectural invariants from CLAUDE.md and phase-2 §2.5:
//
//   - Query/response are P2P wire commands; they travel through
//     dispatchNetworkFrame (this PR), never through CommandTable.
//   - Response send goes through s.sendFrameViaNetwork (single-
//     writer invariant). No conn.Write call site.
//   - "route_query_response_v1" name carries the exact same
//     contract on every transport path: raw claim ingested as
//     Announcement. No enriched payload aliasing — UI aggregation
//     (e.g. multi-route diagnostics) lives in fetchRouteHealth
//     (PR 11.5).
package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// handleRouteQuery answers an inbound route_query_v1. The responder
// consults its local routing.Table.Lookup(target). When at least
// one non-Dead route exists, the best (highest CompositeScore)
// uplink is encoded into a Found=true response. When the table has
// no usable route, Found=false is returned.
//
// The protocol is strictly single-hop: the handler MUST NOT forward
// the query to its own neighbours and MUST NOT block on any other
// peer's lookup. This is enforced structurally — the handler only
// reads its own routing.Table.
//
// Single-writer invariant: the response send goes through
// sendFrameViaNetwork, which funnels into the per-connection
// designated writer goroutine.
//
// Split horizon: claims whose NextHop equals senderIdentity are
// filtered out of the Lookup result before choosing the best
// entry. Without this filter we could answer found=true with a
// claim that routes back through the query sender, forming a
// sender↔responder loop once the sender ingests the response as
// a fresh RouteSourceAnnouncement. This mirrors the existing
// split-horizon rule on the announce-plane
// (`Table.AnnounceableFor(excludeVia)`).
func (s *Service) handleRouteQuery(connID domain.ConnID, senderIdentity domain.PeerIdentity, query protocol.RouteQueryFrame) {
	resp := protocol.RouteQueryResponseFrame{
		Type:           protocol.RouteQueryResponseFrameType,
		QueryID:        query.QueryID,
		TargetIdentity: query.TargetIdentity,
		IssuedAt:       time.Now().UTC().Format(time.RFC3339),
	}

	// PR 11.32 P2 — outbound SeqNo namespace alignment. The
	// response is conceptually a one-shot announce_routes frame
	// to the requester, so the wire SeqNo must come from the
	// responder's per-receiver outbound counter
	// (nextOutboundSeqLockedPerPeer) rather than the raw claim's
	// SeqNo. Without this, a route_query response can carry a
	// SeqNo unrelated to what subsequent normal announces would
	// emit, causing receivers to either reject later legitimate
	// announces as stale (if response.SeqNo > outbound counter
	// state) or trip the cross-Origin lineage guard via a
	// mismatched LastIngressOrigin. Table.AnnounceTargetFor
	// projects the single (target, requester) entry through the
	// same path AnnounceProjectionFor uses for periodic announces,
	// keeping the two timelines aligned.
	//
	// AnnounceTargetFor applies split-horizon (excluding requester
	// as the uplink) and the Dead filter with Direct exemption
	// internally, so the previous filterSplitHorizon call is no
	// longer needed.
	entry, bestUplink, ok := s.routingTable.AnnounceTargetFor(query.TargetIdentity, senderIdentity)
	if ok {
		resp.Found = true
		resp.BestUplink = bestUplink
		// Wire BestHops is the responder's hop count to target.
		// The querier will add +1 on ingest (standard receive-side
		// hop convention).
		if entry.Hops > 0 {
			resp.BestHops = uint8(entry.Hops)
		}
		resp.BestSeqNo = entry.SeqNo
	}

	raw, err := protocol.MarshalRouteQueryResponseFrame(resp)
	if err != nil {
		log.Warn().
			Err(err).
			Uint64("query_id", query.QueryID).
			Str("target_identity", query.TargetIdentity.String()).
			Msg("route_query_response_marshal_failed")
		return
	}
	rawLine := string(raw) + "\n"
	_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{
		Type:    protocol.RouteQueryResponseFrameType,
		RawLine: rawLine,
	})
}

// handleRouteQueryResponse ingests an inbound route_query_response_v1
// into the routing.Table. Found=false is a no-op — the peer
// confirmed it has no usable route, nothing to record.
//
// Found=true synthesises a RouteEntry and feeds it through the
// standard Table.UpdateRoute admission pipeline:
//
//   - Source = RouteSourceAnnouncement: matches the trust class of
//     regular announce_routes — the response is a peer-attested
//     claim, not a direct observation.
//   - NextHop = senderIdentity: the peer that answered is our
//     immediate uplink to the target.
//   - Hops = response.BestHops + 1: receive-side adds one for the
//     hop from us to the responder.
//   - Origin = senderIdentity: matches the wire-Origin a normal
//     announce_routes from the same peer would carry (post-Phase-A
//     sender-originated synthesis, see route_store_lookup.go's
//     emitOrigin = localOrigin contract in AnnounceProjectionFor).
//     The previous synthesis used target_identity, which made the
//     ingested entry land with a different LastIngressOrigin than
//     a normal announce from the same peer, tripping the cross-
//     Origin lineage guard on the next real announce (PR 11.32
//     P2). Post-Phase-A the storage dedup key is (Identity,
//     Uplink) and Origin is wire-only metadata; using
//     senderIdentity keeps both ingest paths funnelling into the
//     same LastIngressOrigin.
//   - SeqNo = response.BestSeqNo: the responder's wire SeqNo for
//     the chosen claim. Standard SeqNo monotonicity guards in
//     UpdateRoute decide whether to accept.
//
// Defensive bounds:
//
//   - response.BestUplink is informational only — the receiver's
//     NextHop is always senderIdentity. We do not derive trust from
//     BestUplink because that would let a peer claim to be relaying
//     through arbitrary third parties.
//   - response.BestHops == 0 with Found=true is treated as invalid
//     (the responder is the target itself? That would be a Direct
//     route, which never appears in single-hop query response per
//     receive convention). We log and drop.
//   - Hops+1 wrap to 16 (HopsInfinity) is treated as a withdrawal
//     by UpdateRoute, which is the correct semantic for a hops=15
//     transit query — no special case needed.
func (s *Service) handleRouteQueryResponse(senderIdentity domain.PeerIdentity, resp protocol.RouteQueryResponseFrame) {
	if !resp.Found {
		log.Debug().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("sender_identity", senderIdentity.String()).
			Msg("route_query_response_found_false")
		return
	}
	// Identity validation BEFORE ingest. RouteEntry.Validate
	// rejects empty Identity but accepts arbitrary strings —
	// the announce path (routing_announce.go::handleAnnounceRoutes)
	// explicitly drops malformed fingerprints via
	// identity.IsValidAddress, and the same guard belongs on
	// the query-response ingest path so an authenticated
	// mesh_route_query_v1 peer cannot inject routes to junk
	// identities ("*", whitespace, non-hex, wrong length).
	if !identity.IsValidAddress(resp.TargetIdentity.String()) {
		log.Warn().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("sender_identity", senderIdentity.String()).
			Msg("route_query_response_dropped_invalid_target_identity")
		return
	}
	if resp.BestHops == 0 {
		log.Warn().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("sender_identity", senderIdentity.String()).
			Msg("route_query_response_dropped_zero_hops")
		return
	}
	if resp.TargetIdentity == senderIdentity {
		// Pathological: peer claims to be reaching itself through
		// something other than a direct route. Drop — we already
		// know how to reach the peer if it is connected; if not,
		// no peer-attested transit claim is going to fix that.
		log.Warn().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Msg("route_query_response_dropped_self_target")
		return
	}
	// PR 11.22 P2: local-self guard. Ingesting a response whose
	// target_identity equals this node's own fingerprint would
	// synthesise (Origin=localIdentity, NextHop=senderIdentity)
	// — a transit route claiming we reach ourselves through some
	// arbitrary peer. The announce-plane ingest already drops
	// such routes (routing_announce.go::handleAnnounceRoutes,
	// "Skip routes about ourselves" branch) because we own
	// our own connectivity; query-response ingest must apply the
	// same guard. Without it the synthetic claim would live in
	// storage, leak through HealthSnapshot / fetchRouteHealth as
	// a normal entry, and could surface in AnnounceProjection's
	// per-Identity live-winner selection (excludeVia filters by
	// senderIdentity == excludeVia, NOT by Origin == localOrigin,
	// so an attacker-injected self-claim with NextHop=otherPeer
	// would be projectable to every other peer).
	if s.identity != nil && resp.TargetIdentity == domain.PeerIdentityFromWire(s.identity.Address) {
		log.Warn().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("sender_identity", senderIdentity.String()).
			Msg("route_query_response_dropped_target_is_local_identity")
		return
	}
	// Triplet capability check on the responder (review concern
	// P2#3): ingesting the response writes senderIdentity as a
	// transit next-hop with Hops=BestHops+1. A transit next-hop
	// requires BOTH mesh_relay_v1 (to accept our relay_message)
	// AND mesh_routing_v1 (to forward through its own table)
	// — see resolveRouteNextHopAddress in routing_relay.go.
	// Without this guard a peer that advertises only the query
	// cap could refresh routes that the relay path would have to
	// drop on every send attempt.
	if !s.peerHasCapabilities(senderIdentity, domain.CapMeshRelayV1, domain.CapMeshRoutingV1) {
		log.Warn().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("sender_identity", senderIdentity.String()).
			Msg("route_query_response_dropped_responder_missing_relay_or_routing_cap")
		return
	}

	// PR 11.32 P2/P3: Origin = senderIdentity matches what a
	// normal announce_routes from this peer would carry — wire
	// AnnounceProjectionFor emits Origin=sender's localOrigin
	// (sender-originated post-Phase-A, see route_store_lookup.go's
	// emitOrigin synthesis). Setting Origin=TargetIdentity
	// previously made route_query_response ingest land with a
	// different LastIngressOrigin than a normal announce from the
	// same peer, tripping the cross-Origin lineage guards on the
	// next real announce. Using senderIdentity keeps both ingest
	// paths funnelling into the same lineage.
	//
	// ExpiresAt is left zero — Table.UpdateRoute applies the
	// table's own defaultTTL and injected clock, matching the
	// announce ingest path (routing_announce.go's RouteEntry
	// construction). Synthesising now+routing.DefaultTTL here
	// would bypass table policy and the clock fixture in tests.
	entry := routing.RouteEntry{
		Identity: resp.TargetIdentity,
		Origin:   senderIdentity,
		NextHop:  senderIdentity,
		Hops:     int(resp.BestHops) + 1,
		SeqNo:    resp.BestSeqNo,
		Source:   routing.RouteSourceAnnouncement,
	}
	status, err := s.routingTable.UpdateRoute(entry)
	if err != nil {
		log.Warn().Err(err).
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("next_hop", senderIdentity.String()).
			Msg("route_query_response_update_failed")
		return
	}
	if status == routing.RouteAccepted {
		log.Debug().
			Uint64("query_id", resp.QueryID).
			Str("target_identity", resp.TargetIdentity.String()).
			Str("next_hop", senderIdentity.String()).
			Int("hops", entry.Hops).
			Msg("route_query_response_ingested")
	}
	// Re-arm held sender-owned DMs whether the route was newly accepted OR
	// merely reconfirmed (RouteUnchanged) — a route that was already in the
	// table but had no usable session can become reachable again the moment
	// the peer answers the route_query. Mirrors handleAnnounceRoutes, which
	// treats RouteUnchanged as a drain trigger too. Self-checked inside the
	// kick, so a route that still does not resolve to a next hop is a no-op.
	if status == routing.RouteAccepted || status == routing.RouteUnchanged {
		s.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
			resp.TargetIdentity: {},
		})
	}
}
