// routing_announce.go hosts the outbound announce-send path
// (SendAnnounceRoutes, SendRoutesUpdate, SendRequestResync, connect-time
// full sync, fan-out for withdrawals), the inbound announce-receive path
// (handleAnnounceRoutes, handleRoutesUpdate, handleRequestResync), the
// routing-capable peer enumeration (routingCapablePeers), the inbound
// sync write helper shared with other relay paths (writeFrameToInbound),
// the periodic TTL tick (routingTableTTLLoop), and the drain trigger
// invoked by announce, TTL, and session paths (triggerDrainForExposed).
//
// This file implements the announce-plane side of the routing contract;
// the relay-plane (table-directed forwarding, gossip fallback, bootstrap
// handshake writes) lives in routing_relay.go, address↔identity resolution
// in routing_resolver.go, session-lifecycle routing hooks in
// routing_session.go, hop_ack-driven route confirmation in
// routing_hop_ack.go, and event-driven pending-queue drain in
// routing_drain.go. The per-file scope table is the durable record — see
// the "internal/core/node/" section of the file map in docs/routing.md
// (both the English and Russian component-map sections).
package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/crashlog"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routingTableTTLLoop runs periodic TTL cleanup on the routing table.
// Expired entries are removed every 10 seconds. When an expiry exposes
// a surviving backup route for an identity, the event-driven drain is
// triggered so pending send_message frames can be delivered immediately
// instead of waiting for the normal retry loop.
func (s *Service) routingTableTTLLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result := s.routingTable.TickTTL()
			s.triggerDrainForExposed(result.Exposed)
			// Notify subscribers (NodeStatusMonitor reachability) when
			// routes were actually removed. Without this event, TTL expiry
			// of a primary route silently changes reachability without the
			// monitor ever learning about it.
			if result.Removed > 0 {
				s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
					Reason:    domain.RouteChangeTTLExpired,
					Withdrawn: result.Removed,
				})
			}
		}
	}
}

// SendAnnounceRoutes implements routing.PeerSender. It builds an
// announce_routes frame and sends it to the peer. Supports both outbound
// sessions (by session address) and inbound connections (by "inbound:"
// prefixed address from inboundConnKeyForID).
//
// ctx is propagated down to the network bridge: a pre-cancelled ctx fails
// fast without touching the transport, and a mid-flight cancel during the
// inbound sync-flush wait aborts the send rather than consuming the full
// syncFlushTimeout. Outbound enqueue is non-blocking so ctx only matters
// for the inbound path.
func (s *Service) SendAnnounceRoutes(ctx context.Context, peerAddress domain.PeerAddress, routes []routing.AnnounceEntry) bool {
	if len(routes) == 0 {
		return true
	}
	frame := buildAnnounceFrame(announceWireLegacy, routes)
	return s.sendAnnouncePlaneFrame(ctx, peerAddress, frame)
}

// SendRoutesUpdate implements routing.PeerSender for the v2 wire path.
// Builds a routes_update frame carrying an incremental delta and dispatches
// it through the same transport as SendAnnounceRoutes. The decision to pick
// this method over SendAnnounceRoutes belongs to the announce loop; the
// invariants are enforced upstream (first sync and forced full always go
// through SendAnnounceRoutes — see docs/routing.md "First-sync wire-frame
// invariant"). An empty delta short-circuits to "success" because the
// protocol is additive and sending an empty routes_update would be noise.
//
// Send-time capability gate: the announce loop classifies the wire mode
// against an AnnounceTarget snapshot taken at cycle start, but the
// underlying session at peerAddress can close and be replaced by a
// session that does NOT hold the full routing-target gate before this
// method runs. The dispatch helper mirrors that gate exactly —
// mesh_routing_v1 AND mesh_routing_v2 AND mesh_relay_v1, the same
// predicate routingCapablePeers uses to pick AnnounceTarget candidates
// (plus the v2 capability the announce loop additionally requires for
// the v2 path). It captures the live transport handle (sendCh for
// outbound sessions, ConnID for inbound conns) under the same peerMu
// RLock as the cap validation, then writes to the captured handle —
// closing the cap-check vs write race that would otherwise let a
// narrower replacement session at the same address receive routes_update
// bytes. Returning false leaves the caller's per-peer cache untouched
// so the next cycle re-classifies and retries on the current session.
func (s *Service) SendRoutesUpdate(ctx context.Context, peerAddress domain.PeerAddress, delta []routing.AnnounceEntry) bool {
	if len(delta) == 0 {
		return true
	}
	frame := buildAnnounceFrame(announceWireV2, delta)
	if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, frame,
		domain.CapMeshRoutingV1,
		domain.CapMeshRoutingV2,
		domain.CapMeshRelayV1,
	) {
		log.Debug().
			Str("peer_address", string(peerAddress)).
			Int("delta_routes", len(delta)).
			Msg("routes_update_skipped_or_failed_at_send_time")
		return false
	}
	return true
}

// SendRequestResync emits a request_resync wire frame to the peer so that
// the peer clears its per-peer announce state and re-delivers a full
// baseline via legacy announce_routes on its next cycle. Called by the
// v2 receive path when routes_update arrives without a prior baseline in
// this session — the forced-full path on the peer side honours the
// first-sync invariant, so recovery walks back through legacy
// announce_routes regardless of the peer's v2 status.
//
// The frame carries no payload: its mere arrival is the signal. This
// matches the single-writer invariant enforced by
// sendAnnouncePlaneFrame: there is no direct conn.Write bypass, every
// request_resync flows through the same enqueue/sync path as the rest
// of the announce plane.
//
// Send-time capability gate: request_resync is a v2-only control frame
// per the receive-dispatcher contract — both the inbound and session
// dispatchers gate this frame on mesh_routing_v2 alone, with no v1 or
// relay requirement (it carries no payload, only signals "clear my
// announce state"). The send-time gate must therefore stop at v2 too —
// widening to v1+v2+relay would have the sender skip a recovery control
// frame that the receiver would accept, leaving v2-only sessions
// permanently desynced. Capture-and-write is still atomic via
// dispatchAnnouncePlaneFrameWithCaps: a session-replacement at the same
// address between cap check and write cannot route the frame to a
// non-v2 transport.
func (s *Service) SendRequestResync(ctx context.Context, peerAddress domain.PeerAddress) bool {
	frame := protocol.Frame{Type: "request_resync"}
	if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, frame,
		domain.CapMeshRoutingV2,
	) {
		log.Debug().
			Str("peer_address", string(peerAddress)).
			Msg("request_resync_skipped_or_failed_at_send_time")
		return false
	}
	return true
}

// announceWireType is the internal tag that picks which wire frame
// buildAnnounceFrame emits. Keeping the decision in a typed enum avoids
// a string literal dependency at every call site and makes the set of
// announce-plane wire types auditable in one place.
type announceWireType int

const (
	announceWireLegacy announceWireType = iota
	announceWireV2
)

// buildAnnounceFrame produces the wire frame carrying the given entries.
// Both wire types share the AnnounceRouteFrame payload shape — the only
// difference is Frame.Type. The factory exists so that
// SendAnnounceRoutes / SendRoutesUpdate cannot accidentally ship the
// wrong frame.Type for the same payload: the mapping from wire-type enum
// to the string lives here, not at the call site.
func buildAnnounceFrame(kind announceWireType, entries []routing.AnnounceEntry) protocol.Frame {
	wireRoutes := make([]protocol.AnnounceRouteFrame, len(entries))
	for i, r := range entries {
		wireRoutes[i] = protocol.AnnounceRouteFrame{
			Identity: string(r.Identity),
			Origin:   string(r.Origin),
			Hops:     r.Hops,
			SeqNo:    r.SeqNo,
			Extra:    r.Extra,
		}
	}
	frame := protocol.Frame{AnnounceRoutes: wireRoutes}
	switch kind {
	case announceWireV2:
		frame.Type = "routes_update"
	default:
		frame.Type = "announce_routes"
	}
	return frame
}

// sendAnnouncePlaneFrame dispatches a single announce-plane frame to the
// correct transport path based on the peer-address shape. Inbound-key
// addresses ("inbound:..." prefix) take the synchronous NetCore write;
// outbound-session addresses take the per-session enqueue. Keeping the
// prefix branch in one helper so that every new announce-plane wire frame
// (announce_routes, routes_update, request_resync) routes through the
// same single-writer invariant without a second copy of the switch.
func (s *Service) sendAnnouncePlaneFrame(ctx context.Context, peerAddress domain.PeerAddress, frame protocol.Frame) bool {
	if strings.HasPrefix(string(peerAddress), "inbound:") {
		return s.writeFrameToInbound(ctx, peerAddress, frame)
	}
	return s.enqueuePeerFrame(peerAddress, frame)
}

// writeFrameToInbound writes a marshaled frame to an inbound connection
// identified by an "inbound:remoteAddr" key. Uses enqueueFrameSync if a
// send channel exists, otherwise falls back to direct write with a deadline
// to prevent head-of-line blocking.
//
// This is the inbound equivalent of enqueuePeerFrame for outbound sessions.
// Used by both announce_routes and relay_message paths.
//
// ctx is the caller's request/cycle context. It is propagated into the
// sync network write so that a pre-cancelled or mid-flight cancelled ctx
// aborts the wait instead of letting the NetCore writer burn through the
// full syncFlushTimeout (~5 s). This is the whole reason the per-cycle
// fan-out from fanoutAnnounceRoutes and request-scoped timeouts from
// ordinary RPC paths actually interrupt the send — the previous
// hard-coded s.runCtx meant those cancellations were silently dropped at
// routing-layer entry.
func (s *Service) writeFrameToInbound(ctx context.Context, address domain.PeerAddress, frame protocol.Frame) bool {
	remoteAddr := strings.TrimPrefix(string(address), "inbound:")

	var targetID domain.ConnID
	var found bool
	s.peerMu.RLock()
	s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
		if info.remoteAddr == remoteAddr {
			targetID = info.id
			found = true
			return false // Stop iteration
		}
		return true
	})
	s.peerMu.RUnlock()

	if !found {
		return false
	}

	return s.writeFrameToInboundConn(ctx, targetID, remoteAddr, frame)
}

// writeFrameToInboundConn marshals and writes a frame to a previously
// resolved inbound connection identified by ConnID. The connID is
// expected to come from a caller that has already located the target
// inbound conn under peerMu (e.g. writeFrameToInbound's foreach above,
// or the v2 dispatch helper that captures connID together with
// capability validation). Splitting the marshal+write tail of
// writeFrameToInbound into this helper lets the v2 path bind capability
// validation to the EXACT same connID that the network-sync write
// targets, closing the cap-check vs write race documented on
// dispatchAnnouncePlaneFrameWithCaps.
//
// remoteAddr is purely a logging tag — sendFrameBytesViaNetworkSync
// dispatches by connID, not address, so a churned replacement
// connection at the same remoteAddr cannot pick up the bytes.
func (s *Service) writeFrameToInboundConn(ctx context.Context, connID domain.ConnID, remoteAddr string, frame protocol.Frame) bool {
	line, err := protocol.MarshalFrameLine(frame)
	if err != nil {
		// Caller-side marshal failure — distinct diagnostic from a
		// transport drop. Kept on the caller so we can surface
		// frame_inbound_marshal_failed without going through the
		// network at all (the strict raw-bytes helper has no
		// marshal-fallback path; see network_consumer.go).
		log.Warn().Err(err).Str("peer", remoteAddr).Msg("frame_inbound_marshal_failed")
		return false
	}

	// Network-routed sync send for fail-fast inbound delivery via the
	// injected Network surface. The raw-bytes helper preserves the full
	// outcome tree:
	// nil → sent; ErrUnregisteredWrite → state-inconsistency branch; any
	// other non-nil (buffer-full / writer-done / chan-closed / sync-timeout
	// / ctx-error) → transport drop. The legacy 3-state enqueueResult
	// switch maps directly onto these classes.
	sendErr := s.sendFrameBytesViaNetworkSync(ctx, connID, []byte(line))
	switch {
	case sendErr == nil:
		return true
	case errors.Is(sendErr, ErrUnregisteredWrite):
		// Tracked inbound connection MUST have a NetCore. If it doesn't,
		// the state is inconsistent — fail closed rather than bypassing
		// the NetCore writer with a raw conn.Write.
		log.Warn().Str("peer", remoteAddr).Msg("frame_inbound_unregistered: tracked conn missing NetCore — state inconsistency")
		return false
	default:
		// Buffer full, writer/chan closed, sync flush timeout, ctx
		// canceled, or unknown sentinel — all collapse onto the legacy
		// frame_inbound_dropped diagnostic.
		log.Debug().Err(sendErr).Str("peer", remoteAddr).Msg("frame_inbound_dropped")
		return false
	}
}

// dispatchAnnouncePlaneFrameWithCaps captures the live transport at
// peerAddress while validating requiredCaps under a single peerMu RLock,
// then writes the frame to the captured handle (sendCh for outbound
// sessions, ConnID for inbound conns) outside the lock. Coupling the
// capability check to the actual write target closes the race between
// validation and write: a session-replacement at the same address can no
// longer slip a narrower transport into the dispatch, because the write
// targets the captured handle directly rather than re-resolving by
// address. peerMu is released BEFORE any blocking I/O, matching the
// CLAUDE.md prohibition on holding domain mutexes across network I/O.
//
// Returns false when:
//   - the address resolves to no live target,
//   - the captured target lacks any of requiredCaps (the gate must be
//     supplied by the caller — passing an empty slice is rejected as a
//     likely caller bug; v2 senders always pass at least one cap),
//   - the outbound session is unhealthy or stalled,
//   - the final write fails (channel full or transport error).
//
// The function assumes the caller is sending an announce-plane frame
// (announce_routes / routes_update / request_resync). Other planes
// should keep using sendAnnouncePlaneFrame / enqueuePeerFrame, which do
// not need the cap-validation coupling.
func (s *Service) dispatchAnnouncePlaneFrameWithCaps(
	ctx context.Context,
	peerAddress domain.PeerAddress,
	frame protocol.Frame,
	requiredCaps ...domain.Capability,
) bool {
	if len(requiredCaps) == 0 {
		return false
	}
	if strings.HasPrefix(string(peerAddress), "inbound:") {
		return s.dispatchInboundAnnouncePlaneFrameWithCaps(ctx, peerAddress, frame, requiredCaps)
	}
	return s.dispatchOutboundAnnouncePlaneFrameWithCaps(peerAddress, frame, requiredCaps)
}

// dispatchInboundAnnouncePlaneFrameWithCaps captures the inbound conn
// matching the "inbound:remoteAddr" suffix and validates requiredCaps
// against the same connInfo under one peerMu RLock. The captured ConnID
// is then handed to writeFrameToInboundConn — sendFrameBytesViaNetworkSync
// dispatches by ConnID (monotonic, never reused), so any replacement
// inbound connection at the same remoteAddr that arrived after our
// capture has its own fresh ConnID and never receives the bytes.
func (s *Service) dispatchInboundAnnouncePlaneFrameWithCaps(
	ctx context.Context,
	address domain.PeerAddress,
	frame protocol.Frame,
	requiredCaps []domain.Capability,
) bool {
	remoteAddr := strings.TrimPrefix(string(address), "inbound:")
	var (
		targetID domain.ConnID
		ok       bool
	)
	s.peerMu.RLock()
	s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
		if info.remoteAddr != remoteAddr {
			return true
		}
		for _, want := range requiredCaps {
			if !info.HasCapability(want) {
				return false // stop iteration; ok stays false
			}
		}
		targetID = info.id
		ok = true
		return false // stop iteration
	})
	s.peerMu.RUnlock()
	if !ok {
		return false
	}
	return s.writeFrameToInboundConn(ctx, targetID, remoteAddr, frame)
}

// dispatchOutboundAnnouncePlaneFrameWithCaps captures the outbound
// session sendCh under one peerMu RLock together with cap validation,
// health, and stalled-state checks, then enqueues to the captured
// channel outside the lock. peerSession.sendCh is owned by exactly one
// session — even if a replacement session at the same address opens
// after our capture, its sendCh is a different channel and does not
// receive our frame. The non-blocking select with default mirrors the
// existing enqueuePeerFrame contract.
func (s *Service) dispatchOutboundAnnouncePlaneFrameWithCaps(
	address domain.PeerAddress,
	frame protocol.Frame,
	requiredCaps []domain.Capability,
) bool {
	var (
		sendCh chan protocol.Frame
		ok     bool
	)
	s.peerMu.RLock()
	session := s.resolveSessionLocked(address)
	if session != nil {
		allMatch := true
		for _, want := range requiredCaps {
			found := false
			for _, c := range session.capabilities {
				if c == want {
					found = true
					break
				}
			}
			if !found {
				allMatch = false
				break
			}
		}
		if allMatch {
			health := s.health[s.resolveHealthAddress(address)]
			if health != nil && health.Connected &&
				s.computePeerStateAtLocked(health, time.Now().UTC()) != peerStateStalled {
				sendCh = session.sendCh
				ok = true
			}
		}
	}
	s.peerMu.RUnlock()
	if !ok {
		return false
	}
	select {
	case sendCh <- frame:
		return true
	default:
		return false
	}
}

// routingCapablePeers returns all peers (outbound sessions AND inbound
// connections) that have negotiated both mesh_routing_v1 and mesh_relay_v1
// capabilities. Used by AnnounceLoop to discover announcement targets.
// A peer that appears in both maps is deduplicated by identity.
//
// Both capabilities are required because a routing-only peer (mesh_routing_v1
// without mesh_relay_v1) cannot carry data-plane relay traffic. Advertising
// routes through such a peer would create non-deliverable paths.
//
// Capabilities plumbing: for every peer that passes the v1 filter, a full
// immutable copy of the peer's negotiated capability slice is attached to
// the AnnounceTarget. The copy is taken inside the same s.peerMu.RLock that
// reads Address and Identity, giving the announce cycle a single consistent
// per-peer snapshot. Callers (the announce loop and its per-peer goroutines)
// can therefore pick a wire format without re-entering s.peerMu per peer —
// that re-entry pattern collides with the writer-preferring RWMutex
// semantics documented in CLAUDE.md and docs/locking.md.
func (s *Service) routingCapablePeers() []routing.AnnounceTarget {
	s.peerMu.RLock()
	defer s.peerMu.RUnlock()

	seen := make(map[domain.PeerIdentity]struct{})
	var targets []routing.AnnounceTarget

	// Outbound sessions — one announce per identity, even if multiple
	// sessions exist for the same peer.
	for address, session := range s.sessions {
		if session.peerIdentity == "" {
			continue
		}
		if _, dup := seen[session.peerIdentity]; dup {
			continue
		}
		if sessionHasBothCaps(session.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			seen[session.peerIdentity] = struct{}{}
			targets = append(targets, routing.AnnounceTarget{
				Address:      domain.PeerAddress(address),
				Identity:     session.peerIdentity,
				Capabilities: copyCapabilitiesForAnnounce(session.capabilities),
			})
		}
	}

	// Inbound connections — only if identity not already covered by an
	// outbound session above (dedup by identity).
	s.forEachTrackedInboundConnLocked(func(info connInfo) bool {
		if info.identity == "" {
			return true
		}
		if _, dup := seen[info.identity]; dup {
			return true
		}
		if sessionHasBothCaps(info.capabilities, domain.CapMeshRoutingV1, domain.CapMeshRelayV1) {
			seen[info.identity] = struct{}{}
			// MUST use inboundConnKeyFromInfo (snapshot field), not
			// inboundConnKeyForID — the latter re-enters s.peerMu.RLock
			// via Network().RemoteAddr and deadlocks as soon as any
			// writer queues between the outer and inner acquisition.
			//
			// info.capabilities is already a fresh copy produced by
			// snapshotEntryLocked, but an additional copy is taken here so
			// that all AnnounceTarget slices come from the same producer
			// and the contract "AnnounceTarget.Capabilities is private to
			// the target" does not depend on connInfo internals.
			targets = append(targets, routing.AnnounceTarget{
				Address:      inboundConnKeyFromInfo(info),
				Identity:     info.identity,
				Capabilities: copyCapabilitiesForAnnounce(info.capabilities),
			})
		}
		return true
	})

	return targets
}

// copyCapabilitiesForAnnounce returns an immutable copy of caps typed as
// []routing.PeerCapability for embedding into routing.AnnounceTarget. A nil
// input yields a nil slice so "no capabilities" stays distinguishable from
// "empty capability set" at the consumer side. Callers must hold s.peerMu
// while the source slice is still backed by session/connInfo state — this
// helper only guarantees that the returned slice no longer aliases it.
func copyCapabilitiesForAnnounce(caps []domain.Capability) []routing.PeerCapability {
	if len(caps) == 0 {
		return nil
	}
	out := make([]routing.PeerCapability, len(caps))
	copy(out, caps)
	return out
}

// sendFullTableSyncToInbound sends the current routing table to a newly
// connected inbound peer. This is the inbound-path counterpart of the
// outbound full-table sync (Phase 1.2: always full sync on connect).
// Without this, inbound-only peers would wait until the next periodic
// or triggered announce cycle before learning the current table.
//
// The per-peer announce cache is updated on successful send so that the
// next announce cycle can compute a meaningful delta instead of
// re-sending the full table.
//
// ctx bounds the whole connect-time sync: callers pass s.runCtx so that
// service shutdown aborts a half-flushed inbound write instead of letting
// NetCore.SendRawSync burn through the full syncFlushTimeout.
func (s *Service) sendFullTableSyncToInbound(ctx context.Context, id domain.ConnID, peerIdentity domain.PeerIdentity) {
	log.Trace().Uint64("conn_id", uint64(id)).Str("peer_identity", string(peerIdentity)).Msg("send_full_table_sync_inbound_begin")
	if peerIdentity == "" {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("send_full_table_sync_inbound_no_identity")
		return
	}
	if s.Network().RemoteAddr(id) == "" {
		log.Trace().Uint64("conn_id", uint64(id)).Msg("send_full_table_sync_inbound_conn_gone")
		return
	}

	sendAddr := s.inboundConnKeyForID(id)
	log.Trace().Uint64("conn_id", uint64(id)).Str("send_addr", string(sendAddr)).Msg("send_full_table_sync_inbound_before_connect_time")
	s.sendConnectTimeFullSync(ctx, peerIdentity, sendAddr)
	log.Trace().Uint64("conn_id", uint64(id)).Msg("send_full_table_sync_inbound_end")
}

// sendOutboundFullTableSync sends the current routing table to an outbound
// peer and updates the per-peer announce cache on success. This is the
// outbound-path counterpart of sendFullTableSyncToInbound. Both paths
// delegate to sendConnectTimeFullSync which builds a canonical snapshot
// so that subsequent announce cycles can compute meaningful deltas.
//
// ctx is threaded through so that caller-scope cancellation (session
// teardown, service shutdown) interrupts the send instead of blocking
// for the full syncFlushTimeout on a stuck writer.
func (s *Service) sendOutboundFullTableSync(ctx context.Context, peerIdentity domain.PeerIdentity, address domain.PeerAddress) {
	s.sendConnectTimeFullSync(ctx, peerIdentity, address)
}

// sendConnectTimeFullSync is the shared core for inbound and outbound
// connect-time full table sync. It builds a canonical snapshot of routes
// visible to the peer (split horizon applied), sends them via
// SendAnnounceRoutes, and updates the per-peer announce cache on
// success. When the snapshot is empty, the empty baseline is recorded
// without sending a wire frame — the protocol is additive so an empty
// table needs no explicit announcement.
//
// First-sync wire-frame invariant: see the "First-sync wire-frame
// invariant" section in docs/routing.md for the normative contract.
// Connect-time sync MUST use SendAnnounceRoutes (legacy announce_routes
// frame); the first sync after session establishment is always legacy
// regardless of peer capabilities, because a v2 routes_update frame
// carries an incremental delta against a baseline the peer does not yet
// have. Any future change that inspects peer capabilities here and picks
// SendRoutesUpdate breaks that contract — both because a fresh session
// has no baseline and because the guard tests in
// routing_integration_connect_sync_test.go will fail immediately. The
// empty-baseline short-circuit below emits neither wire frame (legacy
// nor v2) by design; see TestConnectTimeFullSync_EmptySnapshot_NoWireFrame
// for the regression contract.
//
// ctx flows down into SendAnnounceRoutes → writeFrameToInbound →
// sendFrameBytesViaNetworkSync; cancelling ctx aborts the inbound
// sync-flush wait rather than waiting for the internal syncFlushTimeout.
func (s *Service) sendConnectTimeFullSync(ctx context.Context, peerIdentity domain.PeerIdentity, address domain.PeerAddress) {
	log.Trace().Str("peer_identity", string(peerIdentity)).Str("address", string(address)).Msg("connect_time_full_sync_begin")
	routes := s.routingTable.AnnounceTo(peerIdentity)
	snapshot := routing.BuildAnnounceSnapshot(routes)
	registry := s.announceLoop.StateRegistry()

	now := registry.Clock()
	peerState := registry.GetOrCreate(peerIdentity)
	log.Trace().Str("peer_identity", string(peerIdentity)).Int("entries", len(snapshot.Entries)).Msg("connect_time_full_sync_snapshot_built")

	if len(snapshot.Entries) == 0 {
		// No routes to send, but register the empty baseline so that future
		// announce cycles can compute meaningful deltas. No wire frame is
		// needed — the protocol is additive (not a destructive snapshot), so
		// an empty table is correctly represented by sending nothing. This
		// branch is orthogonal to the first-sync wire-frame invariant (see
		// docs/routing.md): empty-baseline emits neither the legacy
		// announce_routes frame nor the v2 routes_update frame.
		peerState.RecordFullSyncSuccess(snapshot, now)
		log.Debug().
			Str("peer", string(peerIdentity)).
			Str("address", string(address)).
			Msg("routing_connect_time_full_sync_empty_baseline")
		return
	}

	peerState.RecordFullSyncAttempt(now)

	log.Trace().Str("peer_identity", string(peerIdentity)).Str("address", string(address)).Int("routes", len(snapshot.Entries)).Msg("connect_time_full_sync_before_send")
	// First-sync legacy path (see docs/routing.md §"First-sync wire-frame
	// invariant"): always SendAnnounceRoutes, never SendRoutesUpdate — the
	// peer has no baseline to diff against on a freshly established session.
	sendOk := s.SendAnnounceRoutes(ctx, address, snapshot.Entries)
	log.Trace().Str("peer_identity", string(peerIdentity)).Str("address", string(address)).Bool("sent", sendOk).Msg("connect_time_full_sync_after_send")
	if !sendOk {
		log.Warn().
			Str("peer", string(peerIdentity)).
			Str("address", string(address)).
			Int("routes", len(snapshot.Entries)).
			Msg("routing_connect_time_full_sync_failed")
		return
	}

	// A non-empty connect-time announce_routes frame is the very first
	// observable baseline on the wire for this session. Flip the send-side
	// flag so AnnounceLoop's v2 mode selection knows the peer's v2 receive
	// gate is open and subsequent deltas may use SendRoutesUpdate when caps
	// agree. The empty-snapshot branch above intentionally does NOT flip
	// this flag — it records a local baseline without emitting any wire
	// frame, so the peer never observed one.
	peerState.MarkWireBaselineSent()
	peerState.RecordFullSyncSuccess(snapshot, now)
	log.Trace().Str("peer_identity", string(peerIdentity)).Str("address", string(address)).Msg("connect_time_full_sync_end")
}

// handleAnnounceRoutes processes an incoming announce_routes frame from a
// peer. Each route entry is inserted into the local routing table with +1
// hop (the wire carries the sender's local hop count; the receiver adds 1).
//
// Withdrawals (hops=16) are applied directly via Table.WithdrawRoute.
// Normal routes are inserted via Table.UpdateRoute with source=announcement.
//
// An announce_routes arrival is also the baseline signal for the v2
// routes_update receive path: AnnouncePeerState.MarkBaselineReceived is
// set so subsequent routes_update deltas from the peer are safe to apply
// against the known-good first-sync snapshot. See docs/routing.md
// "First-sync wire-frame invariant" for the protocol-level contract.
func (s *Service) handleAnnounceRoutes(senderIdentity domain.PeerIdentity, frame protocol.Frame) {
	if !identity.IsValidAddress(string(senderIdentity)) {
		log.Warn().Str("sender", string(senderIdentity)).Msg("announce_routes_malformed_sender")
		return
	}
	s.applyAnnounceEntries(senderIdentity, frame.AnnounceRoutes, announceReceiveLegacy)
}

// handleRoutesUpdate processes an incoming routes_update frame (v2 wire
// path). The entry-by-entry application is identical to handleAnnounceRoutes
// — the delta frame carries the same AnnounceRouteFrame payload shape —
// but the receive path gates delta application on the baseline: if the
// current session has not received announce_routes yet,
// AnnouncePeerState.HasReceivedBaseline is false and the receiver MUST
// ask the peer for a forced full sync (via request_resync) instead of
// silently accepting the delta against a stale or missing state.
//
// senderAddress is the peer's routing-key address (outbound session or
// "inbound:" prefix) used to dispatch the request_resync reply.
func (s *Service) handleRoutesUpdate(senderIdentity domain.PeerIdentity, senderAddress domain.PeerAddress, frame protocol.Frame) {
	if !identity.IsValidAddress(string(senderIdentity)) {
		log.Warn().Str("sender", string(senderIdentity)).Msg("routes_update_malformed_sender")
		return
	}

	peerState := s.announceLoop.StateRegistry().GetOrCreate(senderIdentity)
	if !peerState.HasReceivedBaseline() {
		// Protocol desync: peer sent a delta before (or independently of)
		// the first-sync baseline this session owes us. The safe recovery
		// is to ask the peer to resync — do NOT attempt to apply the delta
		// against whatever local state happens to exist, because that
		// state is either empty (fresh session) or belongs to a prior
		// session whose SeqNo space may have rolled over.
		log.Warn().
			Str("from", string(senderIdentity)).
			Str("address", string(senderAddress)).
			Int("delta_routes", len(frame.AnnounceRoutes)).
			Msg("routes_update_before_baseline_request_resync")
		if senderAddress == "" {
			// No sendback path available (e.g. unit test path). Without a
			// peer address the request_resync wire frame cannot be
			// delivered. This is not a silent drop: MarkInvalid on the
			// local side still has no effect because the peer is the one
			// that must forget its per-peer send state; we return here
			// and let the next MarkReconnected hook reset our state.
			return
		}
		s.SendRequestResync(s.runCtx, senderAddress)
		return
	}

	s.applyAnnounceEntries(senderIdentity, frame.AnnounceRoutes, announceReceiveV2)
}

// handleRequestResync processes an incoming request_resync frame. The peer
// has detected local state desync (e.g. it received a routes_update
// without a baseline this session) and is asking us to re-deliver a full
// baseline via legacy announce_routes. We fulfil the contract by marking
// our per-peer announce state as NeedsFullResync and triggering an
// immediate announce cycle — the forced-full branch of announceToAllPeers
// takes over from there, and sendFullAnnounce honours the first-sync
// invariant (legacy announce_routes wire frame) regardless of the peer's
// negotiated capabilities.
func (s *Service) handleRequestResync(senderIdentity domain.PeerIdentity) {
	if !identity.IsValidAddress(string(senderIdentity)) {
		log.Warn().Str("sender", string(senderIdentity)).Msg("request_resync_malformed_sender")
		return
	}
	log.Info().
		Str("from", string(senderIdentity)).
		Msg("request_resync_received_forcing_full_sync")
	s.announceLoop.StateRegistry().MarkInvalid(senderIdentity)
	s.announceLoop.TriggerUpdate()
}

// announceReceiveMode tags the wire path that delivered the announce-plane
// entries. Used by applyAnnounceEntries to select the baseline-update side
// effect (set only on legacy announce_routes — routes_update never
// establishes a baseline by itself).
type announceReceiveMode int

const (
	announceReceiveLegacy announceReceiveMode = iota
	announceReceiveV2
)

// applyAnnounceEntries is the shared receive-side core for both the legacy
// announce_routes and the v2 routes_update wire frames. The entry-by-entry
// rules are identical — trust classification, own-origin forgery guard,
// transit-withdrawal guard, Table.UpdateRoute / WithdrawRoute dispatch —
// so the per-entry loop lives in one place to avoid the 3-copy duplication
// CLAUDE.md forbids.
//
// The mode parameter controls one side effect: legacy wire frames set the
// per-session "baseline received" flag so subsequent routes_update deltas
// are safe to apply; the v2 path leaves the flag untouched because
// routes_update never establishes a baseline by itself (see
// handleRoutesUpdate for the gating contract).
func (s *Service) applyAnnounceEntries(senderIdentity domain.PeerIdentity, wireRoutes []protocol.AnnounceRouteFrame, mode announceReceiveMode) {
	if senderIdentity == "" {
		log.Warn().Msg("announce_routes_no_sender_identity")
		return
	}

	accepted := 0
	unchanged := 0
	rejected := 0
	drainIdentities := make(map[domain.PeerIdentity]struct{})

	for _, wireRoute := range wireRoutes {
		if wireRoute.Identity == "" || wireRoute.Origin == "" {
			rejected++
			continue
		}

		// Reject malformed identity fingerprints early — before any table
		// operations. Valid addresses are exactly 40 lowercase hex chars
		// (SHA-256 of Ed25519 public key, truncated to 20 bytes).
		if !identity.IsValidAddress(wireRoute.Identity) || !identity.IsValidAddress(wireRoute.Origin) {
			rejected++
			log.Warn().
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("announce_rejected_malformed_address")
			continue
		}

		// Skip routes about ourselves — we know our own connectivity.
		if wireRoute.Identity == s.identity.Address {
			continue
		}

		// Reject own-origin forgery: a foreign sender must never advertise
		// routes with Origin == our identity. Only this node may originate
		// routes under its own identity. Accepting such an announcement
		// would let a neighbor poison our monotonic SeqNo counter via
		// syncSeqCounterLocked, breaking the per-origin SeqNo invariant.
		if wireRoute.Origin == s.identity.Address {
			rejected++
			log.Debug().
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("announce_rejected_forged_own_origin")
			continue
		}

		if wireRoute.Hops >= routing.HopsInfinity {
			// Withdrawal: only the origin may emit a wire withdrawal.
			// Transit nodes that lose upstream must invalidate locally and
			// stop advertising — they must not forward hops=16 on the wire.
			// Accepting a withdrawal from a non-origin sender would let an
			// intermediate neighbor unilaterally kill a route it does not own.
			if wireRoute.Origin != string(senderIdentity) {
				rejected++
				log.Warn().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Msg("announce_rejected_transit_withdrawal")
				continue
			}

			withdrawnID := domain.PeerIdentity(wireRoute.Identity)
			if s.routingTable.WithdrawRoute(
				withdrawnID,
				domain.PeerIdentity(wireRoute.Origin),
				senderIdentity,
				wireRoute.SeqNo,
			) {
				accepted++
				// After withdrawing the best triple, a less-preferred backup
				// route may now be the active path. If Lookup still returns
				// reachable entries for this identity, trigger a drain so
				// pending send_message frames can be delivered via the backup
				// route immediately instead of waiting for the retry loop.
				if remaining := s.routingTable.Lookup(withdrawnID); len(remaining) > 0 {
					drainIdentities[withdrawnID] = struct{}{}
				}
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Uint64("seq", wireRoute.SeqNo).
					Str("from", string(senderIdentity)).
					Msg("route_withdrawal_applied")
			} else {
				rejected++
			}
			continue
		}

		// Normal route: add +1 hop (receiver convention).
		receivedHops := wireRoute.Hops + 1
		if receivedHops > routing.HopsInfinity {
			receivedHops = routing.HopsInfinity
		}

		// ExpiresAt is left zero — Table.UpdateRoute applies the table's
		// own defaultTTL and clock, ensuring consistent TTL policy between
		// local (AddDirectPeer) and learned (announcement) routes.
		entry := routing.RouteEntry{
			Identity: domain.PeerIdentity(wireRoute.Identity),
			Origin:   domain.PeerIdentity(wireRoute.Origin),
			NextHop:  senderIdentity,
			Hops:     receivedHops,
			SeqNo:    wireRoute.SeqNo,
			Source:   routing.RouteSourceAnnouncement,
			Extra:    wireRoute.Extra,
		}

		status, err := s.routingTable.UpdateRoute(entry)
		if err != nil {
			log.Warn().
				Err(err).
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", string(senderIdentity)).
				Msg("route_update_rejected_invalid")
			rejected++
			continue
		}
		switch status {
		case routing.RouteAccepted:
			accepted++
			drainIdentities[domain.PeerIdentity(wireRoute.Identity)] = struct{}{}
		case routing.RouteUnchanged:
			unchanged++
			drainIdentities[domain.PeerIdentity(wireRoute.Identity)] = struct{}{}
		case routing.RouteRejected:
			rejected++
			existing := s.routingTable.InspectTriple(entry.DedupKey())
			if existing != nil {
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Int("incoming_hops", receivedHops).
					Uint64("incoming_seq", wireRoute.SeqNo).
					Int("existing_hops", existing.Hops).
					Uint64("existing_seq", existing.SeqNo).
					Str("existing_source", existing.Source.String()).
					Bool("existing_withdrawn", existing.IsWithdrawn()).
					Str("existing_expires", existing.ExpiresAt.Format(time.RFC3339)).
					Msg("route_rejected_by_existing")
			} else {
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", string(senderIdentity)).
					Int("incoming_hops", receivedHops).
					Uint64("incoming_seq", wireRoute.SeqNo).
					Msg("route_rejected_no_existing_triple")
			}
		}
	}

	wireType := "announce_routes"
	if mode == announceReceiveV2 {
		wireType = "routes_update"
	}
	log.Debug().
		Str("from", string(senderIdentity)).
		Str("wire_type", wireType).
		Int("total", len(wireRoutes)).
		Int("accepted", accepted).
		Int("unchanged", unchanged).
		Int("rejected", rejected).
		Msg("announce_routes_processed")

	if accepted > 0 {
		s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
			Reason:   domain.RouteChangeAnnouncement,
			PeerID:   senderIdentity,
			Accepted: accepted,
		})
	}

	// Baseline tracking: a legacy announce_routes arrival is the protocol
	// signal that the peer has delivered the first-sync snapshot for this
	// session. Subsequent routes_update deltas from the peer can now be
	// applied safely. The routes_update wire path deliberately leaves the
	// flag untouched — see handleRoutesUpdate for the gating contract.
	if mode == announceReceiveLegacy {
		s.announceLoop.StateRegistry().GetOrCreate(senderIdentity).MarkBaselineReceived()
	}

	// Event-driven pending queue drain: new or reconfirmed transit routes
	// mean that own outbound frames waiting for these recipients can be
	// delivered via the learned next-hop. Both accepted (new/improved) and
	// unchanged (reconfirmed alive) routes qualify — a reconnected peer
	// re-announcing the same table still proves the path is alive.
	// Batched after the entire announce is processed to avoid per-entry
	// scans during high-volume table syncs.
	//
	// Fast-path: skip goroutine launch when the pending queue is empty.
	// This avoids unnecessary Lock contention on every announce_routes,
	// which was causing inter-test timing regressions in full package runs.
	if len(drainIdentities) > 0 {
		s.deliveryMu.RLock()
		hasPending := len(s.pending) > 0
		s.deliveryMu.RUnlock()
		if hasPending {
			s.goBackground(func() { s.drainPendingForIdentities(drainIdentities) })
		}
	}
}

// fanoutAnnounceRoutes dispatches the same announce_routes frame
// concurrently to every peer in targets. Each peer is handled by a
// dedicated goroutine so that one stuck inbound socket — bounded per
// peer by syncFlushTimeout — cannot serialise delivery to the others.
//
// The helper returns once every peer goroutine has finished, either
// because SendAnnounceRoutes returned or because ctx was cancelled.
// Per-peer outcomes are aggregated into (sent, dropped) and surfaced
// through the caller's log line so operators see fan-out health
// without parsing per-peer noise.
//
// Reentrancy: SendAnnounceRoutes itself is safe to call from several
// goroutines in parallel. Inbound delivery takes a brief s.peerMu.RLock
// inside writeFrameToInbound and releases it before the blocking
// sendFrameBytesViaNetworkSync call; outbound delivery pushes onto the
// per-session send channel which owns its own synchronisation. No
// shared Service state is mutated here.
//
// ctx propagates end-to-end: a pre-cancelled ctx causes every per-peer
// goroutine to count as dropped before touching the transport; a
// mid-flight cancel (e.g. service shutdown) interrupts the inbound
// sync-flush wait inside NetCore.SendRawSyncCtx instead of consuming
// the full syncFlushTimeout. The ctx threaded through SendAnnounceRoutes
// is the same ctx observed by each goroutine, so routing-layer
// cancellation is no longer silently upgraded to s.runCtx at writeFrameToInbound.
func (s *Service) fanoutAnnounceRoutes(
	ctx context.Context,
	targets []routing.AnnounceTarget,
	withdrawals []routing.AnnounceEntry,
) (sent, dropped int) {
	if len(targets) == 0 || len(withdrawals) == 0 {
		return 0, 0
	}

	var sentCnt, droppedCnt atomic.Int32
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for _, target := range targets {
		go func(peer routing.AnnounceTarget) {
			defer wg.Done()
			defer crashlog.DeferRecover()
			// Early-abort when the Service is already shutting down —
			// avoids blocking a per-peer goroutine for up to
			// syncFlushTimeout when the outer context is cancelled.
			if err := ctx.Err(); err != nil {
				droppedCnt.Add(1)
				return
			}
			if s.SendAnnounceRoutes(ctx, peer.Address, withdrawals) {
				sentCnt.Add(1)
				// A real announce_routes frame went out to this recipient.
				// From the recipient's perspective this is the wire baseline
				// for its v2 receive gate, so flip the symmetric send-side
				// flag for that peer's announce state. Without this, a peer
				// that only received its baseline as part of a disconnect
				// fan-out would later have its first delta downgraded to
				// legacy because the announce loop still believed no
				// baseline had been emitted to it.
				if state := s.announceLoop.StateRegistry().Get(peer.Identity); state != nil {
					state.MarkWireBaselineSent()
				}
			} else {
				droppedCnt.Add(1)
			}
		}(target)
	}
	wg.Wait()
	return int(sentCnt.Load()), int(droppedCnt.Load())
}

// triggerDrainForExposed converts a slice of routing.PeerIdentity (identities
// with newly exposed backup routes) into the identities map format and launches
// drainPendingForIdentities if the pending queue is non-empty. Used by
// onPeerSessionClosed (disconnect-triggered drain), routingTableTTLLoop
// (TTL-expiry drain), and handleAnnounceRoutes (withdrawal drain) to share
// the drain-trigger logic.
func (s *Service) triggerDrainForExposed(exposed []routing.PeerIdentity) {
	if len(exposed) == 0 {
		return
	}
	identities := make(map[domain.PeerIdentity]struct{}, len(exposed))
	for _, id := range exposed {
		identities[domain.PeerIdentity(id)] = struct{}{}
	}
	s.deliveryMu.RLock()
	hasPending := len(s.pending) > 0
	s.deliveryMu.RUnlock()
	if hasPending {
		s.goBackground(func() { s.drainPendingForIdentities(identities) })
	}
}
