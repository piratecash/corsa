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
	"crypto/ed25519"
	"encoding/base64"
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

// isAnnouncePlaneFrameType reports whether a frame type belongs to the
// announce plane (announce_routes, routes_update, request_resync,
// route_announce_v3, route_poison_v1, route_poison_v2). These frames are
// subject to the strict 128 KiB MaxFrameLine budget even when received over
// peer sessions whose generic line reader accepts up to 8 MiB, because the
// writer side (chunkAnnounceEntriesBySize + the v3 chunker + the poison
// emitters, including the v2 batch chunker at poisonBatchChunkSize) caps them
// at MaxFrameLine and a wider receive budget would let sender and receiver
// diverge on which routes a peer is willing to carry. route_poison_v1 is a
// fixed-size control signal (~200B) and route_poison_v2 is chunked to stay
// under the cap, so the budget is purely a defence: a peer that pads a poison
// frame past 128 KiB is buggy or hostile, and bypassing the cap on poison
// alone would leave us doing base64+ed25519 verify on a multi-megabyte input
// the writer side could never legitimately emit.
// Used by readPeerSession to drop oversize announce-plane frames
// before they reach the routing table.
func isAnnouncePlaneFrameType(frameType string) bool {
	switch frameType {
	case "announce_routes", "routes_update", "request_resync",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
		protocol.RoutePoisonV2FrameType:
		return true
	default:
		return false
	}
}

// isAnnouncePlaneBulkFrameType is the narrower predicate used to
// exempt frames from the inbound per-connection command rate
// limiter (service.go inbound read loop). It covers ONLY the
// bulk/chunked announce path:
//
//	announce_routes (legacy v1), routes_update (v2 delta),
//	route_announce_v3 (v3 compact wire).
//
// These frames batch up to maxRoutesPerAnnounceFrame (100) entries
// each, so a legitimate full-sync of N routes ships as ceil(N/100)
// frames in a tight burst — the cmd limiter's 100-burst / 30 cmd/s
// would silently truncate that burst. Their per-peer rate is
// instead governed by:
//
//   - announceLimiter (route-count budget: 10k burst, 200/s refill) —
//     applies to ALL bulk frames, baselines included.
//   - chatty_routes quarantine trigger (50 frames/s × 10s = 500) —
//     applies to DELTA frames only (routes_update, v3 kind="delta").
//     Full baselines (announce_routes, v3 kind="full") are excluded:
//     a baseline is idempotent and bounded by the route bucket, while
//     the chatty trigger targets the cascading work of delta churn.
//     See handleAnnounceRoutes / recordInboundAnnounceAndMaybeArm.
//
// IMPORTANT: this predicate DOES NOT include request_resync,
// route_poison_v1 or route_poison_v2. Those are control frames — a
// peer's natural rate is bounded by reconnect cycles (request_resync)
// or route lifecycle events (the poison frames), well under 1/s. The
// cmd limiter's 30/s is plenty for them; flooding them past that rate
// is protocol-level misbehaviour, not chattiness, and the TCP close
// that cmd limit produces is the appropriate response. Exempting
// control frames would also leave only the (loose) per-route bucket as
// the per-peer defence — at 200 tokens/s with cost=1 per unit-cost control
// frame (request_resync / route_poison_v1) that is 200/s sustained, far
// above legitimate use — without any chatty trigger to compensate.
// route_poison_v2 is NOT unit-cost: it charges len(identities) tokens, so
// its per-route bound is tighter still.
//
// The broader isAnnouncePlaneFrameType (above) still covers the
// control types for SIZE-budget enforcement (peer_management.go's
// MaxFrameLine check), which IS uniform across all six types.
func isAnnouncePlaneBulkFrameType(frameType string) bool {
	switch frameType {
	case "announce_routes", "routes_update",
		protocol.RouteAnnounceV3FrameType:
		return true
	default:
		return false
	}
}

// isRawLineBackedFrameType reports whether the frame type's dispatch
// re-parses the payload from Frame.RawLine rather than the universal
// Frame fields. This is the set of Phase 2+ / Phase 4 frames that use
// the RawLine bypass pattern to keep capability-gated wire shapes
// compile-time isolated from the universal Frame struct.
//
// Inbound (NetCore) dispatchers receive the raw `line` directly as a
// parameter so they don't need RawLine on the parsed Frame. The
// outbound peer-session reader, by contrast, parses each line via
// protocol.ParseFrameLine — which intentionally does NOT populate
// Frame.RawLine — then pushes the parsed Frame to session.inboxCh,
// dropping the raw bytes on the floor. Dispatch later calls
// Unmarshal*Frame([]byte(frame.RawLine)) and silently drops the frame
// when RawLine is empty. Populating RawLine for these specific types
// (and only these) right after ParseFrameLine in the session reader
// closes the gap without changing the marshal short-circuit semantic
// for frames that don't use the bypass.
func isRawLineBackedFrameType(frameType string) bool {
	switch frameType {
	case "route_sync_digest_v1", "route_sync_summary_v1",
		protocol.RouteAnnounceV3FrameType,
		protocol.RoutePoisonFrameType,
		protocol.RoutePoisonV2FrameType:
		return true
	default:
		return false
	}
}

// maxRoutesPerAnnounceFrame is the Phase 4 13.5 hard cap on the
// number of route entries any single announce-plane frame can carry,
// independently of the byte-size budget (MaxFrameLine). The cap
// bounds receive-side per-frame work (decode + per-entry trust
// classification + UpdateRoute admission) so a peer cannot deliver
// thousands of entries in a single oversized-but-still-under-128 KiB
// frame and tie up the storage writer. Chunking continues to flush
// on either the byte budget or this entry-count budget, whichever
// fires first. Mirrored in the v3 chunker
// (chunkRouteAnnounceV3EntriesBySize) so the cap applies uniformly
// across wire generations.
//
// Receive-side enforcement. Send-side chunking guarantees this node
// never emits a frame with more than maxRoutesPerAnnounceFrame
// entries, but a remote peer is not bound by our chunker — a buggy
// or hostile peer can pack thousands of entries into a single
// announce_routes / routes_update / route_announce_v3 frame, then
// hand us the work to decode and classify every one. The receive-
// path handlers (handleAnnounceRoutes, handleRoutesUpdate,
// handleRouteAnnounceV3) therefore drop any frame whose entry
// count exceeds the cap BEFORE invoking applyAnnounceEntries, so
// the bound is true on both ends of the wire (not just a polite
// suggestion our chunker happens to follow).
//
// Fairness rotation note. The original phase-4 §3.5 also called
// for fairness rotation (direct always included, offset rotation
// for the rest) when a peer has more routes than fit in one frame.
// In the current multi-frame-per-cycle send path the chunker emits
// EVERY route in a cycle — just spread across multiple frames — so
// no entry is "starved" and rotation is a no-op for the steady
// state. Rotation becomes meaningful only if a future change adds
// a single-frame-per-cycle truncation; that work lands as a fresh
// PR with the rotation state on AnnouncePeerState alongside the
// truncation knob.
const maxRoutesPerAnnounceFrame = 100

// announceCostForEntries returns the rate-limiter token cost for an
// announce-plane frame that carries `n` route entries. Empty
// announce frames (n == 0) still cost 1 token — the wire frame
// itself consumes a small slice of work (parse + dispatch), and
// charging 0 would let a hostile peer flood empty frames without
// throttle. Non-empty frames cost exactly len(entries) tokens,
// matching the per-entry trust-classification + UpdateRoute work
// the receive path actually does. Used by handleAnnounceRoutes /
// handleRoutesUpdate / handleRouteAnnounceV3 — see
// announceRateLimiter.allow doc-comment for the Round-10 rationale
// behind charging by route count rather than per-frame.
func announceCostForEntries(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// jsonStringEscapeOverhead returns an UPPER BOUND on the EXTRA bytes Go's JSON
// encoder adds when it serialises v as (part of) a JSON string.  A byte is
// guaranteed to be emitted 1:1 only when it is printable ASCII other than the
// five characters the encoder escapes ('"', '\\', '<', '>', '&').  Every
// other byte — ASCII control chars (→ "\uXXXX" or short escapes), the five
// escaped characters, and all bytes ≥ 0x7F (UTF-8 lead/continuation, which
// may encode the always-escaped U+2028 / U+2029 line/para separators) — is
// charged the maximum single-byte expansion of +5 (one byte → a six-byte
// "\uXXXX" escape).  That over-charges '"'/'\\' (really +1) and ordinary
// UTF-8 (really +0), but over-estimating is exactly what the fast-accept gate
// needs: it can never let an entry whose real wire form expands past the
// budget slip through without a precise check.  Clean hex / base64url
// fingerprints and ASCII Extra charge 0 (or near 0), so the hot path is
// untouched.
//
// Generic over ~string | ~[]byte so it serves PeerIdentity (string) and
// json.RawMessage ([]byte) without allocating a conversion.
func jsonStringEscapeOverhead[T ~string | ~[]byte](v T) int {
	const perByteEscape = 5 // one source byte → "\uXXXX" is +5 bytes.
	n := 0
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 0x20 && c < 0x7f && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
			continue // emitted unchanged
		}
		n += perByteEscape
	}
	return n
}

// announceEntryWireEstimate returns a deliberate OVER-estimate of the JSON
// bytes a single AnnounceEntry contributes to an announce frame (legacy/v2
// AnnounceRouteFrame and the v3 RouteAnnounceV3Entry — whichever is larger,
// so one estimate is safe for both chunkers).  It exists only so the
// chunkers can decide when a chunk is comfortably under budget and SKIP the
// expensive precise marshal; it must never under-estimate, or an oversize
// chunk could slip past the cheap gate and the send path would then fail the
// whole chunk WITHOUT the per-entry skip+report the precise path performs.
//
// Every variable-length string field is charged its raw length PLUS its
// worst-case HTML-escape expansion (jsonStringEscapeOverhead).  This matters
// because AnnounceRouteFrame.MarshalJSON (and the v3 marshaller) round-trips
// the data through Go's encoder, whose post-Marshaler compaction re-escapes
// '<', '>' and '&' (one byte → six) in both keys and values.  Crucially this
// is applied to Identity and Origin as well as Extra: RouteEntry.Validate
// does NOT enforce a hex/base64url shape, so a malicious or buggy route can
// carry raw '<' bytes in its identity that balloon ~6× on re-announce.
// Counting the escape overhead per-field keeps the estimate a true upper
// bound for arbitrary content while charging clean fingerprints zero, so the
// dominant hot path (small ASCII entries, nil Extra) still fast-accepts with
// no marshal.  AttestedSig is base64 (escape-free alphabet), so it is charged
// 1× plus its "sig" wrapper.
//
// The precise byte budget is still enforced by the real marshal at the
// boundary — this estimate only governs the fast path, where its job is to
// be conservatively large, not exact.
func announceEntryWireEstimate(e routing.AnnounceEntry) int {
	// fixedSlack covers: {"identity":"","origin":"","hops":N,"seq_no":N,
	// "extra":}, the trailing comma, and worst-case 20-digit hops/seq_no.
	const fixedSlack = 160
	n := fixedSlack
	// A 20-byte fingerprint serialises as 40-char lowercase hex, which
	// never needs JSON escaping — so the wire cost is exactly 2×len, no
	// escape overhead.
	n += 2 * len(e.Identity)
	n += 2 * len(e.Origin)
	n += len(e.Extra) + jsonStringEscapeOverhead(e.Extra)
	if len(e.AttestedSig) > 0 {
		// v3 only: base64 of the sig (~4/3 expansion) plus its "sig":"" wrapper.
		n += (len(e.AttestedSig)+2)/3*4 + 12
	}
	return n
}

// announceFastAcceptBudget is the running-estimate ceiling below which an
// entry is packed into the current chunk WITHOUT a precise marshal.  Set to
// half the wire budget: with maxBytes = MaxFrameLine (128 KiB) and the
// 100-entry count cap, a typical chunk of ~100 small entries lands near
// ~25 KiB, far under 64 KiB, so the common path does zero per-entry
// marshals (the old code marshalled the whole growing chunk on EVERY entry —
// O(n²) bytes).  Only chunks whose entries carry large Extra/Sig blobs and
// actually approach the byte limit fall through to the precise marshal,
// where the exact boundary still decides.  The 2× headroom dwarfs the
// constant frame-envelope overhead the per-entry estimate omits, so a
// fast-accepted chunk can never exceed maxBytes.
func announceFastAcceptBudget(maxBytes int) int { return maxBytes / 2 }

// chunkAnnounceEntriesBySize splits a route slice into wire-safe chunks.
// Each chunk's serialized announce frame fits within maxBytes.
//
// Algorithm:
//  1. Build chunks greedily — start with empty current chunk.
//  2. For each entry, append it to current chunk and pre-marshal the
//     candidate frame. If the marshaled line fits maxBytes, accept;
//     otherwise close current chunk and start a new one with just
//     this entry.
//  3. If a SINGLE entry's marshaled line exceeds maxBytes, the entry
//     is rejected (logged and reported via skipped indices) — there
//     is no way to split a single AnnounceRouteFrame whose Extra
//     payload alone is oversized.
//
// Returns:
//   - chunks: groups of entries that each fit in maxBytes.
//   - skipped: indices into the input slice for entries that could
//     not fit alone (caller must log/track these — they will not be
//     announced).
//
// kind selects the frame type used for the size probe; both
// announceWireLegacy and announceWireV2 share the same wire shape so
// the probe is accurate either way.
func chunkAnnounceEntriesBySize(entries []routing.AnnounceEntry, kind announceWireType, maxBytes int) (chunks [][]routing.AnnounceEntry, skipped []int) {
	if len(entries) == 0 {
		return nil, nil
	}
	var current []routing.AnnounceEntry
	curEst := 0 // running over-estimate of current's marshalled size.
	fastBudget := announceFastAcceptBudget(maxBytes)
	flushCurrent := func() {
		if len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
		}
		curEst = 0
	}
	for i, entry := range entries {
		// Phase 4 13.5 entry-count cap: flush the current chunk and
		// start a new one when adding this entry would exceed
		// maxRoutesPerAnnounceFrame. Independent of the byte-size
		// branch below — either limit can fire first.
		if len(current) >= maxRoutesPerAnnounceFrame {
			flushCurrent()
		}

		// Fast path: if the running over-estimate plus this entry stays
		// under half the budget, the chunk is provably under maxBytes —
		// accept without marshalling. This is the common case (small
		// route entries) and is what eliminates the per-entry O(n²)
		// marshal of the whole growing chunk. The estimate charges every
		// string field — identity, origin AND Extra — its worst-case
		// HTML-escape expansion (announceEntryWireEstimate), so an entry
		// whose '<'/'>'/'&' bytes balloon ~6× on re-encode cannot fast-pass
		// the gate and then overflow the real frame on the send path.
		//
		// Caveat vs. the precise branch: skipping the marshal also skips
		// the genuine-encode-error check that would otherwise drop a single
		// entry whose Extra is malformed. Extra is valid-JSON-or-nil by
		// construction (received Extra round-tripped through unmarshal;
		// local routes carry nil), so this cannot fire in practice, and the
		// send path re-marshals the assembled frame under the same limit —
		// a corrupt entry would fail there, never ship malformed.
		est := announceEntryWireEstimate(entry)
		if curEst+est <= fastBudget {
			current = append(current, entry)
			curEst += est
			continue
		}

		// Near the budget — fall back to the exact marshal to decide.
		candidate := append(current, entry)
		frame := buildAnnounceFrame(kind, candidate)
		if _, err := protocol.MarshalFrameLineWithLimit(frame, maxBytes); err != nil {
			if !errors.Is(err, protocol.ErrFrameTooLarge) {
				// Genuine encode failure — propagate by skipping this
				// entry (callers see it in `skipped`) and keep building
				// chunks from the rest. We cannot return an error here
				// without dropping every queued entry that came before.
				skipped = append(skipped, i)
				continue
			}
			// Size violation — close current chunk and try entry alone.
			flushCurrent()
			soloFrame := buildAnnounceFrame(kind, []routing.AnnounceEntry{entry})
			if _, soloErr := protocol.MarshalFrameLineWithLimit(soloFrame, maxBytes); soloErr != nil {
				// Entry's own Extra blob already exceeds the budget —
				// no chunking can save it. Skip and report.
				skipped = append(skipped, i)
				continue
			}
			current = []routing.AnnounceEntry{entry}
			curEst = est
			continue
		}
		current = candidate
		curEst += est
	}
	flushCurrent()
	return chunks, skipped
}

// logSkippedAnnounceEntries emits one structured error per dropped
// entry so operators can correlate `announce_routes_entry_oversize_dropped`
// log lines with specific identity/origin pairs. Kept as a helper so
// SendAnnounceRoutes and SendRoutesUpdate share the exact same shape.
func logSkippedAnnounceEntries(peerAddress domain.PeerAddress, routes []routing.AnnounceEntry, skipped []int, wireType string) {
	for _, idx := range skipped {
		if idx < 0 || idx >= len(routes) {
			continue
		}
		log.Error().
			Str("peer_address", string(peerAddress)).
			Str("wire_type", wireType).
			Str("route_identity", routes[idx].Identity.String()).
			Str("route_origin", routes[idx].Origin.String()).
			Msg("announce_routes_entry_oversize_dropped")
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
//
// Chunking is size-aware: entries are packed greedily into frames that
// each fit under MaxFrameLine. Inbound paths share the strict
// command-plane budget (128 KiB); outbound peer-session paths
// technically have a wider 8 MiB budget but we use MaxFrameLine here
// too because the receiver dispatches announce_routes through the
// inbound-style 128 KiB line reader regardless of which end opened
// the TCP connection. Entries whose individual encoding exceeds the
// budget are dropped with announce_routes_entry_oversize_dropped and
// excluded from the wire frames; remaining entries still ship.
func (s *Service) SendAnnounceRoutes(ctx context.Context, peerAddress domain.PeerAddress, routes []routing.AnnounceEntry) bool {
	if len(routes) == 0 {
		return true
	}
	chunks, skipped := chunkAnnounceEntriesBySize(routes, announceWireLegacy, protocol.MaxFrameLine)
	logSkippedAnnounceEntries(peerAddress, routes, skipped, "announce_routes")
	for _, chunk := range chunks {
		frame := buildAnnounceFrame(announceWireLegacy, chunk)
		if !s.sendAnnouncePlaneFrame(ctx, peerAddress, frame) {
			return false
		}
	}
	// Truthful delivery contract: when even one entry was dropped by
	// chunkAnnounceEntriesBySize the snapshot we were handed is NOT
	// fully on the wire. Callers (sendConnectTimeFullSync, AnnounceLoop)
	// use the bool result to decide whether to record this snapshot as
	// the new per-peer baseline / per-peer announce cache — recording
	// it on partial delivery would let the dropped routes silently
	// disappear from the peer's view until they change again. Returning
	// false leaves the caller's cache untouched so the next cycle
	// retries the full snapshot; the entries that did fit shipped this
	// cycle, the receiver dedupes them via (identity, origin, seq), and
	// the broken entry keeps surfacing in the
	// announce_routes_entry_oversize_dropped log until upstream shrinks
	// its Extra payload.
	return len(skipped) == 0
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
	chunks, skipped := chunkAnnounceEntriesBySize(delta, announceWireV2, protocol.MaxFrameLine)
	logSkippedAnnounceEntries(peerAddress, delta, skipped, "routes_update")
	for _, chunk := range chunks {
		frame := buildAnnounceFrame(announceWireV2, chunk)
		if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, frame,
			domain.CapMeshRoutingV1,
			domain.CapMeshRoutingV2,
			domain.CapMeshRelayV1,
		) {
			log.Debug().
				Str("peer_address", string(peerAddress)).
				Int("delta_routes", len(chunk)).
				Msg("routes_update_skipped_or_failed_at_send_time")
			return false
		}
	}
	// Same truthful-delivery contract as SendAnnounceRoutes: any entry
	// dropped by chunkAnnounceEntriesBySize means the delta we were
	// asked to ship is NOT fully on the wire. Returning false leaves
	// the caller's per-peer announce cache unmodified so the next
	// announce cycle re-attempts the full delta — the dropped entry
	// keeps appearing in the announce_routes_entry_oversize_dropped log
	// until upstream shrinks its Extra payload.
	return len(skipped) == 0
}

// SendRequestResync emits a request_resync wire frame to the peer so that
// the peer clears its per-peer announce state and re-delivers a full
// baseline on its next cycle. Called by the receive paths when a delta
// arrives without a prior baseline in this session: the v2 path on a
// routes_update without baseline (handleRoutesUpdate) AND the v3 path
// on a kind="delta" without baseline (handleRouteAnnounceV3, including
// the epoch-reset case where a prior baseline was invalidated). The
// forced-full path on the peer side honours the first-sync invariant
// (docs/routing.md): a v3-capable peer responds with
// route_announce_v3 kind="full", a v1/v2-only peer responds with
// legacy announce_routes — either way the recovery frame is a
// self-contained baseline, never a delta.
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
			Identity: r.IdentityHexString(),
			Origin:   r.Origin.String(),
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

// buildRouteAnnounceV3Frame produces the Phase 4 compact wire frame for
// the given AnnounceEntry slice (phase-4 §3.1, overview §7.1). Origin is
// dropped (the receiver derives the uplink from the sender); Hops int is
// converted to uint8 with HopsInfinity-aware clamping so a wire value of
// 16 — the withdrawal marker — survives the type narrowing.
//
// The returned protocol.Frame carries Frame.Type plus a pre-marshalled
// Frame.RawLine (with trailing newline). This is the RawLine bypass
// pattern that route_sync / route_probe / route_query frames already use,
// keeping the universal Frame struct free of v3-specific fields and
// making a Phase 6 cleanup a file delete rather than a sweep through
// shared types.
//
// Sig encoding (Phase 4 13.2-A, shipped): each entry's stored
// AttestedSig is base64-encoded onto RouteAnnounceV3Entry.Sig when
// non-empty (signer side: signOwnOriginV3Entries populates the
// per-entry sig before this helper assembles the wire frame), and
// omitted via the omitempty JSON tag when the origin did not sign
// (Tier-2 unsigned entries continue to flow through this assembly
// path unchanged). The canonical bytes the signature covers come
// from RouteAnnounceV3Entry.CanonicalSigningBytes — identity and
// extra only, see docs/protocol/attested_links.md "Canonical
// signing bytes" for the multi-hop verification rationale.
func buildRouteAnnounceV3Frame(kind string, epoch uint64, entries []routing.AnnounceEntry) (protocol.Frame, error) {
	wireEntries := make([]protocol.RouteAnnounceV3Entry, len(entries))
	for i, e := range entries {
		wireEntries[i] = protocol.RouteAnnounceV3Entry{
			Identity: e.IdentityHexString(),
			Hops:     hopsIntToUint8(e.Hops),
			SeqNo:    e.SeqNo,
			Extra:    e.Extra,
		}
		// Phase 4 13.2-A: encode the stored attested-links signature
		// for the wire. Empty AttestedSig → omitted "sig" field on the
		// wire (omitempty on RouteAnnounceV3Entry.Sig keeps the JSON
		// shape clean for the Tier-2 unsigned case). Base64 std
		// encoding mirrors the wire-format contract documented on
		// RouteAnnounceV3Entry.Sig.
		if len(e.AttestedSig) > 0 {
			wireEntries[i].Sig = base64.StdEncoding.EncodeToString(e.AttestedSig)
		}
	}
	v3 := protocol.RouteAnnounceV3Frame{
		Type:  protocol.RouteAnnounceV3FrameType,
		Kind:  kind,
		Epoch: epoch,
		// IssuedAt is a diagnostic freshness hint (see
		// RouteAnnounceV3Frame.IssuedAt doc-comment) — receivers do
		// NOT consult it for acceptance; per-(Identity, sender) SeqNo
		// monotonicity governs that. Population is therefore allowed
		// to use a direct time.Now() per CLAUDE.md "Время" — not
		// participating in any business decision. Populated here so
		// the wire payload matches the docs/protocol/route_announce_v3.md
		// contract and so operators inspecting captured frames can
		// correlate timing without consulting send-side telemetry.
		IssuedAt: time.Now().UTC().Format(time.RFC3339),
		Entries:  wireEntries,
	}
	raw, err := protocol.MarshalRouteAnnounceV3Frame(v3)
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.Frame{
		Type:    protocol.RouteAnnounceV3FrameType,
		RawLine: string(raw) + "\n",
	}, nil
}

// hopsIntToUint8 narrows the legacy int Hops field to the v3 wire uint8
// while preserving the withdrawal marker. Negative values are clamped
// to 0 (defensive — RouteEntry.Hops is non-negative by construction);
// values >= HopsInfinity (16) are clamped to HopsInfinity so a
// withdrawal stays a withdrawal even if upstream over-counted; values in
// [0, 15] pass through.
func hopsIntToUint8(hops int) uint8 {
	switch {
	case hops <= 0:
		return 0
	case hops >= int(routing.HopsInfinity):
		return uint8(routing.HopsInfinity)
	default:
		return uint8(hops)
	}
}

// chunkRouteAnnounceV3EntriesBySize splits a route slice into wire-safe
// v3 chunks. Each chunk's marshalled route_announce_v3 frame fits within
// maxBytes. Algorithm mirrors chunkAnnounceEntriesBySize: greedy pack,
// flush when the candidate marshal exceeds the budget, skip and report
// any single entry whose own marshal is oversize (Extra blob too large
// to ever fit, no chunking can save it).
//
// kind and epoch are passed through to every chunk's frame so the size
// probe is faithful to the actual wire bytes.
func chunkRouteAnnounceV3EntriesBySize(entries []routing.AnnounceEntry, kind string, epoch uint64, maxBytes int) (chunks [][]routing.AnnounceEntry, skipped []int) {
	if len(entries) == 0 {
		return nil, nil
	}
	measure := func(es []routing.AnnounceEntry) (int, error) {
		f, err := buildRouteAnnounceV3Frame(kind, epoch, es)
		if err != nil {
			return 0, err
		}
		return len(f.RawLine), nil
	}
	var current []routing.AnnounceEntry
	curEst := 0 // running over-estimate of current's marshalled size.
	fastBudget := announceFastAcceptBudget(maxBytes)
	flushCurrent := func() {
		if len(current) > 0 {
			chunks = append(chunks, current)
			current = nil
		}
		curEst = 0
	}
	for i, entry := range entries {
		// Phase 4 13.5 entry-count cap mirror of the legacy chunker —
		// either limit fires first.
		if len(current) >= maxRoutesPerAnnounceFrame {
			flushCurrent()
		}

		// Fast path: provably under budget by the over-estimate — accept
		// without the per-entry measure() (which here also runs base64 +
		// time formatting through buildRouteAnnounceV3Frame, so skipping
		// it is doubly worth it). Mirrors chunkAnnounceEntriesBySize.
		est := announceEntryWireEstimate(entry)
		if curEst+est <= fastBudget {
			current = append(current, entry)
			curEst += est
			continue
		}

		candidate := append(current, entry)
		size, err := measure(candidate)
		if err != nil {
			// Marshal failure — drop this entry, keep going so previously-
			// accepted entries still ship. Matches chunkAnnounceEntriesBySize.
			skipped = append(skipped, i)
			continue
		}
		if size <= maxBytes {
			current = candidate
			curEst += est
			continue
		}
		// Candidate would exceed the budget — close the current chunk and
		// try the entry alone.
		flushCurrent()
		soloSize, soloErr := measure([]routing.AnnounceEntry{entry})
		if soloErr != nil || soloSize > maxBytes {
			skipped = append(skipped, i)
			continue
		}
		current = []routing.AnnounceEntry{entry}
		curEst = est
	}
	flushCurrent()
	return chunks, skipped
}

// SendRouteAnnounceV3 emits the Phase 4 compact announce frame to peer.
// kind is "full" or "delta" (RouteAnnounceV3KindFull / KindDelta); epoch
// is the local route_announce_v3 epoch (Table.Epoch()); entries carry the
// per-uplink claims with Origin dropped.
//
// Capability gate: mesh_routing_v1 + mesh_routing_v3 + mesh_relay_v1.
// The triplet mirrors handleRouteAnnounceV3's receive gate: v1 is the
// wire-baseline a legacy fallback depends on, v3 is the compact-frame
// opt-in, relay is the data-plane requirement (a non-relay neighbor
// yields unusable NextHops). Capture-and-write is atomic via
// dispatchAnnouncePlaneFrameWithCaps, identical to SendRoutesUpdate, so a
// session replacement at the same address cannot route bytes to a
// narrower transport.
//
// Returns true only when every chunk shipped successfully — same truthful-
// delivery contract as SendAnnounceRoutes / SendRoutesUpdate. A skipped
// entry or any transport drop returns false so the caller's per-peer
// cache stays untouched and the next cycle retries.
func (s *Service) SendRouteAnnounceV3(ctx context.Context, peerAddress domain.PeerAddress, kind string, epoch uint64, entries []routing.AnnounceEntry) bool {
	if len(entries) == 0 {
		return true
	}
	// Phase 4 13.2-B: sign own-origin entries with the local identity
	// private key before chunking. The signature covers
	// CanonicalSigningBytes (identity || extra only — hops, epoch, and
	// seq_no are ALL per-emitter wire fields that transit restamp on
	// re-emit, so they are deliberately excluded so multi-hop transit
	// verification works; see docs/protocol/attested_links.md
	// "Canonical signing bytes" for the multi-hop rationale). Only
	// entries whose Identity equals our local identity get signed;
	// non-own entries either already carry a sig (forwarded verbatim
	// from the original signer via the storage round-trip from 13.2-A)
	// or stay unsigned. The per-call signing is cheap (ed25519.Sign is
	// ~30µs and own-origin entries are few; one per cycle is typical),
	// and the result is not cached back into storage — the per-emit
	// recompute is a known-cost trade-off; sig caching in UplinkClaim
	// is a follow-up. The slice is mutated in place because the caller
	// (the AnnounceLoop / connect-time sync) has already materialised
	// it for this send and we own it for the duration of the call.
	entries = s.signOwnOriginV3Entries(entries, epoch)
	chunks, skipped := chunkRouteAnnounceV3EntriesBySize(entries, kind, epoch, protocol.MaxFrameLine)
	logSkippedAnnounceEntries(peerAddress, entries, skipped, "route_announce_v3")
	for _, chunk := range chunks {
		frame, err := buildRouteAnnounceV3Frame(kind, epoch, chunk)
		if err != nil {
			log.Warn().
				Err(err).
				Str("peer_address", string(peerAddress)).
				Int("chunk_entries", len(chunk)).
				Msg("route_announce_v3_build_failed")
			return false
		}
		if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, frame,
			domain.CapMeshRoutingV1,
			domain.CapMeshRoutingV3,
			domain.CapMeshRelayV1,
		) {
			log.Debug().
				Str("peer_address", string(peerAddress)).
				Str("kind", kind).
				Int("entries", len(chunk)).
				Msg("route_announce_v3_skipped_or_failed_at_send_time")
			return false
		}
	}
	return len(skipped) == 0
}

// signOwnOriginV3Entries signs in-place the own-origin AnnounceEntries
// in the input slice with the local identity's Ed25519 private key. The
// signature covers RouteAnnounceV3Entry.CanonicalSigningBytes() —
// identity and extra ONLY. All three per-emitter wire fields — hops,
// epoch, and seq_no — are deliberately EXCLUDED from the signed
// payload (P1 fixes): each is restamped by transit on re-emit and
// would break multi-hop verification if signed. The epoch parameter
// is preserved on the Go signature for caller symmetry with the
// verifier and for future logging hooks; it does NOT enter the
// canonical bytes. See docs/protocol/attested_links.md
// "Canonical signing bytes" for the full rationale.
//
// Selection rules (idempotent — repeated calls are no-ops):
//   - Skip when the local identity has no PrivateKey wired (test
//     fixtures that construct Service without a real identity).
//   - Skip entries with AttestedSig already populated (transit entries
//     re-emitted from storage carry the original signer's bytes; we
//     never overwrite a sig produced by someone else).
//   - Skip entries whose Identity is not our own (we cannot speak for
//     another identity's announcements; that path is the verifier's
//     job on the other side).
//   - Withdrawals (Hops==HopsInfinity) ARE signed by the origin: a
//     withdrawal is a legitimate own claim (Identity=self, Extra)
//     and canonical bytes exclude hops AND seq_no, so the same
//     signing path applies. No special-case branch needed.
//
// Production status (Round-7 finding). This signer is plumbed but
// currently has no live input on the production emit path:
// route_store.AnnounceProjectionFor iterates the stored bucket map
// and never emits the local identity (the self-route is synthesised
// in Lookup/Snapshot, not stored). The Identity == localIdentity
// branch above therefore matches zero entries in production today,
// and the function is a no-op on every v3 emit. The
// mesh_attested_links_v1 capability is correspondingly unadvertised
// (see localCapabilities). Phase 5 anchor publication wires the
// self-attestation entry stream (Identity == localIdentity, per-
// emitter SeqNo, anchor metadata in Extra) that gives this signer
// real work — at which point the cap re-enables and this doc-comment
// updates. The synthetic-entry tests in
// routing_announce_v3_attest_test.go feed Identity == localIdentity
// directly to exercise the signing path under unit-test conditions
// independent of the emit-path gap.
//
// Returns the same slice (mutated in place). Callers must not retain
// pre-call entry copies across this call boundary.
func (s *Service) signOwnOriginV3Entries(entries []routing.AnnounceEntry, epoch uint64) []routing.AnnounceEntry {
	_ = epoch // reserved for future logging / diagnostics; not in canonical bytes (P1)
	if len(s.identity.PrivateKey) == 0 {
		return entries
	}
	localIdentity := s.identity.Address
	for i := range entries {
		if len(entries[i].AttestedSig) > 0 {
			continue
		}
		if entries[i].IdentityHexString() != localIdentity {
			continue
		}
		v3e := protocol.RouteAnnounceV3Entry{
			Identity: entries[i].IdentityHexString(),
			SeqNo:    entries[i].SeqNo,
			Extra:    entries[i].Extra,
		}
		// Phase 4 P1 fix: canonical bytes do NOT include epoch —
		// epoch is per-emitter (changes across transit nodes), so
		// signing over it would break multi-hop verification (origin
		// signs with origin's epoch; transit forwards verbatim; final
		// receiver recomputes canonical bytes from the LAST emitter's
		// frame epoch and the verification fails). See
		// CanonicalSigningBytes doc-comment.
		entries[i].AttestedSig = ed25519.Sign(s.identity.PrivateKey, v3e.CanonicalSigningBytes())
	}
	return entries
}

// SendRoutePoison emits the Phase 4 explicit poison-reverse signal
// (route_poison_v1, overview §7.7) to peerAddress for identity. The
// frame is signed with the local identity's Ed25519 key when
// available (no-op short-circuit if the test fixture has no private
// key wired), and dispatched via the cap-gated announce-plane path:
// requires the peer to advertise mesh_routing_v1 +
// mesh_poison_reverse_v1 (relay cap NOT required — poison is a
// single-hop control signal, not a data-plane delivery, so a
// routing-capable-but-non-relay peer is a valid recipient).
//
// reason is one of protocol.RoutePoisonReason* — caller bug if
// arbitrary; the marshal step rejects unknown reasons. Returns true
// when the frame was enqueued.
func (s *Service) SendRoutePoison(ctx context.Context, peerAddress domain.PeerAddress, identity domain.PeerIdentity, reason string) bool {
	if peerAddress == "" || identity.IsZero() {
		return false
	}
	frame := protocol.RoutePoisonFrame{
		Type:     protocol.RoutePoisonFrameType,
		Identity: identity.String(),
		Reason:   reason,
		IssuedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if len(s.identity.PrivateKey) != 0 {
		frame.SenderSig = base64.StdEncoding.EncodeToString(
			ed25519.Sign(s.identity.PrivateKey, frame.CanonicalSenderSigBytes()),
		)
	}
	raw, err := protocol.MarshalRoutePoisonFrame(frame)
	if err != nil {
		log.Warn().
			Err(err).
			Str("peer_address", string(peerAddress)).
			Str("identity", identity.String()).
			Str("reason", reason).
			Msg("route_poison_marshal_failed")
		return false
	}
	wireFrame := protocol.Frame{
		Type:    protocol.RoutePoisonFrameType,
		RawLine: string(raw) + "\n",
	}
	if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, wireFrame,
		domain.CapMeshRoutingV1,
		domain.CapMeshPoisonReverseV1,
	) {
		log.Debug().
			Str("peer_address", string(peerAddress)).
			Str("identity", identity.String()).
			Str("reason", reason).
			Msg("route_poison_skipped_or_failed_at_send_time")
		return false
	}
	return true
}

// SendRoutePoisonBatch emits a single batched poison-reverse frame
// (route_poison_v2) naming all `identities` for one shared reason to
// peerAddress. Cap-gated on mesh_routing_v1 + mesh_poison_reverse_v2 (the
// caller only routes here for peers known to advertise v2). Signs over the
// batch canonical bytes when a private key is wired. Returns true when the
// frame was enqueued. Zero identities or all-zero after filtering is a no-op.
func (s *Service) SendRoutePoisonBatch(ctx context.Context, peerAddress domain.PeerAddress, identities []domain.PeerIdentity, reason string) bool {
	if peerAddress == "" || len(identities) == 0 {
		return false
	}
	idStrs := make([]string, 0, len(identities))
	for _, id := range identities {
		if id.IsZero() {
			continue
		}
		idStrs = append(idStrs, id.String())
	}
	if len(idStrs) == 0 {
		return false
	}
	frame := protocol.RoutePoisonV2Frame{
		Type:       protocol.RoutePoisonV2FrameType,
		Identities: idStrs,
		Reason:     reason,
		IssuedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	if len(s.identity.PrivateKey) != 0 {
		frame.SenderSig = base64.StdEncoding.EncodeToString(
			ed25519.Sign(s.identity.PrivateKey, frame.CanonicalSenderSigBytes()),
		)
	}
	raw, err := protocol.MarshalRoutePoisonV2Frame(frame)
	if err != nil {
		log.Warn().Err(err).Str("peer_address", string(peerAddress)).Int("identities", len(idStrs)).Str("reason", reason).Msg("route_poison_v2_marshal_failed")
		return false
	}
	wireFrame := protocol.Frame{
		Type:    protocol.RoutePoisonV2FrameType,
		RawLine: string(raw) + "\n",
	}
	if !s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, wireFrame,
		domain.CapMeshRoutingV1,
		domain.CapMeshPoisonReverseV2,
	) {
		log.Debug().Str("peer_address", string(peerAddress)).Int("identities", len(idStrs)).Str("reason", reason).Msg("route_poison_v2_skipped_or_failed_at_send_time")
		return false
	}
	return true
}

// handleRoutePoison processes an incoming route_poison_v1 frame
// (Phase 4 13.3, overview §7.7). The receiver verifies the
// sender_sig if present (Tier-2 lenient: present-and-invalid
// drops the frame; present-and-pubkey-unknown is accepted; absent
// is accepted — sender identity is already bound by the
// session-level handshake), then invalidates ONLY the
// claims[identity][senderIdentity] storage slot via
// Table.InvalidateUplinkClaim. Other uplinks for the same identity
// and the origin's own SeqNo-governed claim are untouched.
//
// Poison is not relayed verbatim — receivers do not re-broadcast
// the original frame as-is. Instead, this handler emits its OWN
// route_poison_v1 to other direct peers via
// poisonReverseToOtherPeers when the invalidation leaves us with
// no surviving uplink to the target (Round-16 fan-out, see the
// inline comment near the call site below for the gate rationale).
// This is what propagates the count-to-infinity collapse beyond
// the first receiver — relying on the next announce cycle to
// surface a withdrawal does NOT work for transit routes, because
// AnnounceProjectionFor only emits own-direct tombstones; transit
// tombstones from InvalidateUplinkClaim are filtered out.
func (s *Service) handleRoutePoison(senderIdentity domain.PeerIdentity, frame protocol.RoutePoisonFrame) {
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("route_poison_malformed_sender")
		return
	}
	// Route-quarantine gate (top-level) — symmetric with
	// handleRoutesUpdate / handleRouteAnnounceV3 / handleRequestResync.
	// route_poison_v1 from a quarantined peer must be silently
	// dropped: handling it mutates routingTable via
	// InvalidateUplinkClaim (the peer's view of which uplinks are
	// dead overrides our own table state) and, when the last
	// uplink to the target dies, triggers poisonReverseToOtherPeers
	// — outbound routing-control traffic that propagates the
	// quarantined peer's (untrusted) opinion across the mesh.
	// Both effects are exactly what quarantine is meant to
	// suppress: the peer's routing knowledge is not trusted
	// during the cooldown. Position is BEFORE the announceLimiter
	// charge AND BEFORE the ed25519.Verify path so a hostile
	// quarantined peer cannot soak CPU on signature work either.
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Str("frame_type", "route_poison_v1").
			Str("identity", frame.Identity).
			Str("reason", frame.Reason).
			Msg("routing_announce_drop_quarantined_sender")
		return
	}
	// Phase 4 13.7 (Round-5 extension): shared per-peer announce-plane
	// rate limit across ALL receive paths the announceLimiter protects
	// (announce_routes / routes_update / route_announce_v3 /
	// request_resync / route_poison_v1). Charged BEFORE the
	// base64+ed25519 verify path and BEFORE Table.InvalidateUplinkClaim
	// so a flood of poison frames cannot soak CPU on signature
	// verification or storage-writer churn — same defence in depth
	// the announce-plane handlers already get. Sharing the bucket with
	// announce frames means a peer cannot flip wire types to dodge the
	// throttle.
	// Cost: 1 token. Poison is a single-identity control signal,
	// not an entry-batch — sharing the bucket with announce frames
	// at unit cost (Round-10) means a poison flood still consumes
	// announce capacity but doesn't get artificially inflated.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, 1) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "route_poison_v1").
			Str("identity", frame.Identity).
			Str("reason", frame.Reason).
			Msg("announce_rate_limit_drop")
		return
	}
	if !identity.IsValidAddress(frame.Identity) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_identity", frame.Identity).
			Msg("route_poison_malformed_identity")
		return
	}
	// Sig verification — same Tier-2 policy as mesh_attested_links_v1
	// route_announce_v3: present-and-invalid drops; absent or
	// pubkey-unknown accepts. A malformed base64 SenderSig counts as
	// present-and-invalid (the sender claimed to sign but produced
	// undecodable bytes — either a bug on their side or active
	// corruption) and is dropped just like an ed25519 verification
	// failure. Equivalent-to-absent treatment would silently turn any
	// poison sender into an unauthenticated channel by shipping
	// "sig=garbage".
	if frame.SenderSig != "" {
		sig, err := base64.StdEncoding.DecodeString(frame.SenderSig)
		if err != nil {
			log.Warn().
				Err(err).
				Str("from", senderIdentity.String()).
				Str("identity", frame.Identity).
				Str("reason", frame.Reason).
				Msg("route_poison_sig_malformed_drop")
			return
		}
		if pubkey, ok := s.publicKeyForIdentity(senderIdentity); ok {
			if !ed25519.Verify(pubkey, frame.CanonicalSenderSigBytes(), sig) {
				log.Warn().
					Str("from", senderIdentity.String()).
					Str("identity", frame.Identity).
					Str("reason", frame.Reason).
					Msg("route_poison_sig_invalid_drop")
				return
			}
		}
	}
	target := domain.PeerIdentityFromWire(frame.Identity)
	if s.applyRoutePoisonForIdentity(senderIdentity, target, frame.Reason) {
		s.poisonReverseToOtherPeers(s.runCtx, senderIdentity, []routing.PeerIdentity{routing.PeerIdentity(target)})
	}
}

// applyRoutePoisonForIdentity invalidates the claims[target][senderIdentity]
// storage slot. It returns cascade=true when the invalidation left NO
// surviving uplink to target — meaning the caller must fan a poison-reverse
// about target to other peers. The caller owns that fan-out so it can BATCH it
// across all zeroed-out targets (v2) instead of emitting one fan-out per
// identity (which would re-explode the flood on the propagation hop). When a
// backup uplink survives, the drain is triggered here and cascade=false. The
// caller has ALREADY gated quarantine, rate limit, and signature.
func (s *Service) applyRoutePoisonForIdentity(senderIdentity, target domain.PeerIdentity, reason string) (cascade bool) {
	mutated := s.routingTable.InvalidateUplinkClaim(target, senderIdentity)
	log.Debug().
		Str("from", senderIdentity.String()).
		Str("identity", target.String()).
		Str("reason", reason).
		Bool("mutated", mutated).
		Msg("route_poison_applied")
	if !mutated {
		return false
	}
	s.eventBus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason: domain.RouteChangeAnnouncement,
		PeerID: senderIdentity,
	})
	// remaining > 0: a backup uplink survives, our claim stays live and the
	// pending queue may now be deliverable via the alternate path. remaining
	// == 0: no path left — the caller must poison-reverse target to other
	// peers so the wave does not stall at us (transit tombstones are not
	// emitted by AnnounceProjectionFor, so the announce cycle alone would not
	// propagate the withdrawal).
	if len(s.routingTable.Lookup(target)) > 0 {
		s.triggerDrainForExposed([]routing.PeerIdentity{routing.PeerIdentity(target)})
		return false
	}
	return true
}

// handleRoutePoisonV2 processes an incoming batched poison-reverse frame
// (route_poison_v2). Same gates as handleRoutePoison — quarantine, the shared
// announce rate limit (charged len(identities) tokens so v2 is cost-equivalent
// to the v1 fan-out it replaces, no multiplicative bypass), and Tier-2-lenient
// signature verification over the batch canonical bytes (verified ONCE) — then
// applies the poison to every listed identity via the shared core. Zeroed-out
// targets are collected and cascaded ONCE via poisonReverseToOtherPeers (which
// batches per peer), so the propagation hop does not re-explode into singletons.
func (s *Service) handleRoutePoisonV2(senderIdentity domain.PeerIdentity, frame protocol.RoutePoisonV2Frame) {
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("route_poison_v2_malformed_sender")
		return
	}
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().Str("from", senderIdentity.String()).Str("frame_type", "route_poison_v2").Int("identities", len(frame.Identities)).Msg("routing_announce_drop_quarantined_sender")
		return
	}
	// Charge ONE token per identity, not one per frame: the batch does the
	// work of N v1 frames (N invalidations + up to N cascades), so a flat cost
	// of 1 would let an authenticated peer get an N× multiplicative bypass of
	// the announce-plane rate limit relative to v1. Cost == len(identities)
	// keeps v2 cost-equivalent to the v1 fan-out it replaces.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, len(frame.Identities)) {
		log.Warn().Str("from", senderIdentity.String()).Str("frame_type", "route_poison_v2").Int("identities", len(frame.Identities)).Msg("announce_rate_limit_drop")
		return
	}
	// Tier-2-lenient signature check over the batch canonical bytes, once.
	if frame.SenderSig != "" {
		sig, err := base64.StdEncoding.DecodeString(frame.SenderSig)
		if err != nil {
			log.Warn().Err(err).Str("from", senderIdentity.String()).Msg("route_poison_v2_sig_malformed_drop")
			return
		}
		if pubkey, ok := s.publicKeyForIdentity(senderIdentity); ok {
			if !ed25519.Verify(pubkey, frame.CanonicalSenderSigBytes(), sig) {
				log.Warn().Str("from", senderIdentity.String()).Msg("route_poison_v2_sig_invalid_drop")
				return
			}
		}
	}
	// Collect the targets that zeroed out, then fan the cascade ONCE as a
	// batch — applying per-identity fan-out here would re-explode the flood on
	// the propagation hop (a 500-identity batch from A would become 500
	// singleton frames downstream). poisonReverseToOtherPeers batches per peer.
	var cascade []routing.PeerIdentity
	for _, idStr := range frame.Identities {
		if !identity.IsValidAddress(idStr) {
			log.Warn().Str("from", senderIdentity.String()).Str("frame_identity", idStr).Msg("route_poison_v2_malformed_identity")
			continue
		}
		target := domain.PeerIdentityFromWire(idStr)
		if s.applyRoutePoisonForIdentity(senderIdentity, target, frame.Reason) {
			cascade = append(cascade, routing.PeerIdentity(target))
		}
	}
	if len(cascade) > 0 {
		s.poisonReverseToOtherPeers(s.runCtx, senderIdentity, cascade)
	}
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
	// Inbound TCP path: the receiving peer dispatches through
	// handleConn's command-plane reader bound by maxCommandLineBytes
	// (128 KiB). Use the matching writer-side budget so the sender
	// rejects a self-built oversize frame instead of letting the
	// remote close the connection with frame-too-large.
	line, err := protocol.MarshalFrameLineWithLimit(frame, protocol.MaxFrameLine)
	if err != nil {
		// Caller-side marshal failure — distinct diagnostic from a
		// transport drop. Kept on the caller so we can surface
		// frame_inbound_marshal_failed without going through the
		// network at all (the strict raw-bytes helper has no
		// marshal-fallback path; see network_consumer.go).
		//
		// frame-too-large is a self-bug: the upstream layer that built
		// the frame exceeded the wire size budget (announce_routes
		// pagination missed, peers cap missed, etc.). We return false
		// so the caller short-circuits without disconnecting the peer
		// — disconnect-on-self-bug would only restart the same
		// oversize frame.
		if errors.Is(err, protocol.ErrFrameTooLarge) {
			log.Error().Err(err).Str("peer", remoteAddr).Str("type", frame.Type).Msg("frame_inbound_too_large")
			return false
		}
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
		if session.peerIdentity.IsZero() {
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
		if info.identity.IsZero() {
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
			// info.capabilities is a READ-ONLY alias of NetCore storage
			// (snapshotEntryLocked uses CapabilitiesRef — no per-entry copy),
			// so copyCapabilitiesForAnnounce MUST copy before the slice lands
			// in an AnnounceTarget that outlives this walk: it satisfies the
			// "AnnounceTarget.Capabilities is private to the target" contract
			// and prevents retaining/mutating the shared alias.
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
	log.Trace().Uint64("conn_id", uint64(id)).Str("peer_identity", peerIdentity.String()).Msg("send_full_table_sync_inbound_begin")
	if peerIdentity.IsZero() {
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
// visible to the peer (split horizon applied), dispatches to the
// baseline sender selected from peer capabilities (SendRouteAnnounceV3
// with kind="full" when the v3 triplet is negotiated, otherwise legacy
// SendAnnounceRoutes), and updates the per-peer announce cache on
// success. When the snapshot is empty, the empty baseline is recorded
// without sending a wire frame — the protocol is additive so an empty
// table needs no explicit announcement.
//
// First-sync wire-frame invariant: see the "First-sync wire-frame
// invariant" section in docs/routing.md for the normative contract.
// Connect-time sync MUST use a self-contained baseline frame — either
// SendAnnounceRoutes (legacy announce_routes) for v1/v2 peers, or
// SendRouteAnnounceV3 with kind="full" for peers that negotiated the
// v3 triplet (v1+v3+relay). The v3 kind="full" frame is self-contained
// (carries the peer's full snapshot + table epoch) and therefore
// qualifies as a valid baseline alongside the legacy frame. Any future
// change that picks SendRoutesUpdate or kind="delta" here breaks that
// contract — both because a fresh session has no baseline a delta can
// be applied against, and because the guard tests in
// routing_integration_connect_sync_test.go will fail immediately. The
// empty-baseline short-circuit below emits NO wire frame at all
// (neither legacy, nor v2, nor v3) by design; see
// TestConnectTimeFullSync_EmptySnapshot_NoWireFrame for the regression
// contract.
//
// ctx flows down into SendAnnounceRoutes → writeFrameToInbound →
// sendFrameBytesViaNetworkSync; cancelling ctx aborts the inbound
// sync-flush wait rather than waiting for the internal syncFlushTimeout.
func (s *Service) sendConnectTimeFullSync(ctx context.Context, peerIdentity domain.PeerIdentity, address domain.PeerAddress) {
	log.Trace().Str("peer_identity", peerIdentity.String()).Str("address", string(address)).Msg("connect_time_full_sync_begin")
	// AnnounceToWithChangeHead captures the Phase 3 change-journal head atomic
	// with the projection so the cursor commit below stays in lock-step with
	// lastSentSnapshot (committed only on send success). routes is a pooled
	// projection buffer; return it after BuildAnnounceSnapshot consumes it
	// (nothing below reads routes again).
	routes, snapHead := s.routingTable.AnnounceToWithChangeHead(peerIdentity)
	defer s.routingTable.ReleaseAnnounceEntries(routes)
	snapshot := routing.BuildAnnounceSnapshot(routes)
	registry := s.announceLoop.StateRegistry()

	now := registry.Clock()
	peerState := registry.GetOrCreate(peerIdentity)
	log.Trace().Str("peer_identity", peerIdentity.String()).Int("entries", len(snapshot.Entries)).Msg("connect_time_full_sync_snapshot_built")

	if len(snapshot.Entries) == 0 {
		// No routes to send, but register the empty baseline so that future
		// announce cycles can compute meaningful deltas. No wire frame is
		// needed — the protocol is additive (not a destructive snapshot), so
		// an empty table is correctly represented by sending nothing. This
		// branch is orthogonal to the first-sync wire-frame invariant (see
		// docs/routing.md): empty-baseline emits neither the legacy
		// announce_routes frame nor the v2 routes_update frame.
		// Phase 3: an empty-baseline full sync reconciles the peer to the whole
		// (empty) table; snapHead is committed atomically with lastSentSnapshot.
		peerState.RecordFullSyncSuccess(snapshot, snapHead, now)
		log.Debug().
			Str("peer", peerIdentity.String()).
			Str("address", string(address)).
			Msg("routing_connect_time_full_sync_empty_baseline")
		return
	}

	peerState.RecordFullSyncAttempt(now)

	log.Trace().Str("peer_identity", peerIdentity.String()).Str("address", string(address)).Int("routes", len(snapshot.Entries)).Msg("connect_time_full_sync_before_send")
	// Phase 4 wire selection. When the peer advertises the v3 triplet
	// (v1+v3+relay), the very first session sync ships as
	// route_announce_v3 kind="full" — the compact frame is self-
	// contained (overview §7.1), so kind="full" correctly serves as
	// the v3-side baseline without violating the legacy first-sync
	// invariant (which applies specifically to v1/v2 because v2
	// routes_update cannot bootstrap). For non-v3 peers the legacy
	// announce_routes path stays unchanged per docs/routing.md
	// §"First-sync wire-frame invariant": always SendAnnounceRoutes,
	// never SendRoutesUpdate.
	v3 := s.peerSupportsRoutingV3(address)
	var sendOk bool
	if v3 {
		sendOk = s.SendRouteAnnounceV3(ctx, address, protocol.RouteAnnounceV3KindFull, s.routingTable.Epoch(), snapshot.Entries)
	} else {
		sendOk = s.SendAnnounceRoutes(ctx, address, snapshot.Entries)
	}
	log.Trace().Str("peer_identity", peerIdentity.String()).Str("address", string(address)).Bool("sent", sendOk).Bool("v3", v3).Msg("connect_time_full_sync_after_send")
	if !sendOk {
		log.Warn().
			Str("peer", peerIdentity.String()).
			Str("address", string(address)).
			Bool("v3", v3).
			Int("routes", len(snapshot.Entries)).
			Msg("routing_connect_time_full_sync_failed")
		return
	}

	// A non-empty connect-time frame is the very first observable
	// baseline on the wire for this session — flip the matching
	// send-side baseline flag so the AnnounceLoop's mode selection
	// knows the peer's receive gate (v2 or v3) is open and subsequent
	// deltas may use the upgraded wire frame. The empty-snapshot
	// branch above intentionally does NOT flip either flag — it
	// records a local baseline without emitting any wire frame, so the
	// peer never observed one.
	if v3 {
		peerState.MarkWireBaselineV3Sent()
	} else {
		peerState.MarkWireBaselineSent()
	}
	// Phase 3: the peer is now reconciled to the whole table — snapHead is
	// committed atomically with lastSentSnapshot inside RecordFullSyncSuccess
	// (only on this success path; a failed send returned above without touching
	// either).
	peerState.RecordFullSyncSuccess(snapshot, snapHead, now)
	log.Trace().Str("peer_identity", peerIdentity.String()).Str("address", string(address)).Msg("connect_time_full_sync_end")
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
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("announce_routes_malformed_sender")
		return
	}
	// chatty_routes accounting is DELIBERATELY skipped: announce_routes
	// is a full BASELINE (the v2 delta path is routes_update), and the
	// chatty trigger targets DELTA churn, not baselines. The rationale
	// is that a delta mutates state and cascades work (UpdateRoute →
	// TriggerUpdate → an announce cycle → mesh-wide propagation), so a
	// delta flood is what "chattiness" means and what must be
	// quarantined. A full baseline, by contrast, is idempotent — re-
	// applying the same table is near-free — and a baseline flood is
	// already bounded by the announceLimiter route bucket below
	// (200 routes/s). Counting baselines toward chatty only made the
	// quarantine fire on recovery/large full-syncs. (request_resync, a
	// control frame, is likewise excluded — see handleRequestResync.)
	// Phase 4 13.7: per-peer announce-plane rate limit. A flood from
	// any single peer (legitimate-but-buggy or hostile) is throttled
	// here BEFORE applyAnnounceEntries does the per-entry work, so a
	// burst cannot tie up the storage writer. Round-10 fix: charge by
	// route-entry count (min 1) so a legitimate chunked full-sync
	// drains the bucket proportional to its real work, instead of
	// being silently truncated past the previous 30-frame burst —
	// see announceBurstRoutesPerPeer doc for the new sizing.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, announceCostForEntries(len(frame.AnnounceRoutes))) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "announce_routes").
			Int("entries", len(frame.AnnounceRoutes)).
			Msg("announce_rate_limit_drop")
		return
	}
	// Phase 4 13.5 receive-side cap: drop the whole frame when its
	// entry count exceeds maxRoutesPerAnnounceFrame. Our own chunker
	// never produces such a frame; receiving one means the peer is
	// either buggy or hostile. Truncating silently would let a peer
	// hide entries past the cap; dropping the whole frame surfaces
	// the misbehaviour in logs and falls back on the next periodic
	// announce cycle.
	if len(frame.AnnounceRoutes) > maxRoutesPerAnnounceFrame {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "announce_routes").
			Int("entries", len(frame.AnnounceRoutes)).
			Int("cap", maxRoutesPerAnnounceFrame).
			Msg("announce_routes_entry_count_cap_exceeded")
		return
	}
	s.applyAnnounceEntries(senderIdentity, frame.AnnounceRoutes, nil, nil, announceReceiveLegacy)
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
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("routes_update_malformed_sender")
		return
	}
	// chatty_routes quarantine trigger. routes_update is the v2 DELTA
	// path, so it DOES count: delta churn (each delta cascades an
	// announce cycle + mesh-wide propagation) is exactly the pattern
	// the trigger targets. Full baselines (announce_routes, v3
	// kind="full") and the request_resync control frame are excluded —
	// see handleAnnounceRoutes for the baseline-vs-delta rationale.
	s.recordInboundAnnounceAndMaybeArm(senderIdentity, time.Now())
	// Phase 4 13.7: shared per-peer announce rate limit. v2 delta
	// frames count against the same budget as legacy / v3 frames so
	// a peer flipping between wire generations cannot reset its
	// throttle. Round-10 fix: charge by route-entry count — see
	// the matching comment in handleAnnounceRoutes.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, announceCostForEntries(len(frame.AnnounceRoutes))) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "routes_update").
			Int("entries", len(frame.AnnounceRoutes)).
			Msg("announce_rate_limit_drop")
		return
	}
	// Phase 4 13.5 receive-side cap (shared with handleAnnounceRoutes
	// and handleRouteAnnounceV3). v2 delta frames carry the same
	// AnnounceRouteFrame payload shape as legacy announce_routes, so
	// the same per-frame entry-count bound applies — see
	// maxRoutesPerAnnounceFrame doc-comment for the rationale.
	if len(frame.AnnounceRoutes) > maxRoutesPerAnnounceFrame {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "routes_update").
			Int("entries", len(frame.AnnounceRoutes)).
			Int("cap", maxRoutesPerAnnounceFrame).
			Msg("routes_update_entry_count_cap_exceeded")
		return
	}
	// Route-quarantine gate (top-level). Per
	// docs/refactoring/route-withdrawal-grace-period.md a peer in route
	// quarantine MUST be silently dropped from EVERY routing-plane
	// receive path, not just from applyAnnounceEntries. The
	// applyAnnounceEntries gate alone is insufficient here: if the
	// quarantined peer reconnects, this session's
	// HasReceivedBaseline() is false, so the baseline gate below
	// would otherwise reply with SendRequestResync — that's outbound
	// routing-control traffic toward a quarantined peer, which the
	// design doc explicitly forbids. Drop the frame and DO NOT
	// create AnnouncePeerState for the peer either, so quarantine
	// produces a clean "as if peer was never heard from" state.
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Str("frame_type", "routes_update").
			Int("entries", len(frame.AnnounceRoutes)).
			Msg("routing_announce_drop_quarantined_sender")
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
			Str("from", senderIdentity.String()).
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

	s.applyAnnounceEntries(senderIdentity, frame.AnnounceRoutes, nil, nil, announceReceiveV2)
}

// handleRequestResync processes an incoming request_resync frame. The peer
// has detected local state desync (e.g. it received a routes_update or a
// route_announce_v3 kind="delta" without a baseline this session, or saw a
// v3 epoch reset that invalidated the prior baseline) and is asking us to
// re-deliver a full baseline. We fulfil the contract by marking our
// per-peer announce state as NeedsFullResync and triggering an immediate
// announce cycle — the forced-full branch of announceToAllPeers takes over
// from there, and sendFullAnnounce honours the first-sync invariant:
// SendAnnounceRoutes (legacy announce_routes) for v1/v2 peers, or
// SendRouteAnnounceV3 with kind="full" for peers that negotiated the v3
// triplet. Either way the recovery frame is a self-contained baseline,
// never a delta.
func (s *Service) handleRequestResync(senderIdentity domain.PeerIdentity) {
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("request_resync_malformed_sender")
		return
	}
	// chatty_routes accounting is DELIBERATELY skipped for
	// request_resync — and this is now consistent with the bulk-frame
	// contract (isAnnouncePlaneBulkFrameType): request_resync is a
	// CONTROL frame, not bulk, so the cmd limiter (30/s → TCP close)
	// plus acceptRequestResyncDebounced own a request_resync flood,
	// NOT the chatty trigger. The earlier code counted it here anyway,
	// which both contradicted that contract and created a self-
	// reinforcing loop: once a peer was quarantined we dropped its
	// announcements, it desynced, it sent request_resync, and that
	// very recovery attempt re-armed the quarantine (escalating
	// strikes up to the 30m cap). Excluding it is what lets a desynced
	// hub actually recover.
	// Route-quarantine gate (top-level) — symmetric with
	// handleRoutesUpdate / handleRouteAnnounceV3. request_resync
	// from a quarantined peer must be silently dropped: handling
	// it calls MarkInvalid(senderIdentity) + TriggerUpdate(),
	// which would force the next announce cycle to ship a full
	// baseline TO that peer. That is outbound routing-control
	// traffic toward a quarantined peer — the exact thing the
	// gates in handleRoutesUpdate / handleRouteAnnounceV3 close
	// — and it would also let a quarantined peer keep our
	// announce loop spinning on forced-full cycles on demand.
	// Position is BEFORE the announceLimiter charge so a
	// quarantined peer's frames do not consume the per-peer
	// announce token budget either.
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Str("frame_type", "request_resync").
			Msg("routing_announce_drop_quarantined_sender")
		return
	}
	// Phase 4 13.7: shared per-peer announce rate limit. Cost 1 —
	// control frame, not entry-bearing.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, 1) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "request_resync").
			Msg("announce_rate_limit_drop")
		return
	}
	// Dedicated accept-side debounce. The limiters above are generic
	// frame-rate brakes (cmdLimiter 30/s at the dispatcher, announce
	// limiter cost 1 here) — a peer sending request_resync at e.g.
	// 10-20/s sails under both, yet each accepted frame flips
	// NeedsFullResync and triggers an announce cycle: full-sync work
	// amplified far beyond what any legitimate desync recovery needs.
	// A peer legitimately needs at most ONE resync per desync event,
	// and the forced-full announce it triggers completes well within
	// the debounce window; a second desync inside the window is
	// recovered by the next periodic full cycle anyway (request_resync
	// is an optimisation, not a correctness requirement — see
	// handleRouteAnnounceV3's baseline-gate notes). Excess requests
	// are dropped without touching the state registry.
	if !s.acceptRequestResyncDebounced(senderIdentity, time.Now()) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Str("frame_type", "request_resync").
			Msg("request_resync_debounced_drop")
		return
	}
	log.Info().
		Str("from", senderIdentity.String()).
		Msg("request_resync_received_forcing_full_sync")
	s.announceLoop.StateRegistry().MarkInvalid(senderIdentity)
	s.announceLoop.TriggerUpdate()
}

// requestResyncAcceptDebounce is the minimum spacing between two
// ACCEPTED request_resync frames from the same peer. Sized to the
// periodic announce cadence: a desync that strikes twice within one
// window is healed by the next periodic full announce regardless,
// so a tighter debounce buys nothing except more forced-full churn.
const requestResyncAcceptDebounce = 30 * time.Second

// acceptRequestResyncDebounced records and gates request_resync
// acceptance per peer: returns true (and stamps the acceptance) when
// no resync was accepted for the peer within
// requestResyncAcceptDebounce, false otherwise. Guarded by peerMu —
// same domain as the quarantine bookkeeping that shares the
// handler's hot path. Stale entries are purged by
// purgeRouteQuarantineState alongside the other per-peer sliding
// windows.
func (s *Service) acceptRequestResyncDebounced(peer domain.PeerIdentity, now time.Time) bool {
	if peer.IsZero() {
		return false
	}
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if last, ok := s.lastResyncAccepted[peer]; ok && now.Sub(last) < requestResyncAcceptDebounce {
		return false
	}
	if s.lastResyncAccepted == nil {
		s.lastResyncAccepted = make(map[domain.PeerIdentity]time.Time)
	}
	s.lastResyncAccepted[peer] = now
	return true
}

// handleRouteAnnounceV3 processes an incoming route_announce_v3 frame (the
// Phase 4 compact wire path, gated by mesh_routing_v3 — see overview §7.1
// and docs/cluster-mesh/phase-4-compact-wire-signed.md §3.1). The frame
// drops the Origin field: the receiver derives the uplink from the sending
// session, so each entry is ingested under Origin == senderIdentity
// (sender-originated semantic, identical to the Origin = localOrigin that
// legacy v3 senders would otherwise put on the wire). Internal storage is
// per-(Identity, Uplink) and Origin is not part of the dedup key, so this
// reuses the exact entry-by-entry trust/withdrawal/UpdateRoute logic in
// applyAnnounceEntries — only the Origin synthesis and the epoch/baseline
// gating are v3-specific.
//
// Epoch handling (overview §7.1): the sender's local table epoch increments
// on table reset (e.g. process restart). ObserveV3Epoch compares it against
// the per-session watermark:
//   - stale (incoming < known): a replay from an older peer process — drop
//     the whole frame, do not touch storage.
//   - reset (incoming > known): the peer's table is fresh, so any diff
//     baseline we hold for it described the OLD table and is invalid. A
//     kind="full" frame is self-contained and simply re-establishes the
//     baseline for the new epoch (no resync needed). A kind="delta" frame
//     after a reset has no valid baseline to diff against, so it is treated
//     exactly like the delta-before-baseline desync below. Note: after a
//     restart the peer's SeqNo counters restart low, so per-(Identity,
//     Uplink) claims whose stored SeqNo now out-ranks the fresh value are
//     rejected until TTL expiry; a full epoch-driven SeqNo reset is a
//     documented Phase 4 follow-up (phase-4 §7).
//   - apply (incoming == known, or first frame this session): normal path.
//
// Baseline gate: a kind="delta" frame before any baseline this session (or
// after an epoch reset) is a desync — handled exactly like routes_update:
// reply with request_resync and drop the delta. request_resync is the
// v2-gated escape hatch; a v3 peer carries v2 in every real deployment
// (localCapabilities advertises both), so the fast recovery works. A
// hypothetical v3-only peer simply recovers on its next periodic kind=full
// cycle, which re-baselines without any signal — request_resync is an
// optimisation, not a correctness requirement. kind="full" is
// self-contained and establishes the baseline.
func (s *Service) handleRouteAnnounceV3(senderIdentity domain.PeerIdentity, senderAddress domain.PeerAddress, frame protocol.RouteAnnounceV3Frame) {
	if !identity.IsValidAddress(senderIdentity.String()) {
		log.Warn().Str("sender", senderIdentity.String()).Msg("route_announce_v3_malformed_sender")
		return
	}
	// chatty_routes quarantine trigger — only v3 kind="delta" counts.
	// kind="full" is a self-contained BASELINE and is excluded for the
	// same reason as legacy announce_routes: the trigger targets delta
	// churn (which cascades announce cycles + propagation), while a
	// baseline is idempotent and bounded by the announceLimiter route
	// bucket below. See handleAnnounceRoutes for the full rationale.
	// (request_resync, a control frame, is likewise excluded — see
	// handleRequestResync.)
	if frame.Kind == protocol.RouteAnnounceV3KindDelta {
		s.recordInboundAnnounceAndMaybeArm(senderIdentity, time.Now())
	}
	// Phase 4 13.7: shared per-peer announce rate limit across all
	// announce-plane wire generations (legacy v1, v2 delta, v3
	// compact). Counts BEFORE epoch + baseline checks below so a
	// flood of malformed-epoch frames still gets throttled. Round-10
	// fix: charge by route-entry count — see the matching comment
	// in handleAnnounceRoutes. A v3 kind="full" frame in a multi-
	// chunk sync drains tokens proportional to its entries, so
	// the legitimate full-sync of N routes consumes N tokens
	// instead of getting silently truncated at the previous
	// 30-frame burst.
	if s.announceLimiter != nil && !s.announceLimiter.allow(senderIdentity, announceCostForEntries(len(frame.Entries))) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "route_announce_v3").
			Str("kind", frame.Kind).
			Int("entries", len(frame.Entries)).
			Msg("announce_rate_limit_drop")
		return
	}
	// Phase 4 13.5 receive-side cap (shared with handleAnnounceRoutes
	// and handleRoutesUpdate). Applies before epoch / baseline gating
	// so the heavy-weight verifyRouteAnnounceV3Sigs path never runs
	// on a frame that already violates the per-frame entry-count
	// bound — see maxRoutesPerAnnounceFrame doc-comment for rationale.
	if len(frame.Entries) > maxRoutesPerAnnounceFrame {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("frame_type", "route_announce_v3").
			Str("kind", frame.Kind).
			Int("entries", len(frame.Entries)).
			Int("cap", maxRoutesPerAnnounceFrame).
			Msg("route_announce_v3_entry_count_cap_exceeded")
		return
	}
	// Route-quarantine gate (top-level) — symmetric with
	// handleRoutesUpdate above. See that comment for the rationale:
	// the delta/epoch baseline branch below can emit SendRequestResync
	// back to the peer, which is outbound routing-control traffic
	// toward a quarantined peer and explicitly forbidden by
	// docs/refactoring/route-withdrawal-grace-period.md. We also do
	// NOT touch AnnouncePeerState (no GetOrCreate, no ObserveV3Epoch)
	// so the quarantine window leaves no per-peer routing state
	// residue.
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Str("frame_type", "route_announce_v3").
			Str("kind", frame.Kind).
			Int("entries", len(frame.Entries)).
			Msg("routing_announce_drop_quarantined_sender")
		return
	}

	peerState := s.announceLoop.StateRegistry().GetOrCreate(senderIdentity)

	verdict := peerState.ObserveV3Epoch(frame.Epoch)
	if verdict == routing.V3EpochStale {
		log.Debug().
			Str("from", senderIdentity.String()).
			Uint64("frame_epoch", frame.Epoch).
			Msg("route_announce_v3_stale_epoch_dropped")
		return
	}

	isFull := frame.Kind == protocol.RouteAnnounceV3KindFull
	// A delta needs a baseline valid for the peer's CURRENT epoch. No
	// baseline this session, or an epoch reset that invalidated the prior
	// baseline, both force the resync recovery path.
	if !isFull && (verdict == routing.V3EpochReset || !peerState.HasReceivedBaseline()) {
		log.Warn().
			Str("from", senderIdentity.String()).
			Str("address", string(senderAddress)).
			Uint64("frame_epoch", frame.Epoch).
			Bool("epoch_reset", verdict == routing.V3EpochReset).
			Int("delta_routes", len(frame.Entries)).
			Msg("route_announce_v3_delta_needs_baseline_request_resync")
		if senderAddress != "" {
			s.SendRequestResync(s.runCtx, senderAddress)
		}
		return
	}

	if verdict == routing.V3EpochReset {
		log.Info().
			Str("from", senderIdentity.String()).
			Uint64("frame_epoch", frame.Epoch).
			Msg("route_announce_v3_epoch_advanced_full_rebaseline")
	}

	mode := announceReceiveV3Delta
	if isFull {
		mode = announceReceiveV3Full
	}
	frames, sigs := routeAnnounceV3EntriesToWire(senderIdentity, frame.Entries)
	// Phase 4 13.2-B/C/P2: verify attested-links signatures before
	// ingest and tag the verification outcome per entry so storage can
	// rank verified claims above unsigned/unverifiable ones via the
	// Phase 4 13.2-C trust-score bonus (CompositeScore). Verification
	// is gated on the per-session mesh_attested_links_v1 capability
	// negotiation: when the peer did NOT negotiate the cap, the sig
	// bytes are forwarded through storage (so a downstream peer with
	// the cap can still verify) but treated as informational only on
	// our side — no ed25519.Verify, no entry drop on invalid, no
	// trust-score bonus on success. When the cap IS negotiated:
	// present-but-invalid signatures (sig included, pubkey known,
	// ed25519.Verify failed) drop the offending entry; present-with-
	// unknown-pubkey and absent signatures pass through with
	// verified=false (Tier-2 leniency). See
	// docs/protocol/attested_links.md "Capability negotiation" and
	// "Receive contract" for the full trust ladder.
	var verified []bool
	if s.peerSupportsAttestedLinks(senderAddress) {
		frames, sigs, verified = s.verifyRouteAnnounceV3Sigs(senderIdentity, frames, sigs, frame.Epoch)
	} else {
		// Cap not negotiated — sig bytes flow through unchanged for
		// downstream forwarding, but we mark every entry verified=false
		// so CompositeScore does not award the signed-route bonus
		// based on locally-unverified sigs.
		verified = make([]bool, len(frames))
	}
	s.applyAnnounceEntries(senderIdentity, frames, sigs, verified, mode)
}

// verifyRouteAnnounceV3Sigs filters frames+sigs by ed25519 signature
// validity (Phase 4 13.2-B). Returns the surviving entries in parallel
// slices preserving relative order; entries with a present sig that
// fails verification are dropped with a warn log. Entries with no sig
// or with a sig but unknown destination pubkey pass through unchanged.
//
// The verifier uses CanonicalSigningBytes() — identity and extra
// ONLY. Hops, epoch, AND seq_no are all per-emitter wire fields
// (hops by receiver +1 convention; epoch by each sender's local
// table-generation counter; seq_no by the outbound SeqNo helpers
// in route_store.go that can advance the wire value past the
// origin's native seq on transit re-emit) and are deliberately
// EXCLUDED from the signed payload — see
// docs/protocol/attested_links.md "Canonical signing bytes" for
// the multi-hop verification rationale. The epoch parameter is
// preserved on the Go signature for diagnostic logging on invalid
// signatures (so operators can correlate failures with the sending
// peer's table generation) but does NOT enter the canonical bytes.
// Canonical bytes are reconstructible from the wire entry's
// identity and extra alone, so transit nodes that forwarded the
// entry verbatim do not perturb the verification — exactly what
// the Phase 5 anchor-published DHT lookup requires.
func (s *Service) verifyRouteAnnounceV3Sigs(senderIdentity domain.PeerIdentity, frames []protocol.AnnounceRouteFrame, sigs [][]byte, epoch uint64) ([]protocol.AnnounceRouteFrame, [][]byte, []bool) {
	if len(frames) == 0 {
		return frames, sigs, nil
	}
	outFrames := frames[:0]
	outSigs := sigs[:0]
	verified := make([]bool, 0, len(frames))
	for i, f := range frames {
		var sig []byte
		if i < len(sigs) {
			sig = sigs[i]
		}
		if len(sig) == 0 {
			// Unsigned entry — pass through with verified=false;
			// Tier 2 accepts it, the 13.2-C trust-score rank penalty
			// arbitrates the tie-break.
			outFrames = append(outFrames, f)
			outSigs = append(outSigs, sig)
			verified = append(verified, false)
			continue
		}
		pubkey, ok := s.publicKeyForIdentity(domain.PeerIdentityFromWire(f.Identity))
		if !ok {
			// Sig present but we cannot resolve the destination
			// pubkey — treat as unverified (Tier-2 lenient: we cannot
			// distinguish honest-with-unknown-key from malicious-
			// with-fake-key, and dropping would prevent learning
			// about identities we have not yet handshaked with). Log
			// at debug so operators can correlate. The sig bytes
			// still propagate so a peer with the pubkey can verify
			// independently; locally the entry is ranked as
			// unverified (verified=false) by CompositeScore.
			log.Debug().
				Str("from", senderIdentity.String()).
				Str("entry_identity", f.Identity).
				Msg("route_announce_v3_sig_pubkey_unknown_passthrough")
			outFrames = append(outFrames, f)
			outSigs = append(outSigs, sig)
			verified = append(verified, false)
			continue
		}
		v3e := protocol.RouteAnnounceV3Entry{
			Identity: f.Identity,
			SeqNo:    f.SeqNo,
			Extra:    f.Extra,
		}
		if !ed25519.Verify(pubkey, v3e.CanonicalSigningBytes(), sig) {
			// Present-but-invalid signature — concrete malicious or
			// corrupted claim. Drop the entry; do NOT pass to storage.
			log.Warn().
				Str("from", senderIdentity.String()).
				Str("entry_identity", f.Identity).
				Uint64("seq_no", f.SeqNo).
				Uint64("epoch", epoch).
				Msg("route_announce_v3_sig_invalid_entry_dropped")
			continue
		}
		// Verified — pass through with verified=true so the storage
		// receives the trust-tier tag for CompositeScore.
		outFrames = append(outFrames, f)
		outSigs = append(outSigs, sig)
		verified = append(verified, true)
	}
	return outFrames, outSigs, verified
}

// routeAnnounceV3EntriesToWire adapts compact v3 entries to the
// AnnounceRouteFrame shape applyAnnounceEntries consumes, plus a
// parallel attested-links signature slice (Phase 4 13.2-A,
// mesh_attested_links_v1). The dropped Origin field is synthesised as
// senderIdentity (sender-originated semantic): this satisfies the
// own-origin forgery guard (Origin == self only if the sender is us,
// which never happens) and the withdrawal authority guard (Origin ==
// sender holds by construction, so a v3 withdrawal — Hops >=
// HopsInfinity — from the announcing uplink is accepted). Extra is
// forwarded verbatim.
//
// Sig handling: the v3 wire field carries the Ed25519 attestation as a
// base64-encoded string (see RouteAnnounceV3Entry.Sig). The adapter
// decodes each entry's sig into the parallel sigs slice; a malformed
// base64 is treated as "no signature" (nil slot) with a debug log so a
// hostile peer cannot crash ingest, and verification (13.2-B) will
// surface the absent-sig case via the trust-score path. Empty Sig
// strings produce nil slots without any log noise — that is the
// normal Tier-2 unsigned-entry path. The returned slices are always
// the same length; passing them in lockstep to applyAnnounceEntries
// preserves per-entry index alignment.
func routeAnnounceV3EntriesToWire(senderIdentity domain.PeerIdentity, entries []protocol.RouteAnnounceV3Entry) ([]protocol.AnnounceRouteFrame, [][]byte) {
	if len(entries) == 0 {
		return nil, nil
	}
	frames := make([]protocol.AnnounceRouteFrame, len(entries))
	sigs := make([][]byte, len(entries))
	// senderIdentity is constant across the batch — encode its hex once
	// instead of re-allocating the same string per entry.
	senderHex := senderIdentity.String()
	for i, e := range entries {
		frames[i] = protocol.AnnounceRouteFrame{
			Identity: e.Identity,
			Origin:   senderHex,
			Hops:     int(e.Hops),
			SeqNo:    e.SeqNo,
			Extra:    e.Extra,
		}
		if e.Sig != "" {
			decoded, err := base64.StdEncoding.DecodeString(e.Sig)
			if err != nil {
				log.Debug().
					Err(err).
					Str("from", senderIdentity.String()).
					Str("entry_identity", e.Identity).
					Msg("route_announce_v3_sig_decode_failed")
				// Leave sigs[i] nil — verification path treats this as
				// unsigned. The entry itself still gets ingested.
				continue
			}
			sigs[i] = decoded
		}
	}
	return frames, sigs
}

// announceReceiveMode tags the wire path that delivered the announce-plane
// entries. Used by applyAnnounceEntries to select the baseline-update side
// effect: set on baseline-class frames (legacy announce_routes, v3
// kind="full") and NOT on delta-class frames (v2 routes_update, v3
// kind="delta"). Delta frames are gated upstream by handleRoutesUpdate /
// handleRouteAnnounceV3 against the same hasReceivedBaseline flag, so the
// delta path can rely on the baseline already being established before
// applyAnnounceEntries runs.
type announceReceiveMode int

const (
	announceReceiveLegacy announceReceiveMode = iota
	announceReceiveV2
	// announceReceiveV3Full tags a route_announce_v3 frame with
	// kind="full". Like legacy announce_routes it establishes the
	// receive-side baseline for the session (MarkBaselineReceived), so a
	// subsequent kind="delta" frame is safe to apply.
	announceReceiveV3Full
	// announceReceiveV3Delta tags a route_announce_v3 frame with
	// kind="delta". Like routes_update it never establishes a baseline by
	// itself — handleRouteAnnounceV3 gates it on HasReceivedBaseline.
	announceReceiveV3Delta
)

// establishesBaseline reports whether a receive mode delivers a
// self-contained first-sync snapshot that unlocks subsequent delta
// application. Legacy announce_routes and v3 full do; v2 routes_update
// and v3 delta do not.
func (m announceReceiveMode) establishesBaseline() bool {
	return m == announceReceiveLegacy || m == announceReceiveV3Full
}

// wireTypeLabel returns the structured-log wire_type string for the mode.
func (m announceReceiveMode) wireTypeLabel() string {
	switch m {
	case announceReceiveV2:
		return "routes_update"
	case announceReceiveV3Full:
		return "route_announce_v3:full"
	case announceReceiveV3Delta:
		return "route_announce_v3:delta"
	default:
		return "announce_routes"
	}
}

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
//
// attestedSigs is the Phase 4 13.2-A parallel-slice of per-entry Ed25519
// attestation signatures (mesh_attested_links_v1). When nil the entries
// land in storage with empty AttestedSig — the legacy v1/v2 wire path
// has no sig field, so its callers always pass nil. When non-nil the
// slice MUST be the same length as wireRoutes; index i carries the sig
// extracted from the corresponding v3 entry's "sig" field. A nil
// element inside a non-nil slice means "this v3 entry was unsigned",
// which is allowed at Tier 2 (rank penalty in 13.2-C; verification in
// 13.2-B). The bytes are threaded opaquely into RouteEntry.AttestedSig
// → UplinkClaim.AttestedSig so a re-announce on the v3 path can
// forward them unchanged.
//
// attestedVerified is the Phase 4 13.2-C parallel-slice of per-entry
// verification outcomes. When nil all entries land with verified=false
// (legacy callers — no sigs, no verification). When non-nil it must be
// the same length as wireRoutes; index i is true if and only if the
// verifier successfully ed25519.Verify'd attestedSigs[i] against the
// destination identity's public key. The flag is stored on
// RouteEntry.AttestedSigVerified → UplinkClaim.AttestedSigVerified so
// CompositeScore can apply the trust-score bonus at rank time without
// re-verifying.
func (s *Service) applyAnnounceEntries(senderIdentity domain.PeerIdentity, wireRoutes []protocol.AnnounceRouteFrame, attestedSigs [][]byte, attestedVerified []bool, mode announceReceiveMode) {
	if senderIdentity.IsZero() {
		log.Warn().Msg("announce_routes_no_sender_identity")
		return
	}

	// Per-peer route quarantine: if the sender is currently in
	// quarantine because of repeated disconnect/reconnect cycles,
	// silently drop their entire routing snapshot. We keep them as
	// a direct peer in our table (push/relay to the sender still
	// works), we just do not trust their view of the network until
	// the quarantine window elapses. See routing_route_quarantine.go.
	if s.IsPeerInRouteQuarantine(senderIdentity) {
		log.Debug().
			Str("from", senderIdentity.String()).
			Int("entries", len(wireRoutes)).
			Msg("announce_routes_dropped_quarantined_peer")
		return
	}

	accepted := 0
	unchanged := 0
	rejected := 0
	drainIdentities := make(map[domain.PeerIdentity]struct{})

	for i, wireRoute := range wireRoutes {
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
				Str("from", senderIdentity.String()).
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
				Str("from", senderIdentity.String()).
				Msg("announce_rejected_forged_own_origin")
			continue
		}

		if wireRoute.Hops >= routing.HopsInfinity {
			// Withdrawal: only the origin may emit a wire withdrawal.
			// Transit nodes that lose upstream must invalidate locally and
			// stop advertising — they must not forward hops=16 on the wire.
			// Accepting a withdrawal from a non-origin sender would let an
			// intermediate neighbor unilaterally kill a route it does not own.
			if wireRoute.Origin != senderIdentity.String() {
				rejected++
				log.Warn().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", senderIdentity.String()).
					Msg("announce_rejected_transit_withdrawal")
				continue
			}

			withdrawnID := domain.PeerIdentityFromWire(wireRoute.Identity)
			if s.routingTable.WithdrawRoute(
				withdrawnID,
				domain.PeerIdentityFromWire(wireRoute.Origin),
				senderIdentity,
				wireRoute.SeqNo,
			) {
				accepted++
				// After withdrawing the (Identity, Uplink) claim we
				// just tombstoned, a less-preferred backup claim via
				// a different uplink may now be the active path
				// (post-Phase-A storage keys per (Identity, Uplink),
				// so other uplinks for this Identity are untouched
				// by this withdrawal). If Lookup still returns
				// reachable entries for this identity, trigger a
				// drain so pending send_message frames can be
				// delivered via the backup route immediately instead
				// of waiting for the retry loop.
				if remaining := s.routingTable.Lookup(withdrawnID); len(remaining) > 0 {
					drainIdentities[withdrawnID] = struct{}{}
				}
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Uint64("seq", wireRoute.SeqNo).
					Str("from", senderIdentity.String()).
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
			Identity: domain.PeerIdentityFromWire(wireRoute.Identity),
			Origin:   domain.PeerIdentityFromWire(wireRoute.Origin),
			NextHop:  senderIdentity,
			Hops:     receivedHops,
			SeqNo:    wireRoute.SeqNo,
			Source:   routing.RouteSourceAnnouncement,
			Extra:    wireRoute.Extra,
		}
		// Phase 4 13.2-A: thread the parallel attested-links signature
		// (extracted from the v3 wire entry by the caller) into the
		// boundary RouteEntry so toUplinkClaim stores it for later
		// re-emit. nil attestedSigs (legacy v1/v2 callers) or a nil
		// element (unsigned v3 entry) both leave AttestedSig empty.
		if i < len(attestedSigs) {
			entry.AttestedSig = attestedSigs[i]
		}
		// Phase 4 13.2-C: thread the verification outcome into the
		// boundary so CompositeScore can apply the trust-score bonus.
		// nil attestedVerified (legacy callers) leaves verified=false.
		if i < len(attestedVerified) {
			entry.AttestedSigVerified = attestedVerified[i]
		}

		status, err := s.routingTable.UpdateRoute(entry)
		if err != nil {
			log.Warn().
				Err(err).
				Str("identity", wireRoute.Identity).
				Str("origin", wireRoute.Origin).
				Str("from", senderIdentity.String()).
				Msg("route_update_rejected_invalid")
			rejected++
			continue
		}
		switch status {
		case routing.RouteAccepted:
			accepted++
			drainIdentities[domain.PeerIdentityFromWire(wireRoute.Identity)] = struct{}{}
		case routing.RouteUnchanged:
			unchanged++
			drainIdentities[domain.PeerIdentityFromWire(wireRoute.Identity)] = struct{}{}
		case routing.RouteRejected:
			rejected++
			existing := s.routingTable.InspectTriple(entry.DedupKey())
			if existing != nil {
				log.Debug().
					Str("identity", wireRoute.Identity).
					Str("origin", wireRoute.Origin).
					Str("from", senderIdentity.String()).
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
					Str("from", senderIdentity.String()).
					Int("incoming_hops", receivedHops).
					Uint64("incoming_seq", wireRoute.SeqNo).
					Msg("route_rejected_no_existing_triple")
			}
		}
	}

	wireType := mode.wireTypeLabel()
	log.Debug().
		Str("from", senderIdentity.String()).
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

	// Baseline tracking: a baseline-class arrival (legacy announce_routes
	// OR v3 route_announce_v3 kind="full") is the protocol signal that the
	// peer has delivered the first-sync snapshot for this session.
	// Subsequent delta frames from the peer (v2 routes_update or v3
	// kind="delta") can now be applied safely. The delta wire paths
	// deliberately leave the flag untouched — see handleRoutesUpdate and
	// the kind="delta" branch of handleRouteAnnounceV3 for the gating
	// contract.
	if mode.establishesBaseline() {
		s.announceLoop.StateRegistry().GetOrCreate(senderIdentity).MarkBaselineReceived()
	}

	// Stability signal: a successful announce-plane round-trip from the
	// peer is concrete evidence the peer is back to stable operation,
	// so clear its consecutive-flap streak. Without this hook a peer
	// that recovered cleanly would continue to see the exp-backoff
	// hold-down growing on the next disconnect even though it had
	// already proven liveness — see routing.Table.RecordSuccessfulRouteAdd.
	// The stable-window implicit reset is preserved as a fallback for
	// peers that go quiet without flapping; this hook fires earlier on
	// the actual recovery event.
	//
	// Gated on accepted+unchanged > 0: both outcomes prove the wire
	// path is alive — a reconnected peer re-announcing the exact same
	// table (all unchanged) still demonstrates liveness, matching the
	// drain-on-unchanged contract enforced a few lines below. Empty or
	// fully-rejected frames are excluded so a malicious peer cannot
	// mask its own flap streak by spamming announce frames whose
	// entries the trust filter discards.
	if accepted+unchanged > 0 {
		s.routingTable.RecordSuccessfulRouteAdd(senderIdentity)
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
		// Held sender-owned DMs to these now-routable recipients are re-armed
		// immediately (self-checked inside the kick), independent of pending —
		// they live in awaitingDelivered, so the pending-empty fast path below
		// must not skip them. This keeps the "route appeared → deliver now"
		// promise for reachability-gated sends instead of waiting out the
		// retry backoff.
		s.kickDeliveryRetriesForReachable(drainIdentities)
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
	// A newly-reachable recipient also re-arms any held sender-owned delivery
	// retry for it (dispatchEnvelopeRetry holds messages to unreachable
	// recipients). Unlike the pending drain this is unconditional — held
	// retries live in awaitingDelivered, not s.pending, so the pending-empty
	// fast path must not skip them.
	s.kickDeliveryRetriesForReachable(identities)
	s.deliveryMu.RLock()
	hasPending := len(s.pending) > 0
	s.deliveryMu.RUnlock()
	if hasPending {
		s.goBackground(func() { s.drainPendingForIdentities(identities) })
	}
}
