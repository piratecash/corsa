// routing_sync.go hosts the receive-path for Phase 3 PR 12.5
// incremental-sync digest exchange (route_sync_digest_v1 /
// route_sync_summary_v1 wire frames defined in
// internal/core/protocol/frame_route_sync.go and overview §2.2).
//
// Two responsibilities live here:
//
//   - handleRouteSyncDigest answers an inbound digest by computing
//     the receiver's view of (Identity, SeqNo) pairs reachable
//     through the sender peer, comparing against the wire digest,
//     and emitting route_sync_summary_v1 with the verdict.
//   - handleRouteSyncSummary ingests an inbound summary. On a
//     Match=true whose echo correlates with the digest we emitted,
//     AnnounceLoop.ConfirmPeerDigestMatch extends the per-peer
//     suppression window; a mismatch clears it; an empty or
//     uncorrelated echo is dropped. The next forced full sync then
//     proceeds (or stays suppressed) accordingly.
//
// Both paths are wired into dispatchNetworkFrame in service.go
// behind a CapMeshRouteSyncV1 capability gate. The sender side
// (on-reconnect digest emit + on-disconnect snapshot recording)
// lives in the session lifecycle hooks added to the existing
// peer-management paths.
//
// Architectural invariants from CLAUDE.md and Phase 3 §2.4:
//
//   - digest / summary are P2P wire commands gated by
//     mesh_route_sync_v1; they travel through dispatchNetworkFrame,
//     never through CommandTable. The digest match is a local
//     hint, not authoritative state — a malicious peer claiming
//     match=true on mismatched content only causes ONE skipped
//     forced-full-sync window; the next delta cycle restores
//     correctness.
//   - Response send goes through s.sendFrameViaNetwork (single-
//     writer invariant). No conn.Write call site.
//   - "route_sync_digest_v1" / "route_sync_summary_v1" names carry
//     the exact same contract on every transport path. No enriched
//     payload aliasing — UI diagnostics around digest activity
//     should live in a separate RPC command (PR 12.7 reputation
//     observability is a precedent), not in this wire pair.
package node

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// emitRouteSyncDigest builds a RouteSyncDigestFrame from the
// supplied cached snapshot fields and emits it to the peer
// through sendFrameToIdentity. The send is capability-gated to
// CapMeshRouteSyncV1 by sendFrameToIdentity itself, so a peer
// that does not advertise the capability silently never receives
// the frame (additive interop contract).
//
// Marshal failures are logged at warn and the emit is dropped —
// the peer will simply not receive a digest this reconnect and
// the announce cycle proceeds with its normal forced-full-sync,
// which is the safe degradation path.
//
// Production caller: onPeerSessionEstablished after a successful
// ConsumePeerDigestSnapshot.
func (s *Service) emitRouteSyncDigest(peer domain.PeerIdentity, digest string, count uint32, generatedAt time.Time) {
	frame := protocol.RouteSyncDigestFrame{
		Type:                 protocol.RouteSyncDigestFrameType,
		Digest:               digest,
		KnownIdentitiesCount: count,
		GeneratedAt:          generatedAt.UTC().Format(time.RFC3339),
	}
	raw, err := protocol.MarshalRouteSyncDigestFrame(frame)
	if err != nil {
		log.Warn().
			Err(err).
			Str("peer_identity", string(peer)).
			Str("digest", digest).
			Msg("route_sync_digest_marshal_failed")
		return
	}
	wire := protocol.Frame{
		Type:    protocol.RouteSyncDigestFrameType,
		RawLine: string(raw) + "\n",
	}
	if !s.sendFrameToIdentity(peer, wire, domain.CapMeshRouteSyncV1) {
		log.Debug().
			Str("peer_identity", string(peer)).
			Str("digest", digest).
			Msg("route_sync_digest_send_skipped")
		return
	}
	log.Debug().
		Str("peer_identity", string(peer)).
		Str("digest", digest).
		Uint32("count", count).
		Msg("route_sync_digest_emitted")
}

// recordPeerDigestOnSessionClose computes the current per-peer
// OUTBOUND announce digest (Table.AnnounceDigestFor — what we would
// announce to the peer, keyed by the per-peer wire SeqNos the peer
// stored) and stashes it in the in-memory cache so a reconnect within
// SessionDigestCacheTTL can short-circuit the forced full sync.
// Production caller: onPeerSessionClosed on the last-session transition.
//
// Recording happens BEFORE any storage-mutating cleanup
// (RemoveDirectPeer, InvalidateTransitRoutes) so the snapshot
// reflects the outbound view the peer held at the moment the
// session closed — recording after would yield an empty digest
// because RemoveDirectPeer would already have tombstoned the
// peer's claims.
//
// The capability check is done at the caller site (we only
// record when the peer's session set ever advertised
// CapMeshRouteSyncV1) so the cache does not bloat with entries
// for peers that will never consume them.
func (s *Service) recordPeerDigestOnSessionClose(peer domain.PeerIdentity) {
	// Use the OUTBOUND announce projection to the peer, not the routes we
	// learned THROUGH the peer (SyncDigestFor). On reconnect the peer
	// compares this against its own via-(this node) view; the two sets
	// only line up when we send what we would announce to it. See
	// Table.AnnounceDigestFor.
	digest, count := s.routingTable.AnnounceDigestFor(peer)
	s.routingTable.RecordPeerDigestSnapshot(peer, digest, count, time.Now().UTC())
	log.Debug().
		Str("peer_identity", string(peer)).
		Str("digest", digest).
		Uint32("count", count).
		Msg("route_sync_digest_recorded_for_reconnect")
}

// handleRouteSyncDigest answers an inbound route_sync_digest_v1
// from the given senderIdentity. The receiver computes its OWN
// digest over (Identity, SeqNo) pairs of every live claim
// reachable through senderIdentity (split-horizon: the sender's
// own identity is excluded), compares against frame.Digest, and
// emits route_sync_summary_v1 echoing the original digest so the
// sender can correlate.
//
// Single-writer invariant: the summary send goes through
// sendFrameViaNetwork, the only sanctioned write path that
// funnels through the per-connection designated writer goroutine.
//
// Mixed-version compat: the capability gate at the dispatch
// switch already ensures only peers advertising
// mesh_route_sync_v1 reach this handler, so no additional
// negotiation logic is needed here.
//
// Marshal failures on the summary side are surfaced as a debug
// log and the reply is dropped — the sender will time out
// waiting for the summary and proceed with its normal full sync,
// which is the safe degradation path.
func (s *Service) handleRouteSyncDigest(connID domain.ConnID, senderIdentity domain.PeerIdentity, frame protocol.RouteSyncDigestFrame) {
	if senderIdentity == "" {
		// Sender identity not yet resolved (race against auth
		// completion). The frame can't be answered meaningfully
		// because the digest only makes sense scoped to a
		// specific peer — drop silently.
		log.Debug().
			Str("digest", frame.Digest).
			Msg("route_sync_digest_dropped_no_sender_identity")
		return
	}

	localDigest, localCount := s.routingTable.SyncDigestFor(senderIdentity)
	match := localDigest == frame.Digest

	summary := protocol.RouteSyncSummaryFrame{
		Type:           protocol.RouteSyncSummaryFrameType,
		Digest:         frame.Digest,
		Match:          match,
		ExpectFullSync: !match,
	}
	raw, err := protocol.MarshalRouteSyncSummaryFrame(summary)
	if err != nil {
		log.Warn().
			Err(err).
			Str("sender_identity", string(senderIdentity)).
			Str("digest", frame.Digest).
			Bool("match", match).
			Msg("route_sync_summary_marshal_failed")
		return
	}
	rawLine := string(raw) + "\n"
	_ = s.sendFrameViaNetwork(s.runCtx, connID, protocol.Frame{
		Type:    protocol.RouteSyncSummaryFrameType,
		RawLine: rawLine,
	})

	log.Debug().
		Str("sender_identity", string(senderIdentity)).
		Str("their_digest", frame.Digest).
		Uint32("their_count", frame.KnownIdentitiesCount).
		Str("our_digest", localDigest).
		Uint32("our_count", localCount).
		Bool("match", match).
		Msg("route_sync_digest_compared")
}

// handleRouteSyncSummary ingests an inbound route_sync_summary_v1
// from the given senderIdentity. An empty echo digest is dropped
// (no correlation — UnmarshalRouteSyncSummaryFrame contract). On
// Match=true the handler calls AnnounceLoop.ConfirmPeerDigestMatch,
// which extends the suppression window ONLY when the echo correlates
// with the digest we emitted to this peer on reconnect; an
// uncorrelated match is ignored. On Match=false it clears any pending
// window so the next cycle full-syncs promptly.
//
// The suppression window length is owned by the AnnounceLoop:
// ConfirmPeerDigestMatch derives it from EffectiveForcedFullSyncInterval
// of the calling node (min(2*AnnounceInterval, DefaultTTL/2)), so a
// single correlated reply defers at most one forced-full-sync cycle.
// The handler passes only the echo digest and `now`; it does not
// compute the deadline.
//
// Zero-trust invariant (Phase 3 §4.5): the summary is a local
// hint, not authoritative routing state. A malicious peer
// claiming match=true on a mismatched digest only causes ONE
// missed forced-full-sync; the delta path keeps propagating any
// real state changes, and the next forced-full once the
// suppression elapses re-confirms the snapshot. No trust is
// transferred through this exchange — the announce plane
// remains the source of truth for routing state.
func (s *Service) handleRouteSyncSummary(senderIdentity domain.PeerIdentity, frame protocol.RouteSyncSummaryFrame) {
	if senderIdentity == "" {
		log.Debug().
			Str("digest", frame.Digest).
			Bool("match", frame.Match).
			Msg("route_sync_summary_dropped_no_sender_identity")
		return
	}
	if frame.Digest == "" {
		// UnmarshalRouteSyncSummaryFrame deliberately tolerates an empty
		// echo to leave room for future revisions, and documents that the
		// receive handler must treat it as "no correlation, drop the
		// suppression update". An empty echo cannot be tied to a digest we
		// actually emitted, so a malformed or future summary must never
		// move the suppression state in either direction.
		log.Debug().
			Str("sender_identity", string(senderIdentity)).
			Bool("match", frame.Match).
			Msg("route_sync_summary_dropped_empty_digest")
		return
	}
	if s.announceLoop == nil {
		// Defensive — production paths always wire announceLoop.
		// Tests that build a partial Service (no AnnounceLoop)
		// would otherwise dereference a nil interface here.
		log.Debug().
			Str("sender_identity", string(senderIdentity)).
			Msg("route_sync_summary_dropped_no_announce_loop")
		return
	}
	if !frame.Match {
		// Mismatch — the digests diverged, so the receiver needs a
		// fresh full sync. Clear any pending suppression window armed
		// on reconnect (MarkPeerDigestPending) so the next announce
		// cycle full-syncs promptly instead of waiting out the grace.
		// If no window is active this is a no-op. Logged at debug to
		// keep the operator-visible signal honest about which peers had
		// digest churn since their last reconnect.
		s.announceLoop.ClearPeerDigestSuppression(senderIdentity)
		log.Debug().
			Str("sender_identity", string(senderIdentity)).
			Str("digest", frame.Digest).
			Msg("route_sync_summary_mismatch_cleared_suppression")
		return
	}
	// Match — extend suppression ONLY if the echo correlates with the
	// digest we emitted to this peer on reconnect. A stale, unsolicited,
	// or spoofed summary that echoes some other digest is ignored, so it
	// cannot suppress a forced full sync it has no relation to.
	if s.announceLoop.ConfirmPeerDigestMatch(senderIdentity, frame.Digest, time.Now().UTC()) {
		log.Debug().
			Str("sender_identity", string(senderIdentity)).
			Str("digest", frame.Digest).
			Msg("route_sync_summary_match_suppression_armed")
		return
	}
	log.Debug().
		Str("sender_identity", string(senderIdentity)).
		Str("digest", frame.Digest).
		Msg("route_sync_summary_match_uncorrelated_ignored")
}
