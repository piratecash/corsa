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
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routeSyncEntriesToWire converts the routing version vector to its wire
// form (compact (Identity-hex, SeqNo) pairs) for RouteSyncDigestFrame.
// A nil/empty vector yields nil so the frame omits the entries field
// (omitempty), keeping the wire shape identical to a legacy digest.
func routeSyncEntriesToWire(entries []routing.DigestEntry) []protocol.RouteSyncDigestEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]protocol.RouteSyncDigestEntry, len(entries))
	for i, e := range entries {
		out[i] = protocol.RouteSyncDigestEntry{Identity: e.Identity.String(), SeqNo: e.MaxSeqNo}
	}
	return out
}

// routeSyncEntriesFromWire is the inverse: it decodes the wire vector
// into routing.DigestEntry for Table.ReconcileSyncVector. Malformed or
// zero identities are dropped (a zero identity cannot match any stored
// claim's uplink-keyed bucket anyway).
func routeSyncEntriesFromWire(entries []protocol.RouteSyncDigestEntry) []routing.DigestEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]routing.DigestEntry, 0, len(entries))
	for _, e := range entries {
		id := domain.PeerIdentityFromWire(e.Identity)
		if id.IsZero() {
			continue
		}
		out = append(out, routing.DigestEntry{Identity: id, MaxSeqNo: e.SeqNo})
	}
	return out
}

// buildRouteSyncDigestWire marshals a route_sync_digest_v1 wire line,
// dropping the Phase-0 version vector if including it would push the line
// past protocol.MaxFrameLine (128 KiB — the command-plane reader closes
// any frame above it). A large vector blows past that exactly at the
// table size the vector is for (~122 KiB at 2000 entries), which would
// both drop the heartbeat and break the peer's frame stream. The announce
// loop already skips the vector above maxRouteSyncVectorEntries, but the
// reconnect cache path (emitRouteSyncDigest) has no such gate, so this is
// the hard backstop: on a drop the receiver degrades to the hash-only
// compare and converges via the normal full fallback. The hash-only frame
// is tiny and always fits.
func buildRouteSyncDigestWire(digest string, entries []routing.DigestEntry, count uint32, generatedAt time.Time) (protocol.Frame, error) {
	frame := protocol.RouteSyncDigestFrame{
		Type:                 protocol.RouteSyncDigestFrameType,
		Digest:               digest,
		KnownIdentitiesCount: count,
		GeneratedAt:          generatedAt.UTC().Format(time.RFC3339),
		Entries:              routeSyncEntriesToWire(entries),
	}
	raw, err := protocol.MarshalRouteSyncDigestFrame(frame)
	if err != nil {
		return protocol.Frame{}, err
	}
	// +1 for the trailing newline the wire line carries, matching the
	// receive-side line counter that enforces MaxFrameLine.
	if len(raw)+1 > protocol.MaxFrameLine {
		frame.Entries = nil
		raw, err = protocol.MarshalRouteSyncDigestFrame(frame)
		if err != nil {
			return protocol.Frame{}, err
		}
	}
	return protocol.Frame{
		Type:    protocol.RouteSyncDigestFrameType,
		RawLine: string(raw) + "\n",
	}, nil
}

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
func (s *Service) emitRouteSyncDigest(peer domain.PeerIdentity, digest string, entries []routing.DigestEntry, count uint32, generatedAt time.Time) {
	wire, err := buildRouteSyncDigestWire(digest, entries, count, generatedAt)
	if err != nil {
		log.Warn().
			Err(err).
			Str("peer_identity", peer.String()).
			Str("digest", digest).
			Msg("route_sync_digest_marshal_failed")
		return
	}
	if !s.sendFrameToIdentity(peer, wire, domain.CapMeshRouteSyncV1) {
		log.Debug().
			Str("peer_identity", peer.String()).
			Str("digest", digest).
			Msg("route_sync_digest_send_skipped")
		return
	}
	s.digestStats.heartbeatsSent.Add(1)
	log.Debug().
		Str("peer_identity", peer.String()).
		Str("digest", digest).
		Uint32("count", count).
		Msg("route_sync_digest_emitted")
}

// SendRouteSyncDigest implements routing.PeerSender: it emits a
// route_sync_digest_v1 to peerAddress carrying the sender-side announce
// digest, as the announce loop's periodic freshness heartbeat (in place of an
// unconditional full sync). It is the by-address sibling of emitRouteSyncDigest
// (which sends by identity on reconnect); both gate on CapMeshRouteSyncV1 so a
// peer without route_sync support simply never receives a digest and the
// announce loop's "no confirmed match → full" fallback keeps it fresh.
//
// Returns true when the frame was enqueued. A false return (peer lacks
// route_sync, or the transport dropped it) leaves the armed pending window to
// elapse, so the next deadline escalates to a full — the documented safe
// degradation, identical to a silent peer.
func (s *Service) SendRouteSyncDigest(ctx context.Context, peerAddress domain.PeerAddress, digest string, entries []routing.DigestEntry, knownIdentities uint32, generatedAt time.Time) bool {
	wire, err := buildRouteSyncDigestWire(digest, entries, knownIdentities, generatedAt)
	if err != nil {
		log.Warn().
			Err(err).
			Str("peer_address", string(peerAddress)).
			Str("digest", digest).
			Msg("route_sync_digest_marshal_failed")
		return false
	}
	sent := s.dispatchAnnouncePlaneFrameWithCaps(ctx, peerAddress, wire, domain.CapMeshRouteSyncV1)
	if sent {
		s.digestStats.heartbeatsSent.Add(1)
	}
	return sent
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
	digest, entries, count := s.routingTable.AnnounceDigestVectorFor(peer)
	s.routingTable.RecordPeerDigestSnapshot(peer, digest, entries, count, time.Now().UTC())
	log.Debug().
		Str("peer_identity", peer.String()).
		Str("digest", digest).
		Uint32("count", count).
		Msg("route_sync_digest_recorded_for_reconnect")
}

// compareInboundDigest performs the shared receiver-side work for an inbound
// route_sync_digest_v1 from `peer`: compute our digest over the routes we hold
// via that peer (SyncDigestFor), compare it to theirDigest, and on a match
// renew those routes' TTL (the receiver half of digest-as-heartbeat) and bump
// the observability counters. It returns our local digest + count + the match
// verdict so the caller builds the route_sync_summary_v1 reply on whichever
// transport it owns — the inbound dispatcher answers by connID
// (handleRouteSyncDigest), while the outbound-session dispatcher replies via the
// session writer (peer_management.go). Centralising the compare here keeps the
// TTL refresh and the digests_compared / compare_match counters consistent
// across BOTH paths; the earlier inline outbound copy did neither.
func (s *Service) compareInboundDigest(peer domain.PeerIdentity, theirDigest string, theirEntries []protocol.RouteSyncDigestEntry) (localDigest string, localCount uint32, match bool) {
	s.digestStats.digestsCompared.Add(1)

	// Phase-0 version-vector path: when the sender carries the (Identity,
	// SeqNo) vector, reconcile per identity instead of an all-or-nothing
	// hash compare. ReconcileSyncVector refreshes the TTL of every route via
	// the peer whose SeqNo matches (proven-current) and returns match=true
	// only when we hold NO stale via-peer route the vector left unconfirmed
	// — so a route we never adopted under our K-cap (an offer absent from
	// our buckets) does not spoil the match, which is exactly what lets the
	// digest match in a dense capped mesh. localDigest/localCount carry the
	// refreshed count for the diagnostic log only (the summary echoes the
	// sender's hash for correlation, not this value).
	if want := routeSyncEntriesFromWire(theirEntries); len(want) > 0 {
		var refreshed int
		match, refreshed = s.routingTable.ReconcileSyncVector(peer, want, time.Now().UTC())
		if match {
			s.digestStats.compareMatch.Add(1)
		}
		if refreshed > 0 {
			log.Debug().
				Str("sender_identity", peer.String()).
				Int("refreshed", refreshed).
				Bool("match", match).
				Msg("route_sync_vector_reconciled_ttl")
		}
		return theirDigest, uint32(refreshed), match
	}

	// Legacy hash fallback for peers that predate the version vector.
	localDigest, localCount = s.routingTable.SyncDigestFor(peer)
	match = localDigest == theirDigest
	if !match {
		// A mismatch refreshes nothing — the sender follows with a full.
		return localDigest, localCount, false
	}
	s.digestStats.compareMatch.Add(1)
	// Digest-as-heartbeat freshness (receiver half): a matching digest proves
	// our routes learned through this peer are exactly what the peer still
	// announces, so renew their TTL here instead of waiting for the peer to
	// ship a full announce — what lets the sender replace the periodic
	// full-sync with a cheap digest on the stable path without letting
	// unchanged routes age out (docs/routing.md "Refresh interval invariant").
	if refreshed := s.routingTable.RefreshRoutesVia(peer, time.Now().UTC()); refreshed > 0 {
		log.Debug().
			Str("sender_identity", peer.String()).
			Int("refreshed", refreshed).
			Msg("route_sync_digest_match_refreshed_ttl")
	}
	return localDigest, localCount, true
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
	if senderIdentity.IsZero() {
		// Sender identity not yet resolved (race against auth
		// completion). The frame can't be answered meaningfully
		// because the digest only makes sense scoped to a
		// specific peer — drop silently.
		log.Debug().
			Str("digest", frame.Digest).
			Msg("route_sync_digest_dropped_no_sender_identity")
		return
	}

	localDigest, localCount, match := s.compareInboundDigest(senderIdentity, frame.Digest, frame.Entries)

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
			Str("sender_identity", senderIdentity.String()).
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
		Str("sender_identity", senderIdentity.String()).
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
// of the calling node (min(10*AnnounceInterval, DefaultTTL/2)), so a
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
	if senderIdentity.IsZero() {
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
			Str("sender_identity", senderIdentity.String()).
			Bool("match", frame.Match).
			Msg("route_sync_summary_dropped_empty_digest")
		return
	}
	if s.announceLoop == nil {
		// Defensive — production paths always wire announceLoop.
		// Tests that build a partial Service (no AnnounceLoop)
		// would otherwise dereference a nil interface here.
		log.Debug().
			Str("sender_identity", senderIdentity.String()).
			Msg("route_sync_summary_dropped_no_announce_loop")
		return
	}
	now := time.Now().UTC()
	if !frame.Match {
		// Mismatch — the digests diverged, so the receiver needs a fresh full
		// sync. ConsumeDigestMismatch atomically checks correlation AND removes
		// the entry under one lock, so a concurrent / replayed duplicate finds
		// it already gone and cannot double-count (single-shot, like
		// ConfirmPeerDigestMatch for the match path). The window is then cleared
		// unconditionally as the safe default for uncorrelated / stale
		// mismatches (a no-op when the correlated consume above already removed
		// it).
		if s.announceLoop.ConsumeDigestMismatch(senderIdentity, frame.Digest, now) {
			s.digestStats.summaryMismatch.Add(1)
		}
		s.announceLoop.ClearPeerDigestSuppression(senderIdentity)
		log.Debug().
			Str("sender_identity", senderIdentity.String()).
			Str("digest", frame.Digest).
			Msg("route_sync_summary_mismatch_cleared_suppression")
		return
	}
	// Match — ConfirmPeerDigestMatch is single-shot (it refuses an already
	// confirmed echo), so counting summary_match on its true return makes the
	// counter exactly one per correlated heartbeat even under duplicate /
	// replayed / concurrently-delivered summaries. A stale, unsolicited, or
	// spoofed match that echoes some other digest returns false here and is
	// neither counted nor allowed to suppress a full it has no relation to.
	if s.announceLoop.ConfirmPeerDigestMatch(senderIdentity, frame.Digest, now) {
		s.digestStats.summaryMatch.Add(1)
		log.Debug().
			Str("sender_identity", senderIdentity.String()).
			Str("digest", frame.Digest).
			Msg("route_sync_summary_match_suppression_armed")
		return
	}
	log.Debug().
		Str("sender_identity", senderIdentity.String()).
		Str("digest", frame.Digest).
		Msg("route_sync_summary_match_uncorrelated_ignored")
}
