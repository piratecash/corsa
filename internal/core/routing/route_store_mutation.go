package routing

import (
	"bytes"
	"time"

	"github.com/rs/zerolog/log"
)

// logFastInvalidation emits the observability log promised in
// RouteCapStats.FastInvalidations' doc-comment and in docs/routing.md
// "RIB compaction" → "fast invalidation": Identity, Uplink, observed
// hops, accepted-vs-rejected outcome. The log fires on EVERY P3
// decision (accepted invalidation AND rejected stale-SeqNo /
// cross-Origin-replay sample), but the FastInvalidations counter bumps
// ONLY on accepted invalidations — the counter measures successful
// tombstone admissions, not total bad-hops traffic.
//
// Level split: `accepted=true` is steady-state mesh convergence churn —
// during a route storm it fires many times per minute on every node,
// and per-event warn logging (JSON encode + runtime.Caller + log I/O)
// was itself a measurable CPU cost, so the accepted path logs at
// debug. `accepted=false` stays at warn: rejected samples (stale /
// cross-Origin replay) are the anomaly an operator must see.
// Dashboard guidance:
//
//   - FastInvalidations counter bumps are paired 1:1 with
//     `accepted=true` debug lines — at the default (warn) log level
//     the counter is the engage signal, not the log.
//   - `accepted=false` warn lines have NO counter equivalent — the
//     rejection_reason field is the only operator-visible record
//     of stale / cross-Origin samples reaching the P3 branch. If
//     dashboards need a counter for these, that's deferred work
//     (the current monotonic FastInvalidations was designed as
//     the "successful guard fired" metric, not "bad samples seen").
//
// `accepted=false` carries the `reason` ("cross_origin_stale_replay"
// or "stale_seqno") for diagnostic granularity; the accepted path
// keeps reason="" because the operator only needs the outcome to
// be visible.
//
// Lives in route_store_mutation.go (not flap.go) because the
// fast-invalidation decision is part of ApplyUpdate's storage
// pipeline; the symmetric SeqNo-flap-cap engage log lives in
// flap.go::recordSeqAdvanceLocked where the decision is made.
func logFastInvalidation(entry RouteEntry, accepted bool, reason string) {
	event := log.Debug()
	if !accepted {
		event = log.Warn()
	}
	event = event.
		Str("identity", entry.Identity.String()).
		Str("uplink", entry.NextHop.String()).
		Str("origin", entry.Origin.String()).
		Int("observed_hops", entry.Hops).
		Uint64("incoming_seqno", entry.SeqNo).
		Bool("accepted", accepted)
	if reason != "" {
		event = event.Str("rejection_reason", reason)
	}
	event.Msg("routing_fast_invalidation")
}

// This file hosts the write-side storage operations: claim
// admission (ApplyUpdate orchestrating AdmitNew + dedup),
// tombstone-based withdrawal (WithdrawTriple), direct-peer
// registration (AdmitDirectPeer), transit/direct invalidation
// through a disconnected uplink (InvalidateAllVia /
// InvalidateTransitVia), expired-claim compaction
// (CompactExpired), and the per-Identity SeqNo counter helpers
// (nextSeqLocked / syncSeqCounterLocked) used by every
// own-origin path.
//
// Phase 1 P0 Phase A changed the dedup key from
// (Identity, Origin, NextHop) to (Identity, Uplink): Origin is
// consumed by Table.UpdateRoute for anti-spoof and then dropped,
// and storage is per-(Identity, Uplink) UplinkClaim values. Wire
// frames on the boundary still use the legacy AnnounceEntry
// shape (the caller in Table.* / node.routing_announce.go parses
// Origin off the wire and re-emits Origin = localOrigin so that
// pre-A1 receivers' withdrawal anti-spoof keeps accepting our
// frames — see uplink_claim.go for the migration contract).

// ApplyUpdate ingests entry (already structurally validated by
// RouteEntry.Validate and anti-spoofed by the caller for
// RouteSourceDirect's foreign-origin check) into the store.
// Returns the public status outcome and whether storage was
// mutated in a way that should mark the owning Table dirty.
//
// Callers must NOT pass RouteSourceLocal entries; the
// local-source guard is upstream on Table.UpdateRoute. Caller
// must hold t.mu (writer).
//
// Dedup key is (Identity, Uplink=entry.NextHop). If an existing
// claim shares the same pair:
//
//   - If incoming SeqNo > existing SeqNo: replace unconditionally
//     (with a cap admission re-evaluation when the existing claim
//     was a tombstone, see the SeqNo>old block below).
//   - If incoming SeqNo == existing SeqNo: replace only if the
//     incoming source has a higher trust rank or fewer hops.
//   - If incoming SeqNo < existing SeqNo: reject (stale
//     announcement).
//
// Status mapping: returns RouteAccepted on insert/improve,
// RouteUnchanged on alive same-or-worse reconfirmation,
// RouteRejected on tombstone/SeqNo guards.
// ApplyUpdate returns (status, mutated, wireChanged). `mutated` is true whenever
// storage changed in a way the published Snapshot must re-copy — including an
// ExpiresAt-only TTL refresh, a RouteCapStats counter bump on a cap rejection, or
// a tombstone slice reshape on an admit-rejection. `wireChanged` is the narrower
// "did the announce WIRE projection (Origin/Hops/SeqNo/Extra/AttestedSig) change"
// signal that drives the cursor-mode change journal, so TTL refreshes and
// rejections do not fan out unchanged routes across the mesh:
//   - RouteAccepted → wireChanged=true (admission / SeqNo bump / hops improve /
//     lineage replace all change the emitted entry).
//   - RouteRejected → wireChanged=false ALWAYS: the incoming was not accepted, so
//     the stored winner/tombstone is preserved and the projection is unchanged,
//     even when buckets were reshaped or counters moved (mutated=true).
//   - RouteUnchanged on a same-SeqNo/same-hops reconfirmation → wireChanged is
//     true only if the AttestedSig/Extra bytes actually changed (a sig upgrade,
//     which IS wire content); a pure TTL refresh keeps it false.
func (s *routeStore) ApplyUpdate(entry RouteEntry, now time.Time) (RouteUpdateStatus, bool, bool) {
	// Direct routes are socket-bound, not TTL-managed (see
	// docs/routing.md "Direct route lifecycle"): ExpiresAt is
	// intentionally zero so that IsExpired never triggers and
	// TickTTL never evicts an entry whose underlying socket is
	// still alive. Any ApplyUpdate call that lands a
	// RouteSourceDirect entry — whether through state restore,
	// defensive re-application, or the explicit Direct-via-
	// ApplyUpdate test paths — must therefore land with
	// ExpiresAt=zero, regardless of what the caller supplied.
	// Validate() already enforces Hops=1 for RouteSourceDirect, so
	// this branch never sees a withdrawn-direct (Hops=HopsInfinity)
	// tombstone — withdrawal of direct routes goes through
	// InvalidateAllVia / WithdrawTriple, which write ExpiresAt
	// directly without going through ApplyUpdate.
	if entry.Source == RouteSourceDirect {
		entry.ExpiresAt = time.Time{}
	} else if entry.ExpiresAt.IsZero() {
		entry.ExpiresAt = now.Add(s.defaultTTL)
	}

	// Convert to the storage shape. Origin was already anti-spoofed
	// upstream by Table.UpdateRoute; toUplinkClaim drops it. The
	// Withdrawn flag is derived from Hops>=HopsInfinity (wire
	// sentinel translated to internal flag).
	incoming := toUplinkClaim(entry, now)

	bucket, idx := s.findByUplinkLocked(entry.Identity, entry.NextHop)

	// Phase 1 P2 — outbound-projected SeqNo ingest guard during
	// hold-down. MUST run BEFORE the P3 fast-invalidation branch:
	// the P3 branch accepts the bad-hops claim as a tombstone at
	// `incoming.SeqNo` (no bump), which advances stored state and
	// the SeqNo-resurrection guard for the (Identity, Uplink) pair
	// to the runaway value. Without this guard fronting it, a
	// runaway peer in SeqNo hold-down could still poison the
	// tombstone with a huge SeqNo and force any legitimate
	// recovery to advance past it.
	//
	// While Identity X is in SeqNo flap-cap hold-down, reject
	// incoming announces whose outbound-projected SeqNo would
	// advance past `observed+1`. Same-SeqNo reconfirmations are
	// intentionally NOT rejected (they refresh receive-side TTL
	// bookkeeping without advancing outbound), so the guard fires
	// only on entries that would genuinely move the outbound
	// timeline. Hop_ack / direct-source paths are exempt: a
	// hop_ack does not advance the outbound counter (lineage
	// override clears entry.Origin and the same-SeqNo trust uplift
	// never bumps SeqNo); a direct source is own-origin and lives
	// outside the upstream-driven advance flow.
	//
	// `incoming.Withdrawn` is INCLUDED in the guard (unlike the
	// earlier draft of this branch): the same argument the P3
	// branch documents applies here too — genuine wire withdrawals
	// route through `Table.WithdrawRoute`, not `UpdateRoute`, so any
	// `UpdateRoute` call with `incoming.Hops == HopsInfinity` got
	// there via the receive-path `wire.Hops + 1 > HopsInfinity`
	// clamp (wire.Hops was 15, a loop boundary). Exempting
	// withdrawn would let a runaway peer in hold-down send a
	// boundary bad-hops with a huge SeqNo, slip past P2, hit P3
	// fast-invalidation, and poison the SeqNo-resurrection
	// tombstone at the runaway value. The guard rejects the
	// runaway SeqNo regardless of which tier (live bad-hops or
	// clamped-bad-hops) the incoming claim lands on.
	if entry.Source == RouteSourceAnnouncement && s.flap.isInSeqHoldDownLocked(entry.Identity, now) {
		if entry.SeqNo > s.outboundMax[entry.Identity]+1 {
			return RouteRejected, false, false
		}
	}

	// Phase 1 P3 — fast invalidation on hops > MaxSaneHops.
	//
	// Detected synchronously BEFORE the cross-Origin admission rules
	// and the standard SeqNo branches because those branches operate
	// on the assumption that the incoming claim is at least
	// loop-free. A claim approaching HopsInfinity (16) is almost
	// always a count-to-infinity loop or a pre-convergence stage;
	// admitting it just steers send_message / relay traffic onto a
	// known-doomed uplink for up to 120s of TTL, and a naive fall-
	// through into the stale-SeqNo branch would silently DROP the
	// invalidation when the bad-hops claim arrives with
	// `incoming.SeqNo < old.SeqNo` from a different Origin lineage
	// (the cross-Origin lineage admission rule rejects "not strictly
	// better" replacements, and a tombstone is neither lower-hops
	// nor higher-trust).
	//
	// Action: stamp a tombstone at the observed bad SeqNo (NO bump
	// — recovery requires the upstream's next legit advance to
	// strictly exceed the observed bad SeqNo) into the
	// (Identity, Uplink) slot:
	//
	//   - idx<0 → append the tombstone unconditionally (mirrors the
	//     WithdrawTriple idx<0 path; tombstones live OUTSIDE the
	//     K-counted live slots per AdmitNew rule 1, so the bad
	//     claim never displaces a legitimate row).
	//   - idx>=0 → replace bucket[idx] only when entry.SeqNo strictly
	//     exceeds the stored SeqNo. A stale bad-hops replay
	//     (entry.SeqNo <= old.SeqNo) is rejected — the stored claim
	//     was already validated at a higher SeqNo, and the
	//     upstream's recovery path (a fresh legit advance) is the
	//     only legitimate way to undo a live → tombstone transition.
	//
	// Lineage tracking: LastIngressOrigin / SeenOriginSeqs from the
	// stored claim are preserved across the invalidation. We are
	// not the origin of this route and have no authority to forget
	// the upstream's earlier observations on its behalf; cross-
	// Origin lineage tracking continues to operate on subsequent
	// ingest paths.
	//
	// Wire emit suppression is automatic — AnnounceProjectionFor's
	// own-direct tombstone passthrough rule only emits when
	// Source == RouteSourceDirect, and a fast-invalidated transit
	// claim keeps its Source != Direct. We have no authority to
	// broadcast a count-to-infinity withdrawal frame on the
	// upstream's behalf; local invalidation is purely a routing
	// decision.
	//
	// Disabled when maxSaneHops <= 0 (the bare-Table default for
	// fixtures that build a Table without WithMaxSaneHops).
	//
	// Why the `!incoming.Withdrawn` exempt was wrong: the receive
	// path in node/routing_announce.go routes GENUINE upstream
	// withdrawals through Table.WithdrawRoute (NOT UpdateRoute), so
	// any UpdateRoute call whose `incoming.Hops == HopsInfinity`
	// got there by the receive-side `wire.Hops + 1 > HopsInfinity`
	// clamp — i.e. wire.Hops was 15 (a clear loop-detection
	// boundary), not the explicit-withdrawal sentinel. Exempting
	// withdrawn would silently route the loop-boundary case past
	// the FastInvalidations counter and `routing_fast_invalidation`
	// log, contradicting the phase-doc requirement to count and
	// log every `Hops > MaxSaneHops` event. Triggering P3 on
	// either tier (live OR landed-at-HopsInfinity-by-clamp) keeps
	// the operator-facing observability complete; the downstream
	// action (overwrite incoming as withdrawn tombstone at the
	// observed SeqNo) is identical either way, so the change is
	// pure observability and lineage-tracking — no new storage
	// state shape.
	if s.maxSaneHops > 0 && int(incoming.Hops) > s.maxSaneHops {
		// Bad-hops hysteresis gate (see types.go constant block): an
		// Identity whose accepted fast-invalidations exceeded the
		// per-window budget is in hold-down — drop the claim BEFORE
		// any storage mutation. No tombstone rewrite, no dirty mark
		// (snapshot publisher stays asleep), no per-event warn log:
		// the engage/release logs plus the BadHopsHoldowns counter
		// are the operator-facing signal. Claims with sane hops never
		// reach this branch, so upstream recovery (loop converges,
		// short route re-announced) is admitted normally even while
		// hold-down is active.
		if s.flap.isInBadHopsHoldDownLocked(entry.Identity, now) {
			return RouteRejected, false, false
		}

		incoming.Withdrawn = true
		incoming.Hops = HopsInfinity
		incoming.ExpiresAt = now.Add(s.defaultTTL)

		if idx < 0 {
			incoming.SeenOriginSeqs = mergeOriginObservation(nil, entry.Origin, entry.SeqNo)
			s.buckets[entry.Identity] = append(bucket, incoming)
			s.capStats.fastInvalidations.Add(1)
			logFastInvalidation(entry, true, "")
			s.flap.recordBadHopsInvalidationLocked(entry.Identity, entry.Origin, now, &s.capStats.badHopsHoldowns)
			return RouteAccepted, true, true
		}

		old := &bucket[idx]
		// Cross-Origin stale-replay guard MUST run inside the P3
		// branch as well: without it the scenario A10 → B5 → A9
		// (with A9.Hops > MaxSaneHops) would slip past the guard
		// in the standard branches (P3 returns early on line ~140
		// and never reaches them). The bad-hops claim would land
		// on top of the legitimate B lineage purely because
		// `entry.SeqNo > old.SeqNo` (9 > 5), even though A's
		// previous high-water was 10 and 9 is a stale replay on
		// A's own observed timeline. The mixed-version stale-
		// replay invariant requires the guard to apply EVERYWHERE
		// we admit a state mutation against `bucket[idx]`, not
		// just on the standard SeqNo branches. See § 3.1.5.c.
		if !entry.Origin.IsZero() && !old.LastIngressOrigin.IsZero() && entry.Origin != old.LastIngressOrigin {
			if prevHigh, ok := old.SeenOriginSeqs[entry.Origin]; ok && entry.SeqNo <= prevHigh {
				logFastInvalidation(entry, false, "cross_origin_stale_replay")
				return RouteRejected, false, false
			}
		}
		if entry.SeqNo > old.SeqNo {
			// Preserve the active-lineage marker. The incoming
			// LastIngressOrigin is also kept on the tombstone — both
			// it and the merged high-water map are downstream cross-
			// Origin lineage inputs that survive the invalidation.
			incoming.LastIngressOrigin = entry.Origin
			incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			bucket[idx] = incoming
			s.capStats.fastInvalidations.Add(1)
			logFastInvalidation(entry, true, "")
			s.flap.recordBadHopsInvalidationLocked(entry.Identity, entry.Origin, now, &s.capStats.badHopsHoldowns)
			return RouteAccepted, true, true
		}
		// Stale bad-hops replay: the stored claim is already at a
		// higher SeqNo and presumably loop-free. Drop without
		// bumping the counter — the counter measures successful
		// invalidations, not stale samples.
		logFastInvalidation(entry, false, "stale_seqno")
		return RouteRejected, false, false
	}

	// HopAck promotion lineage override. confirmRouteViaHopAck
	// (internal/core/node/routing_hop_ack.go) constructs the
	// RouteEntry argument by reading the existing route via
	// Table.Lookup, whose toRouteEntry contract synthesises
	// Origin = localOrigin (storage dropped per-Origin keying — see
	// uplink_claim.go). The resulting entry.Origin = localOrigin is
	// NOT a fresh wire-frame observation; it is a sentinel produced
	// by the synthesiser to satisfy the boundary RouteEntry shape.
	//
	// Without this override, the standard lineage-tracking paths
	// interpret the synthetic localOrigin as a wire-frame
	// observation:
	//
	//   - the same-SeqNo observation block advances LastIngressOrigin
	//     onto localOrigin and merges SeenOriginSeqs[localOrigin]=
	//     SeqNo;
	//   - the same-SeqNo trust-uplift replace wholesale-replaces
	//     bucket[idx] with `incoming`, locking the synthetic
	//     lineage into storage.
	//
	// After that corruption, the next genuine forced-full-sync
	// re-announce from the actual upstream Origin (e.g. peer B
	// re-emits at the same SeqNo for the per-peer-delta-empty TTL
	// refresh path) is classified cross-Origin (real Origin !=
	// stored localOrigin) AND its Origin's recorded high-water is
	// at this SeqNo from the initial announce — so the per-Origin
	// stale-replay guard rejects it before reaching the
	// reconfirmation refresh path. Confirmed routes age out by TTL
	// despite being actively re-announced.
	//
	// Override the lineage-tracking inputs so all downstream
	// branches treat the HopAck entry as a "no wire-frame
	// observation" trust uplift:
	//
	//   - preserve `incoming.LastIngressOrigin` / `incoming.SeenOrigin
	//     Seqs` from the stored claim so every `bucket[idx] =
	//     incoming` replace site lands with the real wire-observed
	//     lineage intact;
	//   - clear entry.Origin to "" so the pre-check guard, the
	//     cross-Origin live-vs-tombstone branch, the observation
	//     block's mergeOriginObservation, and the cross-Origin
	//     lineage branch in the stale-SeqNo path all skip cleanly
	//     (each gates on entry.Origin != "" or relies on the
	//     LastIngressOrigin comparison).
	//
	// confirmRouteViaHopAck always calls UpdateRoute after a
	// successful Lookup, so idx >= 0 holds in practice. The
	// defensive idx < 0 branch zeroes the synthetic localOrigin so
	// any future caller that constructs a HopAck entry against an
	// empty bucket cannot poison a fresh admission's lineage with
	// the synthesiser sentinel.
	if entry.Source == RouteSourceHopAck {
		if idx >= 0 {
			incoming.LastIngressOrigin = bucket[idx].LastIngressOrigin
			incoming.SeenOriginSeqs = bucket[idx].SeenOriginSeqs
		} else {
			incoming.LastIngressOrigin = PeerIdentity{}
			incoming.SeenOriginSeqs = nil
		}
		entry.Origin = PeerIdentity{}
	}

	if idx < 0 {
		// Tombstone short-circuit: a withdrawn claim is a SeqNo
		// resurrection-guard marker, not a routing slot, and the
		// cap model puts it OUTSIDE the K-counted live entries
		// (see AdmitNew rule 1 and WithdrawTriple idx<0). Routing
		// it through AdmitNew would treat the tombstone as a
		// "dead candidate" that loses to any live row on the
		// liveness pre-tier and gets rejected on a saturated live
		// bucket — silently dropping the SeqNo guard for THIS
		// (Identity, Uplink) pair even though the cap model
		// explicitly leaves room for it. The public Table API
		// allows UpdateRoute(Hops=HopsInfinity) for callers that
		// synthesise tombstones directly (it is also covered by
		// table_test.go's RouteEntry.Validate suite), so we
		// mirror WithdrawTriple idx<0 here: append the tombstone
		// unconditionally and let TickTTL reclaim it on its own
		// schedule.
		if incoming.IsWithdrawn() {
			// Record the wire-frame Origin's first observation so
			// later cross-Origin updates on the same (Identity,
			// Uplink) claim can consult the high-water map. See
			// UplinkClaim.SeenOriginSeqs doc-comment.
			incoming.SeenOriginSeqs = mergeOriginObservation(nil, entry.Origin, entry.SeqNo)
			s.buckets[entry.Identity] = append(bucket, incoming)
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		}

		// Live admission. AdmitNew owns the actual append /
		// in-place replace; we only translate its decision into
		// the public RouteUpdateStatus and update side state on
		// success. Cap rejections (AdmissionRejectedFull,
		// AdmissionRejectedAllProtected) leave the store
		// untouched and surface as RouteRejected — same shape as
		// the existing stale-SeqNo / tombstone-protection
		// rejections, so callers that already treat RouteRejected
		// as "drop and move on" need no new handling. Operators
		// distinguishing the two cases observe the cap-admission
		// metrics.
		//
		// SeenOriginSeqs is populated here even though AdmitNew
		// may reject: the map allocation is owned by the local
		// `incoming` variable, so rejection just garbage-collects
		// it. We avoid the alternative — looking up the new slot
		// post-AdmitNew to populate seen — because AdmitNew's
		// eviction path can place the claim at an arbitrary
		// index and the lookup would require duplicating its
		// internal selection logic.
		incoming.SeenOriginSeqs = mergeOriginObservation(nil, entry.Origin, entry.SeqNo)
		decision := s.AdmitNew(entry.Identity, incoming, now)
		switch decision {
		case AdmissionAccepted, AdmissionAcceptedReplaced:
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		case AdmissionRejectedFull, AdmissionRejectedAllProtected:
			// Counters in AdmitNew were incremented even though
			// the buckets map is unchanged. RouteCapStats rides
			// inside the published Snapshot via
			// rebuildRoutingSnapshot's dirty gate, so without the
			// flag the cached snapshot would keep stale counter
			// values until some unrelated mutation arrived — and
			// fetchRouteSummary would underreport rejected_full /
			// rejected_all_protected on a node that is doing
			// nothing BUT rejecting cap admissions. Signal
			// "dirty" via mutated=true so the publisher
			// republishes with current counters.
			//
			// This is the only path in ApplyUpdate where mutated
			// is true without an actual routes-map mutation;
			// idempotent stale-SeqNo / tombstone-protection /
			// RouteUnchanged branches deliberately return
			// mutated=false because they touch nothing observable
			// in the snapshot.
			return RouteRejected, true, false
		default:
			// Defence in depth: an unrecognised decision from a
			// future AdmitNew extension still surfaces as
			// RouteRejected so the caller does not see a bogus
			// RouteAccepted, and mutated stays false so we do
			// not mask the missing case under a phantom rebuild.
			return RouteRejected, false, false
		}
	}

	old := &bucket[idx]

	// Phase 4 13.6 anti-poisoning rule analysis: the original spec
	// ("announcement advisory-only, cannot override fresher
	// direct/hop_ack") was drafted before the Phase 1 per-(Identity,
	// Uplink) storage refactor. After the refactor, the per-uplink
	// bucket structure already enforces the relevant guarantee:
	// cross-uplink poisoning is impossible (a third-party peer's
	// claim about Identity X lands in a SEPARATE bucket slot from
	// our direct or hop_ack-confirmed claim through a different
	// uplink), and same-uplink "downgrade" from HopAck/Direct →
	// Announcement is the legitimate forced-full-sync TTL-refresh
	// path (see weaker_source_refreshes_ttl_marks_dirty in
	// table_dirty_test.go and TestUpdateRoute_HopAckPromotionPreservesWireLineage
	// in table_test.go). No additional admission gate is required
	// here — the (Identity, Uplink) keying + the
	// HopAck-promotion-sticky reconfirmation already provide the
	// poisoning resistance the phase doc targeted. See the Phase 4
	// doc entry for §3.6 for the full finding.

	// Cross-Origin stale-replay guard. Independent of the SeqNo
	// comparison against the active lineage below, because pre-A1
	// senders use per-Origin SeqNo counters and the active lineage
	// may have shifted to a different Origin since we last saw
	// entry.Origin. Without this guard, the scenario A10 → B5 → A9
	// (genuine winner shift A→B accepted at lower SeqNo via the
	// cross-Origin lineage branch, followed by a delayed in-flight
	// A9 from BEFORE the shift) would land in the SeqNo > old.SeqNo
	// branch (9 > 5) and admit stale A state on top of the
	// legitimate B lineage. The per-Origin high-water map records
	// A's previous high-water of 10, so the guard rejects A9 as a
	// stale replay on A's own observed timeline.
	//
	// The guard fires only on cross-Origin updates: same-Origin
	// stale replays are already handled by the standard
	// incoming.SeqNo < old.SeqNo branch below (which rejects them
	// outright), and same-Origin reconfirmations are handled by
	// the incoming.SeqNo == old.SeqNo branch.
	//
	// Gating: both Origins must be non-empty (defence-in-depth
	// against synthetic test fixtures and malformed wire frames).
	// Comparison is `entry.SeqNo <= prevHigh`: an exact match at
	// the recorded high-water IS a stale observation in the
	// cross-Origin context — the active lineage has shifted to a
	// different Origin since this SeqNo was last seen, so a fresh
	// shift back to the old Origin requires a strictly greater
	// SeqNo. Same-Origin reconfirmations at the same SeqNo are
	// unaffected because the cross-Origin gating above already
	// requires entry.Origin != old.LastIngressOrigin.
	if !entry.Origin.IsZero() && !old.LastIngressOrigin.IsZero() && entry.Origin != old.LastIngressOrigin {
		if prevHigh, ok := old.SeenOriginSeqs[entry.Origin]; ok && entry.SeqNo <= prevHigh {
			return RouteRejected, false, false
		}
	}

	if incoming.SeqNo > old.SeqNo {
		// Tombstone → live promotion needs the cap admission
		// policy applied. Withdrawn claims are excluded from
		// bucketCount (see AdmitNew rule 1) so the slot at idx —
		// currently a tombstone — does not count against the
		// K-live cap. A bare in-place overwrite would therefore
		// push the bucket from up-to-K live to up-to-K+1 live,
		// breaking the "at most K live claims per Identity"
		// invariant.
		//
		// Route the promotion through the same admission policy
		// used for fresh idx<0 admissions. Temporarily detach the
		// tombstone slot via a slice delete, call AdmitNew, and
		// either let it append/evict for the new live claim or
		// restore the tombstone on rejection so the SeqNo
		// resurrection guard for THIS specific (Identity, Uplink)
		// pair stays in place — dropping the tombstone here would
		// expose the lineage to further stale-SeqNo announcements.
		if old.IsWithdrawn() && !incoming.IsWithdrawn() && s.maxNextHopsPerOrigin > 0 {
			savedTombstone := bucket[idx]
			s.buckets[entry.Identity] = append(bucket[:idx], bucket[idx+1:]...)
			// Use the copy variant so the saved tombstone's
			// SeenOriginSeqs map is NOT mutated in place. The
			// in-place variant would aliasing-mutate the savedTombstone's
			// map; on AdmitNew rejection the restored tombstone
			// would then remember the rejected Origin/SeqNo
			// observation, and a subsequent legit retry at the
			// same SeqNo would trip the per-Origin high-water
			// guard at the top of ApplyUpdate and reject
			// incorrectly. mergeOriginObservationCopy returns an
			// independent map for the promoted claim while
			// leaving the saved tombstone's history intact for
			// the restore-on-reject path.
			incoming.SeenOriginSeqs = mergeOriginObservationCopy(savedTombstone.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			decision := s.AdmitNew(entry.Identity, incoming, now)
			switch decision {
			case AdmissionAccepted, AdmissionAcceptedReplaced:
				s.syncSeqCounterLocked(entry)
				return RouteAccepted, true, true
			default:
				s.buckets[entry.Identity] = append(s.buckets[entry.Identity], savedTombstone)
				// AdmitNew already bumped the rejection counter;
				// signal mutated so the snapshot republishes
				// with the updated counter and the rebuilt slice
				// (the tombstone may now sit at a different
				// index).
				return RouteRejected, true, false
			}
		}

		incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
		bucket[idx] = incoming
		s.syncSeqCounterLocked(entry)
		return RouteAccepted, true, true
	}

	if incoming.SeqNo == old.SeqNo {
		// Cross-Origin live-vs-tombstone promotion at equal SeqNo.
		// Pre-A1 senders can shift their per-Identity winner from
		// a now-withdrawn Origin (A) to a different LIVE Origin (B)
		// while B's own per-Origin SeqNo counter happens to be at
		// the same numeric value A reached at its withdrawal. The
		// other two SeqNo branches handle the cross-Origin live-vs-
		// tombstone shape symmetrically:
		//
		//   - stale-SeqNo branch admits cross-Origin live against a
		//     tombstone unconditionally (old.IsWithdrawn → improves);
		//   - newer-SeqNo branch promotes the tombstone through
		//     AdmitNew detach + readmit.
		//
		// Equal-SeqNo would, without this branch, fall into the
		// same-SeqNo tombstone-resurrection guard below and reject —
		// even though that guard exists for SAME-Origin lineages
		// (delayed hop_ack at the withdrawal's SeqNo must not undo
		// it), not for cross-Origin shifts. The cross-Origin stale-
		// replay guard ABOVE this branch already rejects equal-SeqNo
		// arrivals where the incoming Origin's recorded high-water
		// equals or exceeds entry.SeqNo (the `<=` comparison), so
		// reaching here means entry.Origin is fresh relative to its
		// own per-Origin timeline — admit it as the new active
		// lineage.
		//
		// We mirror the SeqNo > old tombstone-promotion path's cap-
		// aware shape exactly: detach the tombstone slot before
		// AdmitNew so the K-live invariant is evaluated against the
		// post-promotion bucket, and re-append the saved tombstone
		// on cap rejection so this (Identity, Uplink) keeps its
		// SeqNo-resurrection guard for any future same-Origin replay
		// against the original lineage.
		if old.IsWithdrawn() && !incoming.IsWithdrawn() &&
			!entry.Origin.IsZero() && !old.LastIngressOrigin.IsZero() &&
			entry.Origin != old.LastIngressOrigin {
			if s.maxNextHopsPerOrigin > 0 {
				savedTombstone := bucket[idx]
				s.buckets[entry.Identity] = append(bucket[:idx], bucket[idx+1:]...)
				// Copy variant — see the newer-SeqNo tombstone-
				// promotion branch above for the rationale.
				// Without the copy, the saved tombstone's
				// SeenOriginSeqs map is mutated in place by the
				// merge, and a subsequent legit retry after an
				// AdmitNew reject would be misclassified as a
				// stale replay by the per-Origin high-water guard.
				incoming.SeenOriginSeqs = mergeOriginObservationCopy(savedTombstone.SeenOriginSeqs, entry.Origin, entry.SeqNo)
				decision := s.AdmitNew(entry.Identity, incoming, now)
				switch decision {
				case AdmissionAccepted, AdmissionAcceptedReplaced:
					s.syncSeqCounterLocked(entry)
					return RouteAccepted, true, true
				default:
					s.buckets[entry.Identity] = append(s.buckets[entry.Identity], savedTombstone)
					return RouteRejected, true, false
				}
			}
			incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			bucket[idx] = incoming
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		}

		// Observation tracking: record entry.Origin's high-water on
		// this (Identity, Uplink) claim, and — on cross-Origin
		// arrivals — advance the active-lineage marker. These two
		// pieces of bookkeeping run on EVERY same-SeqNo branch
		// (reject, replace, reconfirmation, no-op), because what
		// we observed is independent of whether the observation
		// triggers a storage state change.
		//
		// SeenOriginSeqs records the high-water so a later stale
		// replay from this same Origin (after a hypothetical
		// winner shift to a different active lineage) is rejected
		// by the cross-Origin stale-replay guard above. Without
		// the same-SeqNo update, the first cross-Origin observation
		// would silently set up a window where a delayed in-flight
		// frame from the new Origin at a lower SeqNo could be
		// misclassified as a fresh lineage.
		//
		// LastIngressOrigin advance keeps the "currently active
		// lineage" semantic accurate for cross-Origin detection in
		// the stale-SeqNo branch below.
		//
		// Neither bookkeeping write flips mutated=true: both fields
		// are internal to UplinkClaim and never surface in the
		// published snapshot (toRouteEntry drops them), so consumers
		// see no observable change and snapshot rebuild is
		// unnecessary. The replace branches below overwrite
		// bucket[idx] wholesale; they preserve+merge SeenOriginSeqs
		// explicitly below so the early-write here is overridden
		// rather than lost.
		bucket[idx].SeenOriginSeqs = mergeOriginObservation(bucket[idx].SeenOriginSeqs, entry.Origin, entry.SeqNo)
		if !incoming.LastIngressOrigin.IsZero() && incoming.LastIngressOrigin != old.LastIngressOrigin {
			bucket[idx].LastIngressOrigin = incoming.LastIngressOrigin
		}

		// Same-Origin tombstone-resurrection guard. A withdrawn
		// claim must not be replaced by a same-SeqNo update from
		// the SAME lineage, even if the update has higher trust or
		// fewer hops. Only a strictly newer SeqNo from the origin
		// (or a cross-Origin shift — handled by the explicit
		// branch above) may supersede a withdrawal. Without this
		// guard, a delayed hop_ack (higher trust rank) at the
		// withdrawal's SeqNo could resurrect the just-withdrawn
		// lineage. The cross-Origin live-vs-tombstone case is NOT
		// caught here: it was promoted unconditionally above
		// (subject only to the per-Origin high-water guard at the
		// top of ApplyUpdate).
		if old.IsWithdrawn() {
			return RouteRejected, false, false
		}
		if incoming.Source.TrustRank() > old.Source.TrustRank() {
			incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			bucket[idx] = incoming
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		}
		if incoming.Source == old.Source && incoming.Hops < old.Hops {
			incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			bucket[idx] = incoming
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		}
		// Same SeqNo, same or worse trust/hops. The existing
		// claim is alive and unchanged — this is a
		// reconfirmation, not a rejection. Refresh ExpiresAt /
		// UpdatedAt when the incoming routing decision matches
		// the stored one: a same-SeqNo + same-Hops re-announcement
		// on the wire is the origin's signal that the route is
		// still valid, which is what AnnounceLoop's forced full
		// sync delivers between delta cycles. Without this the
		// learned copy on a neighbor ages out at its original
		// deadline even though the origin keeps confirming it,
		// leaving a dead window from when ExpiresAt elapses
		// until the next sync that actually changes a field. See
		// docs/routing.md "Refresh interval invariant" for the
		// full contract; the cadence half is enforced by
		// ForcedFullSyncMultiplier*DefaultAnnounceInterval <=
		// DefaultTTL/2.
		//
		// The refresh gate is hops-equality alone. The previous
		// implementation also required incoming.Source ==
		// old.Source, but that conflicts with the hop_ack
		// promotion contract: after confirmRouteViaHopAck shifts
		// the stored Source up to RouteSourceHopAck, the
		// subsequent forced-full-sync re-announce on the wire
		// always arrives with Source=RouteSourceAnnouncement
		// (hop_ack is a separate frame; ANNOUNCE never carries
		// Source=HopAck). With the Source-equality gate the
		// reconfirmation would never fire on confirmed routes
		// and they would age out by TTL despite being actively
		// re-announced. Hops-equality alone preserves the
		// original hops-divergence protection (a weaker incoming
		// at different hops freezes an optimistic earlier
		// hops=N claim and we want it to age out so TTL-driven
		// reconvergence picks up the current hops=N+1 path),
		// while still refreshing on legitimate same-routing-
		// decision re-announces irrespective of the stored
		// Source's promotion history.
		//
		// Direct routes are exempt: they use ExpiresAt=zero by
		// design (lifecycle is tied to AddDirectPeer/
		// RemoveDirectPeer), and IsExpired returns false for a
		// zero ExpiresAt — so an alive direct claim would reach
		// this branch alongside learned claims. Overwriting zero
		// with now+defaultTTL here would convert a never-expiring
		// direct claim into a 120s-ageing one and let TickTTL
		// evict it while the underlying socket is still connected.
		if !old.IsExpired(now) {
			sameHopsReconfirmation := incoming.Hops == old.Hops
			if sameHopsReconfirmation && old.Source != RouteSourceDirect {
				// Snapshot the wire-content bytes BEFORE the upgrade so we can tell
				// a pure TTL refresh (wire-identical) from a sig/Extra upgrade
				// (wire-changed) for the change journal below. AttestedSigVerified
				// is NOT wire content (local-only), so a verify-only flip with the
				// same bytes stays wireChanged=false.
				prevSig := bucket[idx].AttestedSig
				prevExtra := bucket[idx].Extra
				bucket[idx].ExpiresAt = now.Add(s.defaultTTL)
				bucket[idx].UpdatedAt = now
				// Round-11: upgrade attested-links signature
				// metadata in the same-SeqNo reconfirmation path.
				// Without this branch a legacy/unsigned claim
				// stays unverified forever even after the same
				// peer re-announces with a verified signed v3
				// entry at the same seq/hops — the
				// scoreSignedBonus trust upgrade in score.go
				// reads AttestedSigVerified, so the rolling
				// enable of mesh_attested_links_v1 / Phase 5
				// anchor publication would never converge. Rules:
				//
				//   - Verified incoming UPGRADES the stored
				//     state to verified=true and stores the
				//     fresh sig bytes (most recent canonical
				//     attestation). The Extra bytes the sig is
				//     COMPUTED OVER are stored in the same
				//     update — see Round-12 note below.
				//   - Unverified incoming MUST NOT downgrade an
				//     already-verified stored claim — the verify
				//     was a local observation we still trust;
				//     the peer may simply have stopped including
				//     the sig (e.g. session lost the cap, or
				//     forwarder stripped it). Keep the existing
				//     bytes and the verified bit (and the Extra
				//     they signed over).
				//   - Both unverified, incoming has bytes,
				//     stored does not: forward the bytes through
				//     storage so a downstream peer that DOES
				//     have the cap can verify on its side, even
				//     though we never did locally. Extra is
				//     copied alongside the bytes for the same
				//     Round-12 pair-consistency reason.
				//
				// Round-12: Extra MUST be copied together with
				// the sig in every upgrade branch. The
				// attested-links signature covers
				// `lenpfx(identity) || lenpfx(extra)` (see
				// frame_route_announce_v3.go
				// CanonicalSigningBytes — identity and extra
				// only, no per-emitter wire fields). Updating
				// AttestedSig / AttestedSigVerified without
				// copying the matching Extra would decouple the
				// pair: storage would carry the OLD Extra
				// marked as verified by a sig computed over
				// the NEW Extra, giving CompositeScore a
				// bogus signed-route bonus AND making later
				// re-emits of (old Extra, new sig) fail
				// verification at downstream peers. Treating
				// sig + extra as a transactional pair keeps
				// storage internally consistent even if the
				// origin (mis)behaves by emitting same-SeqNo
				// frames with differing Extra payloads.
				if incoming.AttestedSigVerified {
					bucket[idx].AttestedSig = incoming.AttestedSig
					bucket[idx].AttestedSigVerified = true
					bucket[idx].Extra = incoming.Extra
				} else if !bucket[idx].AttestedSigVerified && len(bucket[idx].AttestedSig) == 0 && len(incoming.AttestedSig) > 0 {
					// Round-15: unverified forward-through is
					// FIRST-OBSERVATION ONLY. Once we have stored any
					// sig bytes for this slot (unverified or verified),
					// subsequent unverified incoming with different
					// bytes is IGNORED. Rationale: unverified bytes
					// are not authenticated locally (no
					// mesh_attested_links_v1 negotiation, OR the
					// destination's pubkey isn't in our knowledge
					// store), so trusting a peer to rotate them at
					// will would let any neighbour churn our SeqNo
					// space — Round-14 made AttestedSig part of the
					// ComputeDelta + outboundEmitSig content keys, so
					// rotating fake bytes at same-seq/same-hops would
					// force fresh wire SeqNos and emit cycles with no
					// verifiable benefit. First observation still
					// stores so a downstream peer that DOES have the
					// cap can verify; the churn surface is closed by
					// refusing rewrites without a Verified upgrade.
					bucket[idx].AttestedSig = incoming.AttestedSig
					bucket[idx].Extra = incoming.Extra
				}
				// ExpiresAt is part of the published snapshot
				// (drives IsExpired and the wire ttl_seconds
				// field), so a TTL refresh must trigger a
				// republish — otherwise the snapshot would
				// prematurely report the route as expired up to
				// one full announce cycle ahead of the actual
				// lifetime. The Round-11 sig-metadata upgrade
				// also feeds CompositeScore directly, so a
				// republish on upgrade is also required for the
				// new bonus to surface to Lookup callers
				// reading from the snapshot.
				//
				// wireChanged distinguishes a sig/Extra upgrade (the announce
				// entry's AttestedSig/Extra bytes changed → journal it so the
				// cursor delta carries it) from a pure TTL refresh (wire-identical
				// → snapshot-only, no journal, no mesh fan-out).
				wireChanged := !bytes.Equal(prevSig, bucket[idx].AttestedSig) ||
					!normalizedExtraEqual(prevExtra, bucket[idx].Extra)
				return RouteUnchanged, true, wireChanged
			}
			return RouteUnchanged, false, false
		}

		// Expired claim with same SeqNo: the route died because
		// TickTTL has not yet cleaned it up (runs every 10s). A
		// same-SeqNo re-announcement from the origin is a valid
		// refresh — the origin has not changed the route, it is
		// simply re-confirming it. Without this, an
		// expired-but-not-yet-cleaned claim blocks same-SeqNo
		// refreshes until TickTTL removes the stale entry,
		// creating a non-deterministic window where routes are
		// lost despite the origin still advertising them.
		incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
		bucket[idx] = incoming
		s.syncSeqCounterLocked(entry)
		return RouteAccepted, true, true
	}

	// Stale SeqNo (incoming.SeqNo < old.SeqNo).
	//
	// Mixed-version edge case: pre-A1 senders may legitimately
	// emit a numerically lower SeqNo across cycles when their
	// per-Identity winner switches between Origin lineages. Pre-A1
	// SeqNos were per-Origin (independent counters per lineage),
	// and pre-A1 BuildAnnounceSnapshot's Stage 4 picks one winner
	// per cycle — so the wire frame for cycle N can carry
	// `(Identity=X, Origin=A, SeqNo=10, Hops=3)` and cycle N+1 can
	// carry `(Identity=X, Origin=B, SeqNo=5, Hops=2)` if the
	// underlying winner shifted. Post-A1 storage compares SeqNo
	// per-(Identity, Uplink) and would naively reject the second
	// frame as stale, even though it carries a strictly better
	// route from the same uplink under a different lineage.
	//
	// LastIngressOrigin lets us detect the lineage shift: when the
	// incoming wire-frame Origin differs from the Origin we last
	// saw on this (Identity, Uplink), this is a cross-Origin
	// lineage update, NOT a stale replay. Accept it as a fresh
	// admission when it is strictly better than the stored claim
	// (or unconditionally when the stored claim is a tombstone — a
	// live route from a different lineage is always preferable to
	// keeping the lineage that withdrew). This resets the per-
	// uplink SeqNo namespace to the new lineage; subsequent
	// same-Origin updates on this uplink are SeqNo-ordered against
	// the new claim normally.
	//
	// Post-A1 senders always emit Origin = localOrigin (constant
	// across cycles) so this branch never fires for A1-to-A1
	// traffic. It only activates during the mixed-version rollout
	// window when pre-A1 transits are still in the mesh.
	//
	// Gating: both Origins must be non-empty. Empty incoming.Origin
	// is a malformed wire frame (RouteEntry.Validate rejects empty
	// Origin upstream, so this is defence-in-depth). Empty
	// old.LastIngressOrigin only happens on synthetic test fixtures
	// that construct UplinkClaim without going through
	// toUplinkClaim / AdmitDirectPeer; we treat it as "no prior
	// lineage observed" and stick with the standard reject.
	if !entry.Origin.IsZero() && !old.LastIngressOrigin.IsZero() && entry.Origin != old.LastIngressOrigin {
		// Strictness rule: cross-Origin replace requires either
		// strictly better hops, higher trust rank, or a withdrawn
		// stored claim. Same-hops / same-trust cross-Origin
		// arrivals are rejected to prevent flapping when a pre-A1
		// sender oscillates between equivalent lineages across
		// cycles — keeping the existing claim is safer than
		// continually resetting the SeqNo namespace and burning
		// outbound wire SeqNo space.
		improves := old.IsWithdrawn() ||
			incoming.Hops < old.Hops ||
			incoming.Source.TrustRank() > old.Source.TrustRank()
		if improves {
			// Tombstone → live promotion needs the cap admission
			// policy applied here for the same reason as the
			// newer-SeqNo and equal-SeqNo branches above:
			// withdrawn claims live OUTSIDE the K-counted live
			// slots (AdmitNew rule 1), so a bare
			// `bucket[idx] = incoming` would silently flip a
			// non-counted slot into a counted live slot and the
			// bucket would balloon from up-to-K live to up-to-K+1
			// live. Route the cross-Origin stale-SeqNo
			// resurrection through the same detach + AdmitNew +
			// restore-on-reject shape so the K-live invariant
			// holds for every tombstone-promotion path.
			//
			// `bucket[idx] = incoming` when old is LIVE (the
			// other `improves` paths — better hops, higher
			// trust) is still a live→live replacement and does
			// not change the live count, so cap admission is
			// unnecessary there.
			if old.IsWithdrawn() && s.maxNextHopsPerOrigin > 0 {
				savedTombstone := bucket[idx]
				s.buckets[entry.Identity] = append(bucket[:idx], bucket[idx+1:]...)
				// Copy variant — see the newer-SeqNo tombstone-
				// promotion branch for the rationale. Restoring
				// the tombstone on AdmitNew reject must leave
				// its SeenOriginSeqs map untouched, so a future
				// legit retry at the same SeqNo can still pass
				// the per-Origin high-water guard.
				incoming.SeenOriginSeqs = mergeOriginObservationCopy(savedTombstone.SeenOriginSeqs, entry.Origin, entry.SeqNo)
				decision := s.AdmitNew(entry.Identity, incoming, now)
				switch decision {
				case AdmissionAccepted, AdmissionAcceptedReplaced:
					s.syncSeqCounterLocked(entry)
					return RouteAccepted, true, true
				default:
					s.buckets[entry.Identity] = append(s.buckets[entry.Identity], savedTombstone)
					return RouteRejected, true, false
				}
			}
			incoming.SeenOriginSeqs = mergeOriginObservation(old.SeenOriginSeqs, entry.Origin, entry.SeqNo)
			bucket[idx] = incoming
			s.syncSeqCounterLocked(entry)
			return RouteAccepted, true, true
		}
	}
	return RouteRejected, false, false
}

// WithdrawTriple marks (identity, uplink=key.NextHop) as
// withdrawn at seqNo. The key.Origin component is preserved on
// the public API for backward compatibility with
// Table.WithdrawRoute callers but is ignored internally — after
// Phase A the dedup key is per-(Identity, Uplink) and Origin
// carries no routing information.
//
// If no claim matches the pair, a tombstone is appended (the
// SeqNo resurrection guard, expiring after defaultTTL). If a
// matching claim exists, the claim's Hops/SeqNo/ExpiresAt/
// UpdatedAt/Withdrawn fields are rewritten in place — only when
// seqNo is strictly newer than the existing SeqNo.
//
// Returns whether the withdrawal applied. Caller must hold t.mu
// (writer).
func (s *routeStore) WithdrawTriple(key RouteTriple, seqNo uint64, now time.Time) bool {
	bucket, idx := s.findByUplinkLocked(key.Identity, key.NextHop)

	if idx < 0 {
		// No active claim for this (Identity, Uplink) pair — store
		// a tombstone so that a delayed older announcement with a
		// lower SeqNo cannot resurrect the withdrawn lineage. The
		// tombstone expires via normal TTL.
		//
		// Tombstones are appended unconditionally and bypass the
		// MaxNextHopsPerOrigin cap by design: AdmitNew excludes
		// withdrawn claims from both the bucket-size counter and
		// the eviction-search, so a tombstone neither competes
		// with live claims for the K slots nor displaces them.
		// This is what keeps the SeqNo resurrection guard intact
		// under cap pressure.
		tombstone := UplinkClaim{
			Uplink:    key.NextHop,
			Hops:      HopsInfinity,
			SeqNo:     seqNo,
			Source:    RouteSourceAnnouncement,
			UpdatedAt: now,
			ExpiresAt: now.Add(s.defaultTTL),
			Withdrawn: true,
			// key.Origin is the wire-frame Origin of the withdrawal
			// (the caller plumbs it through from Table.WithdrawRoute,
			// which receives it from the receive-path frame). Storing
			// it as LastIngressOrigin lets a follow-up live update
			// from a different Origin on the same uplink take the
			// cross-Origin lineage branch in ApplyUpdate.
			LastIngressOrigin: key.Origin,
			// Initialise the per-Origin high-water map with this
			// withdrawal's observation. A subsequent ApplyUpdate
			// from a different Origin then has a baseline to
			// consult; a delayed lower-SeqNo replay from key.Origin
			// itself is rejected by the standard same-Origin SeqNo
			// comparison in ApplyUpdate.
			SeenOriginSeqs: mergeOriginObservation(nil, key.Origin, seqNo),
		}
		s.buckets[key.Identity] = append(bucket, tombstone)
		return true
	}

	old := &bucket[idx]
	if seqNo <= old.SeqNo {
		return false
	}

	bucket[idx].Hops = HopsInfinity
	bucket[idx].SeqNo = seqNo
	bucket[idx].ExpiresAt = now.Add(s.defaultTTL)
	bucket[idx].UpdatedAt = now
	bucket[idx].Withdrawn = true
	// Track the withdrawal-frame Origin as the latest wire-frame
	// Origin we observed for this (Identity, Uplink). A pre-A1
	// follow-up update with yet another Origin then takes the
	// cross-Origin lineage branch in ApplyUpdate cleanly.
	bucket[idx].LastIngressOrigin = key.Origin
	// Merge the withdrawal observation into the per-Origin
	// high-water map so the cross-Origin stale-replay guard in
	// ApplyUpdate can see this Origin's progression. Preserves
	// prior observations from earlier ingest paths.
	bucket[idx].SeenOriginSeqs = mergeOriginObservation(bucket[idx].SeenOriginSeqs, key.Origin, seqNo)
	return true
}

// AdmitDirectPeer is the storage-shape half of Table.AddDirectPeer.
// Handles the idempotent fast path (existing alive direct claim →
// no-op), the new-route admission (NextSeq + AdmitDirect), and
// the tombstone-reactivation admission (detach tombstone +
// NextSeq + AdmitDirect).
//
// Returns the boundary RouteEntry shape that ended up in storage
// (or, on the idempotent fast path, the synthesised view of the
// existing one — Identity=peerIdentity, NextHop=peerIdentity,
// Origin=localOrigin per the toRouteEntry contract). Whether
// storage actually mutated is returned separately; mutated=false
// on the idempotent path so the caller (Table.AddDirectPeer)
// leaves the dirty flag untouched.
//
// Caller contract: t.mu held (writer); peerIdentity is non-empty;
// s.localOrigin is set (Table.AddDirectPeer pre-checks both).
func (s *routeStore) AdmitDirectPeer(peerIdentity PeerIdentity, now time.Time) (RouteEntry, bool) {
	bucket, idx := s.findByUplinkLocked(peerIdentity, peerIdentity)

	// Idempotent fast path: an existing alive DIRECT claim is a
	// no-op (direct claims have no TTL — ExpiresAt is zero — so
	// there is nothing to refresh; their lifetime is managed
	// entirely by the session lifecycle).
	//
	// Source == Direct is required by the idempotent contract.
	// Post-Phase-A the dedup key dropped Origin, so a non-Direct
	// claim with Uplink == peerIdentity could in principle occupy
	// the slot if peerIdentity ever announced itself to us via
	// the routing protocol (degenerate; the protocol does not
	// generate such announces, but a misbehaving peer or a
	// synthetic test fixture could). Falling through to the
	// detach-and-readmit path below replaces the foreign claim
	// with our own-origin Direct one — the live socket is the
	// authoritative truth.
	if idx >= 0 && !bucket[idx].IsWithdrawn() && bucket[idx].Source == RouteSourceDirect {
		return toRouteEntry(peerIdentity, s.localOrigin, bucket[idx]), false
	}

	// New route or re-activation after withdrawal — increment
	// SeqNo.
	seq := s.nextSeqLocked(peerIdentity)

	// ExpiresAt is intentionally left zero: a direct claim
	// represents a live socket and must never expire by time.
	// Only RemoveDirectPeer (triggered by socket close) may
	// invalidate it. Zero ExpiresAt makes IsExpired return false
	// unconditionally, so TickTTL will never remove an active
	// direct claim.
	claim := UplinkClaim{
		Uplink:    peerIdentity,
		Hops:      1,
		SeqNo:     seq,
		Source:    RouteSourceDirect,
		UpdatedAt: now,
		// LastIngressOrigin = localOrigin for own-direct claims:
		// matches the wire-frame Origin that this node would emit
		// on AnnounceProjectionFor's sender-originated synthesis,
		// so that any later ApplyUpdate arriving with a foreign
		// Origin (e.g. a pre-A1 transit relay using its own
		// localOrigin as the wire-frame Origin) cleanly trips the
		// cross-Origin lineage branch in ApplyUpdate's stale-SeqNo
		// path. See UplinkClaim.LastIngressOrigin doc-comment.
		LastIngressOrigin: s.localOrigin,
	}

	// Both the new-direct (idx<0) and tombstone-reactivation
	// (idx>=0 && IsWithdrawn) paths must respect the
	// MaxNextHopsPerOrigin cap on Identity buckets. A bare
	// append/in-place-overwrite would push live count past K
	// when:
	//
	//   - idx<0 and the bucket already holds K live claims (e.g.
	//     K hop_ack confirmations to the same destination via
	//     intermediate uplinks) — the new direct claim would be
	//     the K+1th live row.
	//   - idx>=0 and old is a tombstone: tombstones are NOT
	//     cap-counted (rule 1 of AdmitNew), so replacing one
	//     in-place flips a non-counted slot into a counted live
	//     slot and live count climbs from up-to-K to up-to-K+1.
	//
	// Direct admission has its own contract — it must always
	// succeed (the OS already holds the socket open, so the store
	// cannot refuse) — so this is AdmitDirect, not AdmitNew. The
	// helper evicts the worst evictable row when the bucket is
	// saturated and never rejects.
	if idx < 0 {
		// Fresh direct admission — initialise the per-Origin
		// high-water map with the own-origin observation. Future
		// ApplyUpdate calls for this Identity with a foreign
		// wire-frame Origin will consult this map when the
		// foreign Origin is itself revisited at a stale SeqNo.
		claim.SeenOriginSeqs = mergeOriginObservation(nil, s.localOrigin, seq)
		s.AdmitDirect(peerIdentity, claim, now)
	} else {
		// Reactivation from tombstone (or a foreign-Origin live
		// claim — see the idempotent fast-path doc above for why
		// non-Direct rows can occupy this slot). Detach the prior
		// slot so AdmitDirect sees a tombstone-free bucket and
		// the K-live invariant is evaluated against the post-
		// promotion shape, not pre. Preserve and merge the prior
		// claim's SeenOriginSeqs so the per-Origin high-water
		// history survives the reactivation; subsequent foreign-
		// Origin ingest can still take the cross-Origin stale-
		// replay guard against pre-disconnect observations.
		claim.SeenOriginSeqs = mergeOriginObservation(bucket[idx].SeenOriginSeqs, s.localOrigin, seq)
		s.buckets[peerIdentity] = append(bucket[:idx], bucket[idx+1:]...)
		s.AdmitDirect(peerIdentity, claim, now)
	}

	return toRouteEntry(peerIdentity, s.localOrigin, claim), true
}

// InvalidateAllVia is the storage half of Table.RemoveDirectPeer.
// Marks every claim whose Uplink matches the disconnected peer as
// withdrawn (Hops=HopsInfinity, ExpiresAt=now+defaultTTL,
// Withdrawn=true). Own-origin direct claims produce wire-ready
// withdrawal entries (SeqNo incremented through nextSeq); transit
// claims are silently marked locally.
//
// Direct claims in storage are own-origin by construction: every
// RouteSourceDirect entry comes from AddDirectPeer, which sets
// the origin to s.localOrigin. So `claim.Source == Direct` is
// sufficient as the wire-withdrawal trigger — no separate
// Origin == localOrigin check is needed (Origin is gone from
// storage anyway).
//
// Returns the produced withdrawals, the count of transit claims
// invalidated, and the identities where a non-withdrawn,
// non-expired backup survives via a different uplink.
//
// Caller contract: t.mu held (writer).
func (s *routeStore) InvalidateAllVia(uplink PeerIdentity, now time.Time) ([]AnnounceEntry, int, []PeerIdentity, []PeerIdentity) {
	var withdrawals []AnnounceEntry
	transitInvalidated := 0
	affectedIdentities := make(map[PeerIdentity]struct{})

	for identity, bucket := range s.buckets {
		for i := range bucket {
			c := &bucket[i]
			if c.Uplink != uplink || c.IsWithdrawn() {
				continue
			}

			affectedIdentities[identity] = struct{}{}

			if c.Source == RouteSourceDirect {
				seq := s.nextSeqLocked(identity)
				c.Hops = HopsInfinity
				c.SeqNo = seq
				c.ExpiresAt = now.Add(s.defaultTTL)
				c.UpdatedAt = now
				c.Withdrawn = true

				// Wire withdrawal uses Origin == localOrigin
				// ("sender originated route" legacy semantic).
				// This must match the live-emit Origin in
				// AnnounceProjectionFor so that pre-A1 receivers
				// — which key storage per-(Identity, Origin,
				// NextHop) — find the same triple for both live
				// and withdrawal frames. Otherwise the
				// withdrawal lands in a different triple slot
				// from the live entry and the route stays alive
				// on the legacy receiver until TTL. Post-A1
				// receivers drop Origin on ingest, so the value
				// is opaque to them.
				//
				// Empty-localOrigin fallback to identity matches
				// AnnounceProjectionFor / toRouteEntry so test
				// fixtures emit Origin != "" and pass
				// RouteEntry.Validate everywhere.
				origin := s.localOrigin
				if origin.IsZero() {
					origin = identity
				}
				// Run the wire withdrawal SeqNo through the
				// broadcast outbound SeqNo helper so the
				// outbound timeline stays consistent with
				// subsequent per-peer AnnounceProjectionFor
				// emits. Without this, the immediate disconnect
				// fanout would emit at `seq` (own counter)
				// while the next-cycle AnnounceProjectionFor
				// would observe a sig transition and bump to a
				// different value, leaving receivers with two
				// different SeqNos for the same withdrawal.
				//
				// The Broadcast variant advances
				// outboundBroadcastMax — RemoveDirectPeer's
				// caller fans these withdrawals out to every
				// connected peer, so every receiver's
				// (Identity, sender) timeline moves to this
				// SeqNo. Subsequent per-peer cache hits below
				// this watermark are then invalidated (the
				// broadcast-driven A→B→A guard).
				sig := outboundEmitSig{
					Uplink:    c.Uplink,
					Hops:      c.Hops,
					Withdrawn: true,
					ExtraSig:  string(normalizeExtra(c.Extra)),
					// Round-14: include AttestedSig bytes in the
					// content cache key — matches the live + tombstone
					// emit branches in route_store_lookup.go. See the
					// outboundEmitSig type doc for the sig-upgrade
					// fresh-SeqNo rationale.
					AttestedSig: string(c.AttestedSig),
				}
				// The second return value (armed) is informational
				// only here — InvalidateAllVia's caller
				// (RemoveDirectPeer) unconditionally marks
				// Table.dirty, so a hold-down arming inside this
				// broadcast cannot lag inside the published Snapshot
				// the way an AnnounceTo cycle's arming would.
				wireSeqNo, _ := s.nextOutboundSeqLockedBroadcast(identity, sig, seq, s.localOrigin, now)
				withdrawals = append(withdrawals, AnnounceEntry{
					Identity: identity,
					Origin:   origin,
					Hops:     HopsInfinity,
					SeqNo:    wireSeqNo,
					Extra:    c.Extra,
					// Phase 4 13.2-A: forward the stored attested-links
					// signature on the withdrawal as well. The Round-1
					// canonical-bytes iterations dropped first hops, then
					// epoch, then seq_no from the signed payload — the
					// shipped contract is `(identity || extra)` ONLY (see
					// RouteAnnounceV3Entry.CanonicalSigningBytes). The
					// withdrawal carries the SAME identity and the same
					// Extra bytes as the original announcement, so the
					// stored signature stays valid verbatim across the
					// live→tombstone transition: hops=HopsInfinity and the
					// new SeqNo are per-emitter wire fields outside the
					// canonical bytes. Forwarding the bytes here lets a
					// downstream v3-aware peer verify the tombstone
					// against the same destination-identity key the live
					// announcement used.
					AttestedSig:         c.AttestedSig,
					AttestedSigVerified: c.AttestedSigVerified,
				})
			} else {
				transitInvalidated++
				c.Hops = HopsInfinity
				c.ExpiresAt = now.Add(s.defaultTTL)
				c.UpdatedAt = now
				c.Withdrawn = true
			}
		}
	}

	// affected: every identity whose (identity, uplink) claim was invalidated.
	// The caller (RemoveDirectPeer) evicts their health states so they do not
	// linger for a whole tombstone TTL.
	affected := make([]PeerIdentity, 0, len(affectedIdentities))
	for identity := range affectedIdentities {
		affected = append(affected, identity)
	}

	// For each affected identity, check if a non-withdrawn,
	// non-expired backup claim survives via a different uplink.
	var exposed []PeerIdentity
	for identity := range affectedIdentities {
		for _, c := range s.buckets[identity] {
			if !c.IsWithdrawn() && !c.IsExpired(now) {
				exposed = append(exposed, identity)
				break
			}
		}
	}

	return withdrawals, transitInvalidated, affected, exposed
}

// InvalidateTransitVia is the storage half of
// Table.InvalidateTransitRoutes. Marks transit (non-direct)
// claims whose Uplink matches the peer as withdrawn. Does NOT
// touch direct claims and does NOT generate wire withdrawals —
// it is a defense-in-depth cleanup for peers that had
// mesh_routing_v1 (could advertise routes) but not
// mesh_relay_v1 (no direct route was created).
//
// Returns the count invalidated, the identities whose (identity, uplink)
// claim was tombstoned (affected — the caller evicts their health states so
// they do not linger for a full tombstone TTL), and the identities where the
// invalidation exposed a surviving non-withdrawn backup claim via a different
// uplink (exposed).
//
// Caller contract: t.mu held (writer).
func (s *routeStore) InvalidateTransitVia(uplink PeerIdentity, now time.Time) (int, []PeerIdentity, []PeerIdentity) {
	invalidated := 0
	affectedIdentities := make(map[PeerIdentity]struct{})

	for identity, bucket := range s.buckets {
		for i := range bucket {
			c := &bucket[i]
			if c.Uplink != uplink || c.IsWithdrawn() {
				continue
			}
			if c.Source == RouteSourceDirect {
				continue
			}
			affectedIdentities[identity] = struct{}{}
			c.Hops = HopsInfinity
			c.ExpiresAt = now.Add(s.defaultTTL)
			c.UpdatedAt = now
			c.Withdrawn = true
			invalidated++
		}
	}

	affected := make([]PeerIdentity, 0, len(affectedIdentities))
	for identity := range affectedIdentities {
		affected = append(affected, identity)
	}

	var exposed []PeerIdentity
	for identity := range affectedIdentities {
		for _, c := range s.buckets[identity] {
			if !c.IsWithdrawn() && !c.IsExpired(now) {
				exposed = append(exposed, identity)
				break
			}
		}
	}

	return invalidated, affected, exposed
}

// CompactExpired is the storage half of Table.TickTTL. Removes
// expired claims from every bucket (including own-origin
// tombstones whose TTL elapsed). Identities whose bucket fully
// drains are dropped from the store. Returns the total number of
// claims removed and the identities where at least one survivor
// is non-withdrawn (a backup is exposed).
//
// Only ExpiresAt is checked — withdrawn claims are kept until
// their ExpiresAt elapses. This preserves tombstones created by
// WithdrawTriple that guard against resurrection from delayed
// lower-SeqNo announcements. InvalidateAllVia and
// InvalidateTransitVia set a short ExpiresAt on withdrawn
// claims so they are cleaned up promptly without breaking
// tombstone semantics.
//
// Caller contract: t.mu held (writer).
func (s *routeStore) CompactExpired(now time.Time) (int, []PeerIdentity) {
	totalRemoved := 0
	var exposed []PeerIdentity

	for identity, bucket := range s.buckets {
		origLen := len(bucket)
		n := 0
		for i := range bucket {
			if !bucket[i].IsExpired(now) {
				bucket[n] = bucket[i]
				n++
			}
		}
		removed := origLen - n
		if removed == 0 {
			continue
		}
		totalRemoved += removed
		if n == 0 {
			delete(s.buckets, identity)
			continue
		}
		s.buckets[identity] = bucket[:n]
		// Some claims expired but survivors remain. Check if any
		// survivor is non-withdrawn — that means a usable backup
		// route was exposed by the expiry.
		for j := 0; j < n; j++ {
			if !bucket[j].IsWithdrawn() {
				exposed = append(exposed, identity)
				break
			}
		}
	}

	return totalRemoved, exposed
}

// nextSeqLocked increments and returns the next SeqNo for a
// given identity. Caller must hold t.mu (writer).
func (s *routeStore) nextSeqLocked(identity PeerIdentity) uint64 {
	s.seqCounters[identity]++
	return s.seqCounters[identity]
}

// nextOutboundSeqLockedPerPeer computes the wire SeqNo for a
// per-peer emit (AnnounceProjectionFor's live winner and the
// per-peer own-direct tombstone retry path). Only the receiver
// `peer` will see the returned SeqNo on this call, so only that
// peer's per-receiver timeline (outboundPeerMax) advances.
//
// Cache hit reuse requires the cached SeqNo to be at least both
// of:
//
//   - outboundBroadcastMax[Identity] — the cross-peer broadcast
//     watermark. A broadcast since the cache was set has
//     advanced EVERY connected peer's receiver-side timeline
//     past the cached value; reuse would trip the receiver's
//     tombstone-resurrection guard.
//
//   - outboundPeerMax[(Identity, peer)] — the emitting peer's
//     own per-receiver high water. A previous per-peer emit
//     with a different sig advanced THIS peer's timeline past
//     the cached value (the winner-switch A→B→A scenario), so
//     reuse would be rejected as stale on the receiver.
//
// On cache miss OR stale-cache (either watermark above the
// cached SeqNo), fresh allocation:
//
//	next = max(nativeSeqNo, outboundMax[Identity] + 1)
//
// The `+1` against outboundMax keeps the outbound timeline
// strictly newer than every prior emit for this Identity (any
// peer, any sig). `nativeSeqNo` lets foreign-fresh SeqNos pull
// the outbound timeline forward instead of just bumping by 1 —
// when foreign SeqNo=K with K > prev.SeqNo, the outbound SeqNo
// jumps directly to K, matching what a legacy receiver would
// have observed pre-A1 when transits relayed the foreign
// Origin's SeqNo unchanged. On the FIRST emit for an Identity
// (outboundMax=0) the `+1=1` clamp is at most as restrictive as
// nativeSeqNo>=1, so the first emit lands on nativeSeqNo
// verbatim — matching receivers' expectation that a fresh
// own-direct lineage starts at SeqNo=1.
//
// A fresh allocation updates outboundContent (so subsequent
// emits with the same sig to OTHER peers reuse the fresh value
// when their per-receiver watermark is happy with it),
// outboundMax (the global high water), and the emitting peer's
// outboundPeerMax. The broadcast watermark is NOT bumped — only
// `peer`'s timeline advanced on this call.
//
// Caller must hold t.mu (writer).
func (s *routeStore) nextOutboundSeqLockedPerPeer(identity, peer PeerIdentity, sig outboundEmitSig, nativeSeqNo uint64, trigger PeerIdentity, now time.Time) (uint64, bool) {
	cacheKey := outboundContentKey{Identity: identity, Sig: sig}
	peerKey := outboundPeerKey{Identity: identity, Peer: peer}
	nowNanos := now.UnixNano()
	if ce, ok := s.outboundContent[cacheKey]; ok && ce.seq >= s.outboundBroadcastMax[identity] && ce.seq >= s.outboundPeerMax[peerKey].seq {
		// Cache hit AND cached SeqNo is at least both watermarks:
		// reuse so receivers see no SeqNo change and ComputeDelta
		// stays a no-op on stable content.
		//
		// Per-peer cache reuse is gated on BOTH watermarks because
		// they cover orthogonal failure modes:
		//   * outboundBroadcastMax catches the broadcast-driven
		//     A→B→A scenario (cross-peer watermark) — every
		//     connected peer received the broadcast, so any peer's
		//     timeline is at least that high.
		//   * outboundPeerMax catches the per-peer winner-switch
		//     A→B→A scenario — only THIS peer was advanced, but
		//     reusing a cached SeqNo below its current view would
		//     be rejected.
		//
		// peerMax MUST be refreshed to `seq` on every hit, not
		// just on fresh allocation. The outboundContent cache is
		// shared across peers, so another peer's A→B→A path can
		// have bumped the cached SeqNo from peerMax up to
		// `seq > peerMax`. Reusing `seq` here actually sends THIS
		// peer that higher SeqNo, advancing its receiver-side
		// timeline accordingly — if we left peerMax at the old
		// value, a later cache hit for a DIFFERENT sig stored at
		// some value in (oldPeerMax, seq) would pass the
		// staleness check and emit a SeqNo below this peer's
		// actual view, which the receiver would reject as stale.
		// The assignment is a forward-only step (seq >= peerMax
		// by the check) so it cannot accidentally regress. lastUsed is
		// refreshed on both entries: it keeps an actively-reused
		// outboundContent shape from being TTL-aged. (It is also written
		// to outboundPeerMax for symmetry, but outboundPeerMax is never
		// TTL-aged — it is a high-water watermark evicted only by
		// lifecycle; see pruneOutboundCachesLocked.)
		s.outboundContent[cacheKey] = outboundSeqEntry{seq: ce.seq, lastUsed: nowNanos}
		s.outboundPeerMax[peerKey] = outboundSeqEntry{seq: ce.seq, lastUsed: nowNanos}
		return ce.seq, false
	}
	next := s.outboundMax[identity] + 1
	if nativeSeqNo > next {
		next = nativeSeqNo
	}
	s.outboundContent[cacheKey] = outboundSeqEntry{seq: next, lastUsed: nowNanos}
	s.outboundMax[identity] = next
	s.outboundPeerMax[peerKey] = outboundSeqEntry{seq: next, lastUsed: nowNanos}
	// Phase 1 P2: a FRESH allocation is the definition of an
	// outbound-SeqNo advance for `identity`. Cache hits (handled
	// above) explicitly do NOT advance — they reuse a previously-
	// issued SeqNo so the receiver sees no SeqNo change and
	// ComputeDelta stays a no-op on stable content. The velocity
	// tracker therefore observes one record per advance event, not
	// once per peer the cache fans out to.
	//
	// Returns whether hold-down was newly armed so the caller can
	// propagate the snapshot-visible state change through
	// AnnounceProjectionFor's `mutated` flag up to Table.AnnounceTo,
	// which marks t.dirty. Without that propagation
	// RouteCapStats.SeqNoFlapHoldowns would lag inside the published
	// Snapshot until some unrelated mutation triggered a republish —
	// surprising for the operator-facing engage counter.
	armed := s.flap.recordSeqAdvanceLocked(identity, trigger, now, &s.capStats.seqNoFlapHoldowns)
	return next, armed
}

// nextOutboundSeqLockedBroadcast computes the wire SeqNo for a
// broadcast emit — currently only InvalidateAllVia's own-direct
// tombstones, which RemoveDirectPeer's caller fans out to ALL
// connected peers. Every connected peer's receiver-side timeline
// is logically advanced to the returned SeqNo, so the cross-peer
// broadcast watermark must bump.
//
// Cache miss is the only path in practice (the same withdrawal
// sig is not re-emitted via this helper — the per-peer retry
// path uses nextOutboundSeqLockedPerPeer), but on the off chance
// the same sig reappears, cache reuse requires the cached SeqNo
// to be at least outboundBroadcastMax. Per-receiver state
// (outboundPeerMax) is NOT consulted here — broadcasts go to
// every connected peer and routeStore does not enumerate
// connections, so the conservative behaviour is to issue a
// fresh SeqNo whenever outboundMax has advanced since the last
// broadcast (i.e. some per-peer emit has burnt SeqNo space we
// would need to step over).
//
// Caller must hold t.mu (writer).
func (s *routeStore) nextOutboundSeqLockedBroadcast(identity PeerIdentity, sig outboundEmitSig, nativeSeqNo uint64, trigger PeerIdentity, now time.Time) (uint64, bool) {
	cacheKey := outboundContentKey{Identity: identity, Sig: sig}
	if ce, ok := s.outboundContent[cacheKey]; ok && ce.seq >= s.outboundMax[identity] {
		// Same content AND no per-peer or broadcast emit has
		// burnt a higher SeqNo since the cache was set — safe to
		// reuse. Bump the broadcast watermark so subsequent
		// per-peer cache hits at lower SeqNos are invalidated.
		// Refresh lastUsed so the reused shape is not aged out.
		s.outboundContent[cacheKey] = outboundSeqEntry{seq: ce.seq, lastUsed: now.UnixNano()}
		s.outboundBroadcastMax[identity] = ce.seq
		return ce.seq, false
	}
	next := s.outboundMax[identity] + 1
	if nativeSeqNo > next {
		next = nativeSeqNo
	}
	s.outboundContent[cacheKey] = outboundSeqEntry{seq: next, lastUsed: now.UnixNano()}
	s.outboundMax[identity] = next
	s.outboundBroadcastMax[identity] = next
	// Phase 1 P2: a fresh allocation through the broadcast helper is
	// equally an outbound-SeqNo advance for `identity` — every
	// connected peer's receiver-side timeline moves to `next`. Same
	// cap-engagement contract as the per-peer helper. The broadcast
	// helper is currently only called from InvalidateAllVia, which
	// already marks Table.dirty via the RemoveDirectPeer wrapper, so
	// the second return value is informational only — it lets
	// callers reuse the same propagation contract if a future
	// caller (not RemoveDirectPeer) needs to opt into the dirty
	// flag without an unconditional bump.
	armed := s.flap.recordSeqAdvanceLocked(identity, trigger, now, &s.capStats.seqNoFlapHoldowns)
	return next, armed
}

// syncSeqCounterLocked ensures the monotonic SeqNo counter stays
// ahead of any accepted own-origin direct entry. Without this, a
// store pre-populated via ApplyUpdate (e.g., restored from
// snapshot) could have a higher SeqNo than the counter, causing
// the next AdmitDirectPeer / InvalidateAllVia to emit a stale seq
// and break monotonicity.
//
// Post-Phase-A the canonical own-origin signal is
// `entry.Source == RouteSourceDirect`. Pre-Phase-A relied on
// `entry.Origin == localOrigin` (which was set only for own-origin
// direct routes in storage), but after the storage swap the
// boundary RouteEntry returned by Lookup carries
// Origin = localOrigin for EVERY claim (synthesised — see
// uplink_claim.go's toRouteEntry contract). That synthesis means
// foreign-origin RouteSourceAnnouncement / RouteSourceHopAck
// entries can reach syncSeqCounterLocked with entry.Origin ==
// localOrigin too (e.g., confirmRouteViaHopAck promotion paths
// re-submit the synthesised RouteEntry to UpdateRoute). Without
// the Source == Direct restriction the counter would be poisoned
// by foreign SeqNo values, and the next own-direct withdrawal
// would emit a stale seq that legacy receivers reject.
//
// RouteSourceDirect + Origin == localOrigin is the only own-origin
// shape that survives Table.UpdateRoute's pre-mu anti-spoof
// (foreign-Origin Direct is rejected upstream), so the additional
// Source check is both necessary and sufficient.
//
// Caller must hold t.mu (writer).
func (s *routeStore) syncSeqCounterLocked(entry RouteEntry) {
	if entry.Source != RouteSourceDirect {
		return
	}
	if s.localOrigin.IsZero() || entry.Origin != s.localOrigin {
		return
	}
	if entry.SeqNo > s.seqCounters[entry.Identity] {
		s.seqCounters[entry.Identity] = entry.SeqNo
	}
}
