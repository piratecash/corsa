package routing

import (
	"sync/atomic"
	"time"
)

// routeStore is the internal storage facade for routing.Table. It
// owns:
//   - the (Identity → []UplinkClaim) bucket map;
//   - per-identity SeqNo counters used for own-origin route emission;
//   - the MaxNextHopsPerOrigin cap and its atomic admission counters;
//   - the immutable knobs every storage operation needs (localOrigin,
//     defaultTTL).
//
// Cluster-mesh Phase 1 P0 Phase A landed the storage shape that
// routeStore exposes: entries are per-(Identity, Uplink) UplinkClaim
// values. Origin is consumed at the boundary (Table.UpdateRoute
// anti-spoof check) and dropped before reaching the store, because
// the Origin field carries no routing information beyond "who
// originally created this announce" — see uplink_claim.go for the
// per-uplink shape and the conversion helpers (toUplinkClaim /
// toRouteEntry).
//
// The cap-admission terminology in code, log fields and metric
// names still says "MaxNextHopsPerOrigin" / "cap" — Phase A keeps
// the existing knob (now interpreted as per-(Identity, Uplink)
// instead of per-(Identity, Origin), which is what the field-data
// analysis showed was actually useful). The eventual rename of the
// RPC `cap_admission` JSON key to `uplink_admission` is deferred
// to the Phase 6 cleanup window — it is a breaking dashboard
// change and stays out of Phase 1.
//
// Operations live on this type rather than on *Table because they
// are the methods whose implementation depends on the storage
// shape. Table-level wrappers (table_lookup.go, table_mutation.go,
// table_flap.go) pre-validate, acquire t.mu, delegate, and
// propagate the dirty signal. No low-level primitives over the
// bucket map are exposed.
//
// routeStore has NO mutex of its own. Every method requires the
// owning Table's t.mu to be held in the documented mode (reader
// for pure reads, writer for any mutation). The capStats atomic
// counters are bumped under writer t.mu in the admission path, so
// the atomic-load CapStats() reader can return without taking
// t.mu — the values can lag the routes view by at most one
// in-flight bump, which is acceptable for monitoring purposes.
//
// A single t.mu still protects routes, admission state, SeqNo
// counters, and the flap detector together, so Table.Snapshot /
// Table.TickTTL / Table.RemoveDirectPeer observe and mutate
// everything as one consistent unit.
type routeStore struct {
	// buckets maps destination identity → its current per-uplink
	// claim list. All mutation goes through methods in
	// route_store_*.go; no caller outside routeStore touches this
	// field. Each bucket contains at most one live claim per
	// Uplink (the SeqNo-newest one wins via ApplyUpdate), plus any
	// number of withdrawn tombstones (cap-bypass — see AdmitNew
	// rule 1).
	buckets map[PeerIdentity][]UplinkClaim

	// seqCounters tracks the next SeqNo to use for own-origin
	// routes, keyed by destination identity. Only the owner of
	// localOrigin may increment these counters. nextSeqLocked /
	// syncSeqCounterLocked are the only legitimate writers; both
	// require the owning Table's t.mu (writer). After Phase A
	// scoping is purely per-Identity (the legacy per-(Identity,
	// Origin) scope degenerated to per-Identity once Origin
	// dropped from storage).
	seqCounters map[PeerIdentity]uint64

	// capStats holds the monotonic counters surfaced by CapStats /
	// routing.Snapshot.CapStats. Fields are atomic so CapStats()
	// can read without t.mu — the per-decision increments inside
	// AdmitNew/AdmitDirect happen under writer t.mu anyway, so
	// the atomic reads cannot tear against an in-flight write.
	//
	// `seqNoFlapHoldowns` and `fastInvalidations` join the original
	// four counters in Phase 1 P2/P3 — they are produced by the
	// SeqNo flap cap (recordSeqAdvanceLocked engaging hold-down) and
	// by the hops>MaxSaneHops fast-invalidation path in ApplyUpdate.
	// Both still bump under writer t.mu (same contract).
	capStats struct {
		accepted             atomic.Uint64
		acceptedReplaced     atomic.Uint64
		rejectedFull         atomic.Uint64
		rejectedAllProtected atomic.Uint64
		seqNoFlapHoldowns    atomic.Uint64
		fastInvalidations    atomic.Uint64
		// badHopsHoldowns counts bad-hops hysteresis engage events
		// (recordBadHopsInvalidationLocked arming hold-down for an
		// Identity whose accepted fast-invalidations exceeded the
		// per-window budget). Paired 1:1 with the
		// routing_bad_hops_hold_down_engaged warn log.
		badHopsHoldowns atomic.Uint64
	}

	// maxSaneHops is the Phase 1 P3 fast-invalidation threshold —
	// ingest claims with `Hops > maxSaneHops` are recorded as
	// tombstones at the observed SeqNo (the SeqNo-resurrection
	// guard stays intact) instead of being admitted as live. Zero
	// (or negative) disables the path entirely so the bare Table
	// constructor (no WithMaxSaneHops) keeps the pre-P3 behaviour
	// deterministically — production paths wire MaxSaneHops via
	// WithMaxSaneHops. The default is MaxSaneHops (8); see that
	// constant's doc-comment for the rationale.
	maxSaneHops int

	// maxNextHopsPerOrigin caps the number of LIVE (non-withdrawn)
	// claims the store keeps for each Identity. Zero disables the
	// cap entirely. The field name preserves the operator-facing
	// knob (CORSA_MAX_NEXT_HOPS_PER_ORIGIN env var) for backward
	// compatibility, even though after Phase A the unit of
	// counting is per-(Identity, Uplink) rather than
	// per-(Identity, Origin). The deprecation + rename is deferred
	// to the Phase 6 cleanup window; until then the knob and its
	// env var stay first-class operator surface.
	maxNextHopsPerOrigin int

	// localOrigin is this node's Ed25519 fingerprint. Used by
	// AdmitNew / syncSeqCounterLocked / InvalidateAllVia /
	// AnnounceProjectionFor for own-origin handling. Mirrored from
	// Table.localOrigin by the WithLocalOrigin option; immutable
	// after construction.
	localOrigin PeerIdentity

	// defaultTTL is the route lifetime applied to learned entries
	// whose caller did not set ExpiresAt, and to tombstones created
	// by WithdrawTriple / InvalidateAllVia / InvalidateTransitVia.
	// Mirrored from Table.defaultTTL by the WithDefaultTTL option.
	defaultTTL time.Duration

	// outboundMax[Identity] is the maximum wire SeqNo we have ever
	// emitted for this Identity across all outbound tracks (live
	// and tombstone) and all peers. Maintained as a separate
	// counter from seqCounters (which is the own-direct lifecycle
	// counter bumped by AddDirectPeer / InvalidateAllVia / state-
	// restore via syncSeqCounterLocked) so that the very first
	// outbound emit on an Identity uses the claim's native SeqNo
	// without spuriously bumping past it (e.g. AddDirectPeer sets
	// seqCounters=1, the immediate first AnnounceTo would otherwise
	// emit at SeqNo=2 because of a seqCounters+1 clamp — breaking
	// receivers' expectation that a fresh own-direct lineage
	// starts at SeqNo=1 and any peer-side manually-built oldSnap
	// fixtures keyed by the canonical first SeqNo).
	//
	// On a fresh allocation in nextOutboundSeqLockedPerPeer /
	// nextOutboundSeqLockedBroadcast, the assigned SeqNo is
	// `max(claim.nativeSeqNo, outboundMax[Identity]+1)` — the
	// `+1` guarantees strict newness against any prior emit when
	// the new content lands on top of a prior tombstone or
	// alternate-uplink emit, while still letting the first emit
	// (outboundMax=0) use nativeSeqNo verbatim. After assignment
	// outboundMax tracks the issued SeqNo.
	outboundMax map[PeerIdentity]uint64

	// outboundBroadcastMax[Identity] is the maximum wire SeqNo we
	// have emitted via a BROADCAST path (currently
	// InvalidateAllVia's own-direct withdrawals — these are
	// fanned out by RemoveDirectPeer's caller to ALL connected
	// peers, so every connected peer's (Identity, sender) timeline
	// advances to this SeqNo). It is the cross-peer staleness
	// watermark for the outboundContent cache: a cached SeqNo
	// strictly below the broadcast watermark cannot be safely
	// reused for ANY peer, because every connected peer has
	// already moved its receiver-side timeline past that SeqNo
	// and a re-emit at the cached value would be rejected by the
	// tombstone-resurrection guard.
	//
	// Per-peer emits (AnnounceProjectionFor live/tombstone) do
	// NOT bump this counter — they advance only one peer's
	// timeline, so OTHER peers' cached SeqNos remain valid. This
	// is what keeps the split-horizon stability invariant: peer-P
	// receives sigQ@5, peer-Q receives sigP@6 in cycle 1; cycle 2
	// reuses 5 for peer-P and 6 for peer-Q because no broadcast
	// has crossed the watermark between cycles. Per-peer timeline
	// tracking for the EMITTING peer lives in outboundPeerMax;
	// see nextOutboundSeqLockedPerPeer for the combined check.
	outboundBroadcastMax map[PeerIdentity]uint64

	// outboundPeerMax[(Identity, peer)] is the maximum wire SeqNo
	// we have emitted to that specific peer for that Identity via
	// the per-peer path (AnnounceProjectionFor). It is the
	// per-receiver staleness watermark for outboundContent cache
	// reuse: a cached SeqNo strictly below this peer's current
	// timeline cannot be safely reused even if the cross-peer
	// broadcast watermark is happy with it. This guards the
	// per-peer winner-switch A→B→A scenario: cycle 1 emits sigA@5
	// to peer-Z, cycle 2's winner shifts and emits sigB@6 to
	// peer-Z, cycle 3's winner reverts to sigA — the cache for
	// sigA still says 5, but peer-Z's last-seen SeqNo is 6, so
	// reusing 5 would be silently rejected as stale. The fresh
	// allocation path issues 7 instead.
	//
	// Such silent winner switches can arise from
	// InvalidateTransitVia (a transit uplink disconnects, no wire
	// withdrawal is emitted, the alternate uplink becomes the new
	// winner) or routine alternate-uplink preference shifts from
	// ApplyUpdate's tie-break ordering.
	//
	// Refresh on cache hit: nextOutboundSeqLockedPerPeer bumps
	// peerMax to the cached SeqNo on EVERY hit, not just on fresh
	// allocation. The outboundContent cache is shared across
	// peers, so a different peer's A→B→A path can bump the cached
	// value above THIS peer's previous peerMax — when we then
	// reuse the cache, we genuinely emit that higher SeqNo to
	// this peer and the per-receiver watermark must follow. If we
	// left peerMax stale, a later cache hit on a DIFFERENT sig
	// stored at a value in `(oldPeerMax, seq]` would pass the
	// staleness check and emit a SeqNo below this peer's actual
	// receiver view.
	//
	// Broadcast emits do NOT update this map (the broadcast goes
	// to all peers and routeStore has no peer list to enumerate);
	// the broadcast invariant is carried by outboundBroadcastMax
	// instead. Per-peer cache reuse must satisfy BOTH watermarks:
	// `cached >= max(outboundPeerMax[(Identity, peer)],
	// outboundBroadcastMax[Identity])`.
	//
	// Eviction: not implemented in Phase A. Entries for
	// disconnected peers leak. Tracked alongside outboundContent
	// eviction as a follow-up.
	outboundPeerMax map[outboundPeerKey]uint64

	// flap is the per-peer flap detector. Mirrored from
	// Table.flap at NewTable construction (immutable after); the
	// pointer is non-nil for every routeStore produced by NewTable
	// and nil only for synthetic-test routeStore instances that
	// build a store directly via newRouteStore (Phase 1 P2/P3
	// helpers nil-check defensively).
	//
	// routeStore consults flap for two SeqNo flap-cap interactions:
	//
	//   - `nextOutboundSeqLockedPerPeer` / `nextOutboundSeqLockedBroadcast`
	//     call recordSeqAdvanceLocked on every FRESH allocation (cache
	//     miss / stale-cache invalidation) so the per-Identity
	//     velocity tracker sees each accepted outbound-SeqNo advance.
	//     Cache hits reuse a SeqNo and do NOT advance — they preserve
	//     ComputeDelta's no-op invariant.
	//   - `AnnounceProjectionFor` skips Identities currently in seq
	//     hold-down via isInSeqHoldDownLocked, suppressing wire emit
	//     for the entire DefaultSeqHoldDownDuration window.
	//
	// FlapDetector has no mutex of its own; t.mu (writer) covers
	// the seqVelocities map mutation and the seqHoldDownEngages
	// atomic bump for the duration of the call. Lock contract
	// identical to the rest of FlapDetector's *Locked surface —
	// see flap.go.
	flap *FlapDetector

	// outboundContent maps `(Identity, content-sig)` to the wire
	// SeqNo currently assigned for that exact wire content. The
	// cached SeqNo is REUSED across peers and across announce
	// cycles while both staleness watermarks
	// (outboundBroadcastMax, outboundPeerMax) allow it — so two
	// peers receiving the same content within one stability
	// window observe the same wire SeqNo, and the same peer
	// receiving the same content across cycles also does, and
	// ComputeDelta stays a no-op on stable content. A
	// watermark-driven invalidation (broadcast crossing the
	// cache, or a per-peer winner-switch advancing the receiver's
	// timeline past the cached value) forces a fresh allocation
	// that REWRITES the cache to a higher SeqNo — so over the
	// lifetime of the routeStore the same (Identity, sig) slot
	// can hold different SeqNos at different points, but at any
	// single instant only one.
	//
	// Peer-keyed caches would have flapped under split-horizon:
	// if peer P1's winner is Uplink=A and peer P2's winner is
	// Uplink=B (because A is P2's identity and gets filtered),
	// sequential per-peer emits would alternate the cache slot
	// and bump SeqNo on every cycle, defeating the delta no-op
	// invariant. Content-keyed caching sidesteps this — each
	// distinct content has its own slot.
	//
	// The sig (outboundEmitSig) includes Uplink, Hops, Withdrawn
	// flag, and a normalized fingerprint of the Extra field, so
	// any wire-visible content change forces a new outbound
	// SeqNo assignment.
	//
	// Staleness vs. outboundBroadcastMax and outboundPeerMax: a
	// cache entry is considered stale (forces fresh allocation on
	// next emit) when the entry's SeqNo is below EITHER the
	// cross-peer broadcast watermark OR the per-receiver peer
	// watermark. The broadcast watermark guards the broadcast-
	// driven A→B→A scenario (AddDirect sigA@1, RemoveDirect
	// broadcast tombstone sigB@2 — broadcastMax→2, reconnect-
	// AddDirect sigA hits cache@1 but 1<broadcastMax so issues
	// fresh @3). The per-receiver watermark guards the per-peer
	// winner-switch A→B→A (cycle 1 to peer-Z sigA@5, cycle 2 to
	// peer-Z sigB@6 because the winner changed, cycle 3 winner
	// reverts to sigA — without the peer watermark cache@5 would
	// be reused and peer-Z would reject it as stale). Broadcast
	// emits bump only the cross-peer watermark; per-peer emits
	// bump only the emitting peer's watermark. See
	// nextOutboundSeqLockedPerPeer / nextOutboundSeqLockedBroadcast
	// for the dispatcher logic.
	//
	// Eviction: not implemented in Phase A. The cache grows with
	// the number of distinct wire-content shapes seen over the
	// node's lifetime. For a 100-node mesh with moderate
	// topology churn this is well-bounded (~tens of thousands of
	// entries); long-running nodes with high churn would
	// eventually want a TTL-based eviction pass. Tracked as a
	// follow-up.
	//
	// Mutated under t.mu (writer). Table.AnnounceTo runs under
	// writer lock so AnnounceProjectionFor can update this state.
	outboundContent map[outboundContentKey]uint64
}

// outboundContentKey is the cache key for content-based outbound
// SeqNo assignment. Splitting on the full sig (including
// Withdrawn and normalized Extra) keeps every distinct wire
// content in its own slot — same content gets the same SeqNo
// across peers and across cycles.
type outboundContentKey struct {
	Identity PeerIdentity
	Sig      outboundEmitSig
}

// outboundPeerKey is the lookup key for per-receiver outbound
// SeqNo high-water tracking. Combined with outboundContent's
// cross-peer cache, this lets nextOutboundSeqLockedPerPeer
// detect the per-peer winner-switch A→B→A scenario where a
// cached SeqNo for an old winner is below the EMITTING peer's
// current timeline (the alternate winner moved that peer past
// the cached value via a previous per-peer emit).
type outboundPeerKey struct {
	Identity PeerIdentity
	Peer     PeerIdentity
}

// outboundEmitSig fingerprints the wire content for a single
// emit. Equality means "this is the same wire content we have
// already assigned an outbound SeqNo to — reuse it"; inequality
// forces a fresh assignment so receivers can distinguish the
// new emit from any prior wire state for this Identity.
//
// ExtraSig is the normalized canonical-byte string of the Extra
// field (via normalizeExtra in announce_builder.go) so json.RawMessage
// becomes a comparable map-key-compatible value AND so that
// equivalent JSON encodings collapse to the same sig.
//
// AttestedSig holds the raw attested-links signature bytes converted
// to a string (Go allows []byte→string for arbitrary byte sequences;
// the string just holds the byte sequence and stays
// map-key-comparable). Including it here is the Round-14 fix: a
// sig-only upgrade in storage (Phase 4 13.2-C reconfirmation path)
// must produce a fresh outbound SeqNo so the receiver's per-peer
// monotonicity check accepts the new wire content. Without
// AttestedSig in the cache key, the cache would hit on the previous
// (unsigned) emit, hand back the burnt SeqNo, and the downstream
// peer would reject the new (signed) entry as stale-by-SeqNo —
// stranding the attestation locally and breaking the rolling enable
// of mesh_attested_links_v1. AttestedSigVerified is intentionally
// NOT part of the cache key: it is a local-only observation, not
// wire content, so a verified→verified flip with identical bytes
// must NOT force a fresh SeqNo (the wire frame is byte-identical
// and the receiver already has it).
type outboundEmitSig struct {
	Uplink      PeerIdentity
	Hops        uint8
	Withdrawn   bool
	ExtraSig    string
	AttestedSig string
}

// newRouteStore returns an empty store with the package defaults.
// Options applied to the owning Table (WithMaxNextHopsPerOrigin,
// WithDefaultTTL, WithLocalOrigin) mutate the returned store's
// fields directly through the option closures.
func newRouteStore() *routeStore {
	return &routeStore{
		buckets:              make(map[PeerIdentity][]UplinkClaim),
		seqCounters:          make(map[PeerIdentity]uint64),
		defaultTTL:           DefaultTTL,
		outboundContent:      make(map[outboundContentKey]uint64),
		outboundMax:          make(map[PeerIdentity]uint64),
		outboundBroadcastMax: make(map[PeerIdentity]uint64),
		outboundPeerMax:      make(map[outboundPeerKey]uint64),
	}
}

// findByUplinkLocked locates the bucket and slice index of the
// claim from `uplink` for the destination `identity`. Returns
// (bucket, -1) when no claim from that uplink exists; the bucket
// return value is still valid (may be empty/nil) so callers can
// pass it directly to admission / mutation helpers without a
// separate bucket lookup. The bucket aliases storage — in-place
// mutation via `bucket[idx]` IS visible to subsequent readers;
// structural changes go through direct s.buckets assignment
// from the caller (which lives in the same file as the storage
// type).
//
// Phase A dedup key: (Identity, Uplink) — Origin was dropped
// when the per-uplink storage shape replaced the RouteTriple.
// Boundary callers that come in with RouteTriple (WithdrawRoute /
// InspectTriple) project to (Identity, NextHop) and pass NextHop
// as uplink here.
//
// Package-private and used only inside route_store_*.go. Caller
// must hold t.mu (reader OK for lookup; writer if the caller
// then mutates).
func (s *routeStore) findByUplinkLocked(identity, uplink PeerIdentity) ([]UplinkClaim, int) {
	bucket := s.buckets[identity]
	for i := range bucket {
		if bucket[i].Uplink == uplink {
			return bucket, i
		}
	}
	return bucket, -1
}

// peekLiveUplinkSeqLocked returns the SeqNo stored on the live
// (identity, uplink) claim — non-withdrawn, non-expired against
// `now`. The bool is false when no claim exists for the pair OR
// when the claim is already a withdrawn tombstone OR has expired
// past its TTL. Used by Table.InvalidateUplinkClaim (Phase 4 13.3
// poison-reverse) to pick a strictly-newer SeqNo for the in-place
// withdrawal — see that helper for the contract.
//
// Why "live only" rather than "any slot". An earlier version of
// this helper returned the SeqNo of ANY slot, including already-
// withdrawn tombstones, which made repeated route_poison_v1 frames
// non-idempotent: every duplicate poison bumped the tombstone's
// SeqNo by +1, published a fresh route-change event, and inflated
// the SeqNo space against which a legitimate recovery announce
// would be tested for monotonicity (the recovery announce uses
// the origin's native SeqNo, which after enough duplicate poisons
// could rank below the tombstone and be rejected). Treating
// already-withdrawn / already-expired claims as "no live claim"
// makes the second-and-later poison from the same peer a clean
// no-op at the storage layer — the SeqNo stays put, no
// route-change event fires, and recovery is unimpeded.
//
// Read-only; caller must hold t.mu (reader OK).
func (s *routeStore) peekLiveUplinkSeqLocked(identity, uplink PeerIdentity, now time.Time) (uint64, bool) {
	bucket, idx := s.findByUplinkLocked(identity, uplink)
	if idx < 0 {
		return 0, false
	}
	claim := bucket[idx]
	if claim.IsWithdrawn() || claim.IsExpired(now) {
		return 0, false
	}
	return claim.SeqNo, true
}

// Total returns the total number of claim entries across all
// buckets — including withdrawn tombstones and expired-but-not-
// yet-reclaimed rows. Used by Table.Size. Caller must hold t.mu
// (reader OK).
func (s *routeStore) Total() int {
	n := 0
	for _, bucket := range s.buckets {
		n += len(bucket)
	}
	return n
}

// CountActive returns the number of non-withdrawn, non-expired
// claims against `now`. Used by Table.ActiveSize. Caller must
// hold t.mu (reader OK).
func (s *routeStore) CountActive(now time.Time) int {
	n := 0
	for _, bucket := range s.buckets {
		for i := range bucket {
			if !bucket[i].IsWithdrawn() && !bucket[i].IsExpired(now) {
				n++
			}
		}
	}
	return n
}

// SnapshotRoutes returns a deep copy of every bucket projected
// back into the boundary RouteEntry shape, plus total and active
// counts in a single pass. Intended for Table.Snapshot.
//
// The published map preserves the legacy Identity → []RouteEntry
// shape so callers (RPC handlers, tests, monitoring) keep
// working. Synthesised RouteEntry values carry Origin =
// localOrigin (with fallback to Identity when localOrigin is
// empty — test fixtures without WithLocalOrigin). This matches
// the wire-emit contract used by AnnounceProjectionFor /
// InvalidateAllVia and keeps pre-A1 receivers' withdrawal anti-
// spoof working through the mixed rollout. See uplink_claim.go
// for the migration contract rationale.
//
// Caller must hold t.mu (reader OK).
func (s *routeStore) SnapshotRoutes(now time.Time) (routes map[PeerIdentity][]RouteEntry, total int, active int) {
	routes = make(map[PeerIdentity][]RouteEntry, len(s.buckets))
	for id, bucket := range s.buckets {
		copied := make([]RouteEntry, len(bucket))
		for i := range bucket {
			copied[i] = toRouteEntry(id, s.localOrigin, bucket[i])
			if !bucket[i].IsWithdrawn() && !bucket[i].IsExpired(now) {
				active++
			}
		}
		routes[id] = copied
		total += len(bucket)
	}
	return routes, total, active
}

// CapStats returns a value-copy of the monotonic counters
// surfaced under the legacy `cap_admission` JSON envelope. The
// envelope today carries FOUR independent policies (see
// RouteCapStats in types.go for the per-field semantics):
//
//   - K-cap admission (Accepted / AcceptedReplaced / RejectedFull /
//     RejectedAllProtected) — gated by maxNextHopsPerOrigin.
//   - SeqNo flap cap (SeqNoFlapHoldowns) — gated by the
//     maxSeqAdvancePerWindow + seqAdvanceWindow knobs on
//     FlapDetector (Phase 1 P2).
//   - Fast invalidation (FastInvalidations) — gated by
//     maxSaneHops (Phase 1 P3).
//   - Bad-hops hysteresis (BadHopsHoldowns) — gated by the
//     maxBadHopsPerWindow + badHopsWindow knobs on FlapDetector
//     AND transitively by maxSaneHops (reachable only from the P3
//     branch).
//
// Callers may invoke this without t.mu — every counter is atomic
// and the returned snapshot reflects each field's value at its
// individual Load point. Cross-field consistency is best-effort:
// under heavy churn the seven counters can be observed at different
// points along the timeline by at most one increment each, which
// is fine for monitoring purposes.
//
// Per-policy kill-switch: each policy's counters stay at zero only
// when ITS OWN knob is non-positive — disabling the K-cap does NOT
// silence the P2/P3 counters, and vice versa. The admission paths
// short-circuit before their respective increments in each
// disabled mode.
func (s *routeStore) CapStats() RouteCapStats {
	return RouteCapStats{
		Accepted:             s.capStats.accepted.Load(),
		AcceptedReplaced:     s.capStats.acceptedReplaced.Load(),
		RejectedFull:         s.capStats.rejectedFull.Load(),
		RejectedAllProtected: s.capStats.rejectedAllProtected.Load(),
		SeqNoFlapHoldowns:    s.capStats.seqNoFlapHoldowns.Load(),
		FastInvalidations:    s.capStats.fastInvalidations.Load(),
		BadHopsHoldowns:      s.capStats.badHopsHoldowns.Load(),
	}
}
