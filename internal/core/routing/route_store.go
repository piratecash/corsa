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

	// identityHex memoizes the lowercase-hex wire form of each
	// destination identity (PeerIdentity.String()). The projection
	// path re-encodes every emitted identity to hex on every cycle,
	// once per receiving peer and again inside the v3 size-chunker's
	// per-entry measure() — the single largest non-snapshot allocator
	// in the announce plane (alloc_space profiling, Jun 2026). Because
	// the hex is a pure, immutable function of the 20 identity bytes
	// (the map key itself), a memo never goes stale; the lookup
	// returns a shared string with zero allocation.
	//
	// Lifecycle / leak contract: this is a DERIVED cache, not
	// authoritative state. It is populated lazily under writer t.mu in
	// identityHexLocked (the projection path holds the writer lock —
	// AnnounceToWithChangeHead / AnnounceDeltaTo) and pruned at the
	// single bucket-removal chokepoint in CompactExpired, so an entry
	// lives exactly as long as its identity's bucket. Every dead
	// bucket reaches CompactExpired via TTL, so the memo cannot
	// outgrow the live identity set — unlike an unbounded global hex
	// cache, which would be the same leak class as the ban-domain /
	// seenReceipts maps. It is kept separate from buckets (rather than
	// folded into a per-identity value struct) to avoid churning the
	// ~45 hot bucket-access sites; consolidation is deferred.
	identityHex map[PeerIdentity]string

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
	// Eviction is lifecycle-driven only (NOT TTL/size — this is a
	// high-water watermark, not a reuse cache; regressing it would cause
	// stale-reject loops). forgetReceiverLocked drops a disconnected
	// receiver's entries; pruneOutboundCachesLocked drops entries whose
	// destination identity is fully gone. The lastUsed field is carried for
	// symmetry with outboundContent / hit-path refresh but is not consulted
	// for peerMax eviction.
	outboundPeerMax map[outboundPeerKey]outboundSeqEntry

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
	// Eviction (was: "not implemented in Phase A" — the v1.0.47
	// cluster-mesh memory growth): each entry carries a lastUsed stamp,
	// refreshed on every reuse/allocation. pruneOutboundCachesLocked, run
	// on the TickTTL cadence, drops entries that are either DEAD (SeqNo
	// below the identity's broadcast watermark — never reusable for any
	// peer) or aged past outboundCacheTTL without being re-emitted (the
	// content shape is no longer the live projection). The cache thus
	// collapses back to the set of content shapes actually emitted within
	// the TTL window — the live working set — instead of accumulating
	// every distinct wire shape over the node's lifetime.
	//
	// Mutated under t.mu (writer). Table.AnnounceTo runs under
	// writer lock so AnnounceProjectionFor can update this state.
	outboundContent map[outboundContentKey]outboundSeqEntry
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

// outboundSeqEntry is the value type of the outboundContent and
// outboundPeerMax maps: the issued wire SeqNo plus the wall-clock
// (unix-nanos) instant it was last reused or allocated. lastUsed drives
// TTL eviction of the outboundContent REUSE CACHE (pruneOutboundCachesLocked)
// so it collapses to the live working set. It is also refreshed on
// outboundPeerMax for symmetry, but is NOT consulted for outboundPeerMax
// eviction — that map is a high-water watermark bounded by lifecycle, not
// TTL (see outboundPeerMax / pruneOutboundCachesLocked).
type outboundSeqEntry struct {
	seq      uint64
	lastUsed int64
}

const (
	// outboundCacheTTL is the idle window after which an outboundContent
	// entry is evicted (it applies ONLY to that reuse cache — outboundPeerMax
	// is a high-water watermark and is bounded by lifecycle, never TTL). Sized
	// well above the announce cadence (forced-full ≤ 30s, TickTTL sweep ≈ 10s)
	// so a content shape that is still live — and therefore re-emitted every
	// cycle, refreshing lastUsed — is never evicted; only shapes that stopped
	// being emitted (winner switched, route withdrawn, signature rotated) age
	// out.
	outboundCacheTTL = 5 * time.Minute

	// outboundContentSoftCap is a hard backstop on the outboundContent
	// reuse cache: even if TTL/lifecycle eviction lags (clock skew, a
	// pathological churn burst between sweeps) it cannot exceed this size.
	// Set far above the legitimate working set of a large mesh. (There is
	// deliberately no size cap on outboundPeerMax — arbitrary eviction of
	// a live receiver's high-water watermark would be unsafe; it is bounded
	// by lifecycle eviction instead — see pruneOutboundCachesLocked /
	// forgetReceiverLocked.)
	outboundContentSoftCap = 50000
)

// newRouteStore returns an empty store with the package defaults.
// Options applied to the owning Table (WithMaxNextHopsPerOrigin,
// WithDefaultTTL, WithLocalOrigin) mutate the returned store's
// fields directly through the option closures.
func newRouteStore() *routeStore {
	return &routeStore{
		buckets:              make(map[PeerIdentity][]UplinkClaim),
		seqCounters:          make(map[PeerIdentity]uint64),
		identityHex:          make(map[PeerIdentity]string),
		defaultTTL:           DefaultTTL,
		outboundContent:      make(map[outboundContentKey]outboundSeqEntry),
		outboundMax:          make(map[PeerIdentity]uint64),
		outboundBroadcastMax: make(map[PeerIdentity]uint64),
		outboundPeerMax:      make(map[outboundPeerKey]outboundSeqEntry),
	}
}

// pruneOutboundCachesLocked collapses the outbound-SeqNo state back to the
// live working set. Run on the TickTTL cadence (see Table.TickTTL). The two
// maps are evicted by DIFFERENT rules because only one of them is a cache:
//
//   - outboundContent is a reuse cache, so evicting any entry is
//     correctness-safe (a miss simply allocates a fresh SeqNo via the
//     never-pruned outboundMax+1, always monotonic). It drops entries that
//     are DEAD (SeqNo strictly below the identity's broadcast watermark —
//     unreusable for EVERY peer), aged past outboundCacheTTL without a
//     re-emit (no longer any peer's live projection), or whose destination
//     identity is fully gone (no claims). A soft-cap is the last-resort
//     backstop.
//
//   - outboundPeerMax is NOT a cache — each entry is the sender's copy of a
//     receiver's high-water SeqNo. It is NEVER aged out by TTL and NEVER
//     size-capped: dropping a STILL-LIVE (identity, receiver) watermark and
//     reading the missing entry as zero would let a surviving outboundContent
//     entry reuse a SeqNo below the receiver's timeline (stale-reject loop —
//     the A→B→A hazard the watermark exists to prevent). It is bounded ONLY
//     by lifecycle: this function drops entries whose DESTINATION identity is
//     fully gone (safe because the content for that identity is co-evicted
//     above, so a return cannot trigger a stale hit), and forgetReceiverLocked
//     drops a disconnected RECEIVER's entries on RemoveDirectPeer.
//
// Caller must hold t.mu (writer).
func (s *routeStore) pruneOutboundCachesLocked(now time.Time) {
	ttlCutoff := now.Add(-outboundCacheTTL).UnixNano()

	for k, e := range s.outboundContent {
		// Evict when the destination identity is fully gone (no claims),
		// when the SeqNo is DEAD (below the identity's broadcast watermark
		// — never reusable for any peer), or when the shape has not been
		// re-emitted within the TTL window. Any of these becomes a cache
		// MISS on the next emit, which allocates a fresh monotonic SeqNo
		// via outboundMax+1 (never pruned) — correctness-safe.
		if len(s.buckets[k.Identity]) == 0 ||
			e.seq < s.outboundBroadcastMax[k.Identity] ||
			e.lastUsed < ttlCutoff {
			delete(s.outboundContent, k)
		}
	}
	if len(s.outboundContent) > outboundContentSoftCap {
		target := outboundContentSoftCap * 3 / 4
		for k := range s.outboundContent {
			if len(s.outboundContent) <= target {
				break
			}
			delete(s.outboundContent, k)
		}
	}

	// outboundPeerMax is NOT a reuse cache: each entry is the sender's copy
	// of a receiver's high-water SeqNo. Dropping it for a STILL-LIVE
	// (identity, receiver) pair would let a surviving outboundContent entry
	// reuse a SeqNo below the receiver's timeline — the A→B→A stale-reject
	// hazard the watermark exists to prevent (the sender would then record
	// the lowered value and stay stuck). So it is bounded ONLY by lifecycle,
	// never by TTL or size:
	//   - forgetReceiverLocked drops a disconnected receiver's entries
	//     (it is never emitted to again, so the watermark is never read);
	//   - here we drop entries whose DESTINATION identity is fully gone.
	//     This is safe precisely because the content loop above co-evicts
	//     that identity's outboundContent, so when the identity returns no
	//     surviving content can trigger a stale hit and the fresh-allocation
	//     path issues a SeqNo above the retained outboundMax.
	for k := range s.outboundPeerMax {
		if len(s.buckets[k.Identity]) == 0 {
			delete(s.outboundPeerMax, k)
		}
	}
}

// forgetReceiverLocked drops every per-receiver outbound SeqNo watermark
// for a peer that has disconnected as a RECEIVER. Once the peer is gone we
// no longer emit per-peer projections to it, so the watermarks are never
// consulted again; on reconnect it gets a fresh full sync. This is the
// safe, lifecycle-driven bound for the (•, peer) half of outboundPeerMax
// (TTL/size eviction of a live receiver's watermark would be unsafe — see
// pruneOutboundCachesLocked). Caller must hold t.mu (writer).
func (s *routeStore) forgetReceiverLocked(peer PeerIdentity) {
	for k := range s.outboundPeerMax {
		if k.Peer == peer {
			delete(s.outboundPeerMax, k)
		}
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

// SnapshotRoutesIncremental is the copy-on-write variant of
// SnapshotRoutes. It reuses the caller's previous projection (prev)
// for every identity that is NOT in dirty and whose bucket length is
// unchanged, deep-copying only dirty / newly-appeared / length-changed
// buckets. This removes the per-rebuild full-table allocation that
// dominated routing churn on otherwise-idle nodes (the published
// projection of a bucket is a pure function of its UplinkClaim values —
// see toRouteEntry, which does not read `now` — so an untouched bucket's
// projection is byte-for-byte reusable).
//
// total and active are ALWAYS recomputed by walking every bucket (cheap
// field reads, no allocation), so time-driven expiry (IsExpired(now) on
// a finite-TTL claim that aged out without a writer event) stays exact
// even for reused buckets — only the slice allocation is skipped, never
// the count.
//
// Safety against a missed dirty-mark: the len(prev[id]) == len(bucket)
// guard re-copies any bucket whose claim count changed even if the
// mutation site forgot to mark it dirty (every add/remove is caught
// immediately). The only residual gap is a same-length in-place field
// mutation on an unmarked identity. The caller's periodic full rebuild
// (full == true) heals it, but note the node-side contract
// (rebuildRoutingSnapshot): that periodic full only rides a rebuild that
// is already happening because the table was dirty — a clean idle table is
// not woken, so the heal is bounded by "the next dirty rebuild after the
// interval", not by a fixed wall-clock period. A mis-marked mutation that
// is the very last one before the table goes permanently idle therefore
// stays until the next mutation; in practice any live node sees dirty
// activity (TickTTL, announces) well within the interval.
//
// When prev is nil or full is true every bucket is copied (cold start /
// self-heal pass). Caller must hold t.mu (writer: the Table wrapper
// consumes its snap dirty-set in the same critical section).
func (s *routeStore) SnapshotRoutesIncremental(
	prev map[PeerIdentity][]RouteEntry,
	dirty map[PeerIdentity]struct{},
	full bool,
	now time.Time,
) (routes map[PeerIdentity][]RouteEntry, total int, active int) {
	routes = make(map[PeerIdentity][]RouteEntry, len(s.buckets))
	for id, bucket := range s.buckets {
		total += len(bucket)

		projected, reused := s.reuseBucketProjection(prev, dirty, full, id, bucket)
		if !reused {
			projected = make([]RouteEntry, len(bucket))
			for i := range bucket {
				projected[i] = toRouteEntry(id, s.localOrigin, bucket[i])
			}
		}
		routes[id] = projected

		// Active count walks live state every call (time-driven expiry
		// must not lag behind the reused projection).
		for i := range bucket {
			if !bucket[i].IsWithdrawn() && !bucket[i].IsExpired(now) {
				active++
			}
		}
	}
	return routes, total, active
}

// reuseBucketProjection returns the cached projection for id when it is
// safe to reuse (not a full rebuild, prev exists, id not marked dirty,
// and the cached slice length still matches the live bucket). The second
// return reports whether the cache was reused; on false the caller
// deep-copies the bucket. Caller must hold t.mu.
func (s *routeStore) reuseBucketProjection(
	prev map[PeerIdentity][]RouteEntry,
	dirty map[PeerIdentity]struct{},
	full bool,
	id PeerIdentity,
	bucket []UplinkClaim,
) ([]RouteEntry, bool) {
	if full || prev == nil {
		return nil, false
	}
	if _, isDirty := dirty[id]; isDirty {
		return nil, false
	}
	cached, ok := prev[id]
	if !ok || len(cached) != len(bucket) {
		return nil, false
	}
	return cached, true
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
