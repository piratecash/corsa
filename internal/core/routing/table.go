package routing

import (
	"sync"
	"sync/atomic"
	"time"
)

// RouteUpdateStatus describes the outcome of UpdateRoute.
type RouteUpdateStatus int

const (
	// RouteAccepted means the route was new or improved (inserted/updated).
	RouteAccepted RouteUpdateStatus = iota
	// RouteUnchanged means an existing alive (non-withdrawn, non-expired)
	// route with this triple was found but the incoming entry did not improve
	// it (same or worse SeqNo/trust/hops). The route is usable through
	// this next-hop — callers can treat this as a reconfirmation.
	RouteUnchanged
	// RouteRejected means the route was rejected: tombstone protection
	// blocked a same-SeqNo update on a withdrawn entry, or the incoming
	// SeqNo was strictly lower than the existing one.
	RouteRejected
)

const (
	// DefaultTTL is the default lifetime of a learned route on a neighbour
	// before it must be refreshed by a forced full sync.
	//
	// It was raised from 120s to 600s to move off the RIP-style "re-flood the
	// whole table often" model toward BGP/OSPF: route liveness is carried by
	// cheap, already-existing signals — session close (RemoveDirectPeer emits an
	// immediate withdrawal of every route via the dropped peer) and the Phase 2
	// health path (hop_ack-driven Dead detection excludes a silently-dead uplink
	// from Lookup well before this TTL) — so this TTL is only the soft-state
	// backstop against a missed withdrawal, not the primary freshness mechanism.
	// A longer TTL lets the forced-full cadence (DefaultTTL/2 ceiling) stretch
	// from 60s to 300s, cutting the per-peer full re-projection — the dominant
	// announce-plane allocator on a large mesh — by ~5x. See
	// ForcedFullSyncMultiplier and docs/routing.md "Refresh interval invariant".
	DefaultTTL = 600 * time.Second
)

// Table is the local distance-vector routing table. It stores routes keyed
// by (Identity, Uplink=NextHop) UplinkClaim values inside the embedded
// routeStore (see uplink_claim.go for the per-uplink storage contract).
// Post-Phase-A the legacy (Identity, Origin, NextHop) triple dedup key is
// gone — Origin is consumed by Table.UpdateRoute for the foreign-origin
// Direct anti-spoof and then dropped from storage.
//
// localOrigin is this node's identity (Ed25519 fingerprint). It is used
// as the Origin field for direct routes on emit, as the wire-frame Origin
// stamped by Lookup output (sender-originated semantic, see
// uplink_claim.go::toRouteEntry), and as the scope for the monotonic
// SeqNo counter that own-direct routes consume on AddDirectPeer /
// RemoveDirectPeer. Outbound wire SeqNos go through
// routeStore.nextOutboundSeqLockedPerPeer / nextOutboundSeqLockedBroadcast
// for per-(Identity, sender) monotonicity — see route_store.go's
// outboundContent / outboundMax / outboundBroadcastMax / outboundPeerMax
// field doc-comments.
//
// All public methods are safe for concurrent use.
//
// The implementation is intentionally split across several files inside
// this package so that no single file owns the full table life cycle:
//   - table.go (this file): struct definition, constructor, options,
//     dirty flag, the smallest Table-level accessors (Size / ActiveSize
//     / LocalOrigin / CapStats), the synthetic self-route.
//   - route_store.go: routeStore type — owns the
//     Identity → []UplinkClaim map, the SeqNo counters, the cap
//     atomic counters, and the immutable knobs (localOrigin,
//     defaultTTL, maxNextHopsPerOrigin). Exposes only the
//     aggregate read-only accessors (Total, CountActive,
//     SnapshotRoutes, CapStats); low-level primitives over the
//     bucket map are intentionally NOT public to keep the storage
//     shape contained.
//   - route_store_admission.go: MaxNextHopsPerOrigin admission policy
//     (AdmitNew, AdmitDirect, isEvictable / isLive* / isWorse* /
//     isStrictlyBetter*).
//   - route_store_mutation.go: write-side storage operations
//     (ApplyUpdate, WithdrawTriple, AdmitDirectPeer, InvalidateAllVia,
//     InvalidateTransitVia, CompactExpired, syncSeqCounterLocked,
//     nextSeqLocked).
//   - route_store_lookup.go: read-side storage projections
//     (LookupActive, InspectTriple, AnnounceableFor,
//     AnnounceProjectionFor).
//   - flap.go: FlapDetector type — per-peer flap state machine
//     (isInHoldDownLocked, recordWithdrawalLocked, clearStableLocked,
//     snapshotLocked, tickLocked, holdDownDurationForBurst).
//   - table_flap.go: thin Table wrappers (RecordSuccessfulRouteAdd,
//     FlapSnapshot) that acquire t.mu before delegating to flap.
//   - table_lookup.go: thin Table wrappers for read-side public API
//     (Announceable, AnnounceTo, Lookup, InspectTriple, Snapshot).
//     Acquire t.mu, delegate to routeStore / flap, layer the
//     synthetic local-route injection on top.
//   - table_mutation.go: thin Table wrappers for write-side public
//     API (UpdateRoute, WithdrawRoute, AddDirectPeer,
//     RemoveDirectPeer, InvalidateTransitRoutes, TickTTL,
//     RefreshDirectPeers). Pre-validate, acquire t.mu, delegate to
//     routeStore, mark dirty.
//
// Cluster-mesh Phase 1 P0 Phase A landed the storage shape:
// inner storage is `map[PeerIdentity][]UplinkClaim` with per-
// (Identity, Uplink) dedup, Origin dropped after the anti-spoof
// check in Table.UpdateRoute. Wire format and the public Table
// API remain unchanged — synthesised RouteEntry values returned
// by Lookup/Snapshot/Announceable carry Origin = localOrigin
// (with fallback to Identity for tables without WithLocalOrigin).
// This shape keeps pre-A1 receivers' withdrawal anti-spoof
// (Origin == sender) working by making live and withdrawal emit
// share the same (Identity, Origin=sender, NextHop=sender)
// triple on the receiver side. See uplink_claim.go for the
// conversion helpers and the migration contract rationale.
type Table struct {
	mu sync.RWMutex

	// store owns the bucket map, the SeqNo counters, the cap state,
	// and every storage-shape-dependent operation (admission,
	// dedup, withdrawal, invalidation, compaction, lookup
	// projections). See route_store*.go. routeStore has no mutex;
	// t.mu protects it.
	store *routeStore

	// localOrigin is this node's Ed25519 fingerprint. Kept on Table
	// for the pre-mu validation in UpdateRoute (anti-spoof check on
	// foreign-origin RouteSourceDirect) and for the synthetic
	// self-route injection in Lookup / Snapshot. The store also
	// holds a copy (set via WithLocalOrigin); both fields are
	// immutable after construction so the duplication is safe.
	localOrigin PeerIdentity

	// clock is used for time-dependent operations, allowing tests to
	// inject a controllable clock. Every routeStore method that
	// depends on the wall-clock receives the value as a `now`
	// parameter sampled here under t.mu, which keeps the routes /
	// flap / cap-stats / SnapshotRoutes view internally consistent.
	clock func() time.Time

	// flap owns the per-peer flap-detection state machine plus its
	// configuration knobs (window / threshold / hold-down / penalized
	// TTL). FlapDetector has no mutex of its own; its *Locked methods
	// require t.mu to be held in the documented mode, so the routes
	// map and the flap state continue to share a single lock — see
	// flap.go for the lock contract.
	flap *FlapDetector

	// dirty signals to an external snapshot publisher (Service.rebuildRoutingSnapshot)
	// that table state has changed since the last snapshot was taken.
	// Table-level wrappers set it to true when the routeStore
	// operation they invoke reports `mutated=true` (no-op idempotent
	// paths leave it untouched). The publisher consumes the flag via
	// ConsumeDirty in a CAS true→false; between the consume and the
	// subsequent Snapshot read a new writer may set it again, in
	// which case the next refresh tick observes dirty=true and
	// rebuilds — this matches the bounded-staleness contract of every
	// other hot snapshot in node.Service.
	//
	// Lives outside t.mu because (a) atomic operations do not need
	// the mutex, (b) callers that already hold t.mu.Lock for a writer
	// set it inline without a second synchronization point, and (c)
	// the consumer (publisher) reads it lock-free from the hot-reads
	// refresher.
	dirty atomic.Bool

	// snapDirtyIDs accumulates the identities whose route buckets changed
	// since the last SnapshotIncremental consumed them. Marked under t.mu
	// by every single-identity route-mutating wrapper; consumed and cleared
	// inside SnapshotIncremental. It refines the coarse `dirty atomic.Bool`
	// (which only answers "did anything change?") into the per-identity
	// granularity the copy-on-write projection needs to reuse the unchanged
	// buckets of the previous snapshot. Guarded by t.mu (not atomic): it is
	// only ever touched inside a writer critical section or by the single
	// snapshot publisher.
	snapDirtyIDs map[PeerIdentity]struct{}

	// snapFullDirty forces the next SnapshotIncremental to re-copy every
	// bucket regardless of snapDirtyIDs. Set by the bulk wrappers
	// (RemoveDirectPeer, InvalidateTransitRoutes, TickTTL) that touch an
	// unbounded identity set in one shot, where enumerating the affected
	// identities for snapDirtyIDs would cost more than a full rebuild.
	// Guarded by t.mu.
	snapFullDirty bool

	// snapRawCache is the raw projection (pre self-route injection) returned
	// by the previous SnapshotIncremental, reused for clean identities. nil
	// until the first snapshot. Guarded by t.mu; safe to cache without extra
	// synchronization because the sole caller (Service.rebuildRoutingSnapshot)
	// is single-threaded. Holds RouteEntry slices shared (read-only) with
	// every published snapshot generation that reused them — copy-on-write,
	// never mutated in place.
	snapRawCache map[PeerIdentity][]RouteEntry

	// changeLog is the announce delta-cursor change journal. Every route/health
	// mutation that changes the WIRE projection appends the affected identity here
	// (markRouteChangedLocked / markRouteChangedFullLocked); a per-peer cursor
	// reads the identities changed since it last synced (AnnounceDeltaTo) instead
	// of rebuilding the whole projection and diffing it.
	//
	// AUTHORITATIVE: the journal drives what the announce delta sends. Mutations
	// that do NOT change the wire projection (TTL-only ExpiresAt refresh, cap
	// rejections) deliberately do not append, so cursor deltas do not fan out
	// unchanged routes. See docs/refactoring/phase3-announce-delta-cursor.md.
	// Guarded by t.mu, same as snapDirtyIDs.
	changeLog *routeChangeLog

	// health owns the RouteHealthState map keyed per (Identity,
	// Uplink) — the Phase 2 quality-signal layer introduced in
	// docs/protocol/route_health.md. healthStore has no
	// mutex of its own; t.mu protects it in the same way it protects
	// store and flap, so a Lookup-path can read uplinks, RTT, and
	// health under a single RLock without cross-mutex coordination.
	//
	// PR 11.1 introduces the field and the leaf data structure;
	// integration with Lookup (composite-score ranking) and with the
	// relay_hop_ack handler (MarkHopAck) lands in PR 11.2. Probe and
	// query wire frames + ack handling land in PR 11.3 / 11.4.
	health *healthStore

	// sessionDigestCache is the Phase 3 PR 12.5 incremental-sync
	// digest cache — per-peer snapshots stashed at session-close
	// time so a reconnect within SessionDigestCacheTTL can emit
	// the stored digest without re-walking the table. Lives here
	// (rather than as a separate structure in node.Service) so
	// the digest content and its source-of-truth (the storage
	// buckets) share a single mutex — see Phase 3 plan §2.6 for
	// the placement decision.
	//
	// The cache is NOT persisted across restart: process wipe =
	// cold-start digest re-computation on the first reconnect,
	// which is consistent with reputation persistence policy
	// (Phase 3 §4.9 decision #6). nil until first
	// RecordPeerDigestSnapshot to keep zero-allocation idle
	// nodes cheap.
	sessionDigestCache map[PeerIdentity]sessionDigestEntry

	// epoch is the local route_announce_v3 epoch counter (Phase 4,
	// overview §7.1). It identifies this Table's generation: it ships on
	// every route_announce_v3 frame so a peer can distinguish a stale
	// replay from a fresh table that has reset since the previous frame.
	// The receiver's per-peer watermark advances on increase
	// (V3EpochReset) and rejects strictly-lower frames (V3EpochStale) —
	// see AnnouncePeerState.ObserveV3Epoch.
	//
	// Immutable after construction in this PR. Default 1 (WithEpoch
	// overrides for deterministic tests). A future Phase 4 follow-up
	// adds a Table.Reset / in-process epoch bump so a wiped-in-place
	// table can signal the discontinuity without a process restart —
	// today the implicit reset is the process restart itself, which
	// also closes every session and resets the receive watermark via
	// MarkReconnected, so the on-wire epoch value matters only within
	// a session.
	epoch uint64

	// probeBackoff delays the Good→Questionable transition (TickHealth →
	// applyIdleTick) for proven-stable pairs by stableQuestionableExtension
	// (60s→90s), so they are actively probed less often; Bad/Dead are
	// unchanged. Off restores the flat 60s timeline. Set via WithProbeBackoff.
	probeBackoff bool
}

// TableOption configures optional Table parameters.
type TableOption func(*Table)

// WithClock overrides the default time source, useful for deterministic tests.
func WithClock(clock func() time.Time) TableOption {
	return func(t *Table) {
		t.clock = clock
	}
}

// WithProbeBackoff enables the opt-in probe back-off (CORSA_PROBE_BACKOFF):
// proven-stable pairs delay Good→Questionable by stableQuestionableExtension,
// so they are probed less often (Bad/Dead unchanged). Default false keeps the
// flat timeline.
func WithProbeBackoff(enabled bool) TableOption {
	return func(t *Table) {
		t.probeBackoff = enabled
	}
}

// WithEpoch overrides the local route_announce_v3 epoch counter
// (Phase 4, overview §7.1). The value is opaque to the routing core —
// only the v3 send/receive path inspects it. Tests use this to pin a
// deterministic epoch; production leaves the default of 1.
func WithEpoch(epoch uint64) TableOption {
	return func(t *Table) {
		t.epoch = epoch
	}
}

// WithDefaultTTL overrides the default route TTL. The value lives
// on routeStore — every learned entry whose caller did not set
// ExpiresAt picks it up at ApplyUpdate time, and tombstones created
// by WithdrawTriple / InvalidateAllVia / InvalidateTransitVia use
// it for the resurrection-guard window.
func WithDefaultTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.store.defaultTTL = d
	}
}

// WithLocalOrigin sets this node's identity. Required for AddDirectPeer
// and RemoveDirectPeer. The localOrigin is used as the Origin field for
// direct routes and scopes the monotonic SeqNo counter.
//
// Mirrored into both Table.localOrigin (used for pre-mu validation
// and the synthetic self-route injection) and routeStore.localOrigin
// (used by AdmitDirectPeer, syncSeqCounterLocked, InvalidateAllVia,
// AnnounceProjectionFor). Both fields are immutable after construction
// so the duplication cannot drift.
func WithLocalOrigin(identity PeerIdentity) TableOption {
	return func(t *Table) {
		t.localOrigin = identity
		t.store.localOrigin = identity
	}
}

// WithFlapWindow overrides the time window for counting disconnect events.
func WithFlapWindow(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.window = d
	}
}

// WithFlapThreshold overrides the number of disconnects within flapWindow
// that triggers hold-down.
func WithFlapThreshold(n int) TableOption {
	return func(t *Table) {
		t.flap.threshold = n
	}
}

// WithHoldDownDuration overrides how long a peer stays in hold-down after
// flap detection triggers.
func WithHoldDownDuration(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.holdDown = d
	}
}

// WithPenalizedTTL overrides the shortened TTL applied to routes created
// during hold-down.
func WithPenalizedTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.penalizedTTL = d
	}
}

// WithMaxSeqAdvancePerWindow overrides the SeqNo flap-cap threshold
// (Phase 1 P2). When a single Identity's outbound-SeqNo advance count
// inside SeqAdvanceWindow exceeds this value, AnnounceProjectionFor
// suppresses wire emit for that Identity for DefaultSeqHoldDownDuration.
// Default DefaultMaxSeqAdvancePerWindow (10). Zero (or negative)
// disables the cap entirely — recordSeqAdvanceLocked / isInSeqHold
// DownLocked short-circuit.
func WithMaxSeqAdvancePerWindow(n int) TableOption {
	return func(t *Table) {
		t.flap.maxSeqAdvancePerWindow = n
	}
}

// WithSeqAdvanceWindow overrides the sliding-window length for the
// SeqNo flap-cap detector (Phase 1 P2). Outbound-SeqNo advances older
// than `now - window` are trimmed. Default DefaultSeqAdvanceWindow
// (5 min). Test fixtures use a tighter window (seconds) to keep
// regression suites fast.
func WithSeqAdvanceWindow(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.seqAdvanceWindow = d
	}
}

// WithSeqHoldDownDuration overrides the per-Identity SeqNo flap-cap
// hold-down duration (Phase 1 P2). Production code keeps the derived
// default (DefaultSeqHoldDownDuration = DefaultTTL/2) — the
// refresh-interval invariant ForcedFullSyncMultiplier *
// DefaultAnnounceInterval <= DefaultTTL/2 relies on the hold-down
// being at least one TTL-half so a single missed forced full sync
// during suppression cannot strand the route past TTL.
//
// Exposed primarily for test fixtures that exercise the engage /
// release transitions without burning real wall-clock seconds.
// Operator-facing config does NOT include a corresponding env var.
func WithSeqHoldDownDuration(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.seqHoldDown = d
	}
}

// WithMaxSaneHops overrides the Phase 1 P3 fast-invalidation
// threshold. Ingest with `Hops > MaxSaneHops` is recorded as a
// tombstone at the observed SeqNo and dropped from Lookup
// immediately, sparing the next route-TTL window (DefaultTTL, 600s)
// the cost of steering traffic onto a count-to-infinity uplink. Default
// MaxSaneHops (8); env var CORSA_MAX_SANE_HOPS lets operators on
// deep meshes raise it. Zero (or negative) disables the
// invalidation path (every ingest with `Hops < HopsInfinity`
// reaches the standard ApplyUpdate branches unchanged).
func WithMaxSaneHops(n int) TableOption {
	return func(t *Table) {
		t.store.maxSaneHops = n
	}
}

// WithMaxBadHopsPerWindow overrides the bad-hops hysteresis budget:
// more than n ACCEPTED fast-invalidations for one Identity within
// the bad-hops window arm hold-down. Default
// DefaultMaxBadHopsPerWindow (3). Zero (or negative) disables the
// hysteresis while keeping the per-event P3 guard intact — the knob
// per-event fast-invalidation tests use to opt out. Test-facing; no
// env var.
func WithMaxBadHopsPerWindow(n int) TableOption {
	return func(t *Table) {
		t.flap.maxBadHopsPerWindow = n
	}
}

// WithBadHopsWindow overrides the bad-hops hysteresis sliding-window
// length. Default DefaultBadHopsWindow (1 min). Test-facing; no env
// var.
func WithBadHopsWindow(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.badHopsWindow = d
	}
}

// WithBadHopsHoldDown overrides the FIRST-strike bad-hops hold-down
// duration (escalation still doubles per strike up to
// DefaultBadHopsHoldDownMax). Default DefaultBadHopsHoldDownBase
// (5 min). Test-facing; no env var.
func WithBadHopsHoldDown(d time.Duration) TableOption {
	return func(t *Table) {
		t.flap.badHopsHoldDownBase = d
	}
}

// WithMaxNextHopsPerOrigin caps the number of LIVE (non-withdrawn)
// UplinkClaim rows the table will keep per Identity bucket. The
// knob name preserves the pre-Phase-A "...PerOrigin" suffix for
// operator-facing stability (env var, config field, dashboards
// keyed on the metric label) — semantically the cap bounds
// per-(Identity, Uplink) live claims today, since Phase 1 dropped
// Origin from the dedup key (see uplink_claim.go for the storage
// shape rationale).
//
// Withdrawal tombstones (Hops >= HopsInfinity) bypass the cap
// entirely — they are appended outside the K-counted slots and
// reclaimed by TickTTL on defaultTTL, so the slice can transiently
// exceed K when recent withdrawals contributed tombstones. A
// positive cap activates the admission policy in UpdateRoute —
// see AdmitNew for the eviction rules.
//
// Zero (or negative) disables the cap entirely. The bare Table
// constructor defaults to 0 so unit tests and any caller that wires a
// Table directly observe the pre-cap behaviour deterministically;
// production Services constructed via config.Default() activate the
// cap at DefaultMaxNextHopsPerOrigin (4) — see that constant's
// docstring for the two-layer default story and the rollout history.
//
// Recommended ceiling for production deployments is
// DefaultMaxNextHopsPerOrigin (4); see the docstring on that constant
// for the trade-off rationale. Tests that exercise the cap normally
// inject a small value (often 2) to make eviction scenarios easy to
// construct.
func WithMaxNextHopsPerOrigin(n int) TableOption {
	return func(t *Table) {
		t.store.maxNextHopsPerOrigin = n
	}
}

// NewTable creates an empty routing table with the given options.
//
// The routeStore and FlapDetector are instantiated BEFORE options
// run so that storage-related (WithDefaultTTL, WithLocalOrigin,
// WithMaxNextHopsPerOrigin) and flap-related (WithFlapWindow,
// WithFlapThreshold, WithHoldDownDuration, WithPenalizedTTL)
// options can mutate their fields directly without a second
// initialization pass.
func NewTable(opts ...TableOption) *Table {
	t := &Table{
		store:  newRouteStore(),
		clock:  time.Now,
		flap:   newFlapDetector(),
		health: newHealthStore(),
		// Announce delta-cursor change journal. The ring must remember at least
		// one forced-full-sync window's worth of changes so a peer synced at the
		// previous forced-full can still be served incrementally rather than
		// tripping the overflow force-full; see defaultRouteChangeLogCapacity.
		changeLog: newRouteChangeLog(defaultRouteChangeLogCapacity),
		// Phase 4 v3 epoch default. WithEpoch overrides for tests. See
		// the Table.epoch field doc for the immutability rationale.
		epoch: 1,
	}
	// Link the FlapDetector into routeStore so Phase 1 P2 (SeqNo
	// flap cap) helpers nextOutboundSeqLockedPerPeer /
	// nextOutboundSeqLockedBroadcast can record per-Identity
	// advances and AnnounceProjectionFor can consult hold-down
	// state — all under the same t.mu writer lock the existing
	// FlapDetector *Locked contract already requires. Wired BEFORE
	// options run so any future `With*` option closure that mutates
	// store.flap.* (none today) sees the same pointer.
	t.store.flap = t.flap
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Size returns the total number of route entries (including withdrawn).
func (t *Table) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.store.Total()
}

// ActiveSize returns the number of non-withdrawn, non-expired entries.
func (t *Table) ActiveSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.store.CountActive(t.clock())
}

// LocalOrigin returns this node's identity string.
func (t *Table) LocalOrigin() PeerIdentity {
	return t.localOrigin
}

// Epoch returns the local route_announce_v3 epoch counter (Phase 4,
// overview §7.1). Immutable after construction; see Table.epoch for the
// future-restart semantics.
func (t *Table) Epoch() uint64 {
	return t.epoch
}

// CapStats returns a value-copy of the MaxNextHopsPerOrigin admission
// counters. The actual atomic counters live on routeStore — this is
// a thin wrapper that callers (RPC, tests, monitoring) use without
// reaching into the storage layer.
//
// Lock-free read of atomic counters; safe to call without t.mu.
func (t *Table) CapStats() RouteCapStats {
	return t.store.CapStats()
}

// localRouteEntry returns a synthetic route entry representing the node
// itself. Hops=0 means local delivery with no network traversal. The
// entry never expires and has the highest trust rank (RouteSourceLocal).
// Returns zero-value RouteEntry if localOrigin is not configured.
func (t *Table) localRouteEntry() RouteEntry {
	if t.localOrigin.IsZero() {
		return RouteEntry{}
	}
	return RouteEntry{
		Identity: t.localOrigin,
		Origin:   t.localOrigin,
		NextHop:  t.localOrigin,
		Hops:     0,
		SeqNo:    0,
		Source:   RouteSourceLocal,
	}
}

// ConsumeDirty atomically clears the dirty flag and returns whether the
// table had been mutated since the last consume. Intended for the
// snapshot publisher: a true result means the publisher must rebuild
// the cached snapshot, a false result means the previously published
// snapshot is still authoritative.
//
// Race window with concurrent writers is intentional and harmless —
// see the dirty field comment in Table for the contract. This method
// performs no other synchronisation and may be called concurrently
// with table mutations.
func (t *Table) ConsumeDirty() bool {
	return t.dirty.CompareAndSwap(true, false)
}

// IsDirty returns whether the table has been mutated since the last
// ConsumeDirty call. Intended for diagnostics and tests; production
// publishers should use ConsumeDirty (atomic CAS) to avoid the
// load-then-consume race that IsDirty introduces.
func (t *Table) IsDirty() bool {
	return t.dirty.Load()
}

// markSnapDirtyLocked records a single identity whose route bucket
// changed, so the next SnapshotIncremental re-copies it instead of
// reusing the cached projection. Caller must hold t.mu in W mode (every
// call site already holds it for the storage mutation it accompanies).
//
// It also feeds the Phase 3 change journal (markRouteChangedLocked): every
// per-identity route mutation that needs a snapshot re-copy is exactly a
// change a per-peer announce cursor must learn about.
func (t *Table) markSnapDirtyLocked(identity PeerIdentity) {
	t.markSnapDirtyNoJournalLocked(identity)
	t.markRouteChangedLocked(identity)
}

// markSnapDirtyNoJournalLocked marks the published Snapshot dirty for `identity`
// WITHOUT journaling a change. Used where a mutation touches a Snapshot field
// that is NOT on the announce wire (e.g. ExpiresAt on a same-SeqNo/same-hops TTL
// refresh): the routing.Snapshot must re-copy the bucket, but the AnnounceEntry
// is byte-identical, so journaling would make the cursor-mode delta re-emit an
// unchanged route and fan it out across the mesh on every refresh. The caller
// journals separately only when the wire projection actually changed. Caller
// must hold t.mu in W mode.
func (t *Table) markSnapDirtyNoJournalLocked(identity PeerIdentity) {
	if t.snapDirtyIDs == nil {
		t.snapDirtyIDs = make(map[PeerIdentity]struct{})
	}
	t.snapDirtyIDs[identity] = struct{}{}
}

// markSnapFullDirtyLocked forces the next SnapshotIncremental to re-copy every
// bucket AND records a journal bulk reset so per-peer cursors below it force a
// full sync. It is the FALLBACK for a bulk mutation that touches an UNBOUNDED,
// unknown identity set and so cannot journal precisely. No production path needs
// it today — RemoveDirectPeer, InvalidateTransitRoutes and TickTTL all know their
// affected set and use markSnapshotFullDirtyLocked + per-identity journaling — so
// this is reserved for a future mutation that genuinely cannot enumerate its set
// (and is exercised by TestAnnounceDeltaTo_NeedFullOnBulkReset). Caller holds
// t.mu in W mode.
func (t *Table) markSnapFullDirtyLocked() {
	t.snapFullDirty = true
	t.markRouteChangedFullLocked()
}

// markSnapshotFullDirtyLocked forces the next SnapshotIncremental to re-copy
// every bucket WITHOUT recording a change-journal bulk reset. Used by bulk
// mutations that DO know their affected identity set: the published snapshot
// still needs a full re-copy (buckets removed/shrunk), but the caller journals
// each affected identity precisely (markRouteChangedLocked) so cursor-mode peers
// receive a targeted delta instead of being forced — and then rate-limited —
// into a full sync. Caller must hold t.mu in W mode.
func (t *Table) markSnapshotFullDirtyLocked() {
	t.snapFullDirty = true
}

// markRouteChangedLocked appends a per-identity change to the Phase 3 change
// journal. Called from markSnapDirtyLocked (route mutations) and directly from
// the health/flap mutation sites that set the coarse dirty flag without a
// snapshot re-copy (their per-identity change still alters the announce
// projection). Caller must hold t.mu in W mode. nil-safe for tables built
// without a journal (defensive — production always has one).
func (t *Table) markRouteChangedLocked(identity PeerIdentity) {
	if t.changeLog == nil {
		return
	}
	t.changeLog.recordLocked(identity)
}

// markRouteChangedFullLocked records a bulk reset in the Phase 3 change
// journal. Caller must hold t.mu in W mode.
func (t *Table) markRouteChangedFullLocked() {
	if t.changeLog == nil {
		return
	}
	t.changeLog.recordFullLocked()
}
