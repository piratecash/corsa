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
	// DefaultTTL is the default route lifetime.
	DefaultTTL = 120 * time.Second
)

// Table is the local distance-vector routing table. It stores routes keyed
// by destination identity, with deduplication based on the (Identity, Origin,
// NextHop) triple.
//
// localOrigin is this node's identity (Ed25519 fingerprint). It is used as
// the Origin field for direct routes and as the scope for the monotonic
// SeqNo counter. Only routes where Origin == localOrigin may have their
// SeqNo advanced by this table — this enforces the per-origin SeqNo
// invariant at the data structure level.
//
// All public methods are safe for concurrent use.
type Table struct {
	mu     sync.RWMutex
	routes map[PeerIdentity][]RouteEntry // identity -> routes

	// localOrigin is this node's Ed25519 fingerprint. Used as Origin for
	// direct routes created by AddDirectPeer. Required for AddDirectPeer
	// and RemoveDirectPeer operations.
	localOrigin PeerIdentity

	// seqCounters tracks the next SeqNo to use for own-origin routes,
	// keyed by destination identity. Only the owner of localOrigin may
	// increment these counters.
	seqCounters map[PeerIdentity]uint64

	defaultTTL time.Duration

	// clock is used for time-dependent operations, allowing tests to inject
	// a controllable clock.
	clock func() time.Time

	// flapState tracks per-peer disconnect frequency to detect link
	// flapping. When a peer exceeds flapThreshold disconnects within
	// flapWindow, subsequent reconnections apply penalizedTTL instead
	// of defaultTTL.
	flapState map[PeerIdentity]*peerFlapState

	// Flap detection tuning. Set via options; defaults applied in NewTable.
	flapWindow       time.Duration
	flapThreshold    int
	holdDownDuration time.Duration
	penalizedTTL     time.Duration

	// maxNextHopsPerOrigin caps the number of LIVE (non-withdrawn)
	// routes the table keeps for each (Identity, Origin) pair.
	// Withdrawal tombstones (Hops >= HopsInfinity) are excluded from
	// the cap counter and from the eviction-search — they live
	// alongside the K live slots as SeqNo resurrection guards and are
	// reclaimed by TickTTL after defaultTTL, so the slice may
	// transiently hold up to K live rows plus one or more tombstones.
	// Zero (the runtime default) disables the cap — every accepted
	// entry is stored, and the table grows unbounded with the number
	// of next-hops that have learned the same (destination, origin).
	// A positive value triggers an admission policy in UpdateRoute
	// that may evict the worst evictable LIVE entry (live > expired
	// → lowest trust → highest hops → earliest expiry) when the live
	// bucket is full and the incoming entry is strictly better.
	// Direct and local entries are never evicted — see admitNewLocked.
	maxNextHopsPerOrigin int

	// capStats holds monotonic counters for the cap admission policy.
	// Fields are atomic so the publisher can read them without holding
	// t.mu — RouteCapStats is published into routing.Snapshot via a
	// snapshot read in Snapshot(), and the per-decision increments in
	// admitNewLocked happen under the writer lock anyway, so the
	// atomic reads cannot tear against an in-flight write.
	capStats struct {
		accepted             atomic.Uint64
		acceptedReplaced     atomic.Uint64
		rejectedFull         atomic.Uint64
		rejectedAllProtected atomic.Uint64
	}

	// dirty signals to an external snapshot publisher (Service.rebuildRoutingSnapshot)
	// that table state has changed since the last snapshot was taken.
	// Writers set it to true after a real mutation (no-op idempotent
	// paths leave it untouched). The publisher consumes the flag via
	// ConsumeDirty in a CAS true→false; between the consume and the
	// subsequent Snapshot read a new writer may set it again, in which
	// case the next refresh tick observes dirty=true and rebuilds — this
	// matches the bounded-staleness contract of every other hot snapshot
	// in node.Service.
	//
	// Lives outside t.mu because (a) atomic operations do not need the
	// mutex, (b) callers that already hold t.mu.Lock for a writer set it
	// inline without a second synchronization point, and (c) the consumer
	// (publisher) reads it lock-free from the hot-reads refresher.
	dirty atomic.Bool
}

// TableOption configures optional Table parameters.
type TableOption func(*Table)

// WithClock overrides the default time source, useful for deterministic tests.
func WithClock(clock func() time.Time) TableOption {
	return func(t *Table) {
		t.clock = clock
	}
}

// WithDefaultTTL overrides the default route TTL.
func WithDefaultTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.defaultTTL = d
	}
}

// WithLocalOrigin sets this node's identity. Required for AddDirectPeer
// and RemoveDirectPeer. The localOrigin is used as the Origin field for
// direct routes and scopes the monotonic SeqNo counter.
func WithLocalOrigin(identity PeerIdentity) TableOption {
	return func(t *Table) {
		t.localOrigin = identity
	}
}

// WithFlapWindow overrides the time window for counting disconnect events.
func WithFlapWindow(d time.Duration) TableOption {
	return func(t *Table) {
		t.flapWindow = d
	}
}

// WithFlapThreshold overrides the number of disconnects within flapWindow
// that triggers hold-down.
func WithFlapThreshold(n int) TableOption {
	return func(t *Table) {
		t.flapThreshold = n
	}
}

// WithHoldDownDuration overrides how long a peer stays in hold-down after
// flap detection triggers.
func WithHoldDownDuration(d time.Duration) TableOption {
	return func(t *Table) {
		t.holdDownDuration = d
	}
}

// WithPenalizedTTL overrides the shortened TTL applied to routes created
// during hold-down.
func WithPenalizedTTL(d time.Duration) TableOption {
	return func(t *Table) {
		t.penalizedTTL = d
	}
}

// WithMaxNextHopsPerOrigin caps the number of LIVE (non-withdrawn)
// routes the table will keep for each (Identity, Origin) pair.
// Withdrawal tombstones (Hops >= HopsInfinity) bypass the cap entirely
// — they are appended outside the K-counted slots and reclaimed by
// TickTTL on defaultTTL, so the slice can transiently exceed K when
// recent withdrawals contributed tombstones. A positive cap activates
// the admission policy in UpdateRoute — see admitNewLocked for the
// eviction rules. Zero (or negative) disables the cap entirely; this
// is the runtime default for a freshly constructed Table so the cap
// code can ship in production without changing observable routing
// behaviour until operators opt in.
//
// Recommended ceiling for production deployments is
// DefaultMaxNextHopsPerOrigin (4); see the docstring on that constant
// for the trade-off rationale. Tests that exercise the cap normally
// inject a small value (often 2) to make eviction scenarios easy to
// construct.
func WithMaxNextHopsPerOrigin(n int) TableOption {
	return func(t *Table) {
		t.maxNextHopsPerOrigin = n
	}
}

// NewTable creates an empty routing table with the given options.
func NewTable(opts ...TableOption) *Table {
	t := &Table{
		routes:           make(map[PeerIdentity][]RouteEntry),
		seqCounters:      make(map[PeerIdentity]uint64),
		flapState:        make(map[PeerIdentity]*peerFlapState),
		defaultTTL:       DefaultTTL,
		clock:            time.Now,
		flapWindow:       DefaultFlapWindow,
		flapThreshold:    DefaultFlapThreshold,
		holdDownDuration: DefaultHoldDownDuration,
		penalizedTTL:     DefaultPenalizedTTL,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// UpdateRoute inserts or updates a route in the table. The dedup key is
// (Identity, Origin, NextHop). If an existing entry shares the same triple:
//
//   - If incoming SeqNo > existing SeqNo: replace unconditionally.
//   - If incoming SeqNo == existing SeqNo: replace only if the incoming
//     source has a higher trust rank or fewer hops.
//   - If incoming SeqNo < existing SeqNo: reject (stale announcement).
//
// Returns (RouteAccepted, nil) if the route was new or improved,
// (RouteUnchanged, nil) if an existing alive route was found but not
// improved, (RouteRejected, nil) if rejected by tombstone/SeqNo rules,
// or (RouteRejected, err) if the entry is malformed.
func (t *Table) UpdateRoute(entry RouteEntry) (RouteUpdateStatus, error) {
	if err := entry.Validate(); err != nil {
		return RouteRejected, err
	}

	// RouteSourceLocal is purely synthetic — it exists only in Lookup/Snapshot
	// results and must never be persisted in the table. Allowing it would let
	// any caller inject a zero-hop highest-trust route for an arbitrary
	// identity, bypassing all real routing.
	if entry.Source == RouteSourceLocal {
		return RouteRejected, ErrLocalSourceReserved
	}

	// Direct routes must originate from this node. A RouteSourceDirect entry
	// with a foreign Origin would outrank all announcement/hop_ack routes in
	// Lookup and never be eligible for own-origin withdrawal on disconnect.
	if entry.Source == RouteSourceDirect && t.localOrigin != "" && entry.Origin != t.localOrigin {
		return RouteRejected, ErrDirectForeignOrigin
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	// Direct routes are socket-bound, not TTL-managed (see docs/routing.md
	// "Direct route lifecycle"): ExpiresAt is intentionally zero so that
	// IsExpired never triggers and TickTTL never evicts an entry whose
	// underlying socket is still alive. Any UpdateRoute call that lands a
	// RouteSourceDirect entry — whether through state restore, defensive
	// re-application, or the explicit Direct-via-UpdateRoute test paths —
	// must therefore land with ExpiresAt=zero, regardless of what the
	// caller supplied. Forcing zero here closes the gap that the
	// "if ExpiresAt.IsZero { now+defaultTTL }" default leaves below for
	// learned routes; without this, a Direct entry reaches the table with
	// a finite TTL and gets silently evicted on the next TickTTL pass.
	// Validate() already enforces Hops=1 for RouteSourceDirect, so this
	// branch never sees a withdrawn-direct (Hops=HopsInfinity) tombstone —
	// withdrawal of direct routes goes through RemoveDirectPeer / WithdrawRoute,
	// which write ExpiresAt directly without going through UpdateRoute.
	if entry.Source == RouteSourceDirect {
		entry.ExpiresAt = time.Time{}
	} else if entry.ExpiresAt.IsZero() {
		entry.ExpiresAt = now.Add(t.defaultTTL)
	}

	existing := t.routes[entry.Identity]
	idx := findByTriple(existing, entry.DedupKey())

	if idx < 0 {
		// Tombstone short-circuit: a withdrawn row is a SeqNo
		// resurrection-guard marker, not a routing slot, and the cap
		// model puts it OUTSIDE the K-counted live entries (see
		// admitNewLocked rule 1 and WithdrawRoute idx<0). Routing it
		// through admitNewLocked would treat the tombstone as a "dead
		// candidate" that loses to any live row on the liveness
		// pre-tier and gets rejected on a saturated live bucket —
		// silently dropping the SeqNo guard for THIS triple even
		// though the cap model explicitly leaves room for it. The
		// public Table API allows UpdateRoute(Hops=HopsInfinity) for
		// callers that synthesise tombstones directly (it is also
		// covered by table_test.go's RouteEntry.Validate suite), so
		// we mirror WithdrawRoute idx<0 here: append the tombstone
		// unconditionally and let TickTTL reclaim it on its own
		// schedule.
		if entry.IsWithdrawn() {
			t.routes[entry.Identity] = append(existing, entry)
			t.syncSeqCounterLocked(entry)
			t.dirty.Store(true)
			return RouteAccepted, nil
		}

		// Live admission. admitNewLocked owns the actual append /
		// in-place replace; we only translate its decision into the
		// public RouteUpdateStatus and update side state on success.
		// Cap rejections (AdmissionRejectedFull,
		// AdmissionRejectedAllProtected) leave the table untouched and
		// surface as RouteRejected — same shape as the existing
		// stale-SeqNo / tombstone-protection rejections, so callers
		// that already treat RouteRejected as "drop and move on" need
		// no new handling. Operators distinguishing the two cases
		// observe the cap-admission metrics (see
		// docs/routing-rib-compaction-and-snapshot-refactor.md §6).
		decision := t.admitNewLocked(entry, now)
		switch decision {
		case AdmissionAccepted, AdmissionAcceptedReplaced:
			t.syncSeqCounterLocked(entry)
			t.dirty.Store(true)
			return RouteAccepted, nil
		case AdmissionRejectedFull, AdmissionRejectedAllProtected:
			// Counters in admitNewLocked were incremented even
			// though the routes map is unchanged. RouteCapStats
			// rides inside the published Snapshot via
			// rebuildRoutingSnapshot's dirty gate, so without the
			// flag the cached snapshot would keep stale counter
			// values until some unrelated mutation arrived — and
			// fetchRouteSummary would underreport rejected_full /
			// rejected_all_protected on a node that is doing
			// nothing BUT rejecting cap admissions. Mark dirty so
			// the publisher republishes with current counters.
			//
			// This is the only path in UpdateRoute where dirty
			// fires without a routes-map mutation; idempotent
			// stale-SeqNo / tombstone-protection / RouteUnchanged
			// branches deliberately leave dirty clean because they
			// touch nothing observable in the snapshot.
			t.dirty.Store(true)
			return RouteRejected, nil
		default:
			// Defence in depth: an unrecognised decision from a
			// future admitNewLocked extension still surfaces as
			// RouteRejected so the caller does not see a bogus
			// RouteAccepted, and dirty stays untouched so we do
			// not mask the missing case under a phantom rebuild.
			return RouteRejected, nil
		}
	}

	old := &existing[idx]

	if entry.SeqNo > old.SeqNo {
		// Tombstone → live promotion needs the cap admission policy
		// applied. Withdrawn rows are excluded from bucketCount (see
		// admitNewLocked) so the slot at idx — currently a tombstone —
		// does not count against the K-live cap. A bare in-place
		// overwrite would therefore push the bucket from up-to-K live
		// to up-to-K+1 live, breaking the "at most K live entries per
		// (Identity, Origin)" invariant.
		//
		// Route the promotion through the same admission policy used
		// for fresh idx<0 admissions. Temporarily detach the
		// tombstone slot via a slice delete, call admitNewLocked, and
		// either let it append/evict for the new live entry or
		// restore the tombstone on rejection so the SeqNo
		// resurrection guard for THIS specific triple stays in place
		// — dropping the tombstone here would expose the lineage to
		// further stale-SeqNo announcements.
		//
		// Slice order is not part of any external contract; rebuilding
		// the slice with the tombstone at the end on the rejection
		// path is fine. The append-after-delete pattern uses the same
		// underlying buffer when capacity allows, so it does not
		// allocate in the common case.
		if old.IsWithdrawn() && !entry.IsWithdrawn() && t.maxNextHopsPerOrigin > 0 {
			savedTombstone := existing[idx]
			t.routes[entry.Identity] = append(existing[:idx], existing[idx+1:]...)
			decision := t.admitNewLocked(entry, now)
			switch decision {
			case AdmissionAccepted, AdmissionAcceptedReplaced:
				t.syncSeqCounterLocked(entry)
				t.dirty.Store(true)
				return RouteAccepted, nil
			default:
				t.routes[entry.Identity] = append(t.routes[entry.Identity], savedTombstone)
				// admitNewLocked already bumped the rejection
				// counter; mark dirty so the snapshot republishes
				// with the updated counter and the rebuilt slice
				// (the tombstone may now sit at a different index).
				t.dirty.Store(true)
				return RouteRejected, nil
			}
		}

		existing[idx] = entry
		t.syncSeqCounterLocked(entry)
		t.dirty.Store(true)
		return RouteAccepted, nil
	}

	if entry.SeqNo == old.SeqNo {
		// A withdrawal tombstone (Hops >= HopsInfinity) must not be
		// replaced by a same-SeqNo update, even if the update has
		// higher trust or fewer hops. Only a strictly newer SeqNo
		// from the origin may supersede a withdrawal. Without this
		// guard, a delayed hop_ack (higher trust rank) could
		// resurrect a withdrawn lineage.
		if old.Hops >= HopsInfinity {
			return RouteRejected, nil
		}
		if entry.Source.TrustRank() > old.Source.TrustRank() {
			existing[idx] = entry
			t.syncSeqCounterLocked(entry)
			t.dirty.Store(true)
			return RouteAccepted, nil
		}
		if entry.Source == old.Source && entry.Hops < old.Hops {
			existing[idx] = entry
			t.syncSeqCounterLocked(entry)
			t.dirty.Store(true)
			return RouteAccepted, nil
		}
		// Same SeqNo, same or worse trust/hops. The existing route is
		// alive and unchanged — this is a reconfirmation, not a rejection.
		// Refresh ExpiresAt only on a TRUE reconfirmation (same source AND
		// same hops): a re-application of the identical lineage on the
		// wire is the origin's signal that the route is still valid,
		// which is what AnnounceLoop's forced full sync delivers between
		// delta cycles. Without this the learned copy on a neighbor ages
		// out at its original deadline even though the origin keeps
		// confirming it, leaving a dead window from when ExpiresAt elapses
		// until the next sync that actually changes a field. See
		// docs/routing.md "Refresh interval invariant" for the full
		// contract; the cadence half is enforced by
		// ForcedFullSyncMultiplier*DefaultAnnounceInterval <= DefaultTTL/2.
		//
		// A weaker incoming entry — worse hops or lower trust on the same
		// SeqNo — must NOT extend the old entry's lifetime. Doing so would
		// freeze an optimistic earlier hops=N entry indefinitely and
		// prevent natural TTL-driven reconvergence onto the actually-current
		// hops=N+1 path when the origin does not bump SeqNo (e.g. a
		// transit's local path to the origin lengthened without an origin
		// SeqNo update).
		//
		// Direct routes are also exempt: they use ExpiresAt=zero by design
		// (lifecycle is tied to AddDirectPeer/RemoveDirectPeer, see
		// docs/routing.md "Direct route lifecycle"), and IsExpired returns
		// false for a zero ExpiresAt — so an alive direct entry would
		// reach this branch alongside learned routes. Overwriting zero
		// with now+defaultTTL here would convert a never-expiring direct
		// route into a 120s-ageing one and let TickTTL evict it while the
		// underlying socket is still connected.
		if !old.IsExpired(now) {
			isExactReconfirmation := entry.Source == old.Source && entry.Hops == old.Hops
			if isExactReconfirmation && old.Source != RouteSourceDirect {
				existing[idx].ExpiresAt = now.Add(t.defaultTTL)
				// ExpiresAt is part of the published snapshot (drives
				// IsExpired and the wire ttl_seconds field), so a TTL
				// refresh must trigger a republish — otherwise the
				// snapshot would prematurely report the route as
				// expired up to one full announce cycle ahead of the
				// actual lifetime.
				t.dirty.Store(true)
			}
			return RouteUnchanged, nil
		}

		// Expired entry with same SeqNo: the route died because TickTTL
		// has not yet cleaned it up (runs every 10s). A same-SeqNo
		// re-announcement from the origin is a valid refresh — the
		// origin has not changed the route, it is simply re-confirming
		// it. Without this, an expired-but-not-yet-cleaned entry blocks
		// same-SeqNo refreshes until TickTTL removes the stale entry,
		// creating a non-deterministic window where routes are lost
		// despite the origin still advertising them.
		existing[idx] = entry
		t.syncSeqCounterLocked(entry)
		t.dirty.Store(true)
		return RouteAccepted, nil
	}

	// Stale SeqNo (entry.SeqNo < old.SeqNo).
	return RouteRejected, nil
}

// admitNewLocked decides whether a freshly arrived RouteEntry — whose
// (Identity, Origin, NextHop) triple does not yet exist in the table —
// can be inserted under the MaxNextHopsPerOrigin cap. The function
// performs the insertion (or eviction-then-insertion) itself when the
// admission policy permits and returns the resulting decision; the
// caller must NOT touch t.routes[entry.Identity] in either branch.
//
// Admission rules when t.maxNextHopsPerOrigin > 0 (cap active):
//
//  1. Withdrawn rows (Hops >= HopsInfinity) are EXCLUDED from the
//     cap counter and from the eviction-search entirely. They are
//     metadata markers (the SeqNo resurrection guard for a specific
//     (Identity, Origin, NextHop) triple) and live alongside the
//     K-live slots until TickTTL reclaims them; they neither
//     compete with live entries for cap slots nor get displaced
//     to make room for live admissions. See WithdrawRoute idx<0
//     and the convergence section in docs/routing.md.
//  2. If the (Identity, Origin) bucket has fewer than
//     maxNextHopsPerOrigin LIVE (non-withdrawn) entries, append
//     unconditionally → AdmissionAccepted.
//  3. Otherwise pick the worst evictable existing entry, considering
//     only live (non-withdrawn) entries. The eviction key is
//     (live > dead) → lowest TrustRank → highest Hops → earliest
//     ExpiresAt, evaluated in this order. Liveness as the FIRST
//     tier matters for expired-but-not-withdrawn rows: a live row
//     whose ExpiresAt is in the past (Hops < HopsInfinity, IsExpired
//     true) is "worse" than any fully-live row regardless of trust —
//     this prevents a bucket of K stale hop_acks from blocking a
//     fresh announcement. Withdrawn rows are NOT in this comparison
//     (see rule 1). Direct (RouteSourceDirect) and local
//     (RouteSourceLocal) entries are NEVER evictable: a direct route
//     represents a live socket and can only retire through
//     RemoveDirectPeer; a local route is the synthetic self-route
//     which is never persisted via UpdateRoute anyway, but the
//     filter is kept symmetric.
//  4. If every live entry in the bucket is direct/local, the cap
//     cannot evict anyone → AdmissionRejectedAllProtected, drop the
//     new entry.
//  5. Otherwise compare the incoming entry against the worst
//     evictable candidate using the same key (live > dead →
//     TrustRank → Hops → ExpiresAt). If the incoming entry is
//     strictly better, replace the worst entry's slot in place →
//     AdmissionAcceptedReplaced. Otherwise drop →
//     AdmissionRejectedFull.
//
// When the cap is disabled (maxNextHopsPerOrigin <= 0) the function
// always appends and returns AdmissionAccepted, which is what makes the
// cap rollout backward-compatible: a Table constructed without
// WithMaxNextHopsPerOrigin behaves exactly as it did before the cap was
// added.
//
// Side effects: on Accepted / AcceptedReplaced the function writes
// t.routes[entry.Identity] (append or in-place overwrite). It does NOT
// touch the dirty flag or the SeqNo counter — UpdateRoute does that
// itself once the decision returns. The function must be called with
// t.mu held.
//
// `now` is the wall-clock used for liveness comparisons. Withdrawn
// rows are excluded from bucketCount and from the eviction-search
// entirely (rule 1) and never reach isWorseForEvictionLocked; the
// liveness pre-tier inside that helper distinguishes only fully-live
// rows from expired-but-not-withdrawn rows, where the latter lose to
// any live candidate regardless of trust. Pass the same `now` value
// UpdateRoute uses to fill ExpiresAt, so the admission decision and
// the new entry's TTL agree on the same clock reading.
func (t *Table) admitNewLocked(entry RouteEntry, now time.Time) RouteAdmissionDecision {
	routes := t.routes[entry.Identity]

	if t.maxNextHopsPerOrigin <= 0 {
		// Cap disabled: don't touch the counters. Operators of an
		// uncapped node observe RouteCapStats stuck at zero, which
		// is the correct signal — there is no admission decision to
		// account for.
		t.routes[entry.Identity] = append(routes, entry)
		return AdmissionAccepted
	}

	// Count NON-TOMBSTONE entries that share the (Identity, Origin)
	// bucket. Withdrawn rows (Hops >= HopsInfinity) are intentionally
	// excluded from the cap counter so withdrawal tombstones live
	// "outside" the K-counted slots: their job is the SeqNo-based
	// resurrection guard for a specific (Identity, Origin, NextHop)
	// triple — a delayed lower-SeqNo announcement for the same triple
	// hits the idx>=0 path in UpdateRoute and is rejected by the
	// existing tombstone-vs-stale-SeqNo check. If tombstones competed
	// with live routes for the K slots, a bucket already at the cap
	// would either drop the tombstone (losing the SeqNo guard, so a
	// stale announcement could resurrect the lineage through cap
	// admission alone) or evict a live route to make room (losing
	// real routing capacity for a metadata-only marker). Excluding
	// tombstones from the counter removes the trade-off: the cap
	// keeps "at most K live entries per (Identity, Origin)" intact,
	// and tombstones are bound only by their TTL (TickTTL reclaims
	// them after defaultTTL).
	//
	// Expired-but-not-withdrawn rows (Hops < HopsInfinity, ExpiresAt
	// in the past) ARE counted: they are not metadata markers, they
	// are live-shaped entries that the table simply has not cleaned
	// up yet, and the liveness pre-tier in isWorseForEvictionLocked
	// makes them the first eviction target when a new live row
	// arrives. See TestRIBCap_ExpiredAndWithdrawnEvictedBeforeLiveByTrust.
	bucketCount := 0
	for i := range routes {
		if routes[i].Origin != entry.Origin {
			continue
		}
		if routes[i].IsWithdrawn() {
			continue
		}
		bucketCount++
	}
	if bucketCount < t.maxNextHopsPerOrigin {
		t.routes[entry.Identity] = append(routes, entry)
		t.capStats.accepted.Add(1)
		return AdmissionAccepted
	}

	// Bucket saturated by non-tombstone entries. Find the worst
	// evictable candidate. Tombstones are skipped here too — they
	// are "free" and never displaced to make room for a live
	// admission, mirroring the bucket-count rule above. Evicting a
	// tombstone for a live row would silently drop the SeqNo
	// resurrection guard for that (Identity, Origin, NextHop) triple.
	//
	// Liveness pre-tier still applies to non-tombstone entries: an
	// expired-but-not-withdrawn row loses to any fully-live row,
	// regardless of trust. See isWorseForEvictionLocked.
	worstIdx := -1
	for i := range routes {
		if routes[i].Origin != entry.Origin {
			continue
		}
		if routes[i].IsWithdrawn() {
			continue
		}
		if !isEvictable(&routes[i]) {
			continue
		}
		if worstIdx < 0 || isWorseForEvictionLocked(&routes[i], &routes[worstIdx], now) {
			worstIdx = i
		}
	}
	if worstIdx < 0 {
		// Every bucket entry is direct/local — protected from cap
		// eviction. The incoming entry is dropped without changing
		// the table.
		t.capStats.rejectedAllProtected.Add(1)
		return AdmissionRejectedAllProtected
	}

	// Apply the cap floor: the incoming entry must be strictly better
	// than the worst evictable candidate, or it gets dropped. "Strictly
	// better" means the same key as the eviction order, just inverted:
	// live beats dead first, then higher trust, then strictly fewer
	// hops, then strictly later expiry.
	if !isStrictlyBetterForCapLocked(&entry, &routes[worstIdx], now) {
		t.capStats.rejectedFull.Add(1)
		return AdmissionRejectedFull
	}

	routes[worstIdx] = entry
	t.routes[entry.Identity] = routes
	t.capStats.acceptedReplaced.Add(1)
	return AdmissionAcceptedReplaced
}

// admitDirectLocked admits a freshly-arrived direct (or, by symmetry,
// local) RouteEntry into the (Identity, Origin) bucket and unlike
// admitNewLocked is allowed to displace the worst evictable row to
// make room — but is NEVER allowed to reject. A direct route models a
// session the OS already holds open: refusing to record it because
// the bucket is saturated would silently desync the table from the
// transport layer, and the very next sessionSend(peer, …) would
// disagree with what the routing table claims is reachable.
//
// Algorithm (with cap active):
//
//  1. Withdrawn rows are not counted (rule 1 of admitNewLocked
//     applies symmetrically here): tombstones live outside the
//     K-counted slots, so a bucket with K live rows plus any number
//     of tombstones still has room → no.
//  2. If the (Identity, Origin) bucket has fewer than
//     maxNextHopsPerOrigin LIVE entries, append unconditionally;
//     no cap counter is touched (the AdmissionAccepted shape is
//     reserved for admitNewLocked, not direct admission, so an
//     operator reading CapStats does not misread direct registers
//     as ordinary admission churn).
//  3. Otherwise pick the worst evictable LIVE row using the same
//     eviction key as admitNewLocked ((live > expired) → trust →
//     hops → expiry) and replace it in-place. Counter
//     `acceptedReplaced` is incremented because operationally this
//     is identical to a strict-better cap-driven displacement —
//     `Accepted+AcceptedReplaced` continues to mean "live entries
//     written into the cap regime".
//  4. Defensive fallback: if every live entry in the bucket is
//     direct/local (the AllProtected shape), the new row is still
//     appended without bumping any counter, leaving the bucket
//     transiently at K+1 live. This branch is unreachable through
//     the public AddDirectPeer API — direct routes have
//     NextHop == Identity, the triple uniqueness check upstream
//     guarantees the (Identity, Origin, NextHop=Identity) row does
//     not already exist, and any other row in the same bucket
//     therefore has NextHop != Identity and thus cannot be direct.
//     Local entries live in the (self, self) bucket, which
//     AddDirectPeer never touches. The fallback is kept as a guard
//     for synthetic test fixtures and any future caller that
//     constructs a bucket through a non-public mutation path; in
//     production CapStats stays a clean signal.
//
// When the cap is disabled (maxNextHopsPerOrigin <= 0) the function
// degenerates to an unconditional append, which matches the pre-cap
// behaviour of AddDirectPeer.
//
// Caller contract: t.mu held; entry.Identity / entry.Origin /
// entry.NextHop=Identity / Source=Direct already validated by the
// caller (AddDirectPeer's RouteEntry construction); the
// (Identity, Origin, NextHop=Identity) triple is NOT already present
// as a live row (the idempotent fast path above filters that case)
// — though it MAY be present as a tombstone, in which case the
// caller has already detached the tombstone slot before calling.
func (t *Table) admitDirectLocked(entry RouteEntry, now time.Time) {
	routes := t.routes[entry.Identity]

	if t.maxNextHopsPerOrigin <= 0 {
		t.routes[entry.Identity] = append(routes, entry)
		return
	}

	bucketCount := 0
	for i := range routes {
		if routes[i].Origin != entry.Origin {
			continue
		}
		if routes[i].IsWithdrawn() {
			continue
		}
		bucketCount++
	}
	if bucketCount < t.maxNextHopsPerOrigin {
		t.routes[entry.Identity] = append(routes, entry)
		return
	}

	worstIdx := -1
	for i := range routes {
		if routes[i].Origin != entry.Origin {
			continue
		}
		if routes[i].IsWithdrawn() {
			continue
		}
		if !isEvictable(&routes[i]) {
			continue
		}
		if worstIdx < 0 || isWorseForEvictionLocked(&routes[i], &routes[worstIdx], now) {
			worstIdx = i
		}
	}
	if worstIdx < 0 {
		// Defensive fallback — see step 4 in the doc-comment for
		// why this is unreachable through AddDirectPeer in
		// production.
		t.routes[entry.Identity] = append(routes, entry)
		return
	}

	routes[worstIdx] = entry
	t.routes[entry.Identity] = routes
	t.capStats.acceptedReplaced.Add(1)
}

// CapStats returns a value-copy of the monotonic admission counters
// produced by the MaxNextHopsPerOrigin policy. Callers may invoke this
// without t.mu — the counters are atomic and the returned snapshot
// reflects each field's value at its individual Load point. Cross-field
// consistency is best-effort: under heavy admission churn the four
// counters can be observed at different points along the timeline by
// at most one increment each, which is fine for monitoring purposes.
//
// Returns the zero value when the cap is disabled
// (maxNextHopsPerOrigin <= 0); the admission path skips the increments
// in that mode by design.
func (t *Table) CapStats() RouteCapStats {
	return RouteCapStats{
		Accepted:             t.capStats.accepted.Load(),
		AcceptedReplaced:     t.capStats.acceptedReplaced.Load(),
		RejectedFull:         t.capStats.rejectedFull.Load(),
		RejectedAllProtected: t.capStats.rejectedAllProtected.Load(),
	}
}

// isEvictable reports whether r can be displaced by a cap admission
// decision. Direct and local routes are protected: their lifecycle is
// managed by other paths (RemoveDirectPeer for direct, the synthetic
// self-route construction for local) and the cap must never silently
// retire either of them — that would either break a live socket
// representation or invalidate the "you can always route to yourself"
// invariant.
func isEvictable(r *RouteEntry) bool {
	return r.Source != RouteSourceDirect && r.Source != RouteSourceLocal
}

// isLiveForEviction reports whether r counts as "live" for the cap
// admission ordering: not withdrawn (Hops < HopsInfinity) AND not
// wall-clock expired against `now`. In practice the cap path
// (admitNewLocked rule 0) excludes withdrawn rows from bucketCount
// and from the eviction-search before this comparison ever runs, so
// the predicate effectively distinguishes live from
// expired-but-not-withdrawn rows — and that "expired" arm is the
// FIRST tier of the eviction key, dropping a stale hop_ack ahead of
// a fresh announcement regardless of trust. The IsWithdrawn check is
// kept defensively (admitNewLocked rule 0 is the only place that
// relies on this predicate, and a future caller that forgets to
// pre-filter withdrawn rows still gets the conservative answer).
// See isWorseForEvictionLocked.
//
// Held outside an exported helper because the cap admission policy is
// the only consumer; other callers use IsWithdrawn / IsExpired
// directly with their own semantics.
func isLiveForEviction(r *RouteEntry, now time.Time) bool {
	return !r.IsWithdrawn() && !r.IsExpired(now)
}

// isWorseForEvictionLocked reports whether `a` is a worse candidate to
// keep than `b` under the cap eviction order: live beats dead → lowest
// TrustRank → highest Hops → earliest ExpiresAt. The eviction picks
// whichever entry answers true to this against every other evictable
// candidate, so the key has to be total (no ties on the same triple —
// and across triples
// the lexicographic key is total as well).
//
// "Earliest ExpiresAt wins eviction" feels backward at first glance,
// but it is the right tie-break: an entry that is closer to its TTL
// expiry will be replaced by TickTTL soon anyway, so dropping it now
// to make room for a fresher candidate is strictly cheaper than
// dropping the slot that would have lived longer.
//
// Must be called with t.mu held — the comparison itself is pure, but
// the helper is named *Locked because both arguments come from inside
// t.routes which the caller is reading under the same lock.
func isWorseForEvictionLocked(a, b *RouteEntry, now time.Time) bool {
	// Liveness tier: an expired-but-not-withdrawn row is worse than
	// any fully-live candidate, regardless of trust. This is what
	// keeps a bucket of K stale hop_acks from blocking a fresh
	// announcement: under trust-only ordering the expired hop_acks
	// would dominate (RouteSourceHopAck.TrustRank() = 1 >
	// RouteSourceAnnouncement.TrustRank() = 0) and the announcement
	// would be rejected even though Lookup would return zero usable
	// routes for the bucket. Withdrawn rows do not appear here at
	// all — admitNewLocked rule 0 excludes them from both
	// bucketCount and the eviction-search, so the cap never trades a
	// tombstone against a live candidate; tombstones live alongside
	// the K cap-counted slots and are reclaimed by TickTTL after
	// defaultTTL.
	aLive := isLiveForEviction(a, now)
	bLive := isLiveForEviction(b, now)
	if aLive != bLive {
		return !aLive
	}
	// Within the same liveness tier the original ranking applies.
	if a.Source.TrustRank() != b.Source.TrustRank() {
		return a.Source.TrustRank() < b.Source.TrustRank()
	}
	if a.Hops != b.Hops {
		return a.Hops > b.Hops
	}
	return a.ExpiresAt.Before(b.ExpiresAt)
}

// isStrictlyBetterForCapLocked is the inverse of
// isWorseForEvictionLocked, expressed positively: returns true when
// `cand` is strictly better than `worst` and therefore should displace
// it. The relation is irreflexive (no entry is strictly better than
// itself) and shares the same total order, so feeding back the same
// pointer twice returns false — which is what guarantees the cap
// "floor" semantics: a worse-or-equal incoming entry is dropped, never
// admitted.
//
// Must be called with t.mu held for the same reason as
// isWorseForEvictionLocked.
func isStrictlyBetterForCapLocked(cand, worst *RouteEntry, now time.Time) bool {
	// Liveness tier mirrors isWorseForEvictionLocked: a live candidate
	// is strictly better than a dead worst, and a dead candidate is
	// not strictly better than a live worst — so the cap admits a
	// fresh live entry that displaces a dead row, but never admits a
	// new dead entry into a bucket that still has live routes.
	candLive := isLiveForEviction(cand, now)
	worstLive := isLiveForEviction(worst, now)
	if candLive != worstLive {
		return candLive
	}
	if cand.Source.TrustRank() != worst.Source.TrustRank() {
		return cand.Source.TrustRank() > worst.Source.TrustRank()
	}
	if cand.Hops != worst.Hops {
		return cand.Hops < worst.Hops
	}
	return cand.ExpiresAt.After(worst.ExpiresAt)
}

// syncSeqCounterLocked ensures the monotonic SeqNo counter stays ahead of
// any accepted own-origin entry. Without this, a table pre-populated via
// UpdateRoute (e.g., restored from snapshot) could have a higher SeqNo than
// the counter, causing the next AddDirectPeer/RemoveDirectPeer to emit a
// stale seq and break monotonicity.
//
// Must be called with t.mu held.
func (t *Table) syncSeqCounterLocked(entry RouteEntry) {
	if t.localOrigin == "" || entry.Origin != t.localOrigin {
		return
	}
	if entry.SeqNo > t.seqCounters[entry.Identity] {
		t.seqCounters[entry.Identity] = entry.SeqNo
	}
}

// WithdrawRoute marks a specific route as withdrawn by setting hops to
// HopsInfinity. This should be called when processing an incoming withdrawal
// from the wire (hops=16, incremented SeqNo). For local peer disconnects,
// use RemoveDirectPeer instead.
//
// Returns true if the withdrawal was applied.
func (t *Table) WithdrawRoute(identity, origin, nextHop PeerIdentity, seqNo uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	existing := t.routes[identity]
	key := RouteTriple{Identity: identity, Origin: origin, NextHop: nextHop}
	idx := findByTriple(existing, key)

	if idx < 0 {
		// No active route for this triple — store a tombstone so that a
		// delayed older announcement with a lower SeqNo cannot resurrect
		// the withdrawn lineage. The tombstone expires via normal TTL.
		//
		// Tombstones are appended unconditionally and bypass the
		// MaxNextHopsPerOrigin cap by design: admitNewLocked
		// excludes withdrawn rows from both the bucket-size counter
		// and the eviction-search, so a tombstone neither competes
		// with live routes for the K slots nor displaces them. This
		// is what keeps the SeqNo resurrection guard intact under
		// cap pressure — without it, a bucket already at the cap
		// would either reject the tombstone (losing the guard for
		// THIS specific (Identity, Origin, NextHop) triple, so a
		// delayed lower-SeqNo announcement could resurrect it via
		// regular cap admission) or evict a live route to make room
		// (losing real routing capacity for a metadata marker).
		// Both outcomes were considered and rejected; see
		// docs/routing.md "RIB compaction" for the full rationale.
		//
		// The cap invariant after this design is "at most K LIVE
		// (non-tombstone) entries per (Identity, Origin)". The total
		// number of rows in the slice can temporarily exceed K when
		// recent withdrawals contributed tombstones; TickTTL reclaims
		// the tombstones after defaultTTL.
		tombstone := RouteEntry{
			Identity:  identity,
			Origin:    origin,
			NextHop:   nextHop,
			Hops:      HopsInfinity,
			SeqNo:     seqNo,
			Source:    RouteSourceAnnouncement,
			ExpiresAt: now.Add(t.defaultTTL),
		}
		t.routes[identity] = append(existing, tombstone)
		t.dirty.Store(true)
		return true
	}

	old := &existing[idx]
	if seqNo <= old.SeqNo {
		return false
	}

	existing[idx].Hops = HopsInfinity
	existing[idx].SeqNo = seqNo
	existing[idx].ExpiresAt = now.Add(t.defaultTTL)
	t.dirty.Store(true)
	return true
}

// AddDirectPeerResult describes the outcome of AddDirectPeer.
type AddDirectPeerResult struct {
	// Entry is the route entry that was created or refreshed.
	Entry RouteEntry

	// Penalized is true when the peer triggered flap detection and the
	// route was created with a shortened TTL. The caller can use this
	// to decide whether to delay or suppress the announcement.
	Penalized bool
}

// RemoveDirectPeerResult describes the outcome of RemoveDirectPeer.
type RemoveDirectPeerResult struct {
	// Withdrawals contains wire-ready AnnounceEntry items for direct routes
	// that this node originated. SeqNo is already incremented and Hops is
	// set to HopsInfinity. The caller sends these as-is in announce_routes
	// frames — no further seq arithmetic is needed.
	Withdrawals []AnnounceEntry

	// TransitInvalidated is the count of transit routes (learned via
	// announcement or hop_ack) that were silently marked as withdrawn
	// locally. No wire withdrawal is emitted for these — the originating
	// node is responsible for its own withdrawals.
	TransitInvalidated int

	// ExposedBackups lists identities where the withdrawal/invalidation of
	// routes through the disconnected peer exposed a surviving non-withdrawn,
	// non-expired backup route via a different next-hop. The caller can use
	// this to trigger event-driven pending queue drains — same semantics as
	// TickTTL's exposed return value.
	ExposedBackups []PeerIdentity
}

// AddDirectPeer registers a directly connected peer in the routing table.
// It creates a direct route (Hops=1, Source=RouteSourceDirect) with
// Origin set to this node's localOrigin and an auto-incremented SeqNo
// from the monotonic counter.
//
// Idempotent: if the peer already has an active (non-withdrawn) direct
// route originated by this node, AddDirectPeer refreshes the TTL and
// returns the existing entry without incrementing SeqNo. This prevents
// unnecessary SeqNo churn and triggered updates on duplicate connect
// events or additional sessions to the same peer identity.
//
// SeqNo is only incremented when the route is new or was previously
// withdrawn (reconnect after disconnect).
//
// Flap dampening: if the peer is in hold-down (too many recent
// disconnects), the route is created with penalizedTTL instead of
// defaultTTL, and Result.Penalized is set. The caller can use this
// to suppress or delay the triggered announcement.
//
// Returns AddDirectPeerResult and nil error on success.
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) AddDirectPeer(peerIdentity PeerIdentity) (AddDirectPeerResult, error) {
	if t.localOrigin == "" {
		return AddDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return AddDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	penalized := t.isPeerInHoldDownLocked(peerIdentity, now)

	key := RouteTriple{
		Identity: peerIdentity,
		Origin:   t.localOrigin,
		NextHop:  peerIdentity,
	}
	existing := t.routes[peerIdentity]
	idx := findByTriple(existing, key)

	// Already active — no-op. Direct routes have no TTL (ExpiresAt is
	// zero), so there is nothing to refresh. Their lifetime is managed
	// entirely by the session lifecycle: AddDirectPeer creates them,
	// RemoveDirectPeer withdraws them on socket close.
	if idx >= 0 && !existing[idx].IsWithdrawn() {
		return AddDirectPeerResult{Entry: existing[idx], Penalized: penalized}, nil
	}

	// New route or re-activation after withdrawal — increment SeqNo.
	seq := t.nextSeqLocked(peerIdentity)

	// ExpiresAt is intentionally left zero: a direct route represents a
	// live socket and must never expire by time. Only RemoveDirectPeer
	// (triggered by socket close) may invalidate it. Zero ExpiresAt
	// makes IsExpired return false unconditionally, so TickTTL will
	// never remove an active direct route.
	entry := RouteEntry{
		Identity: peerIdentity,
		Origin:   t.localOrigin,
		NextHop:  peerIdentity,
		Hops:     1,
		SeqNo:    seq,
		Source:   RouteSourceDirect,
	}

	// Both the new-direct (idx<0) and tombstone-reactivation
	// (idx>=0 && IsWithdrawn) paths must respect the
	// MaxNextHopsPerOrigin cap on (Identity, Origin) buckets. A bare
	// append/in-place-overwrite would push live count past K when:
	//
	//   - idx<0 and the bucket already holds K live non-direct rows
	//     (e.g. K hop_ack confirmations to the same destination via
	//     intermediate next-hops) — the new direct entry would be the
	//     K+1th live row.
	//   - idx>=0 and old is a tombstone: tombstones are NOT
	//     cap-counted (rule 1 of admitNewLocked), so replacing one
	//     in-place flips a non-counted slot into a counted live slot
	//     and live count climbs from up-to-K to up-to-K+1.
	//
	// Direct admission has its own contract — it must always succeed
	// (the OS already holds the socket open, so the table cannot
	// refuse) — so this is admitDirectLocked, not admitNewLocked.
	// The helper evicts the worst evictable row when the bucket is
	// saturated and never rejects.
	if idx < 0 {
		t.admitDirectLocked(entry, now)
	} else {
		// Reactivation from tombstone. Detach the tombstone slot
		// so admitDirectLocked sees a tombstone-free bucket and
		// the K-live invariant is evaluated against the post-
		// promotion shape, not pre. Same slice-detach pattern as
		// the tombstone→live promotion path in UpdateRoute (see
		// the SeqNo>old.SeqNo branch above).
		existing = append(existing[:idx], existing[idx+1:]...)
		t.routes[peerIdentity] = existing
		t.admitDirectLocked(entry, now)
	}
	// Reached only on a new direct route or a reactivation after
	// withdrawal — both are real state mutations visible in Snapshot.
	// The idempotent fast path above (idx >= 0 && !IsWithdrawn) returns
	// before this point and intentionally leaves dirty untouched.
	t.dirty.Store(true)

	return AddDirectPeerResult{Entry: entry, Penalized: penalized}, nil
}

// isPeerInHoldDownLocked checks if the peer is currently in flap hold-down.
// Must be called with t.mu held.
func (t *Table) isPeerInHoldDownLocked(peerIdentity PeerIdentity, now time.Time) bool {
	fs := t.flapState[peerIdentity]
	if fs == nil {
		return false
	}
	return now.Before(fs.holdDownUntil)
}

// recordWithdrawalLocked tracks a disconnect event for flap detection.
// If the withdrawal count within flapWindow crosses flapThreshold,
// hold-down is activated. Must be called with t.mu held.
//
// Exponential backoff: every consecutive flap-burst that lands while a
// previous burst is still within FlapStableWindowMultiplier × flapWindow
// doubles the hold-down duration, capped at MaxHoldDownDuration. The
// growth is bounded by FlapBackoffShiftCap so the multiplier never
// overflows int64 ns. Stable peers reset implicitly inside this same
// helper when the gap from the last burst exceeds the stable window;
// callers can also reset explicitly via RecordSuccessfulRouteAdd once
// they observe convergence on the announce plane.
func (t *Table) recordWithdrawalLocked(peerIdentity PeerIdentity, now time.Time) {
	fs := t.flapState[peerIdentity]
	if fs == nil {
		fs = &peerFlapState{}
		t.flapState[peerIdentity] = fs
	}

	fs.withdrawTimes = append(fs.withdrawTimes, now)

	// Trim events outside the window.
	cutoff := now.Add(-t.flapWindow)
	trimmed := fs.withdrawTimes[:0]
	for _, wt := range fs.withdrawTimes {
		if !wt.Before(cutoff) {
			trimmed = append(trimmed, wt)
		}
	}
	fs.withdrawTimes = trimmed

	// Implicit stable-window reset: if the previous burst is older than
	// the stable window, treat this withdrawal as a fresh streak.
	stableWindow := time.Duration(FlapStableWindowMultiplier) * t.flapWindow
	if !fs.lastFlapAt.IsZero() && now.Sub(fs.lastFlapAt) > stableWindow {
		fs.consecutiveFlaps = 0
	}

	if len(fs.withdrawTimes) >= t.flapThreshold {
		// Bump consecutiveFlaps only on the *transition* from
		// "hold-down expired/never armed" to "hold-down active". A
		// single flap-burst that drops the connection threshold+N
		// times must arm exactly one hold-down — without this guard
		// the counter would grow on every withdrawal beyond the
		// threshold and the next burst would already start at a
		// multi-step exp-backoff slot.
		if fs.holdDownUntil.IsZero() || !now.Before(fs.holdDownUntil) {
			fs.consecutiveFlaps++
		}
		fs.lastFlapAt = now
		fs.holdDownUntil = now.Add(holdDownDurationForBurst(t.holdDownDuration, fs.consecutiveFlaps))
	}
}

// holdDownDurationForBurst returns the hold-down duration for the
// consecutiveFlaps-th burst. The first burst (consecutiveFlaps == 1)
// uses the configured base duration; each subsequent burst doubles
// it, capped at MaxHoldDownDuration and bounded by
// FlapBackoffShiftCap so the bit-shift cannot overflow.
//
// A non-positive base disables hold-down entirely and short-circuits
// to base. Without this guard, exp-backoff would push base*2^shift
// down the scaled <= 0 branch starting on the second burst and turn a
// deliberately disabled hold-down into MaxHoldDownDuration — the
// opposite of what WithHoldDownDuration(0) requests.
func holdDownDurationForBurst(base time.Duration, consecutiveFlaps int) time.Duration {
	if base <= 0 {
		return base
	}
	if consecutiveFlaps <= 1 {
		if base > MaxHoldDownDuration {
			return MaxHoldDownDuration
		}
		return base
	}
	shift := consecutiveFlaps - 1
	if shift > FlapBackoffShiftCap {
		shift = FlapBackoffShiftCap
	}
	scaled := base * (1 << shift)
	if scaled > MaxHoldDownDuration || scaled <= 0 {
		return MaxHoldDownDuration
	}
	return scaled
}

// RecordSuccessfulRouteAdd clears the consecutive-flap counter for a
// peer that has demonstrated stability (e.g. a successful announce-plane
// round-trip after the hold-down expired). The withdrawTimes history
// is left untouched so a single successful add does not erase the
// flap-rate evidence inside the current flapWindow — that history
// trims itself naturally as the window slides forward. Callers in
// node.Service invoke this on the post-handshake path after a
// successful announce_routes exchange.
//
// Empty peerIdentity short-circuits as a no-op so callers can pass
// zero-value identities through without a nil-check.
func (t *Table) RecordSuccessfulRouteAdd(peerIdentity PeerIdentity) {
	if peerIdentity == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	fs := t.flapState[peerIdentity]
	if fs == nil {
		return
	}
	mutated := fs.consecutiveFlaps != 0 || !fs.lastFlapAt.IsZero()
	fs.consecutiveFlaps = 0
	fs.lastFlapAt = time.Time{}
	// consecutiveFlaps and lastFlapAt do not appear in Snapshot directly,
	// but they alter the future evolution of flapState.holdDownUntil
	// (subsequent flap-bursts that would have escalated now reset to base
	// duration). FlapEntry.HoldDownUntil is part of the snapshot, so a
	// pending hold-down arming computed from cleared counters could
	// disagree with the prior snapshot view. Mark dirty only when the
	// fields actually changed to avoid a republish on a no-op call.
	if mutated {
		t.dirty.Store(true)
	}
}

// RemoveDirectPeer handles a peer disconnect. It withdraws the direct
// route originated by this node (with an auto-incremented SeqNo) and
// silently invalidates all transit routes learned through that peer.
//
// The returned Withdrawals are wire-ready: SeqNo is already incremented,
// Hops is HopsInfinity, and the entries are in AnnounceEntry form. The
// caller sends them as-is in announce_routes frames.
//
// Returns ErrNoLocalOrigin if localOrigin was not configured, or
// ErrEmptyPeerID if peerIdentity is empty.
func (t *Table) RemoveDirectPeer(peerIdentity PeerIdentity) (RemoveDirectPeerResult, error) {
	if t.localOrigin == "" {
		return RemoveDirectPeerResult{}, ErrNoLocalOrigin
	}
	if peerIdentity == "" {
		return RemoveDirectPeerResult{}, ErrEmptyPeerID
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.recordWithdrawalLocked(peerIdentity, now)

	var result RemoveDirectPeerResult

	// Track which identities had routes invalidated so we can check for
	// surviving backup routes after the invalidation pass.
	affectedIdentities := make(map[PeerIdentity]struct{})

	for _, routes := range t.routes {
		for i := range routes {
			r := &routes[i]
			if r.NextHop != peerIdentity || r.Hops >= HopsInfinity {
				continue
			}

			affectedIdentities[r.Identity] = struct{}{}

			if r.Source == RouteSourceDirect && r.Origin == t.localOrigin {
				seq := t.nextSeqLocked(r.Identity)
				r.Hops = HopsInfinity
				r.SeqNo = seq
				r.ExpiresAt = now.Add(t.defaultTTL)

				result.Withdrawals = append(result.Withdrawals, AnnounceEntry{
					Identity: r.Identity,
					Origin:   r.Origin,
					Hops:     HopsInfinity,
					SeqNo:    seq,
				})
			} else {
				result.TransitInvalidated++
				r.Hops = HopsInfinity
				r.ExpiresAt = now.Add(t.defaultTTL)
			}
		}
	}

	// For each affected identity, check if a non-withdrawn, non-expired
	// backup route survives via a different next-hop.
	for identity := range affectedIdentities {
		for _, r := range t.routes[identity] {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				result.ExposedBackups = append(result.ExposedBackups, identity)
				break
			}
		}
	}

	// Always dirty: recordWithdrawalLocked mutated flapState (withdrawTimes
	// at minimum, possibly hold-down arming) and any affected route had
	// its Hops/SeqNo/ExpiresAt rewritten. Both are part of the published
	// Snapshot.
	t.dirty.Store(true)

	return result, nil
}

// InvalidateTransitRoutes sets hops=HopsInfinity on all non-direct routes
// whose NextHop matches peerIdentity. Unlike RemoveDirectPeer this does
// NOT generate wire withdrawals and does NOT touch direct routes — it is
// a defense-in-depth cleanup for peers that had mesh_routing_v1 (could
// advertise routes) but not mesh_relay_v1 (no direct route was created).
// Returns the number of transit routes invalidated and a slice of identities
// where the invalidation exposed a surviving non-withdrawn backup route via
// a different next-hop (same semantics as TickTTL's exposed return value).
func (t *Table) InvalidateTransitRoutes(peerIdentity PeerIdentity) (int, []PeerIdentity) {
	if peerIdentity == "" {
		return 0, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	invalidated := 0
	affectedIdentities := make(map[PeerIdentity]struct{})
	for _, routes := range t.routes {
		for i := range routes {
			r := &routes[i]
			if r.NextHop != peerIdentity || r.Hops >= HopsInfinity {
				continue
			}
			if r.Source == RouteSourceDirect {
				continue
			}
			affectedIdentities[r.Identity] = struct{}{}
			r.Hops = HopsInfinity
			r.ExpiresAt = now.Add(t.defaultTTL)
			invalidated++
		}
	}

	var exposed []PeerIdentity
	for identity := range affectedIdentities {
		for _, r := range t.routes[identity] {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				exposed = append(exposed, identity)
				break
			}
		}
	}

	if invalidated > 0 {
		t.dirty.Store(true)
	}
	return invalidated, exposed
}

// nextSeqLocked increments and returns the next SeqNo for a given identity.
// Must be called with t.mu held.
func (t *Table) nextSeqLocked(identity PeerIdentity) uint64 {
	t.seqCounters[identity]++
	return t.seqCounters[identity]
}

// TickTTL removes expired routes from the table and cleans up stale
// flap detection state. Should be called periodically (e.g., every
// second or every few seconds).
//
// Returns identities where at least one entry expired AND at least one
// non-withdrawn, non-expired route survives — indicating that a backup
// route has been exposed by the expiry. The caller can use this to
// trigger event-driven pending queue drains.
//
// Only ExpiresAt is checked — withdrawn (Hops >= HopsInfinity) entries
// are kept until their ExpiresAt elapses. This preserves tombstones
// created by WithdrawRoute that guard against resurrection from delayed
// lower-SeqNo announcements. RemoveDirectPeer and InvalidateTransitRoutes
// set a short ExpiresAt on withdrawn entries so they are cleaned up
// promptly without breaking tombstone semantics.
// TickTTLResult holds the outcome of a TTL sweep.
type TickTTLResult struct {
	// Exposed lists identities whose primary route expired but at least one
	// non-withdrawn backup route survives — callers can drain pending frames
	// to the surviving route immediately.
	Exposed []PeerIdentity

	// Removed is the total number of individual route entries that expired
	// across all identities. Zero means the routing table was not modified.
	Removed int
}

func (t *Table) TickTTL() TickTTLResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	var exposed []PeerIdentity
	totalRemoved := 0
	for identity, routes := range t.routes {
		origLen := len(routes)
		n := 0
		for i := range routes {
			if !routes[i].IsExpired(now) {
				routes[n] = routes[i]
				n++
			}
		}
		removed := origLen - n
		totalRemoved += removed
		if n == 0 {
			delete(t.routes, identity)
		} else {
			t.routes[identity] = routes[:n]
			// Some entries expired but survivors remain. Check if any
			// survivor is non-withdrawn — that means a usable backup
			// route was exposed by the expiry.
			if removed > 0 {
				for j := 0; j < n; j++ {
					if !routes[j].IsWithdrawn() {
						exposed = append(exposed, identity)
						break
					}
				}
			}
		}
	}

	// Clean up flap state where both hold-down has expired and all
	// withdrawal timestamps are outside the window.
	cutoff := now.Add(-t.flapWindow)
	flapStateMutated := false
	for peer, fs := range t.flapState {
		if !now.Before(fs.holdDownUntil) {
			// Hold-down has expired. Clear the timestamp so the
			// published snapshot reflects InHoldDown=false on the
			// next refresh — without this, FlapEntry.InHoldDown
			// stays true in the cached snapshot until withdrawTimes
			// drain past flapWindow (up to ~90 s after the hold-down
			// actually ended on defaults: 30 s hold-down + 120 s
			// flap window). Hold-down expiry is time-derived flap
			// state — see docs/routing.md "Snapshot freshness" — so
			// it is bounded by TickTTL_interval + one refresh, not
			// by a single refresh tick. TickTTL is the schedule that
			// converts the wall-clock transition into a writer event;
			// without the clear here, even running TickTTL would not
			// flip InHoldDown until withdrawTimes drained, breaking
			// even that looser bound.
			if !fs.holdDownUntil.IsZero() {
				fs.holdDownUntil = time.Time{}
				flapStateMutated = true
			}

			// Trim stale withdrawal events.
			n := 0
			for _, wt := range fs.withdrawTimes {
				if !wt.Before(cutoff) {
					fs.withdrawTimes[n] = wt
					n++
				}
			}
			if n != len(fs.withdrawTimes) {
				flapStateMutated = true
			}
			fs.withdrawTimes = fs.withdrawTimes[:n]

			if n == 0 {
				delete(t.flapState, peer)
				flapStateMutated = true
			}
		}
	}
	// Snapshot exposes both Routes and FlapState. Mark dirty if either
	// changed; a no-op tick (nothing expired and no flap-state cleanup)
	// must not force a republish.
	if totalRemoved > 0 || flapStateMutated {
		t.dirty.Store(true)
	}
	return TickTTLResult{Exposed: exposed, Removed: totalRemoved}
}

// RefreshDirectPeers is a no-op retained for API compatibility.
//
// Direct routes now use ExpiresAt=zero (never expire by time). Their
// lifetime is managed entirely by the session lifecycle: AddDirectPeer
// creates them on socket connect, RemoveDirectPeer withdraws them on
// socket close. No periodic TTL refresh is needed.
//
// Previously, direct routes had a finite TTL that required periodic
// refresh. This created a race condition: if TickTTL removed an expired
// direct route before the next refresh cycle, the route was permanently
// lost (AddDirectPeer would not be called again for an already-counted
// session).
func (t *Table) RefreshDirectPeers() int {
	return 0
}

// Announceable returns routes suitable for announcing to a specific peer,
// applying split horizon: routes learned from excludeVia are omitted.
// Routes originated by the peer (Origin == excludeVia) are also omitted —
// the peer already knows its own routes and re-sending them wastes
// bandwidth and triggers spurious "forged own origin" rejections on the
// receiver side.
// Withdrawn and expired routes are also excluded.
//
// Split horizon rule: routes where NextHop == excludeVia are not included
// in the announcement. We do NOT send fake hops=16 withdrawals — that
// would violate the per-origin SeqNo invariant.
func (t *Table) Announceable(excludeVia PeerIdentity) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	var result []RouteEntry

	for _, routes := range t.routes {
		for _, r := range routes {
			if r.NextHop == excludeVia {
				continue
			}
			if r.Origin == excludeVia {
				continue
			}
			if r.IsWithdrawn() || r.IsExpired(now) {
				continue
			}
			result = append(result, r)
		}
	}
	return result
}

// AnnounceTo returns the wire-safe projection of routes to announce to
// a specific peer, applying split horizon, origin filtering and the +1
// hop rule. This is the preferred method for building announce_routes
// frames — it ensures the boundary between model and wire format stays
// in the routing package.
//
// Origin filtering: routes where Origin == excludeVia are skipped because
// the peer originated them and would reject them as forged own-origin.
func (t *Table) AnnounceTo(excludeVia PeerIdentity) []AnnounceEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	var result []AnnounceEntry

	for _, routes := range t.routes {
		for _, r := range routes {
			if r.NextHop == excludeVia {
				continue
			}
			if r.Origin == excludeVia {
				continue
			}
			if r.IsExpired(now) {
				continue
			}
			if r.IsWithdrawn() {
				// Own-origin tombstones are included so that the announce
				// delta mechanism can retry withdrawal delivery to peers
				// where the immediate send failed. Only the origin may
				// emit wire withdrawals — transit tombstones are excluded.
				if t.localOrigin != "" && r.Origin == t.localOrigin {
					result = append(result, r.ToAnnounceEntry())
				}
				continue
			}
			result = append(result, r.ToAnnounceEntry())
		}
	}
	return result
}

// Lookup returns all non-withdrawn, non-expired routes for the given identity,
// sorted by preference: source priority (local > direct > hop_ack > announcement),
// then by hops ascending within the same source tier.
//
// When the queried identity matches the node's own localOrigin, a synthetic
// local route (Hops=0, RouteSourceLocal) is prepended. This ensures a node
// can always resolve a route to itself without requiring an external peer
// session or an explicit table entry.
func (t *Table) Lookup(identity PeerIdentity) []RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	routes := t.routes[identity]
	var result []RouteEntry

	// Synthesize a local route for own identity — zero hops, never expires.
	if t.localOrigin != "" && identity == t.localOrigin {
		result = append(result, t.localRouteEntry())
	}

	for _, r := range routes {
		if !r.IsWithdrawn() && !r.IsExpired(now) {
			result = append(result, r)
		}
	}

	sortRoutes(result)
	return result
}

// InspectTriple returns the raw route entry for the given dedup triple,
// or nil if no entry exists. Unlike Lookup, it does not filter by
// withdrawn/expired status — callers see the entry exactly as stored.
// Intended for diagnostics and debugging (e.g. explaining why UpdateRoute
// rejected an incoming entry).
func (t *Table) InspectTriple(key RouteTriple) *RouteEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	routes := t.routes[key.Identity]
	idx := findByTriple(routes, key)
	if idx < 0 {
		return nil
	}
	dup := routes[idx]
	return &dup
}

// Snapshot returns an immutable point-in-time view of the entire table.
// All fields (routes, counts, flap state) are captured under a single
// lock acquisition, ensuring a self-consistent response for RPC consumers.
//
// When localOrigin is configured, a synthetic local route (Hops=0,
// RouteSourceLocal) is injected for the node's own identity, ensuring
// that RPC consumers always see a route to the local node.
func (t *Table) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	snap := Snapshot{
		Routes:  make(map[PeerIdentity][]RouteEntry, len(t.routes)),
		TakenAt: now,
	}

	totalEntries := 0
	activeEntries := 0
	for ident, routes := range t.routes {
		copied := make([]RouteEntry, len(routes))
		copy(copied, routes)
		snap.Routes[ident] = copied
		totalEntries += len(routes)
		for _, r := range routes {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				activeEntries++
			}
		}
	}

	// Inject synthetic local route for own identity. The self-route is
	// read-time only — it is NOT counted in TotalEntries/ActiveEntries
	// because those counters describe persisted table state and must
	// stay consistent with ActiveSize().
	if t.localOrigin != "" {
		localEntry := t.localRouteEntry()
		snap.Routes[t.localOrigin] = append(
			[]RouteEntry{localEntry},
			snap.Routes[t.localOrigin]...,
		)
	}

	snap.TotalEntries = totalEntries
	snap.ActiveEntries = activeEntries

	// Capture flap state atomically with routes.
	cutoff := now.Add(-t.flapWindow)
	for peer, fs := range t.flapState {
		recentCount := 0
		for _, wt := range fs.withdrawTimes {
			if !wt.Before(cutoff) {
				recentCount++
			}
		}
		inHoldDown := now.Before(fs.holdDownUntil)
		if recentCount == 0 && !inHoldDown {
			continue
		}
		// Normalize HoldDownUntil to zero whenever the peer is not
		// in hold-down. Without this, a snapshot taken AFTER
		// fs.holdDownUntil elapsed but BEFORE TickTTL has had a
		// chance to clear it (TickTTL runs every 10 s, an unrelated
		// mutation can trigger rebuildRoutingSnapshot in between)
		// would expose a FlapEntry with InHoldDown=false and a
		// non-zero HoldDownUntil pointing at the past — directly
		// contradicting the FlapEntry doc-comment ("HoldDownUntil
		// is zero when not in hold-down").
		holdDownUntil := fs.holdDownUntil
		if !inHoldDown {
			holdDownUntil = time.Time{}
		}
		snap.FlapState = append(snap.FlapState, FlapEntry{
			PeerIdentity:      peer,
			RecentWithdrawals: recentCount,
			InHoldDown:        inHoldDown,
			HoldDownUntil:     holdDownUntil,
		})
	}

	// CapStats is read AFTER the routes/flap pass: a writer that
	// landed an admission decision after we released that pass would
	// already have moved past the t.mu critical section, so its
	// counters reflect a state that is at most one tick stale relative
	// to the routes view. Acceptable for monitoring purposes.
	snap.CapStats = t.CapStats()

	return snap
}

// Size returns the total number of route entries (including withdrawn).
func (t *Table) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	total := 0
	for _, routes := range t.routes {
		total += len(routes)
	}
	return total
}

// FlapSnapshot returns the current flap detection state for all tracked peers.
// Stale entries are filtered: withdrawals outside the flap window are trimmed,
// and peers with no recent withdrawals and no active hold-down are excluded.
// This avoids reporting false positives between TickTTL cleanup cycles.
func (t *Table) FlapSnapshot() []FlapEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	cutoff := now.Add(-t.flapWindow)
	var entries []FlapEntry
	for peer, fs := range t.flapState {
		// Count only withdrawals within the current flap window.
		recentCount := 0
		for _, wt := range fs.withdrawTimes {
			if !wt.Before(cutoff) {
				recentCount++
			}
		}
		inHoldDown := now.Before(fs.holdDownUntil)

		// Skip peers with no recent withdrawals and no active hold-down.
		if recentCount == 0 && !inHoldDown {
			continue
		}

		// Mirror Snapshot: normalize HoldDownUntil to zero when
		// inHoldDown is false. fs.holdDownUntil may still be a
		// past timestamp here (TickTTL clears it on its own
		// schedule, every 10 s); without normalization the
		// FlapEntry would carry a stale "in the past" deadline
		// that contradicts the InHoldDown=false flag.
		holdDownUntil := fs.holdDownUntil
		if !inHoldDown {
			holdDownUntil = time.Time{}
		}

		entries = append(entries, FlapEntry{
			PeerIdentity:      peer,
			RecentWithdrawals: recentCount,
			InHoldDown:        inHoldDown,
			HoldDownUntil:     holdDownUntil,
		})
	}
	return entries
}

// LocalOrigin returns this node's identity string.
func (t *Table) LocalOrigin() PeerIdentity {
	return t.localOrigin
}

// localRouteEntry returns a synthetic route entry representing the node
// itself. Hops=0 means local delivery with no network traversal. The
// entry never expires and has the highest trust rank (RouteSourceLocal).
// Returns zero-value RouteEntry if localOrigin is not configured.
func (t *Table) localRouteEntry() RouteEntry {
	if t.localOrigin == "" {
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

// ActiveSize returns the number of non-withdrawn, non-expired entries.
func (t *Table) ActiveSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.clock()
	total := 0
	for _, routes := range t.routes {
		for _, r := range routes {
			if !r.IsWithdrawn() && !r.IsExpired(now) {
				total++
			}
		}
	}
	return total
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

// findByTriple returns the index of the entry with the given triple, or -1.
func findByTriple(routes []RouteEntry, key RouteTriple) int {
	for i := range routes {
		if routes[i].Identity == key.Identity &&
			routes[i].Origin == key.Origin &&
			routes[i].NextHop == key.NextHop {
			return i
		}
	}
	return -1
}

// sortRoutes sorts routes by preference: source priority first
// (direct > hop_ack > announcement), then by hops ascending.
func sortRoutes(routes []RouteEntry) {
	for i := 1; i < len(routes); i++ {
		for j := i; j > 0 && isBetter(&routes[j], &routes[j-1]); j-- {
			routes[j], routes[j-1] = routes[j-1], routes[j]
		}
	}
}
