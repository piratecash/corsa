package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
)

// PeerIdentity, PeerAddress and PeerCapability are re-exported from domain so
// that callers importing routing do not need a separate domain import.
type (
	PeerIdentity   = domain.PeerIdentity
	PeerAddress    = domain.PeerAddress
	PeerCapability = domain.Capability
)

var (
	ErrEmptyIdentity       = errors.New("routing: Identity must not be empty")
	ErrEmptyOrigin         = errors.New("routing: Origin must not be empty")
	ErrEmptyNextHop        = errors.New("routing: NextHop must not be empty")
	ErrInvalidHops         = errors.New("routing: Hops must be between 1 and HopsInfinity (16)")
	ErrDirectHopsMust1     = errors.New("routing: direct route must have Hops=1")
	ErrDirectNextHop       = errors.New("routing: direct route NextHop must equal Identity (directly connected)")
	ErrDirectForeignOrigin = errors.New("routing: direct route Origin must equal localOrigin")
	ErrNoLocalOrigin       = errors.New("routing: Table.localOrigin not set (use WithLocalOrigin)")
	ErrEmptyPeerID         = errors.New("routing: peerIdentity must not be empty")
	ErrLocalSourceReserved = errors.New("routing: RouteSourceLocal is synthetic and cannot be persisted via UpdateRoute")
)

// Flap detection defaults. A peer that disconnects and reconnects more than
// FlapThreshold times within FlapWindow is considered unstable. Once the
// threshold is crossed, the peer enters hold-down: reconnections during
// HoldDownDuration still create routes, but with PenalizedTTL instead of
// the default — giving the network time to converge before the route
// expires again if the link flaps once more.
const (
	DefaultFlapWindow       = 120 * time.Second
	DefaultFlapThreshold    = 3
	DefaultHoldDownDuration = 30 * time.Second
	DefaultPenalizedTTL     = 30 * time.Second
	// MaxHoldDownDuration caps the exponential hold-down growth so that
	// a chronically flapping peer can be retried at most every 10
	// minutes. The cap protects mesh convergence — without it, a peer
	// that has been flapping for an hour would have a multi-hour
	// hold-down even after it stabilizes.
	MaxHoldDownDuration = 600 * time.Second
	// FlapBackoffShiftCap bounds the bit-shift exponent used in the
	// exp-backoff multiplier so that consecutiveFlaps cannot grow the
	// shift beyond what an int64 nanosecond duration can hold. The
	// effective multiplier therefore ranges over 1..2^FlapBackoffShiftCap.
	FlapBackoffShiftCap = 5
	// FlapStableWindowMultiplier defines how long after the last
	// flap-burst a peer must remain stable before consecutiveFlaps is
	// allowed to reset implicitly inside recordWithdrawalLocked. Two
	// flap windows give the announce machinery enough room to converge
	// across the network before forgiving the previous burst.
	FlapStableWindowMultiplier = 2
)

// SeqNo flap cap defaults (Phase 1 P2). The cap bounds the rate at
// which a single Identity's outbound wire SeqNo can advance, so a
// misbehaving peer that announces high-frequency SeqNo bumps cannot
// drive AnnounceLoop's wire emit + snapshot rebuild cycle
// indefinitely. The cap protects the per-cycle delta dispatcher, the
// per-peer announce baseline cache, and the CPU cost of
// BuildAnnounceSnapshot from a single Identity hot-loop.
//
// Defaults:
//
//   - DefaultMaxSeqAdvancePerWindow: 10 outbound-SeqNo advances per
//     sliding window before hold-down engages. Operators on noisy
//     meshes can raise this via CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW.
//   - DefaultSeqAdvanceWindow: 5-minute sliding window. Tunable via
//     CORSA_SEQNO_ADVANCE_WINDOW_SECONDS.
//   - DefaultSeqHoldDownDuration: DefaultTTL/2 (60s on defaults).
//     NOT separately tunable — preserves the refresh-interval
//     invariant EffectiveForcedFullSyncInterval <= DefaultTTL/2,
//     so a hold-down cannot be shorter than the receiver-side TTL
//     window that the suppression must outlast.
//
// Hold-down semantics: receive-side ingest keeps processing announces
// (local stored state stays accurate); wire emit for the Identity to
// ALL peers is suppressed for DefaultSeqHoldDownDuration; ApplyUpdate
// rejects incoming announces whose outbound-projected SeqNo would
// advance past `observed+1` while hold-down is active. The full
// contract — including detection model, observability surface, and
// interaction with the other Phase 1 admission rules — is documented
// in docs/routing.md under "SeqNo flap cap"; the engage / release
// log fields are pinned by the FlapDetector docstring in flap.go.
const (
	DefaultMaxSeqAdvancePerWindow = 10
	DefaultSeqAdvanceWindow       = 5 * time.Minute
	DefaultSeqHoldDownDuration    = DefaultTTL / 2
)

// MaxSaneHops bounds the per-claim hop count above which a fresh
// ingest is fast-invalidated locally (Phase 1 P3). DV protocols
// self-bound at HopsInfinity (16); a claim approaching that bound is
// almost always a routing loop or a pre-loop convergence stage —
// admitting it just delays the inevitable and steers send_message /
// relay traffic onto a known-doomed uplink for up to TTL.
//
// Default 8 leaves comfortable headroom over typical mesh diameters
// (5-6 hops for a 100-node mesh) while still catching count-to-
// infinity loops early. Operators on deep meshes can override via
// CORSA_MAX_SANE_HOPS.
//
// Fast-invalidation semantics: the bad claim is stored as a
// tombstone at incoming.SeqNo (NO bump — the upstream's next legit
// advance must strictly exceed the observed bad SeqNo for recovery)
// with ExpiresAt=now+defaultTTL. Wire emit suppression is automatic
// because AnnounceProjectionFor only emits own-direct tombstones
// (Source == RouteSourceDirect) and fast-invalidated transit claims
// stay Source != Direct. The full contract — including the order
// of checks against the cross-Origin admission rules — is
// documented inline in route_store_mutation.go's ApplyUpdate fast-
// invalidation branch and in docs/routing.md under "RIB compaction".
const MaxSaneHops = 8

// DefaultMaxNextHopsPerOrigin is the recommended ceiling for the
// "K next-hops per Identity" cap that bounds local RIB growth on
// large meshes. Without a cap the table grows O(N×M) — N
// destinations × M next-hops that learned the route — which on a
// 1000-node mesh produces hundreds of thousands of entries and turns
// every full-table snapshot into a multi-megabyte deep copy.
//
// Storage shape note: post-Phase-A the cap bounds the count of LIVE
// (non-withdrawn) UplinkClaim rows in each (Identity) bucket — see
// route_store.go's bucket layout and uplink_claim.go's type-level
// doc-comment for why Origin was dropped from the dedup key. The
// constant name keeps the historical "PerOrigin" suffix purely for
// operator-facing knob stability (env var name, config field name,
// dashboards keyed on the metric label); semantically it is
// "max LIVE uplinks per Identity" today. Tombstones (withdrawn
// claims) live OUTSIDE the K-counted slots so the SeqNo-resurrection
// guard cannot be evicted under cap pressure.
//
// Four is enough to keep multipath behaviour intact (primary + 1 backup
// + 1 spare for probing + 1 inertial slot) while bounding the per-
// Identity memory footprint at a constant. Operators can override
// per-deployment via the WithMaxNextHopsPerOrigin Table option, or —
// when constructing the Service from configuration — via
// config.Node.MaxNextHopsPerOrigin (env var
// CORSA_MAX_NEXT_HOPS_PER_ORIGIN), which the loader threads through
// to the Table constructor.
//
// Two distinct defaults apply at different layers:
//
//   - Bare Table (NewTable without WithMaxNextHopsPerOrigin) defaults
//     to 0 — the cap is disabled. Test fixtures and any caller that
//     wires a Table directly without going through config see the
//     pre-cap behaviour by default, which keeps unit tests of the
//     admission logic deterministic.
//   - Production Service constructed via config.Default() defaults to
//     this constant (4). The first rollout release shipped with the
//     config-layer default also at 0 so existing deployments observed
//     pre-cap behaviour exactly during the soak period; the second
//     release flipped the config-layer default to 4. Operators that
//     need to roll back set CORSA_MAX_NEXT_HOPS_PER_ORIGIN=0
//     explicitly.
const DefaultMaxNextHopsPerOrigin = 4

// RouteAdmissionDecision describes how the cap admission policy
// resolved an incoming RouteEntry that did not match an existing
// (Identity, Uplink=NextHop) claim. It is INTERNAL to the cap
// admission helpers (routeStore.AdmitNew, routeStore.AdmitDirect)
// and is NOT returned from public Table APIs — callers of
// UpdateRoute see only the coarser RouteUpdateStatus
// (Accepted / Unchanged / Rejected), which intentionally collapses
// cap-induced rejections and stale-SeqNo rejections into the same
// outward signal because both share the same caller obligation
// ("drop and move on").
//
// The decision surfaces externally only via the aggregate counters
// in RouteCapStats (published into routing.Snapshot.CapStats and
// the cap_admission JSON object on fetchRouteSummary). Operators
// distinguish cap pressure from stale-SeqNo churn by reading those
// monotonic counters, not per-call diagnostics. If a future caller
// needs per-call distinction, the decision must be plumbed out of
// routeStore.Admit* and onto a new public return value — not
// inferred from RouteUpdateStatus.
type RouteAdmissionDecision uint8

const (
	// AdmissionAccepted means the entry was inserted into a
	// per-Identity UplinkClaim bucket that had room — no eviction required.
	AdmissionAccepted RouteAdmissionDecision = iota

	// AdmissionAcceptedReplaced means the per-Identity UplinkClaim bucket was
	// at the cap and a worse existing candidate was evicted to make
	// room. The displaced entry is dropped silently — there is no
	// per-call "admission diagnostic" surfacing the replaced
	// row's identity / origin / next-hop. The only externally
	// observable signal is the monotonic counter in CapStats
	// (`AcceptedReplaced`).
	AdmissionAcceptedReplaced

	// AdmissionRejectedFull means the per-Identity UplinkClaim bucket was at
	// the cap and the incoming entry was not strictly better than the
	// worst evictable candidate. The incoming entry is dropped — caller
	// must NOT insert it. This is the "cap eviction floor" path that
	// keeps an authentic-but-worse next-hop from cycling out a stable
	// best-K when the bucket is saturated with already-good routes.
	AdmissionRejectedFull

	// AdmissionRejectedAllProtected means the per-Identity UplinkClaim bucket
	// was at the cap and every existing entry was direct or local —
	// none were evictable. The incoming entry is dropped because direct
	// routes represent live sessions that the table must never displace
	// implicitly: only the session lifecycle (RemoveDirectPeer on
	// disconnect) may retire them.
	//
	// In practice this branch is rare: post-Phase-A the bucket is
	// keyed by (Identity, Uplink) and a direct route by construction
	// has Uplink == Identity, so a single Identity bucket can hold
	// at most ONE direct entry (the row that represents the live
	// session to that specific destination peer). The local entry
	// is the synthetic self-route, which is also at most one per
	// Identity and only for the local origin's own destination. So
	// "every member is direct/local" can really only mean "K is
	// very small (most often K=1), the single protected slot in
	// this Identity bucket is held by the direct/local row, and
	// an announcement / hop_ack for a different uplink arrived".
	// Operators seeing a non-zero counter with K≥2 should
	// investigate synthetic / test fixtures or a direct-restore
	// edge case rather than fan-out sizing.
	AdmissionRejectedAllProtected
)

// String renders the decision as a stable human-readable token suitable
// for logs and metric labels. Values not produced by UpdateRoute return
// "unknown(N)" so a forgotten case in a switch is visible in diagnostics
// instead of silently collapsing into a default branch.
func (d RouteAdmissionDecision) String() string {
	switch d {
	case AdmissionAccepted:
		return "accepted"
	case AdmissionAcceptedReplaced:
		return "accepted_replaced"
	case AdmissionRejectedFull:
		return "rejected_full"
	case AdmissionRejectedAllProtected:
		return "rejected_all_protected"
	default:
		return fmt.Sprintf("unknown(%d)", d)
	}
}

// HopsInfinity marks a route as withdrawn. Only the origin node may
// set hops to this value on the wire; transit nodes invalidate locally
// and stop advertising the route instead.
const HopsInfinity = 16

// peerFlapState tracks disconnect events for a single peer identity.
// Used by the Table to detect link flapping and apply hold-down.
type peerFlapState struct {
	// withdrawTimes records timestamps of recent RemoveDirectPeer calls.
	// Only events within the flap window are retained.
	withdrawTimes []time.Time

	// holdDownUntil is the time until which AddDirectPeer will apply
	// a penalized (shorter) TTL. Zero means no hold-down active.
	holdDownUntil time.Time

	// consecutiveFlaps counts back-to-back hold-down activations that
	// arrived without a stable window between them. Each new flap-burst
	// while consecutiveFlaps > 0 doubles the hold-down duration (capped
	// at MaxHoldDownDuration) so a peer that keeps flapping does not
	// burn the same short hold-down repeatedly. Reset to zero by
	// RecordSuccessfulRouteAdd or by passing a stable window.
	consecutiveFlaps int

	// lastFlapAt is the wall-clock timestamp of the most recent
	// hold-down activation. Used together with the stable-window check
	// to decide when consecutiveFlaps may be cleared without an
	// explicit RecordSuccessfulRouteAdd call. Zero means "no flap
	// has ever fired", which keeps the field forward-compatible with
	// state-restore paths that rebuild flapState without history.
	lastFlapAt time.Time
}

// RouteSource indicates how a route was learned. The trust hierarchy is:
// direct > hop_ack > announcement. A route learned through a more trusted
// source is preferred over one with the same (identity, origin, nextHop)
// triple learned through a less trusted source.
type RouteSource uint8

const (
	RouteSourceAnnouncement RouteSource = iota // learned via announce_routes frame
	RouteSourceHopAck                          // confirmed by relay_hop_ack
	RouteSourceDirect                          // directly connected peer
	RouteSourceLocal                           // synthetic: the node itself (hops=0, never expires)
)

// String returns a human-readable representation for logging and debugging.
func (s RouteSource) String() string {
	switch s {
	case RouteSourceLocal:
		return "local"
	case RouteSourceDirect:
		return "direct"
	case RouteSourceHopAck:
		return "hop_ack"
	case RouteSourceAnnouncement:
		return "announcement"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// TrustRank returns a numeric rank for comparison. Higher rank means
// more trusted. This avoids relying on iota ordering.
func (s RouteSource) TrustRank() int {
	switch s {
	case RouteSourceLocal:
		return 3
	case RouteSourceDirect:
		return 2
	case RouteSourceHopAck:
		return 1
	case RouteSourceAnnouncement:
		return 0
	default:
		return -1
	}
}

// RouteEntry is the boundary value of a single route in the
// distance-vector table — the shape Table.UpdateRoute / AnnounceTo /
// Lookup pass on their public surface. It is NOT identical to the
// wire frame: announce_routes frames carry AnnounceEntry, which has
// (Identity, Origin, Hops, SeqNo, Extra) and no NextHop (the
// receiver knows the next-hop is the sender of the frame, so the
// wire never serialises it).
//
// Storage shape is different again. Post-Phase-A the routing table
// stores per-(Identity, Uplink) UplinkClaim values; Origin is
// consumed by Table.UpdateRoute for the foreign-origin Direct anti-
// spoof and then dropped, and Lookup output synthesises Origin =
// localOrigin (or Identity fallback). The post-A1 dedup key inside
// storage is (Identity, Uplink=NextHop); the legacy (Identity,
// Origin, NextHop) triple is gone. Origin remains on RouteEntry
// only as wire-frame metadata for pre-A1 receiver compat. See
// uplink_claim.go and table_mutation.go::UpdateRoute for the
// migration contract.
type RouteEntry struct {
	// Identity is the Ed25519 fingerprint of the destination node.
	Identity PeerIdentity

	// Origin is the peer identity that originally advertised this route
	// on the wire. Pre-A1 it scoped SeqNo space and decided who was
	// allowed to send a withdrawal (hops=16). Post-A1 it is wire-only
	// metadata: ingest consumes it for the foreign-Origin Direct anti-
	// spoof (Table.UpdateRoute), and emit always synthesises it to
	// localOrigin (or Identity fallback). Storage does NOT key on
	// Origin anymore — see the type-level doc-comment above.
	Origin PeerIdentity

	// NextHop is the peer identity from which we learned this route.
	// This is a peer identity, not a transport address — a single identity
	// may have multiple concurrent sessions.
	NextHop PeerIdentity

	// Hops is the distance to the destination. 1 means directly connected,
	// HopsInfinity (16) means withdrawn.
	Hops int

	// SeqNo is a monotonically increasing sequence number. Pre-A1 it was
	// scoped per Origin (only the origin could advance it). Post-A1 the
	// wire SeqNo is synthesised per (Identity, sender) by
	// routeStore.nextOutboundSeqLockedPerPeer /
	// nextOutboundSeqLockedBroadcast — see route_store.go's outboundContent
	// + outboundMax + outboundPeerMax field doc-comments. Storage still
	// keeps the native SeqNo on the UplinkClaim (used as the lower bound
	// for outbound synthesis and for the ApplyUpdate freshness check).
	// Within a single Identity's stored claims, the higher native SeqNo
	// still wins the live-vs-tombstone resurrection guard.
	SeqNo uint64

	// Source indicates how this route was learned. Used for trust-based
	// tie-breaking within the same (Identity, Uplink) pair post-Phase-A
	// (storage no longer keys on Origin — see the type-level doc-comment).
	Source RouteSource

	// ExpiresAt is the absolute time when this entry expires. Derived from
	// RemainingTTL at insertion time. After expiry the route is treated as
	// withdrawn.
	ExpiresAt time.Time

	// Extra holds opaque JSON fields from the wire that this node does not
	// understand. Preserved across table updates so that re-announced routes
	// carry forward-compatible extensions unchanged.
	Extra json.RawMessage
}

// Validate checks structural invariants of the entry. Returns an error
// if the entry is malformed and must not be inserted into the table.
//
// Source-specific rules:
//   - RouteSourceDirect: Hops must be 1, NextHop must equal Identity
//     (a direct route means the destination is our immediate neighbor).
func (e RouteEntry) Validate() error {
	if string(e.Identity) == "" {
		return ErrEmptyIdentity
	}
	if string(e.Origin) == "" {
		return ErrEmptyOrigin
	}
	if string(e.NextHop) == "" {
		return ErrEmptyNextHop
	}
	if e.Source == RouteSourceLocal {
		if e.Hops != 0 {
			return errors.New("routing: local route must have Hops=0")
		}
	} else if e.Hops < 1 || e.Hops > HopsInfinity {
		return ErrInvalidHops
	}
	if e.Source == RouteSourceDirect {
		if e.Hops != 1 {
			return ErrDirectHopsMust1
		}
		if e.NextHop != e.Identity {
			return ErrDirectNextHop
		}
	}
	return nil
}

// IsWithdrawn returns true if the route has been explicitly withdrawn
// (hops >= HopsInfinity). Expiry is a SEPARATE check — see
// IsExpired(now). The two predicates are orthogonal: a withdrawn
// route can also be expired (TickTTL hasn't reclaimed it yet), and
// an expired route can still be non-withdrawn (the upstream stopped
// re-announcing but never sent a withdrawal frame).
func (e RouteEntry) IsWithdrawn() bool {
	return e.Hops >= HopsInfinity
}

// IsExpired returns true if the route TTL has elapsed. The check is
// inclusive: a route whose ExpiresAt equals now is considered expired.
// This keeps ttl_seconds=0 and expired=true consistent in RPC output.
func (e RouteEntry) IsExpired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// DedupKey returns the legacy (Identity, Origin, NextHop) triple
// shape of the entry. Post-Phase-A storage keys on (Identity,
// Uplink) per uplink_claim.go; this helper is retained only for
// diagnostic / RPC outputs that still serialise the triple form
// for backward compatibility with pre-A1 tooling. New callers
// should not use it as a storage key — see UplinkClaim and
// routeStore.findByUplinkLocked.
func (e RouteEntry) DedupKey() RouteTriple {
	return RouteTriple{
		Identity: e.Identity,
		Origin:   e.Origin,
		NextHop:  e.NextHop,
	}
}

// RouteTriple is the deduplication key for route entries.
type RouteTriple struct {
	Identity PeerIdentity
	Origin   PeerIdentity
	NextHop  PeerIdentity
}

// Snapshot is an immutable point-in-time view of the routing table.
// Safe to read concurrently without locks. All fields are captured
// under a single lock acquisition, so they represent a consistent state.
type Snapshot struct {
	// Routes maps destination identity to all known routes for that identity.
	Routes map[PeerIdentity][]RouteEntry

	// TakenAt is the timestamp when the snapshot was captured.
	TakenAt time.Time

	// TotalEntries is the count of all route entries (including withdrawn/expired).
	TotalEntries int

	// ActiveEntries is the count of non-withdrawn, non-expired entries.
	ActiveEntries int

	// FlapState contains the flap detection state captured at the same instant
	// as the routes, avoiding inconsistency between separate reads.
	FlapState []FlapEntry

	// CapStats holds the cumulative admission-policy counters for the
	// MaxNextHopsPerOrigin cap. Stays at the zero value on tables with
	// the cap disabled — see RouteCapStats for the per-field semantics.
	// Carried in Snapshot rather than fetched separately so RPC handlers
	// observe a consistent view of "current routes vs. how many were
	// dropped/replaced by the cap" without a second round-trip into the
	// table.
	CapStats RouteCapStats
}

// BestRoute returns the best (lowest hop count, highest trust) non-withdrawn
// route for the given identity, or nil if none exists.
func (s Snapshot) BestRoute(identity PeerIdentity) *RouteEntry {
	routes, ok := s.Routes[identity]
	if !ok {
		return nil
	}
	var best *RouteEntry
	for i := range routes {
		r := &routes[i]
		if r.IsWithdrawn() || r.IsExpired(s.TakenAt) {
			continue
		}
		if best == nil || isBetter(r, best) {
			best = r
		}
	}
	return best
}

// AnnounceEntry is the wire-safe projection of a RouteEntry.
// It contains only the fields transmitted in announce_routes frames.
// Produced by RouteEntry.ToAnnounceEntry or Table.AnnounceTo.
//
// Extra carries opaque JSON fields from the wire that this node does not
// understand. When re-announcing a learned route, Extra is forwarded
// unchanged — enabling forward-compatible relay of future protocol
// extensions (e.g. onion box keys) through older nodes.
type AnnounceEntry struct {
	Identity PeerIdentity
	Origin   PeerIdentity
	Hops     int
	SeqNo    uint64

	// Extra holds unknown wire fields for forward-compatible relay.
	// Nil for locally originated routes.
	Extra json.RawMessage
}

// ToAnnounceEntry projects a RouteEntry into the wire format for
// announce_routes frames. The wire carries the sender's local hop count
// as-is — the receiver adds +1 when inserting into its own table
// (Phase 1.2 receive path). This matches the roadmap convention where
// A sends {X, hops=1} for a direct peer and B stores it as hops=2.
//
// Fields not transmitted on the wire (NextHop, Source, ExpiresAt)
// are stripped — the receiver derives them locally.
func (e RouteEntry) ToAnnounceEntry() AnnounceEntry {
	return AnnounceEntry{
		Identity: e.Identity,
		Origin:   e.Origin,
		Hops:     e.Hops,
		SeqNo:    e.SeqNo,
		Extra:    e.Extra,
	}
}

// RouteCapStats captures monotonic counters for three INDEPENDENT
// admission / invalidation policies that share a JSON envelope:
//
//  1. MaxNextHopsPerOrigin admission (Accepted, AcceptedReplaced,
//     RejectedFull, RejectedAllProtected). Covers the full admission
//     surface — UpdateRoute (announce / hop_ack ingestion via
//     routeStore.AdmitNew) AND AddDirectPeer (direct registration via
//     routeStore.AdmitDirect) — but the two helpers do not contribute
//     symmetrically: only AdmitNew uses every counter, and AdmitDirect
//     only ever bumps AcceptedReplaced (saturated direct admission
//     displacing an evictable row), while a below-K direct
//     registration is intentionally silent. See each field's
//     docstring for the exact accounting.
//  2. SeqNo flap cap (SeqNoFlapHoldowns). Phase 1 P2; gated by the
//     `maxSeqAdvancePerWindow` + `seqAdvanceWindow` knobs on
//     FlapDetector.
//  3. Fast invalidation on hops > MaxSaneHops (FastInvalidations).
//     Phase 1 P3; gated by the `maxSaneHops` knob on routeStore.
//
// The struct is value-safe to copy — it is published as part of
// routing.Snapshot and read lock-free by fetchRouteSummary.
//
// Per-cap kill-switch contract:
//
//   - The K-cap counters (Accepted / AcceptedReplaced / RejectedFull
//     / RejectedAllProtected) stay at zero on a Table with the cap
//     disabled (maxNextHopsPerOrigin <= 0): both admission helpers
//     short-circuit before any counter is touched.
//   - SeqNoFlapHoldowns stays at zero when EITHER the P2 threshold
//     OR the P2 window is non-positive (the FlapDetector
//     short-circuit), independently of the K-cap.
//   - FastInvalidations stays at zero when maxSaneHops <= 0 (the
//     ApplyUpdate fast-invalidation gate), independently of the
//     K-cap and the P2 cap.
//
// Operators reading the cap_admission JSON object must therefore
// gate their dashboards / alerts on the relevant knob (NOT on
// MaxNextHopsPerOrigin alone) — disabling the K-cap does NOT
// silence the P2/P3 counters.
type RouteCapStats struct {
	// Accepted counts entries that fit into a per-Identity UplinkClaim
	// bucket with room — no eviction was required. Sourced exclusively
	// from routeStore.AdmitNew (UpdateRoute). routeStore.AdmitDirect
	// deliberately does NOT bump this counter on a below-K direct
	// registration: the cap's "eviction rate" metric (see
	// AcceptedReplaced) is meant to reflect cap pressure on
	// transit/announcement traffic, and including every direct add
	// would dilute it with steady per-connect noise.
	//
	// Post-Phase-A the cap-admission bucket is per-Identity (storage
	// dropped the Origin dimension — see uplink_claim.go); the
	// pre-A1 (Identity, Origin)-keyed bucket is gone.
	Accepted uint64

	// AcceptedReplaced counts entries that were admitted by displacing
	// the worst evictable existing entry. Both routeStore.AdmitNew
	// (announce / hop_ack pushed a strictly-better row through a
	// saturated bucket) and routeStore.AdmitDirect (direct registration
	// on a saturated bucket — direct admission cannot be rejected, so
	// the cap evicts to make room) contribute here. Together with
	// Accepted this gives the cap "eviction rate"
	// (AcceptedReplaced / (Accepted + AcceptedReplaced)) signal that
	// the cap value is too tight; the metric slightly over-reads on
	// nodes with frequent direct reconnects against saturated
	// buckets, but that is a real form of cap pressure — the
	// reconnect would have appended a K+1 row without the cap.
	AcceptedReplaced uint64

	// RejectedFull counts entries that were dropped because the bucket
	// was at the cap and the incoming entry was not strictly better
	// than the worst evictable candidate. A rising RejectedFull rate
	// indicates the network is offering more next-hop alternatives than
	// the cap admits — usually fine, but a spike alongside flap activity
	// suggests the cap is too tight to track route churn.
	RejectedFull uint64

	// RejectedAllProtected counts entries dropped because every entry
	// in the saturated bucket was direct or local — none could be
	// evicted by the cap. The bucket is per-Identity UplinkClaim
	// storage (post-Phase-A, no Origin dimension); a direct route by
	// construction has Uplink == Identity, so a single bucket holds
	// at most one direct + one local entry — direct routes do NOT
	// stack across buckets. A non-zero counter is therefore a sanity
	// signal for the K=1 corner case (the single slot is held by the
	// direct/local row and a non-direct arrival has no slot) or for
	// synthetic / test / direct-restore edge cases, NOT a fan-out
	// diagnostic. With K≥2 a non-zero counter usually points at
	// fixture state, not at undersizing the cap.
	RejectedAllProtected uint64

	// SeqNoFlapHoldowns counts how many times the per-Identity SeqNo
	// flap cap engaged hold-down (Phase 1 P2). Hold-down suppresses
	// all wire emits for a single Identity for DefaultSeqHoldDownDuration
	// after the per-Identity outbound-SeqNo advance count crosses
	// MaxSeqAdvancePerWindow inside the SeqAdvanceWindow sliding
	// window. A non-zero counter means at least one upstream lineage
	// drove our outbound SeqNo namespace forward fast enough to trip
	// the cap — usually a count-to-infinity loop, a hostile
	// retransmission storm, or a buggy peer. Together with the
	// per-hold-down warn-level log this is the only externally
	// visible signal that the cap is firing.
	//
	// Counter is monotonic: each hold-down ARMING bumps the count
	// once; subsequent advances during an active hold-down are
	// suppressed (no re-arm during hold-down). Hold-down expiry by
	// TickTTL does not decrement.
	SeqNoFlapHoldowns uint64

	// FastInvalidations counts fast-invalidation events triggered by
	// hops > MaxSaneHops on incoming ingest (Phase 1 P3). The bad
	// claim is recorded as a tombstone at incoming.SeqNo so the
	// SeqNo-resurrection guard for the (Identity, Uplink) pair
	// remains intact, and AnnounceProjectionFor's "only own-direct
	// tombstones emit on the wire" rule (Source == Direct) makes the
	// invalidation strictly local — there is no count-to-infinity
	// withdrawal frame we would forward on the upstream's behalf.
	//
	// Each accepted invalidation bumps the counter once; stale
	// invalidations (incoming.SeqNo <= old.SeqNo) are dropped and
	// do NOT bump. Phase 1 ships a single bucket; Phase 2 will
	// split it into by_hop_limit / by_hop_ack_timeout sub-counters
	// when the hop_ack timeout path lands.
	FastInvalidations uint64
}

// OverloadStats captures cumulative counters for the announce-loop
// overload gate. EngagedCycles counts cycles where the gate
// ACTUALLY shed work — at least one peer was skipped specifically
// because of overload (a delta-due peer suppressed by the gate).
// Cycles where the gate engaged but every peer was forced-due
// (initial sync, periodic forced-full deadline) do NOT count, nor
// do forced-only wakes where no delta cycle was due to begin with.
// The counter is therefore a strict "gate actually saved CPU"
// signal, not a proxy for "host briefly reported overloaded". Zero
// when the gate is not wired or has never shed work. Surfaced by
// fetchRouteSummary under the "overload" JSON object so operators
// can see whether backpressure is firing in production.
//
// Wire-safe value type — copy-by-value, no internal pointers, no
// mutex. Each numeric is monotonically non-decreasing.
type OverloadStats struct {
	// EngagedCycles is the cumulative number of cycles where the
	// OverloadGate actually shed work (at least one delta-due peer
	// was suppressed). Reset to zero on process restart (the
	// underlying counter is in-memory atomic).
	EngagedCycles uint64
}

// FlapEntry describes the flap detection state for a single peer.
// Exported for RPC observability — callers should treat this as read-only.
type FlapEntry struct {
	// PeerIdentity is the Ed25519 fingerprint of the peer.
	PeerIdentity PeerIdentity

	// RecentWithdrawals is the number of disconnect events within the flap window.
	RecentWithdrawals int

	// InHoldDown is true if the peer is currently in hold-down.
	InHoldDown bool

	// HoldDownUntil is the time when hold-down expires. Zero if not in hold-down.
	HoldDownUntil time.Time
}

// isBetter returns true if candidate is a better route than current.
// Source priority wins first (direct > hop_ack > announcement),
// then lower hop count breaks the tie.
func isBetter(candidate, current *RouteEntry) bool {
	if candidate.Source.TrustRank() != current.Source.TrustRank() {
		return candidate.Source.TrustRank() > current.Source.TrustRank()
	}
	return candidate.Hops < current.Hops
}
